/*
Copyright 2025 Valkey Contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"strings"

	vclient "github.com/valkey-io/valkey-go"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	klabels "k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	valkeyiov1alpha1 "valkey.io/valkey-operator/api/v1alpha1"
	"valkey.io/valkey-operator/internal/sentinel"
)

// reconcileMonitoring is the core of the ValkeySentinel reconciler:
// for every Valkey matching spec.valkeySelector, point SENTINEL MONITOR
// at any reachable data pod and apply spec.config + auth via SENTINEL
// SET. For masters that no longer match the selector, issue SENTINEL
// REMOVE.
//
// Returns the sorted list of master names currently monitored.
func (r *ValkeySentinelReconciler) reconcileMonitoring(ctx context.Context, s *valkeyiov1alpha1.ValkeySentinel) ([]string, error) {
	log := logf.FromContext(ctx)

	// 1. Open one client per reachable sentinel pod.
	sentinelClients, err := r.dialSentinelPods(ctx, s)
	if err != nil {
		return nil, err
	}
	defer func() {
		for _, c := range sentinelClients {
			c.Close()
		}
	}()
	if len(sentinelClients) == 0 {
		return nil, fmt.Errorf("no sentinel pods reachable")
	}

	// 2. Find matched Valkeys.
	matched, err := r.matchedValkeys(ctx, s)
	if err != nil {
		return nil, err
	}
	matchedNames := map[string]bool{}
	for _, v := range matched {
		matchedNames[v.Name] = true
	}

	// 3. Discover what each sentinel already monitors. Union across all
	//    reachable pods.
	currentlyMonitored := map[string]bool{}
	for _, c := range sentinelClients {
		names, err := c.Masters(ctx)
		if err != nil {
			log.V(1).Info("SENTINEL MASTERS failed", "addr", c.Addr(), "err", err)
			continue
		}
		for _, n := range names {
			currentlyMonitored[n] = true
		}
	}

	// 4. For each matched Valkey, MONITOR + SET on every sentinel that
	//    doesn't already know about this master (or always, to ensure
	//    SET values catch up).
	quorum := int(s.Spec.EffectiveQuorum())
	monitored := []string{}
	for _, v := range matched {
		authUser, authPass, err := r.readSentinelAuth(ctx, v)
		if err != nil {
			log.V(1).Info("sentinel-auth secret unavailable; skipping", "valkey", v.Name, "err", err)
			continue
		}
		entryIP, err := r.pickMasterEntryIP(ctx, v, authUser, authPass)
		if err != nil {
			log.V(1).Info("no reachable data pod yet; skipping", "valkey", v.Name, "err", err)
			continue
		}
		for _, c := range sentinelClients {
			// Per the standard Sentinel model, MONITOR is a one-shot
			// registration. Once a sentinel knows about a master, the
			// operator does not override its view - sentinel manages
			// topology changes (failover, replica discovery, etc.)
			// itself via INFO + gossip. The operator only sets the
			// initial IP and the tuning knobs.
			//
			// SET is also one-shot. Every SENTINEL SET triggers a
			// CONFIG REWRITE + fsync on the sentinel, and chatter
			// from the operator during a failover can starve the
			// sentinel timer enough to trip TILT mode. Treat the
			// initial MONITOR as the only time we push auth + config.
			if currentlyMonitored[v.Name] {
				continue
			}
			if err := c.Monitor(ctx, v.Name, entryIP, DefaultPort, quorum); err != nil {
				log.V(1).Info("SENTINEL MONITOR failed", "addr", c.Addr(), "valkey", v.Name, "err", err)
				continue
			}
			if err := c.Set(ctx, v.Name, "auth-user", authUser); err != nil {
				log.V(1).Info("SENTINEL SET auth-user failed", "addr", c.Addr(), "valkey", v.Name, "err", err)
			}
			if err := c.Set(ctx, v.Name, "auth-pass", authPass); err != nil {
				log.V(1).Info("SENTINEL SET auth-pass failed", "addr", c.Addr(), "valkey", v.Name, "err", err)
			}
			for k, val := range s.Spec.Config {
				_ = c.Set(ctx, v.Name, k, val)
			}
		}
		monitored = append(monitored, v.Name)
	}

	// 5. For masters the sentinels know about that no longer match,
	//    REMOVE immediately (no grace period per design).
	for name := range currentlyMonitored {
		if matchedNames[name] {
			continue
		}
		for _, c := range sentinelClients {
			if err := c.Remove(ctx, name); err != nil {
				log.V(1).Info("SENTINEL REMOVE failed", "addr", c.Addr(), "master", name, "err", err)
			}
		}
		r.Recorder.Eventf(s, nil, corev1.EventTypeNormal, "MasterUnmonitored", "ReconcileMonitoring",
			"Stopped monitoring %q (no longer matched by selector)", name)
	}

	return monitored, nil
}

// dialSentinelPods opens a sentinel client to each pod labelled for
// this ValkeySentinel. Pods without a podIP are skipped.
func (r *ValkeySentinelReconciler) dialSentinelPods(ctx context.Context, s *valkeyiov1alpha1.ValkeySentinel) ([]*sentinel.Client, error) {
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods,
		client.InNamespace(s.Namespace),
		client.MatchingLabels{LabelSentinel: s.Name},
	); err != nil {
		return nil, err
	}
	out := []*sentinel.Client{}
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.Status.PodIP == "" {
			continue
		}
		c, err := sentinel.New(p.Status.PodIP, sentinel.Port, nil)
		if err != nil {
			continue
		}
		out = append(out, c)
	}
	return out, nil
}

// matchedValkeys lists Valkeys in the same namespace whose labels
// satisfy spec.valkeySelector.
func (r *ValkeySentinelReconciler) matchedValkeys(ctx context.Context, s *valkeyiov1alpha1.ValkeySentinel) ([]*valkeyiov1alpha1.Valkey, error) {
	selector, err := metav1.LabelSelectorAsSelector(&s.Spec.ValkeySelector)
	if err != nil {
		return nil, fmt.Errorf("invalid valkeySelector: %w", err)
	}
	if selector.Empty() {
		return nil, nil
	}
	list := &valkeyiov1alpha1.ValkeyList{}
	if err := r.List(ctx, list, client.InNamespace(s.Namespace)); err != nil {
		return nil, err
	}
	out := []*valkeyiov1alpha1.Valkey{}
	for i := range list.Items {
		v := &list.Items[i]
		if v.Spec.Replicas == 0 {
			continue // standalone Valkey - nothing to fail over
		}
		if selector.Matches(klabels.Set(v.Labels)) {
			out = append(out, v)
		}
	}
	return out, nil
}

// pickMasterEntryIP picks the IP of the data pod currently reporting
// role:master, so SENTINEL MONITOR is given the actual primary. In
// practice Sentinel does NOT gracefully follow master_host when handed
// a replica IP - it marks the configured master s_down and gets stuck.
// We probe each pod's role directly via INFO replication using the
// _sentinel credentials (no read of Valkey.status; this is independent
// observation, not cross-CR state reliance).
//
// Falls back to any reachable pod IP if none reports role:master
// (e.g. transient state during initial bring-up). Sentinel will mark
// the master s_down briefly until a primary settles, which is
// preferable to refusing to issue MONITOR at all.
func (r *ValkeySentinelReconciler) pickMasterEntryIP(ctx context.Context, v *valkeyiov1alpha1.Valkey, authUser, authPass string) (string, error) {
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods,
		client.InNamespace(v.Namespace),
		client.MatchingLabels{LabelValkey: v.Name},
	); err != nil {
		return "", err
	}
	var fallback string
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.Status.PodIP == "" {
			continue
		}
		if fallback == "" {
			fallback = p.Status.PodIP
		}
		role, err := dataPodRole(ctx, p.Status.PodIP, authUser, authPass)
		if err != nil {
			continue
		}
		if role == "master" {
			return p.Status.PodIP, nil
		}
	}
	if fallback != "" {
		return fallback, nil
	}
	return "", fmt.Errorf("no data pod has a PodIP yet")
}

// dataPodRole returns the value of the `role` line in INFO replication
// (either "master" or "slave"). Authenticates with the _sentinel user
// since that's the only credential the sentinel controller holds.
func dataPodRole(ctx context.Context, ip, user, password string) (string, error) {
	c, err := vclient.NewClient(vclient.ClientOption{
		InitAddress:       []string{fmt.Sprintf("%s:%d", ip, DefaultPort)},
		ForceSingleClient: true,
		Username:          user,
		Password:          password,
	})
	if err != nil {
		return "", err
	}
	defer c.Close()
	raw, err := c.Do(ctx, c.B().Info().Section("replication").Build()).ToString()
	if err != nil {
		return "", err
	}
	for line := range strings.SplitSeq(raw, "\n") {
		line = strings.TrimSpace(line)
		if k, val, ok := strings.Cut(line, ":"); ok && k == "role" {
			return strings.TrimSpace(val), nil
		}
	}
	return "", nil
}

// readSentinelAuth reads the per-Valkey <name>-sentinel-auth Secret.
func (r *ValkeySentinelReconciler) readSentinelAuth(ctx context.Context, v *valkeyiov1alpha1.Valkey) (string, string, error) {
	secret := &corev1.Secret{}
	err := r.Get(ctx, client.ObjectKey{Name: v.Name + "-sentinel-auth", Namespace: v.Namespace}, secret)
	if err != nil {
		return "", "", err
	}
	return string(secret.Data["username"]), string(secret.Data["password"]), nil
}
