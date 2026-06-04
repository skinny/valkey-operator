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
		entryIP, err := r.pickEntryIP(ctx, v)
		if err != nil {
			log.V(1).Info("no reachable data pod yet; skipping", "valkey", v.Name, "err", err)
			continue
		}
		authUser, authPass, err := r.readSentinelAuth(ctx, v)
		if err != nil {
			log.V(1).Info("sentinel-auth secret unavailable; skipping", "valkey", v.Name, "err", err)
			continue
		}
		for _, c := range sentinelClients {
			if !currentlyMonitored[v.Name] {
				if err := c.Monitor(ctx, v.Name, entryIP, DefaultPort, quorum); err != nil {
					log.V(1).Info("SENTINEL MONITOR failed", "addr", c.Addr(), "valkey", v.Name, "err", err)
					continue
				}
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

// pickEntryIP picks any reachable data pod IP for SENTINEL MONITOR.
// Sentinel itself follows INFO replication from there to find the
// actual master. We do NOT read Valkey.status.primaryPodName.
func (r *ValkeySentinelReconciler) pickEntryIP(ctx context.Context, v *valkeyiov1alpha1.Valkey) (string, error) {
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods,
		client.InNamespace(v.Namespace),
		client.MatchingLabels{LabelValkey: v.Name},
	); err != nil {
		return "", err
	}
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.Status.PodIP != "" {
			return p.Status.PodIP, nil
		}
	}
	return "", fmt.Errorf("no data pod has a PodIP yet")
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
