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

// collectMonitorConfigs builds the per-Valkey monitor blocks that go
// into the sentinel.conf template and the aggregated auth Secret.
//
// A Valkey is included only when:
//   - it matches spec.valkeySelector,
//   - it is replicated (spec.replicas > 0),
//   - its sentinel-auth Secret exists,
//   - the Valkey controller has populated status.primaryEndpoint.
//
// Excluding a Valkey here means the sentinels boot without a `sentinel
// monitor` line for it; when the Valkey controller observes a primary,
// status.primaryEndpoint is set, the watch fires this reconciler, and
// the template + Secret are re-rendered. The hash annotation on the
// StatefulSet pod template then triggers a controlled rolling restart
// so the new config reaches the sentinel pods.
func (r *ValkeySentinelReconciler) collectMonitorConfigs(ctx context.Context, s *valkeyiov1alpha1.ValkeySentinel) ([]monitoredValkey, map[string]string, error) {
	matched, err := r.matchedValkeys(ctx, s)
	if err != nil {
		return nil, nil, err
	}
	log := logf.FromContext(ctx)
	monitors := make([]monitoredValkey, 0, len(matched))
	passwords := make(map[string]string, len(matched))
	quorum := s.Spec.EffectiveQuorum()
	for _, v := range matched {
		if v.Status.PrimaryEndpoint == nil || v.Status.PrimaryEndpoint.IP == "" {
			log.V(1).Info("skipping Valkey without observed primary; will reconcile when status.primaryEndpoint is set", "valkey", v.Name)
			continue
		}
		user, pass, err := r.readSentinelAuth(ctx, v)
		if err != nil {
			log.V(1).Info("sentinel-auth secret unavailable; skipping", "valkey", v.Name, "err", err)
			continue
		}
		monitors = append(monitors, monitoredValkey{
			Name:     v.Name,
			IP:       v.Status.PrimaryEndpoint.IP,
			Port:     v.Status.PrimaryEndpoint.Port,
			Quorum:   quorum,
			Username: user,
			Config:   s.Spec.Config,
		})
		passwords[v.Name] = pass
	}
	return monitors, passwords, nil
}

// reconcileMonitoring drives the SENTINEL REMOVE pass for masters the
// sentinels still know about but the selector no longer matches.
//
// MONITOR + SET no longer happen over the wire in steady state: the
// ConfigMap template carries the full per-Valkey block (monitor + auth
// + tuning) and every sentinel pod reads it on boot. This avoids the
// burst of CONFIG REWRITE writes during startup that previously trip
// SENTINEL_TILT_TRIGGER.
//
// Returns the sorted list of Valkeys that are currently configured for
// monitoring (i.e. baked into the template) so callers can surface it
// on .status.
func (r *ValkeySentinelReconciler) reconcileMonitoring(ctx context.Context, s *valkeyiov1alpha1.ValkeySentinel, monitors []monitoredValkey) ([]string, error) {
	log := logf.FromContext(ctx)

	matchedNames := map[string]bool{}
	for _, m := range monitors {
		matchedNames[m.Name] = true
	}

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
		// Sentinels not yet reachable; the template is already
		// up-to-date so the next reconcile will pick up REMOVE work
		// when pods are ready.
		out := make([]string, 0, len(monitors))
		for _, m := range monitors {
			out = append(out, m.Name)
		}
		return out, nil
	}

	// Sweep stale masters: anything a sentinel still knows about that
	// is not in the current matched set must be REMOVEd, because the
	// template-only model can't subtract a master from a running
	// sentinel's view (the pod has to either restart or be told).
	stale := map[string]bool{}
	for _, c := range sentinelClients {
		names, err := c.Masters(ctx)
		if err != nil {
			log.V(1).Info("SENTINEL MASTERS failed", "addr", c.Addr(), "err", err)
			continue
		}
		for _, n := range names {
			if !matchedNames[n] {
				stale[n] = true
			}
		}
	}
	for name := range stale {
		for _, c := range sentinelClients {
			if err := c.Remove(ctx, name); err != nil {
				log.V(1).Info("SENTINEL REMOVE failed", "addr", c.Addr(), "master", name, "err", err)
			}
		}
		r.Recorder.Eventf(s, nil, corev1.EventTypeNormal, "MasterUnmonitored", "ReconcileMonitoring",
			"Stopped monitoring %q (no longer matched by selector)", name)
	}

	out := make([]string, 0, len(monitors))
	for _, m := range monitors {
		out = append(out, m.Name)
	}
	return out, nil
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

// readSentinelAuth reads the per-Valkey sentinel-auth Secret that the
// Valkey controller projects (see projectSentinelAuthSecret).
func (r *ValkeySentinelReconciler) readSentinelAuth(ctx context.Context, v *valkeyiov1alpha1.Valkey) (string, string, error) {
	secret := &corev1.Secret{}
	err := r.Get(ctx, client.ObjectKey{Name: valkeySentinelAuthSecretName(v), Namespace: v.Namespace}, secret)
	if err != nil {
		return "", "", err
	}
	return string(secret.Data["username"]), string(secret.Data["password"]), nil
}
