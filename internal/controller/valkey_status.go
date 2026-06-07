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
	"reflect"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	valkeyiov1alpha1 "valkey.io/valkey-operator/api/v1alpha1"
)

// observePrimary returns (primaryPodName, primaryHost, masters).
// primaryHost is the per-ValkeyNode Service DNS name, NOT a pod IP -
// sentinels and other consumers monitor by this name so pod restarts
// (which change the pod IP) don't leave stale known-replica entries.
//
// masters is the list of every node that reported role:master at probe
// time. The caller uses it to surface split-brain as Degraded.
//
// Stabilization: when more than one node reports role:master, the
// function prefers previousPrimary if it is still among the observed
// masters. This is what keeps PrimaryEndpoint stable across an
// impostor-master situation (replica cycled, came back as master) and
// what gives wireScaledUpReplicas a stable real-primary identity to
// REPLICAOF the impostor back to. When previousPrimary is empty or no
// longer reports role:master, the function returns ("", "", masters);
// the caller preserves the prior PrimaryEndpoint and surfaces Degraded.
//
// Detection probes data pods by IP for INFO replication; the returned
// identifier is the per-pod headless Service hostname.
func (r *ValkeyReconciler) observePrimary(ctx context.Context, valkey *valkeyiov1alpha1.Valkey, nodes *valkeyiov1alpha1.ValkeyNodeList, previousPrimary string) (string, string, []string) {
	if valkey.Spec.Replicas == 0 {
		// Standalone: the single node is always the "primary".
		for i := range nodes.Items {
			n := &nodes.Items[i]
			if n.Labels[LabelNodeIndex] == "0" && n.Status.Ready {
				return n.Status.PodName, valkeyNodeServiceHost(n), nil
			}
		}
		return "", "", nil
	}

	password, err := fetchSystemUserPassword(ctx, operatorUser, r.Client, valkey.Name, valkey.Namespace)
	if err != nil {
		return "", "", nil
	}
	var seen []primaryCandidate
	for i := range nodes.Items {
		n := &nodes.Items[i]
		if n.Status.PodIP == "" || !n.Status.Ready {
			continue
		}
		info, err := infoReplication(ctx, n.Status.PodIP, password)
		if err != nil {
			continue
		}
		if strings.TrimSpace(info["role"]) == RoleMaster {
			seen = append(seen, primaryCandidate{
				PodName: n.Status.PodName,
				Host:    valkeyNodeServiceHost(n),
			})
		}
	}
	masters := make([]string, 0, len(seen))
	for _, m := range seen {
		masters = append(masters, m.PodName)
	}
	name, host := pickStablePrimary(seen, previousPrimary)
	return name, host, masters
}

// primaryCandidate is the (podName, host) pair pickStablePrimary
// chooses among. observePrimary collects one of these for every node
// that reports role:master at probe time.
type primaryCandidate struct {
	PodName string
	Host    string
}

// pickStablePrimary is the pure decision logic for "which pod do we
// surface as the current primary?", broken out so the stabilization
// behavior can be unit-tested without faking INFO replication.
//
// Rules:
//   - 0 candidates: return ("", ""); caller treats as "no primary
//     observed yet" and waits.
//   - 1 candidate: return it; unambiguous.
//   - >1 candidates AND previousPrimary is among them: prefer
//     previousPrimary. Keeps PrimaryEndpoint stable across an impostor
//     situation; wireScaledUpReplicas uses this stable identity to
//     REPLICAOF the impostor back.
//   - >1 candidates AND previousPrimary unset or not among them: return
//     ("", ""); caller preserves the prior PrimaryEndpoint and flags
//     SplitBrain Degraded for human resolution.
func pickStablePrimary(candidates []primaryCandidate, previousPrimary string) (name, host string) {
	switch len(candidates) {
	case 0:
		return "", ""
	case 1:
		return candidates[0].PodName, candidates[0].Host
	default:
		if previousPrimary != "" {
			for _, c := range candidates {
				if c.PodName == previousPrimary {
					return c.PodName, c.Host
				}
			}
		}
		return "", ""
	}
}

// deriveConditions sets Ready / Progressing / PrimaryElected /
// Monitored / Degraded based on observed state.
//
// masters is the full list of pods that reported role:master at
// observation time (see observePrimary). Split-brain (len > 1) is
// surfaced as Degraded with reason SplitBrain; the resulting State is
// Degraded even when other conditions would otherwise be Ready.
func (r *ValkeyReconciler) deriveConditions(valkey *valkeyiov1alpha1.Valkey, primaryName string, masters []string, monitoredBy []string) {
	// Monitored: informational, not gating Ready.
	if len(monitoredBy) > 0 {
		r.setCondition(valkey, valkeyiov1alpha1.ConditionMonitored, "SentinelSelecting",
			"Selected by ValkeySentinel(s): "+strings.Join(monitoredBy, ", "),
			metav1.ConditionTrue)
	} else if valkey.Spec.Replicas > 0 {
		r.setCondition(valkey, valkeyiov1alpha1.ConditionMonitored, "NoSentinelSelecting",
			"Replicated Valkey has no failover; apply a ValkeySentinel that selects this Valkey",
			metav1.ConditionFalse)
	}

	// Degraded gating. SplitBrain wins over PrimaryLost because the
	// data plane has *some* primaries (just too many) and the user
	// needs to know which to keep before anything else is safe. The
	// caller sets this condition early (before executeRollout) via
	// setSplitBrainCondition; this branch re-asserts it as part of the
	// final status. setSplitBrainCondition is the single source of
	// truth for the wording and the State transition.
	if len(masters) > 1 {
		setSplitBrainCondition(r, valkey, masters)
		return
	}
	// Preserve any Degraded set by the caller (e.g. MultiplyMonitored
	// sets Degraded earlier in Reconcile). Only clear it when no
	// other path is actively flagging degradation.
	if meta.IsStatusConditionTrue(valkey.Status.Conditions, valkeyiov1alpha1.ConditionMultiplyMonitored) {
		return
	}
	r.setCondition(valkey, valkeyiov1alpha1.ConditionDegraded, "NoDegradation",
		"No degraded state detected", metav1.ConditionFalse)

	if primaryName == "" {
		// No primary found. Distinguish three sub-cases:
		//   * Failover in progress: a SENTINEL FAILOVER was just
		//     issued and Sentinel's internal handoff briefly leaves
		//     every pod reporting role:replica (old master demoted
		//     before new master is promoted). The next reconcile
		//     within ~seconds will see the new primary; surface
		//     "FailoverInProgress" so kubectl describe doesn't
		//     look like an outage.
		//   * Sentinel-monitored with no recent failover: still
		//     transient, but not user-actionable; we wait.
		//   * Replicated, no sentinel selecting, no recent failover:
		//     PrimaryLost - a human must intervene.
		reason := "ProvisioningNodes"
		msg := "Waiting for the data plane to come up"
		switch {
		case failoverRecentlyIssued(valkey):
			reason = "FailoverInProgress"
			msg = "Sentinel failover issued; awaiting promotion of the new primary"
		case valkey.Spec.Replicas > 0 && len(monitoredBy) == 0:
			reason = "PrimaryLost"
			msg = "No pod reports role:master and no sentinel is monitoring; manual intervention required"
		}
		r.setCondition(valkey, valkeyiov1alpha1.ConditionReady, reason, msg, metav1.ConditionFalse)
		r.setCondition(valkey, valkeyiov1alpha1.ConditionProgressing, reason, msg, metav1.ConditionTrue)
		valkey.Status.State = valkeyiov1alpha1.ClusterStateReconciling
		valkey.Status.Reason = reason
		valkey.Status.Message = msg
		return
	}

	r.setCondition(valkey, valkeyiov1alpha1.ConditionPrimaryElected, "PrimaryElected",
		"Primary is "+primaryName, metav1.ConditionTrue)
	r.setCondition(valkey, valkeyiov1alpha1.ConditionReady, "ClusterHealthy",
		"Valkey is healthy", metav1.ConditionTrue)
	r.setCondition(valkey, valkeyiov1alpha1.ConditionProgressing, "ReconcileComplete",
		"No changes needed", metav1.ConditionFalse)
	valkey.Status.State = valkeyiov1alpha1.ClusterStateReady
	valkey.Status.Reason = "ClusterHealthy"
	valkey.Status.Message = "Valkey is healthy"
}

// updateStatus patches the status subresource, no-op when unchanged.
func (r *ValkeyReconciler) updateStatus(ctx context.Context, valkey *valkeyiov1alpha1.Valkey) error {
	current := &valkeyiov1alpha1.Valkey{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(valkey), current); err != nil {
		return err
	}
	patchBase := current.DeepCopy()

	// Carry over State derived from conditions if not set explicitly.
	if valkey.Status.State == "" {
		switch {
		case meta.IsStatusConditionTrue(valkey.Status.Conditions, valkeyiov1alpha1.ConditionReady):
			valkey.Status.State = valkeyiov1alpha1.ClusterStateReady
		case meta.IsStatusConditionTrue(valkey.Status.Conditions, valkeyiov1alpha1.ConditionProgressing):
			valkey.Status.State = valkeyiov1alpha1.ClusterStateReconciling
		}
	}

	current.Status = valkey.Status
	if reflect.DeepEqual(patchBase.Status, current.Status) {
		return nil
	}
	return r.Status().Patch(ctx, current, client.MergeFrom(patchBase))
}

// setSplitBrainCondition writes the SplitBrain Degraded state. Single
// source of truth for the wording and State transition so the
// "set early, before executeRollout" call (in Reconcile) and the
// "re-affirm in deriveConditions" call agree.
func setSplitBrainCondition(r *ValkeyReconciler, valkey *valkeyiov1alpha1.Valkey, masters []string) {
	sorted := append([]string(nil), masters...)
	sort.Strings(sorted)
	msg := "Multiple pods report role:master: " + strings.Join(sorted, ", ") +
		". Refusing to update status.primaryEndpoint or progress the rollout until a single primary is observed; manual intervention required."
	r.setCondition(valkey, valkeyiov1alpha1.ConditionDegraded, "SplitBrain", msg, metav1.ConditionTrue)
	r.setCondition(valkey, valkeyiov1alpha1.ConditionReady, "SplitBrain", msg, metav1.ConditionFalse)
	r.setCondition(valkey, valkeyiov1alpha1.ConditionProgressing, "SplitBrain", msg, metav1.ConditionFalse)
	valkey.Status.State = valkeyiov1alpha1.ClusterStateDegraded
	valkey.Status.Reason = "SplitBrain"
	valkey.Status.Message = msg
}
