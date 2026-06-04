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
	"strings"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	valkeyiov1alpha1 "valkey.io/valkey-operator/api/v1alpha1"
)

// observePrimary returns (primaryPodName, primaryIP). When sentinels
// are reachable, asks them via SENTINEL get-master-addr-by-name.
// Otherwise falls back to per-pod INFO replication, picking the pod
// reporting role:master.
func (r *ValkeyReconciler) observePrimary(ctx context.Context, valkey *valkeyiov1alpha1.Valkey, nodes *valkeyiov1alpha1.ValkeyNodeList) (string, string) {
	if valkey.Spec.Replicas == 0 {
		// Standalone: the single node is always the "primary".
		for i := range nodes.Items {
			if nodes.Items[i].Labels[LabelNodeIndex] == "0" && nodes.Items[i].Status.Ready {
				return nodes.Items[i].Status.PodName, nodes.Items[i].Status.PodIP
			}
		}
		return "", ""
	}

	password, err := fetchSystemUserPassword(ctx, operatorUser, r.Client, valkey.Name, valkey.Namespace)
	if err != nil {
		return "", ""
	}
	for i := range nodes.Items {
		n := &nodes.Items[i]
		if n.Status.PodIP == "" || !n.Status.Ready {
			continue
		}
		info, err := infoReplication(ctx, n.Status.PodIP, password)
		if err != nil {
			continue
		}
		if strings.TrimSpace(info["role"]) == "master" {
			return n.Status.PodName, n.Status.PodIP
		}
	}
	return "", ""
}

// deriveConditions sets Ready / Progressing / PrimaryElected /
// Monitored based on observed state.
func (r *ValkeyReconciler) deriveConditions(valkey *valkeyiov1alpha1.Valkey, primaryName string, monitoredBy []string) {
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

	if primaryName == "" {
		// No primary found. If we have replicas and no sentinel, that's PrimaryLost.
		reason := "ProvisioningNodes"
		msg := "Waiting for the data plane to come up"
		if valkey.Spec.Replicas > 0 && len(monitoredBy) == 0 {
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
