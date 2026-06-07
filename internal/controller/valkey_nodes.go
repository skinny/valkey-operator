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
	"strconv"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	valkeyiov1alpha1 "valkey.io/valkey-operator/api/v1alpha1"
)

// reconcileNodes ensures exactly (1 + spec.replicas) ValkeyNodes
// exist. Creates/updates the desired positions and deletes any
// existing ValkeyNode whose index falls outside the new range
// (a scale-down). If any spec mutation lands the function returns
// (true, nil) so the caller requeues.
//
// configHash is the sha256 of the rendered valkey.conf (returned by
// upsertConfigMap). It's plumbed onto every ValkeyNode's
// Spec.ServerConfigHash so the ValkeyNode controller stamps it as a
// pod-template annotation; that's what makes Spec.Config changes
// trigger a rollout. Matches ValkeyCluster's reconcileValkeyNodes.
func (r *ValkeyReconciler) reconcileNodes(ctx context.Context, valkey *valkeyiov1alpha1.Valkey, configHash string) (bool, error) {
	total := 1 + int(valkey.Spec.Replicas)
	for index := range total {
		requeue, _, err := r.reconcileNode(ctx, valkey, index, configHash)
		if err != nil {
			return false, err
		}
		if requeue {
			return true, nil
		}
	}
	if err := r.pruneExtraNodes(ctx, valkey, total); err != nil {
		return false, err
	}
	return false, nil
}

// pruneExtraNodes deletes any ValkeyNode owned by this Valkey whose
// node-index label is >= total (a stale node from a previous higher
// spec.replicas). Best-effort: a stale node without a parseable label
// is left alone for a human to inspect.
func (r *ValkeyReconciler) pruneExtraNodes(ctx context.Context, valkey *valkeyiov1alpha1.Valkey, total int) error {
	list, err := r.listOwnedNodes(ctx, valkey)
	if err != nil {
		return err
	}
	for i := range list.Items {
		n := &list.Items[i]
		idx, err := strconv.Atoi(n.Labels[LabelNodeIndex])
		if err != nil || idx < total {
			continue
		}
		// Deleting node-0 would tear down the primary; we never
		// target it because scale-down floors at replicas: 0 which
		// keeps a single primary (total=1).
		if err := r.Delete(ctx, n); err != nil {
			return err
		}
		r.Recorder.Eventf(valkey, n, corev1.EventTypeNormal,
			"ValkeyNodeDeleted", "ScaleDown",
			"Deleted ValkeyNode %s (spec.replicas shrunk)", n.Name)
	}
	return nil
}

func (r *ValkeyReconciler) reconcileNode(ctx context.Context, valkey *valkeyiov1alpha1.Valkey, index int, configHash string) (bool, bool, error) {
	desired := buildValkeyNode(valkey, index, configHash)
	node := &valkeyiov1alpha1.ValkeyNode{ObjectMeta: metav1.ObjectMeta{
		Name: desired.Name, Namespace: desired.Namespace,
	}}
	result, err := controllerutil.CreateOrUpdate(ctx, r.Client, node, func() error {
		node.Labels = desired.Labels
		node.Spec = desired.Spec
		return controllerutil.SetControllerReference(valkey, node, r.Scheme)
	})
	if err != nil {
		return false, false, err
	}
	switch result {
	case controllerutil.OperationResultCreated:
		r.Recorder.Eventf(valkey, node, corev1.EventTypeNormal, "ValkeyNodeCreated", "Reconcile", "Created ValkeyNode %s", node.Name)
		return false, true, nil
	case controllerutil.OperationResultUpdated:
		return true, false, nil
	}
	// no-op: wait for the node to report ready
	if !node.Status.Ready {
		return true, false, nil
	}
	return false, false, nil
}

// buildValkeyNode constructs the desired ValkeyNode for a position
// inside a Valkey. Data plane only - no sentinel sidecar.
//
// configHash is the sha256 of the rendered valkey.conf. The ValkeyNode
// controller turns this into a pod-template annotation so a Spec.Config
// change on the Valkey CR (which re-renders the ConfigMap and bumps the
// hash) cycles the pods on the next rollout pass. Pass "" only when the
// hash isn't known yet (e.g. a reconcile that erroed out before
// upsertConfigMap returned); the ValkeyNode controller then omits the
// annotation, matching pre-rollout-machinery behavior.
func buildValkeyNode(valkey *valkeyiov1alpha1.Valkey, index int, configHash string) *valkeyiov1alpha1.ValkeyNode {
	l := baseLabels(valkey.Name, "valkey-node")
	for k, v := range valkey.Labels {
		if _, exists := l[k]; !exists {
			l[k] = v
		}
	}
	l[LabelValkey] = valkey.Name
	// LabelCluster is reused as "parent CR name" by downstream resource
	// builders (e.g. the metrics-exporter sidecar derives its password
	// Secret name from this label). Set it to valkey.Name so the same
	// builders work for Valkey-owned ValkeyNodes.
	l[LabelCluster] = valkey.Name
	l[LabelNodeIndex] = strconv.Itoa(index)

	return &valkeyiov1alpha1.ValkeyNode{
		ObjectMeta: metav1.ObjectMeta{
			Name:      valkeyNodeIndexedName(valkey.Name, index),
			Namespace: valkey.Namespace,
			Labels:    l,
		},
		Spec: valkeyiov1alpha1.ValkeyNodeSpec{
			Image:               valkey.Spec.Image,
			WorkloadType:        valkeyiov1alpha1.WorkloadTypeStatefulSet,
			Persistence:         valkey.Spec.Persistence,
			Resources:           valkey.Spec.Resources,
			NodeSelector:        valkey.Spec.NodeSelector,
			Affinity:            valkey.Spec.Affinity,
			Tolerations:         valkey.Spec.Tolerations,
			Exporter:            valkey.Spec.Exporter,
			Containers:          valkey.Spec.Containers,
			ServerConfigMapName: GetServerConfigMapName(valkey.Name),
			ServerConfigHash:    configHash,
			UsersACLSecretName:  getInternalSecretName(valkey.Name),
			TLS:                 valkey.Spec.TLS,
			// Replication topology lives in the REPLICAOF directive
			// CONFIG REWRITE writes into valkey.conf. Without a
			// persistent writable-config volume that file is wiped on
			// every pod cycle, causing replicas to boot as masters
			// until the operator re-wires them - a transient
			// multi-master window the user can observe as a Degraded
			// blip during memory/spec changes. ValkeyCluster nodes do
			// NOT need this because cluster mode persists its role in
			// nodes.conf on the data PVC.
			PersistWritableConfig: true,
		},
	}
}
