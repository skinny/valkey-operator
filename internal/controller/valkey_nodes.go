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

// reconcileNodes ensures one ValkeyNode CR exists per pod position
// (1 primary + spec.replicas replicas). Updates are applied one at a
// time; if any spec mutation lands, the function returns (true, nil)
// so the caller requeues.
func (r *ValkeyReconciler) reconcileNodes(ctx context.Context, valkey *valkeyiov1alpha1.Valkey) (bool, error) {
	total := 1 + int(valkey.Spec.Replicas)
	for index := range total {
		requeue, _, err := r.reconcileNode(ctx, valkey, index)
		if err != nil {
			return false, err
		}
		if requeue {
			return true, nil
		}
	}
	return false, nil
}

func (r *ValkeyReconciler) reconcileNode(ctx context.Context, valkey *valkeyiov1alpha1.Valkey, index int) (bool, bool, error) {
	desired := buildValkeyNode(valkey, index)
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
func buildValkeyNode(valkey *valkeyiov1alpha1.Valkey, index int) *valkeyiov1alpha1.ValkeyNode {
	l := baseLabels(valkey.Name, "valkey-node")
	for k, v := range valkey.Labels {
		if _, exists := l[k]; !exists {
			l[k] = v
		}
	}
	l[LabelValkey] = valkey.Name
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
			UsersACLSecretName:  getInternalSecretName(valkey.Name),
			TLS:                 valkey.Spec.TLS,
		},
	}
}
