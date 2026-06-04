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
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	klabels "k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	valkeyiov1alpha1 "valkey.io/valkey-operator/api/v1alpha1"
)

// ValkeyReconciler reconciles a Valkey object.
//
// Pipeline:
//
//  1. Provision the data plane (Service, PDB, ConfigMap, ACL Secret,
//     `<name>-sentinel-auth` Secret).
//  2. Reconcile (1 + spec.replicas) ValkeyNodes, one at a time.
//  3. Bootstrap initial REPLICAOF wiring (one-shot; once any replica
//     reports a master_link, the controller stops issuing REPLICAOF).
//  4. List selecting ValkeySentinels and observe the current primary
//     (sentinel-aware when monitored, INFO replication otherwise).
//  5. Update status with primaryPodName, monitoredBy, conditions.
//
// The controller does NOT perform failover. A replicated Valkey with
// no selecting ValkeySentinel has manual-failover semantics by design.
type ValkeyReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder events.EventRecorder
}

// +kubebuilder:rbac:groups=valkey.io,resources=valkeys,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=valkey.io,resources=valkeys/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=valkey.io,resources=valkeys/finalizers,verbs=update
// +kubebuilder:rbac:groups=valkey.io,resources=valkeynodes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=valkey.io,resources=valkeysentinels,verbs=get;list;watch

func (r *ValkeyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	log.V(1).Info("reconciling Valkey")

	valkey := &valkeyiov1alpha1.Valkey{}
	if err := r.Get(ctx, req.NamespacedName, valkey); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if err := r.upsertDataService(ctx, valkey); err != nil {
		r.setCondition(valkey, valkeyiov1alpha1.ConditionReady, "ServiceError", err.Error(), metav1.ConditionFalse)
		_ = r.updateStatus(ctx, valkey)
		return ctrl.Result{}, err
	}
	if err := r.reconcilePDB(ctx, valkey); err != nil {
		r.setCondition(valkey, valkeyiov1alpha1.ConditionReady, "PDBError", err.Error(), metav1.ConditionFalse)
		_ = r.updateStatus(ctx, valkey)
		return ctrl.Result{}, err
	}
	if err := r.upsertConfigMap(ctx, valkey); err != nil {
		r.setCondition(valkey, valkeyiov1alpha1.ConditionReady, "ConfigMapError", err.Error(), metav1.ConditionFalse)
		_ = r.updateStatus(ctx, valkey)
		return ctrl.Result{}, err
	}
	if err := r.reconcileACL(ctx, valkey); err != nil {
		r.setCondition(valkey, valkeyiov1alpha1.ConditionReady, "ACLError", err.Error(), metav1.ConditionFalse)
		_ = r.updateStatus(ctx, valkey)
		return ctrl.Result{}, err
	}

	// Observe selecting ValkeySentinels early so status.monitoredBy is
	// populated even while ValkeyNodes are still being provisioned.
	monitoredBy, err := r.listSelectingSentinels(ctx, valkey)
	if err != nil {
		return ctrl.Result{}, err
	}
	valkey.Status.MonitoredBy = monitoredBy
	if len(monitoredBy) > 1 {
		r.Recorder.Eventf(valkey, nil, corev1.EventTypeWarning,
			"MultipleSentinelsSelecting", "Observe",
			"More than one ValkeySentinel selects this Valkey: %v - last-applied SENTINEL SET wins", monitoredBy)
	}

	if requeue, err := r.reconcileNodes(ctx, valkey); err != nil {
		r.setCondition(valkey, valkeyiov1alpha1.ConditionReady, "ValkeyNodeError", err.Error(), metav1.ConditionFalse)
		_ = r.updateStatus(ctx, valkey)
		return ctrl.Result{}, err
	} else if requeue {
		r.setCondition(valkey, valkeyiov1alpha1.ConditionReady, "UpdatingNodes", "ValkeyNodes are updating", metav1.ConditionFalse)
		r.setCondition(valkey, valkeyiov1alpha1.ConditionProgressing, "UpdatingNodes", "ValkeyNodes are updating", metav1.ConditionTrue)
		_ = r.updateStatus(ctx, valkey)
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}

	nodes, err := r.listOwnedNodes(ctx, valkey)
	if err != nil {
		return ctrl.Result{}, err
	}

	if valkey.Spec.Replicas > 0 {
		if err := r.bootstrapReplication(ctx, valkey, nodes); err != nil {
			r.setCondition(valkey, valkeyiov1alpha1.ConditionReady, "BootstrapError", err.Error(), metav1.ConditionFalse)
			_ = r.updateStatus(ctx, valkey)
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
	}

	primaryName, primaryIP := r.observePrimary(ctx, valkey, nodes)
	valkey.Status.PrimaryPodName = primaryName
	valkey.Status.ReadyReplicas = countReplicas(nodes, primaryIP)

	r.deriveConditions(valkey, primaryName, monitoredBy)
	if err := r.updateStatus(ctx, valkey); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

// listOwnedNodes lists every ValkeyNode owned by this Valkey.
func (r *ValkeyReconciler) listOwnedNodes(ctx context.Context, valkey *valkeyiov1alpha1.Valkey) (*valkeyiov1alpha1.ValkeyNodeList, error) {
	list := &valkeyiov1alpha1.ValkeyNodeList{}
	err := r.List(ctx, list,
		client.InNamespace(valkey.Namespace),
		client.MatchingLabels(map[string]string{LabelValkey: valkey.Name}),
	)
	return list, err
}

// listSelectingSentinels returns the sorted names of every
// ValkeySentinel in the same namespace whose spec.valkeySelector
// matches this Valkey's labels.
func (r *ValkeyReconciler) listSelectingSentinels(ctx context.Context, valkey *valkeyiov1alpha1.Valkey) ([]string, error) {
	sentinels := &valkeyiov1alpha1.ValkeySentinelList{}
	if err := r.List(ctx, sentinels, client.InNamespace(valkey.Namespace)); err != nil {
		return nil, err
	}
	out := []string{}
	myLabels := klabels.Set(valkey.Labels)
	for i := range sentinels.Items {
		s := &sentinels.Items[i]
		selector, err := metav1.LabelSelectorAsSelector(&s.Spec.ValkeySelector)
		if err != nil || selector.Empty() {
			continue
		}
		if selector.Matches(myLabels) {
			out = append(out, s.Name)
		}
	}
	sort.Strings(out)
	return out, nil
}

// findSelectedValkeysForSentinel is the watch handler that re-enqueues
// every Valkey matching the changed ValkeySentinel's selector.
func (r *ValkeyReconciler) findSelectedValkeysForSentinel(ctx context.Context, obj client.Object) []reconcile.Request {
	sentinel, ok := obj.(*valkeyiov1alpha1.ValkeySentinel)
	if !ok {
		return nil
	}
	valkeys := &valkeyiov1alpha1.ValkeyList{}
	if err := r.List(ctx, valkeys, client.InNamespace(sentinel.Namespace)); err != nil {
		return nil
	}
	selector, err := metav1.LabelSelectorAsSelector(&sentinel.Spec.ValkeySelector)
	if err != nil {
		return nil
	}
	var out []reconcile.Request
	for i := range valkeys.Items {
		v := &valkeys.Items[i]
		if selector.Matches(klabels.Set(v.Labels)) {
			out = append(out, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(v)})
		}
	}
	return out
}

func (r *ValkeyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&valkeyiov1alpha1.Valkey{}).
		Owns(&valkeyiov1alpha1.ValkeyNode{}).
		Watches(
			&valkeyiov1alpha1.ValkeySentinel{},
			handler.EnqueueRequestsFromMapFunc(r.findSelectedValkeysForSentinel),
		).
		Named("valkey").
		Complete(r)
}

// countReplicas counts ready, non-primary ValkeyNodes.
func countReplicas(nodes *valkeyiov1alpha1.ValkeyNodeList, primaryIP string) int32 {
	var n int32
	for i := range nodes.Items {
		node := &nodes.Items[i]
		if !node.Status.Ready || node.Status.PodIP == "" || node.Status.PodIP == primaryIP {
			continue
		}
		n++
	}
	return n
}

// setCondition writes a status condition with the project's standard
// shape.
func (r *ValkeyReconciler) setCondition(valkey *valkeyiov1alpha1.Valkey, condType, reason, message string, status metav1.ConditionStatus) {
	meta.SetStatusCondition(&valkey.Status.Conditions, metav1.Condition{
		Type:               condType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: valkey.Generation,
	})
}
