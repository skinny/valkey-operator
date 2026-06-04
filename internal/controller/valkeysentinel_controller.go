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
	"time"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	klabels "k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	valkeyiov1alpha1 "valkey.io/valkey-operator/api/v1alpha1"
)

// ValkeySentinelReconciler reconciles a ValkeySentinel object.
//
// Pipeline:
//  1. Provision sentinel infrastructure (Service, ConfigMap, PDB,
//     StatefulSet).
//  2. List Valkeys whose labels match spec.valkeySelector.
//  3. For each matched Valkey: SENTINEL MONITOR + SET (using the
//     per-Valkey sentinel-auth Secret); for masters no longer matched:
//     SENTINEL REMOVE.
//  4. Update status (readyReplicas, monitored).
//
// The reconciler does NOT read Valkey.status. It picks any reachable
// data pod (by label) as the entry IP for SENTINEL MONITOR; Sentinel
// itself follows INFO replication to discover the actual master.
type ValkeySentinelReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder events.EventRecorder
}

// +kubebuilder:rbac:groups=valkey.io,resources=valkeysentinels,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=valkey.io,resources=valkeysentinels/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=valkey.io,resources=valkeys,verbs=get;list;watch
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete

func (r *ValkeySentinelReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	log.V(1).Info("reconciling ValkeySentinel")

	sentinel := &valkeyiov1alpha1.ValkeySentinel{}
	if err := r.Get(ctx, req.NamespacedName, sentinel); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if err := r.upsertSentinelService(ctx, sentinel); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.upsertSentinelConfigMap(ctx, sentinel); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.upsertSentinelPDB(ctx, sentinel); err != nil {
		return ctrl.Result{}, err
	}
	ss, err := r.upsertSentinelStatefulSet(ctx, sentinel)
	if err != nil {
		return ctrl.Result{}, err
	}
	sentinel.Status.ReadyReplicas = ss.Status.ReadyReplicas

	// Sentinel-side monitoring: only attempt when at least one pod is ready.
	monitored := []string{}
	if ss.Status.ReadyReplicas > 0 {
		monitored, err = r.reconcileMonitoring(ctx, sentinel)
		if err != nil {
			log.V(1).Info("monitoring reconcile incomplete; will retry", "err", err)
		}
	}
	sort.Strings(monitored)
	sentinel.Status.Monitored = monitored

	r.deriveSentinelConditions(sentinel, ss)
	if err := r.updateSentinelStatus(ctx, sentinel); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

// findSentinelsForValkey enqueues every ValkeySentinel in the
// Valkey's namespace whose selector matches the Valkey's labels.
func (r *ValkeySentinelReconciler) findSentinelsForValkey(ctx context.Context, obj client.Object) []reconcile.Request {
	valkey, ok := obj.(*valkeyiov1alpha1.Valkey)
	if !ok {
		return nil
	}
	sentinels := &valkeyiov1alpha1.ValkeySentinelList{}
	if err := r.List(ctx, sentinels, client.InNamespace(valkey.Namespace)); err != nil {
		return nil
	}
	var out []reconcile.Request
	myLabels := klabels.Set(valkey.Labels)
	for i := range sentinels.Items {
		s := &sentinels.Items[i]
		selector, err := metav1.LabelSelectorAsSelector(&s.Spec.ValkeySelector)
		if err != nil || selector.Empty() {
			continue
		}
		if selector.Matches(myLabels) {
			out = append(out, reconcile.Request{NamespacedName: types.NamespacedName{
				Name: s.Name, Namespace: s.Namespace,
			}})
		}
	}
	return out
}

func (r *ValkeySentinelReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&valkeyiov1alpha1.ValkeySentinel{}).
		Owns(&appsv1.StatefulSet{}).
		Watches(
			&valkeyiov1alpha1.Valkey{},
			handler.EnqueueRequestsFromMapFunc(r.findSentinelsForValkey),
		).
		Named("valkeysentinel").
		Complete(r)
}

func (r *ValkeySentinelReconciler) deriveSentinelConditions(sentinel *valkeyiov1alpha1.ValkeySentinel, ss *appsv1.StatefulSet) {
	if ss.Status.ReadyReplicas < sentinel.Spec.Replicas {
		setCond(sentinel, valkeyiov1alpha1.ConditionReady, "Reconciling",
			"Waiting for sentinel pods to become ready",
			metav1.ConditionFalse)
		setCond(sentinel, valkeyiov1alpha1.ConditionProgressing, "Reconciling",
			"Sentinel pods are starting", metav1.ConditionTrue)
		sentinel.Status.State = valkeyiov1alpha1.ClusterStateReconciling
		sentinel.Status.Reason = "Reconciling"
		return
	}
	setCond(sentinel, valkeyiov1alpha1.ConditionReady, "ClusterHealthy",
		"All sentinel pods ready", metav1.ConditionTrue)
	setCond(sentinel, valkeyiov1alpha1.ConditionProgressing, "ReconcileComplete",
		"No changes needed", metav1.ConditionFalse)
	sentinel.Status.State = valkeyiov1alpha1.ClusterStateReady
	sentinel.Status.Reason = "ClusterHealthy"
}

func setCond(sentinel *valkeyiov1alpha1.ValkeySentinel, condType, reason, msg string, status metav1.ConditionStatus) {
	meta.SetStatusCondition(&sentinel.Status.Conditions, metav1.Condition{
		Type: condType, Status: status, Reason: reason, Message: msg,
		ObservedGeneration: sentinel.Generation,
	})
}

func (r *ValkeySentinelReconciler) updateSentinelStatus(ctx context.Context, sentinel *valkeyiov1alpha1.ValkeySentinel) error {
	current := &valkeyiov1alpha1.ValkeySentinel{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(sentinel), current); err != nil {
		return err
	}
	patchBase := current.DeepCopy()
	current.Status = sentinel.Status
	if reflect.DeepEqual(patchBase.Status, current.Status) {
		return nil
	}
	return r.Status().Patch(ctx, current, client.MergeFrom(patchBase))
}
