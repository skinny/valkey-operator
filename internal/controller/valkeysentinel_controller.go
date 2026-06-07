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
//  1. Provision sentinel infrastructure (Service, PDB).
//  2. Gather per-Valkey monitor configs from Valkey.status.primaryEndpoint
//     (populated by the Valkey controller). One matched Valkey == one
//     monitor block in the rendered sentinel.conf.
//  3. Maintain the aggregated `<sentinel-name>-auth` Secret with one
//     key per matched Valkey, mounted at /sentinel-auth/ in each pod.
//  4. Render the ConfigMap with `sentinel monitor` + auth + tuning baked
//     in; stamp the content hash as a pod-template annotation so the
//     StatefulSet rolls when the rendered config changes.
//  5. Reconcile the StatefulSet.
//  6. For masters a running sentinel still knows about that are not in
//     the matched set: SENTINEL REMOVE.
//  7. Update status (readyReplicas, monitored).
type ValkeySentinelReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder events.EventRecorder
}

// +kubebuilder:rbac:groups=valkey.io,resources=valkeysentinels,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=valkey.io,resources=valkeysentinels/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=valkey.io,resources=valkeysentinels/finalizers,verbs=update
// +kubebuilder:rbac:groups=valkey.io,resources=valkeys,verbs=get;list;watch
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete

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
	if err := r.upsertSentinelPDB(ctx, sentinel); err != nil {
		return ctrl.Result{}, err
	}

	// Gather per-Valkey monitor blocks from Valkey.status.primaryEndpoint
	// (set by the Valkey controller). This is the single source of truth
	// for what gets baked into the template; no data-pod probing here.
	monitors, passwords, err := r.collectMonitorConfigs(ctx, sentinel)
	if err != nil {
		return ctrl.Result{}, err
	}

	if err := r.upsertSentinelAuthSecret(ctx, sentinel, passwords); err != nil {
		return ctrl.Result{}, err
	}
	configHash, err := r.upsertSentinelConfigMap(ctx, sentinel, monitors)
	if err != nil {
		return ctrl.Result{}, err
	}
	ss, err := r.upsertSentinelStatefulSet(ctx, sentinel, configHash, len(monitors) > 0)
	if err != nil {
		return ctrl.Result{}, err
	}
	if ss == nil {
		// Deferred: no monitors yet and no existing SS. Don't materialise
		// pods that would only be told what to monitor on the next
		// reconcile (and rolled in the process). Requeue shortly to
		// re-check once a Valkey reports its primary endpoint.
		sentinel.Status.ReadyReplicas = 0
		sentinel.Status.Monitored = nil
		sentinel.Status.Endpoints = sentinelEndpoints(sentinel)
		r.deriveSentinelConditions(sentinel, nil)
		if err := r.updateSentinelStatus(ctx, sentinel); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	sentinel.Status.ReadyReplicas = ss.Status.ReadyReplicas

	// REMOVE-only sweep against running sentinels for masters no longer
	// in the matched set. Monitor + auth + tuning are now in the
	// template; no SETs over the wire.
	monitored := []string{}
	deferredSweep := false
	if ss.Status.ReadyReplicas > 0 {
		monitored, deferredSweep, err = r.reconcileMonitoring(ctx, sentinel, monitors)
		if err != nil {
			logSwallowedError(log, err, "monitoring reconcile incomplete; will retry")
		}
	} else {
		// All sentinel pods are unready; the stale-sweep can't run.
		// Treat as deferred so we requeue soon rather than waiting for
		// a watch event - if a Valkey was just removed from the
		// selector, the REMOVE wouldn't otherwise fire until something
		// else triggers reconcile.
		for _, m := range monitors {
			monitored = append(monitored, m.Name)
		}
		deferredSweep = true
	}
	sort.Strings(monitored)
	sentinel.Status.Monitored = monitored
	sentinel.Status.Endpoints = sentinelEndpoints(sentinel)

	r.deriveSentinelConditions(sentinel, ss)
	if err := r.updateSentinelStatus(ctx, sentinel); err != nil {
		return ctrl.Result{}, err
	}
	requeueAfter := 30 * time.Second
	if deferredSweep {
		requeueAfter = 10 * time.Second
	}
	return ctrl.Result{RequeueAfter: requeueAfter}, nil
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
	// Deferred SS: no monitors yet and no existing SS to read status
	// from. Surface "waiting for a Valkey to bootstrap" rather than
	// pretending the cluster is Ready (it would report 0 desired
	// pods Ready, which is misleading on a fresh deploy).
	if ss == nil {
		setCond(sentinel, valkeyiov1alpha1.ConditionReady, "AwaitingMonitor",
			"No matched Valkey has reported a primary endpoint yet; deferring sentinel pod creation",
			metav1.ConditionFalse)
		setCond(sentinel, valkeyiov1alpha1.ConditionProgressing, "AwaitingMonitor",
			"Waiting for at least one selected Valkey to bootstrap",
			metav1.ConditionTrue)
		sentinel.Status.State = valkeyiov1alpha1.ClusterStateReconciling
		sentinel.Status.Reason = "AwaitingMonitor"
		return
	}
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

// sentinelEndpoints lists per-pod sentinel addresses derived from the
// declared replica count and the StatefulSet's DNS naming convention.
// We don't gate on pod readiness: the StatefulSet's governing Service
// publishes pod DNS as soon as the pod is created, so a client trying
// the list will skip unreachable entries and connect to whatever is up
// (which is the point of giving the client all of them).
func sentinelEndpoints(s *valkeyiov1alpha1.ValkeySentinel) []valkeyiov1alpha1.Endpoint {
	svc := sentinelResourceName(s)
	endpoints := make([]valkeyiov1alpha1.Endpoint, 0, s.Spec.Replicas)
	for i := int32(0); i < s.Spec.Replicas; i++ {
		host := fmt.Sprintf("%s-%d.%s.%s.svc.cluster.local", svc, i, svc, s.Namespace)
		endpoints = append(endpoints, valkeyiov1alpha1.Endpoint{
			Host: host,
			Port: int32(valkeyiov1alpha1.SentinelPort),
		})
	}
	return endpoints
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
