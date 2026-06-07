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
	"strings"
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
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;delete

func (r *ValkeyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	log.V(1).Info("reconciling Valkey")

	valkey := &valkeyiov1alpha1.Valkey{}
	if err := r.Get(ctx, req.NamespacedName, valkey); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if err := r.upsertDataService(ctx, valkey); err != nil {
		r.setCondition(valkey, valkeyiov1alpha1.ConditionReady, "ServiceError", err.Error(), metav1.ConditionFalse)
		r.tryUpdateStatus(ctx, valkey)
		return ctrl.Result{}, err
	}
	if err := r.reconcilePDB(ctx, valkey); err != nil {
		r.setCondition(valkey, valkeyiov1alpha1.ConditionReady, "PDBError", err.Error(), metav1.ConditionFalse)
		r.tryUpdateStatus(ctx, valkey)
		return ctrl.Result{}, err
	}
	configHash, err := r.upsertConfigMap(ctx, valkey)
	if err != nil {
		r.setCondition(valkey, valkeyiov1alpha1.ConditionReady, "ConfigMapError", err.Error(), metav1.ConditionFalse)
		r.tryUpdateStatus(ctx, valkey)
		return ctrl.Result{}, err
	}
	if err := r.reconcileACL(ctx, valkey); err != nil {
		r.setCondition(valkey, valkeyiov1alpha1.ConditionReady, "ACLError", err.Error(), metav1.ConditionFalse)
		r.tryUpdateStatus(ctx, valkey)
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
		msg := "More than one ValkeySentinel selects this Valkey: " + strings.Join(monitoredBy, ", ") +
			" - each renders the master into its own ConfigMap; adjust selectors so exactly one matches"
		r.Recorder.Eventf(valkey, nil, corev1.EventTypeWarning,
			"MultipleSentinelsSelecting", "Observe",
			"More than one ValkeySentinel selects this Valkey: %v - each renders the master into its own ConfigMap; behaviour is undefined", monitoredBy)
		r.setCondition(valkey, valkeyiov1alpha1.ConditionMultiplyMonitored, "MultipleSentinelsSelecting",
			msg, metav1.ConditionTrue)
		// Multiple sentinels racing to drive failover on the same
		// master is "undefined behaviour" per the field comment;
		// flag Degraded so the rollout state machine halts before
		// it amplifies the inconsistency.
		r.setCondition(valkey, valkeyiov1alpha1.ConditionDegraded, "MultipleSentinelsSelecting",
			msg, metav1.ConditionTrue)
		r.setCondition(valkey, valkeyiov1alpha1.ConditionReady, "MultipleSentinelsSelecting",
			msg, metav1.ConditionFalse)
	} else {
		r.setCondition(valkey, valkeyiov1alpha1.ConditionMultiplyMonitored, "Unique",
			"At most one ValkeySentinel selects this Valkey", metav1.ConditionFalse)
	}

	if requeue, err := r.reconcileNodes(ctx, valkey, configHash); err != nil {
		r.setCondition(valkey, valkeyiov1alpha1.ConditionReady, "ValkeyNodeError", err.Error(), metav1.ConditionFalse)
		r.tryUpdateStatus(ctx, valkey)
		return ctrl.Result{}, err
	} else if requeue {
		r.setCondition(valkey, valkeyiov1alpha1.ConditionReady, "UpdatingNodes", "ValkeyNodes are updating", metav1.ConditionFalse)
		r.setCondition(valkey, valkeyiov1alpha1.ConditionProgressing, "UpdatingNodes", "ValkeyNodes are updating", metav1.ConditionTrue)
		r.tryUpdateStatus(ctx, valkey)
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}

	nodes, err := r.listOwnedNodes(ctx, valkey)
	if err != nil {
		return ctrl.Result{}, err
	}

	if valkey.Spec.Replicas > 0 {
		if err := r.bootstrapReplication(ctx, valkey, nodes); err != nil {
			r.setCondition(valkey, valkeyiov1alpha1.ConditionReady, "BootstrapError", err.Error(), metav1.ConditionFalse)
			r.tryUpdateStatus(ctx, valkey)
			return ctrl.Result{}, err
		}
	}

	// Observe who currently reports role:master across the data plane.
	// observePrimary is *biased* toward the previously known primary
	// when multiple masters are seen: if Status.PrimaryPodName still
	// reports master, keep returning it. This stops the sentinel
	// configHash from flipping between cache-0 and cache-1 across
	// successive reconciles during a transient impostor situation
	// (replica cycled, came back as master) and gives
	// wireScaledUpReplicas a stable "real primary" identity to
	// REPLICAOF the impostors back to.
	prevPrimary := valkey.Status.PrimaryPodName
	primaryName, primaryHost, primaryMasters := r.observePrimary(ctx, valkey, nodes, prevPrimary)
	if primaryName != "" {
		valkey.Status.PrimaryPodName = primaryName
		valkey.Status.PrimaryEndpoint = &valkeyiov1alpha1.Endpoint{Host: primaryHost, Port: int32(DefaultPort)}
	} else if len(primaryMasters) == 0 {
		// No masters observed at all (e.g. all pods still booting).
		// Clear PrimaryPodName/Endpoint so the next reconcile re-elects
		// from scratch.
		valkey.Status.PrimaryPodName = ""
		valkey.Status.PrimaryEndpoint = nil
	}
	// If primaryName == "" but len(primaryMasters) > 1, we're truly
	// ambiguous (previous primary not in the current master set).
	// Preserve the old Status.PrimaryEndpoint so downstream consumers
	// (sentinel template, rollout target) don't flap. Degraded below
	// surfaces the situation.
	valkey.Status.ReadyReplicas = countReplicas(nodes, valkey.Status.PrimaryPodName)

	// Surface Degraded NOW, before any destructive action. Setting it
	// AFTER executeRollout (the previous ordering) let one reconcile
	// slip through where:
	//   - observePrimary saw 2 masters and returned ambiguous
	//   - Status.PrimaryPodName was empty for this reconcile
	//   - observeRollout treated *no* node as the primary and put the
	//     real primary in the non-primary pending list
	//   - executeRollout cycled the real primary
	// The live cluster log showed this: cache-1 cycled, came back as
	// master, then the very next rollout step deleted cache-0.
	splitBrain := len(primaryMasters) > 1
	if splitBrain {
		setSplitBrainCondition(r, valkey, primaryMasters)
	}

	// Scale-up + impostor-master cleanup. Same code path covers two
	// cases: a freshly-added ValkeyNode (bootstrap is one-shot, so a
	// scale-up's new node would otherwise sit forever as an orphan
	// master) AND a previously-wired replica that came back as master
	// after a pod cycle (the writable config is emptyDir today, so
	// CONFIG REWRITE doesn't survive pod recreation). Safety
	// invariant: only REPLICAOF when DBSIZE is 0 - a non-empty lonely
	// master is left alone and surfaced as Degraded.
	if err := r.wireScaledUpReplicas(ctx, valkey, nodes); err != nil {
		logSwallowedError(log, err, "scale-up wiring step failed; will retry")
	}

	// Operator-driven rolling restart. ValkeyNode StatefulSets use
	// OnDelete, so pods only cycle when the operator deletes them.
	// observeRollout identifies which pods are still on the old
	// template; executeRollout takes at most one step per reconcile
	// (delete a non-primary pod OR trigger a planned failover) and
	// asks for a short requeue. Order: non-primaries first, then
	// SENTINEL FAILOVER (or operator-driven REPLICAOF NO ONE if no
	// sentinel selects), then the former primary.
	rollout, err := r.observeRollout(ctx, valkey, nodes)
	if err != nil {
		return ctrl.Result{}, err
	}
	phase := chooseRolloutPhase(rollout)
	r.publishRolloutStatus(valkey, rollout, phase)
	rolloutWait, err := r.executeRollout(ctx, valkey, nodes, rollout)
	if err != nil {
		// logSwallowedError bumps RBAC/Forbidden to log.Error (always
		// visible) and leaves transient errors at V(1). Without this,
		// a missing pods/delete verb on the controller's ServiceAccount
		// silently no-ops the entire rollout for the lifetime of the
		// branch.
		logSwallowedError(log, err, "rollout step failed; will retry")
		r.Recorder.Eventf(valkey, nil, corev1.EventTypeWarning, "RolloutStepFailed", "Rollout",
			"%s", err.Error())
	}

	r.deriveConditions(valkey, primaryName, primaryMasters, monitoredBy)
	if err := r.updateStatus(ctx, valkey); err != nil {
		return ctrl.Result{}, err
	}
	if rolloutWait > 0 {
		return ctrl.Result{RequeueAfter: rolloutWait}, nil
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

// countReplicas counts ready, non-primary ValkeyNodes. The primary is
// identified by its pod name rather than its IP so the count stays
// correct independent of pod-IP churn.
func countReplicas(nodes *valkeyiov1alpha1.ValkeyNodeList, primaryPodName string) int32 {
	var n int32
	for i := range nodes.Items {
		node := &nodes.Items[i]
		if !node.Status.Ready || node.Status.PodName == "" || node.Status.PodName == primaryPodName {
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

// tryUpdateStatus writes status on best-effort and logs the failure
// rather than swallowing it - the caller is already returning the
// primary error, so a status-write failure should not mask it but must
// still leave a trace.
func (r *ValkeyReconciler) tryUpdateStatus(ctx context.Context, valkey *valkeyiov1alpha1.Valkey) {
	if err := r.updateStatus(ctx, valkey); err != nil {
		logf.FromContext(ctx).Error(err, "update Valkey status")
	}
}
