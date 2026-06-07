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
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	vclient "github.com/valkey-io/valkey-go"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	valkeyiov1alpha1 "valkey.io/valkey-operator/api/v1alpha1"
	"valkey.io/valkey-operator/internal/sentinel"
)

// rolloutRequeue is how long the executor asks to be re-invoked after
// taking an action. Short enough that a multi-pod rollout completes
// quickly, long enough not to thrash the API server.
const rolloutRequeue = 5 * time.Second

// rolloutSnapshot is the per-reconcile view of what the operator-driven
// rolling restart needs to do for this Valkey.
//
// Layout decision: pods are not modelled by ValkeyNode directly. Each
// ValkeyNode owns a single-replica StatefulSet (UpdateStrategy: OnDelete
// - see buildValkeyNodeStatefulSet). A node is "pending" when its pod's
// controller-revision-hash label does NOT equal the StatefulSet's
// UpdateRevision - see podNeedsCycling for why we read the pod label
// rather than sts.Status.CurrentRevision. We collect each node's
// StatefulSet + pod, classify each ValkeyNode as primary or replica
// using Valkey.status.primaryEndpoint, and produce an ordered list of
// nodes that still need to be cycled.
type rolloutSnapshot struct {
	// pending names ValkeyNodes whose pod is not yet at the
	// StatefulSet's UpdateRevision. Ordered: replicas first
	// (by node index), primary last.
	pending []string
	// primaryNode is the ValkeyNode currently hosting the primary, or
	// empty when no primary has been observed yet. Matched against
	// Valkey.status.primaryEndpoint.
	primaryNode string
}

// observeRollout builds a snapshot of which ValkeyNodes still need to be
// rolled. It does NOT take any action - the executor is a separate step.
//
// Returns (nil, nil) when there is nothing to roll - the caller should
// clear ConditionRolling and Valkey.status.rollout in that case.
func (r *ValkeyReconciler) observeRollout(ctx context.Context, valkey *valkeyiov1alpha1.Valkey, nodes *valkeyiov1alpha1.ValkeyNodeList) (*rolloutSnapshot, error) {
	if len(nodes.Items) == 0 {
		return nil, nil
	}

	// Find which ValkeyNode currently hosts the primary by pod name.
	// Status.PrimaryPodName is the pod backing the primary endpoint;
	// matching by name (not pod IP) keeps this stable across pod
	// restarts, which is the same property the Host field gives the
	// rest of the system.
	primaryNode := ""
	if valkey.Status.PrimaryPodName != "" {
		for i := range nodes.Items {
			n := &nodes.Items[i]
			if n.Status.PodName == valkey.Status.PrimaryPodName {
				primaryNode = n.Name
				break
			}
		}
	}

	// Collect each node's single-replica StatefulSet and check whether
	// it has a pending update. Skip StatefulSets that don't exist yet
	// (initial bring-up - reconcileNodes hasn't materialised them).
	type pendingEntry struct {
		nodeName string
		idx      int // LabelNodeIndex, for stable ordering
		isPrim   bool
	}
	var entries []pendingEntry
	for i := range nodes.Items {
		n := &nodes.Items[i]
		sts := &appsv1.StatefulSet{}
		key := client.ObjectKey{Name: valkeyNodeResourceName(n), Namespace: n.Namespace}
		if err := r.Get(ctx, key, sts); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return nil, fmt.Errorf("get StatefulSet %s: %w", key, err)
		}
		// Fetch the SS-managed pod alongside the SS. The rollout uses
		// the pod's controller-revision-hash label - not
		// sts.Status.CurrentRevision - as the source of truth for "is
		// this pod at the desired revision yet?". See podNeedsCycling
		// for why. A missing pod is treated as pending: the SS
		// controller will recreate it stamped at UpdateRevision, but
		// until that happens we have to assume it's still pre-cycle.
		pod := &corev1.Pod{}
		podKey := client.ObjectKey{Name: valkeyNodeResourcePodNameFor(n.Name), Namespace: n.Namespace}
		if err := r.Get(ctx, podKey, pod); err != nil {
			if apierrors.IsNotFound(err) {
				pod = nil
			} else {
				return nil, fmt.Errorf("get pod %s: %w", podKey, err)
			}
		}
		if !podNeedsCycling(sts, pod) {
			continue
		}
		idx, _ := strconv.Atoi(n.Labels[LabelNodeIndex])
		entries = append(entries, pendingEntry{
			nodeName: n.Name,
			idx:      idx,
			isPrim:   n.Name == primaryNode,
		})
	}
	if len(entries) == 0 {
		return nil, nil
	}

	// Order: non-primaries first (ascending by node index), primary
	// last. Matches the state machine: roll replicas, failover, roll
	// former primary.
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].isPrim != entries[j].isPrim {
			return !entries[i].isPrim
		}
		return entries[i].idx < entries[j].idx
	})
	pending := make([]string, len(entries))
	for i, e := range entries {
		pending[i] = e.nodeName
	}
	return &rolloutSnapshot{pending: pending, primaryNode: primaryNode}, nil
}

// podRevisionLabel is set by the StatefulSet controller on every pod
// it creates and names the ControllerRevision the pod was rendered
// from. Comparing this against sts.Status.UpdateRevision is the
// race-free way to ask "is this pod already at the desired revision?".
const podRevisionLabel = "controller-revision-hash"

// podNeedsCycling returns true when the StatefulSet's single pod is
// not yet at the StatefulSet's UpdateRevision - i.e. the operator
// should delete the pod so the SS controller recreates it under the
// new template.
//
// We deliberately read the pod's controller-revision-hash label rather
// than sts.Status.CurrentRevision. Under UpdateStrategy: OnDelete the
// StatefulSet controller does not reliably advance CurrentRevision
// after the operator manually deletes a pod and the SS controller
// recreates it: the pod is provably at UpdateRevision (its own label
// says so, set the instant the SS controller created it) but
// CurrentRevision stays at the old value, causing the rollout
// observer to re-flag the node as pending and the executor to delete
// the same pod over and over. The pod label is set at creation time
// by the SS controller and matches the revision it rendered the pod
// from, so it's race-free and works identically under OnDelete and
// RollingUpdate.
func podNeedsCycling(sts *appsv1.StatefulSet, pod *corev1.Pod) bool {
	if sts.Status.UpdateRevision == "" {
		return false
	}
	if pod == nil {
		// Pod missing (operator just deleted it, or SS controller
		// hasn't materialised it yet). The SS controller will recreate
		// it at UpdateRevision; treat it as pending so the in-flight
		// gate in executeRollout has a chance to wait for it.
		return true
	}
	return pod.Labels[podRevisionLabel] != sts.Status.UpdateRevision
}

// chooseRolloutPhase decides which RolloutPhase to surface and which
// branch the executor will take.
func chooseRolloutPhase(snap *rolloutSnapshot) valkeyiov1alpha1.RolloutPhase {
	if snap == nil || len(snap.pending) == 0 {
		return valkeyiov1alpha1.RolloutPhaseIdle
	}
	if len(snap.pending) == 1 && snap.pending[0] == snap.primaryNode {
		return valkeyiov1alpha1.RolloutPhaseFailover
	}
	if len(snap.pending) == 1 && snap.pending[0] != snap.primaryNode && snap.primaryNode != "" {
		// Only the demoted former primary is left to cycle. Identifies
		// the last phase of a successful planned failover.
		return valkeyiov1alpha1.RolloutPhaseRollingPrimary
	}
	return valkeyiov1alpha1.RolloutPhaseRollingReplicas
}

// executeRollout drives the operator-side rolling-restart state machine.
// At most one mutating action runs per call (pod deletion OR failover
// trigger), then the caller is asked to requeue and re-evaluate. This
// keeps each step small and idempotent: if anything in the world drifts
// (a pod restarts on its own, a sentinel-driven failover races us), the
// next observeRollout reflects the new reality before we act again.
//
// Returns the time after which to requeue (0 means "no rollout work").
func (r *ValkeyReconciler) executeRollout(ctx context.Context, valkey *valkeyiov1alpha1.Valkey, nodes *valkeyiov1alpha1.ValkeyNodeList, snap *rolloutSnapshot) (time.Duration, error) {
	log := logf.FromContext(ctx).WithValues("rollout", "valkey/"+valkey.Name)
	if snap == nil || len(snap.pending) == 0 {
		return 0, nil
	}

	// Halt all rollout actions while the Valkey is Degraded.
	// Degraded means we've already observed a state the operator can't
	// safely auto-recover from (split-brain, rogue master with data),
	// and any further pod cycle or failover trigger would either
	// destroy data or amplify the divergence. The user must clear the
	// underlying issue; the rollout picks up from where it stopped on
	// the next reconcile after Degraded clears.
	if meta.IsStatusConditionTrue(valkey.Status.Conditions, valkeyiov1alpha1.ConditionDegraded) {
		log.V(1).Info("rollout halted: Valkey is Degraded; no new actions until cleared")
		return rolloutRequeue, nil
	}

	// In-flight transition gate. Reads pods DIRECTLY rather than
	// trusting ValkeyNode.Status.Ready, because that status is set by
	// the ValkeyNode controller observing the pod and lags by one
	// reconcile - long enough for a concurrent Valkey reconcile
	// (triggered by an unrelated watch event, e.g. the SS status
	// changing) to slip past, see the stale Ready=true, and delete the
	// same pod a second time. Two parallel deletes in the same second
	// is exactly what the live cluster log showed.
	//
	// A pod with DeletionTimestamp set was already targeted by a prior
	// reconcile in this rollout cycle; do not target it again. A pod
	// that's missing (about to be recreated by the SS controller) or
	// not yet Ready means a transition is in flight; wait.
	for i := range nodes.Items {
		n := &nodes.Items[i]
		pod := &corev1.Pod{}
		podKey := client.ObjectKey{Name: valkeyNodeResourcePodNameFor(n.Name), Namespace: n.Namespace}
		if err := r.Get(ctx, podKey, pod); err != nil {
			if apierrors.IsNotFound(err) {
				log.V(1).Info("rollout: waiting for pod to come back", "node", n.Name, "pod", podKey.Name)
				return rolloutRequeue, nil
			}
			return rolloutRequeue, fmt.Errorf("get pod %s: %w", podKey, err)
		}
		if pod.DeletionTimestamp != nil {
			log.V(1).Info("rollout: waiting for pod terminating", "node", n.Name, "pod", pod.Name)
			return rolloutRequeue, nil
		}
		if !podIsReady(pod) {
			log.V(1).Info("rollout: waiting for pod to become Ready", "node", n.Name, "pod", pod.Name)
			return rolloutRequeue, nil
		}
	}

	// Replication health gate. Before we knock a pod over (or trigger a
	// failover) every pod that's already on the new revision and is
	// supposed to be a replica must be a caught-up replica. Otherwise
	// we'd remove redundancy from an already-degraded cluster.
	caughtUp, err := r.allNonPendingReplicasCaughtUp(ctx, valkey, nodes, snap)
	if err != nil {
		return rolloutRequeue, err
	}
	if !caughtUp {
		log.V(1).Info("waiting for non-pending replicas to catch up before continuing rollout")
		return rolloutRequeue, nil
	}

	switch chooseRolloutPhase(snap) {
	case valkeyiov1alpha1.RolloutPhaseFailover:
		return rolloutRequeue, r.rolloutPlannedFailover(ctx, valkey, nodes)
	default:
		// RollingReplicas or RollingPrimary - both delete the first
		// non-primary pending pod and wait for it to come back. The
		// ordering in snap.pending guarantees replicas first.
		return rolloutRequeue, r.rolloutDeleteNonPrimary(ctx, valkey, snap)
	}
}

// rolloutDeleteNonPrimary deletes the pod backing the first non-primary
// pending ValkeyNode. The StatefulSet (OnDelete strategy) recreates the
// pod under the latest revision. We delete at most one pod per call and
// let the next reconcile decide what's next.
func (r *ValkeyReconciler) rolloutDeleteNonPrimary(ctx context.Context, valkey *valkeyiov1alpha1.Valkey, snap *rolloutSnapshot) error {
	log := logf.FromContext(ctx)
	for _, nodeName := range snap.pending {
		if nodeName == snap.primaryNode {
			continue
		}
		// The pod backing a single-replica StatefulSet is always
		// "<sts-name>-0". sts-name == ValkeyNode resource name.
		podName := valkeyNodeResourcePodNameFor(nodeName)
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: valkey.Namespace}}
		if err := r.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete pod %s: %w", podName, err)
		}
		log.Info("rollout: deleted pod for re-creation under new revision", "pod", podName)
		r.Recorder.Eventf(valkey, pod, corev1.EventTypeNormal, "RolloutPodCycled", "Rollout",
			"Deleted pod %s so the StatefulSet (OnDelete) recreates it under the new template", podName)
		return nil
	}
	// No non-primary pending node found, but executeRollout's phase
	// dispatch should have routed to the failover branch. Treat as a
	// no-op rather than a hard error - the next observeRollout will
	// reflect reality.
	return nil
}

// rolloutPlannedFailover demotes the current primary so it can be rolled
// last. When a ValkeySentinel selects this Valkey, the operator issues
// SENTINEL FAILOVER <name> against one of its pods - sentinel picks the
// promotion target, runs the MULTI/EXEC promotion transaction, and
// updates its own gossiped view atomically. When no sentinel selects,
// the operator does the equivalent by hand: pick a caught-up replica,
// REPLICAOF NO ONE on it, then REPLICAOF on every other pod (including
// the old primary).
func (r *ValkeyReconciler) rolloutPlannedFailover(ctx context.Context, valkey *valkeyiov1alpha1.Valkey, nodes *valkeyiov1alpha1.ValkeyNodeList) error {
	log := logf.FromContext(ctx)
	// Debounce: a failover takes seconds. Re-issuing on every
	// reconcile produces event spam (~10s reconcile cadence * 30s+
	// sentinel promotion) and obscures real failures. Skip if a
	// failover was issued recently and the primary hasn't moved yet.
	if !shouldIssueFailover(valkey) {
		log.V(1).Info("rollout: failover recently issued, awaiting promotion",
			"issuedAt", valkey.Status.Rollout.FailoverIssuedAt,
			"primary", valkey.Status.PrimaryPodName)
		return nil
	}
	// Claim the timestamp BEFORE the wire call and persist it
	// synchronously, so a concurrent reconcile reading from the
	// controller-runtime cache sees a fresh FailoverIssuedAt and
	// shouldIssueFailover refuses. Without this, two back-to-back
	// reconciles can both pass shouldIssueFailover and both issue
	// SENTINEL FAILOVER (the second arrives at sentinel mid-handoff;
	// sentinel returns INPROG, our code treats that as success, and
	// the multi-replica window the user observed gets extended).
	// A failed status patch returns the error - the next reconcile
	// retries the whole flow.
	recordFailoverIssued(valkey)
	if err := r.updateStatus(ctx, valkey); err != nil {
		return fmt.Errorf("persist FailoverIssuedAt before trigger: %w", err)
	}
	if len(valkey.Status.MonitoredBy) > 0 {
		ok, coordinated, err := r.triggerSentinelFailover(ctx, valkey)
		if err != nil {
			return err
		}
		if ok {
			if coordinated {
				log.Info("rollout: triggered SENTINEL FAILOVER COORDINATED")
				r.Recorder.Eventf(valkey, nil, corev1.EventTypeNormal, "RolloutFailover", "Rollout",
					"Issued SENTINEL FAILOVER %s COORDINATED to one of: %v - replicas will psync after promotion", valkey.Name, valkey.Status.MonitoredBy)
			} else {
				log.Info("rollout: triggered SENTINEL FAILOVER (non-coordinated; sentinel < 9.0)")
				r.Recorder.Eventf(valkey, nil, corev1.EventTypeWarning, "RolloutFailoverFallback", "Rollout",
					"Sentinel does not support COORDINATED failover; fell back to plain SENTINEL FAILOVER %s. Upgrade sentinels to Valkey 9.0+ to avoid full-resync on planned failover (see https://github.com/valkey-io/valkey/pull/1292).", valkey.Name)
			}
			return nil
		}
		// Fall through: no sentinel pod reachable, do it ourselves.
		log.V(1).Info("rollout: no sentinel pod reachable; performing operator-driven failover")
	}
	return r.operatorDrivenFailover(ctx, valkey, nodes)
}

// failoverDebounce is the minimum interval between failover triggers
// for the same Valkey. Sentinel-driven promotion typically completes
// in 10-30s on a healthy cluster; 30s gives one full cycle of headroom
// without delaying recovery from a wedged failover too long.
const failoverDebounce = 30 * time.Second

// shouldIssueFailover returns false when a failover was issued in the
// last failoverDebounce window AND Status.PrimaryPodName still names
// a node currently in PendingNodes (i.e. the failover hasn't taken
// effect yet). Returning true on a moved primary lets a second
// rollout phase trigger normally.
func shouldIssueFailover(valkey *valkeyiov1alpha1.Valkey) bool {
	if valkey.Status.Rollout == nil || valkey.Status.Rollout.FailoverIssuedAt == nil {
		return true
	}
	if time.Since(valkey.Status.Rollout.FailoverIssuedAt.Time) >= failoverDebounce {
		return true
	}
	// Primary already moved out of the pending set -> previous
	// failover landed; allow a fresh one.
	for _, n := range valkey.Status.Rollout.PendingNodes {
		if n == valkey.Status.PrimaryPodName {
			return false
		}
	}
	return true
}

func recordFailoverIssued(valkey *valkeyiov1alpha1.Valkey) {
	if valkey.Status.Rollout == nil {
		valkey.Status.Rollout = &valkeyiov1alpha1.RolloutStatus{}
	}
	now := metav1.Now()
	valkey.Status.Rollout.FailoverIssuedAt = &now
}

// failoverRecentlyIssued reports whether a SENTINEL FAILOVER (or
// operator-driven equivalent) was triggered inside the debounce
// window. deriveConditions consults this to distinguish the brief
// "Sentinel handoff in flight" window (where every pod transiently
// reports role:replica) from a real PrimaryLost.
func failoverRecentlyIssued(valkey *valkeyiov1alpha1.Valkey) bool {
	if valkey.Status.Rollout == nil || valkey.Status.Rollout.FailoverIssuedAt == nil {
		return false
	}
	return time.Since(valkey.Status.Rollout.FailoverIssuedAt.Time) < failoverDebounce
}

// triggerSentinelFailover dials any sentinel pod selecting this Valkey
// and issues SENTINEL FAILOVER. It tries the COORDINATED form first
// (Valkey 9.0+, PR #1292 - the chosen replica catches up via the
// master's FAILOVER command, so the remaining replicas keep psync-ing
// instead of doing a full resync after the role flip). If the sentinel
// rejects the keyword, falls back to plain SENTINEL FAILOVER.
//
// Returns (triggered, coordinated, err):
//
//   - triggered=true: a sentinel accepted the command.
//   - coordinated=true: the COORDINATED form was the one accepted.
//   - triggered=false, err=nil: no sentinel was reachable (caller falls
//     back to operator-driven).
func (r *ValkeyReconciler) triggerSentinelFailover(ctx context.Context, valkey *valkeyiov1alpha1.Valkey) (bool, bool, error) {
	for _, sentinelName := range valkey.Status.MonitoredBy {
		pods := &corev1.PodList{}
		if err := r.List(ctx, pods,
			client.InNamespace(valkey.Namespace),
			client.MatchingLabels{LabelSentinel: sentinelName},
		); err != nil {
			return false, false, err
		}
		for i := range pods.Items {
			p := &pods.Items[i]
			if p.Status.PodIP == "" {
				continue
			}
			c, err := sentinel.New(p.Status.PodIP, sentinel.Port, nil)
			if err != nil {
				continue
			}
			coordinated, err := failoverWithFallback(ctx, c, valkey.Name)
			c.Close()
			if err != nil {
				// `INPROG Failover already in progress` is fine - it
				// means a previous reconcile already triggered the
				// failover and sentinel hasn't completed yet. Treat as
				// success so the next reconcile observes the new
				// primary.
				if strings.Contains(strings.ToLower(err.Error()), "already in progress") {
					return true, coordinated, nil
				}
				return false, false, fmt.Errorf("SENTINEL FAILOVER %s on %s: %w", valkey.Name, p.Status.PodIP, err)
			}
			return true, coordinated, nil
		}
	}
	return false, false, nil
}

// failoverWithFallback issues SENTINEL FAILOVER ... COORDINATED first
// and, only if the sentinel doesn't recognise the keyword, retries with
// the plain form. The fallback path is for environments where the
// operator is built against 9.0+ but the running sentinel image is
// older. Returns true when the COORDINATED form was the one accepted.
func failoverWithFallback(ctx context.Context, c *sentinel.Client, masterName string) (bool, error) {
	err := c.FailoverCoordinated(ctx, masterName)
	if err == nil {
		return true, nil
	}
	if !errors.Is(err, sentinel.ErrCoordinatedUnsupported) {
		return false, err
	}
	return false, c.Failover(ctx, masterName)
}

// operatorDrivenFailover performs the failover steps the user's
// reviewer described, with no sentinel involvement:
//
//  1. Pick a caught-up replica.
//  2. REPLICAOF NO ONE on it (promote).
//  3. For every other pod (including the old primary): REPLICAOF
//     <new-primary-ip> <port>.
//
// The next reconcile's observePrimary picks up the role flip via INFO
// and updates Valkey.status.primaryEndpoint, which then re-classifies
// the old primary as a (now-pending) replica.
func (r *ValkeyReconciler) operatorDrivenFailover(ctx context.Context, valkey *valkeyiov1alpha1.Valkey, nodes *valkeyiov1alpha1.ValkeyNodeList) error {
	log := logf.FromContext(ctx)
	password, err := fetchSystemUserPassword(ctx, operatorUser, r.Client, valkey.Name, valkey.Namespace)
	if err != nil {
		return fmt.Errorf("fetch operator password: %w", err)
	}
	// Identify the current primary by pod name (Status.PrimaryPodName).
	// The data-plane direct connects below still go to pod IPs - that's
	// fine, they're transient.
	currentPrimaryPodName := valkey.Status.PrimaryPodName
	// Pick a caught-up replica.
	var targetIP, targetName string
	for i := range nodes.Items {
		n := &nodes.Items[i]
		if n.Status.PodIP == "" || !n.Status.Ready || n.Status.PodName == currentPrimaryPodName {
			continue
		}
		if !isReplicaCaughtUp(ctx, n.Status.PodIP, password) {
			continue
		}
		targetIP = n.Status.PodIP
		targetName = n.Name
		break
	}
	if targetIP == "" {
		return fmt.Errorf("no caught-up replica available for promotion")
	}
	log.Info("rollout: promoting replica via operator-driven failover", "target", targetName, "ip", targetIP)
	var newPrimary, oldPrimary *valkeyiov1alpha1.ValkeyNode
	for i := range nodes.Items {
		n := &nodes.Items[i]
		if n.Status.PodIP == targetIP {
			newPrimary = n
		}
		if n.Status.PodName == currentPrimaryPodName {
			oldPrimary = n
		}
	}
	if err := withDataClient(targetIP, password, func(c vclient.Client) error {
		return c.Do(ctx, c.B().Replicaof().No().One().Build()).Error()
	}); err != nil {
		return fmt.Errorf("REPLICAOF NO ONE on %s: %w", targetIP, err)
	}
	// Point every other Ready pod at the new primary by its stable
	// per-pod Service DNS, NOT the pod IP. CONFIG REWRITE below
	// persists the directive into the writable valkey.conf; using
	// the IP would strand replicas with master_link_status:down the
	// moment the new primary's pod is ever recreated.
	newPrimaryHost := valkeyNodeServiceHost(newPrimary)
	bestEffort := []*valkeyiov1alpha1.ValkeyNode{}
	for i := range nodes.Items {
		n := &nodes.Items[i]
		if n.Status.PodIP == "" || !n.Status.Ready || n.Status.PodIP == targetIP {
			continue
		}
		ip := n.Status.PodIP
		if err := withDataClient(ip, password, func(c vclient.Client) error {
			return c.Do(ctx, c.B().Replicaof().Host(newPrimaryHost).Port(int64(DefaultPort)).Build()).Error()
		}); err != nil {
			return fmt.Errorf("REPLICAOF %s on %s: %w", newPrimaryHost, ip, err)
		}
		// The demoted ex-primary needs blocking CONFIG REWRITE
		// (see below); other replicas are best-effort because
		// wireScaledUpReplicas re-points them on the next
		// reconcile if their REWRITE fails.
		if n != oldPrimary {
			bestEffort = append(bestEffort, n)
		}
	}
	// The new primary and the demoted ex-primary are the two pods
	// whose disk-vs-runtime mismatch produces split-brain on
	// restart. REWRITE must succeed on both before we declare
	// failover done; everyone else is best-effort.
	if err := configRewriteOrFail(ctx, password, newPrimary); err != nil {
		return err
	}
	if oldPrimary != nil {
		if err := configRewriteOrFail(ctx, password, oldPrimary); err != nil {
			return err
		}
	}
	configRewriteAll(ctx, log, password, bestEffort)
	r.Recorder.Eventf(valkey, nil, corev1.EventTypeNormal, "RolloutFailover", "Rollout",
		"Operator-driven failover: promoted %s, repointed remaining pods", targetName)
	return nil
}

// allNonPendingReplicasCaughtUp checks that every replica NOT in the
// pending set is currently a healthy replica of the primary. If any of
// them are mid-resync or have a broken link, we hold the rollout rather
// than weaken the cluster further.
func (r *ValkeyReconciler) allNonPendingReplicasCaughtUp(ctx context.Context, valkey *valkeyiov1alpha1.Valkey, nodes *valkeyiov1alpha1.ValkeyNodeList, snap *rolloutSnapshot) (bool, error) {
	if valkey.Spec.Replicas == 0 || valkey.Status.PrimaryEndpoint == nil {
		return true, nil
	}
	pendingSet := map[string]bool{}
	for _, n := range snap.pending {
		pendingSet[n] = true
	}
	password, err := fetchSystemUserPassword(ctx, operatorUser, r.Client, valkey.Name, valkey.Namespace)
	if err != nil {
		return false, fmt.Errorf("fetch operator password: %w", err)
	}
	primaryPodName := valkey.Status.PrimaryPodName
	for i := range nodes.Items {
		n := &nodes.Items[i]
		if pendingSet[n.Name] {
			continue
		}
		if n.Status.PodIP == "" || !n.Status.Ready {
			return false, nil
		}
		if n.Status.PodName == primaryPodName {
			continue
		}
		if !isReplicaCaughtUp(ctx, n.Status.PodIP, password) {
			return false, nil
		}
	}
	return true, nil
}

// isReplicaCaughtUp reports whether the pod at ip is a replica with an
// active master link. We accept master_link_status:up as "caught up"
// rather than chasing offset equality - the link being up means the
// sync is complete and ongoing replication is streaming.
func isReplicaCaughtUp(ctx context.Context, ip, password string) bool {
	info, err := infoReplication(ctx, ip, password)
	if err != nil {
		return false
	}
	if strings.TrimSpace(info["role"]) != RoleSlave {
		return false
	}
	return strings.TrimSpace(info["master_link_status"]) == "up"
}

// valkeyNodeResourcePodNameFor maps a ValkeyNode CR name to the name of
// the single pod its StatefulSet manages. The StatefulSet name equals
// the ValkeyNode CR name (see valkeyNodeResourceName), and a
// single-replica StatefulSet's pod is always named "<sts-name>-0".
func valkeyNodeResourcePodNameFor(valkeyNodeName string) string {
	return resourcePrefix + valkeyNodeName + "-0"
}

// podIsReady reads PodReady directly from the pod's own conditions
// rather than via the lagged ValkeyNode.Status.Ready. The rollout
// executor's in-flight gate uses this so a concurrent Valkey reconcile
// (woken by a watch event between "executor deletes pod" and
// "ValkeyNode controller observes the deletion") sees the still-fresh
// `DeletionTimestamp != nil` instead of the stale Ready=true, and
// refuses to double-delete the pod.
func podIsReady(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// publishRolloutStatus writes the snapshot to Valkey.status and toggles
// ConditionRolling. Passing nil clears both - the rollout is complete or
// was never needed.
func (r *ValkeyReconciler) publishRolloutStatus(valkey *valkeyiov1alpha1.Valkey, snap *rolloutSnapshot, phase valkeyiov1alpha1.RolloutPhase) {
	if snap == nil || len(snap.pending) == 0 {
		valkey.Status.Rollout = nil
		r.setCondition(valkey, valkeyiov1alpha1.ConditionRolling,
			"NoRollout", "All pods on the desired template",
			metav1.ConditionFalse)
		return
	}
	// Preserve FailoverIssuedAt across re-publishes: this field is the
	// debounce gate for SENTINEL FAILOVER (see shouldIssueFailover),
	// and clobbering it on every reconcile would defeat the debounce.
	var issuedAt *metav1.Time
	if valkey.Status.Rollout != nil {
		issuedAt = valkey.Status.Rollout.FailoverIssuedAt
	}
	valkey.Status.Rollout = &valkeyiov1alpha1.RolloutStatus{
		Phase:            phase,
		PendingNodes:     snap.pending,
		FailoverIssuedAt: issuedAt,
	}
	r.setCondition(valkey, valkeyiov1alpha1.ConditionRolling,
		string(phase),
		fmt.Sprintf("Rolling %d ValkeyNode(s) under operator-driven failover", len(snap.pending)),
		metav1.ConditionTrue)
}
