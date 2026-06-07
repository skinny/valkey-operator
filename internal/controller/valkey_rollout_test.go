/*
Copyright 2025 Valkey Contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	valkeyiov1alpha1 "valkey.io/valkey-operator/api/v1alpha1"
)

func TestChooseRolloutPhase(t *testing.T) {
	tests := []struct {
		name string
		snap *rolloutSnapshot
		want valkeyiov1alpha1.RolloutPhase
	}{
		{"nil snapshot -> Idle", nil, valkeyiov1alpha1.RolloutPhaseIdle},
		{"empty pending -> Idle", &rolloutSnapshot{}, valkeyiov1alpha1.RolloutPhaseIdle},
		{
			"replicas only -> RollingReplicas",
			&rolloutSnapshot{pending: []string{"cache-1", "cache-2"}, primaryNode: "cache-0"},
			valkeyiov1alpha1.RolloutPhaseRollingReplicas,
		},
		{
			"replica and primary -> RollingReplicas (primary last, replicas first)",
			&rolloutSnapshot{pending: []string{"cache-1", "cache-0"}, primaryNode: "cache-0"},
			valkeyiov1alpha1.RolloutPhaseRollingReplicas,
		},
		{
			"only primary pending -> Failover",
			&rolloutSnapshot{pending: []string{"cache-0"}, primaryNode: "cache-0"},
			valkeyiov1alpha1.RolloutPhaseFailover,
		},
		{
			"only former primary (now a replica) pending -> RollingPrimary",
			&rolloutSnapshot{pending: []string{"cache-0"}, primaryNode: "cache-1"},
			valkeyiov1alpha1.RolloutPhaseRollingPrimary,
		},
		{
			"single pending, no primary observed yet -> RollingReplicas",
			&rolloutSnapshot{pending: []string{"cache-0"}, primaryNode: ""},
			valkeyiov1alpha1.RolloutPhaseRollingReplicas,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, chooseRolloutPhase(tc.snap))
		})
	}
}

func TestValkeyNodeResourcePodNameFor(t *testing.T) {
	// The pod backing a single-replica StatefulSet named "valkey-<node>"
	// is always "valkey-<node>-0" by StatefulSet convention.
	assert.Equal(t, "valkey-cache-0-0", valkeyNodeResourcePodNameFor("cache-0"))
	assert.Equal(t, "valkey-mycache-7-0", valkeyNodeResourcePodNameFor("mycache-7"))
}

func TestPodIsReady(t *testing.T) {
	tests := []struct {
		name string
		pod  *corev1.Pod
		want bool
	}{
		{
			"no conditions -> not ready",
			&corev1.Pod{},
			false,
		},
		{
			"PodReady = True -> ready",
			&corev1.Pod{Status: corev1.PodStatus{Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			}}},
			true,
		},
		{
			"PodReady = False -> not ready",
			&corev1.Pod{Status: corev1.PodStatus{Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionFalse},
			}}},
			false,
		},
		{
			"PodReady = Unknown -> not ready",
			&corev1.Pod{Status: corev1.PodStatus{Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionUnknown},
			}}},
			false,
		},
		{
			"other conditions present, no PodReady -> not ready",
			&corev1.Pod{Status: corev1.PodStatus{Conditions: []corev1.PodCondition{
				{Type: corev1.PodInitialized, Status: corev1.ConditionTrue},
			}}},
			false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, podIsReady(tc.pod))
		})
	}
}

func TestPodNeedsCycling(t *testing.T) {
	podWithRevision := func(rev string) *corev1.Pod {
		labels := map[string]string{}
		if rev != "" {
			labels[podRevisionLabel] = rev
		}
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: labels}}
	}
	tests := []struct {
		name string
		sts  *appsv1.StatefulSet
		pod  *corev1.Pod
		want bool
	}{
		{
			"empty UpdateRevision (freshly created SS) -> not pending",
			&appsv1.StatefulSet{},
			podWithRevision("v1"),
			false,
		},
		{
			"pod missing -> pending (SS controller will recreate at UpdateRevision)",
			&appsv1.StatefulSet{Status: appsv1.StatefulSetStatus{UpdateRevision: "v2"}},
			nil,
			true,
		},
		{
			"pod label matches UpdateRevision -> not pending",
			&appsv1.StatefulSet{Status: appsv1.StatefulSetStatus{UpdateRevision: "v2"}},
			podWithRevision("v2"),
			false,
		},
		{
			"pod label differs from UpdateRevision -> pending",
			&appsv1.StatefulSet{Status: appsv1.StatefulSetStatus{UpdateRevision: "v2"}},
			podWithRevision("v1"),
			true,
		},
		{
			"pod has no controller-revision-hash label -> pending",
			&appsv1.StatefulSet{Status: appsv1.StatefulSetStatus{UpdateRevision: "v2"}},
			podWithRevision(""),
			true,
		},
		{
			// Regression for the OnDelete rollout loop: under OnDelete
			// the SS controller does not reliably advance
			// CurrentRevision after the operator manually deletes a
			// pod, but the recreated pod's controller-revision-hash
			// label is already at UpdateRevision. The old check
			// (CurrentRevision != UpdateRevision) returned `pending`
			// here and the executor re-deleted the same pod every
			// reconcile. The new check returns `not pending` because
			// it asks the pod directly.
			"CurrentRevision lagging UpdateRevision but pod already at UpdateRevision -> not pending",
			&appsv1.StatefulSet{Status: appsv1.StatefulSetStatus{
				CurrentRevision: "v1", UpdateRevision: "v2",
			}},
			podWithRevision("v2"),
			false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, podNeedsCycling(tc.sts, tc.pod))
		})
	}
}

func TestDeriveConditionsSplitBrain(t *testing.T) {
	r := &ValkeyReconciler{}
	v := &valkeyiov1alpha1.Valkey{
		ObjectMeta: metav1.ObjectMeta{Name: "cache", Namespace: "ns"},
		Spec:       valkeyiov1alpha1.ValkeySpec{Replicas: 2},
	}

	t.Run("two masters -> Degraded with SplitBrain reason", func(t *testing.T) {
		v.Status = valkeyiov1alpha1.ValkeyStatus{}
		// observePrimary returned "" for the primary because >1 master
		// was seen; pass the masters list along.
		r.deriveConditions(v, "", []string{"valkey-cache-1-0", "valkey-cache-0-0"}, nil)

		degraded := findCondition(v.Status.Conditions, valkeyiov1alpha1.ConditionDegraded)
		if assert.NotNil(t, degraded) {
			assert.Equal(t, metav1.ConditionTrue, degraded.Status)
			assert.Equal(t, "SplitBrain", degraded.Reason)
			// Pod names sorted in the message so it's stable in tests.
			assert.Contains(t, degraded.Message, "valkey-cache-0-0, valkey-cache-1-0")
		}
		ready := findCondition(v.Status.Conditions, valkeyiov1alpha1.ConditionReady)
		if assert.NotNil(t, ready) {
			assert.Equal(t, metav1.ConditionFalse, ready.Status, "Ready must be False during split-brain")
		}
		assert.Equal(t, valkeyiov1alpha1.ClusterStateDegraded, v.Status.State)
	})

	t.Run("single master -> Degraded cleared, Ready True", func(t *testing.T) {
		v.Status = valkeyiov1alpha1.ValkeyStatus{}
		r.deriveConditions(v, "valkey-cache-0-0", []string{"valkey-cache-0-0"}, nil)

		degraded := findCondition(v.Status.Conditions, valkeyiov1alpha1.ConditionDegraded)
		if assert.NotNil(t, degraded) {
			assert.Equal(t, metav1.ConditionFalse, degraded.Status,
				"Degraded must clear when split-brain resolves")
		}
		ready := findCondition(v.Status.Conditions, valkeyiov1alpha1.ConditionReady)
		if assert.NotNil(t, ready) {
			assert.Equal(t, metav1.ConditionTrue, ready.Status)
		}
		assert.Equal(t, valkeyiov1alpha1.ClusterStateReady, v.Status.State)
	})

	t.Run("no master -> Reconciling, not Degraded", func(t *testing.T) {
		v.Status = valkeyiov1alpha1.ValkeyStatus{}
		r.deriveConditions(v, "", nil, nil)

		degraded := findCondition(v.Status.Conditions, valkeyiov1alpha1.ConditionDegraded)
		if assert.NotNil(t, degraded) {
			assert.Equal(t, metav1.ConditionFalse, degraded.Status)
		}
		assert.Equal(t, valkeyiov1alpha1.ClusterStateReconciling, v.Status.State,
			"no primary != split-brain; should reconcile, not Degrade")
	})

	// Sentinel's internal failover handoff briefly leaves every pod
	// reporting role:replica (old master demoted before new master
	// promoted). Without the FailoverInProgress branch, kubectl
	// describe surfaces "PrimaryLost" during this transient and the
	// user sees what looks like an outage. The branch only fires
	// when a failover was issued inside the debounce window.
	t.Run("no master, failover recently issued -> FailoverInProgress (not PrimaryLost)", func(t *testing.T) {
		ts := metav1.NewTime(time.Now().Add(-2 * time.Second))
		v.Status = valkeyiov1alpha1.ValkeyStatus{
			Rollout: &valkeyiov1alpha1.RolloutStatus{FailoverIssuedAt: &ts},
		}
		// monitoredBy is empty here but the recent-failover branch
		// must take precedence: it knows a failover is in flight
		// regardless of whether the sentinel set is currently
		// observable in status.
		r.deriveConditions(v, "", nil, nil)

		ready := findCondition(v.Status.Conditions, valkeyiov1alpha1.ConditionReady)
		if assert.NotNil(t, ready) {
			assert.Equal(t, "FailoverInProgress", ready.Reason,
				"a no-master observation immediately after issuing SENTINEL FAILOVER is a handoff window, not an outage")
		}
		assert.Equal(t, "FailoverInProgress", v.Status.Reason)
	})
}

func findCondition(conds []metav1.Condition, t string) *metav1.Condition {
	for i := range conds {
		if conds[i].Type == t {
			return &conds[i]
		}
	}
	return nil
}

func TestSetSplitBrainCondition(t *testing.T) {
	r := &ValkeyReconciler{}
	v := &valkeyiov1alpha1.Valkey{
		ObjectMeta: metav1.ObjectMeta{Name: "cache", Namespace: "ns"},
		Spec:       valkeyiov1alpha1.ValkeySpec{Replicas: 2},
	}
	setSplitBrainCondition(r, v, []string{"valkey-cache-1-0", "valkey-cache-0-0"})

	// Sort-stable in the message.
	deg := findCondition(v.Status.Conditions, valkeyiov1alpha1.ConditionDegraded)
	if assert.NotNil(t, deg) {
		assert.Equal(t, metav1.ConditionTrue, deg.Status)
		assert.Contains(t, deg.Message, "valkey-cache-0-0, valkey-cache-1-0")
	}
	ready := findCondition(v.Status.Conditions, valkeyiov1alpha1.ConditionReady)
	if assert.NotNil(t, ready) {
		assert.Equal(t, metav1.ConditionFalse, ready.Status)
	}
	prog := findCondition(v.Status.Conditions, valkeyiov1alpha1.ConditionProgressing)
	if assert.NotNil(t, prog) {
		assert.Equal(t, metav1.ConditionFalse, prog.Status,
			"Progressing should be False during split-brain: we are NOT making progress, we are stuck")
	}
	assert.Equal(t, valkeyiov1alpha1.ClusterStateDegraded, v.Status.State)
}

func TestPickStablePrimary(t *testing.T) {
	a := primaryCandidate{PodName: "valkey-cache-0-0", Host: "valkey-cache-0.ns.svc.cluster.local"}
	b := primaryCandidate{PodName: "valkey-cache-1-0", Host: "valkey-cache-1.ns.svc.cluster.local"}
	c := primaryCandidate{PodName: "valkey-cache-2-0", Host: "valkey-cache-2.ns.svc.cluster.local"}

	tests := []struct {
		name           string
		candidates     []primaryCandidate
		prev           string
		wantPod        string
		wantHost       string
	}{
		{
			"no masters observed -> empty (caller treats as 'no primary yet')",
			nil, "valkey-cache-0-0", "", "",
		},
		{
			"single master -> return it (unambiguous happy path)",
			[]primaryCandidate{a}, "", "valkey-cache-0-0", a.Host,
		},
		{
			"two masters, previous still among them -> prefer previous (stabilization)",
			[]primaryCandidate{a, b}, "valkey-cache-0-0", "valkey-cache-0-0", a.Host,
		},
		{
			// Regression for the live-cluster cascade: cache-1 cycled, came
			// back as master, INFO sees both. With stabilization off,
			// observePrimary returned "" and the rollout cycled cache-0
			// (the real primary) in the same reconcile. With stabilization,
			// previous primary (cache-0) is preferred, PrimaryEndpoint
			// stays stable, sentinel template doesn't flip, wireScaledUp-
			// Replicas can REPLICAOF cache-1 back.
			"two masters with cache-1 listed first - previous=cache-0 still preferred over iteration order",
			[]primaryCandidate{b, a}, "valkey-cache-0-0", "valkey-cache-0-0", a.Host,
		},
		{
			"two masters, previous unset -> ambiguous, refuse to choose",
			[]primaryCandidate{a, b}, "", "", "",
		},
		{
			// True failover scenario: the original primary is gone from
			// the master set (it crashed; sentinel promoted a replica).
			// Stabilization must not pin to a dead pod - refuse and let
			// the caller flag Degraded so a human can confirm.
			"two masters, previous no longer among them -> refuse to choose",
			[]primaryCandidate{b, c}, "valkey-cache-0-0", "", "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotPod, gotHost := pickStablePrimary(tc.candidates, tc.prev)
			assert.Equal(t, tc.wantPod, gotPod)
			assert.Equal(t, tc.wantHost, gotHost)
		})
	}
}

func TestBuildValkeyNodeStampsConfigHash(t *testing.T) {
	// Parity with ValkeyCluster: the rendered config's sha256 must land
	// on ValkeyNode.Spec.ServerConfigHash so the ValkeyNode controller
	// stamps it as a pod-template annotation and a Spec.Config change
	// triggers a rollout.
	v := &valkeyiov1alpha1.Valkey{
		ObjectMeta: metav1.ObjectMeta{Name: "cache", Namespace: "ns"},
		Spec:       valkeyiov1alpha1.ValkeySpec{Replicas: 1},
	}
	node := buildValkeyNode(v, 1, "deadbeef")
	assert.Equal(t, "deadbeef", node.Spec.ServerConfigHash,
		"buildValkeyNode must propagate configHash to Spec.ServerConfigHash; without this, Spec.Config changes never trigger a rollout")

	// Empty configHash is also acceptable (caller didn't compute one
	// yet); ValkeyNode controller omits the annotation in that case.
	node = buildValkeyNode(v, 1, "")
	assert.Equal(t, "", node.Spec.ServerConfigHash)
}

// Valkey-managed ValkeyNodes opt into a PVC-backed writable config so
// the REPLICAOF directive survives pod cycles. Without this the rollout
// state machine's planned restart of a replica produces a brief master
// pod that the operator has to re-wire via wireScaledUpReplicas - the
// transient multi-master window the user observed during Spec.Config
// changes.
//
// ValkeyCluster nodes deliberately do NOT set the flag: cluster mode
// persists its role in nodes.conf on the data PVC and adding a new VCT
// entry to existing cluster StatefulSets would fail the immutability
// guard. buildClusterValkeyNode (valkeycluster_controller.go) does not
// set this field; toggling that is a structural change to ValkeyCluster
// which is explicitly out of scope for this branch.
func TestBuildValkeyNode_PersistWritableConfig(t *testing.T) {
	v := &valkeyiov1alpha1.Valkey{
		ObjectMeta: metav1.ObjectMeta{Name: "cache", Namespace: "ns"},
		Spec:       valkeyiov1alpha1.ValkeySpec{Replicas: 1},
	}
	node := buildValkeyNode(v, 0, "")
	assert.True(t, node.Spec.PersistWritableConfig,
		"Valkey-managed ValkeyNodes must opt into writable-config persistence so REPLICAOF survives pod cycles")
}

// shouldIssueFailover is the SENTINEL FAILOVER debounce gate. Without
// it the rollout state machine pokes Sentinel on every reconcile
// during the Failover phase (~10s) for the duration of the promotion
// (30s+ on a healthy cluster), producing event spam and obscuring
// real errors. The debounce allows re-issue when the previous
// failover has clearly taken effect (primary moved out of the pending
// set) or when failoverDebounce has elapsed without progress.
func TestShouldIssueFailover(t *testing.T) {
	cache0 := "cache-0"
	cache1 := "cache-1"

	mk := func(issuedAgo time.Duration, primary string, pending []string) *valkeyiov1alpha1.Valkey {
		v := &valkeyiov1alpha1.Valkey{}
		v.Status.PrimaryPodName = primary
		v.Status.Rollout = &valkeyiov1alpha1.RolloutStatus{
			PendingNodes: pending,
		}
		if issuedAgo >= 0 {
			t := metav1.NewTime(time.Now().Add(-issuedAgo))
			v.Status.Rollout.FailoverIssuedAt = &t
		}
		return v
	}

	t.Run("no prior issuance -> issue", func(t *testing.T) {
		v := &valkeyiov1alpha1.Valkey{Status: valkeyiov1alpha1.ValkeyStatus{}}
		assert.True(t, shouldIssueFailover(v))
	})

	t.Run("recent issuance, primary still pending -> skip", func(t *testing.T) {
		v := mk(5*time.Second, cache0, []string{cache0})
		assert.False(t, shouldIssueFailover(v),
			"recent failover whose target is still in the pending set should not re-issue")
	})

	t.Run("recent issuance, primary moved -> issue", func(t *testing.T) {
		v := mk(5*time.Second, cache1, []string{cache0})
		assert.True(t, shouldIssueFailover(v),
			"primary moved out of the pending set means the prior failover took effect; allow a fresh one")
	})

	t.Run("stale issuance -> issue regardless of pending set", func(t *testing.T) {
		v := mk(failoverDebounce+time.Second, cache0, []string{cache0})
		assert.True(t, shouldIssueFailover(v),
			"after the debounce window we re-issue even if the primary still appears pending; the prior attempt is treated as wedged")
	})
}

// failoverRecentlyIssued is the deriveConditions hook that
// distinguishes the brief "Sentinel handoff in flight" window (every
// pod reports role:replica) from a real PrimaryLost. Without it,
// kubectl describe surfaces a misleading PrimaryLost during the
// ~hundreds-of-milliseconds promotion gap.
func TestFailoverRecentlyIssued(t *testing.T) {
	t.Run("no rollout status -> false", func(t *testing.T) {
		v := &valkeyiov1alpha1.Valkey{}
		assert.False(t, failoverRecentlyIssued(v))
	})

	t.Run("nil timestamp -> false", func(t *testing.T) {
		v := &valkeyiov1alpha1.Valkey{
			Status: valkeyiov1alpha1.ValkeyStatus{Rollout: &valkeyiov1alpha1.RolloutStatus{}},
		}
		assert.False(t, failoverRecentlyIssued(v))
	})

	t.Run("recent issuance -> true", func(t *testing.T) {
		ts := metav1.NewTime(time.Now().Add(-2 * time.Second))
		v := &valkeyiov1alpha1.Valkey{
			Status: valkeyiov1alpha1.ValkeyStatus{
				Rollout: &valkeyiov1alpha1.RolloutStatus{FailoverIssuedAt: &ts},
			},
		}
		assert.True(t, failoverRecentlyIssued(v))
	})

	t.Run("stale issuance -> false", func(t *testing.T) {
		ts := metav1.NewTime(time.Now().Add(-failoverDebounce - time.Second))
		v := &valkeyiov1alpha1.Valkey{
			Status: valkeyiov1alpha1.ValkeyStatus{
				Rollout: &valkeyiov1alpha1.RolloutStatus{FailoverIssuedAt: &ts},
			},
		}
		assert.False(t, failoverRecentlyIssued(v))
	})
}

// recordFailoverIssued stamps Status.Rollout.FailoverIssuedAt. The
// helper exists so the debounce field is set in exactly one place;
// without that, the publishRolloutStatus path could clobber the
// timestamp by overwriting Status.Rollout wholesale.
func TestRecordFailoverIssued(t *testing.T) {
	v := &valkeyiov1alpha1.Valkey{}
	recordFailoverIssued(v)
	if assert.NotNil(t, v.Status.Rollout, "Rollout must be lazily created when recording failover") {
		assert.NotNil(t, v.Status.Rollout.FailoverIssuedAt,
			"FailoverIssuedAt must be stamped so shouldIssueFailover can debounce")
		assert.WithinDuration(t, time.Now(), v.Status.Rollout.FailoverIssuedAt.Time, 5*time.Second)
	}

	// Pre-existing Rollout fields (PendingNodes etc.) must be preserved.
	pending := []string{"cache-1"}
	v = &valkeyiov1alpha1.Valkey{
		Status: valkeyiov1alpha1.ValkeyStatus{
			Rollout: &valkeyiov1alpha1.RolloutStatus{
				Phase:        valkeyiov1alpha1.RolloutPhaseRollingReplicas,
				PendingNodes: pending,
			},
		},
	}
	recordFailoverIssued(v)
	assert.Equal(t, valkeyiov1alpha1.RolloutPhaseRollingReplicas, v.Status.Rollout.Phase,
		"recordFailoverIssued must not overwrite Phase")
	assert.Equal(t, pending, v.Status.Rollout.PendingNodes,
		"recordFailoverIssued must not overwrite PendingNodes")
}
