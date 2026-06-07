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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	valkeyiov1alpha1 "valkey.io/valkey-operator/api/v1alpha1"
)

var _ = Describe("ValkeySentinel controller", func() {
	ctx := context.Background()

	newReconciler := func() *ValkeySentinelReconciler {
		return &ValkeySentinelReconciler{
			Client:   k8sClient,
			Scheme:   k8sClient.Scheme(),
			Recorder: events.NewFakeRecorder(200),
		}
	}

	const name = "test-monitors"
	const monitoredValkeyName = "test-monitored"
	nn := types.NamespacedName{Name: name, Namespace: "default"}
	valkeyNN := types.NamespacedName{Name: monitoredValkeyName, Namespace: "default"}

	// seedMonitoredValkey creates a Valkey with a primary endpoint
	// AND the per-Valkey sentinel-auth Secret that
	// collectMonitorConfigs reads to bake the `sentinel auth-user`
	// line into the rendered template. Without both, the sentinel
	// controller skips the Valkey and defers SS creation (the "one
	// restart per fresh deploy" fix) and the assertions below that
	// expect the SS to exist would fail.
	seedMonitoredValkey := func() {
		v := &valkeyiov1alpha1.Valkey{}
		if err := k8sClient.Get(ctx, valkeyNN, v); apierrors.IsNotFound(err) {
			Expect(k8sClient.Create(ctx, &valkeyiov1alpha1.Valkey{
				ObjectMeta: metav1.ObjectMeta{
					Name:      monitoredValkeyName,
					Namespace: "default",
					Labels:    map[string]string{"tier": "caching"},
				},
				Spec: valkeyiov1alpha1.ValkeySpec{Replicas: 1},
			})).To(Succeed())
		}
		// Set status separately - Status is a subresource.
		Expect(k8sClient.Get(ctx, valkeyNN, v)).To(Succeed())
		v.Status.PrimaryPodName = "valkey-" + monitoredValkeyName + "-0-0"
		v.Status.PrimaryEndpoint = &valkeyiov1alpha1.Endpoint{
			Host: "valkey-" + monitoredValkeyName + "-0.default.svc.cluster.local",
			Port: int32(DefaultPort),
		}
		Expect(k8sClient.Status().Update(ctx, v)).To(Succeed())

		// The sentinel-auth secret the Valkey controller would have
		// projected (see projectSentinelAuthSecret).
		authSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      monitoredValkeyName + "-sentinel-auth",
				Namespace: "default",
			},
			Data: map[string][]byte{
				"username": []byte("_sentinel"),
				"password": []byte("test-password"),
			},
		}
		err := k8sClient.Create(ctx, authSecret)
		if err != nil && !apierrors.IsAlreadyExists(err) {
			Expect(err).NotTo(HaveOccurred())
		}
	}

	BeforeEach(func() {
		seedMonitoredValkey()
		existing := &valkeyiov1alpha1.ValkeySentinel{}
		if err := k8sClient.Get(ctx, nn, existing); apierrors.IsNotFound(err) {
			Expect(k8sClient.Create(ctx, &valkeyiov1alpha1.ValkeySentinel{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
				Spec: valkeyiov1alpha1.ValkeySentinelSpec{
					Replicas: 3,
					ValkeySelector: metav1.LabelSelector{
						MatchLabels: map[string]string{"tier": "caching"},
					},
				},
			})).To(Succeed())
		}
	})

	AfterEach(func() {
		s := &valkeyiov1alpha1.ValkeySentinel{}
		if err := k8sClient.Get(ctx, nn, s); err == nil {
			_ = k8sClient.Delete(ctx, s)
		}
		v := &valkeyiov1alpha1.Valkey{}
		if err := k8sClient.Get(ctx, valkeyNN, v); err == nil {
			_ = k8sClient.Delete(ctx, v)
		}
		_ = k8sClient.Delete(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
			Name: monitoredValkeyName + "-sentinel-auth", Namespace: "default",
		}})
	})

	It("provisions Service, ConfigMap, PDB, and StatefulSet", func() {
		r := newReconciler()
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		Expect(err).NotTo(HaveOccurred())

		svc := &corev1.Service{}
		Expect(k8sClient.Get(ctx, client.ObjectKey{Name: "valkey-sentinel-" + name, Namespace: "default"}, svc)).To(Succeed())
		Expect(svc.Spec.ClusterIP).To(Equal(headlessClusterIP))
		Expect(svc.Spec.Ports[0].Port).To(Equal(int32(valkeyiov1alpha1.SentinelPort)))

		cm := &corev1.ConfigMap{}
		Expect(k8sClient.Get(ctx, client.ObjectKey{Name: "valkey-sentinel-" + name, Namespace: "default"}, cm)).To(Succeed())
		Expect(cm.Data).To(HaveKey(sentinelStartupScriptKey))
		Expect(cm.Data).To(HaveKey(sentinelConfigTemplateKey))
		Expect(cm.Data[sentinelConfigTemplateKey]).To(ContainSubstring("__POD_IP__"))
		// The seeded Valkey has a primary endpoint, so the template
		// includes the sentinel monitor block for it.
		Expect(cm.Data[sentinelConfigTemplateKey]).To(ContainSubstring(
			"sentinel monitor " + monitoredValkeyName))

		// Aggregated auth Secret is always present (empty when no matches).
		authSecret := &corev1.Secret{}
		Expect(k8sClient.Get(ctx, client.ObjectKey{Name: "valkey-sentinel-" + name + "-auth", Namespace: "default"}, authSecret)).To(Succeed())

		pdb := &policyv1.PodDisruptionBudget{}
		Expect(k8sClient.Get(ctx, client.ObjectKey{Name: "valkey-sentinel-" + name, Namespace: "default"}, pdb)).To(Succeed())
		Expect(pdb.Spec.MaxUnavailable.IntValue()).To(Equal(1))

		ss := &appsv1.StatefulSet{}
		Expect(k8sClient.Get(ctx, client.ObjectKey{Name: "valkey-sentinel-" + name, Namespace: "default"}, ss)).To(Succeed())
		Expect(*ss.Spec.Replicas).To(Equal(int32(3)))
		Expect(ss.Spec.Template.Spec.Containers).To(HaveLen(1))
		Expect(ss.Spec.Template.Spec.Containers[0].Ports[0].ContainerPort).To(Equal(int32(valkeyiov1alpha1.SentinelPort)))
		// Pod template carries the config-hash annotation that drives rolling restarts.
		Expect(ss.Spec.Template.Annotations).To(HaveKey(sentinelConfigHashAnnotation))
		// Auth secret is mounted read-only.
		var foundAuth bool
		for _, vol := range ss.Spec.Template.Spec.Volumes {
			if vol.Name == sentinelAuthVolumeName {
				Expect(vol.Secret).NotTo(BeNil())
				Expect(vol.Secret.SecretName).To(Equal("valkey-sentinel-" + name + "-auth"))
				foundAuth = true
			}
		}
		Expect(foundAuth).To(BeTrue())

		// Per-pod sentinel endpoints surfaced for sentinel-aware clients.
		// Reload the CR to read the latest status.
		updated := &valkeyiov1alpha1.ValkeySentinel{}
		Expect(k8sClient.Get(ctx, nn, updated)).To(Succeed())
		Expect(updated.Status.Endpoints).To(HaveLen(3))
		Expect(updated.Status.Endpoints[0].Host).To(Equal(
			"valkey-sentinel-" + name + "-0.valkey-sentinel-" + name + ".default.svc.cluster.local"))
		Expect(updated.Status.Endpoints[0].Port).To(Equal(int32(valkeyiov1alpha1.SentinelPort)))
		Expect(updated.Status.Endpoints[2].Host).To(Equal(
			"valkey-sentinel-" + name + "-2.valkey-sentinel-" + name + ".default.svc.cluster.local"))
	})

	// Regression for the sentinel-pod restart loop seen with the
	// ValkeyNode SS: the spec built by the operator left fields unset
	// that the API server defaults (probe SuccessThreshold/Scheme,
	// ContainerPort.Protocol, SS-level PVC retention etc.), so every
	// reconcile re-Updated the SS. The Template re-assign on Update
	// then bumped updateRevision and rolled the sentinel pods.
	//
	// The cheap way to enforce no-flap end-to-end: reconcile twice
	// against the real API server, capture the SS ResourceVersion
	// after each, and assert it stayed the same. The Update call only
	// bumps ResourceVersion when the persisted spec actually changed.
	It("the sentinel StatefulSet is stable across reconciles", func() {
		r := newReconciler()
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		Expect(err).NotTo(HaveOccurred())

		ssKey := client.ObjectKey{Name: "valkey-sentinel-" + name, Namespace: "default"}
		first := &appsv1.StatefulSet{}
		Expect(k8sClient.Get(ctx, ssKey, first)).To(Succeed())

		// Five reconciles, every one a no-op. The previous failure
		// mode in the live cluster was the operator re-Updating the
		// sentinel SS on every reconcile, bumping updateRevision and
		// rolling all sentinel pods - ~80 pod replacements per 30
		// minutes in the broken state. ResourceVersion must not move.
		for i := 0; i < 5; i++ {
			_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
			Expect(err).NotTo(HaveOccurred())
			next := &appsv1.StatefulSet{}
			Expect(k8sClient.Get(ctx, ssKey, next)).To(Succeed())
			Expect(next.ResourceVersion).To(Equal(first.ResourceVersion),
				"sentinel StatefulSet spec must be stable across reconciles; reconcile #%d bumped ResourceVersion", i+1)
		}
	})

	// Sentinel is designed to recover its state on restart: monitor and
	// auth are baked into the rendered ConfigMap; known-replicas and
	// known-sentinels are re-discovered via INFO and pubsub within
	// seconds; epoch is monotonic across the surviving quorum. We
	// therefore default to emptyDir to avoid a cluster-scoped
	// StorageClass dependency. Users who want durable state set
	// spec.persistence explicitly (covered by the next spec).
	It("uses a disk-backed emptyDir for the sentinel data dir by default", func() {
		r := newReconciler()
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		Expect(err).NotTo(HaveOccurred())

		ss := &appsv1.StatefulSet{}
		Expect(k8sClient.Get(ctx, client.ObjectKey{Name: "valkey-sentinel-" + name, Namespace: "default"}, ss)).To(Succeed())

		Expect(ss.Spec.VolumeClaimTemplates).To(BeEmpty(),
			"default sentinel deploy must NOT provision a PVC")
		var dataVol *corev1.Volume
		for i := range ss.Spec.Template.Spec.Volumes {
			v := &ss.Spec.Template.Spec.Volumes[i]
			if v.Name == sentinelDataVolumeName {
				dataVol = v
				break
			}
		}
		Expect(dataVol).NotTo(BeNil(), "sentinel-data volume missing on default-persistence path")
		Expect(dataVol.EmptyDir).NotTo(BeNil(), "sentinel-data must be emptyDir by default")
		Expect(dataVol.EmptyDir.Medium).To(Equal(corev1.StorageMedium("")),
			"sentinel-data must NOT be tmpfs (Medium: Memory) - disk-backed emptyDir preserves state across container restarts within a pod; tmpfs does not")
	})

	// Opt-in path: setting spec.persistence (any non-disabled value)
	// switches to a per-pod PVC. Used by deployments that want
	// epoch/myid stability across pod recreation rather than the
	// re-discovery flow.
	It("provisions a per-pod PVC when spec.persistence is set", func() {
		// Reset the fixture: delete and recreate with persistence
		// enabled. envtest does NOT run owner-reference garbage
		// collection, so delete the SS explicitly too; otherwise the
		// existing-SS branch hits the immutability check on next
		// reconcile.
		s := &valkeyiov1alpha1.ValkeySentinel{}
		Expect(k8sClient.Get(ctx, nn, s)).To(Succeed())
		Expect(k8sClient.Delete(ctx, s)).To(Succeed())
		ssKey := client.ObjectKey{Name: "valkey-sentinel-" + name, Namespace: "default"}
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: ssKey.Name, Namespace: "default"},
		}))).To(Succeed())
		Eventually(func() bool {
			err := k8sClient.Get(ctx, ssKey, &appsv1.StatefulSet{})
			return apierrors.IsNotFound(err)
		}).Should(BeTrue())
		Eventually(func() bool {
			err := k8sClient.Get(ctx, nn, &valkeyiov1alpha1.ValkeySentinel{})
			return apierrors.IsNotFound(err)
		}).Should(BeTrue())

		Expect(k8sClient.Create(ctx, &valkeyiov1alpha1.ValkeySentinel{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec: valkeyiov1alpha1.ValkeySentinelSpec{
				Replicas: 3,
				ValkeySelector: metav1.LabelSelector{
					MatchLabels: map[string]string{"tier": "caching"},
				},
				Persistence: &valkeyiov1alpha1.SentinelPersistenceSpec{},
			},
		})).To(Succeed())

		r := newReconciler()
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		Expect(err).NotTo(HaveOccurred())

		ss := &appsv1.StatefulSet{}
		Expect(k8sClient.Get(ctx, ssKey, ss)).To(Succeed())
		Expect(ss.Spec.VolumeClaimTemplates).To(HaveLen(1),
			"explicit spec.persistence must include a VolumeClaimTemplates entry")
		vct := ss.Spec.VolumeClaimTemplates[0]
		Expect(vct.Name).To(Equal(sentinelDataVolumeName))
		Expect(vct.Spec.Resources.Requests[corev1.ResourceStorage]).To(Equal(resource.MustParse("100Mi")),
			"default size should be 100Mi")
		// Pod template must NOT also list this volume - the SS
		// controller would refuse to create the pod (volume name
		// collision with the VCT-derived volume).
		for _, v := range ss.Spec.Template.Spec.Volumes {
			Expect(v.Name).NotTo(Equal(sentinelDataVolumeName),
				"sentinel-data volume should come from VolumeClaimTemplates, not pod.Spec.Volumes")
		}
	})
})

var _ = Describe("sentinelEndpoints", func() {
	It("renders per-pod StatefulSet DNS by replica count", func() {
		s := &valkeyiov1alpha1.ValkeySentinel{
			ObjectMeta: metav1.ObjectMeta{Name: "monitors", Namespace: "cache"},
			Spec:       valkeyiov1alpha1.ValkeySentinelSpec{Replicas: 3},
		}
		eps := sentinelEndpoints(s)
		Expect(eps).To(HaveLen(3))
		Expect(eps[0].Host).To(Equal("valkey-sentinel-monitors-0.valkey-sentinel-monitors.cache.svc.cluster.local"))
		Expect(eps[1].Host).To(Equal("valkey-sentinel-monitors-1.valkey-sentinel-monitors.cache.svc.cluster.local"))
		Expect(eps[2].Host).To(Equal("valkey-sentinel-monitors-2.valkey-sentinel-monitors.cache.svc.cluster.local"))
		for _, ep := range eps {
			Expect(ep.Port).To(Equal(int32(valkeyiov1alpha1.SentinelPort)))
		}
	})
})

var _ = Describe("renderSentinelTemplate", func() {
	It("produces only the base block when no Valkeys are monitored", func() {
		out := renderSentinelTemplate(nil)
		Expect(out).To(ContainSubstring("port 26379"))
		Expect(out).To(ContainSubstring("__POD_IP__"))
		Expect(out).NotTo(ContainSubstring("sentinel monitor "))
		Expect(out).NotTo(ContainSubstring("__SENTINEL_AUTH_PASS_"))
	})
	It("bakes monitor + auth-user + tuning per Valkey with sorted, stable output", func() {
		monitors := []monitoredValkey{
			{Name: "b-cache", Host: "valkey-b-cache-0.test.svc.cluster.local", Port: 6379, Quorum: 2, Username: "_sentinel",
				Config: map[string]string{"down-after-milliseconds": "30000", "failover-timeout": "180000"}},
			{Name: "a-cache", Host: "valkey-a-cache-0.test.svc.cluster.local", Port: 6379, Quorum: 3, Username: "_sentinel"},
		}
		out := renderSentinelTemplate(monitors)
		// Sorted alphabetically so the hash is stable across input order.
		Expect(out).To(MatchRegexp(`(?s)sentinel monitor a-cache valkey-a-cache-0\.test\.svc\.cluster\.local 6379 3.*sentinel monitor b-cache valkey-b-cache-0\.test\.svc\.cluster\.local 6379 2`))
		Expect(out).To(ContainSubstring("sentinel auth-user a-cache _sentinel"))
		Expect(out).To(ContainSubstring("sentinel auth-pass a-cache __SENTINEL_AUTH_PASS_a-cache__"))
		Expect(out).To(ContainSubstring("sentinel down-after-milliseconds b-cache 30000"))
		Expect(out).To(ContainSubstring("sentinel failover-timeout b-cache 180000"))
		// Re-rendering with the same input is byte-stable.
		Expect(renderSentinelTemplate(monitors)).To(Equal(out))
	})
})

var _ = Describe("ValkeySentinelSpec.EffectiveQuorum", func() {
	It("defaults to floor(replicas/2)+1", func() {
		Expect((&valkeyiov1alpha1.ValkeySentinelSpec{Replicas: 3}).EffectiveQuorum()).To(Equal(int32(2)))
		Expect((&valkeyiov1alpha1.ValkeySentinelSpec{Replicas: 5}).EffectiveQuorum()).To(Equal(int32(3)))
		Expect((&valkeyiov1alpha1.ValkeySentinelSpec{Replicas: 7}).EffectiveQuorum()).To(Equal(int32(4)))
	})
	It("honours explicit override", func() {
		two := int32(2)
		Expect((&valkeyiov1alpha1.ValkeySentinelSpec{Replicas: 7, Quorum: &two}).EffectiveQuorum()).To(Equal(int32(2)))
	})
})

// Regression for the "one restart per fresh deploy" issue: when a
// ValkeySentinel is created and no selected Valkey has yet reported
// Status.PrimaryEndpoint, the sentinel controller used to create the
// StatefulSet anyway (with an empty-monitor configHash). The moment
// the first Valkey bootstrapped, the configHash flipped and
// RollingUpdate rolled every sentinel pod - the "always restarted
// once before becoming stable" symptom.
//
// With the deferral, the StatefulSet is NOT created until at least
// one monitor exists. The status surfaces AwaitingMonitor until then.
var _ = Describe("ValkeySentinel defers SS creation until a monitor exists", func() {
	ctx := context.Background()

	newReconciler := func() *ValkeySentinelReconciler {
		return &ValkeySentinelReconciler{
			Client:   k8sClient,
			Scheme:   k8sClient.Scheme(),
			Recorder: events.NewFakeRecorder(200),
		}
	}

	const sentinelName = "defer-test"
	const valkeyName = "defer-test-valkey"
	const ns = "default"
	sentinelNN := types.NamespacedName{Name: sentinelName, Namespace: ns}
	valkeyNN := types.NamespacedName{Name: valkeyName, Namespace: ns}
	ssKey := client.ObjectKey{Name: "valkey-sentinel-" + sentinelName, Namespace: ns}

	BeforeEach(func() {
		// Sentinel selects label tier=defer-test.
		Expect(k8sClient.Create(ctx, &valkeyiov1alpha1.ValkeySentinel{
			ObjectMeta: metav1.ObjectMeta{Name: sentinelName, Namespace: ns},
			Spec: valkeyiov1alpha1.ValkeySentinelSpec{
				Replicas: 3,
				ValkeySelector: metav1.LabelSelector{
					MatchLabels: map[string]string{"tier": "defer-test"},
				},
			},
		})).To(Succeed())
	})

	AfterEach(func() {
		s := &valkeyiov1alpha1.ValkeySentinel{}
		if err := k8sClient.Get(ctx, sentinelNN, s); err == nil {
			_ = k8sClient.Delete(ctx, s)
		}
		v := &valkeyiov1alpha1.Valkey{}
		if err := k8sClient.Get(ctx, valkeyNN, v); err == nil {
			_ = k8sClient.Delete(ctx, v)
		}
		_ = k8sClient.Delete(ctx, &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: ssKey.Name, Namespace: ns},
		})
	})

	It("does not create the StatefulSet when no Valkey has a primary endpoint yet", func() {
		r := newReconciler()
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: sentinelNN})
		Expect(err).NotTo(HaveOccurred())

		ss := &appsv1.StatefulSet{}
		err = k8sClient.Get(ctx, ssKey, ss)
		Expect(apierrors.IsNotFound(err)).To(BeTrue(),
			"sentinel SS must be deferred when there are no monitors yet; otherwise the next reconcile flips configHash and rolls every pod")

		// Status should reflect AwaitingMonitor, not Ready.
		updated := &valkeyiov1alpha1.ValkeySentinel{}
		Expect(k8sClient.Get(ctx, sentinelNN, updated)).To(Succeed())
		ready := findValkeySentinelCondition(updated.Status.Conditions, valkeyiov1alpha1.ConditionReady)
		Expect(ready).NotTo(BeNil(), "Ready condition should be set")
		Expect(ready.Status).To(Equal(metav1.ConditionFalse))
		Expect(ready.Reason).To(Equal("AwaitingMonitor"))
	})

	It("creates the StatefulSet once a selected Valkey reports its primary endpoint, with the monitor baked in from the start", func() {
		r := newReconciler()
		// First reconcile: no Valkey yet, SS deferred.
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: sentinelNN})
		Expect(err).NotTo(HaveOccurred())
		Expect(apierrors.IsNotFound(k8sClient.Get(ctx, ssKey, &appsv1.StatefulSet{}))).To(BeTrue())

		// Create a matching Valkey with a primary endpoint AND the
		// sentinel-auth secret the controller would have projected.
		v := &valkeyiov1alpha1.Valkey{
			ObjectMeta: metav1.ObjectMeta{
				Name:      valkeyName,
				Namespace: ns,
				Labels:    map[string]string{"tier": "defer-test"},
			},
			Spec: valkeyiov1alpha1.ValkeySpec{Replicas: 1},
		}
		Expect(k8sClient.Create(ctx, v)).To(Succeed())
		Expect(k8sClient.Get(ctx, valkeyNN, v)).To(Succeed())
		v.Status.PrimaryPodName = "valkey-" + valkeyName + "-0-0"
		v.Status.PrimaryEndpoint = &valkeyiov1alpha1.Endpoint{
			Host: "valkey-" + valkeyName + "-0.default.svc.cluster.local",
			Port: int32(DefaultPort),
		}
		Expect(k8sClient.Status().Update(ctx, v)).To(Succeed())
		Expect(k8sClient.Create(ctx, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: valkeyName + "-sentinel-auth", Namespace: ns},
			Data: map[string][]byte{
				"username": []byte("_sentinel"),
				"password": []byte("test-password"),
			},
		})).To(Succeed())
		DeferCleanup(func() {
			_ = k8sClient.Delete(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
				Name: valkeyName + "-sentinel-auth", Namespace: ns,
			}})
		})

		// Second reconcile: a monitor exists, SS gets created with the
		// monitor block already in the configHash.
		_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: sentinelNN})
		Expect(err).NotTo(HaveOccurred())

		ss := &appsv1.StatefulSet{}
		Expect(k8sClient.Get(ctx, ssKey, ss)).To(Succeed())
		rvAtCreate := ss.ResourceVersion
		hashAtCreate := ss.Spec.Template.Annotations[sentinelConfigHashAnnotation]
		Expect(hashAtCreate).NotTo(BeEmpty())

		// The ConfigMap MUST contain the monitor line for the seeded
		// Valkey. If we deferred but then created with the empty
		// template, we'd be back at the pre-fix behaviour.
		cm := &corev1.ConfigMap{}
		Expect(k8sClient.Get(ctx, client.ObjectKey{Name: "valkey-sentinel-" + sentinelName, Namespace: ns}, cm)).To(Succeed())
		Expect(cm.Data[sentinelConfigTemplateKey]).To(ContainSubstring("sentinel monitor " + valkeyName))

		// Third reconcile: nothing should change. ResourceVersion and
		// configHash both stable. This is the "no restart" property:
		// the SS comes up with the right template the first time, so
		// no subsequent reconcile causes a rolling update.
		_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: sentinelNN})
		Expect(err).NotTo(HaveOccurred())
		after := &appsv1.StatefulSet{}
		Expect(k8sClient.Get(ctx, ssKey, after)).To(Succeed())
		Expect(after.ResourceVersion).To(Equal(rvAtCreate),
			"SS must not be Updated after a monitor-aware initial create; that would cause a rolling restart")
		Expect(after.Spec.Template.Annotations[sentinelConfigHashAnnotation]).To(Equal(hashAtCreate),
			"configHash must be stable across reconciles once monitors are known")
	})
})

func findValkeySentinelCondition(conds []metav1.Condition, t string) *metav1.Condition {
	for i := range conds {
		if conds[i].Type == t {
			return &conds[i]
		}
	}
	return nil
}

// Regression for the persistence immutability contract: StatefulSet's
// VolumeClaimTemplates is immutable after the SS exists. Toggling
// spec.persistence on an already-deployed ValkeySentinel must surface
// a clear error so the user knows to delete and recreate.
var _ = Describe("ValkeySentinel persistence is immutable after creation", func() {
	ctx := context.Background()

	newReconciler := func() *ValkeySentinelReconciler {
		return &ValkeySentinelReconciler{
			Client:   k8sClient,
			Scheme:   k8sClient.Scheme(),
			Recorder: events.NewFakeRecorder(200),
		}
	}

	const sentinelName = "immutable-test"
	const valkeyName = "immutable-test-valkey"
	const ns = "default"
	sentinelNN := types.NamespacedName{Name: sentinelName, Namespace: ns}
	valkeyNN := types.NamespacedName{Name: valkeyName, Namespace: ns}
	ssKey := client.ObjectKey{Name: "valkey-sentinel-" + sentinelName, Namespace: ns}

	seed := func(persistence *valkeyiov1alpha1.SentinelPersistenceSpec) {
		Expect(k8sClient.Create(ctx, &valkeyiov1alpha1.ValkeySentinel{
			ObjectMeta: metav1.ObjectMeta{Name: sentinelName, Namespace: ns},
			Spec: valkeyiov1alpha1.ValkeySentinelSpec{
				Replicas: 3,
				ValkeySelector: metav1.LabelSelector{
					MatchLabels: map[string]string{"tier": "immutable-test"},
				},
				Persistence: persistence,
			},
		})).To(Succeed())
		Expect(k8sClient.Create(ctx, &valkeyiov1alpha1.Valkey{
			ObjectMeta: metav1.ObjectMeta{Name: valkeyName, Namespace: ns,
				Labels: map[string]string{"tier": "immutable-test"}},
			Spec: valkeyiov1alpha1.ValkeySpec{Replicas: 1},
		})).To(Succeed())
		v := &valkeyiov1alpha1.Valkey{}
		Expect(k8sClient.Get(ctx, valkeyNN, v)).To(Succeed())
		v.Status.PrimaryPodName = "valkey-" + valkeyName + "-0-0"
		v.Status.PrimaryEndpoint = &valkeyiov1alpha1.Endpoint{
			Host: "valkey-" + valkeyName + "-0.default.svc.cluster.local",
			Port: int32(DefaultPort),
		}
		Expect(k8sClient.Status().Update(ctx, v)).To(Succeed())
		Expect(k8sClient.Create(ctx, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: valkeyName + "-sentinel-auth", Namespace: ns},
			Data: map[string][]byte{
				"username": []byte("_sentinel"),
				"password": []byte("p"),
			},
		})).To(Succeed())
	}

	AfterEach(func() {
		_ = k8sClient.Delete(ctx, &valkeyiov1alpha1.ValkeySentinel{ObjectMeta: metav1.ObjectMeta{Name: sentinelName, Namespace: ns}})
		_ = k8sClient.Delete(ctx, &valkeyiov1alpha1.Valkey{ObjectMeta: metav1.ObjectMeta{Name: valkeyName, Namespace: ns}})
		_ = k8sClient.Delete(ctx, &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: ssKey.Name, Namespace: ns}})
		_ = k8sClient.Delete(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: valkeyName + "-sentinel-auth", Namespace: ns}})
	})

	It("refuses to disable persistence on an SS created with a VCT", func() {
		seed(&valkeyiov1alpha1.SentinelPersistenceSpec{}) // explicit opt-in
		r := newReconciler()
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: sentinelNN})
		Expect(err).NotTo(HaveOccurred())

		ss := &appsv1.StatefulSet{}
		Expect(k8sClient.Get(ctx, ssKey, ss)).To(Succeed())
		Expect(ss.Spec.VolumeClaimTemplates).NotTo(BeEmpty(), "first reconcile should have created the VCT")

		// Patch the ValkeySentinel to disable persistence.
		s := &valkeyiov1alpha1.ValkeySentinel{}
		Expect(k8sClient.Get(ctx, sentinelNN, s)).To(Succeed())
		s.Spec.Persistence = &valkeyiov1alpha1.SentinelPersistenceSpec{Disabled: true}
		Expect(k8sClient.Update(ctx, s)).To(Succeed())

		// Next reconcile must NOT silently update the SS spec away.
		_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: sentinelNN})
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("cannot disable persistence"))

		// SS still has its VCT - operator refused, didn't destroy state.
		Expect(k8sClient.Get(ctx, ssKey, ss)).To(Succeed())
		Expect(ss.Spec.VolumeClaimTemplates).NotTo(BeEmpty())
	})

	It("refuses to enable persistence on an SS created with emptyDir", func() {
		seed(nil) // persistence default-off (nil)
		r := newReconciler()
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: sentinelNN})
		Expect(err).NotTo(HaveOccurred())

		ss := &appsv1.StatefulSet{}
		Expect(k8sClient.Get(ctx, ssKey, ss)).To(Succeed())
		Expect(ss.Spec.VolumeClaimTemplates).To(BeEmpty())

		// Flip persistence on.
		s := &valkeyiov1alpha1.ValkeySentinel{}
		Expect(k8sClient.Get(ctx, sentinelNN, s)).To(Succeed())
		s.Spec.Persistence = &valkeyiov1alpha1.SentinelPersistenceSpec{}
		Expect(k8sClient.Update(ctx, s)).To(Succeed())

		_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: sentinelNN})
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("cannot enable persistence"))
	})
})
