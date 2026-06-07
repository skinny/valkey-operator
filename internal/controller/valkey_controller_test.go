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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	valkeyiov1alpha1 "valkey.io/valkey-operator/api/v1alpha1"
)

var _ = Describe("Valkey controller", func() {
	ctx := context.Background()

	newReconciler := func() *ValkeyReconciler {
		return &ValkeyReconciler{
			Client:   k8sClient,
			Scheme:   k8sClient.Scheme(),
			Recorder: events.NewFakeRecorder(200),
		}
	}

	Context("replicated Valkey", func() {
		const name = "test-cache"
		nn := types.NamespacedName{Name: name, Namespace: "default"}

		BeforeEach(func() {
			existing := &valkeyiov1alpha1.Valkey{}
			if err := k8sClient.Get(ctx, nn, existing); apierrors.IsNotFound(err) {
				Expect(k8sClient.Create(ctx, &valkeyiov1alpha1.Valkey{
					ObjectMeta: metav1.ObjectMeta{
						Name: name, Namespace: "default",
						Labels: map[string]string{"tier": "caching"},
					},
					Spec: valkeyiov1alpha1.ValkeySpec{Replicas: 2},
				})).To(Succeed())
			}
		})

		AfterEach(func() {
			v := &valkeyiov1alpha1.Valkey{}
			if err := k8sClient.Get(ctx, nn, v); err == nil {
				_ = k8sClient.Delete(ctx, v)
			}
		})

		It("provisions Service, ConfigMap, ACL Secret, sentinel-auth Secret, PDB, and ValkeyNodes", func() {
			r := newReconciler()
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
			Expect(err).NotTo(HaveOccurred())

			svc := &corev1.Service{}
			Expect(k8sClient.Get(ctx, client.ObjectKey{Name: "valkey-" + name, Namespace: "default"}, svc)).To(Succeed())
			Expect(svc.Spec.ClusterIP).To(Equal(headlessClusterIP))
			Expect(svc.Spec.PublishNotReadyAddresses).To(BeTrue())
			Expect(svc.Spec.Ports[0].Port).To(Equal(int32(DefaultPort)))

			cm := &corev1.ConfigMap{}
			Expect(k8sClient.Get(ctx, client.ObjectKey{Name: GetServerConfigMapName(name), Namespace: "default"}, cm)).To(Succeed())
			Expect(cm.Data).To(HaveKey(configFileKey))
			Expect(cm.Data[configFileKey]).To(ContainSubstring("cluster-enabled no"))
			Expect(cm.Data[configFileKey]).To(ContainSubstring("aclfile /config/users/" + aclFilename))
			// Replication auth uses the default (open) user, matching
			// ValkeyCluster - so masteruser is intentionally absent.
			Expect(cm.Data[configFileKey]).NotTo(ContainSubstring("masteruser"))

			aclSecret := &corev1.Secret{}
			Expect(k8sClient.Get(ctx, client.ObjectKey{Name: getInternalSecretName(name), Namespace: "default"}, aclSecret)).To(Succeed())
			Expect(string(aclSecret.Data[aclFilename])).To(SatisfyAll(
				ContainSubstring("user "+operatorUser),
				ContainSubstring("user "+sentinelUser),
			))

			sentinelAuth := &corev1.Secret{}
			Expect(k8sClient.Get(ctx, client.ObjectKey{Name: name + "-sentinel-auth", Namespace: "default"}, sentinelAuth)).To(Succeed())
			Expect(string(sentinelAuth.Data["username"])).To(Equal(sentinelUser))
			Expect(sentinelAuth.Data["password"]).NotTo(BeEmpty())

			pdb := &policyv1.PodDisruptionBudget{}
			Expect(k8sClient.Get(ctx, client.ObjectKey{Name: "valkey-" + name, Namespace: "default"}, pdb)).To(Succeed())
			Expect(pdb.Spec.MaxUnavailable.IntValue()).To(Equal(1))

			nodes := &valkeyiov1alpha1.ValkeyNodeList{}
			Expect(k8sClient.List(ctx, nodes,
				client.InNamespace("default"),
				client.MatchingLabels{LabelValkey: name},
			)).To(Succeed())
			Expect(nodes.Items).To(HaveLen(3)) // 1 primary + 2 replicas
		})

		It("sets MultiplyMonitored=True AND Degraded=True when more than one ValkeySentinel selects it", func() {
			for _, n := range []string{"sent-a", "sent-b"} {
				s := &valkeyiov1alpha1.ValkeySentinel{
					ObjectMeta: metav1.ObjectMeta{Name: n, Namespace: "default"},
					Spec: valkeyiov1alpha1.ValkeySentinelSpec{
						Replicas: 3,
						ValkeySelector: metav1.LabelSelector{
							MatchLabels: map[string]string{"tier": "caching"},
						},
					},
				}
				Expect(k8sClient.Create(ctx, s)).To(Succeed())
				DeferCleanup(func() { _ = k8sClient.Delete(ctx, s) })
			}
			r := newReconciler()
			Eventually(func(g Gomega) {
				_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
				g.Expect(err).NotTo(HaveOccurred())
				updated := &valkeyiov1alpha1.Valkey{}
				g.Expect(k8sClient.Get(ctx, nn, updated)).To(Succeed())
				g.Expect(meta.IsStatusConditionTrue(updated.Status.Conditions,
					valkeyiov1alpha1.ConditionMultiplyMonitored)).To(BeTrue())
				// Multiple sentinels racing on the same Valkey produce
				// "undefined behaviour" per the field comment. The
				// rollout state machine must halt; Degraded is the
				// signal it gates on.
				g.Expect(meta.IsStatusConditionTrue(updated.Status.Conditions,
					valkeyiov1alpha1.ConditionDegraded)).To(BeTrue(),
					"MultipleSentinelsSelecting must flip Degraded so the rollout halts")
				g.Expect(updated.Status.MonitoredBy).To(ConsistOf("sent-a", "sent-b"))
			}).Should(Succeed())
		})

		It("populates status.monitoredBy when a ValkeySentinel selects it", func() {
			sentinel := &valkeyiov1alpha1.ValkeySentinel{
				ObjectMeta: metav1.ObjectMeta{Name: "test-monitor", Namespace: "default"},
				Spec: valkeyiov1alpha1.ValkeySentinelSpec{
					Replicas: 3,
					ValkeySelector: metav1.LabelSelector{
						MatchLabels: map[string]string{"tier": "caching"},
					},
				},
			}
			Expect(k8sClient.Create(ctx, sentinel)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, sentinel) })

			r := newReconciler()
			// Call Reconcile until all ValkeyNodes are created and status fields settle.
			// reconcileNodes creates one node per call and requeues; the final call
			// reaches the listSelectingSentinels step.
			Eventually(func(g Gomega) {
				_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
				g.Expect(err).NotTo(HaveOccurred())
				updated := &valkeyiov1alpha1.Valkey{}
				g.Expect(k8sClient.Get(ctx, nn, updated)).To(Succeed())
				g.Expect(updated.Status.MonitoredBy).To(ContainElement("test-monitor"))
			}).Should(Succeed())
		})
	})

	Context("persistence immutability", func() {
		const name = "test-persist"
		nn := types.NamespacedName{Name: name, Namespace: "default"}

		AfterEach(func() {
			v := &valkeyiov1alpha1.Valkey{}
			if err := k8sClient.Get(ctx, nn, v); err == nil {
				_ = k8sClient.Delete(ctx, v)
			}
		})

		It("permits adding persistence after creation, rejects removal, and rejects shrink", func() {
			v := &valkeyiov1alpha1.Valkey{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
				Spec:       valkeyiov1alpha1.ValkeySpec{Replicas: 0},
			}
			Expect(k8sClient.Create(ctx, v)).To(Succeed())

			// Add persistence after creation: must be allowed (rebuts the
			// originally-overstrict rule that forbade additions).
			Expect(k8sClient.Get(ctx, nn, v)).To(Succeed())
			v.Spec.Persistence = &valkeyiov1alpha1.PersistenceSpec{
				Size: resource.MustParse("1Gi"),
			}
			Expect(k8sClient.Update(ctx, v)).To(Succeed())

			// Shrink: must be rejected.
			Expect(k8sClient.Get(ctx, nn, v)).To(Succeed())
			v.Spec.Persistence.Size = resource.MustParse("500Mi")
			Expect(k8sClient.Update(ctx, v)).To(MatchError(ContainSubstring("expanded")))

			// Remove: must be rejected.
			Expect(k8sClient.Get(ctx, nn, v)).To(Succeed())
			v.Spec.Persistence = nil
			Expect(k8sClient.Update(ctx, v)).To(MatchError(ContainSubstring("removed")))
		})
	})

	Context("standalone Valkey (replicas=0)", func() {
		const name = "test-standalone"
		nn := types.NamespacedName{Name: name, Namespace: "default"}

		BeforeEach(func() {
			if err := k8sClient.Get(ctx, nn, &valkeyiov1alpha1.Valkey{}); apierrors.IsNotFound(err) {
				Expect(k8sClient.Create(ctx, &valkeyiov1alpha1.Valkey{
					ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
					Spec:       valkeyiov1alpha1.ValkeySpec{Replicas: 0},
				})).To(Succeed())
			}
		})

		AfterEach(func() {
			v := &valkeyiov1alpha1.Valkey{}
			if err := k8sClient.Get(ctx, nn, v); err == nil {
				_ = k8sClient.Delete(ctx, v)
			}
		})

		It("creates exactly one ValkeyNode and no sentinel-auth Secret", func() {
			r := newReconciler()
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
			Expect(err).NotTo(HaveOccurred())

			nodes := &valkeyiov1alpha1.ValkeyNodeList{}
			Expect(k8sClient.List(ctx, nodes,
				client.InNamespace("default"),
				client.MatchingLabels{LabelValkey: name},
			)).To(Succeed())
			Expect(nodes.Items).To(HaveLen(1))

			// sentinel-auth Secret should NOT exist for standalone (no replication to monitor).
			err = k8sClient.Get(ctx, client.ObjectKey{Name: name + "-sentinel-auth", Namespace: "default"}, &corev1.Secret{})
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		})
	})
})

// Regression for the OnDelete rollout loop.
//
// Real-cluster log signature: spec change lands, the rollout deletes
// valkey-cache-1-0, the SS controller recreates the pod stamped with
// the new controller-revision-hash, the pod becomes Ready, the
// rollout's in-flight gate passes, then observeRollout reads
// sts.Status.CurrentRevision (which under OnDelete does not advance
// after an operator-driven pod cycle) and concludes the node is still
// pending. The executor deletes the same pod again. Loop forever.
//
// This test drives the leaf decision: observeRollout against a node
// whose SS has CurrentRevision still at the old revision but whose
// pod is already stamped at UpdateRevision must NOT include that
// node in the pending list. The previous statefulSetHasPendingUpdate
// check would have returned `pending` here.
var _ = Describe("observeRollout under OnDelete", func() {
	ctx := context.Background()

	newReconciler := func() *ValkeyReconciler {
		return &ValkeyReconciler{
			Client:   k8sClient,
			Scheme:   k8sClient.Scheme(),
			Recorder: events.NewFakeRecorder(200),
		}
	}

	It("does not flag a node as pending when its pod is already at UpdateRevision (CurrentRevision lagging)", func() {
		const ns = "default"
		const valkeyName = "rollout-test"
		const nodeName = "rollout-test-1"

		// Stand up the minimal set of objects observeRollout reads:
		// a Valkey (for Status.PrimaryPodName), a ValkeyNode list,
		// a StatefulSet per node (with crafted Status.{Current,Update}Revision),
		// and a pod per node (with controller-revision-hash label).
		// We deliberately bypass the controller's Reconcile flow so
		// this test exercises observeRollout's decision logic
		// directly against the exact status shape from the live log.
		v := &valkeyiov1alpha1.Valkey{
			ObjectMeta: metav1.ObjectMeta{Name: valkeyName, Namespace: ns},
			Spec:       valkeyiov1alpha1.ValkeySpec{Replicas: 1},
		}
		Expect(k8sClient.Create(ctx, v)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, v) })

		node := &valkeyiov1alpha1.ValkeyNode{
			ObjectMeta: metav1.ObjectMeta{
				Name:      nodeName,
				Namespace: ns,
				Labels: map[string]string{
					LabelValkey:    valkeyName,
					LabelCluster:   valkeyName,
					LabelNodeIndex: "1",
				},
			},
			Spec: valkeyiov1alpha1.ValkeyNodeSpec{
				Image:               "valkey/valkey:9.0.0",
				WorkloadType:        valkeyiov1alpha1.WorkloadTypeStatefulSet,
				ServerConfigMapName: GetServerConfigMapName(valkeyName),
				UsersACLSecretName:  getInternalSecretName(valkeyName),
			},
		}
		Expect(k8sClient.Create(ctx, node)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, node) })

		sts, err := buildValkeyNodeStatefulSet(node)
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Create(ctx, sts)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, sts) })

		// Force the exact status shape that caused the loop in
		// production: UpdateRevision != CurrentRevision (a pending
		// update from the SS's point of view).
		sts.Status.CurrentRevision = "v1-old"
		sts.Status.UpdateRevision = "v2-new"
		Expect(k8sClient.Status().Update(ctx, sts)).To(Succeed())

		// Create the pod stamped at the NEW revision - mirrors the SS
		// controller recreating the pod after an operator-driven
		// delete. The pod is already at UpdateRevision; only the SS
		// Status hasn't caught up.
		podName := valkeyNodeResourcePodNameFor(node.Name)
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      podName,
				Namespace: ns,
				Labels:    map[string]string{podRevisionLabel: "v2-new"},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "server", Image: "valkey/valkey:9.0.0"}},
			},
		}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, pod) })

		r := newReconciler()
		nodes := &valkeyiov1alpha1.ValkeyNodeList{Items: []valkeyiov1alpha1.ValkeyNode{*node}}

		snap, err := r.observeRollout(ctx, v, nodes)
		Expect(err).NotTo(HaveOccurred())

		// The pod is provably at UpdateRevision (its own label says
		// so). The node must not be flagged as pending - that's the
		// loop-stopping property.
		if snap != nil {
			Expect(snap.pending).NotTo(ContainElement(node.Name),
				"node was flagged pending despite its pod already being at UpdateRevision - this is the OnDelete rollout loop signature")
		}
	})

	It("flags a node as pending when its pod's revision label lags UpdateRevision", func() {
		// Positive control: same setup as above but the pod's
		// controller-revision-hash matches CurrentRevision (the OLD
		// revision), so the pod really IS pre-cycle. observeRollout
		// must flag it.
		const ns = "default"
		const valkeyName = "rollout-test-pos"
		const nodeName = "rollout-test-pos-1"

		v := &valkeyiov1alpha1.Valkey{
			ObjectMeta: metav1.ObjectMeta{Name: valkeyName, Namespace: ns},
			Spec:       valkeyiov1alpha1.ValkeySpec{Replicas: 1},
		}
		Expect(k8sClient.Create(ctx, v)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, v) })

		node := &valkeyiov1alpha1.ValkeyNode{
			ObjectMeta: metav1.ObjectMeta{
				Name:      nodeName,
				Namespace: ns,
				Labels: map[string]string{
					LabelValkey:    valkeyName,
					LabelCluster:   valkeyName,
					LabelNodeIndex: "1",
				},
			},
			Spec: valkeyiov1alpha1.ValkeyNodeSpec{
				Image:               "valkey/valkey:9.0.0",
				WorkloadType:        valkeyiov1alpha1.WorkloadTypeStatefulSet,
				ServerConfigMapName: GetServerConfigMapName(valkeyName),
				UsersACLSecretName:  getInternalSecretName(valkeyName),
			},
		}
		Expect(k8sClient.Create(ctx, node)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, node) })

		sts, err := buildValkeyNodeStatefulSet(node)
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Create(ctx, sts)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, sts) })
		sts.Status.CurrentRevision = "v1-old"
		sts.Status.UpdateRevision = "v2-new"
		Expect(k8sClient.Status().Update(ctx, sts)).To(Succeed())

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      valkeyNodeResourcePodNameFor(node.Name),
				Namespace: ns,
				Labels:    map[string]string{podRevisionLabel: "v1-old"},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "server", Image: "valkey/valkey:9.0.0"}},
			},
		}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, pod) })

		r := newReconciler()
		nodes := &valkeyiov1alpha1.ValkeyNodeList{Items: []valkeyiov1alpha1.ValkeyNode{*node}}

		snap, err := r.observeRollout(ctx, v, nodes)
		Expect(err).NotTo(HaveOccurred())
		Expect(snap).NotTo(BeNil())
		Expect(snap.pending).To(ContainElement(node.Name),
			"node should be pending when the pod's revision label lags UpdateRevision")
	})
})

// Regression for the post-fix split-brain failure mode (and any future
// "we observed a state we can't auto-recover from"): executeRollout
// must halt issuing actions while ConditionDegraded is True. The
// existing operator-facing contract is that the user clears the
// underlying issue; the rollout picks up where it stopped on the next
// reconcile after Degraded clears. Without this guard, the rollout
// kept trying to cycle pods on a split-brain cluster and amplified
// the divergence.
var _ = Describe("executeRollout halts on Degraded", func() {
	ctx := context.Background()

	newReconciler := func() *ValkeyReconciler {
		return &ValkeyReconciler{
			Client:   k8sClient,
			Scheme:   k8sClient.Scheme(),
			Recorder: events.NewFakeRecorder(200),
		}
	}

	It("does not delete pods when Valkey.status.conditions[Degraded]=True", func() {
		const ns = "default"
		const valkeyName = "halt-on-degraded"
		const nodeName = "halt-on-degraded-1"

		v := &valkeyiov1alpha1.Valkey{
			ObjectMeta: metav1.ObjectMeta{Name: valkeyName, Namespace: ns},
			Spec:       valkeyiov1alpha1.ValkeySpec{Replicas: 1},
		}
		Expect(k8sClient.Create(ctx, v)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, v) })

		node := &valkeyiov1alpha1.ValkeyNode{
			ObjectMeta: metav1.ObjectMeta{
				Name:      nodeName,
				Namespace: ns,
				Labels: map[string]string{
					LabelValkey:    valkeyName,
					LabelCluster:   valkeyName,
					LabelNodeIndex: "1",
				},
			},
			Spec: valkeyiov1alpha1.ValkeyNodeSpec{
				Image:               "valkey/valkey:9.0.0",
				WorkloadType:        valkeyiov1alpha1.WorkloadTypeStatefulSet,
				ServerConfigMapName: GetServerConfigMapName(valkeyName),
				UsersACLSecretName:  getInternalSecretName(valkeyName),
			},
		}
		Expect(k8sClient.Create(ctx, node)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, node) })

		sts, err := buildValkeyNodeStatefulSet(node)
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Create(ctx, sts)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, sts) })
		sts.Status.CurrentRevision = "v1"
		sts.Status.UpdateRevision = "v2"
		Expect(k8sClient.Status().Update(ctx, sts)).To(Succeed())

		// Pod stamped at the OLD revision - the node IS pending, the
		// in-flight gate would normally let executeRollout delete it.
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      valkeyNodeResourcePodNameFor(node.Name),
				Namespace: ns,
				Labels:    map[string]string{podRevisionLabel: "v1"},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "server", Image: "valkey/valkey:9.0.0"}},
			},
		}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, pod) })
		pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())

		// Mark the Valkey as Degraded *before* calling executeRollout.
		v.Status.Conditions = []metav1.Condition{{
			Type:               valkeyiov1alpha1.ConditionDegraded,
			Status:             metav1.ConditionTrue,
			Reason:             "SplitBrain",
			Message:            "test fixture",
			LastTransitionTime: metav1.Now(),
		}}

		r := newReconciler()
		snap := &rolloutSnapshot{pending: []string{node.Name}}
		_, err = r.executeRollout(ctx, v, &valkeyiov1alpha1.ValkeyNodeList{Items: []valkeyiov1alpha1.ValkeyNode{*node}}, snap)
		Expect(err).NotTo(HaveOccurred())

		// Pod must still exist. Without the guard, the rollout would
		// have deleted it (it's pending, gate passes, snap.pending is
		// non-empty).
		survivor := &corev1.Pod{}
		Expect(k8sClient.Get(ctx, client.ObjectKey{Name: pod.Name, Namespace: ns}, survivor)).To(Succeed(),
			"pod must survive executeRollout when Degraded; got NotFound which means the guard didn't fire")
		Expect(survivor.DeletionTimestamp).To(BeNil(),
			"pod must not be terminating; the guard should have prevented the delete entirely")
	})
})

// Regression for finding #1 of the post-mortem oddities review:
// changes to Valkey.Spec.Config used to update the ConfigMap silently;
// the running pods kept the old in-memory config until something else
// (resource bump, image change) cycled them. ValkeyCluster already
// plumbed Spec.Config through its ServerConfigHash; Valkey didn't.
// This now matches: the ConfigMap carries the rendered hash as an
// annotation, every ValkeyNode the Valkey controller creates has
// Spec.ServerConfigHash set, and the ValkeyNode controller stamps it
// onto the pod-template annotation so a config change forces a rollout.
var _ = Describe("Valkey propagates Spec.Config changes through ServerConfigHash", func() {
	ctx := context.Background()

	newReconciler := func() *ValkeyReconciler {
		return &ValkeyReconciler{
			Client:   k8sClient,
			Scheme:   k8sClient.Scheme(),
			Recorder: events.NewFakeRecorder(200),
		}
	}

	const valkeyName = "valkey-config-hash-rgr"
	const ns = "default"
	nn := types.NamespacedName{Name: valkeyName, Namespace: ns}
	cmKey := client.ObjectKey{Name: GetServerConfigMapName(valkeyName), Namespace: ns}

	BeforeEach(func() {
		Expect(k8sClient.Create(ctx, &valkeyiov1alpha1.Valkey{
			ObjectMeta: metav1.ObjectMeta{Name: valkeyName, Namespace: ns},
			Spec: valkeyiov1alpha1.ValkeySpec{
				Replicas: 1,
				Config:   map[string]string{"maxmemory": "100mb"},
			},
		})).To(Succeed())
	})
	AfterEach(func() {
		v := &valkeyiov1alpha1.Valkey{}
		if err := k8sClient.Get(ctx, nn, v); err == nil {
			_ = k8sClient.Delete(ctx, v)
		}
		// ValkeyNodes are owner-referenced and would normally be
		// garbage-collected; envtest doesn't run GC, so clean explicitly.
		nodes := &valkeyiov1alpha1.ValkeyNodeList{}
		_ = k8sClient.List(ctx, nodes, client.InNamespace(ns), client.MatchingLabels{LabelValkey: valkeyName})
		for i := range nodes.Items {
			_ = k8sClient.Delete(ctx, &nodes.Items[i])
		}
		_ = k8sClient.Delete(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: cmKey.Name, Namespace: ns}})
	})

	It("stamps a configHash on the ConfigMap and propagates it to every ValkeyNode", func() {
		r := newReconciler()
		// Initial reconcile creates all 3 ValkeyNodes (each Create
		// returns OperationResultCreated, no requeue, so the loop
		// processes every index).
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		Expect(err).NotTo(HaveOccurred())

		cm := &corev1.ConfigMap{}
		Expect(k8sClient.Get(ctx, cmKey, cm)).To(Succeed())
		initialHash := cm.Annotations[configHashKey]
		Expect(initialHash).NotTo(BeEmpty(),
			"ConfigMap must carry the rendered config's sha256 as an annotation; without it the ValkeyNode side has nothing to stamp onto the pod template")

		// Every ValkeyNode owned by this Valkey must have the hash on
		// its Spec.ServerConfigHash. That's how the ValkeyNode
		// controller learns the hash to put on the pod template.
		nodes := &valkeyiov1alpha1.ValkeyNodeList{}
		Expect(k8sClient.List(ctx, nodes, client.InNamespace(ns), client.MatchingLabels{LabelValkey: valkeyName})).To(Succeed())
		Expect(nodes.Items).To(HaveLen(2), "1 primary + 1 replica = 2 ValkeyNodes for Spec.Replicas=1")
		for _, n := range nodes.Items {
			Expect(n.Spec.ServerConfigHash).To(Equal(initialHash),
				"ValkeyNode %s must have Spec.ServerConfigHash set on creation; otherwise Spec.Config changes never bump the pod-template revision", n.Name)
		}

		// envtest doesn't run pods, so ValkeyNode.Status.Ready stays
		// false; reconcileNodes' "Unchanged + NotReady -> requeue"
		// branch would otherwise wedge the loop on index 0. Manually
		// mark each as Ready so subsequent reconciles can iterate
		// past index 0 and update the rest.
		for i := range nodes.Items {
			n := &nodes.Items[i]
			n.Status.Ready = true
			n.Status.PodName = "valkey-" + n.Name + "-0"
			n.Status.PodIP = "10.0.0." + strconv.Itoa(i+1)
			Expect(k8sClient.Status().Update(ctx, n)).To(Succeed())
		}

		// Patch Spec.Config and re-reconcile until every ValkeyNode
		// converges. reconcileNodes returns (requeue, nil) on each
		// Update, so multiple passes are required to walk all indexes.
		v := &valkeyiov1alpha1.Valkey{}
		Expect(k8sClient.Get(ctx, nn, v)).To(Succeed())
		v.Spec.Config["maxmemory"] = "200mb"
		Expect(k8sClient.Update(ctx, v)).To(Succeed())

		var updatedHash string
		Eventually(func(g Gomega) {
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(k8sClient.Get(ctx, cmKey, cm)).To(Succeed())
			updatedHash = cm.Annotations[configHashKey]
			g.Expect(updatedHash).NotTo(Equal(initialHash),
				"ConfigMap configHash annotation must change when Spec.Config changes")
			g.Expect(k8sClient.List(ctx, nodes, client.InNamespace(ns), client.MatchingLabels{LabelValkey: valkeyName})).To(Succeed())
			for _, n := range nodes.Items {
				g.Expect(n.Spec.ServerConfigHash).To(Equal(updatedHash),
					"ValkeyNode %s must have its Spec.ServerConfigHash updated after Spec.Config changes; without this the pod-template hash doesn't move and the rollout state machine never cycles the pods", n.Name)
			}
		}).Should(Succeed())
	})
})

// Regression for the "no sentinel pods are ever created" wedge.
// Sequence the live cluster log showed:
//   1. Bootstrap promotes node-0 (no-op - Valkey defaults to master).
//   2. ConditionBootstrapped=True, but Status.PrimaryPodName is not
//      set.
//   3. observePrimary sees 3 nodes reporting role:master (each one
//      boots master by default), pickStablePrimary has no
//      previousPrimary to bias toward, returns ambiguous.
//   4. Status.PrimaryEndpoint stays nil.
//   5. Sentinel controller's collectMonitorConfigs skips the Valkey
//      ("no observed primary"); sentinel SS never gets created.
//   6. wireScaledUpReplicas requires Status.PrimaryPodName != "" so
//      it never wires the impostor replicas either. Wedged forever.
//
// The fix: bootstrap stamps Status.PrimaryPodName /
// Status.PrimaryEndpoint inline so the same-reconcile observePrimary
// has a previous-primary to stabilise on, AND the sentinel controller
// has an endpoint to render its template against. This test pins
// that behaviour.
var _ = Describe("bootstrap pins Status.PrimaryPodName so the rest of the pipeline can converge", func() {
	ctx := context.Background()

	newReconciler := func() *ValkeyReconciler {
		return &ValkeyReconciler{
			Client:   k8sClient,
			Scheme:   k8sClient.Scheme(),
			Recorder: events.NewFakeRecorder(200),
		}
	}

	const valkeyName = "bootstrap-pins-primary"
	const ns = "default"
	nn := types.NamespacedName{Name: valkeyName, Namespace: ns}

	BeforeEach(func() {
		Expect(k8sClient.Create(ctx, &valkeyiov1alpha1.Valkey{
			ObjectMeta: metav1.ObjectMeta{Name: valkeyName, Namespace: ns},
			Spec:       valkeyiov1alpha1.ValkeySpec{Replicas: 2},
		})).To(Succeed())
	})
	AfterEach(func() {
		v := &valkeyiov1alpha1.Valkey{}
		if err := k8sClient.Get(ctx, nn, v); err == nil {
			_ = k8sClient.Delete(ctx, v)
		}
		nodes := &valkeyiov1alpha1.ValkeyNodeList{}
		_ = k8sClient.List(ctx, nodes, client.InNamespace(ns), client.MatchingLabels{LabelValkey: valkeyName})
		for i := range nodes.Items {
			_ = k8sClient.Delete(ctx, &nodes.Items[i])
		}
	})

	It("sets Status.PrimaryPodName and Status.PrimaryEndpoint as part of the bootstrap call itself", func() {
		// envtest can't run a real Valkey, so bootstrapReplication's
		// INFO probe + REPLICAOF would fail against the (nonexistent)
		// pod. Drive bootstrap by setting up the ValkeyNode state it
		// reads (just the index-0 node Ready with a PodIP) and
		// calling bootstrapReplication directly. The unit-level
		// assertion: it writes Status before returning.

		// First reconcile creates the ValkeyNodes.
		r := newReconciler()
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		Expect(err).NotTo(HaveOccurred())

		// Mark node-0 as Ready with a fake IP so bootstrapReplication
		// passes the wait-for-primary gate.
		node0 := &valkeyiov1alpha1.ValkeyNode{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: valkeyName + "-0", Namespace: ns}, node0)).To(Succeed())
		node0.Status.Ready = true
		node0.Status.PodName = "valkey-" + valkeyName + "-0-0"
		node0.Status.PodIP = "10.244.0.1"
		Expect(k8sClient.Status().Update(ctx, node0)).To(Succeed())

		// Call the function directly: we want to assert what bootstrap
		// writes to valkey.Status before the rest of Reconcile runs.
		v := &valkeyiov1alpha1.Valkey{}
		Expect(k8sClient.Get(ctx, nn, v)).To(Succeed())
		nodes := &valkeyiov1alpha1.ValkeyNodeList{}
		Expect(k8sClient.List(ctx, nodes, client.InNamespace(ns), client.MatchingLabels{LabelValkey: valkeyName})).To(Succeed())

		// bootstrap will try to talk to the (nonexistent) Valkey pod
		// for INFO + REPLICAOF; both fail. We don't care - the
		// invariant under test is that Status is updated BEFORE the
		// REPLICAOF, so even if the wire call later errors out, the
		// next reconcile's observePrimary has a previousPrimary to
		// stabilise on.
		_ = r.bootstrapReplication(ctx, v, nodes)
		// (Tolerating the inevitable wire-side error.)

		// The assertion the live-cluster bug was missing:
		Expect(v.Status.PrimaryPodName).To(Equal("valkey-"+valkeyName+"-0-0"),
			"bootstrap must stamp Status.PrimaryPodName so the same-reconcile observePrimary has a previousPrimary to stabilise on; without it, all three pods reporting role:master at boot makes observePrimary return ambiguous and the whole pipeline (wireScaledUpReplicas + sentinel SS creation) wedges")
		Expect(v.Status.PrimaryEndpoint).NotTo(BeNil(),
			"bootstrap must stamp Status.PrimaryEndpoint so the sentinel controller's collectMonitorConfigs has a host to render `sentinel monitor` against; without it, sentinel SS creation defers forever")
		Expect(v.Status.PrimaryEndpoint.Host).To(Equal("valkey-" + valkeyName + "-0.default.svc.cluster.local"))
		Expect(v.Status.PrimaryEndpoint.Port).To(Equal(int32(DefaultPort)))
	})
})

// The operator owns several valkey.conf keys (port, dir, bind, etc.).
// Letting users set them via spec.config would either fight the
// rendered config or break the data path entirely (e.g. port 0 makes
// the pod fail to listen). A CEL blocklist on the field surfaces the
// misuse at admission time rather than as a confusing CrashLoopBackOff.
var _ = Describe("Valkey spec.config rejects operator-owned keys", func() {
	const name = "valkey-blocked-keys"
	nn := types.NamespacedName{Name: name, Namespace: "default"}

	AfterEach(func() {
		v := &valkeyiov1alpha1.Valkey{}
		if err := k8sClient.Get(ctx, nn, v); err == nil {
			_ = k8sClient.Delete(ctx, v)
		}
	})

	for _, key := range []string{"port", "dir", "bind", "logfile", "dbfilename", "pidfile", "unixsocket", "aclfile", "include", "tls-port"} {
		key := key
		It("rejects "+key, func() {
			err := k8sClient.Create(ctx, &valkeyiov1alpha1.Valkey{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
				Spec: valkeyiov1alpha1.ValkeySpec{
					Replicas: 0,
					Config:   map[string]string{key: "anything"},
				},
			})
			Expect(err).To(HaveOccurred(), "user-supplied %s must be rejected by CEL validation", key)
			Expect(err.Error()).To(ContainSubstring("owned by the operator"))
		})
	}

	It("accepts user-tunable keys (maxmemory, maxmemory-policy)", func() {
		Expect(k8sClient.Create(ctx, &valkeyiov1alpha1.Valkey{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec: valkeyiov1alpha1.ValkeySpec{
				Replicas: 0,
				Config: map[string]string{
					"maxmemory":        "256mb",
					"maxmemory-policy": "allkeys-lfu",
				},
			},
		})).To(Succeed())
	})
})
