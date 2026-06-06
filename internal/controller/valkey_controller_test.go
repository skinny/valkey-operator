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

		It("sets MultiplyMonitored=True when more than one ValkeySentinel selects it", func() {
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
