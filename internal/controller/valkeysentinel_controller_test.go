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
	nn := types.NamespacedName{Name: name, Namespace: "default"}

	BeforeEach(func() {
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
		Expect(cm.Data[sentinelConfigTemplateKey]).NotTo(ContainSubstring("sentinel monitor "))

		pdb := &policyv1.PodDisruptionBudget{}
		Expect(k8sClient.Get(ctx, client.ObjectKey{Name: "valkey-sentinel-" + name, Namespace: "default"}, pdb)).To(Succeed())
		Expect(pdb.Spec.MaxUnavailable.IntValue()).To(Equal(1))

		ss := &appsv1.StatefulSet{}
		Expect(k8sClient.Get(ctx, client.ObjectKey{Name: "valkey-sentinel-" + name, Namespace: "default"}, ss)).To(Succeed())
		Expect(*ss.Spec.Replicas).To(Equal(int32(3)))
		Expect(ss.Spec.Template.Spec.Containers).To(HaveLen(1))
		Expect(ss.Spec.Template.Spec.Containers[0].Ports[0].ContainerPort).To(Equal(int32(valkeyiov1alpha1.SentinelPort)))
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
