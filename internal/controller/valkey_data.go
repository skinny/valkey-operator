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
	"maps"
	"sort"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	valkeyiov1alpha1 "valkey.io/valkey-operator/api/v1alpha1"
)

// upsertDataService creates/updates the headless data Service (port 6379).
func (r *ValkeyReconciler) upsertDataService(ctx context.Context, valkey *valkeyiov1alpha1.Valkey) error {
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{
		Name: valkeyResourceName(valkey), Namespace: valkey.Namespace,
	}}
	result, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		svc.Labels = valkeyLabels(valkey)
		svc.Spec.Type = corev1.ServiceTypeClusterIP
		if svc.Spec.ClusterIP == "" {
			svc.Spec.ClusterIP = headlessClusterIP
		}
		svc.Spec.PublishNotReadyAddresses = true
		svc.Spec.Selector = valkeySelectorLabels(valkey)
		svc.Spec.Ports = []corev1.ServicePort{{Name: "valkey", Port: DefaultPort}}
		return controllerutil.SetControllerReference(valkey, svc, r.Scheme)
	})
	if err != nil {
		return err
	}
	if result == controllerutil.OperationResultCreated {
		r.Recorder.Eventf(valkey, svc, corev1.EventTypeNormal, "ServiceCreated", "Upsert", "Created data Service %s", svc.Name)
	}
	return nil
}

// reconcilePDB creates a PDB with maxUnavailable=1 unless disabled.
func (r *ValkeyReconciler) reconcilePDB(ctx context.Context, valkey *valkeyiov1alpha1.Valkey) error {
	if valkey.Spec.PodDisruptionBudget == valkeyiov1alpha1.PDBPolicyDisabled {
		pdb := &policyv1.PodDisruptionBudget{}
		err := r.Get(ctx, client.ObjectKey{Name: valkeyResourceName(valkey), Namespace: valkey.Namespace}, pdb)
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err == nil {
			_ = r.Delete(ctx, pdb)
		}
		return nil
	}
	maxUnavailable := intstr.FromInt32(1)
	pdb := &policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{
		Name: valkeyResourceName(valkey), Namespace: valkey.Namespace,
	}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, pdb, func() error {
		pdb.Labels = valkeyLabels(valkey)
		pdb.Spec.MaxUnavailable = &maxUnavailable
		pdb.Spec.MinAvailable = nil
		pdb.Spec.Selector = &metav1.LabelSelector{MatchLabels: valkeySelectorLabels(valkey)}
		return controllerutil.SetControllerReference(valkey, pdb, r.Scheme)
	})
	return err
}

// upsertConfigMap creates/updates the per-Valkey ConfigMap holding
// `valkey.conf` and the embedded liveness/readiness scripts. Spec.config
// entries are written verbatim into valkey.conf without validation.
func (r *ValkeyReconciler) upsertConfigMap(ctx context.Context, valkey *valkeyiov1alpha1.Valkey) error {
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Name: GetServerConfigMapName(valkey.Name), Namespace: valkey.Namespace,
	}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
		cm.Labels = valkeyLabels(valkey)

		// Health-check scripts shared with ValkeyCluster.
		readiness, _ := scripts.ReadFile("scripts/readiness-check.sh")
		liveness, _ := scripts.ReadFile("scripts/liveness-check.sh")

		cm.Data = map[string]string{
			configFileKey:      renderValkeyConfig(valkey),
			readinessScriptKey: string(readiness),
			livenessScriptKey:  string(liveness),
		}
		return controllerutil.SetControllerReference(valkey, cm, r.Scheme)
	})
	return err
}

// renderValkeyConfig builds the `valkey.conf` contents. Sorted to keep
// the output deterministic.
func renderValkeyConfig(valkey *valkeyiov1alpha1.Valkey) string {
	cfg := map[string]string{
		"port":            strconv.Itoa(DefaultPort),
		"dir":             "/data",
		"cluster-enabled": "no",
		"masteruser":      operatorUser,
		"protected-mode":  "no",
		"appendonly":      "yes",
		"aclfile":         "/users/" + aclFilename,
	}
	maps.Copy(cfg, valkey.Spec.Config)
	keys := make([]string, 0, len(cfg))
	for k := range cfg {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		b.WriteString(k)
		b.WriteByte(' ')
		b.WriteString(cfg[k])
		b.WriteByte('\n')
	}
	return b.String()
}
