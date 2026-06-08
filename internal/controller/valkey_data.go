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
	"crypto/sha256"
	"fmt"
	"maps"
	"sort"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
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
// Shared with the ValkeySentinel controller; see reconcilePDB below.
func (r *ValkeyReconciler) reconcilePDB(ctx context.Context, valkey *valkeyiov1alpha1.Valkey) error {
	return reconcilePDB(ctx, r.Client, r.Scheme, valkey, valkeyResourceName(valkey),
		valkeyLabels(valkey), valkeySelectorLabels(valkey), valkey.Spec.PodDisruptionBudget)
}

// reconcilePDB creates/updates a PodDisruptionBudget (maxUnavailable=1)
// for owner, or deletes the existing one when policy is Disabled. Used by
// both the Valkey and ValkeySentinel controllers - same shape, two CRDs.
func reconcilePDB(
	ctx context.Context,
	c client.Client,
	scheme *runtime.Scheme,
	owner client.Object,
	name string,
	objectLabels, selectorLabels map[string]string,
	policy valkeyiov1alpha1.PDBPolicy,
) error {
	if policy == valkeyiov1alpha1.PDBPolicyDisabled {
		pdb := &policyv1.PodDisruptionBudget{}
		err := c.Get(ctx, client.ObjectKey{Name: name, Namespace: owner.GetNamespace()}, pdb)
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err == nil {
			_ = c.Delete(ctx, pdb)
		}
		return nil
	}
	maxUnavailable := intstr.FromInt32(1)
	pdb := &policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{
		Name: name, Namespace: owner.GetNamespace(),
	}}
	_, err := controllerutil.CreateOrUpdate(ctx, c, pdb, func() error {
		pdb.Labels = objectLabels
		pdb.Spec.MaxUnavailable = &maxUnavailable
		pdb.Spec.MinAvailable = nil
		pdb.Spec.Selector = &metav1.LabelSelector{MatchLabels: selectorLabels}
		return controllerutil.SetControllerReference(owner, pdb, scheme)
	})
	return err
}

// upsertConfigMap creates/updates the per-Valkey ConfigMap holding
// `valkey.conf` and the embedded liveness/readiness scripts. Spec.config
// entries are written verbatim into valkey.conf without validation.
//
// Returns the sha256 of the rendered `valkey.conf` content. The caller
// threads this through to each ValkeyNode's Spec.ServerConfigHash so
// the ValkeyNode controller stamps it as a pod-template annotation;
// any change to Valkey.Spec.Config then bumps the pod-template hash
// and the rollout state machine cycles the pods to pick up the new
// config. Without this plumbing - the pre-fix state - changes to
// Spec.Config (maxmemory, appendonly, etc.) updated the ConfigMap
// silently and running pods kept the old in-memory config until
// something unrelated cycled them. Matches the equivalent path on
// ValkeyCluster (see valkeycluster_controller.go upsertConfigMap).
func (r *ValkeyReconciler) upsertConfigMap(ctx context.Context, valkey *valkeyiov1alpha1.Valkey) (string, error) {
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Name: GetServerConfigMapName(valkey.Name), Namespace: valkey.Namespace,
	}}
	rendered := renderValkeyConfig(valkey)
	hash := fmt.Sprintf("%x", sha256.Sum256([]byte(rendered)))
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
		cm.Labels = valkeyLabels(valkey)

		// Health-check scripts shared with ValkeyCluster.
		readiness, _ := scripts.ReadFile("scripts/readiness-check.sh")
		liveness, _ := scripts.ReadFile("scripts/liveness-check.sh")

		cm.Data = map[string]string{
			configFileKey:      rendered,
			readinessScriptKey: string(readiness),
			livenessScriptKey:  string(liveness),
		}
		if cm.Annotations == nil {
			cm.Annotations = map[string]string{}
		}
		cm.Annotations[configHashKey] = hash
		return controllerutil.SetControllerReference(valkey, cm, r.Scheme)
	})
	if err != nil {
		return "", err
	}
	return hash, nil
}

// renderValkeyConfig builds the `valkey.conf` contents. Sorted to keep
// the output deterministic.
func renderValkeyConfig(valkey *valkeyiov1alpha1.Valkey) string {
	cfg := map[string]string{
		"port":            strconv.Itoa(DefaultPort),
		"dir":             "/data",
		"cluster-enabled": "no",
		"protected-mode":  "no",
		"appendonly":      "yes",
		"aclfile":         "/config/users/" + aclFilename,
		// Deliberately no `masteruser` / `masterauth` here. Replicas
		// authenticate to the master as the default (open) user,
		// matching how ValkeyCluster handles inter-node auth. Using
		// a named system user would require either persisting
		// masterauth into the ConfigMap (leaking the password into a
		// non-Secret resource) or running a startup script that
		// rewrites the config from a mounted Secret on every pod
		// boot. Both are deferred until we tighten the wider auth
		// model.
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
