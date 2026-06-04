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
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	valkeyiov1alpha1 "valkey.io/valkey-operator/api/v1alpha1"
)

const (
	// LabelSentinel identifies a ValkeySentinel set on every pod and
	// owned resource.
	LabelSentinel = "valkey.io/sentinel"

	sentinelResourcePrefix = "valkey-sentinel-"

	sentinelStartupScriptKey  = "sentinel-startup.sh"
	sentinelConfigTemplateKey = "sentinel.conf.template"

	sentinelDataVolumeName = "sentinel-data"
	sentinelDataMountPath  = "/var/lib/sentinel"
)

func sentinelResourceName(s *valkeyiov1alpha1.ValkeySentinel) string {
	return sentinelResourcePrefix + s.Name
}

func sentinelLabels(s *valkeyiov1alpha1.ValkeySentinel) map[string]string {
	l := baseLabels(s.Name, "valkey-sentinel")
	l[LabelSentinel] = s.Name
	for k, v := range s.Labels {
		if _, exists := l[k]; !exists {
			l[k] = v
		}
	}
	return l
}

func sentinelSelectorLabels(s *valkeyiov1alpha1.ValkeySentinel) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name": appName,
		LabelSentinel:            s.Name,
	}
}

func sentinelImageFor(s *valkeyiov1alpha1.ValkeySentinel) string {
	if s.Spec.Image != "" {
		return s.Spec.Image
	}
	return DefaultImage
}

// upsertSentinelService creates/updates the headless sentinel Service.
func (r *ValkeySentinelReconciler) upsertSentinelService(ctx context.Context, s *valkeyiov1alpha1.ValkeySentinel) error {
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{
		Name: sentinelResourceName(s), Namespace: s.Namespace,
	}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		svc.Labels = sentinelLabels(s)
		svc.Spec.Type = corev1.ServiceTypeClusterIP
		if svc.Spec.ClusterIP == "" {
			svc.Spec.ClusterIP = headlessClusterIP
		}
		svc.Spec.PublishNotReadyAddresses = true
		svc.Spec.Selector = sentinelSelectorLabels(s)
		svc.Spec.Ports = []corev1.ServicePort{{Name: "sentinel", Port: valkeyiov1alpha1.SentinelPort}}
		return controllerutil.SetControllerReference(s, svc, r.Scheme)
	})
	return err
}

// upsertSentinelConfigMap renders the runtime sentinel.conf template
// (no `sentinel monitor` directives - added at runtime by the
// controller) plus the startup script.
func (r *ValkeySentinelReconciler) upsertSentinelConfigMap(ctx context.Context, s *valkeyiov1alpha1.ValkeySentinel) error {
	startup, err := scripts.ReadFile("scripts/" + sentinelStartupScriptKey)
	if err != nil {
		return err
	}
	template := renderSentinelTemplate()
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Name: sentinelResourceName(s), Namespace: s.Namespace,
	}}
	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
		cm.Labels = sentinelLabels(s)
		cm.Data = map[string]string{
			sentinelStartupScriptKey:  string(startup),
			sentinelConfigTemplateKey: template,
		}
		return controllerutil.SetControllerReference(s, cm, r.Scheme)
	})
	return err
}

// renderSentinelTemplate builds the template without any `sentinel
// monitor` lines. Those are added at runtime via SENTINEL MONITOR.
func renderSentinelTemplate() string {
	cfg := map[string]string{
		"port":                       strconv.Itoa(valkeyiov1alpha1.SentinelPort),
		"dir":                        sentinelDataMountPath,
		"sentinel resolve-hostnames": "yes",
		"sentinel announce-ip":       "__POD_IP__",
		"sentinel announce-port":     strconv.Itoa(valkeyiov1alpha1.SentinelPort),
	}
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

// upsertSentinelPDB reconciles a PDB with maxUnavailable=1 unless disabled.
func (r *ValkeySentinelReconciler) upsertSentinelPDB(ctx context.Context, s *valkeyiov1alpha1.ValkeySentinel) error {
	if s.Spec.PodDisruptionBudget == valkeyiov1alpha1.PDBPolicyDisabled {
		pdb := &policyv1.PodDisruptionBudget{}
		err := r.Get(ctx, client.ObjectKey{Name: sentinelResourceName(s), Namespace: s.Namespace}, pdb)
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
		Name: sentinelResourceName(s), Namespace: s.Namespace,
	}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, pdb, func() error {
		pdb.Labels = sentinelLabels(s)
		pdb.Spec.MaxUnavailable = &maxUnavailable
		pdb.Spec.MinAvailable = nil
		pdb.Spec.Selector = &metav1.LabelSelector{MatchLabels: sentinelSelectorLabels(s)}
		return controllerutil.SetControllerReference(s, pdb, r.Scheme)
	})
	return err
}

// upsertSentinelStatefulSet reconciles the StatefulSet that runs the
// sentinel pods.
func (r *ValkeySentinelReconciler) upsertSentinelStatefulSet(ctx context.Context, s *valkeyiov1alpha1.ValkeySentinel) (*appsv1.StatefulSet, error) {
	defaultMode := int32(0o755)
	cmName := sentinelResourceName(s)

	desired := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: sentinelResourceName(s), Namespace: s.Namespace,
			Labels: sentinelLabels(s),
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:    &s.Spec.Replicas,
			ServiceName: sentinelResourceName(s),
			Selector:    &metav1.LabelSelector{MatchLabels: sentinelSelectorLabels(s)},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: sentinelLabels(s)},
				Spec: corev1.PodSpec{
					NodeSelector: s.Spec.NodeSelector,
					Affinity:     s.Spec.Affinity,
					Tolerations:  s.Spec.Tolerations,
					Containers: []corev1.Container{{
						Name:      "sentinel",
						Image:     sentinelImageFor(s),
						Resources: s.Spec.Resources,
						Command:   []string{"/bin/bash", "-c", "/scripts/" + sentinelStartupScriptKey},
						Env: []corev1.EnvVar{{
							Name: "POD_IP",
							ValueFrom: &corev1.EnvVarSource{
								FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.podIP"},
							},
						}},
						Ports: []corev1.ContainerPort{{
							Name: "sentinel", ContainerPort: valkeyiov1alpha1.SentinelPort,
						}},
						ReadinessProbe: &corev1.Probe{
							InitialDelaySeconds: 5, PeriodSeconds: 5,
							ProbeHandler: corev1.ProbeHandler{
								TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(valkeyiov1alpha1.SentinelPort)},
							},
						},
						LivenessProbe: &corev1.Probe{
							InitialDelaySeconds: 30, PeriodSeconds: 10, FailureThreshold: 6,
							ProbeHandler: corev1.ProbeHandler{
								TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(valkeyiov1alpha1.SentinelPort)},
							},
						},
						VolumeMounts: []corev1.VolumeMount{
							{Name: "scripts", MountPath: "/scripts"},
							{Name: "sentinel-conf", MountPath: "/config", ReadOnly: true},
							{Name: sentinelDataVolumeName, MountPath: sentinelDataMountPath},
						},
					}},
					Volumes: []corev1.Volume{
						{Name: "scripts", VolumeSource: corev1.VolumeSource{
							ConfigMap: &corev1.ConfigMapVolumeSource{
								LocalObjectReference: corev1.LocalObjectReference{Name: cmName},
								DefaultMode:          &defaultMode,
							},
						}},
						{Name: "sentinel-conf", VolumeSource: corev1.VolumeSource{
							ConfigMap: &corev1.ConfigMapVolumeSource{
								LocalObjectReference: corev1.LocalObjectReference{Name: cmName},
							},
						}},
						{Name: sentinelDataVolumeName, VolumeSource: corev1.VolumeSource{
							// Memory-backed (tmpfs). sentinel.conf is rewritten +
							// fsynced on every gossip update, vote, and state
							// transition during a failover - on a kind cluster
							// (Docker Desktop VM) each fsync can take hundreds of
							// ms, easily stalling the sentinel timer past
							// SENTINEL_TILT_TRIGGER (2s) and wedging promotion.
							// Backing the volume with tmpfs makes fsync a no-op.
							// State loss on pod restart is fine: the startup
							// script re-renders the base config from the
							// ConfigMap template, the operator re-issues
							// MONITOR+SET on the next reconcile, and gossip
							// repopulates peers.
							EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory},
						}},
					},
				},
			},
		},
	}

	existing := &appsv1.StatefulSet{}
	err := r.Get(ctx, types.NamespacedName{Name: desired.Name, Namespace: desired.Namespace}, existing)
	if apierrors.IsNotFound(err) {
		if err := controllerutil.SetControllerReference(s, desired, r.Scheme); err != nil {
			return nil, err
		}
		if err := r.Create(ctx, desired); err != nil {
			return nil, err
		}
		return desired, nil
	}
	if err != nil {
		return nil, err
	}
	existing.Labels = desired.Labels
	existing.Spec.Replicas = desired.Spec.Replicas
	existing.Spec.Template = desired.Spec.Template
	if err := controllerutil.SetControllerReference(s, existing, r.Scheme); err != nil {
		return nil, err
	}
	if err := r.Update(ctx, existing); err != nil {
		return nil, err
	}
	return existing, nil
}
