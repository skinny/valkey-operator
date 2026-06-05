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
	"encoding/hex"
	"fmt"
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

	sentinelAuthVolumeName = "sentinel-auth"
	sentinelAuthMountPath  = "/sentinel-auth"

	// sentinelConfigHashAnnotation is set on the StatefulSet pod
	// template so a change to the rendered sentinel.conf template
	// triggers a rolling restart of sentinel pods (one at a time,
	// PDB-respecting). Without this the ConfigMap update would not
	// reach running pods until they happen to restart.
	sentinelConfigHashAnnotation = "valkey.io/sentinel-config-hash"
)

// sentinelAuthSecretName is the aggregated Secret holding one password
// per matched Valkey, keyed by Valkey name. Mounted into every sentinel
// pod and substituted into sentinel.conf by the startup script.
func sentinelAuthSecretName(s *valkeyiov1alpha1.ValkeySentinel) string {
	return sentinelResourceName(s) + "-auth"
}

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

// monitoredValkey bundles everything the sentinel template needs about
// one Valkey: the master endpoint observed by the Valkey controller,
// the quorum to use, and the SENTINEL SET tuning to bake in.
type monitoredValkey struct {
	Name     string
	IP       string
	Port     int32
	Quorum   int32
	Username string
	Config   map[string]string
}

// upsertSentinelConfigMap renders sentinel.conf with `sentinel monitor`
// + tuning + auth-user baked in for every matched Valkey whose primary
// endpoint the Valkey controller has observed. The auth password is
// left as a `__SENTINEL_AUTH_PASS_<name>__` placeholder; the startup
// script substitutes it from the per-Valkey password file mounted from
// the aggregated sentinel-auth Secret.
//
// Baking the monitor + tuning into the template is what the hand-rolled
// sentinel YAML does, and it avoids the burst of SENTINEL SET commands
// that previously hit the timer during startup TILT.
func (r *ValkeySentinelReconciler) upsertSentinelConfigMap(ctx context.Context, s *valkeyiov1alpha1.ValkeySentinel, monitors []monitoredValkey) (string, error) {
	startup, err := scripts.ReadFile("scripts/" + sentinelStartupScriptKey)
	if err != nil {
		return "", err
	}
	template := renderSentinelTemplate(monitors)
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
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256([]byte(template))
	return hex.EncodeToString(sum[:]), nil
}

// renderSentinelTemplate produces the sentinel.conf the startup script
// will materialize. Per-Valkey monitor + tuning blocks are deterministic
// (sorted by name) so the hash used to trigger pod rolls is stable.
func renderSentinelTemplate(monitors []monitoredValkey) string {
	base := []string{
		"port " + strconv.Itoa(valkeyiov1alpha1.SentinelPort),
		"dir " + sentinelDataMountPath,
		"sentinel resolve-hostnames yes",
		"sentinel announce-ip __POD_IP__",
		"sentinel announce-port " + strconv.Itoa(valkeyiov1alpha1.SentinelPort),
	}
	var b strings.Builder
	for _, line := range base {
		b.WriteString(line)
		b.WriteByte('\n')
	}

	sortedMonitors := append([]monitoredValkey(nil), monitors...)
	sort.Slice(sortedMonitors, func(i, j int) bool { return sortedMonitors[i].Name < sortedMonitors[j].Name })

	for _, m := range sortedMonitors {
		b.WriteByte('\n')
		fmt.Fprintf(&b, "sentinel monitor %s %s %d %d\n", m.Name, m.IP, m.Port, m.Quorum)
		fmt.Fprintf(&b, "sentinel auth-user %s %s\n", m.Name, m.Username)
		fmt.Fprintf(&b, "sentinel auth-pass %s __SENTINEL_AUTH_PASS_%s__\n", m.Name, m.Name)
		keys := make([]string, 0, len(m.Config))
		for k := range m.Config {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(&b, "sentinel %s %s %s\n", k, m.Name, m.Config[k])
		}
	}
	return b.String()
}

// upsertSentinelAuthSecret maintains one aggregated Secret per
// ValkeySentinel that holds, keyed by Valkey name, the per-Valkey
// sentinel password. Mounted at /sentinel-auth/ into every sentinel
// pod so the startup script can substitute __SENTINEL_AUTH_PASS_<name>__
// placeholders without ever exposing the password to the ConfigMap or
// to pod env.
func (r *ValkeySentinelReconciler) upsertSentinelAuthSecret(ctx context.Context, s *valkeyiov1alpha1.ValkeySentinel, passwords map[string]string) error {
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Name: sentinelAuthSecretName(s), Namespace: s.Namespace,
	}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, secret, func() error {
		secret.Labels = sentinelLabels(s)
		secret.Type = corev1.SecretTypeOpaque
		secret.Data = map[string][]byte{}
		for name, pw := range passwords {
			secret.Data[name] = []byte(pw)
		}
		return controllerutil.SetControllerReference(s, secret, r.Scheme)
	})
	return err
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
// sentinel pods. The configHash annotation triggers a rolling restart
// (one pod at a time, respecting the PDB) when the rendered template
// changes.
func (r *ValkeySentinelReconciler) upsertSentinelStatefulSet(ctx context.Context, s *valkeyiov1alpha1.ValkeySentinel, configHash string) (*appsv1.StatefulSet, error) {
	defaultMode := int32(0o755)
	cmName := sentinelResourceName(s)
	authName := sentinelAuthSecretName(s)

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
				ObjectMeta: metav1.ObjectMeta{
					Labels:      sentinelLabels(s),
					Annotations: map[string]string{sentinelConfigHashAnnotation: configHash},
				},
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
							{Name: sentinelAuthVolumeName, MountPath: sentinelAuthMountPath, ReadOnly: true},
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
						{Name: sentinelAuthVolumeName, VolumeSource: corev1.VolumeSource{
							Secret: &corev1.SecretVolumeSource{SecretName: authName},
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
							// script re-renders the full config from the template
							// (master IP, auth, tuning) and gossip repopulates
							// known peers.
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
