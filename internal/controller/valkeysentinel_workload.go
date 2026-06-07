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
	"reflect"
	"sort"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
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
//
// Host is a DNS name (per-pod Service) so a pod restart, which gives
// the pod a new IP, doesn't leave stale `known-replica` entries in
// sentinel.conf - sentinel re-resolves the host on disconnect.
type monitoredValkey struct {
	Name     string
	Host     string
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
		fmt.Fprintf(&b, "sentinel monitor %s %s %d %d\n", m.Name, m.Host, m.Port, m.Quorum)
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
	return reconcilePDB(ctx, r.Client, r.Scheme, s, sentinelResourceName(s),
		sentinelLabels(s), sentinelSelectorLabels(s), s.Spec.PodDisruptionBudget)
}

// upsertSentinelStatefulSet reconciles the StatefulSet that runs the
// sentinel pods. The configHash annotation triggers a rolling restart
// (one pod at a time, respecting the PDB) when the rendered template
// changes.
//
// Initial-deploy deferral: when there are no monitors yet (the
// selected Valkeys haven't reported Status.PrimaryEndpoint yet, or
// the selector matches nothing) AND the StatefulSet doesn't already
// exist, this returns (nil, nil) and the caller defers SS creation
// until the next reconcile. Without this, the SS is created with a
// configHash computed from the empty-monitor template; the moment a
// Valkey bootstraps and the next reconcile fires, the configHash
// changes and RollingUpdate rolls every sentinel pod. That "one
// restart per fresh deploy" was confusing and pointless.
//
// If the SS already exists when monitors drops to zero (last Valkey
// removed), it is left alone - running sentinels keep running.
func (r *ValkeySentinelReconciler) upsertSentinelStatefulSet(ctx context.Context, s *valkeyiov1alpha1.ValkeySentinel, configHash string, hasMonitors bool) (*appsv1.StatefulSet, error) {
	defaultMode := int32(0o755)
	cmName := sentinelResourceName(s)
	authName := sentinelAuthSecretName(s)
	revisionHistory := int32(10) // matches the API server default

	// Resolve persistence with defaults. enabled=true means the pod
	// template mounts the data volume from a VolumeClaimTemplates entry
	// (per-pod PVC). enabled=false falls back to a disk-backed emptyDir.
	persistence, persistenceEnabled := s.Spec.EffectivePersistence()
	dataVolume, claimTemplates := buildSentinelDataVolume(persistence, persistenceEnabled)
	pvcRetentionPolicy := buildSentinelPVCRetention(persistence, persistenceEnabled)

	desired := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: sentinelResourceName(s), Namespace: s.Namespace,
			Labels: sentinelLabels(s),
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:    &s.Spec.Replicas,
			ServiceName: sentinelResourceName(s),
			Selector:    &metav1.LabelSelector{MatchLabels: sentinelSelectorLabels(s)},
			// Pre-populate the SS-level defaults the API server would
			// otherwise apply (k/k pkg/apis/apps/v1/defaults). Same
			// flap signature as on the ValkeyNode SS - see
			// buildValkeyNodeStatefulSet for the full explanation.
			PodManagementPolicy:  appsv1.OrderedReadyPodManagement,
			RevisionHistoryLimit: &revisionHistory,
			UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
				Type: appsv1.RollingUpdateStatefulSetStrategyType,
			},
			PersistentVolumeClaimRetentionPolicy: pvcRetentionPolicy,
			VolumeClaimTemplates:                 claimTemplates,
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
						Command:   []string{"/scripts/" + sentinelStartupScriptKey},
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
					},
				},
			},
		},
	}
	if dataVolume != nil {
		// Disabled-persistence path: emptyDir volume sourced from
		// buildSentinelDataVolume. With persistence enabled the data
		// dir is mounted from the VolumeClaimTemplates entry above
		// (the PVC the SS controller stamps out per pod) and there's
		// no volume entry to add in the pod template.
		desired.Spec.Template.Spec.Volumes = append(desired.Spec.Template.Spec.Volumes, *dataVolume)
	}
	// Pre-populate the per-container / per-volume / pod-level defaults
	// the API server would otherwise fill in. Same reason as on the
	// ValkeyNode pod template - leaving probe SuccessThreshold,
	// HTTPGet.Scheme, ContainerPort.Protocol etc. zero causes the
	// stored spec to differ from the freshly built one and makes every
	// reconcile re-Update the SS.
	desired.Spec.Template.Spec = applyPodSpecDefaults(desired.Spec.Template.Spec)

	existing := &appsv1.StatefulSet{}
	err := r.Get(ctx, types.NamespacedName{Name: desired.Name, Namespace: desired.Namespace}, existing)
	if apierrors.IsNotFound(err) {
		// Defer initial creation if there is nothing to monitor yet.
		// See the function-level docstring for the rationale.
		if !hasMonitors {
			return nil, nil
		}
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
	// VolumeClaimTemplates is immutable after creation. Refuse to
	// reconcile if the user toggled spec.persistence on an existing
	// sentinel - surfacing a clear error is safer than silently
	// destroying state by recreating the SS. The user has to delete
	// and recreate the ValkeySentinel to change persistence.
	if err := assertPersistenceImmutable(existing, claimTemplates); err != nil {
		return nil, err
	}
	// Only Update when something we manage actually changed. Without
	// this check, every reconcile reassigns Spec.Template wholesale -
	// even when the rendered template is byte-identical to what's
	// already on the server. Under RollingUpdate strategy a no-op
	// Update of Spec.Template still bumps updateRevision, which
	// triggers a roll of all sentinel pods. The live cluster log
	// showed this manifesting as ~80 +sentinel/+sdown events per
	// 28 minutes - sentinels couldn't stay up long enough to observe
	// anything meaningful, let alone act on it.
	//
	// "Meaningful change" = Labels diverge, Replicas diverge, or the
	// rendered pod template (including the configHash annotation,
	// which is the deliberate roll signal) diverges.
	want := existing.DeepCopy()
	want.Labels = desired.Labels
	want.Spec.Replicas = desired.Spec.Replicas
	want.Spec.Template = desired.Spec.Template
	if err := controllerutil.SetControllerReference(s, want, r.Scheme); err != nil {
		return nil, err
	}
	if reflect.DeepEqual(existing.Labels, want.Labels) &&
		reflect.DeepEqual(existing.Spec.Replicas, want.Spec.Replicas) &&
		reflect.DeepEqual(existing.Spec.Template, want.Spec.Template) &&
		reflect.DeepEqual(existing.OwnerReferences, want.OwnerReferences) {
		return existing, nil
	}
	if err := r.Update(ctx, want); err != nil {
		return nil, err
	}
	return want, nil
}

// buildSentinelDataVolume returns the volume entry for the sentinel
// data dir, plus the per-pod VolumeClaimTemplates entry if persistence
// is enabled.
//
//   - persistence enabled  -> volume=nil (the volume comes from the VCT
//     the StatefulSet controller stamps out per pod), claimTemplates
//     contains one PVC template named sentinelDataVolumeName.
//   - persistence disabled -> volume points at a disk-backed emptyDir
//     (current behavior), claimTemplates is nil.
//
// The pod template's VolumeMount on sentinelDataVolumeName resolves to
// either source transparently.
func buildSentinelDataVolume(p *valkeyiov1alpha1.SentinelPersistenceSpec, enabled bool) (*corev1.Volume, []corev1.PersistentVolumeClaim) {
	if !enabled {
		size := resource.MustParse("16Mi")
		return &corev1.Volume{
			Name: sentinelDataVolumeName,
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: &size},
			},
		}, nil
	}
	template := corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: sentinelDataVolumeName},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: p.Size},
			},
			StorageClassName: p.StorageClassName,
		},
	}
	return nil, []corev1.PersistentVolumeClaim{template}
}

// buildSentinelPVCRetention mirrors the user's chosen ReclaimPolicy
// onto the SS-level retention. WhenDeleted controls what happens when
// the StatefulSet is deleted; WhenScaled controls what happens when
// the user scales the SS down. We honour ReclaimPolicy=Delete only on
// SS deletion - scaling down a sentinel set is rare and the user can
// clean up the orphaned PVCs by hand when it happens. Default and
// disabled-persistence paths both use Retain.
func buildSentinelPVCRetention(p *valkeyiov1alpha1.SentinelPersistenceSpec, enabled bool) *appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy {
	policy := appsv1.RetainPersistentVolumeClaimRetentionPolicyType
	if enabled && p.ReclaimPolicy == valkeyiov1alpha1.PersistenceReclaimPolicyDelete {
		policy = appsv1.DeletePersistentVolumeClaimRetentionPolicyType
	}
	return &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
		WhenDeleted: policy,
		WhenScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
	}
}

// assertPersistenceImmutable refuses to reconcile when the user toggles
// spec.persistence on an existing sentinel. StatefulSet's
// VolumeClaimTemplates is immutable after creation; silently ignoring
// the change would let spec drift in a way that the operator can't
// reconcile. Auto-recreating the SS would delete and recreate every
// sentinel pod, which is an aggressive action to take on a user toggle.
// Surfacing a clear error keeps the choice in the user's hands - they
// recreate the ValkeySentinel resource to switch persistence modes.
func assertPersistenceImmutable(existing *appsv1.StatefulSet, desiredTemplates []corev1.PersistentVolumeClaim) error {
	hasExisting := len(existing.Spec.VolumeClaimTemplates) > 0
	hasDesired := len(desiredTemplates) > 0
	if hasExisting == hasDesired {
		return nil
	}
	if hasExisting && !hasDesired {
		return fmt.Errorf("cannot disable persistence on an existing ValkeySentinel: StatefulSet.VolumeClaimTemplates is immutable. Delete and recreate the ValkeySentinel to switch to non-persistent storage.")
	}
	return fmt.Errorf("cannot enable persistence on an existing ValkeySentinel: StatefulSet.VolumeClaimTemplates is immutable. Delete and recreate the ValkeySentinel to switch to persistent storage.")
}
