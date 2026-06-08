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
	"encoding/json"
	"fmt"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	valkeyiov1alpha1 "valkey.io/valkey-operator/api/v1alpha1"
)

// valkeyNodeResourceName returns the name used for resources
// owned by the given ValkeyNode.
func valkeyNodeResourceName(node *valkeyiov1alpha1.ValkeyNode) string {
	return resourcePrefix + node.Name
}

// configInitScript seeds the writable valkey.conf from the read-only
// ConfigMap while preserving the REPLICAOF directive across pod cycles
// when the writable volume is PVC-backed.
//
// The flow is intentionally idempotent and safe under either volume
// backing (emptyDir or PVC):
//
//  1. If an existing writable file is found, capture any leading
//     `replicaof <host> <port>` / `slaveof <host> <port>` directive that
//     CONFIG REWRITE has persisted.
//  2. Copy the ConfigMap-rendered valkey.conf over the writable file -
//     ConfigMap remains the source of truth for everything else.
//  3. Re-append the captured replicaof line (if any) so the cycled pod
//     boots back into the role it was last given.
//
// With emptyDir the existing file never survives a pod cycle, so step 1
// is a no-op and the behavior matches the original cp-only init. With
// PVC backing the captured replicaof line eliminates the brief
// multi-master window otherwise opened every time a replica's pod
// restarts.
const configInitScript = `set -eu
CONF_DIR=` + writableConfigPath + `
PRESERVED=""
if [ -f "$CONF_DIR/valkey.conf" ]; then
    PRESERVED=$(grep -E '^(replicaof|slaveof) ' "$CONF_DIR/valkey.conf" | head -n 1 || true)
fi
cp /config/valkey.conf "$CONF_DIR/valkey.conf"
if [ -n "$PRESERVED" ]; then
    printf '\n%s\n' "$PRESERVED" >> "$CONF_DIR/valkey.conf"
fi
`

// writableConfigPVCSize is the storage request for the writable
// valkey.conf PVC. The file is a few kilobytes - 16Mi leaves ample
// headroom for filesystem overhead on small-block-size storage classes
// while staying small enough to provision instantly on every common
// dynamic provisioner.
var writableConfigPVCSize = resource.MustParse("16Mi")

// buildWritableConfigVCT returns the per-pod VolumeClaimTemplates entry
// that backs the writable valkey.conf when persistence is enabled.
// Returned as nil when the node uses the emptyDir path so callers can
// append unconditionally.
func buildWritableConfigVCT(node *valkeyiov1alpha1.ValkeyNode) *corev1.PersistentVolumeClaim {
	if !node.Spec.PersistWritableConfig {
		return nil
	}
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: writableConfigVolumeName},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: writableConfigPVCSize},
			},
		},
	}
}

// valkeyNodeServiceHost is the per-ValkeyNode Service's cluster-internal
// DNS name. Sentinels monitor data nodes by this name so pod-IP changes
// across restarts don't leave stale `known-replica` entries.
func valkeyNodeServiceHost(node *valkeyiov1alpha1.ValkeyNode) string {
	return fmt.Sprintf("%s.%s.svc.cluster.local", valkeyNodeResourceName(node), node.Namespace)
}

// buildValkeyNodeService is the per-ValkeyNode headless Service. Its
// name matches the StatefulSet's ServiceName so the SS-managed pod
// hostname resolves via this Service. PublishNotReadyAddresses is true
// so sentinel can reach a pod that's briefly NotReady during a planned
// failover. Selector targets the single pod that backs this
// ValkeyNode via valkey.io/valkey + valkey.io/node-index.
func buildValkeyNodeService(node *valkeyiov1alpha1.ValkeyNode) *corev1.Service {
	labels := valkeyNodeLabels(node)
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      valkeyNodeResourceName(node),
			Namespace: node.Namespace,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			Type:                     corev1.ServiceTypeClusterIP,
			ClusterIP:                headlessClusterIP,
			PublishNotReadyAddresses: true,
			Selector: map[string]string{
				LabelValkey:    node.Labels[LabelValkey],
				LabelNodeIndex: node.Labels[LabelNodeIndex],
			},
			Ports: []corev1.ServicePort{
				{Name: "valkey", Port: DefaultPort, Protocol: corev1.ProtocolTCP},
			},
		},
	}
}

// valkeyNodeLabels returns the standard Kubernetes recommended labels for
// child resources of the given ValkeyNode.
func valkeyNodeLabels(node *valkeyiov1alpha1.ValkeyNode) map[string]string {
	l := baseLabels(node.Name, "valkey-node")
	for _, key := range []string{
		LabelCluster,
		LabelValkey,
		LabelShardIndex,
		LabelNodeIndex,
	} {
		if v, ok := node.Labels[key]; ok {
			l[key] = v
		}
	}
	return l
}

// buildValkeyNodeConfigMap builds a ConfigMap containing the embedded liveness
// and readiness probe scripts, plus an empty valkey.conf.
// The ConfigMap is named via config.go:getConfigMapName(node).
func buildValkeyNodeConfigMap(node *valkeyiov1alpha1.ValkeyNode) (*corev1.ConfigMap, error) {
	liveness, err := scripts.ReadFile("scripts/liveness-check.sh")
	if err != nil {
		return nil, fmt.Errorf("reading embedded liveness-check.sh: %w", err)
	}
	readiness, err := scripts.ReadFile("scripts/readiness-check.sh")
	if err != nil {
		return nil, fmt.Errorf("reading embedded readiness-check.sh: %w", err)
	}

	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      GetServerConfigMapName(node.Name),
			Namespace: node.Namespace,
			Labels:    valkeyNodeLabels(node),
		},
		Data: map[string]string{
			"valkey.conf":        generateValkeyNodeConfig(node),
			"liveness-check.sh":  string(liveness),
			"readiness-check.sh": string(readiness),
		},
	}, nil
}

func valkeyNodePVCName(node *valkeyiov1alpha1.ValkeyNode) string {
	return valkeyNodeResourceName(node) + "-data"
}

func buildValkeyNodePVC(node *valkeyiov1alpha1.ValkeyNode) *corev1.PersistentVolumeClaim {
	if node.Spec.Persistence == nil {
		return nil
	}

	labels := valkeyNodeLabels(node)
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      valkeyNodePVCName(node),
			Namespace: node.Namespace,
			Labels:    labels,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: node.Spec.Persistence.Size,
				},
			},
			StorageClassName: node.Spec.Persistence.StorageClassName,
		},
	}
}

// mergePatchContainers applies a strategic merge patch to base containers using
// patches as the patch source. Containers are matched by name; any patch
// container whose name matches a base container is merged into it, while patch
// containers with new names are appended in patch-list order.
func mergePatchContainers(base, patches []corev1.Container) ([]corev1.Container, error) {
	var output []corev1.Container

	patchByName := make(map[string]corev1.Container, len(patches))
	for _, c := range patches {
		patchByName[c.Name] = c
	}

	for _, c := range base {
		patch, ok := patchByName[c.Name]
		if !ok {
			output = append(output, c)
			continue
		}
		baseBytes, err := json.Marshal(c)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal JSON for container %s: %w", c.Name, err)
		}
		patchBytes, err := json.Marshal(patch)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal JSON for patch container %s: %w", c.Name, err)
		}
		merged, err := strategicpatch.StrategicMergePatch(baseBytes, patchBytes, corev1.Container{})
		if err != nil {
			return nil, fmt.Errorf("failed to generate merge patch for container %s: %w", c.Name, err)
		}
		var result corev1.Container
		if err := json.Unmarshal(merged, &result); err != nil {
			return nil, fmt.Errorf("failed to unmarshal merged container %s: %w", c.Name, err)
		}
		output = append(output, result)
		delete(patchByName, c.Name)
	}

	// Append any patch containers that did not match a base container, in
	// the original patch-list order.
	for _, c := range patches {
		if _, remaining := patchByName[c.Name]; remaining {
			output = append(output, c)
		}
	}

	return output, nil
}

// buildContainersDef builds the base containers definition for the ValkeyNode
// and applies any strategic merge patches from node.Spec.Containers.
func buildContainersDef(node *valkeyiov1alpha1.ValkeyNode) ([]corev1.Container, error) {
	image := DefaultImage
	if node.Spec.Image != "" {
		image = node.Spec.Image
	}

	containers := []corev1.Container{
		{
			Name:      "server",
			Image:     image,
			Resources: node.Spec.Resources,
			Command: []string{
				"valkey-server",
				// Run from a writable copy of the config (populated by the
				// config-init initContainer). valkey.conf MUST live on a
				// writable filesystem: CONFIG REWRITE - which Sentinel issues
				// on every failover promotion, and which `CONFIG SET` +
				// persistence rely on - fails with "Read-only file system" if
				// the server is launched against the ConfigMap mount directly.
				writableConfigPath + "/valkey.conf",
			},
			Ports: []corev1.ContainerPort{
				{
					Name:          "client",
					ContainerPort: DefaultPort,
				},
				{
					Name:          "cluster-bus",
					ContainerPort: DefaultClusterBusPort,
				},
			},
			StartupProbe: &corev1.Probe{
				InitialDelaySeconds: 5,
				PeriodSeconds:       5,
				FailureThreshold:    20,
				TimeoutSeconds:      5,
				SuccessThreshold:    1,
				ProbeHandler: corev1.ProbeHandler{
					Exec: &corev1.ExecAction{
						Command: []string{
							"/bin/bash",
							"-c",
							"/scripts/liveness-check.sh",
						},
					},
				},
			},
			LivenessProbe: &corev1.Probe{
				InitialDelaySeconds: 5,
				PeriodSeconds:       5,
				FailureThreshold:    5,
				TimeoutSeconds:      5,
				SuccessThreshold:    1,
				ProbeHandler: corev1.ProbeHandler{
					Exec: &corev1.ExecAction{
						Command: []string{
							"/bin/bash",
							"-c",
							"/scripts/liveness-check.sh",
						},
					},
				},
			},
			ReadinessProbe: &corev1.Probe{
				InitialDelaySeconds: 5,
				PeriodSeconds:       5,
				FailureThreshold:    5,
				TimeoutSeconds:      2,
				SuccessThreshold:    1,
				ProbeHandler: corev1.ProbeHandler{
					Exec: &corev1.ExecAction{
						Command: []string{
							"/bin/bash",
							"-c",
							"/scripts/readiness-check.sh",
						},
					},
				},
			},
			VolumeMounts: []corev1.VolumeMount{
				{
					Name:      "scripts",
					MountPath: "/scripts",
				},
				{
					Name:      "valkey-conf",
					MountPath: "/config",
					ReadOnly:  true,
				},
				{
					Name:      writableConfigVolumeName,
					MountPath: writableConfigPath,
				},
			},
		},
	}

	if node.Spec.Persistence != nil {
		containers[0].VolumeMounts = append(containers[0].VolumeMounts, corev1.VolumeMount{
			Name:      dataVolumeName,
			MountPath: dataMountPath,
		})
	}

	if node.Spec.TLS != nil {
		containers[0].VolumeMounts = append(containers[0].VolumeMounts, corev1.VolumeMount{
			Name:      tlsVolumeName,
			MountPath: tlsCertMountPath,
			ReadOnly:  true,
		})
		containers[0].Env = append(containers[0].Env,
			corev1.EnvVar{Name: "VALKEY_TLS_ENABLED", Value: "true"},
			corev1.EnvVar{Name: "VALKEY_TLS_CA_FILE", Value: tlsCertMountPath + "/" + tlsSecretKeyCA},
			corev1.EnvVar{Name: "VALKEY_TLS_CERT_FILE", Value: tlsCertMountPath + "/" + tlsSecretKeyCert},
			corev1.EnvVar{Name: "VALKEY_TLS_KEY_FILE", Value: tlsCertMountPath + "/" + tlsSecretKeyKey},
			corev1.EnvVar{Name: "VALKEY_TLS_ARGS", Value: fmt.Sprintf("--tls --cacert %s --cert %s --key %s",
				tlsCertMountPath+"/"+tlsSecretKeyCA, tlsCertMountPath+"/"+tlsSecretKeyCert, tlsCertMountPath+"/"+tlsSecretKeyKey)},
		)
	}

	// Add exporter sidecar if enabled.
	if node.Spec.Exporter.Enabled {
		containers = append(containers, generateMetricsExporterContainerDef(node.Spec.Exporter, node.Labels[LabelCluster], node.Spec.TLS))
	}

	return mergePatchContainers(containers, node.Spec.Containers)
}

// buildValkeyNodePodTemplateSpec constructs a PodTemplateSpec for a single
// Valkey node.
func buildValkeyNodePodTemplateSpec(node *valkeyiov1alpha1.ValkeyNode, labels map[string]string) (corev1.PodTemplateSpec, error) {
	containers, err := buildContainersDef(node)
	if err != nil {
		return corev1.PodTemplateSpec{}, err
	}

	// Use the explicitly provided ConfigMap name, or fall back to the default
	// resource name (which the controller creates automatically).
	configMapName := node.Spec.ServerConfigMapName
	if configMapName == "" {
		configMapName = GetServerConfigMapName(node.Name)
	}

	image := DefaultImage
	if node.Spec.Image != "" {
		image = node.Spec.Image
	}

	podSpec := corev1.PodSpec{
		Containers:   containers,
		NodeSelector: node.Spec.NodeSelector,
		Affinity:     node.Spec.Affinity,
		Tolerations:  node.Spec.Tolerations,
		// config-init seeds the writable copy of valkey.conf from the
		// read-only ConfigMap. The ConfigMap stays the source of truth
		// for everything except the REPLICAOF directive: when the
		// writable file is PVC-backed (Spec.PersistWritableConfig) we
		// preserve REPLICAOF across pod cycles so a restarted replica
		// doesn't boot as a master. ConfigMap-driven changes still
		// propagate because we recopy the body unconditionally; only
		// the surviving REPLICAOF line is re-appended.
		InitContainers: []corev1.Container{
			{
				Name:    "config-init",
				Image:   image,
				Command: []string{"sh", "-c", configInitScript},
				VolumeMounts: []corev1.VolumeMount{
					{Name: "valkey-conf", MountPath: "/config", ReadOnly: true},
					{Name: writableConfigVolumeName, MountPath: writableConfigPath},
				},
			},
		},
		Volumes: []corev1.Volume{
			{
				Name: "scripts",
				VolumeSource: corev1.VolumeSource{
					ConfigMap: &corev1.ConfigMapVolumeSource{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: configMapName,
						},
						DefaultMode: func(i int32) *int32 { return &i }(0755),
					},
				},
			},
			{
				Name: "valkey-conf",
				VolumeSource: corev1.VolumeSource{
					ConfigMap: &corev1.ConfigMapVolumeSource{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: configMapName,
						},
					},
				},
			},
		},
	}

	// When the writable config is PVC-backed the volume comes from the
	// StatefulSet's VolumeClaimTemplates (stamped in
	// buildValkeyNodeStatefulSet). For the emptyDir path - ValkeyCluster
	// nodes, Deployments - declare the volume inline here.
	if !node.Spec.PersistWritableConfig {
		podSpec.Volumes = append(podSpec.Volumes, corev1.Volume{
			Name: writableConfigVolumeName,
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		})
	}

	if node.Spec.UsersACLSecretName != "" {
		podSpec.Volumes = append(podSpec.Volumes, corev1.Volume{
			Name: "users-acl",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: node.Spec.UsersACLSecretName,
				},
			},
		})
		// Containers[0] is always the server container (exporter is appended after it).
		podSpec.Containers[0].VolumeMounts = append(podSpec.Containers[0].VolumeMounts, corev1.VolumeMount{
			Name:      "users-acl",
			MountPath: "/config/users",
			ReadOnly:  true,
		})
	}

	if node.Spec.TLS != nil {
		podSpec.Volumes = append(podSpec.Volumes, corev1.Volume{
			Name: tlsVolumeName,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: node.Spec.TLS.Certificate.SecretName,
				},
			},
		})
	}

	if node.Spec.Persistence != nil {
		podSpec.Volumes = append(podSpec.Volumes, corev1.Volume{
			Name: dataVolumeName,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: valkeyNodePVCName(node),
				},
			},
		})
	}

	return corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels: labels,
		},
		Spec: applyPodSpecDefaults(podSpec),
	}, nil
}

// applyPodSpecDefaults pre-populates the fields the Kubernetes API
// server will otherwise default on UPDATE. controllerutil.CreateOrUpdate
// rewrites the SS spec wholesale; if we leave these unset, the API
// server's defaulting causes a diff on every reconcile (-> a new SS
// updateRevision -> the rollout machinery thinks every pod is pending,
// every reconcile). Setting them explicitly stabilises the spec so
// reconciles are no-ops in steady state.
//
// The list mirrors the defaults applied in k/k pkg/apis/core/v1/defaults
// for PodSpec, Container, and the common volume sources we use. Updating
// the runtime defaults here is acceptable - these are the values K8s
// would apply anyway; we're just preempting them.
func applyPodSpecDefaults(s corev1.PodSpec) corev1.PodSpec {
	if s.RestartPolicy == "" {
		s.RestartPolicy = corev1.RestartPolicyAlways
	}
	if s.DNSPolicy == "" {
		s.DNSPolicy = corev1.DNSClusterFirst
	}
	if s.SchedulerName == "" {
		s.SchedulerName = corev1.DefaultSchedulerName
	}
	if s.TerminationGracePeriodSeconds == nil {
		grace := int64(corev1.DefaultTerminationGracePeriodSeconds)
		s.TerminationGracePeriodSeconds = &grace
	}
	if s.SecurityContext == nil {
		s.SecurityContext = &corev1.PodSecurityContext{}
	}
	for i := range s.InitContainers {
		applyContainerDefaults(&s.InitContainers[i])
	}
	for i := range s.Containers {
		applyContainerDefaults(&s.Containers[i])
	}
	for i := range s.Volumes {
		applyVolumeDefaults(&s.Volumes[i])
	}
	return s
}

// applyContainerDefaults sets the per-Container fields the API server
// would otherwise default on UPDATE.
func applyContainerDefaults(c *corev1.Container) {
	if c.TerminationMessagePath == "" {
		c.TerminationMessagePath = corev1.TerminationMessagePathDefault
	}
	if c.TerminationMessagePolicy == "" {
		c.TerminationMessagePolicy = corev1.TerminationMessageReadFile
	}
	if c.ImagePullPolicy == "" {
		// Mirrors the API server's defaulting rule (k/k pkg/apis/core/v1/defaults).
		if strings.HasSuffix(c.Image, ":latest") || !strings.Contains(c.Image, ":") {
			c.ImagePullPolicy = corev1.PullAlways
		} else {
			c.ImagePullPolicy = corev1.PullIfNotPresent
		}
	}
	// ContainerPort.Protocol defaults to TCP on the API server. Leaving
	// it empty causes the same flap.
	for i := range c.Ports {
		if c.Ports[i].Protocol == "" {
			c.Ports[i].Protocol = corev1.ProtocolTCP
		}
	}
	// Probe fields the API server defaults on UPDATE. Any zero value
	// here gets filled in by the server which then differs from a
	// freshly-built spec on the next reconcile - same flap signature
	// as PVC retention / ContainerPort.Protocol but reached through
	// a probe-bearing sidecar (e.g. the metrics-exporter).
	applyProbeDefaults(c.LivenessProbe)
	applyProbeDefaults(c.ReadinessProbe)
	applyProbeDefaults(c.StartupProbe)
}

// applyProbeDefaults pre-populates the Probe fields the API server
// would otherwise default on UPDATE. Safe to call with a nil probe.
func applyProbeDefaults(p *corev1.Probe) {
	if p == nil {
		return
	}
	if p.TimeoutSeconds == 0 {
		p.TimeoutSeconds = 1
	}
	if p.PeriodSeconds == 0 {
		p.PeriodSeconds = 10
	}
	if p.SuccessThreshold == 0 {
		p.SuccessThreshold = 1
	}
	if p.FailureThreshold == 0 {
		p.FailureThreshold = 3
	}
	if p.HTTPGet != nil && p.HTTPGet.Scheme == "" {
		p.HTTPGet.Scheme = corev1.URISchemeHTTP
	}
}

// applyVolumeDefaults sets the DefaultMode the API server would
// otherwise apply for ConfigMap/Secret volumes.
func applyVolumeDefaults(v *corev1.Volume) {
	mode := corev1.ConfigMapVolumeSourceDefaultMode
	switch {
	case v.ConfigMap != nil && v.ConfigMap.DefaultMode == nil:
		v.ConfigMap.DefaultMode = &mode
	case v.Secret != nil && v.Secret.DefaultMode == nil:
		v.Secret.DefaultMode = &mode
	case v.Projected != nil && v.Projected.DefaultMode == nil:
		v.Projected.DefaultMode = &mode
	}
}

// buildValkeyNodeDeployment constructs a single-replica Deployment for a
// ValkeyNode. This is used when node.Spec.WorkloadType is Deployment.
func buildValkeyNodeDeployment(node *valkeyiov1alpha1.ValkeyNode) (*appsv1.Deployment, error) {
	labels := valkeyNodeLabels(node)
	tmpl, err := buildValkeyNodePodTemplateSpec(node, labels)
	if err != nil {
		return nil, err
	}
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      valkeyNodeResourceName(node),
			Namespace: node.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: func(i int32) *int32 { return &i }(1),
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: tmpl,
		},
	}, nil
}

// buildValkeyNodeStatefulSet constructs a single-replica StatefulSet for a
// ValkeyNode. This is used when node.Spec.WorkloadType is StatefulSet (the
// default).
func buildValkeyNodeStatefulSet(node *valkeyiov1alpha1.ValkeyNode) (*appsv1.StatefulSet, error) {
	labels := valkeyNodeLabels(node)
	tmpl, err := buildValkeyNodePodTemplateSpec(node, labels)
	if err != nil {
		return nil, err
	}
	one := int32(1)
	revisionHistory := int32(10) // matches the API server default

	// VolumeClaimTemplates: data PVC (when persistence is enabled) and
	// the writable-config PVC (when PersistWritableConfig is set). Both
	// are stamped per-pod by the StatefulSet controller. VCT itself is
	// immutable after SS creation - ensureStatefulSet asserts that
	// before issuing an Update.
	var volumeClaimTemplates []corev1.PersistentVolumeClaim
	if wc := buildWritableConfigVCT(node); wc != nil {
		volumeClaimTemplates = append(volumeClaimTemplates, *wc)
	}
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      valkeyNodeResourceName(node),
			Namespace: node.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:    &one,
			ServiceName: valkeyNodeResourceName(node),
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: tmpl,
			// OnDelete: the operator drives pod replacement order during
			// rolling restarts. The default RollingUpdate strategy would
			// cycle the primary at an unpredictable moment, costing the
			// down-after-milliseconds window of writes to reactive
			// sentinel failover. With OnDelete the operator can roll
			// replicas first, perform a planned SENTINEL FAILOVER, and
			// only then delete the (now-demoted) old primary. See
			// internal/controller/valkey_rollout.go.
			UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
				Type: appsv1.OnDeleteStatefulSetStrategyType,
			},
			// Set the SS-level defaults explicitly to keep the spec
			// stable across reconciles - leaving these zero/nil makes
			// controllerutil.CreateOrUpdate flap them on every pass,
			// which bumps the SS updateRevision and confuses the
			// rollout observer.
			PodManagementPolicy:  appsv1.OrderedReadyPodManagement,
			RevisionHistoryLimit: &revisionHistory,
			// API server (k8s 1.27+) defaults this to {Retain, Retain}.
			// Pre-populate so CreateOrUpdate is a true no-op in steady
			// state.
			PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
				WhenDeleted: appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
				WhenScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
			},
			VolumeClaimTemplates: volumeClaimTemplates,
		},
	}, nil
}
