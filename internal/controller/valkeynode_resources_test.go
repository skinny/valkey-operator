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
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	valkeyv1 "valkey.io/valkey-operator/api/v1alpha1"
)

func newTestValkeyNode(name, namespace string) *valkeyv1.ValkeyNode {
	return &valkeyv1.ValkeyNode{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: valkeyv1.ValkeyNodeSpec{
			Image:               "valkey/valkey:9.0.0",
			ServerConfigMapName: "valkey-config",
		},
	}
}

func TestValkeyNodeResourceName(t *testing.T) {
	node := newTestValkeyNode("mycluster-0-0", "default")
	got := valkeyNodeResourceName(node)
	assert.Equal(t, "valkey-mycluster-0-0", got)
}

func TestValkeyNodeResourceName_Simple(t *testing.T) {
	node := newTestValkeyNode("foo", "ns1")
	assert.Equal(t, "valkey-foo", valkeyNodeResourceName(node))
}

func TestValkeyNodeServiceHost(t *testing.T) {
	node := newTestValkeyNode("cache-0-0", "data")
	assert.Equal(t, "valkey-cache-0-0.data.svc.cluster.local", valkeyNodeServiceHost(node))
}

func TestBuildValkeyNodeService(t *testing.T) {
	node := newTestValkeyNode("cache-0-0", "data")
	node.Labels = map[string]string{
		LabelValkey:    "cache",
		LabelNodeIndex: "0",
	}
	svc := buildValkeyNodeService(node)

	// Service name matches valkeyNodeResourceName so SS hostname resolves through it.
	assert.Equal(t, "valkey-cache-0-0", svc.Name)
	assert.Equal(t, "data", svc.Namespace)

	// Headless so Sentinel resolves to the pod IP directly, not a VIP.
	assert.Equal(t, corev1.ServiceTypeClusterIP, svc.Spec.Type)
	assert.Equal(t, headlessClusterIP, svc.Spec.ClusterIP)

	// Sentinel needs to reach the pod even when briefly NotReady during failover.
	assert.True(t, svc.Spec.PublishNotReadyAddresses)

	// Selector picks exactly the single pod backing this ValkeyNode.
	assert.Equal(t, "cache", svc.Spec.Selector[LabelValkey])
	assert.Equal(t, "0", svc.Spec.Selector[LabelNodeIndex])

	require.Len(t, svc.Spec.Ports, 1)
	assert.Equal(t, int32(DefaultPort), svc.Spec.Ports[0].Port)
	assert.Equal(t, corev1.ProtocolTCP, svc.Spec.Ports[0].Protocol)
}

func TestBuildValkeyNodePodTemplateSpec(t *testing.T) {
	node := newTestValkeyNode("mynode", "test-ns")
	lbls := valkeyNodeLabels(node)
	pts, err := buildValkeyNodePodTemplateSpec(node, lbls)
	require.NoError(t, err)

	// Verify labels on pod template
	assert.Equal(t, lbls, pts.Labels)

	// Verify single container
	require.Len(t, pts.Spec.Containers, 1, "should have 1 container when exporter is disabled")

	c := pts.Spec.Containers[0]

	// Container name
	assert.Equal(t, "server", c.Name)

	// Image
	assert.Equal(t, "valkey/valkey:9.0.0", c.Image)

	// Command - launched from the writable config copy so CONFIG REWRITE works.
	assert.Equal(t, []string{"valkey-server", "/etc/valkey/valkey.conf"}, c.Command)

	// Ports
	require.Len(t, c.Ports, 2)
	assert.Equal(t, "client", c.Ports[0].Name)
	assert.Equal(t, int32(DefaultPort), c.Ports[0].ContainerPort)
	assert.Equal(t, "cluster-bus", c.Ports[1].Name)
	assert.Equal(t, int32(DefaultClusterBusPort), c.Ports[1].ContainerPort)

	// StartupProbe
	require.NotNil(t, c.StartupProbe)
	assert.Equal(t, int32(5), c.StartupProbe.InitialDelaySeconds)
	assert.Equal(t, int32(5), c.StartupProbe.PeriodSeconds)
	assert.Equal(t, int32(20), c.StartupProbe.FailureThreshold)
	assert.Equal(t, int32(5), c.StartupProbe.TimeoutSeconds)
	assert.Contains(t, c.StartupProbe.Exec.Command, "/scripts/liveness-check.sh")

	// LivenessProbe
	require.NotNil(t, c.LivenessProbe)
	assert.Equal(t, int32(5), c.LivenessProbe.InitialDelaySeconds)
	assert.Equal(t, int32(5), c.LivenessProbe.PeriodSeconds)
	assert.Equal(t, int32(5), c.LivenessProbe.FailureThreshold)
	assert.Equal(t, int32(5), c.LivenessProbe.TimeoutSeconds)
	assert.Contains(t, c.LivenessProbe.Exec.Command, "/scripts/liveness-check.sh")

	// ReadinessProbe
	require.NotNil(t, c.ReadinessProbe)
	assert.Equal(t, int32(5), c.ReadinessProbe.InitialDelaySeconds)
	assert.Equal(t, int32(5), c.ReadinessProbe.PeriodSeconds)
	assert.Equal(t, int32(5), c.ReadinessProbe.FailureThreshold)
	assert.Equal(t, int32(2), c.ReadinessProbe.TimeoutSeconds)
	assert.Contains(t, c.ReadinessProbe.Exec.Command, "/scripts/readiness-check.sh")

	// VolumeMounts
	require.Len(t, c.VolumeMounts, 3)
	assert.Equal(t, "scripts", c.VolumeMounts[0].Name)
	assert.Equal(t, "/scripts", c.VolumeMounts[0].MountPath)
	assert.Equal(t, "valkey-conf", c.VolumeMounts[1].Name)
	assert.Equal(t, "/config", c.VolumeMounts[1].MountPath)
	assert.True(t, c.VolumeMounts[1].ReadOnly, "valkey-conf mount should be read-only")
	assert.Equal(t, writableConfigVolumeName, c.VolumeMounts[2].Name)
	assert.Equal(t, writableConfigPath, c.VolumeMounts[2].MountPath)
	assert.False(t, c.VolumeMounts[2].ReadOnly, "writable config mount must be writable")

	// Volumes
	require.Len(t, pts.Spec.Volumes, 3)
	assert.Equal(t, "scripts", pts.Spec.Volumes[0].Name)
	assert.Equal(t, "valkey-config", pts.Spec.Volumes[0].ConfigMap.Name)
	assert.Equal(t, int32(0755), *pts.Spec.Volumes[0].ConfigMap.DefaultMode)
	assert.Equal(t, "valkey-conf", pts.Spec.Volumes[1].Name)
	assert.Equal(t, "valkey-config", pts.Spec.Volumes[1].ConfigMap.Name)
	assert.Equal(t, writableConfigVolumeName, pts.Spec.Volumes[2].Name)
	require.NotNil(t, pts.Spec.Volumes[2].EmptyDir, "writable config volume must be an emptyDir")

	// config-init populates the writable copy before the server starts.
	require.Len(t, pts.Spec.InitContainers, 1)
	assert.Equal(t, "config-init", pts.Spec.InitContainers[0].Name)
}

func TestBuildValkeyNodeDeployment(t *testing.T) {
	node := newTestValkeyNode("mynode", "test-ns")
	dep, err := buildValkeyNodeDeployment(node)
	require.NoError(t, err)

	assert.Equal(t, "valkey-mynode", dep.Name)
	assert.Equal(t, "test-ns", dep.Namespace)
	require.NotNil(t, dep.Spec.Replicas)
	assert.Equal(t, int32(1), *dep.Spec.Replicas)

	// Verify labels
	assert.Equal(t, "valkey", dep.Labels["app.kubernetes.io/name"])
	assert.Equal(t, "mynode", dep.Labels["app.kubernetes.io/instance"])
	assert.Equal(t, "valkey-operator", dep.Labels["app.kubernetes.io/managed-by"])

	// Verify selector matches template labels
	assert.Equal(t, dep.Spec.Selector.MatchLabels, dep.Spec.Template.Labels)

	// Verify the template has the right container
	require.Len(t, dep.Spec.Template.Spec.Containers, 1)
	assert.Equal(t, "server", dep.Spec.Template.Spec.Containers[0].Name)
}

func TestBuildValkeyNodeStatefulSet(t *testing.T) {
	node := newTestValkeyNode("mynode", "test-ns")
	ss, err := buildValkeyNodeStatefulSet(node)
	require.NoError(t, err)

	assert.Equal(t, "valkey-mynode", ss.Name)
	assert.Equal(t, "test-ns", ss.Namespace)
	assert.Equal(t, "valkey-mynode", ss.Spec.ServiceName, "ServiceName should match resource name")
	require.NotNil(t, ss.Spec.Replicas)
	assert.Equal(t, int32(1), *ss.Spec.Replicas)

	// Verify labels
	assert.Equal(t, "valkey", ss.Labels["app.kubernetes.io/name"])
	assert.Equal(t, "mynode", ss.Labels["app.kubernetes.io/instance"])
	assert.Equal(t, "valkey-operator", ss.Labels["app.kubernetes.io/managed-by"])

	// Verify selector matches template labels
	assert.Equal(t, ss.Spec.Selector.MatchLabels, ss.Spec.Template.Labels)

	// Verify the template has the right container
	require.Len(t, ss.Spec.Template.Spec.Containers, 1)
	assert.Equal(t, "server", ss.Spec.Template.Spec.Containers[0].Name)

	// OnDelete updateStrategy: the operator drives pod replacement order
	// during rolling restarts, not the StatefulSet controller.
	assert.Equal(t, appsv1.OnDeleteStatefulSetStrategyType, ss.Spec.UpdateStrategy.Type)

	// Stability: building twice must produce DeepEqual specs, AND the
	// commonly API-server-defaulted fields must be pre-populated so
	// controllerutil.CreateOrUpdate is a no-op in steady state. Without
	// this the SS spec flaps every reconcile, bumping updateRevision and
	// fooling the rollout observer into thinking every pod is pending.
	ss2, err := buildValkeyNodeStatefulSet(node)
	require.NoError(t, err)
	assert.True(t, equality.Semantic.DeepEqual(ss.Spec, ss2.Spec),
		"SS spec must be stable across builds (otherwise CreateOrUpdate flaps every reconcile)")

	// Specific defaults the API server would otherwise apply on UPDATE.
	require.NotNil(t, ss.Spec.RevisionHistoryLimit)
	assert.Equal(t, int32(10), *ss.Spec.RevisionHistoryLimit)
	assert.Equal(t, appsv1.OrderedReadyPodManagement, ss.Spec.PodManagementPolicy)

	pod := ss.Spec.Template.Spec
	assert.Equal(t, corev1.RestartPolicyAlways, pod.RestartPolicy)
	assert.Equal(t, corev1.DNSClusterFirst, pod.DNSPolicy)
	assert.Equal(t, corev1.DefaultSchedulerName, pod.SchedulerName)
	require.NotNil(t, pod.TerminationGracePeriodSeconds)
	assert.Equal(t, int64(30), *pod.TerminationGracePeriodSeconds)
	require.NotNil(t, pod.SecurityContext)

	c := pod.Containers[0]
	assert.Equal(t, corev1.TerminationMessagePathDefault, c.TerminationMessagePath)
	assert.Equal(t, corev1.TerminationMessageReadFile, c.TerminationMessagePolicy)
	// The default image tag is "9.0.0" (newTestValkeyNode), not :latest -> IfNotPresent.
	assert.Equal(t, corev1.PullIfNotPresent, c.ImagePullPolicy)

	// Every ConfigMap/Secret volume must have DefaultMode set (otherwise
	// the API server defaults it on UPDATE and we get a diff).
	for _, v := range pod.Volumes {
		if v.ConfigMap != nil {
			assert.NotNil(t, v.ConfigMap.DefaultMode, "ConfigMap volume %q missing DefaultMode", v.Name)
		}
		if v.Secret != nil {
			assert.NotNil(t, v.Secret.DefaultMode, "Secret volume %q missing DefaultMode", v.Name)
		}
	}
}

func TestBuildValkeyNodePVC(t *testing.T) {
	node := newTestValkeyNode("mynode", "test-ns")
	storageClassName := "gp3"
	node.Spec.Persistence = &valkeyv1.PersistenceSpec{
		Size:             resource.MustParse("10Gi"),
		StorageClassName: &storageClassName,
	}

	pvc := buildValkeyNodePVC(node)
	require.NotNil(t, pvc)
	assert.Equal(t, "valkey-mynode-data", pvc.Name)
	assert.Equal(t, "test-ns", pvc.Namespace)
	assert.Equal(t, resource.MustParse("10Gi"), pvc.Spec.Resources.Requests[corev1.ResourceStorage])
	require.NotNil(t, pvc.Spec.StorageClassName)
	assert.Equal(t, "gp3", *pvc.Spec.StorageClassName)
	require.Len(t, pvc.Spec.AccessModes, 1)
	assert.Equal(t, corev1.ReadWriteOnce, pvc.Spec.AccessModes[0])
}

func TestBuildValkeyNodePodTemplateSpec_WithExporter(t *testing.T) {
	node := newTestValkeyNode("mynode", "test-ns")
	node.Spec.Exporter = valkeyv1.ExporterSpec{
		Enabled: true,
	}
	lbls := valkeyNodeLabels(node)
	pts, err := buildValkeyNodePodTemplateSpec(node, lbls)
	require.NoError(t, err)

	require.Len(t, pts.Spec.Containers, 2, "should have 2 containers when exporter is enabled")
	assert.Equal(t, "server", pts.Spec.Containers[0].Name)
	assert.Equal(t, "metrics-exporter", pts.Spec.Containers[1].Name)

	// Verify default exporter image
	assert.Equal(t, DefaultExporterImage, pts.Spec.Containers[1].Image)

	// Verify exporter port
	require.Len(t, pts.Spec.Containers[1].Ports, 1)
	assert.Equal(t, "metrics", pts.Spec.Containers[1].Ports[0].Name)
	assert.Equal(t, int32(DefaultExporterPort), pts.Spec.Containers[1].Ports[0].ContainerPort)

	// Verify exporter probes
	require.NotNil(t, pts.Spec.Containers[1].LivenessProbe)
	require.NotNil(t, pts.Spec.Containers[1].ReadinessProbe)
}

func TestBuildValkeyNodePodTemplateSpec_WithExporterCustomImage(t *testing.T) {
	node := newTestValkeyNode("mynode", "test-ns")
	node.Spec.Exporter = valkeyv1.ExporterSpec{
		Enabled: true,
		Image:   "my-exporter:v2.0.0",
	}
	lbls := valkeyNodeLabels(node)
	pts, err := buildValkeyNodePodTemplateSpec(node, lbls)
	require.NoError(t, err)

	require.Len(t, pts.Spec.Containers, 2)
	assert.Equal(t, "my-exporter:v2.0.0", pts.Spec.Containers[1].Image)
}

func TestBuildValkeyNodePodTemplateSpec_Scheduling(t *testing.T) {
	nodeSelector := map[string]string{
		"disktype": "ssd",
		"region":   "us-east-1",
	}
	affinity := &corev1.Affinity{
		PodAntiAffinity: &corev1.PodAntiAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{
				{
					LabelSelector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"app.kubernetes.io/instance": "mynode",
						},
					},
					TopologyKey: "kubernetes.io/hostname",
				},
			},
		},
	}
	tolerations := []corev1.Toleration{
		{
			Key:      "dedicated",
			Operator: corev1.TolerationOpEqual,
			Value:    "valkey",
			Effect:   corev1.TaintEffectNoSchedule,
		},
	}

	node := newTestValkeyNode("mynode", "test-ns")
	node.Spec.NodeSelector = nodeSelector
	node.Spec.Affinity = affinity
	node.Spec.Tolerations = tolerations

	lbls := valkeyNodeLabels(node)
	pts, err := buildValkeyNodePodTemplateSpec(node, lbls)
	require.NoError(t, err)

	assert.Equal(t, nodeSelector, pts.Spec.NodeSelector, "node selector should pass through")
	assert.Equal(t, affinity, pts.Spec.Affinity, "affinity should pass through")
	assert.Equal(t, tolerations, pts.Spec.Tolerations, "tolerations should pass through")
}

func TestBuildValkeyNodePodTemplateSpec_Resources(t *testing.T) {
	resources := corev1.ResourceRequirements{
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("500m"),
			corev1.ResourceMemory: resource.MustParse("512Mi"),
		},
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("250m"),
			corev1.ResourceMemory: resource.MustParse("256Mi"),
		},
	}

	node := newTestValkeyNode("mynode", "test-ns")
	node.Spec.Resources = resources

	lbls := valkeyNodeLabels(node)
	pts, err := buildValkeyNodePodTemplateSpec(node, lbls)
	require.NoError(t, err)

	require.Len(t, pts.Spec.Containers, 1)
	assert.Equal(t, resources, pts.Spec.Containers[0].Resources, "resource requirements should pass through")
}

func TestBuildValkeyNodeConfigMap(t *testing.T) {
	node := newTestValkeyNode("mynode", "test-ns")
	cm, err := buildValkeyNodeConfigMap(node)

	require.NoError(t, err)
	assert.Equal(t, GetServerConfigMapName("mynode"), cm.Name)
	assert.Equal(t, "test-ns", cm.Namespace)
	assert.Equal(t, valkeyNodeLabels(node), cm.Labels)

	// Must contain both script keys and valkey.conf
	assert.Contains(t, cm.Data, "valkey.conf")
	assert.Contains(t, cm.Data, "liveness-check.sh")
	assert.Contains(t, cm.Data, "readiness-check.sh")
}

func TestBuildValkeyNodeConfigMap_WithPersistence(t *testing.T) {
	node := newTestValkeyNode("mynode", "test-ns")
	node.Spec.Persistence = &valkeyv1.PersistenceSpec{Size: resource.MustParse("5Gi")}

	cm, err := buildValkeyNodeConfigMap(node)
	require.NoError(t, err)
	assert.Contains(t, cm.Data["valkey.conf"], "dir /data")
	assert.Contains(t, cm.Data["valkey.conf"], "cluster-config-file /data/nodes.conf")
}

func TestBuildValkeyNodeConfigMap_WithManagedConfig(t *testing.T) {
	node := newTestValkeyNode("mynode", "test-ns")
	node.Spec.Persistence = &valkeyv1.PersistenceSpec{Size: resource.MustParse("5Gi")}
	node.Spec.UsersACLSecretName = "mynode-users"
	node.Spec.TLS = &valkeyv1.TLSConfig{
		Certificate: valkeyv1.CertificateRef{SecretName: "tls-secret"},
	}

	cm, err := buildValkeyNodeConfigMap(node)
	require.NoError(t, err)

	conf := cm.Data["valkey.conf"]
	assert.Contains(t, conf, "aclfile /config/users/users.acl")
	assert.Contains(t, conf, "dir /data")
	assert.Contains(t, conf, "cluster-config-file /data/nodes.conf")
	assert.Contains(t, conf, "tls-port 6379")
	assert.Contains(t, conf, "port 0")
	assert.Contains(t, conf, "tls-auth-clients optional")
}

func TestBuildValkeyNodePodTemplateSpec_ConfigMapNameFallback(t *testing.T) {
	t.Run("uses ServerConfigMapName when set", func(t *testing.T) {
		node := newTestValkeyNode("mynode", "test-ns") // ServerConfigMapName = "valkey-config"
		pts, err := buildValkeyNodePodTemplateSpec(node, valkeyNodeLabels(node))
		require.NoError(t, err)
		assert.Equal(t, "valkey-config", pts.Spec.Volumes[0].ConfigMap.Name)
		assert.Equal(t, "valkey-config", pts.Spec.Volumes[1].ConfigMap.Name)
	})

	t.Run("falls back to resource name when ServerConfigMapName is empty", func(t *testing.T) {
		node := newTestValkeyNode("mynode", "test-ns")
		node.Spec.ServerConfigMapName = ""
		pts, err := buildValkeyNodePodTemplateSpec(node, valkeyNodeLabels(node))
		require.NoError(t, err)
		assert.Equal(t, GetServerConfigMapName("mynode"), pts.Spec.Volumes[0].ConfigMap.Name)
		assert.Equal(t, GetServerConfigMapName("mynode"), pts.Spec.Volumes[1].ConfigMap.Name)
	})
}

func TestBuildValkeyNodePodTemplateSpec_WithPersistence(t *testing.T) {
	node := newTestValkeyNode("mynode", "test-ns")
	node.Spec.Persistence = &valkeyv1.PersistenceSpec{Size: resource.MustParse("5Gi")}

	pts, err := buildValkeyNodePodTemplateSpec(node, valkeyNodeLabels(node))
	require.NoError(t, err)

	require.Len(t, pts.Spec.Volumes, 4)
	assert.Equal(t, dataVolumeName, pts.Spec.Volumes[3].Name)
	require.NotNil(t, pts.Spec.Volumes[3].PersistentVolumeClaim)
	assert.Equal(t, "valkey-mynode-data", pts.Spec.Volumes[3].PersistentVolumeClaim.ClaimName)

	server := pts.Spec.Containers[0]
	require.Len(t, server.VolumeMounts, 4)
	assert.Equal(t, dataVolumeName, server.VolumeMounts[3].Name)
	assert.Equal(t, dataMountPath, server.VolumeMounts[3].MountPath)
}

// PersistWritableConfig=true must drop the inline emptyDir volume entry
// for the writable-config volume - the volume comes from the StatefulSet's
// VolumeClaimTemplates instead. Without this gate, the SS would carry both
// an inline Volume AND a VCT entry with the same name and the API server
// would reject the apply.
func TestBuildValkeyNodePodTemplateSpec_PersistWritableConfig_OmitsInlineEmptyDir(t *testing.T) {
	node := newTestValkeyNode("mynode", "test-ns")
	node.Spec.PersistWritableConfig = true

	pts, err := buildValkeyNodePodTemplateSpec(node, valkeyNodeLabels(node))
	require.NoError(t, err)

	for _, v := range pts.Spec.Volumes {
		assert.NotEqualf(t, writableConfigVolumeName, v.Name,
			"writable-config volume must come from VolumeClaimTemplates when PersistWritableConfig is set, not from an inline emptyDir")
	}

	// The server container must still mount the writable-config volume -
	// without the mount, valkey-server can't read its config from a
	// writable path and CONFIG REWRITE fails with "Read-only file system".
	server := pts.Spec.Containers[0]
	var mounted bool
	for _, m := range server.VolumeMounts {
		if m.Name == writableConfigVolumeName {
			mounted = true
			break
		}
	}
	assert.True(t, mounted, "server container must mount %s even when PersistWritableConfig is set", writableConfigVolumeName)
}

// PersistWritableConfig=false (the ValkeyCluster path) must keep the inline
// emptyDir volume entry so cluster-mode StatefulSets that already exist
// continue to reconcile cleanly. This is the regression test for the
// VolumeClaimTemplates immutability guard: ValkeyCluster never sets the
// flag and so never tries to retrofit a VCT entry into an existing SS.
func TestBuildValkeyNodePodTemplateSpec_DefaultUsesEmptyDirWritableConfig(t *testing.T) {
	node := newTestValkeyNode("mynode", "test-ns")
	require.False(t, node.Spec.PersistWritableConfig)

	pts, err := buildValkeyNodePodTemplateSpec(node, valkeyNodeLabels(node))
	require.NoError(t, err)

	var found *corev1.Volume
	for i := range pts.Spec.Volumes {
		if pts.Spec.Volumes[i].Name == writableConfigVolumeName {
			found = &pts.Spec.Volumes[i]
			break
		}
	}
	require.NotNil(t, found, "writable-config emptyDir volume must remain when PersistWritableConfig is unset")
	require.NotNil(t, found.EmptyDir, "writable-config volume must be an emptyDir on the legacy path")
}

// PersistWritableConfig=true must add a VolumeClaimTemplates entry named
// writableConfigVolumeName. The StatefulSet controller stamps a unique PVC
// per pod from this template; that PVC is what preserves the REPLICAOF
// directive across pod cycles.
func TestBuildValkeyNodeStatefulSet_PersistWritableConfig_AddsVCT(t *testing.T) {
	node := newTestValkeyNode("mynode", "test-ns")
	node.Spec.PersistWritableConfig = true

	ss, err := buildValkeyNodeStatefulSet(node)
	require.NoError(t, err)

	require.Len(t, ss.Spec.VolumeClaimTemplates, 1, "PersistWritableConfig must stamp exactly one VCT entry")
	vct := ss.Spec.VolumeClaimTemplates[0]
	assert.Equal(t, writableConfigVolumeName, vct.Name)
	require.Len(t, vct.Spec.AccessModes, 1)
	assert.Equal(t, corev1.ReadWriteOnce, vct.Spec.AccessModes[0])
	assert.Equal(t, writableConfigPVCSize, vct.Spec.Resources.Requests[corev1.ResourceStorage])

	// Stability: building twice must produce DeepEqual specs. Without
	// this the SS spec flaps every reconcile, bumping updateRevision
	// and tripping the rollout observer. Same guard the base
	// TestBuildValkeyNodeStatefulSet applies, repeated here so the
	// writable-config VCT path is covered.
	ss2, err := buildValkeyNodeStatefulSet(node)
	require.NoError(t, err)
	assert.True(t, equality.Semantic.DeepEqual(ss.Spec, ss2.Spec),
		"SS spec with PersistWritableConfig must be stable across builds")
}

// Default ValkeyNode (PersistWritableConfig unset, no data persistence)
// must produce a StatefulSet with no VolumeClaimTemplates at all -
// otherwise existing ValkeyCluster StatefulSets would fail to reconcile
// once this branch lands. This is the bright-line guard between the new
// behavior and ValkeyCluster's existing emptyDir-based path.
func TestBuildValkeyNodeStatefulSet_DefaultHasNoVCT(t *testing.T) {
	node := newTestValkeyNode("mynode", "test-ns")
	require.False(t, node.Spec.PersistWritableConfig)
	require.Nil(t, node.Spec.Persistence)

	ss, err := buildValkeyNodeStatefulSet(node)
	require.NoError(t, err)

	assert.Empty(t, ss.Spec.VolumeClaimTemplates,
		"default ValkeyNode must not stamp any VCT entries (ValkeyCluster relies on this for SS reconcile stability)")
}

// The config-init script must preserve a REPLICAOF directive captured in
// the previous boot's writable valkey.conf and re-append it after copying
// the fresh ConfigMap body in. Without this, a replica's pod cycle
// produces a transient master that the operator's recovery path has to
// re-wire - the multi-master window Option B is meant to eliminate.
func TestConfigInitScript_PreservesReplicaof(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(dir+"/etc-valkey", 0o755))
	require.NoError(t, os.MkdirAll(dir+"/config", 0o755))

	// Seed the writable file as if a previous CONFIG REWRITE had run -
	// it has the replicaof line we expect to survive.
	prevContents := "maxmemory 256mb\nreplicaof valkey-1.example.svc 6379\n"
	require.NoError(t, os.WriteFile(dir+"/etc-valkey/valkey.conf", []byte(prevContents), 0o644))
	// Seed the ConfigMap with a fresh body (no replicaof).
	freshContents := "maxmemory 512mb\nappendonly yes\n"
	require.NoError(t, os.WriteFile(dir+"/config/valkey.conf", []byte(freshContents), 0o644))

	// Run the init script with the in-script CONF_DIR pinned at the
	// temp dir so we don't need to touch /etc/valkey on the host.
	script := strings.Replace(configInitScript,
		"CONF_DIR="+writableConfigPath,
		"CONF_DIR="+dir+"/etc-valkey", 1)
	script = strings.Replace(script,
		"cp /config/valkey.conf",
		"cp "+dir+"/config/valkey.conf", 1)

	cmd := exec.Command("sh", "-c", script)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "init script failed: %s", string(out))

	got, err := os.ReadFile(dir + "/etc-valkey/valkey.conf")
	require.NoError(t, err)

	// Fresh ConfigMap content is present.
	assert.Contains(t, string(got), "maxmemory 512mb",
		"init script must replace the body with the fresh ConfigMap contents")
	assert.Contains(t, string(got), "appendonly yes",
		"init script must propagate every ConfigMap directive, not just maxmemory")
	// Replicaof line is preserved.
	assert.Contains(t, string(got), "replicaof valkey-1.example.svc 6379",
		"init script must re-append the previously-persisted replicaof directive so the pod boots back into its replica role")
	// The stale maxmemory value is NOT preserved - only replicaof is.
	assert.NotContains(t, string(got), "256mb",
		"init script must not preserve non-replication directives - ConfigMap is the source of truth for everything else")
}

// VolumeClaimTemplates is immutable after SS creation. The reconcile
// path must refuse to enable PersistWritableConfig on an existing SS
// that doesn't have the writable-config VCT - and must refuse the
// reverse toggle too. Without this guard the next Update would fail
// with an opaque immutable-field error from the API server.
func TestAssertWritableConfigImmutable(t *testing.T) {
	withoutVCT := &appsv1.StatefulSet{Spec: appsv1.StatefulSetSpec{}}
	withVCT := &appsv1.StatefulSet{Spec: appsv1.StatefulSetSpec{
		VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
			{ObjectMeta: metav1.ObjectMeta{Name: writableConfigVolumeName}},
		},
	}}

	t.Run("no-op when both lack the VCT", func(t *testing.T) {
		assert.NoError(t, assertWritableConfigImmutable(withoutVCT, withoutVCT))
	})
	t.Run("no-op when both carry the VCT", func(t *testing.T) {
		assert.NoError(t, assertWritableConfigImmutable(withVCT, withVCT))
	})
	t.Run("rejects enabling on existing SS", func(t *testing.T) {
		err := assertWritableConfigImmutable(withoutVCT, withVCT)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot enable writable-config persistence")
	})
	t.Run("rejects disabling on existing SS", func(t *testing.T) {
		err := assertWritableConfigImmutable(withVCT, withoutVCT)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot disable writable-config persistence")
	})
}

// First-boot path: no existing writable file. The init script must
// simply seed the file from the ConfigMap without erroring on the
// missing-replicaof case.
func TestConfigInitScript_FirstBoot(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(dir+"/etc-valkey", 0o755))
	require.NoError(t, os.MkdirAll(dir+"/config", 0o755))
	require.NoError(t, os.WriteFile(dir+"/config/valkey.conf",
		[]byte("maxmemory 256mb\n"), 0o644))

	script := strings.Replace(configInitScript,
		"CONF_DIR="+writableConfigPath,
		"CONF_DIR="+dir+"/etc-valkey", 1)
	script = strings.Replace(script,
		"cp /config/valkey.conf",
		"cp "+dir+"/config/valkey.conf", 1)

	cmd := exec.Command("sh", "-c", script)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "init script failed on first boot: %s", string(out))

	got, err := os.ReadFile(dir + "/etc-valkey/valkey.conf")
	require.NoError(t, err)
	assert.Contains(t, string(got), "maxmemory 256mb")
	assert.NotContains(t, string(got), "replicaof")
}

func TestBuildContainersDef_DefaultImage(t *testing.T) {
	node := newTestValkeyNode("mynode", "test-ns")
	node.Spec.Image = ""
	containers, err := buildContainersDef(node)
	require.NoError(t, err)
	require.Len(t, containers, 1)
	assert.Equal(t, DefaultImage, containers[0].Image)
}

func TestMergePatchContainers(t *testing.T) {
	t.Run("patch overrides image of existing container", func(t *testing.T) {
		base := []corev1.Container{{Name: "server", Image: "old:1.0"}}
		patches := []corev1.Container{{Name: "server", Image: "new:2.0"}}
		out, err := mergePatchContainers(base, patches)
		require.NoError(t, err)
		require.Len(t, out, 1)
		assert.Equal(t, "new:2.0", out[0].Image)
	})

	t.Run("unknown patch container is appended", func(t *testing.T) {
		base := []corev1.Container{{Name: "server", Image: "img:1.0"}}
		patches := []corev1.Container{{Name: "sidecar", Image: "side:1.0"}}
		out, err := mergePatchContainers(base, patches)
		require.NoError(t, err)
		require.Len(t, out, 2)
		assert.Equal(t, "server", out[0].Name)
		assert.Equal(t, "sidecar", out[1].Name)
	})

	t.Run("nil patches returns base unchanged", func(t *testing.T) {
		base := []corev1.Container{{Name: "server", Image: "img:1.0"}}
		out, err := mergePatchContainers(base, nil)
		require.NoError(t, err)
		require.Len(t, out, 1)
		assert.Equal(t, "server", out[0].Name)
	})
}

func TestBuildContainersDef_WithContainerPatches(t *testing.T) {
	node := newTestValkeyNode("mynode", "test-ns")
	node.Spec.Containers = []corev1.Container{
		{Name: "server", Image: "custom-valkey:latest"},
		{Name: "extra", Image: "busybox:latest"},
	}
	containers, err := buildContainersDef(node)
	require.NoError(t, err)

	require.Len(t, containers, 2)
	assert.Equal(t, "server", containers[0].Name)
	assert.Equal(t, "custom-valkey:latest", containers[0].Image)
	assert.Equal(t, "extra", containers[1].Name)
}

func TestBuildValkeyNodePodTemplateSpec_WithContainerPatches(t *testing.T) {
	node := newTestValkeyNode("mynode", "test-ns")
	node.Spec.Containers = []corev1.Container{
		{Name: "metrics-exporter", Image: "custom-exporter:v2.0"},
	}
	node.Spec.Exporter = valkeyv1.ExporterSpec{Enabled: true}
	pts, err := buildValkeyNodePodTemplateSpec(node, valkeyNodeLabels(node))
	require.NoError(t, err)

	require.Len(t, pts.Spec.Containers, 2)
	var exporterContainer *corev1.Container
	for i := range pts.Spec.Containers {
		if pts.Spec.Containers[i].Name == "metrics-exporter" {
			exporterContainer = &pts.Spec.Containers[i]
		}
	}
	require.NotNil(t, exporterContainer, "metrics-exporter container should exist")
	assert.Equal(t, "custom-exporter:v2.0", exporterContainer.Image)
}

func TestParseValkeyRole(t *testing.T) {
	tests := []struct {
		name     string
		info     string
		expected string
	}{
		{
			name:     "master maps to primary",
			info:     "# Replication\r\nrole:master\r\nconnected_slaves:0\r\n",
			expected: RolePrimary,
		},
		{
			name:     "slave maps to replica",
			info:     "# Replication\r\nrole:slave\r\nmaster_host:10.0.0.1\r\n",
			expected: RoleReplica,
		},
		{
			name:     "unknown role returns empty",
			info:     "# Replication\r\nrole:sentinel\r\n",
			expected: "",
		},
		{
			name:     "missing role returns empty",
			info:     "# Replication\r\nconnected_slaves:0\r\n",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, parseValkeyRole(tt.info))
		})
	}
}

func TestBuildExporterContainer(t *testing.T) {
	t.Run("default image", func(t *testing.T) {
		exporter := valkeyv1.ExporterSpec{Enabled: true}
		c := generateMetricsExporterContainerDef(exporter, "", nil)
		assert.Equal(t, DefaultExporterImage, c.Image)
		assert.Equal(t, "metrics-exporter", c.Name)
	})

	t.Run("custom image", func(t *testing.T) {
		exporter := valkeyv1.ExporterSpec{Enabled: true, Image: "custom:1.0"}
		c := generateMetricsExporterContainerDef(exporter, "", nil)
		assert.Equal(t, "custom:1.0", c.Image)
	})

	t.Run("custom resources", func(t *testing.T) {
		resources := corev1.ResourceRequirements{
			Limits: corev1.ResourceList{
				corev1.ResourceCPU: resource.MustParse("100m"),
			},
		}
		exporter := valkeyv1.ExporterSpec{Enabled: true, Resources: resources}
		c := generateMetricsExporterContainerDef(exporter, "", nil)
		assert.Equal(t, resources, c.Resources)
	})

	t.Run("args contain redis addr", func(t *testing.T) {
		exporter := valkeyv1.ExporterSpec{Enabled: true}
		c := generateMetricsExporterContainerDef(exporter, "", nil)
		require.Len(t, c.Args, 1)
		assert.Contains(t, c.Args[0], "--redis.addr=redis://localhost:6379")
		assert.Empty(t, c.VolumeMounts)
	})

	t.Run("args contain rediss addr with tls", func(t *testing.T) {
		exporter := valkeyv1.ExporterSpec{Enabled: true}
		tlsSpec := &valkeyv1.TLSConfig{Certificate: valkeyv1.CertificateRef{SecretName: "my-tls-secret"}}

		c := generateMetricsExporterContainerDef(exporter, "mycluster", tlsSpec)
		assert.Contains(t, c.Args[0], "--redis.addr=rediss://localhost:6379")
		assert.Contains(t, c.Args, fmt.Sprintf("--tls-ca-cert-file=%s/%s", tlsCertMountPath, tlsSecretKeyCA))
		assert.Len(t, c.VolumeMounts, 1)
		assert.Equal(t, tlsVolumeName, c.VolumeMounts[0].Name)
		assert.Equal(t, tlsCertMountPath, c.VolumeMounts[0].MountPath)
	})
}

func TestValkeyNodeLabels_WithClusterLabels(t *testing.T) {
	node := newTestValkeyNode("mycluster-0-0", "default")
	node.Labels = map[string]string{
		"valkey.io/cluster":     "mycluster",
		"valkey.io/shard-index": "0",
		"valkey.io/node-index":  "0",
	}
	got := valkeyNodeLabels(node)
	assert.Equal(t, "mycluster", got["valkey.io/cluster"])
	assert.Equal(t, "0", got["valkey.io/shard-index"])
	assert.Equal(t, "0", got["valkey.io/node-index"])
}

func TestValkeyNodeLabels_WithoutClusterLabels(t *testing.T) {
	node := newTestValkeyNode("standalone", "default")
	got := valkeyNodeLabels(node)
	_, hasCluster := got["valkey.io/cluster"]
	_, hasShard := got["valkey.io/shard-index"]
	_, hasNode := got["valkey.io/node-index"]
	assert.False(t, hasCluster, "should not have cluster label when not set on CR")
	assert.False(t, hasShard, "should not have shard-index label when not set on CR")
	assert.False(t, hasNode, "should not have node-index label when not set on CR")
}

func TestBuildValkeyNodePodTemplateSpec_WithACLSecret(t *testing.T) {
	node := newTestValkeyNode("mynode", "test-ns")
	node.Spec.UsersACLSecretName = "mynode-internal"
	pts, err := buildValkeyNodePodTemplateSpec(node, valkeyNodeLabels(node))
	require.NoError(t, err)

	// Volumes: scripts, valkey-conf, valkey-conf-writable, users-acl
	require.Len(t, pts.Spec.Volumes, 4)
	aclVol := pts.Spec.Volumes[3]
	assert.Equal(t, "users-acl", aclVol.Name)
	require.NotNil(t, aclVol.Secret)
	assert.Equal(t, "mynode-internal", aclVol.Secret.SecretName)

	// VolumeMounts on the server container (always Containers[0])
	c := pts.Spec.Containers[0]
	require.Len(t, c.VolumeMounts, 4)
	aclMount := c.VolumeMounts[3]
	assert.Equal(t, "users-acl", aclMount.Name)
	assert.Equal(t, "/config/users", aclMount.MountPath)
	assert.True(t, aclMount.ReadOnly)
}

func TestBuildValkeyNodePodTemplateSpec_WithoutACLSecret(t *testing.T) {
	node := newTestValkeyNode("mynode", "test-ns")
	// UsersACLSecretName is intentionally empty
	pts, err := buildValkeyNodePodTemplateSpec(node, valkeyNodeLabels(node))
	require.NoError(t, err)

	require.Len(t, pts.Spec.Volumes, 3, "should have scripts, valkey-conf, and writable-config volumes")
	require.Len(t, pts.Spec.Containers[0].VolumeMounts, 3, "should have scripts, valkey-conf, and writable-config mounts")
}

func TestLivenessCheckScript(t *testing.T) {
	scriptPath := filepath.Join("scripts", "liveness-check.sh")

	tests := []struct {
		name     string
		response string
		wantErr  bool
	}{
		{name: "pong", response: "PONG", wantErr: false},
		{name: "loading", response: "LOADING 123", wantErr: false},
		{name: "masterdown", response: "MASTERDOWN Link with MASTER is down", wantErr: false},
		{name: "error", response: "ERR something bad", wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := runProbeScript(t, scriptPath, test.response); (err != nil) != test.wantErr {
				t.Fatalf("unexpected result: err=%v wantErr=%v", err, test.wantErr)
			}
		})
	}
}

func TestReadinessCheckScript(t *testing.T) {
	scriptPath := filepath.Join("scripts", "readiness-check.sh")

	tests := []struct {
		name     string
		response string
		wantErr  bool
	}{
		{name: "pong", response: "PONG", wantErr: false},
		{name: "loading", response: "LOADING 123", wantErr: true},
		{name: "masterdown", response: "MASTERDOWN Link with MASTER is down", wantErr: true},
		{name: "error", response: "ERR something bad", wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := runProbeScript(t, scriptPath, test.response); (err != nil) != test.wantErr {
				t.Fatalf("unexpected result: err=%v wantErr=%v", err, test.wantErr)
			}
		})
	}
}

func TestBuildClusterValkeyNode_PropagatesSpecFields(t *testing.T) {
	storageClassName := "gp3"
	cluster := &valkeyv1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "mycluster", Namespace: "default"},
		Spec: valkeyv1.ValkeyClusterSpec{
			Shards:       3,
			Replicas:     1,
			Image:        "valkey/valkey:9.1.0",
			WorkloadType: valkeyv1.WorkloadTypeStatefulSet,
			Persistence: &valkeyv1.PersistenceSpec{
				Size:             resource.MustParse("10Gi"),
				StorageClassName: &storageClassName,
			},
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceMemory: resource.MustParse("512Mi"),
					corev1.ResourceCPU:    resource.MustParse("250m"),
				},
			},
			NodeSelector: map[string]string{"zone": "us-east-1a"},
			Affinity: &corev1.Affinity{
				NodeAffinity: &corev1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
						NodeSelectorTerms: []corev1.NodeSelectorTerm{
							{MatchExpressions: []corev1.NodeSelectorRequirement{
								{Key: "disktype", Operator: corev1.NodeSelectorOpIn, Values: []string{"ssd"}},
							}},
						},
					},
				},
			},
			Tolerations: []corev1.Toleration{
				{Key: "dedicated", Operator: corev1.TolerationOpEqual, Value: "valkey", Effect: corev1.TaintEffectNoSchedule},
			},
			Exporter: valkeyv1.ExporterSpec{Enabled: true},
			Containers: []corev1.Container{
				{Name: "sidecar", Image: "sidecar:latest"},
			},
		},
	}

	node := buildClusterValkeyNode(cluster, 1, 0)

	assert.Equal(t, cluster.Spec.Image, node.Spec.Image, "Image must be propagated")
	assert.Equal(t, cluster.Spec.WorkloadType, node.Spec.WorkloadType, "WorkloadType must be propagated")
	assert.Equal(t, cluster.Spec.Persistence, node.Spec.Persistence, "Persistence must be propagated")
	assert.Equal(t, cluster.Spec.Resources, node.Spec.Resources, "Resources must be propagated")
	assert.Equal(t, cluster.Spec.NodeSelector, node.Spec.NodeSelector, "NodeSelector must be propagated")
	assert.Equal(t, cluster.Spec.Affinity, node.Spec.Affinity, "Affinity must be propagated")
	assert.Equal(t, cluster.Spec.Tolerations, node.Spec.Tolerations, "Tolerations must be propagated")
	assert.Equal(t, cluster.Spec.Exporter, node.Spec.Exporter, "Exporter must be propagated")
	assert.Equal(t, cluster.Spec.Containers, node.Spec.Containers, "Containers must be propagated")
	assert.Equal(t, GetServerConfigMapName(cluster.Name), node.Spec.ServerConfigMapName, "ServerConfigMapName must match configmap name")
	assert.Equal(t, getInternalSecretName(cluster.Name), node.Spec.UsersACLSecretName, "UsersACLSecretName must match internal secret name")
}

func runProbeScript(t *testing.T, scriptPath, response string) error {
	t.Helper()

	// Stub valkey-cli so the script uses the response we provide.
	binDir := t.TempDir()
	valkeyCli := filepath.Join(binDir, "valkey-cli")
	script := []byte("#!/bin/sh\n" +
		"echo \"${VALKEY_RESPONSE:-PONG}\"\n")
	if err := os.WriteFile(valkeyCli, script, 0o755); err != nil {
		t.Fatalf("write valkey-cli stub: %v", err)
	}

	cmd := exec.Command(scriptPath)
	cmd.Env = append(os.Environ(),
		"PATH="+binDir+string(os.PathListSeparator)+os.Getenv("PATH"),
		"VALKEY_RESPONSE="+response,
	)
	return cmd.Run()
}
