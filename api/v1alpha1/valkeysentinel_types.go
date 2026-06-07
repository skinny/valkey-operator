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

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SentinelPort is the canonical Valkey Sentinel TCP port.
const SentinelPort = 26379

// SentinelPersistenceSpec configures durable storage for sentinel.conf
// and its rewrites. Sentinel persists known-replica lists, peer-sentinel
// handshakes, current_epoch and per-master config_epoch in this file;
// losing it on pod recreation breaks quorum-relevant state and forces a
// gossip+INFO rebuild that leaves the sentinel temporarily unable to
// vote authoritatively in elections.
//
// Default-on: when ValkeySentinelSpec.Persistence is nil, the operator
// provisions a per-pod PVC at /var/lib/sentinel using the cluster's
// default StorageClass with a small default size. To run without
// persistence (e.g. on a cluster with no default StorageClass), set
// Persistence: { Disabled: true } - sentinel pods then use a
// disk-backed emptyDir and rebuild state from gossip + INFO after any
// pod recreation.
type SentinelPersistenceSpec struct {
	// Disabled, when true, falls back to a disk-backed emptyDir. Use
	// this only on clusters without a default StorageClass or when
	// sentinel state loss across pod recreations is acceptable.
	// +optional
	Disabled bool `json:"disabled,omitempty"`

	// Size is the requested PVC size. Defaults to 100Mi when unset
	// and Disabled is false. sentinel.conf is small (a few KB per
	// monitored master) so this is conservative.
	// +optional
	Size resource.Quantity `json:"size,omitempty"`

	// StorageClassName uses the cluster's default StorageClass when nil.
	// +optional
	StorageClassName *string `json:"storageClassName,omitempty"`

	// ReclaimPolicy controls whether the managed PVCs are retained or
	// deleted when the ValkeySentinel is deleted. Defaults to Retain
	// since sentinel state is tiny and re-using an existing PVC
	// during pod re-creation is the entire reason for the PVC.
	// +kubebuilder:default=Retain
	// +optional
	ReclaimPolicy PersistenceReclaimPolicy `json:"reclaimPolicy,omitempty"`
}

// ValkeySentinelSpec defines the desired state of a ValkeySentinel.
//
// A ValkeySentinel runs a standalone set of Valkey Sentinel processes
// that monitor every Valkey in the same namespace whose labels match
// `spec.valkeySelector`. The link is one-way: sentinels pick their
// targets; the data side is unaware.
// +kubebuilder:validation:XValidation:rule="!has(self.quorum) || self.quorum <= self.replicas",message="quorum must be <= replicas"
type ValkeySentinelSpec struct {
	// Image overrides the container image used for the sentinel pods.
	// +optional
	Image string `json:"image,omitempty"`

	// Replicas is the number of sentinel pods. Minimum is 3 so a single
	// pod loss still leaves a quorum.
	// +kubebuilder:validation:Minimum=3
	Replicas int32 `json:"replicas"`

	// ValkeySelector picks the Valkey resources this sentinel set will
	// monitor. Same namespace only. An empty selector matches no
	// Valkeys (use an explicit `matchLabels` / `matchExpressions`).
	ValkeySelector metav1.LabelSelector `json:"valkeySelector"`

	// Quorum is the number of sentinels that must agree the primary is
	// subjectively down before a failover is triggered. Defaults to
	// floor(replicas/2)+1 (a strict majority). Must be ≤ replicas.
	// +kubebuilder:validation:Minimum=1
	// +optional
	Quorum *int32 `json:"quorum,omitempty"`

	// Config is passthrough configuration applied via
	// `SENTINEL SET <master> <key> <value>` to every monitored master.
	// Common keys: `down-after-milliseconds`, `failover-timeout`,
	// `parallel-syncs`. The operator does not validate option names.
	// +optional
	Config map[string]string `json:"config,omitempty"`

	// Resources defines the resource requirements for the sentinel
	// container. Sentinels are lightweight; defaults are intentionally low.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// Tolerations applied to every sentinel pod.
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`

	// NodeSelector applied to every sentinel pod.
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// Affinity applied to every sentinel pod. Anti-affine the set across
	// hosts or zones so a node failure does not take the whole set down.
	// +optional
	Affinity *corev1.Affinity `json:"affinity,omitempty"`

	// Containers patches the pod's container list via strategic merge.
	// +optional
	Containers []corev1.Container `json:"containers,omitempty"`

	// PodDisruptionBudget controls whether the operator manages a PDB.
	// +kubebuilder:default=Managed
	// +optional
	PodDisruptionBudget PDBPolicy `json:"podDisruptionBudget,omitempty"`

	// Persistence configures durable storage for sentinel.conf. The
	// operator provisions a small per-pod PVC by default; see
	// SentinelPersistenceSpec for the rationale and how to opt out.
	// +optional
	Persistence *SentinelPersistenceSpec `json:"persistence,omitempty"`

	// TLS configuration for the sentinel port. Currently not
	// implemented; setting this produces a non-functional set.
	// +optional
	TLS *TLSConfig `json:"tls,omitempty"`
}

// ValkeySentinelStatus defines the observed state of a ValkeySentinel.
type ValkeySentinelStatus struct {
	// State summarises overall health.
	// +kubebuilder:default=Initializing
	// +optional
	State ClusterState `json:"state,omitempty"`

	// Reason is a brief machine-readable explanation of the current state.
	// +optional
	Reason string `json:"reason,omitempty"`

	// Message is a human-readable explanation of the current state.
	// +optional
	Message string `json:"message,omitempty"`

	// ReadyReplicas is the number of sentinel pods reporting ready.
	// +kubebuilder:default=0
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// Monitored lists the Valkeys currently being monitored, intersected
	// from the live `SENTINEL masters` output and the selector match.
	// Sorted.
	// +listType=set
	// +optional
	Monitored []string `json:"monitored,omitempty"`

	// Endpoints is the per-pod sentinel address list, ordered by pod
	// index. Sentinel-aware clients accept a list and try each in
	// order, so surfacing all of them avoids a hard dependency on any
	// single sentinel being reachable for the bootstrap connect. The
	// list reflects the desired replica count; entries appear before
	// the matching pod is necessarily Ready, since the StatefulSet's
	// DNS exists from creation.
	// +optional
	Endpoints []Endpoint `json:"endpoints,omitempty"`

	// Conditions exposes detailed state.
	// Standard types: Ready, Progressing.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=vks

// ValkeySentinel is the Schema for the valkeysentinels API.
// +kubebuilder:printcolumn:name="State",type="string",JSONPath=".status.state"
// +kubebuilder:printcolumn:name="Replicas",type="integer",JSONPath=".spec.replicas"
// +kubebuilder:printcolumn:name="Ready",type="integer",JSONPath=".status.readyReplicas",priority=1
// +kubebuilder:printcolumn:name="Monitored",type="string",JSONPath=".status.monitored[*]",description="Valkeys currently monitored by this sentinel set"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
type ValkeySentinel struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// +required
	Spec ValkeySentinelSpec `json:"spec"`

	// +kubebuilder:default:={state: "Initializing", readyReplicas: 0}
	// +optional
	Status ValkeySentinelStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// ValkeySentinelList contains a list of ValkeySentinel.
type ValkeySentinelList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []ValkeySentinel `json:"items"`
}

// EffectiveQuorum returns the configured quorum or the floor(replicas/2)+1 default.
func (s *ValkeySentinelSpec) EffectiveQuorum() int32 {
	if s.Quorum != nil {
		return *s.Quorum
	}
	return s.Replicas/2 + 1
}

// EffectivePersistence returns the resolved SentinelPersistenceSpec
// with defaults applied. Sentinel is designed to recover its state on
// restart - monitor and auth are baked into the rendered ConfigMap,
// known-replicas/sentinels are re-discovered via INFO + pubsub within
// seconds - so persistence is opt-in:
//
//   - Persistence nil or Disabled=true -> (nil, false), use emptyDir.
//   - Persistence set (any non-Disabled value) -> (spec, true), PVC
//     with Size defaulting to 100Mi.
//
// Flipping the default to off avoids a cluster-scoped StorageClass
// dependency and the matching RBAC grant the operator would otherwise
// need just to surface a precondition warning.
func (s *ValkeySentinelSpec) EffectivePersistence() (*SentinelPersistenceSpec, bool) {
	if s.Persistence == nil || s.Persistence.Disabled {
		return nil, false
	}
	resolved := *s.Persistence
	if resolved.Size.IsZero() {
		resolved.Size = resource.MustParse("100Mi")
	}
	if resolved.ReclaimPolicy == "" {
		resolved.ReclaimPolicy = PersistenceReclaimPolicyRetain
	}
	return &resolved, true
}

func init() {
	SchemeBuilder.Register(&ValkeySentinel{}, &ValkeySentinelList{})
}
