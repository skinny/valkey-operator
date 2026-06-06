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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SentinelPort is the canonical Valkey Sentinel TCP port.
const SentinelPort = 26379

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

func init() {
	SchemeBuilder.Register(&ValkeySentinel{}, &ValkeySentinelList{})
}
