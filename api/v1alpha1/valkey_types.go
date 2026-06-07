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

// ValkeySpec defines the desired state of Valkey.
//
// A Valkey is a non-sharded primary+replicas (or standalone) data plane.
// Failover is delegated to a separately-managed ValkeySentinel that
// selects this Valkey via labels; this CRD has no sentinelRef.
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.persistence) || has(self.persistence)",message="persistence cannot be removed once set"
// +kubebuilder:validation:XValidation:rule="!has(self.persistence) || !has(oldSelf.persistence) || quantity(self.persistence.size).compareTo(quantity(oldSelf.persistence.size)) >= 0",message="persistence.size may only be expanded"
// +kubebuilder:validation:XValidation:rule="!has(self.persistence) || !has(oldSelf.persistence) || ((!has(self.persistence.storageClassName) && !has(oldSelf.persistence.storageClassName)) || (has(self.persistence.storageClassName) && has(oldSelf.persistence.storageClassName) && self.persistence.storageClassName == oldSelf.persistence.storageClassName))",message="persistence.storageClassName is immutable"
type ValkeySpec struct {
	// Image overrides the default Valkey container image.
	// +optional
	Image string `json:"image,omitempty"`

	// Replicas is the number of replicas (not counting the primary).
	// 0 = standalone (one pod, no replication). N > 0 = one primary plus
	// N replicas. Matches the semantics of ValkeyCluster.spec.replicas.
	// +kubebuilder:validation:Minimum=0
	Replicas int32 `json:"replicas"`

	// Resources defines the resource requirements for the Valkey container.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// Tolerations applied to every Valkey pod.
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`

	// NodeSelector applied to every Valkey pod.
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// Affinity applied to every Valkey pod.
	// +optional
	Affinity *corev1.Affinity `json:"affinity,omitempty"`

	// Exporter configures the optional metrics-exporter sidecar.
	// +kubebuilder:default:={enabled:true}
	// +optional
	Exporter ExporterSpec `json:"exporter,omitempty"`

	// Persistence defines durable storage propagated to each ValkeyNode.
	// +optional
	Persistence *PersistenceSpec `json:"persistence,omitempty"`

	// Users and ACL definitions; matches ValkeyCluster.spec.users.
	// +listType=map
	// +listMapKey=name
	// +optional
	Users []UserAclSpec `json:"users,omitempty"`

	// Containers patches the default container list via strategic merge.
	// +optional
	Containers []corev1.Container `json:"containers,omitempty"`

	// Config is passthrough configuration written verbatim into
	// `valkey.conf`. The operator does not validate option values,
	// but it does reject a small set of keys the operator itself
	// owns (port/dir/bind/etc.) - setting those would either fight
	// the rendered config or break the data path.
	// +optional
	// +kubebuilder:validation:XValidation:rule="!self.exists(k, k in ['port','tls-port','bind','dir','logfile','dbfilename','pidfile','unixsocket','aclfile','include'])",message="config keys port, tls-port, bind, dir, logfile, dbfilename, pidfile, unixsocket, aclfile, include are owned by the operator and cannot be overridden"
	Config map[string]string `json:"config,omitempty"`

	// TLS configuration for the data port.
	// +optional
	TLS *TLSConfig `json:"tls,omitempty"`

	// PodDisruptionBudget controls whether the operator manages a PDB.
	// +kubebuilder:default=Managed
	// +optional
	PodDisruptionBudget PDBPolicy `json:"podDisruptionBudget,omitempty"`
}

// Endpoint is a host:port pair observed by the operator. Host is a
// resolvable DNS name (a per-pod Service in the same namespace as the
// operator); Sentinel monitors data nodes by this name so pod-IP
// changes across restarts don't leave stale `known-replica` entries
// in sentinel.conf.
type Endpoint struct {
	// Host is a DNS name resolvable inside the cluster.
	Host string `json:"host"`
	// Port is the TCP port.
	Port int32 `json:"port"`
}

// RolloutPhase enumerates the stages of an operator-driven, zero-data-loss
// rolling restart triggered when the Valkey spec changes.
// +kubebuilder:validation:Enum=Idle;RollingReplicas;Failover;RollingPrimary
type RolloutPhase string

const (
	// RolloutPhaseIdle: no pod template change pending; no rollout
	// in progress.
	RolloutPhaseIdle RolloutPhase = "Idle"
	// RolloutPhaseRollingReplicas: rolling non-primary pods one at a
	// time, waiting for each to come back as a caught-up replica.
	RolloutPhaseRollingReplicas RolloutPhase = "RollingReplicas"
	// RolloutPhaseFailover: planned handoff in progress - replicas
	// are all on the new spec; the operator is promoting one of them
	// so the old primary can be rolled.
	RolloutPhaseFailover RolloutPhase = "Failover"
	// RolloutPhaseRollingPrimary: the former primary is being
	// rolled as the last step. It rejoins as a replica of the new
	// primary.
	RolloutPhaseRollingPrimary RolloutPhase = "RollingPrimary"
)

// RolloutStatus describes operator-driven rolling-restart progress.
type RolloutStatus struct {
	// Phase is the current step of the rollout state machine.
	// +optional
	Phase RolloutPhase `json:"phase,omitempty"`

	// PendingNodes lists ValkeyNodes whose pod still runs the previous
	// template revision and therefore need to be cycled. Sorted by node
	// index, primary last.
	// +listType=set
	// +optional
	PendingNodes []string `json:"pendingNodes,omitempty"`

	// FailoverIssuedAt records the last time the rollout state machine
	// issued a SENTINEL FAILOVER (or operator-driven equivalent). Used
	// to debounce re-issuance while Sentinel is still working on the
	// promotion - without this the operator pokes Sentinel on every
	// reconcile, producing the +sentinel/+sdown event spam observed
	// during long failovers.
	// +optional
	FailoverIssuedAt *metav1.Time `json:"failoverIssuedAt,omitempty"`
}

// ValkeyStatus defines the observed state of Valkey.
type ValkeyStatus struct {
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

	// PrimaryPodName is the name of the pod currently reporting
	// `role:master`. Empty when no primary is known.
	// +optional
	PrimaryPodName string `json:"primaryPodName,omitempty"`

	// PrimaryEndpoint is the network endpoint of the current primary,
	// observed by the operator via INFO replication. Used by selecting
	// ValkeySentinels so they can render `sentinel monitor` lines into
	// their ConfigMap without re-probing every reconcile. Empty until
	// the primary is observed.
	// +optional
	PrimaryEndpoint *Endpoint `json:"primaryEndpoint,omitempty"`

	// Rollout exposes the state of any in-progress operator-driven
	// rolling restart. Phase=Idle (or empty) means no rollout is in
	// flight; the StatefulSets are using OnDelete updateStrategy so
	// pods only restart when the operator deletes them. See the
	// ConditionRolling condition for the user-facing summary.
	// +optional
	Rollout *RolloutStatus `json:"rollout,omitempty"`

	// ReadyReplicas is the number of pods other than the primary that
	// report ready.
	// +kubebuilder:default=0
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// MonitoredBy lists the ValkeySentinels in this namespace whose
	// selector matches this Valkey's labels. Observed, not declared.
	// +listType=set
	// +optional
	MonitoredBy []string `json:"monitoredBy,omitempty"`

	// Conditions exposes detailed state.
	// Standard types: Ready, Progressing, Degraded, PrimaryElected, Monitored.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=vk

// Valkey is the Schema for the valkeys API.
// +kubebuilder:printcolumn:name="State",type="string",JSONPath=".status.state"
// +kubebuilder:printcolumn:name="Replicas",type="integer",JSONPath=".spec.replicas"
// +kubebuilder:printcolumn:name="Primary",type="string",JSONPath=".status.primaryPodName",priority=1
// +kubebuilder:printcolumn:name="Sentinels",type="string",JSONPath=".status.monitoredBy[*]",description="ValkeySentinels currently monitoring this Valkey (empty = no failover)"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
type Valkey struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// +required
	Spec ValkeySpec `json:"spec"`

	// +kubebuilder:default:={state: "Initializing", readyReplicas: 0}
	// +optional
	Status ValkeyStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// ValkeyList contains a list of Valkey.
type ValkeyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []Valkey `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Valkey{}, &ValkeyList{})
}
