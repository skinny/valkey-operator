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
	"crypto/tls"
	"fmt"
	"reflect"
	"strings"
	"time"

	vclient "github.com/valkey-io/valkey-go"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	valkeyiov1alpha1 "valkey.io/valkey-operator/api/v1alpha1"
)

const (
	// valkeyInfoRolePrefix is the key prefix in the INFO replication output.
	valkeyInfoRolePrefix = "role:"
)

// ValkeyNodeReconciler reconciles a ValkeyNode object
type ValkeyNodeReconciler struct {
	client.Client
	Scheme    *runtime.Scheme
	Recorder  events.EventRecorder
	APIReader client.Reader
}

// +kubebuilder:rbac:groups=valkey.io,resources=valkeynodes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=valkey.io,resources=valkeynodes/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=valkey.io,resources=valkeynodes/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="apps",resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="apps",resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

// Reconcile moves the current state of the ValkeyNode closer to the desired state.
func (r *ValkeyNodeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	log.V(1).Info("reconciling ValkeyNode")

	node := &valkeyiov1alpha1.ValkeyNode{}
	if err := r.Get(ctx, req.NamespacedName, node); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !node.DeletionTimestamp.IsZero() {
		return r.reconcileDeletion(ctx, node)
	}
	if requeue, err := r.reconcilePersistenceFinalizer(ctx, node); err != nil {
		return ctrl.Result{}, err
	} else if requeue {
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}
	if err := r.ensureConfigMap(ctx, node); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.ensurePersistentVolumeClaim(ctx, node); err != nil {
		return ctrl.Result{}, err
	}

	if err := r.ensureWorkload(ctx, node); err != nil {
		return ctrl.Result{}, err
	}

	if err := r.updateStatus(ctx, node); err != nil {
		return ctrl.Result{}, err
	}

	if !node.Status.Ready {
		log.V(1).Info("ValkeyNode not ready, requeuing")
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	log.V(1).Info("ValkeyNode reconciliation complete")
	// Requeue after 60 seconds to check on the ValkeyNode role.
	return ctrl.Result{RequeueAfter: 60 * time.Second}, nil
}

func (r *ValkeyNodeReconciler) ensureWorkload(ctx context.Context, node *valkeyiov1alpha1.ValkeyNode) error {
	switch node.Spec.WorkloadType {
	case valkeyiov1alpha1.WorkloadTypeStatefulSet:
		if err := r.ensureService(ctx, node); err != nil {
			return fmt.Errorf("ensure service: %w", err)
		}
		return r.ensureStatefulSet(ctx, node)
	case valkeyiov1alpha1.WorkloadTypeDeployment:
		if err := r.ensureService(ctx, node); err != nil {
			return fmt.Errorf("ensure service: %w", err)
		}
		return r.ensureDeployment(ctx, node)
	default:
		return fmt.Errorf("unsupported workload type: %q", node.Spec.WorkloadType)
	}
}

// buildPodTemplateAnnotations assembles the annotations that must be present on
// the pod template spec to trigger rolling updates when the ACL secret or the
// server config changes.
// aclSecretNameForNode returns the name of the ACL Secret to mount for
// this ValkeyNode. Prefers the explicit spec.usersACLSecretName field
// (set by parents like Valkey) and falls back to the legacy
// label-based lookup keyed on valkey.io/cluster (set by ValkeyCluster).
func aclSecretNameForNode(node *valkeyiov1alpha1.ValkeyNode) string {
	if node.Spec.UsersACLSecretName != "" {
		return node.Spec.UsersACLSecretName
	}
	return getInternalSecretName(node.Labels[LabelCluster])
}

func buildPodTemplateAnnotations(node *valkeyiov1alpha1.ValkeyNode, aclSecret *corev1.Secret) map[string]string {
	annotations := map[string]string{
		hashAnnotationKey: aclSecret.Annotations[hashAnnotationKey],
	}
	if node.Spec.ServerConfigHash != "" {
		annotations[configHashKey] = node.Spec.ServerConfigHash
	}
	return annotations
}

// ensureService creates/updates the per-ValkeyNode headless Service.
// Its name matches the StatefulSet's ServiceName so the pod's DNS
// hostname resolves through this Service. Sentinel monitors the data
// node by this hostname (NOT by pod IP), so pod restarts that change
// the IP don't leave stale `known-replica` entries in sentinel.conf.
func (r *ValkeyNodeReconciler) ensureService(ctx context.Context, node *valkeyiov1alpha1.ValkeyNode) error {
	desired := buildValkeyNodeService(node)
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{
		Name: desired.Name, Namespace: desired.Namespace,
	}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		svc.Labels = desired.Labels
		// Preserve ClusterIP (immutable after creation).
		if svc.Spec.ClusterIP == "" {
			svc.Spec.ClusterIP = desired.Spec.ClusterIP
		}
		svc.Spec.Type = desired.Spec.Type
		svc.Spec.PublishNotReadyAddresses = desired.Spec.PublishNotReadyAddresses
		svc.Spec.Selector = desired.Spec.Selector
		svc.Spec.Ports = desired.Spec.Ports
		return controllerutil.SetControllerReference(node, svc, r.Scheme)
	})
	return err
}

// ensureStatefulSet creates or updates the StatefulSet for the ValkeyNode.
func (r *ValkeyNodeReconciler) ensureStatefulSet(ctx context.Context, node *valkeyiov1alpha1.ValkeyNode) error {
	log := logf.FromContext(ctx)
	desired, err := buildValkeyNodeStatefulSet(node)
	if err != nil {
		return err
	}
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      desired.Name,
			Namespace: desired.Namespace,
		},
	}
	log.V(1).Info("getting internal secret", "node-labels", desired.Labels)
	aclSecretName := aclSecretNameForNode(node)
	aclSecret := &corev1.Secret{}
	err = r.Get(ctx, types.NamespacedName{
		Name:      aclSecretName,
		Namespace: desired.Namespace,
	}, aclSecret)
	if err != nil {
		return err
	}
	result, err := controllerutil.CreateOrUpdate(ctx, r.Client, sts, func() error {
		// VolumeClaimTemplates is immutable after SS creation. Toggling
		// PersistWritableConfig on an existing ValkeyNode would silently
		// fail at apply time; surface a clear error instead so the user
		// recreates the parent CR to switch modes.
		if !sts.CreationTimestamp.IsZero() {
			if err := assertWritableConfigImmutable(sts, desired); err != nil {
				return err
			}
		}
		sts.Labels = desired.Labels
		sts.Spec = desired.Spec
		sts.Spec.Template.Annotations = buildPodTemplateAnnotations(node, aclSecret)
		return controllerutil.SetControllerReference(node, sts, r.Scheme)
	})
	if err != nil {
		return err
	}
	log.V(1).Info("reconciled StatefulSet", "result", result, "name", sts.Name)
	return nil
}

// assertWritableConfigImmutable refuses to reconcile when the
// writable-config volume backing changed between SS create and now.
// Adding or removing a VolumeClaimTemplates entry on an existing
// StatefulSet is rejected by the API server; surfacing it as a
// reconcile error keeps the failure visible and instructs the user how
// to recover (recreate the parent CR) instead of letting the reconcile
// loop spin on opaque admission rejections.
func assertWritableConfigImmutable(existing, desired *appsv1.StatefulSet) error {
	existingHas := hasWritableConfigVCT(existing)
	desiredHas := hasWritableConfigVCT(desired)
	if existingHas == desiredHas {
		return nil
	}
	if existingHas && !desiredHas {
		return fmt.Errorf("cannot disable writable-config persistence on an existing ValkeyNode: StatefulSet.VolumeClaimTemplates is immutable. Delete and recreate the parent CR to switch modes")
	}
	return fmt.Errorf("cannot enable writable-config persistence on an existing ValkeyNode: StatefulSet.VolumeClaimTemplates is immutable. Delete and recreate the parent CR to switch modes")
}

func hasWritableConfigVCT(sts *appsv1.StatefulSet) bool {
	for _, t := range sts.Spec.VolumeClaimTemplates {
		if t.Name == writableConfigVolumeName {
			return true
		}
	}
	return false
}

// ensureDeployment creates or updates the Deployment for the ValkeyNode.
func (r *ValkeyNodeReconciler) ensureDeployment(ctx context.Context, node *valkeyiov1alpha1.ValkeyNode) error {
	log := logf.FromContext(ctx)
	desired, err := buildValkeyNodeDeployment(node)
	if err != nil {
		return err
	}
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      desired.Name,
			Namespace: desired.Namespace,
		},
	}
	log.V(1).Info("getting internal secret", "node-labels", desired.Labels)
	aclSecretName := aclSecretNameForNode(node)
	aclSecret := &corev1.Secret{}
	err = r.Get(ctx, types.NamespacedName{
		Name:      aclSecretName,
		Namespace: desired.Namespace,
	}, aclSecret)
	if err != nil {
		return err
	}

	result, err := controllerutil.CreateOrUpdate(ctx, r.Client, dep, func() error {
		dep.Labels = desired.Labels
		dep.Spec = desired.Spec
		dep.Spec.Template.Annotations = buildPodTemplateAnnotations(node, aclSecret)
		return controllerutil.SetControllerReference(node, dep, r.Scheme)
	})
	if err != nil {
		return err
	}
	log.V(1).Info("reconciled Deployment", "result", result, "name", dep.Name)
	return nil
}

// ensureConfigMap creates or updates the ConfigMap for the ValkeyNode.
// If ServerConfigMapName is set, the ConfigMap is assumed to
// be managed externally and this step is skipped.
func (r *ValkeyNodeReconciler) ensureConfigMap(ctx context.Context, node *valkeyiov1alpha1.ValkeyNode) error {
	log := logf.FromContext(ctx)
	if node.Spec.ServerConfigMapName != "" {
		// ConfigMap is provided externally (e.g. by ValkeyCluster), skip creation.
		return nil
	}
	desired, err := buildValkeyNodeConfigMap(node)
	if err != nil {
		return err
	}
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      desired.Name,
			Namespace: desired.Namespace,
		},
	}
	result, err := controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
		cm.Labels = desired.Labels
		cm.Data = desired.Data
		return controllerutil.SetControllerReference(node, cm, r.Scheme)
	})
	if err != nil {
		return err
	}
	log.V(1).Info("reconciled ConfigMap", "result", result, "name", cm.Name)
	return nil
}

// updateStatus updates the ValkeyNode status based on workload and Pod state.
func (r *ValkeyNodeReconciler) updateStatus(ctx context.Context, node *valkeyiov1alpha1.ValkeyNode) error {
	log := logf.FromContext(ctx)

	current := &valkeyiov1alpha1.ValkeyNode{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(node), current); err != nil {
		return err
	}

	// Snapshot for patch base before mutations.
	patchBase := current.DeepCopy()
	patch := client.MergeFrom(patchBase)

	// Always stamp the observed generation so ValkeyCluster can detect
	// whether the controller has processed the latest spec.
	current.Status.ObservedGeneration = current.Generation

	pvc, err := r.getPersistentVolumeClaim(ctx, node)
	if err != nil {
		return err
	}
	if node.Spec.Persistence != nil {
		pvcStatus, pvcReason, pvcMessage := pvcStatusCondition(pvc)
		meta.SetStatusCondition(&current.Status.Conditions, metav1.Condition{
			Type:               valkeyiov1alpha1.ValkeyNodeConditionPersistentVolumeClaimReady,
			Status:             pvcStatus,
			Reason:             pvcReason,
			Message:            pvcMessage,
			ObservedGeneration: current.Generation,
		})
		pvcSizeStatus, pvcSizeReason, pvcSizeMessage := pvcSizeStatusCondition(current, pvc)
		meta.SetStatusCondition(&current.Status.Conditions, metav1.Condition{
			Type:               valkeyiov1alpha1.ValkeyNodeConditionPersistentVolumeClaimSizeReady,
			Status:             pvcSizeStatus,
			Reason:             pvcSizeReason,
			Message:            pvcSizeMessage,
			ObservedGeneration: current.Generation,
		})
	} else {
		meta.RemoveStatusCondition(&current.Status.Conditions, valkeyiov1alpha1.ValkeyNodeConditionPersistentVolumeClaimReady)
		meta.RemoveStatusCondition(&current.Status.Conditions, valkeyiov1alpha1.ValkeyNodeConditionPersistentVolumeClaimSizeReady)
	}

	pod, err := r.getPod(ctx, node)
	if err != nil {
		return err
	}

	if pod == nil {
		current.Status.Ready = false
		current.Status.PodName = ""
		current.Status.PodIP = ""
		reason := valkeyiov1alpha1.ValkeyNodeReasonPodNotReady
		message := "Pod does not exist yet"
		if node.Spec.Persistence != nil {
			_, reason, message = pvcStatusCondition(pvc)
		}
		meta.SetStatusCondition(&current.Status.Conditions, metav1.Condition{
			Type:               valkeyiov1alpha1.ValkeyNodeConditionReady,
			Status:             metav1.ConditionFalse,
			Reason:             reason,
			Message:            message,
			ObservedGeneration: current.Generation,
		})
	} else {
		current.Status.PodName = pod.Name
		current.Status.PodIP = pod.Status.PodIP

		podReady := false
		for _, cond := range pod.Status.Conditions {
			if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
				podReady = true
				break
			}
		}

		// If the pod appears ready, also verify the workload rollout has completed.
		// The old pod may still be running (and ready) while the StatefulSet is rolling
		// to a new spec; we must not report Ready=true until the rollout is done so the
		// ValkeyCluster controller waits before advancing to the next node.
		if podReady {
			rolled, err := r.isWorkloadRolledOut(ctx, node)
			if err != nil {
				return err
			}
			podReady = rolled
		}

		current.Status.Ready = podReady
		if podReady {
			current.Status.Role = r.getValkeyRole(ctx, current)
			meta.SetStatusCondition(&current.Status.Conditions, metav1.Condition{
				Type:               valkeyiov1alpha1.ValkeyNodeConditionReady,
				Status:             metav1.ConditionTrue,
				Reason:             valkeyiov1alpha1.ValkeyNodeReasonPodRunning,
				Message:            "Pod is running and ready",
				ObservedGeneration: current.Generation,
			})
		} else {
			reason := valkeyiov1alpha1.ValkeyNodeReasonPodNotReady
			message := "Pod is not ready"
			if node.Spec.Persistence != nil {
				if pvcStatus, pvcReason, pvcMessage := pvcStatusCondition(pvc); pvcStatus != metav1.ConditionTrue {
					reason = pvcReason
					message = pvcMessage
				}
			}
			meta.SetStatusCondition(&current.Status.Conditions, metav1.Condition{
				Type:               valkeyiov1alpha1.ValkeyNodeConditionReady,
				Status:             metav1.ConditionFalse,
				Reason:             reason,
				Message:            message,
				ObservedGeneration: current.Generation,
			})
		}
	}

	if !reflect.DeepEqual(patchBase.Status, current.Status) {
		if err := r.Status().Patch(ctx, current, patch); err != nil {
			log.Error(err, "failed to update ValkeyNode status")
			return err
		}
		log.V(1).Info("status updated", "ready", current.Status.Ready, "role", current.Status.Role)
	} else {
		log.V(2).Info("status unchanged, skipping update")
	}

	// Sync Ready back to the caller's object so the requeue check in Reconcile
	// reflects the status we just wrote.
	node.Status.Ready = current.Status.Ready

	return nil
}

// isWorkloadRolledOut returns true if the workload (StatefulSet or Deployment)
// has fully rolled out to the current spec — pod is ready and, for the legacy
// auto-rolling case, on the latest revision.
//
// Strategy semantics:
//
//   - StatefulSet/RollingUpdate: the StatefulSet controller cycles pods on its
//     own. CurrentRevision != UpdateRevision means a rollout is in progress and
//     the operator should NOT advance to dependent work yet (cluster bring-up,
//     bootstrap, etc.). Both gates apply.
//
//   - StatefulSet/OnDelete: the operator drives pod replacement order (see
//     internal/controller/valkey_rollout.go). After a spec patch,
//     CurrentRevision != UpdateRevision is the PERSISTENT steady state until
//     the operator deletes the pod. Treating that drift as "not ready" creates
//     a deadlock: the rollout executor waits for ValkeyNode.Status.Ready, and
//     ValkeyNode.Status.Ready waits for the rollout to advance the revision.
//     So with OnDelete we only check that the SS controller has observed the
//     spec change and the pod itself is ready - revision drift is the rollout
//     executor's responsibility, not this readiness check's.
func (r *ValkeyNodeReconciler) isWorkloadRolledOut(ctx context.Context, node *valkeyiov1alpha1.ValkeyNode) (bool, error) {
	// Use APIReader (direct API server read) when available so we always see the
	// latest metadata.generation, bypassing the informer cache. Without this, the
	// same reconcile that patches the STS spec would read a stale cached object
	// where ObservedGeneration == Generation (both old) and
	// currentRevision == updateRevision (both old), causing isWorkloadRolledOut
	// to incorrectly return true before the STS controller has processed the change.
	reader := client.Reader(r.Client)
	if r.APIReader != nil {
		reader = r.APIReader
	}

	switch node.Spec.WorkloadType {
	case valkeyiov1alpha1.WorkloadTypeStatefulSet:
		sts := &appsv1.StatefulSet{}
		if err := reader.Get(ctx, client.ObjectKey{Name: valkeyNodeResourceName(node), Namespace: node.Namespace}, sts); err != nil {
			return false, client.IgnoreNotFound(err)
		}
		// Gate 1 (always): STS controller hasn't processed the latest spec change yet.
		if sts.Status.ObservedGeneration < sts.Generation {
			return false, nil
		}
		if sts.Status.ReadyReplicas < 1 {
			return false, nil
		}
		// Gate 2: revision equality is only meaningful under RollingUpdate.
		// Under OnDelete, drift is intentional and operator-managed; see the
		// function doc.
		if sts.Spec.UpdateStrategy.Type == appsv1.OnDeleteStatefulSetStrategyType {
			return true, nil
		}
		return sts.Status.CurrentRevision == sts.Status.UpdateRevision, nil
	case valkeyiov1alpha1.WorkloadTypeDeployment:
		dep := &appsv1.Deployment{}
		if err := reader.Get(ctx, client.ObjectKey{Name: valkeyNodeResourceName(node), Namespace: node.Namespace}, dep); err != nil {
			return false, client.IgnoreNotFound(err)
		}
		if dep.Status.ObservedGeneration < dep.Generation {
			return false, nil
		}
		replicas := int32(1)
		if dep.Spec.Replicas != nil {
			replicas = *dep.Spec.Replicas
		}
		return dep.Status.UpdatedReplicas >= replicas && dep.Status.ReadyReplicas >= replicas, nil
	default:
		return false, nil
	}
}

// getPod returns the pod for a ValkeyNode by listing with label selector.
func (r *ValkeyNodeReconciler) getPod(ctx context.Context, node *valkeyiov1alpha1.ValkeyNode) (*corev1.Pod, error) {
	podList := &corev1.PodList{}
	if err := r.List(ctx, podList,
		client.InNamespace(node.Namespace),
		client.MatchingLabels(valkeyNodeLabels(node))); err != nil {
		return nil, fmt.Errorf("listing pods for ValkeyNode %s: %w", node.Name, err)
	}
	if len(podList.Items) > 0 {
		return &podList.Items[0], nil
	}
	return nil, nil
}

// getValkeyRole connects to a Valkey pod and returns its replication role
// ("primary" or "replica"). Returns an empty string if the role cannot be determined.
func (r *ValkeyNodeReconciler) getValkeyRole(ctx context.Context, node *valkeyiov1alpha1.ValkeyNode) string {
	var tlsConfig *tls.Config
	if node.Spec.TLS != nil && node.Spec.TLS.Certificate.SecretName != "" {
		secretName := node.Spec.TLS.Certificate.SecretName
		serverName := ""
		if clusterName, ok := node.Labels[LabelCluster]; ok {
			serverName = fmt.Sprintf("%s.%s.svc.cluster.local", headlessServiceName(clusterName), node.Namespace)
		}

		cfg, err := getTLSConfig(ctx, r.Client, secretName, serverName, node.Namespace)
		if err == nil {
			tlsConfig = cfg
		}
	}

	// Use _operator credentials when available (cluster-managed nodes).
	// Standalone ValkeyNodes have no cluster label and no system password
	// secret, so we skip the lookup entirely and connect unauthenticated.
	var username, operatorPassword string
	if clusterName, ok := node.Labels[LabelCluster]; ok {
		operatorPassword, _ = fetchSystemUserPassword(ctx, operatorUser, r.Client, clusterName, node.Namespace)
		if operatorPassword != "" {
			username = operatorUser
		}
	}

	opt := vclient.ClientOption{
		InitAddress:       []string{fmt.Sprintf("%s:%d", node.Status.PodIP, DefaultPort)},
		ForceSingleClient: true, // Don't connect to another cluster node.
		TLSConfig:         tlsConfig,
		Username:          username,
		Password:          operatorPassword,
	}

	c, err := vclient.NewClient(opt)
	if err != nil {
		return ""
	}
	defer c.Close()

	info, err := c.Do(ctx, c.B().Info().Section("replication").Build()).ToString()
	if err != nil {
		return ""
	}

	return parseValkeyRole(info)
}

// parseValkeyRole extracts the replication role from the output of INFO replication,
// mapping Valkey's internal terms ("master"/"slave") to user-friendly ones ("primary"/"replica").
func parseValkeyRole(info string) string {
	for line := range strings.SplitSeq(info, "\n") {
		line = strings.TrimSpace(line)
		if value, ok := strings.CutPrefix(line, valkeyInfoRolePrefix); ok {
			switch value {
			case RoleMaster:
				return RolePrimary
			case RoleSlave:
				return RoleReplica
			}
		}
	}
	return ""
}

// SetupWithManager sets up the controller with the Manager.
func (r *ValkeyNodeReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.APIReader = mgr.GetAPIReader()
	return ctrl.NewControllerManagedBy(mgr).
		For(&valkeyiov1alpha1.ValkeyNode{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&appsv1.Deployment{}).
		Named("valkeynode").
		Complete(r)
}
