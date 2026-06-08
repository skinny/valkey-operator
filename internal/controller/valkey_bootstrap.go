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
	"fmt"
	"strings"

	"github.com/go-logr/logr"
	vclient "github.com/valkey-io/valkey-go"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	valkeyiov1alpha1 "valkey.io/valkey-operator/api/v1alpha1"
)

// bootstrapReplication is the one-shot initial election: promote
// node-0 as the primary, persist the role to disk, set
// ConditionBootstrapped=True. Gated on Bootstrapped, so this is a
// no-op once the cluster has been elected.
//
// Primary-only: only waits for node-0 to be Ready. Replicas trickle
// in through wireScaledUpReplicas as they come up, using the same
// dbsize=0 safety the scale-up path uses - the operator never wires
// a node that already has data, which is what saves us in the
// pod-restart-as-master case AND in the catastrophic-loss case
// (every pod restarted with persistence still bearing data).
//
// Sentinel, when monitoring, is the authority on topology from
// Bootstrapped=True onwards. The operator does not fight a
// sentinel-initiated failover by re-wiring.
func (r *ValkeyReconciler) bootstrapReplication(ctx context.Context, valkey *valkeyiov1alpha1.Valkey, nodes *valkeyiov1alpha1.ValkeyNodeList) error {
	if meta.IsStatusConditionTrue(valkey.Status.Conditions, valkeyiov1alpha1.ConditionBootstrapped) {
		return nil
	}
	primary := findPrimaryNode(nodes)
	if primary == nil || primary.Status.PodIP == "" || !primary.Status.Ready {
		return nil // wait for node-0 to come up
	}

	log := logf.FromContext(ctx)
	operatorPassword, err := fetchSystemUserPassword(ctx, operatorUser, r.Client, valkey.Name, valkey.Namespace)
	if err != nil {
		return fmt.Errorf("fetch operator password: %w", err)
	}

	log.Info("bootstrapping replication", "primary", primary.Name)

	// Stamp Status.PrimaryPodName / Endpoint FIRST, before any wire
	// call. The chicken-and-egg this avoids: at boot, every data pod
	// reports role:master (Valkey default). observePrimary needs a
	// previousPrimary to break a >1-master tie; without one it
	// returns ambiguous, Status stays empty, wireScaledUpReplicas is
	// gated on Status.PrimaryPodName so it never runs, sentinel sees
	// no PrimaryEndpoint so it defers SS creation, and the whole
	// thing wedges. By writing Status.PrimaryPodName here, the
	// same-reconcile observePrimary captures it as previousPrimary
	// and stabilises on node-0, wireScaledUpReplicas REPLICAOFs the
	// impostor masters back to node-0, and the cluster converges.
	//
	// Set BEFORE the wire calls (REPLICAOF NO ONE + CONFIG REWRITE)
	// so a transient wire failure doesn't strand Status empty -
	// node-0 is the operator's deterministic primary choice (lowest
	// node index when Spec.Replicas > 0), independent of whether the
	// wire commands have landed yet.
	valkey.Status.PrimaryPodName = primary.Status.PodName
	valkey.Status.PrimaryEndpoint = &valkeyiov1alpha1.Endpoint{
		Host: valkeyNodeServiceHost(primary),
		Port: int32(DefaultPort),
	}

	// Skip REPLICAOF NO ONE when node-0 already reports role:master to
	// avoid an unnecessary CONFIG rewrite on every reconcile.
	info, err := infoReplication(ctx, primary.Status.PodIP, operatorPassword)
	if err != nil || strings.TrimSpace(info["role"]) != RoleMaster {
		if err := withDataClient(primary.Status.PodIP, operatorPassword, func(c vclient.Client) error {
			return c.Do(ctx, c.B().Replicaof().No().One().Build()).Error()
		}); err != nil {
			return fmt.Errorf("promote primary: %w", err)
		}
	}
	// Persist node-0's role to disk so it comes back as primary after
	// a restart instead of inheriting whatever was in the read-only
	// ConfigMap. Replicas are wired by wireScaledUpReplicas on
	// subsequent reconciles; their CONFIG REWRITE happens there.
	configRewriteAll(ctx, log, operatorPassword, []*valkeyiov1alpha1.ValkeyNode{primary})

	r.Recorder.Eventf(valkey, primary, corev1.EventTypeNormal, "ReplicationBootstrapped", "Bootstrap",
		"Promoted %s as primary; replicas will wire as they become Ready", primary.Name)
	r.setCondition(valkey, valkeyiov1alpha1.ConditionBootstrapped, "BootstrapComplete",
		fmt.Sprintf("Primary elected: %s; replicas wired through scale-up path", primary.Name),
		metav1.ConditionTrue)
	return nil
}

// configRewriteAll issues `CONFIG REWRITE` on each node's pod IP using
// the operator credentials, logging but not erroring on per-node
// failures. The caller already ensured the runtime state is correct;
// CONFIG REWRITE just persists it to disk so the role survives a pod
// restart. A failure here means "this pod will come back unwired if it
// restarts before the next reconcile", which the next reconcile cleans
// up via wireScaledUpReplicas. Hard-erroring would mask the real win
// from the rest of the bootstrap.
//
// Callers that need disk state guaranteed (the new primary and the
// demoted ex-primary in operatorDrivenFailover) use configRewriteOrFail
// instead - those two pods restarting with stale on-disk roles is
// what produces post-failover split-brain.
func configRewriteAll(ctx context.Context, log logr.Logger, password string, nodes []*valkeyiov1alpha1.ValkeyNode) {
	for _, n := range nodes {
		if n == nil || n.Status.PodIP == "" {
			continue
		}
		if err := configRewriteOne(ctx, n.Status.PodIP, password); err != nil {
			log.V(1).Info("CONFIG REWRITE failed; persistence of role will retry next reconcile",
				"node", n.Name, "err", err)
		}
	}
}

// configRewriteOrFail issues CONFIG REWRITE and returns the error.
// Use for nodes whose on-disk role MUST match runtime before the
// next pod restart - typically the new primary and the demoted
// ex-primary right after operatorDrivenFailover.
func configRewriteOrFail(ctx context.Context, password string, n *valkeyiov1alpha1.ValkeyNode) error {
	if n == nil || n.Status.PodIP == "" {
		return fmt.Errorf("CONFIG REWRITE on %v: pod IP unknown", n)
	}
	if err := configRewriteOne(ctx, n.Status.PodIP, password); err != nil {
		return fmt.Errorf("CONFIG REWRITE on %s (%s): %w", n.Name, n.Status.PodIP, err)
	}
	return nil
}

func configRewriteOne(ctx context.Context, ip, password string) error {
	return withDataClient(ip, password, func(c vclient.Client) error {
		return c.Do(ctx, c.B().ConfigRewrite().Build()).Error()
	})
}

// wireScaledUpReplicas handles every case where a data node is Ready
// and reports role:master but is NOT the cluster primary. Three paths
// share this code:
//
//   - Initial bootstrap replicas: bootstrapReplication only promotes
//     node-0 and sets ConditionBootstrapped. The other ValkeyNodes
//     come up as masters by Valkey default; this function REPLICAOFs
//     each one to node-0 on subsequent reconciles.
//   - Scale-up: spec.Replicas bumped, new ValkeyNode created, its pod
//     starts as master. Same shape as bootstrap replicas.
//   - Replica pod restart that lost its persisted role: a previously-
//     wired replica was cycled (rollout, eviction, node drain), the
//     writable config is emptyDir on the data pods so CONFIG REWRITE
//     didn't survive recreation, and the pod boots as master. Same
//     shape again.
//
// Safety invariant: only wire pods whose dbsize is 0. A non-primary
// master with data is either a split-brain (rogue master with real
// writes) or a deliberate manual change; in either case the operator
// must not REPLICAOF it - that would DISCARD the data on first sync
// from the new primary. observePrimary catches the split-brain shape
// and deriveConditions reports it as Degraded for human resolution.
// This is what makes the unified code safe for the
// catastrophic-loss case the now-removed replicationCollapsed used
// to handle separately: if every pod restarted empty, all the
// non-primary masters have dbsize=0, get REPLICAOF'd to node-0, and
// the cluster recovers without any "all-master-no-slaves" detection.
// If pods restarted with data (persistence enabled), dbsize > 0
// blocks the wire, and the user sees Degraded with the split-brain
// reason instead of silent data destruction.
func (r *ValkeyReconciler) wireScaledUpReplicas(ctx context.Context, valkey *valkeyiov1alpha1.Valkey, nodes *valkeyiov1alpha1.ValkeyNodeList) error {
	log := logf.FromContext(ctx)
	if valkey.Spec.Replicas == 0 {
		return nil
	}
	if !meta.IsStatusConditionTrue(valkey.Status.Conditions, valkeyiov1alpha1.ConditionBootstrapped) {
		return nil
	}
	if valkey.Status.PrimaryPodName == "" || valkey.Status.PrimaryEndpoint == nil {
		return nil
	}
	// Find the primary node by pod name (the operator's canonical
	// primary identity).
	var primary *valkeyiov1alpha1.ValkeyNode
	for i := range nodes.Items {
		n := &nodes.Items[i]
		if n.Status.PodName == valkey.Status.PrimaryPodName {
			primary = n
			break
		}
	}
	if primary == nil || primary.Status.PodIP == "" {
		return nil
	}
	password, err := fetchSystemUserPassword(ctx, operatorUser, r.Client, valkey.Name, valkey.Namespace)
	if err != nil {
		return fmt.Errorf("fetch operator password: %w", err)
	}
	// REPLICAOF against the master's stable per-pod headless Service
	// DNS, NOT primary.Status.PodIP. CONFIG REWRITE persists the
	// directive into the writable valkey.conf; using a pod IP would
	// strand replicas with master_link_status:down the moment the
	// master's pod is ever recreated. The DNS name is published by
	// the per-ValkeyNode Service and re-resolved by Valkey on every
	// reconnect.
	primaryHost := valkeyNodeServiceHost(primary)
	for i := range nodes.Items {
		n := &nodes.Items[i]
		if n == primary || !n.Status.Ready || n.Status.PodIP == "" {
			continue
		}
		info, err := infoReplication(ctx, n.Status.PodIP, password)
		if err != nil {
			continue
		}
		// Self-healing for replicas with a persisted IP-based
		// replicaof pointing at a stale pod IP (this branch's
		// earlier wire behavior). master_host is whatever was
		// passed to REPLICAOF; comparing it to the stable DNS lets
		// us re-point ONLY when the persisted directive disagrees,
		// avoiding event spam during the normal "link briefly down
		// during reconnect" window.
		if strings.TrimSpace(info["role"]) == RoleSlave {
			masterHost := strings.TrimSpace(info["master_host"])
			if masterHost == primaryHost {
				continue
			}
			log.V(1).Info("repointing replica to stable DNS",
				"node", n.Name, "primary", primary.Name, "from", masterHost, "to", primaryHost)
			if err := withDataClient(n.Status.PodIP, password, func(c vclient.Client) error {
				return c.Do(ctx, c.B().Replicaof().Host(primaryHost).Port(int64(DefaultPort)).Build()).Error()
			}); err != nil {
				return fmt.Errorf("heal REPLICAOF on %s: %w", n.Status.PodIP, err)
			}
			configRewriteAll(ctx, log, password, []*valkeyiov1alpha1.ValkeyNode{n})
			r.Recorder.Eventf(valkey, n, corev1.EventTypeNormal, "ReplicaRepointed", "Heal",
				"Repointed %s at %s (was %s)", n.Name, primaryHost, masterHost)
			continue
		}
		if strings.TrimSpace(info["role"]) != RoleMaster {
			continue
		}
		// A pod restoring its RDB/AOF on startup reports dbsize=0
		// until load completes. REPLICAOF-ing during load would
		// have the primary stream over the loading data; defer
		// until the load finishes.
		if persistenceLoading(ctx, n.Status.PodIP, password) {
			log.V(1).Info("scale-up wiring: pod still loading persistence; deferring", "node", n.Name)
			continue
		}
		// Lonely master detected. Inspect dbsize to disambiguate
		// scale-up (empty) from rogue-master-with-data (non-empty).
		size, err := dbsize(ctx, n.Status.PodIP, password)
		if err != nil {
			log.V(1).Info("scale-up wiring: dbsize probe failed; will retry", "node", n.Name, "err", err)
			continue
		}
		if size > 0 {
			// Non-empty rogue master. Leave it for deriveConditions
			// to surface as Degraded; never REPLICAOF a node with
			// data, that would discard it on first sync.
			continue
		}
		if err := withDataClient(n.Status.PodIP, password, func(c vclient.Client) error {
			return c.Do(ctx, c.B().Replicaof().Host(primaryHost).Port(int64(DefaultPort)).Build()).Error()
		}); err != nil {
			return fmt.Errorf("scale-up REPLICAOF on %s: %w", n.Status.PodIP, err)
		}
		configRewriteAll(ctx, log, password, []*valkeyiov1alpha1.ValkeyNode{n})
		log.Info("scale-up: wired new replica", "node", n.Name, "primary", primary.Name)
		r.Recorder.Eventf(valkey, n, corev1.EventTypeNormal, "ReplicaWired", "ScaleUp",
			"Wired %s as a replica of %s after scale-up", n.Name, primary.Name)
	}
	return nil
}

// dbsize queries `DBSIZE` on the given pod and returns the key count.
// Used by wireScaledUpReplicas to distinguish an empty fresh master
// (safe to REPLICAOF) from a rogue master with data (refuse).
func dbsize(ctx context.Context, ip, password string) (int64, error) {
	var size int64
	err := withDataClient(ip, password, func(c vclient.Client) error {
		resp := c.Do(ctx, c.B().Dbsize().Build())
		n, err := resp.AsInt64()
		if err != nil {
			return err
		}
		size = n
		return nil
	})
	return size, err
}

// findPrimaryNode returns the ValkeyNode whose LabelNodeIndex is "0",
// the operator's deterministic primary slot. Returns nil if no such
// node exists yet.
func findPrimaryNode(nodes *valkeyiov1alpha1.ValkeyNodeList) *valkeyiov1alpha1.ValkeyNode {
	for i := range nodes.Items {
		n := &nodes.Items[i]
		if n.Labels[LabelNodeIndex] == "0" {
			return n
		}
	}
	return nil
}

// withDataClient opens an operator-authenticated client to ip:6379,
// runs fn, and closes the client.
func withDataClient(ip, password string, fn func(vclient.Client) error) error {
	opt := vclient.ClientOption{
		InitAddress:       []string{fmt.Sprintf("%s:%d", ip, DefaultPort)},
		ForceSingleClient: true,
		Username:          operatorUser,
		Password:          password,
	}
	c, err := vclient.NewClient(opt)
	if err != nil {
		return fmt.Errorf("connect %s:%d: %w", ip, DefaultPort, err)
	}
	defer c.Close()
	return fn(c)
}

// infoReplication asks ip:6379 for `INFO replication` and parses the
// key:value pairs into a map.
func infoReplication(ctx context.Context, ip, password string) (map[string]string, error) {
	return infoSection(ctx, ip, password, "replication")
}

// persistenceLoading reports whether the node is currently restoring
// an RDB/AOF on startup. wireScaledUpReplicas gates on this so we
// don't REPLICAOF a pod mid-load (the primary would stream over the
// loading dataset). A probe error returns false so the loop falls
// through to the existing dbsize check; the worst case is one
// best-effort wire that the data plane will refuse.
func persistenceLoading(ctx context.Context, ip, password string) bool {
	m, err := infoSection(ctx, ip, password, "persistence")
	if err != nil {
		return false
	}
	return strings.TrimSpace(m["loading"]) == "1"
}

func infoSection(ctx context.Context, ip, password, section string) (map[string]string, error) {
	out := map[string]string{}
	err := withDataClient(ip, password, func(c vclient.Client) error {
		raw, err := c.Do(ctx, c.B().Info().Section(section).Build()).ToString()
		if err != nil {
			return err
		}
		for line := range strings.SplitSeq(raw, "\n") {
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			if k, v, ok := strings.Cut(line, ":"); ok {
				out[k] = v
			}
		}
		return nil
	})
	return out, err
}
