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
	"strconv"
	"strings"

	vclient "github.com/valkey-io/valkey-go"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	valkeyiov1alpha1 "valkey.io/valkey-operator/api/v1alpha1"
)

// bootstrapReplication performs the initial REPLICAOF wiring: node-0
// becomes primary, every other node REPLICAOFs node-0. Once it has
// succeeded the Bootstrapped condition is set and this function is a
// no-op while replication is in any healthy state. Sentinel (if
// monitoring) is the authority on topology from then on - the operator
// does not fight a sentinel-initiated failover by re-wiring.
//
// Re-bootstrap exception: if Bootstrapped is True but every data pod
// reports `role:master` with no `connected_slaves`, the cluster has
// suffered catastrophic loss (e.g. every pod restarted without
// persistence). For Valkeys without persistence the re-bootstrap is
// safe (there is no data to overwrite); for Valkeys with persistence
// it is not, so we surface a Degraded condition instead and require
// the operator to intervene.
func (r *ValkeyReconciler) bootstrapReplication(ctx context.Context, valkey *valkeyiov1alpha1.Valkey, nodes *valkeyiov1alpha1.ValkeyNodeList) error {
	log := logf.FromContext(ctx)

	bootstrapped := meta.IsStatusConditionTrue(valkey.Status.Conditions, valkeyiov1alpha1.ConditionBootstrapped)
	if bootstrapped {
		needsRebootstrap, err := r.replicationCollapsed(ctx, valkey, nodes)
		if err != nil {
			return fmt.Errorf("check replication state: %w", err)
		}
		if !needsRebootstrap {
			return nil
		}
		if valkey.Spec.Persistence != nil {
			r.Recorder.Eventf(valkey, nil, "Warning", "ReplicationLost", "Bootstrap",
				"All data pods report role:master with no replicas connected, but persistence is enabled; refusing to auto-rebootstrap (would discard replica RDBs). Manual intervention required.")
			r.setCondition(valkey, valkeyiov1alpha1.ConditionDegraded,
				"ReplicationLostWithPersistence",
				"All data pods restarted with no replication wired; re-running bootstrap would discard the replicas' on-disk data. Clear status.conditions[Bootstrapped] to opt in to auto-rebootstrap, or wire REPLICAOF manually.",
				metav1.ConditionTrue)
			return nil
		}
		log.Info("re-bootstrapping after replication collapse (no persistence; safe to rewire)")
		r.Recorder.Eventf(valkey, nil, "Warning", "ReplicationLost", "Bootstrap",
			"All data pods report role:master with no replicas; re-running bootstrap")
		meta.RemoveStatusCondition(&valkey.Status.Conditions, valkeyiov1alpha1.ConditionBootstrapped)
	}

	primary, replicas := splitNodesByIndex(nodes)
	if primary == nil || primary.Status.PodIP == "" || !primary.Status.Ready {
		return nil // wait for node-0 to come up
	}
	for _, r := range replicas {
		if !r.Status.Ready || r.Status.PodIP == "" {
			return nil // wait for all data nodes before issuing REPLICAOF
		}
	}

	operatorPassword, err := fetchSystemUserPassword(ctx, operatorUser, r.Client, valkey.Name, valkey.Namespace)
	if err != nil {
		return fmt.Errorf("fetch operator password: %w", err)
	}

	log.Info("bootstrapping replication", "primary", primary.Name)
	if err := withDataClient(primary.Status.PodIP, operatorPassword, func(c vclient.Client) error {
		return c.Do(ctx, c.B().Replicaof().No().One().Build()).Error()
	}); err != nil {
		return fmt.Errorf("promote primary: %w", err)
	}
	for _, replica := range replicas {
		ip := replica.Status.PodIP
		if err := withDataClient(ip, operatorPassword, func(c vclient.Client) error {
			return c.Do(ctx, c.B().Replicaof().Host(primary.Status.PodIP).Port(int64(DefaultPort)).Build()).Error()
		}); err != nil {
			return fmt.Errorf("REPLICAOF on %s: %w", ip, err)
		}
	}
	r.Recorder.Eventf(valkey, primary, "Normal", "ReplicationBootstrapped", "Bootstrap",
		"Initial primary is %s; %d replica(s) wired", primary.Name, len(replicas))
	r.setCondition(valkey, valkeyiov1alpha1.ConditionBootstrapped, "BootstrapComplete",
		fmt.Sprintf("Initial REPLICAOF wiring complete (primary: %s)", primary.Name),
		metav1.ConditionTrue)
	return nil
}

// replicationCollapsed reports true when every Ready data pod for this
// Valkey reports role:master with zero connected replicas - the
// catastrophic-loss signature we re-bootstrap from (full cluster
// restart without persistence). Returns false if any pod is still
// observing a slave link, if any pod is currently a slave with a live
// master_link, or if we can't reach all pods (we only act on a
// complete, confident picture).
func (r *ValkeyReconciler) replicationCollapsed(ctx context.Context, valkey *valkeyiov1alpha1.Valkey, nodes *valkeyiov1alpha1.ValkeyNodeList) (bool, error) {
	if valkey.Spec.Replicas == 0 {
		return false, nil
	}
	primary, replicas := splitNodesByIndex(nodes)
	if primary == nil || !primary.Status.Ready || primary.Status.PodIP == "" {
		return false, nil
	}
	for _, rep := range replicas {
		if !rep.Status.Ready || rep.Status.PodIP == "" {
			return false, nil
		}
	}
	password, err := fetchSystemUserPassword(ctx, operatorUser, r.Client, valkey.Name, valkey.Namespace)
	if err != nil {
		return false, fmt.Errorf("fetch operator password: %w", err)
	}
	check := func(ip string) (role string, connectedSlaves int, ok bool) {
		info, err := infoReplication(ctx, ip, password)
		if err != nil {
			return "", 0, false
		}
		role = strings.TrimSpace(info["role"])
		if v, err := strconv.Atoi(strings.TrimSpace(info["connected_slaves"])); err == nil {
			connectedSlaves = v
		}
		return role, connectedSlaves, true
	}
	role, conn, ok := check(primary.Status.PodIP)
	if !ok || role != RoleMaster || conn != 0 {
		return false, nil
	}
	for _, rep := range replicas {
		role, _, ok := check(rep.Status.PodIP)
		if !ok {
			return false, nil
		}
		if role != RoleMaster {
			return false, nil
		}
	}
	return true, nil
}

// splitNodesByIndex returns the node with LabelNodeIndex=0 and every
// other node, sorted by node index.
func splitNodesByIndex(nodes *valkeyiov1alpha1.ValkeyNodeList) (*valkeyiov1alpha1.ValkeyNode, []*valkeyiov1alpha1.ValkeyNode) {
	var primary *valkeyiov1alpha1.ValkeyNode
	others := map[int]*valkeyiov1alpha1.ValkeyNode{}
	for i := range nodes.Items {
		n := &nodes.Items[i]
		idx, err := strconv.Atoi(n.Labels[LabelNodeIndex])
		if err != nil {
			continue
		}
		if idx == 0 {
			primary = n
			continue
		}
		others[idx] = n
	}
	// emit in index order
	out := make([]*valkeyiov1alpha1.ValkeyNode, 0, len(others))
	for i := 1; i <= len(others); i++ {
		if n := others[i]; n != nil {
			out = append(out, n)
		}
	}
	return primary, out
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
	opt := vclient.ClientOption{
		InitAddress:       []string{fmt.Sprintf("%s:%d", ip, DefaultPort)},
		ForceSingleClient: true,
		Username:          operatorUser,
		Password:          password,
	}
	c, err := vclient.NewClient(opt)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	raw, err := c.Do(ctx, c.B().Info().Section("replication").Build()).ToString()
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	for line := range strings.SplitSeq(raw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		out[k] = v
	}
	return out, nil
}
