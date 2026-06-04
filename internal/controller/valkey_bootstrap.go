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

// bootstrapReplication performs the one-shot initial REPLICAOF wiring:
// node-0 becomes primary, every other node REPLICAOFs node-0. Once it
// has succeeded the Bootstrapped condition is set and this function
// becomes a no-op for the lifetime of the Valkey CR. After that,
// Sentinel (if monitoring) is the only authority on the topology -
// the operator never re-wires, so a sentinel-initiated failover
// won't be fought by a stale "bootstrap" loop.
func (r *ValkeyReconciler) bootstrapReplication(ctx context.Context, valkey *valkeyiov1alpha1.Valkey, nodes *valkeyiov1alpha1.ValkeyNodeList) error {
	log := logf.FromContext(ctx)

	// Sticky one-shot guard. Set on first successful bootstrap and
	// never cleared - even pod restarts (which lose REPLICAOF in-memory
	// state) do not retrigger bootstrap. Recovery from that scenario
	// is Sentinel's job, or manual.
	if meta.IsStatusConditionTrue(valkey.Status.Conditions, valkeyiov1alpha1.ConditionBootstrapped) {
		return nil
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
