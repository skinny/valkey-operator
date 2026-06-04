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
	"strconv"

	valkeyiov1alpha1 "valkey.io/valkey-operator/api/v1alpha1"
)

const (
	// headlessClusterIP is the ClusterIP value marking a Service headless.
	headlessClusterIP = "None"

	// LabelValkey identifies a Valkey instance group. Set on every
	// ValkeyNode and pod owned by a Valkey CR.
	LabelValkey = "valkey.io/valkey"

	// sentinelUser is the ACL system user the operator provisions on
	// every replicated Valkey for ValkeySentinel pods to authenticate.
	sentinelUser = "_sentinel"

	// sentinelUserACL is the least-privilege ACL rule string used by the
	// ValkeySentinel pods. Sentinels need to connect, query INFO/ROLE,
	// receive pubsub messages on the sentinel channel, and (during a
	// failover) issue REPLICAOF and CLIENT KILL against replicas.
	//
	// +slaveof is required in addition to +replicaof: Valkey treats the
	// two as separate ACL entries even though they alias the same
	// command, and Sentinel's failover code path sends the legacy
	// SLAVEOF name on the wire (the +failover-state-send-slaveof-noone
	// event isn't just naming - it's literally what gets transmitted).
	// Without +slaveof, every sentinel-initiated failover hangs in
	// wait_promotion until the failover-timeout fires.
	sentinelUserACL = "-@all +@connection +ping +info +role +replicaof +slaveof " +
		"+subscribe +psubscribe +publish +unsubscribe +punsubscribe " +
		"+multi +exec +discard +command +client " +
		"+config|get +config|rewrite +config|set " +
		"~* resetchannels &__sentinel__:*"
)

// valkeyLabels returns the standard labels for a Valkey instance.
func valkeyLabels(valkey *valkeyiov1alpha1.Valkey) map[string]string {
	l := baseLabels(valkey.Name, "valkey")
	l[LabelValkey] = valkey.Name
	for k, v := range valkey.Labels {
		if _, exists := l[k]; !exists {
			l[k] = v
		}
	}
	return l
}

// valkeySelectorLabels returns the labels used to select pods belonging
// to this Valkey.
func valkeySelectorLabels(valkey *valkeyiov1alpha1.Valkey) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name": appName,
		LabelValkey:              valkey.Name,
	}
}

// valkeyResourceName returns the deterministic resource name used for
// the data Service, ConfigMap, and PDB owned by a Valkey: `valkey-<name>`.
func valkeyResourceName(valkey *valkeyiov1alpha1.Valkey) string {
	return resourcePrefix + valkey.Name
}

// valkeyNodeIndexedName returns the deterministic ValkeyNode name for
// the position `<index>` within a Valkey: `<name>-<index>`.
func valkeyNodeIndexedName(valkeyName string, index int) string {
	return valkeyName + "-" + strconv.Itoa(index)
}

// valkeySentinelAuthSecretName returns the per-Valkey Secret containing
// the `_sentinel` ACL user credentials, projected for consumption by
// selecting ValkeySentinels: `<name>-sentinel-auth`.
func valkeySentinelAuthSecretName(valkey *valkeyiov1alpha1.Valkey) string {
	return valkey.Name + "-sentinel-auth"
}
