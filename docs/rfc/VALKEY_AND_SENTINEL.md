# Valkey + ValkeySentinel CRDs - Selector-Linked Design

**Date:** 2026-06-04
**Status:** Implementing
**Authors:** valkey-operator contributors

---

## Overview

Two CRDs for non-sharded Valkey:

- **`Valkey`** - the data plane. Standalone (`replicas: 0`) or replicated
  (`replicas > 0`). Knows nothing about Sentinel.
- **`ValkeySentinel`** - the monitoring plane. Runs Sentinel pods.
  Selects which Valkeys to monitor via `spec.valkeySelector` (a label
  selector, same namespace).

A `Valkey` resource has no `sentinelRef`. The link is one-way: sentinels
pick their targets. Adding/removing HA is a sentinel-side edit.

## Goals

- One sentinel set, many data sets (1:N via labels).
- Data CR is unaware of monitoring.
- Operator never performs its own failover; with no selecting sentinel,
  a replicated `Valkey` has manual-failover semantics by design.
- Proactive failover during rolling updates: when rolling the current
  primary's pod, and a sentinel selects this Valkey, issue
  `SENTINEL FAILOVER` first.
- No cross-CR finalizer.

## Non-Goals

- Sharding (use `ValkeyCluster`).
- Cross-namespace selection (deferred; needs reference-grant machinery).
- Operator-managed failover (Sentinel is the only failover path).
- TLS for the sentinel port (deferred; spec field reserved).

## Resource Hierarchy

```
User creates:
├─ Valkey (any number)
│  └─ Creates: ValkeyNode per pod position
│     └─ Creates: StatefulSet + PVC
│     Plus: headless Service (6379), ConfigMap, ACL Secret,
│           <name>-sentinel-auth Secret (read by selecting sentinels), PDB
└─ ValkeySentinel (any number)
   └─ Creates: StatefulSet of sentinel pods + headless Service (26379)

Link:
  ValkeySentinel.spec.valkeySelector matches Valkey.metadata.labels
  (same-namespace only)
```

## Spec

### Valkey

```yaml
apiVersion: valkey.io/v1alpha1
kind: Valkey
metadata:
  name: cache
  namespace: default
  labels:
    tier: caching            # selected by ValkeySentinel
spec:
  replicas: 2                # 0 = standalone (1 pod), N = 1 primary + N replicas
  image: valkey/valkey:9.0
  config:
    maxmemory: 1gb
    maxmemory-policy: allkeys-lfu
  # inherits resources, persistence, scheduling, users, exporter, TLS,
  # containers, podDisruptionBudget
```

No `sentinelRef`, no `masterName`. The Sentinel master name is the
Valkey's `metadata.name`; collisions within a namespace are
structurally impossible.

### ValkeySentinel

```yaml
apiVersion: valkey.io/v1alpha1
kind: ValkeySentinel
metadata:
  name: monitors
  namespace: default
spec:
  replicas: 3                # min 3
  valkeySelector:
    matchLabels:
      tier: caching          # picks every Valkey with this label
  config:
    down-after-milliseconds: "30000"
    failover-timeout: "180000"
    parallel-syncs: "1"
  resources:
    requests: {memory: 64Mi, cpu: 50m}
    limits:   {memory: 128Mi, cpu: 250m}
```

`spec.config` is global to every monitored master in this MVP.
Per-master overrides are future work.

## Reconciliation Logic

### Valkey

1. Provision the data plane: headless Service (6379), ConfigMap, ACL
   Secret, `<name>-sentinel-auth` Secret, PDB.
2. Reconcile `1 + spec.replicas` `ValkeyNode`s, one at a time. Before
   rolling the pod that currently holds the primary, see [Proactive
   failover during primary rolls](#proactive-failover-during-primary-rolls).
3. **Initial wiring**: if no replica yet reports a `master_link` to
   node-0, issue `REPLICAOF NO ONE` on node-0 and
   `REPLICAOF <node-0-ip> 6379` on each replica. One-shot bootstrap;
   once any node reports a non-default `master_host`, the controller
   stops issuing `REPLICAOF`. From that point on, Sentinel (if
   monitoring) is the source of truth.
4. List ValkeySentinels in the namespace whose `valkeySelector` matches
   this Valkey's labels. Populate `status.monitoredBy`. If more than
   one matches, emit a `MultipleSentinelsSelecting` warning event.
5. Observe the primary: prefer asking any selecting sentinel via
   `SENTINEL get-master-addr-by-name <valkey-name>`; fall back to
   per-pod `INFO replication` when no sentinel is reachable.
6. Update `status.primaryPodName` and `readyReplicas`. If no pod
   reports `role:master` (a real primary outage with no monitoring
   sentinel), clear `status.primaryPodName` and surface
   `Ready=False/PrimaryLost`.

The Valkey controller does **not** perform failover after the initial
wiring. Period.

### ValkeySentinel

1. Upsert headless Service (26379), ConfigMap with the startup script
   and a `sentinel.conf` template (no `sentinel monitor` directives),
   PDB, and the StatefulSet running `spec.replicas` sentinel pods.
2. List Valkeys matching `spec.valkeySelector`.
3. For each matched Valkey:
   - Pick an entry IP: list its data pods (by label), pick any
     reachable one. The controller does **not** read
     `Valkey.status.primaryPodName`. Sentinel itself follows
     `INFO replication` to discover the actual master.
   - Read the per-Valkey `<name>-sentinel-auth` Secret for the
     `_sentinel` user credentials.
   - Issue `SENTINEL MONITOR <valkey-name> <entry-ip> 6379 <quorum>` on
     each sentinel pod that isn't already monitoring this master.
   - Apply `SENTINEL SET <valkey-name> auth-user/auth-pass` from the
     sentinel-auth Secret, plus every key/value in `spec.config`.
4. For masters in `SENTINEL masters` whose name no longer corresponds
   to a Valkey matching the selector, issue `SENTINEL REMOVE` on each
   pod **immediately** (no grace period; a relabel is intentional).
5. Update `status.monitored` and `readyReplicas`.

The sentinel reconciler reads no field of `Valkey.status`. The two
CRDs are genuinely decoupled at runtime.

### Proactive failover during primary rolls

The Valkey controller's only post-bootstrap interaction with Sentinel.
When about to roll the pod currently holding the primary:

- If `status.monitoredBy` is non-empty AND a synced replica is ready,
  issue `SENTINEL FAILOVER <valkey-name>` against any reachable
  sentinel in any selecting set.
- Poll every reachable sentinel every 1s for up to 10s, watching for
  `SENTINEL get-master-addr-by-name` to return a different IP.
- Timeout / rejection is a soft failure; the controller logs and
  proceeds with the roll anyway.

## Resolved Design Decisions

1. **Per-master `SENTINEL SET` overrides** - global on
   `ValkeySentinel.spec.config` for the MVP. Per-master overrides are
   future work.
2. **Multiple ValkeySentinels selecting one Valkey** - allowed.
   Last-applied SET wins. Valkey emits a
   `MultipleSentinelsSelecting` warning event.
3. **Label changes that no longer match** - `SENTINEL REMOVE`
   immediately on next reconcile. No grace period.
4. **Primary outage without a monitoring sentinel** - `Valkey` sits in
   `Ready=False/PrimaryLost`, `status.primaryPodName` cleared. No
   automatic recovery.
5. **Bootstrap race** - sentinel controller doesn't read
   `Valkey.status.primaryPodName`. Picks any reachable data pod by
   label; Sentinel itself discovers the master.
6. **Sentinel-side auth** - Valkey controller provisions a per-Valkey
   `<name>-sentinel-auth` Secret. ValkeySentinel reads it and pushes
   to `SENTINEL SET auth-user/auth-pass`.
7. **Sentinel-side resource naming** - `valkey-sentinel-<name>`.

## Naming Conventions

| Owning CR        | Resource                  | Naming Pattern                       |
| ---------------- | ------------------------- | ------------------------------------ |
| `Valkey`         | Headless data Service     | `valkey-<name>`                      |
| `Valkey`         | ConfigMap                 | `valkey-<name>`                      |
| `Valkey`         | ACL Secret                | `<name>-acl`                         |
| `Valkey`         | Sentinel-auth Secret      | `<name>-sentinel-auth`               |
| `Valkey`         | PodDisruptionBudget       | `valkey-<name>`                      |
| `Valkey`         | `ValkeyNode` (per pod)    | `<name>-<index>`                     |
| `ValkeySentinel` | Sentinel StatefulSet      | `valkey-sentinel-<name>`             |
| `ValkeySentinel` | Sentinel headless Service | `valkey-sentinel-<name>`             |
| `ValkeySentinel` | Sentinel ConfigMap        | `valkey-sentinel-<name>`             |
| `ValkeySentinel` | Sentinel PDB              | `valkey-sentinel-<name>`             |

## Lifecycle and Deletion

Standard owner-reference cascade on both CRDs. **No cross-CR
finalizer.** Deleting a `ValkeySentinel` is a routine `kubectl
delete`; the data plane keeps running in whatever topology Sentinel
last left it (no automatic failover until another sentinel selects
it).

## Future Work

- Per-master `SENTINEL SET` overrides.
- Cross-namespace selection (`spec.namespaceSelector` gated by
  reference-grant).
- TLS for the sentinel port.
- Cert-manager integration.
- Migrate sentinel pods into `ValkeyNode` when the primitive grows
  the necessary fields.

## References

- ValkeyNode design: [`docs/valkeynode-design.md`](../valkeynode-design.md)
- Prometheus-operator selector pattern:
  <https://prometheus-operator.dev/docs/api-reference/api/#monitoring.coreos.com/v1.ServiceMonitor>
- Sentinel protocol: <https://valkey.io/topics/sentinel/>
