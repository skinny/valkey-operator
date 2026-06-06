# Valkey

`Valkey` deploys a primary/replicas (or standalone) set of Valkey
data pods. It is non-sharded — use [`ValkeyCluster`](./valkeycluster.md)
for cluster mode.

Failover is delegated to a separately-managed
[`ValkeySentinel`](./valkeysentinel.md) that selects this Valkey via
labels. There is no `sentinelRef` on this CR; the link is one-way and
the data CR is unaware of monitoring beyond an observed
`status.monitoredBy` list.

## Quickstart

```yaml
apiVersion: valkey.io/v1alpha1
kind: Valkey
metadata:
  name: cache
  labels:
    tier: caching             # picked up by a ValkeySentinel selector
spec:
  replicas: 2                 # 0 = standalone (1 pod). N = 1 primary + N replicas.
  config:
    maxmemory: 50mb
    maxmemory-policy: allkeys-lfu
```

A `Valkey` is **not highly-available without a `ValkeySentinel`** -
nothing automatically promotes a replica if the primary dies. To turn
on HA, apply a `ValkeySentinel` whose `spec.valkeySelector` matches
this Valkey's labels.

## Features

### Replicas

```yaml
replicas: 0    # standalone (1 pod)
replicas: 2    # 1 primary + 2 replicas (3 pods total)
```

Matches the semantics of `ValkeyCluster.spec.replicas`.

### Config passthrough

```yaml
config:
  maxmemory: 1gb
  maxmemory-policy: allkeys-lfu
```

Written verbatim into `valkey.conf`. The operator does not validate
option names.

### Persistence, resources, scheduling, users, TLS, exporter

Same shape and semantics as `ValkeyCluster`. See its
[reference](./valkeycluster.md) for details.

#### Persistence immutability

`spec.persistence` is partially immutable to avoid silent data loss:

* **Add**: allowed (a Valkey created without persistence can be updated
  to enable it later; the operator rolls the data pods with the new PVC).
* **Remove**: rejected. Going from persisted to non-persisted would
  detach the on-disk data; opt out by recreating the Valkey.
* **Resize**: only expansion is allowed (`persistence.size` may grow,
  not shrink).
* **`storageClassName`**: immutable once set.

## How sentinel monitoring works (high level)

The `Valkey` CR itself never references a sentinel. The link is
discovered from the other side:

1. You create a `Valkey` with some labels.
2. You create a `ValkeySentinel` with `spec.valkeySelector` matching
   those labels.
3. The sentinel controller renders this Valkey's primary endpoint
   into its sentinel.conf template and rolls the sentinel pods.
4. The Valkey controller observes the selecting sentinel and populates
   `status.monitoredBy` and the `Monitored=True` condition.

To stop monitoring, either delete the `ValkeySentinel` or change
the labels on the `Valkey` so the selector no longer matches.

## Status

```yaml
status:
  state: Ready
  primaryPodName: valkey-cache-0
  readyReplicas: 2
  monitoredBy:                # observed, not declared
    - monitors
  conditions:
    - type: Ready
      status: "True"
      reason: ClusterHealthy
    - type: PrimaryElected
      status: "True"
      reason: PrimaryElected
    - type: Monitored
      status: "True"
      reason: SentinelSelecting
```

When `replicas > 0` and no `ValkeySentinel` selects this Valkey,
`Monitored` is `False/NoSentinelSelecting` (informational - not
gating `Ready`). If the primary then dies, `Ready` transitions to
`False/PrimaryLost` until either a human intervenes or a selecting
`ValkeySentinel` is applied.

## Constraints

- `spec.replicas` minimum is 0.
- Same-namespace selection only (no cross-namespace).
- TLS is reserved on the spec but not yet implemented.
- The operator never performs failover. Sentinel does.

## Architecture

```mermaid
graph TD
    V[Valkey CR]
    V -->|owns| N[ValkeyNode × 1+replicas]
    N -->|creates| SS[StatefulSet replicas=1]
    SS -->|manages| P[Pod with server + exporter]
    V -->|creates| Svc[Headless Service :6379]
    V -->|creates| CM[ConfigMap valkey.conf]
    V -->|creates| ACL[ACL Secret + sentinel-auth Secret]

    S[ValkeySentinel CR]
    S -.->|spec.valkeySelector matches V.labels| V
    S -.->|reads V.status.primaryEndpoint| V
    S -.->|sentinel monitor (in sentinel.conf)| P
```
