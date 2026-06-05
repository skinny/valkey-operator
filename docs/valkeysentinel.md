# ValkeySentinel

`ValkeySentinel` deploys a standalone set of Valkey Sentinel processes
that monitor every [`Valkey`](./valkey.md) in the same namespace
whose labels match `spec.valkeySelector`.

The link is one-way: the sentinel picks its targets. The Valkey side
is unaware of monitoring beyond an observed `status.monitoredBy`
field.

## Quickstart

```yaml
apiVersion: valkey.io/v1alpha1
kind: ValkeySentinel
metadata:
  name: monitors
spec:
  replicas: 3
  valkeySelector:
    matchLabels:
      tier: caching
  config:
    down-after-milliseconds: "30000"
    failover-timeout: "180000"
    parallel-syncs: "1"
```

Apply alongside one or more `Valkey` CRs whose labels include
`tier: caching` and the sentinels will monitor them all from a single
3-pod set.

## Features

### `replicas` (minimum 3)

Three is the smallest set where one pod loss still leaves a quorum of
two. For a fleet that needs to monitor many masters, scale this set up
independently of any one Valkey's replica count.

### `valkeySelector` (required)

Standard `metav1.LabelSelector` (`matchLabels` and/or
`matchExpressions`). Same-namespace only.

Every `Valkey` in this namespace whose labels match is monitored.
Every `Valkey` that ceases to match (e.g. a label was removed) gets
`SENTINEL REMOVE`-d on the next reconcile - **immediately**, no grace
period.

### `config` (per-master tuning baked into the template)

```yaml
config:
  down-after-milliseconds: "30000"
  failover-timeout: "180000"
  parallel-syncs: "1"
```

These keys are written as `sentinel <key> <master> <value>` lines into
the rendered `sentinel.conf` template for every monitored master, then
materialized into the sentinel pods' working directory at boot.

The operator does **not** issue `SENTINEL SET` against running
sentinels in steady state - the ConfigMap is the source of truth. When
the rendered content changes, a content-hash annotation on the
StatefulSet pod template triggers a rolling restart (one pod at a
time, respecting the PDB) so the new config reaches the pods without
the burst of CONFIG REWRITE writes that can stall the sentinel timer.

Global to all monitored masters in this version; per-master overrides
are future work.

### `quorum` (optional)

Defaults to `floor(replicas/2)+1` (a strict majority of the sentinel
set). Must be ≤ `replicas`.

### Resources, scheduling, PDB

Standard. Anti-affine the sentinel pods across hosts or zones so a
node failure does not take the whole set down with the data it
monitors.

## How auth works

Each `Valkey` selected by a `ValkeySentinel` exposes a small
`<name>-sentinel-auth` Secret containing the credentials for the
`_sentinel` ACL user. The sentinel controller aggregates the passwords
from every matched Valkey into a single `<sentinel-name>-auth` Secret,
mounted read-only at `/sentinel-auth/` in every sentinel pod (one file
per Valkey, keyed by the Valkey name).

The rendered `sentinel.conf` carries a `__SENTINEL_AUTH_PASS_<name>__`
placeholder. The startup script substitutes each placeholder with the
contents of the matching file at pod start, so the password is never
written to a ConfigMap or pod environment variable.

If multiple `ValkeySentinel`s select the same `Valkey`, each renders
its own copy of the master into its own ConfigMap. The Valkey emits a
`MultipleSentinelsSelecting` warning event so the situation is
visible; behaviour is undefined.

## Status

```yaml
status:
  state: Ready
  readyReplicas: 3
  monitored:
    - cache-A
    - cache-B
    - cache-C
```

`monitored` is the de-duplicated list of masters the sentinels are
currently monitoring (from `SENTINEL masters` filtered by selector
match).

## Constraints

- Same-namespace label selector only. No cross-namespace.
- Standalone Valkeys (`replicas: 0`) are skipped by the selector even
  if their labels match - there's no replication to fail over.
- TLS for the sentinel port is reserved but not yet implemented.

## Architecture

```mermaid
graph TD
    S[ValkeySentinel CR]
    S -->|owns| SS[StatefulSet × replicas]
    SS -->|manages| SP[Sentinel pod :26379]
    S -->|creates| Svc[Headless Service :26379]
    S -->|creates| CM[ConfigMap with sentinel.conf.template]

    V1[Valkey: cache-A]
    V2[Valkey: cache-B]
    V3[Valkey: cache-C]
    S -.->|spec.valkeySelector matches| V1
    S -.->|spec.valkeySelector matches| V2
    S -.->|spec.valkeySelector matches| V3

    SP -.->|"sentinel monitor cache-A (from ConfigMap)"| V1
    SP -.->|"sentinel monitor cache-B (from ConfigMap)"| V2
    SP -.->|"sentinel monitor cache-C (from ConfigMap)"| V3
```
