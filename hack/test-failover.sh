#!/usr/bin/env bash
# Full test + log-gathering script for a freshly deployed Valkey + ValkeySentinel.
#
# Walks the deployment through:
#   0. Pod inventory and credential lookup
#   1. CRD / cluster state dump
#   2. Pre-failover replication topology (INFO replication on every data pod)
#   3. Pre-failover sentinel state (SENTINEL master/replicas/sentinels)
#   4. ACL sanity check from a sentinel pod against a replica
#      (PING / REPLICAOF NO ONE / SLAVEOF NO ONE / ACL WHOAMI)
#   5. Background log tails for every data pod, sentinel pod, and the operator
#   6. SENTINEL FAILOVER <name> triggered from sentinel-0
#   7. 90s polling loop snapshotting SENTINEL master <name>
#   8. Post-failover sentinel state + INFO replication on every data pod
#   9. Full log dumps from every pod
#  10. tar bundle of the output directory
#
# Usage:
#   ./hack/test-failover.sh                            # defaults
#   NS=default VALKEY=cache SENTINEL_CR=valkey-sentinel-monitors \
#     ./hack/test-failover.sh
#
# Environment overrides:
#   NS, VALKEY, SENTINEL_CR, MASTER_NAME, DATA_PORT, SENT_PORT,
#   DATA_CONTAINER, SENT_CONTAINER, OPERATOR_NS, POLL_SECS
#   OUT_DIR    Override the auto-timestamped output directory
#              (default: failover-debug-YYYYMMDD-HHMMSS in cwd).
#              Repeated runs accumulate output; either set OUT_DIR
#              to a throwaway path or `rm -rf failover-debug-*` after.
#
# Requirements: kubectl with cluster access; valkey-cli present inside the
# data/sentinel container images (it is, in the operator's default images).

set -uo pipefail

NS="${NS:-default}"
VALKEY="${VALKEY:-cache}"
SENTINEL_CR="${SENTINEL_CR:-valkey-sentinel-monitors}"
MASTER_NAME="${MASTER_NAME:-$VALKEY}"
DATA_PORT="${DATA_PORT:-6379}"
SENT_PORT="${SENT_PORT:-26379}"
DATA_CONTAINER="${DATA_CONTAINER:-server}"
SENT_CONTAINER="${SENT_CONTAINER:-sentinel}"
OPERATOR_NS="${OPERATOR_NS:-valkey-operator-system}"
POLL_SECS="${POLL_SECS:-90}"

STAMP="$(date +%Y%m%d-%H%M%S)"
OUT="${OUT_DIR:-failover-debug-${STAMP}}"
mkdir -p "$OUT"
exec > >(tee "$OUT/run.log") 2>&1

section() { echo; echo "=================================================="; echo "  $*"; echo "=================================================="; }
log()     { echo "[$(date +%H:%M:%S)] $*"; }

trap 'cleanup_tails 2>/dev/null || true' EXIT

###############################################################################
# 0. Pod inventory + credentials
###############################################################################
section "0. Pod inventory and credentials"

kubectl -n "$NS" get pods -o wide | tee "$OUT/00-pods.txt"

# Only enumerate Ready pods; running INFO / SENTINEL against a Pending
# or NotReady pod produces a confusing "container not found" or
# "Connection refused" that masks the actual replication state we're
# trying to inspect.
ready_pods() {
  kubectl -n "$NS" get pods -l "$1" \
    -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.status.conditions[?(@.type=="Ready")].status}{"\n"}{end}' \
    | awk '$2=="True"{print $1}' | tr '\n' ' '
}
read -r -a VALKEY_PODS <<< "$(ready_pods "valkey.io/valkey=$VALKEY")"
read -r -a SENT_PODS   <<< "$(ready_pods "valkey.io/sentinel=$SENTINEL_CR")"

log "Ready Valkey pods   (${#VALKEY_PODS[@]}): ${VALKEY_PODS[*]:-NONE}"
log "Ready Sentinel pods (${#SENT_PODS[@]}):   ${SENT_PODS[*]:-NONE}"

if [[ ${#VALKEY_PODS[@]} -eq 0 || -z "${VALKEY_PODS[0]:-}" ]]; then echo "FATAL: no Ready Valkey pods matched -l valkey.io/valkey=$VALKEY"; exit 1; fi
if [[ ${#SENT_PODS[@]}   -eq 0 || -z "${SENT_PODS[0]:-}"   ]]; then echo "FATAL: no Ready Sentinel pods matched -l valkey.io/sentinel=$SENTINEL_CR"; exit 1; fi

SENT_USER=$(kubectl -n "$NS" get secret "${VALKEY}-sentinel-auth" -o jsonpath='{.data.username}' | base64 -d)
SENT_PASS=$(kubectl -n "$NS" get secret "${VALKEY}-sentinel-auth" -o jsonpath='{.data.password}' | base64 -d)
if [[ -z "$SENT_USER" || -z "$SENT_PASS" ]]; then echo "FATAL: empty creds in ${VALKEY}-sentinel-auth"; exit 1; fi
log "Sentinel auth user: $SENT_USER (pass redacted, len=${#SENT_PASS})"

# Helpers ---------------------------------------------------------------------
# Pass the password through stdin into REDISCLI_AUTH (valkey-cli reads
# that env var) rather than `--pass`, which would expose the password
# in `ps` and audit logs on the kubelet node. Stdin-then-env keeps the
# secret out of every process listing involved in the exec.
v_cli()    { printf '%s' "$SENT_PASS" | kubectl -n "$NS" exec -i "$1" -c "$DATA_CONTAINER" -- sh -c 'REDISCLI_AUTH="$(cat)" exec valkey-cli --no-auth-warning --user "$1" -p "$2" "${@:3}"' _ "$SENT_USER" "$DATA_PORT" "${@:2}"; }
v_cli_h()  { printf '%s' "$SENT_PASS" | kubectl -n "$NS" exec -i "${SENT_PODS[0]}" -c "$SENT_CONTAINER" -- sh -c 'REDISCLI_AUTH="$(cat)" exec valkey-cli --no-auth-warning --user "$1" -h "$2" -p "$3" "${@:4}"' _ "$SENT_USER" "$1" "$DATA_PORT" "${@:2}"; }
s_cli()    { kubectl -n "$NS" exec "$1" -c "$SENT_CONTAINER" -- valkey-cli -p "$SENT_PORT" "${@:2}"; }
pod_ip()   { kubectl -n "$NS" get pod "$1" -o jsonpath='{.status.podIP}'; }

###############################################################################
# 1. CRD + cluster-wide state
###############################################################################
section "1. CRD + cluster state"

kubectl -n "$NS" get valkey "$VALKEY"               -o yaml > "$OUT/01-valkey.yaml"
kubectl -n "$NS" get valkeysentinel "$SENTINEL_CR"  -o yaml > "$OUT/01-valkeysentinel.yaml"
kubectl -n "$NS" get valkeynodes                    -o yaml > "$OUT/01-valkeynodes.yaml"        2>/dev/null || true
kubectl -n "$NS" describe valkey "$VALKEY"               > "$OUT/01-valkey-describe.txt"
kubectl -n "$NS" describe valkeysentinel "$SENTINEL_CR"  > "$OUT/01-valkeysentinel-describe.txt"
kubectl -n "$NS" get svc,configmap,secret,pdb -o wide   > "$OUT/01-extra-resources.txt"

echo "--- Valkey status ---"
yq '.status' "$OUT/01-valkey.yaml" 2>/dev/null || grep -A 30 '^status:' "$OUT/01-valkey.yaml" || true
echo "--- ValkeySentinel status ---"
yq '.status' "$OUT/01-valkeysentinel.yaml" 2>/dev/null || grep -A 30 '^status:' "$OUT/01-valkeysentinel.yaml" || true

###############################################################################
# 2. Pre-failover replication topology
###############################################################################
section "2. Pre-failover replication topology"

for p in "${VALKEY_PODS[@]}"; do
  log "INFO replication on $p ($(pod_ip "$p"))"
  v_cli "$p" INFO replication > "$OUT/02-${p}-info.txt" 2>&1 || true
  grep -E '^(role|connected_slaves|master_host|master_port|master_link_status|slave_repl_offset|master_repl_offset|slave_priority|slaveX):' "$OUT/02-${p}-info.txt" || true
  echo
done

###############################################################################
# 3. Pre-failover sentinel state
###############################################################################
section "3. Pre-failover sentinel state"

for s in "${SENT_PODS[@]}"; do
  log "Sentinel state from $s ($(pod_ip "$s"))"
  {
    echo "### SENTINEL master $MASTER_NAME";                     s_cli "$s" SENTINEL master "$MASTER_NAME"   2>&1 || true; echo
    echo "### SENTINEL replicas $MASTER_NAME";                   s_cli "$s" SENTINEL replicas "$MASTER_NAME" 2>&1 || true; echo
    echo "### SENTINEL sentinels $MASTER_NAME";                  s_cli "$s" SENTINEL sentinels "$MASTER_NAME" 2>&1 || true; echo
    echo "### SENTINEL get-master-addr-by-name $MASTER_NAME";    s_cli "$s" SENTINEL get-master-addr-by-name "$MASTER_NAME" 2>&1 || true
  } > "$OUT/03-${s}-sentinel-state.txt"
done

# Runtime sentinel.conf from sentinel-0
kubectl -n "$NS" exec "${SENT_PODS[0]}" -c "$SENT_CONTAINER" -- cat /var/lib/sentinel/sentinel.conf > "$OUT/03-sentinel.conf" 2>&1 || true

###############################################################################
# 4. ACL sanity check from a sentinel pod against a replica
###############################################################################
section "4. ACL sanity check (sentinel pod -> replica data port)"

REPLICA=""; REPLICA_IP=""
for p in "${VALKEY_PODS[@]}"; do
  role=$(grep -E '^role:' "$OUT/02-${p}-info.txt" | head -1 | cut -d: -f2 | tr -d '\r' | xargs)
  if [[ "$role" == "slave" ]]; then REPLICA="$p"; REPLICA_IP="$(pod_ip "$p")"; break; fi
done

if [[ -n "$REPLICA" ]]; then
  log "Probing $REPLICA ($REPLICA_IP)"
  {
    echo "### PING (no auth)";                kubectl -n "$NS" exec "${SENT_PODS[0]}" -c "$SENT_CONTAINER" -- valkey-cli -h "$REPLICA_IP" -p "$DATA_PORT" PING 2>&1 || true; echo
    echo "### PING (with _sentinel auth)";    v_cli_h "$REPLICA_IP" PING 2>&1 || true; echo
    echo "### ACL WHOAMI";                    v_cli_h "$REPLICA_IP" ACL WHOAMI 2>&1 || true; echo
    echo "### REPLICAOF NO ONE";              v_cli_h "$REPLICA_IP" REPLICAOF NO ONE 2>&1 || true; echo
    echo "### SLAVEOF NO ONE";                v_cli_h "$REPLICA_IP" SLAVEOF NO ONE 2>&1 || true; echo
    echo "### ACL LOG LATEST 5 (from $REPLICA)"; v_cli "$REPLICA" ACL LOG 5 2>&1 || true
  } > "$OUT/04-acl-sanity.txt"
  cat "$OUT/04-acl-sanity.txt"

  # Restore replica linkage; SLAVEOF NO ONE above will have detached it.
  MASTER_LINE=$(s_cli "${SENT_PODS[0]}" SENTINEL get-master-addr-by-name "$MASTER_NAME" 2>/dev/null | tr -d '\r')
  MASTER_IP=$(echo "$MASTER_LINE" | head -1)
  if [[ -n "$MASTER_IP" && "$MASTER_IP" != "$REPLICA_IP" ]]; then
    log "Restoring $REPLICA_IP as replica of $MASTER_IP:$DATA_PORT"
    v_cli_h "$REPLICA_IP" REPLICAOF "$MASTER_IP" "$DATA_PORT" 2>&1 || true
    sleep 3
  fi
else
  log "WARNING: no replica found in INFO output; skipping ACL probe"
fi

###############################################################################
# 5. Background log tails
###############################################################################
section "5. Starting background log tails"

declare -a TAIL_PIDS=()
for s in "${SENT_PODS[@]}";  do kubectl -n "$NS" logs -f "$s" -c "$SENT_CONTAINER"  > "$OUT/05-${s}.log" 2>&1 & TAIL_PIDS+=($!); done
for p in "${VALKEY_PODS[@]}"; do kubectl -n "$NS" logs -f "$p" -c "$DATA_CONTAINER" > "$OUT/05-${p}.log" 2>&1 & TAIL_PIDS+=($!); done

OPERATOR_POD=$(kubectl -n "$OPERATOR_NS" get pod -l control-plane=controller-manager -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
if [[ -n "$OPERATOR_POD" ]]; then
  kubectl -n "$OPERATOR_NS" logs -f "$OPERATOR_POD" > "$OUT/05-operator.log" 2>&1 & TAIL_PIDS+=($!)
  log "Operator pod: $OPERATOR_POD"
fi

cleanup_tails() { for pid in "${TAIL_PIDS[@]:-}"; do kill "$pid" 2>/dev/null || true; done; wait 2>/dev/null || true; }
log "Tail PIDs: ${TAIL_PIDS[*]}"
sleep 1

###############################################################################
# 6. Trigger failover
###############################################################################
section "6. Triggering SENTINEL FAILOVER $MASTER_NAME"

FAILOVER_T0=$(date +%s)
log "T0 = $(date +%H:%M:%S)"
s_cli "${SENT_PODS[0]}" SENTINEL FAILOVER "$MASTER_NAME" 2>&1 | tee "$OUT/06-failover-trigger.txt"

###############################################################################
# 7. Poll sentinel + replica info for POLL_SECS seconds
###############################################################################
section "7. Polling sentinel state every 1s for ${POLL_SECS}s"

POLL_FILE="$OUT/07-poll.txt"
: > "$POLL_FILE"
for i in $(seq 1 "$POLL_SECS"); do
  ts=$(date +%H:%M:%S)
  {
    echo "===== t+${i}s @ $ts ====="
    s_cli "${SENT_PODS[0]}" SENTINEL master "$MASTER_NAME" 2>&1 \
      | awk '/^(name|ip|port|flags|num-slaves|num-other-sentinels|quorum|failover-state|s-down-time|o-down-time|last-ok-ping-reply|info-refresh|role-reported)$/{k=$0; getline v; printf "  %-26s %s\n", k, v; next} 1' \
      | head -60
  } >> "$POLL_FILE"
  sleep 1
done

echo "--- final 60 lines of poll ---"
tail -60 "$POLL_FILE"

###############################################################################
# 8. Post-failover snapshot
###############################################################################
section "8. Post-failover state"

for s in "${SENT_PODS[@]}"; do
  {
    echo "### SENTINEL master $MASTER_NAME";                   s_cli "$s" SENTINEL master "$MASTER_NAME"   2>&1 || true; echo
    echo "### SENTINEL replicas $MASTER_NAME";                 s_cli "$s" SENTINEL replicas "$MASTER_NAME" 2>&1 || true; echo
    echo "### SENTINEL get-master-addr-by-name $MASTER_NAME";  s_cli "$s" SENTINEL get-master-addr-by-name "$MASTER_NAME" 2>&1 || true
  } > "$OUT/08-${s}-sentinel-state.txt"
done

for p in "${VALKEY_PODS[@]}"; do
  v_cli "$p" INFO replication > "$OUT/08-${p}-info.txt" 2>&1 || true
done

kubectl -n "$NS" get valkey "$VALKEY"              -o yaml > "$OUT/08-valkey-final.yaml"
kubectl -n "$NS" get valkeysentinel "$SENTINEL_CR" -o yaml > "$OUT/08-valkeysentinel-final.yaml"

echo "--- post-failover roles ---"
for p in "${VALKEY_PODS[@]}"; do
  role=$(grep -E '^role:' "$OUT/08-${p}-info.txt" | head -1 | cut -d: -f2 | tr -d '\r')
  master_host=$(grep -E '^master_host:' "$OUT/08-${p}-info.txt" | head -1 | cut -d: -f2 | tr -d '\r')
  printf "  %-30s role=%-7s master_host=%s\n" "$p ($(pod_ip "$p"))" "$role" "$master_host"
done

###############################################################################
# 9. Stop tails and dump full history
###############################################################################
section "9. Stopping tails and dumping full logs"

cleanup_tails
trap - EXIT

for s in "${SENT_PODS[@]}";  do kubectl -n "$NS" logs "$s" -c "$SENT_CONTAINER"  --tail=2000 > "$OUT/09-${s}-full.log"   2>&1 || true; done
for p in "${VALKEY_PODS[@]}"; do kubectl -n "$NS" logs "$p" -c "$DATA_CONTAINER" --tail=2000 > "$OUT/09-${p}-full.log"   2>&1 || true; done
if [[ -n "$OPERATOR_POD" ]]; then
  kubectl -n "$OPERATOR_NS" logs "$OPERATOR_POD" --tail=4000 > "$OUT/09-operator-full.log" 2>&1 || true
fi

# Annotate operator log with REPLICAOF/SLAVEOF lines for quick scan
grep -nE 'bootstrap|REPLICAOF|SLAVEOF|MONITOR|FAILOVER|reconcileMonitoring|primary' "$OUT/09-operator-full.log" > "$OUT/09-operator-grep.txt" 2>/dev/null || true

###############################################################################
# 10. Bundle
###############################################################################
section "10. Bundling output"

TARBALL="${OUT}.tar.gz"
tar czf "$TARBALL" "$OUT"
log "Wrote $TARBALL ($(du -h "$TARBALL" | cut -f1))"
log "Output dir:  $OUT/"
log "Tarball:     $TARBALL"
echo
echo "Quick-look files:"
echo "  $OUT/run.log                  -- full transcript (this output)"
echo "  $OUT/02-*-info.txt            -- pre-failover INFO replication per data pod"
echo "  $OUT/03-*-sentinel-state.txt  -- pre-failover sentinel view"
echo "  $OUT/04-acl-sanity.txt        -- ACL probe (REPLICAOF / SLAVEOF / WHOAMI)"
echo "  $OUT/07-poll.txt              -- per-second SENTINEL master snapshot"
echo "  $OUT/08-*-info.txt            -- post-failover INFO replication"
echo "  $OUT/09-*-full.log            -- full pod logs"
echo "  $OUT/09-operator-grep.txt     -- operator log filtered to REPLICAOF / MONITOR / FAILOVER"
