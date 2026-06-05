#!/bin/bash
# Sentinel startup script.
#
# Copies the read-only sentinel.conf.template from the operator's
# ConfigMap into a writable tmpfs at /var/lib/sentinel, substituting:
#   __POD_IP__                       -> this pod's IP (downward API)
#   __SENTINEL_AUTH_PASS_<name>__    -> contents of /sentinel-auth/<name>
#                                       (per-Valkey password files mounted
#                                       from the aggregated auth Secret)
#
# Sentinel needs the on-disk file to be writable because it rewrites
# it on every gossip update and failover state transition.

set -euo pipefail

CONFIG_DIR="${CONFIG_DIR:-/var/lib/sentinel}"
TEMPLATE="${TEMPLATE:-/config/sentinel.conf.template}"
TARGET="${CONFIG_DIR}/sentinel.conf"
AUTH_DIR="${AUTH_DIR:-/sentinel-auth}"

mkdir -p "${CONFIG_DIR}"

# Substitute the pod IP. Use a delimiter (|) that can't appear in an IP.
sed -e "s|__POD_IP__|${POD_IP}|g" "${TEMPLATE}" > "${TARGET}"

# Substitute one auth password per matched Valkey. The aggregated
# Secret materializes one file per Valkey name. Use sed's r-then-d to
# splice the file contents in literally, avoiding any escaping
# headaches with special characters in the password.
if [ -d "${AUTH_DIR}" ]; then
    for f in "${AUTH_DIR}"/*; do
        [ -f "${f}" ] || continue
        name="$(basename "${f}")"
        placeholder="__SENTINEL_AUTH_PASS_${name}__"
        # Read once to avoid spawning subshells inside sed.
        password="$(cat "${f}")"
        # POSIX-safe replacement using awk: substitutes the literal
        # placeholder string with the literal password string without
        # interpreting either as a regex.
        awk -v ph="${placeholder}" -v pw="${password}" '
            {
                while ((i = index($0, ph)) > 0) {
                    $0 = substr($0, 1, i-1) pw substr($0, i+length(ph))
                }
                print
            }
        ' "${TARGET}" > "${TARGET}.new" && mv "${TARGET}.new" "${TARGET}"
    done
fi

exec valkey-server "${TARGET}" --sentinel
