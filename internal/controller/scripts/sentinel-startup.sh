#!/bin/sh
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
#
# Fails closed if any __SENTINEL_AUTH_PASS_*__ placeholder survives
# substitution - booting with a literal placeholder would silently
# wedge auth at runtime instead of failing fast at startup.

set -eu

CONFIG_DIR="${CONFIG_DIR:-/var/lib/sentinel}"
TEMPLATE="${TEMPLATE:-/config/sentinel.conf.template}"
TARGET="${CONFIG_DIR}/sentinel.conf"
AUTH_DIR="${AUTH_DIR:-/sentinel-auth}"

mkdir -p "${CONFIG_DIR}"

# Substitute the pod IP. Use a delimiter (|) that can't appear in an IP.
sed -e "s|__POD_IP__|${POD_IP}|g" "${TEMPLATE}" > "${TARGET}"

# Substitute one auth password per matched Valkey. The aggregated
# Secret materializes one file per Valkey name. awk does a literal
# substring replace, avoiding any escaping headaches with special
# characters in the password.
if [ -d "${AUTH_DIR}" ]; then
    for f in "${AUTH_DIR}"/*; do
        [ -f "${f}" ] || continue
        name="$(basename "${f}")"
        placeholder="__SENTINEL_AUTH_PASS_${name}__"
        # Read once to avoid spawning subshells inside awk.
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

# Fail fast if any auth placeholder was not substituted - either the
# aggregated auth Secret is missing an entry for a monitored Valkey or
# the mount didn't propagate. Booting with the literal placeholder as a
# password would just fail every AUTH at runtime.
if grep -q '__SENTINEL_AUTH_PASS_' "${TARGET}"; then
    echo "FATAL: unsubstituted __SENTINEL_AUTH_PASS_* placeholder remains in ${TARGET}" >&2
    grep '__SENTINEL_AUTH_PASS_' "${TARGET}" >&2
    exit 1
fi

exec valkey-server "${TARGET}" --sentinel
