#!/bin/bash
# Sentinel startup script.
#
# Copies the read-only sentinel.conf.template from the operator's
# ConfigMap into a writable emptyDir at /var/lib/sentinel and runs
# valkey-server against that copy in --sentinel mode. Substitutes
# __POD_IP__ with the actual pod IP at runtime (via the downward API).
#
# Sentinel rewrites its own config at runtime to record peers and
# monitored masters; the on-disk file must be writable.

set -euo pipefail

CONFIG_DIR="${CONFIG_DIR:-/var/lib/sentinel}"
TEMPLATE="${TEMPLATE:-/config/sentinel.conf.template}"
TARGET="${CONFIG_DIR}/sentinel.conf"

mkdir -p "${CONFIG_DIR}"

# Render the template with POD_IP from the downward API.
sed -e "s|__POD_IP__|${POD_IP}|g" "${TEMPLATE}" > "${TARGET}"

exec valkey-server "${TARGET}" --sentinel
