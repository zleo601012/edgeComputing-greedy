#!/usr/bin/env bash
set -euo pipefail

# Usage examples:
#   bash scripts/deploy_and_run_edges.sh deploy
#   bash scripts/deploy_and_run_edges.sh start --num-slots 20
#   bash scripts/deploy_and_run_edges.sh status
#   bash scripts/deploy_and_run_edges.sh stop

ACTION="${1:-}"
shift || true

# Default username if not using per-host users.
EDGE_USER="${EDGE_USER:-pi1}"
SSH_KEY="${SSH_KEY:-}"
EDGE_PASS="${EDGE_PASS:-123}"
REMOTE_DIR="${REMOTE_DIR:-~/edgeComputing-greedy}"
PYTHON_BIN="${PYTHON_BIN:-python3}"

IPS=(
  "192.168.1.167" # pi1
  "192.168.1.174" # pi2
  "192.168.1.175" # pi3
  "192.168.1.176" # pi4
  "192.168.1.177" # pi7
)
USERS=(
  "${EDGE_USER_PI1:-pi1}"
  "${EDGE_USER_PI2:-pi2}"
  "${EDGE_USER_PI3:-pi3}"
  "${EDGE_USER_PI4:-pi4}"
  "${EDGE_USER_PI7:-pi7}"
)

ssh_opts=(-o StrictHostKeyChecking=accept-new -o ConnectTimeout=8)
ssh_opts+=(-o ControlMaster=auto -o ControlPersist=10m -o ControlPath=/tmp/edge_mux_%r_%h_%p)
if [[ -n "$SSH_KEY" ]]; then
  ssh_opts+=( -i "$SSH_KEY" )
fi
USE_SSHPASS=0
if [[ -n "$EDGE_PASS" ]] && command -v sshpass >/dev/null 2>&1; then
  USE_SSHPASS=1
elif [[ -n "$EDGE_PASS" ]]; then
  echo "[info] sshpass unavailable; continuing with interactive ssh/scp password prompts."
fi

ssh_cmd() {
  local ip="$1" user="$2"; shift 2
  if [[ "$USE_SSHPASS" -eq 1 ]]; then
    sshpass -p "$EDGE_PASS" ssh "${ssh_opts[@]}" "${user}@${ip}" "$@"
  else
    ssh "${ssh_opts[@]}" "${user}@${ip}" "$@"
  fi
}

scp_cmd() {
  local src="$1" dst="$2"
  if [[ "$USE_SSHPASS" -eq 1 ]]; then
    sshpass -p "$EDGE_PASS" scp "${ssh_opts[@]}" -r "$src" "$dst"
  else
    scp "${ssh_opts[@]}" -r "$src" "$dst"
  fi
}

deploy_one() {
  local ip="$1" user="$2"
  echo "[deploy] ${ip} as ${user}"
  ssh_cmd "$ip" "$user" "mkdir -p ${REMOTE_DIR}/dataset ${REMOTE_DIR}/runtime_state ${REMOTE_DIR}/results"
  local bundle
  bundle="$(mktemp /tmp/edge_bundle_XXXXXX.tar.gz)"
  tar -czf "$bundle" greedy_edge_offloading.py dataset/node_1.csv dataset/node_2.csv dataset/node_3.csv dataset/node_4.csv dataset/node_5.csv
  scp_cmd "$bundle" "${user}@${ip}:${REMOTE_DIR}/edge_bundle.tar.gz"
  ssh_cmd "$ip" "$user" "cd ${REMOTE_DIR} && tar -xzf edge_bundle.tar.gz && rm -f edge_bundle.tar.gz"
  rm -f "$bundle"
}

start_one() {
  local ip="$1" user="$2"; shift 2
  local extra_args="$*"
  local log_name="edge_scheduler_${ip//./_}.log"
  local state_name="runtime_state/state_${ip//./_}.json"
  echo "[start] ${ip} as ${user}"
  ssh_cmd "$ip" "$user" "cd ${REMOTE_DIR} && nohup ${PYTHON_BIN} greedy_edge_offloading.py \
    --local-source-only \
    --serve-state \
    --call-services \
    --local-ip ${ip} \
    --state-file ${state_name} \
    ${extra_args} > ${log_name} 2>&1 &"
}

status_one() {
  local ip="$1" user="$2"
  echo "[status] ${ip} as ${user}"
  ssh_cmd "$ip" "$user" "ps -ef | grep -v grep | grep 'greedy_edge_offloading.py' || true"
}

stop_one() {
  local ip="$1" user="$2"
  echo "[stop] ${ip} as ${user}"
  ssh_cmd "$ip" "$user" "pkill -f 'greedy_edge_offloading.py' || true"
}

case "$ACTION" in
  deploy)
    for i in "${!IPS[@]}"; do deploy_one "${IPS[$i]}" "${USERS[$i]}"; done
    ;;
  start)
    EXTRA_ARGS="$*"
    for i in "${!IPS[@]}"; do start_one "${IPS[$i]}" "${USERS[$i]}" "$EXTRA_ARGS"; done
    ;;
  status)
    for i in "${!IPS[@]}"; do status_one "${IPS[$i]}" "${USERS[$i]}"; done
    ;;
  stop)
    for i in "${!IPS[@]}"; do stop_one "${IPS[$i]}" "${USERS[$i]}"; done
    ;;
  *)
    echo "Unknown action: '$ACTION'"
    echo "Usage: bash scripts/deploy_and_run_edges.sh {deploy|start|status|stop} [extra start args]"
    exit 1
    ;;
esac
