#!/usr/bin/env bash
set -euo pipefail

# Usage examples:
#   bash scripts/deploy_and_run_edges.sh deploy
#   bash scripts/deploy_and_run_edges.sh start --num-slots 20
#   bash scripts/deploy_and_run_edges.sh status
#   bash scripts/deploy_and_run_edges.sh stop

ACTION="${1:-}"
shift || true

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

ssh_opts=(-o StrictHostKeyChecking=accept-new -o ConnectTimeout=8)
if [[ -n "$SSH_KEY" ]]; then
  ssh_opts+=( -i "$SSH_KEY" )
fi

ssh_cmd() {
  local ip="$1"; shift
  if [[ -n "$EDGE_PASS" ]]; then
    sshpass -p "$EDGE_PASS" ssh "${ssh_opts[@]}" "${EDGE_USER}@${ip}" "$@"
  else
    ssh "${ssh_opts[@]}" "${EDGE_USER}@${ip}" "$@"
  fi
}

scp_cmd() {
  local src="$1" dst="$2"
  if [[ -n "$EDGE_PASS" ]]; then
    sshpass -p "$EDGE_PASS" scp "${ssh_opts[@]}" -r "$src" "$dst"
  else
    scp "${ssh_opts[@]}" -r "$src" "$dst"
  fi
}

deploy_one() {
  local ip="$1"
  echo "[deploy] ${ip}"
  ssh_cmd "$ip" "mkdir -p ${REMOTE_DIR}/dataset ${REMOTE_DIR}/runtime_state ${REMOTE_DIR}/results"
  scp_cmd "greedy_edge_offloading.py" "${EDGE_USER}@${ip}:${REMOTE_DIR}/greedy_edge_offloading.py"
  scp_cmd "dataset/node_1.csv" "${EDGE_USER}@${ip}:${REMOTE_DIR}/dataset/node_1.csv"
  scp_cmd "dataset/node_2.csv" "${EDGE_USER}@${ip}:${REMOTE_DIR}/dataset/node_2.csv"
  scp_cmd "dataset/node_3.csv" "${EDGE_USER}@${ip}:${REMOTE_DIR}/dataset/node_3.csv"
  scp_cmd "dataset/node_4.csv" "${EDGE_USER}@${ip}:${REMOTE_DIR}/dataset/node_4.csv"
  scp_cmd "dataset/node_5.csv" "${EDGE_USER}@${ip}:${REMOTE_DIR}/dataset/node_5.csv"
}

start_one() {
  local ip="$1"
  local extra_args="$*"
  local log_name="edge_scheduler_${ip//./_}.log"
  local state_name="runtime_state/state_${ip//./_}.json"
  echo "[start] ${ip}"
  ssh_cmd "$ip" "cd ${REMOTE_DIR} && nohup ${PYTHON_BIN} greedy_edge_offloading.py \
    --local-source-only \
    --serve-state \
    --call-services \
    --local-ip ${ip} \
    --state-file ${state_name} \
    ${extra_args} > ${log_name} 2>&1 & echo \\$!"
}

status_one() {
  local ip="$1"
  echo "[status] ${ip}"
  ssh_cmd "$ip" "ps -ef | grep -v grep | grep 'greedy_edge_offloading.py' || true"
}

stop_one() {
  local ip="$1"
  echo "[stop] ${ip}"
  ssh_cmd "$ip" "pkill -f 'greedy_edge_offloading.py' || true"
}

case "$ACTION" in
  deploy)
    if [[ -n "$EDGE_PASS" ]] && ! command -v sshpass >/dev/null 2>&1; then
      echo "sshpass not found. Install it or set EDGE_PASS='' and use SSH keys."
      exit 1
    fi
    for ip in "${IPS[@]}"; do deploy_one "$ip"; done
    ;;
  start)
    if [[ -n "$EDGE_PASS" ]] && ! command -v sshpass >/dev/null 2>&1; then
      echo "sshpass not found. Install it or set EDGE_PASS='' and use SSH keys."
      exit 1
    fi
    EXTRA_ARGS="$*"
    for ip in "${IPS[@]}"; do start_one "$ip" "$EXTRA_ARGS"; done
    ;;
  status)
    if [[ -n "$EDGE_PASS" ]] && ! command -v sshpass >/dev/null 2>&1; then
      echo "sshpass not found. Install it or set EDGE_PASS='' and use SSH keys."
      exit 1
    fi
    for ip in "${IPS[@]}"; do status_one "$ip"; done
    ;;
  stop)
    if [[ -n "$EDGE_PASS" ]] && ! command -v sshpass >/dev/null 2>&1; then
      echo "sshpass not found. Install it or set EDGE_PASS='' and use SSH keys."
      exit 1
    fi
    for ip in "${IPS[@]}"; do stop_one "$ip"; done
    ;;
  *)
    echo "Unknown action: '$ACTION'"
    echo "Usage: bash scripts/deploy_and_run_edges.sh {deploy|start|status|stop} [extra start args]"
    exit 1
    ;;
esac
