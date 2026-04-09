#!/usr/bin/env bash
set -euo pipefail

# Usage examples:
#   bash scripts/deploy_and_run_edges.sh deploy
#   bash scripts/deploy_and_run_edges.sh start --num-slots 20
#   bash scripts/deploy_and_run_edges.sh clean
#   bash scripts/deploy_and_run_edges.sh restart --num-slots 20
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
# Optional hard timeout (seconds) for ssh/scp commands.
# Default is disabled so long-running start/deploy operations are not killed.
# Set SSH_CMD_TIMEOUT to a positive integer to enable.
SSH_CMD_TIMEOUT="${SSH_CMD_TIMEOUT:-0}"

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
PYTHON_BINS=(
  "${PYTHON_BIN_PI1:-$PYTHON_BIN}"
  "${PYTHON_BIN_PI2:-$PYTHON_BIN}"
  "${PYTHON_BIN_PI3:-$PYTHON_BIN}"
  "${PYTHON_BIN_PI4:-$PYTHON_BIN}"
  "${PYTHON_BIN_PI7:-$PYTHON_BIN}"
)

ssh_opts=(-o StrictHostKeyChecking=accept-new -o ConnectTimeout=8)
if [[ -n "$SSH_KEY" ]]; then
  ssh_opts+=( -i "$SSH_KEY" )
fi
USE_SSHPASS=0
if [[ -n "$EDGE_PASS" ]] && command -v sshpass >/dev/null 2>&1; then
  USE_SSHPASS=1
elif [[ -n "$EDGE_PASS" ]]; then
  echo "[info] sshpass unavailable; continuing with interactive ssh/scp password prompts."
fi
if [[ "$USE_SSHPASS" -eq 0 ]]; then
  ssh_opts+=(-o ControlMaster=auto -o ControlPersist=10m -o ControlPath=/tmp/edge_mux_%r_%h_%p)
fi

ssh_cmd() {
  local ip="$1" user="$2"; shift 2
  if [[ "${SSH_CMD_TIMEOUT}" =~ ^[1-9][0-9]*$ ]]; then
    echo "[ssh] ${user}@${ip} (hard-timeout=${SSH_CMD_TIMEOUT}s)"
    if [[ "$USE_SSHPASS" -eq 1 ]]; then
      timeout "${SSH_CMD_TIMEOUT}" sshpass -p "$EDGE_PASS" ssh "${ssh_opts[@]}" "${user}@${ip}" "$@"
    else
      timeout "${SSH_CMD_TIMEOUT}" ssh "${ssh_opts[@]}" "${user}@${ip}" "$@"
    fi
  else
    echo "[ssh] ${user}@${ip} (hard-timeout=disabled)"
    if [[ "$USE_SSHPASS" -eq 1 ]]; then
      sshpass -p "$EDGE_PASS" ssh "${ssh_opts[@]}" "${user}@${ip}" "$@"
    else
      ssh "${ssh_opts[@]}" "${user}@${ip}" "$@"
    fi
  fi
}

scp_cmd() {
  local src="$1" dst="$2"
  if [[ "${SSH_CMD_TIMEOUT}" =~ ^[1-9][0-9]*$ ]]; then
    if [[ "$USE_SSHPASS" -eq 1 ]]; then
      timeout "${SSH_CMD_TIMEOUT}" sshpass -p "$EDGE_PASS" scp "${ssh_opts[@]}" -r "$src" "$dst"
    else
      timeout "${SSH_CMD_TIMEOUT}" scp "${ssh_opts[@]}" -r "$src" "$dst"
    fi
  else
    if [[ "$USE_SSHPASS" -eq 1 ]]; then
      sshpass -p "$EDGE_PASS" scp "${ssh_opts[@]}" -r "$src" "$dst"
    else
      scp "${ssh_opts[@]}" -r "$src" "$dst"
    fi
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
  local ip="$1" user="$2" py_bin="$3"; shift 3
  local extra_args="$*"
  local log_name="edge_scheduler_${ip//./_}.log"
  local state_name="runtime_state/state_${ip//./_}.json"
  echo "[start] ${ip} as ${user} (python=${py_bin})"
  if ! ssh_cmd "$ip" "$user" "${py_bin} -c 'import sys; raise SystemExit(0 if sys.version_info >= (3,6) else 1)'"; then
    echo "[start] ${ip} skipped: ${py_bin} is lower than Python 3.6"
    return 0
  fi
  ssh_cmd "$ip" "$user" "cd ${REMOTE_DIR} && nohup ${py_bin} greedy_edge_offloading.py \
    --local-source-only \
    --serve-state \
    --call-services \
    --local-ip ${ip} \
    --state-file ${state_name} \
    ${extra_args} > ${log_name} 2>&1 &"
}

status_one() {
  local ip="$1" user="$2"
  local log_name="edge_scheduler_${ip//./_}.log"
  local proc_pat="python(3)? .*greedy_edge_offloading.py"
  echo "[status] ${ip} as ${user}"
  ssh_cmd "$ip" "$user" "set -e; \
    if pgrep -af '${proc_pat}' >/dev/null; then \
      pgrep -af '${proc_pat}'; \
    else \
      echo 'NOT_RUNNING'; \
      if [ -f ${REMOTE_DIR}/${log_name} ]; then \
        echo '--- last 30 log lines ---'; \
        tail -n 30 ${REMOTE_DIR}/${log_name}; \
      else \
        echo 'log file not found:' ${REMOTE_DIR}/${log_name}; \
      fi; \
    fi"
}

stop_one() {
  local ip="$1" user="$2"
  echo "[stop] ${ip} as ${user}"
  ssh_cmd "$ip" "$user" "pkill -f 'greedy_edge_offloading.py' || true"
}

clean_one() {
  local ip="$1" user="$2"
  local remote_results="${REMOTE_DIR}/results"
  local remote_runtime="${REMOTE_DIR}/runtime_state"
  local remote_logs="${REMOTE_DIR}/edge_scheduler_*.log"
  echo "[clean] ${ip} as ${user}"
  ssh_cmd "$ip" "$user" "set -e; \
    pkill -f 'greedy_edge_offloading.py' || true; \
    rm -rf ${remote_results} ${remote_runtime}; \
    rm -f ${remote_logs}; \
    mkdir -p ${remote_results} ${remote_runtime} ${REMOTE_DIR}/dataset"
}

run_all_nodes() {
  local fn="$1"; shift || true
  local extra="$*"
  local failed=()
  for i in "${!IPS[@]}"; do
    if ! "$fn" "${IPS[$i]}" "${USERS[$i]}" "${PYTHON_BINS[$i]}" "$extra"; then
      failed+=("${IPS[$i]}")
      echo "[error] ${fn} failed on ${IPS[$i]}"
    fi
  done
  if [[ "${#failed[@]}" -gt 0 ]]; then
    echo "[error] nodes failed: ${failed[*]}"
    return 1
  fi
  return 0
}

case "$ACTION" in
  deploy)
    run_all_nodes deploy_one
    ;;
  start)
    run_all_nodes start_one "$*"
    ;;
  status)
    run_all_nodes status_one
    ;;
  stop)
    run_all_nodes stop_one
    ;;
  clean)
    run_all_nodes clean_one
    ;;
  restart)
    run_all_nodes clean_one
    run_all_nodes deploy_one
    run_all_nodes start_one "$*"
    ;;
  *)
    echo "Unknown action: '$ACTION'"
    echo "Usage: bash scripts/deploy_and_run_edges.sh {deploy|start|status|stop|clean|restart} [extra start args]"
    exit 1
    ;;
esac
