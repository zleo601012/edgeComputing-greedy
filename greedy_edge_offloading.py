#!/usr/bin/env python3
"""Minimal runnable greedy edge task offloading simulator."""

from __future__ import annotations

import argparse
import csv
import json
import random
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Sequence, Tuple


# =========================
# Configuration (centralized defaults)
# =========================
SERVICE_PORTS: List[int] = list(range(6000, 6012))
N_CORES_PER_NODE = 4
SLOT_SECONDS = 60.0
WINDOW_SIZE = 12

DATASET_MAP = {
    "node1": "dataset/node_1.csv",
    "node2": "dataset/node_2.csv",
    "node3": "dataset/node_3.csv",
    "node4": "dataset/node_4.csv",
    "node5": "dataset/node_5.csv",
}

SOURCE_TO_EXEC_NODE = {
    "node1": "pi1",
    "node2": "pi2",
    "node3": "pi3",
    "node4": "pi4",
    "node5": "pi7",
}

NODE_FREQ = {
    "pi1": 1.8e9,
    "pi2": 2.4e9,
    "pi3": 2.4e9,
    "pi4": 1.8e9,
    "pi7": 1.479e9,
}

# Base cycles by node group from user-provided profiling table.
_CYCLES_PI14 = {
    6000: 8593129007,
    6001: 1960902344,
    6002: 1975587405,
    6003: 1938525861,
    6004: 8680189438,
    6005: 8750678997,
    6006: 8450349675,
    6007: 8530011184,
    6008: 18353116339,
    6009: 18758187744,
    6010: 18437727945,
    6011: 18429929257,
}
_CYCLES_PI23 = {
    6000: 4865305194,
    6001: 1083408210,
    6002: 1065149327,
    6003: 1053057515,
    6004: 4810355796,
    6005: 4845037114,
    6006: 4870442831,
    6007: 4805983764,
    6008: 10647088857,
    6009: 10635313443,
    6010: 10630317754,
    6011: 10624142148,
}
_CYCLES_PI7 = {
    6000: 10191198469,
    6001: 2626900109,
    6002: 2510957770,
    6003: 2522746668,
    6004: 10435915377,
    6005: 10482996992,
    6006: 10859276156,
    6007: 10803132045,
    6008: 22609300001,
    6009: 22687589947,
    6010: 22230667388,
    6011: 22086200716,
}

CYCLES: Dict[int, Dict[str, int]] = {}
for p in SERVICE_PORTS:
    CYCLES[p] = {
        "pi1": _CYCLES_PI14[p],
        "pi4": _CYCLES_PI14[p],
        "pi2": _CYCLES_PI23[p],
        "pi3": _CYCLES_PI23[p],
        "pi7": _CYCLES_PI7[p],
    }

# Simple network model for first runnable version.
LOCAL_TX_TIME = 0.0
REMOTE_TX_TIME = 0.020  # 20ms constant for remote offloading

# Optional HTTP endpoint mapping (same host by default, per-port services).
NODE_HOST = {
    "pi1": "127.0.0.1",
    "pi2": "127.0.0.1",
    "pi3": "127.0.0.1",
    "pi4": "127.0.0.1",
    "pi7": "127.0.0.1",
}


@dataclass
class Task:
    task_id: str
    slot_id: int
    source_node: str
    service_port: int
    arrival_time: float
    rows: List[List[float]]


def load_feature_rows(csv_path: Path) -> List[List[float]]:
    """Load only the 12 feature columns used by services."""
    rows: List[List[float]] = []
    with csv_path.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        for raw in reader:
            # 12 features = columns after ts/slot/node_id.
            feat = [
                float(raw["rain_intensity_mmph"]),
                float(raw["flow_m3s"]),
                float(raw["temp_C"]),
                float(raw["pH"]),
                float(raw["DO_mgL"]),
                float(raw["EC_uScm"]),
                float(raw["COD_mgL"]),
                float(raw["NH3N_mgL"]),
                float(raw["TN_mgL"]),
                float(raw["TP_mgL"]),
                float(raw["TSS_mgL"]),
                float(raw["turbidity_NTU"]),
            ]
            rows.append(feat)
    return rows


def build_window(node_rows: Sequence[List[float]], slot_id: int, window_size: int = WINDOW_SIZE) -> List[List[float]]:
    """Build 12-row sliding window using current row and previous 11 rows."""
    start = slot_id - window_size + 1
    end = slot_id + 1
    if start < 0 or end > len(node_rows):
        raise IndexError(f"slot_id={slot_id} out of valid window range")
    return [list(r) for r in node_rows[start:end]]


def adapt_service_payload(task: Task) -> Dict[str, object]:
    """Convert task data into fixed microservice JSON format."""
    return {
        "task_id": task.task_id,
        "source_node_id": task.source_node,
        "rows": task.rows,
    }


def estimate_tx_time(source_exec_node: str, dst_exec_node: str) -> float:
    return LOCAL_TX_TIME if source_exec_node == dst_exec_node else REMOTE_TX_TIME


def min_core_state(available_time: Dict[str, List[float]], exec_node: str) -> Tuple[int, float]:
    core_times = available_time[exec_node]
    idx = min(range(len(core_times)), key=lambda i: core_times[i])
    return idx, core_times[idx]


def predict_task_times(
    available_time: Dict[str, List[float]],
    task: Task,
    exec_node: str,
    source_exec_node: str,
) -> Dict[str, float]:
    tx_time = estimate_tx_time(source_exec_node, exec_node)
    arrival_at_exec = task.arrival_time + tx_time

    core_id, core_ready_time = min_core_state(available_time, exec_node)
    wait_time = max(0.0, core_ready_time - arrival_at_exec)
    comp_time = CYCLES[task.service_port][exec_node] / NODE_FREQ[exec_node]
    start_time = arrival_at_exec + wait_time
    finish_time = start_time + comp_time
    total_latency = tx_time + wait_time + comp_time
    return {
        "core_id": float(core_id),
        "tx_time": tx_time,
        "wait_time": wait_time,
        "comp_time": comp_time,
        "start_time": start_time,
        "finish_time": finish_time,
        "total_latency": total_latency,
    }


def invoke_microservice(exec_node: str, task: Task, timeout_s: float = 5.0) -> Dict[str, object]:
    """Call real service endpoint. Returns fallback payload on network failure."""
    payload = adapt_service_payload(task)
    url = f"http://{NODE_HOST[exec_node]}:{task.service_port}/predict"
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_s) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            if isinstance(data, dict):
                return data
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, json.JSONDecodeError):
        pass

    # Minimal fallback to keep simulator runnable.
    return {
        "status": "simulated",
        "service": f"svc-{task.service_port}",
        "exec_node_id": exec_node,
        "source_node_id": task.source_node,
        "task_id": task.task_id,
        "score": 0.0,
        "latency_s": None,
    }


def simulate(
    start_slot: int,
    num_slots: int,
    output_dir: Path,
    seed: int,
    call_services: bool,
) -> None:
    rng = random.Random(seed)
    output_dir.mkdir(parents=True, exist_ok=True)

    dataset_rows = {
        node: load_feature_rows(Path(path))
        for node, path in DATASET_MAP.items()
    }

    available_time: Dict[str, List[float]] = {
        n: [0.0] * N_CORES_PER_NODE
        for n in NODE_FREQ
    }

    result_files = {
        node: output_dir / f"result_{node}.csv"
        for node in DATASET_MAP
    }
    fieldnames = [
        "slot_id",
        "task_id",
        "source_node",
        "exec_node",
        "service_port",
        "arrival_time",
        "tx_time",
        "wait_time",
        "comp_time",
        "start_time",
        "finish_time",
        "total_latency",
        "score",
        "service_name",
        "latency_s",
    ]
    for path in result_files.values():
        with path.open("w", encoding="utf-8", newline="") as fh:
            csv.DictWriter(fh, fieldnames=fieldnames).writeheader()

    for slot_id in range(start_slot, start_slot + num_slots):
        slot_start_t = slot_id * SLOT_SECONDS
        tasks: List[Task] = []

        # Step 1 + 2 + 3: Build window, generate 12 tasks/node, assign random arrivals.
        for source_node in DATASET_MAP:
            rows = build_window(dataset_rows[source_node], slot_id)
            for service_port in SERVICE_PORTS:
                arrival_time = slot_start_t + rng.uniform(0.0, SLOT_SECONDS)
                task = Task(
                    task_id=f"slot{slot_id}_{source_node}_{service_port}",
                    slot_id=slot_id,
                    source_node=source_node,
                    service_port=service_port,
                    arrival_time=arrival_time,
                    rows=rows,
                )
                tasks.append(task)

        # Step 4: sort by arrival.
        tasks.sort(key=lambda t: t.arrival_time)

        # Step 5/6/7: greedy scheduling + execute/capture result.
        for task in tasks:
            source_exec_node = SOURCE_TO_EXEC_NODE[task.source_node]

            best_node = None
            best_metrics = None
            for exec_node in NODE_FREQ:
                m = predict_task_times(available_time, task, exec_node, source_exec_node)
                if best_metrics is None or m["total_latency"] < best_metrics["total_latency"]:
                    best_node = exec_node
                    best_metrics = m

            assert best_node is not None and best_metrics is not None
            core_id = int(best_metrics["core_id"])
            available_time[best_node][core_id] = best_metrics["finish_time"]

            if call_services:
                result = invoke_microservice(best_node, task)
            else:
                result = {
                    "status": "simulated",
                    "service": f"svc-{task.service_port}",
                    "exec_node_id": best_node,
                    "source_node_id": task.source_node,
                    "task_id": task.task_id,
                    "score": 0.0,
                    "latency_s": None,
                }

            row = {
                "slot_id": task.slot_id,
                "task_id": task.task_id,
                "source_node": task.source_node,
                "exec_node": best_node,
                "service_port": task.service_port,
                "arrival_time": f"{task.arrival_time:.6f}",
                "tx_time": f"{best_metrics['tx_time']:.6f}",
                "wait_time": f"{best_metrics['wait_time']:.6f}",
                "comp_time": f"{best_metrics['comp_time']:.6f}",
                "start_time": f"{best_metrics['start_time']:.6f}",
                "finish_time": f"{best_metrics['finish_time']:.6f}",
                "total_latency": f"{best_metrics['total_latency']:.6f}",
                "score": result.get("score", ""),
                "service_name": result.get("service", f"svc-{task.service_port}"),
                "latency_s": result.get("latency_s", ""),
            }
            with result_files[task.source_node].open("a", encoding="utf-8", newline="") as fh:
                csv.DictWriter(fh, fieldnames=fieldnames).writerow(row)

    print(f"Simulation done. slots=[{start_slot}, {start_slot + num_slots - 1}] outputs={output_dir}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Greedy edge offloading minimal runner")
    parser.add_argument("--start-slot", type=int, default=11, help="Start slot index (>=11 for full 12-row window)")
    parser.add_argument("--num-slots", type=int, default=3, help="How many slots to run")
    parser.add_argument("--seed", type=int, default=20260408)
    parser.add_argument("--output-dir", type=Path, default=Path("results"))
    parser.add_argument("--call-services", action="store_true", help="Enable real HTTP microservice calls")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    simulate(
        start_slot=args.start_slot,
        num_slots=args.num_slots,
        output_dir=args.output_dir,
        seed=args.seed,
        call_services=args.call_services,
    )


if __name__ == "__main__":
    main()
