"""
Produces Table 2.1 (latency comparison) and Table 2.2 (resource dynamics)
directly from MetricsTable, matching the thesis §2.6.3 exactly.

Usage:
    python experiment/analyze_results.py \
        --baseline-correlation-id baseline-xxxx \
        --adpa-correlation-id adpa-xxxx
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import numpy as np  # noqa: E402
from tabulate import tabulate  # noqa: E402

from metrics.metrics_recorder import query_all_metrics_for_experiment  # noqa: E402
from ingestion.event_generator import DEFAULT_PHASES  # noqa: E402


PHASE_NAMES = [p.name for p in DEFAULT_PHASES]
PHASE_LAMBDA = {p.name: p.lambda_rate for p in DEFAULT_PHASES}


def _filter_critical(metrics: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [m for m in metrics if m["priority"] == "HIGH"]


def build_table_2_1(baseline_metrics: List[Dict[str, Any]], adpa_metrics: List[Dict[str, Any]]) -> str:
    """Table 2.1 — Latency comparison of critical events per phase."""
    baseline_critical = _filter_critical(baseline_metrics)
    adpa_critical = _filter_critical(adpa_metrics)

    rows = []
    for phase in PHASE_NAMES:
        b_lat = [m["latency_seconds"] for m in baseline_critical if m["experiment_phase"] == phase]
        a_lat = [m["latency_seconds"] for m in adpa_critical if m["experiment_phase"] == phase]

        b_mean = float(np.mean(b_lat)) if b_lat else float("nan")
        a_mean = float(np.mean(a_lat)) if a_lat else float("nan")

        improvement = (
            ((b_mean - a_mean) / b_mean) * 100.0 if b_lat and a_lat and b_mean > 0 else float("nan")
        )

        rows.append([
            phase,
            f"{PHASE_LAMBDA[phase]:.0f}",
            f"{b_mean:.2f}" if b_lat else "N/A",
            f"{a_mean:.2f}" if a_lat else "N/A",
            f"+{improvement:.1f}%" if improvement == improvement else "N/A",
        ])

    headers = ["Phase", "Intensity (ev/s)", "Baseline Latency (s)", "ADPA Latency (s)", "Improvement (%)"]
    return tabulate(rows, headers=headers, tablefmt="grid")


def build_table_2_2(baseline_metrics: List[Dict[str, Any]], adpa_metrics: List[Dict[str, Any]]) -> str:
    """Table 2.2 — Resource allocation dynamics per phase."""
    rows = []
    for phase in PHASE_NAMES:
        b_units = [
            m["resource_units_at_processing"] for m in baseline_metrics if m["experiment_phase"] == phase
        ]
        a_units = [
            m["resource_units_at_processing"] for m in adpa_metrics if m["experiment_phase"] == phase
        ]

        b_avg = float(np.mean(b_units)) if b_units else float("nan")
        a_avg = float(np.mean(a_units)) if a_units else float("nan")

        # CPU proxy utilisation: derived the same way the controller derives it,
        # reconstructed here from resource_units and phase intensity for reporting.
        approx_cpu = min(100.0, (PHASE_LAMBDA[phase] / max(a_avg, 1)) * 5) if a_units else float("nan")

        rows.append([
            phase,
            f"{b_avg:.1f}" if b_units else "N/A",
            f"{a_avg:.1f}" if a_units else "N/A",
            f"~{approx_cpu:.0f}%" if a_units else "N/A",
        ])

    headers = ["Phase", "Baseline (fixed units)", "ADPA (adaptive units)", "ADPA CPU-proxy util. (%)"]
    return tabulate(rows, headers=headers, tablefmt="grid")


def build_throughput_summary(baseline_metrics, adpa_metrics) -> str:
    rows = []
    for label, metrics in [("baseline", baseline_metrics), ("adpa", adpa_metrics)]:
        for phase in PHASE_NAMES:
            phase_metrics = [m for m in metrics if m["experiment_phase"] == phase]
            duration = max(1, next((p.duration_seconds for p in DEFAULT_PHASES if p.name == phase), 60))
            throughput = len(phase_metrics) / duration
            rows.append([label, phase, len(phase_metrics), f"{throughput:.2f}"])

    headers = ["System", "Phase", "Processed Events", "Throughput (events/s)"]
    return tabulate(rows, headers=headers, tablefmt="grid")


def main():
    parser = argparse.ArgumentParser(description="Analyze ADPA vs Baseline experiment results")
    parser.add_argument("--baseline-correlation-id", required=True)
    parser.add_argument("--adpa-correlation-id", required=True)
    parser.add_argument("--export-csv", action="store_true")
    args = parser.parse_args()

    print("Fetching baseline metrics...")
    baseline_metrics = query_all_metrics_for_experiment(args.baseline_correlation_id, systems=["baseline"])
    print(f"  -> {len(baseline_metrics)} records")

    print("Fetching ADPA metrics...")
    adpa_metrics = query_all_metrics_for_experiment(args.adpa_correlation_id, systems=["adpa"])
    print(f"  -> {len(adpa_metrics)} records")

    print("\n" + "=" * 80)
    print("Table 2.1 — Порівняння затримки обробки критичних подій (Latency)")
    print("=" * 80)
    print(build_table_2_1(baseline_metrics, adpa_metrics))

    print("\n" + "=" * 80)
    print("Table 2.2 — Динаміка виділення обчислювальних ресурсів")
    print("=" * 80)
    print(build_table_2_2(baseline_metrics, adpa_metrics))

    print("\n" + "=" * 80)
    print("Throughput summary")
    print("=" * 80)
    print(build_throughput_summary(baseline_metrics, adpa_metrics))

    if args.export_csv:
        import pandas as pd
        pd.DataFrame(baseline_metrics).to_csv("baseline_metrics.csv", index=False)
        pd.DataFrame(adpa_metrics).to_csv("adpa_metrics.csv", index=False)
        print("\nExported baseline_metrics.csv and adpa_metrics.csv")


if __name__ == "__main__":
    main()
