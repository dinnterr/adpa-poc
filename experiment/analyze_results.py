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


def _pct_str(improvement: float) -> str:
    """Signed percentage string that never produces the double-signed '+-12.3%'."""
    if improvement != improvement:  # NaN
        return "N/A"
    return f"{improvement:+.1f}%"


def _percentile(values: List[float], p: float) -> float:
    return float(np.percentile(values, p)) if values else float("nan")


def build_table_2_1(baseline_metrics: List[Dict[str, Any]], adpa_metrics: List[Dict[str, Any]]) -> str:
    """
    Table 2.1 — Latency comparison of critical events per phase.

    Reports mean AND p95/p99 rather than mean alone: latency distributions
    under load are typically right-skewed (a few slow outliers), and the
    system's own SLA/feedback loop (adpa_controller.config.py.py.Thresholds.SLA_target)
    is itself defined against P95, so mean-only comparison can disagree with
    what the system is actually being tuned against.
    """
    baseline_critical = _filter_critical(baseline_metrics)
    adpa_critical = _filter_critical(adpa_metrics)

    rows = []
    for phase in PHASE_NAMES:
        b_lat = [m["latency_seconds"] for m in baseline_critical if m["experiment_phase"] == phase]
        a_lat = [m["latency_seconds"] for m in adpa_critical if m["experiment_phase"] == phase]

        b_mean = float(np.mean(b_lat)) if b_lat else float("nan")
        a_mean = float(np.mean(a_lat)) if a_lat else float("nan")
        b_p95 = _percentile(b_lat, 95)
        a_p95 = _percentile(a_lat, 95)

        mean_improvement = (
            ((b_mean - a_mean) / b_mean) * 100.0 if b_lat and a_lat and b_mean > 0 else float("nan")
        )
        p95_improvement = (
            ((b_p95 - a_p95) / b_p95) * 100.0 if b_lat and a_lat and b_p95 > 0 else float("nan")
        )

        rows.append([
            phase,
            f"{PHASE_LAMBDA[phase]:.0f}",
            f"{b_mean:.2f}" if b_lat else "N/A",
            f"{a_mean:.2f}" if a_lat else "N/A",
            _pct_str(mean_improvement),
            f"{b_p95:.2f}" if b_lat else "N/A",
            f"{a_p95:.2f}" if a_lat else "N/A",
            _pct_str(p95_improvement),
            len(b_lat),
            len(a_lat),
        ])

    headers = [
        "Phase", "Intensity (ev/s)",
        "Baseline mean (s)", "ADPA mean (s)", "Mean impr. (%)",
        "Baseline p95 (s)", "ADPA p95 (s)", "p95 impr. (%)",
        "n (baseline)", "n (adpa)",
    ]
    return tabulate(rows, headers=headers, tablefmt="grid")


def build_table_latency_breakdown(baseline_metrics: List[Dict[str, Any]], adpa_metrics: List[Dict[str, Any]]) -> str:
    """
    Table 2.1b — Where critical-event latency is actually spent, per phase.

    dispatch = event created -> controller routed it (classification overhead;
               always 0 for baseline, which has no controller hop)
    queue    = routed -> processor picked it up (real queueing/backlog wait —
               this is where a resource-scaling or backlog problem shows up)
    handler  = processor picked it up -> processor finished (actual work)

    Breaking the single end-to-end number down this way is what makes a
    latency regression attributable instead of just "ADPA is slower".
    """
    rows = []
    for label, metrics in [("baseline", baseline_metrics), ("adpa", adpa_metrics)]:
        critical = _filter_critical(metrics)
        for phase in PHASE_NAMES:
            sample = [m for m in critical if m["experiment_phase"] == phase]
            dispatch = [m["dispatch_latency_seconds"] for m in sample if m.get("dispatch_latency_seconds") is not None]
            queue = [m["queue_latency_seconds"] for m in sample if m.get("queue_latency_seconds") is not None]
            handler = [m["handler_duration_seconds"] for m in sample if m.get("handler_duration_seconds") is not None]

            rows.append([
                label, phase,
                f"{np.mean(dispatch):.3f}" if dispatch else "N/A",
                f"{np.mean(queue):.3f}" if queue else "N/A",
                f"{np.mean(handler):.3f}" if handler else "N/A",
            ])

    headers = ["System", "Phase", "Dispatch (s)", "Queue wait (s)", "Handler (s)"]
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


def build_table_resource_efficiency(baseline_metrics: List[Dict[str, Any]], adpa_metrics: List[Dict[str, Any]]) -> str:
    """
    Table 2.3 — Cost-adjusted latency for critical events per phase.

    Raw latency alone isn't a fair comparison when the two systems are
    provisioned with a different number of resource units (Table 2.2 already
    shows ADPA using far fewer). This multiplies mean latency by the mean
    resource_units actually allocated at processing time, giving a rough
    latency-per-unit-of-capacity figure: a system that matches baseline's
    latency with 1/4 the resource units is doing strictly better than the
    raw latency number alone would suggest, and vice versa.
    """
    baseline_critical = _filter_critical(baseline_metrics)
    adpa_critical = _filter_critical(adpa_metrics)

    rows = []
    for phase in PHASE_NAMES:
        b_sample = [m for m in baseline_critical if m["experiment_phase"] == phase]
        a_sample = [m for m in adpa_critical if m["experiment_phase"] == phase]

        b_lat = [m["latency_seconds"] for m in b_sample]
        a_lat = [m["latency_seconds"] for m in a_sample]
        b_units = [m["resource_units_at_processing"] for m in b_sample]
        a_units = [m["resource_units_at_processing"] for m in a_sample]

        b_cost = float(np.mean(b_lat)) * float(np.mean(b_units)) if b_sample else float("nan")
        a_cost = float(np.mean(a_lat)) * float(np.mean(a_units)) if a_sample else float("nan")
        improvement = (
            ((b_cost - a_cost) / b_cost) * 100.0 if b_sample and a_sample and b_cost > 0 else float("nan")
        )

        rows.append([
            phase,
            f"{np.mean(b_units):.1f}" if b_units else "N/A",
            f"{np.mean(a_units):.1f}" if a_units else "N/A",
            f"{b_cost:.2f}" if b_sample else "N/A",
            f"{a_cost:.2f}" if a_sample else "N/A",
            _pct_str(improvement),
        ])

    headers = [
        "Phase", "Baseline units", "ADPA units",
        "Baseline cost (s·units)", "ADPA cost (s·units)", "Cost impr. (%)",
    ]
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
    print("Table 2.1b — Latency breakdown (dispatch / queue wait / handler)")
    print("=" * 80)
    print(build_table_latency_breakdown(baseline_metrics, adpa_metrics))

    print("\n" + "=" * 80)
    print("Table 2.2 — Динаміка виділення обчислювальних ресурсів")
    print("=" * 80)
    print(build_table_2_2(baseline_metrics, adpa_metrics))

    print("\n" + "=" * 80)
    print("Table 2.3 — Resource-cost-adjusted latency")
    print("=" * 80)
    print(build_table_resource_efficiency(baseline_metrics, adpa_metrics))

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
