"""
Generates matplotlib charts from the CSV exports produced by analyze_results.py
(--export-csv flag) — latency-over-time and resource-units-over-time plots,
useful for illustrating §2.6 in the thesis defense presentation.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt


def plot_latency_over_time(baseline_csv: str, adpa_csv: str, out_path: str):
    b = pd.read_csv(baseline_csv)
    a = pd.read_csv(adpa_csv)

    b = b.sort_values("processed_at")
    a = a.sort_values("processed_at")

    b["t_rel"] = b["processed_at"] - b["processed_at"].min()
    a["t_rel"] = a["processed_at"] - a["processed_at"].min()

    b_crit = b[b["priority"] == "HIGH"]
    a_crit = a[a["priority"] == "HIGH"]

    plt.figure(figsize=(12, 6))
    plt.scatter(b_crit["t_rel"], b_crit["latency_seconds"], s=8, alpha=0.5, label="Baseline (CRITICAL)")
    plt.scatter(a_crit["t_rel"], a_crit["latency_seconds"], s=8, alpha=0.5, label="ADPA (CRITICAL)")
    plt.axvspan(60, 120, color="red", alpha=0.05, label="Peak phase")
    plt.xlabel("Time since experiment start (s)")
    plt.ylabel("Latency (s)")
    plt.title("Critical event latency: Baseline vs ADPA")
    plt.legend()
    plt.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    print(f"Saved {out_path}")


def plot_resource_units_over_time(baseline_csv: str, adpa_csv: str, out_path: str):
    b = pd.read_csv(baseline_csv)
    a = pd.read_csv(adpa_csv)

    b = b.sort_values("processed_at")
    a = a.sort_values("processed_at")

    b["t_rel"] = b["processed_at"] - b["processed_at"].min()
    a["t_rel"] = a["processed_at"] - a["processed_at"].min()

    plt.figure(figsize=(12, 6))
    plt.plot(b["t_rel"], b["resource_units_at_processing"], label="Baseline (fixed R)", alpha=0.7)
    plt.plot(a["t_rel"], a["resource_units_at_processing"], label="ADPA (adaptive R)", alpha=0.7)
    plt.axvspan(60, 120, color="red", alpha=0.05, label="Peak phase")
    plt.xlabel("Time since experiment start (s)")
    plt.ylabel("Resource units (R_t)")
    plt.title("Resource allocation dynamics: Baseline vs ADPA")
    plt.legend()
    plt.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    print(f"Saved {out_path}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline-csv", default="baseline_metrics.csv")
    parser.add_argument("--adpa-csv", default="adpa_metrics.csv")
    parser.add_argument("--output-dir", default=".")
    args = parser.parse_args()

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    plot_latency_over_time(args.baseline_csv, args.adpa_csv, str(out_dir / "latency_comparison.png"))
    plot_resource_units_over_time(args.baseline_csv, args.adpa_csv, str(out_dir / "resource_dynamics.png"))


if __name__ == "__main__":
    main()
