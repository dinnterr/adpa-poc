"""
Drives the §2.6 experiment: generates the same three-phase synthetic load
against BOTH the baseline pipeline and the ADPA pipeline, in separate runs
identified by distinct correlation_ids, then leaves the results in
MetricsTable for analyze_results.py to summarise into Table 2.1 / Table 2.2.

Usage:
    python experiment/run_experiment.py --system baseline
    python experiment/run_experiment.py --system adpa
    python experiment/run_experiment.py --system both

Requires environment variables (export these from `sam deploy` outputs):
    RAW_QUEUE_URL
    BASELINE_QUEUE_URL
    AWS_REGION
"""

from __future__ import annotations

import argparse
import os
import sys
import time
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from common.aws_clients import queue_url  # noqa: E402
from ingestion.event_generator import (  # noqa: E402
    DEFAULT_PHASES,
    generate_events_for_phase,
    dispatch_events_batch,
)

import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("run_experiment")


def run_against_target(target_env_var: str, system_label: str, correlation_id: str, tick_seconds: float = 1.0):
    """
    Generates and dispatches events phase by phase, throttled tick-by-tick so
    the *actual* wall-clock arrival rate matches lambda_rate (events/sec) as
    closely as SQS API latency allows — this reproduces real arrival-time
    jitter rather than firing everything instantly, which matters for Q_t
    and LF(t) to behave meaningfully.
    """
    target_url = queue_url(target_env_var)
    logger.info("=== Starting run: system=%s correlation_id=%s target=%s ===",
                system_label, correlation_id, target_url)

    total_sent = 0
    experiment_start = time.time()

    for phase in DEFAULT_PHASES:
        logger.info("--- Phase: %s | lambda=%.1f ev/s | duration=%.0fs ---",
                    phase.name, phase.lambda_rate, phase.duration_seconds)

        n_ticks = int(phase.duration_seconds / tick_seconds)
        events_per_tick_target = phase.lambda_rate * tick_seconds

        for tick in range(n_ticks):
            tick_start = time.time()

            # Generate this tick's worth of events with correct arrival timestamps
            from ingestion.event_generator import _sample_event  # local import: internal helper
            n_events = int(events_per_tick_target)
            import random
            if random.random() < (events_per_tick_target - n_events):
                n_events += 1

            events = [_sample_event(phase, correlation_id) for _ in range(n_events)]
            sent = dispatch_events_batch(events, target_url)
            total_sent += sent

            elapsed = time.time() - tick_start
            sleep_time = max(0.0, tick_seconds - elapsed)
            time.sleep(sleep_time)

            if tick % 10 == 0:
                logger.info("  tick %d/%d: sent %d events (phase %s)",
                             tick, n_ticks, sent, phase.name)

    experiment_end = time.time()
    logger.info(
        "=== Run complete: system=%s total_sent=%d wall_clock=%.1fs ===",
        system_label, total_sent, experiment_end - experiment_start,
    )
    return total_sent, experiment_end - experiment_start


def main():
    parser = argparse.ArgumentParser(description="Run ADPA vs Baseline experiment (§2.6)")
    parser.add_argument("--system", choices=["baseline", "adpa", "both"], default="both")
    parser.add_argument("--tick-seconds", type=float, default=1.0)
    parser.add_argument("--cooldown-seconds", type=float, default=30.0,
                         help="Wait time between baseline and adpa runs to let queues drain")
    args = parser.parse_args()

    run_id = uuid.uuid4().hex[:8]

    results = {}

    if args.system in ("baseline", "both"):
        cid = f"baseline-{run_id}"
        sent, wall = run_against_target("BASELINE_QUEUE_URL", "baseline", cid, args.tick_seconds)
        results["baseline"] = {"correlation_id": cid, "sent": sent, "wall_clock": wall}

        if args.system == "both":
            logger.info("Cooling down for %.0fs before ADPA run...", args.cooldown_seconds)
            time.sleep(args.cooldown_seconds)

    if args.system in ("adpa", "both"):
        cid = f"adpa-{run_id}"
        sent, wall = run_against_target("RAW_QUEUE_URL", "adpa", cid, args.tick_seconds)
        results["adpa"] = {"correlation_id": cid, "sent": sent, "wall_clock": wall}

    print("\n=== EXPERIMENT SUMMARY ===")
    for system, r in results.items():
        print(f"{system}: correlation_id={r['correlation_id']} sent={r['sent']} wall_clock={r['wall_clock']:.1f}s")

    print("\nWait ~60-120s for queues to fully drain, then run:")
    for system, r in results.items():
        print(f"  python experiment/analyze_results.py --correlation-id {r['correlation_id']} --system {system}")


if __name__ == "__main__":
    main()
