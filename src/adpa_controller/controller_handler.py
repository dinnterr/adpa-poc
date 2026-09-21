"""
Main ADPA Lambda entry points.

1. lambda_handler                       -> triggered by SQS (RawEventsQueue)
                                            Implements the ADPA algorithm from §2.3.5
                                            (steps 1-19: classify, route, scale).

2. recalculate_thresholds_handler       -> triggered on a schedule (rate(1 minute))
                                            Implements §2.3.6 feedback loop (step 20-21).
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Dict, List

from common.aws_clients import send_message, get_queue_depth, queue_url
from common.models import (
    TransportEvent,
    SystemState,
    ProcessingDecision,
    ProcessingMode,
    MetricRecord,
)
from adpa_controller.classifier import classify, map_class_to_mode
from adpa_controller.interval import compute_delta_t
from adpa_controller.load_factor import compute_load_factor, decide_resource_units
from adpa_controller.lambda_estimator import record_arrival, estimate_lambda
from adpa_controller.state_store import (
    load_thresholds,
    save_thresholds,
    load_system_state,
    save_system_state,
)
from adpa_controller.scaler import apply_scaling_decision
from metrics.metrics_recorder import record_metric_batch, query_recent_metrics

logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

MODE_QUEUE_ENV = {
    ProcessingMode.STREAM: "STREAM_QUEUE_URL",
    ProcessingMode.MICRO_BATCH: "MICROBATCH_QUEUE_URL",
    ProcessingMode.BATCH: "BATCH_QUEUE_URL",
}

# Fixed CPU proxy model: we don't have real CPU% in Lambda, so we derive a
# proxy from *current concurrency utilisation* = active_estimate / R_t.
# This keeps the mathematical structure of S(t) intact while staying
# strictly within what Free Tier / Lambda actually exposes cheaply.


def _cpu_proxy(mode: ProcessingMode, queue_depth: int, resource_units: int) -> float:
    """
    Proxy for CPU_t: ratio of pending backlog to available resource capacity,
    saturating at 100%. This reflects genuine pressure on the allocated
    concurrency pool without requiring CloudWatch Lambda Insights (paid).
    """
    if resource_units <= 0:
        return 100.0
    utilisation = (queue_depth / resource_units) * 20.0  # empirical scaling factor
    return min(100.0, utilisation)


def _get_or_init_state(mode: ProcessingMode, queue_url_str: str, default_units: int) -> SystemState:
    state = load_system_state(mode.value)
    q_depth = get_queue_depth(queue_url_str)
    if state is None:
        state = SystemState(
            cpu_percent=0.0,
            mem_percent=0.0,
            queue_depth=q_depth,
            resource_units=default_units,
        )
    else:
        state.queue_depth = q_depth
    return state


def _process_single_event(event: TransportEvent, theta) -> ProcessingDecision:
    """Implements ADPA algorithm steps 1-17 of §2.3.5 for one event."""

    record_arrival(event.type.value)
    lambda_t = estimate_lambda(event.type.value)

    event_class = classify(event, theta, lambda_t)
    mode = map_class_to_mode(event_class)

    queue_env = MODE_QUEUE_ENV[mode]
    q_url = queue_url(queue_env)

    default_units = theta.R_min if mode != ProcessingMode.BATCH else theta.R_predefined_batch
    state = _get_or_init_state(mode, q_url, default_units)

    cpu_proxy = _cpu_proxy(mode, state.queue_depth, max(state.resource_units, 1))

    delta_t = None
    if mode == ProcessingMode.MICRO_BATCH:
        delta_t = compute_delta_t(
            queue_depth=state.queue_depth,
            cpu_percent=cpu_proxy,
            lambda_t=lambda_t,
            theta=theta,
        )

    load_factor = None
    resource_units = state.resource_units

    if mode in (ProcessingMode.STREAM, ProcessingMode.MICRO_BATCH):
        load_factor = compute_load_factor(
            cpu_percent=cpu_proxy,
            queue_depth=state.queue_depth,
            lambda_t=lambda_t,
            theta=theta,
        )
        resource_units = decide_resource_units(state.resource_units, load_factor, theta)

        if resource_units != state.resource_units:
            apply_scaling_decision(mode.value, resource_units)
    else:
        resource_units = theta.R_predefined_batch

    new_state = SystemState(
        cpu_percent=cpu_proxy,
        mem_percent=state.mem_percent,
        queue_depth=state.queue_depth,
        resource_units=resource_units,
    )
    save_system_state(mode.value, new_state)

    # --- Route the event (§2.3.5 step 18) ---
    envelope = {
        "event": json.loads(event.to_json()),
        "decision": {
            "mode": mode.value,
            "delta_t": delta_t,
            "resource_units": resource_units,
        },
        "controller_dispatch_time": time.time(),
    }
    send_message(q_url, json.dumps(envelope))

    return ProcessingDecision(
        event_class=event_class,
        mode=mode,
        resource_units=resource_units,
        delta_t=delta_t,
        load_factor=load_factor,
    )


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    SQS-triggered entry point. `event["Records"]` contains up to BatchSize
    raw TransportEvent JSON messages from RawEventsQueue.
    """
    theta = load_thresholds()
    records: List[Dict[str, Any]] = event.get("Records", [])

    processed = 0
    batch_item_failures = []

    for record in records:
        try:
            body = record["body"]
            transport_event = TransportEvent.from_json(body)
            decision = _process_single_event(transport_event, theta)
            logger.info(
                "Routed event %s -> class=%s mode=%s R=%d delta_t=%s",
                transport_event.event_id,
                decision.event_class.value,
                decision.mode.value,
                decision.resource_units,
                decision.delta_t,
            )
            processed += 1
        except Exception as exc:  # noqa: BLE001
            logger.exception("Failed to process record: %s", exc)
            batch_item_failures.append({"itemIdentifier": record.get("messageId")})

    return {"processed": processed, "batchItemFailures": batch_item_failures}


def recalculate_thresholds_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    §2.3.6 — Feedback loop.

    theta_up_new = theta_up_old + alpha * (SLA_target - P95(latency in W_tau))

    Runs on schedule(rate(1 minute)). Reads recent 'adpa' latency samples
    for CRITICAL-class events from MetricsTable and nudges theta_up.
    """
    theta = load_thresholds()

    recent = query_recent_metrics(
        system="adpa",
        window_seconds=theta.T_recalc_seconds * 5,  # W_tau: look back further than the recalc period
        mode_filter="STREAM",  # CRITICAL events are always routed to STREAM
    )

    if not recent:
        logger.info("No recent CRITICAL/STREAM metrics; skipping threshold recalibration.")
        return {"updated": False, "reason": "no_data"}

    latencies = sorted(m["latency_seconds"] for m in recent)
    p95_index = max(0, int(0.95 * (len(latencies) - 1)))
    p95_latency = latencies[p95_index]

    old_theta_up = theta.theta_up
    delta = theta.alpha * (theta.SLA_target - p95_latency)
    new_theta_up = old_theta_up + delta
    new_theta_up = max(theta.theta_up_min, min(theta.theta_up_max, new_theta_up))

    theta.theta_up = new_theta_up
    save_thresholds(theta)

    logger.info(
        "Feedback loop: P95_latency=%.3fs SLA_target=%.3fs theta_up %.3f -> %.3f",
        p95_latency, theta.SLA_target, old_theta_up, new_theta_up,
    )

    return {
        "updated": True,
        "p95_latency": p95_latency,
        "old_theta_up": old_theta_up,
        "new_theta_up": new_theta_up,
        "sample_size": len(recent),
    }
