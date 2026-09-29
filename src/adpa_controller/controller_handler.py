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
from collections import Counter, defaultdict
from typing import Any, Dict, List, Tuple

from common.aws_clients import send_message_batch, get_queue_depth, queue_url
from common.models import (
    TransportEvent,
    SystemState,
    EventClass,
    ProcessingMode,
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

_SQS_SEND_BATCH_LIMIT = 10  # AWS SendMessageBatch hard limit


def _chunked(items: List[Any], size: int) -> List[List[Any]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


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


# One (record, event, event_class, lambda_t) tuple per event routed to a mode.
_ModeItem = Tuple[Dict[str, Any], TransportEvent, EventClass, float]


def _classify_and_group(
    parsed: List[Tuple[Dict[str, Any], TransportEvent]], theta
) -> Dict[ProcessingMode, List[_ModeItem]]:
    """Implements ADPA algorithm steps 1-8 of §2.3.5 for a WHOLE SQS batch.

    lambda(type_i) is a property of the stream, not of a single event, so
    it — and the arrival-window update it depends on — only needs to be
    computed once per distinct event type in the batch, not once per event.
    Doing 2 DynamoDB round-trips per event here (as opposed to once per type)
    is what previously made "dispatch" latency scale with batch/backlog size
    instead of staying flat like the baseline pipeline's.
    """
    type_counts = Counter(te.type.value for _, te in parsed)
    for event_type, count in type_counts.items():
        record_arrival(event_type, count)
    lambda_by_type = {event_type: estimate_lambda(event_type) for event_type in type_counts}

    by_mode: Dict[ProcessingMode, List[_ModeItem]] = defaultdict(list)
    for record, te in parsed:
        lambda_t = lambda_by_type[te.type.value]
        event_class = classify(te, theta, lambda_t)
        mode = map_class_to_mode(event_class)
        by_mode[mode].append((record, te, event_class, lambda_t))
    return by_mode


def _dispatch_mode_batch(mode: ProcessingMode, mode_items: List[_ModeItem], theta) -> int:
    """Implements ADPA algorithm steps 9-18 of §2.3.5 for all same-mode events
    in this SQS batch at once: one queue-depth/state read, one scaling
    decision and one state write per mode per invocation (instead of per
    event), then a single SendMessageBatch call per queue.
    """
    queue_env = MODE_QUEUE_ENV[mode]
    q_url = queue_url(queue_env)

    default_units = theta.R_min if mode != ProcessingMode.BATCH else theta.R_predefined_batch
    state = _get_or_init_state(mode, q_url, default_units)

    cpu_proxy = _cpu_proxy(mode, state.queue_depth, max(state.resource_units, 1))

    if mode in (ProcessingMode.STREAM, ProcessingMode.MICRO_BATCH):
        # Representative lambda_t for this mode's slice of the batch: the
        # busiest of the event types routed here, so scaling reacts to the
        # heaviest contributor rather than diluting it with lighter ones.
        representative_lambda = max(lambda_t for _, _, _, lambda_t in mode_items)
        load_factor = compute_load_factor(
            cpu_percent=cpu_proxy,
            queue_depth=state.queue_depth,
            lambda_t=representative_lambda,
            theta=theta,
        )
        resource_units = decide_resource_units(state.resource_units, load_factor, theta)
        # NOTE: the decided resource_units is only persisted to SystemStateTable
        # here (cheap DynamoDB write). The *real* AWS ReservedConcurrentExecutions
        # change is applied out-of-band by recalculate_thresholds_handler(), never
        # synchronously in this path — PutFunctionConcurrency is a slow
        # control-plane call and would otherwise stall this Lambda invocation (and
        # every other message in its SQS batch) for seconds, directly inflating
        # the very latency this controller is supposed to minimise.
    else:
        resource_units = theta.R_predefined_batch

    new_state = SystemState(
        cpu_percent=cpu_proxy,
        mem_percent=state.mem_percent,
        queue_depth=state.queue_depth,
        resource_units=resource_units,
    )
    save_system_state(mode.value, new_state)

    # --- Route the events (§2.3.5 step 18) ---
    controller_dispatch_time = time.time()
    entries = []
    for idx, (record, te, event_class, lambda_t) in enumerate(mode_items):
        delta_t = None
        if mode == ProcessingMode.MICRO_BATCH:
            delta_t = compute_delta_t(
                queue_depth=state.queue_depth,
                cpu_percent=cpu_proxy,
                lambda_t=lambda_t,
                theta=theta,
            )
        envelope = {
            "event": json.loads(te.to_json()),
            "decision": {
                "mode": mode.value,
                "delta_t": delta_t,
                "resource_units": resource_units,
            },
            "controller_dispatch_time": controller_dispatch_time,
        }
        entries.append({"Id": str(idx), "MessageBody": json.dumps(envelope)})
        logger.info(
            "Routed event %s -> class=%s mode=%s R=%d delta_t=%s",
            te.event_id, event_class.value, mode.value, resource_units, delta_t,
        )

    for chunk in _chunked(entries, _SQS_SEND_BATCH_LIMIT):
        resp = send_message_batch(q_url, chunk)
        failed = resp.get("Failed")
        if failed:
            logger.warning("SendMessageBatch had %d partial failures on %s: %s", len(failed), q_url, failed)

    return len(entries)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    SQS-triggered entry point. `event["Records"]` contains up to BatchSize
    raw TransportEvent JSON messages from RawEventsQueue.
    """
    theta = load_thresholds()
    records: List[Dict[str, Any]] = event.get("Records", [])

    parsed: List[Tuple[Dict[str, Any], TransportEvent]] = []
    batch_item_failures = []

    for record in records:
        try:
            parsed.append((record, TransportEvent.from_json(record["body"])))
        except Exception as exc:  # noqa: BLE001
            logger.exception("Failed to parse record: %s", exc)
            batch_item_failures.append({"itemIdentifier": record.get("messageId")})

    if not parsed:
        return {"processed": 0, "batchItemFailures": batch_item_failures}

    try:
        by_mode = _classify_and_group(parsed, theta)
    except Exception as exc:  # noqa: BLE001
        # Nothing has been dispatched yet at this point, so it's safe to fail
        # every record in the batch for a clean SQS retry.
        logger.exception("Failed to classify/route event batch: %s", exc)
        batch_item_failures.extend({"itemIdentifier": record.get("messageId")} for record, _ in parsed)
        return {"processed": 0, "batchItemFailures": batch_item_failures}

    processed = 0
    for mode, mode_items in by_mode.items():
        try:
            processed += _dispatch_mode_batch(mode, mode_items, theta)
        except Exception as exc:  # noqa: BLE001
            # Scoped to this mode only: other modes in the same invocation may
            # have already been dispatched, and must not be retried/duplicated.
            logger.exception("Failed to dispatch mode=%s batch: %s", mode.value, exc)
            batch_item_failures.extend(
                {"itemIdentifier": record.get("messageId")} for record, _, _, _ in mode_items
            )

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

    # Reconcile real Lambda concurrency with the resource_units the per-batch
    # controller has decided (§2.3.4/§2.3.5). Deliberately done here, on the
    # 1-minute schedule, rather than synchronously in _dispatch_mode_batch —
    # PutFunctionConcurrency is a slow AWS control-plane call and must never
    # sit on the critical event-processing path.
    for mode in (ProcessingMode.STREAM, ProcessingMode.MICRO_BATCH):
        mode_state = load_system_state(mode.value)
        if mode_state is not None:
            apply_scaling_decision(mode.value, mode_state.resource_units)

    return {
        "updated": True,
        "p95_latency": p95_latency,
        "old_theta_up": old_theta_up,
        "new_theta_up": new_theta_up,
        "sample_size": len(recent),
    }
