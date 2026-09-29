"""
Synthetic transport event generator implementing the three-phase load
profile from §2.6.2:

    Phase 1 (Normal,  0-60s):  lambda_base = 20 events/sec
    Phase 2 (Peak,    60-120s): lambda_peak = 100 events/sec
    Phase 3 (Decline, 120-180s): lambda_low = 15 events/sec

Can run either:
  (a) as a Lambda invoked by experiment/run_experiment.py.py with a
      {"duration_seconds": N, "phase": "..."} payload for one phase burst, or
  (b) imported directly as a library by run_experiment.py.py for local-driven
      generation (recommended — gives precise client-side timing control
      not subject to Lambda's own cold starts).

Event type/priority mix per phase (calibrated to produce a realistic
CRITICAL : CONTINUOUS_MONITORING : HISTORICAL ratio of roughly 2% : 90% : 8%,
which is representative of real ITS telemetry compositions).
"""

from __future__ import annotations

import json
import logging
import os
import random
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from common.aws_clients import send_message_batch, queue_url
from common.models import TransportEvent, EventType, Priority

logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

# Event mix: (EventType, Priority, weight)
EVENT_MIX = [
    (EventType.GPS, Priority.LOW, 55),
    (EventType.TRAFFIC_SENSOR, Priority.MEDIUM, 25),
    (EventType.CAMERA_METADATA, Priority.LOW, 10),
    (EventType.HISTORICAL_LOG, Priority.LOW, 5),
    (EventType.TRAFFIC_REPORT, Priority.LOW, 3),
    (EventType.ACCIDENT, Priority.HIGH, 1),
    (EventType.EMERGENCY, Priority.HIGH, 0.5),
    (EventType.ROAD_HAZARD, Priority.HIGH, 0.5),
]

_TYPES = [m[0] for m in EVENT_MIX]
_PRIORITIES = [m[1] for m in EVENT_MIX]
_WEIGHTS = [m[2] for m in EVENT_MIX]


@dataclass
class LoadPhase:
    name: str
    lambda_rate: float          # events/sec
    duration_seconds: float


DEFAULT_PHASES: List[LoadPhase] = [
    LoadPhase("phase_1_normal", 20.0, 60.0),
    LoadPhase("phase_2_peak", 100.0, 60.0),
    LoadPhase("phase_3_decline", 15.0, 60.0),
]


def _sample_event(phase: LoadPhase, correlation_id: str) -> TransportEvent:
    idx = random.choices(range(len(_TYPES)), weights=_WEIGHTS, k=1)[0]
    event_type = _TYPES[idx]
    priority = _PRIORITIES[idx]

    payload = {
        "lat": round(random.uniform(46.40, 46.50), 6),
        "lon": round(random.uniform(30.70, 30.80), 6),
        "speed_kmh": round(random.uniform(0, 120), 1),
        "sensor_id": f"sensor-{random.randint(1, 500)}",
    }

    return TransportEvent.create(
        event_type=event_type,
        priority=priority,
        payload=payload,
        observed_lambda=phase.lambda_rate,
        experiment_phase=phase.name,
        correlation_id=correlation_id,
    )


def generate_events_for_phase(
    phase: LoadPhase,
    correlation_id: str,
    tick_seconds: float = 1.0,
) -> List[TransportEvent]:
    """Generates the full list of events for one phase (used for local dispatch)."""
    events: List[TransportEvent] = []
    n_ticks = int(phase.duration_seconds / tick_seconds)
    events_per_tick = phase.lambda_rate * tick_seconds

    for _ in range(n_ticks):
        n_events = int(events_per_tick)
        fractional = events_per_tick - n_events
        if random.random() < fractional:
            n_events += 1
        for _ in range(n_events):
            events.append(_sample_event(phase, correlation_id))

    return events


def dispatch_events_batch(
    events: List[TransportEvent],
    target_queue_url: str,
    batch_size: int = 10,
) -> int:
    """SQS send_message_batch supports max 10 messages per call."""
    sent = 0
    for i in range(0, len(events), batch_size):
        chunk = events[i : i + batch_size]
        entries = [
            {"Id": str(idx), "MessageBody": e.to_json()}
            for idx, e in enumerate(chunk)
        ]
        try:
            send_message_batch(target_queue_url, entries)
            sent += len(entries)
        except Exception:  # noqa: BLE001
            logger.exception("Failed to dispatch batch starting at index %d", i)
    return sent


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Optional Lambda entry point (invoked by run_experiment.py.py via boto3
    if you prefer server-side generation instead of local-driven dispatch).

    Payload:
        {
          "phase_name": "phase_2_peak",
          "lambda_rate": 100.0,
          "duration_seconds": 60,
          "correlation_id": "...",
          "target": "raw" | "baseline"
        }
    """
    phase = LoadPhase(
        name=event["phase_name"],
        lambda_rate=float(event["lambda_rate"]),
        duration_seconds=float(event["duration_seconds"]),
    )
    correlation_id = event["correlation_id"]
    target = event.get("target", "raw")

    events = generate_events_for_phase(phase, correlation_id)

    target_url = queue_url("RAW_QUEUE_URL" if target == "raw" else "BASELINE_QUEUE_URL")
    sent = dispatch_events_batch(events, target_url)

    return {"generated": len(events), "sent": sent, "phase": phase.name}
