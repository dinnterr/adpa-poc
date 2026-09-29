"""
STREAM path processor — analogue of "Amazon Kinesis Data Streams + AWS Lambda"
for CRITICAL events (§2.3.2 table, §2.2).

Triggered by SQS with BatchSize=1 (see template.yaml.yaml) to minimise added
latency — each critical event is processed the instant it's dequeued,
with no batching window.
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Dict
from decimal import Decimal

from common.aws_clients import table
from common.models import TransportEvent
from metrics.metrics_recorder import record_metric

logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))


def _persist_processed_event(transport_event: TransportEvent, mode: str) -> None:
    """Writes the final processed record — analogue of writing to Redshift/Data Lake."""
    t = table("PROCESSED_TABLE")
    t.put_item(
        Item={
            "pk": f"EVENT#{transport_event.type.value}",
            "sk": f"{transport_event.timestamp:020.6f}#{transport_event.event_id}",
            "event_id": transport_event.event_id,
            "mode": mode,
            "priority": transport_event.priority.value,
            "payload": json.dumps(transport_event.payload),
            "processed_at": Decimal(str(time.time())),
        }
    )


def _handle_single_record(record: Dict[str, Any]) -> None:
    body = json.loads(record["body"])
    raw_event = body["event"]
    decision = body["decision"]
    dispatch_time = body["controller_dispatch_time"]

    transport_event = TransportEvent.from_json(json.dumps(raw_event))

    processing_start = time.time()

    # --- Simulated "business logic": anomaly check / alert formation ---
    # (kept intentionally cheap — the thesis's point is architecture, not ML)
    is_anomaly = transport_event.type.value in ("ACCIDENT", "EMERGENCY", "ROAD_HAZARD")

    _persist_processed_event(transport_event, decision["mode"])

    processing_end = time.time()

    end_to_end_latency = processing_end - transport_event.timestamp
    dispatch_latency = dispatch_time - transport_event.timestamp
    queue_latency = processing_start - dispatch_time
    handler_duration = processing_end - processing_start

    record_metric(
        event_id=transport_event.event_id,
        system="adpa",
        mode=decision["mode"],
        event_type=transport_event.type.value,
        priority=transport_event.priority.value,
        latency_seconds=end_to_end_latency,
        resource_units_at_processing=decision["resource_units"],
        experiment_phase=transport_event.experiment_phase,
        correlation_id=transport_event.correlation_id,
        dispatch_latency_seconds=dispatch_latency,
        queue_latency_seconds=queue_latency,
        handler_duration_seconds=handler_duration,
    )

    logger.info(
        "STREAM processed event %s latency=%.4fs anomaly=%s dispatch=%.4fs queue=%.4fs handler=%.4fs",
        transport_event.event_id, end_to_end_latency, is_anomaly,
        dispatch_latency, queue_latency, handler_duration,
    )


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    records = event.get("Records", [])
    failures = []

    for record in records:
        try:
            _handle_single_record(record)
        except Exception as exc:  # noqa: BLE001
            logger.exception("STREAM processing failed: %s", exc)
            failures.append({"itemIdentifier": record.get("messageId")})

    return {"batchItemFailures": failures}
