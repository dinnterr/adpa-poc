"""
Edge layer (§2.1, §2.2) — analogue of AWS IoT Greengrass local processing.

Since Greengrass requires a physical/virtual device registration and is not
part of the Free Tier, this Lambda simulates the *behaviour* of an edge node:
it performs a cheap local classification pass (is this event obviously
critical?) and, for genuinely critical events, could short-circuit local
alerting *before* forwarding to the cloud pipeline — exactly as described:

    "edge-вузол може сформувати повідомлення для систем управління
     транспортом без необхідності передачі всього потоку даних до
     центрального cloud-середовища"

For the PoC, all events are still forwarded to RawEventsQueue (so the ADPA
controller can classify/route them centrally and we can measure end-to-end
latency), but CRITICAL events are tagged with `edge_prefiltered=True` and a
local timestamp, letting us later measure the edge-processing overhead
component of total latency separately from cloud propagation delay.
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Dict

from common.aws_clients import send_message, queue_url
from common.models import TransportEvent, EventType, T_CRITICAL

logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))


def _local_is_critical(event: TransportEvent) -> bool:
    """Cheap local rule mirroring T_critical membership, without contacting the cloud."""
    return event.type in T_CRITICAL or event.priority.value == "HIGH"


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    API Gateway proxy entry point: POST /edge/ingest
    Body: JSON matching TransportEvent fields (type, priority, payload, ...)
    """
    try:
        body_raw = event.get("body") or "{}"
        if event.get("isBase64Encoded"):
            import base64
            body_raw = base64.b64decode(body_raw).decode("utf-8")
        body = json.loads(body_raw)

        transport_event = TransportEvent.create(
            event_type=EventType(body["type"]),
            priority=__import__("common.models.py", fromlist=["Priority"]).Priority(body.get("priority", "MEDIUM")),
            payload=body.get("payload", {}),
            observed_lambda=body.get("observed_lambda"),
            experiment_phase=body.get("experiment_phase"),
            correlation_id=body.get("correlation_id"),
        )

        edge_start = time.time()
        is_critical_locally = _local_is_critical(transport_event)
        edge_processing_time = time.time() - edge_start

        transport_event.payload["_edge_prefiltered"] = is_critical_locally
        transport_event.payload["_edge_processing_time"] = edge_processing_time
        transport_event.payload["_edge_forward_time"] = time.time()

        q_url = queue_url("RAW_QUEUE_URL")
        send_message(q_url, transport_event.to_json())

        logger.info(
            "Edge ingested event %s type=%s critical_local=%s edge_latency=%.5fs",
            transport_event.event_id, transport_event.type.value,
            is_critical_locally, edge_processing_time,
        )

        return {
            "statusCode": 202,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({
                "event_id": transport_event.event_id,
                "edge_prefiltered_critical": is_critical_locally,
                "edge_processing_time": edge_processing_time,
            }),
        }
    except Exception as exc:  # noqa: BLE001
        logger.exception("Edge ingestion failed: %s", exc)
        return {
            "statusCode": 400,
            "body": json.dumps({"error": str(exc)}),
        }
