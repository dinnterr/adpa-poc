"""
BATCH path processor — analogue of "Apache Spark batch job on Amazon EMR"
for HISTORICAL_ANALYTICAL events.

Triggered with a longer batching window (30s) and lower priority; this path
intentionally trades latency for throughput, exactly as described in §1.4
for pakketна обробка (batch processing).
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Dict, List
from decimal import Decimal

from common.aws_clients import table, s3_client
from common.models import TransportEvent
from metrics.metrics_recorder import record_metric_batch

logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

DATALAKE_BUCKET = os.environ.get("DATALAKE_BUCKET", "")


def _persist_batch_to_datalake(events: List[TransportEvent]) -> None:
    if not DATALAKE_BUCKET or not events:
        return
    payload = {
        "count": len(events),
        "batch_written_at": time.time(),
        "event_ids": [e.event_id for e in events],
        "types": list({e.type.value for e in events}),
    }
    key = f"batch/{int(time.time()*1000)}.json"
    try:
        s3_client().put_object(
            Bucket=DATALAKE_BUCKET,
            Key=key,
            Body=json.dumps(payload).encode("utf-8"),
            ContentType="application/json",
        )
    except Exception:  # noqa: BLE001
        logger.exception("Failed to write batch aggregate to S3")


def _persist_processed_events(events: List[TransportEvent]) -> None:
    t = table("PROCESSED_TABLE")
    with t.batch_writer() as batch:
        for e in events:
            batch.put_item(
                Item={
                    "pk": f"EVENT#{e.type.value}",
                    "sk": f"{e.timestamp:020.6f}#{e.event_id}",
                    "event_id": e.event_id,
                    "mode": "BATCH",
                    "priority": e.priority.value,
                    "payload": json.dumps(e.payload),
                    "processed_at": Decimal(str(time.time())),
                }
            )


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    records = event.get("Records", [])
    if not records:
        return {"batchItemFailures": []}

    transport_events: List[TransportEvent] = []
    decisions_by_event_id: Dict[str, Dict[str, Any]] = {}

    for record in records:
        try:
            env = json.loads(record["body"])
            te = TransportEvent.from_json(json.dumps(env["event"]))
            transport_events.append(te)
            decisions_by_event_id[te.event_id] = env["decision"]
        except Exception:  # noqa: BLE001
            logger.exception("Malformed batch envelope, skipping")

    start = time.time()
    _persist_batch_to_datalake(transport_events)
    _persist_processed_events(transport_events)
    end = time.time()

    metric_records = [
        dict(
            event_id=te.event_id,
            system="adpa",
            mode="BATCH",
            event_type=te.type.value,
            priority=te.priority.value,
            latency_seconds=end - te.timestamp,
            resource_units_at_processing=decisions_by_event_id[te.event_id]["resource_units"],
            experiment_phase=te.experiment_phase,
            correlation_id=te.correlation_id,
        )
        for te in transport_events
    ]
    record_metric_batch(metric_records)

    logger.info("BATCH processed %d events in %.4fs", len(transport_events), end - start)

    return {"batchItemFailures": []}
