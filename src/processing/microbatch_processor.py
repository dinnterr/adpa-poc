"""
MICRO_BATCH path processor — analogue of "Spark Streaming on Amazon EMR"
for CONTINUOUS_MONITORING events (GPS, traffic sensors, camera metadata).

Triggered by SQS with BatchSize=50 and MaximumBatchingWindowInSeconds=5
(template.yaml) — Lambda's own SQS batching window approximates the
delta_t computed by interval.py. The *actual* delta_t value chosen by the
controller for each event is carried in the message envelope for
experimental logging (we can't dynamically change the Lambda's own SQS
event-source-mapping batching window per-message, so we log the *intended*
delta_t and measure the *effective* one from timestamps — see analysis).
"""

from __future__ import annotations

import json
import logging
import os
import time
from collections import defaultdict
from typing import Any, Dict, List

from common.aws_clients import table, s3_client
from common.models import TransportEvent
from metrics.metrics_recorder import record_metric_batch

logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

DATALAKE_BUCKET = os.environ.get("DATALAKE_BUCKET", "")


def _aggregate_by_type(events: List[TransportEvent]) -> Dict[str, List[TransportEvent]]:
    groups: Dict[str, List[TransportEvent]] = defaultdict(list)
    for e in events:
        groups[e.type.value].append(e)
    return groups


def _persist_aggregate_to_datalake(event_type: str, events: List[TransportEvent]) -> None:
    """
    Writes one aggregated micro-batch object to S3 (data lake) — mirrors
    "Spark writing aggregated results to Amazon S3" in the thesis pipeline.
    """
    if not DATALAKE_BUCKET:
        return

    payload = {
        "event_type": event_type,
        "count": len(events),
        "batch_written_at": time.time(),
        "event_ids": [e.event_id for e in events],
    }
    key = f"microbatch/{event_type}/{int(time.time()*1000)}.json"
    try:
        s3_client().put_object(
            Bucket=DATALAKE_BUCKET,
            Key=key,
            Body=json.dumps(payload).encode("utf-8"),
            ContentType="application/json",
        )
    except Exception:  # noqa: BLE001
        logger.exception("Failed to write micro-batch aggregate to S3")


def _persist_processed_events(events: List[TransportEvent], mode: str) -> None:
    t = table("PROCESSED_TABLE")
    with t.batch_writer() as batch:
        for e in events:
            batch.put_item(
                Item={
                    "pk": f"EVENT#{e.type.value}",
                    "sk": f"{e.timestamp:020.6f}#{e.event_id}",
                    "event_id": e.event_id,
                    "mode": mode,
                    "priority": e.priority.value,
                    "payload": json.dumps(e.payload),
                    "processed_at": time.time(),
                }
            )


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    records = event.get("Records", [])
    if not records:
        return {"batchItemFailures": []}

    envelopes = []
    for record in records:
        try:
            envelopes.append(json.loads(record["body"]))
        except Exception:  # noqa: BLE001
            logger.exception("Malformed micro-batch envelope, skipping")

    transport_events: List[TransportEvent] = []
    decisions_by_event_id: Dict[str, Dict[str, Any]] = {}

    for env in envelopes:
        te = TransportEvent.from_json(json.dumps(env["event"]))
        transport_events.append(te)
        decisions_by_event_id[te.event_id] = env["decision"]

    batch_processing_start = time.time()
    grouped = _aggregate_by_type(transport_events)

    for event_type, events in grouped.items():
        _persist_aggregate_to_datalake(event_type, events)

    _persist_processed_events(transport_events, mode="MICRO_BATCH")

    batch_processing_end = time.time()

    metric_records = []
    for te in transport_events:
        decision = decisions_by_event_id[te.event_id]
        latency = batch_processing_end - te.timestamp
        metric_records.append(
            dict(
                event_id=te.event_id,
                system="adpa",
                mode="MICRO_BATCH",
                event_type=te.type.value,
                priority=te.priority.value,
                latency_seconds=latency,
                resource_units_at_processing=decision["resource_units"],
                experiment_phase=te.experiment_phase,
                correlation_id=te.correlation_id,
            )
        )

    record_metric_batch(metric_records)

    logger.info(
        "MICRO_BATCH processed %d events across %d types in %.4fs",
        len(transport_events), len(grouped), batch_processing_end - batch_processing_start,
    )

    return {"batchItemFailures": []}
