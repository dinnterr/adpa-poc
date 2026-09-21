"""
Baseline (control group) processor — §2.6.1.

"Традиційний підхід, за якого всі вхідні події проходять через єдиний
конвеєр обробки з фіксованими параметрами. У якості базової моделі
обрано статичну micro-batch обробку з фіксованим інтервалом (5с) та
статично виділеним пулом обчислювальних ресурсів без використання
класифікатора подій."

Implementation:
  - ALL events, regardless of priority/type, go through ONE queue
    (BaselineQueue) with a FIXED 5-second batching window
    (MaximumBatchingWindowInSeconds: 5 in template.yaml).
  - The Lambda's ReservedConcurrentExecutions is FIXED at 4 (template.yaml)
    and NEVER changed — no adaptive scaling, no classification.
  - No routing by criticality: critical events wait in the same queue
    as GPS pings, which is exactly what produces the high latency for
    critical events observed in Table 2.1 / phase 2 of the experiment.
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Dict, List

from common.aws_clients import table
from common.models import TransportEvent
from metrics.metrics_recorder import record_metric_batch

logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

FIXED_RESOURCE_UNITS = 4  # must match ReservedConcurrentExecutions in template.yaml


def _persist_processed_events(events: List[TransportEvent]) -> None:
    t = table("PROCESSED_TABLE")
    with t.batch_writer() as batch:
        for e in events:
            batch.put_item(
                Item={
                    "pk": f"EVENT#{e.type.value}",
                    "sk": f"{e.timestamp:020.6f}#{e.event_id}",
                    "event_id": e.event_id,
                    "mode": "FIXED",
                    "priority": e.priority.value,
                    "payload": json.dumps(e.payload),
                    "processed_at": time.time(),
                }
            )


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    records = event.get("Records", [])
    if not records:
        return {"batchItemFailures": []}

    transport_events: List[TransportEvent] = []
    for record in records:
        try:
            transport_events.append(TransportEvent.from_json(record["body"]))
        except Exception:  # noqa: BLE001
            logger.exception("Malformed baseline event, skipping")

    start = time.time()
    _persist_processed_events(transport_events)
    end = time.time()

    metric_records = [
        dict(
            event_id=te.event_id,
            system="baseline",
            mode="FIXED",
            event_type=te.type.value,
            priority=te.priority.value,
            latency_seconds=end - te.timestamp,
            resource_units_at_processing=FIXED_RESOURCE_UNITS,
            experiment_phase=te.experiment_phase,
            correlation_id=te.correlation_id,
        )
        for te in transport_events
    ]
    record_metric_batch(metric_records)

    logger.info(
        "BASELINE processed %d events in %.4fs (fixed R=%d, fixed window=5s)",
        len(transport_events), end - start, FIXED_RESOURCE_UNITS,
    )

    return {"batchItemFailures": []}
