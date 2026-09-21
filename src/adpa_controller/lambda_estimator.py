"""
Estimates lambda(type_i) — the observed arrival intensity per event type —
from a sliding window of recent arrival timestamps kept in DynamoDB.

This is required because lambda_i is a *stream* property, not a property
of a single event, yet classify() (§2.3.2) needs a live estimate of it.

Implementation: a simple counting window using DynamoDB atomic counters
bucketed by second, read back over the last WINDOW_SECONDS.
"""

from __future__ import annotations

import time
from typing import Dict

from common.aws_clients import table

WINDOW_SECONDS = 5


def record_arrival(event_type: str) -> None:
    """Increment the per-second, per-type arrival counter (TTL'd)."""
    t = table("SYSTEM_STATE_TABLE")
    bucket = int(time.time())
    t.update_item(
        Key={"pk": f"ARRIVAL#{event_type}", "sk": str(bucket)},
        UpdateExpression="ADD cnt :one SET ttl = :ttl",
        ExpressionAttributeValues={
            ":one": 1,
            ":ttl": bucket + WINDOW_SECONDS + 5,
        },
    )


def estimate_lambda(event_type: str, window_seconds: int = WINDOW_SECONDS) -> float:
    """
    Returns events/sec for `event_type` averaged over the last `window_seconds`.
    Uses a DynamoDB Query on the partition key with a sort-key range condition —
    O(window_seconds) reads, acceptable given window_seconds is small (5s default).
    """
    t = table("SYSTEM_STATE_TABLE")
    now = int(time.time())
    lo = now - window_seconds

    resp = t.query(
        KeyConditionExpression="pk = :pk AND sk BETWEEN :lo AND :hi",
        ExpressionAttributeValues={
            ":pk": f"ARRIVAL#{event_type}",
            ":lo": str(lo),
            ":hi": str(now),
        },
    )
    total = sum(int(item.get("cnt", 0)) for item in resp.get("Items", []))
    return total / max(window_seconds, 1)
