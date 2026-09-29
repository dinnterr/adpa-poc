"""
Writes/reads MetricRecord rows to/from DynamoDB (MetricsTable).

Table layout:
    pk = "METRIC#<system>#<mode>"       sk = "<timestamp>#<event_id>"

This layout allows efficient range queries by (system, mode) + time window,
which is exactly what recalculate_thresholds_handler() and
experiment/analyze_results.py.py need.
"""

from __future__ import annotations

import time
from decimal import Decimal
from typing import Any, Dict, List, Optional

from common.aws_clients import table


def _to_decimal(obj):
    if isinstance(obj, dict):
        return {k: _to_decimal(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_to_decimal(v) for v in obj]
    if isinstance(obj, float):
        return Decimal(str(obj))
    return obj


def _from_decimal(obj):
    if isinstance(obj, dict):
        return {k: _from_decimal(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_from_decimal(v) for v in obj]
    if isinstance(obj, Decimal):
        return float(obj)
    return obj


def record_metric(
    event_id: str,
    system: str,
    mode: str,
    event_type: str,
    priority: str,
    latency_seconds: float,
    resource_units_at_processing: int,
    experiment_phase: Optional[str] = None,
    correlation_id: Optional[str] = None,
    ttl_seconds: int = 7 * 24 * 3600,
    dispatch_latency_seconds: Optional[float] = None,
    queue_latency_seconds: Optional[float] = None,
    handler_duration_seconds: Optional[float] = None,
) -> None:
    t = table("METRICS_TABLE")
    now = time.time()
    item = {
        "pk": f"METRIC#{system}#{mode}",
        "sk": f"{now:020.6f}#{event_id}",
        "event_id": event_id,
        "system": system,
        "mode": mode,
        "event_type": event_type,
        "priority": priority,
        "latency_seconds": latency_seconds,
        "resource_units_at_processing": resource_units_at_processing,
        "experiment_phase": experiment_phase or "",
        "correlation_id": correlation_id or "",
        "processed_at": now,
        "ttl": int(now) + ttl_seconds,
        "dispatch_latency_seconds": dispatch_latency_seconds,
        "queue_latency_seconds": queue_latency_seconds,
        "handler_duration_seconds": handler_duration_seconds,
    }
    t.put_item(Item=_to_decimal(item))


def record_metric_batch(records: List[Dict[str, Any]]) -> None:
    if not records:
        return
    t = table("METRICS_TABLE")
    now = time.time()
    with t.batch_writer() as batch:
        for r in records:
            item = {
                "pk": f"METRIC#{r['system']}#{r['mode']}",
                "sk": f"{now:020.6f}#{r['event_id']}",
                "event_id": r["event_id"],
                "system": r["system"],
                "mode": r["mode"],
                "event_type": r["event_type"],
                "priority": r["priority"],
                "latency_seconds": r["latency_seconds"],
                "resource_units_at_processing": r["resource_units_at_processing"],
                "experiment_phase": r.get("experiment_phase") or "",
                "correlation_id": r.get("correlation_id") or "",
                "processed_at": now,
                "ttl": int(now) + 7 * 24 * 3600,
                "dispatch_latency_seconds": r.get("dispatch_latency_seconds"),
                "queue_latency_seconds": r.get("queue_latency_seconds"),
                "handler_duration_seconds": r.get("handler_duration_seconds"),
            }
            batch.put_item(Item=_to_decimal(item))


def query_recent_metrics(
    system: str,
    window_seconds: int,
    mode_filter: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Queries MetricsTable for all metrics of a given system (and optionally mode)
    within the last `window_seconds`. Used by the feedback loop (§2.3.6) and
    by the experiment analyzer.
    """
    t = table("METRICS_TABLE")
    now = time.time()
    lo = now - window_seconds

    modes_to_query = [mode_filter] if mode_filter else ["STREAM", "MICRO_BATCH", "BATCH", "FIXED"]

    results: List[Dict[str, Any]] = []
    for mode in modes_to_query:
        pk = f"METRIC#{system}#{mode}"
        resp = t.query(
            KeyConditionExpression="pk = :pk AND sk BETWEEN :lo AND :hi",
            ExpressionAttributeValues={
                ":pk": pk,
                ":lo": f"{lo:020.6f}",
                ":hi": f"{now:020.6f}#~",  # '~' sorts after any event_id
            },
        )
        for item in resp.get("Items", []):
            results.append(_from_decimal(item))

    return results


def query_all_metrics_for_experiment(
    correlation_id: str,
    systems: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """
    Scans MetricsTable filtered by a specific experiment's correlation_id.
    Uses Scan (acceptable for PoC scale; MetricsTable is small and Free Tier
    on-demand billing makes this cheap for a few thousand items).
    """
    t = table("METRICS_TABLE")
    systems = systems or ["baseline", "adpa"]

    results: List[Dict[str, Any]] = []
    scan_kwargs = {
        "FilterExpression": "correlation_id = :cid",
        "ExpressionAttributeValues": {":cid": correlation_id},
    }
    while True:
        resp = t.scan(**scan_kwargs)
        results.extend(_from_decimal(item) for item in resp.get("Items", []))
        if "LastEvaluatedKey" not in resp:
            break
        scan_kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]

    return [r for r in results if r.get("system") in systems]
