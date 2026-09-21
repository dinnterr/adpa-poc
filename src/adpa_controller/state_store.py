"""
Persistence helpers for SystemState S(t) and Thresholds Theta in DynamoDB.

Table layout (SystemStateTable):
    pk = "STATE#<mode>"     sk = "CURRENT"           -> latest snapshot per mode
    pk = "STATE#<mode>"     sk = "<timestamp>"       -> historical snapshot (TTL'd)

Table layout (ThresholdsTable):
    pk = "GLOBAL_THETA"                              -> single row, whole Thresholds dict
"""

from __future__ import annotations

import time
from decimal import Decimal
from typing import Optional

from common.aws_clients import table
from common.models import SystemState
from adpa_controller.config import Thresholds, THRESHOLDS_PK


def _to_decimal(obj):
    """DynamoDB requires Decimal instead of float."""
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


def save_system_state(mode: str, state: SystemState, ttl_seconds: int = 3600) -> None:
    t = table("SYSTEM_STATE_TABLE")
    item = _to_decimal(state.to_dict())
    now_iso_sortable = f"{state.timestamp:020.6f}"

    with t.batch_writer() as batch:
        batch.put_item(Item={"pk": f"STATE#{mode}", "sk": "CURRENT", **item})
        batch.put_item(
            Item={
                "pk": f"STATE#{mode}",
                "sk": now_iso_sortable,
                "ttl": int(time.time()) + ttl_seconds,
                **item,
            }
        )


def load_system_state(mode: str) -> Optional[SystemState]:
    t = table("SYSTEM_STATE_TABLE")
    resp = t.get_item(Key={"pk": f"STATE#{mode}", "sk": "CURRENT"})
    item = resp.get("Item")
    if not item:
        return None
    item = _from_decimal(item)
    return SystemState.from_dict(item)


def load_thresholds() -> Thresholds:
    t = table("THRESHOLDS_TABLE")
    resp = t.get_item(Key={"pk": THRESHOLDS_PK})
    item = resp.get("Item")
    if not item:
        from adpa_controller.config import DEFAULT_THRESHOLDS
        save_thresholds(DEFAULT_THRESHOLDS)
        return DEFAULT_THRESHOLDS
    item = _from_decimal(item)
    item.pop("pk", None)
    return Thresholds.from_dict(item)


def save_thresholds(theta: Thresholds) -> None:
    t = table("THRESHOLDS_TABLE")
    item = _to_decimal(theta.to_dict())
    t.put_item(Item={"pk": THRESHOLDS_PK, **item})
