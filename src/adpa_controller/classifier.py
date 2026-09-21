"""
§2.3.2 — Classification of input events.

class(d_i) =
    CRITICAL                  if priority_i = HIGH  OR type_i in T_critical
    CONTINUOUS_MONITORING     if type_i in T_continuous AND lambda(type_i) > theta_stream
    HISTORICAL_ANALYTICAL     otherwise
"""

from __future__ import annotations

from common.models import (
    TransportEvent,
    EventClass,
    Priority,
    T_CRITICAL,
    T_CONTINUOUS,
)
from adpa_controller.config import Thresholds


def classify(event: TransportEvent, theta: Thresholds, current_lambda_for_type: float) -> EventClass:
    """
    Parameters
    ----------
    event : TransportEvent
        The incoming event d_i.
    theta : Thresholds
        Current threshold set Theta (theta_stream is used here).
    current_lambda_for_type : float
        lambda(type_i) — the currently observed intensity (events/sec) for
        this specific event type, computed by the caller from a sliding
        window over recent arrivals (see controller_handler._estimate_lambda).
    """
    if event.priority == Priority.HIGH or event.type in T_CRITICAL:
        return EventClass.CRITICAL

    if event.type in T_CONTINUOUS and current_lambda_for_type > theta.theta_stream:
        return EventClass.CONTINUOUS_MONITORING

    return EventClass.HISTORICAL_ANALYTICAL


# Mapping table used right after classification (§2.3.2, second table)
_CLASS_TO_MODE = {
    EventClass.CRITICAL: "STREAM",
    EventClass.CONTINUOUS_MONITORING: "MICRO_BATCH",
    EventClass.HISTORICAL_ANALYTICAL: "BATCH",
}


def map_class_to_mode(event_class: EventClass) -> str:
    from common.models import ProcessingMode
    return ProcessingMode(_CLASS_TO_MODE[event_class])
