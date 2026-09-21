"""
Core data models for the ADPA system.

Directly implements the formal definitions from §2.3.1 of the thesis:
    d_i = < type_i, priority_i, lambda_i, size_i, t_i >   (Event)
    S(t) = < CPU_t, MEM_t, Q_t, R_t >                     (SystemState)
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, asdict, field
from enum import Enum
from typing import Optional, Dict, Any


class EventType(str, Enum):
    """T — the set of all possible transport event types."""
    # T_critical (§2.3.2)
    ACCIDENT = "ACCIDENT"
    EMERGENCY = "EMERGENCY"
    ROAD_HAZARD = "ROAD_HAZARD"
    # T_continuous (§2.3.2)
    GPS = "GPS"
    TRAFFIC_SENSOR = "TRAFFIC_SENSOR"
    CAMERA_METADATA = "CAMERA_METADATA"
    # Historical / analytical
    HISTORICAL_LOG = "HISTORICAL_LOG"
    TRAFFIC_REPORT = "TRAFFIC_REPORT"


class Priority(str, Enum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


class ProcessingMode(str, Enum):
    """M_i in {STREAM, MICRO_BATCH, BATCH} — §2.3.1"""
    STREAM = "STREAM"
    MICRO_BATCH = "MICRO_BATCH"
    BATCH = "BATCH"


class EventClass(str, Enum):
    """Output of classify() — §2.3.2"""
    CRITICAL = "CRITICAL"
    CONTINUOUS_MONITORING = "CONTINUOUS_MONITORING"
    HISTORICAL_ANALYTICAL = "HISTORICAL_ANALYTICAL"


# Sets used by the classification rule (§2.3.2)
T_CRITICAL = {EventType.ACCIDENT, EventType.EMERGENCY, EventType.ROAD_HAZARD}
T_CONTINUOUS = {EventType.GPS, EventType.TRAFFIC_SENSOR, EventType.CAMERA_METADATA}
T_HISTORICAL = {EventType.HISTORICAL_LOG, EventType.TRAFFIC_REPORT}


@dataclass
class TransportEvent:
    """
    d_i = <type_i, priority_i, lambda_i, size_i, t_i>

    lambda_i is not stored per-event in the strict mathematical sense
    (it's a property of the *stream*, not of a single event) — but we
    carry the *observed* per-type intensity at generation time for
    experimental traceability.
    """
    event_id: str
    type: EventType
    priority: Priority
    size_bytes: int
    timestamp: float                       # t_i — epoch seconds (float, high precision)
    payload: Dict[str, Any] = field(default_factory=dict)
    observed_lambda: Optional[float] = None  # lambda_i at generation time
    experiment_phase: Optional[str] = None   # metadata for experiment analysis
    correlation_id: Optional[str] = None     # links baseline vs proposed runs

    @staticmethod
    def create(
        event_type: EventType,
        priority: Priority,
        payload: Optional[Dict[str, Any]] = None,
        observed_lambda: Optional[float] = None,
        experiment_phase: Optional[str] = None,
        correlation_id: Optional[str] = None,
    ) -> "TransportEvent":
        payload = payload or {}
        body = json.dumps(payload).encode("utf-8")
        return TransportEvent(
            event_id=str(uuid.uuid4()),
            type=event_type,
            priority=priority,
            size_bytes=len(body),
            timestamp=time.time(),
            payload=payload,
            observed_lambda=observed_lambda,
            experiment_phase=experiment_phase,
            correlation_id=correlation_id,
        )

    def to_json(self) -> str:
        d = asdict(self)
        d["type"] = self.type.value
        d["priority"] = self.priority.value
        return json.dumps(d)

    @staticmethod
    def from_json(raw: str) -> "TransportEvent":
        d = json.loads(raw)
        return TransportEvent(
            event_id=d["event_id"],
            type=EventType(d["type"]),
            priority=Priority(d["priority"]),
            size_bytes=d["size_bytes"],
            timestamp=d["timestamp"],
            payload=d.get("payload", {}),
            observed_lambda=d.get("observed_lambda"),
            experiment_phase=d.get("experiment_phase"),
            correlation_id=d.get("correlation_id"),
        )

    def age_seconds(self) -> float:
        """Elapsed time since event creation — used to compute Latency(d)."""
        return time.time() - self.timestamp


@dataclass
class SystemState:
    """
    S(t) = <CPU_t, MEM_t, Q_t, R_t>  — §2.3.1

    In the serverless Free-Tier reimplementation:
      CPU_t  -> proxy derived from Lambda concurrent executions / reserved concurrency
      MEM_t  -> proxy derived from average Lambda duration vs timeout (not critical to ADPA math)
      Q_t    -> ApproximateNumberOfMessagesVisible on the routing queue (real backlog)
      R_t    -> currently allocated "resource units" (simulated executor/concurrency count)
    """
    cpu_percent: float
    mem_percent: float
    queue_depth: int
    resource_units: int
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "SystemState":
        return SystemState(
            cpu_percent=float(d["cpu_percent"]),
            mem_percent=float(d["mem_percent"]),
            queue_depth=int(d["queue_depth"]),
            resource_units=int(d["resource_units"]),
            timestamp=float(d.get("timestamp", time.time())),
        )


@dataclass
class ProcessingDecision:
    """Result of F: (d_i, S(t)) -> (M_i, R_i)  — §2.3.1, plus delta_t for MICRO_BATCH."""
    event_class: EventClass
    mode: ProcessingMode
    resource_units: int
    delta_t: Optional[float] = None   # None unless mode == MICRO_BATCH
    load_factor: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_class": self.event_class.value,
            "mode": self.mode.value,
            "resource_units": self.resource_units,
            "delta_t": self.delta_t,
            "load_factor": self.load_factor,
        }


@dataclass
class MetricRecord:
    """
    One row of experimental observation, matching the columns needed
    to reproduce Table 2.1 and Table 2.2 of the thesis.
    """
    event_id: str
    system: str                # "baseline" | "adpa"
    mode: str                  # STREAM | MICRO_BATCH | BATCH | FIXED
    event_type: str
    priority: str
    latency_seconds: float
    resource_units_at_processing: int
    experiment_phase: Optional[str]
    processed_at: float = field(default_factory=time.time)
    correlation_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
