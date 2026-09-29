"""
Threshold set Theta = (theta_stream, Q_high, CPU_high, CPU_low, lambda_low,
                        theta_up, theta_down, w1, w2, w3, T_base, T_min, T_max,
                        k_dec, k_inc, Q_max, lambda_max, R_min, dR_plus, dR_minus,
                        SLA_target, alpha)

These are the DEFAULT values used to seed DynamoDB on first run. After that,
the feedback loop in §2.3.6 (controller_handler.py.recalculate_thresholds_handler)
mutates theta_up over time based on observed P95 latency.
"""

from __future__ import annotations
from dataclasses import dataclass, asdict, field
from typing import Dict, Any


@dataclass
class Thresholds:
    # --- Classification (§2.3.2) ---
    theta_stream: float = 5.0          # events/sec threshold for "dense enough" continuous stream

    # --- Adaptive interval.py correction (§2.3.3) ---
    T_base: float = 5.0                # seconds, base micro-batch window
    T_min: float = 1.0
    T_max: float = 15.0
    Q_high: int = 50                   # backlog threshold that triggers interval.py shrink
    CPU_high: float = 75.0             # % CPU that triggers interval.py shrink
    CPU_low: float = 25.0              # % CPU that allows interval.py growth
    lambda_low: float = 5.0            # events/sec, low-intensity condition for growth
    k_dec: float = 0.5                 # interval.py shrink multiplier
    k_inc: float = 1.5                 # interval.py growth multiplier

    # --- Load factor (§2.3.4) ---
    w1: float = 0.5                    # CPU weight
    w2: float = 0.3                    # queue weight
    w3: float = 0.2                    # intensity weight
    Q_max: int = 200                   # normalising constant for queue depth
    lambda_max: float = 150.0          # normalising constant for intensity

    # --- Scaling decision (§2.3.4) ---
    theta_up: float = 0.7              # LF threshold to scale up
    theta_down: float = 0.3            # LF threshold to scale down
    dR_plus: int = 2                   # resource units added per step
    dR_minus: int = 1                  # resource units removed per step
    R_min: int = 1                     # minimum resource units
    R_max: int = 20                    # safety ceiling (Free Tier concurrency limits)
    R_predefined_batch: int = 1        # fixed allocation for BATCH mode

    # --- Feedback loop (§2.3.6) ---
    SLA_target: float = 1.0            # target P95 latency (seconds) for CRITICAL events
    alpha: float = 0.05                # learning rate for theta_up adaptation
    theta_up_min: float = 0.2
    theta_up_max: float = 0.95

    T_recalc_seconds: int = 60         # feedback loop period

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "Thresholds":
        # Filter unknown keys defensively (schema evolution safety)
        valid_keys = Thresholds.__dataclass_fields__.keys()
        filtered = {k: v for k, v in d.items() if k in valid_keys}
        return Thresholds(**filtered)


DEFAULT_THRESHOLDS = Thresholds()
THRESHOLDS_PK = "GLOBAL_THETA"
