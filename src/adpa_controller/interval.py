"""
§2.3.3 — Adaptive micro-batch interval (delta_t) computation.

k(Q_t, CPU_t) =
    k_dec   if Q_t > Q_high OR CPU_t > CPU_high
    k_inc   if lambda_t < lambda_low AND CPU_t < CPU_low
    1       otherwise

delta_t = clamp(T_base * k(Q_t, CPU_t), T_min, T_max)

Implemented as pure functions -> O(1), no I/O, trivially unit-testable,
matching the "amortised O(1) per event" complexity claim in §2.3.5.
"""

from __future__ import annotations

from adpa_controller.config import Thresholds


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(x, hi))


def correction_factor(
    queue_depth: float,
    cpu_percent: float,
    lambda_t: float,
    theta: Thresholds,
) -> float:
    """k(Q_t, CPU_t) — piecewise-linear, threshold-based (see §2.3.3 rationale)."""
    if queue_depth > theta.Q_high or cpu_percent > theta.CPU_high:
        return theta.k_dec
    if lambda_t < theta.lambda_low and cpu_percent < theta.CPU_low:
        return theta.k_inc
    return 1.0


def compute_delta_t(
    queue_depth: float,
    cpu_percent: float,
    lambda_t: float,
    theta: Thresholds,
) -> float:
    """delta_t = clamp(T_base * k(Q_t, CPU_t), T_min, T_max)"""
    k = correction_factor(queue_depth, cpu_percent, lambda_t, theta)
    return clamp(theta.T_base * k, theta.T_min, theta.T_max)
