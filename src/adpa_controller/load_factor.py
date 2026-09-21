"""
§2.3.4 — Load factor LF(t) and resource scaling decision.

LF(t) = w1 * (CPU_t / 100) + w2 * min(Q_t / Q_max, 1) + w3 * min(lambda_t / lambda_max, 1)

Scaling rule:
    R_{t+1} = R_t + dR_plus                 if LF(t) > theta_up
    R_{t+1} = max(R_min, R_t - dR_minus)     if LF(t) < theta_down
    R_{t+1} = R_t                            otherwise   (hysteresis "dead zone")
"""

from __future__ import annotations

from adpa_controller.config import Thresholds


def compute_load_factor(
    cpu_percent: float,
    queue_depth: float,
    lambda_t: float,
    theta: Thresholds,
) -> float:
    cpu_term = theta.w1 * (cpu_percent / 100.0)
    queue_term = theta.w2 * min(queue_depth / theta.Q_max, 1.0)
    lambda_term = theta.w3 * min(lambda_t / theta.lambda_max, 1.0)
    return cpu_term + queue_term + lambda_term


def decide_resource_units(
    current_resource_units: int,
    load_factor: float,
    theta: Thresholds,
) -> int:
    if load_factor > theta.theta_up:
        new_units = current_resource_units + theta.dR_plus
    elif load_factor < theta.theta_down:
        new_units = max(theta.R_min, current_resource_units - theta.dR_minus)
    else:
        new_units = current_resource_units

    # Safety ceiling to respect Free Tier Lambda concurrency limits (§ implementation constraint)
    return min(new_units, theta.R_max)
