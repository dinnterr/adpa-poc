"""
Unit tests for the pure ADPA algorithm functions (§2.3.2 - §2.3.4).
These require NO AWS credentials — they test only classifier.py,
interval.py, and load_factor.py in isolation, matching the O(1)
complexity claims made in the thesis.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import pytest  # noqa: E402

from common.models import TransportEvent, EventType, Priority, EventClass, ProcessingMode  # noqa: E402
from adpa_controller.config import Thresholds  # noqa: E402
from adpa_controller.classifier import classify, map_class_to_mode  # noqa: E402
from adpa_controller.interval import correction_factor, compute_delta_t, clamp  # noqa: E402
from adpa_controller.load_factor import compute_load_factor, decide_resource_units  # noqa: E402


@pytest.fixture
def theta():
    return Thresholds()


# ---------- classifier.py ----------

def test_high_priority_is_always_critical(theta):
    e = TransportEvent.create(EventType.GPS, Priority.HIGH)
    assert classify(e, theta, current_lambda_for_type=1.0) == EventClass.CRITICAL


def test_accident_type_is_always_critical_even_if_low_priority(theta):
    e = TransportEvent.create(EventType.ACCIDENT, Priority.LOW)
    assert classify(e, theta, current_lambda_for_type=0.0) == EventClass.CRITICAL


def test_dense_continuous_stream_is_monitoring(theta):
    e = TransportEvent.create(EventType.GPS, Priority.LOW)
    assert classify(e, theta, current_lambda_for_type=theta.theta_stream + 1) == EventClass.CONTINUOUS_MONITORING


def test_sparse_continuous_stream_falls_to_historical(theta):
    e = TransportEvent.create(EventType.GPS, Priority.LOW)
    assert classify(e, theta, current_lambda_for_type=theta.theta_stream - 1) == EventClass.HISTORICAL_ANALYTICAL


def test_historical_type_is_historical(theta):
    e = TransportEvent.create(EventType.HISTORICAL_LOG, Priority.LOW)
    assert classify(e, theta, current_lambda_for_type=0.0) == EventClass.HISTORICAL_ANALYTICAL


def test_class_to_mode_mapping(theta):
    assert map_class_to_mode(EventClass.CRITICAL) == ProcessingMode.STREAM
    assert map_class_to_mode(EventClass.CONTINUOUS_MONITORING) == ProcessingMode.MICRO_BATCH
    assert map_class_to_mode(EventClass.HISTORICAL_ANALYTICAL) == ProcessingMode.BATCH


# ---------- interval.py ----------

def test_clamp_bounds():
    assert clamp(5, 1, 10) == 5
    assert clamp(-1, 1, 10) == 1
    assert clamp(50, 1, 10) == 10


def test_correction_factor_decreases_on_high_queue(theta):
    k = correction_factor(queue_depth=theta.Q_high + 1, cpu_percent=10, lambda_t=1, theta=theta)
    assert k == theta.k_dec


def test_correction_factor_decreases_on_high_cpu(theta):
    k = correction_factor(queue_depth=0, cpu_percent=theta.CPU_high + 1, lambda_t=1, theta=theta)
    assert k == theta.k_dec


def test_correction_factor_increases_on_low_load(theta):
    k = correction_factor(
        queue_depth=0,
        cpu_percent=theta.CPU_low - 1,
        lambda_t=theta.lambda_low - 1,
        theta=theta,
    )
    assert k == theta.k_inc


def test_correction_factor_neutral_zone(theta):
    k = correction_factor(
        queue_depth=theta.Q_high - 1,
        cpu_percent=(theta.CPU_low + theta.CPU_high) / 2,
        lambda_t=theta.lambda_low + 1,
        theta=theta,
    )
    assert k == 1.0


def test_delta_t_respects_bounds(theta):
    dt_shrink = compute_delta_t(queue_depth=1000, cpu_percent=99, lambda_t=1, theta=theta)
    dt_grow = compute_delta_t(queue_depth=0, cpu_percent=0, lambda_t=0, theta=theta)
    assert theta.T_min <= dt_shrink <= theta.T_max
    assert theta.T_min <= dt_grow <= theta.T_max
    assert dt_shrink < dt_grow  # shrink under load, grow when idle


def test_delta_t_monotonicity_wrt_queue(theta):
    dt_low_q = compute_delta_t(queue_depth=0, cpu_percent=10, lambda_t=10, theta=theta)
    dt_high_q = compute_delta_t(queue_depth=theta.Q_high + 10, cpu_percent=10, lambda_t=10, theta=theta)
    assert dt_high_q <= dt_low_q


# ---------- load_factor.py ----------

def test_load_factor_zero_when_all_metrics_zero(theta):
    lf = compute_load_factor(cpu_percent=0, queue_depth=0, lambda_t=0, theta=theta)
    assert lf == 0.0


def test_load_factor_saturates_at_weighted_sum_when_maxed(theta):
    lf = compute_load_factor(
        cpu_percent=100,
        queue_depth=theta.Q_max * 10,   # far beyond Q_max, must clip to 1.0
        lambda_t=theta.lambda_max * 10,
        theta=theta,
    )
    expected = theta.w1 * 1.0 + theta.w2 * 1.0 + theta.w3 * 1.0
    assert abs(lf - expected) < 1e-9
    assert abs(lf - 1.0) < 1e-9  # weights sum to 1 by construction


def test_load_factor_weights_sum_to_one(theta):
    assert abs((theta.w1 + theta.w2 + theta.w3) - 1.0) < 1e-9


def test_decide_resource_units_scales_up_above_theta_up(theta):
    lf = theta.theta_up + 0.01
    new_r = decide_resource_units(current_resource_units=2, load_factor=lf, theta=theta)
    assert new_r == 2 + theta.dR_plus


def test_decide_resource_units_scales_down_below_theta_down(theta):
    lf = theta.theta_down - 0.01
    new_r = decide_resource_units(current_resource_units=5, load_factor=lf, theta=theta)
    assert new_r == max(theta.R_min, 5 - theta.dR_minus)


def test_decide_resource_units_stable_in_hysteresis_zone(theta):
    lf = (theta.theta_up + theta.theta_down) / 2
    new_r = decide_resource_units(current_resource_units=3, load_factor=lf, theta=theta)
    assert new_r == 3


def test_decide_resource_units_never_below_r_min(theta):
    new_r = decide_resource_units(current_resource_units=theta.R_min, load_factor=0.0, theta=theta)
    assert new_r == theta.R_min


def test_decide_resource_units_respects_r_max_ceiling(theta):
    new_r = decide_resource_units(current_resource_units=theta.R_max - 1, load_factor=1.0, theta=theta)
    assert new_r <= theta.R_max
