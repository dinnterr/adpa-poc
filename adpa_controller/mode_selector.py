# adpa_controller/mode_selector.py

def select_mode(event_class):
    """Крок 2"""
    mapping = {
        "CRITICAL": "STREAM",
        "CONTINUOUS_MONITORING": "MICRO_BATCH",
        "HISTORICAL_ANALYTICAL": "BATCH"
    }
    return mapping[event_class]


def compute_batch_interval(lambda_t, q_t, cpu_t, thresholds):
    """Крок 3 — адаптивний інтервал мікропакетів"""
    base_interval = thresholds["batch_interval_base"]
    q_threshold_high = thresholds["q_threshold_high"]
    cpu_threshold_high = thresholds["cpu_threshold_high"]
    cpu_threshold_low = thresholds["cpu_threshold_low"]

    k_decrease = 0.5
    k_increase = 1.5
    t_min, t_max = 1, 30

    if q_t > q_threshold_high or cpu_t > cpu_threshold_high:
        interval = base_interval * k_decrease
    elif lambda_t < 10 and cpu_t < cpu_threshold_low:
        interval = base_interval * k_increase
    else:
        interval = base_interval

    return max(t_min, min(t_max, interval))