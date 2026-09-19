# adpa_controller/resource_allocator.py

def compute_required_resources(mode, system_state, thresholds):
    """Крок 4 — розрахунок ресурсів"""
    cpu_t = system_state["cpu_util"]
    q_t = system_state["queue_length"]
    lambda_t = system_state["incoming_rate"]

    if mode == "STREAM":
        throughput_per_instance = 20  # подій/сек на 1 lambda concurrency
        return {"lambda_concurrency": max(1, int(lambda_t / throughput_per_instance) + 1)}

    if mode == "MICRO_BATCH":
        load_factor = (0.5 * cpu_t / 100) + (0.3 * min(q_t / 1000, 1)) + (0.2 * min(lambda_t / 100, 1))

        current_executors = system_state.get("current_executors", 2)
        if load_factor > 0.7:
            executors = current_executors + 2
        elif load_factor < 0.3:
            executors = max(2, current_executors - 1)
        else:
            executors = current_executors

        return {"executors": executors, "load_factor": round(load_factor, 3)}

    if mode == "BATCH":
        return {"cluster_size": "predefined_small"}  # для POC — фіксовано