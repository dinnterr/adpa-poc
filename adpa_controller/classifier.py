# adpa_controller/classifier.py

CRITICAL_TYPES = {"ACCIDENT", "EMERGENCY", "ROAD_HAZARD"}
CONTINUOUS_TYPES = {"GPS", "TRAFFIC_SENSOR", "CAMERA_METADATA"}

def classify_event(payload, thresholds):
    """
    Реалізація Кроку 1 алгоритму ADPA
    """
    event_type = payload.get("type")
    priority = payload.get("priority", "LOW")

    if priority == "HIGH" or event_type in CRITICAL_TYPES:
        return "CRITICAL"

    if event_type in CONTINUOUS_TYPES:
        current_rate = payload.get("current_type_rate", 0)
        if current_rate > thresholds["lambda_stream_threshold"]:
            return "CONTINUOUS_MONITORING"

    return "HISTORICAL_ANALYTICAL"