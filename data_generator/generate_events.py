# data_generator/generate_events.py
import json, random, time, uuid
import boto3

kinesis = boto3.client('kinesis', region_name='eu-central-1')

EVENT_TYPES = {
    "ACCIDENT": {"priority": "HIGH", "weight": 0.02},
    "EMERGENCY": {"priority": "HIGH", "weight": 0.01},
    "GPS": {"priority": "MEDIUM", "weight": 0.6},
    "TRAFFIC_SENSOR": {"priority": "MEDIUM", "weight": 0.25},
    "HISTORICAL_LOG": {"priority": "LOW", "weight": 0.12},
}

def generate_event():
    event_type = random.choices(
        list(EVENT_TYPES.keys()),
        weights=[v["weight"] for v in EVENT_TYPES.values()]
    )[0]
    return {
        "id": str(uuid.uuid4()),
        "type": event_type,
        "priority": EVENT_TYPES[event_type]["priority"],
        "timestamp": time.time(),
        "payload": {"lat": random.uniform(46.4, 46.5), "lon": random.uniform(30.7, 30.8)}
    }

def simulate_traffic(duration_sec, base_rate, peak_multiplier=5, peak_start=60, peak_end=120):
    """Емуляція динамічного навантаження — імітація 'години пік'"""
    start = time.time()
    while time.time() - start < duration_sec:
        elapsed = time.time() - start
        rate = base_rate * peak_multiplier if peak_start <= elapsed <= peak_end else base_rate

        for _ in range(rate):
            evt = generate_event()
            kinesis.put_record(
                StreamName='its-input-stream',
                Data=json.dumps(evt),
                PartitionKey=evt["type"]
            )
        time.sleep(1)

if __name__ == "__main__":
    simulate_traffic(duration_sec=180, base_rate=20)