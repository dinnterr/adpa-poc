# adpa_controller/lambda_function.py
import json
import time
import boto3
from classifier import classify_event
from mode_selector import select_mode, compute_batch_interval
from resource_allocator import compute_required_resources

cloudwatch = boto3.client('cloudwatch')
kinesis = boto3.client('kinesis')

# Пороги — читаються з DynamoDB/SSM, оновлюються feedback loop'ом
THRESHOLDS = {
    "lambda_stream_threshold": 50,     # подій/сек
    "q_threshold_high": 1000,
    "cpu_threshold_high": 70,
    "cpu_threshold_low": 20,
    "batch_interval_base": 5,          # сек
}

def lambda_handler(event, context):
    results = []
    for record in event['Records']:
        payload = json.loads(record['kinesis']['data'])

        # ---- КРОК 1: Класифікація ----
        event_class = classify_event(payload, THRESHOLDS)

        # ---- КРОК 2: Вибір режиму ----
        mode = select_mode(event_class)

        # ---- КРОК 3: Адаптивний інтервал (якщо micro-batch) ----
        system_state = get_current_system_state()
        if mode == "MICRO_BATCH":
            interval = compute_batch_interval(
                lambda_t=system_state['incoming_rate'],
                q_t=system_state['queue_length'],
                cpu_t=system_state['cpu_util'],
                thresholds=THRESHOLDS
            )
        else:
            interval = None

        # ---- КРОК 4: Розрахунок ресурсів ----
        resources = compute_required_resources(mode, system_state, THRESHOLDS)

        # ---- Виконання: маршрутизація до відповідного шляху ----
        route_event(payload, mode, resources)

        # ---- Публікація метрик для аналізу ----
        publish_metrics(payload, event_class, mode, resources)

        results.append({
            "event_id": payload.get("id"),
            "class": event_class,
            "mode": mode,
            "batch_interval": interval,
            "resources": resources
        })

    return {"statusCode": 200, "body": json.dumps(results)}


def get_current_system_state():
    """Отримуємо поточні метрики системи з CloudWatch"""
    # Спрощено для POC — читаємо останні 1-min метрики
    resp = cloudwatch.get_metric_statistics(
        Namespace='ITS/Adaptive',
        MetricName='IncomingRate',
        StartTime=time.time() - 60,
        EndTime=time.time(),
        Period=60,
        Statistics=['Average']
    )
    incoming_rate = resp['Datapoints'][-1]['Average'] if resp['Datapoints'] else 0

    # аналогічно для queue_length, cpu_util (спрощено)
    return {
        "incoming_rate": incoming_rate,
        "queue_length": get_kinesis_iterator_age(),  # проксі для queue backlog
        "cpu_util": get_lambda_concurrency_utilization()
    }


def route_event(payload, mode, resources):
    if mode == "STREAM":
        # виклик critical_handler напряму (sync invoke)
        lambda_client = boto3.client('lambda')
        lambda_client.invoke(
            FunctionName='critical_handler',
            InvocationType='Event',
            Payload=json.dumps(payload)
        )
    elif mode == "MICRO_BATCH":
        kinesis.put_record(
            StreamName='microbatch-stream',
            Data=json.dumps(payload),
            PartitionKey=payload.get("type", "default")
        )
    else:  # BATCH
        s3 = boto3.client('s3')
        s3.put_object(
            Bucket='its-datalake-poc',
            Key=f"raw/{payload['type']}/{payload['id']}.json",
            Body=json.dumps(payload)
        )


def publish_metrics(payload, event_class, mode, resources):
    cloudwatch.put_metric_data(
        Namespace='ITS/Adaptive',
        MetricData=[
            {
                'MetricName': 'EventProcessed',
                'Dimensions': [
                    {'Name': 'Class', 'Value': event_class},
                    {'Name': 'Mode', 'Value': mode}
                ],
                'Value': 1,
                'Unit': 'Count'
            }
        ]
    )