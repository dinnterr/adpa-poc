"""
Centralised boto3 client/resource factory with lazy singletons,
so every Lambda cold start pays the client-construction cost only once.
"""

from __future__ import annotations

import os
import boto3
from functools import lru_cache
from typing import Any

_DEFAULT_REGION = os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "eu-central-1"))


@lru_cache(maxsize=1)
def sqs_client():
    return boto3.client("sqs", region_name=_DEFAULT_REGION)


@lru_cache(maxsize=1)
def dynamodb_resource():
    return boto3.resource("dynamodb", region_name=_DEFAULT_REGION)


@lru_cache(maxsize=1)
def s3_client():
    return boto3.client("s3", region_name=_DEFAULT_REGION)


@lru_cache(maxsize=1)
def cloudwatch_client():
    return boto3.client("cloudwatch", region_name=_DEFAULT_REGION)


@lru_cache(maxsize=1)
def lambda_client():
    return boto3.client("lambda", region_name=_DEFAULT_REGION)


def table(table_name_env_var: str):
    """Get a DynamoDB Table resource by reading its name from env var."""
    name = os.environ[table_name_env_var]
    return dynamodb_resource().Table(name)


def queue_url(env_var: str) -> str:
    return os.environ[env_var]


def get_queue_depth(queue_url_str: str) -> int:
    """
    Q_t proxy — total backlog on the queue: messages still waiting
    (ApproximateNumberOfMessages) PLUS messages already picked up by a
    consumer but not yet finished processing (ApproximateNumberOfMessagesNotVisible).

    Using only the "visible" count underestimates backlog under real
    overload: once a Lambda's SQS poller pulls a message it immediately
    becomes "not visible" even though it hasn't been processed yet, so a
    queue that is fully saturated with slow-to-process messages can still
    report a visible count of ~0.
    """
    resp = sqs_client().get_queue_attributes(
        QueueUrl=queue_url_str,
        AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"],
    )
    attrs = resp["Attributes"]
    visible = int(attrs.get("ApproximateNumberOfMessages", 0))
    in_flight = int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0))
    return visible + in_flight


def send_message(queue_url_str: str, body: str, delay_seconds: int = 0) -> dict:
    return sqs_client().send_message(
        QueueUrl=queue_url_str,
        MessageBody=body,
        DelaySeconds=delay_seconds,
    )


def send_message_batch(queue_url_str: str, entries: list) -> dict:
    """entries: list of {'Id': str, 'MessageBody': str}"""
    return sqs_client().send_message_batch(QueueUrl=queue_url_str, Entries=entries)
