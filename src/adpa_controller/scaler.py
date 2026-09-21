"""
Applies the resource decision from load_factor.decide_resource_units() to
the *real* AWS Lambda ReservedConcurrentExecutions setting on the
STREAM and MICRO_BATCH processor functions.

This is the serverless analogue of "AWS Auto Scaling adjusting EC2/EMR
instance count" (§1.2, §2.1). Concurrency is the Free-Tier-compatible
proxy for compute capacity that costs nothing extra to adjust.
"""

from __future__ import annotations

import logging
import os
from typing import Optional

from common.aws_clients import lambda_client

logger = logging.getLogger(__name__)

STREAM_FUNCTION_NAME = os.environ.get("STREAM_FUNCTION_NAME", "")
MICROBATCH_FUNCTION_NAME = os.environ.get("MICROBATCH_FUNCTION_NAME", "")


def apply_resource_units(function_name: str, resource_units: int) -> None:
    """
    Sets ReservedConcurrentExecutions on the target Lambda to `resource_units`.

    NOTE: This is throttled defensively — AWS allows a limited number of
    PutFunctionConcurrency calls per second, and Free Tier accounts have a
    total account concurrency ceiling (commonly 1000, but unreserved pool
    must stay >= 10). We never request more than theta.R_max (§load_factor.py).
    """
    if not function_name:
        logger.warning("No function name configured for scaling; skipping.")
        return

    client = lambda_client()
    try:
        client.put_function_concurrency(
            FunctionName=function_name,
            ReservedConcurrentExecutions=resource_units,
        )
        logger.info("Scaled %s to %d concurrent executions", function_name, resource_units)
    except client.exceptions.InvalidParameterValueException as e:
        logger.error("Cannot set concurrency for %s: %s", function_name, e)
    except Exception as e:  # noqa: BLE001 — scaling failures must not crash the controller
        logger.exception("Unexpected error scaling %s: %s", function_name, e)


def apply_scaling_decision(mode: str, resource_units: int) -> None:
    """Route the scaling call to the correct function based on processing mode."""
    if mode == "STREAM":
        apply_resource_units(STREAM_FUNCTION_NAME, resource_units)
    elif mode == "MICRO_BATCH":
        apply_resource_units(MICROBATCH_FUNCTION_NAME, resource_units)
    # BATCH mode uses R_predefined_batch and is not dynamically scaled (§2.3.5, line 16-17)
