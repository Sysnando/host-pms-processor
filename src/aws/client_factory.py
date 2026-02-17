"""Boto3 client factory with Climber padrão credential handling.

If AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are set, use them;
otherwise use the default boto3 credential provider (SSO, role, etc.).
"""

import os
from typing import Any

from src.config import settings


def get_boto3_client_kwargs(service: str = "s3") -> dict[str, Any]:
    """Return kwargs for boto3.client() so that explicit credentials are used only when set.

    When both AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are set in the environment,
    they are included so boto3 uses them. Otherwise, no credentials are passed and
    boto3 uses its default chain (SSO, profile, instance role, etc.).

    Args:
        service: Service name for boto3 (e.g. 's3', 'sqs').

    Returns:
        Dict with at least 'region_name'. May include 'aws_access_key_id' and
        'aws_secret_access_key' when set in env.
    """
    kwargs: dict[str, Any] = {
        "region_name": settings.aws.region,
    }
    access_key = os.environ.get("AWS_ACCESS_KEY_ID", "").strip()
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "").strip()
    if access_key and secret_key:
        kwargs["aws_access_key_id"] = access_key
        kwargs["aws_secret_access_key"] = secret_key
    return kwargs
