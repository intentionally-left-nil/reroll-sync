"""Cloudflare R2 (S3-compatible) upload client, configured from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass

import boto3
from botocore.exceptions import BotoCoreError, ClientError

_ENV_VARS = ("R2_ACCOUNT_ID", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_BUCKET")


class R2ConfigError(Exception):
    """Raised when one or more required R2 environment variables are unset."""


class R2UploadError(Exception):
    """Raised when an upload to R2 fails."""


class R2DownloadError(Exception):
    """Raised when a download from R2 fails."""


@dataclass(frozen=True)
class R2Config:
    """R2 bucket location and credentials."""

    account_id: str
    access_key_id: str
    secret_access_key: str
    bucket: str

    @property
    def endpoint_url(self) -> str:
        return f"https://{self.account_id}.r2.cloudflarestorage.com"


def r2_config_from_env() -> R2Config:
    """Read :class:`R2Config` from ``R2_ACCOUNT_ID``, ``R2_ACCESS_KEY_ID``,
    ``R2_SECRET_ACCESS_KEY``, and ``R2_BUCKET``.

    Raises :class:`R2ConfigError` naming any that are unset or empty.
    """
    missing = [name for name in _ENV_VARS if not os.environ.get(name)]
    if missing:
        raise R2ConfigError(f"Missing required environment variable(s): {', '.join(missing)}")
    return R2Config(
        account_id=os.environ["R2_ACCOUNT_ID"],
        access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        bucket=os.environ["R2_BUCKET"],
    )


def upload_bytes(config: R2Config, key: str, data: bytes) -> None:
    """Upload ``data`` to ``config``'s bucket under ``key``."""
    client = boto3.client(
        "s3",
        endpoint_url=config.endpoint_url,
        aws_access_key_id=config.access_key_id,
        aws_secret_access_key=config.secret_access_key,
    )
    try:
        client.put_object(Bucket=config.bucket, Key=key, Body=data)
    except (BotoCoreError, ClientError) as exc:
        raise R2UploadError(
            f"Failed to upload key '{key}' to bucket '{config.bucket}': {exc}"
        ) from exc


def download_bytes(config: R2Config, key: str) -> bytes:
    """Download and return the bytes stored at ``key`` in ``config``'s bucket."""
    client = boto3.client(
        "s3",
        endpoint_url=config.endpoint_url,
        aws_access_key_id=config.access_key_id,
        aws_secret_access_key=config.secret_access_key,
    )
    try:
        response = client.get_object(Bucket=config.bucket, Key=key)
        return response["Body"].read()
    except (BotoCoreError, ClientError) as exc:
        raise R2DownloadError(
            f"Failed to download key '{key}' from bucket '{config.bucket}': {exc}"
        ) from exc
