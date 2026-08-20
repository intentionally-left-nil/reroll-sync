import boto3
import pytest
from botocore.exceptions import ClientError

from reroll_sync.r2_client import (
    R2Config,
    R2ConfigError,
    R2UploadError,
    r2_config_from_env,
    upload_bytes,
)


def _set_all_env(monkeypatch):
    monkeypatch.setenv("R2_ACCOUNT_ID", "acct123")
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "key123")
    monkeypatch.setenv("R2_SECRET_ACCESS_KEY", "secret123")
    monkeypatch.setenv("R2_BUCKET", "my-bucket")


def test_r2_config_from_env_reads_all_variables(monkeypatch):
    _set_all_env(monkeypatch)

    config = r2_config_from_env()

    assert config == R2Config(
        account_id="acct123",
        access_key_id="key123",
        secret_access_key="secret123",
        bucket="my-bucket",
    )


def test_r2_config_endpoint_url_uses_account_id():
    config = R2Config(account_id="acct123", access_key_id="k", secret_access_key="s", bucket="b")

    assert config.endpoint_url == "https://acct123.r2.cloudflarestorage.com"


def test_r2_config_from_env_raises_when_all_missing(monkeypatch):
    monkeypatch.delenv("R2_ACCOUNT_ID", raising=False)
    monkeypatch.delenv("R2_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("R2_SECRET_ACCESS_KEY", raising=False)
    monkeypatch.delenv("R2_BUCKET", raising=False)

    with pytest.raises(R2ConfigError) as exc_info:
        r2_config_from_env()

    message = str(exc_info.value)
    assert "R2_ACCOUNT_ID" in message
    assert "R2_ACCESS_KEY_ID" in message
    assert "R2_SECRET_ACCESS_KEY" in message
    assert "R2_BUCKET" in message


def test_r2_config_from_env_raises_when_partially_missing(monkeypatch):
    _set_all_env(monkeypatch)
    monkeypatch.delenv("R2_BUCKET", raising=False)

    with pytest.raises(R2ConfigError) as exc_info:
        r2_config_from_env()

    message = str(exc_info.value)
    assert "R2_BUCKET" in message
    assert "R2_ACCOUNT_ID" not in message


def test_upload_bytes_puts_object_with_configured_client(monkeypatch):
    config = R2Config(
        account_id="acct123", access_key_id="key123", secret_access_key="secret123", bucket="b"
    )
    captured_client_kwargs: dict = {}
    captured_put_kwargs: dict = {}

    class _FakeClient:
        def put_object(self, **kwargs):
            captured_put_kwargs.update(kwargs)

    def _fake_client(service_name, **kwargs):
        captured_client_kwargs["service_name"] = service_name
        captured_client_kwargs.update(kwargs)
        return _FakeClient()

    monkeypatch.setattr(boto3, "client", _fake_client)

    upload_bytes(config, "42", b"metadata bytes")

    assert captured_client_kwargs["service_name"] == "s3"
    assert captured_client_kwargs["endpoint_url"] == "https://acct123.r2.cloudflarestorage.com"
    assert captured_client_kwargs["aws_access_key_id"] == "key123"
    assert captured_client_kwargs["aws_secret_access_key"] == "secret123"
    assert captured_put_kwargs == {"Bucket": "b", "Key": "42", "Body": b"metadata bytes"}


def test_upload_bytes_wraps_client_error(monkeypatch):
    config = R2Config(
        account_id="acct123", access_key_id="key123", secret_access_key="secret123", bucket="b"
    )

    class _FakeClient:
        def put_object(self, **kwargs):
            raise ClientError({"Error": {"Code": "AccessDenied", "Message": "nope"}}, "PutObject")

    monkeypatch.setattr(boto3, "client", lambda *args, **kwargs: _FakeClient())

    with pytest.raises(R2UploadError) as exc_info:
        upload_bytes(config, "42", b"data")

    assert "42" in str(exc_info.value)
    assert "b" in str(exc_info.value)
