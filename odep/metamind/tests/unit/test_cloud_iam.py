"""
Unit Tests — Cross-Cloud IAM Federator

File: tests/unit/test_cloud_iam.py
"""

from __future__ import annotations

import json
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from metamind.security.cloud_iam import (
    AWSIAMProvider,
    CloudCredentials,
    CloudIAMFederator,
)


def _future_dt(hours: int = 1) -> datetime:
    return datetime.now(tz=timezone.utc) + timedelta(hours=hours)


class TestAWSIAMProvider(unittest.IsolatedAsyncioTestCase):

    async def test_get_credentials_calls_assume_role_with_tenant_tag(self) -> None:
        mock_sts = MagicMock()
        mock_sts.assume_role.return_value = {
            "Credentials": {
                "SessionToken": "tok123",
                "AccessKeyId": "AKID",
                "SecretAccessKey": "SECRET",
                "Expiration": _future_dt(),
            }
        }
        with patch("metamind.security.cloud_iam.BOTO3_AVAILABLE", True), \
             patch("metamind.security.cloud_iam.boto3") as mock_boto3:
            mock_boto3.client.return_value = mock_sts
            provider = AWSIAMProvider(region="us-east-1")
            creds = await provider.get_credentials(
                tenant_id="acme",
                role_arn="arn:aws:iam::123456:role/MetaMind-acme",
            )

        call_kwargs = mock_sts.assume_role.call_args[1]
        tags = call_kwargs.get("Tags", [])
        tenant_tag = next((t for t in tags if t["Key"] == "tenant_id"), None)
        assert tenant_tag is not None, "tenant_id tag must be present"
        assert tenant_tag["Value"] == "acme"

    async def test_get_credentials_returns_cloud_credentials(self) -> None:
        mock_sts = MagicMock()
        mock_sts.assume_role.return_value = {
            "Credentials": {
                "SessionToken": "tok",
                "AccessKeyId": "AK",
                "SecretAccessKey": "SK",
                "Expiration": _future_dt(),
            }
        }
        with patch("metamind.security.cloud_iam.BOTO3_AVAILABLE", True), \
             patch("metamind.security.cloud_iam.boto3") as mock_boto3:
            mock_boto3.client.return_value = mock_sts
            provider = AWSIAMProvider()
            creds = await provider.get_credentials("acme", "arn:aws:iam::x:role/r")

        assert isinstance(creds, CloudCredentials)
        assert creds.provider == "aws"
        assert creds.tenant_id == "acme"
        assert creds.access_token == "tok"

    async def test_inject_to_trino_sets_session_properties(self) -> None:
        provider = AWSIAMProvider()
        creds = CloudCredentials(
            provider="aws",
            access_token="session_token",
            expires_at=_future_dt(),
            tenant_id="acme",
            extra={"access_key_id": "AK", "secret_access_key": "SK"},
        )
        mock_trino = MagicMock()
        mock_trino.set_session_properties = AsyncMock()
        await provider.inject_to_trino(creds, mock_trino)
        mock_trino.set_session_properties.assert_called_once()
        props = mock_trino.set_session_properties.call_args[0][0]
        assert "hive.s3.aws-access-key" in props

    async def test_raises_when_boto3_unavailable(self) -> None:
        with patch("metamind.security.cloud_iam.BOTO3_AVAILABLE", False):
            provider = AWSIAMProvider()
            with self.assertRaises(RuntimeError):
                await provider.get_credentials("acme", "arn:aws:iam::x:role/r")


class TestCloudCredentials(unittest.TestCase):

    def test_is_expired_false_for_future(self) -> None:
        creds = CloudCredentials(
            provider="aws", access_token="t",
            expires_at=_future_dt(1), tenant_id="x",
        )
        assert creds.is_expired() is False

    def test_is_expired_true_for_past(self) -> None:
        creds = CloudCredentials(
            provider="aws", access_token="t",
            expires_at=datetime.now(tz=timezone.utc) - timedelta(minutes=1),
            tenant_id="x",
        )
        assert creds.is_expired() is True

    def test_ttl_seconds_minus_buffer(self) -> None:
        creds = CloudCredentials(
            provider="aws", access_token="t",
            expires_at=_future_dt(1),
            tenant_id="x",
        )
        ttl = creds.ttl_seconds(buffer_seconds=300)
        # Should be ~3300 s (3600 - 300)
        assert 3000 <= ttl <= 3600


class TestCloudIAMFederatorCaching(unittest.IsolatedAsyncioTestCase):

    def _make_federator(self, redis_mock: MagicMock) -> CloudIAMFederator:
        config = {
            "aws": {"role_arn": "arn:aws:iam::123:role/MetaMind-{tenant_id}", "region": "us-east-1"},
            "gcp": {"project_id": "p", "service_account": "{tenant_id}@p.iam.gserviceaccount.com"},
            "azure": {"client_id": "c"},
        }
        return CloudIAMFederator(config, redis_mock)

    async def test_cache_hit_skips_provider_call(self) -> None:
        redis_mock = AsyncMock()
        cached_creds = CloudCredentials(
            provider="aws", access_token="cached_tok",
            expires_at=_future_dt(), tenant_id="acme",
        )
        redis_mock.get = AsyncMock(return_value=json.dumps({
            "provider": "aws",
            "access_token": "cached_tok",
            "expires_at": cached_creds.expires_at.isoformat(),
            "tenant_id": "acme",
            "scopes": [],
            "extra": {},
        }))
        fed = self._make_federator(redis_mock)
        fed._fetch_from_provider = AsyncMock()

        creds = await fed.get_credentials_for_engine("trino", "acme")
        assert creds is not None
        assert creds.access_token == "cached_tok"
        fed._fetch_from_provider.assert_not_called()

    async def test_cache_miss_calls_provider(self) -> None:
        redis_mock = AsyncMock()
        redis_mock.get = AsyncMock(side_effect=Exception("key not found"))

        provider_creds = CloudCredentials(
            provider="aws", access_token="fresh_tok",
            expires_at=_future_dt(), tenant_id="acme",
        )
        fed = self._make_federator(redis_mock)
        fed._fetch_from_provider = AsyncMock(return_value=provider_creds)
        fed._set_cached = AsyncMock()

        creds = await fed.get_credentials_for_engine("trino", "acme")
        assert creds is not None
        assert creds.access_token == "fresh_tok"
        fed._fetch_from_provider.assert_called_once_with("trino", "acme")

    async def test_cache_stores_with_correct_ttl(self) -> None:
        redis_mock = AsyncMock()
        redis_mock.setex = AsyncMock()

        creds = CloudCredentials(
            provider="aws", access_token="tok",
            expires_at=_future_dt(1), tenant_id="acme",
        )
        fed = self._make_federator(redis_mock)
        await fed._set_cached("iam_cred:acme:trino", creds)

        redis_mock.setex.assert_called_once()
        call_args = redis_mock.setex.call_args[0]
        ttl = call_args[1]
        # TTL should be ~3300 (3600 - 300 buffer)
        assert 3000 <= ttl <= 3600

    async def test_gcp_route_calls_gcp_provider(self) -> None:
        redis_mock = AsyncMock()
        redis_mock.get = AsyncMock(side_effect=Exception("miss"))
        fed = self._make_federator(redis_mock)

        mock_gcp_creds = CloudCredentials(
            provider="gcp", access_token="gcp_tok",
            expires_at=_future_dt(), tenant_id="acme",
        )
        fed._gcp.get_credentials = AsyncMock(return_value=mock_gcp_creds)
        fed._set_cached = AsyncMock()

        creds = await fed.get_credentials_for_engine("bigquery", "acme")
        fed._gcp.get_credentials.assert_called_once()
        assert creds is not None
        assert creds.provider == "gcp"


if __name__ == "__main__":
    unittest.main()
