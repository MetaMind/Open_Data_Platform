"""
Cross-Cloud IAM Federator — Tenant-Scoped Credential Propagation

File: metamind/security/cloud_iam.py
Role: Security Engineer
Dependencies: boto3 (optional), google-auth (optional), azure-identity (optional),
              redis.asyncio, metamind.config.settings

Propagates tenant-scoped IAM credentials to federated execution engines
across AWS, GCP, and Azure.  Each provider is guarded with a try/except
ImportError so the module loads cleanly on any deployment configuration.
Credentials are cached in Redis with TTL = (expires_at − 5 minutes).
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Cloud SDK import guards
# ---------------------------------------------------------------------------
try:
    import boto3                  # type: ignore[import]
    BOTO3_AVAILABLE = True
except ImportError:
    boto3 = None                  # type: ignore[assignment]
    BOTO3_AVAILABLE = False
    logger.info("cloud_iam: boto3 not installed — AWS provider disabled")

try:
    import google.auth.impersonated_credentials as _gcp_imp  # type: ignore[import]
    import google.auth.transport.requests as _gcp_tr          # type: ignore[import]
    import google.oauth2.service_account as _gcp_sa           # type: ignore[import]
    GCP_AVAILABLE = True
except ImportError:
    _gcp_imp = None               # type: ignore[assignment]
    _gcp_tr = None                # type: ignore[assignment]
    _gcp_sa = None                # type: ignore[assignment]
    GCP_AVAILABLE = False
    logger.info("cloud_iam: google-auth not installed — GCP provider disabled")

try:
    from azure.identity import ManagedIdentityCredential      # type: ignore[import]
    AZURE_AVAILABLE = True
except ImportError:
    ManagedIdentityCredential = None  # type: ignore[assignment,misc]
    AZURE_AVAILABLE = False
    logger.info("cloud_iam: azure-identity not installed — Azure provider disabled")


# ---------------------------------------------------------------------------
# DTOs
# ---------------------------------------------------------------------------

@dataclass
class CloudCredentials:
    """Ephemeral cloud credentials for one tenant / engine pair."""
    provider: str
    access_token: str
    expires_at: datetime
    tenant_id: str
    scopes: List[str] = field(default_factory=list)
    extra: Dict[str, Any] = field(default_factory=dict)   # AWS: secret_key, session_token etc.

    def is_expired(self) -> bool:
        now = datetime.now(tz=timezone.utc)
        return self.expires_at.replace(tzinfo=timezone.utc) <= now

    def ttl_seconds(self, buffer_seconds: int = 300) -> int:
        """Seconds until expiry minus a safety buffer."""
        now = datetime.now(tz=timezone.utc)
        exp = self.expires_at.replace(tzinfo=timezone.utc)
        ttl = int((exp - now).total_seconds()) - buffer_seconds
        return max(1, ttl)


# ---------------------------------------------------------------------------
# AWS provider
# ---------------------------------------------------------------------------

class AWSIAMProvider:
    """Obtains short-lived AWS credentials via STS AssumeRole."""

    def __init__(self, region: str = "us-east-1") -> None:
        self._region = region

    async def get_credentials(
        self,
        tenant_id: str,
        role_arn: str,
        session_duration_seconds: int = 3600,
    ) -> CloudCredentials:
        """Call STS AssumeRole and return CloudCredentials."""
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 not installed — cannot assume AWS role")
        try:
            client = boto3.client("sts", region_name=self._region)
            response = client.assume_role(
                RoleArn=role_arn,
                RoleSessionName=f"metamind_{tenant_id}",
                DurationSeconds=session_duration_seconds,
                Tags=[{"Key": "tenant_id", "Value": tenant_id}],
            )
            creds = response["Credentials"]
            return CloudCredentials(
                provider="aws",
                access_token=creds["SessionToken"],
                expires_at=creds["Expiration"].replace(tzinfo=timezone.utc),
                tenant_id=tenant_id,
                scopes=["s3", "glue", "athena"],
                extra={
                    "access_key_id": creds["AccessKeyId"],
                    "secret_access_key": creds["SecretAccessKey"],
                    "session_token": creds["SessionToken"],
                    "role_arn": role_arn,
                },
            )
        except Exception as exc:
            logger.error(
                "AWSIAMProvider.get_credentials failed tenant=%s role=%s: %s",
                tenant_id,
                role_arn,
                exc,
            )
            raise

    async def inject_to_trino(
        self,
        credentials: CloudCredentials,
        trino_engine: Any,
    ) -> None:
        """Set S3 credentials in the Trino session."""
        if not hasattr(trino_engine, "set_session_properties"):
            logger.warning(
                "AWSIAMProvider.inject_to_trino: trino_engine has no set_session_properties"
            )
            return
        await trino_engine.set_session_properties(
            {
                "hive.s3.aws-access-key": credentials.extra.get("access_key_id", ""),
                "hive.s3.aws-secret-key": credentials.extra.get("secret_access_key", ""),
                "hive.s3.aws-session-token": credentials.access_token,
            }
        )
        logger.info(
            "AWSIAMProvider: S3 credentials injected for tenant=%s", credentials.tenant_id
        )


# ---------------------------------------------------------------------------
# GCP provider
# ---------------------------------------------------------------------------

class GCPIAMProvider:
    """Obtains GCP Workload Identity credentials via service account impersonation."""

    def __init__(self, project_id: str) -> None:
        self._project_id = project_id

    async def get_credentials(
        self,
        tenant_id: str,
        service_account: str,
        scopes: Optional[List[str]] = None,
    ) -> CloudCredentials:
        """Obtain impersonated service-account credentials."""
        if not GCP_AVAILABLE:
            raise RuntimeError("google-auth not installed — GCP credentials unavailable")
        if scopes is None:
            scopes = ["https://www.googleapis.com/auth/bigquery"]
        try:
            source_credentials, _ = _gcp_sa.default(scopes=scopes)  # type: ignore[call-arg]
            impersonated = _gcp_imp.Credentials(
                source_credentials=source_credentials,
                target_principal=service_account,
                target_scopes=scopes,
            )
            req = _gcp_tr.Request()
            impersonated.refresh(req)
            expires_at = datetime.now(tz=timezone.utc) + timedelta(hours=1)
            return CloudCredentials(
                provider="gcp",
                access_token=impersonated.token or "",
                expires_at=expires_at,
                tenant_id=tenant_id,
                scopes=scopes,
                extra={
                    "service_account": service_account,
                    "project_id": self._project_id,
                },
            )
        except Exception as exc:
            logger.error(
                "GCPIAMProvider.get_credentials failed tenant=%s sa=%s: %s",
                tenant_id,
                service_account,
                exc,
            )
            raise

    async def inject_to_bigquery(self, credentials: CloudCredentials) -> None:
        """Log credential injection; actual BigQuery client injection done by caller."""
        logger.info(
            "GCPIAMProvider: credentials ready for BigQuery project=%s tenant=%s",
            credentials.extra.get("project_id"),
            credentials.tenant_id,
        )


# ---------------------------------------------------------------------------
# Azure provider
# ---------------------------------------------------------------------------

class AzureIAMProvider:
    """Obtains Azure Managed Identity credentials."""

    def __init__(self, subscription_id: str = "") -> None:
        self._subscription_id = subscription_id

    async def get_credentials(
        self,
        tenant_id: str,
        client_id: str,
        resource: str = "https://database.windows.net/",
    ) -> CloudCredentials:
        """Acquire a Managed Identity access token."""
        if not AZURE_AVAILABLE:
            raise RuntimeError("azure-identity not installed — Azure credentials unavailable")
        try:
            mi_cred = ManagedIdentityCredential(client_id=client_id)
            token = mi_cred.get_token(resource)
            expires_at = datetime.fromtimestamp(token.expires_on, tz=timezone.utc)
            return CloudCredentials(
                provider="azure",
                access_token=token.token,
                expires_at=expires_at,
                tenant_id=tenant_id,
                scopes=[resource],
                extra={"client_id": client_id, "subscription_id": self._subscription_id},
            )
        except Exception as exc:
            logger.error(
                "AzureIAMProvider.get_credentials failed tenant=%s client=%s: %s",
                tenant_id,
                client_id,
                exc,
            )
            raise

    async def inject_to_synapse(self, credentials: CloudCredentials) -> None:
        """Log credential readiness; Synapse connection is built by the caller."""
        logger.info(
            "AzureIAMProvider: Synapse token ready tenant=%s expires=%s",
            credentials.tenant_id,
            credentials.expires_at.isoformat(),
        )


# ---------------------------------------------------------------------------
# CloudIAMFederator — aggregator
# ---------------------------------------------------------------------------

_CACHE_KEY_PREFIX = "iam_cred:"
_CACHE_BUFFER_SECONDS = 300   # refresh 5 min before expiry


class CloudIAMFederator:
    """
    Routes credential requests to the correct provider and caches results.

    Configuration dict example::

        {
            "aws":   {"role_arn": "arn:aws:iam::123:role/MetaMind-{tenant_id}",
                      "region": "us-east-1"},
            "gcp":   {"project_id": "my-project",
                      "service_account": "{tenant_id}@my-project.iam.gserviceaccount.com"},
            "azure": {"client_id": "...", "subscription_id": "..."},
        }
    """

    def __init__(
        self,
        config: Dict[str, Any],
        redis_client: Any,  # aioredis.Redis
    ) -> None:
        self._config = config
        self._redis = redis_client

        aws_cfg = config.get("aws", {})
        gcp_cfg = config.get("gcp", {})
        azure_cfg = config.get("azure", {})

        self._aws = AWSIAMProvider(region=aws_cfg.get("region", "us-east-1"))
        self._gcp = GCPIAMProvider(project_id=gcp_cfg.get("project_id", ""))
        self._azure = AzureIAMProvider(
            subscription_id=azure_cfg.get("subscription_id", "")
        )

    async def get_credentials_for_engine(
        self,
        engine: str,
        tenant_id: str,
    ) -> Optional[CloudCredentials]:
        """Return credentials for *engine* / *tenant_id*, using cache when available."""
        cache_key = f"{_CACHE_KEY_PREFIX}{tenant_id}:{engine}"
        cached = await self._get_cached(cache_key)
        if cached is not None:
            logger.info(
                "CloudIAMFederator: cache HIT engine=%s tenant=%s", engine, tenant_id
            )
            return cached

        creds = await self._fetch_from_provider(engine, tenant_id)
        if creds:
            await self._set_cached(cache_key, creds)
        return creds

    async def propagate_to_engine(
        self,
        engine: str,
        credentials: CloudCredentials,
        engine_obj: Optional[Any] = None,
    ) -> None:
        """Route credential injection to the correct provider."""
        provider = credentials.provider
        try:
            if provider == "aws":
                if engine_obj is not None:
                    await self._aws.inject_to_trino(credentials, engine_obj)
            elif provider == "gcp":
                await self._gcp.inject_to_bigquery(credentials)
            elif provider == "azure":
                await self._azure.inject_to_synapse(credentials)
            else:
                logger.warning(
                    "CloudIAMFederator.propagate_to_engine: unknown provider=%s", provider
                )
        except Exception as exc:
            logger.error(
                "CloudIAMFederator.propagate_to_engine failed engine=%s provider=%s: %s",
                engine,
                provider,
                exc,
            )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _fetch_from_provider(
        self, engine: str, tenant_id: str
    ) -> Optional[CloudCredentials]:
        """Determine provider from engine name and delegate."""
        engine_lower = engine.lower()
        try:
            if engine_lower in ("trino", "s3", "iceberg", "glue", "athena"):
                aws_cfg = self._config.get("aws", {})
                role_arn = aws_cfg.get("role_arn", "").replace("{tenant_id}", tenant_id)
                if role_arn:
                    return await self._aws.get_credentials(tenant_id, role_arn)
            elif engine_lower in ("bigquery", "gcp"):
                gcp_cfg = self._config.get("gcp", {})
                sa = gcp_cfg.get("service_account", "").replace("{tenant_id}", tenant_id)
                if sa:
                    return await self._gcp.get_credentials(tenant_id, sa)
            elif engine_lower in ("synapse", "azure"):
                azure_cfg = self._config.get("azure", {})
                client_id = azure_cfg.get("client_id", "")
                if client_id:
                    return await self._azure.get_credentials(tenant_id, client_id)
        except Exception as exc:
            logger.error(
                "CloudIAMFederator._fetch_from_provider engine=%s tenant=%s: %s",
                engine,
                tenant_id,
                exc,
            )
        return None

    async def _get_cached(self, key: str) -> Optional[CloudCredentials]:
        """Retrieve and deserialize cached credentials from Redis."""
        try:
            raw = await self._redis.get(key)
            if raw is None:
                return None
            data = json.loads(raw)
            return CloudCredentials(
                provider=data["provider"],
                access_token=data["access_token"],
                expires_at=datetime.fromisoformat(data["expires_at"]),
                tenant_id=data["tenant_id"],
                scopes=data.get("scopes", []),
                extra=data.get("extra", {}),
            )
        except Exception as exc:
            logger.error("CloudIAMFederator._get_cached key=%s: %s", key, exc)
            return None

    async def _set_cached(self, key: str, creds: CloudCredentials) -> None:
        """Serialize and store credentials in Redis with appropriate TTL."""
        try:
            ttl = creds.ttl_seconds(_CACHE_BUFFER_SECONDS)
            data = json.dumps(
                {
                    "provider": creds.provider,
                    "access_token": creds.access_token,
                    "expires_at": creds.expires_at.isoformat(),
                    "tenant_id": creds.tenant_id,
                    "scopes": creds.scopes,
                    "extra": creds.extra,
                }
            )
            await self._redis.setex(key, ttl, data)
            logger.info(
                "CloudIAMFederator: cached credentials key=%s ttl=%ds", key, ttl
            )
        except Exception as exc:
            logger.error("CloudIAMFederator._set_cached key=%s: %s", key, exc)
