"""
MetaMind Application Settings

File: metamind/config/settings.py
Role: Configuration Management
Phase: 1
Dependencies: pydantic-settings

CRITICAL RULES:
1. All sensitive values loaded from environment variables
2. Validation on startup - fail fast on misconfiguration
3. Support for .env files in development
4. No dynamic global state - use dependency injection
"""

from __future__ import annotations

import logging
from functools import lru_cache
from typing import Optional, List

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)


class DatabaseSettings(BaseSettings):
    """PostgreSQL metadata store configuration."""
    
    model_config = SettingsConfigDict(env_prefix="METAMIND_DB__")
    
    host: str = Field(default="localhost", description="PostgreSQL host")
    port: int = Field(default=5432, description="PostgreSQL port")
    database: str = Field(default="metamind", description="Database name")
    user: str = Field(default="metamind", description="Database user")
    password: str = Field(default="metamind", description="Database password")
    pool_size: int = Field(default=20, description="Connection pool size")
    max_overflow: int = Field(default=10, description="Max overflow connections")
    echo: bool = Field(default=False, description="Echo SQL statements")
    
    @property
    def async_url(self) -> str:
        """Build async PostgreSQL URL."""
        return f"postgresql+asyncpg://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
    
    @property
    def sync_url(self) -> str:
        """Build sync PostgreSQL URL."""
        return f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


class RedisSettings(BaseSettings):
    """Redis cache configuration."""
    
    model_config = SettingsConfigDict(env_prefix="METAMIND_REDIS__")
    
    host: str = Field(default="localhost", description="Redis host")
    port: int = Field(default=6379, description="Redis port")
    db: int = Field(default=0, description="Redis database")
    password: Optional[str] = Field(default=None, description="Redis password")
    ssl: bool = Field(default=False, description="Use SSL connection")
    socket_timeout: int = Field(default=30, description="Socket timeout in seconds")
    socket_connect_timeout: int = Field(default=5, description="Socket connect timeout")
    health_check_interval: int = Field(default=30, description="Health check interval")
    
    @property
    def url(self) -> str:
        """Build Redis URL."""
        auth = f":{self.password}@" if self.password else ""
        protocol = "rediss" if self.ssl else "redis"
        return f"{protocol}://{auth}{self.host}:{self.port}/{self.db}"


class TrinoSettings(BaseSettings):
    """Trino query engine configuration."""
    
    model_config = SettingsConfigDict(env_prefix="METAMIND_TRINO__")
    
    coordinator_url: str = Field(
        default="http://localhost:8080",
        description="Trino coordinator URL"
    )
    user: str = Field(default="metamind", description="Trino user")
    password: Optional[str] = Field(default=None, description="Trino password")
    catalog: str = Field(default="iceberg", description="Default catalog")
    schema: str = Field(default="default", description="Default schema")
    max_concurrent: int = Field(default=100, description="Max concurrent queries")
    query_timeout: int = Field(default=300, description="Query timeout in seconds")
    fetch_size: int = Field(default=10000, description="Result fetch batch size")
    
    @field_validator("coordinator_url")
    @classmethod
    def validate_url(cls, v: str) -> str:
        """Ensure URL has proper scheme."""
        if not v.startswith(("http://", "https://")):
            raise ValueError("coordinator_url must start with http:// or https://")
        return v.rstrip("/")


class OracleSettings(BaseSettings):
    """Oracle database configuration with safety limits."""
    
    model_config = SettingsConfigDict(env_prefix="METAMIND_ORACLE__")
    
    host: str = Field(default="localhost", description="Oracle host")
    port: int = Field(default=1521, description="Oracle port")
    service_name: str = Field(default="ORCLPDB1", description="Oracle service name")
    user: str = Field(default="metamind_read", description="Oracle user")
    password: Optional[str] = Field(default=None, description="Oracle password")
    
    # Pool configuration
    min_sessions: int = Field(default=2, description="Min pool sessions")
    max_sessions: int = Field(default=20, description="Max pool sessions (global limit)")
    max_per_user: int = Field(default=5, description="Max sessions per user")
    session_timeout: int = Field(default=60, description="Session timeout in seconds")
    max_lifetime_session: int = Field(default=3600, description="Max session lifetime")
    
    # Circuit breaker
    circuit_threshold: int = Field(default=5, description="Errors before opening circuit")
    circuit_timeout: int = Field(default=60, description="Seconds before retry")
    
    # Query limits
    query_timeout: int = Field(default=60, description="Query timeout in seconds")
    max_fetch_size: int = Field(default=10000, description="Max rows per fetch")
    
    # Safety
    enabled: bool = Field(default=True, description="Enable Oracle connector")
    read_only: bool = Field(default=True, description="Enforce read-only mode")


class S3Settings(BaseSettings):
    """S3/Iceberg storage configuration."""
    
    model_config = SettingsConfigDict(env_prefix="METAMIND_S3__")
    
    bucket: str = Field(default="metamind-data-lake", description="S3 bucket name")
    region: str = Field(default="us-east-1", description="AWS region")
    endpoint_url: Optional[str] = Field(
        default=None,
        description="Custom endpoint URL (for MinIO)"
    )
    access_key_id: Optional[str] = Field(default=None, description="AWS access key")
    secret_access_key: Optional[str] = Field(default=None, description="AWS secret key")
    
    # Iceberg settings
    warehouse_path: str = Field(
        default="s3://metamind-data-lake/warehouse",
        description="Iceberg warehouse path"
    )
    
    @property
    def is_minio(self) -> bool:
        """Check if using MinIO (custom endpoint)."""
        return self.endpoint_url is not None and "minio" in self.endpoint_url


class CacheSettings(BaseSettings):
    """Multi-tier cache configuration."""
    
    model_config = SettingsConfigDict(env_prefix="METAMIND_CACHE__")
    
    # L1: In-memory (hot)
    l1_enabled: bool = Field(default=True, description="Enable L1 cache")
    l1_ttl_seconds: int = Field(default=300, description="L1 TTL (5 minutes)")
    l1_max_size: int = Field(default=10000, description="Max L1 entries")
    
    # L2: Redis (warm)
    l2_enabled: bool = Field(default=True, description="Enable L2 cache")
    l2_ttl_seconds: int = Field(default=3600, description="L2 TTL (1 hour)")
    l2_max_size_mb: int = Field(default=100, description="Max L2 entry size in MB")
    
    # L3: S3 (cold)
    l3_enabled: bool = Field(default=True, description="Enable L3 cache")
    l3_ttl_days: int = Field(default=7, description="L3 TTL (7 days)")
    
    # Cache key settings
    fingerprint_algorithm: str = Field(default="sha256", description="Hash algorithm")
    include_user_context: bool = Field(default=True, description="Include user in key")


class MLSettings(BaseSettings):
    """Machine learning model configuration."""
    
    model_config = SettingsConfigDict(env_prefix="METAMIND_ML__")
    
    # Model storage
    model_path: str = Field(default="./models", description="Model storage path")
    
    # Cost model settings
    cost_model_type: str = Field(default="xgboost", description="Model type")
    cost_model_version: str = Field(default="v1", description="Model version")
    
    # Training settings
    training_enabled: bool = Field(default=True, description="Enable auto-training")
    training_schedule: str = Field(default="0 2 * * *", description="Training cron schedule")
    min_training_samples: int = Field(default=1000, description="Min samples for training")
    
    # Prediction settings
    prediction_timeout_ms: int = Field(default=10, description="Max prediction time")
    confidence_threshold: float = Field(default=0.7, description="Min confidence for routing")
    
    # Feature engineering
    feature_cache_ttl: int = Field(default=300, description="Feature cache TTL")


class CDCSettings(BaseSettings):
    """CDC pipeline configuration."""
    
    model_config = SettingsConfigDict(env_prefix="METAMIND_CDC__")
    
    # Kafka settings
    kafka_bootstrap_servers: str = Field(
        default="localhost:9092",
        description="Kafka bootstrap servers"
    )
    kafka_group_id: str = Field(default="metamind-cdc", description="Kafka consumer group")
    
    # Debezium settings
    debezium_url: str = Field(
        default="http://localhost:8083",
        description="Debezium Connect URL"
    )
    
    # Spark settings
    spark_master: str = Field(default="local[*]", description="Spark master URL")
    spark_app_name: str = Field(default="metamind-cdc", description="Spark app name")
    
    # Lag thresholds
    healthy_lag_seconds: int = Field(default=300, description="Healthy lag threshold (5 min)")
    warning_lag_seconds: int = Field(default=600, description="Warning lag threshold (10 min)")
    critical_lag_seconds: int = Field(default=1800, description="Critical lag threshold (30 min)")
    
    # Monitoring
    lag_check_interval: int = Field(default=30, description="Lag check interval in seconds")


class SecuritySettings(BaseSettings):
    """Security and authentication configuration."""
    
    model_config = SettingsConfigDict(env_prefix="METAMIND_SECURITY__")
    
    # JWT settings
    jwt_secret: str = Field(default="change-me-in-production", description="JWT secret key")
    jwt_algorithm: str = Field(default="HS256", description="JWT algorithm")
    jwt_expiration_hours: int = Field(default=24, description="JWT expiration in hours")
    
    # API settings
    api_key_header: str = Field(default="X-API-Key", description="API key header name")
    rate_limit_requests: int = Field(default=1000, description="Rate limit per minute")
    
    # RBAC
    admin_roles: List[str] = Field(
        default_factory=lambda: ["admin", "superuser"],
        description="Admin role names"
    )
    
    # Data masking
    masking_enabled: bool = Field(default=True, description="Enable data masking")
    default_masking_policy: str = Field(default="partial", description="Default masking policy")


class ObservabilitySettings(BaseSettings):
    """Observability and monitoring configuration."""
    
    model_config = SettingsConfigDict(env_prefix="METAMIND_OBSERVABILITY__")
    
    # Metrics
    metrics_enabled: bool = Field(default=True, description="Enable Prometheus metrics")
    metrics_port: int = Field(default=9090, description="Metrics port")
    metrics_path: str = Field(default="/metrics", description="Metrics endpoint path")
    
    # Tracing
    tracing_enabled: bool = Field(default=True, description="Enable OpenTelemetry tracing")
    jaeger_endpoint: Optional[str] = Field(default=None, description="Jaeger collector endpoint")
    sampling_rate: float = Field(default=0.1, description="Trace sampling rate")
    
    # Logging
    log_level: str = Field(default="INFO", description="Log level")
    log_format: str = Field(default="json", description="Log format (json|text)")
    structured_logging: bool = Field(default=True, description="Enable structured logging")
    
    # Alerting
    alert_webhook_url: Optional[str] = Field(default=None, description="Alert webhook URL")


class GPUSettings(BaseSettings):
    """GPU acceleration configuration."""
    
    model_config = SettingsConfigDict(env_prefix="METAMIND_GPU__")
    
    enabled: bool = Field(default=False, description="Enable GPU acceleration")
    device_id: int = Field(default=0, description="CUDA device ID")
    memory_fraction: float = Field(default=0.8, description="GPU memory fraction to use")
    
    # cuDF settings
    cudf_enabled: bool = Field(default=True, description="Enable cuDF acceleration")
    
    # Operator settings
    min_gpu_rows: int = Field(default=100000, description="Min rows for GPU processing")


class AppSettings(BaseSettings):
    """Main application settings aggregating all sub-configs."""
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )
    
    # Application
    app_name: str = Field(default="metamind", description="Application name")
    app_env: str = Field(default="development", description="Environment")
    debug: bool = Field(default=False, description="Debug mode")
    
    # API Server
    host: str = Field(default="0.0.0.0", description="API host")
    port: int = Field(default=8000, description="API port")
    workers: int = Field(default=1, description="Number of worker processes")
    plan_cache_ttl_seconds: int = Field(
        default=3600,
        description="Plan cache TTL in seconds"
    )
    queue_timeout_seconds: int = Field(
        default=300,
        description="Queue timeout in seconds"
    )
    
    # Sub-configurations
    db: DatabaseSettings = Field(default_factory=DatabaseSettings)
    redis: RedisSettings = Field(default_factory=RedisSettings)
    trino: TrinoSettings = Field(default_factory=TrinoSettings)
    oracle: OracleSettings = Field(default_factory=OracleSettings)
    s3: S3Settings = Field(default_factory=S3Settings)
    cache: CacheSettings = Field(default_factory=CacheSettings)
    ml: MLSettings = Field(default_factory=MLSettings)
    cdc: CDCSettings = Field(default_factory=CDCSettings)
    security: SecuritySettings = Field(default_factory=SecuritySettings)
    observability: ObservabilitySettings = Field(default_factory=ObservabilitySettings)
    gpu: GPUSettings = Field(default_factory=GPUSettings)
    
    @property
    def is_development(self) -> bool:
        """Check if running in development mode."""
        return self.app_env == "development"
    
    @property
    def is_production(self) -> bool:
        """Check if running in production mode."""
        return self.app_env == "production"

    # Backward-compatible aliases for modules still using legacy settings names.
    @property
    def env(self) -> str:
        return self.app_env

    @property
    def secret_key(self) -> str:
        return self.security.jwt_secret

    @property
    def jwt_algorithm(self) -> str:
        return self.security.jwt_algorithm


@lru_cache
def get_settings() -> AppSettings:
    """Get cached application settings."""
    settings = AppSettings()
    logger.debug(f"Loaded settings for environment: {settings.app_env}")
    return settings
