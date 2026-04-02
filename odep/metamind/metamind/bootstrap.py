"""MetaMind Application Bootstrap — AppContext DI container with lazy initialization."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional, Any

from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy import create_engine, Engine, text
import redis.asyncio as aioredis

from metamind.config.settings import AppSettings, get_settings
from metamind.bootstrap_tasks import start_background_tasks  # Phase 2 tasks
from metamind.bootstrap_pipeline import PipelineMixin  # W-06

logger = logging.getLogger(__name__)

@dataclass
class AppContext(PipelineMixin):
    """
    Application context containing all initialized resources.
    
    This is the central dependency injection container for MetaMind.
    All components receive the AppContext and access resources through it.
    """
    
    # Configuration
    settings: AppSettings = field(default_factory=get_settings)
    
    # Database connections (initialized lazily)
    _async_db_engine: Optional[AsyncEngine] = field(default=None, repr=False)
    _sync_db_engine: Optional[Engine] = field(default=None, repr=False)
    _redis_client: Optional[Any] = field(default=None, repr=False)
    
    # Engine connectors (initialized lazily)
    _trino_engine: Optional[Any] = field(default=None, repr=False)
    _oracle_connector: Optional[Any] = field(default=None, repr=False)
    _spark_engine: Optional[Any] = field(default=None, repr=False)
    
    # Core components (initialized lazily)
    _catalog: Optional[Any] = field(default=None, repr=False)
    _cdc_monitor: Optional[Any] = field(default=None, repr=False)
    _cache_manager: Optional[Any] = field(default=None, repr=False)
    _cost_model: Optional[Any] = field(default=None, repr=False)
    _query_router: Optional[Any] = field(default=None, repr=False)
    
    # New components (Phase 2)
    _planner: Optional[Any] = field(default=None, repr=False)
    _feature_store: Optional[Any] = field(default=None, repr=False)
    _health_registry: Optional[Any] = field(default=None, repr=False)
    _policy_manager: Optional[Any] = field(default=None, repr=False)
    _adaptive_router: Optional[Any] = field(default=None, repr=False)
    _execution_graph_engine: Optional[Any] = field(default=None, repr=False)
    _query_tracer: Optional[Any] = field(default=None, repr=False)
    _drift_detector: Optional[Any] = field(default=None, repr=False)

    # Phase 3 components
    _synthesis_engine: Optional[Any] = field(default=None, repr=False)
    _cancellation_tracker: Optional[Any] = field(default=None, repr=False)
    _active_tenant_ids: Optional[list] = field(default=None, repr=False)

    # W-06: Unified pipeline (QueryRouter + QueryEngine combined)
    _query_engine: Optional[Any] = field(default=None, repr=False)
    _unified_pipeline: Optional[Any] = field(default=None, repr=False)
    
    @property
    def async_db_engine(self) -> AsyncEngine:
        """Get or create async database engine."""
        if self._async_db_engine is None:
            self._async_db_engine = create_async_engine(
                self.settings.db.async_url,
                pool_size=self.settings.db.pool_size,
                max_overflow=self.settings.db.max_overflow,
                echo=self.settings.db.echo,
                pool_pre_ping=True,
            )
            logger.debug("Created async database engine")
        return self._async_db_engine
    
    @property
    def sync_db_engine(self) -> Engine:
        """Get or create sync database engine."""
        if self._sync_db_engine is None:
            self._sync_db_engine = create_engine(
                self.settings.db.sync_url,
                pool_size=self.settings.db.pool_size,
                max_overflow=self.settings.db.max_overflow,
                echo=self.settings.db.echo,
                pool_pre_ping=True,
            )
            logger.debug("Created sync database engine")
        return self._sync_db_engine
    
    @property
    def redis_client(self) -> Any:
        """Get or create Redis client."""
        if self._redis_client is None:
            self._redis_client = aioredis.from_url(
                self.settings.redis.url,
                socket_timeout=self.settings.redis.socket_timeout,
                socket_connect_timeout=self.settings.redis.socket_connect_timeout,
                health_check_interval=self.settings.redis.health_check_interval,
                decode_responses=True,
            )
            logger.debug("Created Redis client")
        return self._redis_client
    
    @property
    def trino_engine(self) -> Any:
        """Get or create Trino engine."""
        if self._trino_engine is None:
            from metamind.execution.trino_engine import TrinoEngine
            self._trino_engine = TrinoEngine(
                coordinator_url=self.settings.trino.coordinator_url,
                user=self.settings.trino.user,
                password=self.settings.trino.password,
                catalog=self.settings.trino.catalog,
                schema=self.settings.trino.schema,
                max_concurrent=self.settings.trino.max_concurrent,
                query_timeout=self.settings.trino.query_timeout,
            )
            logger.debug("Created Trino engine")
        return self._trino_engine
    
    @property
    def oracle_connector(self) -> Any:
        """Get or create Oracle connector."""
        if self._oracle_connector is None and self.settings.oracle.enabled:
            from metamind.execution.oracle_connector import OracleConnector
            self._oracle_connector = OracleConnector(
                host=self.settings.oracle.host,
                port=self.settings.oracle.port,
                service_name=self.settings.oracle.service_name,
                user=self.settings.oracle.user,
                password=self.settings.oracle.password,
                pool_config=None,
                circuit_threshold=self.settings.oracle.circuit_threshold,
                circuit_timeout=self.settings.oracle.circuit_timeout,
            )
            logger.debug("Created Oracle connector")
        return self._oracle_connector
    
    @property
    def spark_engine(self) -> Any:
        """Get or create Spark engine."""
        if self._spark_engine is None:
            from metamind.execution.spark_engine import SparkEngine
            self._spark_engine = SparkEngine(
                config=None,  # Use defaults
                enable_hive_support=True
            )
            logger.debug("Created Spark engine")
        return self._spark_engine
    
    @property
    def cdc_monitor(self) -> Any:
        """Get or create CDC monitor."""
        if self._cdc_monitor is None:
            from metamind.core.cdc_monitor import CDCMonitor
            self._cdc_monitor = CDCMonitor(engine=self.sync_db_engine)
            logger.debug("Created CDC monitor")
        return self._cdc_monitor
    
    @property
    def cache_manager(self) -> Any:
        """Get or create cache manager."""
        if self._cache_manager is None:
            from metamind.cache.result_cache import CacheManager
            self._cache_manager = CacheManager(
                redis_client=self.redis_client,
                settings=self.settings.cache,
            )
            logger.debug("Created cache manager")
        return self._cache_manager
    
    @property
    def cost_model(self) -> Any:
        """Get or create ML cost model."""
        if self._cost_model is None and self.settings.ml.training_enabled:
            from metamind.ml.cost_model import QueryCostModel
            self._cost_model = QueryCostModel(
                model_path=self.settings.ml.model_path,
                model_type=self.settings.ml.cost_model_type,
            )
            logger.debug("Created cost model")
        return self._cost_model
    
    @property
    def catalog(self) -> Any:
        """Get or create metadata catalog."""
        if self._catalog is None:
            from metamind.core.metadata.catalog import MetadataCatalog
            self._catalog = MetadataCatalog(engine=self.sync_db_engine)
            logger.debug("Created metadata catalog")
        return self._catalog
    
    @property
    def planner(self) -> Any:
        """Get or create cost-based planner."""
        if self._planner is None:
            from metamind.core.logical.planner import CostBasedPlanner
            self._planner = CostBasedPlanner(catalog=self.catalog)
            logger.debug("Created cost-based planner")
        return self._planner
    
    @property
    def feature_store(self) -> Any:
        """Get or create feature store."""
        if self._feature_store is None:
            from metamind.ml.feature_store import FeatureStore
            self._feature_store = FeatureStore(
                redis_client=self.redis_client,
                db_engine=self.sync_db_engine,
                feature_ttl_seconds=3600
            )
            logger.debug("Created feature store")
        return self._feature_store
    
    @property
    def health_registry(self) -> Any:
        """Get or create engine health registry."""
        if self._health_registry is None:
            from metamind.core.control_plane import EngineHealthRegistry
            self._health_registry = EngineHealthRegistry(
                redis_client=self.redis_client,
                check_interval_seconds=30
            )
            # Register engines
            self._health_registry.register_engine("trino", self.trino_engine)
            if self.oracle_connector:
                self._health_registry.register_engine("oracle", self.oracle_connector)
            if self.spark_engine:
                self._health_registry.register_engine("spark", self.spark_engine)
            logger.debug("Created health registry")
        return self._health_registry
    
    @property
    def policy_manager(self) -> Any:
        """Get or create routing policy manager."""
        if self._policy_manager is None:
            from metamind.core.control_plane import RoutingPolicyManager
            self._policy_manager = RoutingPolicyManager(
                db_engine=self.sync_db_engine,
                redis_client=self.redis_client
            )
            logger.debug("Created policy manager")
        return self._policy_manager
    
    @property
    def adaptive_router(self) -> Any:
        """Get or create adaptive router."""
        if self._adaptive_router is None:
            from metamind.core.adaptive_router import CDCLagAdaptiveRouter
            self._adaptive_router = CDCLagAdaptiveRouter(
                cdc_monitor=self.cdc_monitor,
                redis_client=self.redis_client,
                trend_window_minutes=5
            )
            logger.debug("Created adaptive router")
        return self._adaptive_router
    
    @property
    def execution_graph_engine(self) -> Any:
        """Get or create execution graph engine."""
        if self._execution_graph_engine is None:
            from metamind.core.physical.execution_graph import ExecutionGraphEngine
            self._execution_graph_engine = ExecutionGraphEngine(
                oracle_connector=self.oracle_connector,
                trino_engine=self.trino_engine,
                spark_engine=self.spark_engine,
                max_parallel_tasks=10
            )
            logger.debug("Created execution graph engine")
        return self._execution_graph_engine
    
    @property
    def query_tracer(self) -> Any:
        """Get or create query tracer."""
        if self._query_tracer is None:
            from metamind.observability.query_tracer import QueryTracer
            self._query_tracer = QueryTracer(enabled=True)
            logger.debug("Created query tracer")
        return self._query_tracer
    
    @property
    def drift_detector(self) -> Any:
        """Get or create drift detector with a wired alert callback."""
        if self._drift_detector is None:
            from metamind.observability.drift_detector import DriftDetector, DriftAlert
            from metamind.observability.metrics import DRIFT_ALERT_COUNTER

            def _on_drift_alert(alert: "DriftAlert") -> None:
                """Handle drift alerts: log structured warning + increment Prometheus counter."""
                logger.warning(
                    "DRIFT ALERT model_id=%s model_name=%s type=%s score=%.4f "
                    "threshold=%.4f severity=%s",
                    getattr(alert, "model_id", "unknown"),
                    getattr(alert, "model_name", "unknown"),
                    getattr(alert, "drift_type", "unknown"),
                    getattr(alert, "score", 0.0),
                    getattr(alert, "threshold", 0.0),
                    getattr(alert, "severity", "unknown"),
                )
                try:
                    drift_type = str(getattr(alert, "drift_type", "unknown"))
                    DRIFT_ALERT_COUNTER.labels(drift_type=drift_type).inc()
                except Exception as metric_exc:
                    logger.error("Failed to increment drift alert counter: %s", metric_exc)

            self._drift_detector = DriftDetector(
                db_engine=self.sync_db_engine,
                redis_client=self.redis_client,
                alert_callback=_on_drift_alert,
            )
            logger.debug("Created DriftDetector with alert callback")
        return self._drift_detector
    
    @property
    def synthesis_engine(self) -> Any:
        """Get or create the AI synthesis engine (lazy init)."""
        if self._synthesis_engine is None:
            from metamind.synthesis.synthesis_engine import SynthesisEngine
            self._synthesis_engine = SynthesisEngine(
                db_engine=self.async_db_engine,
                cost_model=self.cost_model,
                drift_detector=self.drift_detector,
                active_tenant_ids=list(self._active_tenant_ids or []),
            )
            logger.debug("Created SynthesisEngine")
        return self._synthesis_engine

    @property
    def cancellation_tracker(self) -> Any:
        """Singleton cancellation tracker shared across the request lifecycle."""
        if self._cancellation_tracker is None:
            from metamind.api.security_middleware import QueryCancellationTracker
            self._cancellation_tracker = QueryCancellationTracker()
            logger.debug("Created QueryCancellationTracker")
        return self._cancellation_tracker

    @property
    def query_router(self) -> Any:
        """Get or create query router."""
        if self._query_router is None:
            from metamind.core.router import QueryRouter
            from metamind.execution.gpu_engine import GPUEngine
            from metamind.core.gpu_router import GPURouter

            gpu_engine = GPUEngine(self.settings)
            gpu_router = GPURouter(gpu_engine, self.settings) if getattr(gpu_engine, "is_available", False) else None

            self._query_router = QueryRouter(
                catalog=self.catalog,
                cdc_monitor=self.cdc_monitor,
                cost_model=self.cost_model,
                cache_manager=self.cache_manager,
                settings=self.settings,
                planner=self.planner,
                feature_store=self.feature_store,
                health_registry=self.health_registry,
                policy_manager=self.policy_manager,
                adaptive_router=self.adaptive_router,
                spark_engine=self.spark_engine,
                gpu_router=gpu_router,
            )
            logger.debug("Created query router with GPU support")
        return self._query_router

    async def initialize(self) -> "AppContext":
        """Initialize all connections and verify engine health."""
        logger.info("Initializing MetaMind application context...")
        
        logger.debug("Testing database connection...")
        async with self.async_db_engine.connect() as conn:
            result = await conn.execute(text("SELECT 1"))
            result.fetchone()
        logger.info("Database connection verified")

        # Ensure the default tenant exists so query logging and FK relations
        # cannot fail during initial harness tests and local deployments.
        await self._ensure_default_tenant()
        
        logger.debug("Testing Redis connection...")
        await self.redis_client.ping()
        logger.info("Redis connection verified")
        
        try:
            await self.trino_engine.connect()
            logger.info("Trino connection verified")
        except Exception as e:
            logger.warning(f"Trino not available: {e}")
        
        if self.settings.oracle.enabled and self.oracle_connector:
            try:
                await self.oracle_connector.initialize()
                logger.info("Oracle connection verified")
            except Exception as e:
                logger.warning(f"Oracle not available: {e}")
        
        await self.health_registry.start_monitoring()
        logger.info("Health monitoring started")

        # Phase 2: Start background tasks (MV auto-refresh, latency anomaly)
        await start_background_tasks(self)

        try:
            for tenant_id in (self._active_tenant_ids or []):
                self.synthesis_engine.add_tenant(tenant_id)
            logger.info(
                "SynthesisEngine: registered %d tenants",
                len(self._active_tenant_ids or []),
            )
        except Exception as exc:
            logger.error("Failed to register tenants with SynthesisEngine: %s", exc)

        logger.info("MetaMind application context initialized successfully")
        return self

    async def _ensure_default_tenant(self) -> None:
        """Idempotently seed the built-in default tenant."""
        try:
            async with self.async_db_engine.begin() as conn:
                await conn.execute(
                    text(
                        """
                        INSERT INTO mm_tenants (tenant_id, tenant_name, settings, is_active)
                        VALUES ('default', 'Default Tenant', '{}'::jsonb, TRUE)
                        ON CONFLICT (tenant_id) DO NOTHING
                        """
                    )
                )
            logger.debug("Ensured default tenant exists in mm_tenants")
        except Exception as exc:
            logger.warning("Could not ensure default tenant: %s", exc)
    
    async def close(self) -> None:
        """Close all connections and cleanup resources."""
        logger.info("Shutting down MetaMind application context...")
        
        await self.health_registry.stop_monitoring()
        logger.debug("Stopped health monitoring")
        
        if self._trino_engine:
            await self._trino_engine.close()
            logger.debug("Closed Trino engine")
        
        if self._oracle_connector:
            await self._oracle_connector.close()
            logger.debug("Closed Oracle connector")
        
        if self._spark_engine:
            await self._spark_engine.close()
            logger.debug("Closed Spark engine")
        
        if self._redis_client:
            await self._redis_client.close()
            logger.debug("Closed Redis client")
        
        if self._async_db_engine:
            await self._async_db_engine.dispose()
            logger.debug("Closed async database engine")
        
        if self._sync_db_engine:
            self._sync_db_engine.dispose()
            logger.debug("Closed sync database engine")
        
        logger.info("MetaMind application context shut down successfully")
    
    async def health_check(self) -> dict[str, Any]:
        """Perform comprehensive health check across all engines."""
        checks: dict[str, bool] = {"database": False, "redis": False, "trino": False, "oracle": False, "spark": False}
        try:
            async with self.async_db_engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
            checks["database"] = True
        except Exception as e:
            logger.warning("Database health check failed: %s", e)
        try:
            await self.redis_client.ping()
            checks["redis"] = True
        except Exception as e:
            logger.warning("Redis health check failed: %s", e)
        try:
            await self.trino_engine.connect()
            checks["trino"] = True
        except Exception as e:
            logger.warning("Trino health check failed: %s", e)
        if self.settings.oracle.enabled and self.oracle_connector:
            try:
                health = await self.oracle_connector.health_check()
                checks["oracle"] = health.get("status") == "healthy"
            except Exception as e:
                logger.warning("Oracle health check failed: %s", e)
        if self.spark_engine:
            try:
                health = await self.spark_engine.health_check()
                checks["spark"] = health.get("status") == "healthy"
            except Exception as e:
                logger.warning("Spark health check failed: %s", e)
        return {"status": "healthy" if all(checks.values()) else "degraded", "checks": checks, "version": "4.0.0"}

# Global context instance (for development only, use dependency injection in production)
_context: Optional[AppContext] = None

async def bootstrap(settings: Optional[AppSettings] = None) -> AppContext:
    """Bootstrap the MetaMind application."""
    global _context
    app_settings = settings or get_settings()
    _context = AppContext(settings=app_settings)
    await _context.initialize()
    return _context

def get_context() -> AppContext:
    """Get the global context (for development/testing only)."""
    if _context is None:
        raise RuntimeError("Application not bootstrapped. Call bootstrap() first.")
    return _context
