# ODEP Test Plan

## Overview

This document covers all test scenarios for the Open Data Engineering Platform (ODEP).
Each section maps to a component, lists test cases, inputs, and expected outcomes.

---

## 1. Data Models (`odep/models.py`)

### TC-M-01: Valid DatasetMetadata construction
- **Input**: `urn="urn:li:dataset:(bigquery,prod.events,prod)"`, `name="events"`, `owner="eng"`, `schema=[{"name":"id","type":"INTEGER"}]`
- **Expected**: Object created, no exception

### TC-M-02: Invalid URN pattern
- **Input**: `urn="not-a-urn"`, valid other fields
- **Expected**: `ValidationError` raised

### TC-M-03: Empty name
- **Input**: `name=""`, valid other fields
- **Expected**: `ValidationError` raised

### TC-M-04: Empty owner
- **Input**: `owner=""`, valid other fields
- **Expected**: `ValidationError` raised

### TC-M-05: Empty schema list
- **Input**: `schema=[]`, valid other fields
- **Expected**: `ValidationError` raised

### TC-M-06: LineageEdge self-loop
- **Input**: `source_urn="urn:li:dataset:(a,b,dev)"`, `target_urn="urn:li:dataset:(a,b,dev)"`
- **Expected**: `ValidationError` raised

### TC-M-07: Valid LineageEdge
- **Input**: different source and target URNs
- **Expected**: Object created successfully

### TC-M-08: JobDefinition invalid cron
- **Input**: `schedule="not-a-cron"`
- **Expected**: `ValidationError` raised

### TC-M-09: JobDefinition valid cron
- **Input**: `schedule="0 2 * * *"`
- **Expected**: Object created successfully

### TC-M-10: JobDefinition timeout_minutes = 0
- **Input**: `timeout_minutes=0`
- **Expected**: `ValidationError` raised

### TC-M-11: JobDefinition retries = -1
- **Input**: `retries=-1`
- **Expected**: `ValidationError` raised

### TC-M-12: JobStatus enum values
- **Input**: `JobStatus.PENDING`, `RUNNING`, `SUCCESS`, `FAILED`, `RETRYING`, `CANCELLED`
- **Expected**: All 6 values accessible as strings

### TC-M-13: EngineType enum values
- **Input**: `EngineType.SPARK`, `FLINK`, `DBT`, `PYTHON`, `SQL`
- **Expected**: All 5 values accessible as strings

---

## 2. Configuration (`odep/config.py`)

### TC-C-01: Default OdepConfig loads without env file
- **Input**: `OdepConfig()` with no `.odep.env` present
- **Expected**: Object created with all defaults (`metadata.engine="openmeta"`, `orchestration.engine="airflow"`, `execution.default_engine="duckdb"`)

### TC-C-02: Invalid metadata engine
- **Input**: `ODEP_METADATA__ENGINE=invalid` in environment
- **Expected**: `ValidationError` raised

### TC-C-03: Invalid orchestration engine
- **Input**: `ODEP_ORCHESTRATION__ENGINE=invalid`
- **Expected**: `ValidationError` raised

### TC-C-04: Invalid execution engine
- **Input**: `ODEP_EXECUTION__DEFAULT_ENGINE=invalid`
- **Expected**: `ValidationError` raised

### TC-C-05: Valid metamind config
- **Input**: `ODEP_METAMIND__METAMIND_URL=http://mm:8000`, `ODEP_METAMIND__TENANT_ID=t1`
- **Expected**: `MetaMindConfig` populated correctly

### TC-C-06: get_config() returns singleton
- **Input**: Call `get_config()` twice
- **Expected**: Both calls return the same object (lru_cache)

---

## 3. Exceptions (`odep/exceptions.py`)

### TC-E-01: AdapterNotFoundError message
- **Input**: `AdapterNotFoundError("execution", "oracle", ["spark","duckdb"])`
- **Expected**: Message contains `"oracle"`, `"execution"`, and valid options list

### TC-E-02: ProtocolViolationError message
- **Input**: `ProtocolViolationError(MyClass, MetadataService)`
- **Expected**: Message contains class name and protocol name

### TC-E-03: OrchestratorConnectionError message
- **Input**: `OrchestratorConnectionError("http://airflow:8080", "Run odep local up")`
- **Expected**: Message contains URL and hint

### TC-E-04: QualityGateFailure stores attributes
- **Input**: `QualityGateFailure("row_count", {"rows": 0})`
- **Expected**: `e.rule_name == "row_count"`, `e.metrics == {"rows": 0}`

### TC-E-05: PipelineParseError with line number
- **Input**: `PipelineParseError("pipeline.yaml", "name", "missing", 42)`
- **Expected**: Message contains `"line 42"`

### TC-E-06: SchemaDriftWarning is both Warning and Exception
- **Input**: `SchemaDriftWarning("urn:...", {"added": [], "removed": []})`
- **Expected**: `isinstance(e, UserWarning)` and `isinstance(e, Exception)` both True

---

## 4. AdapterFactory (`odep/factory.py`)

### TC-F-01: get_metadata_adapter("openmeta") returns OpenMetaAdapter
- **Input**: `get_metadata_adapter("openmeta", MetadataConfig())`
- **Expected**: Returns `OpenMetaAdapter` instance satisfying `MetadataService` Protocol

### TC-F-02: get_execution_adapter("duckdb") returns DuckDbAdapter
- **Input**: `get_execution_adapter("duckdb", ExecutionConfig())`
- **Expected**: Returns `DuckDbAdapter` instance satisfying `ExecutionEngine` Protocol

### TC-F-03: get_execution_adapter("metamind") returns MetaMindAdapter
- **Input**: `get_execution_adapter("metamind", MetaMindConfig())`
- **Expected**: Returns `MetaMindAdapter` instance satisfying `ExecutionEngine` Protocol

### TC-F-04: Unknown engine raises AdapterNotFoundError
- **Input**: `get_metadata_adapter("oracle", MetadataConfig())`
- **Expected**: `AdapterNotFoundError` raised with valid options listed

### TC-F-05: Unknown orchestration engine
- **Input**: `get_orchestrator_adapter("jenkins", OrchestrationConfig())`
- **Expected**: `AdapterNotFoundError` raised

### TC-F-06: All registered engines resolve without error
- **Input**: Call each resolver for every registered engine name
- **Expected**: No `AdapterNotFoundError` raised; all return Protocol-satisfying instances

---

## 5. OpenMetaAdapter — Catalog (`odep/adapters/openmeta/adapter.py`)

### TC-OC-01: register_dataset returns URN
- **Input**: Valid `DatasetMetadata`
- **Expected**: Returns `dataset.urn` string

### TC-OC-02: register_dataset is idempotent
- **Input**: Same `DatasetMetadata` registered twice
- **Expected**: Same URN returned both times; `get_dataset` returns one entry

### TC-OC-03: get_dataset returns None for unknown URN
- **Input**: `get_dataset("urn:li:dataset:(x,y,dev)")`
- **Expected**: Returns `None`

### TC-OC-04: get_dataset returns registered dataset
- **Input**: Register then get same URN
- **Expected**: Returns the `DatasetMetadata` object

### TC-OC-05: search_catalog matches name
- **Input**: Register dataset with `name="user_events"`, search `"user"`
- **Expected**: Dataset appears in results

### TC-OC-06: search_catalog matches description
- **Input**: Register dataset with `description="daily batch"`, search `"batch"`
- **Expected**: Dataset appears in results

### TC-OC-07: search_catalog matches tags
- **Input**: Register dataset with `tags=["PII"]`, search `"pii"`
- **Expected**: Dataset appears in results (case-insensitive)

### TC-OC-08: search_catalog skips deleted datasets
- **Input**: Register, delete, then search
- **Expected**: Deleted dataset not in results

### TC-OC-09: delete_dataset returns True for registered URN
- **Input**: Register then delete
- **Expected**: Returns `True`

### TC-OC-10: delete_dataset returns False for unknown URN
- **Input**: `delete_dataset("urn:li:dataset:(x,y,dev)")` without registering
- **Expected**: Returns `False`

### TC-OC-11: get_dataset returns None after soft-delete
- **Input**: Register, delete, then get
- **Expected**: Returns `None`

### TC-OC-12: Schema drift warning on re-registration
- **Input**: Register dataset, re-register with different schema (added/removed column)
- **Expected**: `SchemaDriftWarning` emitted with non-empty diff

### TC-OC-13: No schema drift warning when schema unchanged
- **Input**: Register dataset twice with identical schema
- **Expected**: No `SchemaDriftWarning` emitted

---

## 6. OpenMetaAdapter — Lineage

### TC-OL-01: create_lineage stores edges
- **Input**: `create_lineage([LineageEdge(src, tgt)])`
- **Expected**: `get_upstream(tgt)` returns the edge

### TC-OL-02: create_lineage rejects self-referential edge
- **Input**: `LineageEdge(source_urn="urn:...", target_urn="urn:...")` with same URN
- **Expected**: `ValueError` raised (caught at model level as `ValidationError`)

### TC-OL-03: get_upstream depth=1
- **Input**: A → B → C lineage; call `get_upstream("C", depth=1)`
- **Expected**: Returns only the B→C edge

### TC-OL-04: get_upstream depth=2
- **Input**: A → B → C lineage; call `get_upstream("C", depth=2)`
- **Expected**: Returns both A→B and B→C edges

### TC-OL-05: get_downstream depth=1
- **Input**: A → B → C lineage; call `get_downstream("A", depth=1)`
- **Expected**: Returns only the A→B edge

### TC-OL-06: get_full_upstream terminates on cyclic graph
- **Input**: A → B → C → A (cycle); call `get_full_upstream("A")`
- **Expected**: Returns without infinite loop; A not in its own upstream sources

### TC-OL-07: get_full_upstream respects max_depth=10 cap
- **Input**: Chain of 15 nodes; call `get_full_upstream("node_15", max_depth=15)`
- **Expected**: Only traverses 10 hops maximum

---

## 7. OpenMetaAdapter — Quality & Governance

### TC-OQ-01: get_quality_score returns 0.0 with no checks
- **Input**: `get_quality_score("urn:...")` with no recorded checks
- **Expected**: Returns `0.0`

### TC-OQ-02: get_quality_score with all passing
- **Input**: Record 3 passing checks
- **Expected**: Returns `100.0`

### TC-OQ-03: get_quality_score with mixed results
- **Input**: Record 2 passing, 1 failing
- **Expected**: Returns `66.666...`

### TC-OQ-04: get_quality_score always in [0.0, 100.0]
- **Input**: Any combination of pass/fail checks
- **Expected**: Score always between 0.0 and 100.0 inclusive

### TC-OQ-05: apply_tag associates tag with dataset
- **Input**: `apply_tag(urn, "finance")`
- **Expected**: Tag stored in `_tags[urn]`

### TC-OQ-06: apply_tag "PII" triggers encryption policy
- **Input**: `apply_tag(urn, "PII")`
- **Expected**: `"encryption_policy_applied"` added to `_tags[urn]`

### TC-OQ-07: check_access returns True for admin
- **Input**: `check_access("admin", urn, "delete")`
- **Expected**: Returns `True`

### TC-OQ-08: check_access returns False for unauthorized user
- **Input**: `check_access("bob", urn, "write")` without granting access
- **Expected**: Returns `False`

### TC-OQ-09: check_access returns True after grant_access
- **Input**: `grant_access(urn, "bob", "read")`, then `check_access("bob", urn, "read")`
- **Expected**: Returns `True`

---

## 8. DuckDbAdapter (`odep/adapters/duckdb/adapter.py`)

### TC-DK-01: submit sync returns job_handle
- **Input**: `submit(JobConfig(engine=SQL, code="SELECT 1"), async_run=False)`
- **Expected**: Returns a UUID string; job status is `"SUCCESS"`

### TC-DK-02: submit async returns immediately
- **Input**: `submit(JobConfig(...), async_run=True)`
- **Expected**: Returns handle before job completes; status may be `"RUNNING"`

### TC-DK-03: wait_for_completion returns JobResult on success
- **Input**: Submit `"SELECT 1"`, then `wait_for_completion(handle)`
- **Expected**: `result.success == True`, `result.execution_time_ms >= 0`

### TC-DK-04: wait_for_completion raises TimeoutError
- **Input**: Submit a long-running job with `timeout_sec=0.001`
- **Expected**: `TimeoutError` raised; job status set to `"CANCELLED"`

### TC-DK-05: cancel sets status to CANCELLED
- **Input**: Submit async job, immediately call `cancel(handle)`
- **Expected**: `get_status(handle)["status"] == "CANCELLED"`

### TC-DK-06: get_metrics returns required keys
- **Input**: Submit and complete a job, call `get_metrics(handle)`
- **Expected**: Dict contains `"execution_time_ms"`, `"rows_processed"`, `"backend_used"`

### TC-DK-07: stream_logs yields captured output
- **Input**: Submit SQL that produces stdout output
- **Expected**: `list(stream_logs(handle))` is a list (may be empty for pure SQL)

### TC-DK-08: invalid SQL sets status to FAILED
- **Input**: `submit(JobConfig(engine=SQL, code="SELECT * FROM nonexistent_table_xyz"))`
- **Expected**: `result.success == False`, `result.error_message` is set

---

## 9. AirflowAdapter (`odep/adapters/airflow/adapter.py`)

> These tests require mocking `httpx.Client`. Use `unittest.mock.MagicMock`.

### TC-AF-01: deploy_job calls POST /api/v1/dags
- **Input**: Mock returns 200; call `deploy_job(JobDefinition(...))`
- **Expected**: Returns `job.job_id`; POST was called with correct URL

### TC-AF-02: trigger_job returns dag_run_id
- **Input**: Mock returns `{"dag_run_id": "run_abc"}`
- **Expected**: Returns `"run_abc"`; stored in `_dag_run_map`

### TC-AF-03: get_status maps "success" to JobStatus.SUCCESS
- **Input**: Mock returns `{"state": "success"}`
- **Expected**: Returns `JobStatus.SUCCESS`

### TC-AF-04: get_status maps "running" to JobStatus.RUNNING
- **Input**: Mock returns `{"state": "running"}`
- **Expected**: Returns `JobStatus.RUNNING`

### TC-AF-05: get_status maps "failed" to JobStatus.FAILED
- **Input**: Mock returns `{"state": "failed"}`
- **Expected**: Returns `JobStatus.FAILED`

### TC-AF-06: cancel_run patches state to "failed"
- **Input**: Mock returns 200 on PATCH
- **Expected**: Returns `True`

### TC-AF-07: backfill returns 7 run_ids for 7-day range
- **Input**: `start=2024-01-01`, `end=2024-01-08`
- **Expected**: Returns list of 7 run_ids in chronological order

### TC-AF-08: backfill run_ids are chronologically ordered
- **Input**: Any valid date range
- **Expected**: `run_ids == sorted(run_ids)`

### TC-AF-09: health_check returns True when metadatabase healthy
- **Input**: Mock returns `{"metadatabase": {"status": "healthy"}}`
- **Expected**: Returns `True`

### TC-AF-10: health_check returns False on connection error
- **Input**: Mock raises `httpx.RequestError`
- **Expected**: Returns `False` (no exception propagated)

### TC-AF-11: delete_job returns True on 204
- **Input**: Mock returns status 204
- **Expected**: Returns `True`

### TC-AF-12: delete_job returns False on 404
- **Input**: Mock returns status 404
- **Expected**: Returns `False`

---

## 10. MetaMindClient (`odep/adapters/metamind/client.py`)

### TC-MC-01: query returns QueryResponse
- **Input**: Mock POST returns valid JSON with all 10 fields
- **Expected**: Returns `QueryResponse` with correct field values

### TC-MC-02: query raises AuthenticationError on 401
- **Input**: Mock POST returns status 401
- **Expected**: `AuthenticationError("metamind", ...)` raised

### TC-MC-03: query raises AuthenticationError on 403
- **Input**: Mock POST returns status 403
- **Expected**: `AuthenticationError` raised

### TC-MC-04: query raises RuntimeError on other HTTP errors
- **Input**: Mock POST returns status 500
- **Expected**: `RuntimeError` raised

### TC-MC-05: cancel returns True on success
- **Input**: Mock POST returns 200
- **Expected**: Returns `True`

### TC-MC-06: get_history returns None on 404
- **Input**: Mock GET returns 404
- **Expected**: Returns `None`

### TC-MC-07: get_history returns QueryResponse when found
- **Input**: Mock GET returns valid JSON
- **Expected**: Returns `QueryResponse`

---

## 11. MetaMindAdapter (`odep/adapters/metamind/adapter.py`)

### TC-MA-01: submit returns query_id as job_handle
- **Input**: Mock client returns `QueryResponse(query_id="q-1", ...)`
- **Expected**: Returns `"q-1"`

### TC-MA-02: get_metrics returns all 7 required keys
- **Input**: Submit a job, call `get_metrics(handle)`
- **Expected**: Dict contains `optimization_tier`, `cache_hit`, `workload_type`, `backend_used`, `optimization_ms`, `plan_cost`, `flags_used`

### TC-MA-03: get_metrics returns None defaults for unknown handle
- **Input**: `get_metrics("unknown-handle")`
- **Expected**: Returns dict with all 7 keys set to `None` or empty

### TC-MA-04: wait_for_completion builds JobResult from stored response
- **Input**: Submit then `wait_for_completion(handle)`
- **Expected**: `result.success == True`, `result.records_processed == response.row_count`

### TC-MA-05: MetaMindAdapter does not call MetadataService
- **Input**: Inspect all methods
- **Expected**: No calls to `register_dataset`, `create_lineage`, `apply_tag`, etc.

### TC-MA-06: stream_logs yields nothing
- **Input**: `list(stream_logs(handle))`
- **Expected**: Returns empty list `[]`

---

## 12. Pipeline SDK — Pipeline class (`odep/sdk/pipeline.py`)

### TC-PS-01: from_file loads valid YAML
- **Input**: Valid `odep.yaml` with name, sources, sinks
- **Expected**: Returns `Pipeline` with `is_valid() == True`

### TC-PS-02: from_file raises PipelineParseError on missing "name"
- **Input**: YAML without `name` field
- **Expected**: `PipelineParseError(path, "name", "required field missing")`

### TC-PS-03: from_file raises PipelineParseError on missing "sources"
- **Input**: YAML without `sources` field
- **Expected**: `PipelineParseError(path, "sources", ...)`

### TC-PS-04: from_file raises PipelineParseError on missing "sinks"
- **Input**: YAML without `sinks` field
- **Expected**: `PipelineParseError(path, "sinks", ...)`

### TC-PS-05: from_file raises PipelineParseError on malformed YAML
- **Input**: File with invalid YAML syntax
- **Expected**: `PipelineParseError` with line number when available

### TC-PS-06: from_file loads Python module with `pipeline` variable
- **Input**: `.py` file defining `pipeline = Pipeline(...)`
- **Expected**: Returns the `Pipeline` object

### TC-PS-07: from_file raises PipelineParseError for .py without pipeline
- **Input**: `.py` file with no `pipeline` variable or `get_pipeline()`
- **Expected**: `PipelineParseError` raised

### TC-PS-08: validate() rejects invalid cron
- **Input**: `Pipeline(name="x", schedule="bad-cron", sources=[...], sinks=[...])`
- **Expected**: `PipelineParseError` raised on `validate()`

### TC-PS-09: to_job_definition returns correct job_id
- **Input**: `Pipeline(name="etl")`, call `to_job_definition("prod")`
- **Expected**: `job_def.job_id == "etl_prod"`

### TC-PS-10: extract_lineage_edges covers all source-sink pairs
- **Input**: 2 sources, 2 sinks
- **Expected**: Returns 4 `LineageEdge` objects (2×2 pairs)

### TC-PS-11: extract_lineage_edges deduplicates
- **Input**: Same source-sink pair added twice
- **Expected**: Only 1 edge returned

### TC-PS-12: extract_lineage_edges includes column-level edges
- **Input**: `lineage_config={"column_level": True}`, sources/sinks with `"columns"` lists
- **Expected**: Column-level `LineageEdge` objects with `transformation="column:src->tgt"`

---

## 13. Pipeline SDK — deploy_pipeline (`odep/sdk/deploy.py`)

### TC-DP-01: deploy_pipeline returns job_id on success
- **Input**: Valid pipeline YAML, mock orchestrator returning `"job_123"`, mock metadata
- **Expected**: Returns `"job_123"`

### TC-DP-02: deploy_pipeline raises OrchestratorConnectionError when health_check fails
- **Input**: Mock `health_check()` returns `False`
- **Expected**: `OrchestratorConnectionError` raised with URL

### TC-DP-03: deploy_pipeline registers sink datasets in catalog
- **Input**: Pipeline with 2 sinks, mock metadata adapter
- **Expected**: `register_dataset` called twice

### TC-DP-04: deploy_pipeline records lineage edges
- **Input**: Pipeline with 1 source, 1 sink
- **Expected**: `create_lineage` called with 1 edge

### TC-DP-05: deploy_pipeline raises PipelineParseError on bad file
- **Input**: Path to malformed YAML
- **Expected**: `PipelineParseError` propagated

---

## 14. Pipeline SDK — execute_with_quality_gate (`odep/sdk/execute.py`)

### TC-EQ-01: Returns JobResult when all quality rules pass
- **Input**: Mock orchestrator returns SUCCESS; no failing rules in metrics
- **Expected**: Returns `JobResult` with `success=True`

### TC-EQ-02: Raises QualityGateFailure on blocking rule failure
- **Input**: `result.metrics["quality_rules"] = [{"name": "r1", "passed": False, "is_blocking": True, "dataset_urn": "urn:..."}]`
- **Expected**: `QualityGateFailure("r1", ...)` raised

### TC-EQ-03: Non-blocking failure does not raise
- **Input**: `result.metrics["quality_rules"] = [{"name": "r1", "passed": False, "is_blocking": False, "dataset_urn": "urn:..."}]`
- **Expected**: Returns `JobResult` without raising

### TC-EQ-04: Raises PipelineExecutionError when orchestrator run FAILED
- **Input**: Mock `get_status` returns `JobStatus.FAILED`
- **Expected**: `PipelineExecutionError(run_id, logs)` raised

### TC-EQ-05: Records quality checks in metadata catalog
- **Input**: 2 quality rules in metrics
- **Expected**: `record_quality_check` called twice on metadata adapter

### TC-EQ-06: Raises TimeoutError when polling exceeds timeout
- **Input**: Mock `get_status` always returns `RUNNING`; timeout configured to 0.001s
- **Expected**: `TimeoutError` raised

---

## 15. CLI (`odep/cli/main.py` and commands)

> Use Click's `CliRunner` for all CLI tests.

### TC-CLI-01: odep --help shows all commands
- **Input**: `odep --help`
- **Expected**: Output contains `init`, `deploy`, `run`, `logs`, `test`, `lineage`, `cost`, `config`, `local`, `template`

### TC-CLI-02: odep init creates project directory
- **Input**: `odep init my-project --engine=duckdb`
- **Expected**: `my-project/` created with `.odep.env`, `odep.yaml`, `docker-compose.yml`

### TC-CLI-03: odep init .odep.env contains engine
- **Input**: `odep init my-project --engine=spark`
- **Expected**: `.odep.env` contains `ODEP_EXECUTION__DEFAULT_ENGINE=spark`

### TC-CLI-04: odep local up --help shows profile option
- **Input**: `odep local up --help`
- **Expected**: Output contains `--profile` with choices `full`, `minimal`

### TC-CLI-05: odep local down --help shows volumes flag
- **Input**: `odep local down --help`
- **Expected**: Output contains `--volumes`

### TC-CLI-06: odep deploy calls deploy_pipeline
- **Input**: Mock `deploy_pipeline` to return `"job_123"`; run `odep deploy pipeline.yaml --env=prod`
- **Expected**: Output contains `"job_123"`; exit code 0

### TC-CLI-07: odep deploy prints error on failure
- **Input**: Mock `deploy_pipeline` to raise `OrchestratorConnectionError`
- **Expected**: Output contains `"❌"`; exit code 1

### TC-CLI-08: odep run triggers job
- **Input**: Mock orchestrator `trigger_job` returns `"run_abc"`; run `odep run my_job`
- **Expected**: Output contains `"run_abc"`; exit code 0

### TC-CLI-09: odep run --backfill enqueues runs
- **Input**: Mock `backfill` returns 7 run_ids; run `odep run my_job --backfill --start=2024-01-01 --end=2024-01-08`
- **Expected**: Output contains `"7"`; exit code 0

### TC-CLI-10: odep logs prints log lines
- **Input**: Mock `get_logs` returns `["line1", "line2"]`; run `odep logs run_abc --tail=2`
- **Expected**: Output contains `"line1"` and `"line2"`

### TC-CLI-11: odep test prints placeholder message
- **Input**: `odep test datasets/users --suite=critical`
- **Expected**: Output contains `"🧪"` and `"Great Expectations"`; exit code 0

### TC-CLI-12: odep lineage renders ASCII graph
- **Input**: Mock `get_full_upstream` returns graph with 1 edge; run `odep lineage urn:...`
- **Expected**: Output contains `"→"`; exit code 0

### TC-CLI-13: odep lineage prints "No upstream" when empty
- **Input**: Mock `get_full_upstream` returns empty graph
- **Expected**: Output contains `"No upstream lineage found"`

### TC-CLI-14: odep cost prints hint message
- **Input**: `odep cost`
- **Expected**: Output contains `"💰"`; exit code 0

### TC-CLI-15: odep config get reads value
- **Input**: `odep config get metadata.engine`
- **Expected**: Output contains `"openmeta"`; exit code 0

### TC-CLI-16: odep config get unknown key exits 1
- **Input**: `odep config get metadata.nonexistent`
- **Expected**: Output contains `"❌"`; exit code 1

### TC-CLI-17: odep config set writes to .odep.env
- **Input**: `odep config set execution.default_engine=metamind`
- **Expected**: `.odep.env` updated with `ODEP_EXECUTION__DEFAULT_ENGINE=metamind`; exit code 0

### TC-CLI-18: odep template list shows 4 templates
- **Input**: `odep template list`
- **Expected**: Output contains `batch-pipeline`, `streaming-pipeline`, `ml-feature-pipeline`, `dbt-project`

### TC-CLI-19: odep template use unknown name exits 1
- **Input**: `odep template use unknown-template`
- **Expected**: Output contains `"❌"`; exit code 1

### TC-CLI-20: odep template use valid name prints generation message
- **Input**: `odep template use batch-pipeline --name=my-etl`
- **Expected**: Output contains `"🍪"` and `"my-etl"`

---

## 16. API Gateway (`odep/api/`)

> Use FastAPI's `TestClient` for all API tests.

### TC-API-01: GET /health returns 200
- **Input**: `GET /health`
- **Expected**: `{"status": "ok"}`, status 200

### TC-API-02: GET /metadata/dataset/{urn} without token returns 403
- **Input**: No Authorization header
- **Expected**: Status 403 (HTTPBearer rejects missing token)

### TC-API-03: GET /metadata/dataset/{urn} with invalid JWT returns 401
- **Input**: `Authorization: Bearer invalid.token.here`
- **Expected**: Status 401

### TC-API-04: GET /metadata/dataset/{urn} with valid JWT returns dataset
- **Input**: Valid JWT; mock adapter returns dataset
- **Expected**: Status 200; response body contains dataset fields

### TC-API-05: GET /metadata/dataset/{urn} returns 404 for unknown URN
- **Input**: Valid JWT; mock adapter returns `None`
- **Expected**: Status 404

### TC-API-06: GET /metadata/search returns results
- **Input**: Valid JWT; `?q=events`; mock returns 2 datasets
- **Expected**: Status 200; response is list of 2 items

### TC-API-07: DELETE /metadata/dataset/{urn} returns deleted status
- **Input**: Valid JWT; mock returns `True`
- **Expected**: Status 200; `{"deleted": true}`

### TC-API-08: Rate limit returns 429 after threshold
- **Input**: Send 61 requests from same IP within 60 seconds
- **Expected**: 61st request returns status 429

### TC-API-09: GET /metrics returns Prometheus format
- **Input**: `GET /metrics`
- **Expected**: Status 200; content-type is `text/plain`; body contains `odep_pipeline_runs_total`

### TC-API-10: POST /execution/submit returns job_handle
- **Input**: Valid JWT; valid `JobConfig` body; mock DuckDbAdapter
- **Expected**: Status 200; response contains `"job_handle"`

---

## 17. JWT Authentication (`odep/api/auth.py`)

### TC-JWT-01: verify_jwt_token decodes valid token
- **Input**: Token signed with `"dev-secret-change-in-production"`, HS256
- **Expected**: Returns `TokenData` with correct `sub`

### TC-JWT-02: verify_jwt_token raises AuthenticationError on expired token
- **Input**: Token with `exp` in the past
- **Expected**: `AuthenticationError("api", ...)` raised

### TC-JWT-03: verify_jwt_token raises AuthenticationError on wrong secret
- **Input**: Token signed with different secret
- **Expected**: `AuthenticationError` raised

### TC-JWT-04: TokenData extracts roles from payload
- **Input**: Token with `{"sub": "user1", "roles": ["editor", "viewer"]}`
- **Expected**: `token_data.roles == ["editor", "viewer"]`

---

## 18. Integration Tests (existing, runnable with pytest)

### TC-INT-01: Full pipeline cycle (test_full_pipeline_cycle.py)
- **Run**: `pytest tests/integration/test_full_pipeline_cycle.py`
- **Expected**: 1 passed — registers datasets, creates lineage, executes SQL, records quality check, asserts score=100.0

### TC-INT-02: Engine swap (test_engine_swap.py)
- **Run**: `pytest tests/integration/test_engine_swap.py`
- **Expected**: 1 passed — DuckDbAdapter and mock MetaMindAdapter produce identical catalog state

### TC-INT-03: Backfill ordering (test_backfill.py)
- **Run**: `pytest tests/integration/test_backfill.py`
- **Expected**: 1 passed — 7-day backfill returns 7 run_ids in chronological order

### TC-INT-04: Schema drift (test_schema_drift.py)
- **Run**: `pytest tests/integration/test_schema_drift.py`
- **Expected**: 1 passed — re-registering with changed schema emits `SchemaDriftWarning` with non-empty diff

### TC-INT-05: Run all integration tests
- **Run**: `pytest tests/integration/ -v`
- **Expected**: 4 passed, 0 failed

---

## 19. Stub Adapter Behavior

### TC-STUB-01: DagsterAdapter raises NotImplementedError
- **Input**: `get_orchestrator_adapter("dagster", config).deploy_job(...)`
- **Expected**: `NotImplementedError` raised

### TC-STUB-02: SparkAdapter raises NotImplementedError
- **Input**: `get_execution_adapter("spark", config).submit(...)`
- **Expected**: `NotImplementedError` raised

### TC-STUB-03: All stub adapters satisfy their Protocol
- **Input**: `isinstance(adapter, Protocol)` for each stub
- **Expected**: `True` for all (method signatures present)

---

## 20. Quick Smoke Test Checklist (manual)

Run these commands in sequence after activating the venv:

```bash
# 1. Verify CLI is installed
odep --help

# 2. Init a test project
odep init test-project --engine=duckdb

# 3. Check config reads
odep config get metadata.engine
# Expected output: openmeta

# 4. Set a config value
odep config set execution.default_engine=duckdb
# Expected output: ✅ Set execution.default_engine = duckdb

# 5. List templates
odep template list
# Expected: 4 templates listed

# 6. Run all integration tests
pytest tests/integration/ -v
# Expected: 4 passed

# 7. Verify imports work
python3 -c "from odep.factory import get_metadata_adapter, get_execution_adapter; print('OK')"
# Expected: OK

# 8. Verify DuckDB execution
python3 -c "
from odep.adapters.duckdb.adapter import DuckDbAdapter
from odep.models import JobConfig, EngineType
from odep.config import ExecutionConfig
a = DuckDbAdapter(ExecutionConfig())
h = a.submit(JobConfig(engine=EngineType.SQL, code='SELECT 42 as answer'))
r = a.wait_for_completion(h)
print('success:', r.success, 'rows:', r.records_processed)
"
# Expected: success: True rows: 1
```

---

## Test Coverage Summary

| Component | Test Cases | Runnable Now |
|---|---|---|
| Data Models | TC-M-01 to TC-M-13 | Yes (pytest) |
| Configuration | TC-C-01 to TC-C-06 | Yes |
| Exceptions | TC-E-01 to TC-E-06 | Yes |
| AdapterFactory | TC-F-01 to TC-F-06 | Yes |
| OpenMetaAdapter Catalog | TC-OC-01 to TC-OC-13 | Yes |
| OpenMetaAdapter Lineage | TC-OL-01 to TC-OL-07 | Yes |
| OpenMetaAdapter Quality | TC-OQ-01 to TC-OQ-09 | Yes |
| DuckDbAdapter | TC-DK-01 to TC-DK-08 | Yes |
| AirflowAdapter | TC-AF-01 to TC-AF-12 | Yes (with mocks) |
| MetaMindClient | TC-MC-01 to TC-MC-07 | Yes (with mocks) |
| MetaMindAdapter | TC-MA-01 to TC-MA-06 | Yes (with mocks) |
| Pipeline SDK | TC-PS-01 to TC-PS-12 | Yes |
| deploy_pipeline | TC-DP-01 to TC-DP-05 | Yes (with mocks) |
| execute_with_quality_gate | TC-EQ-01 to TC-EQ-06 | Yes (with mocks) |
| CLI | TC-CLI-01 to TC-CLI-20 | Yes (CliRunner) |
| API Gateway | TC-API-01 to TC-API-10 | Yes (TestClient) |
| JWT Auth | TC-JWT-01 to TC-JWT-04 | Yes |
| Integration Tests | TC-INT-01 to TC-INT-05 | Yes — 4 passing |
| Stub Adapters | TC-STUB-01 to TC-STUB-03 | Yes |
| Smoke Tests | 8 manual checks | Manual |

**Total: 130 test cases**
