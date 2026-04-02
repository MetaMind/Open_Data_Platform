 These are expected from two different causes:

  1. table_name/backend_used/executed_at errors

  - Those are from old MetaMind code still running (before my patch).
  - Your DB log shows the old SQL text exactly.
  - Fix: rebuild + recreate only metamind so new code is used.

  2. Role "hive" does not exist

  - hive-metastore is trying to connect to Postgres with user hive, but that role wasn’t created.
  - This affects Hive metastore path, not basic API health.

  Run this sequence:

  cd /home/oh20210736-ud/Documents/metamind/MetaMind_v104/metamind_complete

  docker compose build --no-cache metamind
  docker compose up -d --force-recreate metamind
  docker compose logs --tail=120 metamind

  Then verify old SQL errors stop:

  docker compose logs --tail=200 postgres | grep -E "backend_used|executed_at|table_name"

  For Hive role, create it once:

  docker compose exec postgres psql -U metamind -d postgres -c "CREATE ROLE hive LOGIN PASSWORD 'hive';"
  docker compose exec postgres psql -U metamind -d postgres -c "CREATE DATABASE metastore OWNER hive;"
  docker compose restart hive-metastore

  About the two metamind warnings:

  - TrinoSettings schema shadows BaseSettings: warning only.
  - strawberry-graphql not installed: warning only (GraphQL endpoint disabled), not API blocker.

sudo docker compose exec postgres sh -lc '
  export PGPASSWORD=metamind
  for f in /docker-entrypoint-initdb.d/*.sql; do
    echo "Applying $f"
    psql -h localhost -U metamind -d metamind -v ON_ERROR_STOP=1 -f "$f"
  done
  '

 Under current firewall policy, default tenant is effectively locked for ad-hoc SQL.
  So without changing rules, only already-approved fingerprints can run.

  /api/v1/query is guarded by fingerprint firewall, so “which query is allowed” depends on current Redis policy.

  Use these read-only checks (no rule changes):

  cd /home/oh20210736-ud/Documents/metamind/MetaMind_v104/metamind_complete
  sudo docker compose exec redis redis-cli GET mm:firewall:mode:default
  sudo docker compose exec redis redis-cli SMEMBERS mm:firewall:allow:default
  sudo docker compose exec redis redis-cli SMEMBERS mm:firewall:deny:default

  Then compute a query fingerprint (also read-only) and compare:

  sudo docker compose exec metamind python - <<'PY'
  import hashlib, re, sqlglot
  _LITERAL_RE = re.compile(r"'[^']*'|\b\d+(?:\.\d+)?\b", re.IGNORECASE)
  def fp(sql: str) -> str:
      try:
          stmts = sqlglot.parse(sql)
          if stmts and stmts[0] is not None:
              sql = stmts[0].sql(pretty=False)
      except Exception:
          pass
      s = sql.lower()
      s = _LITERAL_RE.sub("?", s)
      s = re.sub(r"\s+", " ", s).strip()
      print(hashlib.sha256(s.encode()).hexdigest())
  fp("SELECT status, COUNT(*) AS cnt FROM orders GROUP BY status ORDER BY cnt DESC")
  PY

  If that fingerprint is in denylist or not in allowlist (when mode=allowlist), it will be blocked.

  So answer to “what allowed to run”:

  - Only queries whose fingerprint is allowed by current tenant policy.
  - If allowlist is empty (or denylist broad), effectively no ad-hoc query is allowed for default.
  - In that case, use non-query endpoints (/api/v1/health, /metrics, /api/v1/cache/stats, /api/v1/cdc/status) or test with a tenant that already has approved fingerprints.

This explains it: query firewall is likely receiving an async Redis client but using sync-style calls, which can make the deny check behave incorrectly (and won’t show Redis commands in MONITOR). I’m patching
  QueryFirewall.check() to properly await async Redis operations.

• Edited MetaMind_v104/metamind_complete/metamind/core/security/query_firewall.py (+11 -2)
     13  import hashlib
     14 +import inspect
     15  import logging
        ⋮
     94              deny_key = self._DENY_KEY.format(tenant=tenant_id)
     94 -            if self._redis.sismember(deny_key, fp):  # type: ignore[union-attr]
     95 +            deny_member = self._redis.sismember(deny_key, fp)  # type: ignore[union-attr]
     96 +            if inspect.isawaitable(deny_member):
     97 +                deny_member = await deny_member
     98 +            if bool(deny_member):
     99                  logger.warning(
        ⋮
    109              mode = self._redis.get(mode_key)  # type: ignore[union-attr]
    110 +            if inspect.isawaitable(mode):
    111 +                mode = await mode
    112              if mode and (
        ⋮
    115                  allow_key = self._ALLOW_KEY.format(tenant=tenant_id)
    110 -                if not self._redis.sismember(allow_key, fp):  # type: ignore[union-attr]
    116 +                allow_member = self._redis.sismember(allow_key, fp)  # type: ignore[union-attr]
    117 +                if inspect.isawaitable(allow_member):
    118 +                    allow_member = await allow_member
    119 +                if not bool(allow_member):
    120                      logger.warning(

• Ran python3 -m py_compile /home/oh20210736-ud/Documents/metamind/MetaMind_v104/metamind_complete/metamind/core/security/query_firewall.py
  └ (no output)

───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────

• Found the root cause and fixed it.

  Why you saw firewall denies with empty Redis keys:

  - QueryFirewall.check() was calling Redis methods in sync style.
  - But runtime Redis client is async (redis.asyncio), so calls like sismember(...) returned coroutine objects.
  - Coroutine objects are truthy, so the code incorrectly treated queries as denylisted.

  Fix applied:

  - query_firewall.py
      - now detects awaitables and awaits:
          - sismember (deny/allow checks)
          - get (mode check)

  Now do:

  cd /home/oh20210736-ud/Documents/metamind/MetaMind_v104/metamind_complete
  sudo docker compose build metamind
  sudo docker compose up -d metamind

  Retest:

  curl -i -X POST http://localhost:8000/api/v1/query \
    -H "Content-Type: application/json" \
    -d '{"sql":"SELECT status, COUNT(*) AS cnt FROM orders GROUP BY status ORDER BY cnt DESC","tenant_id":"default","use_cache":true}'

  You should no longer get false denylist blocks from empty Redis sets.
  If it still errors, paste the new response/log tail and I’ll continue.

• Good catch. I fixed both code issues behind that response:

  1. SELECT has no FROM clause (false negative)

  - Patched builder.py to support sqlglot’s current key:
      - checks from or from_.

  2. PlanCache.get ... bytes-like object is required, not 'coroutine'

  - Patched plan_cache.py for async-redis compatibility in sync cache path:
      - detects awaitables and skips L2 redis in that case
      - decodes bytes before json.loads.

  Also, your log shows a separate DB data issue:

  - mm_query_logs insert failed because tenant default is missing from mm_tenants.

  Run this once:

  cd /home/oh20210736-ud/Documents/metamind/MetaMind_v104/metamind_complete
  sudo docker compose exec postgres psql -U metamind -d metamind -c \
  "INSERT INTO mm_tenants (tenant_id, tenant_name, settings) VALUES ('default','Default Tenant','{}'::jsonb) ON CONFLICT (tenant_id) DO NOTHING;"

  Now rebuild/restart metamind:

  sudo docker compose build metamind
  sudo docker compose up -d metamind

  Retest your query after restart.
=====================================================================
Run this final checklist from metamind_complete:

  1. Health

  curl -sS http://localhost:8000/api/v1/health

  2. Metrics reachable

  curl -sS -o /dev/null -w "%{http_code}\n" http://localhost:8000/metrics

  3. API docs

  curl -sS -o /dev/null -w "%{http_code}\n" http://localhost:8000/docs

  4. Basic aggregate query

  curl -sS -X POST http://localhost:8000/api/v1/query \
    -H "Content-Type: application/json" \
    -d '{"sql":"SELECT status, COUNT(*) AS cnt FROM orders GROUP BY status ORDER BY status","tenant_id":"default","use_cache":true}'

  5. Cache stats before repeated query

  curl -sS http://localhost:8000/api/v1/cache/stats

  6. Repeat same query twice (cache behavior)

  for i in 1 2; do
    curl -sS -X POST http://localhost:8000/api/v1/query \
      -H "Content-Type: application/json" \
      -d '{"sql":"SELECT status, COUNT(*) AS cnt FROM orders GROUP BY status ORDER BY status","tenant_id":"default","use_cache":true}';
    echo;
  done

  7. Cache stats after repeat

  curl -sS http://localhost:8000/api/v1/cache/stats

  8. CDC status endpoint

  curl -sS "http://localhost:8000/api/v1/cdc/status?tenant_id=default"

  9. Query history endpoint

  curl -sS "http://localhost:8000/api/v1/query/history?tenant_id=default&limit=5"

  10. DB-side verification of logs

  sudo docker compose exec postgres psql -U metamind -d metamind -c \
  "SELECT status, COUNT(*) FROM mm_query_logs GROUP BY status ORDER BY status;"

  Pass criteria:

  - 1/2/3 return OK (200)
  - 4 returns status: success with rows
  - 5→7 shows cache metrics moving
  - 8/9 return valid JSON
  - 10 shows query log rows increasing after tests

=====================================================================
 Evidence from your output:

  - Metastore connection URL: jdbc:derby:...
  - Derby then fails on Postgres SQL (statement_timeout).

  Use explicit Postgres args with schematool:

  # 1) reset metastore DB cleanly (optional but recommended after failed init)
  sudo docker compose exec postgres psql -U metamind -d postgres -c "DROP DATABASE IF EXISTS metastore;"
  sudo docker compose exec postgres psql -U metamind -d postgres -c "CREATE DATABASE metastore OWNER metamind;"

  # 2) initialize schema against Postgres explicitly
  sudo docker compose exec hive-metastore bash -lc "/opt/hive/bin/schematool \
    -dbType postgres \
    -initSchema \
    -url jdbc:postgresql://postgres:5432/metastore \
    -driver org.postgresql.Driver \
    -userName metamind \
    -passWord metamind \
    --verbose"

  # 3) restart services that depend on metastore
  sudo docker compose restart hive-metastore trino

  # 4) verify recent logs
  sudo docker compose logs --since=2m hive-metastore
  sudo docker compose logs --since=2m trino
  sudo docker compose logs --since=2m postgres

  SLF4J multiple bindings warnings are non-blocking here.


==========================================================================================
  Do a clean reset of only the metastore DB, then initialize once:

  # 1) kill active sessions to metastore DB
  sudo docker compose exec postgres psql -U metamind -d postgres -c \
  "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='metastore' AND pid <> pg_backend_pid();"

  # 2) recreate metastore DB clean
  sudo docker compose exec postgres psql -U metamind -d postgres -c "DROP DATABASE IF EXISTS metastore;"
  sudo docker compose exec postgres psql -U metamind -d postgres -c "CREATE DATABASE metastore OWNER metamind;"

  # 3) init schema (explicit Postgres args)
  sudo docker compose exec hive-metastore bash -lc "/opt/hive/bin/schematool \
    -dbType postgres \
    -initSchema \
    -url jdbc:postgresql://postgres:5432/metastore \
    -driver org.postgresql.Driver \
    -userName metamind \
    -passWord metamind \
    --verbose"

  # 4) verify VERSION row exists
  sudo docker compose exec postgres psql -U metamind -d metastore -c 'SELECT * FROM "VERSION";'

  # 5) restart dependent services
  sudo docker compose restart hive-metastore trino

  If step 3 still fails, send:

  - last 30 lines of schematool output
  - output of step 4 (SELECT * FROM "VERSION";)







