#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8000}"
TENANT_ID="${TENANT_ID:-default}"

pass_count=0
fail_count=0

run_check() {
  local name="$1"
  shift
  echo "\n[CHECK] ${name}"
  if "$@"; then
    echo "[PASS] ${name}"
    pass_count=$((pass_count + 1))
  else
    echo "[FAIL] ${name}"
    fail_count=$((fail_count + 1))
  fi
}

check_health() {
  curl -fsS "${BASE_URL}/api/v1/health" >/tmp/mm_health.json
  grep -Eq '"status"|"checks"' /tmp/mm_health.json
}

check_query() {
  local code
  code=$(curl -sS -o /tmp/mm_query.json -w "%{http_code}" \
    -X POST "${BASE_URL}/api/v1/query" \
    -H "Content-Type: application/json" \
    -d '{"sql":"SELECT status, COUNT(*) AS cnt FROM orders GROUP BY status ORDER BY status","tenant_id":"'"${TENANT_ID}"'","use_cache":true}')
  [[ "${code}" == "200" ]]
}

check_query_history() {
  curl -fsS "${BASE_URL}/api/v1/query/history?tenant_id=${TENANT_ID}&limit=5" >/tmp/mm_history.json
  grep -Eq '"queries"|"count"' /tmp/mm_history.json
}

check_cache_stats() {
  curl -fsS "${BASE_URL}/api/v1/cache/stats" >/tmp/mm_cache.json
  grep -Eq '"l1_size"|"misses"|"total_requests"' /tmp/mm_cache.json
}

check_cache_invalidate() {
  local code
  code=$(curl -sS -o /tmp/mm_cache_inv.json -w "%{http_code}" \
    -X POST "${BASE_URL}/api/v1/cache/invalidate?pattern=${TENANT_ID}")
  [[ "${code}" == "200" ]]
}

check_cdc_status() {
  curl -fsS "${BASE_URL}/api/v1/cdc/status?tenant_id=${TENANT_ID}" >/tmp/mm_cdc.json
  grep -Eq '"overall_status"|"total_tables"' /tmp/mm_cdc.json
}

check_metrics() {
  curl -fsS "${BASE_URL}/metrics" >/tmp/mm_metrics.txt
  grep -Eq 'python_gc_objects_collected_total|metamind_info|process_cpu_seconds_total' /tmp/mm_metrics.txt
}

check_security_headers() {
  curl -sSI "${BASE_URL}/api/v1/health" >/tmp/mm_headers.txt
  grep -Eq 'strict-transport-security:' /tmp/mm_headers.txt && \
  grep -Eq 'content-security-policy:' /tmp/mm_headers.txt && \
  grep -Eq 'x-frame-options:' /tmp/mm_headers.txt && \
  grep -Eq 'x-content-type-options:' /tmp/mm_headers.txt
}

check_synthesis_status() {
  local code
  code=$(curl -sS -o /tmp/mm_synth_status.json -w "%{http_code}" \
    "${BASE_URL}/api/v1/synthesis/status")
  [[ "${code}" == "200" || "${code}" == "404" ]]
}

echo "MetaMind test guide runner"
echo "BASE_URL=${BASE_URL} TENANT_ID=${TENANT_ID}"

run_check "Health endpoint" check_health
run_check "Query execution" check_query
run_check "Query history" check_query_history
run_check "Cache stats" check_cache_stats
run_check "Cache invalidate" check_cache_invalidate
run_check "CDC status" check_cdc_status
run_check "Metrics endpoint" check_metrics
run_check "Security headers" check_security_headers
run_check "Synthesis status endpoint" check_synthesis_status

echo "\nSummary: pass=${pass_count} fail=${fail_count}"
if [[ ${fail_count} -gt 0 ]]; then
  exit 1
fi
