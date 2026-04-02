# Security Testing Guide

## Scope
Validates API hardening controls exposed at HTTP layer.

## 1. Response security headers
```bash
curl -i http://localhost:8000/api/v1/health
```

Pass criteria:
- headers include:
  - `Strict-Transport-Security`
  - `Content-Security-Policy`
  - `X-Frame-Options`
  - `X-Content-Type-Options`
  - `Referrer-Policy`
  - `Permissions-Policy`
  - `X-Request-ID`

## 2. Method controls
```bash
curl -I http://localhost:8000/api/v1/health
```

Pass criteria:
- HTTP `405` for unsupported method (`HEAD`), confirms method restrictions are enforced

## 3. Firewall behavior smoke test
```bash
curl -i -X POST http://localhost:8000/api/v1/query \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT 1","tenant_id":"default","use_cache":true}'
```

Pass criteria:
- request returns deterministic policy outcome
- if blocked, error clearly indicates firewall policy/fingerprint

## 4. Redis firewall key visibility (optional)
```bash
sudo docker compose exec redis redis-cli --scan --pattern 'mm:firewall:*'
```

Pass criteria:
- command executes successfully
- keyspace reflects configured firewall mode/lists (if enabled)
