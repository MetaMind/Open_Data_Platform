#!/usr/bin/env bash
set -euo pipefail

DB_HOST="${METAMIND_DB__HOST:-localhost}"
DB_PORT="${METAMIND_DB__PORT:-5432}"
DB_NAME="${METAMIND_DB__DATABASE:-metamind}"
DB_USER="${METAMIND_DB__USER:-metamind}"
DB_PASSWORD="${METAMIND_DB__PASSWORD:-metamind}"

export PGPASSWORD="$DB_PASSWORD"

echo "Applying migrations to ${DB_USER}@${DB_HOST}:${DB_PORT}/${DB_NAME}"
for f in migrations/*.sql; do
  echo "  -> $f"
  psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -v ON_ERROR_STOP=1 -f "$f"
done

echo "Migrations applied successfully"
