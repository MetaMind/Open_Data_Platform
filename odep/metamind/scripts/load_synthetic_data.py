#!/usr/bin/env python3
"""Load synthetic data for harness testing.

Creates and populates:
- Business tables in public schema: customers, products, orders, order_items, payments
- MetaMind tables: mm_tenants, mm_tables, mm_columns, mm_query_logs, mm_cdc_status

Default target is 5000 rows per table.
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass

import psycopg2


@dataclass
class DBConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load synthetic MetaMind test data")
    parser.add_argument("--host", default=os.getenv("METAMIND_DB_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("METAMIND_DB_PORT", "5432")))
    parser.add_argument("--dbname", default=os.getenv("METAMIND_DB_NAME", "metamind"))
    parser.add_argument("--user", default=os.getenv("METAMIND_DB_USER", "metamind"))
    parser.add_argument("--password", default=os.getenv("METAMIND_DB_PASSWORD", "metamind"))
    parser.add_argument("--rows", type=int, default=5000, help="Rows per target table")
    parser.add_argument(
        "--prefix",
        default="synthetic",
        help="Prefix for generated tenant/table names (default: synthetic)",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Delete previously generated rows for this prefix before inserting",
    )
    return parser.parse_args()


def exec_sql(cur, sql: str, params: tuple | dict | None = None) -> None:
    cur.execute(sql, params)


def create_business_tables(cur) -> None:
    exec_sql(
        cur,
        """
        CREATE TABLE IF NOT EXISTS customers (
            id BIGSERIAL PRIMARY KEY,
            email TEXT NOT NULL UNIQUE,
            full_name TEXT NOT NULL,
            region TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS products (
            id BIGSERIAL PRIMARY KEY,
            sku TEXT NOT NULL UNIQUE,
            product_name TEXT NOT NULL,
            category TEXT NOT NULL,
            price NUMERIC(12,2) NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS orders (
            id BIGSERIAL PRIMARY KEY,
            customer_id BIGINT NOT NULL REFERENCES customers(id),
            status TEXT NOT NULL,
            total NUMERIC(12,2) NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS order_items (
            id BIGSERIAL PRIMARY KEY,
            order_id BIGINT NOT NULL REFERENCES orders(id),
            product_id BIGINT NOT NULL REFERENCES products(id),
            quantity INTEGER NOT NULL CHECK (quantity > 0),
            unit_price NUMERIC(12,2) NOT NULL
        );
        CREATE TABLE IF NOT EXISTS payments (
            id BIGSERIAL PRIMARY KEY,
            order_id BIGINT NOT NULL REFERENCES orders(id),
            method TEXT NOT NULL,
            paid_amount NUMERIC(12,2) NOT NULL,
            paid_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """,
    )


def reset_existing(cur, prefix: str) -> None:
    exec_sql(cur, "DELETE FROM payments WHERE order_id IN (SELECT id FROM orders WHERE id > 0);")
    exec_sql(cur, "DELETE FROM order_items WHERE order_id IN (SELECT id FROM orders WHERE id > 0);")
    exec_sql(cur, "DELETE FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE email LIKE %s);", (f"{prefix}_cust_%",))
    exec_sql(cur, "DELETE FROM products WHERE sku LIKE %s;", (f"{prefix}_sku_%",))
    exec_sql(cur, "DELETE FROM customers WHERE email LIKE %s;", (f"{prefix}_cust_%@example.com",))

    exec_sql(cur, "DELETE FROM mm_cdc_status WHERE tenant_id LIKE %s;", (f"{prefix}_tenant_%",))
    exec_sql(cur, "DELETE FROM mm_query_logs WHERE tenant_id LIKE %s;", (f"{prefix}_tenant_%",))
    exec_sql(cur, "DELETE FROM mm_columns WHERE table_id IN (SELECT table_id FROM mm_tables WHERE tenant_id LIKE %s);", (f"{prefix}_tenant_%",))
    exec_sql(cur, "DELETE FROM mm_tables WHERE tenant_id LIKE %s;", (f"{prefix}_tenant_%",))
    exec_sql(cur, "DELETE FROM mm_tenants WHERE tenant_id LIKE %s;", (f"{prefix}_tenant_%",))


def load_business_data(cur, rows: int, prefix: str) -> None:
    exec_sql(
        cur,
        """
        INSERT INTO customers (email, full_name, region, created_at)
        SELECT
            %(prefix)s || '_cust_' || gs || '@example.com',
            'Customer ' || gs,
            (ARRAY['NA','EMEA','APAC','LATAM'])[1 + (gs %% 4)],
            NOW() - (gs || ' minutes')::interval
        FROM generate_series(1, %(rows)s) gs
        ON CONFLICT (email) DO NOTHING;
        """,
        {"rows": rows, "prefix": prefix},
    )

    exec_sql(
        cur,
        """
        INSERT INTO products (sku, product_name, category, price, created_at)
        SELECT
            %(prefix)s || '_sku_' || gs,
            'Product ' || gs,
            (ARRAY['electronics','fashion','home','books','grocery'])[1 + (gs %% 5)],
            ROUND((5 + random() * 995)::numeric, 2),
            NOW() - (gs || ' minutes')::interval
        FROM generate_series(1, %(rows)s) gs
        ON CONFLICT (sku) DO NOTHING;
        """,
        {"rows": rows, "prefix": prefix},
    )

    exec_sql(
        cur,
        """
        INSERT INTO orders (customer_id, status, total, created_at)
        SELECT
            ((random() * (%(rows)s - 1))::int + 1),
            (ARRAY['pending','paid','shipped','cancelled'])[1 + (gs %% 4)],
            ROUND((10 + random() * 2000)::numeric, 2),
            NOW() - (gs || ' minutes')::interval
        FROM generate_series(1, %(rows)s) gs;
        """,
        {"rows": rows},
    )

    exec_sql(
        cur,
        """
        INSERT INTO order_items (order_id, product_id, quantity, unit_price)
        SELECT
            ((random() * (%(rows)s - 1))::int + 1),
            ((random() * (%(rows)s - 1))::int + 1),
            ((random() * 4)::int + 1),
            ROUND((5 + random() * 500)::numeric, 2)
        FROM generate_series(1, %(rows)s) gs;
        """,
        {"rows": rows},
    )

    exec_sql(
        cur,
        """
        INSERT INTO payments (order_id, method, paid_amount, paid_at)
        SELECT
            ((random() * (%(rows)s - 1))::int + 1),
            (ARRAY['card','upi','bank_transfer','wallet'])[1 + (gs %% 4)],
            ROUND((10 + random() * 2000)::numeric, 2),
            NOW() - (gs || ' minutes')::interval
        FROM generate_series(1, %(rows)s) gs;
        """,
        {"rows": rows},
    )


def load_metamind_data(cur, rows: int, prefix: str) -> None:
    exec_sql(
        cur,
        """
        INSERT INTO mm_tenants (tenant_id, tenant_name, settings, is_active)
        SELECT
            %(prefix)s || '_tenant_' || gs,
            'Synthetic Tenant ' || gs,
            '{}'::jsonb,
            TRUE
        FROM generate_series(1, %(rows)s) gs
        ON CONFLICT (tenant_id) DO NOTHING;
        """,
        {"rows": rows, "prefix": prefix},
    )

    exec_sql(
        cur,
        """
        INSERT INTO mm_tables (
            tenant_id, source_id, source_type, schema_name, table_name,
            row_count, size_bytes, table_properties, is_partitioned
        )
        SELECT
            %(prefix)s || '_tenant_' || gs,
            'synthetic_src',
            'postgres',
            'public',
            %(prefix)s || '_table_' || gs,
            10000 + gs,
            (10000 + gs) * 128,
            '{}'::jsonb,
            FALSE
        FROM generate_series(1, %(rows)s) gs
        ON CONFLICT (tenant_id, source_id, schema_name, table_name) DO NOTHING;
        """,
        {"rows": rows, "prefix": prefix},
    )

    exec_sql(
        cur,
        """
        INSERT INTO mm_columns (
            table_id, column_name, ordinal_position, data_type, is_nullable,
            is_primary_key, statistics
        )
        SELECT
            t.table_id,
            'value_' || x.idx,
            x.idx,
            'varchar',
            TRUE,
            (x.idx = 1),
            '{}'::jsonb
        FROM (
            SELECT table_id
            FROM mm_tables
            WHERE tenant_id LIKE %(tenant_like)s
            ORDER BY table_id
            LIMIT %(rows)s
        ) t
        CROSS JOIN LATERAL (VALUES (1)) AS x(idx)
        ON CONFLICT (table_id, column_name) DO NOTHING;
        """,
        {"tenant_like": f"{prefix}_tenant_%", "rows": rows},
    )

    exec_sql(
        cur,
        """
        INSERT INTO mm_query_logs (
            tenant_id, user_id, session_id, original_sql, rewritten_sql,
            target_source, execution_strategy, submitted_at,
            total_time_ms, row_count, cache_hit, query_features,
            predicted_cost_ms, actual_cost_ms, status,
            freshness_tolerance_seconds, actual_freshness_seconds
        )
        SELECT
            %(prefix)s || '_tenant_' || ((gs %% %(rows)s) + 1),
            'synthetic_user_' || ((gs %% 100) + 1),
            'sess_' || gs,
            'SELECT * FROM orders WHERE id = ' || gs,
            NULL,
            (ARRAY['oracle','trino','spark'])[1 + (gs %% 3)],
            (ARRAY['route','cached','batch'])[1 + (gs %% 3)],
            NOW() - (gs || ' seconds')::interval,
            ((random() * 900)::int + 100),
            ((random() * 50)::int + 1),
            (gs %% 5 = 0),
            jsonb_build_object('table_name', 'orders', 'complexity', (gs %% 10) + 1),
            ((random() * 1000)::int + 50),
            ((random() * 1000)::int + 50),
            (ARRAY['success','failed','cancelled'])[1 + (gs %% 3)],
            300,
            ((random() * 500)::int)
        FROM generate_series(1, %(rows)s) gs;
        """,
        {"rows": rows, "prefix": prefix},
    )

    exec_sql(
        cur,
        """
        INSERT INTO mm_cdc_status (
            tenant_id, source_id, table_name, last_cdc_timestamp, last_s3_timestamp,
            lag_seconds, messages_behind, kafka_topic, kafka_partition, kafka_offset,
            last_processed_at, processing_rate_per_second
        )
        SELECT
            %(prefix)s || '_tenant_' || ((gs %% %(rows)s) + 1),
            'synthetic_source_' || gs,
            'synthetic_table_' || gs,
            NOW() - (gs || ' seconds')::interval,
            NOW() - ((gs + 10) || ' seconds')::interval,
            (gs %% 900),
            gs %% 10000,
            'synthetic_topic',
            (gs %% 8),
            gs * 100,
            NOW() - ((gs %% 60) || ' seconds')::interval,
            (10 + random() * 100)
        FROM generate_series(1, %(rows)s) gs
        ON CONFLICT (tenant_id, source_id, table_name) DO NOTHING;
        """,
        {"rows": rows, "prefix": prefix},
    )


def print_summary(cur) -> None:
    tables = [
        "customers",
        "products",
        "orders",
        "order_items",
        "payments",
        "mm_tenants",
        "mm_tables",
        "mm_columns",
        "mm_query_logs",
        "mm_cdc_status",
    ]
    print("\nLoaded row counts:")
    for table in tables:
        cur.execute(f"SELECT COUNT(*) FROM {table};")
        count = cur.fetchone()[0]
        print(f"  {table:<15} {count}")


def main() -> int:
    args = parse_args()
    cfg = DBConfig(
        host=args.host,
        port=args.port,
        dbname=args.dbname,
        user=args.user,
        password=args.password,
    )

    try:
        conn = psycopg2.connect(
            host=cfg.host,
            port=cfg.port,
            dbname=cfg.dbname,
            user=cfg.user,
            password=cfg.password,
        )
    except Exception as exc:
        print(f"Database connection failed: {exc}", file=sys.stderr)
        return 1

    try:
        with conn:
            with conn.cursor() as cur:
                create_business_tables(cur)
                if args.reset:
                    reset_existing(cur, args.prefix)
                load_business_data(cur, args.rows, args.prefix)
                load_metamind_data(cur, args.rows, args.prefix)
                print_summary(cur)
    except Exception as exc:
        print(f"Synthetic data load failed: {exc}", file=sys.stderr)
        return 2
    finally:
        conn.close()

    print("\nSynthetic load completed successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
