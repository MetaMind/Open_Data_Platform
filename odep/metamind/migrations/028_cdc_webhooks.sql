-- Migration 028: CDC Outbound Webhook Subscriptions and Delivery Log

CREATE TABLE IF NOT EXISTS mm_cdc_webhook_subs (
    sub_id      BIGSERIAL    PRIMARY KEY,
    table_name  TEXT         NOT NULL,
    url         TEXT         NOT NULL,
    secret      TEXT         NOT NULL DEFAULT '',
    is_active   BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_cdc_webhook_subs_table
    ON mm_cdc_webhook_subs (table_name, is_active);

CREATE TABLE IF NOT EXISTS mm_webhook_delivery_log (
    delivery_id   BIGSERIAL    PRIMARY KEY,
    sub_id        BIGINT       REFERENCES mm_cdc_webhook_subs(sub_id) ON DELETE CASCADE,
    event_id      TEXT         NOT NULL,
    status_code   INT          NOT NULL DEFAULT 0,
    error_message TEXT,
    attempted_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_webhook_delivery_sub
    ON mm_webhook_delivery_log (sub_id, attempted_at DESC);

CREATE INDEX IF NOT EXISTS idx_webhook_delivery_event
    ON mm_webhook_delivery_log (event_id);

COMMENT ON TABLE mm_cdc_webhook_subs IS
    'CDC webhook subscription registry; one row per subscriber per table.';

COMMENT ON TABLE mm_webhook_delivery_log IS
    'Audit log of every webhook delivery attempt including HTTP status.';
