"""Usage-Based Billing Exporter.

Aggregates mm_query_costs into invoiceable BillingPeriod line items and
exports to Stripe (via billing meter events) or CSV.
"""
from __future__ import annotations

import csv
import io
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


@dataclass
class BillingLineItem:
    """A single engine-level billing line item."""

    engine: str
    query_count: int
    total_cost_usd: float
    result_rows: int


@dataclass
class BillingPeriod:
    """Aggregated billing data for one tenant/period."""

    tenant_id: str
    period_start: datetime
    period_end: datetime
    line_items: list[BillingLineItem] = field(default_factory=list)

    @property
    def total_cost_usd(self) -> float:
        return sum(li.total_cost_usd for li in self.line_items)


class UsageBillingExporter:
    """Aggregate and export per-tenant billing data.

    Args:
        db_engine: SQLAlchemy Engine for reading mm_query_costs and writing exports.
        stripe_api_key: Optional Stripe API key for UsageRecord creation.
    """

    def __init__(
        self,
        db_engine: Engine,
        stripe_api_key: Optional[str] = None,
    ) -> None:
        self._engine = db_engine
        self._stripe_key = stripe_api_key

    async def aggregate(
        self,
        tenant_id: str,
        period_start: datetime,
        period_end: datetime,
    ) -> BillingPeriod:
        """Aggregate query costs for tenant in the given period.

        Returns:
            BillingPeriod with one line item per engine.
        """
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(
                    text(
                        "SELECT engine, "
                        "COUNT(*) AS query_count, "
                        "SUM(actual_cost_usd) AS total_cost_usd, "
                        "SUM(result_rows) AS result_rows "
                        "FROM mm_query_costs "
                        "WHERE tenant_id = :tid "
                        "  AND billed_at BETWEEN :s AND :e "
                        "GROUP BY engine "
                        "ORDER BY total_cost_usd DESC"
                    ),
                    {"tid": tenant_id, "s": period_start, "e": period_end},
                ).fetchall()
        except Exception as exc:
            logger.error("UsageBillingExporter.aggregate failed: %s", exc)
            rows = []

        line_items = [
            BillingLineItem(
                engine=r.engine,
                query_count=int(r.query_count or 0),
                total_cost_usd=float(r.total_cost_usd or 0),
                result_rows=int(r.result_rows or 0),
            )
            for r in rows
        ]
        return BillingPeriod(
            tenant_id=tenant_id,
            period_start=period_start,
            period_end=period_end,
            line_items=line_items,
        )

    async def export_to_stripe(self, billing_period: BillingPeriod) -> str:
        """Push usage records to Stripe via the Billing Meter Events API.

        Returns:
            A synthetic invoice reference string (Stripe event ID).
        """
        if not self._stripe_key:
            raise ValueError("Stripe API key not configured")
        try:
            import stripe  # type: ignore[import]
            stripe.api_key = self._stripe_key

            event_ids: list[str] = []
            for item in billing_period.line_items:
                event = stripe.billing.MeterEvent.create(
                    event_name="metamind_query",
                    payload={
                        "stripe_customer_id": billing_period.tenant_id,
                        "value": str(int(item.total_cost_usd * 100)),
                    },
                    timestamp=int(billing_period.period_end.timestamp()),
                )
                event_ids.append(event.identifier)

            invoice_id = ";".join(event_ids) or "stripe_event_ok"
            self._record_export(billing_period, invoice_id)
            return invoice_id
        except ImportError:
            logger.error("stripe library not installed")
            raise
        except Exception as exc:
            logger.error("export_to_stripe failed: %s", exc)
            raise

    async def export_to_csv(
        self, billing_period: BillingPeriod, dest_path: str
    ) -> str:
        """Write billing period to a CSV file.

        Returns:
            The dest_path written.
        """
        output = io.StringIO()
        writer = csv.DictWriter(
            output,
            fieldnames=["tenant_id", "engine", "queries", "cost_usd",
                        "result_rows", "period_start", "period_end"],
        )
        writer.writeheader()
        for item in billing_period.line_items:
            writer.writerow({
                "tenant_id": billing_period.tenant_id,
                "engine": item.engine,
                "queries": item.query_count,
                "cost_usd": round(item.total_cost_usd, 6),
                "result_rows": item.result_rows,
                "period_start": billing_period.period_start.isoformat(),
                "period_end": billing_period.period_end.isoformat(),
            })
        with open(dest_path, "w", encoding="utf-8", newline="") as fh:
            fh.write(output.getvalue())
        logger.info("Billing CSV written to %s", dest_path)
        return dest_path

    def _record_export(self, period: BillingPeriod, ref: str) -> None:
        try:
            with self._engine.begin() as conn:
                conn.execute(
                    text(
                        "INSERT INTO mm_billing_exports "
                        "(tenant_id, period_start, period_end, "
                        " total_cost_usd, export_ref, exported_at) "
                        "VALUES (:tid, :ps, :pe, :cost, :ref, NOW())"
                    ),
                    {
                        "tid": period.tenant_id,
                        "ps": period.period_start,
                        "pe": period.period_end,
                        "cost": period.total_cost_usd,
                        "ref": ref,
                    },
                )
        except Exception as exc:
            logger.error("UsageBillingExporter._record_export failed: %s", exc)
