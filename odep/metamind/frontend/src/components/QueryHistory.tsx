/**
 * QueryHistory — Query Execution History with Filtering and Drill-Down
 *
 * File: frontend/src/components/QueryHistory.tsx
 *
 * Columns: timestamp, SQL preview, engine, duration, rows, cache hit, status.
 * Filters: date range, engine, min/max duration, tenant (admin).
 * Row click → drill-down with full SQL, routing reason, plan features, cost estimate.
 * Export to CSV button. Infinite scroll pagination.
 */

import React, { useEffect, useState, useCallback, useRef } from "react";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface QueryRecord {
  query_id: string;
  sql_text: string;
  target_source: string;
  execution_time_ms: number | null;
  rows_returned: number | null;
  status: string;
  submitted_at: string;
  cache_hit?: boolean;
  routing_reason?: string;
  plan_features?: Record<string, unknown>;
  estimated_cost_ms?: number;
  cdc_lag_at_execution?: number;
}

interface Filters {
  engine: string;
  from_ts: string;
  to_ts: string;
  min_ms: string;
  max_ms: string;
  tenant_id: string;
}

const BASE_URL = "/api/v1";
const PAGE_SIZE = 50;

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------

const statusBadge = (status: string) => {
  const map: Record<string, string> = {
    completed: "bg-green-100 text-green-700",
    running:   "bg-blue-100 text-blue-700",
    failed:    "bg-red-100 text-red-700",
    cancelled: "bg-gray-100 text-gray-500",
  };
  const cls = map[status] ?? "bg-gray-100 text-gray-600";
  return <span className={`px-2 py-0.5 rounded text-xs font-medium ${cls}`}>{status}</span>;
};

const truncate = (s: string, n = 80) => (s?.length > n ? s.slice(0, n) + "…" : s ?? "—");

const fmtMs = (ms: number | null) =>
  ms == null ? "—" : ms >= 1000 ? `${(ms / 1000).toFixed(2)}s` : `${ms.toFixed(0)}ms`;

function toCSV(rows: QueryRecord[]): string {
  const headers = ["query_id", "submitted_at", "engine", "duration_ms", "rows", "status", "cache_hit", "sql"];
  const lines = [headers.join(",")];
  for (const r of rows) {
    lines.push([
      r.query_id, r.submitted_at, r.target_source,
      r.execution_time_ms ?? "", r.rows_returned ?? "",
      r.status, r.cache_hit ? "true" : "false",
      `"${(r.sql_text ?? "").replace(/"/g, '""')}"`,
    ].join(","));
  }
  return lines.join("\n");
}

// ---------------------------------------------------------------------------
// DrillDownPanel
// ---------------------------------------------------------------------------

const DrillDownPanel: React.FC<{ record: QueryRecord; onClose: () => void }> = ({ record, onClose }) => (
  <div className="fixed inset-0 bg-black bg-opacity-40 flex items-center justify-center z-50 p-4">
    <div className="bg-white rounded-xl shadow-2xl w-full max-w-3xl max-h-[90vh] overflow-auto">
      <div className="flex items-center justify-between px-6 py-4 border-b">
        <h3 className="text-lg font-semibold text-gray-900">Query Detail</h3>
        <button onClick={onClose} className="text-gray-400 hover:text-gray-600 text-xl">✕</button>
      </div>

      <div className="p-6 space-y-5">
        {/* SQL */}
        <div>
          <h4 className="text-xs font-semibold text-gray-500 uppercase mb-2">SQL</h4>
          <pre className="bg-gray-50 border rounded p-3 text-sm font-mono overflow-auto whitespace-pre-wrap">
            {record.sql_text ?? "—"}
          </pre>
        </div>

        {/* Routing */}
        <div className="grid grid-cols-2 gap-4 text-sm">
          <div>
            <span className="text-gray-500 text-xs block">Engine</span>
            <span className="font-medium">{record.target_source}</span>
          </div>
          <div>
            <span className="text-gray-500 text-xs block">Duration</span>
            <span className="font-medium">{fmtMs(record.execution_time_ms)}</span>
          </div>
          <div>
            <span className="text-gray-500 text-xs block">Rows</span>
            <span className="font-medium">{record.rows_returned?.toLocaleString() ?? "—"}</span>
          </div>
          <div>
            <span className="text-gray-500 text-xs block">Cache Hit</span>
            <span className="font-medium">{record.cache_hit ? "✓ Yes" : "✗ No"}</span>
          </div>
          {record.estimated_cost_ms != null && (
            <div>
              <span className="text-gray-500 text-xs block">Est. Cost (ML)</span>
              <span className="font-medium">{fmtMs(record.estimated_cost_ms)}</span>
            </div>
          )}
          {record.cdc_lag_at_execution != null && (
            <div>
              <span className="text-gray-500 text-xs block">CDC Lag at Exec</span>
              <span className="font-medium">{record.cdc_lag_at_execution}s</span>
            </div>
          )}
        </div>

        {/* Routing reason */}
        {record.routing_reason && (
          <div>
            <h4 className="text-xs font-semibold text-gray-500 uppercase mb-2">Routing Reason</h4>
            <p className="text-sm bg-blue-50 border border-blue-200 rounded px-3 py-2 text-blue-800">
              {record.routing_reason}
            </p>
          </div>
        )}

        {/* Plan features */}
        {record.plan_features && Object.keys(record.plan_features).length > 0 && (
          <div>
            <h4 className="text-xs font-semibold text-gray-500 uppercase mb-2">Plan Features</h4>
            <div className="grid grid-cols-3 gap-2 text-xs">
              {Object.entries(record.plan_features).map(([k, v]) => (
                <div key={k} className="bg-gray-50 rounded px-2 py-1">
                  <span className="text-gray-500">{k}:</span>{" "}
                  <span className="font-medium">{String(v)}</span>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  </div>
);

// ---------------------------------------------------------------------------
// QueryHistory
// ---------------------------------------------------------------------------

interface QueryHistoryProps {
  tenantId?: string;
  isAdmin?: boolean;
}

const QueryHistory: React.FC<QueryHistoryProps> = ({ tenantId = "default", isAdmin = false }) => {
  const [rows, setRows] = useState<QueryRecord[]>([]);
  const [offset, setOffset] = useState(0);
  const [hasMore, setHasMore] = useState(true);
  const [loading, setLoading] = useState(false);
  const [selected, setSelected] = useState<QueryRecord | null>(null);
  const [filters, setFilters] = useState<Filters>({
    engine: "", from_ts: "", to_ts: "", min_ms: "", max_ms: "",
    tenant_id: tenantId,
  });
  const loaderRef = useRef<HTMLDivElement>(null);

  const buildQuery = (off: number, f: Filters) => {
    const p = new URLSearchParams({
      limit: String(PAGE_SIZE),
      offset: String(off),
      tenant_id: f.tenant_id,
      engine: f.engine,
      from: f.from_ts,
      to: f.to_ts,
    });
    return `${BASE_URL}/query/history?${p.toString()}`;
  };

  const load = useCallback(async (reset = false) => {
    if (loading) return;
    setLoading(true);
    const currentOffset = reset ? 0 : offset;
    try {
      const res = await fetch(buildQuery(currentOffset, filters));
      if (!res.ok) throw new Error("Fetch failed");
      const data = await res.json();
      const items: QueryRecord[] = data.items ?? [];
      if (reset) {
        setRows(items);
        setOffset(items.length);
      } else {
        setRows((prev) => [...prev, ...items]);
        setOffset((o) => o + items.length);
      }
      setHasMore(items.length === PAGE_SIZE);
    } catch {
      // silently fail — user can retry via filter change
    } finally {
      setLoading(false);
    }
  }, [filters, offset, loading]);

  // Reload on filter change
  useEffect(() => {
    setOffset(0);
    setHasMore(true);
    setRows([]);
    load(true);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [filters]);

  // Infinite scroll via IntersectionObserver
  useEffect(() => {
    if (!loaderRef.current || !hasMore) return;
    const obs = new IntersectionObserver((entries) => {
      if (entries[0].isIntersecting && !loading) load(false);
    }, { threshold: 1.0 });
    obs.observe(loaderRef.current);
    return () => obs.disconnect();
  }, [hasMore, loading, load]);

  const exportCSV = () => {
    const blob = new Blob([toCSV(rows)], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `query_history_${new Date().toISOString().slice(0, 10)}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  };

  const updateFilter = (key: keyof Filters, val: string) =>
    setFilters((prev) => ({ ...prev, [key]: val }));

  return (
    <div className="p-6 space-y-4">
      {/* Header */}
      <div className="flex items-center justify-between flex-wrap gap-3">
        <h1 className="text-2xl font-bold text-gray-900">Query History</h1>
        <button
          onClick={exportCSV}
          className="px-4 py-2 bg-gray-700 text-white rounded hover:bg-gray-800 text-sm"
        >
          Export CSV
        </button>
      </div>

      {/* Filters */}
      <div className="bg-white shadow rounded-lg p-4 flex flex-wrap gap-3 items-end">
        <div>
          <label className="text-xs text-gray-500 block mb-1">Engine</label>
          <select
            value={filters.engine}
            onChange={(e) => updateFilter("engine", e.target.value)}
            className="border rounded px-2 py-1 text-sm"
          >
            <option value="">All</option>
            {["oracle", "trino", "spark", "gpu"].map((e) => <option key={e}>{e}</option>)}
          </select>
        </div>
        <div>
          <label className="text-xs text-gray-500 block mb-1">From</label>
          <input type="datetime-local" value={filters.from_ts}
            onChange={(e) => updateFilter("from_ts", e.target.value)}
            className="border rounded px-2 py-1 text-sm" />
        </div>
        <div>
          <label className="text-xs text-gray-500 block mb-1">To</label>
          <input type="datetime-local" value={filters.to_ts}
            onChange={(e) => updateFilter("to_ts", e.target.value)}
            className="border rounded px-2 py-1 text-sm" />
        </div>
        {isAdmin && (
          <div>
            <label className="text-xs text-gray-500 block mb-1">Tenant</label>
            <input value={filters.tenant_id}
              onChange={(e) => updateFilter("tenant_id", e.target.value)}
              className="border rounded px-2 py-1 text-sm w-28" placeholder="tenant_id" />
          </div>
        )}
        <button
          onClick={() => setFilters({ engine: "", from_ts: "", to_ts: "", min_ms: "", max_ms: "", tenant_id: tenantId })}
          className="text-xs text-gray-500 hover:text-gray-700 underline"
        >
          Clear
        </button>
      </div>

      {/* Table */}
      <div className="bg-white shadow rounded-lg overflow-auto">
        <table className="w-full text-sm min-w-max">
          <thead className="bg-gray-50 border-b sticky top-0">
            <tr>
              {["Timestamp", "SQL", "Engine", "Duration", "Rows", "Cache", "Status"].map((h) => (
                <th key={h} className="text-left px-4 py-2 text-gray-600 font-medium">{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr
                key={row.query_id}
                className="border-b hover:bg-blue-50 cursor-pointer"
                onClick={() => setSelected(row)}
              >
                <td className="px-4 py-2 text-gray-500 text-xs whitespace-nowrap">
                  {new Date(row.submitted_at).toLocaleString()}
                </td>
                <td className="px-4 py-2 font-mono text-xs text-gray-700 max-w-xs">
                  {truncate(row.sql_text)}
                </td>
                <td className="px-4 py-2">
                  <span className="px-2 py-0.5 rounded text-xs bg-indigo-100 text-indigo-700 font-mono">
                    {row.target_source}
                  </span>
                </td>
                <td className="px-4 py-2 text-right tabular-nums">{fmtMs(row.execution_time_ms)}</td>
                <td className="px-4 py-2 text-right tabular-nums">
                  {row.rows_returned?.toLocaleString() ?? "—"}
                </td>
                <td className="px-4 py-2 text-center">
                  {row.cache_hit ? "✓" : "—"}
                </td>
                <td className="px-4 py-2">{statusBadge(row.status)}</td>
              </tr>
            ))}

            {rows.length === 0 && !loading && (
              <tr>
                <td colSpan={7} className="px-4 py-8 text-center text-gray-400">
                  No queries found for the selected filters.
                </td>
              </tr>
            )}
          </tbody>
        </table>

        {/* Infinite scroll sentinel */}
        <div ref={loaderRef} className="h-8 flex items-center justify-center">
          {loading && (
            <div className="animate-spin rounded-full h-5 w-5 border-b-2 border-blue-600" />
          )}
          {!hasMore && rows.length > 0 && (
            <span className="text-xs text-gray-400">— End of results —</span>
          )}
        </div>
      </div>

      {/* Drill-down modal */}
      {selected && <DrillDownPanel record={selected} onClose={() => setSelected(null)} />}
    </div>
  );
};

export default QueryHistory;
