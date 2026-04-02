/**
 * BudgetDashboard — Cloud Cost Tracking (F23 Feature Flag)
 *
 * File: frontend/src/components/BudgetDashboard.tsx
 *
 * Displays current spend vs. budget per tenant, cost breakdown by engine,
 * query cost distribution, top expensive queries, and budget alerts.
 */

import React, { useEffect, useState, useCallback } from "react";
import {
  LineChart, Line, PieChart, Pie, Cell,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer,
} from "recharts";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface BudgetSummary {
  tenant_id: string;
  budget_name: string;
  budget_limit_usd: number;
  current_spend_usd: number;
  pct_used: number;
  billing_cycle: string;
  alert_threshold_pct: number;
  alert_color: "green" | "yellow" | "red";
  budget_configured: boolean;
}

interface EngineBreakdown {
  engine: string;
  total_cost: number;
  query_count: number;
  avg_ms: number;
}

interface BudgetAlert {
  alert_id: string;
  alert_type: string;
  threshold_pct: number;
  current_spend: number;
  budget_limit: number;
  pct_used: number;
  fired_at: string;
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const ENGINE_COLORS: Record<string, string> = {
  oracle: "#f59e0b",
  trino: "#3b82f6",
  spark: "#8b5cf6",
  gpu: "#10b981",
  s3: "#6366f1",
};

const ALERT_BADGE_STYLES: Record<string, string> = {
  green:  "bg-green-100 text-green-800 border border-green-300",
  yellow: "bg-yellow-100 text-yellow-800 border border-yellow-300",
  red:    "bg-red-100 text-red-800 border border-red-300",
};

const BASE_URL = "/api/v1";

// ---------------------------------------------------------------------------
// Sub-components
// ---------------------------------------------------------------------------

const AlertBadge: React.FC<{ color: string; pct: number }> = ({ color, pct }) => (
  <span className={`px-3 py-1 rounded-full text-sm font-semibold ${ALERT_BADGE_STYLES[color] ?? ALERT_BADGE_STYLES.green}`}>
    {pct.toFixed(1)}% used
  </span>
);

const SpendMeter: React.FC<{ pct: number; color: string }> = ({ pct, color }) => {
  const barColor = color === "red" ? "bg-red-500" : color === "yellow" ? "bg-yellow-400" : "bg-green-500";
  return (
    <div className="w-full bg-gray-200 rounded-full h-4">
      <div
        className={`${barColor} h-4 rounded-full transition-all duration-500`}
        style={{ width: `${Math.min(pct, 100)}%` }}
      />
    </div>
  );
};

// ---------------------------------------------------------------------------
// BudgetDashboard
// ---------------------------------------------------------------------------

interface BudgetDashboardProps {
  tenantId?: string;
}

const BudgetDashboard: React.FC<BudgetDashboardProps> = ({ tenantId = "default" }) => {
  const [summary, setSummary] = useState<BudgetSummary | null>(null);
  const [breakdown, setBreakdown] = useState<EngineBreakdown[]>([]);
  const [alerts, setAlerts] = useState<BudgetAlert[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const fetchAll = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [summaryRes, breakdownRes, alertsRes] = await Promise.all([
        fetch(`${BASE_URL}/budget/summary?tenant_id=${tenantId}`),
        fetch(`${BASE_URL}/budget/breakdown?tenant_id=${tenantId}`),
        fetch(`${BASE_URL}/budget/alerts?tenant_id=${tenantId}`),
      ]);
      if (!summaryRes.ok) throw new Error(`Budget summary fetch failed: ${summaryRes.status}`);
      const [s, b, a] = await Promise.all([
        summaryRes.json(), breakdownRes.json(), alertsRes.json(),
      ]);
      setSummary(s);
      setBreakdown(b.by_engine ?? []);
      setAlerts(a);
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "Unknown error");
    } finally {
      setLoading(false);
    }
  }, [tenantId]);

  useEffect(() => { fetchAll(); }, [fetchAll]);

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="animate-spin rounded-full h-10 w-10 border-b-2 border-blue-600" />
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-red-50 border border-red-300 rounded p-4 text-red-700">
        <strong>Error loading budget data:</strong> {error}
        <button onClick={fetchAll} className="ml-4 text-sm underline">Retry</button>
      </div>
    );
  }

  const pieData = breakdown.map((e) => ({
    name: e.engine,
    value: Number(e.total_cost.toFixed(4)),
  }));

  return (
    <div className="p-6 space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold text-gray-900">Cloud Budget Dashboard</h1>
        <button
          onClick={fetchAll}
          className="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700 text-sm"
        >
          Refresh
        </button>
      </div>

      {/* Budget summary card */}
      {summary && summary.budget_configured !== false && (
        <div className="bg-white shadow rounded-lg p-6">
          <div className="flex items-center justify-between mb-4">
            <div>
              <h2 className="text-lg font-semibold text-gray-800">{summary.budget_name}</h2>
              <p className="text-sm text-gray-500 capitalize">{summary.billing_cycle} budget</p>
            </div>
            <AlertBadge color={summary.alert_color} pct={summary.pct_used} />
          </div>
          <SpendMeter pct={summary.pct_used} color={summary.alert_color} />
          <div className="flex justify-between mt-2 text-sm text-gray-600">
            <span>${summary.current_spend_usd.toFixed(2)} spent</span>
            <span>${summary.budget_limit_usd.toFixed(2)} limit</span>
          </div>
          {summary.pct_used >= summary.alert_threshold_pct && (
            <p className="mt-2 text-sm text-yellow-700 bg-yellow-50 rounded px-3 py-1">
              ⚠ Alert threshold ({summary.alert_threshold_pct}%) reached
            </p>
          )}
        </div>
      )}

      {summary?.budget_configured === false && (
        <div className="bg-gray-50 border border-gray-300 rounded p-4 text-gray-600">
          No budget configured for this tenant.
        </div>
      )}

      {/* Engine cost breakdown + active alerts */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        {/* Pie chart */}
        <div className="bg-white shadow rounded-lg p-6">
          <h3 className="text-md font-semibold text-gray-800 mb-4">Cost by Engine (30d)</h3>
          {pieData.length > 0 ? (
            <ResponsiveContainer width="100%" height={220}>
              <PieChart>
                <Pie data={pieData} dataKey="value" nameKey="name" cx="50%" cy="50%" outerRadius={80} label>
                  {pieData.map((entry, i) => (
                    <Cell key={entry.name} fill={ENGINE_COLORS[entry.name] ?? "#94a3b8"} />
                  ))}
                </Pie>
                <Tooltip formatter={(v: number) => `$${v.toFixed(4)}`} />
                <Legend />
              </PieChart>
            </ResponsiveContainer>
          ) : (
            <p className="text-gray-400 text-sm">No cost data yet</p>
          )}
        </div>

        {/* Engine table */}
        <div className="bg-white shadow rounded-lg p-6 overflow-auto">
          <h3 className="text-md font-semibold text-gray-800 mb-4">Engine Breakdown</h3>
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b text-gray-500">
                <th className="text-left pb-2">Engine</th>
                <th className="text-right pb-2">Cost ($)</th>
                <th className="text-right pb-2">Queries</th>
                <th className="text-right pb-2">Avg ms</th>
              </tr>
            </thead>
            <tbody>
              {breakdown.length === 0 ? (
                <tr><td colSpan={4} className="text-gray-400 pt-4">No data</td></tr>
              ) : breakdown.map((e) => (
                <tr key={e.engine} className="border-b hover:bg-gray-50">
                  <td className="py-2 flex items-center gap-2">
                    <span
                      className="w-2 h-2 rounded-full inline-block"
                      style={{ background: ENGINE_COLORS[e.engine] ?? "#94a3b8" }}
                    />
                    {e.engine}
                  </td>
                  <td className="text-right">${Number(e.total_cost).toFixed(4)}</td>
                  <td className="text-right">{e.query_count}</td>
                  <td className="text-right">{Number(e.avg_ms).toFixed(0)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Active alerts */}
      {alerts.length > 0 && (
        <div className="bg-white shadow rounded-lg p-6">
          <h3 className="text-md font-semibold text-gray-800 mb-4">
            Active Budget Alerts ({alerts.length})
          </h3>
          <ul className="space-y-2">
            {alerts.map((a) => (
              <li key={a.alert_id} className="flex items-center justify-between bg-red-50 rounded px-4 py-2 text-sm">
                <span className="text-red-700 font-medium">
                  {a.pct_used.toFixed(1)}% of ${a.budget_limit.toFixed(0)} budget used
                </span>
                <span className="text-gray-500 text-xs">
                  {new Date(a.fired_at).toLocaleString()}
                </span>
              </li>
            ))}
          </ul>
        </div>
      )}
    </div>
  );
};

export default BudgetDashboard;
