/**
 * WhatIfSimulator/WhatIfSimulator.tsx
 *
 * F30 What-If Simulator UI: scenario builder, cost comparison, per-query breakdown.
 */

import React, { useState, useCallback } from "react";

/* ─────────────────────── Types ─────────────────────────────────── */

interface HypotheticalChange {
  type: "add_index" | "remove_index" | "enable_feature" | "change_backend";
  table: string;
  column: string;
  index_type?: string;
  feature?: string;
  target_backend?: string;
}

interface PerQueryResult {
  query_id: string;
  sql: string;
  original_cost: number;
  simulated_cost: number;
  delta_pct: number;
}

interface SimulationResult {
  scenario_id: string;
  queries_replayed: number;
  original_total_cost: number;
  simulated_total_cost: number;
  cost_improvement_pct: number;
  top_benefiting_queries: PerQueryResult[];
  recommendation: string;
}

interface WhatIfSimulatorProps {
  apiBaseUrl?: string;
  tenantId?: string;
}

/* ──────────────────── API Helpers ──────────────────────────────── */

async function createAndRunScenario(
  baseUrl: string, tenantId: string, name: string,
  changes: HypotheticalChange[], sampleSize: number
): Promise<SimulationResult> {
  const createRes = await fetch(`${baseUrl}/api/v1/replay/scenarios`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      tenant_id: tenantId, name, changes, query_sample_size: sampleSize,
    }),
  });
  const createData = await createRes.json();
  const scenarioId = createData.data.scenario_id;

  const runRes = await fetch(`${baseUrl}/api/v1/replay/scenarios/run`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ scenario_id: scenarioId, tenant_id: tenantId }),
  });
  const runData = await runRes.json();
  return runData.data;
}

/* ──────────────────── Sub-Components ──────────────────────────── */

const CostGauge: React.FC<{ original: number; simulated: number; improvement: number }> = ({
  original, simulated, improvement,
}) => {
  const barWidth = Math.min(Math.abs(improvement), 100);
  const isImprovement = improvement > 0;
  return (
    <div style={{ marginBottom: 20 }}>
      <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 8 }}>
        <div>
          <div style={{ fontSize: 12, color: "#6b7280" }}>Original Cost</div>
          <div style={{ fontSize: 24, fontWeight: 700 }}>{original.toFixed(0)}</div>
        </div>
        <div style={{ textAlign: "center" }}>
          <div style={{ fontSize: 12, color: "#6b7280" }}>Improvement</div>
          <div style={{
            fontSize: 28, fontWeight: 700,
            color: isImprovement ? "#22c55e" : "#ef4444",
          }}>
            {isImprovement ? "↓" : "↑"}{Math.abs(improvement).toFixed(1)}%
          </div>
        </div>
        <div style={{ textAlign: "right" }}>
          <div style={{ fontSize: 12, color: "#6b7280" }}>Simulated Cost</div>
          <div style={{ fontSize: 24, fontWeight: 700 }}>{simulated.toFixed(0)}</div>
        </div>
      </div>
      <div style={{ height: 12, background: "#e5e7eb", borderRadius: 6, overflow: "hidden" }}>
        <div style={{
          width: `${barWidth}%`, height: "100%", borderRadius: 6,
          background: isImprovement
            ? "linear-gradient(90deg, #22c55e, #16a34a)"
            : "linear-gradient(90deg, #ef4444, #dc2626)",
          transition: "width 0.5s ease",
        }} />
      </div>
    </div>
  );
};

const QueryTable: React.FC<{ queries: PerQueryResult[] }> = ({ queries }) => (
  <div style={{ overflowX: "auto" }}>
    <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
      <thead>
        <tr style={{ borderBottom: "2px solid #e5e7eb" }}>
          <th style={{ textAlign: "left", padding: "8px 12px" }}>Query</th>
          <th style={{ textAlign: "right", padding: "8px 12px" }}>Original</th>
          <th style={{ textAlign: "right", padding: "8px 12px" }}>Simulated</th>
          <th style={{ textAlign: "right", padding: "8px 12px" }}>Delta</th>
        </tr>
      </thead>
      <tbody>
        {queries.map((q, i) => (
          <tr key={i} style={{ borderBottom: "1px solid #f3f4f6" }}>
            <td style={{ padding: "8px 12px", maxWidth: 300, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
              <code style={{ fontSize: 12 }}>{q.sql}</code>
            </td>
            <td style={{ textAlign: "right", padding: "8px 12px" }}>{q.original_cost.toFixed(1)}</td>
            <td style={{ textAlign: "right", padding: "8px 12px" }}>{q.simulated_cost.toFixed(1)}</td>
            <td style={{
              textAlign: "right", padding: "8px 12px", fontWeight: 600,
              color: q.delta_pct > 0 ? "#22c55e" : q.delta_pct < 0 ? "#ef4444" : "#6b7280",
            }}>
              {q.delta_pct > 0 ? "+" : ""}{q.delta_pct.toFixed(1)}%
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  </div>
);

/* ──────────────────── Main Component ──────────────────────────── */

export const WhatIfSimulator: React.FC<WhatIfSimulatorProps> = ({
  apiBaseUrl = "http://localhost:8080",
  tenantId = "default",
}) => {
  const [changeType, setChangeType] = useState<HypotheticalChange["type"]>("add_index");
  const [table, setTable] = useState("");
  const [column, setColumn] = useState("");
  const [indexType, setIndexType] = useState("btree");
  const [sampleSize, setSampleSize] = useState(1000);
  const [scenarioName, setScenarioName] = useState("");
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<SimulationResult | null>(null);
  const [error, setError] = useState("");

  const handleRun = useCallback(async () => {
    if (!table) { setError("Table name is required"); return; }
    setLoading(true);
    setError("");
    setResult(null);

    const change: HypotheticalChange = { type: changeType, table, column };
    if (changeType === "add_index") change.index_type = indexType;

    try {
      const res = await createAndRunScenario(
        apiBaseUrl, tenantId, scenarioName || `${changeType} on ${table}.${column}`,
        [change], sampleSize,
      );
      setResult(res);
    } catch (e) {
      setError(`Simulation failed: ${e instanceof Error ? e.message : "Unknown error"}`);
    }
    setLoading(false);
  }, [changeType, table, column, indexType, sampleSize, scenarioName, apiBaseUrl, tenantId]);

  return (
    <div style={{
      display: "flex", gap: 20, fontFamily: "system-ui, sans-serif",
      border: "1px solid #e5e7eb", borderRadius: 12, overflow: "hidden",
    }}>
      {/* Left: Scenario Builder */}
      <div style={{ width: 340, padding: 20, borderRight: "1px solid #e5e7eb", background: "#f9fafb" }}>
        <h3 style={{ marginTop: 0, fontSize: 16 }}>Scenario Builder</h3>

        <label style={{ display: "block", marginBottom: 4, fontSize: 13, color: "#374151" }}>Change Type</label>
        <select value={changeType} onChange={(e) => setChangeType(e.target.value as HypotheticalChange["type"])}
          style={{ width: "100%", padding: 8, borderRadius: 6, border: "1px solid #d1d5db", marginBottom: 12 }}>
          <option value="add_index">Add Index</option>
          <option value="remove_index">Remove Index</option>
          <option value="enable_feature">Enable Feature</option>
          <option value="change_backend">Move Table</option>
        </select>

        <label style={{ display: "block", marginBottom: 4, fontSize: 13, color: "#374151" }}>Table</label>
        <input value={table} onChange={(e) => setTable(e.target.value)} placeholder="orders"
          style={{ width: "100%", padding: 8, borderRadius: 6, border: "1px solid #d1d5db", marginBottom: 12, boxSizing: "border-box" }} />

        <label style={{ display: "block", marginBottom: 4, fontSize: 13, color: "#374151" }}>Column</label>
        <input value={column} onChange={(e) => setColumn(e.target.value)} placeholder="status"
          style={{ width: "100%", padding: 8, borderRadius: 6, border: "1px solid #d1d5db", marginBottom: 12, boxSizing: "border-box" }} />

        {changeType === "add_index" && (
          <>
            <label style={{ display: "block", marginBottom: 4, fontSize: 13, color: "#374151" }}>Index Type</label>
            <select value={indexType} onChange={(e) => setIndexType(e.target.value)}
              style={{ width: "100%", padding: 8, borderRadius: 6, border: "1px solid #d1d5db", marginBottom: 12 }}>
              <option value="btree">B-Tree</option>
              <option value="hash">Hash</option>
              <option value="HNSW">HNSW (Vector)</option>
              <option value="IVFFlat">IVFFlat (Vector)</option>
            </select>
          </>
        )}

        <label style={{ display: "block", marginBottom: 4, fontSize: 13, color: "#374151" }}>
          Query Sample Size: {sampleSize}
        </label>
        <input type="range" min={100} max={10000} step={100} value={sampleSize}
          onChange={(e) => setSampleSize(parseInt(e.target.value))}
          style={{ width: "100%", marginBottom: 12 }} />

        <label style={{ display: "block", marginBottom: 4, fontSize: 13, color: "#374151" }}>Scenario Name</label>
        <input value={scenarioName} onChange={(e) => setScenarioName(e.target.value)}
          placeholder="Optional name..."
          style={{ width: "100%", padding: 8, borderRadius: 6, border: "1px solid #d1d5db", marginBottom: 16, boxSizing: "border-box" }} />

        <button onClick={handleRun} disabled={loading}
          style={{
            width: "100%", padding: "10px 0", borderRadius: 8, border: "none",
            background: loading ? "#9ca3af" : "#3b82f6", color: "#fff",
            fontWeight: 600, fontSize: 14, cursor: loading ? "default" : "pointer",
          }}>
          {loading ? "Running Simulation..." : "Run Simulation"}
        </button>
        {error && <div style={{ marginTop: 8, color: "#ef4444", fontSize: 13 }}>{error}</div>}
      </div>

      {/* Right: Results */}
      <div style={{ flex: 1, padding: 20 }}>
        {!result && !loading && (
          <div style={{ textAlign: "center", color: "#9ca3af", marginTop: 60 }}>
            Configure a scenario and click "Run Simulation" to see results.
          </div>
        )}
        {loading && (
          <div style={{ textAlign: "center", color: "#6b7280", marginTop: 60 }}>
            Replaying workload with hypothetical changes...
          </div>
        )}
        {result && (
          <>
            <h3 style={{ marginTop: 0, fontSize: 16 }}>Simulation Results</h3>
            <div style={{ fontSize: 13, color: "#6b7280", marginBottom: 16 }}>
              {result.queries_replayed} queries replayed
            </div>
            <CostGauge original={result.original_total_cost} simulated={result.simulated_total_cost}
              improvement={result.cost_improvement_pct} />
            <div style={{
              padding: 12, background: "#f0fdf4", borderRadius: 8, marginBottom: 16,
              borderLeft: "4px solid #22c55e", fontSize: 14,
            }}>
              {result.recommendation}
            </div>
            <h4 style={{ fontSize: 14, marginBottom: 8 }}>Top Improved Queries</h4>
            <QueryTable queries={result.top_benefiting_queries} />
          </>
        )}
      </div>
    </div>
  );
};

export default WhatIfSimulator;
