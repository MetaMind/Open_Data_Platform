/**
 * VectorSearch/VectorSearchPanel.tsx
 *
 * F19 Vector Search UI: input vector, table/column selectors, distance metric,
 * top-K slider, results table, and index creation.
 */

import React, { useState, useCallback } from "react";

/* ─────────────────────── Types ─────────────────────────────────── */

interface VectorSearchResultRow {
  [key: string]: unknown;
  __distance?: number;
}

interface VectorSearchResponse {
  rows: VectorSearchResultRow[];
  distances: number[];
  row_count: number;
  duration_ms: number;
  index_used: string | null;
  backend_used: string;
}

interface VectorSearchPanelProps {
  apiBaseUrl?: string;
  tenantId?: string;
}

/* ──────────────────── API Helpers ──────────────────────────────── */

async function executeVectorSearch(
  baseUrl: string, tenantId: string, table: string,
  column: string, queryVector: number[], topK: number, metric: string
): Promise<VectorSearchResponse> {
  const res = await fetch(`${baseUrl}/api/v1/vector/search`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      tenant_id: tenantId, table, embedding_column: column,
      query_vector: queryVector, top_k: topK, distance_metric: metric,
    }),
  });
  const data = await res.json();
  return data.data;
}

async function createVectorIndex(
  baseUrl: string, tenantId: string, table: string,
  column: string, indexType: string, metric: string
): Promise<{ index_name: string }> {
  const res = await fetch(`${baseUrl}/api/v1/vector/indexes`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      tenant_id: tenantId, table, column,
      index_type: indexType, metric,
      params: { dimensions: 768 },
    }),
  });
  const data = await res.json();
  return data.data;
}

/* ──────────────────── Main Component ──────────────────────────── */

export const VectorSearchPanel: React.FC<VectorSearchPanelProps> = ({
  apiBaseUrl = "http://localhost:8080",
  tenantId = "default",
}) => {
  const [table, setTable] = useState("");
  const [column, setColumn] = useState("");
  const [vectorInput, setVectorInput] = useState("");
  const [metric, setMetric] = useState<"cosine" | "l2" | "inner_product">("cosine");
  const [topK, setTopK] = useState(10);
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<VectorSearchResponse | null>(null);
  const [error, setError] = useState("");
  const [indexCreating, setIndexCreating] = useState(false);
  const [indexMsg, setIndexMsg] = useState("");

  const handleSearch = useCallback(async () => {
    if (!table || !column || !vectorInput) {
      setError("Table, column, and query vector are required");
      return;
    }
    setLoading(true);
    setError("");
    setResult(null);

    let queryVector: number[];
    try {
      queryVector = JSON.parse(vectorInput);
      if (!Array.isArray(queryVector) || !queryVector.every((v) => typeof v === "number")) {
        throw new Error("Must be array of numbers");
      }
    } catch {
      setError("Invalid vector format. Enter a JSON array of numbers, e.g. [0.1, 0.2, 0.3]");
      setLoading(false);
      return;
    }

    try {
      const res = await executeVectorSearch(apiBaseUrl, tenantId, table, column, queryVector, topK, metric);
      setResult(res);
    } catch (e) {
      setError(`Search failed: ${e instanceof Error ? e.message : "Unknown error"}`);
    }
    setLoading(false);
  }, [table, column, vectorInput, metric, topK, apiBaseUrl, tenantId]);

  const handleCreateIndex = useCallback(async () => {
    if (!table || !column) { setIndexMsg("Table and column required"); return; }
    setIndexCreating(true);
    setIndexMsg("");
    try {
      const res = await createVectorIndex(apiBaseUrl, tenantId, table, column, "HNSW", metric);
      setIndexMsg(`Created index: ${res.index_name}`);
    } catch (e) {
      setIndexMsg(`Failed: ${e instanceof Error ? e.message : "Unknown error"}`);
    }
    setIndexCreating(false);
  }, [table, column, metric, apiBaseUrl, tenantId]);

  const resultColumns = result && result.rows.length > 0
    ? Object.keys(result.rows[0]).filter((k) => k !== "__distance").slice(0, 6)
    : [];

  return (
    <div style={{
      fontFamily: "system-ui, sans-serif",
      border: "1px solid #e5e7eb", borderRadius: 12, padding: 20,
    }}>
      <h3 style={{ marginTop: 0, fontSize: 18, marginBottom: 16 }}>Vector Similarity Search</h3>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12, marginBottom: 16 }}>
        <div>
          <label style={{ display: "block", fontSize: 13, color: "#374151", marginBottom: 4 }}>Table</label>
          <input value={table} onChange={(e) => setTable(e.target.value)} placeholder="documents"
            style={{ width: "100%", padding: 8, borderRadius: 6, border: "1px solid #d1d5db", boxSizing: "border-box" }} />
        </div>
        <div>
          <label style={{ display: "block", fontSize: 13, color: "#374151", marginBottom: 4 }}>Embedding Column</label>
          <input value={column} onChange={(e) => setColumn(e.target.value)} placeholder="embedding"
            style={{ width: "100%", padding: 8, borderRadius: 6, border: "1px solid #d1d5db", boxSizing: "border-box" }} />
        </div>
      </div>

      <div style={{ marginBottom: 12 }}>
        <label style={{ display: "block", fontSize: 13, color: "#374151", marginBottom: 4 }}>
          Query Vector (JSON array)
        </label>
        <textarea value={vectorInput} onChange={(e) => setVectorInput(e.target.value)}
          placeholder='[0.1, -0.23, 0.87, ...]' rows={3}
          style={{
            width: "100%", padding: 8, borderRadius: 6, border: "1px solid #d1d5db",
            fontFamily: "monospace", fontSize: 13, boxSizing: "border-box", resize: "vertical",
          }} />
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12, marginBottom: 16 }}>
        <div>
          <label style={{ display: "block", fontSize: 13, color: "#374151", marginBottom: 4 }}>Distance Metric</label>
          <select value={metric} onChange={(e) => setMetric(e.target.value as typeof metric)}
            style={{ width: "100%", padding: 8, borderRadius: 6, border: "1px solid #d1d5db" }}>
            <option value="cosine">Cosine</option>
            <option value="l2">L2 (Euclidean)</option>
            <option value="inner_product">Inner Product</option>
          </select>
        </div>
        <div>
          <label style={{ display: "block", fontSize: 13, color: "#374151", marginBottom: 4 }}>
            Top K: {topK}
          </label>
          <input type="range" min={5} max={1000} step={5} value={topK}
            onChange={(e) => setTopK(parseInt(e.target.value))}
            style={{ width: "100%" }} />
        </div>
      </div>

      <div style={{ display: "flex", gap: 8, marginBottom: 16 }}>
        <button onClick={handleSearch} disabled={loading}
          style={{
            padding: "10px 24px", borderRadius: 8, border: "none",
            background: loading ? "#9ca3af" : "#3b82f6", color: "#fff",
            fontWeight: 600, cursor: loading ? "default" : "pointer",
          }}>
          {loading ? "Searching..." : "Search"}
        </button>
        <button onClick={handleCreateIndex} disabled={indexCreating}
          style={{
            padding: "10px 24px", borderRadius: 8, border: "1px solid #d1d5db",
            background: "#fff", color: "#374151", cursor: indexCreating ? "default" : "pointer",
          }}>
          {indexCreating ? "Creating..." : "Create Index"}
        </button>
      </div>

      {error && <div style={{ color: "#ef4444", fontSize: 13, marginBottom: 12 }}>{error}</div>}
      {indexMsg && <div style={{ color: "#3b82f6", fontSize: 13, marginBottom: 12 }}>{indexMsg}</div>}

      {result && (
        <div>
          <div style={{ display: "flex", gap: 16, marginBottom: 12, fontSize: 13, color: "#6b7280" }}>
            <span>{result.row_count} results</span>
            <span>{result.duration_ms.toFixed(1)}ms</span>
            <span>Backend: {result.backend_used}</span>
            <span>Index: {result.index_used ?? "exact scan"}</span>
          </div>

          <div style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
              <thead>
                <tr style={{ borderBottom: "2px solid #e5e7eb" }}>
                  <th style={{ textAlign: "right", padding: "8px 12px" }}>Distance</th>
                  {resultColumns.map((col) => (
                    <th key={col} style={{ textAlign: "left", padding: "8px 12px" }}>{col}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {result.rows.map((row, i) => (
                  <tr key={i} style={{ borderBottom: "1px solid #f3f4f6" }}>
                    <td style={{ textAlign: "right", padding: "8px 12px", fontFamily: "monospace", color: "#3b82f6" }}>
                      {result.distances[i]?.toFixed(4) ?? "N/A"}
                    </td>
                    {resultColumns.map((col) => (
                      <td key={col} style={{ padding: "8px 12px", maxWidth: 200, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                        {String(row[col] ?? "")}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
};

export default VectorSearchPanel;
