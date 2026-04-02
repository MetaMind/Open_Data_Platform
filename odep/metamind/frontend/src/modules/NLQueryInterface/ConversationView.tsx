/**
 * NLQueryInterface/ConversationView.tsx
 *
 * Multi-turn NL conversation view for MetaMind's F28 Natural Language Interface.
 * Features: chat bubbles, generated SQL display, feedback buttons, query refinement.
 */

import React, { useState, useRef, useEffect, useCallback } from "react";

/* ─────────────────────────── Types ─────────────────────────────── */

interface ConversationTurn {
  role: "user" | "assistant";
  content: string;
  generatedSql?: string;
  confidence?: number;
  tablesUsed?: string[];
  wasValidated?: boolean;
  feedbackGiven?: "up" | "down" | null;
  timestamp: string;
}

interface NLQueryResponse {
  session_id: string;
  sql: string;
  confidence: number;
  tables_used: string[];
  was_validated: boolean;
  validation_error?: string;
}

interface ConversationViewProps {
  apiBaseUrl?: string;
  tenantId?: string;
  initialSessionId?: string;
}

/* ─────────────────────── API Helpers ───────────────────────────── */

async function createSession(baseUrl: string, tenantId: string): Promise<string> {
  const res = await fetch(`${baseUrl}/api/v1/nl/session`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ tenant_id: tenantId }),
  });
  const data = await res.json();
  return data.data.session_id;
}

async function sendQuery(
  baseUrl: string, sessionId: string, query: string, tenantId: string
): Promise<NLQueryResponse> {
  const res = await fetch(`${baseUrl}/api/v1/nl/session/query`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ session_id: sessionId, query, tenant_id: tenantId }),
  });
  const data = await res.json();
  return data.data;
}

async function sendFeedback(
  baseUrl: string, tenantId: string, nlText: string,
  generatedSql: string, wasCorrect: boolean, correction?: string
): Promise<void> {
  await fetch(`${baseUrl}/api/v1/nl/feedback`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      tenant_id: tenantId, nl_text: nlText,
      generated_sql: generatedSql, was_correct: wasCorrect,
      correction: correction ?? null,
    }),
  });
}

/* ─────────────────── Main Component ───────────────────────────── */

export const ConversationView: React.FC<ConversationViewProps> = ({
  apiBaseUrl = "http://localhost:8080",
  tenantId = "default",
  initialSessionId,
}) => {
  const [sessionId, setSessionId] = useState<string>(initialSessionId ?? "");
  const [turns, setTurns] = useState<ConversationTurn[]>([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [expandedSql, setExpandedSql] = useState<Record<number, boolean>>({});
  const scrollRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [turns]);

  const handleSubmit = useCallback(async () => {
    const query = input.trim();
    if (!query || loading) return;
    setInput("");
    setLoading(true);

    let sid = sessionId;
    if (!sid) {
      try {
        sid = await createSession(apiBaseUrl, tenantId);
        setSessionId(sid);
      } catch {
        sid = "local-" + Date.now();
        setSessionId(sid);
      }
    }

    const userTurn: ConversationTurn = {
      role: "user", content: query, timestamp: new Date().toISOString(),
    };
    setTurns((prev) => [...prev, userTurn]);

    try {
      const response = await sendQuery(apiBaseUrl, sid, query, tenantId);
      const assistantTurn: ConversationTurn = {
        role: "assistant",
        content: response.sql
          ? "Here is the generated SQL query:"
          : "Could not generate valid SQL. Please rephrase your question.",
        generatedSql: response.sql,
        confidence: response.confidence,
        tablesUsed: response.tables_used,
        wasValidated: response.was_validated,
        feedbackGiven: null,
        timestamp: new Date().toISOString(),
      };
      setTurns((prev) => [...prev, assistantTurn]);
    } catch {
      setTurns((prev) => [...prev, {
        role: "assistant",
        content: "Connection error. Check that the API server is running.",
        timestamp: new Date().toISOString(),
      }]);
    }
    setLoading(false);
  }, [input, loading, sessionId, apiBaseUrl, tenantId]);

  const handleFeedback = useCallback(async (idx: number, positive: boolean) => {
    const turn = turns[idx];
    if (!turn.generatedSql) return;
    const userTurn = turns.slice(0, idx).reverse().find((t) => t.role === "user");
    try {
      await sendFeedback(apiBaseUrl, tenantId, userTurn?.content ?? "", turn.generatedSql, positive);
    } catch { /* best-effort */ }
    setTurns((prev) => prev.map((t, i) =>
      i === idx ? { ...t, feedbackGiven: positive ? "up" : "down" } : t
    ));
  }, [turns, apiBaseUrl, tenantId]);

  return (
    <div style={{
      display: "flex", flexDirection: "column", height: "100%",
      maxHeight: 700, border: "1px solid #e5e7eb", borderRadius: 12,
      overflow: "hidden", fontFamily: "system-ui, sans-serif",
    }}>
      <div style={{
        padding: "12px 16px", borderBottom: "1px solid #e5e7eb",
        background: "#f9fafb", fontWeight: 600, fontSize: 15,
      }}>
        MetaMind NL Query Interface
        {sessionId && <span style={{ fontWeight: 400, fontSize: 12, color: "#9ca3af", marginLeft: 12 }}>
          Session: {sessionId.slice(0, 8)}...
        </span>}
      </div>

      <div ref={scrollRef} style={{ flex: 1, overflowY: "auto", padding: 16 }}>
        {turns.length === 0 && (
          <div style={{ textAlign: "center", color: "#9ca3af", marginTop: 40 }}>
            Ask a question about your data in natural language.
          </div>
        )}
        {turns.map((turn, i) => (
          <div key={i} style={{
            display: "flex", justifyContent: turn.role === "user" ? "flex-end" : "flex-start",
            marginBottom: 12,
          }}>
            <div style={{
              maxWidth: "80%", padding: "10px 14px", borderRadius: 12,
              background: turn.role === "user" ? "#3b82f6" : "#f3f4f6",
              color: turn.role === "user" ? "#fff" : "#1f2937", fontSize: 14,
            }}>
              {turn.content}
              {turn.generatedSql && (
                <div style={{ marginTop: 8 }}>
                  <button onClick={() => setExpandedSql((p) => ({ ...p, [i]: !p[i] }))}
                    style={{
                      background: "none", border: "1px solid #d1d5db", borderRadius: 6,
                      padding: "4px 12px", cursor: "pointer", fontSize: 13, color: "#6b7280",
                    }}>
                    {expandedSql[i] === false ? "▶ Show SQL" : "▼ Hide SQL"}
                    <span style={{
                      marginLeft: 8, fontWeight: 600,
                      color: (turn.confidence ?? 0) >= 0.8 ? "#22c55e" : (turn.confidence ?? 0) >= 0.5 ? "#eab308" : "#ef4444",
                    }}>
                      {Math.round((turn.confidence ?? 0) * 100)}%
                    </span>
                  </button>
                  {expandedSql[i] !== false && (
                    <pre style={{
                      background: "#1e293b", color: "#e2e8f0", padding: 12,
                      borderRadius: 6, marginTop: 6, fontSize: 13, overflowX: "auto",
                    }}>
                      {turn.generatedSql}
                    </pre>
                  )}
                  <div style={{ marginTop: 6, display: "flex", gap: 8 }}>
                    <button onClick={() => handleFeedback(i, true)} style={{
                      border: "none", borderRadius: 4, padding: "4px 8px", cursor: "pointer",
                      background: turn.feedbackGiven === "up" ? "#dcfce7" : "#f3f4f6",
                    }}>👍</button>
                    <button onClick={() => handleFeedback(i, false)} style={{
                      border: "none", borderRadius: 4, padding: "4px 8px", cursor: "pointer",
                      background: turn.feedbackGiven === "down" ? "#fef2f2" : "#f3f4f6",
                    }}>👎</button>
                  </div>
                </div>
              )}
              {turn.tablesUsed && turn.tablesUsed.length > 0 && (
                <div style={{ marginTop: 6, fontSize: 12, color: "#6b7280" }}>
                  Tables: {turn.tablesUsed.join(", ")}
                </div>
              )}
            </div>
          </div>
        ))}
        {loading && <div style={{ color: "#9ca3af", fontStyle: "italic" }}>Generating SQL...</div>}
      </div>

      <div style={{ padding: 12, borderTop: "1px solid #e5e7eb", display: "flex", gap: 8 }}>
        <input type="text" value={input} onChange={(e) => setInput(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && handleSubmit()}
          placeholder="Ask about your data..." disabled={loading}
          style={{
            flex: 1, padding: "10px 14px", borderRadius: 8,
            border: "1px solid #d1d5db", fontSize: 14, outline: "none",
          }}
        />
        <button onClick={handleSubmit} disabled={loading || !input.trim()}
          style={{
            padding: "10px 20px", borderRadius: 8, border: "none",
            background: loading || !input.trim() ? "#d1d5db" : "#3b82f6",
            color: "#fff", fontWeight: 600, cursor: loading || !input.trim() ? "default" : "pointer",
          }}>
          Send
        </button>
      </div>
    </div>
  );
};

export default ConversationView;
