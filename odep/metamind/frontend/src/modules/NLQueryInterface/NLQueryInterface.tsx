import React, { useState } from 'react'
import { MessageSquare, Send, Copy, Play } from 'lucide-react'
import api, { NLQueryResponse } from '../../api/client'

interface NLQueryInterfaceProps {
  tenantId: string
}

const EXAMPLE_QUESTIONS = [
  "Show me the top 10 customers by total revenue this month",
  "How many orders were placed in the last 7 days grouped by status?",
  "Find all products with less than 10 units in stock",
]

const NLQueryInterface: React.FC<NLQueryInterfaceProps> = ({ tenantId }) => {
  const [question, setQuestion] = useState('')
  const [tables, setTables] = useState('')
  const [result, setResult] = useState<NLQueryResponse | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)

  const handleSubmit = async () => {
    if (!question.trim()) return
    setLoading(true)
    setError(null)
    try {
      api.setTenant(tenantId)
      const tableHints = tables.split(',').map(t => t.trim()).filter(Boolean)
      const res = await api.nlQuery({
        nl_text: question,
        table_hints: tableHints.length > 0 ? tableHints : undefined,
        execute: true,
      })
      setResult(res)
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setLoading(false)
    }
  }

  const copySQL = () => result && navigator.clipboard.writeText(result.generated_sql)

  return (
    <div style={{ padding: 24, maxWidth: 900, margin: '0 auto' }}>
      <div style={{ marginBottom: 24 }}>
        <h2 style={{ margin: 0, fontSize: 22, fontWeight: 700, color: '#0f172a', display: 'flex', alignItems: 'center', gap: 10 }}>
          <MessageSquare size={22} color="#6366f1" />
          Natural Language Query Interface
        </h2>
        <p style={{ margin: '4px 0 0', color: '#64748b', fontSize: 14 }}>
          F28 · Powered by GPT-4 · Ask questions in plain English
        </p>
      </div>

      {/* Feature flag warning */}
      <div style={{
        background: '#fef9c3', border: '1px solid #fde047', borderRadius: 8,
        padding: '10px 14px', marginBottom: 20, fontSize: 13, color: '#854d0e'
      }}>
        ⚠️ Requires <strong>F28_nl_interface</strong> feature flag enabled and <strong>OPENAI_API_KEY</strong> configured.
      </div>

      {/* Example questions */}
      <div style={{ marginBottom: 16 }}>
        <div style={{ fontSize: 12, color: '#64748b', marginBottom: 8 }}>EXAMPLE QUESTIONS</div>
        <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
          {EXAMPLE_QUESTIONS.map((q, i) => (
            <button key={i} onClick={() => setQuestion(q)} style={{
              padding: '6px 12px', fontSize: 13, borderRadius: 20,
              border: '1px solid #e2e8f0', background: '#f8fafc',
              cursor: 'pointer', color: '#475569'
            }}>
              {q}
            </button>
          ))}
        </div>
      </div>

      {/* Input area */}
      <div style={{
        background: '#fff', borderRadius: 12, padding: 20,
        boxShadow: '0 1px 3px rgba(0,0,0,0.1)', marginBottom: 20
      }}>
        <textarea
          value={question}
          onChange={(e) => setQuestion(e.target.value)}
          placeholder="Ask a question about your data in plain English..."
          rows={3}
          style={{
            width: '100%', padding: '10px 14px', fontSize: 15,
            border: '1px solid #e2e8f0', borderRadius: 8, resize: 'vertical',
            fontFamily: 'inherit', outline: 'none', boxSizing: 'border-box',
            color: '#0f172a', lineHeight: 1.5
          }}
          onKeyDown={(e) => e.key === 'Enter' && e.ctrlKey && handleSubmit()}
        />
        <div style={{ marginTop: 12, display: 'flex', gap: 10, alignItems: 'center' }}>
          <input
            value={tables}
            onChange={(e) => setTables(e.target.value)}
            placeholder="Table hints (optional): orders, customers, products"
            style={{
              flex: 1, padding: '8px 12px', fontSize: 13,
              border: '1px solid #e2e8f0', borderRadius: 6,
              fontFamily: 'inherit', outline: 'none', color: '#374151'
            }}
          />
          <button onClick={handleSubmit} disabled={loading || !question.trim()} style={{
            display: 'flex', alignItems: 'center', gap: 6,
            padding: '9px 18px', background: '#6366f1', color: '#fff',
            border: 'none', borderRadius: 8, cursor: 'pointer', fontSize: 14, fontWeight: 600
          }}>
            <Send size={14} />
            {loading ? 'Generating...' : 'Generate SQL'}
          </button>
        </div>
      </div>

      {error && (
        <div style={{
          background: '#fef2f2', border: '1px solid #fecaca',
          borderRadius: 8, padding: '12px 16px', color: '#dc2626', marginBottom: 16
        }}>
          {error}
        </div>
      )}

      {result && (
        <div>
          {/* Generated SQL */}
          <div style={{
            background: '#1e293b', borderRadius: 12, padding: 20, marginBottom: 16
          }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 12 }}>
              <div style={{ fontSize: 13, color: '#64748b' }}>
                GENERATED SQL · Confidence: {(result.confidence * 100).toFixed(0)}%
              </div>
              <button onClick={copySQL} style={{
                display: 'flex', alignItems: 'center', gap: 4,
                padding: '4px 10px', background: '#334155', color: '#e2e8f0',
                border: 'none', borderRadius: 6, cursor: 'pointer', fontSize: 12
              }}>
                <Copy size={12} /> Copy
              </button>
            </div>
            <pre style={{ margin: 0, color: '#7dd3fc', fontSize: 13, lineHeight: 1.6, whiteSpace: 'pre-wrap' }}>
              {result.generated_sql}
            </pre>
          </div>

          {/* Execution result */}
          {result.execution_result && (
            <div style={{
              background: '#fff', borderRadius: 12, padding: 20,
              boxShadow: '0 1px 3px rgba(0,0,0,0.1)'
            }}>
              <div style={{ marginBottom: 12, fontSize: 13, color: '#64748b' }}>
                {result.execution_result.row_count} rows · {result.execution_result.duration_ms.toFixed(1)}ms
              </div>
              <div style={{ overflowX: 'auto' }}>
                <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
                  <thead>
                    <tr style={{ background: '#f1f5f9' }}>
                      {result.execution_result.columns.map(col => (
                        <th key={col} style={{ padding: '8px 12px', textAlign: 'left', fontWeight: 600, color: '#374151', borderBottom: '2px solid #e5e7eb' }}>
                          {col}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {result.execution_result.rows.slice(0, 50).map((row, i) => (
                      <tr key={i} style={{ background: i % 2 === 0 ? '#fff' : '#f9fafb' }}>
                        {result.execution_result!.columns.map(col => (
                          <td key={col} style={{ padding: '7px 12px', color: '#374151', borderBottom: '1px solid #f3f4f6' }}>
                            {String(row[col] ?? '')}
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
      )}
    </div>
  )
}

export default NLQueryInterface
