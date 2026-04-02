import React, { useState, useCallback } from 'react'
import Editor from '@monaco-editor/react'
import { Play, Zap, Copy, Download, RefreshCw } from 'lucide-react'
import api, { ExecuteResponse } from '../../api/client'

interface QueryWorkbenchProps {
  tenantId: string
}

const EXAMPLE_QUERIES = [
  'SELECT * FROM orders WHERE status = \'pending\' LIMIT 100',
  'SELECT customer_id, COUNT(*) as order_count, SUM(total) as revenue\nFROM orders\nGROUP BY customer_id\nORDER BY revenue DESC',
  'SELECT o.order_id, c.name, o.total\nFROM orders o\nJOIN customers c ON o.customer_id = c.id\nWHERE o.created_at >= NOW() - INTERVAL \'7 days\'',
]

const QueryWorkbench: React.FC<QueryWorkbenchProps> = ({ tenantId }) => {
  const [sql, setSql] = useState(EXAMPLE_QUERIES[0])
  const [result, setResult] = useState<ExecuteResponse | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)

  const executeQuery = useCallback(async () => {
    if (!sql.trim()) return
    setLoading(true)
    setError(null)
    try {
      api.setTenant(tenantId)
      const res = await api.executeQuery({ sql })
      setResult(res)
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setLoading(false)
    }
  }, [sql, tenantId])

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') executeQuery()
  }

  const copySQL = () => navigator.clipboard.writeText(sql)

  const downloadCSV = () => {
    if (!result) return
    const columns = result.columns || []
    const data = result.data || []
    const header = columns.join(',')
    const rows = data.map(r => columns.map(c => `"${r[c] ?? ''}"`).join(','))
    const csv = [header, ...rows].join('\n')
    const blob = new Blob([csv], { type: 'text/csv' })
    const a = document.createElement('a'); a.href = URL.createObjectURL(blob)
    a.download = 'metamind_result.csv'; a.click()
  }

  return (
    <div style={{ height: '100vh', display: 'flex', flexDirection: 'column' }}>
      {/* Header */}
      <div style={{
        padding: '12px 20px', background: '#fff', borderBottom: '1px solid #e2e8f0',
        display: 'flex', alignItems: 'center', justifyContent: 'space-between'
      }}>
        <div>
          <h2 style={{ margin: 0, fontSize: 18, fontWeight: 700, color: '#0f172a' }}>
            Query Workbench
          </h2>
          <p style={{ margin: 0, fontSize: 12, color: '#64748b' }}>
            MetaMind Optimization Pipeline — Ctrl+Enter to run
          </p>
        </div>
        <div style={{ display: 'flex', gap: 8 }}>
          {['Simple Query', 'Aggregation', 'Join Query'].map((label, i) => (
            <button key={i} onClick={() => setSql(EXAMPLE_QUERIES[i])}
              style={{
                padding: '4px 10px', fontSize: 12, borderRadius: 4,
                border: '1px solid #e2e8f0', background: '#f8fafc',
                cursor: 'pointer', color: '#475569'
              }}>
              {label}
            </button>
          ))}
        </div>
      </div>

      {/* Editor */}
      <div style={{ height: 280, borderBottom: '1px solid #e2e8f0' }} onKeyDown={handleKeyDown}>
        <Editor
          height="100%"
          defaultLanguage="sql"
          value={sql}
          onChange={(v) => setSql(v || '')}
          theme="vs-dark"
          options={{
            minimap: { enabled: false },
            fontSize: 14,
            padding: { top: 12 },
            scrollBeyondLastLine: false,
            wordWrap: 'on',
          }}
        />
      </div>

      {/* Toolbar */}
      <div style={{
        padding: '8px 16px', background: '#1e293b',
        display: 'flex', alignItems: 'center', gap: 8
      }}>
        <button onClick={executeQuery} disabled={loading} style={{
          display: 'flex', alignItems: 'center', gap: 6,
          padding: '7px 16px', background: '#6366f1', color: '#fff',
          border: 'none', borderRadius: 6, cursor: 'pointer', fontSize: 14, fontWeight: 600
        }}>
          {loading ? <RefreshCw size={14} className="spin" /> : <Play size={14} />}
          {loading ? 'Running...' : 'Run Query'}
        </button>
        <button onClick={copySQL} title="Copy SQL" style={{
          padding: '7px 10px', background: '#334155', color: '#e2e8f0',
          border: 'none', borderRadius: 6, cursor: 'pointer'
        }}>
          <Copy size={14} />
        </button>
        {result && (
          <button onClick={downloadCSV} title="Download CSV" style={{
            padding: '7px 10px', background: '#334155', color: '#e2e8f0',
            border: 'none', borderRadius: 6, cursor: 'pointer'
          }}>
            <Download size={14} />
          </button>
        )}
        {result && (
          <div style={{ marginLeft: 'auto', display: 'flex', gap: 16, fontSize: 12, color: '#94a3b8' }}>
            <span><Zap size={12} style={{ display: 'inline', verticalAlign: 'middle' }} /> Strategy {result.execution_strategy}</span>
            <span style={{ color: result.cache_hit ? '#22c55e' : '#94a3b8' }}>
              {result.cache_hit ? '✓ Cache Hit' : 'Cache Miss'}
            </span>
            <span>{(result.estimated_cost_ms ?? 0).toFixed(1)}ms est</span>
            <span>{(result.execution_time_ms ?? 0).toFixed(1)}ms total</span>
            <span>{result.routed_to}</span>
            <span style={{ color: '#6366f1' }}>{result.status}</span>
          </div>
        )}
      </div>

      {/* Results */}
      <div style={{ flex: 1, overflow: 'auto', padding: 16 }}>
        {error && (
          <div style={{
            background: '#fef2f2', border: '1px solid #fecaca',
            borderRadius: 8, padding: '12px 16px', color: '#dc2626', fontSize: 14
          }}>
            <strong>Error:</strong> {error}
          </div>
        )}
        {result && !error && (
          <>
            <div style={{ marginBottom: 8, fontSize: 13, color: '#64748b' }}>
              {(result.row_count ?? 0).toLocaleString()} rows
            </div>
            <div style={{ overflowX: 'auto' }}>
              <table style={{
                width: '100%', borderCollapse: 'collapse', fontSize: 13,
                background: '#fff', borderRadius: 8, overflow: 'hidden',
                boxShadow: '0 1px 3px rgba(0,0,0,0.1)'
              }}>
                <thead>
                  <tr style={{ background: '#f1f5f9' }}>
                    {(result.columns || []).map(col => (
                      <th key={col} style={{
                        padding: '8px 12px', textAlign: 'left',
                        fontWeight: 600, color: '#374151', borderBottom: '2px solid #e5e7eb'
                      }}>{col}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {(result.data || []).slice(0, 200).map((row, i) => (
                    <tr key={i} style={{ background: i % 2 === 0 ? '#fff' : '#f9fafb' }}>
                      {(result.columns || []).map(col => (
                        <td key={col} style={{
                          padding: '7px 12px', color: '#374151',
                          borderBottom: '1px solid #f3f4f6', maxWidth: 300,
                          overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap'
                        }}>
                          {String(row[col] ?? '')}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
              {(result.data || []).length > 200 && (
                <div style={{ textAlign: 'center', padding: 12, color: '#94a3b8', fontSize: 13 }}>
                  Showing 200 of {result.row_count ?? 0} rows
                </div>
              )}
            </div>
          </>
        )}
        {!result && !error && !loading && (
          <div style={{ textAlign: 'center', padding: 60, color: '#94a3b8' }}>
            <Terminal size={48} color="#cbd5e1" style={{ display: 'block', margin: '0 auto 12px' }} />
            <div style={{ fontSize: 16, fontWeight: 500 }}>Ready to query</div>
            <div style={{ fontSize: 14, marginTop: 4 }}>Press Ctrl+Enter or click Run Query</div>
          </div>
        )}
      </div>
    </div>
  )
}

// Fix Terminal import
import { Terminal } from 'lucide-react'
export default QueryWorkbench
