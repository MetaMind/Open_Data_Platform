import React from 'react'
import { Zap } from 'lucide-react'

const PlanExplorer: React.FC = () => (
  <div style={{ padding: 24 }}>
    <div style={{ marginBottom: 24 }}>
      <h2 style={{ margin: 0, fontSize: 22, fontWeight: 700, color: '#0f172a', display: 'flex', alignItems: 'center', gap: 10 }}>
        <Zap size={22} color="#6366f1" />
        Plan Explorer
      </h2>
      <p style={{ margin: '4px 0 0', color: '#64748b', fontSize: 14 }}>
        F30 · Visualize and replay optimization decisions
      </p>
    </div>
    <div style={{ background: '#fff', borderRadius: 12, padding: 40, textAlign: 'center', boxShadow: '0 1px 3px rgba(0,0,0,0.1)' }}>
      <Zap size={48} color="#cbd5e1" style={{ display: 'block', margin: '0 auto 16px' }} />
      <div style={{ fontSize: 16, fontWeight: 500, color: '#374151' }}>No plan loaded</div>
      <p style={{ color: '#94a3b8', marginTop: 8 }}>
        Execute a query in the workbench with <code style={{ background: '#f1f5f9', padding: '2px 6px', borderRadius: 4 }}>dry_run: true</code> to see the plan tree here.
      </p>
    </div>
  </div>
)

export default PlanExplorer
