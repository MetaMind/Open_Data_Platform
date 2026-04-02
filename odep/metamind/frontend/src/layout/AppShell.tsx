import React from 'react'
import { NavLink } from 'react-router-dom'
import {
  Terminal, BarChart2, History, MessageSquare,
  Settings, Activity, Database, Zap
} from 'lucide-react'

interface AppShellProps {
  tenantId: string
  onTenantChange: (id: string) => void
  children: React.ReactNode
}

const navItems = [
  { to: '/workbench', icon: Terminal, label: 'Query Workbench' },
  { to: '/plan', icon: Zap, label: 'Plan Explorer' },
  { to: '/metrics', icon: BarChart2, label: 'Metrics' },
  { to: '/history', icon: History, label: 'Query History' },
  { to: '/nl', icon: MessageSquare, label: 'NL Interface' },
  { to: '/admin', icon: Settings, label: 'Admin' },
]

const AppShell: React.FC<AppShellProps> = ({ tenantId, onTenantChange, children }) => {
  return (
    <div style={{ display: 'flex', height: '100vh', fontFamily: 'Inter, system-ui, sans-serif' }}>
      {/* Sidebar */}
      <nav style={{
        width: 240, background: '#0f172a', color: '#e2e8f0',
        display: 'flex', flexDirection: 'column', padding: '0',
        boxShadow: '2px 0 8px rgba(0,0,0,0.3)',
      }}>
        {/* Logo */}
        <div style={{
          padding: '20px 16px', borderBottom: '1px solid #1e293b',
          display: 'flex', alignItems: 'center', gap: 10
        }}>
          <Database size={24} color="#6366f1" />
          <div>
            <div style={{ fontWeight: 700, fontSize: 16, color: '#f1f5f9' }}>MetaMind</div>
            <div style={{ fontSize: 11, color: '#64748b' }}>Query Intelligence v3.0</div>
          </div>
        </div>

        {/* Tenant selector */}
        <div style={{ padding: '12px 16px', borderBottom: '1px solid #1e293b' }}>
          <label style={{ fontSize: 11, color: '#64748b', display: 'block', marginBottom: 4 }}>
            TENANT
          </label>
          <input
            value={tenantId}
            onChange={(e) => onTenantChange(e.target.value)}
            style={{
              width: '100%', background: '#1e293b', border: '1px solid #334155',
              borderRadius: 6, padding: '6px 8px', color: '#e2e8f0', fontSize: 13,
              boxSizing: 'border-box'
            }}
          />
        </div>

        {/* Nav items */}
        <div style={{ flex: 1, padding: '8px 0' }}>
          {navItems.map(({ to, icon: Icon, label }) => (
            <NavLink key={to} to={to} style={({ isActive }) => ({
              display: 'flex', alignItems: 'center', gap: 10,
              padding: '10px 16px', textDecoration: 'none',
              color: isActive ? '#6366f1' : '#94a3b8',
              background: isActive ? '#1e293b' : 'transparent',
              borderLeft: isActive ? '3px solid #6366f1' : '3px solid transparent',
              fontSize: 14, fontWeight: isActive ? 600 : 400,
              transition: 'all 0.15s',
            })}>
              <Icon size={16} />
              {label}
            </NavLink>
          ))}
        </div>

        {/* Footer */}
        <div style={{ padding: '12px 16px', borderTop: '1px solid #1e293b' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 12, color: '#475569' }}>
            <Activity size={12} color="#22c55e" />
            Platform Online
          </div>
        </div>
      </nav>

      {/* Main content */}
      <main style={{ flex: 1, overflow: 'auto', background: '#f8fafc' }}>
        {children}
      </main>
    </div>
  )
}

export default AppShell
