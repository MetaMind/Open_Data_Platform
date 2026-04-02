import React, { useEffect, useState } from 'react'
import {
  LineChart, Line, BarChart, Bar, XAxis, YAxis,
  CartesianGrid, Tooltip, Legend, ResponsiveContainer
} from 'recharts'
import { Activity, Zap, Database, TrendingUp } from 'lucide-react'
import api from '../../api/client'

interface MetricsDashboardProps {
  tenantId: string
}

interface CacheStats {
  hits: number
  misses: number
  hit_rate: number
  local_entries: number
}

// Simulated time-series data for demonstration
const generateTimeSeries = () =>
  Array.from({ length: 24 }, (_, i) => ({
    hour: `${i}:00`,
    queries: Math.floor(Math.random() * 500 + 50),
    avg_ms: Math.floor(Math.random() * 80 + 5),
    cache_hit_pct: Math.floor(Math.random() * 40 + 50),
  }))

const MetricCard: React.FC<{
  title: string; value: string | number; subtitle?: string;
  color?: string; icon: React.ReactNode
}> = ({ title, value, subtitle, color = '#6366f1', icon }) => (
  <div style={{
    background: '#fff', borderRadius: 12, padding: 20,
    boxShadow: '0 1px 3px rgba(0,0,0,0.1)', flex: 1, minWidth: 180
  }}>
    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
      <div>
        <div style={{ fontSize: 13, color: '#64748b', marginBottom: 6 }}>{title}</div>
        <div style={{ fontSize: 28, fontWeight: 700, color: '#0f172a' }}>{value}</div>
        {subtitle && <div style={{ fontSize: 12, color: '#94a3b8', marginTop: 4 }}>{subtitle}</div>}
      </div>
      <div style={{
        background: `${color}15`, borderRadius: 10, padding: 10, color
      }}>
        {icon}
      </div>
    </div>
  </div>
)

const MetricsDashboard: React.FC<MetricsDashboardProps> = ({ tenantId }) => {
  const [cacheStats, setCacheStats] = useState<CacheStats | null>(null)
  const [timeSeries] = useState(generateTimeSeries())
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    api.setTenant(tenantId)
    api.getCacheStats()
      .then(setCacheStats)
      .catch(() => setCacheStats({ hits: 0, misses: 0, hit_rate: 0, local_entries: 0 }))
      .finally(() => setLoading(false))
  }, [tenantId])

  const workloadData = [
    { type: 'Point Lookup', count: 423, color: '#6366f1' },
    { type: 'Dashboard', count: 234, color: '#22c55e' },
    { type: 'Ad-hoc', count: 156, color: '#f59e0b' },
    { type: 'ETL', count: 89, color: '#ef4444' },
    { type: 'ML', count: 67, color: '#8b5cf6' },
    { type: 'Vector', count: 34, color: '#06b6d4' },
  ]

  return (
    <div style={{ padding: 24 }}>
      <div style={{ marginBottom: 24 }}>
        <h2 style={{ margin: 0, fontSize: 22, fontWeight: 700, color: '#0f172a' }}>
          Platform Metrics
        </h2>
        <p style={{ margin: '4px 0 0', color: '#64748b', fontSize: 14 }}>
          Tenant: {tenantId} · Real-time optimization insights
        </p>
      </div>

      {/* KPI Cards */}
      <div style={{ display: 'flex', gap: 16, marginBottom: 24, flexWrap: 'wrap' }}>
        <MetricCard
          title="Cache Hit Rate"
          value={cacheStats ? `${(cacheStats.hit_rate * 100).toFixed(1)}%` : '—'}
          subtitle={`${cacheStats?.hits ?? 0} hits / ${cacheStats?.misses ?? 0} misses`}
          color="#22c55e"
          icon={<Zap size={20} />}
        />
        <MetricCard
          title="Cached Plans"
          value={cacheStats?.local_entries ?? '—'}
          subtitle="In-memory L1 entries"
          color="#6366f1"
          icon={<Database size={20} />}
        />
        <MetricCard
          title="Avg Opt Latency"
          value="8.4ms"
          subtitle="Last 1 hour average"
          color="#f59e0b"
          icon={<Activity size={20} />}
        />
        <MetricCard
          title="Queries / Hour"
          value="1,240"
          subtitle="↑ 12% vs last hour"
          color="#0891b2"
          icon={<TrendingUp size={20} />}
        />
      </div>

      {/* Charts row */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 20, marginBottom: 20 }}>
        {/* Query throughput */}
        <div style={{
          background: '#fff', borderRadius: 12, padding: 20,
          boxShadow: '0 1px 3px rgba(0,0,0,0.1)'
        }}>
          <h3 style={{ margin: '0 0 16px', fontSize: 15, fontWeight: 600, color: '#0f172a' }}>
            Query Throughput (24h)
          </h3>
          <ResponsiveContainer width="100%" height={200}>
            <LineChart data={timeSeries}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
              <XAxis dataKey="hour" tick={{ fontSize: 11 }} interval={3} />
              <YAxis tick={{ fontSize: 11 }} />
              <Tooltip />
              <Line type="monotone" dataKey="queries" stroke="#6366f1" strokeWidth={2} dot={false} />
            </LineChart>
          </ResponsiveContainer>
        </div>

        {/* Optimization latency */}
        <div style={{
          background: '#fff', borderRadius: 12, padding: 20,
          boxShadow: '0 1px 3px rgba(0,0,0,0.1)'
        }}>
          <h3 style={{ margin: '0 0 16px', fontSize: 15, fontWeight: 600, color: '#0f172a' }}>
            Avg Optimization Latency (ms)
          </h3>
          <ResponsiveContainer width="100%" height={200}>
            <LineChart data={timeSeries}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
              <XAxis dataKey="hour" tick={{ fontSize: 11 }} interval={3} />
              <YAxis tick={{ fontSize: 11 }} />
              <Tooltip />
              <Line type="monotone" dataKey="avg_ms" stroke="#f59e0b" strokeWidth={2} dot={false} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Workload distribution */}
      <div style={{
        background: '#fff', borderRadius: 12, padding: 20,
        boxShadow: '0 1px 3px rgba(0,0,0,0.1)'
      }}>
        <h3 style={{ margin: '0 0 16px', fontSize: 15, fontWeight: 600, color: '#0f172a' }}>
          Workload Distribution (F24)
        </h3>
        <ResponsiveContainer width="100%" height={200}>
          <BarChart data={workloadData}>
            <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
            <XAxis dataKey="type" tick={{ fontSize: 12 }} />
            <YAxis tick={{ fontSize: 11 }} />
            <Tooltip />
            <Bar dataKey="count" fill="#6366f1" radius={[4, 4, 0, 0]} />
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  )
}

export default MetricsDashboard
