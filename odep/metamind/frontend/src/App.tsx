import React, { useState } from 'react'
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom'
import AppShell from './layout/AppShell'
import QueryWorkbench from './modules/QueryWorkbench/QueryWorkbench'
import PlanExplorer from './modules/PlanExplorer/PlanExplorer'
import MetricsDashboard from './modules/MetricsDashboard/MetricsDashboard'
import QueryHistory from './components/QueryHistory'
import NLQueryInterface from './modules/NLQueryInterface/NLQueryInterface'
import AdminPanel from './components/AdminPanel'

export interface AppState {
  tenantId: string
  lastQuery: string
  lastResult: unknown
}

const App: React.FC = () => {
  const [tenantId, setTenantId] = useState<string>(
    localStorage.getItem('metamind_tenant') || 'default'
  )

  const handleTenantChange = (id: string) => {
    setTenantId(id)
    localStorage.setItem('metamind_tenant', id)
  }

  return (
    <BrowserRouter>
      <AppShell tenantId={tenantId} onTenantChange={handleTenantChange}>
        <Routes>
          <Route path="/" element={<Navigate to="/workbench" replace />} />
          <Route path="/workbench" element={<QueryWorkbench tenantId={tenantId} />} />
          <Route path="/plan" element={<PlanExplorer />} />
          <Route path="/metrics" element={<MetricsDashboard tenantId={tenantId} />} />
          <Route path="/history" element={<QueryHistory tenantId={tenantId} />} />
          <Route path="/nl" element={<NLQueryInterface tenantId={tenantId} />} />
          <Route path="/admin" element={<AdminPanel tenantId={tenantId} />} />
          <Route path="*" element={<Navigate to="/workbench" replace />} />
        </Routes>
      </AppShell>
    </BrowserRouter>
  )
}

export default App
