/**
 * AdminPanel — Tenant Management & Platform Configuration
 *
 * File: frontend/src/components/AdminPanel.tsx
 *
 * Tabbed admin interface covering: Tenants, Routing Policies,
 * Feature Flags (F01–F30), and System Health.
 */

import React, { useEffect, useState, useCallback } from "react";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface Tenant {
  tenant_id: string;
  tenant_name: string;
  is_active: boolean;
  max_query_rate_per_minute: number | null;
  max_concurrent_queries: number | null;
  max_result_rows: number | null;
}

interface RoutingPolicy {
  policy_id: string;
  policy_name: string;
  description: string;
  priority: number;
  target_engine: string;
  is_active: boolean;
}

interface FeatureFlagStatus {
  enabled: boolean;
  updated_at: string;
}

interface EngineHealthCard {
  engine: string;
  status: string;
  latency_ms: number | null;
}

type TabKey = "tenants" | "policies" | "flags" | "health";

const BASE_URL = "/api/v1";

// ---------------------------------------------------------------------------
// Utility
// ---------------------------------------------------------------------------

const badge = (ok: boolean) => ok
  ? <span className="px-2 py-0.5 rounded text-xs bg-green-100 text-green-700">Active</span>
  : <span className="px-2 py-0.5 rounded text-xs bg-red-100 text-red-700">Inactive</span>;

// ---------------------------------------------------------------------------
// Tabs
// ---------------------------------------------------------------------------

const TABS: { key: TabKey; label: string }[] = [
  { key: "tenants",  label: "Tenants" },
  { key: "policies", label: "Routing Policies" },
  { key: "flags",    label: "Feature Flags" },
  { key: "health",   label: "System Health" },
];

// ---------------------------------------------------------------------------
// TenantsTab
// ---------------------------------------------------------------------------

const TenantsTab: React.FC<{ tenants: Tenant[]; onUpdate: () => void }> = ({ tenants, onUpdate }) => {
  const [editId, setEditId] = useState<string | null>(null);
  const [concur, setConcur] = useState<number>(10);
  const [rate, setRate] = useState<number>(100);
  const [saving, setSaving] = useState(false);

  const save = async (tenantId: string) => {
    setSaving(true);
    try {
      await fetch(`${BASE_URL}/admin/tenants/${tenantId}/quota`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ max_concurrent_queries: concur, max_query_rate_per_minute: rate }),
      });
      setEditId(null);
      onUpdate();
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="overflow-auto">
      <table className="w-full text-sm">
        <thead className="bg-gray-50 border-b">
          <tr>
            {["Tenant", "Status", "Rate Limit", "Concurrency", "Max Rows", "Actions"].map((h) => (
              <th key={h} className="text-left px-4 py-2 text-gray-600 font-medium">{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {tenants.map((t) => (
            <tr key={t.tenant_id} className="border-b hover:bg-gray-50">
              <td className="px-4 py-3 font-medium">{t.tenant_name ?? t.tenant_id}</td>
              <td className="px-4 py-3">{badge(t.is_active)}</td>
              <td className="px-4 py-3">{t.max_query_rate_per_minute ?? "—"}/min</td>
              <td className="px-4 py-3">{t.max_concurrent_queries ?? "—"}</td>
              <td className="px-4 py-3">{t.max_result_rows?.toLocaleString() ?? "—"}</td>
              <td className="px-4 py-3">
                {editId === t.tenant_id ? (
                  <div className="flex gap-2 items-center flex-wrap">
                    <input
                      type="number" value={rate} onChange={(e) => setRate(+e.target.value)}
                      className="w-20 border rounded px-1 py-0.5 text-xs" placeholder="rate"
                    />
                    <input
                      type="number" value={concur} onChange={(e) => setConcur(+e.target.value)}
                      className="w-16 border rounded px-1 py-0.5 text-xs" placeholder="concur"
                    />
                    <button
                      onClick={() => save(t.tenant_id)} disabled={saving}
                      className="px-2 py-0.5 bg-blue-600 text-white rounded text-xs"
                    >
                      {saving ? "…" : "Save"}
                    </button>
                    <button onClick={() => setEditId(null)} className="text-xs text-gray-500">Cancel</button>
                  </div>
                ) : (
                  <button
                    onClick={() => { setEditId(t.tenant_id); setRate(t.max_query_rate_per_minute ?? 100); setConcur(t.max_concurrent_queries ?? 10); }}
                    className="text-blue-600 hover:underline text-xs"
                  >
                    Edit Quota
                  </button>
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};

// ---------------------------------------------------------------------------
// PoliciesTab
// ---------------------------------------------------------------------------

const PoliciesTab: React.FC<{ tenantId: string }> = ({ tenantId }) => {
  const [policies, setPolicies] = useState<RoutingPolicy[]>([]);
  const [newName, setNewName] = useState("");
  const [newEngine, setNewEngine] = useState("trino");
  const [newPriority, setNewPriority] = useState(50);
  const [loading, setLoading] = useState(false);

  const load = useCallback(async () => {
    const res = await fetch(`${BASE_URL}/admin/policies?tenant_id=${tenantId}`);
    if (res.ok) setPolicies(await res.json());
  }, [tenantId]);

  useEffect(() => { load(); }, [load]);

  const create = async () => {
    if (!newName.trim()) return;
    setLoading(true);
    try {
      await fetch(`${BASE_URL}/admin/policies?tenant_id=${tenantId}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ policy_name: newName, target_engine: newEngine, priority: newPriority }),
      });
      setNewName(""); load();
    } finally { setLoading(false); }
  };

  const deactivate = async (id: string) => {
    await fetch(`${BASE_URL}/admin/policies/${id}?tenant_id=${tenantId}`, { method: "DELETE" });
    load();
  };

  return (
    <div className="space-y-4">
      <div className="flex gap-2 items-end flex-wrap bg-gray-50 p-4 rounded">
        <div>
          <label className="text-xs text-gray-600 block mb-1">Policy Name</label>
          <input value={newName} onChange={(e) => setNewName(e.target.value)}
            className="border rounded px-2 py-1 text-sm w-44" placeholder="e.g. orders_to_trino" />
        </div>
        <div>
          <label className="text-xs text-gray-600 block mb-1">Engine</label>
          <select value={newEngine} onChange={(e) => setNewEngine(e.target.value)}
            className="border rounded px-2 py-1 text-sm">
            {["oracle", "trino", "spark", "gpu"].map((e) => <option key={e}>{e}</option>)}
          </select>
        </div>
        <div>
          <label className="text-xs text-gray-600 block mb-1">Priority (1–100)</label>
          <input type="number" value={newPriority} min={1} max={100}
            onChange={(e) => setNewPriority(+e.target.value)}
            className="border rounded px-2 py-1 text-sm w-20" />
        </div>
        <button onClick={create} disabled={loading}
          className="px-4 py-1.5 bg-blue-600 text-white rounded text-sm">
          {loading ? "Adding…" : "Add Policy"}
        </button>
      </div>

      <table className="w-full text-sm">
        <thead className="bg-gray-50 border-b">
          <tr>
            {["Name", "Engine", "Priority", "Status", ""].map((h) => (
              <th key={h} className="text-left px-4 py-2 text-gray-600">{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {policies.map((p) => (
            <tr key={p.policy_id} className="border-b hover:bg-gray-50">
              <td className="px-4 py-2 font-medium">{p.policy_name}</td>
              <td className="px-4 py-2">{p.target_engine}</td>
              <td className="px-4 py-2">{p.priority}</td>
              <td className="px-4 py-2">{badge(p.is_active)}</td>
              <td className="px-4 py-2">
                {p.is_active && (
                  <button onClick={() => deactivate(p.policy_id)}
                    className="text-red-600 hover:underline text-xs">Deactivate</button>
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};

// ---------------------------------------------------------------------------
// FeatureFlagsTab
// ---------------------------------------------------------------------------

const FeatureFlagsTab: React.FC<{ tenantId: string }> = ({ tenantId }) => {
  const [flags, setFlags] = useState<Record<string, FeatureFlagStatus>>({});
  const [saving, setSaving] = useState<string | null>(null);

  const load = useCallback(async () => {
    const res = await fetch(`${BASE_URL}/admin/feature-flags?tenant_id=${tenantId}`);
    if (res.ok) { const d = await res.json(); setFlags(d.flags ?? {}); }
  }, [tenantId]);

  useEffect(() => { load(); }, [load]);

  const toggle = async (flagName: string, current: boolean) => {
    setSaving(flagName);
    try {
      await fetch(`${BASE_URL}/admin/feature-flags`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ tenant_id: tenantId, flag_name: flagName, enabled: !current }),
      });
      load();
    } finally { setSaving(null); }
  };

  const sortedFlags = Object.entries(flags).sort(([a], [b]) => a.localeCompare(b));

  return (
    <div className="grid grid-cols-2 md:grid-cols-3 gap-3">
      {sortedFlags.map(([name, status]) => (
        <div key={name} className="flex items-center justify-between bg-gray-50 rounded px-3 py-2 border">
          <span className="text-sm font-mono text-gray-700">{name}</span>
          <button
            onClick={() => toggle(name, status.enabled)}
            disabled={saving === name}
            className={`ml-2 relative inline-flex h-5 w-10 rounded-full transition-colors duration-200 ${
              status.enabled ? "bg-blue-600" : "bg-gray-300"
            }`}
          >
            <span className={`inline-block h-4 w-4 rounded-full bg-white shadow transform transition-transform duration-200 mt-0.5 ${
              status.enabled ? "translate-x-5" : "translate-x-0.5"
            }`} />
          </button>
        </div>
      ))}
      {sortedFlags.length === 0 && (
        <p className="text-gray-400 col-span-3 text-sm">No feature flags configured for this tenant.</p>
      )}
    </div>
  );
};

// ---------------------------------------------------------------------------
// SystemHealthTab
// ---------------------------------------------------------------------------

const SystemHealthTab: React.FC = () => {
  const [health, setHealth] = useState<Record<string, any>>({});
  const [cdcLag, setCdcLag] = useState<number | null>(null);
  const [redisStats, setRedisStats] = useState<Record<string, any>>({});

  useEffect(() => {
    Promise.all([
      fetch(`${BASE_URL}/health`).then((r) => r.json()).catch(() => ({})),
      fetch(`${BASE_URL}/cdc/status?tenant_id=default`).then((r) => r.json()).catch(() => ({})),
      fetch(`${BASE_URL}/cache/stats`).then((r) => r.json()).catch(() => ({})),
    ]).then(([h, cdc, cache]) => {
      setHealth(h);
      setCdcLag(cdc?.max_lag_seconds ?? null);
      setRedisStats(cache);
    });
  }, []);

  const engines = ["oracle", "trino", "spark", "gpu"];
  const statusColor = (s: string) =>
    s === "healthy" ? "bg-green-500" : s === "degraded" ? "bg-yellow-400" : "bg-red-500";

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        {engines.map((e) => {
          const eng = health?.engines?.[e] ?? {};
          const st = eng.status ?? "unknown";
          return (
            <div key={e} className="bg-white border rounded-lg p-4 shadow-sm">
              <div className="flex items-center gap-2 mb-2">
                <span className={`w-3 h-3 rounded-full ${statusColor(st)}`} />
                <span className="font-semibold capitalize">{e}</span>
              </div>
              <p className="text-xs text-gray-500 capitalize">{st}</p>
              {eng.latency_ms != null && (
                <p className="text-xs text-gray-400">{eng.latency_ms.toFixed(0)} ms</p>
              )}
            </div>
          );
        })}
      </div>

      {cdcLag !== null && (
        <div className="bg-white border rounded-lg p-4 shadow-sm">
          <h4 className="font-semibold text-sm mb-2">CDC Lag</h4>
          <div className="flex items-center gap-4">
            <span className={`text-2xl font-bold ${cdcLag < 300 ? "text-green-600" : "text-red-600"}`}>
              {cdcLag}s
            </span>
            <span className="text-xs text-gray-500">Target: &lt;300s</span>
          </div>
        </div>
      )}

      {redisStats?.hit_ratio != null && (
        <div className="bg-white border rounded-lg p-4 shadow-sm">
          <h4 className="font-semibold text-sm mb-2">Redis Cache</h4>
          <div className="flex gap-6 text-sm">
            <span>Hit ratio: <strong>{(redisStats.hit_ratio * 100).toFixed(1)}%</strong></span>
            {redisStats.memory_used_mb != null && (
              <span>Memory: <strong>{redisStats.memory_used_mb.toFixed(0)} MB</strong></span>
            )}
          </div>
        </div>
      )}
    </div>
  );
};

// ---------------------------------------------------------------------------
// AdminPanel (root)
// ---------------------------------------------------------------------------

interface AdminPanelProps {
  tenantId?: string;
}

const AdminPanel: React.FC<AdminPanelProps> = ({ tenantId = "default" }) => {
  const [activeTab, setActiveTab] = useState<TabKey>("tenants");
  const [tenants, setTenants] = useState<Tenant[]>([]);

  const loadTenants = useCallback(async () => {
    const res = await fetch(`${BASE_URL}/admin/tenants`);
    if (res.ok) setTenants(await res.json());
  }, []);

  useEffect(() => { loadTenants(); }, [loadTenants]);

  return (
    <div className="min-h-screen bg-gray-100">
      <div className="max-w-7xl mx-auto px-6 py-8">
        <h1 className="text-2xl font-bold text-gray-900 mb-6">MetaMind Admin Panel</h1>

        {/* Tab bar */}
        <div className="flex border-b mb-6">
          {TABS.map((tab) => (
            <button
              key={tab.key}
              onClick={() => setActiveTab(tab.key)}
              className={`px-5 py-2.5 text-sm font-medium border-b-2 -mb-px transition-colors ${
                activeTab === tab.key
                  ? "border-blue-600 text-blue-600"
                  : "border-transparent text-gray-500 hover:text-gray-700"
              }`}
            >
              {tab.label}
            </button>
          ))}
        </div>

        {/* Tab content */}
        <div className="bg-white shadow rounded-lg p-6">
          {activeTab === "tenants" && (
            <TenantsTab tenants={tenants} onUpdate={loadTenants} />
          )}
          {activeTab === "policies" && <PoliciesTab tenantId={tenantId} />}
          {activeTab === "flags" && <FeatureFlagsTab tenantId={tenantId} />}
          {activeTab === "health" && <SystemHealthTab />}
        </div>
      </div>
    </div>
  );
};

export default AdminPanel;
