// MetaMind API client — TypeScript SDK for the frontend
import axios, { AxiosInstance } from 'axios'

const API_BASE = import.meta.env.VITE_API_URL || ''

export interface ExecuteRequest {
  sql: string
  backend?: string
  timeout_seconds?: number
  dry_run?: boolean
}

export interface ExecuteResponse {
  query_id: string
  status: string
  routed_to: string
  execution_strategy: string
  freshness_seconds: number
  estimated_cost_ms: number
  confidence: number
  cache_hit: boolean
  execution_time_ms?: number
  row_count?: number
  columns?: string[]
  data?: Record<string, unknown>[]
  rewritten_sql?: string
  reason: string
}

export interface HealthResponse {
  status: string
  checks: Record<string, unknown>
  version: string
}

export interface NLQueryRequest {
  nl_text: string
  table_hints?: string[]
  execute?: boolean
}

export interface NLQueryResponse {
  nl_text: string
  generated_sql: string
  confidence: number
  execution_result?: {
    columns: string[]
    rows: Record<string, unknown>[]
    row_count: number
    duration_ms: number
  }
}

export interface FeatureFlags {
  [key: string]: boolean
}

export interface TableListResponse {
  tables: string[]
  count: number
}

class MetaMindAPI {
  private client: AxiosInstance
  private tenantId: string

  constructor(tenantId: string = 'default') {
    this.tenantId = tenantId
    this.client = axios.create({
      baseURL: API_BASE,
      headers: {
        'Content-Type': 'application/json',
        'X-Tenant-ID': tenantId,
      },
      timeout: 120_000,
    })

    // Response interceptor for error handling
    this.client.interceptors.response.use(
      (res) => res,
      (err) => {
        const detail = err.response?.data?.detail || err.message
        return Promise.reject(new Error(detail))
      }
    )
  }

  setToken(token: string): void {
    this.client.defaults.headers.common['Authorization'] = `Bearer ${token}`
  }

  setTenant(tenantId: string): void {
    this.tenantId = tenantId
    this.client.defaults.headers.common['X-Tenant-ID'] = tenantId
  }

  async executeQuery(req: ExecuteRequest): Promise<ExecuteResponse> {
    const { data } = await this.client.post<ExecuteResponse>('/api/v1/query', req)
    return data
  }

  async nlQuery(req: NLQueryRequest): Promise<NLQueryResponse> {
    const { data } = await this.client.post<NLQueryResponse>('/api/v1/nl/query', req)
    return data
  }

  async health(): Promise<HealthResponse> {
    const { data } = await this.client.get<HealthResponse>('/api/v1/health')
    return data
  }

  async listTables(schema?: string): Promise<TableListResponse> {
    const { data } = await this.client.get<TableListResponse>('/api/v1/tables/search', {
      params: schema ? { q: schema } : {},
    })
    return data
  }

  async registerTable(
    tableName: string,
    schemaName: string = 'public',
    backend: string = 'postgres',
    rowCount: number = 0
  ): Promise<{ table_id: number; message: string }> {
    const { data } = await this.client.post('/api/v1/tables', {
      table_name: tableName,
      schema_name: schemaName,
      backend,
      row_count: rowCount,
    })
    return data
  }

  async getFeatures(): Promise<{ tenant_id: string; flags: FeatureFlags }> {
    const { data } = await this.client.get('/api/v1/admin/feature-flags', {
      params: { tenant_id: this.tenantId },
    })
    return data
  }

  async setFeature(featureName: string, enabled: boolean): Promise<void> {
    await this.client.put('/api/v1/admin/feature-flags', null, {
      params: { tenant_id: this.tenantId, flag_name: featureName, is_enabled: enabled },
    })
  }

  async invalidateCache(tableName?: string): Promise<{ invalidated: number }> {
    const { data } = await this.client.post('/api/v1/cache/invalidate', null, {
      params: tableName ? { pattern: tableName } : {},
    })
    return data
  }

  async getCacheStats(): Promise<{
    hits: number
    misses: number
    hit_rate: number
    local_entries: number
  }> {
    const { data } = await this.client.get('/api/v1/cache/stats')
    return data
  }
}

// Singleton instance
export const api = new MetaMindAPI()
export { MetaMindAPI }
export default api
