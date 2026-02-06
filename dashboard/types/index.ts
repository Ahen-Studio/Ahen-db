/**
 * SkypyDB Dashboard API Types
 */

// Health Types
export interface HealthStatus {
  timestamp: number
  status: 'healthy' | 'degraded' | 'unhealthy'
  databases: {
    main?: {
      status: 'connected' | 'error'
      tables?: number
      error?: string
    }
    vector?: {
      status: 'connected' | 'error'
      collections?: number
      error?: string
    }
  }
}

// Table Types
export interface TableInfo {
  name: string
  row_count: number
  columns: string[]
  config: Record<string, unknown> | null
}

export interface TableSchema {
  name: string
  columns: string[]
  config: Record<string, unknown> | null
}

export interface PaginatedTableData {
  data: Record<string, unknown>[]
  total: number
  limit: number
  offset: number
  has_more: boolean
}

export interface SearchResult {
  data: Record<string, unknown>[]
  total: number
  limit: number
}

// Vector Collection Types
export interface VectorCollectionInfo {
  name: string
  document_count: number
  metadata: Record<string, unknown>
}

export interface VectorCollectionDetails {
  name: string
  exists: boolean
  document_count?: number
  metadata?: Record<string, unknown>
  error?: string
}

export interface PaginatedVectorDocuments {
  ids: string[]
  documents: (string | null)[]
  metadatas: (Record<string, unknown> | null)[]
  total: number
  limit: number
  offset: number
  has_more: boolean
}

export interface VectorSearchResult {
  id: string
  document: string | null
  metadata: Record<string, unknown> | null
  similarity_score: number | null
}

export interface VectorSearchResponse {
  results: VectorSearchResult[]
  query: string
  n_results: number
  error?: string
}

// Statistics Types
export interface DatabaseStats {
  timestamp: number
  tables: {
    count: number
    total_rows: number
    error?: string
  }
  collections: {
    count: number
    total_documents: number
    error?: string
  }
}

export interface DashboardSummary {
  status: 'healthy' | 'degraded' | 'unhealthy'
  timestamp: number
  summary: {
    tables: DatabaseStats['tables']
    collections: DatabaseStats['collections']
  }
  health_details: HealthStatus['databases']
}
