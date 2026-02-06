/**
 * SkypyDB Dashboard API Client
 */

import {
  HealthStatus,
  TableInfo,
  TableSchema,
  PaginatedTableData,
  SearchResult,
  VectorCollectionInfo,
  VectorCollectionDetails,
  PaginatedVectorDocuments,
  VectorSearchResponse,
  DatabaseStats,
  DashboardSummary,
} from '@/types'

const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000/api'

// Get database paths from localStorage or use defaults
function getDbConfig() {
  if (typeof window === 'undefined') {
    return {
      mainPath: './db/_generated/skypydb.db',
      vectorPath: './db/_generated/vector.db',
    }
  }
  
  return {
    mainPath: localStorage.getItem('SKYPYDB_PATH') || './db/_generated/skypydb.db',
    vectorPath: localStorage.getItem('SKYPYDB_VECTOR_PATH') || './db/_generated/vector.db',
  }
}

class APIError extends Error {
  constructor(
    message: string,
    public status?: number,
    public isConnectionError: boolean = false
  ) {
    super(message)
    this.name = 'APIError'
  }
}

async function fetchAPI<T>(endpoint: string, options?: RequestInit): Promise<T> {
  const config = getDbConfig()
  
  try {
    const response = await fetch(`${API_BASE_URL}${endpoint}`, {
      headers: {
        'Content-Type': 'application/json',
        'X-SkypyDB-Path': config.mainPath,
        'X-SkypyDB-Vector-Path': config.vectorPath,
      },
      ...options,
    })

    if (!response.ok) {
      throw new APIError(
        `API error: ${response.status} ${response.statusText}`,
        response.status
      )
    }

    return response.json()
  } catch (error) {
    if (error instanceof APIError) {
      throw error
    }
    
    if (error instanceof TypeError && error.message.includes('fetch')) {
      throw new APIError(
        'Cannot connect to API server. Please ensure the SkypyDB backend is running on port 8000.',
        undefined,
        true
      )
    }
    
    throw new APIError(error instanceof Error ? error.message : 'Unknown error')
  }
}

export async function checkAPIConnection(): Promise<boolean> {
  try {
    const response = await fetch(`${API_BASE_URL}/health`, {
      method: 'GET',
      headers: { 'Content-Type': 'application/json' },
    })
    return response.ok
  } catch {
    return false
  }
}

export async function checkHealth(): Promise<HealthStatus> {
  return fetchAPI<HealthStatus>('/health')
}

export async function listTables(): Promise<TableInfo[]> {
  return fetchAPI<TableInfo[]>('/tables')
}

export async function getTableSchema(tableName: string): Promise<TableSchema> {
  return fetchAPI<TableSchema>(`/tables/${tableName}/schema`)
}

export async function getTableData(
  tableName: string,
  limit: number = 100,
  offset: number = 0
): Promise<PaginatedTableData> {
  return fetchAPI<PaginatedTableData>(
    `/tables/${tableName}/data?limit=${limit}&offset=${offset}`
  )
}

export async function searchTable(
  tableName: string,
  query?: string,
  limit: number = 100,
  filters?: Record<string, string>
): Promise<SearchResult> {
  const params = new URLSearchParams()
  if (query) params.append('query', query)
  params.append('limit', limit.toString())
  
  if (filters) {
    Object.entries(filters).forEach(([key, value]) => {
      params.append(key, value)
    })
  }

  return fetchAPI<SearchResult>(`/tables/${tableName}/search?${params.toString()}`)
}

export async function listCollections(): Promise<VectorCollectionInfo[]> {
  return fetchAPI<VectorCollectionInfo[]>('/collections')
}

export async function getCollectionDetails(
  collectionName: string
): Promise<VectorCollectionDetails> {
  return fetchAPI<VectorCollectionDetails>(`/collections/${collectionName}`)
}

export async function getCollectionDocuments(
  collectionName: string,
  limit: number = 100,
  offset: number = 0,
  documentIds?: string[],
  metadataFilter?: Record<string, unknown>
): Promise<PaginatedVectorDocuments> {
  const body: Record<string, unknown> = { limit, offset }
  if (documentIds) body.document_ids = documentIds
  if (metadataFilter) body.metadata_filter = metadataFilter

  return fetchAPI<PaginatedVectorDocuments>(`/collections/${collectionName}/documents`, {
    method: 'POST',
    body: JSON.stringify(body),
  })
}

export async function searchVectors(
  collectionName: string,
  queryText: string,
  nResults: number = 10,
  metadataFilter?: Record<string, unknown>
): Promise<VectorSearchResponse> {
  const body: Record<string, unknown> = {
    query_text: queryText,
    n_results: nResults,
  }
  if (metadataFilter) body.metadata_filter = metadataFilter

  return fetchAPI<VectorSearchResponse>(`/collections/${collectionName}/search`, {
    method: 'POST',
    body: JSON.stringify(body),
  })
}

export async function getStatistics(): Promise<DatabaseStats> {
  return fetchAPI<DatabaseStats>('/statistics')
}

export async function getSummary(): Promise<DashboardSummary> {
  return fetchAPI<DashboardSummary>('/summary')
}

export { APIError }
