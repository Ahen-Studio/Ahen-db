"use client"

import React, { createContext, useContext, useState, useEffect, useCallback, ReactNode } from "react"
import { getSummary, checkHealth, checkAPIConnection, APIError } from "@/lib/api"
import { DashboardSummary, HealthStatus } from "@/types"

interface DashboardContextType {
  summary: DashboardSummary | null
  health: HealthStatus | null
  loading: boolean
  connectionError: string | null
  refresh: () => void
}

const DashboardContext = createContext<DashboardContextType | undefined>(undefined)

export function DashboardProvider({ children }: { children: ReactNode }) {
  const [summary, setSummary] = useState<DashboardSummary | null>(null)
  const [health, setHealth] = useState<HealthStatus | null>(null)
  const [loading, setLoading] = useState(true)
  const [connectionError, setConnectionError] = useState<string | null>(null)

  const loadData = useCallback(async (silent: boolean = false) => {
    try {
      if (!silent) {
        setLoading(true)
      }
      setConnectionError(null)
      
      const isConnected = await checkAPIConnection()
      if (!isConnected) {
        setConnectionError("API server is not running. Please start the backend server on port 8000.")
        return
      }
      
      const [summaryData, healthData] = await Promise.all([
        getSummary(),
        checkHealth(),
      ])
      setSummary(summaryData)
      setHealth(healthData)
    } catch (error) {
      if (error instanceof APIError && error.isConnectionError) {
        setConnectionError(error.message)
      }
      console.error(error)
    } finally {
      if (!silent) {
        setLoading(false)
      }
    }
  }, [])

  const refresh = useCallback(() => {
    loadData(false)
  }, [loadData])

  useEffect(() => {
    // Initial load
    loadData(false)
    
    // Auto-refresh every 2 seconds
    const interval = setInterval(() => {
      loadData(true)
    }, 2000)
    
    return () => clearInterval(interval)
  }, [loadData])

  return (
    <DashboardContext.Provider value={{ summary, health, loading, connectionError, refresh }}>
      {children}
    </DashboardContext.Provider>
  )
}

export function useDashboard() {
  const context = useContext(DashboardContext)
  if (context === undefined) {
    throw new Error("useDashboard must be used within a DashboardProvider")
  }
  return context
}
