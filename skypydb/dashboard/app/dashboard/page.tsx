"use client"

import { AppSidebar } from "@/components/app-sidebar"
import { SiteHeader } from "@/components/site-header"
import { useDashboard } from "@/components/dashboard-provider"
import {
  SidebarInset,
  SidebarProvider,
} from "@/components/ui/sidebar"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Skeleton } from "@/components/ui/skeleton"
import { Button } from "@/components/ui/button"
import {
  IconTable,
  IconDatabase,
  IconActivity,
  IconCircleCheck,
  IconAlertCircle,
  IconRefresh,
  IconServerOff,
  IconChartBar,
} from "@tabler/icons-react"
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Cell } from "recharts"

export default function DashboardPage() {
  const { summary, health, loading, connectionError, refresh } = useDashboard()

  const getStatusIcon = (status: string) => {
    if (status === "healthy" || status === "connected") {
      return <IconCircleCheck className="h-4 w-4 text-green-500" />
    }
    return <IconAlertCircle className="h-4 w-4 text-red-500" />
  }

  const getStatusColor = (status: string) => {
    if (status === "healthy" || status === "connected") {
      return "bg-green-500/10 text-green-500 border-green-500/20"
    }
    return "bg-red-500/10 text-red-500 border-red-500/20"
  }

  const chartData = [
    {
      name: "Tables",
      count: summary?.summary.tables.count || 0,
      color: "#FFD700",
    },
    {
      name: "Collections",
      count: summary?.summary.collections.count || 0,
      color: "#FF4500",
    },
  ]

  return (
    <SidebarProvider
      style={
        {
          "--sidebar-width": "calc(var(--spacing) * 72)",
          "--header-height": "calc(var(--spacing) * 12)",
        } as React.CSSProperties
      }
    >
      <AppSidebar variant="inset" />
      <SidebarInset>
        <SiteHeader title="Dashboard" />
        <div className="flex flex-1 flex-col" suppressHydrationWarning>
          <div className="@container/main flex flex-1 flex-col gap-2" suppressHydrationWarning>
            <div className="flex flex-col gap-4 py-4 md:gap-6 md:py-6" suppressHydrationWarning>
              {connectionError && (
                <div className="px-4 lg:px-6">
                  <Card className="border-red-500/50 bg-red-500/10">
                    <CardHeader>
                      <CardTitle className="flex items-center gap-2 text-red-500">
                        <IconServerOff className="h-5 w-5" />
                        Connection Error
                      </CardTitle>
                      <CardDescription className="text-red-500/80">
                        {connectionError}
                      </CardDescription>
                    </CardHeader>
                    <CardContent>
                      <div className="text-sm text-red-500/80 mb-4">
                        To start the API server, run:<br/>
                        <code className="bg-red-500/20 px-2 py-1 rounded mt-1 inline-block">
                          python skypydb/api/server.py
                        </code>
                      </div>
                      <Button onClick={refresh} variant="outline" className="border-red-500/50">
                        <IconRefresh className="mr-2 h-4 w-4" />
                        Retry Connection
                      </Button>
                    </CardContent>
                  </Card>
                </div>
              )}

              <div className="grid gap-4 px-4 lg:px-6 @xl/main:grid-cols-2 @4xl/main:grid-cols-4" suppressHydrationWarning>
                {loading ? (
                  <>
                    <Skeleton className="h-32" />
                    <Skeleton className="h-32" />
                    <Skeleton className="h-32" />
                    <Skeleton className="h-32" />
                  </>
                ) : connectionError ? (
                  <>
                    <Card className="opacity-50">
                      <CardHeader className="flex flex-row items-center justify-between pb-2">
                        <CardTitle className="text-sm font-medium">Total Tables</CardTitle>
                        <IconTable className="h-4 w-4 text-muted-foreground" />
                      </CardHeader>
                      <CardContent>
                        <div className="text-2xl font-bold">-</div>
                      </CardContent>
                    </Card>
                    <Card className="opacity-50">
                      <CardHeader className="flex flex-row items-center justify-between pb-2">
                        <CardTitle className="text-sm font-medium">Collections</CardTitle>
                        <IconDatabase className="h-4 w-4 text-muted-foreground" />
                      </CardHeader>
                      <CardContent>
                        <div className="text-2xl font-bold">-</div>
                      </CardContent>
                    </Card>
                    <Card className="opacity-50">
                      <CardHeader className="flex flex-row items-center justify-between pb-2">
                        <CardTitle className="text-sm font-medium">System Status</CardTitle>
                        <IconActivity className="h-4 w-4 text-muted-foreground" />
                      </CardHeader>
                      <CardContent>
                        <Badge variant="outline" className="bg-red-500/10 text-red-500">Disconnected</Badge>
                      </CardContent>
                    </Card>
                    <Card className="opacity-50">
                      <CardHeader className="flex flex-row items-center justify-between pb-2">
                        <CardTitle className="text-sm font-medium">Database Connections</CardTitle>
                        <IconDatabase className="h-4 w-4 text-muted-foreground" />
                      </CardHeader>
                      <CardContent>
                        <div className="text-sm text-muted-foreground">No connection</div>
                      </CardContent>
                    </Card>
                  </>
                ) : (
                  <>
                    <Card>
                      <CardHeader className="flex flex-row items-center justify-between pb-2">
                        <CardTitle className="text-sm font-medium">Total Tables</CardTitle>
                        <IconTable className="h-4 w-4 text-muted-foreground" />
                      </CardHeader>
                      <CardContent>
                        <div className="text-2xl font-bold">{summary?.summary.tables.count || 0}</div>
                        <p className="text-xs text-muted-foreground">{summary?.summary.tables.total_rows || 0} total rows</p>
                      </CardContent>
                    </Card>
                    <Card>
                      <CardHeader className="flex flex-row items-center justify-between pb-2">
                        <CardTitle className="text-sm font-medium">Collections</CardTitle>
                        <IconDatabase className="h-4 w-4 text-muted-foreground" />
                      </CardHeader>
                      <CardContent>
                        <div className="text-2xl font-bold">{summary?.summary.collections.count || 0}</div>
                        <p className="text-xs text-muted-foreground">{summary?.summary.collections.total_documents || 0} documents</p>
                      </CardContent>
                    </Card>
                    <Card>
                      <CardHeader className="flex flex-row items-center justify-between pb-2">
                        <CardTitle className="text-sm font-medium">System Status</CardTitle>
                        <IconActivity className="h-4 w-4 text-muted-foreground" />
                      </CardHeader>
                      <CardContent>
                        <Badge variant="outline" className={getStatusColor(summary?.status || "unknown")}>
                          {summary?.status || "Unknown"}
                        </Badge>
                        <p className="text-xs text-muted-foreground mt-1">
                          {health?.timestamp ? new Date(health.timestamp / 1000000).toLocaleTimeString() : "Never"}
                        </p>
                      </CardContent>
                    </Card>
                    <Card>
                      <CardHeader className="flex flex-row items-center justify-between pb-2">
                        <CardTitle className="text-sm font-medium">Database Connections</CardTitle>
                        <IconDatabase className="h-4 w-4 text-muted-foreground" />
                      </CardHeader>
                      <CardContent>
                        <div className="flex flex-col gap-1">
                          <div className="flex items-center gap-2 text-xs">
                            {getStatusIcon(health?.databases.main?.status || "error")}
                            <span>Main DB</span>
                          </div>
                          <div className="flex items-center gap-2 text-xs">
                            {getStatusIcon(health?.databases.vector?.status || "error")}
                            <span>Vector DB</span>
                          </div>
                        </div>
                      </CardContent>
                    </Card>
                  </>
                )}
              </div>

              {!connectionError && (
                <div className="px-4 lg:px-6" suppressHydrationWarning>
                  <Card>
                    <CardHeader>
                      <CardTitle className="flex items-center gap-2">
                        <IconChartBar className="h-5 w-5" />
                        Database Overview
                      </CardTitle>
                      <CardDescription>
                        Number of tables and collections in your database
                      </CardDescription>
                    </CardHeader>
                    <CardContent>
                      {loading ? (
                        <Skeleton className="h-64" />
                      ) : (
                        <div className="h-64 w-full">
                          <ResponsiveContainer width="100%" height="100%">
                            <BarChart
                              data={chartData}
                              margin={{
                                top: 20,
                                right: 30,
                                left: 20,
                                bottom: 5,
                              }}
                            >
                              <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
                              <XAxis 
                                dataKey="name" 
                                tick={{ fill: 'var(--foreground)' }}
                                axisLine={{ stroke: 'var(--border)' }}
                                tickLine={{ stroke: 'var(--border)' }}
                              />
                              <YAxis 
                                tick={{ fill: 'var(--foreground)' }}
                                axisLine={{ stroke: 'var(--border)' }}
                                tickLine={{ stroke: 'var(--border)' }}
                                allowDecimals={false}
                              />
                              <Tooltip 
                                contentStyle={{
                                  backgroundColor: 'var(--card)',
                                  border: '1px solid var(--border)',
                                  borderRadius: '6px',
                                  color: 'var(--card-foreground)',
                                }}
                                cursor={{ fill: 'var(--muted)' }}
                              />
                              <Bar dataKey="count" radius={[4, 4, 0, 0]}>
                                {chartData.map((entry, index) => (
                                  <Cell key={`cell-${index}`} fill={entry.color} />
                                ))}
                              </Bar>
                            </BarChart>
                          </ResponsiveContainer>
                        </div>
                      )}
                    </CardContent>
                  </Card>
                </div>
              )}

              {!connectionError && (
                <div className="px-4 lg:px-6" suppressHydrationWarning>
                  <Card>
                    <CardHeader>
                      <CardTitle>Health Details</CardTitle>
                      <CardDescription>Current status of all database components</CardDescription>
                    </CardHeader>
                    <CardContent>
                      {loading ? (
                        <Skeleton className="h-24" />
                      ) : (
                        <div className="grid gap-4 @xl/main:grid-cols-2">
                          <div className="space-y-2">
                            <h4 className="font-medium">Main Database</h4>
                            {health?.databases.main ? (
                              <div className="text-sm text-muted-foreground">
                                <div className="flex items-center gap-2">
                                  {getStatusIcon(health.databases.main.status)}
                                  <span>Status: {health.databases.main.status}</span>
                                </div>
                                {health.databases.main.tables !== undefined && <div>Tables: {health.databases.main.tables}</div>}
                                {health.databases.main.error && <div className="text-red-500">{health.databases.main.error}</div>}
                              </div>
                            ) : (
                              <div className="text-sm text-muted-foreground">No data available</div>
                            )}
                          </div>
                          <div className="space-y-2">
                            <h4 className="font-medium">Vector Database</h4>
                            {health?.databases.vector ? (
                              <div className="text-sm text-muted-foreground">
                                <div className="flex items-center gap-2">
                                  {getStatusIcon(health.databases.vector.status)}
                                  <span>Status: {health.databases.vector.status}</span>
                                </div>
                                {health.databases.vector.collections !== undefined && <div>Collections: {health.databases.vector.collections}</div>}
                                {health.databases.vector.error && <div className="text-red-500">{health.databases.vector.error}</div>}
                              </div>
                            ) : (
                              <div className="text-sm text-muted-foreground">No data available</div>
                            )}
                          </div>
                        </div>
                      )}
                    </CardContent>
                  </Card>
                </div>
              )}
            </div>
          </div>
        </div>
      </SidebarInset>
    </SidebarProvider>
  )
}
