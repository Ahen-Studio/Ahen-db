"use client"

import { useEffect, useState, useCallback } from "react"
import Link from "next/link"
import { AppSidebar } from "@/components/app-sidebar"
import { SiteHeader } from "@/components/site-header"
import {
  SidebarInset,
  SidebarProvider,
} from "@/components/ui/sidebar"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Skeleton } from "@/components/ui/skeleton"
import { Badge } from "@/components/ui/badge"
import { toast } from "sonner"
import {
  IconTable,
  IconChevronRight,
  IconRefresh,
} from "@tabler/icons-react"
import { listTables } from "@/lib/api"
import { TableInfo } from "@/types"

export default function TablesPage() {
  const [tables, setTables] = useState<TableInfo[]>([])
  const [loading, setLoading] = useState(true)
  const [searchTerm, setSearchTerm] = useState("")

  const loadTables = useCallback(async (silent: boolean = false) => {
    try {
      if (!silent) {
        setLoading(true)
      }
      const data = await listTables()
      setTables(data)
    } catch (error) {
      if (!silent) {
        toast.error("Failed to load tables")
      }
      console.error(error)
    } finally {
      if (!silent) {
        setLoading(false)
      }
    }
  }, [])

  useEffect(() => {
    // Initial load
    loadTables(false)
    
    // Auto-refresh every 2 seconds
    const interval = setInterval(() => {
      loadTables(true)
    }, 2000)
    
    return () => clearInterval(interval)
  }, [loadTables])

  const filteredTables = tables.filter((table) =>
    table.name.toLowerCase().includes(searchTerm.toLowerCase())
  )

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
        <SiteHeader title="Tables" />
        <div className="flex flex-1 flex-col">
          <div className="@container/main flex flex-1 flex-col gap-2">
            <div className="flex flex-col gap-4 py-4 md:gap-6 md:py-6">
              <div className="px-4 lg:px-6">
                <Card>
                  <CardHeader className="flex flex-row items-center justify-between">
                    <div>
                      <CardTitle className="flex items-center gap-2">
                        <IconTable className="h-5 w-5" />
                        Database Tables
                      </CardTitle>
                      <CardDescription>
                        {tables.length} table{tables.length !== 1 ? "s" : ""} in the main database
                      </CardDescription>
                    </div>
                    <Button
                      variant="outline"
                      size="icon"
                      onClick={loadTables}
                      disabled={loading}
                    >
                      <IconRefresh className={`h-4 w-4 ${loading ? "animate-spin" : ""}`} />
                    </Button>
                  </CardHeader>
                  <CardContent>
                    <div className="mb-4">
                      <Input
                        placeholder="Search tables..."
                        value={searchTerm}
                        onChange={(e) => setSearchTerm(e.target.value)}
                        className="max-w-sm"
                      />
                    </div>

                    {loading ? (
                      <Skeleton className="h-64" />
                    ) : (
                      <div className="rounded-md border">
                        <Table>
                          <TableHeader>
                            <TableRow>
                              <TableHead>Table Name</TableHead>
                              <TableHead>Columns</TableHead>
                              <TableHead>Row Count</TableHead>
                              <TableHead>Configuration</TableHead>
                              <TableHead className="text-right">Actions</TableHead>
                            </TableRow>
                          </TableHeader>
                          <TableBody>
                            {filteredTables.length === 0 ? (
                              <TableRow>
                                <TableCell
                                  colSpan={5}
                                  className="h-24 text-center text-muted-foreground"
                                >
                                  {searchTerm
                                    ? "No tables found matching your search"
                                    : "No tables in the database"}
                                </TableCell>
                              </TableRow>
                            ) : (
                              filteredTables.map((table) => (
                                <TableRow key={table.name}>
                                  <TableCell className="font-medium">
                                    {table.name}
                                  </TableCell>
                                  <TableCell>
                                    <Badge variant="secondary">
                                      {table.columns.length} column{table.columns.length !== 1 ? "s" : ""}
                                    </Badge>
                                  </TableCell>
                                  <TableCell>{table.row_count.toLocaleString()}</TableCell>
                                  <TableCell>
                                    {table.config ? (
                                      <Badge variant="outline">Configured</Badge>
                                    ) : (
                                      <span className="text-muted-foreground">-</span>
                                    )}
                                  </TableCell>
                                  <TableCell className="text-right">
                                    <Link href={`/tables/${table.name}`}>
                                      <Button variant="ghost" size="sm">
                                        View Data
                                        <IconChevronRight className="ml-1 h-4 w-4" />
                                      </Button>
                                    </Link>
                                  </TableCell>
                                </TableRow>
                              ))
                            )}
                          </TableBody>
                        </Table>
                      </div>
                    )}
                  </CardContent>
                </Card>
              </div>
            </div>
          </div>
        </div>
      </SidebarInset>
    </SidebarProvider>
  )
}
