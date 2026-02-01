"use client"

import { useEffect, useState, useCallback } from "react"
import { useParams, useRouter } from "next/navigation"
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
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Skeleton } from "@/components/ui/skeleton"
import { Badge } from "@/components/ui/badge"
import { toast } from "sonner"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import {
  IconChevronLeft,
  IconChevronRight,
  IconChevronsLeft,
  IconChevronsRight,
  IconArrowLeft,
  IconSearch,
  IconRefresh,
} from "@tabler/icons-react"
import { getTableSchema, getTableData, searchTable } from "@/lib/api"
import { TableSchema, PaginatedTableData, SearchResult } from "@/types"

export default function TableDetailPage() {
  const params = useParams()
  const router = useRouter()
  const tableName = params.tableName as string

  const [schema, setSchema] = useState<TableSchema | null>(null)
  const [data, setData] = useState<PaginatedTableData | null>(null)
  const [loading, setLoading] = useState(true)
  const [searchQuery, setSearchQuery] = useState("")
  const [searchMode, setSearchMode] = useState(false)
  const [pagination, setPagination] = useState({
    limit: 50,
    offset: 0,
  })

  const loadSchema = useCallback(async () => {
    try {
      const schemaData = await getTableSchema(tableName)
      setSchema(schemaData)
    } catch (error) {
      toast.error("Failed to load table schema")
      console.error(error)
    }
  }, [tableName])

  const loadData = useCallback(async () => {
    try {
      setLoading(true)
      if (searchMode && searchQuery) {
        const searchData = await searchTable(tableName, searchQuery, pagination.limit)
        setData({
          data: searchData.data,
          total: searchData.total,
          limit: searchData.limit,
          offset: 0,
          has_more: false,
        })
      } else {
        const tableData = await getTableData(
          tableName,
          pagination.limit,
          pagination.offset
        )
        setData(tableData)
      }
    } catch (error) {
      toast.error("Failed to load table data")
      console.error(error)
    } finally {
      setLoading(false)
    }
  }, [tableName, pagination.limit, pagination.offset, searchMode, searchQuery])

  useEffect(() => {
    if (tableName) {
      loadSchema()
      loadData()
    }
  }, [tableName, loadSchema, loadData])

  const handleSearch = () => {
    if (searchQuery.trim()) {
      setSearchMode(true)
      setPagination({ ...pagination, offset: 0 })
      loadData()
    }
  }

  const handleClearSearch = () => {
    setSearchQuery("")
    setSearchMode(false)
    setPagination({ ...pagination, offset: 0 })
    loadData()
  }

  const totalPages = data ? Math.ceil(data.total / data.limit) : 0
  const currentPage = data ? Math.floor(data.offset / data.limit) + 1 : 1

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
        <SiteHeader title={`Table: ${tableName}`} />
        <div className="flex flex-1 flex-col">
          <div className="@container/main flex flex-1 flex-col gap-2">
            <div className="flex flex-col gap-4 py-4 md:gap-6 md:py-6">
              {/* Back button and actions */}
              <div className="flex items-center justify-between px-4 lg:px-6">
                <Link href="/tables">
                  <Button variant="ghost" size="sm">
                    <IconArrowLeft className="mr-1 h-4 w-4" />
                    Back to Tables
                  </Button>
                </Link>
                <div className="flex items-center gap-2">
                  <div className="flex items-center gap-2">
                    <Input
                      placeholder="Search in table..."
                      value={searchQuery}
                      onChange={(e) => setSearchQuery(e.target.value)}
                      className="w-64"
                      onKeyDown={(e) => {
                        if (e.key === "Enter") handleSearch()
                      }}
                    />
                    <Button
                      variant="outline"
                      size="icon"
                      onClick={handleSearch}
                    >
                      <IconSearch className="h-4 w-4" />
                    </Button>
                    {searchMode && (
                      <Button variant="ghost" size="sm" onClick={handleClearSearch}>
                        Clear
                      </Button>
                    )}
                  </div>
                  <Button
                    variant="outline"
                    size="icon"
                    onClick={loadData}
                    disabled={loading}
                  >
                    <IconRefresh className={`h-4 w-4 ${loading ? "animate-spin" : ""}`} />
                  </Button>
                </div>
              </div>

              {/* Schema Card */}
              <div className="px-4 lg:px-6">
                <Card>
                  <CardHeader>
                    <CardTitle>Schema</CardTitle>
                    <CardDescription>
                      Table structure and configuration
                    </CardDescription>
                  </CardHeader>
                  <CardContent>
                    {!schema ? (
                      <Skeleton className="h-16" />
                    ) : (
                      <div className="space-y-2">
                        <div className="flex flex-wrap gap-2">
                          {schema.columns.map((column) => (
                            <Badge key={column} variant="secondary">
                              {column}
                            </Badge>
                          ))}
                        </div>
                        {schema.config && (
                          <div className="text-sm text-muted-foreground">
                            <pre className="mt-2 rounded bg-muted p-2 overflow-auto">
                              {JSON.stringify(schema.config, null, 2)}
                            </pre>
                          </div>
                        )}
                      </div>
                    )}
                  </CardContent>
                </Card>
              </div>

              {/* Data Table */}
              <div className="px-4 lg:px-6">
                <Card>
                  <CardHeader>
                    <CardTitle>Table Data</CardTitle>
                    <CardDescription>
                      {data ? (
                        <>
                          Showing {data.data.length} of {data.total.toLocaleString()} rows
                          {searchMode && " (filtered by search)"}
                        </>
                      ) : (
                        "Loading data..."
                      )}
                    </CardDescription>
                  </CardHeader>
                  <CardContent>
                    {loading ? (
                      <Skeleton className="h-96" />
                    ) : data?.data.length === 0 ? (
                      <div className="flex h-64 items-center justify-center text-muted-foreground">
                        No data found
                      </div>
                    ) : (
                      <>
                        <div className="rounded-md border overflow-auto max-h-[600px]">
                          <Table>
                            <TableHeader className="sticky top-0 bg-muted z-10">
                              <TableRow>
                                {schema?.columns.map((column) => (
                                  <TableHead key={column}>{column}</TableHead>
                                ))}
                              </TableRow>
                            </TableHeader>
                            <TableBody>
                              {data?.data.map((row, index) => (
                                <TableRow key={index}>
                                  {schema?.columns.map((column) => (
                                    <TableCell key={column} className="max-w-xs truncate">
                                      {typeof row[column] === "object"
                                        ? JSON.stringify(row[column])
                                        : String(row[column] ?? "null")}
                                    </TableCell>
                                  ))}
                                </TableRow>
                              ))}
                            </TableBody>
                          </Table>
                        </div>

                        {/* Pagination */}
                        {!searchMode && data && (
                          <div className="flex items-center justify-between mt-4">
                            <div className="text-sm text-muted-foreground">
                              Page {currentPage} of {totalPages}
                            </div>
                            <div className="flex items-center gap-2">
                              <Button
                                variant="outline"
                                size="icon"
                                onClick={() =>
                                  setPagination({ ...pagination, offset: 0 })
                                }
                                disabled={currentPage === 1}
                              >
                                <IconChevronsLeft className="h-4 w-4" />
                              </Button>
                              <Button
                                variant="outline"
                                size="icon"
                                onClick={() =>
                                  setPagination({
                                    ...pagination,
                                    offset: pagination.offset - pagination.limit,
                                  })
                                }
                                disabled={currentPage === 1}
                              >
                                <IconChevronLeft className="h-4 w-4" />
                              </Button>
                              <Button
                                variant="outline"
                                size="icon"
                                onClick={() =>
                                  setPagination({
                                    ...pagination,
                                    offset: pagination.offset + pagination.limit,
                                  })
                                }
                                disabled={!data.has_more}
                              >
                                <IconChevronRight className="h-4 w-4" />
                              </Button>
                              <Button
                                variant="outline"
                                size="icon"
                                onClick={() =>
                                  setPagination({
                                    ...pagination,
                                    offset: (totalPages - 1) * pagination.limit,
                                  })
                                }
                                disabled={currentPage === totalPages || totalPages === 0}
                              >
                                <IconChevronsRight className="h-4 w-4" />
                              </Button>
                            </div>
                          </div>
                        )}
                      </>
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
