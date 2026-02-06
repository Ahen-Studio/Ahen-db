"use client"

import { useEffect, useState, useCallback } from "react"
import { useParams } from "next/navigation"
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
import { Skeleton } from "@/components/ui/skeleton"
import { Badge } from "@/components/ui/badge"
import { toast } from "sonner"
import {
  IconChevronLeft,
  IconChevronRight,
  IconChevronsLeft,
  IconChevronsRight,
  IconArrowLeft,
  IconRefresh,
} from "@tabler/icons-react"
import { getCollectionDetails, getCollectionDocuments } from "@/lib/api"
import { VectorCollectionDetails, PaginatedVectorDocuments } from "@/types"

export default function CollectionDetailPage() {
  const params = useParams()
  const collectionName = params.collectionName as string

  const [details, setDetails] = useState<VectorCollectionDetails | null>(null)
  const [documents, setDocuments] = useState<PaginatedVectorDocuments | null>(null)
  const [loading, setLoading] = useState(true)
  const [pagination, setPagination] = useState({
    limit: 50,
    offset: 0,
  })

  const loadDetails = useCallback(async () => {
    try {
      const detailsData = await getCollectionDetails(collectionName)
      setDetails(detailsData)
    } catch (error) {
      toast.error("Failed to load collection details")
      console.error(error)
    }
  }, [collectionName])

  const loadDocuments = useCallback(async () => {
    try {
      setLoading(true)
      const docsData = await getCollectionDocuments(
        collectionName,
        pagination.limit,
        pagination.offset
      )
      setDocuments(docsData)
    } catch (error) {
      toast.error("Failed to load documents")
      console.error(error)
    } finally {
      setLoading(false)
    }
  }, [collectionName, pagination.limit, pagination.offset])

  useEffect(() => {
    if (collectionName) {
      loadDetails()
      loadDocuments()
    }
  }, [collectionName, loadDetails, loadDocuments])

  const totalPages = documents ? Math.ceil(documents.total / documents.limit) : 0
  const currentPage = documents ? Math.floor(documents.offset / documents.limit) + 1 : 1

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
        <SiteHeader title={`Collection: ${collectionName}`} />
        <div className="flex flex-1 flex-col">
          <div className="@container/main flex flex-1 flex-col gap-2">
            <div className="flex flex-col gap-4 py-4 md:gap-6 md:py-6">
              {/* Back button and actions */}
              <div className="flex items-center justify-between px-4 lg:px-6">
                <Link href="/collections">
                  <Button variant="ghost" size="sm">
                    <IconArrowLeft className="mr-1 h-4 w-4" />
                    Back to Collections
                  </Button>
                </Link>
                <div className="flex items-center gap-2">
                  <Button
                    variant="outline"
                    size="icon"
                    onClick={loadDocuments}
                    disabled={loading}
                  >
                    <IconRefresh className={`h-4 w-4 ${loading ? "animate-spin" : ""}`} />
                  </Button>
                </div>
              </div>

              {/* Details Card */}
              <div className="px-4 lg:px-6">
                <Card>
                  <CardHeader>
                    <CardTitle>Collection Details</CardTitle>
                    <CardDescription>
                      Metadata and configuration
                    </CardDescription>
                  </CardHeader>
                  <CardContent>
                    {!details ? (
                      <Skeleton className="h-16" />
                    ) : details.error ? (
                      <div className="text-red-500">{details.error}</div>
                    ) : (
                      <div className="space-y-4">
                        <div className="grid grid-cols-2 gap-4 @xl/main:grid-cols-4">
                          <div>
                            <div className="text-sm text-muted-foreground">Status</div>
                            <Badge variant={details.exists ? "default" : "destructive"}>
                              {details.exists ? "Exists" : "Not Found"}
                            </Badge>
                          </div>
                          <div>
                            <div className="text-sm text-muted-foreground">Documents</div>
                            <div className="font-medium">
                              {details.document_count?.toLocaleString() ?? "Unknown"}
                            </div>
                          </div>
                        </div>
                        {details.metadata && Object.keys(details.metadata).length > 0 && (
                          <div>
                            <div className="text-sm text-muted-foreground mb-2">Metadata</div>
                            <pre className="text-sm rounded bg-muted p-2 overflow-auto max-h-40">
                              {JSON.stringify(details.metadata, null, 2)}
                            </pre>
                          </div>
                        )}
                      </div>
                    )}
                  </CardContent>
                </Card>
              </div>

              {/* Documents Table */}
              <div className="px-4 lg:px-6">
                <Card>
                  <CardHeader>
                    <CardTitle>Documents</CardTitle>
                    <CardDescription>
                      {documents ? (
                        <>
                          Showing {documents.ids.length} of {documents.total.toLocaleString()} documents
                        </>
                      ) : (
                        "Loading documents..."
                      )}
                    </CardDescription>
                  </CardHeader>
                  <CardContent>
                    {loading ? (
                      <Skeleton className="h-96" />
                    ) : documents?.ids.length === 0 ? (
                      <div className="flex h-64 items-center justify-center text-muted-foreground">
                        No documents found
                      </div>
                    ) : (
                      <>
                        <div className="rounded-md border overflow-auto max-h-[600px]">
                          <Table>
                            <TableHeader className="sticky top-0 bg-muted z-10">
                              <TableRow>
                                <TableHead className="w-48">ID</TableHead>
                                <TableHead>Document</TableHead>
                                <TableHead>Metadata</TableHead>
                              </TableRow>
                            </TableHeader>
                            <TableBody>
                              {documents?.ids.map((id, index) => (
                                <TableRow key={id}>
                                  <TableCell className="font-mono text-xs max-w-48 truncate">
                                    {id}
                                  </TableCell>
                                  <TableCell className="max-w-md">
                                    <div className="truncate text-sm">
                                      {documents.documents[index] ?? "null"}
                                    </div>
                                  </TableCell>
                                  <TableCell className="max-w-xs">
                                    {documents.metadatas[index] ? (
                                      <pre className="text-xs rounded bg-muted p-1 overflow-hidden">
                                        {JSON.stringify(documents.metadatas[index], null, 2)}
                                      </pre>
                                    ) : (
                                      <span className="text-muted-foreground">null</span>
                                    )}
                                  </TableCell>
                                </TableRow>
                              ))}
                            </TableBody>
                          </Table>
                        </div>

                        {/* Pagination */}
                        {documents && (
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
                                disabled={!documents.has_more}
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
