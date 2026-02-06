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
  IconDatabase,
  IconChevronRight,
  IconRefresh,
} from "@tabler/icons-react"
import { listCollections } from "@/lib/api"
import { VectorCollectionInfo } from "@/types"

export default function CollectionsPage() {
  const [collections, setCollections] = useState<VectorCollectionInfo[]>([])
  const [loading, setLoading] = useState(true)
  const [searchTerm, setSearchTerm] = useState("")

  const loadCollections = useCallback(async (silent: boolean = false) => {
    try {
      if (!silent) {
        setLoading(true)
      }
      const data = await listCollections()
      setCollections(data)
    } catch (error) {
      if (!silent) {
        toast.error("Failed to load collections")
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
    loadCollections(false)
    
    // Auto-refresh every 2 seconds
    const interval = setInterval(() => {
      loadCollections(true)
    }, 2000)
    
    return () => clearInterval(interval)
  }, [loadCollections])

  const filteredCollections = collections.filter((collection) =>
    collection.name.toLowerCase().includes(searchTerm.toLowerCase())
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
        <SiteHeader title="Vector Collections" />
        <div className="flex flex-1 flex-col">
          <div className="@container/main flex flex-1 flex-col gap-2">
            <div className="flex flex-col gap-4 py-4 md:gap-6 md:py-6">
              <div className="px-4 lg:px-6">
                <Card>
                  <CardHeader className="flex flex-row items-center justify-between">
                    <div>
                      <CardTitle className="flex items-center gap-2">
                        <IconDatabase className="h-5 w-5" />
                        Vector Collections
                      </CardTitle>
                      <CardDescription>
                        {collections.length} collection{collections.length !== 1 ? "s" : ""} in the vector database
                      </CardDescription>
                    </div>
                    <Button
                      variant="outline"
                      size="icon"
                      onClick={loadCollections}
                      disabled={loading}
                    >
                      <IconRefresh className={`h-4 w-4 ${loading ? "animate-spin" : ""}`} />
                    </Button>
                  </CardHeader>
                  <CardContent>
                    <div className="mb-4">
                      <Input
                        placeholder="Search collections..."
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
                              <TableHead>Collection Name</TableHead>
                              <TableHead>Documents</TableHead>
                              <TableHead>Metadata</TableHead>
                              <TableHead className="text-right">Actions</TableHead>
                            </TableRow>
                          </TableHeader>
                          <TableBody>
                            {filteredCollections.length === 0 ? (
                              <TableRow>
                                <TableCell
                                  colSpan={4}
                                  className="h-24 text-center text-muted-foreground"
                                >
                                  {searchTerm
                                    ? "No collections found matching your search"
                                    : "No collections in the database"}
                                </TableCell>
                              </TableRow>
                            ) : (
                              filteredCollections.map((collection) => (
                                <TableRow key={collection.name}>
                                  <TableCell className="font-medium">
                                    {collection.name}
                                  </TableCell>
                                  <TableCell>
                                    <Badge variant="secondary">
                                      {collection.document_count.toLocaleString()} document{collection.document_count !== 1 ? "s" : ""}
                                    </Badge>
                                  </TableCell>
                                  <TableCell>
                                    {Object.keys(collection.metadata).length > 0 ? (
                                      <Badge variant="outline">
                                        {Object.keys(collection.metadata).length} key{Object.keys(collection.metadata).length !== 1 ? "s" : ""}
                                      </Badge>
                                    ) : (
                                      <span className="text-muted-foreground">-</span>
                                    )}
                                  </TableCell>
                                  <TableCell className="text-right">
                                    <Link href={`/collections/${collection.name}`}>
                                      <Button variant="ghost" size="sm">
                                        View Documents
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
