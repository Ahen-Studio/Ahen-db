"use client"

import { useState, useEffect } from "react"
import { AppSidebar } from "@/components/app-sidebar"
import { SiteHeader } from "@/components/site-header"
import {
  SidebarInset,
  SidebarProvider,
} from "@/components/ui/sidebar"
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
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
  IconSearch,
  IconFileSearch,
} from "@tabler/icons-react"
import { searchVectors, listCollections } from "@/lib/api"
import { VectorSearchResponse, VectorCollectionInfo } from "@/types"

export default function SearchPage() {
  const [collections, setCollections] = useState<VectorCollectionInfo[]>([])
  const [selectedCollection, setSelectedCollection] = useState<string>("")
  const [queryText, setQueryText] = useState("")
  const [nResults, setNResults] = useState("10")
  const [loading, setLoading] = useState(false)
  const [results, setResults] = useState<VectorSearchResponse | null>(null)
  const [collectionsLoading, setCollectionsLoading] = useState(true)

  // Load collections on mount
  useEffect(() => {
    async function loadCollections() {
      try {
        const data = await listCollections()
        setCollections(data)
        if (data.length > 0) {
          setSelectedCollection(data[0].name)
        }
      } catch (error) {
        toast.error("Failed to load collections")
      } finally {
        setCollectionsLoading(false)
      }
    }
    loadCollections()
  }, [])

  async function handleSearch() {
    if (!selectedCollection) {
      toast.error("Please select a collection")
      return
    }
    if (!queryText.trim()) {
      toast.error("Please enter a search query")
      return
    }

    try {
      setLoading(true)
      const searchResults = await searchVectors(
        selectedCollection,
        queryText,
        parseInt(nResults)
      )
      setResults(searchResults)
      if (searchResults.error) {
        toast.error(searchResults.error)
      }
    } catch (error) {
      toast.error("Search failed")
      console.error(error)
    } finally {
      setLoading(false)
    }
  }

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
        <SiteHeader title="Vector Search" />
        <div className="flex flex-1 flex-col">
          <div className="@container/main flex flex-1 flex-col gap-2">
            <div className="flex flex-col gap-4 py-4 md:gap-6 md:py-6">
              {/* Search Card */}
              <div className="px-4 lg:px-6">
                <Card>
                  <CardHeader>
                    <CardTitle className="flex items-center gap-2">
                      <IconFileSearch className="h-5 w-5" />
                      Vector Similarity Search
                    </CardTitle>
                    <CardDescription>
                      Search for similar documents using vector embeddings
                    </CardDescription>
                  </CardHeader>
                  <CardContent>
                    <div className="grid gap-4 @xl/main:grid-cols-4">
                      <div className="space-y-2">
                        <Label htmlFor="collection">Collection</Label>
                        {collectionsLoading ? (
                          <Skeleton className="h-10" />
                        ) : (
                          <Select
                            value={selectedCollection}
                            onValueChange={setSelectedCollection}
                          >
                            <SelectTrigger id="collection">
                              <SelectValue placeholder="Select collection" />
                            </SelectTrigger>
                            <SelectContent>
                              {collections.map((coll) => (
                                <SelectItem key={coll.name} value={coll.name}>
                                  {coll.name} ({coll.document_count} docs)
                                </SelectItem>
                              ))}
                            </SelectContent>
                          </Select>
                        )}
                      </div>
                      <div className="space-y-2 @xl/main:col-span-2">
                        <Label htmlFor="query">Search Query</Label>
                        <Input
                          id="query"
                          placeholder="Enter text to search for similar documents..."
                          value={queryText}
                          onChange={(e) => setQueryText(e.target.value)}
                          onKeyDown={(e) => {
                            if (e.key === "Enter") handleSearch()
                          }}
                        />
                      </div>
                      <div className="space-y-2">
                        <Label htmlFor="n-results">Results</Label>
                        <Select value={nResults} onValueChange={setNResults}>
                          <SelectTrigger id="n-results">
                            <SelectValue />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="5">5</SelectItem>
                            <SelectItem value="10">10</SelectItem>
                            <SelectItem value="20">20</SelectItem>
                            <SelectItem value="50">50</SelectItem>
                          </SelectContent>
                        </Select>
                      </div>
                    </div>
                    <Button
                      className="mt-4"
                      onClick={handleSearch}
                      disabled={loading || !selectedCollection}
                    >
                      {loading ? (
                        <>
                          <IconSearch className="mr-2 h-4 w-4 animate-pulse" />
                          Searching...
                        </>
                      ) : (
                        <>
                          <IconSearch className="mr-2 h-4 w-4" />
                          Search
                        </>
                      )}
                    </Button>
                  </CardContent>
                </Card>
              </div>

              {/* Results */}
              {results && (
                <div className="px-4 lg:px-6">
                  <Card>
                    <CardHeader>
                      <CardTitle>Search Results</CardTitle>
                      <CardDescription>
                        Query: &quot;{results.query}&quot; • {results.results.length} results
                      </CardDescription>
                    </CardHeader>
                    <CardContent>
                      {results.error ? (
                        <div className="text-red-500">{results.error}</div>
                      ) : results.results.length === 0 ? (
                        <div className="text-center text-muted-foreground py-8">
                          No results found
                        </div>
                      ) : (
                        <div className="space-y-4">
                          {results.results.map((result, index) => (
                            <div
                              key={result.id}
                              className="rounded-lg border p-4 space-y-2"
                            >
                              <div className="flex items-center justify-between">
                                <div className="flex items-center gap-2">
                                  <Badge variant="secondary">#{index + 1}</Badge>
                                  <span className="font-mono text-xs text-muted-foreground">
                                    {result.id}
                                  </span>
                                </div>
                                {result.similarity_score !== null && (
                                  <Badge variant="outline">
                                    Score: {result.similarity_score.toFixed(4)}
                                  </Badge>
                                )}
                              </div>
                              <div className="text-sm">
                                {result.document || (
                                  <span className="text-muted-foreground italic">
                                    No document content
                                  </span>
                                )}
                              </div>
                              {result.metadata && Object.keys(result.metadata).length > 0 && (
                                <div className="text-xs">
                                  <div className="text-muted-foreground mb-1">Metadata:</div>
                                  <pre className="rounded bg-muted p-2 overflow-auto">
                                    {JSON.stringify(result.metadata, null, 2)}
                                  </pre>
                                </div>
                              )}
                            </div>
                          ))}
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
