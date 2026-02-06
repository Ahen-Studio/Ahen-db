"use client"

import * as React from "react"
import Link from "next/link"
import {
  IconActivity,
  IconBook,
  IconDatabase,
  IconDashboard,
  IconFileSearch,
  IconSearch,
  IconSettings,
  IconTable,
  IconX,
} from "@tabler/icons-react"

import { NavMain } from "@/components/nav-main"
import {
  Sidebar,
  SidebarContent,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
} from "@/components/ui/sidebar"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"

const data = {
  navMain: [
    {
      title: "Health",
      url: "/health",
      icon: IconActivity,
    },
    {
      title: "Dashboard",
      url: "/dashboard",
      icon: IconDashboard,
    },
    {
      title: "Tables",
      url: "/tables",
      icon: IconTable,
    },
    {
      title: "Collections",
      url: "/collections",
      icon: IconDatabase,
    },
    {
      title: "Vector Search",
      url: "/search",
      icon: IconFileSearch,
    },
  ],
}

// Get saved paths from localStorage
function getSavedDbPaths() {
  if (typeof window === "undefined") {
    return {
      main: "./db/_generated/skypydb.db",
      vector: "./db/_generated/vector.db",
    }
  }
  return {
    main: localStorage.getItem("SKYPYDB_PATH") || "./db/_generated/skypydb.db",
    vector: localStorage.getItem("SKYPYDB_VECTOR_PATH") || "./db/_generated/vector.db",
  }
}

export function AppSidebar({ ...props }: React.ComponentProps<typeof Sidebar>) {
  const [settingsOpen, setSettingsOpen] = React.useState(false)
  const [mainDbPath, setMainDbPath] = React.useState("./db/_generated/skypydb.db")
  const [vectorDbPath, setVectorDbPath] = React.useState("./db/_generated/vector.db")
  const [searchQuery, setSearchQuery] = React.useState("")

  // Load saved paths when dialog opens
  React.useEffect(() => {
    if (settingsOpen) {
      const paths = getSavedDbPaths()
      setMainDbPath(paths.main)
      setVectorDbPath(paths.vector)
    }
  }, [settingsOpen])

  const handleSaveSettings = () => {
    localStorage.setItem("SKYPYDB_PATH", mainDbPath)
    localStorage.setItem("SKYPYDB_VECTOR_PATH", vectorDbPath)
    setSettingsOpen(false)
    window.location.reload()
  }

  // Filter nav items based on search
  const filteredNavItems = React.useMemo(() => {
    if (!searchQuery.trim()) return data.navMain
    return data.navMain.filter(item => 
      item.title.toLowerCase().includes(searchQuery.toLowerCase())
    )
  }, [searchQuery])

  return (
    <>
      <Sidebar collapsible="offcanvas" {...props}>
        <SidebarHeader>
          <SidebarMenu>
            <SidebarMenuItem>
              <SidebarMenuButton
                asChild
                className="data-[slot=sidebar-menu-button]:!p-1.5"
              >
                <Link href="/dashboard">
                  <IconDatabase className="!size-5" />
                  <span className="text-base font-semibold">SkypyDB</span>
                </Link>
              </SidebarMenuButton>
            </SidebarMenuItem>
          </SidebarMenu>
        </SidebarHeader>
        <SidebarContent>
          {/* Search Input */}
          <div className="px-3 py-2">
            <div className="relative">
              <IconSearch className="absolute left-3 top-1/2 -translate-y-1/2 size-4 text-muted-foreground" />
              <Input
                placeholder="Search..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                className="pl-9 pr-9 h-10"
              />
              {searchQuery && (
                <button
                  onClick={() => setSearchQuery("")}
                  className="absolute right-3 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground"
                >
                  <IconX className="size-4" />
                </button>
              )}
            </div>
          </div>

          <NavMain items={filteredNavItems} />
          
          {/* Separator */}
          <div className="my-4 mx-2 h-px bg-border" />
          
          {/* Settings and Docs buttons */}
          <div className="space-y-1" suppressHydrationWarning>
            <SidebarMenu>
              <SidebarMenuItem>
                <SidebarMenuButton 
                  onClick={() => setSettingsOpen(true)}
                  className="w-full h-11 cursor-pointer text-base justify-start"
                >
                  <IconSettings className="size-5 ml-3 mr-3" />
                  <span className="font-medium">Settings</span>
                </SidebarMenuButton>
              </SidebarMenuItem>
            </SidebarMenu>
            <SidebarMenu>
              <SidebarMenuItem>
                <SidebarMenuButton 
                  asChild
                  className="w-full h-11 cursor-pointer text-base justify-start"
                >
                  <Link 
                    href="https://ahen.mintlify.app/getting-started/introduction" 
                    target="_blank"
                    rel="noopener noreferrer"
                    className="flex items-center"
                  >
                    <IconBook className="size-5 ml-3 mr-3" />
                    <span className="font-medium">Docs</span>
                  </Link>
                </SidebarMenuButton>
              </SidebarMenuItem>
            </SidebarMenu>
          </div>
        </SidebarContent>
      </Sidebar>

      {/* Settings Dialog */}
      <Dialog open={settingsOpen} onOpenChange={setSettingsOpen}>
        <DialogContent className="sm:max-w-[425px]">
          <DialogHeader>
            <DialogTitle>Database Settings</DialogTitle>
            <DialogDescription>
              Configure your database paths. Changes will take effect on next refresh.
            </DialogDescription>
          </DialogHeader>
          <div className="grid gap-4 py-4">
            <div className="grid gap-2">
              <Label htmlFor="main-db-path">Main Database Path</Label>
              <Input
                id="main-db-path"
                value={mainDbPath}
                onChange={(e) => setMainDbPath(e.target.value)}
                placeholder="./db/_generated/skypydb.db"
              />
            </div>
            <div className="grid gap-2">
              <Label htmlFor="vector-db-path">Vector Database Path</Label>
              <Input
                id="vector-db-path"
                value={vectorDbPath}
                onChange={(e) => setVectorDbPath(e.target.value)}
                placeholder="./db/_generated/vector.db"
              />
            </div>
          </div>
          <DialogFooter>
            <Button onClick={handleSaveSettings}>
              Save Changes
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  )
}
