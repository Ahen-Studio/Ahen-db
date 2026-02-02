"use client"

import Link from "next/link"
import { AppSidebar } from "@/components/app-sidebar"
import { SiteHeader } from "@/components/site-header"
import {
  SidebarInset,
  SidebarProvider,
} from "@/components/ui/sidebar"

export default function HomePage() {
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
        <SiteHeader title="Welcome" />
        <div className="flex flex-1 items-center justify-center">
          <div className="text-center">
            <h1 className="text-4xl font-bold mb-4">SkypyDB Dashboard</h1>
            <p className="text-muted-foreground mb-8">
              Monitor and manage your databases with ease
            </p>
            <Link
              href="/dashboard"
              className="inline-flex items-center justify-center rounded-md bg-primary px-6 py-3 text-sm font-medium text-primary-foreground shadow transition-colors hover:bg-primary/90"
            >
              Go to Dashboard
            </Link>
          </div>
        </div>
      </SidebarInset>
    </SidebarProvider>
  )
}
