import { Search, Settings, Triangle } from 'lucide-react'

export function DashboardHeader() {
  return (
    <header className="glass sticky top-0 z-20 flex h-16 items-center justify-between border-b border-border px-4 md:px-6">
      <div className="flex items-center gap-3">
        <div className="flex h-9 w-9 items-center justify-center rounded-md border border-primary/40 bg-primary/10">
          <Triangle className="h-5 w-5 fill-primary text-primary" aria-hidden="true" />
        </div>
        <div className="leading-tight">
          <h1 className="font-mono text-lg font-bold tracking-tight text-foreground">
            Trident
          </h1>
          <p className="text-[10px] uppercase tracking-[0.2em] text-muted-foreground">
            量化交易平台
          </p>
        </div>
      </div>

      <div className="hidden items-center gap-2 rounded-md border border-border bg-secondary/40 px-3 py-1.5 md:flex">
        <Search className="h-3.5 w-3.5 text-muted-foreground" aria-hidden="true" />
        <span className="font-mono text-xs text-muted-foreground">
          BTC / USDT · Perpetual
        </span>
      </div>

      <div className="flex items-center gap-3 md:gap-4">
        <div className="flex items-center gap-2 rounded-md border border-long/30 bg-long/5 px-3 py-1.5">
          <span className="relative flex h-2.5 w-2.5">
            <span className="pulse-dot absolute inline-flex h-full w-full rounded-full bg-long" />
            <span className="relative inline-flex h-2.5 w-2.5 rounded-full bg-long" />
          </span>
          <span className="font-mono text-xs font-medium text-long">
            WebSocket 已连接
          </span>
        </div>
        <button
          type="button"
          className="flex h-9 w-9 items-center justify-center rounded-md border border-border bg-secondary/40 text-muted-foreground transition-colors hover:text-foreground"
          aria-label="设置"
        >
          <Settings className="h-4 w-4" aria-hidden="true" />
        </button>
      </div>
    </header>
  )
}
