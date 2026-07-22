'use client'

import { type MarketCategory, MARKET_TABS } from '@/lib/quant-data'
import { cn } from '@/lib/utils'

type Props = {
  active: MarketCategory
  onChange: (key: MarketCategory) => void
}

export function MarketTabs({ active, onChange }: Props) {
  return (
    <div
      className="inline-flex rounded-lg border border-border bg-secondary/50 p-0.5"
      role="tablist"
      aria-label="市场分类"
    >
      {MARKET_TABS.map((tab) => (
        <button
          key={tab.key}
          role="tab"
          aria-selected={active === tab.key}
          onClick={() => onChange(tab.key)}
          className={cn(
            'rounded-md px-3 py-1.5 text-xs font-medium transition-all',
            active === tab.key
              ? 'bg-background text-foreground shadow-sm'
              : 'text-muted-foreground hover:text-foreground',
          )}
        >
          {tab.label}
        </button>
      ))}
    </div>
  )
}
