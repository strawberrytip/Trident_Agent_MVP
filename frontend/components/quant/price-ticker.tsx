'use client'

import { cn } from '@/lib/utils'
import type { MarketCategory } from '@/lib/quant-data'
import type { PriceMap } from '@/hooks/use-binance-prices'

type TickerSymbol = {
  asset: string
  label: string
  /** Which markets this symbol should appear in */
  showIn: MarketCategory[]
}

const SYMBOLS: TickerSymbol[] = [
  { asset: 'BTC',  label: 'BTC',  showIn: ['ALL', 'CRYPTO', 'MACRO'] },
  { asset: 'ETH',  label: 'ETH',  showIn: ['ALL', 'CRYPTO', 'MACRO'] },
  { asset: 'SOL',  label: 'SOL',  showIn: ['ALL', 'CRYPTO']           },
  { asset: 'XAU',  label: 'XAU',  showIn: ['ALL', 'GOLD', 'MACRO']    },
  { asset: 'WTI',  label: 'WTI',  showIn: ['ALL', 'OIL', 'MACRO']     },
]

type Props = {
  prices: PriceMap
  connected: boolean
  activeMarket: MarketCategory
}

/**
 * Format an asset price for display — 2‑decimal institutional standard.
 *
 * - sub‑$1 tokens: 4 decimal places (e.g. $0.5218)
 * - everything else: 2 decimal places (e.g. $64,180.50 / $4,113.16 / $148.22)
 */
export function formatPrice(p: number, _asset?: string): string {
  if (p < 1) return p.toFixed(4)
  return p.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })
}

export function PriceTicker({ prices, connected, activeMarket }: Props) {
  const visible = SYMBOLS.filter((s) => s.showIn.includes(activeMarket))

  return (
    <div className="inline-flex items-center gap-0.5 rounded-lg border border-border bg-secondary/50 p-0.5">
      {visible.map((s) => {
        const info = prices[s.asset]
        const up = (info?.change24h ?? 0) >= 0

        return (
          <div
            key={s.asset}
            className="flex items-center gap-1.5 rounded-md px-2.5 py-1.5"
          >
            <span className="font-mono text-[11px] font-semibold text-foreground">
              {s.label}
            </span>
            {info ? (
              <>
                <span className="font-mono text-[11px] tabular-nums text-foreground/90">
                  ${formatPrice(info.price, s.asset)}
                </span>
                <span
                  className={cn(
                    'font-mono text-[10px] tabular-nums',
                    up ? 'text-long' : 'text-short',
                  )}
                >
                  {up ? '+' : ''}{info.change24h.toFixed(2)}%
                </span>
              </>
            ) : (
              <span className="font-mono text-[10px] text-muted-foreground">
                {connected ? '加载中' : '断开'}
              </span>
            )}
          </div>
        )
      })}
      <div
        className={cn(
          'mx-1 h-2 w-2 rounded-full',
          connected ? 'bg-long' : 'bg-muted-foreground',
        )}
        title={connected ? '币安已连接' : '币安断开'}
      />
    </div>
  )
}
