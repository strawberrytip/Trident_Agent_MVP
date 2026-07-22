'use client'

import React, { useState } from 'react'
import { ChevronDown, ChevronRight } from 'lucide-react'
import { SignalBadge } from './signal-badge'
import { formatPrice } from './price-ticker'
import type { ApiEvent, MarketCategory } from '@/lib/quant-data'
import type { PriceMap } from '@/hooks/use-binance-prices'

type Props = {
  events: ApiEvent[]
  activeMarket: MarketCategory
  livePrices: PriceMap
  entryPrices?: Record<number, number>
}

const CATEGORY_LABEL: Record<string, string> = {
  CRYPTO: '加密货币',
  GOLD: '黄金',
  OIL: '原油',
  MACRO: '宏观',
  OTHER: '其他',
}

const TRACKED_ASSETS = new Set(['BTC', 'ETH', 'SOL', 'XAU', 'GOLD', 'WTI'])

function computeMFEForLedger(
  action: string,
  entry: number | null,
  max: number | null,
  min: number | null,
): number | null {
  if (!entry || entry <= 0) return null
  if (action === 'BUY' && max && max > 0) return ((max - entry) / entry) * 100
  if (action === 'SELL' && min && min > 0) return ((entry - min) / entry) * 100
  return null
}

function computePnL(
  action: string,
  entry: number,
  current: number,
): { pct: number; positive: boolean } {
  if (!entry || entry === 0) return { pct: 0, positive: false }
  const raw = action === 'SELL'
    ? ((entry - current) / entry) * 100
    : ((current - entry) / entry) * 100
  return { pct: Math.round(raw * 100) / 100, positive: raw >= 0 }
}

function VerdictLabel({ verdict }: { verdict: string }) {
  const v = verdict?.toUpperCase()
  if (!v) return <span className="text-[10px] text-muted-foreground/40">—</span>
  if (v === 'WIN') return <span className="inline-flex items-center gap-0.5 text-[10px] font-medium text-emerald-500">✅ 预测正确</span>
  if (v === 'LOSS') return <span className="inline-flex items-center gap-0.5 text-[10px] font-medium text-red-500">❌ 预测错误</span>
  return <span className="text-[10px] text-muted-foreground/50">➖ 无实质影响</span>
}

function VerdictLabelSm({ verdict }: { verdict: string }) {
  const v = verdict?.toUpperCase()
  if (!v) return <span className="text-[9px] text-muted-foreground/40">—</span>
  if (v === 'WIN') return <span className="inline-flex items-center gap-0.5 text-[9px] font-medium text-emerald-500">✅ 预测正确</span>
  if (v === 'LOSS') return <span className="inline-flex items-center gap-0.5 text-[9px] font-medium text-red-500">❌ 预测错误</span>
  return <span className="text-[9px] text-muted-foreground/50">➖ 无实质影响</span>
}

function filterAndGroup(events: ApiEvent[], activeMarket: MarketCategory) {
  const filtered = events.filter((e) => {
    if (e.action === 'HOLD') return false
    if (activeMarket !== 'ALL' && e.market_category !== activeMarket) return false
    if (e.parent_id !== null && e.parent_id !== undefined) return false
    const asset = e.target_asset ?? ''
    if (e.market_category === 'GOLD' || e.market_category === 'OIL') return true
    if (!asset || asset === 'NONE' || !TRACKED_ASSETS.has(asset.toUpperCase())) return false
    return true
  })

  const childMap = new Map<number, ApiEvent[]>()
  for (const ev of events) {
    if (ev.parent_id === null || ev.parent_id === undefined) continue
    const list = childMap.get(ev.parent_id) || []
    list.push(ev)
    childMap.set(ev.parent_id, list)
  }

  return { parents: filtered, childMap }
}

export function SignalLedger({ events, activeMarket, livePrices, entryPrices }: Props) {
  const { parents, childMap } = filterAndGroup(events, activeMarket)
  const [expandedIds, setExpandedIds] = useState<Set<number>>(new Set())

  const toggleExpand = (id: number) => {
    setExpandedIds((prev) => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }

  const label = CATEGORY_LABEL[activeMarket] ?? activeMarket
  const title = activeMarket === 'ALL' ? 'AI 活跃信号台账' : `AI 活跃信号台账 - ${label}`

  return (
    <section className="glass rounded-lg border border-border">
      <div className="border-b border-border px-4 py-3">
        <h2 className="text-sm font-semibold text-foreground">{title}</h2>
      </div>
      <div className="overflow-x-auto">
        {parents.length === 0 ? (
          <p className="px-4 py-6 text-center text-sm text-muted-foreground">
            暂无 {label} 信号
          </p>
        ) : (
          <table className="w-full text-sm">
            <thead>
              <tr className="text-left font-mono text-[11px] uppercase tracking-wider text-muted-foreground">
                <th className="w-8 px-1 py-2" />
                <th className="px-4 py-2 font-medium">交易对</th>
                <th className="px-4 py-2 font-medium">信号</th>
                <th className="px-4 py-2 text-right font-medium">入场价</th>
                <th className="px-4 py-2 text-right font-medium">当前价</th>
                <th className="px-4 py-2 text-right font-medium">最大浮盈</th>
                <th className="px-4 py-2 text-right font-medium">未实现盈亏</th>
                <th className="px-4 py-2 text-center font-medium">状态</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {parents.map((ev) => {
                const children = childMap.get(ev.id) || []
                const actualChildren = ev.child_count > 0 && children.length === 0
                  ? []
                  : children
                const hasChildren = ev.child_count > 0
                const isExpanded = expandedIds.has(ev.id)

                const isCommodity = ev.market_category === 'GOLD' || ev.market_category === 'OIL'
                  || ev.target_asset === 'XAU' || ev.target_asset === 'GOLD' || ev.target_asset === 'WTI'
                const asset = ev.target_asset && ev.target_asset !== 'NONE'
                  ? ev.target_asset
                  : ev.market_category === 'GOLD' ? 'XAU'
                  : ev.market_category === 'OIL' ? 'WTI'
                  : null
                const assetSuffix = isCommodity ? 'USD' : 'USDT'
                const pair = asset
                  ? `${asset}/${assetSuffix}`
                  : '—'
                const pairKey = asset ? `${asset}USDT` : ''

                const entry = entryPrices?.[ev.id] ?? ev.entry_price ?? undefined
                const currentInfo = pairKey ? livePrices[pairKey] : undefined
                const current = currentInfo?.price ?? 0

                const pnl = entry && current
                  ? computePnL(ev.action, entry, current)
                  : null

                return (
                  <React.Fragment key={ev.id}>
                    <tr className="transition-colors hover:bg-secondary/30">
                      <td className="px-1 py-2.5 text-center">
                        {hasChildren ? (
                          <button
                            onClick={() => toggleExpand(ev.id)}
                            className="inline-flex h-5 w-5 items-center justify-center rounded text-muted-foreground hover:text-foreground transition-colors"
                            aria-label={isExpanded ? '折叠子事件' : '展开子事件'}
                          >
                            {isExpanded
                              ? <ChevronDown className="h-3.5 w-3.5" />
                              : <ChevronRight className="h-3.5 w-3.5" />
                            }
                          </button>
                        ) : (
                          <span className="inline-block w-5" />
                        )}
                      </td>
                      <td className="px-4 py-2.5 font-mono font-medium text-foreground">
                        {pair}
                      </td>
                      <td className="px-4 py-2.5">
                        <div className="flex items-center gap-1.5">
                          <SignalBadge signal={ev.action} />
                          {hasChildren && (
                            <span className="rounded-full bg-primary/15 px-1.5 py-0 text-[10px] font-bold text-primary">
                              {ev.child_count}
                            </span>
                          )}
                        </div>
                      </td>
                      <td className="px-4 py-2.5 text-right font-mono tabular-nums text-muted-foreground">
                        {entry ? `$${formatPrice(entry, asset ?? undefined)}` : '—'}
                      </td>
                      <td className="px-4 py-2.5 text-right font-mono tabular-nums text-muted-foreground">
                        {current ? `$${formatPrice(current, asset ?? undefined)}` : '—'}
                      </td>
                      <td className="px-4 py-2.5 text-right font-mono tabular-nums text-emerald-500/80">
                        {(() => {
                          const mfeVal = computeMFEForLedger(ev.action, entry ?? null, ev.max_price, ev.min_price)
                          return mfeVal !== null ? `${mfeVal >= 0 ? '+' : ''}${Math.round(mfeVal * 100) / 100}%` : '—'
                        })()}
                      </td>
                      <td
                        className={`px-4 py-2.5 text-right font-mono font-semibold tabular-nums ${
                          pnl?.positive ? 'text-long' : 'text-short'
                        }`}
                      >
                        {pnl ? `${pnl.positive ? '+' : ''}${pnl.pct}%` : '—'}
                      </td>
                      <td className="px-2 py-2.5 text-center">
                        <VerdictLabel verdict={ev.is_correct} />
                      </td>
                    </tr>

                    {/* Child rows — rendered when expanded */}
                    {isExpanded && children.map((child) => {
                      const childEntry = entryPrices?.[child.id] ?? child.entry_price ?? undefined
                      const childCurrentInfo = livePrices[`${child.target_asset}USDT`]
                      const childCurrent = childCurrentInfo?.price ?? 0
                      const childPnl = childEntry && childCurrent
                        ? computePnL(child.action, childEntry, childCurrent)
                        : null

                      return (
                        <tr
                          key={child.id}
                          className="bg-secondary/10 transition-colors hover:bg-secondary/30"
                        >
                          <td className="px-1 py-2 text-center">
                            <span className="text-[10px] text-muted-foreground/60 font-mono">
                              └
                            </span>
                          </td>
                          <td className="px-4 py-2 font-mono text-xs text-muted-foreground">
                            {child.reason?.slice(0, 40) || `#${child.id}`}
                          </td>
                          <td className="px-4 py-2">
                            <div className="flex items-center gap-1.5">
                              <SignalBadge signal={child.action} />
                              <span className="rounded bg-muted px-1 py-0 text-[9px] text-muted-foreground">
                                +1
                              </span>
                            </div>
                          </td>
                          <td className="px-4 py-2 text-right font-mono text-xs tabular-nums text-muted-foreground">
                            {childEntry ? `$${formatPrice(childEntry, child.target_asset ?? undefined)}` : '—'}
                          </td>
                          <td className="px-4 py-2 text-right font-mono text-xs tabular-nums text-muted-foreground">
                            {childCurrent ? `$${formatPrice(childCurrent, child.target_asset ?? undefined)}` : '—'}
                          </td>
                          <td className="px-4 py-2 text-right font-mono text-xs tabular-nums text-emerald-500/80">
                            {(() => {
                              const childMfe = computeMFEForLedger(child.action, childEntry ?? null, child.max_price, child.min_price)
                              return childMfe !== null ? `${childMfe >= 0 ? '+' : ''}${Math.round(childMfe * 100) / 100}%` : '—'
                            })()}
                          </td>
                          <td
                            className={`px-4 py-2 text-right font-mono text-xs tabular-nums ${
                              childPnl?.positive ? 'text-long' : 'text-short'
                            }`}
                          >
                            {childPnl ? `${childPnl.positive ? '+' : ''}${childPnl.pct}%` : '—'}
                          </td>
                          <td className="px-2 py-2 text-center">
                            <VerdictLabelSm verdict={child.is_correct} />
                          </td>
                        </tr>
                      )
                    })}

                    {/* Show message when children exist but none in current batch */}
                    {isExpanded && ev.child_count > 0 && children.length === 0 && (
                      <tr className="bg-secondary/10">
                        <td />
                        <td colSpan={7} className="px-4 py-2 text-xs text-muted-foreground">
                          聚合了 {ev.child_count} 个子信号（数据未在当前批次加载）
                        </td>
                      </tr>
                    )}
                  </React.Fragment>
                )
              })}
            </tbody>
          </table>
        )}
      </div>
    </section>
  )
}

