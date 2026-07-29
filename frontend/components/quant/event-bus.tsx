'use client'

import { useCallback, useEffect, useRef, useState } from 'react'
import { ChevronDown, ChevronRight, Download, Radio, Wifi, WifiOff } from 'lucide-react'
import type { ApiEvent } from '@/lib/quant-data'
import { computeMFE, computeMAE, computeMFETime, computeMAETime, getImpactThreshold, calculateHeatScore } from '@/lib/signal-utils'
import { SignalBadge } from './signal-badge'

const API_BASE = process.env.NEXT_PUBLIC_API_URL || ''
const SSE_URL = `${API_BASE}/api/events/stream`
const MAX_EVENTS = 100

type Props = {
  onEventsUpdated?: (events: ApiEvent[]) => void
  onPriceUpdate?: (asset: string, price: number) => void
}

function VipBadge({ tag }: { tag: string }) {
  const label = tag.replace('[', '').replace(']', '')
  let color = 'border-amber-500/50 bg-amber-500/10 text-amber-500'
  if (tag.includes('FED')) color = 'border-blue-500/50 bg-blue-500/10 text-blue-500'
  if (tag.includes('MUSK')) color = 'border-purple-500/50 bg-purple-500/10 text-purple-500'
  return (
    <span className={'inline-flex items-center rounded border px-1.5 py-0 text-[9px] font-bold uppercase tracking-wider ' + color}>
      {label}
    </span>
  )
}

function VerdictBadge({ verdict }: { verdict: string }) {
  if (!verdict) return null
  let color = 'border-muted-foreground/30 bg-muted/20 text-muted-foreground'
  if (verdict === 'WIN') color = 'border-emerald-500/50 bg-emerald-500/10 text-emerald-500'
  if (verdict === 'LOSS') color = 'border-red-500/50 bg-red-500/10 text-red-500'
  return (
    <span className={'inline-flex items-center rounded border px-1.5 py-0 text-[9px] font-bold uppercase tracking-wider ' + color}>
      {verdict === 'WIN' ? 'CORRECT' : verdict === 'LOSS' ? 'WRONG' : verdict}
    </span>
  )
}

function HeatBadge({ size }: { size: number }) {
  if (size < 2) return null
  const score = calculateHeatScore(size)
  let colors: string
  if (score > 80) {
    colors = 'border-red-500/60 bg-red-500/15 text-red-400'
  } else if (score > 50) {
    colors = 'border-amber-500/50 bg-amber-500/10 text-amber-500'
  } else {
    colors = 'border-muted-foreground/30 bg-muted/20 text-muted-foreground'
  }
  return (
    <span className={'inline-flex items-center gap-1 whitespace-nowrap rounded border px-1.5 py-0 text-[10px] font-bold tracking-wider ' + colors}>
      🔥 热度 {score} (关联 {size} 源)
    </span>
  )
}

function DoubaoBadge({ action, reason }: { action: string; reason: string }) {
  const disagree = action && action !== 'HOLD'
  return (
    <span
      className="inline-flex items-center gap-1 rounded border border-sky-500/40 bg-sky-500/8 px-1.5 py-0 text-[9px] font-bold uppercase tracking-wider text-sky-500"
      title={reason?.slice(0, 200) || 'Doubao secondary verification'}
    >
      DB {action}
      {disagree && <span className="inline-block h-1 w-1 rounded-full bg-amber-400" title="Model disagreement" />}
    </span>
  )
}

export function EventBus({ onEventsUpdated, onPriceUpdate }: Props) {
  const [events, setEvents] = useState<ApiEvent[]>([])
  const [error, setError] = useState<string | null>(null)
  const [connected, setConnected] = useState(false)
  const [expandedIds, setExpandedIds] = useState<Set<number>>(new Set())

  const esRef = useRef<EventSource | null>(null)
  const mountedRef = useRef(true)
  const onEventsRef = useRef(onEventsUpdated)
  onEventsRef.current = onEventsUpdated
  const onPriceRef = useRef(onPriceUpdate)
  onPriceRef.current = onPriceUpdate

  const handleSseMessage = useCallback((ev: MessageEvent) => {
    if (!mountedRef.current) return
    try {
      const parsed = JSON.parse(ev.data)
      if (parsed.type === 'price_update' && parsed.asset) {
        onPriceRef.current?.(parsed.asset, parsed.price)
        return
      }
      if (typeof parsed.id === 'number') {
        setEvents((prev) => {
          if (prev.some((e) => e.id === parsed.id)) return prev
          const next = [parsed as ApiEvent, ...prev]
          if (next.length > MAX_EVENTS) next.length = MAX_EVENTS
          return next
        })
      }
    } catch {
      // ignore
    }
  }, [])

  useEffect(() => {
    mountedRef.current = true
    async function loadHistory() {
      try {
        const res = await fetch(`${API_BASE}/api/events`)
        if (!res.ok) throw new Error(`HTTP ${res.status}`)
        const data: ApiEvent[] = await res.json()
        if (mountedRef.current) { setEvents(data); setError(null) }
      } catch (err) {
        if (mountedRef.current) {
          setError(err instanceof Error ? err.message : 'fetch failed')
        }
      }
    }
    loadHistory()
    const es = new EventSource(SSE_URL)
    esRef.current = es
    es.onopen = () => { if (mountedRef.current) { setConnected(true); setError(null) } }
    es.onmessage = handleSseMessage
    es.onerror = () => { if (mountedRef.current) setConnected(false) }
    return () => {
      mountedRef.current = false
      es.close()
      esRef.current = null
    }
  }, [handleSseMessage])

  useEffect(() => {
    onEventsRef.current?.(events)
  }, [events])

  const toggleExpand = (id: number) => {
    setExpandedIds((prev) => {
      const next = new Set(prev)
      next.has(id) ? next.delete(id) : next.add(id)
      return next
    })
  }

  const exportExcel = () => {
    window.open(`${API_BASE}/api/export/excel`, '_blank')
  }

  return (
    <section className="glass flex h-full flex-col rounded-lg border border-border" aria-label="real time event bus">
      <div className="flex items-center justify-between border-b border-border px-4 py-3">
        <div className="flex items-center gap-2">
          <Radio className="h-4 w-4 text-primary" aria-hidden="true" />
          <h2 className="text-sm font-semibold text-foreground">real time event bus</h2>
        </div>
        <div className="flex items-center gap-3">
          <button
            onClick={exportExcel}
            className="inline-flex items-center gap-1.5 rounded border border-border px-2 py-1 text-[10px] font-medium text-muted-foreground hover:text-foreground hover:border-primary/50 transition-colors"
            title="Export daily report"
          >
            <Download className="h-3 w-3" />
            Report
          </button>
          {connected ? <Wifi className="h-3.5 w-3.5 text-long" /> : <WifiOff className="h-3.5 w-3.5 text-muted-foreground" />}
          <span className="font-mono text-[10px] uppercase tracking-widest text-muted-foreground">live</span>
        </div>
      </div>
      <ol className="thin-scroll flex-1 divide-y divide-border overflow-y-auto">
        {error && events.length === 0 && (
          <li className="px-4 py-6 text-center text-sm text-muted-foreground">connecting {error}</li>
        )}
        {!error && events.length === 0 && (
          <li className="px-4 py-6 text-center text-sm text-muted-foreground">waiting for data</li>
        )}
        {events.map((item) => {
          const open = expandedIds.has(item.id)
          const hasPath = !!(item.reasoning_path && item.reasoning_path.trim())
          const hasTracking = !!(item.entry_price && item.entry_price > 0)
          const hasDoubao = !!(item.doubao_action && item.doubao_action !== 'HOLD')
          const expandable = hasPath || hasTracking || (!!(item.doubao_reasoning && item.doubao_reasoning.trim()))
          return (
            <li key={item.id} onClick={() => expandable && toggleExpand(item.id)}
              className={expandable ? 'cursor-pointer transition-colors hover:bg-secondary/30' + (open ? ' bg-secondary/20' : '') : ''}>
              <div className="px-4 py-3">
                <div className="mb-1.5 flex items-start justify-between gap-4">
                  <div className="flex items-center gap-2 min-w-0">
                    <span className="font-mono text-[11px] tabular-nums text-muted-foreground shrink-0">news {item.timestamp}</span>
                    <span className="text-[10px] text-muted-foreground/50 shrink-0">.</span>
                    <span className="font-mono text-[10px] tabular-nums text-muted-foreground/60 shrink-0">AI {item.ai_time}</span>
                    {expandable && (
                      <span className="inline-flex h-4 w-4 items-center justify-center rounded text-muted-foreground/40 group-hover:text-muted-foreground/70">
                        {open ? <ChevronDown className="h-3 w-3" /> : <ChevronRight className="h-3 w-3" />}
                      </span>
                    )}
                  </div>
                  <div className="flex items-center gap-1.5 flex-wrap justify-end shrink-0">
                    <SignalBadge signal={item.action} />
                    {item.vip_tag && <VipBadge tag={item.vip_tag} />}
                    {hasDoubao && <DoubaoBadge action={item.doubao_action} reason={item.doubao_reasoning} />}
                    {item.settled === 1 && item.is_correct && <VerdictBadge verdict={item.is_correct} />}
                  </div>
                </div>
                <p className="text-lg font-bold leading-snug text-foreground/90 text-pretty">{item.news_text}</p>
                <HeatBadge size={item.cluster_size ?? 1} />
                <div className="mt-1.5 flex items-center justify-between gap-2">
                  <span className="font-mono text-[11px] tabular-nums text-muted-foreground">
                    score {item.score > 0 ? '+' : ''}{item.score.toFixed(2)}
                  </span>
                  {item.reason && (
                    <p className="max-w-[70%] truncate text-[11px] leading-relaxed text-muted-foreground">AI {item.reason}</p>
                  )}
                </div>
              </div>
              {open && (
                <div className="border-t border-border/50 bg-black/[0.03] dark:bg-white/[0.02]">
                  {hasPath && (
                    <div className="px-4 py-2.5 border-b border-border/30">
                      <div className="mb-1.5 flex items-center gap-2">
                        <span className="font-mono text-[10px] uppercase tracking-wider text-indigo-400/70">Kimi K3</span>
                        {item.action && (
                          <span className={'font-mono text-[10px] font-bold uppercase tracking-wider ' + (item.action === 'BUY' ? 'text-long' : item.action === 'SELL' ? 'text-short' : 'text-muted-foreground')}>
                            {item.action}
                          </span>
                        )}
                      </div>
                      <pre className="whitespace-pre-wrap break-words font-mono text-[11px] leading-relaxed text-muted-foreground/80 selection:bg-primary/20">{item.reasoning_path}</pre>
                    </div>
                  )}
                  {item.doubao_reasoning && item.doubao_reasoning.trim() && (
                    <div className="px-4 py-2.5 border-b border-border/30">
                      <div className="mb-1.5 flex items-center gap-2">
                        <span className="font-mono text-[10px] uppercase tracking-wider text-sky-500/70">Doubao</span>
                        {item.doubao_action && (
                          <span className={'font-mono text-[10px] font-bold uppercase tracking-wider ' + (item.doubao_action === 'BUY' ? 'text-long' : item.doubao_action === 'SELL' ? 'text-short' : 'text-muted-foreground')}>
                            {item.doubao_action}
                          </span>
                        )}
                      </div>
                      <pre className="whitespace-pre-wrap break-words font-mono text-[11px] leading-relaxed text-muted-foreground/80 selection:bg-primary/20">{item.doubao_reasoning}</pre>
                    </div>
                  )}
                  {hasTracking && (
                    <div className="px-4 py-2.5">
                      <div className="mb-2 font-mono text-[10px] uppercase tracking-wider text-muted-foreground/60">
                        2h forward test {item.settled === 0 && '(tracking...)'}
                      </div>
                      <div className="grid grid-cols-4 gap-x-3 gap-y-1 font-mono text-[11px]">
                        <span className="text-muted-foreground/60">Entry</span>
                        <span className="text-right tabular-nums text-foreground/90">${item.entry_price?.toFixed(2) ?? '—'}</span>
                        <span className="text-muted-foreground/60">Max</span>
                        <span className="text-right tabular-nums text-emerald-500/80">${item.max_price?.toFixed(2) ?? '—'}</span>
                        <span className="text-muted-foreground/60">Exit</span>
                        <span className="text-right tabular-nums text-foreground/90">
                          {item.settled ? '$' + (item.exit_price?.toFixed(2) ?? '—') : 'pending'}
                        </span>
                        <span className="text-muted-foreground/60">Min</span>
                        <span className="text-right tabular-nums text-red-500/80">${item.min_price?.toFixed(2) ?? '—'}</span>
                      </div>
                      {(() => {
                        const entry = item.entry_price ?? 0
                        const max = item.max_price ?? 0
                        const min = item.min_price ?? 0
                        const mfe = entry > 0 ? computeMFE(item.action, entry, max, min) : null
                        const mae = entry > 0 ? computeMAE(item.action, entry, max, min) : null
                        const mfeTime = computeMFETime(item.action, item.entry_time, item.max_price_time, item.min_price_time)
                        const maeTime = computeMAETime(item.action, item.entry_time, item.max_price_time, item.min_price_time)
                        const threshold = getImpactThreshold(item.target_asset)
                        const isHighImpact = mfe !== null && mfe > threshold
                        return (
                          <>
                            {(mfe !== null || mae !== null) && (
                              <div className="mt-1.5 flex items-center gap-4 font-mono text-[10px]">
                                {mfe !== null && (
                                  <span>
                                    <span className="text-muted-foreground/60">最大浮盈 </span>
                                    <span className="tabular-nums text-emerald-500/80">{mfe >= 0 ? '+' : ''}{mfe.toFixed(2)}%</span>
                                    {mfeTime && <span className="tabular-nums text-muted-foreground/50"> ({mfeTime}m)</span>}
                                  </span>
                                )}
                                {mae !== null && (
                                  <span>
                                    <span className="text-muted-foreground/60">最大浮亏 </span>
                                    <span className="tabular-nums text-red-500/80">{mae >= 0 ? '+' : ''}{mae.toFixed(2)}%</span>
                                    {maeTime && <span className="tabular-nums text-muted-foreground/50"> ({maeTime}m)</span>}
                                  </span>
                                )}
                              </div>
                            )}
                            {isHighImpact && (
                              <div className="mt-1.5 inline-flex items-center gap-1.5 rounded border border-amber-500/50 bg-amber-500/10 px-2 py-0.5">
                                <span className="text-[10px]">🔥</span>
                                <span className="font-mono text-[9px] font-bold tracking-wider text-amber-500">
                                  强影响
                                </span>
                              </div>
                            )}
                          </>
                        )
                      })()}
                      {item.settled === 1 && item.is_correct && (
                        <div className="mt-2 flex items-center gap-2">
                          <span className="text-[10px] text-muted-foreground/60">Verdict</span>
                          <VerdictBadge verdict={item.is_correct} />
                        </div>
                      )}
                    </div>
                  )}
                  {!hasPath && !hasTracking && !(!!(item.doubao_reasoning && item.doubao_reasoning.trim())) && (
                    <div className="px-4 py-2.5 font-mono text-[10px] italic text-muted-foreground/40">no detail available</div>
                  )}
                </div>
              )}
            </li>
          )
        })}
      </ol>
    </section>
  )
}
