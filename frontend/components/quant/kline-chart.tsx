'use client'

import { useCallback, useEffect, useRef, useState } from 'react'
import { TrendingUp } from 'lucide-react'
import {
  createChart,
  createSeriesMarkers,
  ColorType,
  CrosshairMode,
  CandlestickSeries,
  HistogramSeries,
  type IChartApi,
  type ISeriesApi,
  type CandlestickData,
  type HistogramData,
  type Time,
  type SeriesMarker,
  type MouseEventParams,
} from 'lightweight-charts'
import type { ApiEvent } from '@/lib/quant-data'

/* ------------------------------------------------------------------ */
/*  Labels                                                             */
/* ------------------------------------------------------------------ */

const SYMBOL_LABELS: Record<string, string> = {
  BTCUSDT: 'BTC/USDT',
  XAUUSD: 'XAU/USD',
}

type Props = {
  symbol: string
  events: ApiEvent[]
}

/* ------------------------------------------------------------------ */
/*  Helpers                                                            */
/* ------------------------------------------------------------------ */

function symbolToKlineId(symbol: string): string {
  if (symbol.includes('BTC')) return 'BTCUSDT'
  if (symbol.includes('XAU')) return 'XAUUSD'
  return symbol.split(':').pop() ?? symbol
}

function assetToKlineId(asset: string): string | null {
  const a = asset.toUpperCase()
  if (a === 'BTC') return 'BTCUSDT'
  if (a === 'XAU' || a === 'GOLD') return 'XAUUSD'
  return null
}

/** Parse HH:MM:SS → Unix seconds (today). Returns 0 on failure. */
function eventTimeToSeconds(ev: ApiEvent): number {
  const t = ev.timestamp
  if (!t) return 0
  const m = /(\d{1,2}):(\d{2}):(\d{2})/.exec(t)
  if (!m) return 0
  const now = new Date()
  const d = new Date(now.getFullYear(), now.getMonth(), now.getDate(), +m[1], +m[2], +m[3])
  return Math.floor(d.getTime() / 1000)
}

/** Snap a second-precision Unix ts down to the nearest hour boundary. */
function snapToHour(ts: number): number {
  return Math.floor(ts / 3600) * 3600
}

function escHtml(s: string): string {
  return s
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
}

interface MarkerTooltip {
  time: number
  newsText: string
  reasoning: string
  action: 'BUY' | 'SELL'
  score: number
}

/* ================================================================== */
/*  Component                                                          */
/* ================================================================== */

export function KlineChart({ symbol, events }: Props) {
  const containerRef = useRef<HTMLDivElement>(null)
  const tooltipRef = useRef<HTMLDivElement>(null)
  const chartRef = useRef<IChartApi | null>(null)
  const cancelsRef = useRef<(() => void)[]>([])
  const tooltipsRef = useRef<MarkerTooltip[]>([])
  const [ready, setReady] = useState(false)
  const [errMsg, setErrMsg] = useState<string | null>(null)

  const klineId = symbolToKlineId(symbol)
  const label = SYMBOL_LABELS[klineId] ?? klineId

  /* ---------- fetch + sort ---------- */

  const fetchSortedKlines = useCallback(async (): Promise<{
    data: CandlestickData[]
    timeSet: Set<number>
  }> => {
    const resp = await fetch(`http://localhost:8000/api/klines?symbol=${klineId}&limit=200`)
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`)
    const raw = await resp.json()

    // 1. coerce time to Number, 2. sort ascending — mandatory for lw-charts
    const sorted: CandlestickData[] = (raw as any[])
      .map((r) => ({
        time: Number(r.time) as Time,
        open: r.open,
        high: r.high,
        low: r.low,
        close: r.close,
      }))
      .sort((a, b) => (a.time as number) - (b.time as number))

    const timeSet = new Set<number>()
    for (const c of sorted) timeSet.add(c.time as number)

    return { data: sorted, timeSet }
  }, [klineId])

  /* ---------- markers (hour-snapped + safety-filtered) ---------- */

  const buildMarkers = useCallback(
    (evts: ApiEvent[], candleTimeSet: Set<number>): {
      markers: SeriesMarker<Time>[]
      tooltips: MarkerTooltip[]
    } => {
      const markers: SeriesMarker<Time>[] = []
      const tooltips: MarkerTooltip[] = []

      for (const ev of evts) {
        if (ev.action !== 'BUY' && ev.action !== 'SELL') continue
        const ak = assetToKlineId(ev.target_asset)
        if (!ak || ak !== klineId) continue

        const rawTs = eventTimeToSeconds(ev)
        if (rawTs === 0) continue

        // floor-align to the nearest hour boundary: 15:19 → 15:00
        const snapped = snapToHour(rawTs)

        // safety: only place markers on bars that actually exist
        if (!candleTimeSet.has(snapped)) continue

        const isBuy = ev.action === 'BUY'
        markers.push({
          time: snapped as Time,
          position: isBuy ? 'belowBar' : 'aboveBar',
          color: isBuy ? '#00b061' : '#ef4444',
          shape: isBuy ? 'arrowUp' : 'arrowDown',
          text: isBuy ? 'AI BUY' : 'AI SELL',
          size: 2,
        })

        tooltips.push({
          time: snapped,
          newsText: ev.news_text ?? '',
          reasoning: ev.reasoning_path ?? ev.reason ?? '',
          action: ev.action as 'BUY' | 'SELL',
          score: ev.score,
        })
      }
      return { markers, tooltips }
    },
    [klineId],
  )

  /* ---------- main effect ---------- */

  useEffect(() => {
    const container = containerRef.current
    if (!container) return

    let disposed = false
    const cancels: (() => void)[] = []
    cancelsRef.current = cancels

    async function init() {
      const chart = createChart(container!, {
        layout: {
          background: { type: ColorType.Solid, color: '#ffffff' },
          textColor: '#333333',
        },
        grid: {
          vertLines: { color: '#f0f0f0' },
          horzLines: { color: '#f0f0f0' },
        },
        crosshair: { mode: CrosshairMode.Normal },
        timeScale: {
          timeVisible: true,
          secondsVisible: false,
          borderColor: '#e5e7eb',
        },
        rightPriceScale: {
          borderColor: '#e5e7eb',
          scaleMargins: { top: 0.1, bottom: 0.2 },
        },
        width: container!.clientWidth,
        height: container!.clientHeight,
      })
      if (disposed) { chart.remove(); return }
      chartRef.current = chart

      const candleSeries = chart.addSeries(CandlestickSeries, {
        upColor: '#00b061',
        downColor: '#ef4444',
        borderUpColor: '#00b061',
        borderDownColor: '#ef4444',
        wickUpColor: '#00b061',
        wickDownColor: '#ef4444',
      })

      const volSeries = chart.addSeries(HistogramSeries, {
        color: '#d1d5db',
        priceFormat: { type: 'volume' },
        priceScaleId: '',
      })
      volSeries.priceScale().applyOptions({ scaleMargins: { top: 0.8, bottom: 0 } })

      const ro = new ResizeObserver(() => {
        if (container!.clientWidth > 0 && container!.clientHeight > 0) {
          chart.applyOptions({ width: container!.clientWidth, height: container!.clientHeight })
        }
      })
      ro.observe(container!)
      cancels.push(() => ro.disconnect())

      /* -- fetch sorted klines -- */
      const { data: klines, timeSet } = await fetchSortedKlines()
      if (disposed) return

      if (klines.length > 0) {
        candleSeries.setData(klines)
        const vols: HistogramData[] = klines.map((c) => ({
          time: c.time,
          value: (c as any).volume ?? 0,
          color: 'rgba(209,213,219,0.5)',
        }))
        volSeries.setData(vols)
      }

      /* -- markers (snapped to hour, safety-filtered) -- */
      const { markers, tooltips } = buildMarkers(events, timeSet)
      if (markers.length > 0) {
        createSeriesMarkers(candleSeries, markers)
      }
      tooltipsRef.current = tooltips

      /* -- crosshair tooltip -- */
      const tpEl = tooltipRef.current
      chart.subscribeCrosshairMove((param: MouseEventParams) => {
        if (!tpEl) return
        if (!param.time || tooltips.length === 0) {
          tpEl.style.display = 'none'
          return
        }
        const t = param.time as number
        const hit = tooltips.find((m) => m.time === t)
        if (hit) {
          const x = (param.point?.x ?? 0) + 20
          const y = (param.point?.y ?? 0) - 10
          tpEl.style.left = `${Math.min(x, (container!.clientWidth ?? 400) - 260)}px`
          tpEl.style.top = `${Math.max(y - 150, 10)}px`
          tpEl.style.display = 'block'
          tpEl.innerHTML = [
            `<div style="font-size:11px;font-weight:600;margin-bottom:4px;color:${hit.action === 'BUY' ? '#00b061' : '#ef4444'}">`,
            `${hit.action} · 评分 ${hit.score > 0 ? '+' : ''}${hit.score.toFixed(2)}`,
            '</div>',
            `<div style="font-size:12px;line-height:1.5;margin-bottom:6px;color:#1f2937">${escHtml(hit.newsText)}</div>`,
            `<div style="font-size:11px;line-height:1.5;color:#6b7280;max-height:120px;overflow-y:auto">${escHtml(hit.reasoning || '(无思维链)')}</div>`,
          ].join('')
        } else {
          tpEl.style.display = 'none'
        }
      })

      chart.timeScale().fitContent()
      if (!disposed) setReady(true)
    }

    init().catch((e: any) => {
      if (!disposed) setErrMsg(e?.message ?? String(e))
    })

    return () => {
      disposed = true
      for (const cancel of cancels) cancel()
      if (chartRef.current) {
        chartRef.current.remove()
        chartRef.current = null
      }
    }
  }, [events, klineId, fetchSortedKlines, buildMarkers])

  /* ---------- render ---------- */

  if (errMsg) {
    return (
      <section className="glass flex h-full flex-col overflow-hidden rounded-lg border border-border">
        <div className="flex shrink-0 items-center gap-2 border-b border-border px-4 py-2.5">
          <TrendingUp className="h-4 w-4 text-primary" />
          <h2 className="text-sm font-semibold text-foreground">{label} · 1H K线</h2>
        </div>
        <div className="flex flex-col flex-1 items-center justify-center gap-1 text-sm text-muted-foreground">
          <span>图表加载失败</span>
          <span className="font-mono text-xs text-red-400">{errMsg}</span>
        </div>
      </section>
    )
  }

  return (
    <section className="glass flex h-full flex-col overflow-hidden rounded-lg border border-border">
      <div className="flex shrink-0 items-center gap-2 border-b border-border px-4 py-2.5">
        <TrendingUp className="h-4 w-4 text-primary" />
        <h2 className="text-sm font-semibold text-foreground">{label} · 1H K线</h2>
        <span className="ml-auto inline-flex items-center rounded-full bg-primary/10 px-2 py-0.5 font-mono text-[10px] text-primary">
          {label}
        </span>
      </div>

      <div className="relative min-h-0 flex-1" style={{ overflow: 'hidden' }}>
        <div ref={containerRef} style={{ width: '100%', height: '100%' }} />

        <div
          ref={tooltipRef}
          className="pointer-events-none absolute z-50 hidden rounded-md border border-border bg-white/95 px-3 py-2.5 shadow-lg"
          style={{ maxWidth: 280, minWidth: 200 }}
        />

        {!ready && (
          <div className="pointer-events-none absolute inset-0 flex items-center justify-center bg-white/60">
            <div className="flex items-center gap-3 rounded-lg bg-white px-4 py-3 shadow-sm">
              <div className="h-4 w-4 animate-spin rounded-full border-2 border-primary border-t-transparent" />
              <span className="text-sm text-muted-foreground">加载 K 线中...</span>
            </div>
          </div>
        )}
      </div>
    </section>
  )
}
