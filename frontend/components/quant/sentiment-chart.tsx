'use client'

import { useMemo } from 'react'
import { TrendingUp } from 'lucide-react'
import {
  Area,
  AreaChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import type { ApiEvent, MarketCategory, SentimentPoint } from '@/lib/quant-data'

type Props = {
  events: ApiEvent[]
  activeMarket: MarketCategory
}

const LABELS: Record<string, string> = {
  ALL: '全市场',
  CRYPTO: '加密货币',
  GOLD: '黄金',
  OIL: '原油',
  MACRO: '宏观',
  OTHER: '其他',
}

function ChartTooltip({
  active,
  payload,
  label,
}: {
  active?: boolean
  payload?: Array<{ value: number }>
  label?: string
}) {
  if (!active || !payload?.length) return null
  const score = payload[0].value
  const mood = score >= 60 ? '看涨' : score <= 40 ? '看跌' : '中性'
  return (
    <div className="glass rounded-md border border-border px-3 py-2 shadow-lg">
      <p className="font-mono text-[11px] text-muted-foreground">{label}</p>
      <p className="font-mono text-sm font-bold text-primary tabular-nums">
        {score} · {mood}
      </p>
    </div>
  )
}

export function SentimentChart({ events, activeMarket }: Props) {
  const filtered = activeMarket === 'ALL'
    ? events
    : events.filter((e) => e.market_category === activeMarket)

  const { series, latestScore, change } = useMemo(() => {
    if (filtered.length === 0) {
      return { series: [] as SentimentPoint[], latestScore: 0, change: 0 }
    }

    // Build chart points — one per event, using accumulated average
    const points: SentimentPoint[] = []
    let sum = 0
    filtered.forEach((ev) => {
      sum += ev.score
      const avg = Math.round(((sum / (points.length + 1)) * 0.5 + 0.5) * 100)
      points.push({ time: ev.timestamp, score: Math.max(0, Math.min(100, avg)) })
    })

    const latest = points[points.length - 1]?.score ?? 0
    const prev = points.length > 1 ? points[points.length - 2]?.score ?? latest : latest
    return { series: points, latestScore: latest, change: latest - prev }
  }, [filtered])

  const label = LABELS[activeMarket] ?? activeMarket

  return (
    <section className="glass flex h-full flex-col rounded-lg border border-border p-4 md:p-5">
      <div className="mb-4 flex items-start justify-between">
        <div>
          <div className="flex items-center gap-2">
            <TrendingUp className="h-4 w-4 text-primary" aria-hidden="true" />
            <h2 className="text-sm font-semibold text-foreground">
              AI 情绪评分走势 - {label}
            </h2>
          </div>
          <p className="mt-1 text-xs text-muted-foreground">
            基于事件总线聚合的模型置信度 · 0&ndash;100
          </p>
        </div>
        <div className="text-right">
          <p className="font-mono text-3xl font-bold text-long tabular-nums">
            {latestScore}
          </p>
          <p className="font-mono text-[11px] text-long">
            今日 {change >= 0 ? '+' : ''}{change}
          </p>
        </div>
      </div>

      <div className="min-h-0 flex-1">
        {series.length === 0 ? (
          <div className="flex h-full items-center justify-center text-sm text-muted-foreground">
            暂无 {label} 数据
          </div>
        ) : (
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart
              data={series}
              margin={{ top: 8, right: 8, left: -16, bottom: 0 }}
            >
              <defs>
                <linearGradient id="sentimentFill" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor="var(--long)" stopOpacity={0.45} />
                  <stop offset="100%" stopColor="var(--long)" stopOpacity={0} />
                </linearGradient>
              </defs>
              <CartesianGrid stroke="var(--grid)" vertical={false} />
              <XAxis
                dataKey="time"
                stroke="var(--muted-foreground)"
                tick={{ fontSize: 11, fontFamily: 'var(--font-mono)' }}
                tickLine={false}
                axisLine={{ stroke: 'var(--border)' }}
              />
              <YAxis
                domain={[0, 100]}
                stroke="var(--muted-foreground)"
                tick={{ fontSize: 11, fontFamily: 'var(--font-mono)' }}
                tickLine={false}
                axisLine={false}
                width={40}
              />
              <Tooltip
                content={<ChartTooltip />}
                cursor={{ stroke: 'var(--long)', strokeOpacity: 0.3, strokeWidth: 1 }}
              />
              <Area
                type="monotone"
                dataKey="score"
                stroke="var(--long)"
                strokeWidth={2.5}
                fill="url(#sentimentFill)"
                isAnimationActive={false}
                dot={false}
                activeDot={{
                  r: 4,
                  fill: 'var(--long)',
                  stroke: 'var(--background)',
                  strokeWidth: 2,
                }}
              />
            </AreaChart>
          </ResponsiveContainer>
        )}
      </div>
    </section>
  )
}
