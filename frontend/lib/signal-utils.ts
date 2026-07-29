// 信号指标计算工具 — event-bus.tsx 与 signal-ledger.tsx 共用
// MFE = Max Favorable Excursion（最大浮盈%），MAE = Max Adverse Excursion（最大浮亏%）

export function computeMFE(
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

export function computeMAE(
  action: string,
  entry: number | null,
  max: number | null,
  min: number | null,
): number | null {
  if (!entry || entry <= 0) return null
  if (action === 'BUY' && min && min > 0) return ((min - entry) / entry) * 100
  if (action === 'SELL' && max && max > 0) return ((entry - max) / entry) * 100
  return null
}

export function computePnL(
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

export function getImpactThreshold(asset: string): number {
  const a = asset?.toUpperCase() || ''
  if (a === 'BTC') return 2.0
  if (a === 'XAU' || a === 'GOLD') return 1.0
  if (a === 'WTI') return 1.5
  return 2.0 // default
}

function _formatTimeDelta(entryTime: string, targetUnix: number): string | null {
  if (!targetUnix || targetUnix <= 0 || !entryTime) return null
  try {
    const etMs = Date.parse(entryTime)
    if (isNaN(etMs)) return null
    const etUnix = Math.floor(etMs / 1000)
    const minutes = Math.round(((targetUnix - etUnix) / 60) * 10) / 10
    if (minutes < 0) return null
    return `${minutes}`
  } catch {
    return null
  }
}

export function computeMFETime(action: string, entryTime: string, maxPtime: number, minPtime: number): string | null {
  return _formatTimeDelta(entryTime, action === 'BUY' ? maxPtime : minPtime)
}

export function computeMAETime(action: string, entryTime: string, maxPtime: number, minPtime: number): string | null {
  return _formatTimeDelta(entryTime, action === 'BUY' ? minPtime : maxPtime)
}

export function calculateHeatScore(clusterSize: number): number {
  if (clusterSize <= 1) return 0
  return Math.min(99, Math.floor(100 * (1 - Math.exp(-0.4 * (clusterSize - 1)))))
}
