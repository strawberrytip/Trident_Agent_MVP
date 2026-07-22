export type Signal = 'BUY' | 'SELL' | 'HOLD'

export type MarketCategory = 'CRYPTO' | 'GOLD' | 'OIL' | 'MACRO' | 'OTHER' | 'ALL'

export type ApiEvent = {
  id: number
  timestamp: string
  ai_time: string
  source: string
  news_text: string
  action: Signal
  score: number
  reason: string
  market_category: MarketCategory
  target_asset: string
  parent_id: number | null
  child_count: number
  reasoning_path: string
  vip_tag: string
  entry_price: number | null
  exit_price: number | null
  max_price: number | null
  min_price: number | null
  max_price_time: number
  min_price_time: number
  entry_time: string
  is_correct: string
  settled: number
  doubao_action: Signal
  doubao_reasoning: string
  cluster_size: number
}

export type SentimentPoint = {
  time: string
  score: number
}

export const MARKET_TABS: { key: MarketCategory; label: string }[] = [
  { key: 'ALL',    label: 'All' },
  { key: 'CRYPTO', label: 'Crypto' },
  { key: 'GOLD',   label: 'Gold' },
  { key: 'OIL',    label: 'Oil' },
  { key: 'MACRO',  label: 'Macro' },
]
