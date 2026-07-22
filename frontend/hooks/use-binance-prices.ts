'use client'

import { useCallback, useEffect, useRef, useState } from 'react'

export type PriceInfo = { price: number; change24h: number }
export type PriceMap = Record<string, PriceInfo>

const BINANCE_WS =
  'wss://stream.binance.com:9443/ws/btcusdt@ticker/ethusdt@ticker/solusdt@ticker'

// Stream name (lowercase) → our canonical asset code
const STREAM_TO_ASSET: Record<string, string> = {
  btcusdt: 'BTC',
  ethusdt: 'ETH',
  solusdt: 'SOL',
}

export function useBinancePrices() {
  const [prices, setPrices] = useState<PriceMap>({})
  const wsRef = useRef<WebSocket | null>(null)
  const reconnectTimer = useRef<ReturnType<typeof setTimeout> | null>(null)

  const connect = useCallback(() => {
    const ws = new WebSocket(BINANCE_WS)
    wsRef.current = ws

    ws.onmessage = (ev: MessageEvent) => {
      try {
        const data = JSON.parse(ev.data)
        const stream: string = (data.s ?? '').toLowerCase()
        const asset = STREAM_TO_ASSET[stream]
        if (!asset) return

        const price = parseFloat(data.c)
        const change24h = parseFloat(data.P)
        if (isNaN(price)) return

        setPrices((prev) => ({
          ...prev,
          [asset]: { price, change24h: isNaN(change24h) ? 0 : change24h },
        }))
      } catch {
        // ignore malformed frames
      }
    }

    ws.onclose = () => {
      reconnectTimer.current = setTimeout(connect, 3000)
    }

    ws.onerror = () => {
      ws.close()
    }
  }, [])

  useEffect(() => {
    connect()
    return () => {
      if (reconnectTimer.current) clearTimeout(reconnectTimer.current)
      if (wsRef.current) {
        wsRef.current.onclose = null
        wsRef.current.onerror = null
        wsRef.current.close()
        wsRef.current = null
      }
    }
  }, [connect])

  // Prices keyed by BTCUSDT / ETHUSDT / SOLUSDT for easy lookup
  const pricesByPair: PriceMap = {}
  for (const [asset, info] of Object.entries(prices)) {
    pricesByPair[`${asset}USDT`] = info
  }

  return { prices, pricesByPair }
}
