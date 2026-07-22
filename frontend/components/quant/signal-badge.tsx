import { ArrowDownRight, ArrowUpRight, Minus } from 'lucide-react'
import type { Signal } from '@/lib/quant-data'
import { cn } from '@/lib/utils'

const config: Record<
  Signal,
  { label: string; className: string; Icon: typeof ArrowUpRight }
> = {
  BUY: {
    label: 'BUY',
    className: 'border-long/40 bg-long/10 text-long',
    Icon: ArrowUpRight,
  },
  SELL: {
    label: 'SELL',
    className: 'border-short/40 bg-short/10 text-short',
    Icon: ArrowDownRight,
  },
  HOLD: {
    label: 'HOLD',
    className: 'border-hold/40 bg-hold/10 text-hold',
    Icon: Minus,
  },
}

export function SignalBadge({
  signal,
  className,
}: {
  signal: Signal
  className?: string
}) {
  const { label, className: tone, Icon } = config[signal]
  return (
    <span
      className={cn(
        'inline-flex items-center gap-1 rounded-md border px-2 py-0.5 font-mono text-xs font-bold tracking-wider tabular-nums',
        tone,
        className,
      )}
    >
      <Icon className="h-3 w-3" aria-hidden="true" />
      {label}
    </span>
  )
}
