"""Forward tracker — 2-hour simulated trade verification.

Tracks max/min, settles with WIN/LOSS verdict.
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime
from typing import List

from config import IMPACT_THRESHOLD, TZ_SHANGHAI

from .prices import _get_current_price
from .utils import _now, _open_db


_FORWARD_DURATION_HOURS = 2


async def forward_tracker() -> None:
    """2-hour simulated trade verification. Tracks max/min, settles with WIN/LOSS verdict."""
    print("[TRACKER] Forward tracker started - 2h verification window")
    loop = asyncio.get_running_loop()

    while True:
        try:

            def _track_cycle():
                updates = []
                conn = _open_db()
                try:
                    rows = conn.execute(
                        "SELECT id, suggested_action, target_asset, entry_price,"
                        " max_price, min_price, max_price_time, min_price_time, entry_time, settled"
                        " FROM ai_decisions"
                        " WHERE settled = 0 AND entry_price IS NOT NULL AND entry_time != ''"
                    ).fetchall()

                    # Asset-specific impact thresholds for verdict ruling — IMPACT_THRESHOLD 见 config.py
                    for row in rows:
                        eid = row["id"]
                        action = (row["suggested_action"] or "").upper()
                        asset_raw = (row["target_asset"] or "NONE").upper()
                        entry = row["entry_price"]
                        cur_max = row["max_price"]
                        cur_min = row["min_price"]
                        max_ptime = row["max_price_time"] or 0
                        min_ptime = row["min_price_time"] or 0
                        entry_ts = row["entry_time"]

                        try:
                            et = datetime.fromisoformat(entry_ts)
                        except (ValueError, TypeError):
                            continue

                        elapsed = datetime.now(TZ_SHANGHAI) - et
                        price = _get_current_price(asset_raw)  # used for tracking only

                        if elapsed.total_seconds() >= _FORWARD_DURATION_HOURS * 3600:
                            exit_p = price if price is not None else entry
                            entry_unix = int(et.timestamp())

                            # ── Impact metrics (defensive against zero entry) ──
                            mfe_pct: float = 0.0
                            mae_pct: float = 0.0
                            mfe_time_mins: float = 0.0

                            if entry and entry > 0:
                                if action == "BUY":
                                    if cur_max and cur_max > 0:
                                        mfe_pct = (cur_max - entry) / entry * 100
                                    if cur_min and cur_min > 0:
                                        mae_pct = (entry - cur_min) / entry * 100
                                    if max_ptime > 0:
                                        mfe_time_mins = (max_ptime - entry_unix) / 60
                                elif action == "SELL":
                                    if cur_min and cur_min > 0:
                                        mfe_pct = (entry - cur_min) / entry * 100
                                    if cur_max and cur_max > 0:
                                        mae_pct = (entry - cur_max) / entry * 100
                                    if min_ptime > 0:
                                        mfe_time_mins = (min_ptime - entry_unix) / 60

                            # ── Get asset-specific threshold ──
                            threshold = IMPACT_THRESHOLD.get(asset_raw, 1.0)

                            # ── 3D empirical verdict decision tree ──
                            if mfe_pct < threshold and mae_pct < threshold:
                                # Condition A: neither side exceeded threshold
                                verdict = "HOLD"    # NO_IMPACT — market did not break window
                            elif mfe_pct >= threshold and mfe_time_mins <= 45 and mfe_pct > mae_pct:
                                # Condition B: favourable excursion hit hard & fast
                                verdict = "WIN"     # CORRECT — direction right, rapid reaction
                            elif mae_pct >= threshold and mae_pct > mfe_pct:
                                # Condition C: adverse excursion dominated
                                verdict = "LOSS"    # INCORRECT — direction wrong
                            else:
                                # Catch-all: e.g. MFE hit but too slow (>45min), or tie
                                verdict = "HOLD"    # NOT_DRIVEN — late/sector move, not news-driven

                            # ── forward_pnl: signed PnL % from entry to exit ──
                            fwd_pnl: float = 0.0
                            if entry and entry > 0 and exit_p is not None:
                                if action == "BUY":
                                    fwd_pnl = (exit_p - entry) / entry * 100
                                elif action == "SELL":
                                    fwd_pnl = (entry - exit_p) / entry * 100
                                else:
                                    fwd_pnl = 0.0

                            conn.execute(
                                "UPDATE ai_decisions SET exit_price = ?, is_correct = ?,"
                                " settled = 1, mfe_pct = ?, mae_pct = ?, forward_pnl = ?,"
                                " mfe_time_mins = ? WHERE id = ?",
                                (round(exit_p, 2), verdict,
                                 round(mfe_pct, 4), round(mae_pct, 4), round(fwd_pnl, 4),
                                 round(mfe_time_mins, 1), eid),
                            )
                            conn.commit()
                            updates.append({
                                "id": eid, "asset": asset_raw, "action": action,
                                "entry": entry, "exit": round(exit_p, 2), "verdict": verdict,
                                "mfe": round(mfe_pct, 4), "mae": round(mae_pct, 4),
                                "forward_pnl": round(fwd_pnl, 4), "mfe_mins": round(mfe_time_mins, 1),
                            })
                        elif price is not None:
                            now_unix = int(time.time())
                            new_max = round(max(cur_max or price, price), 2)
                            new_min = round(min(cur_min or price, price), 2)
                            sets: List[str] = []
                            params: list = []
                            if new_max != cur_max:
                                sets.append("max_price = ?")
                                params.append(new_max)
                                sets.append("max_price_time = ?")
                                params.append(now_unix)
                            if new_min != cur_min:
                                sets.append("min_price = ?")
                                params.append(new_min)
                                sets.append("min_price_time = ?")
                                params.append(now_unix)
                            if sets:
                                params.append(eid)
                                conn.execute(
                                    f"UPDATE ai_decisions SET {', '.join(sets)} WHERE id = ?",
                                    params,
                                )
                    conn.commit()

                finally:
                    conn.close()
                return updates

            settled = await loop.run_in_executor(None, _track_cycle)
            for s in settled:
                print(
                    f"  [{_now()}] SETTLED #{s['id']} {s['action']} {s['asset']}"
                    f" | entry={s['entry']} exit={s['exit']} -> {s['verdict']}"
                    f" | MFE={s.get('mfe', 0):+.2f}% MAE={s.get('mae', 0):+.2f}% PnL={s.get('forward_pnl', 0):+.2f}%"
                )

        except Exception as e:
            print(f"[TRACKER] ERROR: {type(e).__name__}: {e}")

        await asyncio.sleep(30)
