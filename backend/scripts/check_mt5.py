#!/usr/bin/env python3
"""Quick MT5 connectivity test — run this to diagnose gold tick issues."""

import os
import sys

# 本机 MT5 终端数据目录（仅在无路径直连失败时作为 fallback）。
# 通过环境变量 MT5_TERMINAL_PATH 覆盖，默认为 None（不尝试显式路径）。
MT5_PATH = os.getenv("MT5_TERMINAL_PATH")

print("=" * 50)
print("1. Checking MetaTrader5 package ...")
try:
    import MetaTrader5 as mt5
    print(f"   OK — version: {mt5.__version__}")
except ImportError as e:
    print(f"   NOT INSTALLED: {e}")
    print("   Run: pip install MetaTrader5 --break-system-packages")
    sys.exit(1)

print("\n2. Connecting to running MT5 terminal (no path) ...")
if mt5.initialize():
    print("   SUCCESS! MT5 terminal found and connected.")
    mt5.symbol_select("XAUUSD", True)

    print("\n3. Reading XAUUSD tick ...")
    tick = mt5.symbol_info_tick("XAUUSD")
    if tick is None:
        print("   FAILED — XAUUSD not available in Market Watch")
        print("   Open MT5 → Market Watch → right-click → Show All → find XAUUSD")
    else:
        print(f"   bid={tick.bid:.2f}  ask={tick.ask:.2f}  last={tick.last:.2f}")
        print(f"   mid={(tick.bid + tick.ask) / 2:.2f}  spread={(tick.ask - tick.bid):.2f}")
        print("   TICK STREAMING WORKS!")

    mt5.shutdown()
else:
    code, desc = mt5.last_error()
    print(f"   FAILED: [{code}] {desc}")
    if MT5_PATH:
        print(f"\n   Trying with explicit path: {MT5_PATH} ...")
        if mt5.initialize(path=MT5_PATH):
            print("   SUCCESS with path!")
            mt5.shutdown()
        else:
            code, desc = mt5.last_error()
            print(f"   FAILED: [{code}] {desc}")
    else:
        print("\n   (set MT5_TERMINAL_PATH env var to retry with an explicit terminal path)")
    print("\n   TROUBLESHOOTING:")
    print("   1. Make sure MT5 terminal is OPEN and LOGGED IN")
    print("   2. Tools → Options → Expert Advisors → Allow WebRequest")
    print("   3. Try restarting MT5, then re-run this script")
