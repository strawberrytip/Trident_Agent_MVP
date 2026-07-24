#!/usr/bin/env python3
"""
Trident Agent MVP — 历史新闻批量注入
=====================================

把历史新闻塞进 raw_news 表（status=PENDING, is_noise=0），
运行中的 engine.py 每 1 秒扫描一次，自动拾取 → 翻译 → 过滤 → Kimi K3 分析。

用法:
    # 从文件读取（一行一条新闻）
    python scripts/feed_news.py headlines.txt

    # 直接传字符串（分号分隔）
    python scripts/feed_news.py "美联储加息25bp; 中东局势升级; CPI超预期"

    # 使用内置测试集
    python scripts/feed_news.py --demo

    # 清空旧的 PENDING 新闻（避免重复）
    python scripts/feed_news.py --clear
"""

from __future__ import annotations

import hashlib
import os
import sqlite3
import sys
from datetime import datetime, timezone, timedelta

TZ_SHANGHAI = timezone(timedelta(hours=8))
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "trident_event_bus.db")


# ═══════════════════════════════════════════════════════════════════════════
# 内置演示数据集 — 2024-2025 真实宏观/加密重大事件
# ═══════════════════════════════════════════════════════════════════════════
DEMO_HEADLINES = [
    # ── 加密货币 ──
    "SEC批准现货比特币ETF，Grayscale胜诉后首日交易量突破46亿美元",
    "比特币跌破60000美元，24小时爆仓超8亿美元，多单清算主导",
    "MicroStrategy以4.58亿美元增持7420枚比特币，均价61750美元",
    "币安CEO赵长鹏辞职并认罪，罚款43亿美元，Richard Teng接任",
    "比特币减半完成，区块奖励降至3.125 BTC，矿工收入骤降",
    "萨尔瓦多每日定投1枚比特币，总统称永不卖出",
    "BlackRock比特币ETF持仓突破30万枚，超越MicroStrategy",
    "SEC对Coinbase提起诉讼，指控其作为未注册证券交易所运营",
    "比特币突破73000美元创历史新高，ETF净流入超5亿美元/日",
    "德国政府出售5万枚扣押比特币，市场承压跌破55000",
    "以太坊现货ETF获批，SEC突然转变立场引发市场震动",
    "美联储主席鲍威尔称加密货币不会威胁美元地位",
    "Tether冻结与非法活动相关的2.25亿USDT，配合美国司法部调查",
    "FTX破产重组计划获法院批准，债权人将获得118%现金赔付",
    "Solana meme币BONK暴涨120%，链上交易量超越以太坊",

    # ── 宏观经济 / 央行 ──
    "美联储维持利率5.25-5.50%不变，点阵图暗示年内降息三次",
    "美国1月CPI同比3.1%超预期，市场削减3月降息押注至20%以下",
    "美国非农就业新增35.3万人远超预期，失业率维持3.7%",
    "日本央行17年来首次加息，结束负利率政策，利率上调至0-0.1%",
    "欧洲央行降息25bp至3.75%，拉加德称通胀前景明显改善",
    "中国央行下调5年期LPR 25bp至3.95%，创历史最大单次降幅",
    "美国GDP Q1初值1.6%远低于预期，滞胀担忧升温",
    "英国通胀率降至2.0%目标位，为2021年以来首次",
    "美国核心PCE物价指数2.8%，市场预期2.7%，美元走强",
    "瑞士央行意外降息25bp，成为首个放松政策的G10央行",
    "中国人民银行降准50bp释放1万亿流动性，A50期货跳涨",

    # ── 地缘政治 / 大宗商品 ──
    "伊朗总统莱希直升机坠毁身亡，中东局势不确定性激增",
    "以色列对加沙发动地面进攻，布伦特原油突破90美元",
    "哈马斯领导人哈尼亚在德黑兰遇袭身亡，伊朗誓言报复",
    "OPEC+延长自愿减产至Q2，沙特维持100万桶/日额外减产",
    "黄金突破2400美元创历史新高，央行购金与降息预期双重驱动",
    "也门胡塞武装袭击红海商船，马士基暂停红海航线，运价暴涨",
    "美国对俄罗斯实施新一轮500项制裁，涉及能源和金融领域",
    "乌克兰无人机袭击俄罗斯炼油厂，布伦特原油大涨4%",
    "中国央行连续17个月增持黄金储备，4月新增6万盎司",
    "伦敦金库黄金外流加速，瑞士对印度黄金出口创三年新高",
    "俄罗斯宣布减产石油50万桶/日并延长至Q3，油价企稳",

    # ── 加密监管 & 行业 ──
    "特朗普在比特币大会上宣布若当选将解雇SEC主席Gensler",
    "香港批准首批比特币和以太坊现货ETF，4月30日上市交易",
    "欧盟MiCA法规正式生效，USDC成为首个合规稳定币",
    "美国财政部提议对加密货币挖矿征收30%电力税",
    "Visa和Mastercard重新考虑与币安的合作关系",
    "PayPal推出美元稳定币PYUSD，市值突破10亿美元",
    "Uniswap收到SEC Wells通知，DeFi监管风暴来临",
    "Coinbase在法务战中胜诉关键动议，法院部分驳回SEC指控",
    "英国FCA批准加密货币ETN面向专业投资者",
    "新加坡要求加密货币交易所将客户资产存入信托",

    # ── 科技 / 市场 ──
    "英伟达市值突破3万亿美元超越苹果，AI芯片需求暴涨",
    "美国债务突破34万亿美元，财长耶伦警告违约风险",
    "日元兑美元跌破160创38年新低，日本当局疑似干预汇市",
    "阿根廷通货膨胀率达211%，新总统米莱推行美元化改革",
    "特斯拉接受狗狗币作为支付方式，DOGE暴涨30%",
    "美国地区银行纽约社区银行暴跌40%，商业地产危机重现",
    "全球半导体销售Q1增长15%，AI芯片需求驱动供应链复苏",
]


def _now() -> str:
    return datetime.now(TZ_SHANGHAI).strftime("%Y-%m-%d %H:%M:%S")


def _hash(text: str) -> str:
    return hashlib.sha256(text.strip().encode()).hexdigest()[:16]


def insert_headlines(headlines: list[str], source: str = "HISTORICAL") -> int:
    """Insert headlines into raw_news. Returns count of inserted rows."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA busy_timeout=5000;")
    inserted = 0
    skipped = 0

    for text in headlines:
        text = text.strip()
        if not text or text.startswith("#"):
            continue

        h = _hash(text)
        ts = _now()
        content = f"[hash:{h}] {text[:500]}"
        source_label = f"FEED:{source}"

        # 去重
        ex = conn.execute(
            "SELECT id FROM raw_news WHERE content LIKE ? LIMIT 1",
            (f"[hash:{h}]%",),
        ).fetchone()
        if ex:
            skipped += 1
            continue

        conn.execute(
            "INSERT INTO raw_news (source, content, timestamp, status, is_noise, relevance_score)"
            " VALUES (?, ?, ?, 'PENDING', 0, 0.90);",
            (source_label, content, ts),
        )
        inserted += 1

    conn.commit()
    conn.close()
    return inserted, skipped


def clear_pending() -> int:
    """Delete all PENDING rows — useful for reset before feeding."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA busy_timeout=5000;")
    n = conn.execute("SELECT COUNT(*) FROM raw_news WHERE status = 'PENDING'").fetchone()[0]
    conn.execute("DELETE FROM raw_news WHERE status = 'PENDING'")
    conn.commit()
    conn.close()
    return n


def show_queue() -> None:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA busy_timeout=5000;")
    pending = conn.execute(
        "SELECT COUNT(*) FROM raw_news WHERE status = 'PENDING' AND is_noise = 0"
    ).fetchone()[0]
    total = conn.execute("SELECT COUNT(*) FROM raw_news WHERE is_noise = 0").fetchone()[0]
    processed = conn.execute(
        "SELECT COUNT(*) FROM raw_news WHERE status IN ('DONE', 'PROCESSING') AND is_noise = 0"
    ).fetchone()[0]
    noise = conn.execute("SELECT COUNT(*) FROM raw_news WHERE is_noise = 1").fetchone()[0]
    conn.close()
    print(f"  队列: {pending} PENDING | {processed} 已处理 | {total} 总计 | {noise} 噪音过滤")


def main():
    os.chdir(BASE_DIR)

    if len(sys.argv) < 2:
        print(__doc__)
        print("当前队列状态:")
        show_queue()
        return

    arg = sys.argv[1]

    if arg == "--clear":
        n = clear_pending()
        print(f"[{_now()}] 已清空 {n} 条 PENDING 新闻")
        show_queue()
        return

    if arg == "--demo":
        inserted, skipped = insert_headlines(DEMO_HEADLINES, "DEMO")
        print(f"[{_now()}] ✅ 演示数据: {inserted} 条注入, {skipped} 条跳过 (已存在)")
        show_queue()
        return

    if arg == "--status":
        show_queue()
        return

    # ── 从文件读取 ──
    if os.path.isfile(arg):
        with open(arg, "r", encoding="utf-8") as f:
            lines = f.readlines()
        if not lines:
            print("文件为空")
            return
        inserted, skipped = insert_headlines(lines, os.path.basename(arg))
        print(f"[{_now()}] ✅ 文件导入: {inserted} 条注入, {skipped} 条跳过")
        show_queue()
        return

    # ── 分号分隔的字符串 ──
    headlines = [h.strip() for h in arg.replace("；", ";").split(";") if h.strip()]
    if headlines:
        inserted, skipped = insert_headlines(headlines, "CLI")
        print(f"[{_now()}] ✅ CLI 注入: {inserted} 条注入, {skipped} 条跳过")
        show_queue()
        return

    print(f"无法识别参数: {arg}")
    print(__doc__)


if __name__ == "__main__":
    main()
