"""Smoke tests for realtime_filter.py L1 rule layer (offline — no L2/LLM).

Only L1 keyword classification is exercised: JUNK / CORE hits are returned
without any network call. UNCERTAIN inputs (which would escalate to the L2
LLM classifier) are only passed to _l1_classify directly, never to
evaluate_news.
"""

from realtime_filter import _l1_classify, evaluate_news


# ── L1 JUNK ──────────────────────────────────────────────────────────────

def test_l1_junk_keyword():
    r = _l1_classify("新用户注册即送空投奖励，快来领取")
    assert r is not None
    assert r["verdict"] == "JUNK"
    assert r["is_noise"] == 1
    assert r["layer"] == "L1"
    assert r["matched_keyword"]


def test_l1_junk_english_keyword():
    r = _l1_classify("Big airdrop giveaway for all new token holders")
    assert r is not None
    assert r["verdict"] == "JUNK"


# ── L1 CORE ──────────────────────────────────────────────────────────────

def test_l1_core_keyword():
    r = _l1_classify("美联储宣布降息25个基点")
    assert r is not None
    assert r["verdict"] == "CORE"
    assert r["is_noise"] == 0
    assert r["relevance_score"] == 0.90
    assert r["status"] == "PENDING"


def test_l1_core_fomc():
    r = _l1_classify("FOMC meeting minutes released ahead of schedule")
    assert r is not None
    assert r["verdict"] == "CORE"


# ── L1 UNCERTAIN (would escalate to L2 — do NOT call evaluate_news) ─────

def test_l1_uncertain_returns_none():
    assert _l1_classify("某公司发布季度产品公告与市场安排") is None


# ── evaluate_news — L1-hit paths only (no network involved) ─────────────

def test_evaluate_news_l1_junk():
    r = evaluate_news("转发抽奖送福利，空投领取入口已开启")
    assert r["verdict"] == "JUNK"
    assert r["is_noise"] == 1
    assert r["status"] == "FILTERED"
    assert r["layer"] == "L1"
    assert "elapsed_ms" in r


def test_evaluate_news_l1_core():
    r = evaluate_news("鲍威尔：美联储将维持利率决议不变")
    assert r["verdict"] == "CORE"
    assert r["is_noise"] == 0
    assert r["status"] == "PENDING"
    assert r["layer"] == "L1"
