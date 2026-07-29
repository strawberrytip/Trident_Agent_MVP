"""Smoke tests for engine core logic — VIP detect, dedup hash, junk filter,
and parent/child signal aggregation helpers."""

from engine.utils import _content_hash, _detect_vip, _is_content_junk
from engine.ai_worker import _bump_parent_score, _find_active_parent


# ── _detect_vip ──────────────────────────────────────────────────────────

def test_detect_vip_trump():
    assert _detect_vip("Trump says something about tariffs") == ("[VIP:TRUMP]", "Trump")


def test_detect_vip_powell_chinese():
    assert _detect_vip("鲍威尔发表最新讲话") == ("[VIP:FED]", "鲍威尔")


def test_detect_vip_no_match():
    assert _detect_vip("普通财经文本没有任何关键人物") == ("", "")


# ── _content_hash ────────────────────────────────────────────────────────

def test_content_hash_stable():
    h1 = _content_hash("title", "http://x", "body text")
    h2 = _content_hash("title", "http://x", "body text")
    assert h1 == h2
    assert len(h1) == 16


def test_content_hash_differs():
    assert _content_hash("a", "http://x", "b") != _content_hash("c", "http://x", "b")


# ── _is_content_junk ─────────────────────────────────────────────────────

def test_is_content_junk_positive():
    assert _is_content_junk("无具体内容") is True          # _CONTENT_BLACKLIST 真实条目
    assert _is_content_junk("暂无内容，稍后再看") is True     # _CONTENT_BLACKLIST 真实条目
    assert _is_content_junk("short") is True                 # 过短


def test_is_content_junk_negative():
    text = "Federal Reserve cuts interest rates by 25bps amid inflation concerns"
    assert _is_content_junk(text) is False


# ── 父子聚合 (_find_active_parent / _bump_parent_score) ─────────────────

def _insert_parent(conn, score=0.5, action="BUY", category="GOLD", asset="XAU"):
    cur = conn.execute("INSERT INTO raw_news (source, content) VALUES ('test', 'x')")
    news_id = cur.lastrowid
    cur = conn.execute(
        "INSERT INTO ai_decisions"
        " (news_id, sentiment_score, suggested_action, reasoning,"
        "  market_category, target_asset)"
        " VALUES (?, ?, ?, 'parent', ?, ?)",
        (news_id, score, action, category, asset),
    )
    conn.commit()
    return cur.lastrowid


def test_find_active_parent_hit(temp_db):
    parent_id = _insert_parent(temp_db)
    assert _find_active_parent(temp_db, "GOLD", "XAU", "BUY") == parent_id


def test_find_active_parent_miss(temp_db):
    _insert_parent(temp_db)
    assert _find_active_parent(temp_db, "GOLD", "XAU", "SELL") is None      # 方向不同
    assert _find_active_parent(temp_db, "CRYPTO", "BTC", "BUY") is None     # 资产不同
    assert _find_active_parent(temp_db, "GOLD", "XAU", "HOLD") is None      # HOLD 不参与聚合


def test_bump_parent_score_more_extreme(temp_db):
    parent_id = _insert_parent(temp_db, score=0.5)
    assert _bump_parent_score(temp_db, parent_id, 0.8, "child") is True
    row = temp_db.execute(
        "SELECT sentiment_score FROM ai_decisions WHERE id = ?", (parent_id,)
    ).fetchone()
    assert abs(row[0] - 0.8) < 1e-9


def test_bump_parent_score_less_extreme_noop(temp_db):
    parent_id = _insert_parent(temp_db, score=0.5)
    assert _bump_parent_score(temp_db, parent_id, 0.3, "child") is False
    row = temp_db.execute(
        "SELECT sentiment_score FROM ai_decisions WHERE id = ?", (parent_id,)
    ).fetchone()
    assert abs(row[0] - 0.5) < 1e-9
