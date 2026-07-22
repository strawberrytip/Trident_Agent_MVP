#!/usr/bin/env python3
"""Patch api_server.py: add reasoning_path to SQL queries and _row_to_event."""

path = "/sessions/trusting-pensive-davinci/mnt/Trident_Agent_MVP/backend/src_python/api_server.py"
with open(path, 'r', encoding='utf-8') as f:
    content = f.read()

# =====================================================================
# 1. _row_to_event — add reasoning_path
# =====================================================================
old_return = """        "parent_id": row["parent_id"],
        "child_count": row["child_count"] or 0,
    }"""

assert old_return in content, "_row_to_event return not found"

new_return = """        "parent_id": row["parent_id"],
        "child_count": row["child_count"] or 0,
        "reasoning_path": (row["reasoning_path"] or "")[:200],
    }"""

content = content.replace(old_return, new_return)
print("1. _row_to_event: reasoning_path added")

# =====================================================================
# 2. _fetch_rows_since SQL — add ad.reasoning_path
# =====================================================================
old_since = """                ad.created_at,
                ad.parent_id,
                ad.child_count
            FROM ai_decisions ad"""

assert old_since in content, "_fetch_rows_since SQL not found"

new_since = """                ad.created_at,
                ad.parent_id,
                ad.child_count,
                ad.reasoning_path
            FROM ai_decisions ad"""

content = content.replace(old_since, new_since)
print("2. _fetch_rows_since: reasoning_path added to SELECT")

# =====================================================================
# 3. /api/events SQL — add ad.reasoning_path
# =====================================================================
old_events = """                ad.created_at,
                ad.parent_id,
                ad.child_count
            FROM ai_decisions ad
            INNER JOIN raw_news rn ON rn.id = ad.news_id
            ORDER BY ad.id DESC"""

assert old_events in content, "/api/events SQL not found"

new_events = """                ad.created_at,
                ad.parent_id,
                ad.child_count,
                ad.reasoning_path
            FROM ai_decisions ad
            INNER JOIN raw_news rn ON rn.id = ad.news_id
            ORDER BY ad.id DESC"""

content = content.replace(old_events, new_events)
print("3. /api/events: reasoning_path added to SELECT")

with open(path, 'w', encoding='utf-8') as f:
    f.write(content)

import ast
ast.parse(content)
print("api_server.py AST OK")
