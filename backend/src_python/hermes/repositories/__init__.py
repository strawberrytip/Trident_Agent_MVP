"""
Hermes Repository Layer

All database access flows through Repository classes.
No tool function, agent runtime, or memory component writes raw SQL.

Usage:
    from hermes.repositories.news_repo import NewsRepository
    from hermes.repositories.signal_repo import SignalRepository
    from hermes.repositories.obs_repo import ObservationRepository
    from hermes.repositories.insight_repo import InsightRepository
    from hermes.repositories.base import BaseRepository, _now

Migration (called once at api_server startup):
    from hermes.repositories.base import _resolve_db_path
    from hermes.repositories.news_repo import NewsRepository
    from hermes.repositories.signal_repo import SignalRepository
    from hermes.repositories.obs_repo import ObservationRepository
    from hermes.repositories.insight_repo import InsightRepository

    for repo_cls in [NewsRepository, SignalRepository, ObservationRepository, InsightRepository]:
        for stmt in repo_cls.migration_sql():
            conn.execute(stmt)
"""

# Re-export public API
from .base import BaseRepository, _resolve_db_path, _now, assert_readonly_sql
from .news_repo import NewsRepository
from .signal_repo import SignalRepository
from .obs_repo import ObservationRepository
from .insight_repo import InsightRepository

__all__ = [
    "BaseRepository",
    "NewsRepository",
    "SignalRepository",
    "ObservationRepository",
    "InsightRepository",
    "_resolve_db_path",
    "_now",
    "assert_readonly_sql",
]
