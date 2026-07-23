"""
Hermes Tools — Stateless analysis functions.

Tools call Repositories for raw data, then compute statistics / insights
in pure Python.  They never write to the DB and never touch a cursor directly.

performance_tool.py  — Phase 1 Performance Intelligence Layer
"""

from .performance_tool import PerformanceQuery, StatsResult
