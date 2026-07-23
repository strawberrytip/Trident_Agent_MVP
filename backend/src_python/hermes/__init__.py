"""
Hermes — Read-Only Research Agent Layer

Architecture (bottom-up):
  repositories/  ← DB access layer (all SQL lives here — nowhere else)
  memory/        ← Vector store abstraction (interface + implementations)
  tools/         ← Stateless analysis functions (call repositories, not raw SQL)
                   └── performance_tool.py  ← Phase 1: Performance Intelligence Layer
  hermes_runtime.py  ← Agent loop (plan → execute → synthesize)  [future]
  hermes_config.py   ← Prompts, tool registry, intent routes     [future]
  intent_router.py   ← Lightweight query classifier              [future]
"""

from .tools.performance_tool import PerformanceQuery, StatsResult

