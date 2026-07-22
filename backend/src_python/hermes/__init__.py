"""
Hermes — Read-Only Research Agent Layer

Architecture (bottom-up):
  repositories/  ← DB access layer (all SQL lives here — nowhere else)
  memory/        ← Vector store abstraction (interface + implementations)
  tools/         ← Stateless tool functions (call repositories, not raw SQL)
  hermes_runtime.py  ← Agent loop (plan → execute → synthesize)
  hermes_config.py   ← Prompts, tool registry, intent routes
  intent_router.py   ← Lightweight query classifier (DIRECT vs DELEGATE)
"""
