"""Import regression tests — every runtime module must import cleanly.

engine.py is a thin entry script shadowed by the engine/ package, so it is
executed via runpy (with a non-__main__ run_name, so asyncio.run is NOT
triggered) rather than imported.
"""

import os
import runpy

import conftest


def test_import_config():
    import config  # noqa: F401


def test_import_db():
    import db  # noqa: F401


def test_import_engine_package():
    import engine  # noqa: F401
    assert engine.__file__.endswith(os.path.join("engine", "__init__.py"))


def test_import_engine_submodules():
    from engine import (  # noqa: F401
        ai_worker, alerts, forward, ingest, main, prices, utils, webhook,
        ws_client,
    )


def test_engine_py_thin_entry():
    ns = runpy.run_path(os.path.join(conftest.SRC_PYTHON, "engine.py"))
    assert callable(ns["main"])


def test_import_api_server():
    import api_server  # noqa: F401


def test_import_realtime_filter():
    import realtime_filter  # noqa: F401


def test_import_market_snapshot():
    import market_snapshot  # noqa: F401
