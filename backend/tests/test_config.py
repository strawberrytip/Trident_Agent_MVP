"""Smoke tests for src_python/config.py — single config entry point."""

import os
import subprocess
import sys

import conftest  # tests/conftest.py (sys.path bootstrap lives there)

import config


def test_config_importable():
    assert config.BASE_DIR
    assert config.TZ_SHANGHAI is not None


def test_db_path_default():
    expected = os.path.join(config.BASE_DIR, "trident_event_bus.db")
    # Only meaningful when TRIDENT_DB_PATH is not set in the environment
    if not os.getenv("TRIDENT_DB_PATH"):
        assert config.DB_PATH == expected
        assert config.DB_PATH.endswith(os.path.join("backend", "trident_event_bus.db"))


def test_db_path_env_override(tmp_path):
    """TRIDENT_DB_PATH overrides the default — verified in a subprocess so
    the reload cannot pollute config state for other test modules."""
    override = str(tmp_path / "override_event_bus.db")
    env = os.environ.copy()
    env["TRIDENT_DB_PATH"] = override
    out = subprocess.run(
        [sys.executable, "-c", "import config; print(config.DB_PATH)"],
        capture_output=True, text=True, env=env, cwd=conftest.SRC_PYTHON,
    )
    assert out.returncode == 0, out.stderr
    assert out.stdout.strip() == override


def test_thresholds():
    assert config.VIP_SCORE_BOOST == 1.25
    assert config.BATCH_SIZE == 10
    assert config.IMPACT_THRESHOLD["BTC"] == 2.0
    assert "[VIP:TRUMP]" in config.VIP_KOLS.values()


def test_cors_default():
    if not os.getenv("CORS_ALLOW_ORIGINS"):
        assert "http://localhost:3000" in config.CORS_ALLOW_ORIGINS
        assert "http://127.0.0.1:3000" in config.CORS_ALLOW_ORIGINS
