"""
Unit tests for settings.
It asserts expected behavior and guards against regressions in the corresponding component.
These tests are executed by `pytest` locally and in CI and should remain deterministic.
"""

import pytest

from src.common import settings as settings_module


def test_load_settings_success() -> None:
    settings_module.get_settings.cache_clear()
    settings = settings_module.get_settings()
    assert settings.PROJECT_NAME
    assert settings.API_PORT > 0


def test_load_settings_missing_required(monkeypatch: pytest.MonkeyPatch) -> None:
    settings_module.get_settings.cache_clear()
    monkeypatch.delenv("PROJECT_NAME", raising=False)
    with pytest.raises(RuntimeError, match="Missing required environment variables"):
        settings_module.load_settings(load_env=False)
