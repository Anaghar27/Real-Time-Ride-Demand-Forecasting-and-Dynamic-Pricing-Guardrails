# This file keeps the historical API module path stable for existing run commands.
# It exists so `uvicorn src.api.main:app` continues to work after the new app bootstrap split.
# The actual FastAPI construction now lives in `src.api.app` to keep code organized.
# This small compatibility shim avoids breaking existing scripts and docs.

from __future__ import annotations

from src.api.app import app

__all__ = ["app"]
