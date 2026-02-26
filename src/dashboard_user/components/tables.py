# This file wraps table rendering behavior used across dashboard pages.
# It exists so empty states and sizing behavior are consistent for every tab.
# Centralizing table output keeps pages focused on business interpretation logic.
# The helper accepts already-prepared DataFrames and only handles presentation.

from __future__ import annotations

import pandas as pd
import streamlit as st


def render_table(
    dataframe: pd.DataFrame,
    *,
    title: str,
    empty_message: str,
    help_text: str | None = None,
    height: int = 320,
) -> None:
    st.subheader(title, help=help_text)
    if dataframe.empty:
        st.info(empty_message)
        return
    st.dataframe(dataframe, use_container_width=True, hide_index=True, height=height)
