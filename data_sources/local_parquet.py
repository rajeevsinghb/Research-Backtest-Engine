"""
data_sources/local_parquet.py
-------------------------------
Data source: load already-saved Parquet file from disk (no network call).

Registered name: "local_parquet"

params expected:
    path : str   e.g. "data/crypto/raw/BTCUSDT_15m_okx.parquet"
"""

import pandas as pd
from core.registry import register_data_source


@register_data_source("local_parquet")
def get_data(params: dict) -> pd.DataFrame:
    df = pd.read_parquet(params["path"])
    df = df.sort_values("timestamp").reset_index(drop=True)
    return df
