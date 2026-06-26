"""
data_sources/ccxt_fetch.py
----------------------------
Data source: fetch fresh OHLCV directly from a crypto exchange via CCXT.

Registered name: "ccxt_fetch"

params expected:
    exchange   : str  e.g. "okx"
    symbol     : str  e.g. "BTC/USDT"
    timeframe  : str  e.g. "15m"
    since_date : str  ISO8601, e.g. "2024-01-01T00:00:00Z"
    until_date : str or None
    limit      : int  (optional, default 1000)
"""

import asyncio
import pandas as pd
import ccxt.async_support as ccxt_async

from core.registry import register_data_source


async def _fetch_async(exchange_name, symbol, timeframe, since_date, until_date, limit):
    exchange_class = getattr(ccxt_async, exchange_name)
    exchange = exchange_class({"enableRateLimit": True})

    try:
        since_ts = exchange.parse8601(since_date)
        until_ts = exchange.parse8601(until_date) if until_date else exchange.milliseconds()

        all_candles = []
        cursor = since_ts

        while cursor < until_ts:
            batch = await exchange.fetch_ohlcv(symbol, timeframe, since=cursor, limit=limit)
            if not batch:
                break
            all_candles += batch
            last_ts = batch[-1][0]
            if last_ts <= cursor:
                break
            cursor = last_ts + 1
            await asyncio.sleep(exchange.rateLimit / 1000)

        if not all_candles:
            return pd.DataFrame()

        df = pd.DataFrame(all_candles, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        df = df.drop_duplicates(subset="timestamp").sort_values("timestamp").reset_index(drop=True)
        if until_date:
            df = df[df["timestamp"] <= pd.to_datetime(until_ts, unit="ms")]
        return df
    finally:
        await exchange.close()


@register_data_source("ccxt_fetch")
def get_data(params: dict) -> pd.DataFrame:
    return asyncio.run(_fetch_async(
        exchange_name=params["exchange"],
        symbol=params["symbol"],
        timeframe=params["timeframe"],
        since_date=params["since_date"],
        until_date=params.get("until_date"),
        limit=params.get("limit", 1000),
    ))
