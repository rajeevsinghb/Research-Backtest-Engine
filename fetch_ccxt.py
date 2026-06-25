"""
fetch_ccxt.py
-------------
Phase 1: Fast, parameterized historical OHLCV fetcher.
Fetches multiple coins concurrently from a single exchange and saves
each coin's data as a separate Parquet file.

Usage: edit the PARAMETERS section below, then run:
    python fetch_ccxt.py
"""

import asyncio
import os
import time
import pandas as pd
import ccxt.async_support as ccxt_async


# ============================================================
# PARAMETERS — change these as per your requirement
# ============================================================
EXCHANGE_NAME = "okx"              # exchange id (e.g. "okx", "bybit", "binance")
TIMEFRAME = "15m"                  # candle timeframe (e.g. "1m", "15m", "1h", "4h", "1d")
SYMBOLS = [                        # multiple coin pairs to fetch
    "BTC/USDT",
    "ETH/USDT",
    "SOL/USDT",
]
SINCE_DATE = "2021-01-01T00:00:00Z"   # from date (ISO 8601, UTC)
UNTIL_DATE = None                     # to date (None = fetch till now)
OUTPUT_DIR = "data/crypto/raw"        # where Parquet files get saved
LIMIT_PER_CALL = 1000                  # candles per API call (exchange max, usually 1000)
MAX_CONCURRENT_SYMBOLS = 5             # how many coins fetched in parallel
# ============================================================


async def fetch_one_symbol(exchange, symbol: str, timeframe: str,
                            since_date: str, until_date: str = None,
                            limit: int = 1000) -> pd.DataFrame:
    """Fetch full historical OHLCV for a single symbol using pagination."""
    since_ts = exchange.parse8601(since_date)
    until_ts = exchange.parse8601(until_date) if until_date else exchange.milliseconds()

    all_candles = []
    cursor = since_ts

    while cursor < until_ts:
        try:
            batch = await exchange.fetch_ohlcv(symbol, timeframe, since=cursor, limit=limit)
        except Exception as e:
            print(f"  [{symbol}] error: {e} — retrying in 2s")
            await asyncio.sleep(2)
            continue

        if not batch:
            break

        all_candles += batch
        last_ts = batch[-1][0]

        # stop if no progress (avoid infinite loop) or we've passed until_ts
        if last_ts <= cursor:
            break
        cursor = last_ts + 1

        # be polite to the exchange — respects rate limits automatically via .sleep
        await asyncio.sleep(exchange.rateLimit / 1000)

    if not all_candles:
        return pd.DataFrame()

    df = pd.DataFrame(all_candles, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    df = df.drop_duplicates(subset="timestamp").sort_values("timestamp").reset_index(drop=True)

    # trim anything beyond until_date (last batch can overshoot)
    if until_date:
        df = df[df["timestamp"] <= pd.to_datetime(until_ts, unit="ms")]

    return df


async def fetch_symbol_with_semaphore(sem, exchange, symbol, timeframe, since_date, until_date, limit, output_dir):
    async with sem:
        print(f"Fetching {symbol} ...")
        start = time.time()
        df = await fetch_one_symbol(exchange, symbol, timeframe, since_date, until_date, limit)
        elapsed = time.time() - start

        if df.empty:
            print(f"  [{symbol}] no data returned.")
            return

        safe_symbol = symbol.replace("/", "")
        filename = f"{safe_symbol}_{timeframe}_{EXCHANGE_NAME}.parquet"
        filepath = os.path.join(output_dir, filename)
        df.to_parquet(filepath, index=False)

        print(f"  [{symbol}] done — {len(df):,} rows, {elapsed:.1f}s, saved -> {filepath}")


async def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    exchange_class = getattr(ccxt_async, EXCHANGE_NAME)
    exchange = exchange_class({"enableRateLimit": True})

    try:
        sem = asyncio.Semaphore(MAX_CONCURRENT_SYMBOLS)
        tasks = [
            fetch_symbol_with_semaphore(
                sem, exchange, symbol, TIMEFRAME, SINCE_DATE, UNTIL_DATE,
                LIMIT_PER_CALL, OUTPUT_DIR
            )
            for symbol in SYMBOLS
        ]
        await asyncio.gather(*tasks)
    finally:
        await exchange.close()


if __name__ == "__main__":
    overall_start = time.time()
    asyncio.run(main())
    print(f"\nAll done in {time.time() - overall_start:.1f} seconds.")
