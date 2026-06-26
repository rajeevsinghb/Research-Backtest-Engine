"""
main.py
---------
SINGLE CONTROL PANEL.
Edit only the CONFIG dictionary below to select what to run.
You should never need to edit core/, data_sources/, indicators/, or
scenarios/ logic from here — this file just selects and orchestrates.
"""

from core.loader import load_everything
from core.registry import DATA_SOURCE_REGISTRY, INDICATOR_REGISTRY, SCENARIO_REGISTRY, list_registered


# ============================================================
# CONFIG — change this section to run different research setups
# ============================================================
CONFIG = {
    "datasets": {
        # key name (your choice) -> {source, params}
        "okx_btc": {
            "source": "local_parquet",
            "params": {"path": "data/crypto/raw/BTCUSDT_15m_okx.parquet"},
        },
        "bybit_btc": {
            "source": "local_parquet",
            "params": {"path": "data/crypto/raw/BTCUSDT_15m_bybit.parquet"},
        },
    },

    "indicators": ["RSI", "ATR"],          # single or multiple — empty list [] = skip indicators

    "scenarios": ["simple_summary", "leadlag"],   # multi-scenario test

    "scenario_params": {
        # optional per-scenario params, e.g.
        # "leadlag": {"move_threshold_pct": 0.05, "max_lag": 10}
    },
}
# ============================================================


def run(config: dict):
    load_everything()  # auto-discovers everything in data_sources/, indicators/, scenarios/

    # 1. Load all configured datasets
    data = {}
    for key, spec in config["datasets"].items():
        source_func = DATA_SOURCE_REGISTRY[spec["source"]]
        df = source_func(spec["params"])
        data[key] = df
        print(f"[loaded] {key} -> {len(df):,} rows (source: {spec['source']})")

    # 2. Apply selected indicators to every dataset
    for key in data:
        for ind_name in config["indicators"]:
            indicator_func = INDICATOR_REGISTRY[ind_name]
            data[key] = indicator_func(data[key])
        if config["indicators"]:
            print(f"[indicators applied] {key} -> {config['indicators']}")

    # 3. Run selected scenarios
    all_results = {}
    for scenario_name in config["scenarios"]:
        scenario_func = SCENARIO_REGISTRY[scenario_name]
        params = config.get("scenario_params", {}).get(scenario_name, {})
        result = scenario_func(data, params)
        all_results[scenario_name] = result
        print(f"\n=== Scenario: {scenario_name} ===")
        print(result)

    return all_results


if __name__ == "__main__":
    load_everything()
    print("Available components:", list_registered())
    print()
    run(CONFIG)
