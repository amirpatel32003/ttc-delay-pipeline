"""Data-quality checks run against the warehouse. Each check returns (name, passed, detail)."""
from __future__ import annotations

import duckdb

CHECKS = {
    "silver_not_empty": "SELECT count(*) = 0 FROM silver_delays",
    "no_null_dates": "SELECT count(*) FROM silver_delays WHERE delay_date IS NULL",
    "no_negative_delay": "SELECT count(*) FROM silver_delays WHERE min_delay < 0",
    "delay_within_a_day": "SELECT count(*) FROM silver_delays WHERE min_delay > 1440",
    "no_future_dates": "SELECT count(*) FROM silver_delays WHERE delay_date > current_date",
    "unique_delay_id": "SELECT count(*) - count(DISTINCT delay_id) FROM silver_delays",
    "reject_rate_under_5pct": (
        "SELECT (SELECT count(*) FROM silver_rejects) * 1.0 / "
        "greatest((SELECT count(*) FROM silver_delays) + (SELECT count(*) FROM silver_rejects), 1) > 0.05"
    ),
    "unknown_line_rate_under_2pct": (
        "SELECT count(*) FILTER (WHERE line IS NULL OR line = 'OTHER') * 1.0 / greatest(count(*), 1) > 0.02 "
        "FROM silver_delays"
    ),
    "gold_matches_silver": (
        "SELECT abs((SELECT sum(incidents) FROM gold_daily_summary) - (SELECT count(*) FROM silver_delays))"
    ),
}


def run_checks(con: duckdb.DuckDBPyConnection) -> list[tuple[str, bool, str]]:
    """Every query returns a count of violations (or a boolean violation flag); 0/False means pass."""
    results = []
    for name, sql in CHECKS.items():
        value = con.execute(sql).fetchone()[0]
        passed = not bool(value)
        results.append((name, passed, f"violations={value}"))
    return results
