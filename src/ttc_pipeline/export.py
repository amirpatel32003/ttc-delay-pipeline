"""Export the dashboard's aggregates from the warehouse as one JSON document.

Every key is a small, already-aggregated table (or a scalar), so the static dashboard needs no
database and the file stays under 100 KB. Windows ("last 12 months") are relative to the latest
date in the data, not to today, so a delayed City refresh never produces misleading deltas.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from . import quality


def _rows(con: duckdb.DuckDBPyConnection, sql: str) -> list[dict]:
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    out = []
    for r in cur.fetchall():
        row = {}
        for c, v in zip(cols, r):
            if hasattr(v, "isoformat"):
                v = v.isoformat()
            elif v is not None and not isinstance(v, (str, float, bool)) and hasattr(v, "__int__"):
                v = int(v)
            row[c] = v
        out.append(row)
    return out


def build_payload(con: duckdb.DuckDBPyConnection) -> dict:
    last = con.execute("SELECT max(delay_date) FROM silver_delays").fetchone()[0]
    cur = f"delay_date > DATE '{last}' - INTERVAL 365 DAY"
    prev = f"delay_date <= DATE '{last}' - INTERVAL 365 DAY AND delay_date > DATE '{last}' - INTERVAL 730 DAY"
    out: dict = {"generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "asof": last.isoformat()}
    out["kpi"] = _rows(con, """
        SELECT count(*) incidents, sum(min_delay) total_delay_min, count(*) FILTER (WHERE min_delay > 0) delay_incidents,
               min(delay_date) first_date, max(delay_date) last_date,
               count(DISTINCT station) FILTER (WHERE is_station) stations,
               round(avg(min_delay) FILTER (WHERE min_delay > 0), 2) avg_nonzero_delay,
               (SELECT count(*) FROM silver_delays_all) source_rows, (SELECT count(*) FROM silver_rejects) rejects
        FROM silver_delays""")
    out["yearly"] = _rows(con, "SELECT year(delay_date) AS yr, count(*) incidents, sum(min_delay) total_delay_min FROM silver_delays GROUP BY 1 ORDER BY 1")
    out["monthly"] = _rows(con, "SELECT month, sum(incidents) incidents, sum(total_delay_min) total_delay_min FROM gold_monthly_trend GROUP BY 1 ORDER BY 1")
    out["stations"] = _rows(con, "SELECT station, incidents, total_delay_min, avg_delay_min, delay_rank FROM gold_station_ranking ORDER BY delay_rank LIMIT 15")
    out["hourly"] = _rows(con, "SELECT hour_of_day, day_name, incidents, avg_delay_min FROM gold_hourly_pattern ORDER BY 1, 2")
    out["lines"] = _rows(con, "SELECT coalesce(line, 'UNKNOWN') line, count(*) incidents, sum(min_delay) total_delay_min FROM silver_delays GROUP BY 1 ORDER BY 3 DESC")
    out["line_year"] = _rows(con, "SELECT year(delay_date) AS yr, line, count(*) incidents, sum(min_delay) total_delay_min FROM silver_delays WHERE line IN ('YU','BD','SHP','SRT') GROUP BY 1, 2 ORDER BY 1, 2")
    out["buckets"] = _rows(con, """
        SELECT year(delay_date) yr,
               count(*) FILTER (WHERE min_delay = 0) b0, count(*) FILTER (WHERE min_delay BETWEEN 1 AND 4) b1,
               count(*) FILTER (WHERE min_delay BETWEEN 5 AND 14) b2, count(*) FILTER (WHERE min_delay BETWEEN 15 AND 29) b3,
               count(*) FILTER (WHERE min_delay >= 30) b4, count(*) incidents, count(DISTINCT delay_date) active_days,
               sum(min_delay) FILTER (WHERE min_delay >= 30) major_min, sum(min_delay) total_min,
               count(*) FILTER (WHERE day_name NOT IN ('SATURDAY','SUNDAY')
                                AND (hour(delay_time) BETWEEN 6 AND 9 OR hour(delay_time) BETWEEN 15 AND 18)) peak_incidents
        FROM silver_delays GROUP BY 1 ORDER BY 1""")
    out["causes"] = _rows(con, "SELECT code, description, incidents, total_delay_min FROM gold_cause_breakdown ORDER BY total_delay_min DESC LIMIT 10")
    out["cause_year"] = _rows(con, """
        WITH t AS (SELECT year(s.delay_date) yr, s.code, coalesce(c.description, 'Unknown code') description,
                          count(*) incidents, sum(s.min_delay) total_min
                   FROM silver_delays s LEFT JOIN dim_codes c ON c.code = s.code GROUP BY 1, 2, 3),
             top AS (SELECT code FROM t GROUP BY code ORDER BY sum(total_min) DESC LIMIT 6)
        SELECT * FROM t WHERE code IN (SELECT code FROM top) ORDER BY yr, total_min DESC""")
    out["watch"] = _rows(con, f"""
        SELECT station,
               sum(min_delay) FILTER (WHERE {cur}) cur_min, count(*) FILTER (WHERE {cur}) cur_inc,
               sum(min_delay) FILTER (WHERE {prev}) prev_min, count(*) FILTER (WHERE {prev}) prev_inc
        FROM silver_delays WHERE is_station GROUP BY 1 ORDER BY cur_min DESC NULLS LAST LIMIT 15""")
    out["causes_ltm"] = _rows(con, f"""
        SELECT s.code, coalesce(c.description, 'Unknown code') description,
               count(*) FILTER (WHERE {cur}) cur_inc,
               coalesce(sum(min_delay) FILTER (WHERE {cur}), 0) cur_min,
               coalesce(sum(min_delay) FILTER (WHERE {prev}), 0) prev_min
        FROM silver_delays s LEFT JOIN dim_codes c ON c.code = s.code GROUP BY 1, 2 ORDER BY cur_min DESC LIMIT 10""")
    out["line_ltm"] = _rows(con, f"""
        SELECT coalesce(line, 'UNKNOWN') line,
               count(*) FILTER (WHERE {cur}) cur_inc, coalesce(sum(min_delay) FILTER (WHERE {cur}), 0) cur_min,
               count(*) FILTER (WHERE {prev}) prev_inc, coalesce(sum(min_delay) FILTER (WHERE {prev}), 0) prev_min
        FROM silver_delays GROUP BY 1 ORDER BY cur_min DESC""")
    out["ingestion"] = _rows(con, "SELECT name, format, last_modified, rows, bytes, ingested_at FROM ingestion_log ORDER BY name")
    out["checks"] = [{"name": n, "passed": bool(ok), "detail": d} for n, ok, d in quality.run_checks(con)]
    return out


def write_payload(con: duckdb.DuckDBPyConnection, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(build_payload(con), default=str, separators=(",", ":")), encoding="utf-8")
    return path
