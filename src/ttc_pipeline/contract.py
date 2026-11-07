"""The data contract, as code. docs/data_contract.md is rendered from this module and a test fails when the
contract and the real DuckDB catalog drift apart (column names, order or types)."""
from __future__ import annotations

from dataclasses import dataclass, field

OWNER = "Amir Patel (data owner and steward)"
SOURCE_OWNER = "City of Toronto / Toronto Transit Commission, published on Toronto Open Data (Open Government Licence - Toronto)"
CONSUMERS = "Analysts and the Day 3 dashboard; read-only access to the gold_* tables"
VERSION = "1.0.0"

SLAS = {
    "Freshness": "Pipeline runs daily (Airflow @daily). A resource the City re-publishes is picked up by the next run, so "
                 "the warehouse is at most 24 hours behind the portal. The portal itself publishes irregularly "
                 "(the 'since 2025' file was last modified 2026-10-26 when checked on 2026-11-07).",
    "Completeness": "Reject rate below 5% of source rows (enforced by check reject_rate_under_5pct). Rows with unknown or "
                    "blank line below 2% (enforced by unknown_line_rate_under_2pct).",
    "Correctness gate": "All 9 SQL data-quality checks pass or the run exits non-zero and the DAG task fails.",
    "Reconciliation": "sum(gold_daily_summary.incidents) equals count(silver_delays) exactly.",
    "Run time": "Incremental run under 10 minutes on a laptop; unchanged sources are skipped without a download.",
    "Change policy": "Adding a column is non-breaking. Renaming, removing or re-typing a column is breaking: bump the "
                     "contract MAJOR version and update this file in the same commit (a test enforces it).",
}

KNOWN_ISSUES = [
    "The 2017-2021 files are Excel workbooks with one sheet per month; the loader reads every sheet that has a Date column "
    "(a first version read only the first sheet and silently dropped 11 of 12 months a year).",
    "min_delay = 0 for about two thirds of rows (an incident was logged but no delay was recorded), so averages "
    "mix zero and non-zero delays; gold tables expose both incidents and delay_incidents (min_delay > 0).",
    "bound is blank for roughly 29% of rows in the source and is NULL in silver; values outside N/S/E/W/B are set to NULL.",
    "vehicle = 0 means 'no vehicle recorded', not train number zero.",
    "Station names are free text. Silver strips the STATION suffix (often truncated at 20 characters in the source) and "
    "trailing line qualifiers (BD, YU, YUS, SRT, SHP), so KENNEDY BD and KENNEDY SRT merge into KENNEDY. Rows naming a "
    "segment ('A TO B') or a whole line are kept with is_station = FALSE and excluded from gold_station_ranking.",
    "Exact duplicate rows (same date, time, station, code, delays, bound, line and vehicle) are collapsed to one; a "
    "genuine second incident with identical values in the same minute would also be collapsed.",
    "A small number of delay codes are not in the current lookup; legacy codes are filled from the 2022 workbook and "
    "the rest appear as 'Unknown code' in gold_cause_breakdown.",
]


@dataclass(frozen=True)
class Col:
    name: str
    type: str
    description: str
    rule: str = ""


@dataclass
class Table:
    name: str
    layer: str
    description: str
    grain: str
    columns: list[Col] = field(default_factory=list)
    rules: list[str] = field(default_factory=list)


_SILVER_COLS = [
    Col("delay_date", "DATE", "Calendar date of the incident.", "NOT NULL; not in the future"),
    Col("delay_time", "TIME", "Time of day the incident was logged.", "Parsed from HH:MM; NULL if unparseable"),
    Col("day_name", "VARCHAR", "Day of week as published, upper-cased.", ""),
    Col("station", "VARCHAR", "Cleaned station (or location) name.", "NOT NULL and non-empty; upper-case; STATION suffix and line qualifier removed"),
    Col("code", "VARCHAR", "TTC delay code, upper-cased.", "Joined to the code lookup in gold_cause_breakdown"),
    Col("min_delay", "INTEGER", "Minutes of delay to service.", "NOT NULL; 0 to 1440"),
    Col("min_gap", "INTEGER", "Minutes between trains caused by the incident.", "May be NULL"),
    Col("bound", "VARCHAR", "Direction of travel.", "One of N, S, E, W, B or NULL"),
    Col("line", "VARCHAR", "Canonical line.", "One of YU, BD, SHP, SRT, YU/BD, OTHER or NULL"),
    Col("vehicle", "INTEGER", "Vehicle (train) number; 0 means none recorded.", "May be NULL"),
    Col("_source", "VARCHAR", "Source file the row came from (lineage).", "NOT NULL"),
    Col("_ingested_at", "VARCHAR", "UTC timestamp when the bronze file was written (lineage).", "ISO 8601"),
    Col("is_station", "BOOLEAN", "FALSE when the location is a track segment or a whole line rather than one station.", ""),
]

_INC = Col("incidents", "BIGINT", "Number of incidents.", "> 0")
_TOT = Col("total_delay_min", "HUGEINT", "Sum of min_delay in minutes.", ">= 0")
_AVG = Col("avg_delay_min", "DOUBLE", "Average min_delay, rounded to 2 decimals.", ">= 0")

TABLES: dict[str, Table] = {t.name: t for t in [
    Table("silver_delays", "silver", "Typed, cleaned, de-duplicated delay incidents. The single source of truth for gold.",
          "one row per distinct delay incident",
          [Col("delay_id", "BIGINT", "Surrogate key assigned at build time (not stable across rebuilds).", "Unique; NOT NULL")] + _SILVER_COLS,
          ["Rows failing validation are routed to silver_rejects, never silently dropped.",
           "Exact duplicates are removed."]),
    Table("silver_rejects", "silver", "Rows that failed validation, with the reason.",
          "one row per rejected source row", _SILVER_COLS + [
              Col("reject_reason", "VARCHAR", "Why the row was rejected.",
                  "One of bad_date, missing_station, bad_min_delay, min_delay_out_of_range")]),
    Table("ingestion_log", "bronze", "Incremental-load ledger: what was downloaded from which CKAN resource and when.",
          "one row per source resource", [
              Col("resource_id", "VARCHAR", "CKAN resource id.", "Unique"),
              Col("name", "VARCHAR", "CKAN resource name.", ""),
              Col("url", "VARCHAR", "Download URL.", ""),
              Col("format", "VARCHAR", "csv or xlsx.", ""),
              Col("last_modified", "VARCHAR", "last_modified reported by CKAN at ingest; unchanged value means skip.", "May be NULL"),
              Col("sha256", "VARCHAR", "SHA-256 of the downloaded bytes.", "64 hex characters"),
              Col("bytes", "BIGINT", "Size of the download.", ""),
              Col("rows", "BIGINT", "Rows written to bronze.", ""),
              Col("bronze_file", "VARCHAR", "Parquet file written under data/bronze.", ""),
              Col("ingested_at", "VARCHAR", "UTC time the data was last written to bronze.", "ISO 8601"),
              Col("checked_at", "VARCHAR", "UTC time the resource was last checked.", "ISO 8601")]),
    Table("gold_daily_summary", "gold", "Daily totals.", "one row per delay_date", [
        Col("delay_date", "DATE", "Calendar date.", "Unique"), _INC, _TOT, _AVG,
        Col("delay_incidents", "BIGINT", "Incidents with min_delay > 0.", ">= 0")]),
    Table("gold_station_ranking", "gold", "Stations ranked by total delay minutes (segments and whole-line rows excluded).",
          "one row per station", [
              Col("station", "VARCHAR", "Station name.", "Unique"), _INC, _TOT, _AVG,
              Col("delay_rank", "BIGINT", "Rank by total_delay_min, 1 = most minutes lost (ties share a rank).", ">= 1")]),
    Table("gold_hourly_pattern", "gold", "When delays happen.", "one row per hour_of_day and day_name", [
        Col("hour_of_day", "INTEGER", "Hour 0-23.", "0 to 23"),
        Col("day_name", "VARCHAR", "Day of week.", ""), _INC, _AVG]),
    Table("gold_monthly_trend", "gold", "Monthly trend per line with month-over-month change.", "one row per month and line", [
        Col("month", "DATE", "First day of the month.", ""),
        Col("line", "VARCHAR", "Canonical line.", "May be NULL"), _INC, _TOT, _AVG,
        Col("mom_change_pct", "DOUBLE", "Change in total_delay_min versus the previous month with data for the same line.", "NULL for the first month")]),
    Table("gold_cause_breakdown", "gold", "Delay causes with descriptions.", "one row per delay code", [
        Col("code", "VARCHAR", "Delay code.", "Unique"),
        Col("description", "VARCHAR", "Description from the code lookup.", "'Unknown code' when not found"),
        _INC, _TOT, _AVG]),
]}
