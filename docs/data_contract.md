# Data contract: TTC subway delays

Version 1.0.0. Rendered from `src/ttc_pipeline/contract.py` by `python -m ttc_pipeline.cli docs`; a test fails if the contract and the DuckDB catalog disagree.

## Ownership

- **Data owner / steward:** Amir Patel (data owner and steward)
- **Source owner:** City of Toronto / Toronto Transit Commission, published on Toronto Open Data (Open Government Licence - Toronto)
- **Consumers:** Analysts and the Day 3 dashboard; read-only access to the gold_* tables

## Service levels

| Area | Commitment |
|---|---|
| Freshness | Pipeline runs daily (Airflow @daily). A resource the City re-publishes is picked up by the next run, so the warehouse is at most 24 hours behind the portal. The portal itself publishes irregularly (the 'since 2025' file was last modified 2026-10-26 when checked on 2026-11-07). |
| Completeness | Reject rate below 5% of source rows (enforced by check reject_rate_under_5pct). Rows with unknown or blank line below 2% (enforced by unknown_line_rate_under_2pct). |
| Correctness gate | All 9 SQL data-quality checks pass or the run exits non-zero and the DAG task fails. |
| Reconciliation | sum(gold_daily_summary.incidents) equals count(silver_delays) exactly. |
| Run time | Incremental run under 10 minutes on a laptop; unchanged sources are skipped without a download. |
| Change policy | Adding a column is non-breaking. Renaming, removing or re-typing a column is breaking: bump the contract MAJOR version and update this file in the same commit (a test enforces it). |

## Tables

### silver_delays

Typed, cleaned, de-duplicated delay incidents. The single source of truth for gold. Layer: silver. Grain: one row per distinct delay incident.

| Column | Type | Description | Rule |
|---|---|---|---|
| `delay_id` | BIGINT | Surrogate key assigned at build time (not stable across rebuilds). | Unique; NOT NULL |
| `delay_date` | DATE | Calendar date of the incident. | NOT NULL; not in the future |
| `delay_time` | TIME | Time of day the incident was logged. | Parsed from HH:MM; NULL if unparseable |
| `day_name` | VARCHAR | Day of week as published, upper-cased. | - |
| `station` | VARCHAR | Cleaned station (or location) name. | NOT NULL and non-empty; upper-case; STATION suffix and line qualifier removed |
| `code` | VARCHAR | TTC delay code, upper-cased. | Joined to the code lookup in gold_cause_breakdown |
| `min_delay` | INTEGER | Minutes of delay to service. | NOT NULL; 0 to 1440 |
| `min_gap` | INTEGER | Minutes between trains caused by the incident. | May be NULL |
| `bound` | VARCHAR | Direction of travel. | One of N, S, E, W, B or NULL |
| `line` | VARCHAR | Canonical line. | One of YU, BD, SHP, SRT, YU/BD, OTHER or NULL |
| `vehicle` | INTEGER | Vehicle (train) number; 0 means none recorded. | May be NULL |
| `_source` | VARCHAR | Source file the row came from (lineage). | NOT NULL |
| `_ingested_at` | VARCHAR | UTC timestamp when the bronze file was written (lineage). | ISO 8601 |
| `is_station` | BOOLEAN | FALSE when the location is a track segment or a whole line rather than one station. | - |

- Rows failing validation are routed to silver_rejects, never silently dropped.
- Exact duplicates are removed.

### silver_rejects

Rows that failed validation, with the reason. Layer: silver. Grain: one row per rejected source row.

| Column | Type | Description | Rule |
|---|---|---|---|
| `delay_date` | DATE | Calendar date of the incident. | NOT NULL; not in the future |
| `delay_time` | TIME | Time of day the incident was logged. | Parsed from HH:MM; NULL if unparseable |
| `day_name` | VARCHAR | Day of week as published, upper-cased. | - |
| `station` | VARCHAR | Cleaned station (or location) name. | NOT NULL and non-empty; upper-case; STATION suffix and line qualifier removed |
| `code` | VARCHAR | TTC delay code, upper-cased. | Joined to the code lookup in gold_cause_breakdown |
| `min_delay` | INTEGER | Minutes of delay to service. | NOT NULL; 0 to 1440 |
| `min_gap` | INTEGER | Minutes between trains caused by the incident. | May be NULL |
| `bound` | VARCHAR | Direction of travel. | One of N, S, E, W, B or NULL |
| `line` | VARCHAR | Canonical line. | One of YU, BD, SHP, SRT, YU/BD, OTHER or NULL |
| `vehicle` | INTEGER | Vehicle (train) number; 0 means none recorded. | May be NULL |
| `_source` | VARCHAR | Source file the row came from (lineage). | NOT NULL |
| `_ingested_at` | VARCHAR | UTC timestamp when the bronze file was written (lineage). | ISO 8601 |
| `is_station` | BOOLEAN | FALSE when the location is a track segment or a whole line rather than one station. | - |
| `reject_reason` | VARCHAR | Why the row was rejected. | One of bad_date, missing_station, bad_min_delay, min_delay_out_of_range |

### ingestion_log

Incremental-load ledger: what was downloaded from which CKAN resource and when. Layer: bronze. Grain: one row per source resource.

| Column | Type | Description | Rule |
|---|---|---|---|
| `resource_id` | VARCHAR | CKAN resource id. | Unique |
| `name` | VARCHAR | CKAN resource name. | - |
| `url` | VARCHAR | Download URL. | - |
| `format` | VARCHAR | csv or xlsx. | - |
| `last_modified` | VARCHAR | last_modified reported by CKAN at ingest; unchanged value means skip. | May be NULL |
| `sha256` | VARCHAR | SHA-256 of the downloaded bytes. | 64 hex characters |
| `bytes` | BIGINT | Size of the download. | - |
| `rows` | BIGINT | Rows written to bronze. | - |
| `bronze_file` | VARCHAR | Parquet file written under data/bronze. | - |
| `ingested_at` | VARCHAR | UTC time the data was last written to bronze. | ISO 8601 |
| `checked_at` | VARCHAR | UTC time the resource was last checked. | ISO 8601 |

### gold_daily_summary

Daily totals. Layer: gold. Grain: one row per delay_date.

| Column | Type | Description | Rule |
|---|---|---|---|
| `delay_date` | DATE | Calendar date. | Unique |
| `incidents` | BIGINT | Number of incidents. | > 0 |
| `total_delay_min` | HUGEINT | Sum of min_delay in minutes. | >= 0 |
| `avg_delay_min` | DOUBLE | Average min_delay, rounded to 2 decimals. | >= 0 |
| `delay_incidents` | BIGINT | Incidents with min_delay > 0. | >= 0 |

### gold_station_ranking

Stations ranked by total delay minutes (segments and whole-line rows excluded). Layer: gold. Grain: one row per station.

| Column | Type | Description | Rule |
|---|---|---|---|
| `station` | VARCHAR | Station name. | Unique |
| `incidents` | BIGINT | Number of incidents. | > 0 |
| `total_delay_min` | HUGEINT | Sum of min_delay in minutes. | >= 0 |
| `avg_delay_min` | DOUBLE | Average min_delay, rounded to 2 decimals. | >= 0 |
| `delay_rank` | BIGINT | Rank by total_delay_min, 1 = most minutes lost (ties share a rank). | >= 1 |

### gold_hourly_pattern

When delays happen. Layer: gold. Grain: one row per hour_of_day and day_name.

| Column | Type | Description | Rule |
|---|---|---|---|
| `hour_of_day` | INTEGER | Hour 0-23. | 0 to 23 |
| `day_name` | VARCHAR | Day of week. | - |
| `incidents` | BIGINT | Number of incidents. | > 0 |
| `avg_delay_min` | DOUBLE | Average min_delay, rounded to 2 decimals. | >= 0 |

### gold_monthly_trend

Monthly trend per line with month-over-month change. Layer: gold. Grain: one row per month and line.

| Column | Type | Description | Rule |
|---|---|---|---|
| `month` | DATE | First day of the month. | - |
| `line` | VARCHAR | Canonical line. | May be NULL |
| `incidents` | BIGINT | Number of incidents. | > 0 |
| `total_delay_min` | HUGEINT | Sum of min_delay in minutes. | >= 0 |
| `avg_delay_min` | DOUBLE | Average min_delay, rounded to 2 decimals. | >= 0 |
| `mom_change_pct` | DOUBLE | Change in total_delay_min versus the previous month with data for the same line. | NULL for the first month |

### gold_cause_breakdown

Delay causes with descriptions. Layer: gold. Grain: one row per delay code.

| Column | Type | Description | Rule |
|---|---|---|---|
| `code` | VARCHAR | Delay code. | Unique |
| `description` | VARCHAR | Description from the code lookup. | 'Unknown code' when not found |
| `incidents` | BIGINT | Number of incidents. | > 0 |
| `total_delay_min` | HUGEINT | Sum of min_delay in minutes. | >= 0 |
| `avg_delay_min` | DOUBLE | Average min_delay, rounded to 2 decimals. | >= 0 |

## Automated enforcement

The checks in `src/ttc_pipeline/quality.py` run on every `run` and `check`; each returns a violation count and the command fails if any is non-zero.

## Known data issues (observed on the live files)

- The 2017-2021 files are Excel workbooks with one sheet per month; the loader reads every sheet that has a Date column (a first version read only the first sheet and silently dropped 11 of 12 months a year).
- min_delay = 0 for about two thirds of rows (an incident was logged but no delay was recorded), so averages mix zero and non-zero delays; gold tables expose both incidents and delay_incidents (min_delay > 0).
- bound is blank for roughly 29% of rows in the source and is NULL in silver; values outside N/S/E/W/B are set to NULL.
- vehicle = 0 means 'no vehicle recorded', not train number zero.
- Station names are free text. Silver strips the STATION suffix (often truncated at 20 characters in the source) and trailing line qualifiers (BD, YU, YUS, SRT, SHP), so KENNEDY BD and KENNEDY SRT merge into KENNEDY. Rows naming a segment ('A TO B') or a whole line are kept with is_station = FALSE and excluded from gold_station_ranking.
- Exact duplicate rows (same date, time, station, code, delays, bound, line and vehicle) are collapsed to one; a genuine second incident with identical values in the same minute would also be collapsed.
- A small number of delay codes are not in the current lookup; legacy codes are filled from the 2022 workbook and the rest appear as 'Unknown code' in gold_cause_breakdown.
