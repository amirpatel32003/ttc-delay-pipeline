# Warehouse schema

Generated from the DuckDB catalog by `python -m ttc_pipeline.cli docs` on 2026-11-07. Do not edit by hand. Row counts are for the warehouse the command was run against.

| Table | Layer | Rows | Columns |
|---|---|---:|---:|
| [`bronze_codes`](#bronze_codes) | internal | 212 | 2 |
| [`dim_codes`](#dim_codes) | internal | 212 | 2 |
| [`gold_cause_breakdown`](#gold_cause_breakdown) | gold | 236 | 5 |
| [`gold_daily_summary`](#gold_daily_summary) | gold | 4,656 | 5 |
| [`gold_hourly_pattern`](#gold_hourly_pattern) | gold | 168 | 4 |
| [`gold_monthly_trend`](#gold_monthly_trend) | gold | 935 | 6 |
| [`gold_station_ranking`](#gold_station_ranking) | gold | 920 | 5 |
| [`ingestion_log`](#ingestion_log) | bronze | 10 | 11 |
| [`silver_delays`](#silver_delays) | silver | 270,007 | 14 |
| [`silver_delays_all`](#silver_delays_all) | internal | 270,282 | 13 |
| [`silver_rejects`](#silver_rejects) | silver | 0 | 14 |

Contract status: catalog matches `contract.py`.

## bronze_codes

Internal working table (not part of the contract).

| Column | Type | Nullable | Description |
|---|---|---|---|
| `code` | VARCHAR | yes | (undocumented) |
| `description` | VARCHAR | yes | (undocumented) |

## dim_codes

Internal working table (not part of the contract).

| Column | Type | Nullable | Description |
|---|---|---|---|
| `code` | VARCHAR | yes | (undocumented) |
| `description` | VARCHAR | yes | (undocumented) |

## gold_cause_breakdown

Delay causes with descriptions. Grain: one row per delay code.

| Column | Type | Nullable | Description |
|---|---|---|---|
| `code` | VARCHAR | yes | Delay code. |
| `description` | VARCHAR | yes | Description from the code lookup. |
| `incidents` | BIGINT | yes | Number of incidents. |
| `total_delay_min` | HUGEINT | yes | Sum of min_delay in minutes. |
| `avg_delay_min` | DOUBLE | yes | Average min_delay, rounded to 2 decimals. |

## gold_daily_summary

Daily totals. Grain: one row per delay_date.

| Column | Type | Nullable | Description |
|---|---|---|---|
| `delay_date` | DATE | yes | Calendar date. |
| `incidents` | BIGINT | yes | Number of incidents. |
| `total_delay_min` | HUGEINT | yes | Sum of min_delay in minutes. |
| `avg_delay_min` | DOUBLE | yes | Average min_delay, rounded to 2 decimals. |
| `delay_incidents` | BIGINT | yes | Incidents with min_delay > 0. |

## gold_hourly_pattern

When delays happen. Grain: one row per hour_of_day and day_name.

| Column | Type | Nullable | Description |
|---|---|---|---|
| `hour_of_day` | INTEGER | yes | Hour 0-23. |
| `day_name` | VARCHAR | yes | Day of week. |
| `incidents` | BIGINT | yes | Number of incidents. |
| `avg_delay_min` | DOUBLE | yes | Average min_delay, rounded to 2 decimals. |

## gold_monthly_trend

Monthly trend per line with month-over-month change. Grain: one row per month and line.

| Column | Type | Nullable | Description |
|---|---|---|---|
| `month` | DATE | yes | First day of the month. |
| `line` | VARCHAR | yes | Canonical line. |
| `incidents` | BIGINT | yes | Number of incidents. |
| `total_delay_min` | HUGEINT | yes | Sum of min_delay in minutes. |
| `avg_delay_min` | DOUBLE | yes | Average min_delay, rounded to 2 decimals. |
| `mom_change_pct` | DOUBLE | yes | Change in total_delay_min versus the previous month with data for the same line. |

## gold_station_ranking

Stations ranked by total delay minutes (segments and whole-line rows excluded). Grain: one row per station.

| Column | Type | Nullable | Description |
|---|---|---|---|
| `station` | VARCHAR | yes | Station name. |
| `incidents` | BIGINT | yes | Number of incidents. |
| `total_delay_min` | HUGEINT | yes | Sum of min_delay in minutes. |
| `avg_delay_min` | DOUBLE | yes | Average min_delay, rounded to 2 decimals. |
| `delay_rank` | BIGINT | yes | Rank by total_delay_min, 1 = most minutes lost (ties share a rank). |

## ingestion_log

Incremental-load ledger: what was downloaded from which CKAN resource and when. Grain: one row per source resource.

| Column | Type | Nullable | Description |
|---|---|---|---|
| `resource_id` | VARCHAR | yes | CKAN resource id. |
| `name` | VARCHAR | yes | CKAN resource name. |
| `url` | VARCHAR | yes | Download URL. |
| `format` | VARCHAR | yes | csv or xlsx. |
| `last_modified` | VARCHAR | yes | last_modified reported by CKAN at ingest; unchanged value means skip. |
| `sha256` | VARCHAR | yes | SHA-256 of the downloaded bytes. |
| `bytes` | BIGINT | yes | Size of the download. |
| `rows` | BIGINT | yes | Rows written to bronze. |
| `bronze_file` | VARCHAR | yes | Parquet file written under data/bronze. |
| `ingested_at` | VARCHAR | yes | UTC time the data was last written to bronze. |
| `checked_at` | VARCHAR | yes | UTC time the resource was last checked. |

## silver_delays

Typed, cleaned, de-duplicated delay incidents. The single source of truth for gold. Grain: one row per distinct delay incident.

| Column | Type | Nullable | Description |
|---|---|---|---|
| `delay_id` | BIGINT | yes | Surrogate key assigned at build time (not stable across rebuilds). |
| `delay_date` | DATE | yes | Calendar date of the incident. |
| `delay_time` | TIME | yes | Time of day the incident was logged. |
| `day_name` | VARCHAR | yes | Day of week as published, upper-cased. |
| `station` | VARCHAR | yes | Cleaned station (or location) name. |
| `code` | VARCHAR | yes | TTC delay code, upper-cased. |
| `min_delay` | INTEGER | yes | Minutes of delay to service. |
| `min_gap` | INTEGER | yes | Minutes between trains caused by the incident. |
| `bound` | VARCHAR | yes | Direction of travel. |
| `line` | VARCHAR | yes | Canonical line. |
| `vehicle` | INTEGER | yes | Vehicle (train) number; 0 means none recorded. |
| `_source` | VARCHAR | yes | Source file the row came from (lineage). |
| `_ingested_at` | VARCHAR | yes | UTC timestamp when the bronze file was written (lineage). |
| `is_station` | BOOLEAN | yes | FALSE when the location is a track segment or a whole line rather than one station. |

## silver_delays_all

Internal working table (not part of the contract).

| Column | Type | Nullable | Description |
|---|---|---|---|
| `delay_date` | DATE | yes | (undocumented) |
| `delay_time` | TIME | yes | (undocumented) |
| `day_name` | VARCHAR | yes | (undocumented) |
| `station` | VARCHAR | yes | (undocumented) |
| `code` | VARCHAR | yes | (undocumented) |
| `min_delay` | INTEGER | yes | (undocumented) |
| `min_gap` | INTEGER | yes | (undocumented) |
| `bound` | VARCHAR | yes | (undocumented) |
| `line` | VARCHAR | yes | (undocumented) |
| `vehicle` | INTEGER | yes | (undocumented) |
| `_source` | VARCHAR | yes | (undocumented) |
| `_ingested_at` | VARCHAR | yes | (undocumented) |
| `is_station` | BOOLEAN | yes | (undocumented) |

## silver_rejects

Rows that failed validation, with the reason. Grain: one row per rejected source row.

| Column | Type | Nullable | Description |
|---|---|---|---|
| `delay_date` | DATE | yes | Calendar date of the incident. |
| `delay_time` | TIME | yes | Time of day the incident was logged. |
| `day_name` | VARCHAR | yes | Day of week as published, upper-cased. |
| `station` | VARCHAR | yes | Cleaned station (or location) name. |
| `code` | VARCHAR | yes | TTC delay code, upper-cased. |
| `min_delay` | INTEGER | yes | Minutes of delay to service. |
| `min_gap` | INTEGER | yes | Minutes between trains caused by the incident. |
| `bound` | VARCHAR | yes | Direction of travel. |
| `line` | VARCHAR | yes | Canonical line. |
| `vehicle` | INTEGER | yes | Vehicle (train) number; 0 means none recorded. |
| `_source` | VARCHAR | yes | Source file the row came from (lineage). |
| `_ingested_at` | VARCHAR | yes | UTC timestamp when the bronze file was written (lineage). |
| `is_station` | BOOLEAN | yes | FALSE when the location is a track segment or a whole line rather than one station. |
| `reject_reason` | VARCHAR | yes | Why the row was rejected. |
