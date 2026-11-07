-- Silver: typed, cleaned, de-duplicated delays. Bad rows go to silver_rejects with a reason.
CREATE OR REPLACE TABLE silver_delays_all AS
SELECT
    try_cast("date" AS DATE)                                        AS delay_date,
    try_strptime(trim("time"), '%H:%M')::TIME                       AS delay_time,
    upper(trim("day"))                                              AS day_name,
    regexp_replace(upper(trim(station)), '\s+STATION$', '')         AS station,
    upper(trim(code))                                               AS code,
    try_cast(min_delay AS INTEGER)                                  AS min_delay,
    try_cast(min_gap AS INTEGER)                                    AS min_gap,
    upper(trim(bound))                                              AS bound,
    CASE
        WHEN upper(trim(line)) IN ('YU', 'YONGE-UNIVERSITY', 'YONGE UNIVERSITY', 'YU LINE') THEN 'YU'
        WHEN upper(trim(line)) IN ('BD', 'BLOOR-DANFORTH', 'BLOOR DANFORTH', 'BD LINE')     THEN 'BD'
        WHEN upper(trim(line)) IN ('SHP', 'SHEPPARD', 'SHEPPARD LINE')                      THEN 'SHP'
        WHEN upper(trim(line)) IN ('SRT', 'SCARBOROUGH RT')                                 THEN 'SRT'
        ELSE upper(trim(line))
    END                                                             AS line,
    try_cast(vehicle AS INTEGER)                                    AS vehicle,
    _source,
    _ingested_at
FROM read_parquet('{bronze_glob}');

CREATE OR REPLACE TABLE silver_rejects AS
SELECT *, CASE
        WHEN delay_date IS NULL THEN 'bad_date'
        WHEN station IS NULL OR station = '' THEN 'missing_station'
        WHEN min_delay IS NULL THEN 'bad_min_delay'
        WHEN min_delay < 0 OR min_delay > 1440 THEN 'min_delay_out_of_range'
    END AS reject_reason
FROM silver_delays_all
WHERE delay_date IS NULL OR station IS NULL OR station = '' OR min_delay IS NULL
      OR min_delay < 0 OR min_delay > 1440;

CREATE OR REPLACE TABLE silver_delays AS
SELECT DISTINCT ON (delay_date, delay_time, station, code, min_delay, bound, line, vehicle)
       row_number() OVER ()::BIGINT AS delay_id, *
FROM silver_delays_all
WHERE delay_date IS NOT NULL AND station IS NOT NULL AND station <> ''
      AND min_delay IS NOT NULL AND min_delay BETWEEN 0 AND 1440;
