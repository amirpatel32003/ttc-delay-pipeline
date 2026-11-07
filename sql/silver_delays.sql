-- Silver: typed, cleaned, de-duplicated delays. Bad rows go to silver_rejects with a reason.
CREATE OR REPLACE TABLE silver_delays_all AS
-- is_station = FALSE for rows that name a track segment ('A TO B') or a whole line, not one station
SELECT *, NOT coalesce(regexp_matches(station, ' TO |UNIVERSITY|\bLINE\b|SUBWAY|^(YUS?|BD)[/-]'), FALSE) AS is_station
FROM (
SELECT
    try_cast("date" AS DATE)                                        AS delay_date,
    try_strptime(trim("time"), '%H:%M')::TIME                       AS delay_time,
    upper(trim("day"))                                              AS day_name,
    -- strip the (often truncated) STATION suffix, then a trailing line qualifier such as 'KENNEDY BD'
    nullif(trim(regexp_replace(regexp_replace(regexp_replace(upper(trim(station)), '\s+', ' ', 'g'),
        '\s+STAT(ION|IO|I)?(\s.*)?$', ''), '\s+(BD|YUS?|SRT|SHP)$', '')), '')  AS station,
    upper(trim(code))                                               AS code,
    try_cast(min_delay AS INTEGER)                                  AS min_delay,
    try_cast(min_gap AS INTEGER)                                    AS min_gap,
    CASE WHEN upper(trim(bound)) IN ('N', 'S', 'E', 'W', 'B') THEN upper(trim(bound)) END AS bound,
    CASE
        WHEN line IS NULL OR trim(line) = ''                                              THEN NULL
        WHEN regexp_replace(upper(line), '[^A-Z/]', '', 'g') SIMILAR TO '(YUS?|YULINE|YONGEUNIVERSITY)' THEN 'YU'
        WHEN regexp_replace(upper(line), '[^A-Z/]', '', 'g') IN ('BD', 'BDLINE', 'B/D', 'BLOORDANF', 'BLOORDANFORTH') THEN 'BD'
        WHEN regexp_replace(upper(line), '[^A-Z/]', '', 'g') IN ('SHP', 'SHEPPARD', 'SHEPPARDLINE') THEN 'SHP'
        WHEN regexp_replace(upper(line), '[^A-Z/]', '', 'g') IN ('SRT', 'SCARBOROUGHRT')   THEN 'SRT'
        WHEN regexp_replace(upper(line), '[^A-Z/]', '', 'g') LIKE '%YU%'
         AND regexp_replace(upper(line), '[^A-Z/]', '', 'g') LIKE '%BD%'                  THEN 'YU/BD'
        ELSE 'OTHER'
    END                                                             AS line,
    try_cast(vehicle AS INTEGER)                                    AS vehicle,
    _source,
    _ingested_at
FROM read_parquet('{bronze_glob}')
) src;

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
