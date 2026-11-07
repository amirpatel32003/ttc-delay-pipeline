-- Gold: analytics marts built from silver_delays.
CREATE OR REPLACE TABLE gold_daily_summary AS
SELECT delay_date,
       count(*)                          AS incidents,
       sum(min_delay)                    AS total_delay_min,
       round(avg(min_delay), 2)          AS avg_delay_min,
       count(*) FILTER (WHERE min_delay > 0) AS delay_incidents
FROM silver_delays
GROUP BY delay_date;

CREATE OR REPLACE TABLE gold_station_ranking AS
SELECT station,
       count(*)                  AS incidents,
       sum(min_delay)            AS total_delay_min,
       round(avg(min_delay), 2)  AS avg_delay_min,
       rank() OVER (ORDER BY sum(min_delay) DESC) AS delay_rank
FROM silver_delays
GROUP BY station;

CREATE OR REPLACE TABLE gold_hourly_pattern AS
SELECT extract('hour' FROM delay_time)::INTEGER AS hour_of_day,
       day_name,
       count(*)                 AS incidents,
       round(avg(min_delay), 2) AS avg_delay_min
FROM silver_delays
WHERE delay_time IS NOT NULL
GROUP BY ALL;

CREATE OR REPLACE TABLE gold_monthly_trend AS
SELECT date_trunc('month', delay_date)::DATE AS month,
       line,
       count(*)                 AS incidents,
       sum(min_delay)           AS total_delay_min,
       round(avg(min_delay), 2) AS avg_delay_min,
       round(100.0 * (sum(min_delay) - lag(sum(min_delay)) OVER (PARTITION BY line ORDER BY date_trunc('month', delay_date)))
             / nullif(lag(sum(min_delay)) OVER (PARTITION BY line ORDER BY date_trunc('month', delay_date)), 0), 1) AS mom_change_pct
FROM silver_delays
GROUP BY date_trunc('month', delay_date), line;

CREATE OR REPLACE TABLE gold_cause_breakdown AS
SELECT s.code,
       coalesce(c.description, 'Unknown code') AS description,
       count(*)                 AS incidents,
       sum(s.min_delay)         AS total_delay_min,
       round(avg(s.min_delay), 2) AS avg_delay_min
FROM silver_delays s
LEFT JOIN {codes_table} c ON c.code = s.code
GROUP BY s.code, description;
