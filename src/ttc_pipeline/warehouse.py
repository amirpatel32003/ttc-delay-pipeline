"""Build silver + gold layers inside a DuckDB file from the bronze parquet files."""
from __future__ import annotations

import logging
from pathlib import Path

import duckdb

from . import config

log = logging.getLogger(__name__)


def _run_script(con: duckdb.DuckDBPyConnection, path: Path, **fmt: str) -> None:
    sql = path.read_text(encoding="utf-8").format(**fmt)
    con.execute(sql)


def build(bronze_dir: Path = config.BRONZE, db_path: Path = config.WAREHOUSE) -> duckdb.DuckDBPyConnection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    glob = (bronze_dir / "delays_*.parquet").as_posix()
    _run_script(con, config.SQL / "silver_delays.sql", bronze_glob=glob)

    codes = bronze_dir / "delay_codes.parquet"
    if codes.exists():
        con.execute(f"CREATE OR REPLACE TABLE bronze_codes AS SELECT * FROM read_parquet('{codes.as_posix()}')")
    else:
        con.execute("CREATE OR REPLACE TABLE bronze_codes AS SELECT NULL::VARCHAR AS code, NULL::VARCHAR AS description WHERE false")
    con.execute("CREATE OR REPLACE VIEW dim_codes AS SELECT upper(trim(code)) AS code, any_value(trim(description)) AS description FROM bronze_codes GROUP BY 1")
    _run_script(con, config.SQL / "gold_models.sql", codes_table="dim_codes")

    counts = {t: con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
              for t in ("silver_delays", "silver_rejects", "gold_daily_summary", "gold_station_ranking")}
    log.info("warehouse built: %s", counts)
    return con
