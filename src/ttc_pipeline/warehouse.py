"""Build silver + gold layers inside a DuckDB file from the bronze parquet files."""
from __future__ import annotations

import logging
from pathlib import Path

import duckdb
import pandas as pd

from . import config, ingest

log = logging.getLogger(__name__)


def _run_script(con: duckdb.DuckDBPyConnection, path: Path, **fmt: str) -> None:
    sql = path.read_text(encoding="utf-8").format(**fmt)
    con.execute(sql)


def _load_ingestion_log(con: duckdb.DuckDBPyConnection, bronze_dir: Path) -> None:
    """Expose the incremental-load manifest as a queryable table."""
    manifest = ingest.load_manifest(bronze_dir)
    cols = ["resource_id", "name", "url", "format", "last_modified", "sha256", "bytes", "rows",
            "bronze_file", "ingested_at", "checked_at"]
    df = pd.DataFrame([{c: m.get(c) for c in cols} for m in manifest.values()], columns=cols)
    df = df.astype({"bytes": "Int64", "rows": "Int64"}).astype({c: "string" for c in cols if c not in ("bytes", "rows")})
    con.register("_manifest_df", df)
    con.execute("CREATE OR REPLACE TABLE ingestion_log AS SELECT * FROM _manifest_df")
    con.unregister("_manifest_df")


def build(bronze_dir: Path | None = None, db_path: Path | None = None) -> duckdb.DuckDBPyConnection:
    bronze_dir = bronze_dir or config.BRONZE
    db_path = db_path or config.WAREHOUSE
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

    _load_ingestion_log(con, bronze_dir)

    counts = {t: con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
              for t in ("silver_delays", "silver_rejects", "ingestion_log", "gold_daily_summary", "gold_station_ranking")}
    log.info("warehouse built: %s", counts)
    return con
