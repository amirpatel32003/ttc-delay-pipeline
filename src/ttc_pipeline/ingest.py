"""Bronze layer: download raw delay files from Toronto Open Data and store them as parquet.

Bronze keeps the data exactly as published (all columns as text) plus lineage columns, so
every later layer can be rebuilt from it.
"""
from __future__ import annotations

import io
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

from . import config

log = logging.getLogger(__name__)
HEADERS = {"User-Agent": "ttc-delay-pipeline/0.1 (portfolio project)"}


def list_resources(package: str = config.DELAY_PACKAGE) -> list[dict]:
    r = requests.get(f"{config.CKAN}/package_show", params={"id": package}, headers=HEADERS, timeout=60)
    r.raise_for_status()
    return r.json()["result"]["resources"]


def pick_delay_files(resources: list[dict]) -> list[dict]:
    """Yearly delay files (csv/xlsx). Skips the code lookup and readme resources."""
    out = []
    for res in resources:
        name = (res.get("name") or "").lower()
        fmt = (res.get("format") or "").lower()
        if fmt not in {"csv", "xlsx"}:
            continue
        if "code" in name or "readme" in name or "description" in name:
            continue
        out.append(res)
    return out


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Map the slightly different headers used across years onto one schema."""
    lookup = {a: canon for canon, aliases in config.COLUMN_ALIASES.items() for a in aliases}
    renamed = {}
    for col in df.columns:
        key = str(col).strip().lower()
        if key in lookup:
            renamed[col] = lookup[key]
    df = df.rename(columns=renamed)
    for canon in config.COLUMN_ALIASES:
        if canon not in df.columns:
            df[canon] = None
    return df[list(config.COLUMN_ALIASES)]


def read_resource(content: bytes, fmt: str) -> pd.DataFrame:
    if fmt.lower() == "xlsx":
        df = pd.read_excel(io.BytesIO(content), dtype=str)
    else:
        df = pd.read_csv(io.BytesIO(content), dtype=str, encoding_errors="replace")
    return normalize_columns(df)


def to_bronze(df: pd.DataFrame, source: str, out_dir: Path | None = None) -> Path:
    out_dir = out_dir or config.BRONZE
    out_dir.mkdir(parents=True, exist_ok=True)
    df = df.copy()
    df["_source"] = source
    df["_ingested_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    path = out_dir / f"{Path(source).stem if Path(source).stem.startswith('delays_') else 'delays_' + Path(source).stem}.parquet"
    df.to_parquet(path, index=False)
    log.info("bronze %s -> %s (%d rows)", source, path.name, len(df))
    return path


def ingest_from_api(limit_files: int | None = None) -> list[Path]:
    files = pick_delay_files(list_resources())
    if limit_files:
        files = files[:limit_files]
    paths = []
    for res in files:
        url, fmt = res["url"], res["format"]
        log.info("downloading %s", res.get("name"))
        r = requests.get(url, headers=HEADERS, timeout=180)
        r.raise_for_status()
        df = read_resource(r.content, fmt)
        paths.append(to_bronze(df, f"{res.get('name', 'delays')}.{fmt.lower()}".replace(" ", "_")))
    return paths


def ingest_from_sample() -> list[Path]:
    """Offline path used by tests / CI: load the CSV files in data/sample."""
    paths = []
    for f in sorted(config.SAMPLE.glob("delays_*.csv")):
        df = read_resource(f.read_bytes(), "csv")
        paths.append(to_bronze(df, f.name))
    return paths
