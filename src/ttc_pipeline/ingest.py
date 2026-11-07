"""Bronze layer: download raw delay files from Toronto Open Data and store them as parquet.

Bronze keeps the data exactly as published (all columns as text) plus lineage columns, so
every later layer can be rebuilt from it.

Loads are incremental: a manifest (``_manifest.json`` next to the parquet files) records, per CKAN
resource, the ``last_modified`` timestamp and the SHA-256 of the downloaded bytes. A resource whose
``last_modified`` is unchanged is skipped without downloading; one that was re-published with
identical bytes is downloaded but not rewritten.
"""
from __future__ import annotations

import hashlib
import io
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import pandas as pd
import requests

from . import config

log = logging.getLogger(__name__)
HEADERS = {"User-Agent": "ttc-delay-pipeline/0.2 (portfolio project)"}
FORMAT_PREFERENCE = {"csv": 0, "xlsx": 1}  # the portal publishes the same data in several formats; take the cheapest


# --------------------------------------------------------------------------- resource discovery
def list_resources(package: str = config.DELAY_PACKAGE) -> list[dict]:
    r = requests.get(f"{config.CKAN}/package_show", params={"id": package}, headers=HEADERS, timeout=60)
    r.raise_for_status()
    return r.json()["result"]["resources"]


def _logical_name(res: dict) -> str:
    name = (res.get("name") or "").strip().lower()
    for ext in (".csv", ".xlsx", ".xml", ".json"):
        if name.endswith(ext):
            name = name[: -len(ext)]
    return name


def _is_datastore_dump(res: dict) -> bool:
    return "/datastore/dump/" in (res.get("url") or "")


def pick_delay_files(resources: list[dict]) -> list[dict]:
    """One resource per yearly delay dataset.

    Live catalogue findings (Nov 2026): yearly XLSX files 2014-2024, plus "since 2025" published as
    a datastore XLSX dump *and* CSV/XML/JSON copies. We skip lookup/readme resources and datastore
    dumps, and keep a single file per logical dataset, preferring CSV over XLSX.
    """
    best: dict[str, dict] = {}
    for res in resources:
        name = _logical_name(res)
        fmt = (res.get("format") or "").lower()
        if fmt not in FORMAT_PREFERENCE or _is_datastore_dump(res):
            continue
        if "code" in name or "readme" in name or "description" in name:
            continue
        cur = best.get(name)
        if cur is None or FORMAT_PREFERENCE[fmt] < FORMAT_PREFERENCE[cur["format"].lower()]:
            best[name] = res
    return list(best.values())


def pick_code_file(resources: list[dict]) -> dict | None:
    """The delay-code lookup. Prefer the current 'Code Descriptions' CSV over the legacy 2022 workbook."""
    cands = []
    for res in resources:
        name = _logical_name(res)
        fmt = (res.get("format") or "").lower()
        if fmt not in FORMAT_PREFERENCE or _is_datastore_dump(res) or "readme" in name or "code" not in name:
            continue
        cands.append(((0 if "description" in name else 1), FORMAT_PREFERENCE[fmt], res))
    return min(cands, key=lambda c: c[:2])[2] if cands else None


# --------------------------------------------------------------------------- parsing
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


def _read_table(content: bytes, fmt: str) -> pd.DataFrame:
    if fmt.lower() == "xlsx":
        return pd.read_excel(io.BytesIO(content), dtype=str)
    return pd.read_csv(io.BytesIO(content), dtype=str, encoding_errors="replace")


def read_resource(content: bytes, fmt: str) -> pd.DataFrame:
    return normalize_columns(_read_table(content, fmt))


def _fix_mojibake(text):
    """The published code CSV contains UTF-8 text that was decoded as cp1252 once ('â\x80\x93' for an en dash)."""
    if not isinstance(text, str):
        return text
    try:
        return text.encode("cp1252").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return text


def read_codes(content: bytes, fmt: str) -> pd.DataFrame:
    """Delay-code lookup -> columns (code, description). Accepts 'CODE/DESCRIPTION' and the legacy header."""
    raw = _read_table(content, fmt)
    lows = {c: str(c).lower() for c in raw.columns}
    code_col = next((c for c, low in lows.items() if "code" in low and "desc" not in low), raw.columns[0])
    desc_col = next((c for c, low in lows.items() if "desc" in low), raw.columns[1])
    out = raw[[code_col, desc_col]].rename(columns={code_col: "code", desc_col: "description"})
    out = out.dropna(subset=["code"]).reset_index(drop=True)
    out["description"] = out["description"].map(_fix_mojibake)
    return out


# --------------------------------------------------------------------------- manifest
def manifest_path(bronze_dir: Path | None = None) -> Path:
    return (bronze_dir or config.BRONZE) / config.MANIFEST_NAME


def load_manifest(bronze_dir: Path | None = None) -> dict[str, dict]:
    p = manifest_path(bronze_dir)
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}


def save_manifest(manifest: dict[str, dict], bronze_dir: Path | None = None) -> None:
    p = manifest_path(bronze_dir)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")


def sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# --------------------------------------------------------------------------- bronze writer
def bronze_filename(source: str) -> str:
    stem = Path(source).stem
    return f"{stem if stem.startswith('delays_') else 'delays_' + stem}.parquet"


def to_bronze(df: pd.DataFrame, source: str, out_dir: Path | None = None) -> Path:
    out_dir = out_dir or config.BRONZE
    out_dir.mkdir(parents=True, exist_ok=True)
    df = df.copy()
    df["_source"] = source
    df["_ingested_at"] = _now()
    path = out_dir / bronze_filename(source)
    df.to_parquet(path, index=False)
    log.info("bronze %s -> %s (%d rows)", source, path.name, len(df))
    return path


# --------------------------------------------------------------------------- incremental ingest
@dataclass
class IngestResult:
    ingested: list[str] = field(default_factory=list)
    skipped_unchanged: list[str] = field(default_factory=list)   # last_modified identical -> not downloaded
    skipped_same_hash: list[str] = field(default_factory=list)   # re-published but identical bytes

    def summary(self) -> str:
        return (f"ingested={len(self.ingested)} skipped_unchanged={len(self.skipped_unchanged)} "
                f"skipped_same_hash={len(self.skipped_same_hash)}")


def ingest_resources(resources: list[dict], fetch: Callable[[str], bytes], bronze_dir: Path | None = None,
                     force: bool = False) -> IngestResult:
    """Download only what is new or changed. ``fetch(url) -> bytes`` is injectable so tests need no network."""
    bronze_dir = bronze_dir or config.BRONZE
    manifest = load_manifest(bronze_dir)
    result = IngestResult()
    for res in resources:
        key = res.get("id") or res["url"]
        fmt = res["format"].lower()
        source = f"{_logical_name(res) or 'delays'}.{fmt}".replace(" ", "_")
        parquet = bronze_dir / bronze_filename(source)
        entry = manifest.get(key)
        last_modified = res.get("last_modified")
        have_file = parquet.exists()

        if not force and entry and have_file and last_modified and entry.get("last_modified") == last_modified:
            log.info("skip %s (unchanged since %s)", source, last_modified)
            result.skipped_unchanged.append(source)
            continue

        log.info("downloading %s", res.get("name"))
        content = fetch(res["url"])
        digest = sha256(content)
        if not force and entry and have_file and entry.get("sha256") == digest:
            log.info("skip %s (same content hash)", source)
            entry.update(last_modified=last_modified, checked_at=_now())
            result.skipped_same_hash.append(source)
            save_manifest(manifest, bronze_dir)
            continue

        df = read_resource(content, fmt)
        to_bronze(df, source, bronze_dir)
        manifest[key] = {
            "resource_id": key, "name": res.get("name"), "url": res["url"], "format": fmt,
            "last_modified": last_modified, "sha256": digest, "bytes": len(content), "rows": int(len(df)),
            "bronze_file": parquet.name, "ingested_at": _now(), "checked_at": _now(),
        }
        result.ingested.append(source)
        save_manifest(manifest, bronze_dir)
    return result


def _http_fetch(url: str) -> bytes:
    r = requests.get(url, headers=HEADERS, timeout=180)
    r.raise_for_status()
    return r.content


def ingest_from_api(limit_files: int | None = None, force: bool = False, bronze_dir: Path | None = None) -> IngestResult:
    files = pick_delay_files(list_resources())
    if limit_files:
        files = files[:limit_files]
    result = ingest_resources(files, _http_fetch, bronze_dir, force=force)
    log.info("ingest done: %s", result.summary())
    return result


def ingest_from_sample(bronze_dir: Path | None = None) -> IngestResult:
    """Offline path used by tests / CI: load the CSV files in data/sample (same incremental logic)."""
    res = [{"id": f.name, "name": f.stem, "format": "csv", "url": str(f), "last_modified": None}
           for f in sorted(config.SAMPLE.glob("delays_*.csv"))]
    return ingest_resources(res, lambda u: Path(u).read_bytes(), bronze_dir)


def read_legacy_codes(content: bytes) -> pd.DataFrame:
    """The 2022 workbook holds two side-by-side tables (subway code|description, SRT code|description)."""
    raw = pd.read_excel(io.BytesIO(content), header=None, dtype=str)
    parts = []
    for a, b in ((2, 3), (6, 7)):
        if raw.shape[1] > b:
            part = raw.iloc[:, [a, b]].copy()
            part.columns = ["code", "description"]
            parts.append(part)
    out = pd.concat(parts, ignore_index=True).dropna(subset=["code", "description"])
    out["code"] = out["code"].str.strip()
    out = out[~out["code"].str.upper().str.contains("RMENU")]
    return out.drop_duplicates("code").reset_index(drop=True)


def pick_legacy_code_file(resources: list[dict], primary: dict | None) -> dict | None:
    for res in resources:
        name = _logical_name(res)
        if (res is not primary and "code" in name and "description" not in name and "readme" not in name
                and (res.get("format") or "").lower() == "xlsx" and not _is_datastore_dump(res)):
            return res
    return None


def merge_codes(current: pd.DataFrame, legacy: pd.DataFrame) -> pd.DataFrame:
    """Current lookup wins; legacy rows only fill codes the current table no longer lists."""
    known = set(current["code"].str.strip().str.upper())
    extra = legacy[~legacy["code"].str.strip().str.upper().isin(known)]
    return pd.concat([current, extra], ignore_index=True)


def ingest_codes(bronze_dir: Path | None = None, resources: list[dict] | None = None,
                 fetch: Callable[[str], bytes] = _http_fetch) -> Path | None:
    """Refresh the (tiny) delay-code lookup every run: current CSV plus legacy workbook as fallback."""
    resources = resources if resources is not None else list_resources()
    res = pick_code_file(resources)
    if res is None:
        log.warning("no delay-code resource found")
        return None
    out_dir = bronze_dir or config.BRONZE
    out_dir.mkdir(parents=True, exist_ok=True)
    codes = read_codes(fetch(res["url"]), res["format"])
    legacy_res = pick_legacy_code_file(resources, res)
    if legacy_res is not None:
        try:
            codes = merge_codes(codes, read_legacy_codes(fetch(legacy_res["url"])))
        except Exception as exc:  # legacy layout is best-effort
            log.warning("legacy code workbook skipped: %s", exc)
    path = out_dir / "delay_codes.parquet"
    codes.to_parquet(path, index=False)
    log.info("codes %s (+legacy) -> %d rows", res.get("name"), len(codes))
    return path
