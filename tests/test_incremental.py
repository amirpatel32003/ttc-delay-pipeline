import pandas as pd
import pytest

from ttc_pipeline import config, ingest, warehouse

CSV = b"Date,Time,Day,Station,Code,Min Delay,Min Gap,Bound,Line,Vehicle\n2025-01-01,02:10,Wednesday,BATHURST STATION,MUSAN,5,9,E,BD,5227\n"
CSV2 = CSV + b"2025-01-02,03:10,Thursday,DUNDAS STATION,MUIS,2,4,N,YU,5001\n"


class Fetcher:
    def __init__(self, content):
        self.content, self.calls = content, 0

    def __call__(self, url):
        self.calls += 1
        return self.content


def res(last_modified="2026-10-26T12:50:38"):
    return [{"id": "r1", "name": "TTC Subway Delay Data since 2025.csv", "format": "CSV",
             "url": "https://x/2025.csv", "last_modified": last_modified}]


def test_first_run_ingests_and_writes_manifest(tmp_path):
    f = Fetcher(CSV)
    out = ingest.ingest_resources(res(), f, tmp_path)
    assert out.ingested and f.calls == 1
    m = ingest.load_manifest(tmp_path)["r1"]
    assert m["rows"] == 1 and m["last_modified"] == "2026-10-26T12:50:38" and len(m["sha256"]) == 64
    assert (tmp_path / m["bronze_file"]).exists()


def test_second_run_skips_without_downloading(tmp_path):
    ingest.ingest_resources(res(), Fetcher(CSV), tmp_path)
    f = Fetcher(CSV)
    out = ingest.ingest_resources(res(), f, tmp_path)
    assert out.skipped_unchanged and not out.ingested and f.calls == 0


def test_new_last_modified_with_new_content_reingests(tmp_path):
    ingest.ingest_resources(res(), Fetcher(CSV), tmp_path)
    out = ingest.ingest_resources(res("2026-11-01T00:00:00"), Fetcher(CSV2), tmp_path)
    assert out.ingested
    bronze = next(tmp_path.glob("delays_*.parquet"))
    assert len(pd.read_parquet(bronze)) == 2
    assert ingest.load_manifest(tmp_path)["r1"]["rows"] == 2


def test_new_last_modified_same_bytes_is_not_rewritten(tmp_path):
    ingest.ingest_resources(res(), Fetcher(CSV), tmp_path)
    bronze = next(tmp_path.glob("delays_*.parquet"))
    before = bronze.stat().st_mtime_ns
    out = ingest.ingest_resources(res("2026-11-01T00:00:00"), Fetcher(CSV), tmp_path)
    assert out.skipped_same_hash and not out.ingested
    assert bronze.stat().st_mtime_ns == before
    assert ingest.load_manifest(tmp_path)["r1"]["last_modified"] == "2026-11-01T00:00:00"


def test_missing_last_modified_falls_back_to_hash(tmp_path):
    ingest.ingest_resources(res(None), Fetcher(CSV), tmp_path)
    f = Fetcher(CSV)
    out = ingest.ingest_resources(res(None), f, tmp_path)
    assert f.calls == 1 and out.skipped_same_hash


def test_deleted_bronze_file_is_restored(tmp_path):
    ingest.ingest_resources(res(), Fetcher(CSV), tmp_path)
    next(tmp_path.glob("delays_*.parquet")).rename(tmp_path / "moved.bak")
    out = ingest.ingest_resources(res(), Fetcher(CSV), tmp_path)
    assert out.ingested


def test_force_redownloads(tmp_path):
    ingest.ingest_resources(res(), Fetcher(CSV), tmp_path)
    f = Fetcher(CSV)
    out = ingest.ingest_resources(res(), f, tmp_path, force=True)
    assert f.calls == 1 and out.ingested


def test_sample_ingest_is_incremental(tmp_path, monkeypatch):
    from ttc_pipeline import sample
    monkeypatch.setattr(config, "SAMPLE", tmp_path / "sample")
    sample.write_sample(config.SAMPLE)
    bronze = tmp_path / "bronze"
    assert ingest.ingest_from_sample(bronze).ingested
    again = ingest.ingest_from_sample(bronze)
    assert not again.ingested and again.skipped_same_hash


def test_ingestion_log_table_exposes_manifest(tmp_path):
    ingest.ingest_resources(res(), Fetcher(CSV), tmp_path / "b")
    con = warehouse.build(tmp_path / "b", tmp_path / "w.duckdb")
    row = con.execute("SELECT resource_id, rows, last_modified FROM ingestion_log").fetchall()
    assert row == [("r1", 1, "2026-10-26T12:50:38")]


def test_ingestion_log_empty_without_manifest(tmp_path):
    (tmp_path / "b").mkdir()
    pd.DataFrame({c: ["x"] for c in list(config.COLUMN_ALIASES) + ["_source", "_ingested_at"]}).to_parquet(
        tmp_path / "b" / "delays_x.parquet", index=False)
    con = warehouse.build(tmp_path / "b", tmp_path / "w.duckdb")
    assert con.execute("SELECT count(*) FROM ingestion_log").fetchone()[0] == 0
