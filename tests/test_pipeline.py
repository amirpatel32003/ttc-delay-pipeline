import duckdb
import pytest

from ttc_pipeline import cli, config, ingest, quality, sample, warehouse


@pytest.fixture()
def env(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "SAMPLE", tmp_path / "sample")
    monkeypatch.setattr(config, "BRONZE", tmp_path / "bronze")
    monkeypatch.setattr(config, "WAREHOUSE", tmp_path / "wh" / "ttc.duckdb")
    sample.write_sample(config.SAMPLE)
    ingest.ingest_from_sample()
    cli.load_codes(True)
    con = warehouse.build(config.BRONZE, config.WAREHOUSE)
    yield con
    con.close()


def test_normalize_columns_handles_alternate_headers():
    import pandas as pd
    df = pd.DataFrame({" Min Delay ": ["3"], "STATION": ["X"], "Date": ["2024-01-01"]})
    out = ingest.normalize_columns(df)
    assert list(out.columns) == list(config.COLUMN_ALIASES)
    assert out.loc[0, "min_delay"] == "3"


def test_silver_rejects_bad_rows(env):
    reasons = {r[0] for r in env.execute("SELECT DISTINCT reject_reason FROM silver_rejects").fetchall()}
    assert {"bad_date", "missing_station", "min_delay_out_of_range"} & reasons
    assert env.execute("SELECT count(*) FROM silver_rejects").fetchone()[0] >= 3


def test_silver_removes_duplicates(env):
    dupes = env.execute("""SELECT count(*) FROM (
        SELECT delay_date, delay_time, station, code, min_delay, bound, line, vehicle, count(*) c
        FROM silver_delays GROUP BY ALL HAVING c > 1)""").fetchone()[0]
    assert dupes == 0


def test_station_suffix_stripped(env):
    assert env.execute("SELECT count(*) FROM silver_delays WHERE station LIKE '% STATION'").fetchone()[0] == 0


def test_gold_totals_match_silver(env):
    total = env.execute("SELECT sum(incidents) FROM gold_daily_summary").fetchone()[0]
    assert total == env.execute("SELECT count(*) FROM silver_delays").fetchone()[0]


def test_cause_breakdown_joins_descriptions(env):
    desc = dict(env.execute("SELECT code, description FROM gold_cause_breakdown").fetchall())
    assert desc["MUIS"] == "Injured or ill customer"


def test_all_quality_checks_pass(env):
    results = quality.run_checks(env)
    assert all(ok for _, ok, _ in results), results
