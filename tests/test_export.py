"""The dashboard export: shape, JSON-serialisable, and every table the page reads is present."""
import json

import pytest

from ttc_pipeline import config, export, ingest, warehouse
from ttc_pipeline.cli import load_codes
from ttc_pipeline.sample import write_sample

KEYS = {"asof", "generated_at", "kpi", "yearly", "monthly", "stations", "hourly", "lines", "line_year", "buckets",
        "causes", "cause_year", "watch", "causes_ltm", "line_ltm", "ingestion", "checks", "group_year", "group_ltm", "heat", "no_time", "daily", "stations_all"}


@pytest.fixture()
def payload(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "BRONZE", tmp_path / "bronze")
    monkeypatch.setattr(config, "WAREHOUSE", tmp_path / "wh")
    monkeypatch.setattr(config, "SAMPLE", tmp_path / "sample")
    write_sample(config.SAMPLE)
    ingest.ingest_from_sample()
    load_codes(True)
    con = warehouse.build()
    return export.build_payload(con), con


def test_payload_has_every_key_and_is_json(payload):
    data, _ = payload
    assert KEYS <= set(data)
    json.dumps(data)


def test_kpi_matches_the_warehouse(payload):
    data, con = payload
    n = con.execute("SELECT count(*) FROM silver_delays").fetchone()[0]
    assert data["kpi"][0]["incidents"] == n
    assert sum(r["incidents"] for r in data["yearly"]) == n
    assert sum(r["incidents"] for r in data["buckets"]) == n


def test_severity_buckets_add_up(payload):
    data, _ = payload
    for r in data["buckets"]:
        assert r["b0"] + r["b1"] + r["b2"] + r["b3"] + r["b4"] == r["incidents"]


def test_write_payload_creates_file(payload, tmp_path):
    _, con = payload
    path = export.write_payload(con, tmp_path / "out" / "dashboard_data.json")
    assert json.loads(path.read_text())["asof"]


def test_groups_heat_and_daily_reconcile_to_totals(payload):
    data, con = payload
    n = con.execute("SELECT count(*) FROM silver_delays").fetchone()[0]
    minutes = con.execute("SELECT sum(min_delay) FROM silver_delays").fetchone()[0]
    assert sum(r["incidents"] for r in data["group_year"]) == n
    assert sum(r["minutes"] for r in data["group_year"]) == minutes
    assert sum(r["incidents"] for r in data["heat"]) + data["no_time"] == n
    assert sum(d[1] for d in data["daily"]) == n
    assert sum(d[2] for d in data["daily"]) == minutes
    assert sum(r["total_delay_min"] for r in data["monthly"]) == minutes


def test_every_description_gets_exactly_one_group():
    from ttc_pipeline.export import CAUSE_GROUPS, OTHER_GROUP
    names = [n for n, _ in CAUSE_GROUPS] + [OTHER_GROUP]
    assert len(names) == len(set(names))


def test_station_list_is_ranked_and_within_totals(payload):
    data, con = payload
    st = data["stations_all"]
    assert st and st[0]["min_rank"] == 1
    assert [r["total_min"] for r in st] == sorted((r["total_min"] for r in st), reverse=True)
    assert sum(r["incidents"] for r in st) <= data["kpi"][0]["incidents"]
    assert len({r["station"] for r in st}) == len(st)
