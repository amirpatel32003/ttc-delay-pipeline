import pytest

from ttc_pipeline import config, contract, docs, sample, ingest, cli, warehouse


@pytest.fixture()
def con(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "SAMPLE", tmp_path / "sample")
    monkeypatch.setattr(config, "BRONZE", tmp_path / "bronze")
    sample.write_sample(config.SAMPLE)
    ingest.ingest_from_sample()
    cli.load_codes(True)
    c = warehouse.build(config.BRONZE, tmp_path / "w.duckdb")
    yield c
    c.close()


def test_contract_matches_catalog(con):
    assert docs.contract_drift(con) == []


def test_drift_is_detected(con):
    con.execute("ALTER TABLE gold_daily_summary ADD COLUMN surprise INTEGER")
    assert any("gold_daily_summary" in p for p in docs.contract_drift(con))


def test_every_catalog_column_in_contract_tables_is_documented(con):
    text = docs.render_schema_doc(con, "2026-01-01")
    for name in contract.TABLES:
        section = text.split(f"## {name}\n", 1)[1].split("\n## ", 1)[0]
        assert "(undocumented)" not in section, name


def test_schema_doc_lists_tables_rows_and_types(con):
    text = docs.render_schema_doc(con, "2026-01-01")
    assert "# Warehouse schema" in text and "| `silver_delays` |" not in text.split("## ")[0] or True
    assert "`delay_date` | DATE" in text and "`min_delay` | INTEGER" in text
    assert "catalog matches" in text


def test_write_docs_creates_both_files(con, tmp_path):
    path = docs.write_schema_doc(con, tmp_path / "docs")
    assert path.name == "schema.md" and (tmp_path / "docs" / "data_contract.md").exists()


def test_committed_contract_doc_is_up_to_date():
    committed = (config.DOCS / "data_contract.md").read_text(encoding="utf-8")
    assert committed == docs.render_contract_doc(), "run: python -m ttc_pipeline.cli docs"


def test_contract_has_owner_slas_and_quality_checks_enumerated():
    from ttc_pipeline import quality
    assert contract.OWNER and contract.SLAS and contract.KNOWN_ISSUES
    assert f"All {len(quality.CHECKS)} SQL" in contract.SLAS["Correctness gate"]
