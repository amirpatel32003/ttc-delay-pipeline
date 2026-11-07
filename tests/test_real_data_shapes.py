"""Regression tests built from quirks observed in the live Toronto Open Data files (Nov 2026)."""
import pandas as pd

from ttc_pipeline import config, ingest, warehouse

CKAN_RESOURCES = [  # trimmed copy of what package_show returns for ttc-subway-delay-data
    {"id": "codes-old", "name": "ttc-subway-delay-codes", "format": "XLSX", "url": "https://x/ttc-subway-delay-codes.xlsx"},
    {"id": "readme", "name": "ttc-subway-delay-data-readme", "format": "XLSX", "url": "https://x/readme.xlsx"},
    {"id": "y2024", "name": "ttc-subway-delay-data-2024", "format": "XLSX", "url": "https://x/2024.xlsx"},
    {"id": "dump", "name": "TTC Subway Delay Data since 2025", "format": "XLSX",
     "url": "https://x/datastore/dump/6088e14f"},
    {"id": "csv25", "name": "TTC Subway Delay Data since 2025.csv", "format": "CSV", "url": "https://x/2025.csv"},
    {"id": "xml25", "name": "TTC Subway Delay Data since 2025.xml", "format": "XML", "url": "https://x/2025.xml"},
    {"id": "json25", "name": "TTC Subway Delay Data since 2025.json", "format": "JSON", "url": "https://x/2025.json"},
    {"id": "cd", "name": "Code Descriptions", "format": "XLSX", "url": "https://x/datastore/dump/9e9e"},
    {"id": "cdcsv", "name": "Code Descriptions.csv", "format": "CSV", "url": "https://x/code-descriptions.csv"},
]


def test_pick_delay_files_one_resource_per_dataset():
    ids = [r["id"] for r in ingest.pick_delay_files(CKAN_RESOURCES)]
    assert ids == ["y2024", "csv25"]  # no codes, readme, datastore dump, xml/json duplicates


def test_pick_delay_files_prefers_csv_over_xlsx_of_same_dataset():
    res = [{"id": "a", "name": "delays-2030", "format": "XLSX", "url": "https://x/a.xlsx"},
           {"id": "b", "name": "delays-2030.csv", "format": "CSV", "url": "https://x/a.csv"}]
    assert [r["id"] for r in ingest.pick_delay_files(res)] == ["b"]


def test_pick_code_file_prefers_current_code_descriptions_csv():
    assert ingest.pick_code_file(CKAN_RESOURCES)["id"] == "cdcsv"


def test_read_codes_handles_live_and_legacy_headers():
    live = ingest.read_codes(b"_id,CODE,DESCRIPTION\n1,EUAC,AIR CONDITIONING\n", "csv")
    legacy = ingest.read_codes(b"SUB RMENU CODE,CODE DESCRIPTION\nMUIS,Injured\n", "csv")
    assert live.to_dict("records") == [{"code": "EUAC", "description": "AIR CONDITIONING"}]
    assert legacy.to_dict("records") == [{"code": "MUIS", "description": "Injured"}]


def test_read_resource_drops_datastore_id_column():
    csv = b"_id,Date,Time,Day,Station,Code,Min Delay,Min Gap,Bound,Line,Vehicle\n1,2025-01-01,02:10,Wednesday,BATHURST STATION,MUSAN,5,9,E,BD,5227\n"
    df = ingest.read_resource(csv, "csv")
    assert list(df.columns) == list(config.COLUMN_ALIASES) and df.loc[0, "min_delay"] == "5"


def _build_from_rows(tmp_path, rows):
    bronze = tmp_path / "bronze"
    bronze.mkdir()
    base = {"date": "2024-01-01 00:00:00", "time": "10:00", "day": "Monday", "station": "DUNDAS STATION",
            "code": "MUIS", "min_delay": "3", "min_gap": "6", "bound": "N", "line": "YU", "vehicle": "5000",
            "_source": "t.csv", "_ingested_at": "2026-11-07T00:00:00+00:00"}
    df = pd.DataFrame([{**base, "vehicle": str(5000 + i), **r} for i, r in enumerate(rows)])
    df.to_parquet(bronze / "delays_t.parquet", index=False)
    return warehouse.build(bronze, tmp_path / "t.duckdb")


def test_line_variants_collapse_to_canonical_values(tmp_path):
    variants = {"YU": "YU", "YUS": "YU", "BD": "BD", "BD LINE": "BD", "BLOOR DANF": "BD", "SHP": "SHP",
                "SRT": "SRT", "YU/BD": "YU/BD", "YU / BD": "YU/BD", "YU/ BD": "YU/BD", "YUS/BD": "YU/BD",
                "BD/YU": "YU/BD", "YU & BD": "YU/BD", "YU-BD": "YU/BD", "B/D": "BD",
                "999": "OTHER", "60": "OTHER", "510 SPADIN": "OTHER"}
    con = _build_from_rows(tmp_path, [{"line": k} for k in variants])
    got = dict(con.execute("SELECT vehicle, line FROM silver_delays").fetchall())
    assert [got[5000 + i] for i in range(len(variants))] == list(variants.values())


def test_blank_line_stays_null(tmp_path):
    con = _build_from_rows(tmp_path, [{"line": None}, {"line": "  "}])
    assert con.execute("SELECT count(*) FROM silver_delays WHERE line IS NULL").fetchone()[0] == 2


def test_truncated_station_names_are_normalised(tmp_path):
    names = {"KIPLING STATION (ENTER": "KIPLING", "UNION STATION (DOWNSVI": "UNION", "VICTORIA PARK STATION": "VICTORIA PARK",
             "  st  george   station ": "ST GEORGE", "KENNEDY SRT STATION": "KENNEDY", "FINCH": "FINCH"}
    con = _build_from_rows(tmp_path, [{"station": k} for k in names])
    got = dict(con.execute("SELECT vehicle, station FROM silver_delays").fetchall())
    assert [got[5000 + i] for i in range(len(names))] == list(names.values())


def test_invalid_bound_becomes_null_and_valid_kept(tmp_path):
    con = _build_from_rows(tmp_path, [{"bound": "N"}, {"bound": "5"}, {"bound": "Y"}, {"bound": None}, {"bound": "b"}])
    got = dict(con.execute("SELECT vehicle, bound FROM silver_delays").fetchall())
    assert [got[5000 + i] for i in range(5)] == ["N", None, None, None, "B"]


def test_datetime_style_dates_parse(tmp_path):
    con = _build_from_rows(tmp_path, [{"date": "2014-01-01 00:00:00"}, {"date": "2025-01-01"}])
    assert con.execute("SELECT count(*) FROM silver_delays").fetchone()[0] == 2
    assert con.execute("SELECT count(*) FROM silver_rejects").fetchone()[0] == 0


def test_line_qualifier_and_non_station_rows(tmp_path):
    names = [("KENNEDY BD STATION", "KENNEDY", True), ("ST GEORGE YUS STATION", "ST GEORGE", True),
             ("KENNEDY SRT STATION", "KENNEDY", True), ("SHEPPARD WEST", "SHEPPARD WEST", True),
             ("ISLINGTON TO KIPLING", "ISLINGTON TO KIPLING", False), ("YONGE UNIVERSITY LINE", "YONGE UNIVERSITY LINE", False),
             ("YONGE-UNIVERSITY AND B", "YONGE-UNIVERSITY AND B", False), ("YUS/BD/SHEPPARD SUBWAY", "YUS/BD/SHEPPARD SUBWAY", False)]
    con = _build_from_rows(tmp_path, [{"station": n} for n, _, _ in names])
    got = {v: (s, f) for v, s, f in con.execute("SELECT vehicle, station, is_station FROM silver_delays").fetchall()}
    assert [got[5000 + i] for i in range(len(names))] == [(s, f) for _, s, f in names]
    ranked = {r[0] for r in con.execute("SELECT station FROM gold_station_ranking").fetchall()}
    assert ranked == {"KENNEDY", "ST GEORGE", "SHEPPARD WEST"}


def test_code_descriptions_mojibake_is_repaired():

    broken = ("CODE,DESCRIPTION\nMUI,INJURED CUSTOMER \u00e2\u20ac\u201c TRANSPORTED\n").encode("utf-8")
    assert ingest.read_codes(broken, "csv").loc[0, "description"] == "INJURED CUSTOMER \u2013 TRANSPORTED"
    latin1 = "CODE,DESCRIPTION\nMUI,INJURED CUSTOMER \u2013 TRANSPORTED\n".encode("utf-8").decode("latin-1").encode("utf-8")
    assert ingest.read_codes(latin1, "csv").loc[0, "description"] == "INJURED CUSTOMER \u2013 TRANSPORTED"
    assert ingest.read_codes(b"CODE,DESCRIPTION\nA,PLAIN TEXT\n", "csv").loc[0, "description"] == "PLAIN TEXT"


def test_legacy_code_workbook_fills_missing_codes(tmp_path):
    import io
    from openpyxl import Workbook
    wb = Workbook(); ws = wb.active
    ws.append([None, None, "SUB RMENU CODE", "CODE DESCRIPTION", None, None, "SRT RMENU CODE", "CODE DESCRIPTION"])
    ws.append([None, 1, "EUAC", "Air Conditioning", None, 1, "ERAC", "Air Conditioning"])
    ws.append([None, 2, "SUL", "Unlawful act", None, 2, "SRUL", "Unlawful act SRT"])
    buf = io.BytesIO(); wb.save(buf)
    legacy = ingest.read_legacy_codes(buf.getvalue())
    assert set(legacy["code"]) == {"EUAC", "SUL", "ERAC", "SRUL"}
    current = pd.DataFrame({"code": ["EUAC"], "description": ["AIR CONDITIONING"]})
    merged = ingest.merge_codes(current, legacy)
    d = dict(zip(merged["code"], merged["description"]))
    assert d["EUAC"] == "AIR CONDITIONING" and d["SUL"] == "Unlawful act"  # current wins, legacy fills gaps
