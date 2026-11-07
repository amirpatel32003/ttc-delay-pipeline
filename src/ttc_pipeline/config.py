from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data"
RAW = DATA / "raw"
BRONZE = DATA / "bronze"
BRONZE_SAMPLE = DATA / "bronze_sample"   # --sample runs never touch real bronze
WAREHOUSE = DATA / "warehouse" / "ttc.duckdb"
WAREHOUSE_SAMPLE = DATA / "warehouse" / "ttc_sample.duckdb"
MANIFEST_NAME = "_manifest.json"   # incremental-load ledger, lives next to the bronze parquet files
SQL = ROOT / "sql"
DOCS = ROOT / "docs"
SAMPLE = DATA / "sample"

CKAN = "https://ckan0.cf.opendata.inter.prod-toronto.ca/api/3/action"
DELAY_PACKAGE = "ttc-subway-delay-data"

# canonical column name -> accepted raw spellings (lower-cased, stripped)
COLUMN_ALIASES = {
    "date": ["date"],
    "time": ["time"],
    "day": ["day"],
    "station": ["station"],
    "code": ["code"],
    "min_delay": ["min delay", "min_delay"],
    "min_gap": ["min gap", "min_gap"],
    "bound": ["bound"],
    "line": ["line"],
    "vehicle": ["vehicle"],
}
