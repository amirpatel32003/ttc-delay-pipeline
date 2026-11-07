from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data"
RAW = DATA / "raw"
BRONZE = DATA / "bronze"
WAREHOUSE = DATA / "warehouse" / "ttc.duckdb"
SQL = ROOT / "sql"
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
