"""Command line: python -m ttc_pipeline.cli [ingest|build|check|run|docs|export] [--sample] [--limit-files N] [--force]"""
from __future__ import annotations

import argparse
import logging
import sys

import pandas as pd

from . import config, ingest, quality, warehouse


def load_codes(from_sample: bool) -> None:
    """Write the delay-code lookup into bronze (sample file offline, 'Code Descriptions' CSV online)."""
    if from_sample:
        config.BRONZE.mkdir(parents=True, exist_ok=True)
        raw = ingest.read_codes((config.SAMPLE / "codes_sample.csv").read_bytes(), "csv")
        raw.to_parquet(config.BRONZE / "delay_codes.parquet", index=False)
    else:
        ingest.ingest_codes()


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="ttc_pipeline")
    p.add_argument("step", choices=["ingest", "build", "check", "run", "docs", "export"])
    p.add_argument("--out", default=None, help="export: output path (default site/dashboard_data.json)")
    p.add_argument("--sample", action="store_true", help="use bundled synthetic data (offline, separate warehouse)")
    p.add_argument("--limit-files", type=int, default=None, help="only download the first N yearly files")
    p.add_argument("--force", action="store_true", help="ignore the manifest and re-download everything")
    a = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if a.sample:  # keep synthetic data completely apart from real bronze/warehouse
        config.BRONZE, config.WAREHOUSE = config.BRONZE_SAMPLE, config.WAREHOUSE_SAMPLE

    if a.step in ("ingest", "run"):
        if a.sample:
            from .sample import write_sample
            write_sample(config.SAMPLE)
            ingest.ingest_from_sample()
        else:
            ingest.ingest_from_api(a.limit_files, force=a.force)
        load_codes(a.sample)
    if a.step in ("build", "run", "check", "docs", "export"):
        con = warehouse.build()
        if a.step == "docs":
            from . import docs
            path = docs.write_schema_doc(con)
            print(f"wrote {path}")
        if a.step == "export":
            from pathlib import Path
            from . import export
            path = export.write_payload(con, Path(a.out or config.ROOT / "site" / "dashboard_data.json"))
            print(f"wrote {path}")
        if a.step in ("check", "run"):
            results = quality.run_checks(con)
            for name, ok, detail in results:
                print(f"{'PASS' if ok else 'FAIL'}  {name}  ({detail})")
            if not all(ok for _, ok, _ in results):
                return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
