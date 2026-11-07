"""Command line: python -m ttc_pipeline.cli [ingest|build|check|run] [--sample]"""
from __future__ import annotations

import argparse
import logging
import sys

import pandas as pd

from . import config, ingest, quality, warehouse


def load_codes(from_sample: bool) -> None:
    config.BRONZE.mkdir(parents=True, exist_ok=True)
    if from_sample:
        raw = pd.read_csv(config.SAMPLE / "codes_sample.csv", dtype=str)
    else:
        import requests
        res = [r for r in ingest.list_resources() if "code" in (r.get("name") or "").lower()
               and (r.get("format") or "").lower() in {"csv", "xlsx"}]
        if not res:
            return
        r = requests.get(res[0]["url"], headers=ingest.HEADERS, timeout=120)
        r.raise_for_status()
        raw = pd.read_excel(pd.io.common.BytesIO(r.content), dtype=str) if res[0]["format"].lower() == "xlsx" \
            else pd.read_csv(pd.io.common.BytesIO(r.content), dtype=str)
    cols = {c: c.lower() for c in raw.columns}
    code_col = next((c for c, low in cols.items() if "code" in low and "desc" not in low), raw.columns[0])
    desc_col = next((c for c, low in cols.items() if "desc" in low), raw.columns[1])
    out = raw[[code_col, desc_col]].rename(columns={code_col: "code", desc_col: "description"}).dropna(subset=["code"])
    out.to_parquet(config.BRONZE / "delay_codes.parquet", index=False)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="ttc_pipeline")
    p.add_argument("step", choices=["ingest", "build", "check", "run"])
    p.add_argument("--sample", action="store_true", help="use bundled synthetic data (offline)")
    p.add_argument("--limit-files", type=int, default=None, help="only download the first N yearly files")
    a = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if a.step in ("ingest", "run"):
        if a.sample:
            from .sample import write_sample
            write_sample(config.SAMPLE)
            ingest.ingest_from_sample()
        else:
            ingest.ingest_from_api(a.limit_files)
        load_codes(a.sample)
    if a.step in ("build", "run", "check"):
        con = warehouse.build()
        if a.step in ("check", "run"):
            results = quality.run_checks(con)
            for name, ok, detail in results:
                print(f"{'PASS' if ok else 'FAIL'}  {name}  ({detail})")
            if not all(ok for _, ok, _ in results):
                return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
