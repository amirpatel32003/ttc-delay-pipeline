"""Deterministic synthetic data with the same shape as the real TTC files (used for tests and CI)."""
from __future__ import annotations

import random
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

STATIONS = ["UNION", "BLOOR", "KENNEDY", "FINCH", "KIPLING", "SHEPPARD-YONGE", "DUNDAS", "ST GEORGE"]
LINES = ["YU", "BD", "SHP"]
CODES = ["MUIS", "TUSC", "PUOPO", "MUESA", "SUDP"]


def make_sample(n: int = 600, seed: int = 7, start: date = date(2024, 1, 1)) -> pd.DataFrame:
    rng = random.Random(seed)
    rows = []
    for _ in range(n):
        d = start + timedelta(days=rng.randrange(0, 120))
        rows.append({
            "Date": d.isoformat(),
            "Time": f"{rng.randrange(5, 24):02d}:{rng.randrange(0, 60):02d}",
            "Day": d.strftime("%A"),
            "Station": rng.choice(STATIONS) + " STATION",
            "Code": rng.choice(CODES),
            "Min Delay": rng.choice([0, 0, 2, 3, 4, 5, 8, 12, 25]),
            "Min Gap": rng.choice([0, 4, 6, 9, 14]),
            "Bound": rng.choice(["N", "S", "E", "W"]),
            "Line": rng.choice(LINES),
            "Vehicle": rng.randrange(5000, 6000),
        })
    df = pd.DataFrame(rows)
    # inject known problems so the cleaning + rejects logic is exercised
    df.loc[0, "Date"] = "not-a-date"
    df.loc[1, "Min Delay"] = -5
    df.loc[2, "Station"] = ""
    df = pd.concat([df, df.iloc[[10]]], ignore_index=True)  # exact duplicate row
    return df


def write_sample(dir_: Path) -> Path:
    dir_.mkdir(parents=True, exist_ok=True)
    path = dir_ / "delays_sample.csv"
    make_sample().to_csv(path, index=False)
    codes = pd.DataFrame({
        "SUB RMENU CODE": CODES,
        "CODE DESCRIPTION": ["Injured or ill customer", "Operator overspeeding", "Passenger other than offender",
                             "Escalator/elevator incident", "Disorderly patron"],
    })
    codes.to_csv(dir_ / "codes_sample.csv", index=False)
    return path


if __name__ == "__main__":
    from . import config
    print(write_sample(config.SAMPLE))
