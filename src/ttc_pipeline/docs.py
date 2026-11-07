"""Generate documentation: schema.md from the live DuckDB catalog, data_contract.md from contract.py."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import duckdb

from . import config, contract


def _esc(text: str) -> str:
    return text.replace("|", "\\|").replace("\n", " ")


def catalog_columns(con: duckdb.DuckDBPyConnection, table: str) -> list[tuple[str, str, bool]]:
    rows = con.execute(
        "SELECT column_name, data_type, is_nullable = 'YES' FROM information_schema.columns "
        "WHERE table_schema = 'main' AND table_name = ? ORDER BY ordinal_position", [table]).fetchall()
    return [(c, t, bool(n)) for c, t, n in rows]


def contract_drift(con: duckdb.DuckDBPyConnection) -> list[str]:
    """Differences between contract.TABLES and the catalog (empty list = in sync)."""
    problems = []
    for name, table in contract.TABLES.items():
        actual = [(c, t) for c, t, _ in catalog_columns(con, name)]
        if not actual:
            problems.append(f"{name}: table missing from catalog")
            continue
        expected = [(c.name, c.type) for c in table.columns]
        if actual != expected:
            problems.append(f"{name}: catalog {actual} != contract {expected}")
    return problems


def render_schema_doc(con: duckdb.DuckDBPyConnection, generated_on: str | None = None) -> str:
    generated_on = generated_on or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    names = [r[0] for r in con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' ORDER BY table_name").fetchall()]
    out = ["# Warehouse schema", "",
           f"Generated from the DuckDB catalog by `python -m ttc_pipeline.cli docs` on {generated_on}. "
           "Do not edit by hand. Row counts are for the warehouse the command was run against.", "",
           "| Table | Layer | Rows | Columns |", "|---|---|---:|---:|"]
    sections = []
    for name in names:
        cols = catalog_columns(con, name)
        rows = con.execute(f'SELECT count(*) FROM "{name}"').fetchone()[0]
        t = contract.TABLES.get(name)
        layer = t.layer if t else "internal"
        out.append(f"| [`{name}`](#{name}) | {layer} | {rows:,} | {len(cols)} |")
        desc = {c.name: c.description for c in t.columns} if t else {}
        sec = [f"## {name}", ""]
        sec.append(t.description + (f" Grain: {t.grain}." if t.grain else "") if t else "Internal working table (not part of the contract).")
        sec += ["", "| Column | Type | Nullable | Description |", "|---|---|---|---|"]
        for c, ty, nullable in cols:
            sec.append(f"| `{c}` | {ty} | {'yes' if nullable else 'no'} | {_esc(desc.get(c, '(undocumented)'))} |")
        sections.append("\n".join(sec))
    drift = contract_drift(con)
    out += ["", "Contract status: " + ("catalog matches `contract.py`." if not drift else "**DRIFT** - " + "; ".join(drift)), ""]
    return "\n".join(out) + "\n" + "\n\n".join(sections) + "\n"


def render_contract_doc() -> str:
    out = ["# Data contract: TTC subway delays", "",
           f"Version {contract.VERSION}. Rendered from `src/ttc_pipeline/contract.py` by `python -m ttc_pipeline.cli docs`; "
           "a test fails if the contract and the DuckDB catalog disagree.", "",
           "## Ownership", "",
           f"- **Data owner / steward:** {contract.OWNER}",
           f"- **Source owner:** {contract.SOURCE_OWNER}",
           f"- **Consumers:** {contract.CONSUMERS}", "",
           "## Service levels", "", "| Area | Commitment |", "|---|---|"]
    out += [f"| {k} | {_esc(v)} |" for k, v in contract.SLAS.items()]
    out += ["", "## Tables", ""]
    for t in contract.TABLES.values():
        out += [f"### {t.name}", "", f"{t.description} Layer: {t.layer}. Grain: {t.grain}.", ""]
        out += ["| Column | Type | Description | Rule |", "|---|---|---|---|"]
        out += [f"| `{c.name}` | {c.type} | {_esc(c.description)} | {_esc(c.rule) or '-'} |" for c in t.columns]
        if t.rules:
            out += [""] + [f"- {r}" for r in t.rules]
        out.append("")
    out += ["## Automated enforcement", "",
            "The checks in `src/ttc_pipeline/quality.py` run on every `run` and `check`; each returns a violation count and "
            "the command fails if any is non-zero.", "",
            "## Known data issues (observed on the live files)", ""]
    out += [f"- {i}" for i in contract.KNOWN_ISSUES]
    return "\n".join(out) + "\n"


def write_schema_doc(con: duckdb.DuckDBPyConnection, docs_dir: Path | None = None) -> Path:
    d = docs_dir or config.DOCS
    d.mkdir(parents=True, exist_ok=True)
    (d / "data_contract.md").write_text(render_contract_doc(), encoding="utf-8", newline="\n")
    path = d / "schema.md"
    path.write_text(render_schema_doc(con), encoding="utf-8", newline="\n")
    return path
