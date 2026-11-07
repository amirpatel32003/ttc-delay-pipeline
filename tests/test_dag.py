"""Structure tests for the Airflow DAG.

The AST tests always run (Airflow is not installable on Windows and is heavy). The import test runs only
where Airflow is installed (the dedicated CI job) and fails on any import error.
"""
import ast
import importlib.util
from pathlib import Path

import pytest

DAG_FILE = Path(__file__).resolve().parents[1] / "dags" / "ttc_daily_dag.py"


@pytest.fixture(scope="module")
def tree():
    return ast.parse(DAG_FILE.read_text(encoding="utf-8"))


def _const(node):
    return node.value if isinstance(node, ast.Constant) else None


def _bash_tasks(tree):
    tasks = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Call) \
                and getattr(node.value.func, "id", "") == "BashOperator":
            kw = {k.arg: k.value for k in node.value.keywords}
            tasks[node.targets[0].id] = (_const(kw["task_id"]), ast.unparse(kw["bash_command"]))
    return tasks


def _chain(tree):
    for node in ast.walk(tree):
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.BinOp) and isinstance(node.value.op, ast.RShift):
            names, cur = [], node.value
            while isinstance(cur, ast.BinOp):
                names.append(cur.right.id)
                cur = cur.left
            names.append(cur.id)
            return names[::-1]
    return []


def test_dag_file_parses_and_has_three_bash_tasks(tree):
    tasks = _bash_tasks(tree)
    assert {t for t, _ in tasks.values()} == {"ingest_bronze", "build_silver_gold", "data_quality_checks"}


def test_tasks_run_in_pipeline_order(tree):
    assert _chain(tree) == ["ingest", "build", "check"]


def test_each_task_calls_a_real_cli_step(tree):
    from ttc_pipeline import cli
    steps = []
    for _, cmd in _bash_tasks(tree).values():
        assert cmd.startswith("f'{CLI} ")
        steps.append(cmd.split("{CLI} ")[1].strip("'\" "))
    assert steps == ["ingest", "build", "check"]
    with pytest.raises(SystemExit):  # the CLI really rejects unknown steps, so the names above are meaningful
        cli.main(["no-such-step"])


def test_dag_settings_are_safe_for_a_daily_job(tree):
    src = ast.unparse(tree)
    assert "dag_id='ttc_delay_pipeline'" in src
    assert "schedule='@daily'" in src and "catchup=False" in src and "max_active_runs=1" in src
    assert "'retries': 2" in src


def test_dag_imports_cleanly_and_has_expected_structure():
    pytest.importorskip("airflow", reason="Apache Airflow is not installed (runs in the CI 'dag' job)")
    from airflow.models import DagBag
    bag = DagBag(dag_folder=str(DAG_FILE.parent), include_examples=False)
    assert bag.import_errors == {}, bag.import_errors
    dag = bag.get_dag("ttc_delay_pipeline")
    assert dag is not None and not dag.catchup
    assert [t.task_id for t in dag.topological_sort()] == ["ingest_bronze", "build_silver_gold", "data_quality_checks"]
    assert dag.get_task("build_silver_gold").upstream_task_ids == {"ingest_bronze"}
    assert dag.get_task("data_quality_checks").upstream_task_ids == {"build_silver_gold"}
