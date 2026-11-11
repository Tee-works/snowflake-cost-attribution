import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[1]))
import generate  # noqa: E402
import run  # noqa: E402


@pytest.fixture(scope="module")
def con(tmp_path_factory):
    db = str(tmp_path_factory.mktemp("d") / "t.duckdb")
    generate.generate(db, seed=5)
    return run.build(db)


def test_all_checks_pass(con):
    assert all(v == 0 for v in run.check(con).values()), run.check(con)


def test_concurrent_queries_not_double_counted_as_busy(con):
    # sum of query seconds can exceed wall-clock busy time when queries overlap; busy must use the union
    over = con.execute("select count(*) from warehouse_hour_usage where query_secs > busy_secs + 1").fetchone()[0]
    assert over > 0


def test_planted_problems_are_flagged(con):
    alerts = {(a, s) for a, s in con.execute("select alert, subject from cost_alerts").fetchall()}
    assert ("warehouse_idle_over_25pct", "ADHOC_WH") in alerts
    assert ("team_week_over_week_spike", "marketing") in alerts
    assert any(a == "top_workload_over_10pct" and "exec_dashboard_7" in s for a, s in alerts)


def test_rebuild_is_deterministic(con):
    before = con.execute("select round(sum(credits), 9) from query_cost").fetchone()[0]
    for sql in sorted((run.HERE / "models").glob("*.sql")):
        con.execute(sql.read_text().replace("$credit_price", str(run.CREDIT_PRICE_USD)))
    assert con.execute("select round(sum(credits), 9) from query_cost").fetchone()[0] == before
