"""Tests cho DQ framework — chạy với local Spark session (cần pyspark + PyYAML)."""

import pytest
from dq import REGISTRY, DQFailure, load_rules, run_gate

# ---------- pure logic ----------


def test_registry_has_all_rules():
    expected = {
        "no_nulls",
        "unique",
        "range",
        "row_count_at_least",
        "allowed_values",
        "referential_integrity",
        "custom_sql",
    }
    assert expected.issubset(REGISTRY.keys())


def test_load_rules_silver():
    rules = load_rules("silver")
    types = {r["rule"] for r in rules}
    assert {"no_nulls", "unique", "range", "allowed_values", "row_count_at_least"} <= types


def test_load_rules_unknown_raises():
    with pytest.raises(KeyError):
        load_rules("does_not_exist")


def test_dq_failure_message_lists_failed():
    results = [
        {"rule": "no_nulls", "passed": True, "failing_count": 0, "detail": "ok"},
        {"rule": "range", "passed": False, "failing_count": 3, "detail": "3 outside"},
    ]
    err = DQFailure(results)
    assert "1 rule(s) violated" in str(err)
    assert "range" in str(err)


# ---------- rule-level (need spark) ----------


def test_no_nulls_pass(spark):
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
    res = REGISTRY["no_nulls"](spark, df, {"columns": ["id", "name"]})
    assert res["passed"] and res["failing_count"] == 0


def test_no_nulls_fail(spark):
    df = spark.createDataFrame([(1, None), (2, "b")], ["id", "name"])
    res = REGISTRY["no_nulls"](spark, df, {"columns": ["name"]})
    assert not res["passed"] and res["failing_count"] == 1


def test_unique_fail(spark):
    df = spark.createDataFrame([(1,), (1,), (2,)], ["k"])
    res = REGISTRY["unique"](spark, df, {"columns": ["k"]})
    assert not res["passed"] and res["failing_count"] == 1


def test_range_fail(spark):
    df = spark.createDataFrame([(5.0,), (-1.0,), (3.0,)], ["price"])
    res = REGISTRY["range"](spark, df, {"column": "price", "min": 0})
    assert not res["passed"] and res["failing_count"] == 1


def test_allowed_values_fail(spark):
    df = spark.createDataFrame([("view",), ("click",)], ["event_type"])
    res = REGISTRY["allowed_values"](
        spark, df, {"column": "event_type", "values": ["view", "cart", "purchase"]}
    )
    assert not res["passed"] and res["failing_count"] == 1


def test_referential_integrity_fail(spark):
    spark.createDataFrame([(1,), (2,), (3,)], ["product_key"]).createOrReplaceTempView(
        "dim_product"
    )
    fact = spark.createDataFrame([(1,), (99,)], ["product_key"])
    res = REGISTRY["referential_integrity"](
        spark,
        fact,
        {
            "child_columns": ["product_key"],
            "parent_table": "dim_product",
            "parent_columns": ["product_key"],
        },
    )
    assert not res["passed"] and res["failing_count"] == 1


def test_custom_sql_pass(spark):
    spark.sql("CREATE OR REPLACE TEMP VIEW v AS SELECT 0 AS cnt")
    res = REGISTRY["custom_sql"](spark, None, {"sql": "SELECT cnt FROM v"})
    assert res["passed"]


# ---------- run_gate end-to-end ----------


def test_run_gate_inline_pass(spark):
    df = spark.createDataFrame([(1, 10.0), (2, 20.0)], ["id", "price"])
    report = run_gate(
        spark,
        df,
        rules=[
            {"rule": "no_nulls", "columns": ["id"]},
            {"rule": "range", "column": "price", "min": 0},
        ],
    )
    assert report["passed"] == 2 and report["failed"] == 0


def test_run_gate_raises(spark):
    df = spark.createDataFrame([(1, -5.0)], ["id", "price"])
    with pytest.raises(DQFailure):
        run_gate(spark, df, rules=[{"rule": "range", "column": "price", "min": 0}])


def test_run_gate_fail_fast_short_circuits(spark):
    # 2 rule fail; fail_fast=True chỉ chạy đến rule đầu tiên fail.
    df = spark.createDataFrame([(1, -5.0)], ["id", "price"])
    with pytest.raises(DQFailure) as exc:
        run_gate(
            spark,
            df,
            rules=[
                {"rule": "range", "column": "price", "min": 0, "name": "r1"},
                {"rule": "no_nulls", "columns": ["missing_col"], "name": "r2"},
            ],
            fail_fast=True,
        )
    assert len(exc.value.results) == 1  # chỉ r1 được chạy


def test_run_gate_collects_all_when_not_fail_fast(spark):
    df = spark.createDataFrame([(1, -5.0)], ["id", "price"])
    with pytest.raises(DQFailure) as exc:
        run_gate(
            spark,
            df,
            rules=[
                {"rule": "range", "column": "price", "min": 0, "name": "r1"},
                {"rule": "row_count_at_least", "min": 999, "name": "r2"},
            ],
            fail_fast=False,
        )
    assert len(exc.value.results) == 2


def test_run_gate_from_yaml_silver(spark):
    # Silver rule-set áp lên 1 df sạch -> pass.
    df = spark.createDataFrame(
        [("2024-01-01 00:00:00", "view", "p1", "u1", "h1", 10.0)],
        ["event_time", "event_type", "product_id", "user_id", "_row_hash", "price"],
    )
    report = run_gate(spark, df, rule_set="silver")
    assert report["failed"] == 0
