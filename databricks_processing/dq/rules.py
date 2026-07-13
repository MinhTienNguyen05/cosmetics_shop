"""Data Quality rule implementations (lightweight, PySpark-only).

Mỗi rule là 1 hàm: (spark, df, params) -> dict với:
  { "passed": bool, "failing_count": int, "detail": str }

Không dependency ngoài PySpark — chạy được trên Databricks Community Edition.
File này là bản chính; dq_framework.ipynb giữ bản mirror cho chạy %run trên CE.
"""

from pyspark.sql.functions import col, count, lit, when


def _check_no_nulls(spark, df, params):
    cols = params["columns"]
    nulls_row = (
        df.select([count(when(col(c).isNull() | (col(c) == ""), c)).alias(c) for c in cols])
        .collect()[0]
        .asDict()
    )
    bad = {c: int(v) for c, v in nulls_row.items() if v}
    return {
        "passed": not bad,
        "failing_count": sum(bad.values()),
        "detail": f"null/empty counts: {bad}" if bad else "no nulls",
    }


def _check_unique(spark, df, params):
    cols = params["columns"]
    total = df.count()
    distinct = df.select(*cols).distinct().count()
    dups = total - distinct
    return {
        "passed": dups == 0,
        "failing_count": dups,
        "detail": f"{dups} duplicate row(s) on {cols} (total={total})",
    }


def _check_range(spark, df, params):
    c = params["column"]
    mn = params.get("min")
    mx = params.get("max")
    cond = lit(False)
    if mn is not None:
        cond = cond | (col(c) < mn)
    if mx is not None:
        cond = cond | (col(c) > mx)
    out = df.filter(cond).count()
    return {
        "passed": out == 0,
        "failing_count": out,
        "detail": f"{out} row(s) outside [{mn}, {mx}] on {c}",
    }


def _check_row_count_at_least(spark, df, params):
    n = df.count()
    mn = params["min"]
    return {
        "passed": n >= mn,
        "failing_count": max(0, mn - n),
        "detail": f"{n} rows (min required {mn})",
    }


def _check_allowed_values(spark, df, params):
    c = params["column"]
    allowed = params["values"]
    bad = df.filter(~col(c).isin(*allowed)).count()
    return {
        "passed": bad == 0,
        "failing_count": bad,
        "detail": f"{bad} row(s) with {c} not in {allowed}",
    }


def _check_referential_integrity(spark, df, params):
    """Bảo toàn quan hệ FK: mọi khoá con đều phải tồn tại trong bảng cha."""
    child_cols = params["child_columns"]
    parent_table = params["parent_table"]
    parent_cols = params["parent_columns"]
    assert len(child_cols) == len(parent_cols), "child/parent column count mismatch"

    parent = spark.table(parent_table).select(*parent_cols).distinct()
    child = df.select(*child_cols).distinct()
    # Đổi tên cột parent để join không trùng.
    for cc, pc in zip(child_cols, parent_cols):
        parent = parent.withColumnRenamed(pc, f"_p_{pc}")
    join_cond = None
    for cc, pc in zip(child_cols, parent_cols):
        clause = col(cc) == col(f"_p_{pc}")
        join_cond = clause if join_cond is None else join_cond & clause
    orphans = child.join(parent, join_cond, "left_anti")
    n = orphans.count()
    return {
        "passed": n == 0,
        "failing_count": n,
        "detail": f"{n} orphan child key(s) not in {parent_table}",
    }


def _check_custom_sql(spark, df, params):
    """Query SQL trả về số dòng vi phạm; pass khi = 0."""
    sql = params["sql"]
    n = spark.sql(sql).collect()[0][0]
    return {
        "passed": n == 0,
        "failing_count": int(n),
        "detail": f"{n} violating row(s) per custom SQL",
    }


# Registry: tên rule -> hàm check.
REGISTRY = {
    "no_nulls": _check_no_nulls,
    "unique": _check_unique,
    "range": _check_range,
    "row_count_at_least": _check_row_count_at_least,
    "allowed_values": _check_allowed_values,
    "referential_integrity": _check_referential_integrity,
    "custom_sql": _check_custom_sql,
}
