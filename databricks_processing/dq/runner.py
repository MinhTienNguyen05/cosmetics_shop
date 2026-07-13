"""DQ runner: chạy 1 rule-set khai báo (YAML) trên DataFrame/bảng, fail-fast.

Cách dùng (notebook):
    from dq import run_gate, DQFailure
    run_gate(spark, "workspace.silver_cosmetics.cosmetics_events_silver", rule_set="silver")
    # -> raise DQFailure nếu vi phạm (fail-fast); trả về report dict nếu pass.

Có thể truyền rules inline (không cần file YAML) — hữu ích cho test và CE:
    run_gate(spark, df, rules=[{"rule":"no_nulls","columns":["user_id"]}])
"""

import os

from .rules import REGISTRY

try:
    import yaml  # PyYAML có sẵn trong Databricks runtime; optional cho inline usage.
except ImportError:  # pragma: no cover
    yaml = None

DEFAULT_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "validations.yml")


class DQFailure(Exception):
    """Raise khi ≥1 rule fail. Mang theo kết quả chi tiết để debug."""

    def __init__(self, results):
        self.results = results
        failed = [r for r in results if not r["passed"]]
        lines = [f"  - [{r['rule']}] {r['detail']} (failing={r['failing_count']})" for r in failed]
        msg = f"DQ gate FAILED: {len(failed)} rule(s) violated.\n" + "\n".join(lines)
        super().__init__(msg)


def load_rules(rule_set, config_path=None):
    """Load rules theo key từ file YAML."""
    if yaml is None:
        raise RuntimeError("PyYAML chưa cài; truyền rules inline hoặc %pip install PyYAML.")
    path = config_path or DEFAULT_CONFIG_PATH
    with open(path) as f:
        data = yaml.safe_load(f) or {}
    if rule_set not in data:
        raise KeyError(f"rule_set '{rule_set}' không có trong {path}")
    return data[rule_set]


def _resolve_df(spark, target):
    if isinstance(target, str):
        return spark.table(target)
    return target


def run_gate(spark, target, rule_set=None, rules=None, config_path=None, fail_fast=True):
    """Chạy DQ gate.

    target: DataFrame hoặc tên bảng (str).
    rule_set: key trong validations.yml (bỏ qua nếu truyền `rules`).
    rules: list dict inline [{"rule": <type>, ...params, "name"?}].
    fail_fast: True -> raise ngay rule đầu tiên fail; False -> chạy hết rồi raise.
    """
    if rules is None:
        if rule_set is None:
            raise ValueError("cần `rule_set` (load YAML) hoặc `rules` (inline).")
        rules = load_rules(rule_set, config_path)

    df = _resolve_df(spark, target)
    results = []
    for rule in rules:
        rtype = rule["rule"]
        if rtype not in REGISTRY:
            raise KeyError(f"rule type '{rtype}' không hỗ trợ. Có: {list(REGISTRY)}")
        params = {k: v for k, v in rule.items() if k not in ("rule", "name")}
        name = rule.get("name", rtype)
        res = REGISTRY[rtype](spark, df, params)
        res["rule"] = name
        results.append(res)
        if fail_fast and not res["passed"]:
            raise DQFailure(results)

    failed = [r for r in results if not r["passed"]]
    if failed:
        raise DQFailure(results)
    return {
        "rule_set": rule_set,
        "total": len(results),
        "passed": len(results) - len(failed),
        "failed": len(failed),
        "results": results,
    }
