"""Lightweight PySpark Data Quality framework — declarative, fail-fast.

Public API:
    from dq import run_gate, DQFailure, load_rules
"""

from .rules import REGISTRY
from .runner import DEFAULT_CONFIG_PATH, DQFailure, load_rules, run_gate

__all__ = ["run_gate", "load_rules", "DQFailure", "REGISTRY", "DEFAULT_CONFIG_PATH"]
