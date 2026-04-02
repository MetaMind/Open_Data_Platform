"""Transformation rule registry."""
from __future__ import annotations
import logging
logger = logging.getLogger(__name__)
class Rule:
    def __init__(self, name: str) -> None:
        self.name = name
    def applies(self, node: object) -> bool:
        return False
    def apply(self, node: object) -> object:
        return node

class RuleRegistry:
    def __init__(self) -> None:
        self._rules: list[Rule] = []
    def register(self, rule: Rule) -> None:
        self._rules.append(rule)
    def applicable(self, node: object) -> list[Rule]:
        return [r for r in self._rules if r.applies(node)]
