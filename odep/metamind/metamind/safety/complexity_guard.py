"""
Complexity Guard - Query Complexity Limits

File: metamind/safety/complexity_guard.py
Role: Security Engineer
Phase: 1
Dependencies: sqlglot

Enforces query complexity limits to prevent resource exhaustion.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, Any, Optional

import sqlglot
from sqlglot import Expression

logger = logging.getLogger(__name__)


@dataclass
class ComplexityLimits:
    """Query complexity limits."""
    max_tables: int = 10
    max_joins: int = 5
    max_subqueries: int = 3
    max_aggregations: int = 20
    max_query_length: int = 10000
    max_nesting_depth: int = 5


class ComplexityGuard:
    """
    Enforces query complexity limits.
    
    Prevents resource exhaustion by rejecting overly complex queries.
    """
    
    def __init__(self, limits: Optional[ComplexityLimits] = None):
        """
        Initialize complexity guard.
        
        Args:
            limits: Complexity limits (uses defaults if not provided)
        """
        self.limits = limits or ComplexityLimits()
        logger.debug("ComplexityGuard initialized")
    
    def check(self, sql: str) -> Dict[str, Any]:
        """
        Check if query is within complexity limits.
        
        Args:
            sql: SQL query to check
            
        Returns:
            Dict with 'allowed' boolean and 'reason' if rejected
        """
        try:
            parsed = sqlglot.parse_one(sql)
        except Exception as e:
            return {
                "allowed": False,
                "reason": f"Failed to parse SQL: {e}"
            }
        
        # Check query length
        if len(sql) > self.limits.max_query_length:
            return {
                "allowed": False,
                "reason": (
                    f"Query too long ({len(sql)} chars, "
                    f"max {self.limits.max_query_length})"
                )
            }
        
        # Count tables
        tables = list(parsed.find_all(sqlglot.exp.Table))
        if len(tables) > self.limits.max_tables:
            return {
                "allowed": False,
                "reason": (
                    f"Too many tables ({len(tables)}, "
                    f"max {self.limits.max_tables})"
                )
            }
        
        # Count joins
        joins = list(parsed.find_all(sqlglot.exp.Join))
        if len(joins) > self.limits.max_joins:
            return {
                "allowed": False,
                "reason": (
                    f"Too many joins ({len(joins)}, "
                    f"max {self.limits.max_joins})"
                )
            }
        
        # Count subqueries
        subqueries = list(parsed.find_all(sqlglot.exp.Subquery))
        if len(subqueries) > self.limits.max_subqueries:
            return {
                "allowed": False,
                "reason": (
                    f"Too many subqueries ({len(subqueries)}, "
                    f"max {self.limits.max_subqueries})"
                )
            }
        
        # Count aggregations
        aggregations = list(parsed.find_all(sqlglot.exp.AggFunc))
        if len(aggregations) > self.limits.max_aggregations:
            return {
                "allowed": False,
                "reason": (
                    f"Too many aggregations ({len(aggregations)}, "
                    f"max {self.limits.max_aggregations})"
                )
            }
        
        # Check nesting depth
        depth = self._calculate_nesting_depth(parsed)
        if depth > self.limits.max_nesting_depth:
            return {
                "allowed": False,
                "reason": (
                    f"Query nesting too deep ({depth}, "
                    f"max {self.limits.max_nesting_depth})"
                )
            }
        
        return {"allowed": True}
    
    def _calculate_nesting_depth(self, parsed: Expression) -> int:
        """Calculate maximum nesting depth of query."""
        max_depth = 0
        
        def traverse(node: Expression, depth: int) -> None:
            nonlocal max_depth
            max_depth = max(max_depth, depth)
            for child in node.iter_expressions():
                traverse(child, depth + 1)
        
        traverse(parsed, 1)
        return max_depth
    
    def get_complexity_score(self, sql: str) -> Dict[str, Any]:
        """
        Get complexity metrics for a query.
        
        Args:
            sql: SQL query
            
        Returns:
            Dict with complexity metrics
        """
        try:
            parsed = sqlglot.parse_one(sql)
        except Exception:
            return {"error": "Failed to parse SQL"}
        
        return {
            "query_length": len(sql),
            "num_tables": len(list(parsed.find_all(sqlglot.exp.Table))),
            "num_joins": len(list(parsed.find_all(sqlglot.exp.Join))),
            "num_subqueries": len(list(parsed.find_all(sqlglot.exp.Subquery))),
            "num_aggregations": len(list(parsed.find_all(sqlglot.exp.AggFunc))),
            "nesting_depth": self._calculate_nesting_depth(parsed),
            "complexity_score": self._calculate_score(parsed)
        }
    
    def _calculate_score(self, parsed: Expression) -> int:
        """Calculate overall complexity score."""
        score = 0
        
        # Base score from components
        score += len(list(parsed.find_all(sqlglot.exp.Table))) * 2
        score += len(list(parsed.find_all(sqlglot.exp.Join))) * 5
        score += len(list(parsed.find_all(sqlglot.exp.Subquery))) * 10
        score += len(list(parsed.find_all(sqlglot.exp.AggFunc))) * 3
        
        # Nesting penalty
        depth = self._calculate_nesting_depth(parsed)
        score += (depth - 1) * 5
        
        return score
