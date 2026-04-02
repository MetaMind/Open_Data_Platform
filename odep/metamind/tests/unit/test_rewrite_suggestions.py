"""Unit tests for F29 Query Rewrite Suggestions."""
from __future__ import annotations

import sys
import os
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from metamind.core.catalog.metadata import MetadataCatalog
from metamind.core.rewrite.analyzer import (
    AntiPatternDetector,
    RewriteAnalyzer,
    RewriteSuggester,
)
from metamind.core.types import ColumnMeta, TableMeta


class TestDetectSelectStar(unittest.TestCase):
    """Test SELECT * detection."""

    def setUp(self) -> None:
        self.catalog = MetadataCatalog()
        self.catalog.register_table(
            "t1",
            TableMeta(
                table_name="orders",
                schema_name="public",
                tenant_id="t1",
                columns=[
                    ColumnMeta(name="id", dtype="int"),
                    ColumnMeta(name="customer_id", dtype="int"),
                    ColumnMeta(name="product_id", dtype="int"),
                    ColumnMeta(name="quantity", dtype="int"),
                    ColumnMeta(name="total_amount", dtype="decimal"),
                    ColumnMeta(name="status", dtype="varchar"),
                    ColumnMeta(name="created_at", dtype="timestamp"),
                    ColumnMeta(name="updated_at", dtype="timestamp"),
                    ColumnMeta(name="notes", dtype="text"),
                    ColumnMeta(name="shipping_address", dtype="text"),
                ],
                row_count=100_000,
            ),
        )
        self.detector = AntiPatternDetector(catalog=self.catalog)

    def test_detects_select_star(self) -> None:
        sql = "SELECT * FROM orders WHERE status = 'active'"
        result = self.detector.detect_select_star(sql, "t1")
        self.assertIsNotNone(result)
        self.assertEqual(result.rule_name, "select_star")
        self.assertIn("SELECT *", result.description)
        self.assertGreater(result.estimated_improvement_pct, 0)

    def test_no_detection_for_explicit_columns(self) -> None:
        sql = "SELECT id, status FROM orders"
        result = self.detector.detect_select_star(sql, "t1")
        self.assertIsNone(result)


class TestDetectOrToIn(unittest.TestCase):
    """Test OR-to-IN rewrite detection."""

    def setUp(self) -> None:
        self.detector = AntiPatternDetector()

    def test_detects_or_pattern(self) -> None:
        sql = "SELECT id FROM orders WHERE status = 'active' OR status = 'pending'"
        result = self.detector.detect_or_in_filter(sql)
        self.assertIsNotNone(result)
        self.assertEqual(result.rule_name, "or_to_in")
        self.assertIn("IN", result.rewritten_sql)

    def test_no_detection_for_different_columns(self) -> None:
        sql = "SELECT id FROM orders WHERE status = 'active' OR region = 'US'"
        result = self.detector.detect_or_in_filter(sql)
        self.assertIsNone(result)

    def test_in_clause_contains_both_values(self) -> None:
        sql = "SELECT * FROM t WHERE col = 'a' OR col = 'b'"
        result = self.detector.detect_or_in_filter(sql)
        self.assertIsNotNone(result)
        self.assertIn("'a'", result.rewritten_sql)
        self.assertIn("'b'", result.rewritten_sql)


class TestDetectFunctionOnIndexedColumn(unittest.TestCase):
    """Test function-on-indexed-column detection."""

    def setUp(self) -> None:
        self.detector = AntiPatternDetector()

    def test_detects_upper_function(self) -> None:
        sql = "SELECT * FROM users WHERE UPPER(email) = 'TEST@EXAMPLE.COM'"
        result = self.detector.detect_function_on_indexed_column(sql)
        self.assertIsNotNone(result)
        self.assertEqual(result.rule_name, "function_on_indexed_column")
        self.assertNotIn("UPPER(email)", result.rewritten_sql)

    def test_no_detection_without_function(self) -> None:
        sql = "SELECT * FROM users WHERE email = 'test@example.com'"
        result = self.detector.detect_function_on_indexed_column(sql)
        self.assertIsNone(result)


class TestDetectCorrelatedSubquery(unittest.TestCase):
    """Test correlated subquery detection."""

    def setUp(self) -> None:
        self.detector = AntiPatternDetector()

    def test_detects_exists_subquery(self) -> None:
        sql = (
            "SELECT * FROM orders o WHERE EXISTS "
            "(SELECT 1 FROM returns r WHERE r.order_id = o.id)"
        )
        result = self.detector.detect_correlated_subquery(sql)
        self.assertIsNotNone(result)
        self.assertEqual(result.rule_name, "correlated_subquery")
        self.assertGreater(result.estimated_improvement_pct, 0)

    def test_no_detection_for_simple_query(self) -> None:
        sql = "SELECT * FROM orders WHERE id = 1"
        result = self.detector.detect_correlated_subquery(sql)
        self.assertIsNone(result)


class TestDetectMissingLimit(unittest.TestCase):
    """Test missing LIMIT detection."""

    def setUp(self) -> None:
        self.detector = AntiPatternDetector()

    def test_detects_missing_limit(self) -> None:
        sql = "SELECT id, name FROM large_table WHERE status = 'active'"
        result = self.detector.detect_missing_limit(sql)
        self.assertIsNotNone(result)
        self.assertIn("LIMIT", result.rewritten_sql)

    def test_no_detection_with_limit(self) -> None:
        sql = "SELECT id FROM orders LIMIT 100"
        result = self.detector.detect_missing_limit(sql)
        self.assertIsNone(result)

    def test_no_detection_for_aggregate(self) -> None:
        sql = "SELECT COUNT(*) FROM orders"
        result = self.detector.detect_missing_limit(sql)
        self.assertIsNone(result)


class TestDetectCountDistinct(unittest.TestCase):
    """Test COUNT(DISTINCT) to HLL detection."""

    def setUp(self) -> None:
        self.detector = AntiPatternDetector()

    def test_detects_count_distinct(self) -> None:
        sql = "SELECT COUNT(DISTINCT user_id) FROM events"
        result = self.detector.detect_count_distinct_on_hll(sql)
        self.assertIsNotNone(result)
        self.assertIn("approx_count_distinct", result.rewritten_sql)


class TestDetectNonSargableLike(unittest.TestCase):
    """Test non-sargable LIKE detection."""

    def setUp(self) -> None:
        self.detector = AntiPatternDetector()

    def test_detects_leading_wildcard(self) -> None:
        sql = "SELECT * FROM users WHERE email LIKE '%@gmail.com'"
        result = self.detector.detect_non_sargable_predicate(sql)
        self.assertIsNotNone(result)
        self.assertEqual(result.rule_name, "non_sargable_like")


class TestNoFalsePositives(unittest.TestCase):
    """Test that well-written SQL produces no suggestions."""

    def test_clean_sql_no_suggestions(self) -> None:
        catalog = MetadataCatalog()
        catalog.register_table(
            "t1",
            TableMeta(
                table_name="users",
                schema_name="public",
                tenant_id="t1",
                columns=[
                    ColumnMeta(name="id", dtype="int"),
                    ColumnMeta(name="name", dtype="varchar"),
                ],
                row_count=100,
            ),
        )
        analyzer = RewriteAnalyzer(catalog=catalog)
        suggestions = analyzer.analyze(
            None, "SELECT id, name FROM users WHERE id = 1 LIMIT 10", "t1"
        )
        self.assertEqual(len(suggestions), 0)


class TestImprovementEstimatePositive(unittest.TestCase):
    """Test that all suggestions have positive improvement estimates."""

    def test_correlated_subquery_positive_improvement(self) -> None:
        detector = AntiPatternDetector()
        sql = (
            "SELECT * FROM orders WHERE EXISTS "
            "(SELECT 1 FROM items WHERE items.order_id = orders.id)"
        )
        result = detector.detect_correlated_subquery(sql)
        self.assertIsNotNone(result)
        self.assertGreater(result.estimated_improvement_pct, 0)


class TestRewriteAnalyzerSorting(unittest.TestCase):
    """Test that suggestions are sorted by improvement DESC."""

    def test_sorted_by_improvement(self) -> None:
        analyzer = RewriteAnalyzer()
        sql = (
            "SELECT * FROM orders WHERE status = 'a' OR status = 'b'"
        )
        suggestions = analyzer.analyze(None, sql, "t1")
        if len(suggestions) >= 2:
            for i in range(len(suggestions) - 1):
                self.assertGreaterEqual(
                    suggestions[i].estimated_improvement_pct,
                    suggestions[i + 1].estimated_improvement_pct,
                )


class TestRewriteSuggester(unittest.TestCase):
    """Test the RewriteSuggester wrapper."""

    def test_suggest_delegates_to_analyzer(self) -> None:
        analyzer = RewriteAnalyzer()
        suggester = RewriteSuggester(analyzer)
        suggestions = suggester.suggest(None, "SELECT * FROM big_table", "t1")
        self.assertTrue(len(suggestions) > 0)

    def test_apply_rewrite_returns_rewritten_sql(self) -> None:
        analyzer = RewriteAnalyzer()
        suggester = RewriteSuggester(analyzer)
        suggestions = suggester.suggest(None, "SELECT * FROM big_table", "t1")
        if suggestions:
            result = suggester.apply_rewrite(suggestions[0], "SELECT * FROM big_table")
            self.assertIsInstance(result, str)

    def test_persist_without_engine(self) -> None:
        analyzer = RewriteAnalyzer()
        suggester = RewriteSuggester(analyzer)
        suggestions = suggester.suggest(None, "SELECT * FROM t", "t1")
        if suggestions:
            # Should not raise
            suggester.persist_suggestion(suggestions[0], "t1", None)


if __name__ == "__main__":
    unittest.main()
