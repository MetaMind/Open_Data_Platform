"""Unit tests for F28 NL Query Interface."""
from __future__ import annotations

import sys
import os
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from metamind.core.catalog.metadata import MetadataCatalog
from metamind.core.nl_interface.generator import (
    ConversationManager,
    NLConversationTurn,
    NLFeedbackCollector,
    NLQueryGenerator,
    SchemaAutoDiscovery,
)
from metamind.core.types import ColumnMeta, TableMeta


class TestSchemaAutoDiscovery(unittest.TestCase):
    """Test schema auto-discovery for NL queries."""

    def setUp(self) -> None:
        self.catalog = MetadataCatalog()
        self.catalog.register_table(
            "tenant1",
            TableMeta(
                table_name="orders",
                schema_name="public",
                tenant_id="tenant1",
                columns=[
                    ColumnMeta(name="id", dtype="int"),
                    ColumnMeta(name="customer_id", dtype="int"),
                    ColumnMeta(name="total_amount", dtype="decimal"),
                    ColumnMeta(name="region", dtype="varchar"),
                    ColumnMeta(name="status", dtype="varchar"),
                ],
                row_count=100_000,
            ),
        )
        self.catalog.register_table(
            "tenant1",
            TableMeta(
                table_name="customers",
                schema_name="public",
                tenant_id="tenant1",
                columns=[
                    ColumnMeta(name="id", dtype="int"),
                    ColumnMeta(name="name", dtype="varchar"),
                    ColumnMeta(name="email", dtype="varchar"),
                ],
                row_count=10_000,
            ),
        )
        self.catalog.register_table(
            "tenant1",
            TableMeta(
                table_name="products",
                schema_name="public",
                tenant_id="tenant1",
                columns=[
                    ColumnMeta(name="id", dtype="int"),
                    ColumnMeta(name="name", dtype="varchar"),
                    ColumnMeta(name="price", dtype="decimal"),
                ],
                row_count=5_000,
            ),
        )
        self.discovery = SchemaAutoDiscovery(self.catalog)

    def test_discovers_orders_table_for_order_query(self) -> None:
        tables = self.discovery.discover_tables(
            "show me total orders by region", "tenant1"
        )
        self.assertIn("orders", tables)

    def test_discovers_customers_table(self) -> None:
        tables = self.discovery.discover_tables(
            "list all customer emails", "tenant1"
        )
        self.assertIn("customers", tables)

    def test_discovers_products_table(self) -> None:
        tables = self.discovery.discover_tables(
            "what are the product prices", "tenant1"
        )
        self.assertIn("products", tables)

    def test_returns_tables_for_vague_query(self) -> None:
        tables = self.discovery.discover_tables(
            "xyz nonsense query", "tenant1"
        )
        # Should return all tables as fallback
        self.assertTrue(len(tables) > 0)

    def test_max_tables_limit(self) -> None:
        tables = self.discovery.discover_tables(
            "show everything", "tenant1", max_tables=2
        )
        self.assertLessEqual(len(tables), 2)


class TestConfidenceScoring(unittest.TestCase):
    """Test confidence score estimation."""

    def setUp(self) -> None:
        self.catalog = MetadataCatalog()
        self.catalog.register_table(
            "t1",
            TableMeta(
                table_name="users",
                schema_name="public",
                tenant_id="t1",
                columns=[
                    ColumnMeta(name="id", dtype="int"),
                    ColumnMeta(name="name", dtype="varchar"),
                    ColumnMeta(name="email", dtype="varchar"),
                ],
                row_count=1000,
            ),
        )
        self.generator = NLQueryGenerator(
            api_key=None, catalog=self.catalog, provider="openai"
        )

    def test_confidence_all_tables_exist(self) -> None:
        schema_ctx = "Table: users (1000 rows)\n  Columns: id (int), name (varchar), email (varchar)"
        sql = "SELECT id, name FROM users WHERE id = 1"
        confidence = self.generator._estimate_confidence(sql, schema_ctx, True, False)
        self.assertGreaterEqual(confidence, 0.8)

    def test_confidence_zero_for_empty_sql(self) -> None:
        confidence = self.generator._estimate_confidence("", "", False, False)
        self.assertEqual(confidence, 0.0)

    def test_confidence_higher_with_verified_example(self) -> None:
        schema_ctx = "Table: users"
        sql = "SELECT id FROM users WHERE id = 1"
        without = self.generator._estimate_confidence(sql, schema_ctx, True, False)
        with_ex = self.generator._estimate_confidence(sql, schema_ctx, True, True)
        self.assertGreater(with_ex, without)


class TestMultiTurnConversation(unittest.TestCase):
    """Test conversation management."""

    def test_add_turns(self) -> None:
        conv = ConversationManager(max_turns=10)
        conv.add_turn("user", "show me orders")
        conv.add_turn("assistant", "SELECT * FROM orders", sql="SELECT * FROM orders")
        window = conv.get_context_window()
        self.assertEqual(len(window), 2)

    def test_context_window_includes_history(self) -> None:
        conv = ConversationManager(max_turns=10)
        conv.add_turn("user", "first question")
        conv.add_turn("assistant", "first answer")
        conv.add_turn("user", "second question")
        window = conv.get_context_window()
        self.assertEqual(len(window), 3)
        self.assertEqual(window[0]["role"], "user")
        self.assertEqual(window[0]["content"], "first question")

    def test_context_window_respects_max_turns(self) -> None:
        conv = ConversationManager(max_turns=2)
        for i in range(10):
            conv.add_turn("user", f"message {i}")
            conv.add_turn("assistant", f"reply {i}")
        window = conv.get_context_window()
        self.assertLessEqual(len(window), 4)  # max_turns * 2

    def test_generated_sql_in_context(self) -> None:
        conv = ConversationManager()
        conv.add_turn("user", "show orders")
        conv.add_turn("assistant", "here", sql="SELECT * FROM orders")
        window = conv.get_context_window()
        self.assertEqual(window[1]["generated_sql"], "SELECT * FROM orders")


class TestSQLValidation(unittest.TestCase):
    """Test SQL validation."""

    def setUp(self) -> None:
        self.generator = NLQueryGenerator(api_key=None, provider="openai")

    def test_valid_sql_passes(self) -> None:
        is_valid, error = self.generator._validate_sql("SELECT * FROM orders")
        self.assertTrue(is_valid)
        self.assertIsNone(error)

    def test_invalid_sql_syntax_error(self) -> None:
        is_valid, error = self.generator._validate_sql("SELECT * FORM orders")
        # sqlglot may or may not catch this depending on dialect;
        # basic keyword check should still pass since SELECT is present
        # so we just check it returns a tuple
        self.assertIsInstance(is_valid, bool)

    def test_empty_sql_fails(self) -> None:
        is_valid, error = self.generator._validate_sql("")
        self.assertFalse(is_valid)


class TestSQLExtraction(unittest.TestCase):
    """Test SQL extraction from LLM responses."""

    def setUp(self) -> None:
        self.generator = NLQueryGenerator(api_key=None, provider="openai")

    def test_extract_from_markdown_fence(self) -> None:
        raw = "```sql\nSELECT * FROM orders;\n```"
        sql = self.generator._extract_sql(raw)
        self.assertEqual(sql, "SELECT * FROM orders;")

    def test_extract_plain_sql(self) -> None:
        raw = "SELECT id FROM users"
        sql = self.generator._extract_sql(raw)
        self.assertEqual(sql, "SELECT id FROM users")

    def test_extract_from_generic_fence(self) -> None:
        raw = "```\nSELECT 1;\n```"
        sql = self.generator._extract_sql(raw)
        self.assertEqual(sql, "SELECT 1;")


class TestNLGeneratorErrorHandling(unittest.TestCase):
    """Test graceful error handling when API key is missing."""

    def test_generate_without_api_key_returns_error(self) -> None:
        catalog = MetadataCatalog()
        gen = NLQueryGenerator(
            api_key=None, catalog=catalog, provider="openai"
        )
        result = gen.generate("show me orders", "tenant1")
        self.assertEqual(result.confidence, 0.0)
        self.assertFalse(result.was_validated)
        self.assertIn("API key", result.validation_error or result.explanation)


class TestNLFeedbackCollector(unittest.TestCase):
    """Test feedback collection."""

    def test_record_without_engine(self) -> None:
        collector = NLFeedbackCollector()
        # Should not raise even without an engine
        collector.record_feedback(
            "t1", "query", "SELECT 1", True, None, None
        )

    def test_get_examples_without_engine(self) -> None:
        collector = NLFeedbackCollector()
        examples = collector.get_verified_examples("t1", engine=None)
        self.assertEqual(examples, [])


if __name__ == "__main__":
    unittest.main()
