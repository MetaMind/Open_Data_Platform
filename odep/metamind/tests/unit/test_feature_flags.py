"""Unit tests for feature flag system (Phase 2 expansion)."""
from __future__ import annotations

from dataclasses import fields
from unittest.mock import MagicMock, patch

import pytest

from metamind.config.feature_flags import FeatureFlags, FeatureFlagManager


class TestFeatureFlags:
    """Tests for FeatureFlags dataclass."""

    def test_all_flags_disabled_by_default(self) -> None:
        """All flags should default to False for backward compatibility."""
        flags = FeatureFlags()
        for field in fields(flags):
            assert getattr(flags, field.name) is False, (
                f"Flag {field.name} should be False by default"
            )

    def test_all_enabled_preset(self) -> None:
        """all_enabled() should set every flag to True."""
        flags = FeatureFlags.all_enabled()
        for field in fields(flags):
            assert getattr(flags, field.name) is True, (
                f"Flag {field.name} should be True in all_enabled preset"
            )

    def test_phase1_preset_has_expected_flags(self) -> None:
        """phase1() preset should enable the core Phase 1 features."""
        flags = FeatureFlags.phase1()
        # Phase 1 includes skew detection and predicate inference
        assert flags.F03_skew_detection is True
        assert flags.F05_predicate_inference is True

    def test_to_dict_returns_all_flags(self) -> None:
        """to_dict() should return a dict entry for every flag."""
        flags = FeatureFlags()
        d = flags.to_dict()
        assert len(d) == len(fields(flags))
        for field in fields(flags):
            assert field.name in d

    def test_from_dict_ignores_unknown_keys(self) -> None:
        """from_dict() should silently ignore keys not in FeatureFlags."""
        flags = FeatureFlags.from_dict({
            "F01_learned_cardinality": True,
            "unknown_future_flag": True,
        })
        assert flags.F01_learned_cardinality is True

    def test_from_dict_sets_specified_flags(self) -> None:
        """from_dict() should set listed flags to True."""
        flags = FeatureFlags.from_dict({
            "F09_plan_caching": True,
            "F13_universal_connectors": True,
            "F17_dialect_aware_sql": True,
        })
        assert flags.F09_plan_caching is True
        assert flags.F13_universal_connectors is True
        assert flags.F17_dialect_aware_sql is True
        # Unspecified flags remain False
        assert flags.F01_learned_cardinality is False

    def test_individual_flag_toggle(self) -> None:
        """Flags can be toggled individually via constructor."""
        flags = FeatureFlags(F23_cloud_budget=True)
        assert flags.F23_cloud_budget is True
        assert flags.F24_workload_classification is False

    def test_f13_enables_connectors(self) -> None:
        """F13 flag controls universal connectors."""
        flags = FeatureFlags(F13_universal_connectors=True)
        assert flags.F13_universal_connectors is True

    def test_f17_enables_dialect_sql(self) -> None:
        """F17 flag controls dialect-aware SQL generation."""
        flags = FeatureFlags(F17_dialect_aware_sql=True)
        assert flags.F17_dialect_aware_sql is True

    def test_thirty_flags_exist(self) -> None:
        """Exactly 30 feature flags should be defined."""
        count = len(fields(FeatureFlags()))
        assert count == 30, f"Expected 30 flags, found {count}"

    def test_flag_names_follow_convention(self) -> None:
        """All flag names should follow the F##_description pattern."""
        import re
        pattern = re.compile(r"^F\d{2}_")
        for field in fields(FeatureFlags()):
            assert pattern.match(field.name), (
                f"Flag '{field.name}' does not follow F##_description naming"
            )


class TestFeatureFlagManager:
    """Tests for FeatureFlagManager persistence layer."""

    def test_get_flags_returns_feature_flags(self) -> None:
        """get_flags() should return a FeatureFlags instance."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [
            ("F09_plan_caching", True),
            ("F13_universal_connectors", True),
        ]
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.execute.return_value = mock_result
        mock_engine.connect.return_value = mock_conn

        manager = FeatureFlagManager(mock_engine, tenant_id="test-tenant")
        flags = manager.get_flags()
        assert isinstance(flags, FeatureFlags)

    def test_flag_manager_with_empty_db(self) -> None:
        """An empty DB should return all-default flags."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.execute.return_value = mock_result
        mock_engine.connect.return_value = mock_conn

        manager = FeatureFlagManager(mock_engine, tenant_id="tenant-x")
        flags = manager.get_flags()
        assert isinstance(flags, FeatureFlags)
        # With empty DB, all flags should be defaults (False)
        assert flags.F01_learned_cardinality is False
