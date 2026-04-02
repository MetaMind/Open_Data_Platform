"""Integration test: schema drift detection.

Validates Requirement 19.1 — re-registering a dataset with a changed schema
emits SchemaDriftWarning with a non-empty column diff.
"""
import warnings

from odep.adapters.openmeta.adapter import OpenMetaAdapter
from odep.config import MetadataConfig
from odep.exceptions import SchemaDriftWarning
from odep.models import DatasetMetadata


def test_schema_drift_emits_warning_with_nonempty_diff():
    """Register dataset, modify schema, re-register — assert SchemaDriftWarning with diff."""
    metadata = OpenMetaAdapter(MetadataConfig())

    original = DatasetMetadata(
        **{
            "urn": "urn:li:dataset:(duckdb,raw.events,dev)",
            "name": "raw_events",
            "platform": "duckdb",
            "env": "dev",
            "schema": [
                {"name": "id", "type": "INTEGER"},
                {"name": "event_type", "type": "VARCHAR"},
            ],
            "owner": "test-engineer",
        }
    )
    metadata.register_dataset(original)

    # Modified schema: added 'created_at', removed 'event_type'
    modified = DatasetMetadata(
        **{
            "urn": "urn:li:dataset:(duckdb,raw.events,dev)",
            "name": "raw_events",
            "platform": "duckdb",
            "env": "dev",
            "schema": [
                {"name": "id", "type": "INTEGER"},
                {"name": "created_at", "type": "TIMESTAMP"},
            ],
            "owner": "test-engineer",
        }
    )

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        metadata.register_dataset(modified)

    schema_drift_warnings = [w for w in caught if issubclass(w.category, SchemaDriftWarning)]
    assert len(schema_drift_warnings) == 1, "Expected exactly one SchemaDriftWarning"

    warning = schema_drift_warnings[0].message
    assert isinstance(warning, SchemaDriftWarning)
    diff = warning.diff

    # Diff must be non-empty
    assert diff["added"] or diff["removed"] or diff["changed"], f"Expected non-empty diff, got: {diff}"

    # Specifically: 'created_at' added, 'event_type' removed
    added_names = [f["name"] for f in diff["added"]]
    removed_names = [f["name"] for f in diff["removed"]]
    assert "created_at" in added_names
    assert "event_type" in removed_names
