"""Tests for {{ cookiecutter.pipeline_name }}."""
from pipeline import pipeline


def test_pipeline_is_valid():
    pipeline.validate()
    assert pipeline.is_valid()


def test_pipeline_has_quality_rules():
    assert len(pipeline.quality_rules) > 0


def test_pipeline_has_sources_and_sinks():
    assert len(pipeline.sources) > 0
    assert len(pipeline.sinks) > 0
