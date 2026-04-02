"""
AI Synthesis Layer — Package Init

File: metamind/synthesis/__init__.py
Role: Package entry point

Exposes the primary public API for the AI Synthesis Layer.
"""

from __future__ import annotations

from metamind.synthesis.synthesis_engine import SynthesisEngine, SynthesisCycleResult
from metamind.synthesis.workload_profiler import WorkloadProfiler, WorkloadStats
from metamind.synthesis.rule_generator import RuleGenerator, SynthesizedRule
from metamind.synthesis.feedback_trainer import FeedbackTrainer, RetrainResult
from metamind.synthesis.plan_feature_extractor import PlanFeatureExtractor, PlanFeatures
from metamind.synthesis.training_dataset import TrainingDatasetBuilder, TrainingSample

__all__ = [
    "SynthesisEngine",
    "SynthesisCycleResult",
    "WorkloadProfiler",
    "WorkloadStats",
    "RuleGenerator",
    "SynthesizedRule",
    "FeedbackTrainer",
    "RetrainResult",
    "PlanFeatureExtractor",
    "PlanFeatures",
    "TrainingDatasetBuilder",
    "TrainingSample",
]
