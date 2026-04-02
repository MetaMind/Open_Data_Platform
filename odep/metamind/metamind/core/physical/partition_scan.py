"""Partition pruning for scan nodes."""
from __future__ import annotations
from metamind.core.metadata.models import PartitionMeta
from metamind.core.logical.nodes import Predicate

def prune_partitions(partitions: list[PartitionMeta], predicates: list[Predicate]) -> list[PartitionMeta]:
    if not predicates: return partitions
    return [p for p in partitions if p.is_prunable is not False]
