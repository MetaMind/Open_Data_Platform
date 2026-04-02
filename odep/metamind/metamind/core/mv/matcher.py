"""MV query matching."""
from __future__ import annotations
import logging
from metamind.core.metadata.models import MaterializedViewMeta
from metamind.core.logical.nodes import LogicalNode
logger = logging.getLogger(__name__)
class MVMatcher:
    def find_matching_mvs(self, root: LogicalNode, mvs: list[MaterializedViewMeta]) -> list[MaterializedViewMeta]:
        return []  # Base impl; full matching requires SQL equivalence check
