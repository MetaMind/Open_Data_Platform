"""MV-based query rewriter."""
from __future__ import annotations
from metamind.core.logical.nodes import LogicalNode, ScanNode
from metamind.core.metadata.models import MaterializedViewMeta
class MVRewriter:
    def rewrite(self, root: LogicalNode, mv: MaterializedViewMeta) -> LogicalNode:
        return ScanNode(table_name=mv.mv_name, schema_name=mv.schema_name)
