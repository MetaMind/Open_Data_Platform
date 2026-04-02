"""SQL template extractor for workload analysis."""
from __future__ import annotations
from metamind.core.cache.plan_cache import QueryFingerprint
class TemplateExtractor:
    def __init__(self) -> None:
        self._fp = QueryFingerprint()
    def extract(self, sql: str) -> str:
        return self._fp.extract_template(sql)
