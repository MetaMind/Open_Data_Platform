"""Pipeline SDK — load, validate, and convert pipeline definitions."""

from __future__ import annotations

import importlib.util
import re
from typing import Any, Dict, List, Optional

from odep.exceptions import PipelineParseError
from odep.models import JobDefinition, LineageEdge

_CRON_FIELD_PATTERN = re.compile(r"^[\d\*\/\-\,]+$")


class Pipeline:
    """Represents a parsed and validated ODEP pipeline definition."""

    def __init__(
        self,
        name: str,
        description: str = "",
        schedule: Optional[str] = None,
        sources: Optional[List[Dict[str, Any]]] = None,
        sinks: Optional[List[Dict[str, Any]]] = None,
        transforms: Optional[List[Dict[str, Any]]] = None,
        quality_rules: Optional[List[Dict[str, Any]]] = None,
        lineage_config: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.name = name
        self.description = description
        self.schedule = schedule
        self.sources: List[Dict[str, Any]] = sources if sources is not None else []
        self.sinks: List[Dict[str, Any]] = sinks if sinks is not None else []
        self.transforms: List[Dict[str, Any]] = transforms if transforms is not None else []
        self.quality_rules: List[Dict[str, Any]] = quality_rules if quality_rules is not None else []
        self.lineage_config: Dict[str, Any] = lineage_config if lineage_config is not None else {}
        self._valid: bool = False
        self._path: Optional[str] = None

    @classmethod
    def from_file(cls, path: str) -> "Pipeline":
        """Load a Pipeline from a YAML or Python file."""
        if path.endswith(".yaml") or path.endswith(".yml"):
            pipeline = cls._from_yaml(path)
        elif path.endswith(".py"):
            pipeline = cls._from_python(path)
        else:
            raise PipelineParseError(path, "format", "unsupported file type; expected .yaml, .yml, or .py")
        pipeline._path = path
        pipeline.validate()
        return pipeline

    @classmethod
    def _from_yaml(cls, path: str) -> "Pipeline":
        import yaml
        try:
            with open(path, "r") as fh:
                data = yaml.safe_load(fh)
        except yaml.YAMLError as exc:
            line_number: Optional[int] = None
            if hasattr(exc, "problem_mark") and exc.problem_mark is not None:
                line_number = exc.problem_mark.line + 1
            raise PipelineParseError(path, "yaml", str(exc), line_number) from exc
        except OSError as exc:
            raise PipelineParseError(path, "file", str(exc)) from exc

        if not isinstance(data, dict):
            raise PipelineParseError(path, "yaml", "top-level YAML must be a mapping")

        for required_field in ("name", "sources", "sinks"):
            if required_field not in data:
                raise PipelineParseError(path, required_field, "required field missing")

        sources = data["sources"]
        sinks = data["sinks"]
        if not sources:
            raise PipelineParseError(path, "sources", "required field missing")
        if not sinks:
            raise PipelineParseError(path, "sinks", "required field missing")

        lineage_raw = data.get("lineage", {})
        return cls(
            name=data["name"],
            description=data.get("description", ""),
            schedule=data.get("schedule"),
            sources=sources if isinstance(sources, list) else [],
            sinks=sinks if isinstance(sinks, list) else [],
            transforms=data.get("transforms") or [],
            quality_rules=data.get("quality_rules") or [],
            lineage_config=lineage_raw if isinstance(lineage_raw, dict) else {},
        )

    @classmethod
    def _from_python(cls, path: str) -> "Pipeline":
        spec = importlib.util.spec_from_file_location("_odep_pipeline_module", path)
        if spec is None or spec.loader is None:
            raise PipelineParseError(path, "pipeline", f"cannot load Python module from {path!r}")
        module = importlib.util.module_from_spec(spec)
        try:
            spec.loader.exec_module(module)  # type: ignore[union-attr]
        except Exception as exc:
            raise PipelineParseError(path, "pipeline", str(exc)) from exc

        if hasattr(module, "pipeline") and isinstance(module.pipeline, cls):
            return module.pipeline
        if hasattr(module, "get_pipeline") and callable(module.get_pipeline):
            result = module.get_pipeline()
            if isinstance(result, cls):
                return result
        raise PipelineParseError(
            path, "pipeline",
            "module must define a 'pipeline' variable or 'get_pipeline()' function",
        )

    def validate(self) -> bool:
        """Local validation — no network calls. Sets _valid=True on success."""
        path = self._path or ""
        if not self.name or not self.name.strip():
            raise PipelineParseError(path, "name", "name must be non-empty")
        if not self.sources:
            raise PipelineParseError(path, "sources", "at least one source required")
        if not self.sinks:
            raise PipelineParseError(path, "sinks", "at least one sink required")
        if self.schedule is not None:
            self._validate_cron(path, self.schedule)
        self._valid = True
        return True

    def _validate_cron(self, path: str, schedule: str) -> None:
        parts = schedule.strip().split()
        if len(parts) not in (5, 6):
            raise PipelineParseError(
                path, "schedule",
                f"cron expression must have 5 or 6 fields, got {len(parts)}: {schedule!r}",
            )
        for part in parts:
            if not _CRON_FIELD_PATTERN.match(part):
                raise PipelineParseError(
                    path, "schedule",
                    f"invalid cron field {part!r} in schedule {schedule!r}",
                )

    def is_valid(self) -> bool:
        return self._valid

    def to_job_definition(self, env: str) -> JobDefinition:
        return JobDefinition(
            job_id=f"{self.name}_{env}",
            name=self.name,
            schedule=self.schedule,
            task_config={"sources": self.sources, "sinks": self.sinks, "transforms": self.transforms},
            dependencies=[],
        )

    def extract_lineage_edges(self) -> List[LineageEdge]:
        """Return deduplicated lineage edges for all source-to-sink pairs.

        If ``lineage_config["column_level"]`` is True, also emits per-column
        edges (zipped source columns → sink columns).
        """
        edges: List[LineageEdge] = []
        seen: set[tuple[str, str]] = set()

        for source in self.sources:
            src_urn = source["urn"]
            for sink in self.sinks:
                sink_urn = sink["urn"]
                if src_urn == sink_urn:
                    continue
                key = (src_urn, sink_urn)
                if key not in seen:
                    edges.append(LineageEdge(source_urn=src_urn, target_urn=sink_urn))
                    seen.add(key)

        if self.lineage_config.get("column_level"):
            for source in self.sources:
                src_urn = source["urn"]
                src_cols = source.get("columns", [])
                for sink in self.sinks:
                    sink_urn = sink["urn"]
                    if src_urn == sink_urn:
                        continue
                    sink_cols = sink.get("columns", [])
                    for src_col, sink_col in zip(src_cols, sink_cols):
                        transformation = f"column:{src_col}->{sink_col}"
                        key = (src_urn, sink_urn)
                        edges.append(
                            LineageEdge(
                                source_urn=src_urn,
                                target_urn=sink_urn,
                                transformation=transformation,
                            )
                        )

        return edges
