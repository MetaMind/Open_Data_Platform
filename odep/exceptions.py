"""Custom exceptions for the Open Data Engineering Platform (ODEP)."""

from __future__ import annotations

__all__ = [
    "AdapterNotFoundError",
    "ProtocolViolationError",
    "OrchestratorConnectionError",
    "QualityGateFailure",
    "PipelineParseError",
    "PipelineExecutionError",
    "AuthenticationError",
    "SchemaDriftWarning",
]


class AdapterNotFoundError(ValueError):
    """Raised when AdapterFactory is called with an unregistered engine name."""

    def __init__(self, layer: str, engine_name: str, valid_options: list[str] = None):
        self.layer = layer
        self.engine_name = engine_name
        self.valid_options = valid_options
        message = (
            f"No adapter registered for layer={layer!r}, engine={engine_name!r}. "
            f"Valid options: {valid_options}"
        )
        super().__init__(message)


class ProtocolViolationError(TypeError):
    """Raised when an adapter does not satisfy the required Protocol."""

    def __init__(self, adapter_class, protocol):
        self.adapter_class = adapter_class
        self.protocol = protocol
        message = (
            f"{adapter_class.__name__} does not satisfy the {protocol.__name__} Protocol"
        )
        super().__init__(message)


class OrchestratorConnectionError(ConnectionError):
    """Raised when the orchestrator is unreachable."""

    def __init__(self, url: str, hint: str = ""):
        self.url = url
        self.hint = hint
        message = f"Orchestrator unreachable at {url!r}. {hint}"
        super().__init__(message)


class QualityGateFailure(Exception):
    """Raised when a blocking quality rule fails after job execution."""

    def __init__(self, rule_name: str, metrics: dict):
        self.rule_name = rule_name
        self.metrics = metrics
        message = f"Quality gate failed for rule {rule_name!r}. Metrics: {metrics}"
        super().__init__(message)


class PipelineParseError(ValueError):
    """Raised when Pipeline.from_file() encounters malformed input."""

    def __init__(self, path: str, field: str, reason: str, line_number: int = None):
        self.path = path
        self.field = field
        self.reason = reason
        self.line_number = line_number
        message = f"Failed to parse pipeline at {path!r}, field={field!r}: {reason}"
        if line_number is not None:
            message += f" (line {line_number})"
        super().__init__(message)


class PipelineExecutionError(RuntimeError):
    """Raised when a pipeline run fails."""

    def __init__(self, run_id: str, logs: list[str]):
        self.run_id = run_id
        self.logs = logs
        message = f"Pipeline run {run_id!r} failed. Last logs: {logs[-5:] if logs else []}"
        super().__init__(message)


class AuthenticationError(PermissionError):
    """Raised when authentication fails for a service."""

    def __init__(self, service: str, reason: str):
        self.service = service
        self.reason = reason
        message = f"Authentication failed for service={service!r}: {reason}"
        super().__init__(message)


class SchemaDriftWarning(UserWarning, Exception):
    """Emitted when an incoming dataset schema differs from the registered catalog schema.

    Inherits from both UserWarning and Exception so it can be raised or warned.
    """

    def __init__(self, urn: str, diff: dict):
        self.urn = urn
        self.diff = diff
        message = f"Schema drift detected for dataset {urn!r}. Diff: {diff}"
        super().__init__(message)
