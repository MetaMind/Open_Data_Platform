"""MetaMind interactive SQL shell — REPL powered by prompt_toolkit (F13/CLI)."""
from __future__ import annotations

import logging
import sys
from typing import Any, Optional

logger = logging.getLogger(__name__)

# ANSI colour helpers (degrade gracefully in non-TTY environments)
_RED = "\033[31m"
_GREEN = "\033[32m"
_YELLOW = "\033[33m"
_CYAN = "\033[36m"
_RESET = "\033[0m"


def _colour(text: str, code: str) -> str:
    """Apply an ANSI colour code when stdout is a terminal."""
    if sys.stdout.isatty():
        return f"{code}{text}{_RESET}"
    return text


# Shell command dispatch table
_SHELL_COMMANDS: dict[str, str] = {
    r"\d": "List all tables in the catalog",
    r"\f": "List enabled feature flags",
    r"\b": "List registered backend connectors",
    r"\q": "Quit the shell",
    r"\h": "Show this help message",
}


class MetaMindShell:
    """Interactive REPL for MetaMind.

    Supports SQL execution, metacommands (\\d, \\f, \\b, \\q), command
    history, and basic autocomplete via prompt_toolkit.

    Args:
        url: Base URL of the MetaMind API server.
        tenant_id: Tenant identifier used for all requests.
    """

    def __init__(self) -> None:
        """Initialise shell with empty state."""
        self._url: str = ""
        self._tenant_id: str = ""
        self._history: list[str] = []
        self._client: Optional[Any] = None

    def start(self, url: str, tenant_id: str) -> None:
        """Start the interactive shell REPL.

        Attempts to use prompt_toolkit for rich editing experience.
        Falls back to basic input() if prompt_toolkit is unavailable.

        Args:
            url: MetaMind API base URL.
            tenant_id: Tenant ID for all API calls.
        """
        self._url = url.rstrip("/")
        self._tenant_id = tenant_id
        self._client = self._build_client()

        print(_colour("MetaMind Interactive Shell", _CYAN))
        print(_colour(f"Connected to: {url}  |  Tenant: {tenant_id}", _YELLOW))
        print("Type \\h for help, \\q to quit.\n")

        try:
            self._start_prompt_toolkit()
        except ImportError:
            logger.debug("prompt_toolkit not available; using basic input")
            self._start_basic()

    def _start_prompt_toolkit(self) -> None:
        """Run the REPL using prompt_toolkit for rich editing."""
        from prompt_toolkit import PromptSession  # type: ignore[import]
        from prompt_toolkit.history import InMemoryHistory  # type: ignore[import]
        from prompt_toolkit.auto_suggest import AutoSuggestFromHistory  # type: ignore[import]
        from prompt_toolkit.completion import WordCompleter  # type: ignore[import]

        keywords = [
            "SELECT", "FROM", "WHERE", "JOIN", "GROUP BY", "ORDER BY",
            "LIMIT", "HAVING", "INSERT", "UPDATE", "DELETE", "EXPLAIN",
            "WITH", "UNION", "CREATE", "DROP", "ALTER",
        ]
        completer = WordCompleter(
            keywords + list(_SHELL_COMMANDS.keys()), ignore_case=True
        )
        session: PromptSession = PromptSession(  # type: ignore[type-arg]
            history=InMemoryHistory(),
            auto_suggest=AutoSuggestFromHistory(),
            completer=completer,
        )

        while True:
            try:
                line = session.prompt("metamind> ").strip()
            except (KeyboardInterrupt, EOFError):
                print("\nBye!")
                break

            if not line:
                continue
            if self._handle_command(line):
                break  # \q was issued

    def _start_basic(self) -> None:
        """Fallback REPL using Python's built-in input()."""
        while True:
            try:
                line = input("metamind> ").strip()
            except (KeyboardInterrupt, EOFError):
                print("\nBye!")
                break

            if not line:
                continue
            if self._handle_command(line):
                break

    def _handle_command(self, line: str) -> bool:
        """Dispatch a line to a metacommand or SQL executor.

        Args:
            line: Raw input line from the user.

        Returns:
            True if the shell should exit, False otherwise.
        """
        if line.startswith("\\"):
            return self._dispatch_metacommand(line)
        self._execute_sql(line)
        return False

    def _dispatch_metacommand(self, cmd: str) -> bool:
        """Handle backslash metacommands.

        Args:
            cmd: Metacommand string (e.g. ``\\d``, ``\\q``).

        Returns:
            True if the shell should quit.
        """
        base = cmd.split()[0].lower()
        if base == r"\q":
            print("Bye!")
            return True
        if base == r"\h":
            self._print_help()
        elif base == r"\d":
            self._list_tables()
        elif base == r"\f":
            self._list_features()
        elif base == r"\b":
            self._list_backends()
        else:
            print(_colour(f"Unknown command: {cmd}", _RED))
        return False

    def _execute_sql(self, sql: str) -> None:
        """Send SQL to the API and print results."""
        self._history.append(sql)
        if self._client is None:
            print(_colour("Not connected to API.", _RED))
            return
        try:
            resp = self._client.post(
                f"{self._url}/api/v1/query",
                json={"sql": sql, "tenant_id": self._tenant_id},
            )
            resp.raise_for_status()
            data = resp.json()
            rows = data.get("rows", [])
            cols = data.get("columns", list(rows[0].keys()) if rows else [])
            self._print_table(cols, rows)
            print(_colour(
                f"\n({data.get('row_count', len(rows))} rows, "
                f"{data.get('duration_ms', 0):.1f} ms)",
                _GREEN,
            ))
        except Exception as exc:
            print(_colour(f"Error: {exc}", _RED))

    def _list_tables(self) -> None:
        """Fetch and display table list from catalog."""
        if self._client is None:
            print(_colour("Not connected.", _RED))
            return
        try:
            resp = self._client.get(
                f"{self._url}/api/v1/tables",
                headers={"X-Tenant-ID": self._tenant_id},
            )
            resp.raise_for_status()
            data = resp.json()
            tables = data.get("tables", [])
            if not tables:
                print(_colour("No tables registered.", _YELLOW))
                return
            print(_colour(f"Tables ({len(tables)}):", _CYAN))
            for tbl in tables:
                schema = tbl.get("schema_name", "public")
                name = tbl.get("table_name", tbl.get("name", str(tbl)))
                backend = tbl.get("backend", "")
                print(f"  {schema}.{name}  [{backend}]")
        except Exception as exc:
            print(_colour(f"Could not list tables: {exc}", _RED))

    def _list_features(self) -> None:
        """Fetch and display enabled feature flags."""
        if self._client is None:
            print(_colour("Not connected.", _RED))
            return
        try:
            resp = self._client.get(
                f"{self._url}/api/v1/features",
                headers={"X-Tenant-ID": self._tenant_id},
            )
            resp.raise_for_status()
            flags = resp.json().get("flags", {})
            print(_colour("Feature flags:", _CYAN))
            for name, enabled in sorted(flags.items()):
                status = _colour("✓", _GREEN) if enabled else _colour("✗", _RED)
                print(f"  {status}  {name}")
        except Exception as exc:
            print(_colour(f"Could not list features: {exc}", _RED))

    def _list_backends(self) -> None:
        """Fetch and display registered backend connectors."""
        if self._client is None:
            print(_colour("Not connected.", _RED))
            return
        try:
            resp = self._client.get(
                f"{self._url}/api/v1/backends",
                headers={"X-Tenant-ID": self._tenant_id},
            )
            resp.raise_for_status()
            data = resp.json()
            backends = data.get("backends", [])
            types_ = data.get("registered_types", [])
            print(_colour(f"Registered backend types: {', '.join(types_)}", _CYAN))
            print(_colour(f"Active connectors ({len(backends)}):", _CYAN))
            for b in backends:
                status = b.get("status", "unknown")
                colour = _GREEN if status == "healthy" else _RED
                print(f"  {_colour(status, colour)}  {b.get('backend_id')}")
        except Exception as exc:
            print(_colour(f"Could not list backends: {exc}", _RED))

    @staticmethod
    def _print_help() -> None:
        """Print available metacommands."""
        print(_colour("Metacommands:", _CYAN))
        for cmd, desc in _SHELL_COMMANDS.items():
            print(f"  {_colour(cmd, _YELLOW):12s}  {desc}")

    @staticmethod
    def _print_table(
        columns: list[str], rows: list[dict[str, Any]]
    ) -> None:
        """Render query results as an ASCII table."""
        if not columns:
            print("(no columns)")
            return
        widths = [max(len(c), 4) for c in columns]
        for row in rows:
            for i, col in enumerate(columns):
                val_len = len(str(row.get(col, "")))
                if val_len > widths[i]:
                    widths[i] = val_len
        sep = "+" + "+".join("-" * (w + 2) for w in widths) + "+"
        header = "|" + "|".join(
            f" {c:<{widths[i]}} " for i, c in enumerate(columns)
        ) + "|"
        print(sep)
        print(header)
        print(sep)
        for row in rows[:500]:  # Cap at 500 rows for display
            line = "|" + "|".join(
                f" {str(row.get(c, '')):<{widths[i]}} " for i, c in enumerate(columns)
            ) + "|"
            print(line)
        print(sep)

    def _build_client(self) -> Optional[Any]:
        """Build an httpx sync client for API calls."""
        try:
            import httpx  # type: ignore[import]
            return httpx.Client(timeout=30)
        except ImportError:
            logger.warning("httpx not available; API calls from shell disabled")
            return None
