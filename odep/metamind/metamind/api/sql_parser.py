"""SQL parser using sqlglot."""
from __future__ import annotations
import sqlglot
import sqlglot.expressions as exp
from typing import Optional

def parse_sql(sql: str, dialect: str = "postgres") -> exp.Expression:
    try:
        return sqlglot.parse_one(sql, dialect=dialect)
    except sqlglot.errors.ParseError as e:
        raise ValueError(f"SQL parse error: {e}") from e

def validate_sql(sql: str, dialect: str = "postgres") -> tuple[bool, Optional[str]]:
    try:
        parse_sql(sql, dialect)
        return True, None
    except ValueError as e:
        return False, str(e)
