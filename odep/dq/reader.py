"""Universal data reader for the ODEP DQ engine.

Reads data from any source format into a pandas DataFrame for rule evaluation.

Supported formats:
  File-based:   CSV, Parquet, ORC, Avro, JSON, JSONL, TSV, Excel, Feather, Delta
  Query-based:  SQL via DuckDB (reads local files + in-memory tables)
  Remote:       S3, GCS, Azure Blob (via fsspec / DuckDB httpfs)
  Engines:      Spark DataFrame, Trino query result, DuckDB relation

Install extras:
  pip install pyarrow          # Parquet, ORC, Feather, Avro
  pip install fastavro         # Avro (alternative)
  pip install openpyxl         # Excel (.xlsx)
  pip install deltalake        # Delta Lake
  pip install fsspec s3fs      # S3 remote reads
  pip install gcsfs            # GCS remote reads
  pip install adlfs            # Azure Blob remote reads
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Union


def read_data(
    source: Union[str, Any],
    format: Optional[str] = None,
    options: Optional[Dict[str, Any]] = None,
    sample_rows: Optional[int] = None,
) -> "Any":
    """Read data from any supported source into a pandas DataFrame.

    Args:
        source: One of:
            - File path string: "orders.parquet", "data/events.avro", "s3://bucket/file.csv"
            - SQL query string: "SELECT * FROM orders LIMIT 1000"
            - pandas DataFrame (pass-through)
            - PySpark DataFrame (converted via toPandas())
            - DuckDB relation (converted via .df())
            - list of dicts
        format: Optional format hint. Auto-detected from file extension if omitted.
                Values: csv, parquet, orc, avro, json, jsonl, tsv, excel, feather, delta, sql
        options: Format-specific options passed to the reader (e.g. {"sep": "|"} for CSV)
        sample_rows: If set, only read this many rows (for large files / LLM prompts)

    Returns:
        pandas DataFrame

    Raises:
        ValueError: if the format is unsupported or the source cannot be read
    """
    import pandas as pd

    opts = options or {}

    # Pass-through for already-loaded data
    if isinstance(source, pd.DataFrame):
        return source.head(sample_rows) if sample_rows else source

    # PySpark DataFrame
    try:
        from pyspark.sql import DataFrame as SparkDF
        if isinstance(source, SparkDF):
            df = source.toPandas()
            return df.head(sample_rows) if sample_rows else df
    except ImportError:
        pass

    # DuckDB relation
    try:
        import duckdb
        if isinstance(source, duckdb.DuckDBPyRelation):
            df = source.df()
            return df.head(sample_rows) if sample_rows else df
    except (ImportError, AttributeError):
        pass

    # List of dicts
    if isinstance(source, list):
        df = pd.DataFrame(source)
        return df.head(sample_rows) if sample_rows else df

    # String source — file path or SQL query
    if not isinstance(source, str):
        raise TypeError(f"Unsupported source type: {type(source)}")

    source_str = source.strip()

    # Detect if it's a SQL query (not a file path)
    if _is_sql_query(source_str) and not Path(source_str).exists():
        return _read_sql(source_str, sample_rows)

    # File-based read
    path = Path(source_str)
    detected_format = format or _detect_format(source_str)

    if detected_format == "csv":
        df = pd.read_csv(source_str, **opts)
    elif detected_format == "tsv":
        df = pd.read_csv(source_str, sep="\t", **opts)
    elif detected_format == "parquet":
        df = pd.read_parquet(source_str, **opts)
    elif detected_format == "orc":
        df = _read_orc(source_str, opts)
    elif detected_format == "avro":
        df = _read_avro(source_str, opts)
    elif detected_format in ("json", "jsonl"):
        df = _read_json(source_str, detected_format, opts)
    elif detected_format in ("excel", "xlsx", "xls"):
        df = pd.read_excel(source_str, **opts)
    elif detected_format == "feather":
        df = pd.read_feather(source_str, **opts)
    elif detected_format == "delta":
        df = _read_delta(source_str, opts)
    elif detected_format == "sql":
        # Treat source as a SQL query
        df = _read_sql(source_str, sample_rows)
        return df  # already limited
    else:
        # Fall back to DuckDB auto-detection (handles many formats)
        df = _read_via_duckdb(source_str, opts)

    return df.head(sample_rows) if sample_rows else df


def read_data_spark(
    source: str,
    format: Optional[str] = None,
    options: Optional[Dict[str, Any]] = None,
    spark=None,
) -> "Any":
    """Read data using PySpark and return a Spark DataFrame.

    Useful for large-scale DQ evaluation via SparkQualityEngine.

    Args:
        source: File path (local or S3/GCS/ADLS) or table name
        format: parquet, orc, avro, csv, json, delta, iceberg, hudi
        options: Spark reader options
        spark: SparkSession (creates one if None)

    Returns:
        PySpark DataFrame
    """
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        raise RuntimeError("pyspark not installed. Run: pip install pyspark")

    if spark is None:
        spark = SparkSession.builder.getOrCreate()

    opts = options or {}
    detected_format = format or _detect_format(source)

    reader = spark.read.options(**opts)

    if detected_format == "parquet":
        return reader.parquet(source)
    elif detected_format == "orc":
        return reader.orc(source)
    elif detected_format == "avro":
        return reader.format("avro").load(source)
    elif detected_format == "csv":
        return reader.option("header", "true").option("inferSchema", "true").csv(source)
    elif detected_format == "json":
        return reader.json(source)
    elif detected_format == "delta":
        return reader.format("delta").load(source)
    elif detected_format == "iceberg":
        return spark.table(source)  # Iceberg tables are accessed by name
    elif detected_format == "hudi":
        return reader.format("hudi").load(source)
    else:
        return reader.format(detected_format).load(source)


def read_data_trino(
    sql: str,
    host: str = "localhost",
    port: int = 8082,
    user: str = "odep",
    catalog: str = "tpch",
    schema: str = "tiny",
) -> "Any":
    """Execute a SQL query on Trino and return a pandas DataFrame.

    Args:
        sql: SQL query to execute
        host, port, user, catalog, schema: Trino connection parameters

    Returns:
        pandas DataFrame
    """
    try:
        import trino
        import pandas as pd
    except ImportError:
        raise RuntimeError("trino not installed. Run: pip install trino")

    conn = trino.dbapi.connect(host=host, port=port, user=user, catalog=catalog, schema=schema)
    cursor = conn.cursor()
    cursor.execute(sql)
    columns = [desc[0] for desc in cursor.description] if cursor.description else []
    rows = cursor.fetchall()
    return pd.DataFrame(rows, columns=columns)


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _detect_format(source: str) -> str:
    """Detect file format from extension or URL pattern."""
    s = source.lower().split("?")[0]  # strip query params from URLs
    if s.endswith(".csv"):
        return "csv"
    elif s.endswith(".tsv") or s.endswith(".tab"):
        return "tsv"
    elif s.endswith(".parquet") or s.endswith(".pq"):
        return "parquet"
    elif s.endswith(".orc"):
        return "orc"
    elif s.endswith(".avro"):
        return "avro"
    elif s.endswith(".json"):
        return "json"
    elif s.endswith(".jsonl") or s.endswith(".ndjson"):
        return "jsonl"
    elif s.endswith(".xlsx") or s.endswith(".xls"):
        return "excel"
    elif s.endswith(".feather") or s.endswith(".arrow"):
        return "feather"
    elif "/_delta_log" in s or s.endswith(".delta"):
        return "delta"
    elif s.endswith(".txt"):
        return "csv"  # treat plain text as CSV
    else:
        return "auto"


def _is_sql_query(source: str) -> bool:
    """Heuristic: does this string look like a SQL query rather than a file path?"""
    upper = source.upper().strip()
    sql_keywords = ("SELECT ", "WITH ", "SHOW ", "DESCRIBE ", "EXPLAIN ")
    return any(upper.startswith(kw) for kw in sql_keywords)


def _read_sql(sql: str, sample_rows: Optional[int]) -> "Any":
    """Execute SQL via DuckDB and return a pandas DataFrame."""
    try:
        import duckdb
        conn = duckdb.connect()
        # Install httpfs for remote reads if needed
        if "s3://" in sql or "gs://" in sql or "az://" in sql:
            try:
                conn.execute("INSTALL httpfs; LOAD httpfs;")
            except Exception:
                pass
        if sample_rows:
            # Wrap in a LIMIT if not already present
            if "LIMIT" not in sql.upper():
                sql = f"SELECT * FROM ({sql}) _q LIMIT {sample_rows}"
        return conn.execute(sql).df()
    except ImportError:
        raise RuntimeError("duckdb not installed. Run: pip install duckdb")


def _read_orc(source: str, opts: Dict[str, Any]) -> "Any":
    """Read ORC file via pyarrow."""
    try:
        import pyarrow.orc as orc
        table = orc.read_table(source)
        return table.to_pandas()
    except ImportError:
        raise RuntimeError("pyarrow not installed. Run: pip install pyarrow")


def _read_avro(source: str, opts: Dict[str, Any]) -> "Any":
    """Read Avro file via fastavro or pyarrow."""
    # Try fastavro first (lighter dependency)
    try:
        import fastavro
        import pandas as pd
        with open(source, "rb") as f:
            reader = fastavro.reader(f)
            records = list(reader)
        return pd.DataFrame(records)
    except ImportError:
        pass
    # Fall back to pyarrow
    try:
        import pyarrow.dataset as ds
        dataset = ds.dataset(source, format="avro")
        return dataset.to_table().to_pandas()
    except ImportError:
        raise RuntimeError("fastavro or pyarrow not installed. Run: pip install fastavro")


def _read_json(source: str, fmt: str, opts: Dict[str, Any]) -> "Any":
    """Read JSON or JSONL file."""
    import pandas as pd
    if fmt == "jsonl":
        return pd.read_json(source, lines=True, **opts)
    return pd.read_json(source, **opts)


def _read_delta(source: str, opts: Dict[str, Any]) -> "Any":
    """Read Delta Lake table via deltalake."""
    try:
        from deltalake import DeltaTable
        dt = DeltaTable(source)
        return dt.to_pandas()
    except ImportError:
        raise RuntimeError("deltalake not installed. Run: pip install deltalake")


def _read_via_duckdb(source: str, opts: Dict[str, Any]) -> "Any":
    """Use DuckDB's auto-detection to read the file."""
    try:
        import duckdb
        conn = duckdb.connect()
        if "s3://" in source or "gs://" in source:
            try:
                conn.execute("INSTALL httpfs; LOAD httpfs;")
            except Exception:
                pass
        return conn.execute(f"SELECT * FROM '{source}'").df()
    except ImportError:
        raise RuntimeError("duckdb not installed. Run: pip install duckdb")
    except Exception as e:
        raise ValueError(f"Could not read {source!r}: {e}")
