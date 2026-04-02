"""
Metadata Catalog - Table and Column Registry

File: metamind/core/metadata/catalog.py
Role: Data Engineer
Phase: 1
Dependencies: SQLAlchemy

Manages table and column metadata for query optimization.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional, List, Dict, Any

from sqlalchemy import Engine, text

logger = logging.getLogger(__name__)


@dataclass
class TableMetadata:
    """Table metadata."""
    table_id: str
    tenant_id: str
    source_id: str
    source_type: str
    schema_name: str
    table_name: str
    full_name: str
    row_count: Optional[int]
    size_bytes: Optional[int]
    is_partitioned: bool
    partition_columns: List[str]


@dataclass
class ColumnMetadata:
    """Column metadata."""
    column_id: str
    table_id: str
    column_name: str
    data_type: str
    is_nullable: bool
    is_primary_key: bool


class MetadataCatalog:
    """
    Table and column registry for query optimization.
    
    Provides metadata lookup for the query router and optimizer.
    """
    
    def __init__(self, engine: Engine):
        """
        Initialize metadata catalog.
        
        Args:
            engine: SQLAlchemy engine for metadata database
        """
        self.engine = engine
        logger.debug("MetadataCatalog initialized")
    
    def get_table(
        self,
        table_name: str,
        tenant_id: str = "default"
    ) -> Optional[TableMetadata]:
        """
        Get table metadata by name.
        
        Args:
            table_name: Table name (can include schema)
            tenant_id: Tenant identifier
            
        Returns:
            TableMetadata or None
        """
        # Parse table name
        parts = table_name.split(".")
        if len(parts) == 2:
            schema_name, table = parts
        else:
            schema_name = "public"
            table = parts[0]
        
        with self.engine.connect() as conn:
            result = conn.execute(
                text("""
                SELECT table_id, tenant_id, source_id, source_type,
                       schema_name, table_name, full_name,
                       row_count, size_bytes, is_partitioned, partition_columns
                FROM mm_tables
                WHERE tenant_id = :tenant_id
                AND schema_name = :schema
                AND table_name = :table
                """),
                {
                    "tenant_id": tenant_id,
                    "schema": schema_name,
                    "table": table
                }
            ).fetchone()
            
            if not result:
                return None
            
            return TableMetadata(
                table_id=str(result.table_id),
                tenant_id=result.tenant_id,
                source_id=result.source_id,
                source_type=result.source_type,
                schema_name=result.schema_name,
                table_name=result.table_name,
                full_name=result.full_name,
                row_count=result.row_count,
                size_bytes=result.size_bytes,
                is_partitioned=result.is_partitioned or False,
                partition_columns=result.partition_columns or []
            )
    
    def get_table_columns(
        self,
        table_id: str
    ) -> List[ColumnMetadata]:
        """
        Get columns for a table.
        
        Args:
            table_id: Table identifier
            
        Returns:
            List of ColumnMetadata
        """
        with self.engine.connect() as conn:
            results = conn.execute(
                text("""
                SELECT column_id, table_id, column_name, data_type,
                       is_nullable, is_primary_key
                FROM mm_columns
                WHERE table_id = :table_id
                ORDER BY ordinal_position
                """),
                {"table_id": table_id}
            ).fetchall()
            
            return [
                ColumnMetadata(
                    column_id=str(r.column_id),
                    table_id=str(r.table_id),
                    column_name=r.column_name,
                    data_type=r.data_type,
                    is_nullable=r.is_nullable or True,
                    is_primary_key=r.is_primary_key or False
                )
                for r in results
            ]
    
    def get_tables_by_source(
        self,
        source_id: str,
        tenant_id: str = "default"
    ) -> List[TableMetadata]:
        """
        Get all tables for a source.
        
        Args:
            source_id: Source identifier
            tenant_id: Tenant identifier
            
        Returns:
            List of TableMetadata
        """
        with self.engine.connect() as conn:
            results = conn.execute(
                text("""
                SELECT table_id, tenant_id, source_id, source_type,
                       schema_name, table_name, full_name,
                       row_count, size_bytes, is_partitioned, partition_columns
                FROM mm_tables
                WHERE tenant_id = :tenant_id
                AND source_id = :source_id
                ORDER BY table_name
                """),
                {
                    "tenant_id": tenant_id,
                    "source_id": source_id
                }
            ).fetchall()
            
            return [
                TableMetadata(
                    table_id=str(r.table_id),
                    tenant_id=r.tenant_id,
                    source_id=r.source_id,
                    source_type=r.source_type,
                    schema_name=r.schema_name,
                    table_name=r.table_name,
                    full_name=r.full_name,
                    row_count=r.row_count,
                    size_bytes=r.size_bytes,
                    is_partitioned=r.is_partitioned or False,
                    partition_columns=r.partition_columns or []
                )
                for r in results
            ]
    
    def register_table(
        self,
        tenant_id: str,
        source_id: str,
        source_type: str,
        schema_name: str,
        table_name: str,
        row_count: Optional[int] = None,
        size_bytes: Optional[int] = None,
        is_partitioned: bool = False,
        partition_columns: Optional[List[str]] = None
    ) -> str:
        """
        Register a new table.
        
        Args:
            tenant_id: Tenant identifier
            source_id: Source identifier
            source_type: Source type
            schema_name: Schema name
            table_name: Table name
            row_count: Optional row count
            size_bytes: Optional size in bytes
            is_partitioned: Whether table is partitioned
            partition_columns: Partition column names
            
        Returns:
            Table ID
        """
        with self.engine.connect() as conn:
            result = conn.execute(
                text("""
                INSERT INTO mm_tables
                (tenant_id, source_id, source_type, schema_name, table_name,
                 row_count, size_bytes, is_partitioned, partition_columns, updated_at)
                VALUES (:tenant_id, :source_id, :source_type, :schema, :table,
                        :row_count, :size_bytes, :is_partitioned, :partition_columns, NOW())
                ON CONFLICT (tenant_id, source_id, schema_name, table_name)
                DO UPDATE SET
                    row_count = EXCLUDED.row_count,
                    size_bytes = EXCLUDED.size_bytes,
                    is_partitioned = EXCLUDED.is_partitioned,
                    partition_columns = EXCLUDED.partition_columns,
                    updated_at = NOW()
                RETURNING table_id
                """),
                {
                    "tenant_id": tenant_id,
                    "source_id": source_id,
                    "source_type": source_type,
                    "schema": schema_name,
                    "table": table_name,
                    "row_count": row_count,
                    "size_bytes": size_bytes,
                    "is_partitioned": is_partitioned,
                    "partition_columns": partition_columns or []
                }
            )
            conn.commit()
            
            table_id = result.fetchone()[0]
            logger.debug(f"Registered table {schema_name}.{table_name}: {table_id}")
            
            return str(table_id)
    
    def update_statistics(
        self,
        table_id: str,
        row_count: Optional[int] = None,
        size_bytes: Optional[int] = None
    ) -> None:
        """
        Update table statistics.
        
        Args:
            table_id: Table identifier
            row_count: New row count
            size_bytes: New size in bytes
        """
        with self.engine.connect() as conn:
            conn.execute(
                text("""
                UPDATE mm_tables
                SET row_count = COALESCE(:row_count, row_count),
                    size_bytes = COALESCE(:size_bytes, size_bytes),
                    last_analyzed = NOW(),
                    updated_at = NOW()
                WHERE table_id = :table_id
                """),
                {
                    "table_id": table_id,
                    "row_count": row_count,
                    "size_bytes": size_bytes
                }
            )
            conn.commit()
            
            logger.debug(f"Updated statistics for table {table_id}")
    
    def search_tables(
        self,
        pattern: str,
        tenant_id: str = "default",
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Search tables by name pattern.
        
        Args:
            pattern: Search pattern
            tenant_id: Tenant identifier
            limit: Max results
            
        Returns:
            List of matching tables
        """
        with self.engine.connect() as conn:
            results = conn.execute(
                text("""
                SELECT table_id, source_id, schema_name, table_name, full_name,
                       row_count, size_bytes
                FROM mm_tables
                WHERE tenant_id = :tenant_id
                AND (table_name ILIKE :pattern OR full_name ILIKE :pattern)
                ORDER BY table_name
                LIMIT :limit
                """),
                {
                    "tenant_id": tenant_id,
                    "pattern": f"%{pattern}%",
                    "limit": limit
                }
            ).fetchall()
            
            return [
                {
                    "table_id": str(r.table_id),
                    "source_id": r.source_id,
                    "schema_name": r.schema_name,
                    "table_name": r.table_name,
                    "full_name": r.full_name,
                    "row_count": r.row_count,
                    "size_bytes": r.size_bytes
                }
                for r in results
            ]
