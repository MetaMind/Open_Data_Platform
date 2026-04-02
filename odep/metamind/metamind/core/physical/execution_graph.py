"""
Execution Graph Engine - DAG-based Query Orchestration

File: metamind/core/physical/execution_graph.py
Role: Distributed Systems Engineer
Phase: 1
Dependencies: asyncio, networkx

Implements:
- DAG-based orchestration
- Partial plan dispatch
- Result stitching
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional, Callable, Set
from enum import Enum
from collections import defaultdict

import pyarrow as pa

from metamind.core.physical.result_stitcher import ResultStitcher

logger = logging.getLogger(__name__)


class TaskStatus(Enum):
    """Task execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class TaskType(Enum):
    """Task types."""
    SCAN = "scan"
    FILTER = "filter"
    JOIN = "join"
    AGGREGATE = "aggregate"
    SORT = "sort"
    LIMIT = "limit"
    UNION = "union"
    CUSTOM = "custom"


@dataclass
class ExecutionTask:
    """A task in the execution graph."""
    task_id: str
    task_type: TaskType
    engine: str  # oracle, trino, spark
    sql: str
    
    # Dependencies
    dependencies: List[str] = field(default_factory=list)
    
    # Execution
    status: TaskStatus = TaskStatus.PENDING
    result: Optional[pa.Table] = None
    error: Optional[str] = None
    execution_time_ms: int = 0
    
    # Metadata
    estimated_rows: int = 0
    estimated_cost: float = 0.0
    
    # Callbacks
    on_complete: Optional[Callable[["ExecutionTask"], None]] = None


@dataclass
class ExecutionGraph:
    """Execution graph containing all tasks."""
    graph_id: str
    tasks: Dict[str, ExecutionTask] = field(default_factory=dict)
    edges: Dict[str, List[str]] = field(default_factory=dict)  # task_id -> dependent task_ids
    
    def add_task(self, task: ExecutionTask) -> None:
        """Add a task to the graph."""
        self.tasks[task.task_id] = task
        self.edges[task.task_id] = []
    
    def add_dependency(self, from_task: str, to_task: str) -> None:
        """Add dependency edge (from_task must complete before to_task)."""
        if from_task in self.tasks and to_task in self.tasks:
            self.tasks[to_task].dependencies.append(from_task)
            self.edges[from_task].append(to_task)
    
    def get_ready_tasks(self) -> List[ExecutionTask]:
        """Get tasks that are ready to execute (all dependencies completed)."""
        ready = []
        for task in self.tasks.values():
            if task.status == TaskStatus.PENDING:
                deps_completed = all(
                    self.tasks[dep_id].status == TaskStatus.COMPLETED
                    for dep_id in task.dependencies
                )
                if deps_completed:
                    ready.append(task)
        return ready
    
    def get_execution_order(self) -> List[List[str]]:
        """
        Get tasks in topological order (batches of independent tasks).
        
        Returns:
            List of task ID batches that can execute in parallel
        """
        # Kahn's algorithm for topological sort
        in_degree = {task_id: len(task.dependencies) for task_id, task in self.tasks.items()}
        
        batches = []
        remaining = set(self.tasks.keys())
        
        while remaining:
            # Find all tasks with no remaining dependencies
            batch = [task_id for task_id in remaining if in_degree[task_id] == 0]
            
            if not batch:
                # Cycle detected
                raise ValueError("Cycle detected in execution graph")
            
            batches.append(batch)
            remaining -= set(batch)
            
            # Update in-degrees
            for task_id in batch:
                for dependent in self.edges[task_id]:
                    in_degree[dependent] -= 1
        
        return batches
    
    def is_complete(self) -> bool:
        """Check if all tasks are complete."""
        return all(
            task.status in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED)
            for task in self.tasks.values()
        )
    
    def get_final_result(self) -> Optional[pa.Table]:
        """Get the final result (from last task with no dependents)."""
        # Find tasks with no outgoing edges (final tasks)
        final_tasks = [
            task for task_id, task in self.tasks.items()
            if not self.edges[task_id] and task.status == TaskStatus.COMPLETED
        ]
        
        if not final_tasks:
            return None
        
        # Return result from last completed task
        return final_tasks[-1].result


class ExecutionGraphEngine:
    """
    DAG-based query execution engine.
    
    Orchestrates complex queries across multiple engines:
    - Breaks query into partial plans
    - Dispatches tasks to appropriate engines
    - Stitches results together
    """
    
    def __init__(
        self,
        oracle_connector: Any,
        trino_engine: Any,
        spark_engine: Any,
        max_parallel_tasks: int = 10
    ):
        """
        Initialize execution graph engine.
        
        Args:
            oracle_connector: Oracle connector
            trino_engine: Trino engine
            spark_engine: Spark engine
            max_parallel_tasks: Maximum parallel tasks
        """
        self.oracle = oracle_connector
        self.trino = trino_engine
        self.spark = spark_engine
        self.max_parallel_tasks = max_parallel_tasks
        self._semaphore = asyncio.Semaphore(max_parallel_tasks)
        logger.debug("ExecutionGraphEngine initialized")
    
    def build_execution_graph(
        self,
        logical_plan: Any,
        routing_decisions: Dict[str, str]
    ) -> ExecutionGraph:
        """
        Build execution graph from logical plan.
        
        Args:
            logical_plan: Root of logical plan tree
            routing_decisions: Map of table names to engines
            
        Returns:
            Execution graph
        """
        import uuid
        graph_id = str(uuid.uuid4())
        graph = ExecutionGraph(graph_id=graph_id)
        
        # Build graph recursively
        self._build_graph_recursive(
            logical_plan, graph, routing_decisions, parent_task=None
        )
        
        return graph
    
    def _build_graph_recursive(
        self,
        plan_node: Any,
        graph: ExecutionGraph,
        routing_decisions: Dict[str, str],
        parent_task: Optional[str] = None
    ) -> Optional[str]:
        """Build graph recursively from plan node."""
        import uuid
        
        task_id = f"task_{uuid.uuid4().hex[:8]}"
        
        # Determine task type and engine
        if plan_node.node_type.value == "scan":
            task_type = TaskType.SCAN
            engine = routing_decisions.get(plan_node.table_name, "trino")
            sql = f"SELECT * FROM {plan_node.table_name}"
        
        elif plan_node.node_type.value == "filter":
            task_type = TaskType.FILTER
            engine = parent_task and graph.tasks.get(parent_task, {}).engine or "trino"
            sql = f"SELECT * FROM ({parent_task}) WHERE {plan_node.filter_conditions[0]}"
        
        elif plan_node.node_type.value == "join":
            task_type = TaskType.JOIN
            engine = "trino"  # Joins typically go to Trino
            sql = "-- join will be constructed from children"
        
        elif plan_node.node_type.value == "aggregate":
            task_type = TaskType.AGGREGATE
            engine = "spark" if plan_node.estimated_rows > 1000000 else "trino"
            sql = "-- aggregate will be constructed"
        
        else:
            task_type = TaskType.CUSTOM
            engine = "trino"
            sql = "-- custom task"
        
        # Create task
        task = ExecutionTask(
            task_id=task_id,
            task_type=task_type,
            engine=engine,
            sql=sql,
            estimated_rows=plan_node.estimated_rows,
            estimated_cost=plan_node.estimated_cost
        )
        
        graph.add_task(task)
        
        # Add dependency from parent
        if parent_task:
            graph.add_dependency(parent_task, task_id)
        
        # Process children
        if hasattr(plan_node, 'left_child') and plan_node.left_child:
            left_task = self._build_graph_recursive(
                plan_node.left_child, graph, routing_decisions, task_id
            )
            if left_task and task_type == TaskType.JOIN:
                task.dependencies.append(left_task)
        
        if hasattr(plan_node, 'right_child') and plan_node.right_child:
            right_task = self._build_graph_recursive(
                plan_node.right_child, graph, routing_decisions, task_id
            )
            if right_task and task_type == TaskType.JOIN:
                task.dependencies.append(right_task)
        
        return task_id
    
    async def execute_graph(
        self,
        graph: ExecutionGraph,
        timeout_seconds: int = 3600
    ) -> pa.Table:
        """
        Execute the execution graph.
        
        Args:
            graph: Execution graph
            timeout_seconds: Execution timeout
            
        Returns:
            Final result as Arrow table
        """
        logger.info(f"Executing graph {graph.graph_id} with {len(graph.tasks)} tasks")
        
        start_time = asyncio.get_running_loop().time()
        
        try:
            async with asyncio.timeout(timeout_seconds):
                # Execute in topological order
                batches = graph.get_execution_order()
                
                for batch_num, batch in enumerate(batches):
                    logger.debug(f"Executing batch {batch_num + 1}/{len(batches)}: {batch}")
                    
                    # Execute batch in parallel
                    tasks = [
                        self._execute_task(graph.tasks[task_id])
                        for task_id in batch
                    ]
                    
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    # Check for failures
                    for task_id, result in zip(batch, results):
                        if isinstance(result, Exception):
                            graph.tasks[task_id].status = TaskStatus.FAILED
                            graph.tasks[task_id].error = str(result)
                            raise ExecutionGraphError(
                                f"Task {task_id} failed: {result}"
                            )
                
                execution_time = int(
                    (asyncio.get_running_loop().time() - start_time) * 1000
                )
                logger.info(f"Graph execution completed in {execution_time}ms")
                
                return graph.get_final_result()
                
        except asyncio.TimeoutError:
            raise ExecutionGraphError(f"Graph execution timed out after {timeout_seconds}s")
    
    async def _execute_task(self, task: ExecutionTask) -> None:
        """Execute a single task."""
        async with self._semaphore:
            task.status = TaskStatus.RUNNING
            start_time = asyncio.get_running_loop().time()
            
            try:
                # Route to appropriate engine
                if task.engine == "oracle":
                    result = await self._execute_on_oracle(task)
                elif task.engine == "spark":
                    result = await self._execute_on_spark(task)
                else:  # trino
                    result = await self._execute_on_trino(task)
                
                task.result = result
                task.status = TaskStatus.COMPLETED
                task.execution_time_ms = int(
                    (asyncio.get_running_loop().time() - start_time) * 1000
                )
                
                logger.debug(f"Task {task.task_id} completed in {task.execution_time_ms}ms")
                
                if task.on_complete:
                    task.on_complete(task)
                    
            except Exception as e:
                task.status = TaskStatus.FAILED
                task.error = str(e)
                logger.error(f"Task {task.task_id} failed: {e}")
                raise
    
    async def _execute_on_oracle(self, task: ExecutionTask) -> pa.Table:
        """Execute task on Oracle."""
        if not self.oracle:
            raise ExecutionGraphError("Oracle connector not available")
        
        result = await self.oracle.execute(
            task.sql,
            user_id="metamind_graph"
        )
        return result
    
    async def _execute_on_trino(self, task: ExecutionTask) -> pa.Table:
        """Execute task on Trino."""
        result = await self.trino.execute(task.sql)
        return result.data
    
    async def _execute_on_spark(self, task: ExecutionTask) -> pa.Table:
        """Execute task on Spark."""
        result = await self.spark.execute(task.sql)
        return result.data
    
    def stitch_results(
        self,
        results: List[pa.Table],
        stitch_type: str = "union",
        stitch_key: Optional[str] = None,
    ) -> pa.Table:
        """
        Combine partial results from multiple execution tasks.

        stitch_type values:
          "union"  — schema-unified concatenation (existing behaviour preserved)
          "join"   — PyArrow hash-join on stitch_key
          "merge"  — upsert deduplication on stitch_key, keeping latest row
        """
        if not results:
            return pa.Table.from_pydict({})

        if len(results) == 1:
            return results[0]

        if stitch_type == "union":
            try:
                return pa.concat_tables(results, promote_options="default")
            except Exception as exc:
                logger.error(
                    "ExecutionGraph.stitch_results union failed: %s — "
                    "attempting schema-coerce concat",
                    exc,
                )
                return pa.concat_tables(results, promote_options="permissive")

        if stitch_type in ("join", "merge"):
            if not stitch_key:
                logger.error(
                    "ExecutionGraph.stitch_results: stitch_type=%s requires stitch_key",
                    stitch_type,
                )
                return pa.concat_tables(results, promote_options="default")
            try:
                stitcher = ResultStitcher()
                if stitch_type == "join":
                    result = results[0]
                    for right in results[1:]:
                        result = stitcher.join(result, right, key=stitch_key)
                    return result
                else:  # merge
                    result = results[0]
                    for right in results[1:]:
                        result = stitcher.merge(result, right, dedup_key=stitch_key)
                    return result
            except Exception as exc:
                logger.error(
                    "ExecutionGraph.stitch_results %s failed key=%s: %s — "
                    "falling back to union",
                    stitch_type,
                    stitch_key,
                    exc,
                )
                return pa.concat_tables(results, promote_options="default")

        logger.error(
            "ExecutionGraph.stitch_results: unknown stitch_type=%s, using union",
            stitch_type,
        )
        return pa.concat_tables(results, promote_options="default")
    
    async def cancel_graph(self, graph: ExecutionGraph) -> None:
        """Cancel all running tasks in graph."""
        for task in graph.tasks.values():
            if task.status == TaskStatus.RUNNING:
                task.status = TaskStatus.CANCELLED
        
        logger.info(f"Cancelled graph {graph.graph_id}")


class ExecutionGraphError(Exception):
    """Execution graph error."""
    pass
