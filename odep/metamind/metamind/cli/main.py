#!/usr/bin/env python3
"""
MetaMind CLI - Command Line Interface

Entry point for MetaMind commands.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from metamind.bootstrap import bootstrap, get_context
from metamind.config.settings import get_settings

app = typer.Typer(help="MetaMind Enterprise Query Intelligence Platform")
console = Console()
logger = logging.getLogger(__name__)


@app.command()
def shell():
    """Start interactive shell."""
    console.print("[bold green]MetaMind Interactive Shell[/bold green]")
    console.print("Type 'exit' to quit\n")
    
    asyncio.run(_shell_async())


async def _shell_async():
    """Async shell implementation."""
    ctx = await bootstrap()
    
    try:
        while True:
            query = console.input("[bold blue]sql>[/bold blue] ")
            
            if query.lower() in ("exit", "quit"):
                break
            
            if not query.strip():
                continue
            
            try:
                decision = await ctx.query_router.route(
                    sql=query,
                    tenant_id="default",
                    user_context={}
                )
                
                console.print(f"[green]Routed to:[/green] {decision.target_source}")
                console.print(f"[green]Strategy:[/green] {decision.execution_strategy.value}")
                console.print(f"[green]Estimated cost:[/green] {decision.estimated_cost_ms:.1f}ms")
                console.print(f"[green]Reason:[/green] {decision.reason}\n")
                
            except Exception as e:
                console.print(f"[red]Error:[/red] {e}\n")
    
    finally:
        await ctx.close()


@app.command()
def health():
    """Check system health."""
    asyncio.run(_health_async())


async def _health_async():
    """Async health check."""
    ctx = await bootstrap()
    
    try:
        health = await ctx.health_check()
        
        table = Table(title="MetaMind Health Status")
        table.add_column("Component", style="cyan")
        table.add_column("Status", style="green")
        
        table.add_row("Overall", health["status"])
        for component, status in health["checks"].items():
            status_str = "✓" if status else "✗"
            table.add_row(component, status_str)
        
        console.print(table)
    
    finally:
        await ctx.close()


@app.command()
def cdc_status(
    tenant_id: str = typer.Option("default", "--tenant", "-t")
):
    """Show CDC replication status."""
    asyncio.run(_cdc_status_async(tenant_id))


async def _cdc_status_async(tenant_id: str):
    """Async CDC status."""
    ctx = await bootstrap()
    
    try:
        summary = ctx.cdc_monitor.get_health_summary(tenant_id)
        
        table = Table(title=f"CDC Status - {tenant_id}")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green")
        
        table.add_row("Total Tables", str(summary["total_tables"]))
        table.add_row("Healthy", str(summary["healthy"]))
        table.add_row("Warning", str(summary["warning"]))
        table.add_row("Critical", str(summary["critical"]))
        table.add_row("Max Lag", f"{summary['max_lag_seconds']}s")
        table.add_row("Overall", summary["overall_status"])
        
        console.print(table)
        
        if summary["lagging_tables"]:
            console.print("\n[yellow]Lagging Tables:[/yellow]")
            for t in summary["lagging_tables"]:
                console.print(f"  - {t['table']}: {t['lag_seconds']}s")
    
    finally:
        await ctx.close()


@app.command()
def cache_stats():
    """Show cache statistics."""
    asyncio.run(_cache_stats_async())


async def _cache_stats_async():
    """Async cache stats."""
    ctx = await bootstrap()
    
    try:
        stats = ctx.cache_manager.get_stats()
        
        table = Table(title="Cache Statistics")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green")
        
        for key, value in stats.items():
            table.add_row(key.replace("_", " ").title(), str(value))
        
        console.print(table)
    
    finally:
        await ctx.close()


def main():
    """Main entry point."""
    logging.basicConfig(level=logging.INFO)
    app()


if __name__ == "__main__":
    main()
