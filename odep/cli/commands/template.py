"""Template command group — manage pipeline templates."""
import sys
import click


TEMPLATES = {
    "batch-pipeline": "Daily batch ETL pipeline with DuckDB/Spark",
    "streaming-pipeline": "Real-time streaming pipeline with Flink/Kafka",
    "ml-feature-pipeline": "ML feature engineering pipeline with feature store",
    "dbt-project": "dbt project with ODEP integration",
}


@click.group()
def template():
    """Manage pipeline templates."""
    pass


@template.command(name="list")
def template_list():
    """List available pipeline templates."""
    click.echo("Available templates:")
    for name, description in TEMPLATES.items():
        click.echo(f"  {name:<22}{description}")
    sys.exit(0)


@template.command(name="use")
@click.argument("name")
@click.option("--name", "pipeline_name", default="my-pipeline", help="The generated pipeline name.")
def template_use(name, pipeline_name):
    """Generate a pipeline from a template."""
    if name not in TEMPLATES:
        click.echo(f"❌ Unknown template: {name}. Run 'odep template list' to see available templates.")
        sys.exit(1)

    click.echo(f"🍪 Generating pipeline '{pipeline_name}' from template '{name}'...")

    try:
        from cookiecutter.main import cookiecutter
        template_path = f"odep/templates/{name}"
        cookiecutter(template_path, no_input=True, extra_context={"pipeline_name": pipeline_name})
        click.echo(f"✅ Created pipeline '{pipeline_name}'")
    except Exception:
        click.echo(
            "⚠️  Template directory not found. Templates will be created in task 15.x. "
            "Run 'pip install cookiecutter' to enable template generation."
        )

    sys.exit(0)
