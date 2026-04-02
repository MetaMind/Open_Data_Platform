"""Config command group — manage ODEP configuration."""
import os
import sys

import click

from odep.config import OdepConfig


@click.group(name="config")
def config_group():
    """Manage ODEP configuration."""
    pass


@config_group.command(name="get")
@click.argument("key")
def config_get(key: str) -> None:
    """Read and print the current value of a config KEY (dot notation)."""
    config = OdepConfig()
    parts = key.split(".")
    try:
        value = config
        for part in parts:
            value = getattr(value, part)
        click.echo(value)
    except AttributeError:
        click.echo(f"❌ Unknown config key: {key}")
        sys.exit(1)


@config_group.command(name="set")
@click.argument("key_value")
def config_set(key_value: str) -> None:
    """Write KEY=VALUE to .odep.env (dot notation for KEY)."""
    if "=" not in key_value:
        click.echo("❌ Invalid format. Use KEY=VALUE (e.g. execution.default_engine=duckdb)")
        sys.exit(1)

    key, value = key_value.split("=", 1)
    parts = key.split(".", 1)
    if len(parts) == 2:
        section, field = parts
        env_var = f"ODEP_{section.upper()}__{field.upper()}"
    else:
        env_var = f"ODEP_{key.upper()}"

    env_file = ".odep.env"
    new_line = f"{env_var}={value}\n"

    try:
        lines = []
        updated = False
        if os.path.exists(env_file):
            with open(env_file, "r") as f:
                lines = f.readlines()
            for i, line in enumerate(lines):
                if line.startswith(f"{env_var}="):
                    lines[i] = new_line
                    updated = True
                    break
        if not updated:
            lines.append(new_line)
        with open(env_file, "w") as f:
            f.writelines(lines)
        click.echo(f"✅ Set {key} = {value}")
    except Exception as e:
        click.echo(f"❌ Error writing config: {e}")
        sys.exit(1)
