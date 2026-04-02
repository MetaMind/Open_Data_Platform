"""Local stack command group — manages Docker Compose dev stack."""
import subprocess
import sys

import click


@click.group()
def local():
    """Manage the local Docker Compose development stack."""
    pass


@local.command()
@click.option(
    "--profile",
    default="full",
    type=click.Choice(["full", "minimal"]),
    show_default=True,
    help="Stack profile to start.",
)
def up(profile: str) -> None:
    """Start the local Docker Compose development stack."""
    click.echo(f"🚀 Starting local stack (profile={profile})...")
    cmd = ["docker", "compose", "--profile", profile, "up", "-d"]
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        click.echo(f"Error starting local stack: {e}")
        sys.exit(1)


@local.command()
@click.option("--volumes", is_flag=True, default=False, help="Remove associated Docker volumes.")
def down(volumes: bool) -> None:
    """Stop the local Docker Compose development stack."""
    click.echo("🛑 Stopping local stack...")
    cmd = ["docker", "compose", "down"]
    if volumes:
        cmd.append("--volumes")
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        click.echo(f"Error stopping local stack: {e}")
        sys.exit(1)
