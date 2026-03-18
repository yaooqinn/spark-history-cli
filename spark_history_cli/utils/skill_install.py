"""Install the bundled Copilot skill into a user or repository skill directory."""

from __future__ import annotations

import shutil
from importlib.resources import as_file, files
from pathlib import Path


SKILL_NAME = "spark-history-cli"


def default_skill_target(scope: str) -> Path:
    """Return the default installation target for the given scope."""
    if scope == "repo":
        return Path.cwd() / ".github" / "skills" / SKILL_NAME
    return Path.home() / ".copilot" / "skills" / SKILL_NAME


def install_copilot_skill(destination: Path, force: bool = False) -> Path:
    """Copy the packaged SKILL.md into a Copilot skill directory."""
    destination = Path(destination).expanduser().resolve()
    skill_md = destination / "SKILL.md"

    if destination.exists():
        if not force:
            raise FileExistsError(
                f"Skill directory already exists at {destination}. Use --force to overwrite it."
            )
        shutil.rmtree(destination)

    destination.mkdir(parents=True, exist_ok=True)

    source = files("spark_history_cli").joinpath("skills", "SKILL.md")
    with as_file(source) as source_path:
        shutil.copy2(source_path, skill_md)

    return destination
