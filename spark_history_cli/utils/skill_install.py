"""Install the bundled Copilot skills into a user or repository skill directory."""

from __future__ import annotations

import shutil
from importlib.resources import as_file, files
from pathlib import Path


SKILLS = {
    "spark-history-cli": {
        "files": [("skills", "SKILL.md")],
    },
    "spark-advisor": {
        "files": [
            ("skills/spark-advisor", "SKILL.md"),
            ("skills/spark-advisor/references", "diagnostics.md"),
            ("skills/spark-advisor/references", "comparison.md"),
        ],
    },
}


def default_skill_target(scope: str) -> Path:
    """Return the default installation base for the given scope."""
    if scope == "repo":
        return Path.cwd() / ".github" / "skills"
    return Path.home() / ".copilot" / "skills"


def _install_one_skill(
    name: str, base_dir: Path, force: bool = False
) -> Path:
    """Install a single skill into base_dir/name."""
    destination = (base_dir / name).expanduser().resolve()

    if destination.exists():
        if not force:
            raise FileExistsError(
                f"Skill directory already exists at {destination}. Use --force to overwrite it."
            )
        shutil.rmtree(destination)

    spec = SKILLS[name]
    for pkg_subpath, filename in spec["files"]:
        source = files("spark_history_cli").joinpath(pkg_subpath, filename)
        # Determine relative output path within the skill directory
        rel = pkg_subpath.replace(f"skills/{name}/", "").replace(f"skills/{name}", "").replace("skills/", "").replace("skills", "")
        if name == "spark-history-cli":
            target_dir = destination
        else:
            target_dir = destination / rel if rel else destination
        target_dir.mkdir(parents=True, exist_ok=True)
        with as_file(source) as source_path:
            shutil.copy2(source_path, target_dir / filename)

    return destination


def install_copilot_skill(destination: Path, force: bool = False) -> Path:
    """Install the spark-history-cli skill (backward-compatible)."""
    # destination is already the full path including skill name
    base = destination.parent
    name = destination.name
    if name not in SKILLS:
        name = "spark-history-cli"
        base = destination.parent
    return _install_one_skill(name, base, force)


def install_all_skills(base_dir: Path, force: bool = False) -> list[Path]:
    """Install all bundled skills into base_dir."""
    installed = []
    for name in SKILLS:
        path = _install_one_skill(name, base_dir, force)
        installed.append(path)
    return installed
