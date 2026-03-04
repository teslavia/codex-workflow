from __future__ import annotations

import shlex
import subprocess
from pathlib import Path
from typing import Tuple


MARKER = "# codex-workflow-managed-pre-push"


def _resolve_git_paths(repo_root: Path) -> Tuple[Path, Path]:
    root = repo_root.resolve()
    completed = subprocess.run(
        ["git", "-C", str(root), "rev-parse", "--git-dir"],
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(f"{root} is not a git repository")

    git_dir_raw = completed.stdout.strip()
    git_dir = Path(git_dir_raw)
    if not git_dir.is_absolute():
        git_dir = (root / git_dir).resolve()
    return root, git_dir


def install_pre_push_hook(
    repo_root: Path,
    toolkit_root: Path,
    goal: str,
    force: bool = False,
    no_evolve: bool = True,
) -> Path:
    root, git_dir = _resolve_git_paths(repo_root)
    hooks_dir = git_dir / "hooks"
    hooks_dir.mkdir(parents=True, exist_ok=True)
    hook_path = hooks_dir / "pre-push"

    if hook_path.exists() and not force:
        current = hook_path.read_text(encoding="utf-8", errors="ignore")
        if MARKER not in current:
            raise RuntimeError(
                f"{hook_path} already exists and is not managed by codex-workflow; use --force to overwrite"
            )

    quoted_root = shlex.quote(str(root))
    quoted_toolkit = shlex.quote(str(toolkit_root.resolve()))
    default_goal = goal.replace('"', '\\"')
    no_evolve_arg = " --no-evolve" if no_evolve else ""

    script = (
        "#!/bin/sh\n"
        "set -eu\n"
        f"{MARKER}\n"
        f"REPO_ROOT={quoted_root}\n"
        f"TOOLKIT_ROOT={quoted_toolkit}\n"
        f"GOAL=\"${{CODEX_WORKFLOW_GOAL:-{default_goal}}}\"\n\n"
        "echo \"[codex-workflow] pre-push gate start: ${GOAL}\" >&2\n"
        "PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=\"${TOOLKIT_ROOT}\" "
        "python3 -m codex_workflow.cli run --target \"${REPO_ROOT}\" --goal \"${GOAL}\""
        f"{no_evolve_arg}\n"
        "status=$?\n"
        "if [ \"$status\" -ne 0 ]; then\n"
        "  echo \"[codex-workflow] pre-push gate failed; push blocked.\" >&2\n"
        "  exit \"$status\"\n"
        "fi\n"
        "echo \"[codex-workflow] pre-push gate passed.\" >&2\n"
        "exit 0\n"
    )

    hook_path.write_text(script, encoding="utf-8")
    hook_path.chmod(0o755)
    return hook_path
