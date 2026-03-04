from __future__ import annotations

import platform
from pathlib import Path
from typing import Dict

from .utils import dump_json, ensure_dir


def _default_profile(project_name: str) -> Dict[str, object]:
    return {
        "project_name": project_name,
        "language_stack": ["C++20", "C11 ABI", "Python"],
        "primary_build_system": "cmake",
        "quality_focus": [
            "build_stability",
            "test_regression_control",
            "api_abi_safety",
            "minimal_diff",
        ],
        "notes": "Keep diffs minimal and always close work with build + targeted tests.",
    }


def _default_quality_gates() -> Dict[str, object]:
    build_parallel = "$(sysctl -n hw.ncpu)" if platform.system() == "Darwin" else "$(nproc)"
    return {
        "required": [
            {
                "name": "configure",
                "command": "cmake -B build -DCMAKE_BUILD_TYPE=Release",
            },
            {
                "name": "build",
                "command": f"cmake --build build -j{build_parallel}",
            },
            {
                "name": "tests",
                "command": "ctest --test-dir build --output-on-failure -j4",
            },
        ],
        "optional": [
            {
                "name": "python_api",
                "command": "python3 -m pytest user/api -q",
            }
        ],
    }


def _default_workflow() -> Dict[str, object]:
    return {
        "version": 1,
        "codex": {
            "enabled": False,
            "command": "codex exec --skip-git-repo-check -o {output_file} - < {prompt_file}",
            "cwd": "{repo_root}",
        },
        "stages": [
            {
                "id": "crew_orchestrate",
                "kind": "crewai",
                "description": "Default CrewAI orchestration (planner/coder/tester/reviewer)",
                "continue_on_error": True,
                "enabled": True,
                "prompt_template": (
                    "任务目标: {{goal}}\n"
                    "项目画像: {{project_profile}}\n"
                    "质量门禁: {{quality_gates}}\n"
                    "近期经验: {{recent_lessons}}\n"
                    "输出要求: 先给最小变更方案，再给执行与验证结论。"
                ),
            },
            {
                "id": "verify",
                "kind": "shell",
                "description": "Run required quality gates",
                "command_source": "quality_gates.required",
                "commands": [],
            },
        ],
    }


def _default_evolution_policy() -> Dict[str, object]:
    return {
        "lookback_runs": 30,
        "min_failure_rate_to_promote": 0.2,
        "max_lessons_in_prompt": 6,
    }


def bootstrap(target: Path, project_name: str, force: bool = False) -> Dict[str, str]:
    repo_root = target.resolve()
    wf_root = repo_root / ".codex-workflow"
    ensure_dir(wf_root)
    ensure_dir(wf_root / "runs")
    ensure_dir(wf_root / "memory")

    outputs = {
        "project_profile": str(wf_root / "project_profile.json"),
        "quality_gates": str(wf_root / "quality_gates.json"),
        "workflow": str(wf_root / "workflow.json"),
        "evolution": str(wf_root / "evolution.json"),
        "playbook": str(wf_root / "memory" / "playbook.md"),
        "lessons": str(wf_root / "memory" / "lessons.jsonl"),
    }

    def write_if_needed(path: Path, payload: Dict[str, object]) -> None:
        if path.exists() and not force:
            return
        dump_json(path, payload)

    write_if_needed(wf_root / "project_profile.json", _default_profile(project_name))
    write_if_needed(wf_root / "quality_gates.json", _default_quality_gates())
    write_if_needed(wf_root / "workflow.json", _default_workflow())
    write_if_needed(wf_root / "evolution.json", _default_evolution_policy())

    playbook_path = wf_root / "memory" / "playbook.md"
    if force or not playbook_path.exists():
        playbook_path.write_text(
            "# Codex Workflow Playbook\n\n"
            "该文件由 `codex-workflow evolve` 自动更新。\n\n"
            "## Stable Rules\n"
            "- 编码阶段后必须执行最小必要验证。\n"
            "- 失败优先修复根因，避免绕过测试。\n",
            encoding="utf-8",
        )

    lessons_path = wf_root / "memory" / "lessons.jsonl"
    if force or not lessons_path.exists():
        lessons_path.write_text("", encoding="utf-8")

    return outputs
