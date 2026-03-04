from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple

from .bootstrap import bootstrap
from .evolution import evolve
from .runner import run_workflow
from .utils import append_jsonl, dump_json, load_json


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _detect_repo_mode(repo_root: Path) -> str:
    if (repo_root / "CMakeLists.txt").exists():
        return "cmake"
    if (
        (repo_root / "pyproject.toml").exists()
        or (repo_root / "setup.py").exists()
        or (repo_root / "requirements.txt").exists()
    ):
        return "python"
    return "generic"


def _recommended_quality_gates(repo_root: Path) -> Dict[str, object]:
    mode = _detect_repo_mode(repo_root)
    if mode == "cmake":
        return {
            "required": [
                {"name": "configure", "command": "cmake -B build -DCMAKE_BUILD_TYPE=Release"},
                {"name": "build", "command": "cmake --build build -j$(sysctl -n hw.ncpu || nproc)"},
                {"name": "tests", "command": "ctest --test-dir build --output-on-failure -j4"},
            ],
            "optional": [],
        }
    if mode == "python":
        return {
            "required": [
                {
                    "name": "py_compile",
                    "command": "python3 -m compileall -q -x '(^|/)(\\.venv|\\.git|build|dist|__pycache__)(/|$)' .",
                }
            ],
            "optional": [{"name": "pytest", "command": "python3 -m pytest -q"}],
        }
    return {"required": [{"name": "sanity", "command": "echo 'No default quality gate configured'"}], "optional": []}


def _ensure_initialized(repo_root: Path, project_name: str) -> None:
    wf_root = repo_root / ".codex-workflow"
    if not (wf_root / "workflow.json").exists():
        bootstrap(target=repo_root, project_name=project_name, force=False)


def _normalize_quality_gates(repo_root: Path) -> List[str]:
    actions: List[str] = []
    wf_root = repo_root / ".codex-workflow"
    quality_path = wf_root / "quality_gates.json"
    current = load_json(quality_path)
    recommended = _recommended_quality_gates(repo_root)

    current_required = [item.get("name") for item in current.get("required", []) if isinstance(item, dict)]
    rec_required = [item.get("name") for item in recommended.get("required", []) if isinstance(item, dict)]

    if current_required != rec_required:
        dump_json(quality_path, recommended)
        actions.append("aligned quality_gates with repository mode")
    return actions


def _ensure_crewai_stage(workflow: Dict[str, object]) -> Tuple[Dict[str, object], List[str]]:
    actions: List[str] = []
    stages = workflow.get("stages", [])
    if not isinstance(stages, list):
        return workflow, actions

    crew_index = -1
    verify_index = -1
    for idx, stage in enumerate(stages):
        if not isinstance(stage, dict):
            continue
        if stage.get("kind") == "crewai":
            crew_index = idx
        if stage.get("id") == "verify":
            verify_index = idx

    if crew_index == -1:
        stages.insert(
            0,
            {
                "id": "crew_orchestrate",
                "kind": "crewai",
                "description": "Default CrewAI orchestration (planner/coder/tester/reviewer)",
                "continue_on_error": True,
                "prompt_template": (
                    "任务目标: {{goal}}\\n"
                    "项目画像: {{project_profile}}\\n"
                    "质量门禁: {{quality_gates}}\\n"
                    "近期经验: {{recent_lessons}}\\n"
                    "输出要求: 先给最小变更方案，再给执行与验证结论。"
                ),
            },
        )
        actions.append("inserted missing crew_orchestrate stage")
        crew_index = 0

    if crew_index != -1:
        crew_stage = stages[crew_index]
        if isinstance(crew_stage, dict) and not bool(crew_stage.get("continue_on_error", False)):
            crew_stage["continue_on_error"] = True
            actions.append("set crew_orchestrate as non-blocking (continue_on_error=true)")

    if verify_index != -1 and crew_index > verify_index:
        crew_stage = stages.pop(crew_index)
        stages.insert(0, crew_stage)
        actions.append("moved crew_orchestrate before verify")

    workflow["stages"] = stages
    return workflow, actions


def _ensure_verify_stage(workflow: Dict[str, object]) -> Tuple[Dict[str, object], List[str]]:
    actions: List[str] = []
    stages = workflow.get("stages", [])
    if not isinstance(stages, list):
        return workflow, actions

    has_verify = any(isinstance(stage, dict) and stage.get("id") == "verify" for stage in stages)
    if not has_verify:
        stages.append(
            {
                "id": "verify",
                "kind": "shell",
                "description": "Run required quality gates",
                "command_source": "quality_gates.required",
                "commands": [],
            }
        )
        actions.append("inserted missing verify stage")

    workflow["stages"] = stages
    return workflow, actions


def _normalize_codex_runtime(workflow: Dict[str, object]) -> Tuple[Dict[str, object], List[str]]:
    actions: List[str] = []
    codex_cfg = workflow.get("codex")
    if not isinstance(codex_cfg, dict):
        codex_cfg = {}
        workflow["codex"] = codex_cfg
        actions.append("inserted missing codex runtime config")

    command = codex_cfg.get("command")
    target_cmd = "codex exec --skip-git-repo-check - < {prompt_file}"
    if not isinstance(command, str) or not command.strip():
        codex_cfg["command"] = target_cmd
        actions.append("set default codex.command for stdin prompt mode")
    elif "--prompt-file" in command or "codex exec - < {prompt_file}" in command:
        codex_cfg["command"] = target_cmd
        actions.append("migrated legacy codex.command to stdin mode with skip-git-repo-check")

    cwd = codex_cfg.get("cwd")
    if not isinstance(cwd, str) or not cwd.strip():
        codex_cfg["cwd"] = "{repo_root}"
        actions.append("set default codex.cwd")

    return workflow, actions


def _normalize_workflow_structure(repo_root: Path) -> List[str]:
    wf_root = repo_root / ".codex-workflow"
    workflow_path = wf_root / "workflow.json"
    workflow = load_json(workflow_path)
    workflow, actions = _ensure_crewai_stage(workflow)
    workflow, verify_actions = _ensure_verify_stage(workflow)
    actions.extend(verify_actions)
    workflow, codex_actions = _normalize_codex_runtime(workflow)
    actions.extend(codex_actions)
    if actions:
        dump_json(workflow_path, workflow)
    return actions


def _adapt_workflow_after_report(repo_root: Path, report: Dict[str, object], iteration: int) -> List[str]:
    actions: List[str] = []
    wf_root = repo_root / ".codex-workflow"

    workflow_path = wf_root / "workflow.json"
    workflow = load_json(workflow_path)
    codex_cfg = workflow.get("codex", {})
    if not isinstance(codex_cfg, dict):
        codex_cfg = {}
        workflow["codex"] = codex_cfg
    workflow, stage_actions = _ensure_crewai_stage(workflow)
    workflow, verify_actions = _ensure_verify_stage(workflow)
    stage_actions.extend(verify_actions)
    workflow, codex_actions = _normalize_codex_runtime(workflow)
    stage_actions.extend(codex_actions)
    actions.extend(stage_actions)

    evo_path = wf_root / "evolution.json"
    evo = load_json(evo_path)
    if not isinstance(evo.get("max_lessons_in_prompt"), int):
        evo["max_lessons_in_prompt"] = 6

    status = str(report.get("status", "unknown"))
    if status == "success" and iteration % 10 == 0:
        evo["max_lessons_in_prompt"] = min(20, int(evo.get("max_lessons_in_prompt", 6)) + 1)
        evo["lookback_runs"] = min(300, int(evo.get("lookback_runs", 30)) + 5)
        actions.append("increased evolution lookback and lesson window after stable streak")

    stages = report.get("stages", [])
    crew_degraded = False
    if isinstance(stages, list):
        for stage in stages:
            if (
                isinstance(stage, dict)
                and stage.get("kind") == "crewai"
                and stage.get("status") in {"failed", "degraded"}
            ):
                crew_degraded = True
                break

    if crew_degraded:
        wf_stages = workflow.get("stages", [])
        if isinstance(wf_stages, list):
            crew_idx = -1
            for idx, item in enumerate(wf_stages):
                if isinstance(item, dict) and item.get("id") == "crew_orchestrate":
                    crew_idx = idx
                    break
            has_codex_fallback = any(
                isinstance(item, dict) and str(item.get("id", "")).startswith("codex_fallback")
                for item in wf_stages
            )
            if not has_codex_fallback:
                wf_stages.insert(
                    max(crew_idx + 1, 0),
                    {
                        "id": "codex_fallback",
                        "kind": "codex",
                        "description": "Fallback codex stage when crewai fails",
                        "continue_on_error": True,
                        "prompt_template": "CrewAI failed. Fallback to Codex for goal: {{goal}}",
                    },
                )
                actions.append("added codex fallback stage after crewai failure")
            if not bool(codex_cfg.get("enabled", False)):
                codex_cfg["enabled"] = True
                actions.append("enabled codex runtime after crewai degradation")
            for item in wf_stages:
                if (
                    isinstance(item, dict)
                    and str(item.get("id", "")).startswith("codex_fallback")
                    and not bool(item.get("continue_on_error", False))
                ):
                    item["continue_on_error"] = True
                    actions.append("set codex_fallback as non-blocking (continue_on_error=true)")

    dump_json(workflow_path, workflow)
    dump_json(evo_path, evo)

    actions.extend(_normalize_quality_gates(repo_root))
    return actions


def _record_iteration(
    repo_root: Path,
    iteration: int,
    goal: str,
    report_path: Path,
    actions: List[str],
) -> None:
    report = load_json(report_path)
    payload = {
        "ts": _utc_now(),
        "iteration": iteration,
        "goal": goal,
        "status": report.get("status", "unknown"),
        "report_path": str(report_path),
        "actions": actions,
    }
    append_jsonl(repo_root / ".codex-workflow" / "memory" / "autopilot_journal.jsonl", payload)


def iterate_goal(
    repo_root: Path,
    goal: str,
    iterations: int = 100,
    dry_run: bool = False,
    enable_codex: bool = False,
    project_name: str = "project",
    until_success: bool = False,
    min_iterations: int = 1,
) -> Path:
    root = repo_root.resolve()
    _ensure_initialized(root, project_name=project_name)
    _normalize_workflow_structure(root)
    _normalize_quality_gates(root)

    wf_root = root / ".codex-workflow"
    summary_path = wf_root / "memory" / "autopilot_latest.json"

    done = 0
    success_count = 0
    last_report_path = None

    for idx in range(1, iterations + 1):
        run_goal = f"{goal} [iteration {idx}/{iterations}]"
        report_path = run_workflow(
            repo_root=root,
            goal=run_goal,
            dry_run=dry_run,
            enable_codex=enable_codex,
            evolve_after_run=False,
        )
        report = load_json(report_path)
        evolve(repo_root=root)
        actions = _adapt_workflow_after_report(root, report, idx)
        _record_iteration(root, idx, goal, report_path, actions)

        done = idx
        last_report_path = str(report_path)
        if report.get("status") == "success":
            success_count += 1

        if until_success and idx >= max(1, min_iterations) and report.get("status") == "success":
            break

    summary = {
        "ts": _utc_now(),
        "goal": goal,
        "iterations_requested": iterations,
        "iterations_completed": done,
        "dry_run": dry_run,
        "until_success": until_success,
        "success_count": success_count,
        "success_rate": (success_count / done) if done else 0.0,
        "last_report_path": last_report_path,
    }
    dump_json(summary_path, summary)
    return summary_path
