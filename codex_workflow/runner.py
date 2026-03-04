from __future__ import annotations

import json
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

from .models import CommandResult, RunReport, StageConfig, StageResult, WorkflowConfig
from .utils import append_jsonl, dump_json, load_json


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _run_id() -> str:
    return _utc_now().strftime("%Y%m%dT%H%M%SZ")


def _read_recent_lessons(path: Path, limit: int = 6) -> List[Dict[str, object]]:
    if not path.exists():
        return []

    lines = path.read_text(encoding="utf-8").splitlines()
    data: List[Dict[str, object]] = []
    for line in lines[-limit:]:
        if not line.strip():
            continue
        try:
            data.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return data


def _render_template(template: str, context: Dict[str, str]) -> str:
    rendered = template
    for key, value in context.items():
        rendered = rendered.replace("{{" + key + "}}", value)
    return rendered


def _run_shell_command(command: str, cwd: Path, log_path: Path) -> int:
    completed = subprocess.run(  # nosec B602
        command,
        cwd=str(cwd),
        shell=True,
        capture_output=True,
        text=True,
        check=False,
    )
    with log_path.open("w", encoding="utf-8") as f:
        f.write(f"$ {command}\n\n")
        if completed.stdout:
            f.write("[stdout]\n")
            f.write(completed.stdout)
            if not completed.stdout.endswith("\n"):
                f.write("\n")
        if completed.stderr:
            f.write("\n[stderr]\n")
            f.write(completed.stderr)
            if not completed.stderr.endswith("\n"):
                f.write("\n")
        f.write(f"\n[return_code] {completed.returncode}\n")
    return int(completed.returncode)


def _commands_from_quality_gates(quality_gates: Dict[str, object], section: str) -> List[str]:
    raw_items = quality_gates.get(section, [])
    if not isinstance(raw_items, list):
        return []
    commands: List[str] = []
    for item in raw_items:
        if isinstance(item, dict):
            command = item.get("command", "")
            if isinstance(command, str) and command.strip():
                commands.append(command.strip())
    return commands


def _resolve_shell_commands(stage: StageConfig, quality_gates: Dict[str, object]) -> List[str]:
    if stage.command_source == "quality_gates.required":
        resolved = _commands_from_quality_gates(quality_gates, "required")
        if resolved:
            return resolved
    if stage.command_source == "quality_gates.optional":
        resolved = _commands_from_quality_gates(quality_gates, "optional")
        if resolved:
            return resolved
    return list(stage.commands)


def run_workflow(
    repo_root: Path,
    goal: str,
    dry_run: bool = False,
    enable_codex: bool = False,
    evolve_after_run: bool = True,
) -> Path:
    wf_root = repo_root / ".codex-workflow"
    workflow = WorkflowConfig.from_dict(load_json(wf_root / "workflow.json"))
    project_profile = load_json(wf_root / "project_profile.json")
    quality_gates = load_json(wf_root / "quality_gates.json")

    if enable_codex:
        workflow.codex.enabled = True

    run_id = _run_id()
    run_dir = wf_root / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    lessons = _read_recent_lessons(wf_root / "memory" / "lessons.jsonl", limit=6)

    context = {
        "goal": goal,
        "project_profile": json.dumps(project_profile, ensure_ascii=False),
        "quality_gates": json.dumps(quality_gates, ensure_ascii=False),
        "recent_lessons": json.dumps(lessons, ensure_ascii=False),
    }

    stage_results: List[StageResult] = []
    run_status = "success"

    for stage in workflow.stages:
        start = time.time()
        command_results: List[CommandResult] = []
        prompt_path = run_dir / f"{stage.stage_id}.prompt.txt"
        stage_message = ""
        stage_status = "success"

        if stage.kind == "shell":
            shell_commands = _resolve_shell_commands(stage, quality_gates)
            if not shell_commands:
                stage_status = "failed"
                run_status = "failed"
                stage_message = "no shell commands resolved for this stage"
            for index, command in enumerate(shell_commands, start=1):
                cmd = _render_template(command, context)
                log_path = run_dir / f"{stage.stage_id}.cmd{index}.log"
                if dry_run:
                    return_code = 0
                    log_path.write_text(f"[dry-run] $ {cmd}\n", encoding="utf-8")
                else:
                    return_code = _run_shell_command(cmd, repo_root, log_path)

                command_results.append(
                    CommandResult(
                        command=cmd,
                        return_code=return_code,
                        log_path=str(log_path),
                    )
                )

                if return_code != 0:
                    stage_status = "failed"
                    run_status = "failed"
                    stage_message = f"command failed: {cmd}"
                    if not stage.continue_on_error:
                        break

        elif stage.kind == "codex":
            prompt = _render_template(stage.prompt_template, context)
            prompt_path.write_text(prompt + "\n", encoding="utf-8")
            if workflow.codex.enabled:
                codex_cwd = _render_template(workflow.codex.cwd, {"repo_root": str(repo_root)})
                codex_cmd = _render_template(
                    workflow.codex.command,
                    {
                        "prompt_file": str(prompt_path),
                        "repo_root": str(repo_root),
                    },
                )
                log_path = run_dir / f"{stage.stage_id}.codex.log"
                if dry_run:
                    return_code = 0
                    log_path.write_text(f"[dry-run] $ {codex_cmd}\n", encoding="utf-8")
                else:
                    return_code = _run_shell_command(codex_cmd, Path(codex_cwd), log_path)

                command_results.append(
                    CommandResult(
                        command=codex_cmd,
                        return_code=return_code,
                        log_path=str(log_path),
                    )
                )
                if return_code != 0:
                    stage_status = "failed"
                    run_status = "failed"
                    stage_message = "codex command failed"
            else:
                stage_status = "skipped"
                stage_message = (
                    "codex stage skipped (set --enable-codex or codex.enabled=true in workflow.json)"
                )

        else:
            stage_status = "manual"
            stage_message = stage.description or "manual stage"

        elapsed = time.time() - start
        stage_results.append(
            StageResult(
                stage_id=stage.stage_id,
                kind=stage.kind,
                status=stage_status,
                elapsed_seconds=elapsed,
                command_results=command_results,
                prompt_path=str(prompt_path) if prompt_path.exists() else "",
                message=stage_message,
            )
        )

        if stage_status == "failed" and not stage.continue_on_error:
            break

    report = RunReport(
        run_id=run_id,
        created_at=_utc_now().isoformat(),
        repo_root=str(repo_root),
        goal=goal,
        status=run_status,
        stages=stage_results,
    )

    report_path = run_dir / "run_report.json"
    dump_json(report_path, report.to_dict())
    append_jsonl(
        wf_root / "memory" / "run_index.jsonl",
        {
            "run_id": run_id,
            "created_at": report.created_at,
            "status": run_status,
            "report_path": str(report_path),
            "goal": goal,
        },
    )

    if evolve_after_run and not dry_run:
        from .evolution import evolve

        evolve(repo_root=repo_root)

    return report_path
