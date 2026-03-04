from __future__ import annotations

import json
import os
import subprocess
import time
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from typing import Dict, List

from .models import CommandResult, RunReport, StageConfig, StageResult, WorkflowConfig
from .utils import append_jsonl, dump_json, load_json


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _run_id() -> str:
    # Microsecond precision avoids run-id collisions in high-frequency iteration loops.
    return _utc_now().strftime("%Y%m%dT%H%M%S%fZ")


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
        rendered = rendered.replace("{" + key + "}", value)
    return rendered


def _run_shell_command(command: str, cwd: Path, log_path: Path, timeout_seconds: int | None = None) -> int:
    timed_out = False
    try:
        completed = subprocess.run(  # nosec B602
            command,
            cwd=str(cwd),
            shell=True,
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout_seconds,
        )
        stdout_text = completed.stdout or ""
        stderr_text = completed.stderr or ""
        return_code = int(completed.returncode)
    except subprocess.TimeoutExpired as exc:
        timed_out = True
        stdout_text = exc.stdout or ""
        stderr_text = exc.stderr or ""
        return_code = 124

        if isinstance(stdout_text, bytes):
            stdout_text = stdout_text.decode("utf-8", errors="replace")
        if isinstance(stderr_text, bytes):
            stderr_text = stderr_text.decode("utf-8", errors="replace")

    with log_path.open("w", encoding="utf-8") as f:
        f.write(f"$ {command}\n\n")
        if stdout_text:
            f.write("[stdout]\n")
            f.write(stdout_text)
            if not stdout_text.endswith("\n"):
                f.write("\n")
        if stderr_text:
            f.write("\n[stderr]\n")
            f.write(stderr_text)
            if not stderr_text.endswith("\n"):
                f.write("\n")
        if timed_out:
            f.write(f"\n[timeout_seconds] {timeout_seconds}\n")
            f.write("[timeout] command exceeded time limit and was terminated\n")
        f.write(f"\n[return_code] {return_code}\n")
    return return_code


def _run_crewai_stage(goal_text: str, log_path: Path) -> int:
    try:
        from .crewai_blueprint import build_default_crew, resolve_codex_llm_runtime

        os.environ.setdefault("CREWAI_TRACING_ENABLED", "false")
        os.environ.setdefault("CREWAI_DISABLE_TELEMETRY", "true")
        os.environ.setdefault("CREWAI_TESTING", "true")
        os.environ.setdefault("OTEL_SDK_DISABLED", "true")

        runtime = resolve_codex_llm_runtime(apply_env=False)
        fallback_raw = os.getenv("CODEX_WORKFLOW_CREWAI_FALLBACK_MODELS", "")
        fallback_models = [item.strip() for item in fallback_raw.split(",") if item.strip()]
        primary_model = runtime.get("model")

        candidates: List[str | None] = []
        if isinstance(primary_model, str) and primary_model.strip():
            candidates.append(primary_model.strip())
        for model in fallback_models:
            if model not in candidates:
                candidates.append(model)
        if not candidates:
            candidates = [None]

        attempts: List[Dict[str, str]] = []
        for model_name in candidates:
            stdout_buffer = StringIO()
            stderr_buffer = StringIO()
            try:
                with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
                    crew = build_default_crew(goal=goal_text, model_override=model_name)
                    result = crew.kickoff()
                log_path.write_text(
                    "[crewai_runtime]\n"
                    f"{json.dumps(runtime, ensure_ascii=False)}\n\n"
                    "[crewai_model]\n"
                    f"{model_name or 'default'}\n\n"
                    "[crewai_result]\n"
                    f"{result}\n\n"
                    "[crewai_stdout]\n"
                    f"{stdout_buffer.getvalue()}\n"
                    "[crewai_stderr]\n"
                    f"{stderr_buffer.getvalue()}\n",
                    encoding="utf-8",
                )
                return 0
            except Exception as exc:  # pragma: no cover - external dependency/runtime config
                attempts.append(
                    {
                        "model": model_name or "default",
                        "error": str(exc),
                        "stdout": stdout_buffer.getvalue(),
                        "stderr": stderr_buffer.getvalue(),
                    }
                )

        log_path.write_text(
            "[crewai_runtime]\n"
            f"{json.dumps(runtime, ensure_ascii=False)}\n\n"
            "[crewai_error]\n"
            "all model attempts failed\n\n"
            "[crewai_attempts]\n"
            f"{json.dumps(attempts, ensure_ascii=False, indent=2)}\n",
            encoding="utf-8",
        )
    except Exception as exc:  # pragma: no cover - external dependency/runtime config
        log_path.write_text(f"[crewai_error]\n{exc}\n", encoding="utf-8")
    return 1


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

        if not stage.enabled:
            elapsed = time.time() - start
            stage_results.append(
                StageResult(
                    stage_id=stage.stage_id,
                    kind=stage.kind,
                    status="skipped",
                    elapsed_seconds=elapsed,
                    command_results=[],
                    prompt_path="",
                    message="stage disabled by workflow policy",
                )
            )
            continue

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
                    if stage.continue_on_error:
                        stage_status = "degraded"
                        stage_message = f"non-blocking command failed: {cmd}"
                    else:
                        stage_status = "failed"
                        run_status = "failed"
                        stage_message = f"command failed: {cmd}"
                        break

        elif stage.kind == "crewai":
            crew_goal = _render_template(stage.prompt_template or "{{goal}}", context)
            log_path = run_dir / f"{stage.stage_id}.crewai.log"
            prompt_path.write_text(crew_goal + "\n", encoding="utf-8")
            if dry_run:
                return_code = 0
                log_path.write_text(f"[dry-run] crewai goal: {crew_goal}\n", encoding="utf-8")
            else:
                return_code = _run_crewai_stage(crew_goal, log_path)

            command_results.append(
                CommandResult(
                    command="crewai kickoff",
                    return_code=return_code,
                    log_path=str(log_path),
                )
            )
            if return_code != 0:
                if stage.continue_on_error:
                    stage_status = "degraded"
                    stage_message = "crewai stage failed (non-blocking)"
                else:
                    stage_status = "failed"
                    run_status = "failed"
                    stage_message = "crewai stage failed"

        elif stage.kind == "codex":
            prompt = _render_template(stage.prompt_template, context)
            prompt_path.write_text(prompt + "\n", encoding="utf-8")
            if workflow.codex.enabled:
                codex_cwd = _render_template(workflow.codex.cwd, {"repo_root": str(repo_root)})
                output_file = run_dir / f"{stage.stage_id}.codex.final.txt"
                codex_cmd = _render_template(
                    workflow.codex.command,
                    {
                        "prompt_file": str(prompt_path),
                        "output_file": str(output_file),
                        "repo_root": str(repo_root),
                    },
                )
                log_path = run_dir / f"{stage.stage_id}.codex.log"
                if dry_run:
                    return_code = 0
                    log_path.write_text(f"[dry-run] $ {codex_cmd}\n", encoding="utf-8")
                else:
                    try:
                        codex_timeout = int(os.getenv("CODEX_WORKFLOW_CODEX_TIMEOUT_SECONDS", "180"))
                    except ValueError:
                        codex_timeout = 180
                    return_code = _run_shell_command(
                        codex_cmd,
                        Path(codex_cwd),
                        log_path,
                        timeout_seconds=max(30, codex_timeout),
                    )

                command_results.append(
                    CommandResult(
                        command=codex_cmd,
                        return_code=return_code,
                        log_path=str(log_path),
                    )
                )
                output_required = (
                    "{output_file}" in workflow.codex.command or "{{output_file}}" in workflow.codex.command
                )
                if return_code == 0 and output_required and not dry_run:
                    check_log = run_dir / f"{stage.stage_id}.output_check.log"
                    has_output = output_file.exists() and output_file.stat().st_size > 0
                    if has_output:
                        check_log.write_text(
                            f"output_file={output_file}\nstatus=ok\n",
                            encoding="utf-8",
                        )
                        check_code = 0
                    else:
                        check_log.write_text(
                            f"output_file={output_file}\nstatus=missing_or_empty\n",
                            encoding="utf-8",
                        )
                        check_code = 65
                    command_results.append(
                        CommandResult(
                            command="codex output check",
                            return_code=check_code,
                            log_path=str(check_log),
                        )
                    )
                    if check_code != 0:
                        return_code = check_code
                        stage_message = "codex output file missing or empty"
                if return_code != 0:
                    if stage.continue_on_error:
                        stage_status = "degraded"
                        if not stage_message:
                            stage_message = "codex command failed (non-blocking)"
                    else:
                        stage_status = "failed"
                        run_status = "failed"
                        if not stage_message:
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
