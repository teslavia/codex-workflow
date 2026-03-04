from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import time
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from typing import Dict, List, Tuple

from .models import CommandResult, RunReport, StageConfig, StageResult, WorkflowConfig
from .utils import append_jsonl, dump_json, load_json

_CREWAI_INTERPRETER_CACHE: Dict[str, str] = {"path": "", "reason": ""}


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


def _read_model_cache(path: Path) -> Dict[str, object]:
    if not path.exists():
        return {"models": {}}
    data = load_json(path)
    models = data.get("models", {})
    if not isinstance(models, dict):
        models = {}
    return {"models": models}


def _write_model_cache(path: Path, cache: Dict[str, object]) -> None:
    dump_json(path, cache)


def _filter_blocked_models(
    candidates: List[str],
    cache: Dict[str, object],
    now_ts: float,
    probe_window_seconds: int,
) -> Tuple[List[str], List[str], List[str]]:
    models = cache.get("models", {})
    if not isinstance(models, dict):
        return candidates, [], []

    usable: List[str] = []
    blocked: List[str] = []
    probe: List[str] = []
    for model in candidates:
        item = models.get(model, {})
        if not isinstance(item, dict):
            usable.append(model)
            continue
        blocked_until = float(item.get("blocked_until", 0))
        if blocked_until > now_ts:
            remaining = blocked_until - now_ts
            last_probe_at = float(item.get("last_probe_at", 0))
            can_probe = (
                probe_window_seconds > 0
                and remaining <= probe_window_seconds
                and (now_ts - last_probe_at) >= probe_window_seconds
            )
            if can_probe and not probe:
                probe.append(model)
                usable.append(model)
                item["last_probe_at"] = now_ts
                models[model] = item
            else:
                blocked.append(model)
        else:
            usable.append(model)
    cache["models"] = models
    return usable, blocked, probe


def _is_blocked_error(text: str) -> bool:
    lowered = text.lower()
    return "request was blocked" in lowered or "your request was blocked" in lowered


def _is_crewai_unavailable_error(text: str) -> bool:
    lowered = text.lower()
    return (
        "crewai is not installed" in lowered
        or "install with: pip install '.[crewai]'" in lowered
        or "current python is unsupported" in lowered
    )


def _toolkit_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _candidate_crewai_interpreters() -> List[str]:
    env_one = os.getenv("CODEX_WORKFLOW_CREWAI_PYTHON", "").strip()
    env_many = [item.strip() for item in os.getenv("CODEX_WORKFLOW_CREWAI_PYTHON_CANDIDATES", "").split(",") if item.strip()]
    toolkit = _toolkit_root()
    candidates = [
        env_one,
        *env_many,
        str(toolkit / ".venv313" / "bin" / "python"),
        str(toolkit / ".venv" / "bin" / "python"),
        sys.executable,
        shutil.which("python3.13") or "",
        shutil.which("python3.12") or "",
        shutil.which("python3.11") or "",
        shutil.which("python3") or "",
        shutil.which("python") or "",
    ]
    ordered: List[str] = []
    for item in candidates:
        path = item.strip() if isinstance(item, str) else ""
        if not path or path in ordered:
            continue
        ordered.append(path)
    return ordered


def _python_can_import_crewai(interpreter: str) -> bool:
    cmd = [interpreter, "-c", "import crewai"]
    env = os.environ.copy()
    toolkit = str(_toolkit_root())
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = toolkit if not existing else (toolkit + os.pathsep + existing)
    try:
        completed = subprocess.run(
            cmd,
            cwd=toolkit,
            capture_output=True,
            text=True,
            check=False,
            timeout=6,
            env=env,
        )
    except Exception:
        return False
    return int(completed.returncode) == 0


def _resolve_crewai_interpreter() -> Tuple[str, str]:
    cached = _CREWAI_INTERPRETER_CACHE.get("path", "").strip()
    if cached:
        return cached, _CREWAI_INTERPRETER_CACHE.get("reason", "")

    for candidate in _candidate_crewai_interpreters():
        if _python_can_import_crewai(candidate):
            _CREWAI_INTERPRETER_CACHE["path"] = candidate
            _CREWAI_INTERPRETER_CACHE["reason"] = f"resolved via interpreter probe: {candidate}"
            return candidate, _CREWAI_INTERPRETER_CACHE["reason"]
    _CREWAI_INTERPRETER_CACHE["path"] = ""
    _CREWAI_INTERPRETER_CACHE["reason"] = "no interpreter with importable crewai found"
    return "", _CREWAI_INTERPRETER_CACHE["reason"]


def detect_crewai_runtime_available() -> Tuple[bool, str]:
    try:
        from .crewai_blueprint import _require_crewai  # type: ignore[attr-defined]

        _require_crewai()
        return True, f"in-process runtime via {sys.executable}"
    except Exception as exc:
        interpreter, reason = _resolve_crewai_interpreter()
        if interpreter:
            return True, reason
        return False, str(exc) if str(exc).strip() else reason


def _run_crewai_stage_subprocess(
    goal_text: str,
    log_path: Path,
    candidates: List[str],
    blocked_model_names: List[str],
) -> Tuple[int, Dict[str, object]]:
    meta: Dict[str, object] = {"attempts": [], "blocked_models": [], "successful_model": ""}
    interpreter, reason = _resolve_crewai_interpreter()
    if not interpreter:
        log_path.write_text(
            "[crewai_error]\n"
            f"subprocess runtime unavailable: {reason}\n",
            encoding="utf-8",
        )
        meta["attempts"] = [{"model": "default", "error": reason, "stdout": "", "stderr": ""}]
        return 1, meta

    runtime_log_path = log_path.parent / f"{log_path.stem}.runtime.json"
    payload = {
        "goal": goal_text,
        "candidates": candidates,
        "blocked_model_names": blocked_model_names,
        "log_path": str(log_path),
        "meta_path": str(runtime_log_path),
    }
    env = os.environ.copy()
    toolkit = str(_toolkit_root())
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = toolkit if not existing else (toolkit + os.pathsep + existing)
    env["CODEX_WORKFLOW_CREWAI_SUBPROCESS_PAYLOAD"] = json.dumps(payload, ensure_ascii=False)

    script = r"""
import json
import os
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path

from codex_workflow.crewai_blueprint import build_default_crew, resolve_codex_llm_runtime

def _blocked(text: str) -> bool:
    lowered = text.lower()
    return "request was blocked" in lowered or "your request was blocked" in lowered

payload = json.loads(os.environ["CODEX_WORKFLOW_CREWAI_SUBPROCESS_PAYLOAD"])
goal = str(payload.get("goal", ""))
candidates = payload.get("candidates", [])
if not isinstance(candidates, list):
    candidates = []
blocked_names = payload.get("blocked_model_names", [])
if not isinstance(blocked_names, list):
    blocked_names = []
log_path = Path(str(payload.get("log_path", "")))
meta_path = Path(str(payload.get("meta_path", "")))

runtime = resolve_codex_llm_runtime(apply_env=False)
runtime_model = runtime.get("model")
full_candidates = [str(item).strip() for item in candidates if str(item).strip()]
if isinstance(runtime_model, str) and runtime_model.strip() and runtime_model not in full_candidates:
    full_candidates.insert(0, runtime_model)
blocked_set = {str(item).strip() for item in blocked_names if str(item).strip()}
if blocked_set:
    full_candidates = [item for item in full_candidates if item not in blocked_set]

meta = {"attempts": [], "blocked_models": [], "successful_model": ""}
if not full_candidates:
    log_path.write_text(
        "[crewai_error]\n"
        "no model candidates available (all candidates currently blocked by cache)\n",
        encoding="utf-8",
    )
    meta_path.write_text(json.dumps(meta, ensure_ascii=False), encoding="utf-8")
    raise SystemExit(2)

attempts = []
blocked_models = []
for model_name in full_candidates:
    stdout_buffer = StringIO()
    stderr_buffer = StringIO()
    try:
        with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
            crew = build_default_crew(goal=goal, model_override=(model_name or None))
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
        meta = {"attempts": attempts, "blocked_models": blocked_models, "successful_model": model_name or "default"}
        meta_path.write_text(json.dumps(meta, ensure_ascii=False), encoding="utf-8")
        raise SystemExit(0)
    except Exception as exc:
        err_text = str(exc)
        out_text = stdout_buffer.getvalue()
        err_buf = stderr_buffer.getvalue()
        if _blocked(err_text) or _blocked(out_text) or _blocked(err_buf):
            blocked_models.append(model_name or "default")
        attempts.append(
            {
                "model": model_name or "default",
                "error": err_text,
                "stdout": out_text,
                "stderr": err_buf,
            }
        )

log_path.write_text(
    "[crewai_error]\n"
    "all model attempts failed\n\n"
    "[crewai_attempts]\n"
    f"{json.dumps(attempts, ensure_ascii=False, indent=2)}\n",
    encoding="utf-8",
)
meta = {"attempts": attempts, "blocked_models": blocked_models, "successful_model": ""}
meta_path.write_text(json.dumps(meta, ensure_ascii=False), encoding="utf-8")
raise SystemExit(1)
"""

    try:
        completed = subprocess.run(
            [interpreter, "-c", script],
            cwd=toolkit,
            capture_output=True,
            text=True,
            check=False,
            timeout=max(15, int(os.getenv("CODEX_WORKFLOW_CREWAI_SUBPROCESS_TIMEOUT_SECONDS", "120"))),
            env=env,
        )
        return_code = int(completed.returncode)
    except Exception as exc:
        log_path.write_text(
            "[crewai_error]\n"
            f"failed to execute crewai subprocess ({interpreter}): {exc}\n",
            encoding="utf-8",
        )
        meta["attempts"] = [{"model": "default", "error": str(exc), "stdout": "", "stderr": ""}]
        return 1, meta

    if runtime_log_path.exists():
        try:
            raw = json.loads(runtime_log_path.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                meta = raw
        except Exception:
            pass

    if return_code not in {0, 1, 2}:
        if log_path.exists():
            base_log = log_path.read_text(encoding="utf-8", errors="ignore")
        else:
            base_log = ""
        with log_path.open("w", encoding="utf-8") as f:
            f.write(base_log)
            if base_log and not base_log.endswith("\n"):
                f.write("\n")
            f.write("\n[crewai_subprocess]\n")
            f.write(f"interpreter={interpreter}\n")
            f.write(f"return_code={return_code}\n")
            if completed.stdout:
                f.write("[stdout]\n")
                f.write(completed.stdout)
                if not completed.stdout.endswith("\n"):
                    f.write("\n")
            if completed.stderr:
                f.write("[stderr]\n")
                f.write(completed.stderr)
                if not completed.stderr.endswith("\n"):
                    f.write("\n")
        return_code = 1
    return return_code, meta


def _run_crewai_stage(
    goal_text: str,
    log_path: Path,
    candidates: List[str],
    blocked_model_names: List[str],
) -> Tuple[int, Dict[str, object]]:
    meta: Dict[str, object] = {"attempts": [], "blocked_models": [], "successful_model": ""}
    local_runtime_available = True
    try:
        from .crewai_blueprint import _require_crewai, build_default_crew, resolve_codex_llm_runtime

        _require_crewai()

        os.environ.setdefault("CREWAI_TRACING_ENABLED", "false")
        os.environ.setdefault("CREWAI_DISABLE_TELEMETRY", "true")
        os.environ.setdefault("CREWAI_TESTING", "true")
        os.environ.setdefault("OTEL_SDK_DISABLED", "true")

        runtime = resolve_codex_llm_runtime(apply_env=False)
        runtime_model = runtime.get("model")
        full_candidates = list(candidates)
        if isinstance(runtime_model, str) and runtime_model.strip() and runtime_model not in full_candidates:
            full_candidates.insert(0, runtime_model)
        if blocked_model_names:
            full_candidates = [item for item in full_candidates if item not in set(blocked_model_names)]
        if not full_candidates:
            log_path.write_text(
                "[crewai_error]\n"
                "no model candidates available (all candidates currently blocked by cache)\n",
                encoding="utf-8",
            )
            return 2, meta

        attempts: List[Dict[str, str]] = []
        blocked_models: List[str] = []
        for model_name in full_candidates:
            model_override = model_name.strip() if isinstance(model_name, str) else ""
            stdout_buffer = StringIO()
            stderr_buffer = StringIO()
            try:
                with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
                    crew = build_default_crew(goal=goal_text, model_override=(model_override or None))
                    result = crew.kickoff()
                log_path.write_text(
                    "[crewai_runtime]\n"
                    f"{json.dumps(runtime, ensure_ascii=False)}\n\n"
                    "[crewai_model]\n"
                    f"{model_override or 'default'}\n\n"
                    "[crewai_result]\n"
                    f"{result}\n\n"
                    "[crewai_stdout]\n"
                    f"{stdout_buffer.getvalue()}\n"
                    "[crewai_stderr]\n"
                    f"{stderr_buffer.getvalue()}\n",
                    encoding="utf-8",
                )
                meta["attempts"] = attempts
                meta["blocked_models"] = blocked_models
                meta["successful_model"] = model_override or "default"
                return 0, meta
            except Exception as exc:  # pragma: no cover - external dependency/runtime config
                err_text = str(exc)
                stdout_text = stdout_buffer.getvalue()
                stderr_text = stderr_buffer.getvalue()
                if _is_blocked_error(err_text) or _is_blocked_error(stdout_text) or _is_blocked_error(stderr_text):
                    blocked_models.append(model_override or "default")
                attempts.append(
                    {
                        "model": model_override or "default",
                        "error": err_text,
                        "stdout": stdout_text,
                        "stderr": stderr_text,
                    }
                )

        log_path.write_text(
            "[crewai_error]\n"
            "all model attempts failed\n\n"
            "[crewai_attempts]\n"
            f"{json.dumps(attempts, ensure_ascii=False, indent=2)}\n",
            encoding="utf-8",
        )
        meta["attempts"] = attempts
        meta["blocked_models"] = blocked_models
        meta["successful_model"] = ""
        unavailable_errors = [
            attempt for attempt in attempts if _is_crewai_unavailable_error(str(attempt.get("error", "")))
        ]
        if attempts and len(unavailable_errors) == len(attempts):
            return _run_crewai_stage_subprocess(
                goal_text=goal_text,
                log_path=log_path,
                candidates=candidates,
                blocked_model_names=blocked_model_names,
            )
        return 1, meta
    except Exception as exc:  # pragma: no cover - external dependency/runtime config
        local_runtime_available = False
        if not _is_crewai_unavailable_error(str(exc)):
            log_path.write_text(f"[crewai_error]\n{exc}\n", encoding="utf-8")
            meta["attempts"] = [{"model": "default", "error": str(exc), "stdout": "", "stderr": ""}]
            meta["blocked_models"] = []
            meta["successful_model"] = ""
            return 1, meta

    if not local_runtime_available:
        return _run_crewai_stage_subprocess(
            goal_text=goal_text,
            log_path=log_path,
            candidates=candidates,
            blocked_model_names=blocked_model_names,
        )
    return 1, meta


def _update_model_cache(
    cache: Dict[str, object],
    meta: Dict[str, object],
    now_ts: float,
    ttl_hours: int,
    hard_skip_hours: int,
) -> Dict[str, object]:
    models = cache.get("models", {})
    if not isinstance(models, dict):
        models = {}

    ttl_seconds = max(1, ttl_hours) * 3600
    hard_skip_seconds = max(1, hard_skip_hours) * 3600
    attempts = meta.get("attempts", [])
    if not isinstance(attempts, list):
        attempts = []

    success_model = str(meta.get("successful_model", "")).strip()
    blocked_set = set()
    blocked_raw = meta.get("blocked_models", [])
    if isinstance(blocked_raw, list):
        blocked_set = {str(item).strip() for item in blocked_raw if str(item).strip()}

    for attempt in attempts:
        if not isinstance(attempt, dict):
            continue
        model_name = str(attempt.get("model", "")).strip()
        if not model_name:
            continue
        item = models.get(model_name, {})
        if not isinstance(item, dict):
            item = {}
        item["last_seen"] = now_ts
        item["last_error"] = str(attempt.get("error", ""))
        if model_name in blocked_set:
            item["last_status"] = "blocked"
            item["blocked_until"] = now_ts + max(ttl_seconds, hard_skip_seconds)
        else:
            item["last_status"] = "error"
            item["blocked_until"] = float(item.get("blocked_until", 0))
        models[model_name] = item

    if success_model:
        item = models.get(success_model, {})
        if not isinstance(item, dict):
            item = {}
        item["last_seen"] = now_ts
        item["last_status"] = "ok"
        item["last_error"] = ""
        item["blocked_until"] = 0
        models[success_model] = item

    for key in list(models.keys()):
        value = models.get(key, {})
        if not isinstance(value, dict):
            continue
        blocked_until = float(value.get("blocked_until", 0))
        if blocked_until > 0 and blocked_until <= now_ts:
            value["blocked_until"] = 0
            if value.get("last_status") == "blocked":
                value["last_status"] = "expired"
            models[key] = value

    cache["models"] = models
    return cache


def _evaluate_codex_output(
    output_file: Path,
    goal_text: str,
    check_log: Path,
) -> Tuple[int, str]:
    goal_en_tokens = re.findall(r"[A-Za-z][A-Za-z0-9_-]{2,}", goal_text.lower())
    goal_zh_tokens = re.findall(r"[\u4e00-\u9fff]{2,}", goal_text)
    semantic_units = len(goal_en_tokens) + len(goal_zh_tokens)

    min_chars_raw = os.getenv("CODEX_WORKFLOW_CODEX_MIN_CHARS", "").strip()
    adaptive_min_chars = max(60, min(300, 50 + semantic_units * 12))
    if min_chars_raw:
        try:
            min_chars = max(20, int(min_chars_raw))
        except ValueError:
            min_chars = adaptive_min_chars
    else:
        min_chars = adaptive_min_chars

    raw_keywords = os.getenv("CODEX_WORKFLOW_CODEX_REQUIRE_KEYWORDS", "").strip()
    required_keywords = [item.strip() for item in raw_keywords.split(",") if item.strip()]

    if not required_keywords:
        stop_words = {
            "the",
            "and",
            "for",
            "with",
            "that",
            "this",
            "from",
            "into",
            "then",
            "goal",
            "iteration",
            "工业级",
            "项目",
            "工作流",
            "自动化",
            "验证",
            "目标",
            "完成",
            "质量",
            "鲁棒性",
        }
        merged: List[str] = []
        for token in goal_en_tokens + goal_zh_tokens:
            token = token.strip()
            if token and token not in stop_words and token not in merged:
                merged.append(token)
        required_keywords = merged[:8]

    min_keyword_hits_raw = os.getenv("CODEX_WORKFLOW_CODEX_MIN_KEYWORD_HITS", "").strip()
    if min_keyword_hits_raw:
        try:
            min_keyword_hits = max(0, int(min_keyword_hits_raw))
        except ValueError:
            min_keyword_hits = 1
    else:
        if required_keywords:
            min_keyword_hits = max(1, min(3, len(required_keywords) // 2))
        else:
            min_keyword_hits = 0

    payload: Dict[str, object] = {
        "output_file": str(output_file),
        "goal": goal_text,
        "min_chars": min_chars,
        "required_keywords": required_keywords,
        "min_keyword_hits": min_keyword_hits,
        "status": "ok",
        "checks": [],
    }

    if not output_file.exists():
        payload["status"] = "missing"
        payload["checks"] = [{"name": "exists", "ok": False}]
        check_log.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return 65, "codex output file missing"

    text = output_file.read_text(encoding="utf-8", errors="ignore")
    stripped = text.strip()
    length_ok = len(stripped) >= min_chars
    payload["checks"] = [
        {"name": "exists", "ok": True},
        {"name": "min_chars", "ok": length_ok, "actual": len(stripped)},
    ]
    if not length_ok:
        payload["status"] = "too_short"
        check_log.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return 66, f"codex output too short (< {min_chars} chars)"

    if required_keywords and min_keyword_hits > 0:
        lowered = stripped.lower()
        hit_keywords = [word for word in required_keywords if word.lower() in lowered]
        missing = [word for word in required_keywords if word.lower() not in lowered]
        keywords_ok = len(hit_keywords) >= min(min_keyword_hits, len(required_keywords))
        payload["checks"].append(
            {
                "name": "keywords",
                "ok": keywords_ok,
                "hit": hit_keywords,
                "missing": missing,
            }
        )
        if not keywords_ok:
            payload["status"] = "missing_keywords"
            check_log.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            return 67, "codex output missing required semantic keywords"

    check_log.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return 0, ""


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
    model_cache_path = wf_root / "memory" / "model_availability.json"
    model_cache = _read_model_cache(model_cache_path)

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
                crew_meta: Dict[str, object] = {"attempts": [], "blocked_models": [], "successful_model": ""}
            else:
                fallback_raw = os.getenv("CODEX_WORKFLOW_CREWAI_FALLBACK_MODELS", "")
                fallback_models = [item.strip() for item in fallback_raw.split(",") if item.strip()]
                try:
                    from .crewai_blueprint import resolve_codex_llm_runtime

                    runtime = resolve_codex_llm_runtime(apply_env=False)
                    runtime_model = runtime.get("model", "")
                    if isinstance(runtime_model, str) and runtime_model.strip() and runtime_model not in fallback_models:
                        fallback_models.insert(0, runtime_model)
                except Exception:
                    pass
                now_ts = time.time()
                try:
                    probe_window_minutes = int(os.getenv("CODEX_WORKFLOW_MODEL_PROBE_WINDOW_MINUTES", "0"))
                except ValueError:
                    probe_window_minutes = 0
                candidates, blocked_from_cache, probe_from_cache = _filter_blocked_models(
                    fallback_models,
                    model_cache,
                    now_ts,
                    probe_window_seconds=max(0, probe_window_minutes) * 60,
                )
                return_code, crew_meta = _run_crewai_stage(
                    crew_goal,
                    log_path,
                    candidates=candidates,
                    blocked_model_names=blocked_from_cache,
                )
                crew_meta["blocked_by_cache"] = blocked_from_cache
                crew_meta["probe_from_cache"] = probe_from_cache
                try:
                    ttl_hours = int(os.getenv("CODEX_WORKFLOW_MODEL_BLOCK_TTL_HOURS", "24"))
                except ValueError:
                    ttl_hours = 24
                try:
                    hard_skip_hours = int(os.getenv("CODEX_WORKFLOW_MODEL_BLOCK_HARD_SKIP_HOURS", "24"))
                except ValueError:
                    hard_skip_hours = 24
                model_cache = _update_model_cache(
                    model_cache,
                    crew_meta,
                    now_ts,
                    ttl_hours=ttl_hours,
                    hard_skip_hours=hard_skip_hours,
                )
                _write_model_cache(model_cache_path, model_cache)

            command_results.append(
                CommandResult(
                    command="crewai kickoff",
                    return_code=return_code,
                    log_path=str(log_path),
                )
            )
            cache_log = run_dir / f"{stage.stage_id}.model_cache.log"
            cache_log.write_text(
                json.dumps(
                    {
                        "blocked_by_cache": crew_meta.get("blocked_by_cache", []),
                        "probe_from_cache": crew_meta.get("probe_from_cache", []),
                        "blocked_models_detected": crew_meta.get("blocked_models", []),
                        "successful_model": crew_meta.get("successful_model", ""),
                    },
                    ensure_ascii=False,
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )
            should_record_cache_command = return_code != 2 or bool(crew_meta.get("probe_from_cache", []))
            if should_record_cache_command:
                command_results.append(
                    CommandResult(
                        command="crewai model cache update",
                        return_code=0,
                        log_path=str(cache_log),
                    )
                )
            if return_code != 0:
                if stage.continue_on_error:
                    if return_code == 2:
                        stage_status = "skipped"
                        stage_message = "crewai stage skipped by model block cache (non-blocking)"
                    else:
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
                        timeout_seconds=max(5, codex_timeout),
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
                    check_code, check_message = _evaluate_codex_output(
                        output_file=output_file,
                        goal_text=goal,
                        check_log=check_log,
                    )
                    command_results.append(
                        CommandResult(
                            command="codex completion check",
                            return_code=check_code,
                            log_path=str(check_log),
                        )
                    )
                    if check_code != 0:
                        return_code = check_code
                        stage_message = check_message
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
