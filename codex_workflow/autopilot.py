from __future__ import annotations

from datetime import datetime, timezone
import json
import os
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
                    "command": (
                        "python3 -m compileall -q "
                        "-x '(^|/)(\\.venv[^/]*|\\.git|build|dist|__pycache__|\\._[^/]*)(/|$)' ."
                    ),
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
                "enabled": True,
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
        if isinstance(crew_stage, dict):
            if not bool(crew_stage.get("continue_on_error", False)):
                crew_stage["continue_on_error"] = True
                actions.append("set crew_orchestrate as non-blocking (continue_on_error=true)")
            if "enabled" not in crew_stage:
                crew_stage["enabled"] = True
                actions.append("set crew_orchestrate as enabled")

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
    target_cmd = "codex exec --skip-git-repo-check -o {output_file} - < {prompt_file}"
    if not isinstance(command, str) or not command.strip():
        codex_cfg["command"] = target_cmd
        actions.append("set default codex.command for stdin prompt mode with output file")
    elif (
        "--prompt-file" in command
        or "codex exec - < {prompt_file}" in command
        or "codex exec --skip-git-repo-check - < {prompt_file}" in command
    ):
        codex_cfg["command"] = target_cmd
        actions.append("migrated legacy codex.command to stdin mode with output file")

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


def _load_autopilot_state(repo_root: Path) -> Dict[str, int]:
    state_path = repo_root / ".codex-workflow" / "memory" / "autopilot_state.json"
    if not state_path.exists():
        return {
            "crew_blocked_streak": 0,
            "crew_cooldown_remaining": 0,
            "crew_blocked_threshold": 3,
            "crew_cooldown_rounds": 5,
            "codex_timeout_streak": 0,
            "codex_cooldown_remaining": 0,
            "codex_timeout_threshold": 3,
            "codex_cooldown_rounds": 4,
        }
    raw = load_json(state_path)
    return {
        "crew_blocked_streak": int(raw.get("crew_blocked_streak", 0)),
        "crew_cooldown_remaining": int(raw.get("crew_cooldown_remaining", 0)),
        "crew_blocked_threshold": int(raw.get("crew_blocked_threshold", 3)),
        "crew_cooldown_rounds": int(raw.get("crew_cooldown_rounds", 5)),
        "codex_timeout_streak": int(raw.get("codex_timeout_streak", 0)),
        "codex_cooldown_remaining": int(raw.get("codex_cooldown_remaining", 0)),
        "codex_timeout_threshold": int(raw.get("codex_timeout_threshold", 3)),
        "codex_cooldown_rounds": int(raw.get("codex_cooldown_rounds", 4)),
    }


def _save_autopilot_state(repo_root: Path, state: Dict[str, int]) -> None:
    state_path = repo_root / ".codex-workflow" / "memory" / "autopilot_state.json"
    dump_json(state_path, state)


def _set_crewai_stage_enabled(repo_root: Path, enabled: bool) -> List[str]:
    wf_path = repo_root / ".codex-workflow" / "workflow.json"
    workflow = load_json(wf_path)
    actions: List[str] = []

    stages = workflow.get("stages", [])
    changed = False
    if isinstance(stages, list):
        for stage in stages:
            if isinstance(stage, dict) and stage.get("kind") == "crewai":
                if bool(stage.get("enabled", True)) != enabled:
                    stage["enabled"] = enabled
                    changed = True
                    if enabled:
                        actions.append("re-enabled crew_orchestrate stage after cooldown")
                    else:
                        actions.append("disabled crew_orchestrate stage due to blocked cooldown")
                break

    if changed:
        dump_json(wf_path, workflow)
    return actions


def _set_codex_fallback_stage_enabled(repo_root: Path, enabled: bool) -> List[str]:
    wf_path = repo_root / ".codex-workflow" / "workflow.json"
    workflow = load_json(wf_path)
    actions: List[str] = []

    stages = workflow.get("stages", [])
    changed = False
    found_fallback = False
    if isinstance(stages, list):
        for stage in stages:
            if not isinstance(stage, dict):
                continue
            stage_id = str(stage.get("id", ""))
            if not stage_id.startswith("codex_fallback"):
                continue
            found_fallback = True
            if bool(stage.get("enabled", True)) != enabled:
                stage["enabled"] = enabled
                changed = True

    if changed:
        dump_json(wf_path, workflow)
        if enabled:
            actions.append("re-enabled codex_fallback stage after timeout cooldown")
        else:
            actions.append("disabled codex_fallback stage due to timeout cooldown")
    elif not found_fallback and not enabled:
        actions.append("codex timeout cooldown requested but no codex_fallback stage exists")
    return actions


def _is_crewai_blocked(repo_root: Path, report: Dict[str, object]) -> bool:
    stages = report.get("stages", [])
    if not isinstance(stages, list):
        return False

    for stage in stages:
        if not isinstance(stage, dict):
            continue
        if stage.get("kind") != "crewai":
            continue
        if stage.get("status") not in {"failed", "degraded"}:
            continue
        message = str(stage.get("message", "")).lower()
        if "model block cache" in message:
            return True
        cmd_results = stage.get("command_results", [])
        if isinstance(cmd_results, list):
            for cmd in cmd_results:
                if not isinstance(cmd, dict):
                    continue
                if int(cmd.get("return_code", 0)) == 2:
                    return True

        for cmd in cmd_results:
            if not isinstance(cmd, dict):
                continue
            log_path = cmd.get("log_path")
            if not isinstance(log_path, str) or not log_path:
                continue
            path = Path(log_path)
            if not path.is_absolute():
                path = repo_root / path
            if not path.exists():
                continue
            content = path.read_text(encoding="utf-8", errors="ignore").lower()
            if "your request was blocked" in content or "request was blocked" in content:
                return True
    return False


def _is_crewai_unavailable(repo_root: Path, report: Dict[str, object]) -> bool:
    stages = report.get("stages", [])
    if not isinstance(stages, list):
        return False

    patterns = [
        "crewai is not installed",
        "install with: pip install '.[crewai]'",
        "current python is unsupported",
    ]
    for stage in stages:
        if not isinstance(stage, dict):
            continue
        if stage.get("kind") != "crewai":
            continue
        if stage.get("status") not in {"failed", "degraded"}:
            continue
        message = str(stage.get("message", "")).lower()
        if any(item in message for item in patterns):
            return True

        cmd_results = stage.get("command_results", [])
        if not isinstance(cmd_results, list):
            continue
        for cmd in cmd_results:
            if not isinstance(cmd, dict):
                continue
            log_path = cmd.get("log_path")
            if not isinstance(log_path, str) or not log_path:
                continue
            path = Path(log_path)
            if not path.is_absolute():
                path = repo_root / path
            if not path.exists():
                continue
            content = path.read_text(encoding="utf-8", errors="ignore").lower()
            if any(item in content for item in patterns):
                return True
    return False


def _has_codex_timeout(report: Dict[str, object]) -> bool:
    stages = report.get("stages", [])
    if not isinstance(stages, list):
        return False
    for stage in stages:
        if not isinstance(stage, dict) or stage.get("kind") != "codex":
            continue
        cmd_results = stage.get("command_results", [])
        if not isinstance(cmd_results, list):
            continue
        for item in cmd_results:
            if not isinstance(item, dict):
                continue
            if int(item.get("return_code", 0)) == 124:
                return True
    return False


def _load_jsonl_records(path: Path) -> List[Dict[str, object]]:
    if not path.exists():
        return []
    records: List[Dict[str, object]] = []
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        if not line.strip():
            continue
        try:
            parsed = json.loads(line)
        except Exception:
            continue
        if isinstance(parsed, dict):
            records.append(parsed)
    return records


def _parse_metric_ts(value: object) -> float:
    if not isinstance(value, str) or not value.strip():
        return 0.0
    try:
        return datetime.fromisoformat(value).timestamp()
    except Exception:
        return 0.0


def _normalize_metrics_history(records: List[Dict[str, object]]) -> List[Dict[str, object]]:
    indexed: Dict[str, Tuple[float, int, Dict[str, object]]] = {}
    no_id: List[Tuple[float, int, Dict[str, object]]] = []
    for idx, item in enumerate(records):
        if not isinstance(item, dict):
            continue
        ts = _parse_metric_ts(item.get("ts"))
        cid = str(item.get("campaign_id", "")).strip()
        if not cid:
            no_id.append((ts, idx, item))
            continue
        prev = indexed.get(cid)
        if prev is None or (ts, idx) >= (prev[0], prev[1]):
            indexed[cid] = (ts, idx, item)

    merged: List[Tuple[float, int, Dict[str, object]]] = list(indexed.values()) + no_id
    merged.sort(key=lambda pair: (pair[0], pair[1]))
    return [item for _, _, item in merged]


def _rewrite_jsonl_records(path: Path, records: List[Dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for item in records:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")


def _compute_timeout_rate(item: Dict[str, object]) -> float:
    try:
        runs = int(item.get("runs", 0))
    except Exception:
        runs = 0
    if runs <= 0:
        return 0.0
    try:
        if "timeout_rate" in item:
            return float(item.get("timeout_rate", 0.0))
        codex_timeouts = int(item.get("codex_timeout_count", 0))
    except Exception:
        codex_timeouts = 0
    return max(0.0, min(1.0, codex_timeouts / runs))


def _compute_campaign_quality_score(success_rate: float, degraded_rate: float, timeout_rate: float) -> float:
    # 0..100 score prioritizing completion, then stability and latency risk proxy.
    score = 100.0 * (
        0.60 * max(0.0, min(1.0, success_rate))
        + 0.25 * (1.0 - max(0.0, min(1.0, degraded_rate)))
        + 0.15 * (1.0 - max(0.0, min(1.0, timeout_rate)))
    )
    return round(score, 3)


def _derive_adaptive_policy(history: List[Dict[str, object]]) -> Tuple[Dict[str, int], List[str]]:
    defaults = {
        "crew_blocked_threshold": 3,
        "crew_cooldown_rounds": 5,
        "codex_timeout_threshold": 3,
        "codex_cooldown_rounds": 4,
        "codex_timeout_seconds": 12,
    }
    if not history:
        return defaults, ["adaptive policy: no history, use defaults"]

    real_history = [item for item in history if isinstance(item, dict) and not bool(item.get("dry_run", False))]
    source = "real-history"
    if real_history:
        recent = real_history[-5:]
    else:
        recent = [item for item in history if isinstance(item, dict)][-5:]
        source = "mixed-history"
    if not recent:
        return defaults, ["adaptive policy: empty recent history, use defaults"]

    timeout_weighted = 0.0
    degraded_weighted = 0.0
    strict_weighted = 0.0
    weight_total = 0.0
    for item in recent:
        try:
            runs = int(item.get("runs", 0))
        except Exception:
            runs = 0
        weight = float(max(1, runs))
        timeout_value = _compute_timeout_rate(item)
        try:
            degraded_value = float(item.get("degraded_run_rate", 0.0))
        except Exception:
            degraded_value = 0.0
        try:
            if "strict_success_rate" in item:
                strict_value = float(item.get("strict_success_rate", 0.0))
            else:
                strict_value = max(
                    0.0, float(item.get("success_rate", 0.0)) - float(item.get("degraded_run_rate", 0.0))
                )
        except Exception:
            strict_value = 0.0

        timeout_weighted += timeout_value * weight
        degraded_weighted += degraded_value * weight
        strict_weighted += strict_value * weight
        weight_total += weight

    if weight_total <= 0:
        avg_timeout = 0.0
        avg_degraded = 0.0
        avg_strict = 0.0
    else:
        avg_timeout = timeout_weighted / weight_total
        avg_degraded = degraded_weighted / weight_total
        avg_strict = strict_weighted / weight_total

    if avg_timeout >= 0.50 and avg_strict <= 0.20:
        codex_timeout_threshold = 1
        codex_cooldown_rounds = 5
        codex_timeout_seconds = 5
    elif avg_timeout >= 0.75:
        codex_timeout_threshold = 2
        codex_cooldown_rounds = 4
        codex_timeout_seconds = 6
    elif avg_timeout >= 0.45:
        codex_timeout_threshold = 3
        codex_cooldown_rounds = 3
        codex_timeout_seconds = 8
    else:
        codex_timeout_threshold = 4
        codex_cooldown_rounds = 2
        codex_timeout_seconds = 12

    if avg_degraded >= 0.90 and avg_strict <= 0.10:
        crew_blocked_threshold = 1
        crew_cooldown_rounds = 8
    elif avg_degraded >= 0.80:
        crew_blocked_threshold = 2
        crew_cooldown_rounds = 6
    elif avg_degraded >= 0.50:
        crew_blocked_threshold = 3
        crew_cooldown_rounds = 5
    else:
        crew_blocked_threshold = 4
        crew_cooldown_rounds = 3

    policy = {
        "crew_blocked_threshold": crew_blocked_threshold,
        "crew_cooldown_rounds": crew_cooldown_rounds,
        "codex_timeout_threshold": codex_timeout_threshold,
        "codex_cooldown_rounds": codex_cooldown_rounds,
        "codex_timeout_seconds": codex_timeout_seconds,
    }
    actions = [
        (
            "adaptive policy from recent campaigns: "
            f"source={source}, weighted=true, avg_timeout_rate={avg_timeout:.3f}, "
            f"avg_degraded_rate={avg_degraded:.3f}, "
            f"avg_strict_success_rate={avg_strict:.3f}, "
            f"crew_threshold={crew_blocked_threshold}, crew_cooldown={crew_cooldown_rounds}, "
            f"codex_timeout_threshold={codex_timeout_threshold}, codex_cooldown={codex_cooldown_rounds}, "
            f"codex_timeout_seconds={codex_timeout_seconds}"
        )
    ]
    return policy, actions


def _metrics_scale_bucket(runs: int) -> str:
    if runs <= 3:
        return "micro"
    if runs <= 10:
        return "small"
    if runs <= 30:
        return "medium"
    if runs <= 100:
        return "large"
    return "xlarge"


def _choose_nearest_runs_baseline(
    candidates: List[Dict[str, object]],
    target_runs: int,
) -> Dict[str, object] | None:
    best_item: Dict[str, object] | None = None
    best_key: Tuple[int, int] | None = None
    for item in reversed(candidates):
        try:
            runs = int(item.get("runs", 0))
        except Exception:
            runs = 0
        key = (abs(runs - target_runs), 0 if runs >= target_runs else 1)
        if best_key is None or key < best_key:
            best_key = key
            best_item = item
    return best_item


def _select_metrics_baseline(
    history: List[Dict[str, object]],
    current: Dict[str, object],
) -> Tuple[Dict[str, object] | None, str]:
    cur_campaign_id = str(current.get("campaign_id", "")).strip()
    cur_goal = str(current.get("goal", "")).strip()
    cur_dry_run = bool(current.get("dry_run", False))
    try:
        cur_runs = int(current.get("runs", 0))
    except Exception:
        cur_runs = 0
    cur_bucket = _metrics_scale_bucket(cur_runs)

    candidates = [
        item
        for item in history
        if isinstance(item, dict)
        and str(item.get("campaign_id", "")).strip()
        and str(item.get("campaign_id", "")).strip() != cur_campaign_id
    ]
    if not candidates:
        return None, "no history baseline available"

    mode_candidates = [item for item in candidates if bool(item.get("dry_run", False)) == cur_dry_run]
    if mode_candidates:
        candidates = mode_candidates

    same_goal = [item for item in candidates if str(item.get("goal", "")).strip() == cur_goal]
    scoped = same_goal if same_goal else candidates
    scope_reason = "same-goal history" if same_goal else "cross-goal history"

    bucket_candidates = []
    for item in scoped:
        try:
            runs = int(item.get("runs", 0))
        except Exception:
            runs = 0
        if _metrics_scale_bucket(runs) == cur_bucket:
            bucket_candidates.append(item)

    if bucket_candidates:
        exact = []
        for item in bucket_candidates:
            try:
                runs = int(item.get("runs", 0))
            except Exception:
                runs = 0
            if runs == cur_runs:
                exact.append(item)
        if exact:
            return exact[-1], f"{scope_reason}; exact runs={cur_runs}"
        nearest = _choose_nearest_runs_baseline(bucket_candidates, cur_runs)
        if nearest is not None:
            return nearest, f"{scope_reason}; same scale bucket={cur_bucket} by nearest runs"

    nearest_scoped = _choose_nearest_runs_baseline(scoped, cur_runs)
    if nearest_scoped is not None:
        return nearest_scoped, f"{scope_reason}; nearest runs fallback"
    return None, "no comparable baseline found"


def _build_campaign_metrics(
    goal: str,
    campaign_id: str,
    report_paths: List[Path],
    dry_run: bool,
) -> Dict[str, object]:
    totals = {
        "runs": 0,
        "success_runs": 0,
        "strict_success_runs": 0,
        "failed_runs": 0,
        "degraded_stage_runs": 0,
    }
    stage_health: Dict[str, Dict[str, int]] = {}
    stage_time: Dict[str, float] = {}
    codex_timeout_count = 0

    for report_path in report_paths:
        report = load_json(report_path)
        totals["runs"] += 1
        if report.get("status") == "success":
            totals["success_runs"] += 1
        else:
            totals["failed_runs"] += 1

        stages = report.get("stages", [])
        degraded_in_run = False
        strict_success_in_run = True
        if isinstance(stages, list):
            for stage in stages:
                if not isinstance(stage, dict):
                    continue
                stage_id = str(stage.get("stage_id", "unknown"))
                status = str(stage.get("status", "unknown"))
                elapsed = float(stage.get("elapsed_seconds", 0.0))
                stat = stage_health.get(
                    stage_id,
                    {
                        "total": 0,
                        "success": 0,
                        "degraded": 0,
                        "failed": 0,
                        "skipped": 0,
                    },
                )
                stat["total"] += 1
                if status in stat:
                    stat[status] += 1
                stage_health[stage_id] = stat
                stage_time[stage_id] = stage_time.get(stage_id, 0.0) + elapsed
                if status == "degraded":
                    degraded_in_run = True
                if status in {"failed", "degraded", "manual"}:
                    strict_success_in_run = False

                cmd_results = stage.get("command_results", [])
                if isinstance(cmd_results, list):
                    for cmd in cmd_results:
                        if not isinstance(cmd, dict):
                            continue
                        if int(cmd.get("return_code", 0)) == 124:
                            codex_timeout_count += 1

        if degraded_in_run:
            totals["degraded_stage_runs"] += 1
        if strict_success_in_run:
            totals["strict_success_runs"] += 1

    avg_stage_seconds: Dict[str, float] = {}
    for stage_id, total_elapsed in stage_time.items():
        count = stage_health.get(stage_id, {}).get("total", 0)
        avg_stage_seconds[stage_id] = (total_elapsed / count) if count else 0.0

    model_cache_path = report_paths[-1].parents[2] / "memory" / "model_availability.json" if report_paths else None
    blocked_models = []
    if model_cache_path and model_cache_path.exists():
        model_cache = load_json(model_cache_path)
        models = model_cache.get("models", {})
        if isinstance(models, dict):
            now_ts = datetime.now(timezone.utc).timestamp()
            for name, item in models.items():
                if isinstance(item, dict) and float(item.get("blocked_until", 0)) > now_ts:
                    blocked_models.append(str(name))

    success_rate = (totals["success_runs"] / totals["runs"]) if totals["runs"] else 0.0
    strict_success_rate = (totals["strict_success_runs"] / totals["runs"]) if totals["runs"] else 0.0
    degraded_rate = (totals["degraded_stage_runs"] / totals["runs"]) if totals["runs"] else 0.0
    timeout_rate = (codex_timeout_count / totals["runs"]) if totals["runs"] else 0.0
    quality_score = _compute_campaign_quality_score(success_rate, degraded_rate, timeout_rate)
    return {
        "ts": _utc_now(),
        "goal": goal,
        "campaign_id": campaign_id,
        "dry_run": bool(dry_run),
        "runs": totals["runs"],
        "success_runs": totals["success_runs"],
        "strict_success_runs": totals["strict_success_runs"],
        "failed_runs": totals["failed_runs"],
        "success_rate": success_rate,
        "strict_success_rate": strict_success_rate,
        "degraded_run_rate": degraded_rate,
        "codex_timeout_count": codex_timeout_count,
        "timeout_rate": timeout_rate,
        "quality_score": quality_score,
        "stage_health": stage_health,
        "avg_stage_seconds": avg_stage_seconds,
        "blocked_models_active": blocked_models,
    }


def _build_metrics_diff(previous: Dict[str, object], current: Dict[str, object]) -> Dict[str, object]:
    def _num(value: object) -> float:
        try:
            return float(value)
        except Exception:
            return 0.0

    def _timeout_rate(payload: Dict[str, object]) -> float:
        if "timeout_rate" in payload:
            return _num(payload.get("timeout_rate", 0.0))
        try:
            runs = int(payload.get("runs", 0))
        except Exception:
            runs = 0
        if runs <= 0:
            return 0.0
        try:
            timeouts = int(payload.get("codex_timeout_count", 0))
        except Exception:
            timeouts = 0
        return max(0.0, min(1.0, timeouts / runs))

    def _quality_score(payload: Dict[str, object]) -> float:
        if "quality_score" in payload:
            return _num(payload.get("quality_score", 0.0))
        return _compute_campaign_quality_score(
            success_rate=_num(payload.get("success_rate", 0.0)),
            degraded_rate=_num(payload.get("degraded_run_rate", 0.0)),
            timeout_rate=_timeout_rate(payload),
        )

    def _strict_success_runs(payload: Dict[str, object]) -> int:
        if "strict_success_runs" in payload:
            try:
                return int(payload.get("strict_success_runs", 0))
            except Exception:
                return 0
        try:
            runs = int(payload.get("runs", 0))
        except Exception:
            runs = 0
        if runs <= 0:
            return 0
        if "strict_success_rate" in payload:
            rate = _num(payload.get("strict_success_rate", 0.0))
            return max(0, min(runs, int(round(max(0.0, min(1.0, rate)) * runs))))
        success_rate = _num(payload.get("success_rate", 0.0))
        degraded_rate = _num(payload.get("degraded_run_rate", 0.0))
        estimated_rate = max(0.0, min(1.0, success_rate - degraded_rate))
        return max(0, min(runs, int(round(estimated_rate * runs))))

    def _strict_success_rate(payload: Dict[str, object]) -> float:
        if "strict_success_rate" in payload:
            return _num(payload.get("strict_success_rate", 0.0))
        try:
            runs = int(payload.get("runs", 0))
        except Exception:
            runs = 0
        if runs <= 0:
            return 0.0
        return _strict_success_runs(payload) / runs

    stage_delta: Dict[str, Dict[str, int]] = {}
    current_stage = current.get("stage_health", {})
    prev_stage = previous.get("stage_health", {})
    if isinstance(current_stage, dict) and isinstance(prev_stage, dict):
        for stage_id in sorted(set(current_stage.keys()) | set(prev_stage.keys())):
            cur = current_stage.get(stage_id, {})
            prv = prev_stage.get(stage_id, {})
            if not isinstance(cur, dict):
                cur = {}
            if not isinstance(prv, dict):
                prv = {}
            stage_delta[stage_id] = {
                "total": int(cur.get("total", 0)) - int(prv.get("total", 0)),
                "success": int(cur.get("success", 0)) - int(prv.get("success", 0)),
                "degraded": int(cur.get("degraded", 0)) - int(prv.get("degraded", 0)),
                "failed": int(cur.get("failed", 0)) - int(prv.get("failed", 0)),
                "skipped": int(cur.get("skipped", 0)) - int(prv.get("skipped", 0)),
            }

    prev_blocked = previous.get("blocked_models_active", [])
    cur_blocked = current.get("blocked_models_active", [])
    prev_set = {str(item) for item in prev_blocked} if isinstance(prev_blocked, list) else set()
    cur_set = {str(item) for item in cur_blocked} if isinstance(cur_blocked, list) else set()

    return {
        "ts": _utc_now(),
        "goal": current.get("goal", ""),
        "campaign_id": current.get("campaign_id", ""),
        "base_campaign_id": previous.get("campaign_id", ""),
        "delta": {
            "runs": int(_num(current.get("runs", 0)) - _num(previous.get("runs", 0))),
            "success_runs": int(_num(current.get("success_runs", 0)) - _num(previous.get("success_runs", 0))),
            "strict_success_runs": _strict_success_runs(current) - _strict_success_runs(previous),
            "failed_runs": int(_num(current.get("failed_runs", 0)) - _num(previous.get("failed_runs", 0))),
            "success_rate": _num(current.get("success_rate", 0.0)) - _num(previous.get("success_rate", 0.0)),
            "strict_success_rate": _strict_success_rate(current) - _strict_success_rate(previous),
            "degraded_run_rate": _num(current.get("degraded_run_rate", 0.0)) - _num(previous.get("degraded_run_rate", 0.0)),
            "timeout_rate": _timeout_rate(current) - _timeout_rate(previous),
            "codex_timeout_count": int(
                _num(current.get("codex_timeout_count", 0)) - _num(previous.get("codex_timeout_count", 0))
            ),
            "quality_score": _quality_score(current) - _quality_score(previous),
        },
        "stage_health_delta": stage_delta,
        "blocked_models_added": sorted(cur_set - prev_set),
        "blocked_models_removed": sorted(prev_set - cur_set),
    }


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
                        "enabled": True,
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
                if isinstance(item, dict) and str(item.get("id", "")).startswith("codex_fallback") and "enabled" not in item:
                    item["enabled"] = True
                    actions.append("set codex_fallback as enabled")

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
    campaign_id: str,
) -> None:
    report = load_json(report_path)
    payload = {
        "ts": _utc_now(),
        "campaign_id": campaign_id,
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
    metrics_path = wf_root / "memory" / "autopilot_metrics_latest.json"
    metrics_diff_path = wf_root / "memory" / "autopilot_metrics_diff_latest.json"
    metrics_history_path = wf_root / "memory" / "autopilot_metrics_history.jsonl"
    policy_path = wf_root / "memory" / "autopilot_policy_latest.json"
    previous_metrics: Dict[str, object] = {}
    if metrics_path.exists():
        previous_metrics = load_json(metrics_path)

    done = 0
    success_count = 0
    last_report_path = None
    campaign_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    campaign_reports: List[Path] = []
    state = _load_autopilot_state(root)
    raw_history_records = _load_jsonl_records(metrics_history_path)
    history_records = _normalize_metrics_history(raw_history_records)
    if len(history_records) != len(raw_history_records):
        _rewrite_jsonl_records(metrics_history_path, history_records)
    adaptive_policy, adaptive_actions = _derive_adaptive_policy(history_records)
    codex_timeout_events = 0
    timeout_seconds_overridden_by_env = "CODEX_WORKFLOW_CODEX_TIMEOUT_SECONDS" in os.environ
    try:
        blocked_threshold = int(os.getenv("CODEX_WORKFLOW_CREWAI_BLOCKED_THRESHOLD", "3"))
    except ValueError:
        blocked_threshold = 3
    try:
        cooldown_rounds = int(os.getenv("CODEX_WORKFLOW_CREWAI_COOLDOWN_ROUNDS", "5"))
    except ValueError:
        cooldown_rounds = 5
    try:
        codex_timeout_threshold = int(os.getenv("CODEX_WORKFLOW_CODEX_TIMEOUT_THRESHOLD", "3"))
    except ValueError:
        codex_timeout_threshold = 3
    try:
        codex_cooldown_rounds = int(os.getenv("CODEX_WORKFLOW_CODEX_COOLDOWN_ROUNDS", "4"))
    except ValueError:
        codex_cooldown_rounds = 4
    try:
        codex_timeout_seconds = int(os.getenv("CODEX_WORKFLOW_CODEX_TIMEOUT_SECONDS", "180"))
    except ValueError:
        codex_timeout_seconds = 180

    if "CODEX_WORKFLOW_CREWAI_BLOCKED_THRESHOLD" not in os.environ:
        blocked_threshold = int(adaptive_policy.get("crew_blocked_threshold", blocked_threshold))
    if "CODEX_WORKFLOW_CREWAI_COOLDOWN_ROUNDS" not in os.environ:
        cooldown_rounds = int(adaptive_policy.get("crew_cooldown_rounds", cooldown_rounds))
    if "CODEX_WORKFLOW_CODEX_TIMEOUT_THRESHOLD" not in os.environ:
        codex_timeout_threshold = int(adaptive_policy.get("codex_timeout_threshold", codex_timeout_threshold))
    if "CODEX_WORKFLOW_CODEX_COOLDOWN_ROUNDS" not in os.environ:
        codex_cooldown_rounds = int(adaptive_policy.get("codex_cooldown_rounds", codex_cooldown_rounds))
    if not timeout_seconds_overridden_by_env:
        codex_timeout_seconds = int(adaptive_policy.get("codex_timeout_seconds", codex_timeout_seconds))

    state["crew_blocked_threshold"] = max(1, blocked_threshold)
    state["crew_cooldown_rounds"] = max(1, cooldown_rounds)
    state["codex_timeout_threshold"] = max(1, codex_timeout_threshold)
    state["codex_cooldown_rounds"] = max(1, codex_cooldown_rounds)
    codex_timeout_seconds = max(5, codex_timeout_seconds)

    dump_json(
        policy_path,
        {
            "ts": _utc_now(),
            "goal": goal,
            "history_window": min(5, len(history_records)),
            "adaptive_policy": adaptive_policy,
            "effective_policy": {
                "crew_blocked_threshold": state["crew_blocked_threshold"],
                "crew_cooldown_rounds": state["crew_cooldown_rounds"],
                "codex_timeout_threshold": state["codex_timeout_threshold"],
                "codex_cooldown_rounds": state["codex_cooldown_rounds"],
                "codex_timeout_seconds": codex_timeout_seconds,
            },
            "env_overrides": {
                "CODEX_WORKFLOW_CREWAI_BLOCKED_THRESHOLD": "CODEX_WORKFLOW_CREWAI_BLOCKED_THRESHOLD" in os.environ,
                "CODEX_WORKFLOW_CREWAI_COOLDOWN_ROUNDS": "CODEX_WORKFLOW_CREWAI_COOLDOWN_ROUNDS" in os.environ,
                "CODEX_WORKFLOW_CODEX_TIMEOUT_THRESHOLD": "CODEX_WORKFLOW_CODEX_TIMEOUT_THRESHOLD" in os.environ,
                "CODEX_WORKFLOW_CODEX_COOLDOWN_ROUNDS": "CODEX_WORKFLOW_CODEX_COOLDOWN_ROUNDS" in os.environ,
                "CODEX_WORKFLOW_CODEX_TIMEOUT_SECONDS": "CODEX_WORKFLOW_CODEX_TIMEOUT_SECONDS" in os.environ,
            },
        },
    )
    campaign_boot_actions = list(adaptive_actions)
    if not timeout_seconds_overridden_by_env:
        os.environ["CODEX_WORKFLOW_CODEX_TIMEOUT_SECONDS"] = str(codex_timeout_seconds)
        campaign_boot_actions.append(
            f"set runtime CODEX_WORKFLOW_CODEX_TIMEOUT_SECONDS={codex_timeout_seconds} from adaptive policy"
        )

    for idx in range(1, iterations + 1):
        pre_actions: List[str] = []
        post_actions: List[str] = []
        if idx == 1 and campaign_boot_actions:
            pre_actions.extend(campaign_boot_actions)
        crew_cooldown_before = int(state.get("crew_cooldown_remaining", 0))
        crew_enabled = crew_cooldown_before <= 0
        pre_actions.extend(_set_crewai_stage_enabled(root, enabled=crew_enabled))
        if not crew_enabled:
            pre_actions.append(f"crewai cooldown active ({crew_cooldown_before} iterations remaining)")

        codex_cooldown_before = int(state.get("codex_cooldown_remaining", 0))
        codex_enabled = codex_cooldown_before <= 0
        pre_actions.extend(_set_codex_fallback_stage_enabled(root, enabled=codex_enabled))
        if not codex_enabled:
            pre_actions.append(f"codex timeout cooldown active ({codex_cooldown_before} iterations remaining)")

        run_goal = f"{goal} [iteration {idx}/{iterations}]"
        report_path = run_workflow(
            repo_root=root,
            goal=run_goal,
            dry_run=dry_run,
            enable_codex=enable_codex,
            evolve_after_run=False,
        )
        report = load_json(report_path)
        crew_blocked = _is_crewai_blocked(root, report)
        crew_unavailable = _is_crewai_unavailable(root, report)
        codex_timeout = _has_codex_timeout(report)
        if codex_timeout:
            codex_timeout_events += 1
        if crew_blocked:
            post_actions.append("detected crewai blocked response")
        if crew_unavailable:
            post_actions.append("detected crewai runtime unavailable")
        if codex_timeout:
            post_actions.append("detected codex timeout")

        if crew_unavailable:
            crew_blocked = True

        if codex_timeout and int(state.get("codex_timeout_threshold", 3)) > 1:
            state["codex_timeout_threshold"] = 1
            state["codex_cooldown_rounds"] = max(5, int(state.get("codex_cooldown_rounds", 4)))
            post_actions.append("fast-tightened codex timeout policy after immediate timeout")
            if not timeout_seconds_overridden_by_env:
                if int(os.getenv("CODEX_WORKFLOW_CODEX_TIMEOUT_SECONDS", "180")) > 5:
                    os.environ["CODEX_WORKFLOW_CODEX_TIMEOUT_SECONDS"] = "5"
                    post_actions.append(
                        "fast-tightened CODEX_WORKFLOW_CODEX_TIMEOUT_SECONDS=5 after immediate timeout"
                    )

        if idx >= 2:
            live_timeout_rate = codex_timeout_events / idx
            if live_timeout_rate >= 0.35 and int(state.get("codex_timeout_threshold", 3)) > 1:
                state["codex_timeout_threshold"] = 1
                state["codex_cooldown_rounds"] = max(5, int(state.get("codex_cooldown_rounds", 4)))
                post_actions.append(
                    "tightened codex timeout policy in-flight due to high live timeout rate"
                )
                if not timeout_seconds_overridden_by_env:
                    if int(os.getenv("CODEX_WORKFLOW_CODEX_TIMEOUT_SECONDS", "180")) > 5:
                        os.environ["CODEX_WORKFLOW_CODEX_TIMEOUT_SECONDS"] = "5"
                        post_actions.append(
                            "tightened CODEX_WORKFLOW_CODEX_TIMEOUT_SECONDS=5 in-flight"
                        )

        if crew_enabled:
            if crew_blocked:
                state["crew_blocked_streak"] = int(state.get("crew_blocked_streak", 0)) + 1
            else:
                state["crew_blocked_streak"] = 0

            if int(state.get("crew_blocked_streak", 0)) >= int(state.get("crew_blocked_threshold", 3)):
                state["crew_cooldown_remaining"] = int(state.get("crew_cooldown_rounds", 5))
                state["crew_blocked_streak"] = 0
                post_actions.append(
                    "entered crewai cooldown after repeated blocked responses"
                )
            if crew_unavailable:
                state["crew_cooldown_remaining"] = max(
                    int(state.get("crew_cooldown_remaining", 0)),
                    int(state.get("crew_cooldown_rounds", 5)) + 2,
                )
                state["crew_blocked_streak"] = 0
                post_actions.append("extended crewai cooldown due to missing runtime")
        else:
            state["crew_blocked_streak"] = 0
            state["crew_cooldown_remaining"] = max(0, crew_cooldown_before - 1)
            if int(state.get("crew_cooldown_remaining", 0)) == 0:
                post_actions.append("crewai cooldown completed")

        if codex_enabled:
            if codex_timeout:
                state["codex_timeout_streak"] = int(state.get("codex_timeout_streak", 0)) + 1
            else:
                state["codex_timeout_streak"] = 0

            if int(state.get("codex_timeout_streak", 0)) >= int(state.get("codex_timeout_threshold", 3)):
                state["codex_cooldown_remaining"] = int(state.get("codex_cooldown_rounds", 4))
                state["codex_timeout_streak"] = 0
                post_actions.append("entered codex timeout cooldown after repeated timeouts")
        else:
            state["codex_timeout_streak"] = 0
            state["codex_cooldown_remaining"] = max(0, codex_cooldown_before - 1)
            if int(state.get("codex_cooldown_remaining", 0)) == 0:
                post_actions.append("codex timeout cooldown completed")

        evolve(repo_root=root)
        actions = pre_actions + _adapt_workflow_after_report(root, report, idx) + post_actions
        _save_autopilot_state(root, state)
        _record_iteration(root, idx, goal, report_path, actions, campaign_id=campaign_id)

        done = idx
        last_report_path = str(report_path)
        campaign_reports.append(report_path)
        if report.get("status") == "success":
            success_count += 1

        if until_success and idx >= max(1, min_iterations) and report.get("status") == "success":
            break

    summary = {
        "ts": _utc_now(),
        "goal": goal,
        "campaign_id": campaign_id,
        "iterations_requested": iterations,
        "iterations_completed": done,
        "dry_run": dry_run,
        "until_success": until_success,
        "success_count": success_count,
        "success_rate": (success_count / done) if done else 0.0,
        "last_report_path": last_report_path,
    }
    dump_json(summary_path, summary)

    metrics = _build_campaign_metrics(goal=goal, campaign_id=campaign_id, report_paths=campaign_reports, dry_run=dry_run)
    dump_json(metrics_path, metrics)
    summary["strict_success_count"] = int(metrics.get("strict_success_runs", 0) or 0)
    summary["strict_success_rate"] = float(metrics.get("strict_success_rate", 0.0) or 0.0)
    dump_json(summary_path, summary)

    updated_history = _normalize_metrics_history(history_records + [metrics])
    _rewrite_jsonl_records(metrics_history_path, updated_history)

    baseline, baseline_reason = _select_metrics_baseline(history_records, metrics)
    if baseline is None and previous_metrics:
        baseline = previous_metrics
        baseline_reason = "fallback to previous latest metrics snapshot"

    if baseline:
        metrics_diff = _build_metrics_diff(baseline, metrics)
        metrics_diff["baseline_reason"] = baseline_reason
        metrics_diff["baseline_runs"] = int(baseline.get("runs", 0) or 0)
        dump_json(metrics_diff_path, metrics_diff)
    return summary_path
