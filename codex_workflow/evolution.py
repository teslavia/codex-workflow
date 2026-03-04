from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple

from .utils import append_jsonl, dump_json, load_json


def _scan_reports(run_dir: Path, lookback: int) -> List[Path]:
    paths = sorted(run_dir.glob("*/run_report.json"))
    if lookback <= 0:
        return paths
    return paths[-lookback:]


def _classify_failure(command: str) -> str:
    cmd = command.lower()
    if "ctest" in cmd:
        return "test_failure"
    if "pytest" in cmd:
        return "python_test_failure"
    if "cmake --build" in cmd:
        return "build_failure"
    if "cmake -b" in cmd or "cmake -s" in cmd:
        return "configure_failure"
    if "codex" in cmd:
        return "codex_stage_failure"
    return "other_failure"


def _build_recommendations(
    stage_stats: Dict[str, Dict[str, int]],
    failure_types: Counter,
    runs_count: int,
) -> List[str]:
    recs: List[str] = []
    if runs_count == 0:
        return recs

    for stage_id, stats in sorted(stage_stats.items()):
        failure_rate = stats["failed"] / float(stats["total"])
        if failure_rate >= 0.30:
            recs.append(
                f"阶段 `{stage_id}` 失败率 {failure_rate:.0%}，建议在该阶段前置输入校验并减少任务粒度。"
            )

    if failure_types.get("build_failure", 0) > 0:
        recs.append("高频构建失败：在 implement 阶段结束后先做一次局部编译再进入全量验证。")
    if failure_types.get("test_failure", 0) > 0:
        recs.append("高频测试失败：把失败测试名写入 prompt，要求 coder 先复现再修复。")
    if failure_types.get("codex_stage_failure", 0) > 0:
        recs.append("Codex 执行失败：检查 workflow.json 中 codex.command 与本机 CLI 参数是否一致。")

    if not recs:
        recs.append("近几次运行整体稳定，建议继续保持当前 workflow 配置。")
    return recs


def _render_playbook(
    generated_at: str,
    runs_count: int,
    success_runs: int,
    stage_stats: Dict[str, Dict[str, int]],
    top_failures: List[Tuple[str, int]],
    recommendations: List[str],
) -> str:
    lines: List[str] = []
    lines.append("# Codex Workflow Playbook")
    lines.append("")
    lines.append(f"Last updated: {generated_at}")
    lines.append("")
    lines.append("## Summary")
    lines.append(f"- Runs analyzed: {runs_count}")
    lines.append(f"- Success runs: {success_runs}")
    ratio = (success_runs / runs_count * 100.0) if runs_count else 0.0
    lines.append(f"- Success rate: {ratio:.1f}%")
    lines.append("")
    lines.append("## Stage Health")
    for stage_id, stats in sorted(stage_stats.items()):
        fail_rate = (stats["failed"] / stats["total"] * 100.0) if stats["total"] else 0.0
        lines.append(
            f"- {stage_id}: total={stats['total']}, failed={stats['failed']}, failure_rate={fail_rate:.1f}%"
        )
    lines.append("")
    lines.append("## Top Failure Types")
    if top_failures:
        for name, count in top_failures:
            lines.append(f"- {name}: {count}")
    else:
        lines.append("- none")
    lines.append("")
    lines.append("## Stable Rules")
    for item in recommendations:
        lines.append(f"- {item}")
    lines.append("")
    return "\n".join(lines)


def evolve(repo_root: Path, lookback: int | None = None) -> Path:
    wf_root = repo_root / ".codex-workflow"
    evo_cfg = load_json(wf_root / "evolution.json")
    lookback_runs = int(lookback if lookback is not None else evo_cfg.get("lookback_runs", 30))

    report_paths = _scan_reports(wf_root / "runs", lookback_runs)

    stage_stats: Dict[str, Dict[str, int]] = defaultdict(lambda: {"total": 0, "failed": 0})
    failure_types: Counter = Counter()
    success_runs = 0

    for path in report_paths:
        report = load_json(path)
        if report.get("status") == "success":
            success_runs += 1

        for stage in report.get("stages", []):
            stage_id = str(stage.get("stage_id", "unknown"))
            status = str(stage.get("status", "unknown"))
            stage_stats[stage_id]["total"] += 1
            if status == "failed":
                stage_stats[stage_id]["failed"] += 1

            for cmd in stage.get("command_results", []):
                if int(cmd.get("return_code", 0)) != 0:
                    failure_types[_classify_failure(str(cmd.get("command", "")))] += 1

    runs_count = len(report_paths)
    recommendations = _build_recommendations(stage_stats, failure_types, runs_count)
    generated_at = datetime.now(timezone.utc).isoformat()
    top_failures = failure_types.most_common(5)

    summary = {
        "generated_at": generated_at,
        "runs_analyzed": runs_count,
        "success_runs": success_runs,
        "top_failure_types": [{"name": name, "count": count} for name, count in top_failures],
        "recommendations": recommendations,
    }

    append_jsonl(wf_root / "memory" / "lessons.jsonl", summary)
    dump_json(wf_root / "memory" / "latest_summary.json", summary)

    playbook_text = _render_playbook(
        generated_at=generated_at,
        runs_count=runs_count,
        success_runs=success_runs,
        stage_stats=stage_stats,
        top_failures=top_failures,
        recommendations=recommendations,
    )
    playbook_path = wf_root / "memory" / "playbook.md"
    playbook_path.write_text(playbook_text, encoding="utf-8")
    return playbook_path
