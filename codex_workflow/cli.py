from __future__ import annotations

import argparse
from pathlib import Path

from .autopilot import iterate_goal
from .bootstrap import bootstrap
from .crewai_blueprint import write_example_script
from .evolution import evolve
from .hooks import install_pre_push_hook
from .runner import run_workflow
from .utils import load_json


def _cmd_bootstrap(args: argparse.Namespace) -> int:
    outputs = bootstrap(
        target=Path(args.target),
        project_name=args.project_name,
        force=bool(args.force),
    )
    print("Bootstrap completed:")
    for key, value in outputs.items():
        print(f"- {key}: {value}")
    return 0


def _cmd_run(args: argparse.Namespace) -> int:
    report_path = run_workflow(
        repo_root=Path(args.target).resolve(),
        goal=args.goal,
        dry_run=bool(args.dry_run),
        enable_codex=bool(args.enable_codex),
        evolve_after_run=not bool(args.no_evolve),
    )
    report = load_json(report_path)
    print(f"Run report: {report_path}")
    return 0 if report.get("status") == "success" else 1


def _cmd_iterate(args: argparse.Namespace) -> int:
    summary_path = iterate_goal(
        repo_root=Path(args.target).resolve(),
        goal=args.goal,
        iterations=int(args.iterations),
        dry_run=bool(args.dry_run),
        enable_codex=bool(args.enable_codex),
        project_name=args.project_name,
        until_success=bool(args.until_success),
        min_iterations=int(args.min_iterations),
    )
    summary = load_json(summary_path)
    print(f"Autopilot summary: {summary_path}")
    print(
        "iterations_completed="
        f"{summary.get('iterations_completed')} success_rate={summary.get('success_rate')}"
    )
    if bool(args.until_success):
        return 0 if int(summary.get("success_count", 0)) > 0 else 1
    return 0


def _cmd_evolve(args: argparse.Namespace) -> int:
    playbook_path = evolve(
        repo_root=Path(args.target).resolve(),
        lookback=args.lookback,
    )
    print(f"Playbook updated: {playbook_path}")
    return 0


def _cmd_crewai_example(args: argparse.Namespace) -> int:
    path = write_example_script(Path(args.target).resolve())
    print(f"CrewAI example script generated: {path}")
    return 0


def _cmd_install_hook(args: argparse.Namespace) -> int:
    toolkit_root = Path(args.toolkit_root).resolve() if args.toolkit_root else Path(__file__).resolve().parents[1]
    hook_path = install_pre_push_hook(
        repo_root=Path(args.target).resolve(),
        toolkit_root=toolkit_root,
        goal=args.goal,
        force=bool(args.force),
        no_evolve=not bool(args.evolve),
    )
    print(f"Installed pre-push hook: {hook_path}")
    print("Use CODEX_WORKFLOW_GOAL env var to override goal per push.")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="codex-workflow",
        description="Reusable Codex-assisted multi-agent engineering workflow",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_bootstrap = sub.add_parser("bootstrap", help="Initialize .codex-workflow in a project")
    p_bootstrap.add_argument("--target", default=".", help="Project root path")
    p_bootstrap.add_argument("--project-name", default="project", help="Project name")
    p_bootstrap.add_argument("--force", action="store_true", help="Overwrite existing files")
    p_bootstrap.set_defaults(func=_cmd_bootstrap)

    p_run = sub.add_parser("run", help="Run workflow stages")
    p_run.add_argument("--target", default=".", help="Project root path")
    p_run.add_argument("--goal", required=True, help="Task goal")
    p_run.add_argument("--dry-run", action="store_true", help="Do not execute commands")
    p_run.add_argument("--enable-codex", action="store_true", help="Execute codex stages")
    p_run.add_argument("--no-evolve", action="store_true", help="Skip post-run evolution")
    p_run.set_defaults(func=_cmd_run)

    p_iter = sub.add_parser("iterate", help="Autopilot loop: run/evolve/improve in multiple iterations")
    p_iter.add_argument("--target", default=".", help="Project root path")
    p_iter.add_argument("--goal", required=True, help="Single goal for autonomous iteration")
    p_iter.add_argument("--iterations", type=int, default=100, help="Maximum iteration count")
    p_iter.add_argument("--min-iterations", type=int, default=1, help="Minimum iterations before early-stop")
    p_iter.add_argument("--until-success", action="store_true", help="Stop early after success (>= min-iterations)")
    p_iter.add_argument("--dry-run", action="store_true", help="Run each iteration in dry-run mode")
    p_iter.add_argument("--enable-codex", action="store_true", help="Enable codex stage if workflow contains it")
    p_iter.add_argument("--project-name", default="project", help="Project name used when auto-bootstrap is needed")
    p_iter.set_defaults(func=_cmd_iterate)

    p_evolve = sub.add_parser("evolve", help="Summarize runs and update playbook")
    p_evolve.add_argument("--target", default=".", help="Project root path")
    p_evolve.add_argument("--lookback", type=int, default=None, help="Analyze latest N runs")
    p_evolve.set_defaults(func=_cmd_evolve)

    p_crewai = sub.add_parser("crewai-example", help="Generate optional CrewAI starter script")
    p_crewai.add_argument("--target", default=".", help="Output directory")
    p_crewai.set_defaults(func=_cmd_crewai_example)

    p_hook = sub.add_parser("install-hook", help="Install a managed pre-push hook")
    p_hook.add_argument("--target", default=".", help="Git repository root")
    p_hook.add_argument("--goal", default="pre-push quality gate", help="Default goal text for hook")
    p_hook.add_argument("--evolve", action="store_true", help="Run evolve after each pre-push run")
    p_hook.add_argument("--force", action="store_true", help="Overwrite existing pre-push hook")
    p_hook.add_argument(
        "--toolkit-root",
        default=None,
        help="Path to codex-workflow-kit root (defaults to current package root)",
    )
    p_hook.set_defaults(func=_cmd_install_hook)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
