from __future__ import annotations

from pathlib import Path


def _require_crewai() -> None:
    try:
        import crewai  # noqa: F401
    except Exception as exc:  # pragma: no cover - optional dependency
        raise RuntimeError(
            "CrewAI is not installed. Install with: pip install '.[crewai]'"
        ) from exc


def build_default_crew(goal: str):
    """Create a minimal crew for code tasks.

    This helper is optional and only used when CrewAI is installed.
    """
    _require_crewai()
    from crewai import Agent, Crew, Process, Task  # type: ignore

    planner = Agent(
        role="planner",
        goal="Create the smallest safe implementation plan",
        backstory="Senior engineer focused on minimal diff and clear validation steps.",
        verbose=False,
    )
    coder = Agent(
        role="coder",
        goal="Implement changes that satisfy requirements with minimal risk",
        backstory="Hands-on engineer that follows repository constraints strictly.",
        verbose=False,
    )
    tester = Agent(
        role="tester",
        goal="Run quality gates and produce deterministic pass/fail evidence",
        backstory="Quality engineer for build/test gates.",
        verbose=False,
    )
    reviewer = Agent(
        role="reviewer",
        goal="Review risk, regressions and testing gaps before merge",
        backstory="Strict reviewer prioritizing correctness and maintainability.",
        verbose=False,
    )

    tasks = [
        Task(
            description=f"Create implementation plan for: {goal}",
            expected_output="Plan with risk list and verification commands",
            agent=planner,
        ),
        Task(
            description="Implement the plan with minimal changes.",
            expected_output="Patch summary with files changed",
            agent=coder,
        ),
        Task(
            description="Run build and tests, report exact failures if any.",
            expected_output="Quality gate report",
            agent=tester,
        ),
        Task(
            description="Review regression risk and decide merge readiness.",
            expected_output="Review verdict and required follow-ups",
            agent=reviewer,
        ),
    ]

    return Crew(agents=[planner, coder, tester, reviewer], tasks=tasks, process=Process.sequential)


def write_example_script(target: Path) -> Path:
    script = target / "crew_pipeline_example.py"
    script.write_text(
        "from codex_workflow.crewai_blueprint import build_default_crew\n\n"
        "if __name__ == '__main__':\n"
        "    crew = build_default_crew(goal='实现并验证一个最小可用变更')\n"
        "    result = crew.kickoff()\n"
        "    print(result)\n",
        encoding="utf-8",
    )
    return script
