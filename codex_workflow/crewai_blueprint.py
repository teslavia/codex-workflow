from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict, Optional

try:  # pragma: no cover - python version dependent
    import tomllib  # type: ignore[attr-defined]
except Exception:  # pragma: no cover
    tomllib = None  # type: ignore[assignment]


def _require_crewai() -> None:
    try:
        import crewai  # noqa: F401
    except Exception as exc:  # pragma: no cover - optional dependency
        raise RuntimeError(
            "CrewAI is not installed (or current Python is unsupported). "
            "Install with: pip install '.[crewai]' using Python 3.10-3.13"
        ) from exc


def _load_codex_config() -> Dict[str, object]:
    config_path = Path.home() / ".codex" / "config.toml"
    if not config_path.exists() or tomllib is None:
        return {}
    try:
        return tomllib.loads(config_path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _load_codex_auth() -> Dict[str, object]:
    auth_path = Path.home() / ".codex" / "auth.json"
    if not auth_path.exists():
        return {}
    try:
        data = json.loads(auth_path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _resolve_base_url(config: Dict[str, object]) -> Optional[str]:
    provider_name = config.get("model_provider")
    providers = config.get("model_providers")
    if not isinstance(provider_name, str) or not isinstance(providers, dict):
        return None

    provider_cfg = providers.get(provider_name)
    if not isinstance(provider_cfg, dict):
        return None

    base_url = provider_cfg.get("base_url")
    return base_url if isinstance(base_url, str) and base_url.strip() else None


def resolve_codex_llm_runtime(apply_env: bool = True) -> Dict[str, str]:
    """Resolve model/base_url/key from ~/.codex files.

    Returns only non-sensitive fields. API key value is never returned.
    """
    config = _load_codex_config()
    auth = _load_codex_auth()

    model = config.get("model") if isinstance(config.get("model"), str) else None
    base_url = _resolve_base_url(config)

    api_key = auth.get("OPENAI_API_KEY")
    if not isinstance(api_key, str) or not api_key.strip():
        api_key = None

    if apply_env:
        if api_key:
            os.environ["OPENAI_API_KEY"] = api_key
        if base_url:
            os.environ["OPENAI_BASE_URL"] = base_url
            os.environ["OPENAI_API_BASE"] = base_url
        if model:
            os.environ["OPENAI_MODEL_NAME"] = model

    resolved: Dict[str, str] = {}
    if model:
        resolved["model"] = model
    if base_url:
        resolved["base_url"] = base_url
    resolved["api_key"] = "present" if api_key else "missing"
    return resolved


def build_default_crew(goal: str):
    """Create a minimal crew for code tasks.

    This helper is optional and only used when CrewAI is installed.
    It auto-loads model runtime from ~/.codex/config.toml and ~/.codex/auth.json.
    """
    _require_crewai()
    runtime = resolve_codex_llm_runtime(apply_env=True)

    from crewai import Agent, Crew, Process, Task  # type: ignore

    agent_kwargs = {"verbose": False}
    model_name = runtime.get("model")
    if model_name:
        agent_kwargs["llm"] = model_name

    planner = Agent(
        role="planner",
        goal="Create the smallest safe implementation plan",
        backstory="Senior engineer focused on minimal diff and clear validation steps.",
        **agent_kwargs,
    )
    coder = Agent(
        role="coder",
        goal="Implement changes that satisfy requirements with minimal risk",
        backstory="Hands-on engineer that follows repository constraints strictly.",
        **agent_kwargs,
    )
    tester = Agent(
        role="tester",
        goal="Run quality gates and produce deterministic pass/fail evidence",
        backstory="Quality engineer for build/test gates.",
        **agent_kwargs,
    )
    reviewer = Agent(
        role="reviewer",
        goal="Review risk, regressions and testing gaps before merge",
        backstory="Strict reviewer prioritizing correctness and maintainability.",
        **agent_kwargs,
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
