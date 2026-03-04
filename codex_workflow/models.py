from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class StageConfig:
    stage_id: str
    kind: str
    description: str = ""
    prompt_template: str = ""
    commands: List[str] = field(default_factory=list)
    command_source: str = ""
    continue_on_error: bool = False

    @staticmethod
    def from_dict(raw: Dict[str, Any]) -> "StageConfig":
        stage_id = str(raw.get("id", "")).strip()
        kind = str(raw.get("kind", "")).strip()
        if not stage_id:
            raise ValueError("stage.id is required")
        if kind not in {"codex", "shell", "manual", "crewai"}:
            raise ValueError("stage.kind must be one of: codex, shell, manual, crewai")
        commands = raw.get("commands", [])
        if commands is None:
            commands = []
        if not isinstance(commands, list):
            raise ValueError("stage.commands must be a list")

        return StageConfig(
            stage_id=stage_id,
            kind=kind,
            description=str(raw.get("description", "")),
            prompt_template=str(raw.get("prompt_template", "")),
            commands=[str(item) for item in commands],
            command_source=str(raw.get("command_source", "")),
            continue_on_error=bool(raw.get("continue_on_error", False)),
        )


@dataclass
class CodexRuntimeConfig:
    enabled: bool = False
    command: str = "codex exec --skip-git-repo-check - < {prompt_file}"
    cwd: str = "{repo_root}"

    @staticmethod
    def from_dict(raw: Dict[str, Any]) -> "CodexRuntimeConfig":
        return CodexRuntimeConfig(
            enabled=bool(raw.get("enabled", False)),
            command=str(raw.get("command", "codex exec --skip-git-repo-check - < {prompt_file}")),
            cwd=str(raw.get("cwd", "{repo_root}")),
        )


@dataclass
class WorkflowConfig:
    version: int
    codex: CodexRuntimeConfig
    stages: List[StageConfig]

    @staticmethod
    def from_dict(raw: Dict[str, Any]) -> "WorkflowConfig":
        version = int(raw.get("version", 1))
        codex = CodexRuntimeConfig.from_dict(dict(raw.get("codex", {})))
        stage_items = raw.get("stages", [])
        if not isinstance(stage_items, list) or not stage_items:
            raise ValueError("workflow.stages must be a non-empty list")

        stages = [StageConfig.from_dict(dict(item)) for item in stage_items]
        return WorkflowConfig(version=version, codex=codex, stages=stages)


@dataclass
class CommandResult:
    command: str
    return_code: int
    log_path: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "command": self.command,
            "return_code": self.return_code,
            "log_path": self.log_path,
        }


@dataclass
class StageResult:
    stage_id: str
    kind: str
    status: str
    elapsed_seconds: float
    command_results: List[CommandResult] = field(default_factory=list)
    prompt_path: str = ""
    message: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "stage_id": self.stage_id,
            "kind": self.kind,
            "status": self.status,
            "elapsed_seconds": round(self.elapsed_seconds, 3),
            "command_results": [item.to_dict() for item in self.command_results],
            "prompt_path": self.prompt_path,
            "message": self.message,
        }


@dataclass
class RunReport:
    run_id: str
    created_at: str
    repo_root: str
    goal: str
    status: str
    stages: List[StageResult]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "created_at": self.created_at,
            "repo_root": self.repo_root,
            "goal": self.goal,
            "status": self.status,
            "stages": [item.to_dict() for item in self.stages],
        }
