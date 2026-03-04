# .codex-workflow

该目录由 `codex-workflow bootstrap` 初始化。

## 核心配置（建议长期维护）

- `workflow.json`: 阶段定义（默认 `crewai -> verify`）
- `project_profile.json`: 项目画像和硬约束
- `quality_gates.json`: 构建/测试门禁
- `evolution.json`: 经验沉淀策略参数

默认 `workflow.json` 的 `crew_orchestrate` 阶段为 CrewAI，
并自动读取 `~/.codex/config.toml` 与 `~/.codex/auth.json` 作为 LLM 运行时配置。

默认 `workflow.json` 的 `verify` 阶段使用 `command_source: quality_gates.required`，
即由 `quality_gates.json` 驱动实际验证命令。

## 运行产物

- `runs/<run_id>/`: 每次执行的 prompt、日志、run_report
- `memory/playbook.md`: 自动归纳的稳定规则
- `memory/lessons.jsonl`: 历次经验条目
- `memory/latest_summary.json`: 最近一次统计结果
- `memory/run_index.jsonl`: 运行索引

## 推荐实践

1. `workflow.json` 固化团队流程。
2. `quality_gates.json` 与 CI 命令保持一致。
3. 每周执行一次 `codex-workflow evolve --lookback 30`，复核 playbook。
4. 跨项目复用时优先复制这四个核心配置文件，再按项目调整。
