# Codex Workflow Kit

可复用的 Codex 辅助编码编排骨架，目标是：
- 在任意项目快速建立 `plan -> implement -> verify -> review` 流程
- 自动沉淀失败类型和稳定规则，持续优化下一轮执行质量
- 不污染主项目依赖（默认零依赖，CrewAI 为可选依赖）

## 目录

- `codex_workflow/bootstrap.py`: 初始化 `.codex-workflow/` 配置
- `codex_workflow/runner.py`: 执行工作流并落盘 run 报告
- `codex_workflow/evolution.py`: 汇总历史运行，更新 playbook
- `codex_workflow/hooks.py`: 安装/管理 `pre-push` hook
- `codex_workflow/crewai_blueprint.py`: 可选 CrewAI 骨架

## 安装

在 `codex-workflow-kit` 根目录：

```bash
python3 -m pip install -e .
```

如果你要启用 CrewAI 示例：

```bash
python3 -m pip install -e '.[crewai]'
```

## 在项目里初始化

```bash
codex-workflow bootstrap --target /path/to/repo --project-name your_repo
```

## 执行工作流

先 dry-run 检查配置：

```bash
codex-workflow run --target /path/to/repo --goal "修复 xxx 并补测试" --dry-run
```

正式执行：

```bash
codex-workflow run --target /path/to/repo --goal "修复 xxx 并补测试" --enable-codex
```

注意：`codex` 命令参数在不同版本 CLI 可能不同，请按本机 CLI 调整 `.codex-workflow/workflow.json` 中 `codex.command`。

## 自进化

每次 `run` 完成后默认会自动执行 `evolve`，你也可以单独执行：

```bash
codex-workflow evolve --target /path/to/repo --lookback 30
```

产物：
- `.codex-workflow/memory/playbook.md`: 当前稳定规则
- `.codex-workflow/memory/lessons.jsonl`: 历次经验摘要
- `.codex-workflow/memory/latest_summary.json`: 最新统计

## 自动唤醒（选项 1：pre-push hook）

安装 hook（默认每次 push 前跑 workflow，失败则阻止 push）：

```bash
codex-workflow install-hook --target /path/to/repo --goal "pre-push quality gate"
```

可选参数：
- `--force`: 覆盖已有 `pre-push`
- `--evolve`: 每次 pre-push 后执行 evolve
- `--toolkit-root`: 显式指定 kit 根目录

临时覆盖本次 push 的目标描述：

```bash
CODEX_WORKFLOW_GOAL="修复回归并验证" git push
```

## 跨项目复用

随后根据项目实际情况编辑以下文件：
- `.codex-workflow/quality_gates.json`
- `.codex-workflow/workflow.json`
- `.codex-workflow/project_profile.json`

默认 `verify` 阶段会读取 `quality_gates.required`，这样你只需要改一处门禁配置。
