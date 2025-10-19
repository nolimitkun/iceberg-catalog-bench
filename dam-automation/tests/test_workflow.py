from __future__ import annotations

import pytest

from dam_automation.workflow import WorkflowExecutionError, WorkflowRunner, WorkflowStep


def test_runner_executes_steps_in_order() -> None:
    runner = WorkflowRunner()
    context: dict[str, object] = {}
    execution_log: list[str] = []

    def step_one(ctx: dict[str, object]) -> None:
        ctx["value"] = 1
        execution_log.append("step_one")

    def step_two(ctx: dict[str, object]) -> None:
        ctx["value"] = int(ctx["value"]) + 1  # type: ignore[arg-type]
        execution_log.append("step_two")

    steps = [
        WorkflowStep(name="one", action=step_one),
        WorkflowStep(name="two", action=step_two),
    ]

    result = runner.run(steps, context)

    assert result is context
    assert context["value"] == 2
    assert execution_log == ["step_one", "step_two"]


def test_runner_compensates_on_failure() -> None:
    runner = WorkflowRunner()
    context = {"log": []}

    def action_success(ctx: dict[str, object]) -> None:
        ctx["log"].append("executed")  # type: ignore[union-attr]

    def compensator(ctx: dict[str, object]) -> None:
        ctx["log"].append("compensated")  # type: ignore[union-attr]

    def action_failure(_: dict[str, object]) -> None:
        raise RuntimeError("boom")

    steps = [
        WorkflowStep(name="first", action=action_success, compensator=compensator),
        WorkflowStep(name="second", action=action_failure),
    ]

    with pytest.raises(WorkflowExecutionError) as excinfo:
        runner.run(steps, context)

    assert excinfo.value.args[0] == "second"
    assert context["log"] == ["executed", "compensated"]
