"""Simple workflow runner for sequential provisioning steps."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Callable, Dict, Optional

logger = logging.getLogger(__name__)

WorkflowContext = Dict[str, object]


@dataclass(slots=True)
class WorkflowStep:
    name: str
    action: Callable[[WorkflowContext], None]
    compensator: Optional[Callable[[WorkflowContext], None]] = None


class WorkflowExecutionError(RuntimeError):
    """Raised when a workflow step fails."""


class WorkflowRunner:
    def __init__(self) -> None:
        self._log = logger

    def run(self, steps: list[WorkflowStep], context: WorkflowContext) -> WorkflowContext:
        executed: list[WorkflowStep] = []
        for step in steps:
            self._log.info("Running workflow step '%s'", step.name)
            try:
                step.action(context)
                executed.append(step)
            except Exception as exc:  # noqa: BLE001 - we want to capture and raise
                self._log.exception("Workflow step '%s' failed: %s", step.name, exc)
                self._compensate(executed, context)
                raise WorkflowExecutionError(step.name) from exc
        return context

    def _compensate(self, executed: list[WorkflowStep], context: WorkflowContext) -> None:
        for step in reversed(executed):
            if step.compensator is None:
                continue
            try:
                self._log.info("Compensating for step '%s'", step.name)
                step.compensator(context)
            except Exception as exc:  # noqa: BLE001
                self._log.error("Compensation for step '%s' failed: %s", step.name, exc)
