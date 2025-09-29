from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

from .config import ConfigBundle, PlanConfig, PlanStepConfig, TestCaseConfig
from .engines import create_engine_factory
from .engines.base import ExecutionResult
from .sql import render_sql_template
from .validators import ValidationError, ValidationOutcome, apply_validations

logger = logging.getLogger(__name__)


@dataclass
class StepReport:
    step: PlanStepConfig
    status: str
    execution: Optional[ExecutionResult] = None
    validations: List[ValidationOutcome] = field(default_factory=list)
    error: Optional[str] = None


@dataclass
class PlanReport:
    plan: PlanConfig
    namespace: str
    run_id: str
    steps: List[StepReport] = field(default_factory=list)

    @property
    def status(self) -> str:
        if any(step.status == "failed" for step in self.steps):
            return "failed"
        if all(step.status == "skipped" for step in self.steps):
            return "skipped"
        return "passed"


class Runner:
    def __init__(self, bundle: ConfigBundle) -> None:
        self.bundle = bundle
        self.factory = create_engine_factory(bundle.root, bundle.framework)
        self.state: Dict[str, Any] = {}

    def close(self) -> None:
        self.factory.close_all()

    def _base_variables(
        self, namespace: str, run_id: str, step: PlanStepConfig, test_case: TestCaseConfig
    ) -> tuple[Dict[str, Any], Dict[str, Any]]:
        framework = self.bundle.framework
        catalog_config = framework.catalogs.get(step.catalog)
        engine_config = framework.engines.get(step.engine)
        dataset_config = framework.datasets.get(step.dataset) if step.dataset else None
        catalog_override = (
            engine_config.catalog_overrides.get(step.catalog) if engine_config else None
        )

        render_context = {
            "namespace": namespace,
            "run_id": run_id,
            "catalog": catalog_config.model_dump() if catalog_config else None,
            "engine": engine_config.model_dump() if engine_config else None,
            "storage": framework.storage.model_dump(),
        }

        target_namespace = namespace
        catalog_override_dict = None
        if catalog_override:
            override_dump = catalog_override.model_dump()
            resolved_options: Dict[str, Any] = {}
            for key, value in override_dump.get("options", {}).items():
                if isinstance(value, str) and "{{" in value:
                    resolved_options[key] = render_sql_template(value, render_context)
                else:
                    resolved_options[key] = value
            override_dump["options"] = resolved_options
            catalog_override_dict = override_dump

            template = resolved_options.get("namespace_template")
            if template:
                target_namespace = template
            else:
                ns_root = resolved_options.get("namespace_root")
                if ns_root:
                    sep = resolved_options.get("namespace_separator", ".")
                    target_namespace = f"{ns_root}{sep}{namespace}"

        test_case_dict = test_case.model_dump()

        base: Dict[str, Any] = {
            "run_id": run_id,
            "namespace": namespace,
            "target_namespace": target_namespace,
            "step": step.model_dump(),
            "catalog": catalog_config.model_dump() if catalog_config else None,
            "engine": engine_config.model_dump() if engine_config else None,
            "storage": framework.storage.model_dump(),
            "now_utc": datetime.utcnow().isoformat(),
            "state": self.state,
            "test_case": test_case_dict,
        }
        if catalog_override_dict:
            base["catalog_override"] = catalog_override_dict
        variables = dict(base)
        if dataset_config:
            variables["dataset"] = dataset_config.model_dump()
        variables["target_namespace"] = target_namespace
        variables["state"] = self.state
        if engine_config:
            variables.update(engine_config.sql_variables)
        if catalog_override_dict:
            variables["engine_catalog"] = catalog_override_dict
        if test_case.variables:
            variables.update(test_case.variables)
        variables["test_case"] = test_case_dict
        if step.variables:
            variables.update(step.variables)
        return base, variables

    def run_plan(self, plan_name: str, namespace: str, extra_variables: Optional[Dict[str, Any]] = None) -> PlanReport:
        framework = self.bundle.framework
        plan = framework.plans.get(plan_name)
        if not plan:
            raise KeyError(f"Plan '{plan_name}' not found")

        run_id = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        report = PlanReport(plan=plan, namespace=namespace, run_id=run_id)
        logger.info("Starting plan '%s' with namespace '%s'", plan.name, namespace)

        for step in plan.steps:
            step_report = StepReport(step=step, status="pending")
            report.steps.append(step_report)

            try:
                test_case = framework.test_cases.get(step.test_case)
                if not test_case:
                    raise KeyError(f"Test case '{step.test_case}' not defined")

                adapter = self.factory.get(step.engine, step.catalog)

                base_context, template_variables = self._base_variables(namespace, run_id, step, test_case)
                if extra_variables:
                    template_variables.update(extra_variables)

                adapter.configure(base_context)

                sql_path = test_case.resolve_script(step.engine, step.catalog)
                execution = adapter.run(step.name, sql_path, template_variables)
                step_report.execution = execution

                validations = list(test_case.validations) + list(step.validations)
                if validations:
                    outcomes = apply_validations(validations, execution, template_variables, self.state)
                    step_report.validations.extend(outcomes)
                step_report.status = "passed"
            except ValidationError as exc:
                logger.error("Validation failed on step '%s': %s", step.name, exc)
                step_report.status = "failed"
                step_report.error = str(exc)
                if not step.continue_on_error:
                    break
            except Exception as exc:  # noqa: BLE001
                logger.exception("Error executing step '%s'", step.name)
                step_report.status = "failed"
                step_report.error = str(exc)
                if not step.continue_on_error:
                    break

        logger.info("Plan '%s' completed with status %s", plan.name, report.status)
        return report
