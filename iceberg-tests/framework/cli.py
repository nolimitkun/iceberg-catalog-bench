from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from typing import Dict

from dotenv import load_dotenv

from .config import load_framework_config
from .runner import Runner


def _parse_kv(pairs: list[str]) -> Dict[str, str]:
    result: Dict[str, str] = {}
    for pair in pairs:
        if "=" not in pair:
            raise ValueError(f"Invalid --var '{pair}', expected KEY=VALUE")
        key, value = pair.split("=", 1)
        result[key] = value
    return result


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


def main(argv: list[str] | None = None) -> int:
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    load_dotenv(os.path.join(project_root, ".env"), override=False)

    parser = argparse.ArgumentParser(description="Iceberg interoperability test runner")
    parser.add_argument("--config", default=os.path.join(project_root, "config", "framework.yaml"))
    parser.add_argument("--plan", required=True, help="Plan name to execute")
    parser.add_argument("--namespace", required=True, help="Namespace for the test run")
    parser.add_argument("--var", action="append", default=[], help="Additional template variable KEY=VALUE")
    parser.add_argument("--log-level", default="INFO", help="Logging level (INFO, DEBUG, ...)")
    parser.add_argument("--json", action="store_true", help="Emit run report as JSON")
    args = parser.parse_args(argv)

    configure_logging(args.log_level)

    try:
        bundle = load_framework_config(args.config)
    except Exception as exc:  # noqa: BLE001
        logging.getLogger(__name__).error("Failed to load config: %s", exc)
        return 1

    extra_variables = _parse_kv(args.var)

    runner = Runner(bundle)
    try:
        report = runner.run_plan(args.plan, args.namespace, extra_variables)
    finally:
        runner.close()

    if args.json:
        def serialize(obj):
            if hasattr(obj, "model_dump"):
                return obj.model_dump()
            if hasattr(obj, "__dict__"):
                return obj.__dict__
            return str(obj)

        print(json.dumps(report, default=serialize, indent=2))
    else:
        print(f"Plan: {report.plan.name} (status={report.status})")
        for step_report in report.steps:
            print(f"- Step {step_report.step.name} [{step_report.status}] -> {step_report.step.engine}/{step_report.step.catalog}")
            if step_report.error:
                print(f"  Error: {step_report.error}")
            if step_report.validations:
                for validation in step_report.validations:
                    status = "ok" if validation.success else "failed"
                    print(f"  Validation {validation.validation.get('type')}: {status}")
                    if validation.details:
                        print(f"    Details: {validation.details}")

    return 0 if report.status == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
