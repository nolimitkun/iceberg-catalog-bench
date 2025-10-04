from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

from .engines.base import ExecutionResult, StatementResult
from .sql import render_sql_template


class ValidationError(Exception):
    pass


@dataclass
class ValidationOutcome:
    validation: Dict[str, Any]
    success: bool
    details: str = ""


def _render_value(value: Any, variables: Dict[str, Any]) -> Any:
    if isinstance(value, str):
        return render_sql_template(value, variables)
    if isinstance(value, list):
        return [_render_value(v, variables) for v in value]
    if isinstance(value, dict):
        return {k: _render_value(v, variables) for k, v in value.items()}
    return value


def _get_statement(result: ExecutionResult, index: int) -> Any:
    if not result.statements:
        raise ValidationError("No statements executed for validation")
    return result.statements[index]


def apply_validations(
    validations: List[Dict[str, Any]],
    execution_result: ExecutionResult,
    variables: Dict[str, Any],
    state: Dict[str, Any],
) -> List[ValidationOutcome]:
    outcomes: List[ValidationOutcome] = []
    for validation in validations:
        vtype = validation.get("type")
        try:
            if vtype == "rowcount_equals":
                statement = _get_statement(execution_result, validation.get("statement_index", -1))
                expected = int(_render_value(validation.get("expected"), variables))
                actual = _derive_rowcount(statement)
                if actual != expected:
                    raise ValidationError(f"Rowcount mismatch: expected={expected} actual={actual}")
                outcomes.append(ValidationOutcome(validation, True))
            elif vtype == "rowcount_at_least":
                statement = _get_statement(execution_result, validation.get("statement_index", -1))
                threshold = int(_render_value(validation.get("threshold"), variables))
                actual = _derive_rowcount(statement) or 0
                if actual < threshold:
                    raise ValidationError(f"Rowcount {actual} below threshold {threshold}")
                outcomes.append(ValidationOutcome(validation, True))
            elif vtype == "store_rows_as":
                statement = _get_statement(execution_result, validation.get("statement_index", -1))
                key = validation.get("name")
                if not key:
                    raise ValidationError("store_rows_as validation missing 'name'")
                state[key] = statement.rows
                outcomes.append(ValidationOutcome(validation, True))
            elif vtype == "store_rowcount_as":
                statement = _get_statement(execution_result, validation.get("statement_index", -1))
                key = validation.get("name")
                if not key:
                    raise ValidationError("store_rowcount_as validation missing 'name'")
                state[key] = _derive_rowcount(statement)
                outcomes.append(ValidationOutcome(validation, True))
            elif vtype == "compare_rows_with_state":
                statement = _get_statement(execution_result, validation.get("statement_index", -1))
                key = validation.get("name")
                previous = state.get(key)
                if previous != statement.rows:
                    raise ValidationError("Result rows differ from stored state")
                outcomes.append(ValidationOutcome(validation, True))
            else:
                raise ValidationError(f"Unknown validation type '{vtype}'")
        except ValidationError as exc:
            outcomes.append(ValidationOutcome(validation, False, str(exc)))
            raise
    return outcomes
def _derive_rowcount(statement: StatementResult) -> int | None:
    if statement.rows:
        first_row = statement.rows[0]
        if isinstance(first_row, dict):
            for key in ("row_count", "count", "count(1)", "count(*)"):
                if key in first_row:
                    value = first_row[key]
                    if value is None:
                        continue
                    try:
                        return int(value)
                    except (TypeError, ValueError):
                        pass
            if len(first_row) == 1:
                try:
                    value = next(iter(first_row.values()))
                except StopIteration:
                    value = None
                if value is not None:
                    try:
                        return int(value)
                    except (TypeError, ValueError):
                        pass
        elif isinstance(first_row, (list, tuple)):
            if first_row:
                value = first_row[0]
                if value is not None:
                    try:
                        return int(value)
                    except (TypeError, ValueError):
                        pass
    if statement.rowcount is not None:
        try:
            return int(statement.rowcount)
        except (TypeError, ValueError):
            return None
    return None
