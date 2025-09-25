from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable, List

import sqlparse
from jinja2 import BaseLoader, Environment, StrictUndefined


_env = Environment(loader=BaseLoader(), autoescape=False, undefined=StrictUndefined, trim_blocks=True, lstrip_blocks=True)


def render_sql_template(template_text: str, variables: dict[str, Any]) -> str:
    template = _env.from_string(template_text)
    return template.render(**variables)


def load_sql_script(base_path: Path, relative_path: str) -> str:
    script_path = (base_path / relative_path).resolve()
    if not script_path.exists():
        raise FileNotFoundError(f"SQL script not found: {script_path}")
    return script_path.read_text()


def split_statements(sql_text: str) -> List[str]:
    fragments: Iterable[str] = sqlparse.split(sql_text)
    statements = [fragment.strip() for fragment in fragments if fragment.strip()]
    return statements


def render_sql_statements(base_path: Path, relative_path: str, variables: dict[str, Any]) -> List[str]:
    template_text = load_sql_script(base_path, relative_path)
    rendered = render_sql_template(template_text, variables)
    return split_statements(rendered)
