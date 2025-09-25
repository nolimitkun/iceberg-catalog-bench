import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from string import Template
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import requests
from requests import Response
from requests.auth import HTTPBasicAuth


SUCCESS_CODES: Tuple[int, ...] = (200, 201, 202, 204)


class ConfigurationError(RuntimeError):
    """Raised when required configuration is missing."""


@dataclass
class HttpTest:
    name: str
    method: str
    path_template: str
    base: str = "catalog"
    json_builder: Optional[Callable[[Dict[str, Any]], Any]] = None
    params_builder: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None
    success_codes: Tuple[int, ...] = SUCCESS_CODES
    expected_status_codes: Tuple[int, ...] = ()
    capture: Optional[Callable[[Dict[str, Any], Response], None]] = None
    description: str = ""

    def resolve_json(self, context: Dict[str, Any]) -> Any:
        if callable(self.json_builder):
            return self.json_builder(context)
        return self.json_builder

    def resolve_params(self, context: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if callable(self.params_builder):
            return self.params_builder(context)
        return self.params_builder


@dataclass
class TestResult:
    test: HttpTest
    status_code: Optional[int]
    ok: bool
    duration: float
    error: Optional[str] = None
    body_excerpt: Optional[str] = None
    expected: bool = False


class PolarisApiTester:
    def __init__(self, account: str, scope: str, client_id: str, client_secret: str) -> None:
        self.account = account.rstrip("/")
        self.scope = scope
        self.client_id = client_id
        self.client_secret = client_secret
        self.session = requests.Session()
        self.base_urls = {
            "management": f"https://{self.account}/api/management/v1",
            "catalog": f"https://{self.account}/api/catalog",
        }
        self._token: Optional[str] = None

    def authenticate(self) -> None:
        token_url = f"{self.base_urls['catalog']}/v1/oauth/tokens"
        response = requests.post(
            token_url,
            data={"grant_type": "client_credentials", "scope": self.scope},
            auth=HTTPBasicAuth(self.client_id, self.client_secret),
        )
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            raise RuntimeError(
                f"Failed to obtain access token ({response.status_code}): {response.text}"
            ) from exc
        token = response.json().get("access_token")
        if not token:
            raise RuntimeError("OAuth response did not include an access_token")
        self._token = token
        self.session.headers.update({"Authorization": f"Bearer {token}"})

    def _ensure_token(self) -> None:
        if not self._token:
            self.authenticate()

    def request(
        self,
        method: str,
        base: str,
        path: str,
        *,
        json_body: Any = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Response:
        self._ensure_token()
        base_url = self.base_urls[base]
        url = f"{base_url}{path}"
        response = self.session.request(method, url, json=json_body, params=params, timeout=30)
        return response


class TestSuite:
    def __init__(self, tester: PolarisApiTester, context: Dict[str, Any]) -> None:
        self.tester = tester
        self.context = context
        self.results: List[TestResult] = []

    def run(self, tests: Iterable[HttpTest]) -> None:
        for test in tests:
            path = test.path_template.format(**self.context)
            json_body = test.resolve_json(self.context)
            params = test.resolve_params(self.context)
            start = time.time()
            error = None
            body_excerpt = None
            status_code = None
            ok = False
            expected = False
            try:
                response = self.tester.request(
                    test.method,
                    test.base,
                    path,
                    json_body=json_body,
                    params=params,
                )
                status_code = response.status_code
                if status_code in test.success_codes:
                    ok = True
                elif status_code in test.expected_status_codes:
                    ok = True
                    expected = True
                if response.content:
                    body_excerpt = _safe_excerpt(response)
                if test.capture is not None:
                    test.capture(self.context, response)
            except Exception as exc:  # noqa: BLE001
                error = str(exc)
            finally:
                duration = time.time() - start
                if error and not body_excerpt:
                    body_excerpt = error
                self.results.append(
                    TestResult(
                        test=test,
                        status_code=status_code,
                        ok=ok,
                        duration=duration,
                        error=error,
                        body_excerpt=body_excerpt,
                        expected=expected,
                    )
                )

    def summarize(self) -> Dict[str, Any]:
        passed = [r for r in self.results if r.ok]
        failed = [r for r in self.results if not r.ok]
        expected = [r for r in self.results if r.expected]
        return {
            "total": len(self.results),
            "passed": len(passed),
            "failed": len(failed),
            "expected": len(expected),
        }


def _safe_excerpt(response: Response, limit: int = 500) -> str:
    try:
        data = response.json()
        text = json.dumps(data, separators=(",", ":"))
    except ValueError:
        text = response.text
    text = " ".join(text.split())
    return text[:limit]


def _template_variables(context: Dict[str, Any], extra: Optional[Dict[str, Any]] = None) -> Dict[str, str]:
    now = time.time()
    vars_map: Dict[str, Any] = {
        "catalog": context.get("catalog", ""),
        "namespace": context.get("write_namespace") or context.get("namespace") or "",
        "warehouse": context.get("warehouse", ""),
        "table": context.get("write_table", ""),
        "view": context.get("write_view", ""),
        "namespace_location": context.get("write_namespace_location", ""),
        "default_base_location": context.get("default_base_location", ""),
        "timestamp": int(now),
        "timestamp_ms": int(now * 1000),
    }
    if extra:
        vars_map.update(extra)
    return {key: str(value) for key, value in vars_map.items() if value is not None}


def _render_json_template(template_text: str, variables: Dict[str, str]) -> Dict[str, Any]:
    try:
        rendered = Template(template_text).safe_substitute(variables)
        payload = json.loads(rendered)
    except Exception as exc:  # noqa: BLE001
        raise ValueError(f"template rendering failed: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError("template must resolve to a JSON object")
    return payload


def load_configuration() -> Tuple[str, str, str, str]:
    account = os.environ.get("OC_ACCOUNT")
    scope = os.environ.get("OC_SCOPE")
    client_id = os.environ.get("OC_CLIENT_ID")
    client_secret = os.environ.get("OC_CLIENT_SECRET")
    if not account or not scope:
        raise ConfigurationError("OC_ACCOUNT and OC_SCOPE must be set")

    if not client_id or not client_secret:
        cred = os.environ.get("OC_CRED")
        if cred and ":" in cred:
            client_id, client_secret = cred.split(":", 1)

    if not client_id or not client_secret:
        raise ConfigurationError(
            "Provide client credentials via OC_CLIENT_ID/OC_CLIENT_SECRET or OC_CRED"
        )

    return account, scope, client_id, client_secret


def build_management_tests(ctx: Dict[str, Any]) -> List[HttpTest]:
    catalog = ctx.get("catalog")
    return [
        HttpTest(
            name="List catalogs",
            method="GET",
            base="management",
            path_template="/catalogs",
            description="Enumerate catalogs available to the principal.",
            expected_status_codes=(403,),
        ),
        HttpTest(
            name="Describe catalog",
            method="GET",
            base="management",
            path_template="/catalogs/{catalog}",
            description="Fetch metadata for the target catalog.",
        ),
        HttpTest(
            name="List catalog roles",
            method="GET",
            base="management",
            path_template="/catalogs/{catalog}/catalog-roles",
            description="Enumerate catalog roles in the catalog.",
            expected_status_codes=(403,),
        ),
        HttpTest(
            name="List principal roles",
            method="GET",
            base="management",
            path_template="/principal-roles",
            description="Inspect principal roles available to the principal.",
            expected_status_codes=(403,),
        ),
    ]


def build_catalog_tests(ctx: Dict[str, Any]) -> List[HttpTest]:
    def capture_config(context: Dict[str, Any], response: Response) -> None:
        try:
            payload = response.json()
        except ValueError:
            return
        defaults = payload.get("defaults") or {}
        base_location = defaults.get("default-base-location")
        if base_location:
            context.setdefault("default_base_location", base_location)

    def capture_namespaces(context: Dict[str, Any], response: Response) -> None:
        try:
            payload = response.json()
        except ValueError:
            return
        namespaces = [".".join(parts) for parts in payload.get("namespaces", []) if parts]
        if namespaces and not context.get("namespace"):
            context["namespace"] = namespaces[0]
        context["namespaces"] = namespaces

    def capture_tables(context: Dict[str, Any], response: Response) -> None:
        try:
            payload = response.json()
        except ValueError:
            return
        identifiers = payload.get("identifiers") or []
        if identifiers:
            context["table"] = identifiers[0].get("name") or identifiers[0]
        context["tables"] = identifiers

    return [
        HttpTest(
            name="Get config",
            method="GET",
            base="catalog",
            path_template="/v1/config",
            description="Check controller configuration.",
            params_builder=lambda context: {"warehouse": context.get("warehouse")} if context.get("warehouse") else None,
            capture=capture_config,
        ),
        HttpTest(
            name="List namespaces",
            method="GET",
            base="catalog",
            path_template="/v1/{catalog}/namespaces",
            description="Enumerate namespaces within the catalog prefix.",
            capture=capture_namespaces,
        ),
        HttpTest(
            name="Describe namespace",
            method="GET",
            base="catalog",
            path_template="/v1/{catalog}/namespaces/{namespace}",
            description="Fetch namespace properties for the first discovered namespace.",
        ),
        HttpTest(
            name="Namespace exists",
            method="HEAD",
            base="catalog",
            path_template="/v1/{catalog}/namespaces/{namespace}",
            description="HEAD probe to assert namespace existence.",
        ),
        HttpTest(
            name="List tables",
            method="GET",
            base="catalog",
            path_template="/v1/{catalog}/namespaces/{namespace}/tables",
            description="List tables within the namespace.",
            capture=capture_tables,
        ),
        HttpTest(
            name="List views",
            method="GET",
            base="catalog",
            path_template="/v1/{catalog}/namespaces/{namespace}/views",
            description="List views within the namespace.",
        ),
        HttpTest(
            name="Get applicable policies",
            method="GET",
            base="catalog",
            path_template="/polaris/v1/{catalog}/applicable-policies",
            description="Inspect policies applicable to the catalog prefix.",
            expected_status_codes=(406,),
        ),
    ]


def build_catalog_write_tests(ctx: Dict[str, Any]) -> List[HttpTest]:
    write_namespace = ctx.get("write_namespace")
    if not write_namespace:
        write_namespace = f"{ctx.get('catalog', 'catalog')}_codex_{int(time.time())}"
        ctx["write_namespace"] = write_namespace

    ctx["write_namespace_parts"] = write_namespace.split(".")
    table_template_text = ctx.get("table_template_text")
    require_table_success = bool(ctx.get("table_require_success"))
    write_table = ctx.get("table_name_override")
    if table_template_text:
        if not write_table:
            write_table = f"{write_namespace.replace('.', '_')}_tbl_{int(time.time())}"
        ctx["write_table"] = write_table

    def make_table_body(context: Dict[str, Any]) -> Dict[str, Any]:
        template_text = context.get("table_template_text")
        if not template_text:
            return {}
        variables = _template_variables(context, {"table": context.get("write_table", "")})
        body = _render_json_template(template_text, variables)
        table_name = context.get("write_table")
        if table_name and "name" not in body:
            body["name"] = table_name
        namespace_location = context.get("write_namespace_location")
        base_location = namespace_location or context.get("default_base_location")
        location_value = body.get("location")
        auto_location = location_value in (None, "__AUTO__", "${location}")
        if base_location and ("location" not in body or auto_location):
            suffix = table_name or f"table_{int(time.time())}"
            namespace_suffix = context.get("write_namespace", "").replace(".", "/").strip("/")
            base_path = base_location.rstrip("/")
            if namespace_location is None and namespace_suffix:
                base_path = f"{base_path}/{namespace_suffix}"
            body["location"] = f"{base_path}/{suffix}"
        return body

    def make_namespace_body(context: Dict[str, Any]) -> Dict[str, Any]:
        properties = {
            "created-by": "codex-cli",
            "created-at": str(int(time.time())),
        }
        base_location = context.get("default_base_location")
        namespace_name = context.get("write_namespace")
        if base_location and namespace_name:
            suffix = namespace_name.replace(".", "/")
            properties["location"] = f"{base_location.rstrip('/')}/{suffix.strip('/')}"
        return {
            "namespace": context.get("write_namespace_parts", []),
            "properties": properties,
        }

    def capture_namespace(context: Dict[str, Any], response: Response) -> None:
        try:
            payload = response.json()
        except ValueError:
            return
        namespace = payload.get("namespace")
        if isinstance(namespace, list) and namespace:
            context["write_namespace_parts"] = namespace
            context["write_namespace"] = ".".join(namespace)
        properties = payload.get("properties") or {}
        namespace_location = properties.get("location")
        if namespace_location:
            context["write_namespace_location"] = namespace_location

    def capture_table(context: Dict[str, Any], response: Response) -> None:
        if response.status_code == 200:
            context["table_created"] = True
            try:
                payload = response.json()
            except ValueError:
                return
            metadata_location = payload.get("metadata-location")
            if metadata_location:
                context["table_metadata_location"] = metadata_location

    def make_update_body(_: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "updates": {
                "owner": "codex-cli",
            }
        }

    tests: List[HttpTest] = [
        HttpTest(
            name="Create namespace (write)",
            method="POST",
            base="catalog",
            path_template="/v1/{catalog}/namespaces",
            json_builder=make_namespace_body,
            description="Create a scratch namespace for write-path validation.",
            capture=capture_namespace,
            success_codes=(200,),
        ),
        HttpTest(
            name="Update namespace properties",
            method="POST",
            base="catalog",
            path_template="/v1/{catalog}/namespaces/{write_namespace}/properties",
            json_builder=make_update_body,
            description="Apply a metadata update on the scratch namespace.",
            success_codes=(200, 204),
        ),
        HttpTest(
            name="Namespace exists (write)",
            method="HEAD",
            base="catalog",
            path_template="/v1/{catalog}/namespaces/{write_namespace}",
            description="Confirm the scratch namespace exists via HEAD.",
        ),
    ]

    cleanup_tests: List[HttpTest] = []

    if table_template_text and ctx.get("write_table"):
        table_create_expected = () if require_table_success else (400, 403, 409)
        table_lookup_expected = () if require_table_success else (404,)
        tests.extend(
            [
                HttpTest(
                    name="Create table",
                    method="POST",
                    base="catalog",
                    path_template="/v1/{catalog}/namespaces/{write_namespace}/tables",
                    json_builder=make_table_body,
                    description="Create a scratch table inside the scratch namespace.",
                    success_codes=(200,),
                    expected_status_codes=table_create_expected,
                    capture=capture_table,
                ),
                HttpTest(
                    name="Load table",
                    method="GET",
                    base="catalog",
                    path_template="/v1/{catalog}/namespaces/{write_namespace}/tables/{write_table}",
                    description="Fetch metadata for the scratch table.",
                    expected_status_codes=table_lookup_expected,
                ),
                HttpTest(
                    name="Table exists",
                    method="HEAD",
                    base="catalog",
                    path_template="/v1/{catalog}/namespaces/{write_namespace}/tables/{write_table}",
                    description="HEAD probe to confirm table existence.",
                    success_codes=(200, 204),
                    expected_status_codes=table_lookup_expected,
                ),
            ]
        )
        if not ctx.get("keep_artifacts"):
            cleanup_tests.append(
                HttpTest(
                    name="Drop table",
                    method="DELETE",
                    base="catalog",
                    path_template="/v1/{catalog}/namespaces/{write_namespace}/tables/{write_table}",
                    description="Drop the scratch table.",
                    success_codes=(204, 404),
                )
            )

    if not ctx.get("keep_artifacts"):
        cleanup_tests.append(
            HttpTest(
                name="Drop namespace",
                method="DELETE",
                base="catalog",
                path_template="/v1/{catalog}/namespaces/{write_namespace}",
                description="Tear down the scratch namespace if creation succeeded.",
                success_codes=(204, 404),
            )
        )

    if cleanup_tests:
        ctx.setdefault("cleanup_tests", []).extend(cleanup_tests)

    return tests


def build_view_write_tests(ctx: Dict[str, Any]) -> List[HttpTest]:
    template_text = ctx.get("view_template_text")
    if not template_text:
        return []

    write_namespace = ctx.get("write_namespace")
    if not write_namespace:
        return []

    write_view = ctx.get("view_name_override")
    if not write_view:
        write_view = f"{write_namespace.replace('.', '_')}_view_{int(time.time())}"
    ctx["write_view"] = write_view

    require_view_success = bool(ctx.get("view_require_success"))

    def make_view_body(context: Dict[str, Any]) -> Dict[str, Any]:
        template = context.get("view_template_text")
        if not template:
            return {}
        variables = _template_variables(context, {"view": context.get("write_view", "")})
        body = _render_json_template(template, variables)
        view_name = context.get("write_view")
        if view_name and "name" not in body:
            body["name"] = view_name
        namespace_location = context.get("write_namespace_location")
        base_location = namespace_location or context.get("default_base_location")
        location_value = body.get("location")
        auto_location = location_value in (None, "__AUTO__", "${location}")
        if base_location and ("location" not in body or auto_location):
            suffix = view_name or f"view_{int(time.time())}"
            base_path = base_location.rstrip("/")
            body["location"] = f"{base_path}/{suffix}"
        return body

    def make_replace_body(context: Dict[str, Any]) -> Dict[str, Any]:
        body = make_view_body(context)
        properties = body.setdefault("properties", {})
        properties.setdefault("replaced", str(int(time.time())))
        return body

    def capture_view(context: Dict[str, Any], response: Response) -> None:
        if response.status_code == 200:
            context["view_created"] = True
            try:
                payload = response.json()
            except ValueError:
                return
            metadata_location = payload.get("metadata-location")
            if metadata_location:
                context["view_metadata_location"] = metadata_location

    view_create_expected = () if require_view_success else (400, 403, 409)
    view_lookup_expected = () if require_view_success else (404,)

    tests: List[HttpTest] = [
        HttpTest(
            name="Create view",
            method="POST",
            base="catalog",
            path_template="/v1/{catalog}/namespaces/{write_namespace}/views",
            json_builder=make_view_body,
            description="Create a scratch view inside the scratch namespace.",
            success_codes=(200,),
            expected_status_codes=view_create_expected,
            capture=capture_view,
        ),
        HttpTest(
            name="Load view",
            method="GET",
            base="catalog",
            path_template="/v1/{catalog}/namespaces/{write_namespace}/views/{write_view}",
            description="Fetch metadata for the scratch view.",
            expected_status_codes=view_lookup_expected,
        ),
        HttpTest(
            name="View exists",
            method="HEAD",
            base="catalog",
            path_template="/v1/{catalog}/namespaces/{write_namespace}/views/{write_view}",
            description="HEAD probe to confirm view existence.",
            success_codes=(200, 204),
            expected_status_codes=view_lookup_expected,
        ),
        HttpTest(
            name="Replace view",
            method="POST",
            base="catalog",
            path_template="/v1/{catalog}/namespaces/{write_namespace}/views/{write_view}",
            json_builder=make_replace_body,
            description="Replace the scratch view definition.",
            success_codes=(200,),
            expected_status_codes=view_create_expected,
        ),
    ]

    if not ctx.get("keep_artifacts"):
        ctx.setdefault("cleanup_tests", []).append(
            HttpTest(
                name="Drop view",
                method="DELETE",
                base="catalog",
                path_template="/v1/{catalog}/namespaces/{write_namespace}/views/{write_view}",
                description="Drop the scratch view.",
                success_codes=(204, 404),
            )
        )

    return tests


def build_table_metrics_tests(ctx: Dict[str, Any]) -> List[HttpTest]:
    template_text = ctx.get("table_metrics_template_text")
    if not template_text:
        return []

    if not ctx.get("write_table"):
        return []

    def make_metrics_body(context: Dict[str, Any]) -> Dict[str, Any]:
        template = context.get("table_metrics_template_text")
        if not template:
            return {}
        variables = _template_variables(
            context,
            {
                "table": context.get("write_table", ""),
                "metadata_location": context.get("table_metadata_location", ""),
            },
        )
        return _render_json_template(template, variables)

    require_metrics_success = bool(ctx.get("metrics_require_success"))
    metrics_expected = () if require_metrics_success else (400, 403, 404, 501)

    return [
        HttpTest(
            name="Report table metrics",
            method="POST",
            base="catalog",
            path_template="/v1/{catalog}/namespaces/{write_namespace}/tables/{write_table}/metrics",
            json_builder=make_metrics_body,
            description="Report metrics for the scratch table.",
            success_codes=(200, 202, 204),
            expected_status_codes=metrics_expected,
        )
    ]


def print_results(title: str, results: List[TestResult], verbose: bool = False) -> None:
    print(f"\n== {title} ==")
    for result in results:
        status_text = result.status_code if result.status_code is not None else "ERR"
        outcome = "PASS"
        if result.expected:
            outcome = "EXP"
        elif not result.ok:
            outcome = "FAIL"
        line = f"[{outcome}] {result.test.name}: {status_text}"
        if result.body_excerpt:
            excerpt = result.body_excerpt if verbose else result.body_excerpt[:160]
            line += f" | {excerpt}"
        print(line)


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Polaris OpenCatalog REST API tester")
    parser.add_argument(
        "--catalog",
        default=os.environ.get("OC_CATALOG", "open_snowflake"),
        help="Catalog prefix (i.e. Polaris catalog name)",
    )
    parser.add_argument(
        "--namespace",
        default=os.environ.get("OC_NAMESPACE"),
        help="Namespace to target for namespace-scoped calls",
    )
    parser.add_argument(
        "--warehouse",
        default=os.environ.get("OC_WAREHOUSE"),
        help="Warehouse identifier to supply when fetching catalog configuration",
    )
    parser.add_argument(
        "--include-writes",
        action="store_true",
        help="Exercise create/update/delete flows (may leave artifacts if cleanup fails)",
    )
    parser.add_argument(
        "--keep-artifacts",
        action="store_true",
        help="Skip cleanup for write flows so you can inspect created resources",
    )
    parser.add_argument(
        "--table-create-spec",
        help="Path to a JSON template used for table creation when write flows are enabled",
    )
    parser.add_argument(
        "--table-name",
        help="Override the autogenerated table name when creating scratch tables",
    )
    parser.add_argument(
        "--table-require-success",
        action="store_true",
        help="Treat scratch table operations as failures when they do not succeed",
    )
    parser.add_argument(
        "--view-create-spec",
        help="Path to a JSON template used for view create/replace flows",
    )
    parser.add_argument(
        "--view-name",
        help="Override the autogenerated view name when creating scratch views",
    )
    parser.add_argument(
        "--view-require-success",
        action="store_true",
        help="Treat scratch view operations as failures when they do not succeed",
    )
    parser.add_argument(
        "--table-metrics-spec",
        help="Path to a JSON template used when reporting table metrics",
    )
    parser.add_argument(
        "--metrics-require-success",
        action="store_true",
        help="Treat table metrics reporting as a failure unless the call succeeds",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print verbose response excerpts",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    try:
        account, scope, client_id, client_secret = load_configuration()
    except ConfigurationError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2

    warehouse = args.warehouse or args.catalog
    context: Dict[str, Any] = {
        "catalog": args.catalog,
        "namespace": args.namespace,
        "warehouse": warehouse,
        "keep_artifacts": args.keep_artifacts,
        "table_require_success": args.table_require_success,
        "view_require_success": args.view_require_success,
        "metrics_require_success": args.metrics_require_success,
    }

    if args.table_create_spec:
        try:
            template_text = Path(args.table_create_spec).read_text()
        except OSError as exc:
            print(f"Failed to read table spec file: {exc}", file=sys.stderr)
            return 3
        context["table_template_text"] = template_text
        context["table_template_path"] = args.table_create_spec

    if args.table_name:
        context["table_name_override"] = args.table_name

    if args.view_create_spec:
        try:
            view_text = Path(args.view_create_spec).read_text()
        except OSError as exc:
            print(f"Failed to read view spec file: {exc}", file=sys.stderr)
            return 3
        context["view_template_text"] = view_text
        context["view_template_path"] = args.view_create_spec

    if args.view_name:
        context["view_name_override"] = args.view_name

    if args.table_metrics_spec:
        try:
            metrics_text = Path(args.table_metrics_spec).read_text()
        except OSError as exc:
            print(f"Failed to read metrics spec file: {exc}", file=sys.stderr)
            return 3
        context["table_metrics_template_text"] = metrics_text
        context["table_metrics_template_path"] = args.table_metrics_spec

    tester = PolarisApiTester(account, scope, client_id, client_secret)
    suite = TestSuite(tester, context)

    management_tests = build_management_tests(context)
    suite.run(management_tests)
    catalog_tests = build_catalog_tests(context)
    suite.run(catalog_tests)

    catalog_write_tests: List[HttpTest] = []
    view_write_tests: List[HttpTest] = []
    metrics_tests: List[HttpTest] = []
    cleanup_tests: List[HttpTest] = []

    if args.include_writes:
        catalog_write_tests = build_catalog_write_tests(context)
        suite.run(catalog_write_tests)

        if context.get("view_template_text"):
            view_write_tests = build_view_write_tests(context)
            if view_write_tests:
                suite.run(view_write_tests)

        if context.get("table_metrics_template_text"):
            metrics_tests = build_table_metrics_tests(context)
            if metrics_tests:
                suite.run(metrics_tests)

        cleanup_tests = context.pop("cleanup_tests", [])
        if cleanup_tests:
            namespace_cleanups: List[HttpTest] = []
            other_cleanups: List[HttpTest] = []
            for test in cleanup_tests:
                if "namespace" in test.name.lower():
                    namespace_cleanups.append(test)
                else:
                    other_cleanups.append(test)
            cleanup_tests = other_cleanups + namespace_cleanups
            suite.run(cleanup_tests)

    summary = suite.summarize()
    mgmt_count = len(management_tests)
    catalog_count = len(catalog_tests)
    write_count = len(catalog_write_tests)
    view_count = len(view_write_tests)
    metrics_count = len(metrics_tests)
    cleanup_count = len(cleanup_tests)

    mgmt_results = suite.results[:mgmt_count]
    catalog_results = suite.results[mgmt_count : mgmt_count + catalog_count]
    print_results("Management API", mgmt_results, args.verbose)
    print_results("Catalog API", catalog_results, args.verbose)
    cursor = mgmt_count + catalog_count
    if catalog_write_tests:
        write_results = suite.results[cursor : cursor + write_count]
        print_results("Catalog Writes", write_results, args.verbose)
        cursor += write_count
    if view_write_tests:
        view_results = suite.results[cursor : cursor + view_count]
        print_results("View Writes", view_results, args.verbose)
        cursor += view_count
    if metrics_tests:
        metrics_results = suite.results[cursor : cursor + metrics_count]
        print_results("Table Metrics", metrics_results, args.verbose)
        cursor += metrics_count
    if cleanup_tests:
        cleanup_results = suite.results[cursor : cursor + cleanup_count]
        print_results("Cleanup", cleanup_results, args.verbose)
    print(
        f"\nExecuted {summary['total']} calls | Passed: {summary['passed']} | Expected: {summary['expected']} | Failed: {summary['failed']}"
    )
    return 0 if summary["failed"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
