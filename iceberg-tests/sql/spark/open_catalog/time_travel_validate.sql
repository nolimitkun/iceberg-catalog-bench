USE CATALOG {{ engine_catalog.options.catalog_name }};

SELECT COUNT(*) AS current_row_count
FROM {{ target_namespace }}.{{ test_case.variables.table_name }};

SELECT COUNT(*) AS baseline_row_count
FROM {{ target_namespace }}.{{ test_case.variables.table_name }}
VERSION AS OF {{ state.baseline_snapshot[0].snapshot_id }};

SELECT SUM(qty) AS baseline_qty
FROM {{ target_namespace }}.{{ test_case.variables.table_name }}
VERSION AS OF {{ state.baseline_snapshot[0].snapshot_id }};
