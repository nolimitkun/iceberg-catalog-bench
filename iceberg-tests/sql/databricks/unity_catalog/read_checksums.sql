USE CATALOG {{ engine_catalog.options.catalog_name }};

SELECT
  COUNT(*) AS row_count,
  SUM(qty) AS total_qty,
  SUM(price * qty) AS revenue
FROM {{ target_namespace }}.{{ test_case.variables.table_name }};
