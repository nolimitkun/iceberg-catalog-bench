USE DATABASE {{ catalog_override.database }};
USE SCHEMA IDENTIFIER('{{ target_namespace }}');

SELECT COUNT(*) AS row_count
  FROM IDENTIFIER('{{ test_case.variables.table_name }}');

SELECT event_id, tenant_id, event_ts, sku, qty, price, country, ds
  FROM IDENTIFIER('{{ test_case.variables.table_name }}')
  ORDER BY event_id;
