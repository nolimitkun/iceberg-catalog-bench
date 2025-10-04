USE DATABASE {{ catalog_override.database }};
USE SCHEMA {{ target_namespace }};

SELECT COUNT(*) AS row_count
  FROM {{ test_case.variables.table_name }};

SELECT event_id, tenant_id, event_ts, sku, qty, price, country, ds
  FROM {{ test_case.variables.table_name }}
  ORDER BY event_id;
