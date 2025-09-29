

SELECT COUNT(*) AS row_count
  FROM {{ target_namespace }}.{{ test_case.variables.table_name }};

SELECT event_id, tenant_id, event_ts, sku, qty, price, country, ds
  FROM {{ target_namespace }}.{{ test_case.variables.table_name }}
  ORDER BY event_id;
