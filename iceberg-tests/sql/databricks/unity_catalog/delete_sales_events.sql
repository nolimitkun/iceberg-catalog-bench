USE CATALOG {{ engine_catalog.options.catalog }};

DELETE FROM {{ target_namespace }}.{{ test_case.variables.table_name }}
  WHERE event_id = 8;

SELECT COUNT(*) AS row_count
  FROM {{ target_namespace }}.{{ test_case.variables.table_name }};

SELECT event_id
  FROM {{ target_namespace }}.{{ test_case.variables.table_name }}
  ORDER BY event_id;
