USE DATABASE {{ catalog_override.database }};
USE SCHEMA {{ target_namespace }};

DELETE FROM {{ test_case.variables.table_name }}
  WHERE event_id = 8;

SELECT COUNT(*) AS row_count
  FROM {{ test_case.variables.table_name }};

SELECT event_id
  FROM {{ test_case.variables.table_name }}
  ORDER BY event_id;
