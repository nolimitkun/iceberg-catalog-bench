USE DATABASE {{ catalog_override.database }};
USE SCHEMA IDENTIFIER('{{ target_namespace }}');

DELETE FROM IDENTIFIER('{{ test_case.variables.table_name }}')
  WHERE event_id = 8;

SELECT COUNT(*) AS row_count
  FROM IDENTIFIER('{{ test_case.variables.table_name }}');

SELECT event_id
  FROM IDENTIFIER('{{ test_case.variables.table_name }}')
  ORDER BY event_id;
