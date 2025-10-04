USE DATABASE {{ catalog_override.database }};
USE SCHEMA IDENTIFIER('{{ target_namespace }}');

UPDATE IDENTIFIER('{{ test_case.variables.table_name }}')
  SET price = price * 1.1
  WHERE event_id = 1;

SELECT COUNT(*) AS row_count
  FROM IDENTIFIER('{{ test_case.variables.table_name }}');

SELECT event_id, price
  FROM IDENTIFIER('{{ test_case.variables.table_name }}')
  WHERE event_id = 1;
