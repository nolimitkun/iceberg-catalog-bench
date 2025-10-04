USE DATABASE {{ catalog_override.database }};
USE SCHEMA {{ target_namespace }};

UPDATE {{ test_case.variables.table_name }}
  SET price = price * 1.1
  WHERE event_id = 1;

SELECT COUNT(*) AS row_count
  FROM {{ test_case.variables.table_name }};

SELECT event_id, price
  FROM {{ test_case.variables.table_name }}
  WHERE event_id = 1;
