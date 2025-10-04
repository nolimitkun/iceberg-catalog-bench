

UPDATE {{ target_namespace }}.{{ test_case.variables.table_name }}
  SET price = price * 1.1
  WHERE event_id = 1;

SELECT COUNT(*) AS row_count
  FROM {{ target_namespace }}.{{ test_case.variables.table_name }};

SELECT event_id, price
  FROM {{ target_namespace }}.{{ test_case.variables.table_name }}
  WHERE event_id = 1;
