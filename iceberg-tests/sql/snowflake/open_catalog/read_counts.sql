USE DATABASE {{ catalog_override.database }};
USE SCHEMA {{ target_namespace }};

SELECT
  COUNT(*) AS row_count,
  SUM(qty) AS total_qty,
  SUM(price * qty) AS revenue
FROM {{ test_case.variables.table_name }};

SELECT snapshot_id, committed_at
FROM {{ test_case.variables.table_name }}$snapshots
ORDER BY committed_at DESC
LIMIT 2;
