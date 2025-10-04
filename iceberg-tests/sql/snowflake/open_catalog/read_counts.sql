USE DATABASE {{ catalog_override.database }};
USE SCHEMA {{ target_namespace }};

SELECT
  COUNT(*) AS row_count,
  SUM(qty) AS total_qty,
  SUM(price * qty) AS revenue
FROM {{ test_case.variables.table_name }};

-- Snowflake Open Catalog Iceberg tables do not currently expose a public snapshots view.
-- Emit placeholder values so downstream validations keep the same statement indexes.
SELECT NULL::VARCHAR AS snapshot_id,
       CURRENT_TIMESTAMP() AS committed_at;
