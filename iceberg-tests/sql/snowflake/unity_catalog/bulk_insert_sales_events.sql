USE DATABASE {{ catalog_override.database }};
USE SCHEMA IDENTIFIER('{{ target_namespace }}');

INSERT INTO IDENTIFIER('{{ test_case.variables.table_name }}') VALUES
  (1, 10, '2024-01-01 00:00:00'::TIMESTAMP, 'sku-0001', 3, 19.99, 'US', '2024-01-01'::DATE),
  (2, 11, '2024-01-01 00:05:00'::TIMESTAMP, 'sku-0002', 5, 5.00, 'US', '2024-01-01'::DATE),
  (3, 12, '2024-01-02 09:30:00'::TIMESTAMP, 'sku-0003', 2, 10.00, 'GB', '2024-01-02'::DATE),
  (4, 13, '2024-01-02 10:45:00'::TIMESTAMP, 'sku-0004', 8, 7.50, 'FR', '2024-01-02'::DATE),
  (5, 10, '2024-01-03 12:00:00'::TIMESTAMP, 'sku-0005', 1, 99.99, 'US', '2024-01-03'::DATE),
  (6, 11, '2024-01-03 13:25:00'::TIMESTAMP, 'sku-0002', 10, 5.00, 'US', '2024-01-03'::DATE),
  (7, 12, '2024-01-04 15:55:00'::TIMESTAMP, 'sku-0003', 4, 11.00, 'GB', '2024-01-04'::DATE),
  (8, 13, '2024-01-05 16:10:00'::TIMESTAMP, 'sku-0004', 6, 7.50, 'FR', '2024-01-05'::DATE);

SELECT COUNT(*) AS row_count FROM IDENTIFIER('{{ test_case.variables.table_name }}');
-- Snowflake Open Catalog Iceberg tables do not currently expose a public snapshots view.
-- Emit placeholder values so downstream validations keep the same statement indexes.
SELECT NULL::VARCHAR AS snapshot_id,
       CURRENT_TIMESTAMP() AS committed_at;
