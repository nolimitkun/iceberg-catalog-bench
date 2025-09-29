

INSERT INTO {{ target_namespace }}.{{ test_case.variables.table_name }} VALUES
  (1, 10, TIMESTAMP '2024-01-01 00:00:00', 'sku-0001', 3, 19.99, 'US', DATE '2024-01-01'),
  (2, 11, TIMESTAMP '2024-01-01 00:05:00', 'sku-0002', 5, 5.00, 'US', DATE '2024-01-01'),
  (3, 12, TIMESTAMP '2024-01-02 09:30:00', 'sku-0003', 2, 10.00, 'GB', DATE '2024-01-02'),
  (4, 13, TIMESTAMP '2024-01-02 10:45:00', 'sku-0004', 8, 7.50, 'FR', DATE '2024-01-02'),
  (5, 10, TIMESTAMP '2024-01-03 12:00:00', 'sku-0005', 1, 99.99, 'US', DATE '2024-01-03'),
  (6, 11, TIMESTAMP '2024-01-03 13:25:00', 'sku-0002', 10, 5.00, 'US', DATE '2024-01-03'),
  (7, 12, TIMESTAMP '2024-01-04 15:55:00', 'sku-0003', 4, 11.00, 'GB', DATE '2024-01-04'),
  (8, 13, TIMESTAMP '2024-01-05 16:10:00', 'sku-0004', 6, 7.50, 'FR', DATE '2024-01-05');

SELECT COUNT(*) AS row_count FROM {{ target_namespace }}.{{ test_case.variables.table_name }};
SELECT snapshot_id, committed_at
  FROM {{ target_namespace }}.{{ test_case.variables.table_name }}.snapshots
  ORDER BY committed_at DESC
  LIMIT 1;
