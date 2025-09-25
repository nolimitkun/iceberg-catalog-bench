USE CATALOG {{ engine_catalog.options.catalog }};

INSERT INTO {{ target_namespace }}.{{ test_case.variables.table_name }} (
  event_id, tenant_id, event_ts, product_sku, qty, price, country, ds, channel
) VALUES
  (10, 10, TIMESTAMP '2024-01-06 09:05:00', 'sku-0001', 2, 19.99, 'US', DATE '2024-01-06', 'app'),
  (11, 12, TIMESTAMP '2024-01-06 10:10:00', 'sku-0003', 3, 10.00, 'GB', DATE '2024-01-06', 'store');

SELECT COUNT(*) AS post_append_row_count
FROM {{ target_namespace }}.{{ test_case.variables.table_name }};
