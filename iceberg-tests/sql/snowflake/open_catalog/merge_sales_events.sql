USE DATABASE {{ catalog_override.database }};
USE SCHEMA {{ target_namespace }};

MERGE INTO {{ test_case.variables.table_name }} AS tgt
USING (
  SELECT * FROM (
    VALUES
      (2, 11, '2024-01-01 00:05:00'::TIMESTAMP, 'sku-0002', 6, 5.50, 'US', '2024-01-01'::DATE, 'app'),
      (9, 14, '2024-01-06 08:10:00'::TIMESTAMP, 'sku-0006', 7, 15.00, 'DE', '2024-01-06'::DATE, 'store')
  ) AS updates(event_id, tenant_id, event_ts, product_sku, qty, price, country, ds, channel)
) AS src
ON tgt.event_id = src.event_id
WHEN MATCHED THEN UPDATE SET
  qty = src.qty,
  price = src.price,
  channel = src.channel
WHEN NOT MATCHED THEN INSERT (
  event_id, tenant_id, event_ts, product_sku, qty, price, country, ds, channel
) VALUES (
  src.event_id, src.tenant_id, src.event_ts, src.product_sku, src.qty, src.price, src.country, src.ds, src.channel
);

DELETE FROM {{ test_case.variables.table_name }} WHERE event_id = 4;

SELECT COUNT(*) AS row_count FROM {{ test_case.variables.table_name }};
SELECT SUM(qty) AS total_qty FROM {{ test_case.variables.table_name }};
