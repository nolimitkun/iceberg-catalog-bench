USE CATALOG {{ engine_catalog.options.catalog_name }};

ALTER TABLE {{ target_namespace }}.{{ test_case.variables.table_name }}
  ADD COLUMN channel STRING DEFAULT 'web';

ALTER TABLE {{ target_namespace }}.{{ test_case.variables.table_name }}
  RENAME COLUMN sku TO product_sku;

ALTER TABLE {{ target_namespace }}.{{ test_case.variables.table_name }}
  ALTER COLUMN price TYPE DECIMAL(18,2);

DESCRIBE TABLE {{ target_namespace }}.{{ test_case.variables.table_name }};
