USE DATABASE {{ catalog_override.database }};
CREATE SCHEMA IF NOT EXISTS IDENTIFIER('{{ target_namespace }}');
USE SCHEMA IDENTIFIER('{{ target_namespace }}');
