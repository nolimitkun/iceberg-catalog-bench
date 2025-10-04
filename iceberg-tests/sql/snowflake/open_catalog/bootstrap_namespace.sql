USE DATABASE {{ catalog_override.database }};
CREATE SCHEMA IF NOT EXISTS {{ target_namespace }};
USE SCHEMA {{ target_namespace }};
