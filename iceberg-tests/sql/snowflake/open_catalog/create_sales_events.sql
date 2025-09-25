USE DATABASE {{ catalog_override.database }};
CREATE SCHEMA IF NOT EXISTS {{ target_namespace }};
USE SCHEMA {{ target_namespace }};

CREATE OR REPLACE ICEBERG TABLE {{ test_case.variables.table_name }} (
{% for column in dataset.columns -%}
  {{ column.name }} {{ column.type | upper }}{% if not loop.last %},{% endif %}
{% endfor %}
)
CATALOG = '{{ engine_catalog.options.catalog_name }}'
EXTERNAL_VOLUME = '{{ engine_catalog.options.external_volume }}'
BASE_LOCATION = '{{ engine_catalog.options.base_location_prefix }}{{ target_namespace }}/{{ test_case.variables.table_name }}'
PARTITION BY (
{% for partition in dataset.partition_spec -%}
  {{ partition.transform }}({{ partition.column }}{% if partition.get('num_buckets') %}, {{ partition.get('num_buckets') }}{% endif %}){% if not loop.last %},{% endif %}
{% endfor %}
);
