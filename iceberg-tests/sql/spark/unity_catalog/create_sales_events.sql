USE CATALOG {{ engine_catalog.options.catalog_name }};

CREATE TABLE IF NOT EXISTS {{ target_namespace }}.{{ test_case.variables.table_name }} (
{% for column in dataset.columns -%}
  {{ column.name }} {{ column.type }}{% if not loop.last %},{% endif %}
{% endfor %}
)
USING iceberg
PARTITIONED BY (
{% for partition in dataset.partition_spec -%}
  {{ partition.transform }}({{ partition.column }}{% if partition.get('num_buckets') %}, {{ partition.get('num_buckets') }}{% endif %}){% if not loop.last %},{% endif %}
{% endfor %}
)
TBLPROPERTIES (
{% for key, value in dataset.table_properties.items() -%}
  '{{ key }}'='{{ value }}'{% if not loop.last %},{% endif %}
{% endfor %}
);

{% if dataset.sort_order %}
ALTER TABLE {{ target_namespace }}.{{ test_case.variables.table_name }}
  WRITE ORDERED BY {{ dataset.sort_order | join(', ') }};
{% endif %}
