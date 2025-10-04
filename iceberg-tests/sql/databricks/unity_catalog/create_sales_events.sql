USE CATALOG {{ engine_catalog.options.catalog }};

CREATE TABLE IF NOT EXISTS {{ target_namespace }}.{{ test_case.variables.table_name }} (
{% for column in dataset.columns -%}
  {{ column.name }} {{ column.type }}{% if not loop.last %},{% endif %}
{% endfor %}
)
USING iceberg
{% if dataset.partition_spec %}
PARTITIONED BY (
{% for partition in dataset.partition_spec -%}
  {{ partition.column }}{% if not loop.last %},{% endif %}
{% endfor %}
)
{% endif %}
TBLPROPERTIES (
{% for key, value in dataset.table_properties.items() -%}
  '{{ key }}'='{{ value }}'{% if not loop.last %},{% endif %}
{% endfor %}
);
