USE DATABASE {{ catalog_override.database }};
USE SCHEMA IDENTIFIER('{{ target_namespace }}');

CREATE OR REPLACE ICEBERG TABLE IDENTIFIER('{{ test_case.variables.table_name }}') (
{% for column in dataset.columns -%}
  {{ column.name }} {{ column.type | upper }}{% if not loop.last %},{% endif %}
{% endfor %}
)
{% if dataset.partition_spec %}
PARTITION BY (
{% for partition in dataset.partition_spec -%}
  {%- set transform_name = partition.transform | default('identity') | lower -%}
  {%- if transform_name == 'days' -%}
    {%- set expression = '  DAY(' ~ partition.column ~ ')' -%}
  {%- elif transform_name == 'hours' -%}
    {%- set expression = '  HOUR(' ~ partition.column ~ ')' -%}
  {%- elif transform_name == 'months' -%}
    {%- set expression = '  MONTH(' ~ partition.column ~ ')' -%}
  {%- elif transform_name == 'years' -%}
    {%- set expression = '  YEAR(' ~ partition.column ~ ')' -%}
  {%- elif transform_name == 'bucket' and partition.get('num_buckets') -%}
    {%- set expression = '  BUCKET(' ~ partition.get('num_buckets') ~ ', ' ~ partition.column ~ ')' -%}
  {%- else -%}
    {%- set expression = '  ' ~ partition.column -%}
  {%- endif -%}
{{ expression }}{% if not loop.last %},{% endif %}
{% endfor %}
)
{% endif %}
;
