{% materialization external_table, adapter='clickhouse' -%}
  {%- set identifier = model['alias'] -%}
  {%- set columns = config.get('columns') -%}
  {%- set conn_config = config.get('conn_config') -%}
  {%- set engine = config.get('engine') -%}
  
  {%- set target_relation = api.Relation.create(
      identifier=identifier,
      schema=schema,
      database=database,
      type='table'
  ) -%}

  {{ drop_relation_if_exists(target_relation) }}

  {%- call statement('main') -%}
    CREATE TABLE IF NOT EXISTS {{ target_relation }} (
    {% if config.get('columns') -%}
      {{ config.get('columns') }}
    {%- endif %}
    ) ENGINE = {{engine}}(
      {{ conn_config.connection_name }},
      table='{{ conn_config.table }}',
      schema='{{ conn_config.schema }}'
    )
    {% if conn_config.settings %} 
    SETTINGS {{ conn_config.settings }}
    {% endif %}
  {%- endcall -%}

  {{ return({'relations': [target_relation]}) }}
{%- endmaterialization %}