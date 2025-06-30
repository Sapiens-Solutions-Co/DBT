{% materialization proplum, adapter='clickhouse' %}

  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') -%}

  {%- set unique_key = config.get('unique_key') -%}
  {% if unique_key is not none and unique_key|length == 0 %}
    {% set unique_key = none %}
  {% endif %}
  {% if unique_key is iterable and (unique_key is not string and unique_key is not mapping) %}
     {% set unique_key = unique_key|join(', ') %}
  {% endif %}
  {%- set inserts_only = config.get('inserts_only') -%}
  {%- set grant_config = config.get('grants') -%}
  {%- set full_refresh_mode = (should_full_refresh() or existing_relation.is_view) -%}
  {%- set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') -%}

  {%- set intermediate_relation = make_intermediate_relation(target_relation)-%}
  {%- set backup_relation_type = 'table' if existing_relation is none else existing_relation.type -%}
  {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}
  {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation)-%}
  {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}

  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  {% set incremental_strategy = config.get('incremental_strategy') or 'default'  %}
  {% set merge_keys = config.get('merge_keys') %}
  {% set delta_field = config.get('delta_field') %}
  {% set partition_by = config.get('partition_by') %} 
  {{ clickhouse__proplum_validate_strategy(incremental_strategy, merge_keys, delta_field, partition_by) }}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}
  {% set to_drop = [] %}
  {% if existing_relation is none %}
    -- No existing table, simply create a new one
    {% call statement('main') %}
        {{ get_create_table_as_sql(False, target_relation, sql) }}
    {% endcall %}
    {% set need_log = true %} 
  {% elif full_refresh_mode %}
    -- Completely replacing the old table, so create a temporary table and then swap it
    {% call statement('main') %}
        {{ get_create_table_as_sql(False, intermediate_relation, sql) }}
    {% endcall %}
    {% set need_swap = true %}
    {% set need_log = true %}
  {% else %}
    {% set new_data_relation = existing_relation.incorporate(path={"identifier": existing_relation.identifier
        + '__dbt_new_data'}) %}
    {{ drop_relation_if_exists(new_data_relation) }}
    {% call statement('main') %}
        {{ get_create_table_as_sql(False, new_data_relation, sql) }}
    {% endcall %}    
    {% if incremental_strategy == 'full' %}
        {% do clickhouse__proplum_full(existing_relation, new_data_relation) %}
    {% elif incremental_strategy == 'delta_upsert' %}
        {% do clickhouse__proplum_delta_upsert(existing_relation, new_data_relation) %}
    {% elif incremental_strategy == 'partitions' %}
        {% do clickhouse__proplum_partitions(existing_relation, new_data_relation) %}
    {% endif %}
    {% do adapter.drop_relation(new_data_relation) %}
  {% endif %}
  {% if need_swap %}
      {% if existing_relation.can_exchange %}
        {% do adapter.rename_relation(intermediate_relation, backup_relation) %}
        {% do exchange_tables_atomic(backup_relation, target_relation) %}
      {% else %}
        {% do adapter.rename_relation(target_relation, backup_relation) %}
        {% do adapter.rename_relation(intermediate_relation, target_relation) %}
      {% endif %}
      {% do to_drop.append(backup_relation) %}
  {% endif %}
  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}
  
  {% if need_log %}
    {{ clickhouse__proplum_log_full_load(target_relation) }}
  {% endif %}
  
  {% do persist_docs(target_relation, model) %}
  {% if existing_relation is none or existing_relation.is_view or should_full_refresh() %}
    {% do create_indexes(target_relation) %}
  {% endif %}
  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {% do adapter.commit() %}
  
  {% for rel in to_drop %}
      {% do adapter.drop_relation(rel) %}
  {% endfor %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}
{%- endmaterialization %}