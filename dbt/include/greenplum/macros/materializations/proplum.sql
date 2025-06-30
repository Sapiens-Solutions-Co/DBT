
{% materialization proplum, adapter='greenplum' -%}

  -- relations
  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set intermediate_relation = make_intermediate_relation(target_relation)-%}
  {%- set backup_relation_type = 'table' if existing_relation is none else existing_relation.type -%}
  {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}
  {% set delta_relation = greenplum__proplum_create_temp_relation(target_relation,'delta_') %}

  -- configs
  {%- set unique_key = config.get('unique_key') -%}
  {% set merge_keys = config.get('merge_keys') %}
  {% set delta_field = config.get('delta_field') %}
  {% set raw_partition = config.get('raw_partition') %}
  {% set fields_string = config.get('fields_string') %}
  {% set safety_period = config.get('safety_period', default='0 days') %}
  {% set load_interval = config.get('load_interval', default='') %}  
  {% set add_delta = config.get('add_delta', default=True) %}
  {%- set full_refresh_mode = (should_full_refresh()  or existing_relation.is_view) -%}
  {% if delta_field and existing_relation and not full_refresh_mode and add_delta  %}
    {% set modified_sql = greenplum__proplum_filter_add(sql,delta_field,this.name,safety_period,load_interval) %}
    {% do model.update({'compiled_code': modified_sql}) %}
  {% else %}
    {% set modified_sql = sql %}
  {% endif %}  
  {%- set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') -%}

  -- the temp_ and backup_ relations should not already exist in the database; get_relation
  -- will return None in that case. Otherwise, we get a relation that we can drop
  -- later, before we try to use this name for the current operation. This has to happen before
  -- BEGIN, in a separate transaction
  {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation)-%}
  {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}
  {%- set preexisting_delta_relation = load_cached_relation(delta_relation) -%}
   -- grab current tables grants config for comparision later on
  {% set grant_config = config.get('grants') %}
  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}
  {{ drop_relation_if_exists(preexisting_delta_relation) }}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set to_drop = [] %}

  {% set incremental_strategy = config.get('incremental_strategy') or 'default' %}
  {% set lock_id = greenplum__proplum_generate_lock(target_relation) %}
  {{ greenplum__proplum_validate_strategy(incremental_strategy, merge_keys, delta_field,raw_partition,fields_string) }}

  {% if existing_relation is none %}
      {% set build_sql = get_create_table_as_sql(False, target_relation, modified_sql) %}
      {% set relation_for_indexes = target_relation %}
      {% set need_log = true %}
      {% call statement("main") %}
          {{ build_sql }}
      {% endcall %}      
  {% elif full_refresh_mode %}
      {% set build_sql = get_create_table_as_sql(False, intermediate_relation, modified_sql) %}
      {% set relation_for_indexes = intermediate_relation %}
      {% set need_swap = true %}
      {% set need_log = true %}
      {% call statement("main") %}
          {{ build_sql }}
      {% endcall %}       
  {% else %}
    {% do greenplum_proplum_create_schema(delta_relation.database,delta_relation.schema)%}
    {% set build_sql = (False, delta_relation, modified_sql) %}
    {% set build_sql = get_create_table_as_sql(False, delta_relation, modified_sql) %}
    {% set relation_for_indexes = delta_relation %}
    {% set contract_config = config.get('contract') %}
    {% if not contract_config or not contract_config.enforced %}
      {% do adapter.expand_target_column_types(
               from_relation=delta_relation,
               to_relation=target_relation) %}
    {% endif %}

    {% call statement("main") %}
        {{ build_sql }}
    {% endcall %}

    {% if incremental_strategy == 'full'%}
      {% set incremental_sql = greenplum__proplum_full(target_relation,delta_relation) %}
    {% elif incremental_strategy == 'delta' %}
      {% set incremental_sql = greenplum__proplum_delta(target_relation,delta_relation) %}
    {% elif incremental_strategy == 'delta_upsert' %}
      {% set incremental_sql = greenplum__proplum_delta_upsert(target_relation,delta_relation) %}
    {% elif incremental_strategy == 'delta_merge' %}
      {% set incremental_sql = greenplum__proplum_delta_merge(target_relation,delta_relation) %}
    {% elif incremental_strategy == 'partitions' %}
      {% set incremental_sql = greenplum__proplum_partitions(target_relation,delta_relation) %}
      --All logic already executed in macros so we don't real incremental_sql
      {% set incremental_sql = none %}
    {% endif %}

    {% if incremental_sql %}
      {% call statement("incremental_sql") %}
        {{ incremental_sql }}
      {% endcall %}
    {% endif %}    

  {% endif %}


  {% if existing_relation is none or existing_relation.is_view or should_full_refresh() %}
    {% do create_indexes(relation_for_indexes) %}
  {% endif %}

  {% if need_swap %}
      {% do adapter.rename_relation(target_relation, backup_relation) %}
      {% do adapter.rename_relation(intermediate_relation, target_relation) %}
      {% do to_drop.append(backup_relation) %}
  {% endif %}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% if need_log %}
    {{ greenplum__proplum_update_load_info_complete(target_relation) }}
  {% endif %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {% do adapter.commit() %}

  {% for rel in to_drop %}
      {% do adapter.drop_relation(rel) %}
  {% endfor %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}