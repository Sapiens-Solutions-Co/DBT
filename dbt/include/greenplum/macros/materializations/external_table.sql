{% materialization external_table, adapter='greenplum' -%}
  {% set identifier = model['alias'] %}
  {# Simple external table creation (original logic) #}  
  {% set sql_conn = config.get('connect_string', default=none) %}
  {% set load_method = config.get('load_method', default=none) %}
  {% set model_target = config.get('model_target', default=none) %}
  {% set delta_field = config.get('delta_field', default=none) %}
  {% set node_cnt = config.get('node_cnt', default=16) %}
  {% set safety_period = config.get('safety_period', default='0 days') %}
  {% set load_interval = config.get('load_interval', default='') %}
  {% set delta_start_date = config.get('delta_start_date', default=modules.datetime.datetime(2000, 1, 1))  %}
  {% set extraction_type = config.get('extraction_type', default='FULL') %}

  --Calculate extraction dates
  {% set dates = proplum_get_extraction_dates(
    model_target=model_target,
    safety_period=safety_period,
    load_interval=load_interval,
    delta_start_date=delta_start_date,
    log_data=true,
    load_method=load_method,
    extraction_type=extraction_type,
    delta_field=delta_field
  ) %}
  {% set extraction_from = dates.extraction_from %}
  {% set extraction_to = dates.extraction_to %}
  
  {% if load_method == 'pxf' %}
    {% if extraction_type == 'FULL' %}
      {% set v_part_string = '' %}
    {% elif   extraction_type == 'DELTA' %}
      -- Calculate range
      {% set range_days = ((extraction_to - extraction_from).total_seconds() / 86400) / node_cnt %}
      {% set range_days = range_days | round(0, 'floor') | int %}
      {% if range_days <= 0 %}
        {% set range_days = 1 %}
      {% endif %}   

      -- Build partition string
      {% set v_conn_str = sql_conn %}
      {% if delta_field and extraction_from and extraction_to and '&PARTITION_BY' not in v_conn_str %}
        {% set v_part_string = '&PARTITION_BY=' ~ delta_field ~ ':date&RANGE=' ~ extraction_from.strftime('%Y-%m-%d') ~ ':' ~ extraction_to.strftime('%Y-%m-%d') ~ '&INTERVAL=' ~ range_days ~ ':day' %}
      {% else %}
        {% set v_part_string = '' %}
      {% endif %}
    {% endif %}
    
    {% set sql_conn = "LOCATION ('pxf://"~v_conn_str~v_part_string~"') ON ALL FORMAT 'CUSTOM' ( FORMATTER='pxfwritable_import' ) ENCODING 'UTF8'" %}
  {% elif load_method == 'gpfdist' or load_method == 'odata' or load_method == 'odata2' or load_method == 'zservice' or load_method == 'zservice_async'  %}
    {% set sql_conn = sql_conn %}
  {% elif not load_method %}
    {% set sql_conn = sql_conn %}
  {% else %}
    {% do exceptions.raise_compiler_error("Invalid load_method '" ~ load_method ~ "' provided. Supported methods are: pxf, gpfdist, odata, odata2, zservice, zservice_async") %}
  {% endif %}
  
  {% set target_relation = api.Relation.create(
      identifier=identifier,
      schema=schema,
      database=database,
      type='table'
  ) %}

  {% set full_refresh_mode = flags.FULL_REFRESH %}

  -- Check if table exists
  {% set table_exists = adapter.get_relation(
      database=target_relation.database,
      schema=target_relation.schema,
      identifier=target_relation.identifier
  ) %}


  -- Handle existing table cases
  {% if table_exists %}
      {% do log("Dropping and recreating existing external table " ~ target_relation) %}
      {% call statement('drop_existing') %}
        DROP EXTERNAL TABLE IF EXISTS {{ target_relation }}
      {% endcall %}
  {% endif %}

  -- Create the external table
  {% do log("Creating external table " ~ target_relation) %}
  {% call statement('main') -%}
    CREATE READABLE EXTERNAL TABLE {{ target_relation }}
    {% if config.get('columns') -%}
      ({{ config.get('columns') }})
    {%- endif %}
      {{ sql_conn }}
  {%- endcall %}
    -- `COMMIT` happens here
  {{ adapter.commit() }}

  {{ return({'relations': [target_relation]}) }}
{%- endmaterialization %}