{% macro clickhouse__proplum_filter_delta(source_column='',log_dates=false) %}
    {# Get object name in schema.table format #}
    {% set object_name = this.name %}
    {% set existing_relation = load_cached_relation(this) %}
    {% if execute and not flags.WHICH in ['generate','serve'] and not should_full_refresh() and existing_relation %}   
        {% if log_dates %}
            {% set safety_period = config.get('safety_period', default='0 days') %}
            {% set load_interval = config.get('load_interval', default='') %}
            {% set dates = proplum_get_extraction_dates(model_target=object_name, safety_period=safety_period, load_interval=load_interval, delta_field=source_column,load_method='filter_delta',flag_commit=true, log_data=true) %}
            AND {{ source_column }} BETWEEN timestamp('{{ dates.extraction_from }}') 
            AND timestamp('{{ dates.extraction_to }}')
        {% else %}
            {# Get extraction range from load_info table #}
            {% set load_info_table = proplum_get_load_info_table_name() %}
            {% set extraction_range_sql %}
                SELECT extraction_from, extraction_to, delta_field
                FROM {{ load_info_table }} 
                WHERE invocation_id = '{{ invocation_id }}' 
                AND object_name = '{{ object_name }}'
                AND status = 1
                LIMIT 1
            {% endset %}
            {% set extraction_range = run_query(extraction_range_sql) %}
            {% if extraction_range and extraction_range.rows %}
                {% set extraction_from = extraction_range.rows[0][0] %}
                {% set extraction_to = extraction_range.rows[0][1] %}
                {% if source_column %}
                    {% set delta_column = source_column %}
                {% else %}
                    {% set delta_column = extraction_range.rows[0][2] %}
                {% endif %}
                AND {{ delta_column }} BETWEEN timestamp('{{ extraction_from }}')
                AND timestamp('{{ extraction_to }}')
            {% endif %}
        {% endif %}
    {% endif %}
{% endmacro %}

{% macro clickhouse__proplum_filter_add(sql,delta_field,object_name,safety_period,load_interval) %}
    {# Get extraction range from load_info table #}
    {% set load_info_table = proplum_get_load_info_table_name() %}
    {% set extraction_range_sql %}
        SELECT extraction_from, extraction_to, delta_field
        FROM {{ load_info_table }} 
        WHERE invocation_id = '{{ invocation_id }}' 
        AND object_name = '{{ object_name }}'
        AND status = 1
        LIMIT 1
    {% endset %}
    {% set extraction_range = run_query(extraction_range_sql) %}
    {% if extraction_range and extraction_range.rows %}
        {% set extraction_from = extraction_range.rows[0][0] %}
        {% set extraction_to = extraction_range.rows[0][1] %}
        {%- set modified_sql -%}
            SELECT * FROM (
            {{ sql }}
            ) as main_sql
            WHERE 1=1  
            AND {{ delta_field }} BETWEEN timestamp('{{ extraction_from }}')
            AND timestamp('{{ extraction_to }}')
        {%- endset -%}
    {% else %}
        {% set dates = proplum_get_extraction_dates(model_target=object_name, safety_period=safety_period, load_interval=load_interval, delta_field=source_column,load_method='filter_delta',flag_commit=true, log_data=true) %}
        {%- set modified_sql -%}
            SELECT * FROM (
            {{ sql }}
            ) as main_sql
            WHERE 1=1  
            AND {{ delta_field }} BETWEEN timestamp('{{ dates.extraction_from }}') 
            AND timestamp('{{ dates.extraction_to }}')
        {%- endset -%}        
    {% endif %}
    {{ return(modified_sql) }}
{% endmacro %}


{% macro clickhouse__proplum_log_extraction_dates(model_target, extraction_from, extraction_to,load_method,extraction_type,delta_field) %}
    {# Logs extraction dates to dbt_load_info #}
    {% set load_info_table = proplum_get_load_info_table_name() %}
    {% call statement('log_extraction') %}
        INSERT INTO {{ load_info_table }} (
            object_name,
            status,
            extraction_from,
            extraction_to,
            invocation_id,
            updated_at,
            created_at,
            extraction_from_original,
            extraction_to_original,
            load_method,
            extraction_type,
            delta_field
        ) VALUES (
            '{{ model_target }}',
            1,
            '{{ extraction_from }}',
            '{{ extraction_to }}',
            '{{ invocation_id }}',
            now(),
            now(),
            '{{ extraction_from }}',
            '{{ extraction_to }}',
            '{{load_method}}',
            '{{extraction_type}}',
            '{{delta_field}}'
        )
    {% endcall %}
{% endmacro %}

{% macro clickhouse__proplum_validate_strategy(strategy, merge_keys, delta_field,partition_by) %}
    {# Validate strategy is one of the allowed options #}
    {% set valid_strategies = ['full', 'delta_upsert', 'partitions'] %}
    {% if strategy not in valid_strategies %}
        {% do exceptions.raise_compiler_error(
            "The incremental strategy '" ~ strategy ~ "' is not valid for this materialization."
        ) %}
    {% endif %}

    {% if strategy in ('delta_upsert') %}
        {% if not merge_keys %}
            {% do exceptions.raise_compiler_error(
                "'" ~ strategy ~ "' strategy requires a non-empty 'merge_keys'."
            ) %}
        {% endif %}
    {% endif %}

    {% if strategy in ('delta_upsert', 'partitions') %}
        {% if not delta_field %}
            {% do exceptions.raise_compiler_error(
                "'" ~ strategy ~ "' strategy requires a non-empty 'delta_field'."
            ) %}
        {% endif %}
    {% endif %}

    {% if strategy in ('partitions') %}
        {% if not partition_by %}
            {% do exceptions.raise_compiler_error(
                "'" ~ strategy ~ "' strategy requires a non-empty 'partition_by'."
            ) %}
        {% endif %}
    {% endif %}    
{% endmacro %}

{% macro clickhouse__proplum_update_load_info_complete(target_relation, delta_relation, load_type=none, row_cnt=none) %}
    {# Updates the dbt_load_info table with execution details for ClickHouse #}
    
    {% set model_sql = model.get('compiled_code', '') %}
    {% set delta_field = config.get('delta_field', none) %}
    {% set load_info_table = proplum_get_load_info_table_name() %}
    {% if delta_field %}
        {% call statement('update_load_info') %}
            ALTER TABLE {{ load_info_table }} 
            UPDATE 
                status = 2,
                extraction_to = coalesce((SELECT toDateTime(MAX({{ delta_field }})) FROM {{ delta_relation }}), toDateTime('1970-01-01 00:00:00')),
                updated_at = now(),
                load_type = '{{ load_type }}',
                row_cnt = {{ row_cnt }},
                model_sql = '{{ model_sql | replace("'", "''") }}'
            WHERE 
                invocation_id = '{{ invocation_id }}' 
                AND object_name = '{{ target_relation.name }}';
        {% endcall %}
    {% endif %}    
{% endmacro %}

{% macro clickhouse__proplum_log_full_load(target_relation) %}
    {# Logs extraction dates to dbt_load_info #}
    {% set model_sql = model.get('compiled_code', '') %}
    {% set load_info_table = proplum_get_load_info_table_name() %}
    {% set delta_field = config.get('delta_field', none) %}
    {% call statement('log_extraction') %}
        INSERT INTO {{ load_info_table }} (
            object_name,
            status,
            extraction_to,
            invocation_id,
            load_type,
            updated_at,
            created_at,
            delta_field,
            model_sql,
            row_cnt
        ) VALUES (
            '{{ target_relation.name }}',
            2,
            {% if delta_field is not none %}
                (SELECT toDateTime(MAX({{ delta_field }})) FROM {{ target_relation }})
            {% else %}
                0
            {% endif %},
            '{{ invocation_id }}',
            'init',
            now(),
            now(),
            '{{delta_field}}',
            '{{ model_sql | replace("'", "''") }}',
            (SELECT count(*) FROM {{ target_relation }})
        )
    {% endcall %}
{% endmacro %}