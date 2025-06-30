{% macro greenplum__proplum_delta(target_relation, delta_relation) %}
    {#
    PARTITIONED incremental strategy for Greenplum
    1. Creates date partitions if needed
    2. Loads data into appropriate partitions
    #}
    {% set lock_id = greenplum__proplum_generate_lock(target_relation) %}
        
    -- Check number of rows in source data
    {% set check_sql %}
        SELECT COUNT(*) FROM {{ delta_relation }}
    {% endset %}
    {% set row_cnt = run_query(check_sql)[0][0] %}

    {% if row_cnt > 0 %}
        -- Get all columns from target table
        {% set table_columns = adapter.get_columns_in_relation(target_relation) %}
        {% set column_list = table_columns | map(attribute='name') | join(', ') %}

            -- Get partition key (assuming date/timestamp partitioning)
        {% set partition_key = greenplum__proplum_get_partition_key(target_relation) %}
        {% if not partition_key %}
            {{ exceptions.raise_compiler_error("Partition key not specified for PARTITIONED_DELTA strategy") }}
        {% endif %}
        -- Get date range from temp table
        {% set date_range_sql %}
            SELECT 
                MIN({{ partition_key }})::text,
                MAX({{ partition_key }})::text
            FROM {{ delta_relation }}
        {% endset %}
        {% set date_range = run_query(date_range_sql) %}
        
        {% set start_date = date_range[0][0] %}
        {% set end_date = date_range[0][1] %}
        -- Create partitions for new data range
        {% do greenplum__proplum_create_date_partitions(
            table_name=target_relation,
            partition_value=end_date
        ) %}
        
        -- Insert data into target table
        INSERT INTO {{ target_relation }} ({{ column_list }})
        SELECT {{ column_list }}
        FROM {{ delta_relation }};
        
        -- Analyze table if configured
        {% set analyze_table = config.get('analyze', true) %}
        {{greenplum__proplum_analyze_table_macro(target_relation, analyze_table)}}
    {% endif %}

    -- Update load info
    {{ greenplum__proplum_update_load_info_complete(target_relation, delta_relation,'proplum_delta',row_cnt) }}
{% endmacro %}