{% macro greenplum__proplum_partitions(target_relation, delta_relation) %}
    {#
    Partitioned delta load strategy:
    1. Creates partitions for new data range
    2. Loads data in partition intervals
    3. Switches partitions into target table
    #}
    {% set lock_id = greenplum__proplum_generate_lock(target_relation) %}

    {% set storage_param = greenplum__proplum_get_table_attributes(target_relation) %}
    {% set dist_key = greenplum__proplum_get_distribution_key(target_relation.schema + '.' + target_relation.identifier) %}
  
    -- Check number of rows in source data
    {% set check_sql %}
        SELECT COUNT(*) FROM {{ delta_relation }}
    {% endset %}
    {% set row_cnt = run_query(check_sql)[0][0] %} 
    {% if row_cnt > 0 %}    
        
        {% set merge_partitions = config.get('merge_partitions', true) %}
        {% set delete_duplicates = config.get('delete_duplicates', false) %}
        {% set merge_keys = proplum_get_merge_key(target_relation) %}

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
        
        -- Create temporary buffer table
        {% set buffer_relation = greenplum__proplum_create_temp_relation(target_relation, 'bfr_') %}
        {% set buffer_exists = adapter.get_relation(buffer_relation.database, buffer_relation.schema, buffer_relation.name) %}
        {% if buffer_exists %}
            {% do adapter.drop_relation(buffer_relation) %}
        {% endif %}

        {% set partitions = greenplum__proplum_partition_name_list_by_date(target_relation,start_date,end_date) %}   
        -- Process each partition interval
        {% for partition in partitions %}
            {% set part_start = partition[1] %}
            {% set part_end = partition[2] %}
            {% set part_name = partition[0] %}

            {% set create_sql %}
                CREATE TABLE {{ buffer_relation }} (LIKE {{ target_relation }} )  {{ storage_param }} {{ dist_key }}; 
            {% endset %}
            {% do run_query(create_sql) %}       
            
            -- Where clause for this partition
            {% set part_where = "1=1 " ~ 
                " AND " ~ partition_key ~ " >= '" ~ part_start ~ "'::timestamp" ~
                " AND " ~ partition_key ~ " < '" ~ part_end ~ "'::timestamp" %}

            -- Get partition name for this interval
            {% set part_name = greenplum__proplum_get_partition_name_by_value(target_relation,partition_value=part_start) %}

            -- Load data into buffer
            {% if merge_partitions %}
                -- Merge mode (upsert)
                {% set merge_sql %}
                    INSERT INTO {{ buffer_relation }} ({{ column_list }})
                    SELECT {{ column_list }}
                    FROM (
                        SELECT q.*, 
                            {{ 'row_number()' if delete_duplicates else 'rank()' }} OVER (
                                PARTITION BY {% for key in merge_keys %}"{{ key }}"{% if not loop.last %}, {% endif %}{% endfor %} 
                                ORDER BY rnk
                            ) as rnk_f
                        FROM (
                            SELECT {{ column_list }}, 1 as rnk
                            FROM {{ delta_relation }}
                            WHERE {{ part_where }}
                            UNION ALL
                            SELECT {{ column_list }}, 2 as rnk
                            FROM {{ target_relation }}
                            WHERE {{ partition_key }} >= '{{ part_start }}'::timestamp
                            AND {{ partition_key }} < '{{ part_end }}'::timestamp
                        ) q
                    ) qr
                    WHERE rnk_f = 1
                {% endset %}
            {% else %}
                -- Direct insert mode
                {% set merge_sql %}
                    INSERT INTO {{ buffer_relation }} ({{ column_list }})
                    SELECT {{ 'DISTINCT' if delete_duplicates else '' }} 
                    {{ column_list }}
                    FROM {{ delta_relation }}
                    WHERE {{ part_where }};
                {% endset %}
            {% endif %}
            {% do run_query(merge_sql) %}

            -- Check number of rows in source data
            {% set check_buffer %}
                SELECT COUNT(*) FROM {{ buffer_relation }}
            {% endset %}
            {% set row_cnt_buffer = run_query(check_buffer)[0][0] %} 
            {% if row_cnt_buffer > 0 %}  
                -- Switch partition
                {{greenplum__proplum_switch_partition(target_relation,part_name,buffer_relation)}}
            {% endif %}
            -- Clean up
            {% do adapter.drop_relation(buffer_relation) %}
        {% endfor %}
        
        -- Analyze table if configured
        {% set analyze_table = config.get('analyze', true) %}
        {{greenplum__proplum_analyze_table_macro(target_relation, analyze_table)}}

    {% endif %}

    {% set delta_field = config.get('delta_field', none) %}
    {% set model_sql = model.get('compiled_code', '') %}
    {% if delta_field %}
        {% set load_info_table = proplum_get_load_info_table_name() %}
        {% set run_id = invocation_id %}
        UPDATE {{ load_info_table }} SET 
            status = 2,
            extraction_to=( select MAX({{ delta_field }}) from {{ delta_relation }} ),
            updated_at=current_timestamp,
            row_cnt={{row_cnt}},
            load_type='proplum_partitions',
            model_sql='{{ model_sql | replace("'", "''")}}'
        where 
            invocation_id = '{{ run_id }}' 
            AND object_name = '{{ target_relation.name }}';
    {% endif %}

    select 1;
{% endmacro %}