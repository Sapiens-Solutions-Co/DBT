{% macro greenplum__proplum_delta_merge(target_relation, delta_relation) %}
    {% do log("Creating delta table for " ~ target_relation) %}  
    
    -- Create temporary buffer table
    {% set buffer_relation = greenplum__proplum_create_temp_relation(target_relation,'bfr_') %}
    {{ greenplum__proplum_create_delta_table(target_relation, delta_relation,'bfr_',false) }}

    {% set merge_keys = proplum_get_merge_key(target_relation) %}
    {% set delete_duplicates = config.get('delete_duplicates', false) %}
    
    -- Get all columns from target table
    {% set table_columns = adapter.get_columns_in_relation(target_relation) %}
    {% set column_list = table_columns | map(attribute='name') | join(', ') %}
    
    -- Check number of rows in source data
    {% set check_sql %}
        SELECT COUNT(*) FROM {{ delta_relation }}
    {% endset %}
    {% set row_cnt = run_query(check_sql)[0][0] %}
    
    {% if row_cnt > 0 %}
        -- Build merge SQL
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
                UNION ALL
                SELECT {{ column_list }}, 2 as rnk
                FROM {{ target_relation }}
            ) q
        ) qr
        WHERE rnk_f = 1;

        set gp_enable_exchange_default_partition to on; 

        -- Swap partitions default
        ALTER TABLE {{ target_relation }} 
        EXCHANGE DEFAULT PARTITION 
        WITH TABLE {{ buffer_relation }};
        
    {% endif %}

    -- Analyze table if configured
    {% set analyze_table = config.get('analyze', true) %}
    {{greenplum__proplum_analyze_table_macro(target_relation, analyze_table)}}

    -- Update load info
    {{ greenplum__proplum_update_load_info_complete(target_relation, delta_relation,'delta_merge',row_cnt) }}
{% endmacro %}