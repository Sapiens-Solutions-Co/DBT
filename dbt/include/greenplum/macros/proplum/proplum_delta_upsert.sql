{% macro greenplum__proplum_delta_upsert(target_relation, delta_relation ) %}
    {#
    UPSERT strategy implementation:
    1. Deletes matching records from target
    2. Inserts new records from source
    #}

    {% set lock_id = greenplum__proplum_generate_lock(target_relation) %}
    
    {% set table_exists = adapter.get_relation(target_relation.database, target_relation.schema, target_relation.name) %}
    {% do log("Creating delta table: " ~ backup_relation) %}

    -- Check number of rows in source data
    {% set check_sql %}
        SELECT COUNT(*) FROM {{ delta_relation }}
    {% endset %}
    {% set row_cnt = run_query(check_sql)[0][0] %}
    
    {% if row_cnt > 0 %}
        {% set merge_keys = proplum_get_merge_key(target_relation) %}
        {% set delete_duplicates = config.get('delete_duplicates', false) %}
        
        -- Get all columns from target table
        {% set table_columns = adapter.get_columns_in_relation(target_relation) %}
        {% set column_list = table_columns | map(attribute='name') | join(', ') %}
        
        -- Step 1: Delete overlapping records
        DELETE FROM {{ target_relation }} 
        USING {{ delta_relation }} 
        WHERE 1=1
        {% for key in merge_keys %}
            AND ({{ delta_relation }}."{{ key }}" IS NOT DISTINCT FROM {{ target_relation }}."{{ key }}")
        {% endfor %} ;
        
        
        -- Step 2: Insert new records (with optional deduplication)
        {% set delta_field = config.get('delta_field', '1') %}
        {% if delete_duplicates %}
            INSERT INTO {{ target_relation }} ({{ column_list }})
            SELECT {{ column_list }} FROM (
                SELECT {{ column_list }}, 
                    ROW_NUMBER() OVER (
                        PARTITION BY {% for key in merge_keys %}"{{ key }}"{% if not loop.last %}, {% endif %}{% endfor %}
                        ORDER BY {{ delta_field }} DESC
                    ) as rnk
                FROM {{ delta_relation }}
            ) q WHERE rnk = 1;
        {% else %}
            INSERT INTO {{ target_relation }} ({{ column_list }})
            SELECT {{ column_list }}
            FROM {{ delta_relation }};
        {% endif %}
        
        -- Analyze table if configured
        {% set analyze_table = config.get('analyze', true) %}
        {{greenplum__proplum_analyze_table_macro(target_relation, analyze_table)}}
    {% endif %}
    -- Update load info
    {{ greenplum__proplum_update_load_info_complete(target_relation, delta_relation,'proplum_delta_upsert',row_cnt) }}
{% endmacro %}

