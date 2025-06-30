{% macro clickhouse__proplum_delta_upsert(target_relation, tmp_relation ) %}
    {#
    UPSERT strategy implementation:
    1. Deletes matching records from target
    2. Inserts new records from source
    #}
    -- Check number of rows in source data
    {% set check_sql %}
        SELECT COUNT(*) FROM {{ tmp_relation }}
    {% endset %}
    {% set row_cnt = run_query(check_sql)[0][0] %}
    {% if row_cnt > 0 %}
        {% set merge_keys = proplum_get_merge_key(target_relation) %}
        {% set delete_duplicates = config.get('delete_duplicates', false) %}
        
        -- Get all columns from target table
        {% set table_columns = adapter.get_columns_in_relation(target_relation) %}
        {% set column_list = table_columns | map(attribute='name') | join(', ') %}
        {% call statement('Delete_records') %}
            -- Step 1: Delete overlapping records         
            DELETE FROM {{ target_relation }} 
            WHERE ({% for key in merge_keys %}{{ key }}{% if not loop.last %}, {% endif %}{% endfor %}) IN (
                SELECT {% for key in merge_keys %}{{ key }}{% if not loop.last %}, {% endif %}{% endfor %}
                FROM {{ tmp_relation }}
            );
        {% endcall %}
        
        
        -- Step 2: Insert new records (with optional deduplication)
        {% set delta_field = config.get('delta_field', '1') %}
        {% call statement('insert_new_records') %}
            {% if delete_duplicates %}
                INSERT INTO {{ target_relation }} ({{ column_list }})
                SELECT {{ column_list }} FROM (
                    SELECT {{ column_list }}, 
                        rowNumberInAllBlocks() as rnk
                    FROM {{ tmp_relation }}
                    GROUP BY {% for key in merge_keys %}{{ key }}{% if not loop.last %}, {% endif %}{% endfor %}
                    HAVING rnk = 1
                );
            {% else %}
                INSERT INTO {{ target_relation }} ({{ column_list }})
                SELECT {{ column_list }}
                FROM {{ tmp_relation }};
            {% endif %}
        {% endcall %}
        
    {% endif %}
    -- Update load info
    {{ clickhouse__proplum_update_load_info_complete(target_relation, tmp_relation,'delta_upsert',row_cnt) }}
{% endmacro %}

