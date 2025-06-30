{% macro clickhouse__proplum_partitions(target_relation, tmp_relation) %}
    {#
    Partitioned delta load strategy:
    1. Creates partitions for new data range
    2. Loads data in partition intervals
    3. Switches partitions into target table
    #}

    -- Check number of rows in source data
    {% set check_sql %}
        SELECT COUNT(*) FROM {{ tmp_relation }}
    {% endset %}
    {% set row_cnt = run_query(check_sql)[0][0] %} 
    {% if row_cnt > 0 %}    
        
        {% set merge_partitions = config.get('merge_partitions', true) %}
        {# 1. Identify changed partitions from new data #}
        {% if execute %}
            {% set select_changed_partitions %}
                SELECT DISTINCT partition_id
                FROM system.parts
                WHERE active
                    AND database = '{{ tmp_relation.schema }}'
                    AND table = '{{ tmp_relation.identifier }}'
            {% endset %}
            {% set changed_partitions = run_query(select_changed_partitions).rows %}
        {% else %}
            {% set changed_partitions = [] %}
        {% endif %}

        {% set merge_keys = proplum_get_merge_key(target_relation) %}
        {% if merge_partitions %}
            {% set merged_data_relation = target_relation.incorporate(path={"identifier": target_relation.identifier
            + '__dbt_merged_data'}) %}
            {{ drop_relation_if_exists(merged_data_relation) }}      

            {# 2. Create merged table with combined data #}
            {% call statement('create_merged_data') %}
                CREATE TABLE {{ merged_data_relation }} AS {{ target_relation }}
            {% endcall %}

            {% set partition_by = config.get('partition_by', none) %}
            -- Get all columns from target table
            {% set table_columns = adapter.get_columns_in_relation(target_relation) %}
            {% set column_list = table_columns | map(attribute='name') | join(', ') %}            
            {# 3. Single insert with deduplication using UNION and row_number() #}        
            {% call statement('populate_merged_data') %}
                INSERT INTO {{ merged_data_relation }} ({{column_list}})
                SELECT {{column_list}} FROM (
                    SELECT 
                        *,
                        row_number() OVER (
                            PARTITION BY {% for key in merge_keys %}{{ key }}{% if not loop.last %}, {% endif %}{% endfor %}
                            ORDER BY source_table
                        ) as rn
                    FROM (
                        -- New data takes priority (comes first in UNION)
                        SELECT *, 1 as source_table FROM {{ tmp_relation }}
                        WHERE {{ partition_by }} IN (
                            {% for partition in changed_partitions %}
                                '{{ partition['partition_id'] }}'{% if not loop.last %},{% endif %}
                            {% endfor %}
                        )
                        
                        UNION ALL
                        
                        -- Existing data from same partitions
                        SELECT *, 2 as source_table FROM {{ target_relation }} e
                        WHERE {{ partition_by }} IN (
                            {% for partition in changed_partitions %}
                                '{{ partition['partition_id'] }}'{% if not loop.last %},{% endif %}
                            {% endfor %}
                        )
                    )
                )
                WHERE rn = 1; -- Keep only first occurrence (from new data)
            {% endcall %}
        {% else %}
            {% set merged_data_relation = tmp_relation %}
        {% endif %}      

        {# 4. Replace only the changed partitions with merged data #}
        {% if changed_partitions %}
            {% call statement('replace_partitions') %}
                alter table {{ target_relation }}
                {%- for partition in changed_partitions %}
                    replace partition id '{{ partition['partition_id'] }}'
                    from {{ merged_data_relation }}
                    {{- ', ' if not loop.last }}
                {%- endfor %}
            {% endcall %}
        {% endif %}

    {% endif %}

    {# 5. Log delta #}
    {{ clickhouse__proplum_update_load_info_complete(target_relation, tmp_relation,'partitions',row_cnt) }}

    {# 6. Cleanup #}
    {% if merged_data_relation %}
        {% do adapter.drop_relation(merged_data_relation) %}
    {% endif %}    
{% endmacro %}