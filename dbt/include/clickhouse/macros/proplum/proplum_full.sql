{% macro clickhouse__proplum_full(target_relation, tmp_relation) %}
    {# ClickHouse optimized implementation #}
    
    {# Get row count from temp table #}
    {% set row_cnt = run_query("SELECT count() FROM " ~ tmp_relation)[0][0] %}
    
    {% do log("Performing atomic table swap for " ~ target_relation) %}
    
    {# Atomic table exchange - ClickHouse 21.4+ #}
    {% call statement('exchange_table') %}
        EXCHANGE TABLES {{ target_relation }} AND {{ tmp_relation }}
    {% endcall %}
    
    {# Log the operation #}
    {% set run_id = invocation_id %}
    {% set model_sql = model.get('compiled_code', '') %}
    {% call statement (update_load_info) %}
        INSERT INTO {{ proplum_get_load_info_table_name() }} (
            object_name,
            status,
            extraction_to,
            invocation_id,
            created_at,
            updated_at,
            load_type,
            row_cnt,
            model_sql
        ) VALUES (
            '{{ target_relation.name }}',
            2,
            now(),
            '{{ run_id }}',
            now(),
            now(),
            'full',
            {{ row_cnt }},
            '{{ model_sql | replace("'", "''") }}'
        );
    {% endcall %}
{% endmacro %}