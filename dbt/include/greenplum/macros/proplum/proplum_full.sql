{% macro greenplum__proplum_full(target_relation,delta_relation) %}

    {% set lock_id = greenplum__proplum_generate_lock(target_relation) %}
    
    -- Check number of rows in source data
    {% set check_sql %}
        SELECT COUNT(*) FROM {{ delta_relation }}
    {% endset %}
    {% set row_cnt = run_query(check_sql)[0][0] %}    

    {% do log("Creating backup table: " ~ backup_relation) %}

    {% set backup_relation = greenplum__proplum_create_temp_relation(target_relation,'bkp_') %}
    {{ greenplum__proplum_create_delta_table(target_relation, target_relation,'bkp_',true) }}        

    {% do log("Updating target table: " ~ target_relation) %}
    
    TRUNCATE TABLE {{ target_relation }};

    INSERT INTO {{ target_relation }} SELECT * FROM {{ delta_relation }};
    
    -- Analyze table if configured
    {% set analyze_table = config.get('analyze', true) %}
    {{greenplum__proplum_analyze_table_macro(target_relation, analyze_table)}}

    {% set run_id = invocation_id %}

    --update log table with resulted periods of load
    {% set model_sql = model.get('compiled_code', '') %}
    {% set load_info_table = proplum_get_load_info_table_name() %}
    INSERT INTO {{ load_info_table }} (object_name,status,extraction_to,invocation_id,created_at,updated_at,load_type,row_cnt,model_sql)
    VALUES('{{target_relation.name}}',2,CURRENT_TIMESTAMP,'{{run_id}}',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'proplum_full',{{row_cnt}},'{{ model_sql | replace("'", "''")}}');
{% endmacro %}

