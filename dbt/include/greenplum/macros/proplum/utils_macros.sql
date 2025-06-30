{% macro greenplum__proplum_log_extraction_dates(model_target, extraction_from, extraction_to,load_method,extraction_type,delta_field) %}
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
            current_timestamp,
            current_timestamp,
            '{{ extraction_from }}',
            '{{ extraction_to }}',
            '{{load_method}}',
            '{{extraction_type}}',
            '{{delta_field}}'
        );
        COMMIT;
    {% endcall %}
{% endmacro %}

{% macro greenplum__proplum_filter_delta(source_column='',log_dates=false) %}
    {# Get object name in schema.table format #}
    {% set object_name = this.name %}
    {% set existing_relation = load_cached_relation(this) %}  
    {% if execute and not flags.WHICH in ['generate','serve'] and not should_full_refresh() and existing_relation  %}    
        {% if log_dates %}
            {% set safety_period = config.get('safety_period', default='0 days') %}
            {% set load_interval = config.get('load_interval', default='') %}
            {% set dates = proplum_get_extraction_dates(model_target=object_name, safety_period=safety_period, load_interval=load_interval, delta_field=source_column,load_method='filter_delta',flag_commit=true, log_data=true) %}
            AND {{ source_column }} BETWEEN '{{ dates.extraction_from }}'::timestamp AND '{{ dates.extraction_to }}'::timestamp
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
                AND {{ delta_column }} BETWEEN '{{ extraction_from }}'::timestamp AND '{{ extraction_to }}'::timestamp
            {% endif %}
        {% endif %}
    {% endif %}
{% endmacro %}

{% macro greenplum__proplum_create_temp_relation(base_relation,prefix='bkp_') %}
    {% set backup_name = prefix ~ base_relation.identifier %}
    {% set staging_schema = 'stg_' ~ base_relation.schema %}
    {% do return(base_relation.incorporate(
        path={"schema": staging_schema, "identifier": backup_name},
        type='table'
    )) %}
{% endmacro %}

{% macro greenplum__proplum_get_distribution_key(table_name) %}
    {# 
    Returns the distribution key for a Greenplum table
    Parameters:
        table_name (string): Fully qualified table name (schema.table)
    Returns:
        string: DISTRIBUTED BY clause or 'DISTRIBUTED RANDOMLY'
    #}
    {% set unified_name = table_name | replace('"', '') | lower %}
    
    {% set get_oid_sql %}
        SELECT c.oid
        FROM pg_class AS c 
        JOIN pg_namespace AS n ON c.relnamespace = n.oid
        WHERE n.nspname || '.' || c.relname = '{{ unified_name }}'
        LIMIT 1
    {% endset %}

    {% set oid_result = run_query(get_oid_sql) %}
    {% set table_oid = oid_result[0][0] if oid_result and oid_result[0][0] else None %}

    {% if not table_oid %}
        {{ return('DISTRIBUTED RANDOMLY') }}
    {% else %}
        {% set get_dist_key_sql %}
            SELECT pg_get_table_distributedby({{ table_oid }})
        {% endset %}
        {% set dist_key_result = run_query(get_dist_key_sql) %}
        {{ return(dist_key_result[0][0] if dist_key_result and dist_key_result[0][0] else 'DISTRIBUTED RANDOMLY') }}
    {% endif %}
{% endmacro %}

{% macro greenplum__proplum_get_table_attributes(table_name) %}
    {# 
    Returns the storage parameters for a Greenplum table
    Parameters:
        table_name (string): Fully qualified table name (schema.table)
    Returns:
        string: Storage parameters in 'WITH (...)' format or empty string
    #}
    {% set get_attributes_sql %}
        SELECT COALESCE('WITH (' || array_to_string(reloptions, ', ') || ')', '')
        FROM pg_class
        WHERE oid = '{{ table_name }}'::regclass
    {% endset %}
    {% set attributes_result = run_query(get_attributes_sql) %}
    {{ return(attributes_result[0][0] if attributes_result and attributes_result[0][0] else '') }}
{% endmacro %}

{% macro greenplum__proplum_create_date_partitions(table_name, partition_value, limit_value=none) %}
    {#
    Creates date-based partitions for a Greenplum table
    Parameters:
        table_name (string): Fully qualified table name (schema.table)
        partition_value (timestamp): Starting partition value
        limit_value (timestamp): Optional upper bound for partitions (defaults to '9999-12-31')
    #}
    
    {% set default_limit = "9999-12-31" %}
    {% set min_value = "1000-01-01" %}
    {% set max_value = limit_value if limit_value else default_limit %}

    -- Convert string input to timestamp if needed
    {% set partition_value = partition_value if partition_value is string 
                            else partition_value|string %}
    {% set partition_value = "to_timestamp('" ~ partition_value ~ "', 'YYYY-MM-DD')" %}   
    
    -- Validate input
    {% if not partition_value %}
        {{ exceptions.raise_compiler_error("Partition value cannot be null for table " ~ table_name) }}
    {% endif %}
    
    -- Check if table is partitioned
    {% set check_partitioned_sql %}
        SELECT COUNT(*) 
        FROM pg_partitions 
        WHERE schemaname = '{{table_name.schema }}'
        AND tablename = '{{table_name.identifier }}'
    {% endset %}
    {% set partition_count = run_query(check_partitioned_sql)[0][0] %}
    {% if partition_count > 0 %}
       -- Get last partition range
        {% set last_partition = greenplum__proplum_get_partition_range(
            relation=table_name,
            min_value=min_value,
            max_value=max_value
        ) %}
        {% if last_partition[0] %}
            {% set last_partition_end = last_partition[0] %}
            {% set last_interval = last_partition[1] %}
            {% set interval_days = last_interval.split(' ')[0] | int %}            
            
            -- Determine partition interval and name format
            {% if interval_days >= 28 and interval_days <= 31 %}
                {% set interval = "'1 month'::interval" %}
                {% set name_format = "'m_' || to_char(part_end, 'mm_yyyy')" %}
            {% elif interval_days < 28 %}
                {% set interval = "'1 day'::interval" %}
                {% set name_format = "'d_' || to_char(part_end, 'dd_mm_yyyy')" %}
            {% elif interval_days > 31 and interval_days <= 92 %}
                {% set interval = "'3 months'::interval" %}
                {% set name_format = "'q_' || to_char(part_end, 'Q_yyyy')" %}
            {% elif interval_days > 92 %}
                {% set interval = "'1 year'::interval" %}
                {% set name_format = "'y_' || to_char(part_end, 'yyyy')" %}
            {% else %}
                {{ exceptions.raise_compiler_error("Unable to determine partition interval for table " ~ table_name) }}
            {% endif %}
            -- Create partitions until we cover the requested date
            {% set create_partitions_sql %}
                DO $$
                DECLARE
                    part_end timestamp := '{{ last_partition_end }}'::timestamp;
                    part_name text;
                BEGIN
                    WHILE part_end <= {{ partition_value }}::timestamp LOOP
                        part_name := {{ name_format }};
                        EXECUTE format('
                            ALTER TABLE {{ table_name }} 
                            SPLIT DEFAULT PARTITION 
                            START (%L::timestamp) END (%L::timestamp)
                            INTO (PARTITION %I, DEFAULT PARTITION)',
                            part_end, 
                            part_end + {{ interval }},
                            part_name
                        );
                        part_end := part_end + {{ interval }};
                    END LOOP;
                END $$;
            {% endset %}
            {% do run_query(create_partitions_sql) %}
        {% endif %}
    {% endif %}
{% endmacro %}

{% macro greenplum__proplum_get_partition_range(relation, min_value, max_value) %}
    {#
    Returns last partition 
    Returns: (last_partition_end, partition_interval)
    #}
    {% set database = relation.database %}
    {% set schema = relation.schema %}
    {% set table = relation.identifier %}
    
    {% set get_partition_sql %}
        WITH partition_ranges AS (
            SELECT 
                partitiontablename::text,
                split_part(partitionrangestart, '::', 1)::timestamp as part_start,
                split_part(partitionrangeend, '::', 1)::timestamp as part_end
            FROM pg_partitions
            WHERE schemaname = '{{ schema }}'
              AND tablename = '{{ table }}'
              AND partitionisdefault = false
              AND (
                  ( split_part(partitionrangestart, '::', 1)::timestamp BETWEEN 
                      to_timestamp(to_char('{{ min_value }}'::timestamp,'YYYYMMDDHH24MISS'),'YYYYMMDDHH24MISS')  AND 
                      to_timestamp(to_char('{{ max_value }}'::timestamp,'YYYYMMDDHH24MISS'),'YYYYMMDDHH24MISS')
                  )    
                  OR 
                  ( split_part(partitionrangeend, '::', 1)::timestamp - interval '1 second' BETWEEN 
                      to_timestamp(to_char('{{ min_value }}'::timestamp,'YYYYMMDDHH24MISS'),'YYYYMMDDHH24MISS')  AND 
                      to_timestamp(to_char('{{ max_value }}'::timestamp,'YYYYMMDDHH24MISS'),'YYYYMMDDHH24MISS')
                  )    
                  OR 
                  ( to_timestamp(to_char('{{ min_value }}'::timestamp,'YYYYMMDDHH24MISS'),'YYYYMMDDHH24MISS') BETWEEN 
                      split_part(partitionrangestart, '::', 1)::timestamp AND 
                      split_part(partitionrangeend, '::', 1)::timestamp - interval '1 second'
                  )
                  OR 
                  ( to_timestamp(to_char('{{ max_value }}'::timestamp,'YYYYMMDDHH24MISS'),'YYYYMMDDHH24MISS') BETWEEN 
                      split_part(partitionrangestart, '::', 1)::timestamp AND 
                      split_part(partitionrangeend, '::', 1)::timestamp - interval '1 second'
                  )
              )
            ORDER BY part_end DESC
            LIMIT 1
        )
        SELECT 
            part_end::text,
            (part_end - part_start)::text
        FROM partition_ranges
    {% endset %}
    {% set partition_info = run_query(get_partition_sql) %}
    
    {% if partition_info and partition_info.rows %}
        {{ return((partition_info[0][0], partition_info[0][1])) }}
    {% else %}
        {{ return((none, none)) }}
    {% endif %}
{% endmacro %}

{% macro greenplum__proplum_get_partition_key(table_name) %}
    {# 
    Returns the partition key for a Greenplum table
    Parameters:
        table_name (string): Fully qualified table name (schema.table)
    Returns:
        string: name of column that used for partition 
    #}
    {% set schema = table_name.schema %}
    {% set table = table_name.identifier %}

    {% set get_column_sql %}
        select columnname
        from pg_catalog.pg_partition_columns
            WHERE schemaname = '{{ schema }}'
              AND tablename = '{{ table }}';
    {% endset %}
    
    {% set oid_result = run_query(get_column_sql) %}
    {% set column = oid_result[0][0] if oid_result and oid_result[0][0] else None %}
    
     {{ return(column) }}
{% endmacro %}

{% macro greenplum__proplum_partition_name_list_by_date(relation, min_value, max_value) %}
    {#
    Replaces f_partition_name_list_by_date functionality
    Returns: table with 
    #}
    {% set database = relation.database %}
    {% set schema = relation.schema %}
    {% set table = relation.identifier %}
    
    {% set get_partition_sql %}
        SELECT 
            partitiontablename::text,
            split_part(partitionrangestart, '::', 1)::timestamp as part_start,
            split_part(partitionrangeend, '::', 1)::timestamp as part_end
        FROM pg_partitions
        WHERE schemaname = '{{ schema }}'
            AND tablename = '{{ table }}'
            AND partitionisdefault = false
            AND (
                ( split_part(partitionrangestart, '::', 1)::timestamp BETWEEN 
                    to_timestamp(to_char('{{ min_value }}'::timestamp,'YYYYMMDDHH24MISS'),'YYYYMMDDHH24MISS')  AND 
                    to_timestamp(to_char('{{ max_value }}'::timestamp,'YYYYMMDDHH24MISS'),'YYYYMMDDHH24MISS')
                )    
                OR 
                ( split_part(partitionrangeend, '::', 1)::timestamp - interval '1 second' BETWEEN 
                    to_timestamp(to_char('{{ min_value }}'::timestamp,'YYYYMMDDHH24MISS'),'YYYYMMDDHH24MISS')  AND 
                    to_timestamp(to_char('{{ max_value }}'::timestamp,'YYYYMMDDHH24MISS'),'YYYYMMDDHH24MISS')
                )    
                OR 
                ( to_timestamp(to_char('{{ min_value }}'::timestamp,'YYYYMMDDHH24MISS'),'YYYYMMDDHH24MISS') BETWEEN 
                    split_part(partitionrangestart, '::', 1)::timestamp AND 
                    split_part(partitionrangeend, '::', 1)::timestamp - interval '1 second'
                )
                OR 
                ( to_timestamp(to_char('{{ max_value }}'::timestamp,'YYYYMMDDHH24MISS'),'YYYYMMDDHH24MISS') BETWEEN 
                    split_part(partitionrangestart, '::', 1)::timestamp AND 
                    split_part(partitionrangeend, '::', 1)::timestamp - interval '1 second'
                )
            )
        ORDER BY partitionposition
    {% endset %}
    {% set partition_info = run_query(get_partition_sql) %}
    {% do return(partition_info) %}
{% endmacro %}

{% macro greenplum__proplum_get_partition_name_by_value(relation, partition_value) %}
    {#
    Returns the partition name for a given partition value
    Replaces f_partition_name_by_value functionality
    #}
    {% set partitions = greenplum__proplum_partition_name_list_by_date(
        relation=relation,
        min_value=partition_value,
        max_value=partition_value
    ) %}
    
    {% if partitions and partitions.rows %}
        {% set partition_name = partitions[0][0] %} {# partname from first row #}
        {% do return(partition_name) %}
    {% else %}
        {% do return(none) %}
    {% endif %}
{% endmacro %}

{% macro greenplum__proplum_switch_partition(table_name, partition_name, switch_table_name) %}
  {# Main macro to switch a partition with another table #}
  
  {% set database = table_name.database %}
  {% set schema = table_name.schema %}
  {% set table = table_name.identifier %}

  {# Find partition rank #}
  {% set find_rank_sql %}
    SELECT partitionrank 
    FROM pg_partitions
    WHERE schemaname = '{{ schema }}'
        AND tablename = '{{ table }}' 
    AND partitiontablename = lower('{{ partition_name }}')
  {% endset %}
  {% set rank_result = run_query(find_rank_sql) %}
  
  {% if not rank_result or rank_result.rows|length == 0 %}
    {% do log("ERROR: Unable to switch partition - could not find rank for table " ~ 
             table ~ " partition " ~ partition_name, info=True) %}
    {{ exceptions.raise_compiler_error("Unable to find partition rank") }}
  {% endif %}
  
  {% set rank = rank_result.columns[0].values()[0] %}
  
  {# Execute the partition switch #}
  {% set switch_sql %}
    SET gp_enable_exchange_default_partition TO on;
    ALTER TABLE {{ table_name }} 
    EXCHANGE PARTITION FOR (RANK({{ rank }})) 
    WITH TABLE {{ switch_table_name }} WITH VALIDATION;
  {% endset %}
  
  {% do run_query(switch_sql) %}
  {% do log("Successfully switched partition " ~ partition_name ~ 
           " with table " ~ switch_table_name) %}
{% endmacro %}

{% macro greenplum__proplum_analyze_table_macro(target_relation, analyze=true) %}
    {% if analyze and target_relation %}
        ANALYZE {{ target_relation }};
    {% endif %}
{% endmacro %}

{% macro greenplum__proplum_create_delta_table(target_relation, tmp_relation, suffix = 'delta_',insert_flag=false) %}
    {% set delta_relation = greenplum__proplum_create_temp_relation(target_relation, suffix) %}
    
    -- Create staging schema if needed
    {% set staging_schema = delta_relation.schema %}
    {% if not adapter.check_schema_exists(target_relation.database, staging_schema) %}
        {% do log("Creating staging schema: " ~ staging_schema) %}
        CREATE SCHEMA IF NOT EXISTS {{ staging_schema }};
    {% endif %}
    
    -- Drop table if it exists
    {% if adapter.get_relation(delta_relation.database, delta_relation.schema, delta_relation.name) %}
        {% do adapter.drop_relation(delta_relation) %}
    {% endif %}
    
    -- Create  table with same structure as target
    {% set storage_param = greenplum__proplum_get_table_attributes(target_relation) %}
    {% set dist_key = greenplum__proplum_get_distribution_key(target_relation.schema + '.' + target_relation.identifier) %}
    
    CREATE TABLE {{ delta_relation }} (LIKE {{ target_relation }} ) {{ storage_param }} {{ dist_key }};
    
    {% if insert_flag %} 
        -- Populate table from temp relation
        INSERT INTO {{ delta_relation }} 
        SELECT * FROM {{ tmp_relation }} 
        WHERE 1=1;
    {% endif %}
{% endmacro %}

{% macro greenplum__proplum_generate_lock(relation) %}
  {% if execute %}
    {% set lock_query %}
      SELECT ('x' || substr(md5('dbt_{{ relation.schema }}_{{ relation.identifier }}'), 1, 8))::bit(32)::int
    {% endset %}
    {% set result = run_query(lock_query) %}
    {% set lock_id = result.columns[0][0] if result else 99999 %}
    {% call statement('create_lock') %}    
        SELECT pg_advisory_xact_lock({{ lock_id }});
    {% endcall %}
    {{ return(lock_id) }}
  {% else %}
    {{ return(99999) }}
  {% endif %}
{% endmacro %}

{% macro greenplum_proplum_create_schema(database,schema) %}
    {% set schema_exists = adapter.check_schema_exists(database, schema) %}
    {% if not schema_exists %}
        {% do log("Schema " ~ schema ~ " does not exist. Creating it.") %}
        {% call statement('create_schema') %}
            CREATE SCHEMA IF NOT EXISTS {{ schema }}
        {% endcall %}
    {% endif %}        
{% endmacro %}

{% macro greenplum__proplum_validate_strategy(strategy, merge_keys, delta_field,raw_partition,fields_string) %}
    {# Validate strategy is one of the allowed options #}
    {% set valid_strategies = ['full', 'delta_upsert', 'delta_merge', 'delta', 'partitions'] %}
    {% if strategy not in valid_strategies %}
        {% do exceptions.raise_compiler_error(
            "The incremental strategy '" ~ strategy ~ "' is not valid for this materialization."
        ) %}
    {% endif %}

    {% if strategy in ('delta_upsert', 'delta_merge') %}
        {% if not merge_keys %}
            {% do exceptions.raise_compiler_error(
                "'" ~ strategy ~ "' strategy requires a non-empty 'merge_keys'."
            ) %}
        {% endif %}
    {% endif %}

    {% if strategy in ('delta_upsert', 'delta_merge','delta','partitions') %}
        {% if not delta_field %}
            {% do exceptions.raise_compiler_error(
                "'" ~ strategy ~ "' strategy requires a non-empty 'delta_field'."
            ) %}
        {% endif %}
    {% endif %}    

    {% if strategy in ('delta_merge','partitions') %}
        {% if not raw_partition %}
            {% do exceptions.raise_compiler_error(
                "'" ~ strategy ~ "' strategy requires a non-empty 'raw_partition'."
            ) %}
        {% endif %}
        {% if not fields_string %}
            {% do exceptions.raise_compiler_error(
                "'" ~ strategy ~ "' strategy requires a non-empty 'fields_string'."
            ) %}
        {% endif %}        
    {% endif %}     
{% endmacro %}

{% macro greenplum__proplum_update_load_info_complete(target_relation, delta_relation,load_type=none,row_cnt=none ) %}
    {# Updates the dbt_load_info table with execution details #}
    
    {% set model_sql = model.get('compiled_code', '') %}
    {% set delta_field = config.get('delta_field', none) %}
    {% set load_info_table = proplum_get_load_info_table_name() %}
    {% if delta_field %}
        UPDATE {{ load_info_table }} SET 
            status = 2,
            extraction_to = (SELECT MAX({{ delta_field }}) FROM {{ delta_relation }}),
            updated_at = current_timestamp,
            load_type='{{load_type}}',
            row_cnt={{row_cnt}},
            model_sql = '{{ model_sql | replace("'", "''") }}'
        WHERE 
            invocation_id = '{{ invocation_id }}' 
            AND object_name = '{{ target_relation.name }}';
    {% endif %}    
{% endmacro %}

{% macro greenplum__proplum_log_full_load(target_relation) %}
    {# Logs extraction dates to dbt_load_info with upsert logic #}
    {% set model_sql = model.get('compiled_code', '') %}
    {% set load_info_table = proplum_get_load_info_table_name() %}
    {% set delta_field = config.get('delta_field', none) %}
    
    {# First check if record exists #}
    {% set check_exists_sql %}
        SELECT 1 FROM {{ load_info_table }}
        WHERE object_name = '{{ target_relation.name }}'
        AND invocation_id = '{{ invocation_id }}'
        LIMIT 1
    {% endset %}
    
    {% set record_exists = run_query(check_exists_sql).rows|length > 0 %}
    
    {% if record_exists %}
        {# Update existing record #}
        {% call statement('update_extraction') %}
            UPDATE {{ load_info_table }} SET
                status = 2,
                extraction_to = {% if delta_field is not none %}
                    (SELECT MAX({{ delta_field }}) FROM {{ target_relation }})
                {% else %}
                    NULL
                {% endif %},
                updated_at = current_timestamp,
                load_type = 'init',
                extraction_from = NULL,
                delta_field = '{{ delta_field }}',
                model_sql = '{{ model_sql | replace("'", "''") }}',
                row_cnt = (SELECT count(*) FROM {{ target_relation }})
            WHERE object_name = '{{ target_relation.name }}'
            AND invocation_id = '{{ invocation_id }}'
        {% endcall %}
    {% else %}
        {# Insert new record #}
        {% call statement('insert_extraction') %}
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
                    (SELECT MAX({{ delta_field }}) FROM {{ target_relation }})
                {% else %}
                    NULL
                {% endif %},
                '{{ invocation_id }}',
                'init',
                current_timestamp,
                current_timestamp,
                '{{ delta_field }}',
                '{{ model_sql | replace("'", "''") }}',
                (SELECT count(*) FROM {{ target_relation }})
            )
        {% endcall %}
    {% endif %}
{% endmacro %}