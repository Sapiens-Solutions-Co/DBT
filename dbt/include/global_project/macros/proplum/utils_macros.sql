{% macro proplum_filter_delta(source_column='',log_dates=false) %}
    {# 
    Macro to generate incremental filter based on extraction range from load_info
    Parameters:
        - source_column: The column to filter on
        - log_dates: Log dates in log_info and then use this dates to make where condition
    #}
    {{ return(adapter.dispatch('proplum_filter_delta')(source_column, log_dates)) }}
{% endmacro %}

{% macro default__proplum_filter_delta(source_column, log_dates) %}
    {{ exceptions.raise_compiler_error("Unsupported database type: " ~ target.type) }}
{% endmacro %}

{% macro proplum_clear_delta_load_info(model_name, cutoff_date=none) %}
    {#
    Clears delta load information from dbt_load_info table
    
    Parameters:
        - model_name (string): Name of the model to clear
        - cutoff_date (string/datetime, optional): Only delete records after this date
    #}
    {% if execute %}
        {% set load_info_table = proplum_get_load_info_table_name() %}
        {% set count_sql %}
            SELECT COUNT(*) 
            FROM {{ load_info_table }}
            WHERE object_name = '{{ model_name }}'
            {% if cutoff_date is not none %}
                {% set  cutoff_date_dt = proplum_convert_var_to_datetime(cutoff_date) %}
                AND created_at >= '{{ cutoff_date_dt }}'::timestamp
            {% endif %}
        {% endset %}
        
        {% set count_result = run_query(count_sql) %}
        {% set record_count = count_result.rows[0][0] if count_result else 0 %}
        
        {% if record_count > 0 %}
            {% do log("Deleting " ~ record_count ~ " records for model: " ~ model_name, info=true) %}
            
            {% set delete_sql %}
                DELETE FROM {{ load_info_table }}
                WHERE object_name = '{{ model_name }}'
                {% if cutoff_date is not none %}
                    {% set  cutoff_date_dt = proplum_convert_var_to_datetime(cutoff_date) %}
                    AND created_at >= '{{ cutoff_date_dt }}'::timestamp
                {% endif %}
            {% endset %}
            
            {% do run_query(delete_sql) %}
            {% do log("Deletion complete", info=true) %}
        {% else %}
            {% do log("No records found to delete for model: " ~ model_name, info=true) %}
        {% endif %}
    {% endif %}
{% endmacro %}

{% macro proplum_create_load_info_table() %}
  {% set table_params = proplum_get_load_info_table_relation() %}

  {% set target_database = table_params.database %}
  {% set target_schema = table_params.schema %}
  {% set target_table = table_params.table %}

  {% if not  target_schema  %}
    {% set target_schema = 'dbt_etl' %}
  {% endif%}

  {% set required_columns = [
    {'name': 'invocation_id', 'type': 'text', 'pg_type': 'text', 'ch_type': 'String'},
    {'name': 'object_name', 'type': 'text', 'pg_type': 'text', 'ch_type': 'String'},
    {'name': 'status', 'type': 'bigint', 'pg_type': 'bigint', 'ch_type': 'Int64'},
    {'name': 'extraction_from', 'type': 'timestamp', 'pg_type': 'timestamp', 'ch_type': 'DateTime'},
    {'name': 'extraction_to', 'type': 'timestamp', 'pg_type': 'timestamp', 'ch_type': 'DateTime'},
    {'name': 'updated_at', 'type': 'timestamp', 'pg_type': 'timestamp', 'ch_type': 'DateTime'},
    {'name': 'created_at', 'type': 'timestamp', 'pg_type': 'timestamp', 'ch_type': 'DateTime'},
    {'name': 'extraction_from_original', 'type': 'timestamp', 'pg_type': 'timestamp', 'ch_type': 'DateTime'},
    {'name': 'extraction_to_original', 'type': 'timestamp', 'pg_type': 'timestamp', 'ch_type': 'DateTime'},
    {'name': 'model_sql', 'type': 'text', 'pg_type': 'text', 'ch_type': 'String'},
    {'name': 'load_method', 'type': 'text', 'pg_type': 'text', 'ch_type': 'String'},
    {'name': 'extraction_type', 'type': 'text', 'pg_type': 'text', 'ch_type': 'String'},
    {'name': 'load_type', 'type': 'text', 'pg_type': 'text', 'ch_type': 'String'},
    {'name': 'row_cnt', 'type': 'int8', 'pg_type': 'int8', 'ch_type': 'Int32'},
    {'name': 'delta_field', 'type': 'text', 'pg_type': 'text', 'ch_type': 'String'}
  ] %}

  {% set table_relation = adapter.get_relation(
      database=target.database,
      schema=target_schema,
      identifier=target_table
  ) %}

  {% if table_relation is none %}
    {# Table creation logic using target_database/target_schema/target_table #}
    {% if target.type == 'greenplum' or target.type == 'postgres' %}
      {% set create_table_sql %}
        CREATE SCHEMA IF NOT EXISTS {{ target_schema }};
        CREATE TABLE {{ target_schema }}.{{ target_table }} (
          {% for column in required_columns %}
            {{ column.name }} {{ column.pg_type }}{% if not loop.last %},{% endif %}
          {% endfor %}
        );
        COMMIT;
      {% endset %}
    {% elif target.type == 'clickhouse' %}
      {% set create_database_sql %}
        CREATE DATABASE IF NOT EXISTS {{ target_schema }};
      {% endset %}
      {% do run_query(create_database_sql) %}
      {% set create_table_sql %}
        CREATE TABLE IF NOT EXISTS {{ target_schema }}.{{ target_table }} (
          {% for column in required_columns %}
            {{ column.name }} {{ column.ch_type }}{% if not loop.last %},{% endif %}
          {% endfor %}
        )
        ENGINE = MergeTree()
        ORDER BY (toDate(created_at), object_name)
      {% endset %}
    {% endif %}
    {% do run_query(create_table_sql) %}
    {% do log("Created " ~ target_schema ~ "." ~ target_table ~ " table in " ~ target.type, info=true) %}
  {% else %}
    {# Table exists - verify structure #}
    {% set existing_columns = adapter.get_columns_in_relation(table_relation) %}
    {% set column_issues = [] %}

    {% for req_col in required_columns %}    
      {% set found = [] %}
      {% for exist_col in existing_columns %}
        {% if exist_col.name|lower == req_col.name|lower %}
          {% do found.append(true) %}
        {% endif %}
      {% endfor %}

      {% if not found %}
        {% do column_issues.append("Missing column: " ~ req_col.name) %}
      {% endif %}
    {% endfor %}
    
    {% if column_issues %}
      {% do log("WARNING: " ~ target_table ~ " table exists but has issues: " ~ column_issues|join(', '), info=true) %}
    {% else %}
      {% do log("No issues with " ~ target_table ~ " table in " ~ target.type, info=true) %}
    {% endif %}
  {% endif %}
{% endmacro %}

{% macro proplum_get_merge_key(relation) %}
    {#
    Returns merge keys for a table as an array
    Replace with your actual merge key detection logic
    #}
    {% set merge_keys = config.get('merge_keys') %}
    {% if not merge_keys %}
        {% set merge_keys = ['id'] %} {# Default fallback #}
    {% endif %}
    {{ return(merge_keys) }}
{% endmacro %}

{% macro proplum_get_load_info_table_relation() %}
  {# Get the configuration #}
  {% set config = var('load_info_config', {}) %}
  
  {# Get target-specific config or fall back to default #}
  {% set target_config = config.get(target.name, config.get('default', {})) %}
  
  {# Set values with proper precedence: #}
  {# 1. Target-specific config #}
  {# 2. Default config #}
  {# 3. Target settings #}
  {# 4. Hardcoded defaults #}
  {% set target_database = target_config.get('database', config.get('default', {}).get('database', target.database)) %}
  {% set target_schema = target_config.get('schema', config.get('default', {}).get('schema', target.schema)) %}
  {% set target_table = target_config.get('table', config.get('default', {}).get('table', 'dbt_load_info')) %}
  
  {# Final fallback for schema if still not specified #}
  {% if not target_schema %}
    {% set target_schema = 'dbt_etl' %}
  {% endif %}
  
  {# Return a Relation object #}
  {% if target_database %} 
    {% do return(api.Relation.create(
        database=target_database,
        schema=target_schema,
        identifier=target_table
    )) %}
  {% else %}
    {% do return(api.Relation.create(
        database=none,
        schema=target_schema,
        identifier=target_table
    )) %}
  {% endif %}
{% endmacro %}

{% macro proplum_get_load_info_table_name() %}
  {% set relation = proplum_get_load_info_table_relation() %}  
  {% do return(relation.render()) %}
{% endmacro %}

{% macro proplum_get_last_extraction_date(model_target) %}
    {# Gets the last extraction date from dbt_load_info for a model #}
    {% set load_info_table = proplum_get_load_info_table_name() %}
    {% set lastdate_query %}
        SELECT COALESCE(MAX(extraction_to)) 
        FROM {{ load_info_table }}
        WHERE object_name = '{{ model_target }}'
        AND status = 2
    {% endset %}
    {% set result = run_query(lastdate_query) %}
    {% if result and result.columns[0][0] %}
        {{ return(result.columns[0][0]) }}
    {% endif %}
    {{ return(none) }}
{% endmacro %}

{% macro proplum_apply_safety_period(date, safety_period) %}
    {# Applies safety period to a date #}
    {% if not date %}
        {{ return(modules.datetime.datetime(2000, 1, 1)) }}
    {% endif %}
    
    {% if safety_period is string %}
        {% set safety_period = safety_period.lower() %}
        {% if 'day' in safety_period %}
            {% set days = safety_period.split(' ')[0] | int %}
            {{ return(date - modules.datetime.timedelta(days=days)) }}
        {% elif 'month' in safety_period %}
            {% set months = safety_period.split(' ')[0] | int %}
            {{ return(date - modules.datetime.timedelta(days=months*31)) }}
        {% endif %}
    {% endif %}
    
    {{ return(date) }}
{% endmacro %}

{% macro proplum_calculate_adjusted_dates(start_date, end_date, load_interval) %}
    {#
    Returns adjusted date
    Usage:
        {% set extraction_from, extraction_to = proplum_calculate_adjusted_dates(...) %}
    #}
    {% set load_interval_lower = load_interval | lower %}
    
    {# Convert string inputs to datetime objects if needed #}
    {% if start_date is string %}
        {% set start_date = modules.datetime.datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S') %}
    {% endif %}
    {% if end_date is string %}
        {% set end_date = modules.datetime.datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S') %}
    {% endif %}

    {# Extract numeric value and unit from interval string #}
    {% set interval_parts = load_interval_lower.split() %}
    {% set interval_value = interval_parts[0] | int %}
    {% set interval_unit = interval_parts[1] if interval_parts|length > 1 else '' %}
    {% if 'month' in interval_unit or 'mon' in interval_unit %}
        {# Month handling - approximate with 31 days #}
        {% set adjusted_start = start_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0) %}
        {% set total_days = interval_value * 31 %}
        {% set adjusted_end = end_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0) + modules.datetime.timedelta(days=total_days) %}
        
    {% elif 'day' in interval_unit %}
        {% set adjusted_start = start_date.replace(hour=0, minute=0, second=0, microsecond=0) %}
        {% set total_days = interval_value %}
        {% set adjusted_end = end_date.replace(hour=0, minute=0, second=0, microsecond=0) + modules.datetime.timedelta(days=total_days) %}
        
    {% elif 'year' in interval_unit %}
        {# Year handling - approximate with 365 days #}
        {% set adjusted_start = start_date.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0) %}
        {% set total_days = interval_value * 365 %}
        {% set adjusted_end = end_date.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0) + modules.datetime.timedelta(days=total_days) %}
        
    {% elif 'hour' in interval_unit %}
        {% set adjusted_start = start_date.replace(minute=0, second=0, microsecond=0) %}
        {% set adjusted_end = end_date.replace(minute=0, second=0, microsecond=0) + modules.datetime.timedelta(hours=interval_value) %}
        
    {% else %}
        {% set adjusted_start = start_date %}
        {% set adjusted_end = end_date %}
    {% endif %}
    {{ return((adjusted_start, adjusted_end)) }}
{% endmacro %}

{% macro proplum_convert_var_to_datetime(date_input) %}
    {#
    Converts various date input types to datetime objects
    #}
    {% if date_input is none %}
        {{ return(none) }}
    {% elif date_input is string %}
        {% if ' ' in date_input %}
            {# Format with time component #}
            {% set dt = modules.datetime.datetime.strptime(date_input, '%Y-%m-%d %H:%M:%S') %}
        {% else %}
            {# Just date component #}
            {% set dt = modules.datetime.datetime.strptime(date_input, '%Y-%m-%d') %}
        {% endif %}
        {{ return(dt) }}
    {% else %}
        {% set date_string = date_input|string %}
        
        {# Check if it like a date/datetime #}
        {% if '-' in date_string and ':' in date_string %}
            {# Likely a datetime - convert from string representation #}
            {% set dt = modules.datetime.datetime.strptime(date_string.split('.')[0], '%Y-%m-%d %H:%M:%S') %}
            {{ return(dt) }}
        {% elif '-' in date_string %}
            {# Likely a date - convert from string representation #}
            {% set dt = modules.datetime.datetime.strptime(date_string, '%Y-%m-%d') %}
            {{ return(dt) }}
        {% else %}
            {{ exceptions.raise_compiler_error("Invalid date format. Expected string, date or datetime, got: " ~ date_string) }}
        {% endif %}
    {% endif %}
{% endmacro %}

{% macro proplum_get_extraction_dates(model_target=none, safety_period='0 days', delta_start_date=none, load_interval='', log_data=false, load_method='',extraction_type='',delta_field='',flag_commit=false) %}
    {#
    Calculates extraction_from and extraction_to dates based on configuration
    Parameters:
        - model_target: The target model name to lookup last extraction date
        - safety_period: Safety period string (e.g., '2 days')
        - delta_start_date: Fallback start date if no history exists
        - load_interval: Interval string for date adjustment
        - log_data: Boolean to control whether to log to dbt_load_info
        - load_method: type of connection used in external table
        - extraction_type: flag partition delta field in external table
    Returns:
        Dictionary with extraction_from and extraction_to dates
    #}

    {# Check for user-provided variables first #}
    {% set user_start_date = var('start_date', default=none) %}
    {% set user_end_date = var('end_date', default=none) %}  
    {% if user_start_date is not none and user_end_date is not none %}
        {% set start_extr = proplum_convert_var_to_datetime(user_start_date) %}
        {% set end_extr = proplum_convert_var_to_datetime(user_end_date) %}
    {% else %}
        {% set delta_start_date = delta_start_date or modules.datetime.datetime(2000, 1, 1) %}
        {% set end_extr = modules.datetime.datetime.now() %}

        {# Get last extraction date if model_target provided #}
        {% if model_target %}
            {% set lastdate = proplum_get_last_extraction_date(model_target) %}
            {% set start_extr = lastdate if lastdate and lastdate != modules.datetime.datetime(1970, 1, 1) else delta_start_date %}
        {% else %}
            {% set start_extr = delta_start_date %} 
        {% endif %}
    {% endif %}
    
    {# Adjust dates based on load interval if provided #}
    {% if load_interval %}
        {% set start_extr, end_extr = proplum_calculate_adjusted_dates(
            start_date=start_extr,
            end_date=end_extr,
            load_interval=load_interval
        ) %}
    {% endif %}
    
    {# Apply safety period #}
    {% set extraction_from = proplum_apply_safety_period(start_extr, safety_period) %}
    {% set extraction_to = end_extr %}

    {% if log_data and model_target %}
        {% do adapter.dispatch('proplum_log_extraction_dates')(model_target=model_target, extraction_from=extraction_from,extraction_to=extraction_to, load_method=load_method,extraction_type=extraction_type,delta_field=delta_field) %}
    {% endif %}
    
    {{ return({
        'extraction_from': extraction_from,
        'extraction_to': extraction_to
    }) }}
{% endmacro %}