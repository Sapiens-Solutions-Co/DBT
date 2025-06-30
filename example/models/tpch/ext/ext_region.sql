{% set connect_string %}
  LOCATION ('pxf://tpch.region?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://{{ var('external_db_address') }}/postgres&USER={{ var('external_db_user') }}&PASS={{ var('external_db_password') }}') ON ALL 
  FORMAT 'CUSTOM' ( FORMATTER='pxfwritable_import' ) 
  ENCODING 'UTF8'
{% endset %}

{{ config(
    materialized='external_table',
    connect_string=connect_string,
    columns="""
    r_regionkey int4,
    r_name text,
    r_comment text
    """,
    schema='stg_dbt',
    tags=["region","tpch"]
) }}
