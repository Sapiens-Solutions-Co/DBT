{% set connect_string %}
  LOCATION ('pxf://tpch.nation?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://{{ var('external_db_address') }}/postgres&USER={{ var('external_db_user') }}&PASS={{ var('external_db_password') }}') ON ALL 
  FORMAT 'CUSTOM' ( FORMATTER='pxfwritable_import' ) 
  ENCODING 'UTF8'
{% endset %}

{{ config(
    materialized='external_table',
    connect_string=connect_string,
    columns="""
    n_nationkey int4,
    n_name text,
    n_regionkey int4,
    n_comment text
    """,
    schema='dbt',
    tags=["nation","tpch"]
) }}