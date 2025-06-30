{% set connect_string %}
  LOCATION ('pxf://tpch.part?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://{{ var('external_db_address') }}/postgres&USER={{ var('external_db_user') }}&PASS={{ var('external_db_password') }}') ON ALL 
  FORMAT 'CUSTOM' ( FORMATTER='pxfwritable_import' ) 
  ENCODING 'UTF8'
{% endset %}

{{ config(
    materialized='external_table',
    connect_string=connect_string,
    columns="""
    p_partkey int8,
    p_type text,
    p_size int4,
    p_brand text,
    p_name text,
    p_container text,
    p_mfgr text,
    p_retailprice numeric,
    p_comment text
    """,
    schema='stg_dbt',
    tags=["part","tpch"]
) }}

--{{ref('nation')}}
--{{ref('region')}}
--{{ref('customer')}}