{% set connect_string %}
  LOCATION ('pxf://tpch.supplier?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://{{ var('external_db_address') }}/postgres&USER={{ var('external_db_user') }}&PASS={{ var('external_db_password') }}') ON ALL 
  FORMAT 'CUSTOM' ( FORMATTER='pxfwritable_import' ) 
  ENCODING 'UTF8'
{% endset %}

{{ config(
    materialized='external_table',
    connect_string=connect_string,
    columns="""
    s_suppkey int4,
    s_nationkey int4,
    s_comment text,
    s_name text,
    s_address text,
    s_phone text,
    s_acctbal numeric
    """,
    schema='stg_dbt',
    tags=["supplier","tpch"]
) }}

--{{ref('nation')}}
--{{ref('region')}}
--{{ref('customer')}}