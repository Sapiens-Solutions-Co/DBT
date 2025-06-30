{% set connect_string = 'tpch.partsupp2?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://'~ var('external_db_address')~'/postgres&USER='~ var('external_db_user')~'&PASS='~var('external_db_password') %}

{{ config(
    materialized='external_table',
    connect_string=connect_string,
    columns="""
	ps_partkey int8,
	ps_suppkey int4,
	ps_supplycost numeric,
	ps_availqty int4,
	ps_comment text,
	ps_modifiedts timestamp
    """,
    schema='stg_dbt',
    load_method = 'pxf',
    extraction_type = 'DELTA',
    tags=["partsupp","tpch"],
    model_target = 'partsupp',
    delta_field = 'ps_modifiedts',
    safety_period = '0 days',
    load_interval = '1 day'
) }}

--{{ref('part')}}
--{{ref('supplier')}}