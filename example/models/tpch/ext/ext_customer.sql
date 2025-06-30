{% set connect_string = 'tpch.customer2?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://'~ var('external_db_address')~'/postgres&USER='~ var('external_db_user')~'&PASS='~var('external_db_password') %}

{{ config(
    materialized='external_table',
    connect_string=connect_string,
    columns="""
	c_custkey int8,
	c_mktsegment text,
	c_nationkey int4,
	c_name text,
	c_address text,
	c_phone text,
	c_acctbal numeric,
	c_comment text,
	c_modifiedts timestamp
    """,
    schema='stg_dbt',
    load_method = 'pxf',
    extraction_type = 'DELTA',
    tags=["customer","tpch"],
    model_target = 'customer',
    delta_field = 'c_modifiedts',
    safety_period = '0 days'
) }}