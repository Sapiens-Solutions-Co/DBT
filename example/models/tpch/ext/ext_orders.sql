{% set connect_string = 'tpch.orders?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://'~ var('external_db_address')~'/postgres&USER='~ var('external_db_user')~'&PASS='~var('external_db_password') %}

{{ config(
    materialized='external_table',
    connect_string=connect_string,
    columns="""
	o_orderkey int4,
	o_custkey int4,
	o_orderstatus text,
	o_totalprice float4,
	o_orderdate date,
	o_orderpriority text,
	o_clerk text,
	o_shippriority int4,
	o_comment text
    """,
    schema='stg_dbt',
    load_method = 'pxf',
    extraction_type = 'DELTA',
    tags=["orders","tpch"],
    model_target = 'orders',
    delta_field = 'o_orderdate',
    safety_period = '0 days',
    load_interval = '1 mon'
) }}

--{{ref('partsupp')}}