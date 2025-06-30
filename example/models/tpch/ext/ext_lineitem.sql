{% set connect_string = 'tpch.lineitem?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://'~ var('external_db_address')~'/postgres&USER='~ var('external_db_user')~'&PASS='~var('external_db_password') %}

{{ config(
    materialized='external_table',
    connect_string=connect_string,
	columns="""
	l_shipdate date,
	l_orderkey int8,
	l_discount numeric,
	l_extendedprice numeric,
	l_suppkey int4,
	l_quantity int8,
	l_returnflag text,
	l_partkey int8,
	l_linestatus text,
	l_tax numeric,
	l_commitdate date,
	l_receiptdate date,
	l_shipmode text,
	l_linenumber int8,
	l_shipinstruct text,
	l_comment text
    """,
    schema='stg_dbt',
    load_method = 'pxf',
    extraction_type = 'DELTA',
    tags=["lineitem","tpch"],
    model_target = 'lineitem',
    delta_field = 'l_receiptdate',
    safety_period = '0 days',
    load_interval = '1 mon'
) }}

--{{ref('partsupp')}}