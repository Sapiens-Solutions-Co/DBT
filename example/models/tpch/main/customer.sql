{{ config(
    materialized='proplum',
    incremental_strategy='delta_upsert',
    schema = 'dbt',
    tags=["customer","tpch"],
    appendonly = true,
    orientation = 'column',
    compresstype = 'zstd',
    compresslevel = '1',
    distributed_by = 'custkey',
    merge_keys=['custkey'],
    delta_field = 'modifiedts'
) }}


select 
    CAST(c_custkey as int8) as custkey,
	CAST(c_mktsegment as varchar(10)) as mktsegment,
	CAST(c_nationkey as int4) as nationkey,
	CAST(c_name as varchar(25)) as name,
	CAST(c_address as varchar(40)) as address,
	CAST(c_phone as varchar(15)) as phone,
	CAST(c_acctbal as numeric(19,4)) as acctbal,
	CAST(c_comment as text) as comment,
	CAST(c_modifiedts as timestamp) as modifiedts
    from {{ref('ext_customer')}}