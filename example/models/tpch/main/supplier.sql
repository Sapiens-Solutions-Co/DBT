{{ config(
    materialized='proplum',
    incremental_strategy='full',
    schema = 'dbt',
    tags=["supplier","tpch"],
    appendonly = true,
    orientation = 'column',
    compresstype = 'zstd',
    compresslevel = '1',
    distributed_replicated = true
) }}

select 
    CAST(s_suppkey as int4) as suppkey,
    CAST(s_nationkey as int4) as nationkey,
    CAST(s_comment as text) as comment,
    CAST(s_name as bpchar(25)) as name,
    CAST(s_address as varchar(40)) as address,
    CAST(s_phone as bpchar(15)) as phone,
    CAST(s_acctbal as numeric(19,4)) as acctbal
    from {{ref('ext_supplier')}}