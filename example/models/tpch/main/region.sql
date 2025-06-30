{{ config(
    materialized='proplum',
    incremental_strategy='full',
    schema = 'dbt',
    tags=["region","tpch"],
    appendonly = true,
    orientation = 'column',
    compresstype = 'zstd',
    compresslevel = '1',
    distributed_replicated = true
) }}


select 
    CAST(r_regionkey as int4) as regionkey,
    CAST(r_name as bpchar(25)) as name,
    CAST(r_comment as text) as comment
    from {{ref('ext_region')}}