
{{ config(
    materialized='proplum',
    incremental_strategy='full',
    schema = 'dbt',
    tags=["nation","tpch"],
    appendonly = true,
    orientation = 'column',
    compresstype = 'zstd',
    compresslevel = '1',
    distributed_replicated = true
) }}

select 
    CAST(n_nationkey AS int4) as nationkey,
    CAST(n_name as bpchar(25)) as name,
    CAST(n_regionkey as int4) as regionkey,
    CAST(n_comment as text) as comment 
    from {{ref('ext_nation')}}
    