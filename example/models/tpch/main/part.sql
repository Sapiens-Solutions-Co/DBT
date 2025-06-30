{{ config(
    materialized='proplum',
    incremental_strategy='full',
    schema = 'dbt',
    tags=["part","tpch"],
    appendonly = true,
    orientation = 'column',
    compresstype = 'zstd',
    compresslevel = '1',
    distributed_by = 'partkey'
) }}

select 
    CAST(p_partkey as int8) as partkey,
    CAST(p_type as varchar(25)) as type,
    CAST(p_size as int4) as size,
    CAST(p_brand as bpchar(10)) as brand,
    CAST(p_name as varchar(55)) as name,
    CAST(p_container as bpchar(10)) as container,
    CAST(p_mfgr as bpchar(25)) as mfgr,
    CAST(p_retailprice as numeric(19, 4)) as retailprice,
    CAST(p_comment as text) as comment 
    from {{ref('ext_part')}}