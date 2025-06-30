{% set raw_partition %}
PARTITION BY RANGE(partkey) 
    (
    DEFAULT PARTITION def  WITH (appendonly='true', orientation='column', compresstype=zstd, compresslevel='1') 
            COLUMN partkey ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
            COLUMN suppkey ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
            COLUMN supplycost ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
            COLUMN availqty ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
            COLUMN comment ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
            COLUMN modifiedts ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768)
    );
{% endset %}

{% set fields_string %}
	partkey int8 NULL,
	suppkey int4 NULL,
	supplycost numeric(19, 4) NULL,
	availqty int4 NULL,
	comment text NULL,
	modifiedts timestamp NULL
{% endset %}

{{ config(
    materialized='proplum',
    incremental_strategy='delta_merge',
    schema = 'dbt',
    tags=["partsupp","tpch"],
    appendonly = true,
    orientation = 'column',
    compresstype = 'zstd',
    compresslevel = '1',
    merge_keys=['partkey','suppkey'],
    delta_field = 'modifiedts',
    raw_partition=raw_partition,
    fields_string=fields_string,
    distributed_by = 'partkey, suppkey'    
) }}


select 
	CAST(ps_partkey as int8) as partkey,
	CAST(ps_suppkey as int4) as suppkey,
	CAST(ps_supplycost as numeric(19,4)) as supplycost,
	CAST(ps_availqty as int4) as availqty,
	CAST(ps_comment as text) as comment,
	CAST(ps_modifiedts as timestamp) as modifiedts
    from {{ref('ext_partsupp')}}