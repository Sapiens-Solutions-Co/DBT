{% set raw_partition %}
    PARTITION BY RANGE(orderdate)
    (
        START ('2018-01-01'::date) END ('2023-01-01'::date) EVERY ('1 year'::interval) WITH (appendonly='true', orientation='column', compresstype=zstd, compresslevel='1') ,
        START ('2023-01-01'::date) END ('2023-08-01'::date) EVERY ('1 mon'::interval) WITH (appendonly='true', orientation='column', compresstype=zstd, compresslevel='1'), 
        DEFAULT PARTITION def  WITH (appendonly='true', orientation='column', compresstype=zstd, compresslevel='1') 
    );
{% endset %}

{% set fields_string %}
	orderkey int4 NULL,
	custkey int4 NULL,
	orderstatus varchar(50) NULL,
	totalprice float4 NULL,
	orderdate date NULL,
	orderpriority varchar(50) NULL,
	clerk varchar(50) NULL,
	shippriority int4 NULL,
	comment text NULL
{% endset %}

{{ config(
    materialized='proplum',
    incremental_strategy='partitions',
    schema = 'dbt',
    tags=["orders","tpch"],
    appendonly = true,
    orientation = 'column',
    compresstype = 'zstd',
    compresslevel = '1',
    delta_field = 'orderdate',
    raw_partition=raw_partition,
    fields_string=fields_string,
    distributed_by = 'orderkey',
    merge_partitions=false    
) }}


select
    CAST(o_orderkey as int4) as orderkey,
	CAST(o_custkey as int4) as custkey,
	CAST(o_orderstatus as varchar(50)) as orderstatus,
	CAST(o_totalprice as float4) as totalprice,
	CAST(o_orderdate as date) as orderdate,
	CAST(o_orderpriority as varchar(50)) as orderpriority,
	CAST(o_clerk as varchar(50)) as clerk,
	CAST(o_shippriority as int4) as shippriority,
	CAST(o_comment as text) as comment
    from {{ref('ext_orders')}}