{% set raw_partition %}
    PARTITION BY RANGE(receiptdate) 
    (
        START ('2020-01-01'::date) END ('2023-01-01'::date) EVERY ('1 year'::interval) WITH (appendonly='true', orientation='column', compresstype=zstd, compresslevel='1'),
        START ('2023-01-01'::date) END ('2023-08-01'::date) EVERY ('1 mon'::interval) WITH (appendonly='true', orientation='column', compresstype=zstd, compresslevel='1'), 
        DEFAULT PARTITION def  WITH (appendonly='true', orientation='column', compresstype=zstd, compresslevel='1')
    );
{% endset %}

{% set fields_string %}
	shipdate date NULL,
	orderkey int8 NULL,
	discount numeric(19, 4) NULL,
	extendedprice numeric(19, 4) NULL,
	suppkey int4 NULL,
	quantity int8 NULL,
	returnflag bpchar(1) NULL,
	partkey int8 NULL,
	linestatus bpchar(1) NULL,
	tax numeric(19, 4) NULL,
	commitdate date NULL,
	receiptdate date NULL,
	shipmode bpchar(10) NULL,
	linenumber int8 NULL,
	shipinstruct bpchar(25) NULL,
	comment text NULL
{% endset %}

{{ config(
    materialized='proplum',
    incremental_strategy='partitions',
    schema = 'dbt',
    tags=["lineitem","tpch"],
    appendonly = true,
    orientation = 'column',
    compresstype = 'zstd',
    compresslevel = '1',
    delta_field = 'receiptdate',
    raw_partition=raw_partition,
    fields_string=fields_string,
    distributed_by = 'orderkey',
    merge_partitions=false     
) }}


select 
	CAST(l_shipdate as date) as shipdate,
	CAST(l_orderkey as int8) orderkey,
	CAST(l_discount as numeric(19,4)) as discount,
	CAST(l_extendedprice as numeric(19,4)) as extendedprice,
	CAST(l_suppkey as int4) as suppkey,
	CAST(l_quantity as int8) as quantity,
	CAST(l_returnflag as bpchar(1)) as returnflag,
	CAST(l_partkey as int8) partkey,
	CAST(l_linestatus as bpchar(1)) as linestatus,
	CAST(l_tax as numeric(19,4)) as tax,
	CAST(l_commitdate as date) as commitdate,
	CAST(l_receiptdate as date) as receiptdate,
	CAST(l_shipmode as bpchar(10)) as shipmode,
	CAST(l_linenumber as int8) as linenumber,
	CAST(l_shipinstruct as bpchar(25)) as shipinstruct,
	CAST(l_comment as text) as comment
    from {{ref('ext_lineitem')}}