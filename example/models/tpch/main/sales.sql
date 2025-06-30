{% set raw_partition %}
    PARTITION BY RANGE(orderdate)
    (
        START ('2018-01-01'::date) END ('2023-01-01'::date) EVERY ('1 year'::interval) WITH (appendonly='true', orientation='column', compresstype=zstd, compresslevel='1'),
        START ('2023-01-01'::date) END ('2023-08-01'::date) EVERY ('1 mon'::interval) WITH (appendonly='true', orientation='column', compresstype=zstd, compresslevel='1'), 
        DEFAULT PARTITION def  WITH (appendonly='true', orientation='column', compresstype=zstd, compresslevel='1') 
    );
{% endset %}

{% set fields_string %}
	orderkey int8 NULL,
	linenumber int8 NULL,
	partkey int8 NULL,
	suppkey int4 NULL,
	receiptdate date NULL,
	orderdate date NULL,
	orderstatus varchar(50) NULL,
	address varchar(40) NULL,
	line_comment text NULL,
	returnflag bpchar(1) NULL,
	order_comment text NULL,
	orderpriority varchar(50) NULL,
	shipmode bpchar(10) NULL,
	quantity numeric NULL,
	discount numeric NULL,
	load_timestamp timestamp NULL
{% endset %}

{{ config(
    materialized='proplum',
    incremental_strategy='partitions',
    schema='dbt',
    tags=["sales","tpch"],
    appendonly=true,
    orientation='column',
    compresstype='zstd',
    compresslevel='1',
    delta_field='receiptdate',
    raw_partition=raw_partition,
    fields_string=fields_string,
    distributed_by='orderkey', 
    merge_partitions=true,
    safety_period='0 days',
    load_interval='1 mon',
    add_delta=False,
    merge_keys=['orderkey','linenumber'],
    post_hook= [
        "GRANT USAGE ON SCHEMA {{ this.schema }} TO \"bot.noreply\";",
        "GRANT SELECT ON {{ this }} TO \"bot.noreply\";"]
) }}


select
    *,
    current_timestamp as load_timestamp
    from {{ref('v_sales')}}
where 1=1    
{{ proplum_filter_delta('receiptdate', true) }}    