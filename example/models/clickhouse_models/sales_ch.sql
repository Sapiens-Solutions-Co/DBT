{{
  config(
    materialized='proplum',
    incremental_strategy='partitions',
    engine='MergeTree()',
    partition_by= 'toYYYYMM(orderdate)',
    merge_keys=['orderkey','linenumber'],
    tags=["tpch_ch"],
    merge_partitions=true,
    add_delta=False,
    delta_field = 'receiptdate',
    order_by='orderkey,linenumber',
    settings={
      'allow_nullable_key': 1,
      'index_granularity': 8192
    }    
  )
}}

SELECT *
FROM {{ref('ext_sales_ch')}}
where 1=1
{{ proplum_filter_delta('receiptdate',true) }}