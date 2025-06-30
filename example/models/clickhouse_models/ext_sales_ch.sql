{{
  config(
    materialized='external_table',
    engine='PostgreSQL',
    tags=["tpch_ch"],
    conn_config={ 
      'connection_name': 'mypg',
      'schema': 'dbt',
      'table': 'sales'
    },
    columns='
    orderkey Nullable(Int64),
    linenumber Nullable(UInt64),
    partkey Nullable(Int64),
    suppkey Nullable(Int32),
    receiptdate Nullable(Date32),
    orderdate Nullable(Date32),
    orderstatus Nullable(String),
    address Nullable(String),
    line_comment Nullable(String),
    returnflag Nullable(String),
    order_comment Nullable(String),
    orderpriority Nullable(String),
    shipmode Nullable(String),
    quantity Nullable(Decimal(18,
 2)),
    discount Nullable(Decimal(5,
 2)),
    load_timestamp Nullable(DateTime)
    
    '
  )
}}