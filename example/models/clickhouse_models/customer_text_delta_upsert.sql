{{
  config(
    materialized='proplum',
    delta_field='tmstmp',
    merge_keys=['customer'],
    incremental_strategy='delta_upsert',
    engine='MergeTree()',
    order_by='tmstmp'
  )
}}

SELECT 
  customer::String AS customer,
  txtmd::Nullable(String) AS txtmd,
  tmstmp::DateTime AS tmstmp
FROM default.ext_customer_text_dbt_model