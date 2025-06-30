{{
  config(
    materialized='proplum',
    incremental_strategy='full',
    engine='MergeTree()',
    order_by='tmstmp'
  )
}}

SELECT 
  customer::String AS customer,
  txtmd::Nullable(String) AS txtmd,
  tmstmp::DateTime AS tmstmp
FROM default.ext_customer_text_dbt
where 1=1