{{
  config(
    materialized='external_table',
    engine='PostgreSQL',
    conn_config={
      'connection_name': 'mypg',
      'schema': 'ods',
      'table': 'customer_text'
    },
    columns='
    customer String,
    txtmd Nullable(String),
    tmstmp DateTime
    '
  )
}}