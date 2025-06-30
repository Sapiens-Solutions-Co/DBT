{{
  config(
    materialized='proplum',
    incremental_strategy='partitions',
    engine='MergeTree()',
    partition_by= 'toYYYYMM(pstng_date)',
    merge_keys=['ac_doc_ln','ac_doc_nr','ac_ledger','comp_code','fiscper','fiscvarnt','fiscvrnt_e','rectype','version'],
    merge_partitions = False,
    delta_field = 'tmstmp',
    order_by='ac_doc_ln,ac_doc_nr,ac_ledger,comp_code,fiscper,fiscvarnt,fiscvrnt_e,rectype,version',
    settings={
      'allow_nullable_key': 1,
      'index_granularity': 8192
    }    
  )
}}

SELECT *
FROM default.ext_fi_gl_14_dbt