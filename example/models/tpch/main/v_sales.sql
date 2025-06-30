{{ config(
    materialized='view',
    schema = 'dbt',
    tags=["sales","tpch"]
) }}


SELECT l.orderkey,
    l.linenumber,
    l.partkey,
    l.suppkey,
    l.receiptdate,
    o.orderdate,
    o.orderstatus,
    c.address,
    l.comment AS line_comment,
    l.returnflag,
    o.comment AS order_comment,
    o.orderpriority,
    l.shipmode,
    sum(l.quantity) AS quantity,
    sum(l.discount) AS discount
   FROM {{ref('orders')}} o
     JOIN {{ref('lineitem')}} l ON o.orderkey = l.orderkey
     JOIN {{ref('customer')}} c ON o.custkey = c.custkey
  GROUP BY l.orderkey, l.linenumber, l.partkey, l.suppkey, l.receiptdate, o.orderdate, o.orderstatus, c.address, l.comment, l.returnflag, o.comment, o.orderpriority, l.shipmode