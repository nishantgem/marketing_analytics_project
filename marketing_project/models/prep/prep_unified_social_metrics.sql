{{ config(materialized='view') }}

select * from {{ ref('stg_meta_mx') }}
union all
select * from {{ ref('stg_meta_non_mx') }}
union all
select * from {{ ref('stg_pinterest') }}
union all
select * from {{ ref('stg_reddit') }}
