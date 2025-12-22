{{ config(materialized='view') }}

with meta_mx as (
    select * from {{ ref('stg_meta_mx') }}
),

meta_non_mx as (
    select * from {{ ref('stg_meta_non_mx') }}
),

pinterest as (
    select * from {{ ref('stg_pinterest') }}
),

reddit as (
    select * from {{ ref('stg_reddit') }}
),

unified as (

    select
        -- Join keys
        coalesce(mx.campaign_id, mn.campaign_id, p.campaign_id, r.campaign_id) as campaign_id,

        -- FIXED: Convert nanoseconds → microseconds → timestamp → date
        date(
            timestamp_micros(
                coalesce(mx.date, mn.date, p.date, r.date) / 1000
            )
        ) as date,

        -- Platform identifiers
        mx.platform as meta_mx_platform,
        mn.platform as meta_non_mx_platform,
        p.platform as pinterest_platform,
        r.platform as reddit_platform,

        -- Meta MX metrics
        mx.cost_usd as meta_mx_cost_usd,
        mx.impressions as meta_mx_impressions,
        mx.clicks as meta_mx_clicks,
        mx.total_conversions as meta_mx_conversions,
        mx.total_conversion_revenue_usd as meta_mx_revenue_usd,

        -- Meta Non-MX metrics
        mn.cost_usd as meta_non_mx_cost_usd,
        mn.impressions as meta_non_mx_impressions,
        mn.clicks as meta_non_mx_clicks,
        mn.total_conversions as meta_non_mx_conversions,
        mn.total_conversion_revenue_usd as meta_non_mx_revenue_usd,

        -- Pinterest metrics
        p.cost_usd as pinterest_cost_usd,
        p.impressions as pinterest_impressions,
        p.clicks as pinterest_clicks,
        p.total_conversions as pinterest_conversions,
        p.total_conversion_revenue_usd as pinterest_revenue_usd,

        -- Reddit metrics
        r.cost_us)