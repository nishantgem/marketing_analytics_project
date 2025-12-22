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
        coalesce(mx.date, mn.date, p.date, r.date) as date,

        -- Platform identifiers (optional)
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

        -- Reddit metrics (no conversions)
        r.cost_usd as reddit_cost_usd,
        r.impressions as reddit_impressions,
        r.clicks as reddit_clicks,

        -- Optional: add metadata fields from any platform
        coalesce(mx.campaign_name, mn.campaign_name, p.campaign_name, r.campaign_name) as campaign_name

    from meta_mx mx
    full outer join meta_non_mx mn
        on mx.campaign_id = mn.campaign_id
        and mx.date = mn.date

    full outer join pinterest p
        on coalesce(mx.campaign_id, mn.campaign_id) = p.campaign_id
        and coalesce(mx.date, mn.date) = p.date

    full outer join reddit r
        on coalesce(mx.campaign_id, mn.campaign_id, p.campaign_id) = r.campaign_id
        and coalesce(mx.date, mn.date, p.date) = r.date
)

select * from unified
