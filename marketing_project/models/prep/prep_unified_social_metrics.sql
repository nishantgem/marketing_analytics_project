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

        -- FIXED: Convert nanoseconds → seconds → timestamp → date (Postgres)
        to_timestamp(
            coalesce(mx.date, mn.date, p.date, r.date) / 1000000000
        )::date as date,

        -- Core metadata (from your column list)
        coalesce(mx.platform, mn.platform, p.platform, r.platform) as platform,
        coalesce(mx.funding_source_fs, mn.funding_source_fs, p.funding_source_fs, r.funding_source_fs) as funding_source_fs,
        coalesce(mx.kpi_pk, mn.kpi_pk, p.kpi_pk, r.kpi_pk) as kpi_pk,
        coalesce(mx.objective_ob, mn.objective_ob, p.objective_ob, r.objective_ob) as objective_ob,
        coalesce(mx.quarter, mn.quarter, p.quarter, r.quarter) as quarter,
        coalesce(mx.month, mn.month, p.month, r.month) as month,
        coalesce(mx.cw_iso, mn.cw_iso, p.cw_iso, r.cw_iso) as cw_iso,
        coalesce(mx.year, mn.year, p.year, r.year) as year,
        coalesce(mx.division_bs, mn.division_bs, p.division_bs, r.division_bs) as division_bs,
        coalesce(mx.campaign_name_cn, mn.campaign_name_cn, p.campaign_name_cn, r.campaign_name_cn) as campaign_name_cn,

        -- Optional high‑level attributes you mentioned (if present in staging)
        coalesce(mx.media_channel_ch, mn.media_channel_ch, p.media_channel_ch, r.media_channel_ch) as media_channel_ch,
        coalesce(mx.type, mn.type, p.type, r.type) as type,
        coalesce(mx.platform_pl, mn.platform_pl, p.platform_pl, r.platform_pl) as platform_pl,
        coalesce(mx.data_source, mn.data_source, p.data_source, r.data_source) as data_source,
        coalesce(mx.sub_brand_sb, mn.sub_brand_sb, p.sub_brand_sb, r.sub_brand_sb) as sub_brand_sb,
        coalesce(mx.product_category_pr, mn.product_category_pr, p.product_category_pr, r.product_category_pr) as product_category_pr,
        coalesce(mx.mindset_md, mn.mindset_md, p.mindset_md, r.mindset_md) as mindset_md,
        coalesce(mx.business_activity, mn.business_activity, p.business_activity, r.business_activity) as business_activity,

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
        r.cost_usd as reddit_cost_usd,
        r.impressions as reddit_impressions,
        r.clicks as reddit_clicks,
        r.total_conversions as reddit_conversions,
        r.total_conversion_revenue_usd as reddit_revenue_usd

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
