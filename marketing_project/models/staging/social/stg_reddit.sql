{{ config(materialized='view') }}

select
    to_timestamp(date / 1000000000)::date as date,
    'reddit' as platform,

    media_channel_ch,
    type,
    platform_pl,
    data_source,
    funding_source_fs,
    sub_brand_sb,
    product_category_pr,
    campaign_name,
    campaign_name_cn,
    mindset_md,
    quarter,
    month,
    cw_iso,
    year,
    division_bs,
    business_activity,
    kpi_pk,

    costs,
    cost_usd,
    impressions,
    clicks,
    null as total_conversions,
    null as total_conversion_revenue,
    null as total_conversion_revenue_usd,
    null as mobile_app_installs,
    objective_ob

from {{ source('social_raw', 'reddit') }}
