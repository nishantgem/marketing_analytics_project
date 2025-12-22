{{ config(materialized='view') }}

select
    -- Python already converted date
    date as date,

    -- Platform identifier
    'reddit' as platform,

    -- Campaign ID extracted in Python
    campaign_id,

    -- Metadata fields
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

    -- Metrics Reddit actually has
    cast(costs as numeric) as costs,
    cast(cost_usd as numeric) as cost_usd,
    cast(impressions as bigint) as impressions,
    cast(clicks as bigint) as clicks,

    -- Reddit does NOT have conversion metrics → add NULL placeholders
    cast(null as bigint) as total_conversions,
    cast(null as numeric) as total_conversions_revenue,
    cast(null as numeric) as total_conversion_revenue_usd,

    -- Reddit does not have app installs
    cast(null as bigint) as mobile_app_installs,

    objective_ob

from {{ source('social_raw', 'reddit') }}
