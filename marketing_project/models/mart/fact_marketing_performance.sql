{{ config(materialized='table') }}

select
    date,
    campaign_id,

    -- Total Spend (USD)
    coalesce(meta_mx_cost_usd, 0)
    + coalesce(meta_non_mx_cost_usd, 0)
    + coalesce(pinterest_cost_usd, 0)
    + coalesce(reddit_cost_usd, 0) as total_spend_usd,

    -- Total Impressions
    coalesce(meta_mx_impressions, 0)
    + coalesce(meta_non_mx_impressions, 0)
    + coalesce(pinterest_impressions, 0)
    + coalesce(reddit_impressions, 0) as total_impressions,

    -- Total Clicks
    coalesce(meta_mx_clicks, 0)
    + coalesce(meta_non_mx_clicks, 0)
    + coalesce(pinterest_clicks, 0)
    + coalesce(reddit_clicks, 0) as total_clicks,

    -- Total Conversions
    coalesce(meta_mx_conversions, 0)
    + coalesce(meta_non_mx_conversions, 0)
    + coalesce(pinterest_conversions, 0)
    + coalesce(null, 0) as total_conversions,

    -- Total Revenue (USD)
    coalesce(meta_mx_revenue_usd, 0)
    + coalesce(meta_non_mx_revenue_usd, 0)
    + coalesce(pinterest_revenue_usd, 0)
    + coalesce(null, 0) as total_revenue_usd,

    -- ROAS (Revenue / Spend)
    case 
        when (
            coalesce(meta_mx_cost_usd, 0)
            + coalesce(meta_non_mx_cost_usd, 0)
            + coalesce(pinterest_cost_usd, 0)
            + coalesce(reddit_cost_usd, 0)
        ) = 0 then null
        else (
            coalesce(meta_mx_revenue_usd, 0)
            + coalesce(meta_non_mx_revenue_usd, 0)
            + coalesce(pinterest_revenue_usd, 0)
        ) / (
            coalesce(meta_mx_cost_usd, 0)
            + coalesce(meta_non_mx_cost_usd, 0)
            + coalesce(pinterest_cost_usd, 0)
            + coalesce(reddit_cost_usd, 0)
        )
    end as roas_usd,

    -- CTR (Clicks / Impressions)
    case 
        when (
            coalesce(meta_mx_impressions, 0)
            + coalesce(meta_non_mx_impressions, 0)
            + coalesce(pinterest_impressions, 0)
            + coalesce(reddit_impressions, 0)
        ) = 0 then null
        else (
            coalesce(meta_mx_clicks, 0)
            + coalesce(meta_non_mx_clicks, 0)
            + coalesce(pinterest_clicks, 0)
            + coalesce(reddit_clicks, 0)
        ) * 1.0 / (
            coalesce(meta_mx_impressions, 0)
            + coalesce(meta_non_mx_impressions, 0)
            + coalesce(pinterest_impressions, 0)
            + coalesce(reddit_impressions, 0)
        )
    end as ctr,

    -- CPC (Cost / Clicks)
    case 
        when (
            coalesce(meta_mx_clicks, 0)
            + coalesce(meta_non_mx_clicks, 0)
            + coalesce(pinterest_clicks, 0)
            + coalesce(reddit_clicks, 0)
        ) = 0 then null
        else (
            coalesce(meta_mx_cost_usd, 0)
            + coalesce(meta_non_mx_cost_usd, 0)
            + coalesce(pinterest_cost_usd, 0)
            + coalesce(reddit_cost_usd, 0)
        ) / (
            coalesce(meta_mx_clicks, 0)
            + coalesce(meta_non_mx_clicks, 0)
            + coalesce(pinterest_clicks, 0)
            + coalesce(reddit_clicks, 0)
        )
    end as cpc_usd,

    -- CPM (Cost per 1000 impressions)
    case 
        when (
            coalesce(meta_mx_impressions, 0)
            + coalesce(meta_non_mx_impressions, 0)
            + coalesce(pinterest_impressions, 0)
            + coalesce(reddit_impressions, 0)
        ) = 0 then null
        else (
            coalesce(meta_mx_cost_usd, 0)
            + coalesce(meta_non_mx_cost_usd, 0)
            + coalesce(pinterest_cost_usd, 0)
            + coalesce(reddit_cost_usd, 0)
        ) * 1000.0 / (
            coalesce(meta_mx_impressions, 0)
            + coalesce(meta_non_mx_impressions, 0)
            + coalesce(pinterest_impressions, 0)
            + coalesce(reddit_impressions, 0)
        )
    end as cpm_usd

from {{ ref('prep_unified_social_metrics') }}
