{{ config(materialized='table') }}

select
    date,
    platform,

    sum(costs) as total_spend,
    sum(cost_usd) as total_spend_usd,

    sum(impressions) as total_impressions,
    sum(clicks) as total_clicks,

    sum(total_conversions) as total_conversions,
    sum(total_conversion_revenue) as total_revenue,
    sum(total_conversion_revenue_usd) as total_revenue_usd,

    -- Metrics
    case when sum(costs) = 0 then null
         else sum(total_conversion_revenue) / sum(costs)
    end as roas,

    case when sum(cost_usd) = 0 then null
         else sum(total_conversion_revenue_usd) / sum(cost_usd)
    end as roas_usd,

    case when sum(impressions) = 0 then null
         else sum(clicks) * 1.0 / sum(impressions)
    end as ctr,

    case when sum(clicks) = 0 then null
         else sum(costs) / sum(clicks)
    end as cpc,

    case when sum(impressions) = 0 then null
         else (sum(costs) * 1000.0) / sum(impressions)
    end as cpm

from {{ ref('prep_unified_social_metrics') }}
group by 1, 2
