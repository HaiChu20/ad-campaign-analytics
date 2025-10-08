select
    age,
    gender,
    
    -- Aggregated metrics
    count(distinct ad_id) as total_ads,
    count(distinct campaign_id) as total_campaigns,
    sum(impressions) as total_impressions,
    sum(clicks) as total_clicks,
    sum(spent) as total_spent,
    sum(total_conversion) as total_conversions,
    sum(approved_conversion) as approved_conversions,
    
    -- Performance metrics
    safe_divide(sum(clicks), sum(impressions)) as avg_ctr,
    safe_divide(sum(spent), nullif(sum(clicks), 0)) as avg_cpc,
    safe_divide(sum(spent), nullif(sum(impressions), 0)) * 1000 as avg_cpm,
    safe_divide(sum(approved_conversion), nullif(sum(clicks), 0)) as avg_conversion_rate,
    safe_divide(sum(approved_conversion), nullif(sum(total_conversion), 0)) as avg_approval_rate,
    
    -- Efficiency metrics
    safe_divide(sum(approved_conversion), nullif(sum(spent), 0)) as conversions_per_dollar,
    safe_divide(sum(spent), nullif(sum(approved_conversion), 0)) as cost_per_conversion,
    
    -- Performance ranking
    row_number() over (order by safe_divide(sum(clicks), sum(impressions)) desc) as ctr_rank,
    row_number() over (order by safe_divide(sum(approved_conversion), nullif(sum(clicks), 0)) desc) as conversion_rank,
    row_number() over (order by safe_divide(sum(approved_conversion), nullif(sum(spent), 0)) desc) as efficiency_rank

from {{ ref('stg_fb_ads_data') }}
group by age, gender
