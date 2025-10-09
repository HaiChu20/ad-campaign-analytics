select
    age,
    gender,
    
    {{ all_performance_metrics() }},
    
    -- Performance ranking
    row_number() over (order by safe_divide(sum(clicks), sum(impressions)) desc) as ctr_rank,
    row_number() over (order by safe_divide(sum(approved_conversion), nullif(sum(clicks), 0)) desc) as conversion_rank,
    row_number() over (order by safe_divide(sum(approved_conversion), nullif(sum(spent), 0)) desc) as efficiency_rank

from {{ ref('stg_fb_ads_data') }}
group by age, gender
