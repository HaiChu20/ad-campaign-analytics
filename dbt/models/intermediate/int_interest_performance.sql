select
    interest_combination,
    interest1,
    interest2,
    interest3,
    
    -- Use macros for consistent metrics
    {{ all_performance_metrics() }},
    
    -- Volume categorization
    case 
        when sum(impressions) > 100000 then 'High Volume'
        when sum(impressions) > 10000 then 'Medium Volume'
        else 'Low Volume'
    end as volume_category,
    
    -- Use macro for CTR categorization
    {{ ctr_performance_category('safe_divide(sum(clicks), sum(impressions))') }} as ctr_category

from {{ ref('stg_fb_ads_data') }}
group by interest_combination, interest1, interest2, interest3
