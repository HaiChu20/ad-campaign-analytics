with campaign_metrics as (
    select
        concat(coalesce(campaign_id, 'null'), '_', coalesce(fb_campaign_id, 'null')) as campaign_id,  -- Create composite key
        fb_campaign_id,
        
        -- Aggregated metrics
        count(distinct ad_id) as total_ads,
        sum(impressions) as total_impressions,
        sum(clicks) as total_clicks,
        sum(spent) as total_spent,
        sum(total_conversion) as total_conversions,
        sum(approved_conversion) as approved_conversions,
        
        -- Calculated performance metrics
        safe_divide(sum(clicks), sum(impressions)) as overall_ctr,
        safe_divide(sum(spent), nullif(sum(clicks), 0)) as overall_cpc,
        safe_divide(sum(spent), nullif(sum(impressions), 0)) * 1000 as overall_cpm,
        safe_divide(sum(approved_conversion), nullif(sum(clicks), 0)) as overall_conversion_rate,
        safe_divide(sum(approved_conversion), nullif(sum(total_conversion), 0)) as overall_approval_rate,
        
        -- Date range
        min(reporting_start_date) as campaign_start_date,
        max(reporting_end_date) as campaign_end_date,
        date_diff(max(reporting_end_date), min(reporting_start_date), day) + 1 as campaign_duration_days

    from {{ ref('stg_fb_ads_data') }}
    where campaign_id is not null and fb_campaign_id is not null  -- Filter out null campaign_ids
    group by campaign_id, fb_campaign_id
),

percentiles as (
    select
        percentile_cont(overall_ctr, 0.25) over() as percentile_25_ctr,
        percentile_cont(overall_ctr, 0.75) over() as percentile_75_ctr
    from campaign_metrics
    limit 1
)

select
    cm.*,
    
    -- Performance indicators using percentiles
    case
        when cm.overall_ctr >= p.percentile_75_ctr then 'High CTR'
        when cm.overall_ctr >= p.percentile_25_ctr then 'Medium CTR'
        else 'Low CTR'
    end as ctr_performance,
    
    case 
        when cm.overall_conversion_rate > 0.1 then 'High Conversion'
        when cm.overall_conversion_rate > 0.05 then 'Medium Conversion'
        else 'Low Conversion'
    end as conversion_performance

from campaign_metrics cm
cross join percentiles p
