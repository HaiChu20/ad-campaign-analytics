with campaign_metrics as (
    select
        concat(coalesce(campaign_id, 'null'), '_', coalesce(fb_campaign_id, 'null')) as campaign_id,  -- Create composite key
        fb_campaign_id,
        
        {{ all_performance_metrics() }},
        
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
        percentile_cont(avg_ctr, 0.25) over() as percentile_25_ctr,
        percentile_cont(avg_ctr, 0.75) over() as percentile_75_ctr
    from campaign_metrics
    limit 1
)

select
    cm.*,
    
    -- Performance indicators using percentiles
    case
        when cm.avg_ctr >= p.percentile_75_ctr then 'High CTR'
        when cm.avg_ctr >= p.percentile_25_ctr then 'Medium CTR'
        else 'Low CTR'
    end as ctr_performance,
    
    {{ conversion_performance_category('cm.avg_conversion_rate') }} as conversion_performance

from campaign_metrics cm
cross join percentiles p
