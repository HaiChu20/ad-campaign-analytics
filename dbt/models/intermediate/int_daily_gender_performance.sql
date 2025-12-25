with daily_gender_metrics as (
    select
        {{ date_extractions('reporting_start_date') }},
        gender,
        
        {{ all_performance_metrics() }}

    from {{ ref('stg_fb_ads_data') }}
    where gender is not null  -- Filter out null genders
    group by 
        extract(day from reporting_start_date),
        extract(month from reporting_start_date),
        extract(year from reporting_start_date),
        extract(dayofweek from reporting_start_date),
        gender
),

-- Overall daily performance (across all genders)
daily_overall as (
    select
        day_of_month,
        month,
        year,
        day_of_week,
        
        -- Overall metrics
        sum(total_ads) as total_ads_overall,
        sum(total_impressions) as total_impressions_overall,
        sum(total_clicks) as total_clicks_overall,
        sum(total_spent) as total_spent_overall,
        sum(approved_conversions) as approved_conversions_overall,
        
        -- Overall performance metrics
        safe_divide(sum(total_clicks), sum(total_impressions)) as overall_ctr,
        safe_divide(sum(approved_conversions), nullif(sum(total_clicks), 0)) as overall_conversion_rate,
        safe_divide(sum(approved_conversions), nullif(sum(total_spent), 0)) as overall_conversions_per_dollar,
        
        -- Day name
        {{ day_name_macro('day_of_week') }} as day_name
        
    from daily_gender_metrics
    group by day_of_month, month, year, day_of_week
),

-- Rankings for overall daily performance
daily_rankings as (
    select
        *,
        row_number() over (order by overall_ctr desc) as ctr_rank,
        row_number() over (order by overall_conversion_rate desc) as conversion_rank,
        row_number() over (order by overall_conversions_per_dollar desc) as efficiency_rank
    from daily_overall
)

select
    dgm.*,
    dr.day_name,
    dr.overall_ctr,
    dr.overall_conversion_rate,
    dr.overall_conversions_per_dollar,
    dr.ctr_rank,
    dr.conversion_rank,
    dr.efficiency_rank,
    
    -- Gender-specific rankings within each day
    row_number() over (partition by dgm.day_of_month, dgm.month, dgm.year order by dgm.avg_ctr desc) as gender_ctr_rank_in_day,
    row_number() over (partition by dgm.day_of_month, dgm.month, dgm.year order by dgm.avg_conversion_rate desc) as gender_conversion_rank_in_day,
    row_number() over (partition by dgm.day_of_month, dgm.month, dgm.year order by dgm.conversions_per_dollar desc) as gender_efficiency_rank_in_day,
    
    -- Performance categories
    {{ ctr_performance_category('dgm.avg_ctr') }} as ctr_performance,
    {{ conversion_performance_category('dgm.avg_conversion_rate') }} as conversion_performance,
    {{ efficiency_performance_category('dgm.conversions_per_dollar') }} as efficiency_performance,
    
    -- Gender performance vs overall day performance
    case 
        when dgm.avg_ctr > dr.overall_ctr then 'Above Average CTR'
        when dgm.avg_ctr < dr.overall_ctr then 'Below Average CTR'
        else 'Average CTR'
    end as gender_vs_overall_ctr,
    
    case 
        when dgm.avg_conversion_rate > dr.overall_conversion_rate then 'Above Average Conversion'
        when dgm.avg_conversion_rate < dr.overall_conversion_rate then 'Below Average Conversion'
        else 'Average Conversion'
    end as gender_vs_overall_conversion,
    
    case 
        when dgm.conversions_per_dollar > dr.overall_conversions_per_dollar then 'Above Average Efficiency'
        when dgm.conversions_per_dollar < dr.overall_conversions_per_dollar then 'Below Average Efficiency'
        else 'Average Efficiency'
    end as gender_vs_overall_efficiency

from daily_gender_metrics dgm
join daily_rankings dr on dgm.day_of_month = dr.day_of_month 
    and dgm.month = dr.month 
    and dgm.year = dr.year
order by 
    dgm.year,
    dgm.month,
    dgm.day_of_month,
    dgm.gender
