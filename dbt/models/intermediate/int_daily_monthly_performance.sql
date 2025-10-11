with daily_metrics as (
    select
        {{ date_extractions('reporting_start_date') }},
        
        {{ all_performance_metrics() }}

    from {{ ref('stg_fb_ads_data') }}
    group by 
        extract(day from reporting_start_date),
        extract(month from reporting_start_date),
        extract(year from reporting_start_date),
        extract(dayofweek from reporting_start_date)
)

select
    *,
    
    -- Performance ranking by day of month
    row_number() over (order by avg_ctr desc) as ctr_rank,
    row_number() over (order by avg_conversion_rate desc) as conversion_rank,
    row_number() over (order by conversions_per_dollar desc) as efficiency_rank,
    
    -- Day effectiveness indicators
    {{ ctr_performance_category('avg_ctr') }} as ctr_performance,
    {{ conversion_performance_category('avg_conversion_rate') }} as conversion_performance,
    
    -- Day name
    {{ day_name_macro('day_of_week') }} as day_name

from daily_metrics
order by 
    year,
    month,
    day_of_month
