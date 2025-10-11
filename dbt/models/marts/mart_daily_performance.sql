-- ==============================================
-- MART: DAILY PERFORMANCE ANALYSIS
-- ==============================================
-- This mart combines daily gender performance and monthly performance
-- to provide comprehensive time-based campaign insights

with daily_gender_performance as (
    select * from {{ ref('int_daily_gender_performance') }}
),

daily_monthly_performance as (
    select * from {{ ref('int_daily_monthly_performance') }}
),

-- Combine daily performance across all dimensions
unified_daily_performance as (
    select
        -- Date dimensions
        day_of_month,
        month,
        year,
        day_of_week,
        day_name,
        
        -- Gender performance (when available)
        gender,
        
        -- Core metrics
        total_ads,
        total_campaigns,
        total_impressions,
        total_clicks,
        total_spent,
        total_conversions,
        approved_conversions,
        
        -- Performance metrics
        avg_ctr,
        avg_cpc,
        avg_cpm,
        avg_conversion_rate,
        avg_approval_rate,
        
        -- Efficiency metrics
        conversions_per_dollar,
        cost_per_conversion,
        
        -- Performance categories
        ctr_performance,
        conversion_performance,
        efficiency_performance,
        
        -- Rankings
        ctr_rank,
        conversion_rank,
        efficiency_rank,
        
        -- Gender-specific rankings (when gender is present)
        gender_ctr_rank_in_day,
        gender_conversion_rank_in_day,
        gender_efficiency_rank_in_day,
        
        -- Performance comparisons
        gender_vs_overall_ctr,
        gender_vs_overall_conversion,
        gender_vs_overall_efficiency,
        
        -- Data source indicator
        case 
            when gender is not null then 'gender_specific'
            else 'overall_daily'
        end as data_granularity

    from daily_gender_performance
    
    union all
    
    select
        -- Date dimensions
        day_of_month,
        month,
        year,
        day_of_week,
        day_name,
        
        -- Gender performance (null for overall daily)
        null as gender,
        
        -- Core metrics
        total_ads,
        total_campaigns,
        total_impressions,
        total_clicks,
        total_spent,
        total_conversions,
        approved_conversions,
        
        -- Performance metrics
        avg_ctr,
        avg_cpc,
        avg_cpm,
        avg_conversion_rate,
        avg_approval_rate,
        
        -- Efficiency metrics
        conversions_per_dollar,
        cost_per_conversion,
        
        -- Performance categories
        ctr_performance,
        conversion_performance,
        null as efficiency_performance,  -- Not available in monthly performance
        
        -- Rankings
        ctr_rank,
        conversion_rank,
        efficiency_rank,
        
        -- Gender-specific rankings (null for overall)
        null as gender_ctr_rank_in_day,
        null as gender_conversion_rank_in_day,
        null as gender_efficiency_rank_in_day,
        
        -- Performance comparisons (null for overall)
        null as gender_vs_overall_ctr,
        null as gender_vs_overall_conversion,
        null as gender_vs_overall_efficiency,
        
        -- Data source indicator
        'overall_daily' as data_granularity

    from daily_monthly_performance
),

-- Add monthly aggregations and trends
monthly_aggregations as (
    select
        month,
        year,
        
        -- Monthly totals
        sum(total_ads) as monthly_total_ads,
        sum(total_campaigns) as monthly_total_campaigns,
        sum(total_impressions) as monthly_total_impressions,
        sum(total_clicks) as monthly_total_clicks,
        sum(total_spent) as monthly_total_spent,
        sum(total_conversions) as monthly_total_conversions,
        sum(approved_conversions) as monthly_approved_conversions,
        
        -- Monthly averages
        avg(avg_ctr) as monthly_avg_ctr,
        avg(avg_cpc) as monthly_avg_cpc,
        avg(avg_cpm) as monthly_avg_cpm,
        avg(avg_conversion_rate) as monthly_avg_conversion_rate,
        avg(avg_approval_rate) as monthly_avg_approval_rate,
        avg(conversions_per_dollar) as monthly_avg_conversions_per_dollar,
        avg(cost_per_conversion) as monthly_avg_cost_per_conversion,
        
        -- Monthly performance distribution
        count(distinct case when ctr_performance = 'High CTR Day' then day_of_month end) as high_ctr_days,
        count(distinct case when conversion_performance = 'High Conversion Day' then day_of_month end) as high_conversion_days,
        count(distinct case when efficiency_performance = 'High Efficiency Day' then day_of_month end) as high_efficiency_days,
        
        count(distinct day_of_month) as total_days_in_month

    from unified_daily_performance
    where data_granularity = 'overall_daily'
    group by month, year
)

select
    udp.*,
    
    -- Monthly context
    ma.monthly_total_ads,
    ma.monthly_total_campaigns,
    ma.monthly_total_impressions,
    ma.monthly_total_clicks,
    ma.monthly_total_spent,
    ma.monthly_total_conversions,
    ma.monthly_approved_conversions,
    
    -- Monthly performance benchmarks
    ma.monthly_avg_ctr,
    ma.monthly_avg_cpc,
    ma.monthly_avg_cpm,
    ma.monthly_avg_conversion_rate,
    ma.monthly_avg_approval_rate,
    ma.monthly_avg_conversions_per_dollar,
    ma.monthly_avg_cost_per_conversion,
    
    -- Monthly performance distribution
    ma.high_ctr_days,
    ma.high_conversion_days,
    ma.high_efficiency_days,
    ma.total_days_in_month,
    
    -- Performance vs monthly average
    case 
        when udp.avg_ctr > ma.monthly_avg_ctr then 'Above Monthly Average'
        when udp.avg_ctr < ma.monthly_avg_ctr then 'Below Monthly Average'
        else 'At Monthly Average'
    end as ctr_vs_monthly_avg,
    
    case 
        when udp.avg_conversion_rate > ma.monthly_avg_conversion_rate then 'Above Monthly Average'
        when udp.avg_conversion_rate < ma.monthly_avg_conversion_rate then 'Below Monthly Average'
        else 'At Monthly Average'
    end as conversion_vs_monthly_avg,
    
    case 
        when udp.conversions_per_dollar > ma.monthly_avg_conversions_per_dollar then 'Above Monthly Average'
        when udp.conversions_per_dollar < ma.monthly_avg_conversions_per_dollar then 'Below Monthly Average'
        else 'At Monthly Average'
    end as efficiency_vs_monthly_avg,
    
    -- Day of week performance insights
    case 
        when udp.day_of_week in (1, 7) then 'Weekend'
        else 'Weekday'
    end as day_type,
    
    -- Performance tier based on multiple metrics
    case 
        when udp.ctr_performance = 'High CTR Day' 
             and udp.conversion_performance = 'High Conversion Day'
             and udp.efficiency_performance = 'High Efficiency Day' then 'Tier 1 - Excellent'
        when udp.ctr_performance in ('High CTR Day', 'Medium CTR Day')
             and udp.conversion_performance in ('High Conversion Day', 'Medium Conversion Day')
             and udp.efficiency_performance in ('High Efficiency Day', 'Medium Efficiency Day') then 'Tier 2 - Good'
        when udp.ctr_performance = 'Low CTR Day'
             and udp.conversion_performance = 'Low Conversion Day'
             and udp.efficiency_performance = 'Low Efficiency Day' then 'Tier 4 - Poor'
        else 'Tier 3 - Average'
    end as overall_performance_tier,
    
    -- Campaign optimization recommendations
    case 
        when udp.ctr_performance = 'High CTR Day' and udp.conversion_performance = 'High Conversion Day' then 'Increase Budget'
        when udp.ctr_performance = 'Low CTR Day' and udp.conversion_performance = 'Low Conversion Day' then 'Reduce Budget'
        when udp.ctr_performance = 'High CTR Day' and udp.conversion_performance = 'Low Conversion Day' then 'Optimize Landing Page'
        when udp.ctr_performance = 'Low CTR Day' and udp.conversion_performance = 'High Conversion Day' then 'Improve Ad Creative'
        else 'Monitor Performance'
    end as optimization_recommendation

from unified_daily_performance udp
left join monthly_aggregations ma 
    on udp.month = ma.month 
    and udp.year = ma.year
