-- ==============================================
-- PERFORMANCE METRICS MACROS
-- ==============================================

-- Macro for basic performance metrics
{% macro performance_metrics() %}
    -- Performance metrics
    safe_divide(sum(clicks), sum(impressions)) as avg_ctr,
    safe_divide(sum(spent), nullif(sum(clicks), 0)) as avg_cpc,
    safe_divide(sum(spent), nullif(sum(impressions), 0)) * 1000 as avg_cpm,
    safe_divide(sum(approved_conversion), nullif(sum(clicks), 0)) as avg_conversion_rate,
    safe_divide(sum(approved_conversion), nullif(sum(total_conversion), 0)) as avg_approval_rate
{% endmacro %}

-- Macro for efficiency metrics
{% macro efficiency_metrics() %}
    -- Efficiency metrics
    safe_divide(sum(approved_conversion), nullif(sum(spent), 0)) as conversions_per_dollar,
    safe_divide(sum(spent), nullif(sum(approved_conversion), 0)) as cost_per_conversion
{% endmacro %}

-- Macro for aggregated metrics
{% macro aggregated_metrics() %}
    -- Aggregated metrics
    count(distinct ad_id) as total_ads,
    count(distinct campaign_id) as total_campaigns,
    sum(impressions) as total_impressions,
    sum(clicks) as total_clicks,
    sum(spent) as total_spent,
    sum(total_conversion) as total_conversions,
    sum(approved_conversion) as approved_conversions
{% endmacro %}

-- Macro for all performance metrics combined
{% macro all_performance_metrics() %}
    {{ aggregated_metrics() }},
    {{ performance_metrics() }},
    {{ efficiency_metrics() }}
{% endmacro %}

-- Macro for CTR performance categorization
{% macro ctr_performance_category(ctr_column) %}
    case 
        when {{ ctr_column }} > 0.02 then 'High CTR Day'
        when {{ ctr_column }} > 0.01 then 'Medium CTR Day'
        else 'Low CTR Day'
    end
{% endmacro %}

-- Macro for conversion performance categorization
{% macro conversion_performance_category(conversion_column) %}
    case 
        when {{ conversion_column }} > 0.1 then 'High Conversion Day'
        when {{ conversion_column }} > 0.05 then 'Medium Conversion Day'
        else 'Low Conversion Day'
    end
{% endmacro %}

-- Macro for efficiency performance categorization
{% macro efficiency_performance_category(efficiency_column) %}
    case 
        when {{ efficiency_column }} > 0.1 then 'High Efficiency Day'
        when {{ efficiency_column }} > 0.05 then 'Medium Efficiency Day'
        else 'Low Efficiency Day'
    end
{% endmacro %}

-- Macro for performance rankings
{% macro performance_rankings() %}
    -- Performance ranking
    row_number() over (order by safe_divide(sum(clicks), sum(impressions)) desc) as ctr_rank,
    row_number() over (order by safe_divide(sum(approved_conversion), nullif(sum(clicks), 0)) desc) as conversion_rank,
    row_number() over (order by safe_divide(sum(approved_conversion), nullif(sum(spent), 0)) desc) as efficiency_rank
{% endmacro %}

-- Macro for day name extraction
{% macro day_name_macro(day_of_week_column) %}
    case {{ day_of_week_column }}
        when 1 then 'Sunday'
        when 2 then 'Monday'
        when 3 then 'Tuesday'
        when 4 then 'Wednesday'
        when 5 then 'Thursday'
        when 6 then 'Friday'
        when 7 then 'Saturday'
    end
{% endmacro %}

-- Macro for date extractions
{% macro date_extractions(date_column) %}
    extract(day from {{ date_column }}) as day_of_month,
    extract(month from {{ date_column }}) as month,
    extract(year from {{ date_column }}) as year,
    extract(dayofweek from {{ date_column }}) as day_of_week
{% endmacro %}
