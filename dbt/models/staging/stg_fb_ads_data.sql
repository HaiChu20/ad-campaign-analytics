with raw as (
    select *
    from {{ source('raw', 'raw_advertisement') }}
),

base as (
    select
        ad_id,
        reporting_start,
        reporting_end,
        {{ fix_shifted_rows() }}
    from raw
),

enriched as (
    select
        *,
        -- Calculate derived metrics
        safe_divide(clicks, impressions) as ctr,  -- click-through rate
        safe_divide(spent, nullif(clicks, 0)) as cpc,  -- cost per click
        safe_divide(spent, nullif(impressions, 0)) * 1000 as cpm,  -- cost per 1000 impressions
        safe_divide(approved_conversion, nullif(clicks, 0)) as conversion_rate,
        safe_divide(approved_conversion, nullif(total_conversion, 0)) as approval_rate,
        
        -- Use dates directly (already in DATE format)
        reporting_start as reporting_start_date,
        reporting_end as reporting_end_date,
        
        -- Create interest combination
        concat(cast(interest1 as string), '-', cast(interest2 as string), '-', cast(interest3 as string)) as interest_combination,
        
        -- Calculate campaign duration
        date_diff(reporting_end, reporting_start, day) + 1 as campaign_duration_days
        
    from base
)

select * from enriched
