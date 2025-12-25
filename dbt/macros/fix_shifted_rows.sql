{% macro fix_shifted_rows() %}

    case 
        when regexp_contains(campaign_id, r'^\d{2}-\d{2}$') or regexp_contains(campaign_id, r'^\d{2}\+$')
        then null
        else campaign_id
    end as campaign_id,

    case 
        when regexp_contains(campaign_id, r'^\d{2}-\d{2}$') or regexp_contains(campaign_id, r'^\d{2}\+$')
        then null
        else fb_campaign_id
    end as fb_campaign_id,

    case 
        when regexp_contains(campaign_id, r'^\d{2}-\d{2}$') or regexp_contains(campaign_id, r'^\d{2}\+$')
        then campaign_id
        else age
    end as age,

    case 
        when fb_campaign_id in ('M','F')
        then fb_campaign_id
        else gender
    end as gender,

    case when fb_campaign_id in ('M','F') then cast(age as string) else cast(interest1 as string) end as interest1,
    case when fb_campaign_id in ('M','F') then cast(gender as string) else cast(interest2 as string) end as interest2,
    case when fb_campaign_id in ('M','F') then cast(interest1 as string) else cast(interest3 as string) end as interest3,

    case when fb_campaign_id in ('M','F') then cast(interest2 as int64) else cast(impressions as int64) end as impressions,
    case when fb_campaign_id in ('M','F') then cast(interest3 as int64) else cast(clicks as int64) end as clicks,
    case when fb_campaign_id in ('M','F') then cast(impressions as float64) else cast(spent as float64) end as spent,
    case when fb_campaign_id in ('M','F') then cast(clicks as int64) else cast(total_conversion as int64) end as total_conversion,
    case when fb_campaign_id in ('M','F') then cast(spent as int64) else cast(approved_conversion as int64) end as approved_conversion

{% endmacro %}
