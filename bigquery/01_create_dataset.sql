-- Create the main dataset for ad campaign analytics
CREATE SCHEMA IF NOT EXISTS `ad_campaign_raw`
OPTIONS (
  description = "Raw data from ad campaign APIs",
  location = "EU"
);

-- Create campaigns table (simple structure)
CREATE TABLE IF NOT EXISTS `ad_campaign_raw.campaigns` (
  ad_id INT64,
  reporting_start STRING,
  reporting_end STRING,
  campaign_id STRING,
  fb_campaign_id STRING,
  age STRING,
  gender STRING,
  interest1 INT64,
  interest2 INT64,
  interest3 INT64,
  impressions FLOAT64,
  clicks INT64,
  spent FLOAT64,
  total_conversion FLOAT64,
  approved_conversion FLOAT64
)
