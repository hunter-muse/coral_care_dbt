SELECT
    CONCAT(event_id, '-', device_mobile_brand_name) as device_sk,
    event_id,
    device_category,
    device_mobile_brand_name,
    device_mobile_model_name,
    device_mobile_marketing_name,
    device_operating_system,
    device_operating_system_version,
    device_language,
    device_is_limited_ad_tracking,
    device_web_info_browser,
    device_web_info_browser_version,
    device_web_info_hostname
FROM {{ ref("stg__google_analytics__event") }}