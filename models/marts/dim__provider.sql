select provider.*, hspf.provider_lead_status from {{ ref('int__provider') }} provider
left join {{ ref('int__hubspot_provider_funnel') }} hspf on provider.coral_provider_id = hspf.coral_provider_id