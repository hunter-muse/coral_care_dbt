select school.*, funnel.deal_name from {{ ref('int__school') }} school 
left join {{ ref('int__hubspot_school_funnel')}} funnel 
ON school.coral_school_id = funnel.coral_school_id