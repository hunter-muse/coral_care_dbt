with recruiting_funnel as (
    select * from {{ ref('int__hubspot_provider_funnel') }}
),

sales_funnel as (
    select * from {{ ref('int__hubspot_provider_sales_funnel') }}
),

combined as (
    select
        COALESCE(r.hubspot_provider_id, s.hubspot_provider_id) as hubspot_provider_id,
        COALESCE(r.coral_provider_id, s.coral_provider_id) as coral_provider_id,
        COALESCE(r.provider_state, s.provider_state) as provider_state,
        COALESCE(r.provider_hubspot_created_date, s.provider_hubspot_created_date) as provider_hubspot_created_date,
        
        -- Recruiting funnel data
        r.current_stage as recruiting_current_stage,
        r.provider_lead_status as recruiting_lead_status,
        r.hours_in_current_stage as recruiting_hours_in_current_stage,
        r.total_days_in_funnel as recruiting_total_days_in_funnel,
        r.longest_stage_duration as recruiting_longest_stage_duration,
        r.progression_path as recruiting_progression_path,
        
        -- Sales funnel data
        s.current_stage as sales_current_stage,
        s.provider_sales_status as sales_lead_status,
        s.hours_in_current_stage as sales_hours_in_current_stage,
        s.total_days_in_funnel as sales_total_days_in_funnel,
        s.longest_stage_duration as sales_longest_stage_duration,
        s.progression_path as sales_progression_path,
        
        -- Determine which funnel the provider is in
        CASE
            WHEN r.current_stage IS NOT NULL AND s.current_stage IS NOT NULL THEN 'Both Funnels'
            WHEN r.current_stage IS NOT NULL THEN 'Recruiting Funnel'
            WHEN s.current_stage IS NOT NULL THEN 'Sales Funnel'
            ELSE 'No Funnel'
        END as active_funnel,
        
        -- Determine the overall status
        CASE
            -- Provider completed onboarding
            WHEN r.current_stage = 'Onboarding Complete (Provider Onboarding)' THEN 'Onboarded'
            
            -- Provider is in onboarding
            WHEN r.current_stage IN (
                'Pre-Launch (Provider Onboarding)',
                'Ready to Onboard (Provider Onboarding)',
                'Pending Onboarding Tasks (Provider Onboarding)',
                'Schedule Onboarding Call (Provider Onboarding)'
            ) THEN 'In Onboarding'
            
            -- Provider is in a terminal negative state
            WHEN r.current_stage IN (
                'Closed Lost (Provider Recruiting)',
                'Disqualified (Provider Recruiting)',
                'Closed Lost (Provider Onboarding)',
                'Disqualified (Provider Onboarding)',
                'Checkr Fail (Provider Onboarding)'
            ) OR s.current_stage = 'Closed Lost (Provider Sales)' THEN 'Lost'
            
            -- Provider is in a hold state
            WHEN r.current_stage = 'Hold (Provider Onboarding)' OR 
                 r.current_stage = 'Cold (Provider Recruiting)' OR
                 r.current_stage = 'Cold (Provider Onboarding)' THEN 'On Hold'
            
            -- Provider is in recruiting or sales
            WHEN r.current_stage IS NOT NULL OR s.current_stage IS NOT NULL THEN 'Active Lead'
            
            ELSE 'Unknown'
        END as overall_status,
        
        -- Determine the primary funnel (which one is further along)
        CASE
            -- If provider is in onboarding, that's the primary funnel
            WHEN r.current_stage IN (
                'Pre-Launch (Provider Onboarding)',
                'Ready to Onboard (Provider Onboarding)',
                'Pending Onboarding Tasks (Provider Onboarding)',
                'Schedule Onboarding Call (Provider Onboarding)',
                'Onboarding Complete (Provider Onboarding)',
                'Hold (Provider Onboarding)',
                'Checkr Fail (Provider Onboarding)',
                'Closed Lost (Provider Onboarding)',
                'Disqualified (Provider Onboarding)'
            ) THEN 'Onboarding'
            
            -- If provider completed recruiting, that's the primary funnel
            WHEN r.current_stage = 'Recruitment Complete (Provider Recruiting)' THEN 'Recruiting'
            
            -- If provider is in sales and not in recruiting, sales is primary
            WHEN s.current_stage IS NOT NULL AND r.current_stage IS NULL THEN 'Sales'
            
            -- If provider is in recruiting and not in sales, recruiting is primary
            WHEN r.current_stage IS NOT NULL AND s.current_stage IS NULL THEN 'Recruiting'
            
            -- If provider is in both, compare stages to determine which is further along
            WHEN r.current_stage IS NOT NULL AND s.current_stage IS NOT NULL THEN
                CASE
                    -- If in offer letter in recruiting, that's further than any sales stage
                    WHEN r.current_stage = 'Offer Letter (Provider Recruiting)' THEN 'Recruiting'
                    
                    -- If in clinical interview in both, prioritize recruiting
                    WHEN r.current_stage = 'Clinical Interview (Provider Recruiting)' AND 
                         s.current_stage = 'Clinical Interview (Provider Sales)' THEN 'Recruiting'
                    
                    -- If in pending tasks in sales, that's further than early recruiting stages
                    WHEN s.current_stage = 'Pending Tasks (Provider Sales)' AND 
                         r.current_stage IN (
                            'Referrals (Provider Recruiting)',
                            'Lead Magnet (Provider Recruiting)',
                            'New Provider Lead (Provider Recruiting)',
                            'Interview Booked (Provider Recruiting)'
                         ) THEN 'Sales'
                    
                    -- Default to recruiting if both are active
                    ELSE 'Recruiting'
                END
            
            ELSE 'None'
        END as primary_funnel
    from recruiting_funnel r
    full outer join sales_funnel s
        on r.hubspot_provider_id = s.hubspot_provider_id
)

select * from combined
order by coral_provider_id, hubspot_provider_id