with source as (
    select 
        deal.deal_id,
        deal.deaL_created_date,
        deal.deal_sequence,
        -- Determine pipeline based on deal sequence
        -- CASE 
        --     WHEN deal.deal_sequence = 1 THEN 'Provider Recruiting'
        --     WHEN deal.deal_sequence = 2 THEN 'Provider Onboarding'
        --     ELSE 'Unknown Pipeline'
        -- END as pipeline_name,
        provider.provider_hubspot_created_date,
        provider_enriched.coral_provider_id,
        provider_enriched.state as provider_state, 
        provider_enriched.hubspot_provider_id,
        -- Provider Recruiting Stages
        deal.date_entered_referrals_provider_recruiting,
        deal.date_exited_referrals_provider_recruiting,
        -- deal.date_entered_lead_magnet_provider_recruiting,
        -- deal.date_exited_lead_magnet_provider_recruiting,
        deal.date_entered_new_provider_lead_provider_recruiting,
        deal.date_exited_new_provider_lead_provider_recruiting,
        deal.date_entered_interview_booked_provider_recruiting,
        deal.date_exited_interview_booked_provider_recruiting,
        deal.date_entered_post_interview_provider_recruiting,
        deal.date_exited_post_interview_provider_recruiting,
        deal.date_entered_clinical_interview_provider_recruiting,
        deal.date_exited_clinical_interview_provider_recruiting,
        deal.date_entered_offer_letter_provider_recruiting,
        deal.date_exited_offer_letter_provider_recruiting,
        deal.date_entered_pending_tasks_provider_recruiting, 
        deal.date_exited_pending_tasks_provider_recruiting, 
        -- deal.date_entered_onboarding_call_provider_recruiting, 
        -- deal.date_exited_onboarding_call_provider_recruiting, 
        deal.date_entered_pre_launch_provider_recruiting, 
        deal.date_exited_pre_launch_provider_recruiting,
        deal.date_entered_recruitment_complete_provider_recruiting,
        deal.date_exited_recruitment_complete_provider_recruiting,
        deal.date_entered_cold_provider_recruiting,
        deal.date_exited_cold_provider_recruiting,
        deal.date_entered_closed_lost_provider_recruiting,
        deal.date_exited_closed_lost_provider_recruiting,
        deal.date_entered_disqualified_provider_recruiting,
        deal.date_entered_re_activate_provider_recruiting, 
        deal.date_exited_re_activate_provider_recruiting, 
        deal.date_entered_interview_cx_provider_recruiting, 
        deal.date_exited_interview_cx_provider_recruiting, 
        deal.date_entered_pending_referral_specialties_provider_recruiting, 
        deal.date_exited_pending_referral_specialties_provider_recruiting, 
        deal.date_entered_ready_for_kickoff_provider_recruiting, 
        deal.date_exited_ready_for_kickoff_provider_recruiting, 
        deal.date_entered_hold_provider_recruiting, 
        deal.date_exited_hold_provider_recruiting, 
        -- Provider Onboarding Stages
        deal.date_entered_ready_to_onboard_provider_onboarding,
        deal.date_exited_ready_to_onboard_provider_onboarding,
        deal.date_entered_pending_onboarding_tasks_provider_onboarding,
        deal.date_exited_pending_onboarding_tasks_provider_onboarding,
        deal.date_entered_schedule_onboarding_call_provider_onboarding,
        deal.date_exited_schedule_onboarding_call_provider_onboarding,
        deal.date_entered_onboarding_complete_provider_onboarding,
        deal.date_exited_onboarding_complete_provider_onboarding,
        deal.date_entered_checkr_fail_provider_onboarding,
        deal.date_exited_checkr_fail_provider_onboarding,
        deal.date_entered_hold_provider_onboarding,
        deal.date_exited_hold_provider_onboarding,
        deal.date_entered_cold_provider_onboarding,
        deal.date_exited_cold_provider_onboarding,
        deal.date_entered_disqualified_provider_onboarding,
        deal.date_entered_closed_lost_provider_onboarding,
        deal.closed_lost_provider_reason,
        deal.date_entered_pre_launch_provider_onboarding,
        deal.date_exited_pre_launch_provider_onboarding
    from {{ ref('stg__hubspot__deal') }} deal
    INNER JOIN {{ ref('stg__hubspot__contact_provider')}} provider
        ON deal.contact_id = provider.hubspot_provider_id  
    LEFT JOIN {{ ref('int__provider')}} provider_enriched
        ON provider.hubspot_provider_id = provider_enriched.hubspot_provider_id
),

-- Group deals by provider to combine recruitment and onboarding data
provider_deals as (
    select
        hubspot_provider_id,
        coral_provider_id,
        provider_state,
        provider_hubspot_created_date,
        -- Recruitment pipeline fields - take from recruitment pipeline deals
        MAX(date_entered_referrals_provider_recruiting) as date_entered_referrals_provider_recruiting,
        MAX(date_exited_referrals_provider_recruiting) as date_exited_referrals_provider_recruiting,
        -- MAX(date_entered_lead_magnet_provider_recruiting) as date_entered_lead_magnet_provider_recruiting,
        -- MAX(date_exited_lead_magnet_provider_recruiting) as date_exited_lead_magnet_provider_recruiting,
        MAX(date_entered_new_provider_lead_provider_recruiting) as date_entered_new_provider_lead_provider_recruiting,
        MAX(date_exited_new_provider_lead_provider_recruiting) as date_exited_new_provider_lead_provider_recruiting,
        MAX(date_entered_interview_booked_provider_recruiting) as date_entered_interview_booked_provider_recruiting,
        MAX(date_exited_interview_booked_provider_recruiting) as date_exited_interview_booked_provider_recruiting,
        MAX(date_entered_post_interview_provider_recruiting) as date_entered_post_interview_provider_recruiting,
        MAX(date_exited_post_interview_provider_recruiting) as date_exited_post_interview_provider_recruiting,
        MAX(date_entered_clinical_interview_provider_recruiting) as date_entered_clinical_interview_provider_recruiting,
        MAX(date_exited_clinical_interview_provider_recruiting) as date_exited_clinical_interview_provider_recruiting,
        MAX(date_entered_offer_letter_provider_recruiting) as date_entered_offer_letter_provider_recruiting,
        MAX(date_exited_offer_letter_provider_recruiting) as date_exited_offer_letter_provider_recruiting,
        -- New stages added after offer letter
        MAX(date_entered_pending_tasks_provider_recruiting) as date_entered_pending_tasks_provider_recruiting,
        MAX(date_exited_pending_tasks_provider_recruiting) as date_exited_pending_tasks_provider_recruiting,
        -- MAX(date_entered_onboarding_call_provider_recruiting) as date_entered_onboarding_call_provider_recruiting,
        -- MAX(date_exited_onboarding_call_provider_recruiting) as date_exited_onboarding_call_provider_recruiting,
        MAX(date_entered_pre_launch_provider_recruiting) as date_entered_pre_launch_provider_recruiting,
        MAX(date_exited_pre_launch_provider_recruiting) as date_exited_pre_launch_provider_recruiting,
        MAX(date_entered_recruitment_complete_provider_recruiting) as date_entered_recruitment_complete_provider_recruiting,
        MAX(date_exited_recruitment_complete_provider_recruiting) as date_exited_recruitment_complete_provider_recruiting,
        MAX(date_entered_cold_provider_recruiting) as date_entered_cold_provider_recruiting,
        MAX(date_exited_cold_provider_recruiting) as date_exited_cold_provider_recruiting,
        MAX(date_entered_closed_lost_provider_recruiting) as date_entered_closed_lost_provider_recruiting,
        MAX(date_exited_closed_lost_provider_recruiting) as date_exited_closed_lost_provider_recruiting,
        MAX(date_entered_disqualified_provider_recruiting) as date_entered_disqualified_provider_recruiting,
        MAX(date_entered_re_activate_provider_recruiting) as date_entered_re_activate_provider_recruiting,
        MAX(date_exited_re_activate_provider_recruiting) as date_exited_re_activate_provider_recruiting,
        MAX(date_entered_interview_cx_provider_recruiting) as date_entered_interview_cx_provider_recruiting,
        MAX(date_exited_interview_cx_provider_recruiting) as date_exited_interview_cx_provider_recruiting,
        MAX(date_entered_pending_referral_specialties_provider_recruiting) as date_entered_pending_referral_specialties_provider_recruiting,
        MAX(date_exited_pending_referral_specialties_provider_recruiting) as date_exited_pending_referral_specialties_provider_recruiting,
        MAX(date_entered_ready_for_kickoff_provider_recruiting) as date_entered_ready_for_kickoff_provider_recruiting,
        MAX(date_exited_ready_for_kickoff_provider_recruiting) as date_exited_ready_for_kickoff_provider_recruiting,
        MAX(date_entered_hold_provider_recruiting) as date_entered_hold_provider_recruiting,
        MAX(date_exited_hold_provider_recruiting) as date_exited_hold_provider_recruiting,
        -- Onboarding pipeline fields - take from onboarding pipeline deals
        MAX(date_entered_ready_to_onboard_provider_onboarding) as date_entered_ready_to_onboard_provider_onboarding,
        MAX(date_exited_ready_to_onboard_provider_onboarding) as date_exited_ready_to_onboard_provider_onboarding,
        MAX(date_entered_pending_onboarding_tasks_provider_onboarding) as date_entered_pending_onboarding_tasks_provider_onboarding,
        MAX(date_exited_pending_onboarding_tasks_provider_onboarding) as date_exited_pending_onboarding_tasks_provider_onboarding,
        MAX(date_entered_schedule_onboarding_call_provider_onboarding) as date_entered_schedule_onboarding_call_provider_onboarding,
        MAX(date_exited_schedule_onboarding_call_provider_onboarding) as date_exited_schedule_onboarding_call_provider_onboarding,
        MAX(date_entered_onboarding_complete_provider_onboarding) as date_entered_onboarding_complete_provider_onboarding,
        MAX(date_exited_onboarding_complete_provider_onboarding) as date_exited_onboarding_complete_provider_onboarding,
        MAX(date_entered_checkr_fail_provider_onboarding) as date_entered_checkr_fail_provider_onboarding,
        MAX(date_exited_checkr_fail_provider_onboarding) as date_exited_checkr_fail_provider_onboarding,
        MAX(date_entered_hold_provider_onboarding) as date_entered_hold_provider_onboarding,
        MAX(date_exited_hold_provider_onboarding) as date_exited_hold_provider_onboarding,
        MAX(date_entered_cold_provider_onboarding) as date_entered_cold_provider_onboarding,
        MAX(date_exited_cold_provider_onboarding) as date_exited_cold_provider_onboarding,
        MAX(date_entered_closed_lost_provider_onboarding) as date_entered_closed_lost_provider_onboarding,
        MAX(date_entered_disqualified_provider_onboarding) as date_entered_disqualified_provider_onboarding,
        MAX(closed_lost_provider_reason) as closed_lost_provider_reason,
        
        -- Use MIN of deal_created_date to get the earliest deal
        MIN(deaL_created_date) as first_deal_created_date,
        
        -- Onboarding pipeline fields
        MAX(date_entered_pre_launch_provider_onboarding) as date_entered_pre_launch_provider_onboarding,
        MAX(date_exited_pre_launch_provider_onboarding) as date_exited_pre_launch_provider_onboarding
    from source
    group by hubspot_provider_id, coral_provider_id, provider_hubspot_created_date, provider_state
),

enriched as (
    select 
        hubspot_provider_id, 
        coral_provider_id,
        provider_state,
        provider_hubspot_created_date,
        
        -- All date fields remain the same
        -- date_entered_pre_launch_provider_onboarding,
        -- date_exited_pre_launch_provider_onboarding,
        -- date_entered_ready_to_onboard_provider_onboarding,
        -- date_exited_ready_to_onboard_provider_onboarding,
        -- date_entered_pending_onboarding_tasks_provider_onboarding,
        -- date_exited_pending_onboarding_tasks_provider_onboarding,
        -- date_entered_schedule_onboarding_call_provider_onboarding,
        -- date_exited_schedule_onboarding_call_provider_onboarding,
        -- date_entered_onboarding_complete_provider_onboarding,
        -- date_exited_onboarding_complete_provider_onboarding,
        -- date_entered_checkr_fail_provider_onboarding,
        -- date_exited_checkr_fail_provider_onboarding,
        -- date_entered_hold_provider_onboarding,
        -- date_exited_hold_provider_onboarding,
        -- date_entered_cold_provider_onboarding,
        -- date_exited_cold_provider_onboarding,
        -- date_entered_closed_lost_provider_onboarding,
        -- date_entered_disqualified_provider_onboarding,
        
        date_entered_referrals_provider_recruiting,
        date_exited_referrals_provider_recruiting,
        --date_entered_lead_magnet_provider_recruiting,
        --date_exited_lead_magnet_provider_recruiting,
        date_entered_new_provider_lead_provider_recruiting,
        date_exited_new_provider_lead_provider_recruiting,
        date_entered_interview_booked_provider_recruiting,
        date_exited_interview_booked_provider_recruiting,
        date_entered_post_interview_provider_recruiting,
        date_exited_post_interview_provider_recruiting,
        date_entered_clinical_interview_provider_recruiting,
        date_exited_clinical_interview_provider_recruiting,
        date_entered_offer_letter_provider_recruiting,
        date_exited_offer_letter_provider_recruiting,
        date_entered_pending_tasks_provider_recruiting, 
        date_exited_pending_tasks_provider_recruiting, 
        --date_entered_onboarding_call_provider_recruiting, 
        --date_exited_onboarding_call_provider_recruiting, 
        date_entered_pre_launch_provider_recruiting, 
        date_exited_pre_launch_provider_recruiting,

        date_entered_recruitment_complete_provider_recruiting,
        date_exited_recruitment_complete_provider_recruiting,
        date_entered_cold_provider_recruiting,
        date_exited_cold_provider_recruiting,
        date_entered_closed_lost_provider_recruiting,
        date_entered_disqualified_provider_recruiting,
        date_entered_re_activate_provider_recruiting,
        date_exited_re_activate_provider_recruiting,
        date_entered_interview_cx_provider_recruiting,
        date_exited_interview_cx_provider_recruiting,
        date_entered_pending_referral_specialties_provider_recruiting,
        date_exited_pending_referral_specialties_provider_recruiting,
        date_entered_ready_for_kickoff_provider_recruiting,
        date_exited_ready_for_kickoff_provider_recruiting,
        date_entered_hold_provider_recruiting,
        date_exited_hold_provider_recruiting,
        first_deal_created_date, 
        
        -- Provider Onboarding Stages
        -- Add Pre-Launch Provider Onboarding Stage
        -- case 
        --     when date_entered_pre_launch_provider_onboarding is null and date_exited_pre_launch_provider_onboarding is null then 'never_entered'
        --     when date_entered_pre_launch_provider_onboarding is not null and date_exited_pre_launch_provider_onboarding is null then 'current'
        --     when date_entered_pre_launch_provider_onboarding = date_exited_pre_launch_provider_onboarding then 'skipped'
        --     else 'completed'
        -- end as pre_launch_provider_onboarding_status,
        
        -- -- Ready to Onboard Provider Onboarding Stage
        -- case 
        --     when date_entered_ready_to_onboard_provider_onboarding is null and date_exited_ready_to_onboard_provider_onboarding is null then 'never_entered'
        --     when date_entered_ready_to_onboard_provider_onboarding is not null and date_exited_ready_to_onboard_provider_onboarding is null then 'current'
        --     when date_entered_ready_to_onboard_provider_onboarding = date_exited_ready_to_onboard_provider_onboarding then 'skipped'
        --     else 'completed'
        -- end as ready_to_onboard_provider_onboarding_status,
        
        -- -- Pending Onboarding Tasks Provider Onboarding Stage
        -- case 
        --     when date_entered_pending_onboarding_tasks_provider_onboarding is null and date_exited_pending_onboarding_tasks_provider_onboarding is null then 'never_entered'
        --     when date_entered_pending_onboarding_tasks_provider_onboarding is not null and date_exited_pending_onboarding_tasks_provider_onboarding is null then 'current'
        --     when date_entered_pending_onboarding_tasks_provider_onboarding = date_exited_pending_onboarding_tasks_provider_onboarding then 'skipped'
        --     else 'completed'
        -- end as pending_onboarding_tasks_provider_onboarding_status,
        
        -- -- Schedule Onboarding Call Provider Onboarding Stage
        -- case 
        --     when date_entered_schedule_onboarding_call_provider_onboarding is null and date_exited_schedule_onboarding_call_provider_onboarding is null then 'never_entered'
        --     when date_entered_schedule_onboarding_call_provider_onboarding is not null and date_exited_schedule_onboarding_call_provider_onboarding is null then 'current'
        --     when date_entered_schedule_onboarding_call_provider_onboarding = date_exited_schedule_onboarding_call_provider_onboarding then 'skipped'
        --     else 'completed'
        -- end as schedule_onboarding_call_provider_onboarding_status,
        
        -- Provider Recruiting Stages
        -- Referrals Provider Recruiting Stage
        case 
            when date_entered_referrals_provider_recruiting is null and date_exited_referrals_provider_recruiting is null then 'never_entered'
            when date_entered_referrals_provider_recruiting is not null and date_exited_referrals_provider_recruiting is null then 'current'
            when date_entered_referrals_provider_recruiting = date_exited_referrals_provider_recruiting then 'skipped'
            else 'completed'
        end as referrals_provider_recruiting_status,
        
        -- Lead Magnet Provider Recruiting Stage
        -- case 
        --     when date_entered_lead_magnet_provider_recruiting is null and date_exited_lead_magnet_provider_recruiting is null then 'never_entered'
        --     when date_entered_lead_magnet_provider_recruiting is not null and date_exited_lead_magnet_provider_recruiting is null then 'current'
        --     when date_entered_lead_magnet_provider_recruiting = date_exited_lead_magnet_provider_recruiting then 'skipped'
        --     else 'completed'
        -- end as lead_magnet_provider_recruiting_status,
        
        -- New Provider Lead Provider Recruiting Stage
        case 
            when date_entered_new_provider_lead_provider_recruiting is null and date_exited_new_provider_lead_provider_recruiting is null then 'never_entered'
            when date_entered_new_provider_lead_provider_recruiting is not null and date_exited_new_provider_lead_provider_recruiting is null then 'current'
            when date_entered_new_provider_lead_provider_recruiting = date_exited_new_provider_lead_provider_recruiting then 'skipped'
            else 'completed'
        end as new_provider_lead_provider_recruiting_status,
        
        -- Interview Booked Provider Recruiting Stage
        case 
            when date_entered_interview_booked_provider_recruiting is null and date_exited_interview_booked_provider_recruiting is null then 'never_entered'
            when date_entered_interview_booked_provider_recruiting is not null and date_exited_interview_booked_provider_recruiting is null then 'current'
            when date_entered_interview_booked_provider_recruiting = date_exited_interview_booked_provider_recruiting then 'skipped'
            else 'completed'
        end as interview_booked_provider_recruiting_status,
        
        -- Post Interview Provider Recruiting Stage
        case 
            when date_entered_post_interview_provider_recruiting is null and date_exited_post_interview_provider_recruiting is null then 'never_entered'
            when date_entered_post_interview_provider_recruiting is not null and date_exited_post_interview_provider_recruiting is null then 'current'
            when date_entered_post_interview_provider_recruiting = date_exited_post_interview_provider_recruiting then 'skipped'
            else 'completed'
        end as post_interview_provider_recruiting_status,
        
        -- Clinical Interview Provider Recruiting Stage
        case 
            when date_entered_clinical_interview_provider_recruiting is null and date_exited_clinical_interview_provider_recruiting is null then 'never_entered'
            when date_entered_clinical_interview_provider_recruiting is not null and date_exited_clinical_interview_provider_recruiting is null then 'current'
            when date_entered_clinical_interview_provider_recruiting = date_exited_clinical_interview_provider_recruiting then 'skipped'
            else 'completed'
        end as clinical_interview_provider_recruiting_status,
        
        -- Offer Letter Provider Recruiting Stage
        case 
            when date_entered_offer_letter_provider_recruiting is null and date_exited_offer_letter_provider_recruiting is null then 'never_entered'
            when date_entered_offer_letter_provider_recruiting is not null and date_exited_offer_letter_provider_recruiting is null then 'current'
            when date_entered_offer_letter_provider_recruiting = date_exited_offer_letter_provider_recruiting then 'skipped'
            else 'completed'
        end as offer_letter_provider_recruiting_status,

        -- Pending Tasks Provider Recruiting Stage
        case 
            when date_entered_pending_tasks_provider_recruiting is null and date_exited_pending_tasks_provider_recruiting is null then 'never_entered'
            when date_entered_pending_tasks_provider_recruiting is not null and date_exited_pending_tasks_provider_recruiting is null then 'current'
            when date_entered_pending_tasks_provider_recruiting = date_exited_pending_tasks_provider_recruiting then 'skipped'
            else 'completed'
        end as pending_tasks_provider_recruiting_status,

        -- Onboarding Call Provider Recruiting Stage
        -- case 
        --     when date_entered_onboarding_call_provider_recruiting is null and date_exited_onboarding_call_provider_recruiting is null then 'never_entered'
        --     when date_entered_onboarding_call_provider_recruiting is not null and date_exited_onboarding_call_provider_recruiting is null then 'current'
        --     when date_entered_onboarding_call_provider_recruiting = date_exited_onboarding_call_provider_recruiting then 'skipped'
        --     else 'completed'
        -- end as onboarding_call_provider_recruiting_status,

        --Pre-launch Provider Recruiting Stage
        case 
            when date_entered_pre_launch_provider_recruiting is null and date_exited_pre_launch_provider_recruiting is null then 'never_entered'
            when date_entered_pre_launch_provider_recruiting is not null and date_exited_pre_launch_provider_recruiting is null then 'current'
            when date_entered_pre_launch_provider_recruiting = date_exited_pre_launch_provider_recruiting then 'skipped'
            else 'completed'
        end as pre_launch_provider_recruiting_status,

        -- Recruitment Complete Provider Recruiting Stage
        case 
            when date_entered_recruitment_complete_provider_recruiting is null and date_exited_recruitment_complete_provider_recruiting is null then 'never_entered'
            when date_entered_recruitment_complete_provider_recruiting is not null and date_exited_recruitment_complete_provider_recruiting is null then 'current'
            when date_entered_recruitment_complete_provider_recruiting = date_exited_recruitment_complete_provider_recruiting then 'skipped'
            else 'completed'
        end as recruitment_complete_provider_recruiting_status,
        
        -- Cold Provider Recruiting Stage
        case 
            when date_entered_cold_provider_recruiting is null and date_exited_cold_provider_recruiting is null then 'never_entered'
            when date_entered_cold_provider_recruiting is not null and date_exited_cold_provider_recruiting is null then 'current'
            when date_entered_cold_provider_recruiting = date_exited_cold_provider_recruiting then 'skipped'
            else 'completed'
        end as cold_provider_recruiting_status,
        
        -- Closed Lost Provider Recruiting Stage
        case 
            when date_entered_closed_lost_provider_recruiting is null then 'never_entered'
            when date_entered_closed_lost_provider_recruiting is not null then 'current'
            else 'completed'
        end as closed_lost_provider_recruiting_status,

        --Disqualified Provider Recruiting Stage
        case 
            when date_entered_disqualified_provider_recruiting is null then 'never_entered'
            when date_entered_disqualified_provider_recruiting is not null then 'current'
            else 'completed'
        end as disqualified_provider_recruiting_status,

        -- Re-Activate Provider Recruiting Stage
        case 
            when date_entered_re_activate_provider_recruiting is null and date_exited_re_activate_provider_recruiting is null then 'never_entered'
            when date_entered_re_activate_provider_recruiting is not null and date_exited_re_activate_provider_recruiting is null then 'current'
            when date_entered_re_activate_provider_recruiting = date_exited_re_activate_provider_recruiting then 'skipped'
            else 'completed'
        end as re_activate_provider_recruiting_status,

        -- Interview CX Provider Recruiting Stage
        case 
            when date_entered_interview_cx_provider_recruiting is null and date_exited_interview_cx_provider_recruiting is null then 'never_entered'
            when date_entered_interview_cx_provider_recruiting is not null and date_exited_interview_cx_provider_recruiting is null then 'current'
            when date_entered_interview_cx_provider_recruiting = date_exited_interview_cx_provider_recruiting then 'skipped'
            else 'completed'
        end as interview_cx_provider_recruiting_status,

        -- Pending Referral Specialties Provider Recruiting Stage
        case 
            when date_entered_pending_referral_specialties_provider_recruiting is null and date_exited_pending_referral_specialties_provider_recruiting is null then 'never_entered'
            when date_entered_pending_referral_specialties_provider_recruiting is not null and date_exited_pending_referral_specialties_provider_recruiting is null then 'current'
            when date_entered_pending_referral_specialties_provider_recruiting = date_exited_pending_referral_specialties_provider_recruiting then 'skipped'
            else 'completed'
        end as pending_referral_specialties_provider_recruiting_status,

        -- Ready for Kickoff Provider Recruiting Stage
        case 
            when date_entered_ready_for_kickoff_provider_recruiting is null and date_exited_ready_for_kickoff_provider_recruiting is null then 'never_entered'
            when date_entered_ready_for_kickoff_provider_recruiting is not null and date_exited_ready_for_kickoff_provider_recruiting is null then 'current'
            when date_entered_ready_for_kickoff_provider_recruiting = date_exited_ready_for_kickoff_provider_recruiting then 'skipped'
            else 'completed'
        end as ready_for_kickoff_provider_recruiting_status,

        -- Hold Provider Recruiting Stage
        case 
            when date_entered_hold_provider_recruiting is null and date_exited_hold_provider_recruiting is null then 'never_entered'
            when date_entered_hold_provider_recruiting is not null and date_exited_hold_provider_recruiting is null then 'current'
            when date_entered_hold_provider_recruiting = date_exited_hold_provider_recruiting then 'skipped'
            else 'completed'
        end as hold_provider_recruiting_status,
        
        
        -- Calculate hours in each stage
        -- Pre-Launch Provider Onboarding Stage
        -- ROUND(
        --     CASE
        --         WHEN date_entered_pre_launch_provider_onboarding IS NULL THEN 0
        --         WHEN date_exited_pre_launch_provider_onboarding IS NULL THEN 
        --             CAST(DATEDIFF('second', date_entered_pre_launch_provider_onboarding, CURRENT_TIMESTAMP()) AS FLOAT) * (1/3600)
        --         ELSE 
        --             CAST(DATEDIFF('second', date_entered_pre_launch_provider_onboarding, date_exited_pre_launch_provider_onboarding) AS FLOAT) * (1/3600)
        --     END, 2
        -- ) as hours_in_pre_launch_provider_onboarding,
        
        -- -- Ready to Onboard Provider Onboarding Stage
        -- ROUND(
        --     CASE
        --         WHEN date_entered_ready_to_onboard_provider_onboarding IS NULL THEN 0
        --         WHEN date_exited_ready_to_onboard_provider_onboarding IS NULL THEN 
        --             CAST(DATEDIFF('second', date_entered_ready_to_onboard_provider_onboarding, CURRENT_TIMESTAMP()) AS FLOAT) * (1/3600)
        --         ELSE 
        --             CAST(DATEDIFF('second', date_entered_ready_to_onboard_provider_onboarding, date_exited_ready_to_onboard_provider_onboarding) AS FLOAT) * (1/3600)
        --     END, 2
        -- ) as hours_in_ready_to_onboard_provider_onboarding,
        
        -- -- Pending Onboarding Tasks Provider Onboarding Stage
        -- ROUND(
        --     CASE
        --         WHEN date_entered_pending_onboarding_tasks_provider_onboarding IS NULL THEN 0
        --         WHEN date_exited_pending_onboarding_tasks_provider_onboarding IS NULL THEN 
        --             CAST(DATEDIFF('second', date_entered_pending_onboarding_tasks_provider_onboarding, CURRENT_TIMESTAMP()) AS FLOAT) * (1/3600)
        --         ELSE 
        --             CAST(DATEDIFF('second', date_entered_pending_onboarding_tasks_provider_onboarding, date_exited_pending_onboarding_tasks_provider_onboarding) AS FLOAT) * (1/3600)
        --     END, 2
        -- ) as hours_in_pending_onboarding_tasks_provider_onboarding,
        
        -- -- Schedule Onboarding Call Provider Onboarding Stage
        -- ROUND(
        --     CASE
        --         WHEN date_entered_schedule_onboarding_call_provider_onboarding IS NULL THEN 0
        --         WHEN date_exited_schedule_onboarding_call_provider_onboarding IS NULL THEN 
        --             CAST(DATEDIFF('second', date_entered_schedule_onboarding_call_provider_onboarding, CURRENT_TIMESTAMP()) AS FLOAT) * (1/3600)
        --         ELSE 
        --             CAST(DATEDIFF('second', date_entered_schedule_onboarding_call_provider_onboarding, date_exited_schedule_onboarding_call_provider_onboarding) AS FLOAT) * (1/3600)
        --     END, 2
        -- ) as hours_in_schedule_onboarding_call_provider_onboarding,
        
        -- -- Checkr Fail Provider Onboarding Stage
        -- ROUND(
        --     CASE
        --         WHEN date_entered_checkr_fail_provider_onboarding IS NULL THEN 0
        --         WHEN date_exited_checkr_fail_provider_onboarding IS NULL THEN 
        --             CAST(DATEDIFF('second', date_entered_checkr_fail_provider_onboarding, CURRENT_TIMESTAMP()) AS FLOAT) * (1/3600)
        --         ELSE 
        --             CAST(DATEDIFF('second', date_entered_checkr_fail_provider_onboarding, date_exited_checkr_fail_provider_onboarding) AS FLOAT) * (1/3600)
        --     END, 2
        -- ) as hours_in_checkr_fail_provider_onboarding,
        
        -- -- Hold Provider Onboarding Stage
        -- ROUND(
        --     CASE
        --         WHEN date_entered_hold_provider_onboarding IS NULL THEN 0
        --         WHEN date_exited_hold_provider_onboarding IS NULL THEN 
        --             CAST(DATEDIFF('second', date_entered_hold_provider_onboarding, CURRENT_TIMESTAMP()) AS FLOAT) * (1/3600)
        --         ELSE 
        --             CAST(DATEDIFF('second', date_entered_hold_provider_onboarding, date_exited_hold_provider_onboarding) AS FLOAT) * (1/3600)
        --     END, 2
        -- ) as hours_in_hold_provider_onboarding,
        
        -- Referrals Provider Recruiting Stage
        ROUND(
            CASE
                WHEN date_entered_referrals_provider_recruiting IS NULL THEN 0
                WHEN date_exited_referrals_provider_recruiting IS NULL THEN 
                    CAST(DATEDIFF('second', date_entered_referrals_provider_recruiting, CURRENT_TIMESTAMP()) AS FLOAT) * (1/3600)
                ELSE 
                    CAST(DATEDIFF('second', date_entered_referrals_provider_recruiting, date_exited_referrals_provider_recruiting) AS FLOAT) * (1/3600)
            END, 2
        ) as hours_in_referrals_provider_recruiting,
        
        -- Lead Magnet Provider Recruiting Stage
        -- ROUND(
        --     CASE
        --         WHEN date_entered_lead_magnet_provider_recruiting IS NULL THEN 0
        --         WHEN date_exited_lead_magnet_provider_recruiting IS NULL THEN 
        --             CAST(DATEDIFF('second', date_entered_lead_magnet_provider_recruiting, CURRENT_TIMESTAMP()) AS FLOAT) * (1/3600)
        --         ELSE 
        --             CAST(DATEDIFF('second', date_entered_lead_magnet_provider_recruiting, date_exited_lead_magnet_provider_recruiting) AS FLOAT) * (1/3600)
        --     END, 2
        -- ) as hours_in_lead_magnet_provider_recruiting,
        
        -- New Provider Lead Provider Recruiting Stage
        ROUND(
            CASE
                WHEN date_entered_new_provider_lead_provider_recruiting IS NULL THEN 0
                WHEN date_exited_new_provider_lead_provider_recruiting IS NULL THEN 
                    CAST(DATEDIFF('second', date_entered_new_provider_lead_provider_recruiting, CURRENT_TIMESTAMP()) AS FLOAT) * (1/3600)
                ELSE 
                    CAST(DATEDIFF('second', date_entered_new_provider_lead_provider_recruiting, date_exited_new_provider_lead_provider_recruiting) AS FLOAT) * (1/3600)
            END, 2
        ) as hours_in_new_provider_lead_provider_recruiting,
        
        -- Interview Booked Provider Recruiting Stage
        ROUND(
            CASE
                WHEN date_entered_interview_booked_provider_recruiting IS NULL THEN 0
                WHEN date_exited_interview_booked_provider_recruiting IS NULL THEN 
                    CAST(DATEDIFF('second', date_entered_interview_booked_provider_recruiting, CURRENT_TIMESTAMP()) AS FLOAT) * (1/3600)
                ELSE 
                    CAST(DATEDIFF('second', date_entered_interview_booked_provider_recruiting, date_exited_interview_booked_provider_recruiting) AS FLOAT) * (1/3600)
            END, 2
        ) as hours_in_interview_booked_provider_recruiting,
        
        -- Post Interview Provider Recruiting Stage
        ROUND(
            CASE
                WHEN date_entered_post_interview_provider_recruiting IS NULL THEN 0
                WHEN date_exited_post_interview_provider_recruiting IS NULL THEN 
                    CAST(DATEDIFF('second', date_entered_post_interview_provider_recruiting, CURRENT_TIMESTAMP()) AS FLOAT) * (1/3600)
                ELSE 
                    CAST(DATEDIFF('second', date_entered_post_interview_provider_recruiting, date_exited_post_interview_provider_recruiting) AS FLOAT) * (1/3600)
            END, 2
        ) as hours_in_post_interview_provider_recruiting,
        
        -- Clinical Interview Provider Recruiting Stage
        ROUND(
            CASE
                WHEN date_entered_clinical_interview_provider_recruiting IS NULL THEN 0
                WHEN date_exited_clinical_interview_provider_recruiting IS NULL THEN 
                    CAST(DATEDIFF('second', date_entered_clinical_interview_provider_recruiting, CURRENT_TIMESTAMP()) AS FLOAT) * (1/3600)
                ELSE 
                    CAST(DATEDIFF('second', date_entered_clinical_interview_provider_recruiting, date_exited_clinical_interview_provider_recruiting) AS FLOAT) * (1/3600)
            END, 2
        ) as hours_in_clinical_interview_provider_recruiting,
        
        -- Offer Letter Provider Recruiting Stage
        ROUND(
            CASE
                WHEN date_entered_offer_letter_provider_recruiting IS NULL THEN 0
                WHEN date_exited_offer_letter_provider_recruiting IS NULL THEN 
                    CAST(DATEDIFF('second', date_entered_offer_letter_provider_recruiting, CURRENT_TIMESTAMP()) AS FLOAT) * (1/3600)
                ELSE 
                    CAST(DATEDIFF('second', date_entered_offer_letter_provider_recruiting, date_exited_offer_letter_provider_recruiting) AS FLOAT) * (1/3600)
            END, 2
        ) as hours_in_offer_letter_provider_recruiting,

        ROUND(
            CASE 
                WHEN date_entered_pending_tasks_provider_recruiting IS NULL THEN 0
                WHEN date_exited_pending_tasks_provider_recruiting IS NULL THEN 
                    CAST(DATEDIFF('second', date_entered_pending_tasks_provider_recruiting, CURRENT_TIMESTAMP()) AS FLOAT) * (1/3600)
                ELSE 
                    CAST(DATEDIFF('second', date_entered_pending_tasks_provider_recruiting, date_exited_pending_tasks_provider_recruiting) AS FLOAT) * (1/3600)
            END, 2
        ) as hours_in_pending_tasks_provider_recruiting,

        -- ROUND(
        --     CASE 
        --         WHEN date_entered_onboarding_call_provider_recruiting IS NULL THEN 0
        --         WHEN date_exited_onboarding_call_provider_recruiting IS NULL THEN 
        --             CAST(DATEDIFF('second', date_entered_onboarding_call_provider_recruiting, CURRENT_TIMESTAMP()) AS FLOAT) * (1/3600)
        --         ELSE 
        --             CAST(DATEDIFF('second', date_entered_onboarding_call_provider_recruiting, date_exited_onboarding_call_provider_recruiting) AS FLOAT) * (1/3600)
        --     END, 2
        -- ) as hours_in_onboarding_call_provider_recruiting,

        ROUND(
            CASE 
                WHEN date_entered_pre_launch_provider_recruiting IS NULL THEN 0
                WHEN date_exited_pre_launch_provider_recruiting IS NULL THEN 
                    CAST(DATEDIFF('second', date_entered_pre_launch_provider_recruiting, CURRENT_TIMESTAMP()) AS FLOAT) * (1/3600)
                ELSE 
                    CAST(DATEDIFF('second', date_entered_pre_launch_provider_recruiting, date_exited_pre_launch_provider_recruiting) AS FLOAT) * (1/3600)
            END, 2
        ) as hours_in_pre_launch_provider_recruiting,
        
        -- Recruitment Complete Provider Recruiting Stage
        ROUND(
            CASE
                WHEN date_entered_recruitment_complete_provider_recruiting IS NULL THEN 0
                WHEN date_exited_recruitment_complete_provider_recruiting IS NULL THEN 
                    CAST(DATEDIFF('second', date_entered_recruitment_complete_provider_recruiting, CURRENT_TIMESTAMP()) AS FLOAT) * (1/3600)
                ELSE 
                    CAST(DATEDIFF('second', date_entered_recruitment_complete_provider_recruiting, date_exited_recruitment_complete_provider_recruiting) AS FLOAT) * (1/3600)
            END, 2
        ) as hours_in_recruitment_complete_provider_recruiting,
        
        -- Re-Activate Provider Recruiting Stage
        ROUND(
            CASE
                WHEN date_entered_re_activate_provider_recruiting IS NULL THEN 0
                WHEN date_exited_re_activate_provider_recruiting IS NULL THEN 
                    CAST(DATEDIFF('second', date_entered_re_activate_provider_recruiting, CURRENT_TIMESTAMP()) AS FLOAT) * (1/3600)
                ELSE 
                    CAST(DATEDIFF('second', date_entered_re_activate_provider_recruiting, date_exited_re_activate_provider_recruiting) AS FLOAT) * (1/3600)
            END, 2
        ) as hours_in_re_activate_provider_recruiting,
        
        -- Interview CX Provider Recruiting Stage
        ROUND(
            CASE    
                WHEN date_entered_interview_cx_provider_recruiting IS NULL THEN 0
                WHEN date_exited_interview_cx_provider_recruiting IS NULL THEN 
                    CAST(DATEDIFF('second', date_entered_interview_cx_provider_recruiting, CURRENT_TIMESTAMP()) AS FLOAT) * (1/3600)
                ELSE 
                    CAST(DATEDIFF('second', date_entered_interview_cx_provider_recruiting, date_exited_interview_cx_provider_recruiting) AS FLOAT) * (1/3600)
            END, 2
        ) as hours_in_interview_cx_provider_recruiting,

        -- Pending Referral Specialties Provider Recruiting Stage
        ROUND(
            CASE
                WHEN date_entered_pending_referral_specialties_provider_recruiting IS NULL THEN 0
                WHEN date_exited_pending_referral_specialties_provider_recruiting IS NULL THEN 
                    CAST(DATEDIFF('second', date_entered_pending_referral_specialties_provider_recruiting, CURRENT_TIMESTAMP()) AS FLOAT) * (1/3600)
                ELSE 
                    CAST(DATEDIFF('second', date_entered_pending_referral_specialties_provider_recruiting, date_exited_pending_referral_specialties_provider_recruiting) AS FLOAT) * (1/3600)
            END, 2
        ) as hours_in_pending_referral_specialties_provider_recruiting,

        -- Ready for Kickoff Provider Recruiting Stage  
        ROUND(
            CASE
                WHEN date_entered_ready_for_kickoff_provider_recruiting IS NULL THEN 0
                WHEN date_exited_ready_for_kickoff_provider_recruiting IS NULL THEN 
                    CAST(DATEDIFF('second', date_entered_ready_for_kickoff_provider_recruiting, CURRENT_TIMESTAMP()) AS FLOAT) * (1/3600)
                ELSE 
                    CAST(DATEDIFF('second', date_entered_ready_for_kickoff_provider_recruiting, date_exited_ready_for_kickoff_provider_recruiting) AS FLOAT) * (1/3600)
            END, 2
        ) as hours_in_ready_for_kickoff_provider_recruiting,

        -- Hold Provider Recruiting Stage
        ROUND(
            CASE
                WHEN date_entered_hold_provider_recruiting IS NULL THEN 0
                WHEN date_exited_hold_provider_recruiting IS NULL THEN 
                    CAST(DATEDIFF('second', date_entered_hold_provider_recruiting, CURRENT_TIMESTAMP()) AS FLOAT) * (1/3600)
                ELSE 
                    CAST(DATEDIFF('second', date_entered_hold_provider_recruiting, date_exited_hold_provider_recruiting) AS FLOAT) * (1/3600)
            END, 2
        ) as hours_in_hold_provider_recruiting,
        
        -- -- Convert hours to days for each stage
        -- ROUND(hours_in_pre_launch_provider_onboarding / 24, 2) as days_in_pre_launch_provider_onboarding,
        -- ROUND(hours_in_ready_to_onboard_provider_onboarding / 24, 2) as days_in_ready_to_onboard_provider_onboarding,
        -- ROUND(hours_in_pending_onboarding_tasks_provider_onboarding / 24, 2) as days_in_pending_onboarding_tasks_provider_onboarding,
        -- ROUND(hours_in_schedule_onboarding_call_provider_onboarding / 24, 2) as days_in_schedule_onboarding_call_provider_onboarding,
        -- ROUND(hours_in_checkr_fail_provider_onboarding / 24, 2) as days_in_checkr_fail_provider_onboarding,
        -- ROUND(hours_in_hold_provider_onboarding / 24, 2) as days_in_hold_provider_onboarding,
        
        ROUND(hours_in_referrals_provider_recruiting / 24, 2) as days_in_referrals_provider_recruiting,
        -- ROUND(hours_in_lead_magnet_provider_recruiting / 24, 2) as days_in_lead_magnet_provider_recruiting,
        ROUND(hours_in_new_provider_lead_provider_recruiting / 24, 2) as days_in_new_provider_lead_provider_recruiting,
        ROUND(hours_in_interview_booked_provider_recruiting / 24, 2) as days_in_interview_booked_provider_recruiting,
        ROUND(hours_in_post_interview_provider_recruiting / 24, 2) as days_in_post_interview_provider_recruiting,
        ROUND(hours_in_clinical_interview_provider_recruiting / 24, 2) as days_in_clinical_interview_provider_recruiting,
        ROUND(hours_in_offer_letter_provider_recruiting / 24, 2) as days_in_offer_letter_provider_recruiting,
        ROUND(hours_in_pending_tasks_provider_recruiting / 24, 2) as days_in_pending_tasks_provider_recruiting,
        -- ROUND(hours_in_onboarding_call_provider_recruiting / 24, 2) as days_in_onboarding_call_provider_recruiting,
        ROUND(hours_in_pre_launch_provider_recruiting / 24, 2) as days_in_pre_launch_provider_recruiting,
        ROUND(hours_in_recruitment_complete_provider_recruiting / 24, 2) as days_in_recruitment_complete_provider_recruiting,
        ROUND(hours_in_re_activate_provider_recruiting / 24, 2) as days_in_re_activate_provider_recruiting,
        ROUND(hours_in_interview_cx_provider_recruiting / 24, 2) as days_in_interview_cx_provider_recruiting,
        ROUND(hours_in_pending_referral_specialties_provider_recruiting / 24, 2) as days_in_pending_referral_specialties_provider_recruiting,
        ROUND(hours_in_ready_for_kickoff_provider_recruiting / 24, 2) as days_in_ready_for_kickoff_provider_recruiting,
        ROUND(hours_in_hold_provider_recruiting / 24, 2) as days_in_hold_provider_recruiting
        
        -- Add Checkr Fail Provider Onboarding Stage status
        -- case 
        --     when date_entered_checkr_fail_provider_onboarding is null and date_exited_checkr_fail_provider_onboarding is null then 'never_entered'
        --     when date_entered_checkr_fail_provider_onboarding is not null and date_exited_checkr_fail_provider_onboarding is null then 'current'
        --     when date_entered_checkr_fail_provider_onboarding = date_exited_checkr_fail_provider_onboarding then 'skipped'
        --     else 'completed'
        -- end as checkr_fail_provider_onboarding_status
    from provider_deals
),

current_stage_summary as (
    select 
        hubspot_provider_id,
        coral_provider_id,
        case

            --DEPRECATED STAGES 
            /*
            -- First check terminal onboarding stages
            when date_entered_onboarding_complete_provider_onboarding is not null and 
                (date_exited_onboarding_complete_provider_onboarding is null or 
                 date_exited_onboarding_complete_provider_onboarding < date_entered_onboarding_complete_provider_onboarding) 
                then 'Onboarding Complete (Provider Onboarding)'
            when date_entered_checkr_fail_provider_onboarding is not null and 
                (date_exited_checkr_fail_provider_onboarding is null or 
                 date_exited_checkr_fail_provider_onboarding < date_entered_checkr_fail_provider_onboarding) 
                then 'Checkr Fail (Provider Onboarding)'
            when date_entered_hold_provider_onboarding is not null and 
                (date_exited_hold_provider_onboarding is null or 
                 date_exited_hold_provider_onboarding < date_entered_hold_provider_onboarding) 
                then 'Hold (Provider Onboarding)'
            when date_entered_closed_lost_provider_onboarding is not null then 'Closed Lost (Provider Onboarding)'
            when date_entered_disqualified_provider_onboarding is not null then 'Disqualified (Provider Onboarding)'
            
            -- Then check if any onboarding stages are current (in reverse order - most advanced first)
            when schedule_onboarding_call_provider_onboarding_status = 'current' or
                 (date_entered_schedule_onboarding_call_provider_onboarding is not null and 
                  date_exited_schedule_onboarding_call_provider_onboarding < date_entered_schedule_onboarding_call_provider_onboarding)
                then 'Schedule Onboarding Call (Provider Onboarding)'
            when pending_onboarding_tasks_provider_onboarding_status = 'current' or
                 (date_entered_pending_onboarding_tasks_provider_onboarding is not null and 
                  date_exited_pending_onboarding_tasks_provider_onboarding < date_entered_pending_onboarding_tasks_provider_onboarding)
                then 'Pending Onboarding Tasks (Provider Onboarding)'
            when ready_to_onboard_provider_onboarding_status = 'current' or
                 (date_entered_ready_to_onboard_provider_onboarding is not null and 
                  date_exited_ready_to_onboard_provider_onboarding < date_entered_ready_to_onboard_provider_onboarding)
                then 'Ready to Onboard (Provider Onboarding)'
            when date_entered_pre_launch_provider_onboarding is not null and 
                 (date_exited_pre_launch_provider_onboarding is null or 
                  date_exited_pre_launch_provider_onboarding < date_entered_pre_launch_provider_onboarding)
                then 'Pre-Launch (Provider Onboarding)'
            when date_entered_cold_provider_onboarding is not null and 
                 (date_exited_cold_provider_onboarding is null or 
                  date_exited_cold_provider_onboarding < date_entered_cold_provider_onboarding)
                then 'Cold (Provider Onboarding)'
                */
            
            -- Then check recruiting stages only if no onboarding stages are current (also in reverse order)
            -- Treat recruitment_complete as terminal within the recruiting funnel

            when date_entered_recruitment_complete_provider_recruiting is not null and 
                 (date_exited_recruitment_complete_provider_recruiting is null or 
                  date_exited_recruitment_complete_provider_recruiting < date_entered_recruitment_complete_provider_recruiting)
                then 'Recruitment Complete (Provider Recruiting)'
            when date_entered_disqualified_provider_recruiting is not null then 'Disqualified (Provider Recruiting)'
            when date_entered_closed_lost_provider_recruiting is not null then 'Closed Lost (Provider Recruiting)'
            when date_entered_hold_provider_recruiting is not null then 'Hold (Provider Recruiting)'
            when pre_launch_provider_recruiting_status = 'current' or 
                 (date_entered_pre_launch_provider_recruiting is not null and
                  date_exited_pre_launch_provider_recruiting < date_entered_pre_launch_provider_recruiting)
                then 'Pre-Launch (Provider Recruiting)'
            when date_entered_ready_for_kickoff_provider_recruiting is not null then 'Ready for Kickoff Call (Provider Recruiting)'
            when pending_tasks_provider_recruiting_status = 'current' or
                 (date_entered_pending_tasks_provider_recruiting is not null and 
                  date_exited_pending_tasks_provider_recruiting < date_entered_pending_tasks_provider_recruiting)
                then 'Pending Tasks (Provider Recruiting)'
            when offer_letter_provider_recruiting_status = 'current' or
                 (date_entered_offer_letter_provider_recruiting is not null and 
                  date_exited_offer_letter_provider_recruiting < date_entered_offer_letter_provider_recruiting)
                then 'Offer Letter (Provider Recruiting)'
            when date_entered_pending_referral_specialties_provider_recruiting is not null then 'Pending Referral/Specialty'
            when clinical_interview_provider_recruiting_status = 'current' or
                 (date_entered_clinical_interview_provider_recruiting is not null and 
                  date_exited_clinical_interview_provider_recruiting < date_entered_clinical_interview_provider_recruiting)
                then 'Clinical Interview (Provider Recruiting)'
            when date_entered_interview_cx_provider_recruiting is not null then 'Interview CX'
            when interview_booked_provider_recruiting_status = 'current' or
                 (date_entered_interview_booked_provider_recruiting is not null and 
                  date_exited_interview_booked_provider_recruiting < date_entered_interview_booked_provider_recruiting) then 
                'Interview Booked (Provider Recruiting)'
            when new_provider_lead_provider_recruiting_status = 'current' or
                 (date_entered_new_provider_lead_provider_recruiting is not null and 
                  date_exited_new_provider_lead_provider_recruiting < date_entered_new_provider_lead_provider_recruiting) then 
                'New Provider Lead (Provider Recruiting)'
            when date_entered_re_activate_provider_recruiting is not null then 'Re-Activate (Provider Recruiting)'
            when referrals_provider_recruiting_status = 'current' or
                 (date_entered_referrals_provider_recruiting is not null and 
                  date_exited_referrals_provider_recruiting < date_entered_referrals_provider_recruiting) then 
                'Referrals (Provider Recruiting)'
            
            
            -- DEPRECATED STAGES 
/*
            when onboarding_call_provider_recruiting_status = 'current' or
                 (date_entered_onboarding_call_provider_recruiting is not null and 
                  date_exited_onboarding_call_provider_recruiting < date_entered_onboarding_call_provider_recruiting)
                then 'Onboarding Call (Provider Recruiting)'


            when post_interview_provider_recruiting_status = 'current' or
                 (date_entered_post_interview_provider_recruiting is not null and 
                  date_exited_post_interview_provider_recruiting < date_entered_post_interview_provider_recruiting)
                then 'Post Interview (Provider Recruiting)'


            when lead_magnet_provider_recruiting_status = 'current' or
                 (date_entered_lead_magnet_provider_recruiting is not null and 
                  date_exited_lead_magnet_provider_recruiting < date_entered_lead_magnet_provider_recruiting) then 
                'Lead Magnet (Provider Recruiting)'

            when date_entered_cold_provider_recruiting is not null and 
                 (date_exited_cold_provider_recruiting is null or 
                  date_exited_cold_provider_recruiting < date_entered_cold_provider_recruiting) then 
                'Cold (Provider Recruiting)'
            
            when date_entered_disqualified_provider_recruiting is not null then 'Disqualified (Provider Recruiting)'
            
            -- If no current stages, check if any onboarding stages have been completed (in reverse order)
            when date_entered_onboarding_complete_provider_onboarding is not null then 'Onboarding Complete (Provider Onboarding)'
            when date_entered_schedule_onboarding_call_provider_onboarding is not null then 'Schedule Onboarding Call (Provider Onboarding)'
            when date_entered_pending_onboarding_tasks_provider_onboarding is not null then 'Pending Onboarding Tasks (Provider Onboarding)'
            when date_entered_ready_to_onboard_provider_onboarding is not null then 'Ready to Onboard (Provider Onboarding)'
            when date_entered_hold_provider_onboarding is not null then 'Hold (Provider Onboarding)'
            when date_entered_checkr_fail_provider_onboarding is not null then 'Checkr Fail (Provider Onboarding)'
            when date_entered_closed_lost_provider_onboarding is not null then 'Closed Lost (Provider Onboarding)'
            when date_entered_disqualified_provider_onboarding is not null then 'Disqualified (Provider Onboarding)'
            when date_entered_pre_launch_provider_onboarding is not null then 'Pre-Launch (Provider Onboarding)'
            
            -- If no onboarding stages, check if any recruiting stages have been completed (in reverse order)
            when date_entered_recruitment_complete_provider_recruiting is not null then 'Recruitment Complete (Provider Recruiting)'
            when date_entered_offer_letter_provider_recruiting is not null then 'Offer Letter (Provider Recruiting)'
            when date_entered_clinical_interview_provider_recruiting is not null then 'Clinical Interview (Provider Recruiting)'
            when date_entered_post_interview_provider_recruiting is not null then 'Post Interview (Provider Recruiting)'
            when date_entered_interview_booked_provider_recruiting is not null then 'Interview Booked (Provider Recruiting)'
            when date_entered_new_provider_lead_provider_recruiting is not null then 'New Provider Lead (Provider Recruiting)'
            when date_entered_lead_magnet_provider_recruiting is not null then 'Lead Magnet (Provider Recruiting)'
   
*/
            
            else null
        end as current_stage,
        
        -- Similar updates for hours_in_current_stage calculation
        case
            
            --DEPRECATED 
            /*
            -- Terminal onboarding stages
            when date_entered_onboarding_complete_provider_onboarding is not null and 
                (date_exited_onboarding_complete_provider_onboarding is null or 
                 date_exited_onboarding_complete_provider_onboarding < date_entered_onboarding_complete_provider_onboarding) then 
                ROUND(CAST(DATEDIFF('second', date_entered_onboarding_complete_provider_onboarding, CURRENT_TIMESTAMP()) AS FLOAT) * (1/3600), 2)
            when date_entered_checkr_fail_provider_onboarding is not null and 
                (date_exited_checkr_fail_provider_onboarding is null or 
                 date_exited_checkr_fail_provider_onboarding < date_entered_checkr_fail_provider_onboarding) then 
                hours_in_checkr_fail_provider_onboarding
            when date_entered_hold_provider_onboarding is not null and 
                (date_exited_hold_provider_onboarding is null or 
                 date_exited_hold_provider_onboarding < date_entered_hold_provider_onboarding) then 
                hours_in_hold_provider_onboarding
            when date_entered_closed_lost_provider_onboarding is not null then 
                ROUND(CAST(DATEDIFF('second', date_entered_closed_lost_provider_onboarding, CURRENT_TIMESTAMP()) AS FLOAT) * (1/3600), 2)
            when date_entered_disqualified_provider_onboarding is not null then 
                ROUND(CAST(DATEDIFF('second', date_entered_disqualified_provider_onboarding, CURRENT_TIMESTAMP()) AS FLOAT) * (1/3600), 2)
            
            -- Provider Onboarding Stages (in reverse order)
            when schedule_onboarding_call_provider_onboarding_status = 'current' or
                 (date_entered_schedule_onboarding_call_provider_onboarding is not null and 
                  date_exited_schedule_onboarding_call_provider_onboarding < date_entered_schedule_onboarding_call_provider_onboarding) then 
                hours_in_schedule_onboarding_call_provider_onboarding
            when pending_onboarding_tasks_provider_onboarding_status = 'current' or
                 (date_entered_pending_onboarding_tasks_provider_onboarding is not null and 
                  date_exited_pending_onboarding_tasks_provider_onboarding < date_entered_pending_onboarding_tasks_provider_onboarding) then 
                hours_in_pending_onboarding_tasks_provider_onboarding
            when ready_to_onboard_provider_onboarding_status = 'current' or
                 (date_entered_ready_to_onboard_provider_onboarding is not null and 
                  date_exited_ready_to_onboard_provider_onboarding < date_entered_ready_to_onboard_provider_onboarding) then 
                hours_in_ready_to_onboard_provider_onboarding
            when date_entered_pre_launch_provider_onboarding is not null and 
                 (date_exited_pre_launch_provider_onboarding is null or 
                  date_exited_pre_launch_provider_onboarding < date_entered_pre_launch_provider_onboarding) then 
                ROUND(CAST(DATEDIFF('second', date_entered_pre_launch_provider_onboarding, CURRENT_TIMESTAMP()) AS FLOAT) * (1/3600), 2)
            when date_entered_cold_provider_onboarding is not null and 
                 (date_exited_cold_provider_onboarding is null or 
                  date_exited_cold_provider_onboarding < date_entered_cold_provider_onboarding) then 
                ROUND(CAST(DATEDIFF('second', date_entered_cold_provider_onboarding, CURRENT_TIMESTAMP()) AS FLOAT) * (1/3600), 2)
            */

            -- Provider Recruiting Stages (in reverse order)
            -- Treat recruitment_complete as terminal within the recruiting funnel
            when date_entered_recruitment_complete_provider_recruiting is not null then 
                hours_in_recruitment_complete_provider_recruiting
            when date_entered_hold_provider_recruiting is not null then 
                hours_in_hold_provider_recruiting 
            when date_entered_pre_launch_provider_recruiting is not null then 
                hours_in_pre_launch_provider_recruiting 
            when date_entered_ready_for_kickoff_provider_recruiting is not null then 
                hours_in_ready_for_kickoff_provider_recruiting 
            when date_entered_pending_tasks_provider_recruiting is not null then 
                hours_in_pending_tasks_provider_recruiting 
            when offer_letter_provider_recruiting_status = 'current' or
                 (date_entered_offer_letter_provider_recruiting is not null and 
                  date_exited_offer_letter_provider_recruiting < date_entered_offer_letter_provider_recruiting) then 
                hours_in_offer_letter_provider_recruiting
            when date_entered_pending_referral_specialties_provider_recruiting is not null then 
                hours_in_pending_referral_specialties_provider_recruiting 
            when clinical_interview_provider_recruiting_status = 'current' or
                 (date_entered_clinical_interview_provider_recruiting is not null and 
                  date_exited_clinical_interview_provider_recruiting < date_entered_clinical_interview_provider_recruiting) then 
                hours_in_clinical_interview_provider_recruiting
            when date_entered_interview_cx_provider_recruiting is not null then 
                hours_in_interview_cx_provider_recruiting 
            when interview_booked_provider_recruiting_status = 'current' or
                 (date_entered_interview_booked_provider_recruiting is not null and 
                  date_exited_interview_booked_provider_recruiting < date_entered_interview_booked_provider_recruiting) then 
                hours_in_interview_booked_provider_recruiting
            when new_provider_lead_provider_recruiting_status = 'current' or
                 (date_entered_new_provider_lead_provider_recruiting is not null and 
                  date_exited_new_provider_lead_provider_recruiting < date_entered_new_provider_lead_provider_recruiting) then 
                hours_in_new_provider_lead_provider_recruiting
            when date_entered_re_activate_provider_recruiting is not null then 
                hours_in_re_activate_provider_recruiting 
            when referrals_provider_recruiting_status = 'current' or
                 (date_entered_referrals_provider_recruiting is not null and 
                  date_exited_referrals_provider_recruiting < date_entered_referrals_provider_recruiting) then 
                hours_in_referrals_provider_recruiting
            


            --DEPRECATED STAGES 
            /*
            when post_interview_provider_recruiting_status = 'current' or
                 (date_entered_post_interview_provider_recruiting is not null and 
                  date_exited_post_interview_provider_recruiting < date_entered_post_interview_provider_recruiting) then 
                hours_in_post_interview_provider_recruiting
           
            
            when lead_magnet_provider_recruiting_status = 'current' or
                 (date_entered_lead_magnet_provider_recruiting is not null and 
                  date_exited_lead_magnet_provider_recruiting < date_entered_lead_magnet_provider_recruiting) then 
                hours_in_lead_magnet_provider_recruiting
           
            when cold_provider_recruiting_status = 'current' or
                 (date_entered_cold_provider_recruiting is not null and 
                  date_exited_cold_provider_recruiting < date_entered_cold_provider_recruiting) then 
                ROUND(CAST(DATEDIFF('second', date_entered_cold_provider_recruiting, CURRENT_TIMESTAMP()) AS FLOAT) * (1/3600), 2)
            when closed_lost_provider_recruiting_status = 'current' then 
                ROUND(CAST(DATEDIFF('second', date_entered_closed_lost_provider_recruiting, CURRENT_TIMESTAMP()) AS FLOAT) * (1/3600), 2)
            when date_entered_disqualified_provider_recruiting is not null then 
                ROUND(CAST(DATEDIFF('second', date_entered_disqualified_provider_recruiting, CURRENT_TIMESTAMP()) AS FLOAT) * (1/3600), 2)
            */
        end as hours_in_current_stage
    from enriched
),

stage_timing_summary as (
    select
        hubspot_provider_id,
        -- Total time in funnel
        ROUND(
            -- Provider Recruiting Stages
            COALESCE(days_in_referrals_provider_recruiting, 0) +
            --COALESCE(days_in_lead_magnet_provider_recruiting, 0) +
            COALESCE(days_in_new_provider_lead_provider_recruiting, 0) +
            COALESCE(days_in_interview_booked_provider_recruiting, 0) +
            COALESCE(days_in_post_interview_provider_recruiting, 0) +
            COALESCE(days_in_clinical_interview_provider_recruiting, 0) +
            COALESCE(days_in_offer_letter_provider_recruiting, 0) +
            COALESCE(days_in_pending_tasks_provider_recruiting, 0) + 
            -- COALESCE(days_in_onboarding_call_provider_recruiting, 0) + 
            COALESCE(days_in_pre_launch_provider_recruiting, 0) + 
            COALESCE(days_in_recruitment_complete_provider_recruiting, 0) +
            COALESCE(days_in_re_activate_provider_recruiting, 0) + 
            COALESCE(days_in_interview_cx_provider_recruiting, 0) + 
            COALESCE(days_in_pending_referral_specialties_provider_recruiting, 0) + 
            COALESCE(days_in_ready_for_kickoff_provider_recruiting, 0) + 
            COALESCE(days_in_hold_provider_recruiting, 0), 
        --DEPRECATED STAGES 
        /*
            -- Provider Onboarding Stages
            COALESCE(days_in_pre_launch_provider_onboarding, 0) +
            COALESCE(days_in_ready_to_onboard_provider_onboarding, 0) +
            COALESCE(days_in_pending_onboarding_tasks_provider_onboarding, 0) +
            COALESCE(days_in_schedule_onboarding_call_provider_onboarding, 0) + 
            COALESCE(days_in_checkr_fail_provider_onboarding, 0) +
            COALESCE(days_in_hold_provider_onboarding, 0),
        */
            2
        ) as total_days_in_funnel,
        -- Longest stage
        GREATEST(
            -- Provider Recruiting Stages
            COALESCE(days_in_referrals_provider_recruiting, 0),
            --COALESCE(days_in_lead_magnet_provider_recruiting, 0),
            COALESCE(days_in_new_provider_lead_provider_recruiting, 0),
            COALESCE(days_in_interview_booked_provider_recruiting, 0),
            COALESCE(days_in_post_interview_provider_recruiting, 0),
            COALESCE(days_in_clinical_interview_provider_recruiting, 0),
            COALESCE(days_in_offer_letter_provider_recruiting, 0),
            COALESCE(days_in_recruitment_complete_provider_recruiting, 0),
            COALESCE(days_in_re_activate_provider_recruiting, 0) + 
            COALESCE(days_in_interview_cx_provider_recruiting, 0) + 
            COALESCE(days_in_pending_referral_specialties_provider_recruiting, 0) + 
            COALESCE(days_in_ready_for_kickoff_provider_recruiting, 0) + 
            COALESCE(days_in_hold_provider_recruiting, 0)
            
            -- Provider Onboarding Stages
            /*
            COALESCE(days_in_pre_launch_provider_onboarding, 0),
            COALESCE(days_in_ready_to_onboard_provider_onboarding, 0),
            COALESCE(days_in_pending_onboarding_tasks_provider_onboarding, 0),
            COALESCE(days_in_schedule_onboarding_call_provider_onboarding, 0),
            COALESCE(days_in_checkr_fail_provider_onboarding, 0),
            COALESCE(days_in_hold_provider_onboarding, 0).
            */
        ) as longest_stage_duration
    from enriched
),

stage_statuses as (
    select 
        hubspot_provider_id,
        coral_provider_id,
        'referrals_provider_recruiting_status' as stage_name,
        referrals_provider_recruiting_status as status,
        1 as stage_order
    from enriched
    where referrals_provider_recruiting_status = 'completed'
    
    union all
    
    -- select 
    --     hubspot_provider_id,
    --     coral_provider_id,
    --     'lead_magnet_provider_recruiting_status' as stage_name,
    --     lead_magnet_provider_recruiting_status as status,
    --     2 as stage_order
    -- from enriched
    -- where lead_magnet_provider_recruiting_status = 'completed'

    select 
        hubspot_provider_id,
        coral_provider_id,
        're_activate_provider_recruiting_status' as stage_name,
        re_activate_provider_recruiting_status as status,
        2 as stage_order
    from enriched
    where re_activate_provider_recruiting_status = 'completed'
    
    union all
    
    select 
        hubspot_provider_id,
        coral_provider_id,
        'new_provider_lead_provider_recruiting_status' as stage_name,
        new_provider_lead_provider_recruiting_status as status,
        3 as stage_order
    from enriched
    where new_provider_lead_provider_recruiting_status = 'completed'
    
    union all
    
    select 
        hubspot_provider_id, 
        coral_provider_id,
        'interview_booked_provider_recruiting_status' as stage_name,
        interview_booked_provider_recruiting_status as status,
        4 as stage_order
    from enriched
    where interview_booked_provider_recruiting_status = 'completed'
    
    union all
    
    select 
        hubspot_provider_id,
        coral_provider_id,
        'interview_cx_provider_recruiting' as stage_name,
        interview_cx_provider_recruiting_status as status,
        5 as stage_order
    from enriched
    where interview_cx_provider_recruiting_status = 'completed'
    
    union all
    
    select 
        hubspot_provider_id,
        coral_provider_id,
        'clinical_interview_provider_recruiting_status' as stage_name,
        clinical_interview_provider_recruiting_status as status,
        6 as stage_order
    from enriched
    where clinical_interview_provider_recruiting_status = 'completed'
    
    union all
    
    select 
        hubspot_provider_id,
        coral_provider_id,
        'pending_referral_specialties_provider_recruiting_status' as stage_name,
        pending_referral_specialties_provider_recruiting_status as status,
        7 as stage_order
    from enriched
    where pending_referral_specialties_provider_recruiting_status = 'completed'
    
    union all


    select 
        hubspot_provider_id,
        coral_provider_id,
        'offer_letter_provider_recruiting_status' as stage_name,
        offer_letter_provider_recruiting_status as status,
        8 as stage_order
    from enriched
    where offer_letter_provider_recruiting_status = 'completed'
    
    union all
    
    -- New stages added after offer letter
    select 
        hubspot_provider_id,
        coral_provider_id,
        'pending_tasks_provider_recruiting_status' as stage_name,
        pending_tasks_provider_recruiting_status as status,
        9 as stage_order
    from enriched
    where pending_tasks_provider_recruiting_status = 'completed'

    union all

    select 
        hubspot_provider_id,
        coral_provider_id,
        'ready_for_kickoff_provider_recruiting_status' as stage_name,
        ready_for_kickoff_provider_recruiting_status as status,
        10 as stage_order
    from enriched
    where ready_for_kickoff_provider_recruiting_status = 'completed'

    union all

    select 
        hubspot_provider_id,
        coral_provider_id,
        'pre_launch_provider_recruiting_status' as stage_name,
        pre_launch_provider_recruiting_status as status,
        11 as stage_order
    from enriched
    where pre_launch_provider_recruiting_status = 'completed'

    union all

/*

    select 
        hubspot_provider_id,
        coral_provider_id,
        'onboarding_call_provider_recruiting_status' as stage_name,
        onboarding_call_provider_recruiting_status as status,
        9 as stage_order
    from enriched
    where onboarding_call_provider_recruiting_status = 'completed'

    union all
*/

    -- Recruitment Complete Provider Recruiting Stage
    select 
        hubspot_provider_id,
        coral_provider_id,
        'recruitment_complete_provider_recruiting_status' as stage_name,
        recruitment_complete_provider_recruiting_status as status,
        12 as stage_order
    from enriched
    where recruitment_complete_provider_recruiting_status = 'completed'
    
    union all

    select 
        hubspot_provider_id,
        coral_provider_id,
        'hold_provider_recruiting_status' as stage_name,
        hold_provider_recruiting_status as status,
        13 as stage_order
    from enriched
    where hold_provider_recruiting_status = 'completed'

    union all 

    select 
        hubspot_provider_id,
        coral_provider_id,
        'closed_lost_provider_recruiting_status' as stage_name,
        closed_lost_provider_recruiting_status as status,
        14 as stage_order
    from enriched
    where closed_lost_provider_recruiting_status = 'completed'

    union all 

    select 
        hubspot_provider_id,
        coral_provider_id,
        'disqualified_provider_recruiting_status' as stage_name,
        disqualified_provider_recruiting_status as status,
        15 as stage_order
    from enriched
    where disqualified_provider_recruiting_status = 'completed'

    
    /*
    select 
        hubspot_provider_id,
        coral_provider_id,
        'ready_to_onboard_provider_onboarding_status' as stage_name,
        ready_to_onboard_provider_onboarding_status as status,
        12 as stage_order
    from enriched
    where ready_to_onboard_provider_onboarding_status = 'completed'
    
    union all
    

    select 
        hubspot_provider_id,
        coral_provider_id,
        'pending_onboarding_tasks_provider_onboarding_status' as stage_name,
        pending_onboarding_tasks_provider_onboarding_status as status,
        13 as stage_order
    from enriched
    where pending_onboarding_tasks_provider_onboarding_status = 'completed'

    union all
    
    select 
        hubspot_provider_id,
        coral_provider_id,
        'schedule_onboarding_call_provider_onboarding_status' as stage_name,
        schedule_onboarding_call_provider_onboarding_status as status,
        14 as stage_order
    from enriched
    where schedule_onboarding_call_provider_onboarding_status = 'completed'

    union all

    select 
        hubspot_provider_id,
        coral_provider_id,
        'checkr_fail_provider_onboarding' as stage_name,
        case
            when date_entered_checkr_fail_provider_onboarding is null then 'never_entered'
            when date_entered_checkr_fail_provider_onboarding is not null and date_exited_checkr_fail_provider_onboarding is null then 'current'
            when date_entered_checkr_fail_provider_onboarding = date_exited_checkr_fail_provider_onboarding then 'skipped'
            else 'completed'
        end as status,
        15 as stage_order
    from enriched

    union all

    select 
        hubspot_provider_id,
        coral_provider_id,
        'hold_provider_onboarding' as stage_name,
        case
            when date_entered_hold_provider_onboarding is null then 'never_entered'
            when date_entered_hold_provider_onboarding is not null and date_exited_hold_provider_onboarding is null then 'current'
            when date_entered_hold_provider_onboarding = date_exited_hold_provider_onboarding then 'skipped'
            else 'completed'
        end as status,
        16 as stage_order
    from enriched

    union all

    select 
        hubspot_provider_id,
        coral_provider_id,
        'pre_launch_provider_onboarding' as stage_name,
        case
            when date_entered_pre_launch_provider_onboarding is null then 'never_entered'
            when date_entered_pre_launch_provider_onboarding is not null and date_exited_pre_launch_provider_onboarding is null then 'current'
            when date_entered_pre_launch_provider_onboarding = date_exited_pre_launch_provider_onboarding then 'skipped'
            else 'completed'
        end as status,
        17 as stage_order
    from enriched

    union all

    select 
        hubspot_provider_id,
        coral_provider_id,
        'cold_provider_recruiting' as stage_name,
        case
            when date_entered_cold_provider_recruiting is null then 'never_entered'
            when date_entered_cold_provider_recruiting is not null and date_exited_cold_provider_recruiting is null then 'current'
            when date_entered_cold_provider_recruiting = date_exited_cold_provider_recruiting then 'skipped'
            else 'completed'
        end as status,
        18 as stage_order
    from enriched
    */

),

funnel_progression as (
    select
        hubspot_provider_id, 
        coral_provider_id,
        LISTAGG(stage_name, ' -> ') within group (order by stage_order) as progression_path
    from stage_statuses
    group by hubspot_provider_id, coral_provider_id
)

-- Final output with all the enriched data plus summaries
select distinct 
    e.*,
    cs.current_stage,
    CASE 
        WHEN cs.current_stage IS NULL THEN 'Inactive_Lead'
        WHEN cs.current_stage IN ('Closed Lost (Provider Recruiting)', 'Recruitment Complete (Provider Recruiting)') THEN 'Inactive_Lead'
        WHEN cs.current_stage IN ('Pending Onboarding Tasks (Provider Onboarding)', 'Schedule Onboarding Call (Provider Onboarding)') THEN 'Onboarding'
        ELSE 'Active_Lead' 
    END AS provider_lead_status,
    cs.hours_in_current_stage,
    sts.total_days_in_funnel,
    sts.longest_stage_duration,
    fp.progression_path,
    -- Calculate total days to onboarding complete (closed won)
    -- CASE 
    --     WHEN date_entered_onboarding_complete_provider_onboarding IS NOT NULL THEN
    --         ROUND(CAST(DATEDIFF('second', first_deal_created_date, date_entered_onboarding_complete_provider_onboarding) AS FLOAT) * (1/86400), 2)
    --     ELSE NULL
    -- END as total_days_to_onboarding_complete,
    -- Calculate total days to closed lost in provider onboarding
    -- CASE 
    --     WHEN date_entered_closed_lost_provider_onboarding IS NOT NULL THEN
    --         ROUND(CAST(DATEDIFF('second', first_deal_created_date, date_entered_closed_lost_provider_onboarding) AS FLOAT) * (1/86400), 2)
    --     ELSE NULL
    -- END as total_days_to_closed_lost_onboarding,
    -- Calculate total days to closed lost in provider recruiting
    CASE 
        WHEN date_entered_closed_lost_provider_recruiting IS NOT NULL THEN
            ROUND(CAST(DATEDIFF('second', first_deal_created_date, date_entered_closed_lost_provider_recruiting) AS FLOAT) * (1/86400), 2)
        ELSE NULL
    END as total_days_to_closed_lost_recruiting
from enriched e
left join current_stage_summary cs on e.hubspot_provider_id = cs.hubspot_provider_id
left join stage_timing_summary sts on e.hubspot_provider_id = sts.hubspot_provider_id
left join funnel_progression fp on e.hubspot_provider_id = fp.hubspot_provider_id
order by 2,1