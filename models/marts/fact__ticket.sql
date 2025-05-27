-- Mart: HubSpot Ticket Analysis Fact Table
-- Combines ticket data with engagement responses to answer all key questions
-- This is a FACT table containing measurable ticket events and metrics

{{ config(
    materialized='table',
    indexes=[
      {'columns': ['ticket_id'], 'unique': true},
      {'columns': ['created_date']},
      {'columns': ['ticket_origin_type']},
      {'columns': ['pipeline_type']},
      {'columns': ['priority_clean']},
      {'columns': ['ticket_status']},
      {'columns': ['owner_id']}
    ]
) }}

WITH base_tickets AS (
  SELECT * FROM {{ ref('int__hubspot_ticket') }}
),

engagement_responses AS (
  SELECT * FROM {{ ref('int__hubspot_ticket_engagement') }}
),

final AS (
  SELECT 
    -- === PRIMARY KEYS & IDENTIFIERS ===
    bt.ticket_id,
    bt.hubspot_object_id,
    bt.ticket_number,
    bt.contact_id,
    
    -- === QUESTION 1: CORAL CARE vs INBOUND ===
    bt.last_email_type,
    bt.is_outbound_ticket,
    bt.object_source,
    bt.object_source_label,
    bt.ticket_origin_type,
    
    -- === QUESTION 2: URGENCY/PRIORITY ===
    bt.ticket_priority,
    bt.priority_clean,
    
    -- === QUESTION 3: SPAM FLAG (Not Available) ===
    -- Note: No spam detection fields available in current data
    
    -- === QUESTION 4: SUBJECT LINES ===
    bt.subject_line,
    bt.email_subject,
    
    -- === QUESTION 5: FIRST 4-5 RESPONSES ===
    er.response_1_type,
    er.response_1_timestamp,
    er.response_1_content,
    er.response_1_owner,
    er.response_2_type,
    er.response_2_timestamp,
    er.response_2_content,
    er.response_2_owner,
    er.response_3_type,
    er.response_3_timestamp,
    er.response_3_content,
    er.response_3_owner,
    er.response_4_type,
    er.response_4_timestamp,
    er.response_4_content,
    er.response_4_owner,
    er.response_5_type,
    er.response_5_timestamp,
    er.response_5_content,
    er.response_5_owner,
    
    -- === QUESTION 6 & 8: TIME TO FIRST RESPONSE ===
    bt.first_response_sla_status,
    bt.time_to_first_response_hours,
    bt.first_response_sla_timestamp,
    bt.time_to_first_agent_reply,
    bt.first_agent_reply_date,
    bt.first_response_time_hours,
    
    -- === QUESTION 7: CORAL CARE MEMBER/OWNER ===
    bt.owner_id,
    bt.team_id,
    bt.assigned_team_ids,
    bt.owner_assigned_date,
    
    -- === QUESTION 9: TIME TO TICKET CLOSE ===
    bt.time_to_close,
    bt.time_to_close_hours,
    bt.close_sla_timestamp,
    bt.closed_date,
    bt.last_closed_date,
    bt.time_to_close_hours_calc,
    
    -- === QUESTION 10: SLA STATUS ===
    bt.first_response_sla,
    bt.close_sla_status,
    bt.next_response_sla_status,
    bt.sla_rule_config_id,
    bt.sla_schedule_id,
    -- -- Derived SLA summary
    -- CASE 
    --   WHEN bt.first_response_sla = 'MET' AND bt.close_sla_status = 'MET' THEN 'All SLAs Met'
    --   WHEN bt.first_response_sla = 'BREACHED' OR bt.close_sla_status = 'BREACHED' THEN 'SLA Breached'
    --   WHEN bt.first_response_sla = 'PENDING' OR bt.close_sla_status = 'PENDING' THEN 'SLA Pending'
    --   ELSE 'No SLA Tracking'
    -- END AS overall_sla_status,
    
    -- === QUESTION 11: SUPPORT vs PROVIDER PIPELINE ===
    bt.pipeline,
    bt.pipeline_stage,
    bt.pipeline_type,
    
    -- === ENGAGEMENT SUMMARY METRICS ===
    COALESCE(er.total_first_five_engagements, 0) AS total_first_five_engagements,
    COALESCE(er.email_count_in_first_five, 0) AS email_count_in_first_five,
    COALESCE(er.call_count_in_first_five, 0) AS call_count_in_first_five,
    
    -- === TICKET CLASSIFICATION & STATUS ===
    bt.ticket_category,
    bt.ticket_status,
    bt.in_helpdesk,
    bt.visible_in_help_desk,
    
    -- === RELATIONSHIP METRICS ===
    bt.num_conversations,
    bt.num_companies,
    bt.num_contacts_on_ticket,
    bt.primary_company_id,
    bt.primary_company_name,
    
    -- === KEY TIMESTAMPS ===
    bt.created_date,
    bt.last_modified_date,
    bt.last_activity_date,
    bt.last_contacted_date,
    
    -- === CONTENT & INTERACTION METRICS ===
    bt.ticket_content,
    bt.num_notes,
    bt.num_times_contacted,
    
    -- === ASSIGNMENT & ROUTING ===
    bt.assignment_method,
    bt.created_by,
    
    -- === EMAIL TRACKING ===
    bt.last_email_date,
    bt.last_email_id,
    bt.sales_email_last_replied,
    
    -- === ADVANCED METRICS (Derived) ===
    -- Response pattern analysis
    CASE 
      WHEN er.response_1_type = 'Customer Email' THEN 'Customer Initiated'
      WHEN er.response_1_type IN ('Agent Email', 'Phone Call') THEN 'Agent Initiated'
      ELSE 'Unknown Initiation'
    END AS conversation_initiation_type,
    
    -- Communication complexity
    CASE 
      WHEN COALESCE(er.total_first_five_engagements, 0) = 0 THEN 'No Engagement'
      WHEN COALESCE(er.total_first_five_engagements, 0) = 1 THEN 'Single Touch'
      WHEN COALESCE(er.total_first_five_engagements, 0) <= 3 THEN 'Low Complexity'
      WHEN COALESCE(er.total_first_five_engagements, 0) <= 5 THEN 'High Complexity'
      ELSE 'Very High Complexity'
    END AS communication_complexity,
    
    -- Channel mix in first responses
    CASE 
      WHEN COALESCE(er.email_count_in_first_five, 0) > 0 AND COALESCE(er.call_count_in_first_five, 0) > 0 
        THEN 'Multi-Channel'
      WHEN COALESCE(er.email_count_in_first_five, 0) > 0 
        THEN 'Email Only'
      WHEN COALESCE(er.call_count_in_first_five, 0) > 0 
        THEN 'Call Only'
      ELSE 'No Engagement'
    END AS communication_channel_mix,
    
    -- Response efficiency
    CASE 
      WHEN bt.first_response_time_hours IS NULL THEN 'No Response Time'
      WHEN bt.first_response_time_hours <= 1 THEN 'Very Fast (<1hr)'
      WHEN bt.first_response_time_hours <= 4 THEN 'Fast (<4hrs)'
      WHEN bt.first_response_time_hours <= 24 THEN 'Same Day'
      WHEN bt.first_response_time_hours <= 72 THEN 'Within 3 Days'
      ELSE 'Slow (>3 days)'
    END AS response_speed_category,
    
    -- Resolution efficiency  
    CASE 
      WHEN bt.time_to_close_hours_calc IS NULL THEN 'Not Closed'
      WHEN bt.time_to_close_hours_calc <= 24 THEN 'Same Day Resolution'
      WHEN bt.time_to_close_hours_calc <= 72 THEN 'Within 3 Days'
      WHEN bt.time_to_close_hours_calc <= 168 THEN 'Within 1 Week'
      ELSE 'Long Resolution (>1 week)'
    END AS resolution_speed_category,
    
    -- === METADATA ===
    bt.fivetran_synced,
    bt.is_deleted,
    CURRENT_TIMESTAMP AS mart_created_at
    
  FROM base_tickets bt
  LEFT JOIN engagement_responses er ON bt.ticket_id = er.ticket_id
)

SELECT * FROM final