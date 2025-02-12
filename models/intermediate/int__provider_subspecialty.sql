with base as (
SELECT 
provider.coral_provider_id,
hubspot_provider.qualified_age_groups,
concat(coalesce(hubspot_provider.slp_specialties,' '),coalesce(hubspot_provider.complex_cases_slp,' '),coalesce(hubspot_provider.ot_specialties,' '),coalesce(hubspot_provider.complex_cases_ot,' '),coalesce(hubspot_provider.pt_specialties,' '),coalesce(hubspot_provider.complex_cases_pt,' ')) as total_specialities
from 
{{ref('int__provider')}} provider 
left join 
{{ref('stg__hubspot__contact_provider')}} as hubspot_provider
on provider.hubspot_provider_Id = hubspot_provider.hubspot_provider_id 
) 

select 
coral_provider_id
, case when position('Infants' IN qualified_age_groups) > 0 then True else False end as Infants
, case when position('Toddlers' IN qualified_age_groups) > 0 then True else False end as Toddlers
, case when position('Preschoolers' IN qualified_age_groups) > 0 then True else False end as Preschoolers
, case when position('School-Aged Children' IN qualified_age_groups) > 0 then True else False end as School_Aged_Children
, case when position('Adolescents' IN qualified_age_groups) > 0 then True else False end as Adolescents

-- SLP Specialties
, case when position('Articulation and Phonological Disorders' IN total_specialities) > 0 then True else False end as Articulation_and_Phonological_Disorders
, case when position('Augmentative and Alternative Communication (AAC)' IN total_specialities) > 0 then True else False end as Augmentative_and_Alternative_Communication_AAC
, case when position('Autism Spectrum Disorder (ASD)' IN total_specialities) > 0 then True else False end as Autism_Spectrum_Disorder_ASD
, case when 
   (position('Feeding and Swallowing Disorders' IN total_specialities) > 0
    OR position('Feeding and Swallowing' IN total_specialities) > 0 )
    then True else False end as Feeding_and_Swallowing_Disorders
, case when position('Early Childhood Language Development' IN total_specialities) > 0 then True else False end as Early_Childhood_Language_Development
, case when position('Language Delay and Disorders' IN total_specialities) > 0 then True else False end as Language_Delay_and_Disorders
, case when position('Receptive and Expressive Language' IN total_specialities) > 0 then True else False end as Receptive_and_Expressive_Language

-- OT Specialties
-- ASD is already captured in SLP
, case when position('Complex Cases' IN total_specialities) > 0 then True else False end as Complex_Cases
, case when position('Delayed Potty Training' IN total_specialities) > 0 then True else False end as Delayed_Potty_Training
, case when position('Developmental Delays' IN total_specialities) > 0 then True else False end as Developmental_Delays
, case when position('Executive Functioning' IN total_specialities) > 0 then True else False end as Executive_Functioning
, case when position('Fine Motor Skills Development' IN total_specialities) > 0 then True else False end as Fine_Motor_Skills_Development
, case when position('Handwriting and Coordination' IN total_specialities) > 0 then True else False end as Handwriting_and_Coordination
, case when position('Motor Planning and Praxis' IN total_specialities) > 0 then True else False end as Motor_Planning_and_Praxis
, case when position('Neurodiversity Affirming Care' IN total_specialities) > 0 then True else False end as Neurodiversity_Affirming_Care
, case when position('Play-Based Therapy' IN total_specialities) > 0 then True else False end as Play_Based_Therapy
, case when position('Self Care/Daily Living Skills' IN total_specialities) > 0 then True else False end as Self_Care_Daily_Living_Skills
, case when position('Sensory Integration Therapy' IN total_specialities) > 0 then True else False end as Sensory_Integration_Therapy
, case when position('Social Skills Training' IN total_specialities) > 0 then True else False end as Social_Skills_Training

--PT Specialties

, case when position('Cerebral Palsy' IN total_specialities) > 0 then True else False end as Cerebral_Palsy
, case when position('Cerebral Visual Impairment (CVI)' IN total_specialities) > 0 then True else False end as Cerebral_Visual_Impairment_CVI
, case when position('Gait and Balance Training' IN total_specialities) > 0 then True else False end as Gait_and_Balance_Training
, case when position('Gross Motor Skills Development' IN total_specialities) > 0 then True else False end as Gross_Motor_Skills_Development
, case when position('Musculoskeletal Disorders' IN total_specialities) > 0 then True else False end as Musculoskeletal_Disorders
, case when position('Post-Surgical Rehabilitation' IN total_specialities) > 0 then True else False end as Post_Surgical_Rehabilitation
, case when position('Torticollis' IN total_specialities) > 0 then True else False end as Torticollis
from 
base 