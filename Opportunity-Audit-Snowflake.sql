UPDATE MARKETINGOPERATIONS.DM_CORE.dm_core_fact_opportunity_dev

SET
dm_core_fact_opportunity_dev.f_Ad_f_IsAuditIssueAccepted = 1
FROM MARKETINGOPERATIONS.REF_TABLES.rh_temp_ref_2017Q2_audit_validated


WHERE MARKETINGOPERATIONS.DM_CORE.dm_core_fact_opportunity_dev.id_primary_Opportunity = MARKETINGOPERATIONS.REF_TABLES.rh_temp_ref_2017Q2_audit_validated.Opp_ID
;


USE WAREHOUSE MARKETINGOPERATIONS;



----North America

SELECT MARKETINGOPERATIONS.DM_CORE.dm_core_fact_opportunity_dev.id_primary_Opportunity
  ,MARKETINGOPERATIONS.DM_CORE.dm_core_dim_accountlead_dev.DIM_STATEPROVINCE
  ,MARKETINGOPERATIONS.DM_CORE.dm_core_dim_opportunity_dev.dim_geo_OpportunitySubSubRegion
  ,MARKETINGOPERATIONS.DM_CORE.dm_core_dim_opportunity_dev.dim_geo_OpportunitySubRegion
  ,MARKETINGOPERATIONS.DM_CORE.dm_core_dim_opportunity_dev.dim_geo_OpportunityCountry
  ,MARKETINGOPERATIONS.DM_CORE.DM_CORE_DIM_ACCOUNTLEAD_DEV.DIM_GEO_ACCOUNTSUBREGION AS TableauSubRegion
  ,dm_core_dim_opportunity_dev.dim_OpportunityLDRName
  ,dm_core_fact_opportunity_dev.DATE_OPPCREATED_DATE
  ,dm_core_fact_opportunity_dev.date_OppClosedDate
  ,dm_core_dim_opportunity_dev.dim_OpportunityNumberofAgents
  ,dm_core_dim_opportunity_dev.dim_OpportunityOwnerSegment AS TableauOpportunityOwnerSegment
  ,dm_core_dim_opportunity_dev.dim_OpportunityCreatedByName
  ,dm_core_dim_opportunity_dev.dim_OpportunityOwnerName
  ,dm_core_dim_opportunity_dev.dim_OpportunityOwnerRoleName
  ,dm_core_dim_opportunity_dev.dim_OpportunityName
  ,eopp.CONVERTEDOPPCREATEDBYMAORLDR
  ,elc.CONVERTEDPRIMARYCONTACTCREATEDBYMAORLDR
  ,MARKETINGOPERATIONS.DM_CORE.dm_core_dim_opportunity_dev.DIM_OPPORTUNITYSTAGE_NAME
  ,CONCAT('https://genesys.my.salesforce.com/',dm_core_fact_opportunity_dev.id_primary_Opportunity) AS URL
  ,CASE WHEN dm_core_fact_opportunity_dev.f_Ad_f_IsAuditIssueAccepted = 1 THEN 0
    ELSE GREATEST(dm_core_fact_opportunity_dev.f_Ad_01_MissingPrimary
                  ,dm_core_fact_opportunity_dev.f_Ad_02_BANTIssue
                  ,dm_core_fact_opportunity_dev.f_Ad_03_LDRnoCampaign
                  ,dm_core_fact_opportunity_dev.f_Ad_04_CampaignnoLDR
                  ,dm_core_fact_opportunity_dev.f_Ad_08_BadOwner
                  ,dm_core_fact_opportunity_dev.f_Ad_09_LDROwned
                  ,CASE WHEN dm_core_fact_opportunity_dev.f_Ad_17_ClaimedOpp = 1
                              AND dm_core_dim_opportunity_dev.dim_OpportunityCreatedByName LIKE '%Integration%' THEN 0 ELSE dm_core_fact_opportunity_dev.f_Ad_17_ClaimedOpp END) END AS INVALID_OPP
  ,dm_core_fact_opportunity_dev.f_Ad_01_MissingPrimary AS Missing_Primary_Contact
  ,dm_core_fact_opportunity_dev.f_Ad_02_BANTIssue AS BANT_Issue
  ,dm_core_fact_opportunity_dev.f_Ad_03_LDRnoCampaign AS No_Campaign_Source
  ,dm_core_fact_opportunity_dev.f_Ad_04_CampaignnoLDR AS No_Journey_Team_on_Opportunity
  ,dm_core_fact_opportunity_dev.f_Ad_08_BadOwner AS NonSales_Owner

  ,CASE WHEN dm_core_fact_opportunity_dev.f_Ad_17_ClaimedOpp = 1
                              AND dm_core_dim_opportunity_dev.dim_OpportunityCreatedByName LIKE '%Integration%' THEN 0 ELSE dm_core_fact_opportunity_dev.f_Ad_17_ClaimedOpp END AS Not_Created_by_Journey_Team
  ,dm_core_fact_opportunity_dev.f_Ad_f_IsAuditIssueAccepted AS Exception_Accepted
  ,dm_core_dim_opportunity_dev.DIM_OPPORTUNITYWINLOSS
  ,dm_core_dim_opportunity_dev.dim_OpportunityBusinessType
  ,dm_core_dim_opportunity_dev.dim_PlatformType
  ,dm_core_dim_opportunity_dev.dim_OfferingsType
  ,dm_core_fact_opportunity_dev.date_OppPipelineAddedDate
  ,dm_core_fact_opportunity_dev.m_AmountOfConvertedOppLicenseEquivalency
  ,dm_core_fact_opportunity_dev.m_AmountOfConvertedOppLicenseAmount
  ,dm_core_fact_opportunity_dev.m_AmountOfConvertedOppACV
  ,dm_core_fact_opportunity_dev.m_AmountOfConvertedOppTCV
  ,dm_core_fact_opportunity_dev.m_AmountOfConvertedOppTermACV
  ,dm_core_dim_accountlead_dev.dim_AccountName
  ,dm_core_fact_opportunity_dev.id_Account
  ,CONCAT('https://genesys.my.salesforce.com/',dm_core_fact_opportunity_dev.id_Account) AS ACCOUNT_URL
  ,dm_core_dim_opportunity_dev.dim_geo_OpportunityRegion

--  ,dm_core_fact_opportunity_dev.m_IsPipelineValidation
--  ,dm_core_fact_opportunity_dev.m_IsBookingValidation
--  ,dm_core_fact_opportunity_dev.m_IsValidForReporting
FROM MARKETINGOPERATIONS.DM_CORE.dm_core_fact_opportunity_dev
  JOIN MARKETINGOPERATIONS.DM_CORE.dm_core_dim_opportunity_dev
    ON MARKETINGOPERATIONS.DM_CORE.dm_core_fact_opportunity_dev.id_primary_Opportunity = MARKETINGOPERATIONS.DM_CORE.dm_core_dim_opportunity_dev.id_primary_Opportunity
  LEFT JOIN MARKETINGOPERATIONS.DM_CALC.V_EXT_OPPORTUNITY eopp
  ON MARKETINGOPERATIONS.DM_CORE.DM_CORE_FACT_OPPORTUNITY_DEV.ID_PRIMARY_OPPORTUNITY = eopp.ID
  LEFT JOIN MARKETINGOPERATIONS.DM_CORE.dm_core_dim_accountlead_dev
    ON MARKETINGOPERATIONS.DM_CORE.dm_core_fact_opportunity_dev.id_AccountLead = MARKETINGOPERATIONS.DM_CORE.dm_core_dim_accountlead_dev.id_primary_AccountLead
  LEFT JOIN MARKETINGOPERATIONS.DM_CALC.V_EXT_LEADCONTACT elc
  ON MARKETINGOPERATIONS.DM_CORE.DM_CORE_DIM_ACCOUNTLEAD_DEV.ID_PRIMARY_ACCOUNTLEAD = elc.ACCOUNTLEAD_ID
  LEFT JOIN MARKETINGOPERATIONS.SFDC.OPPORTUNITY
    ON MARKETINGOPERATIONS.DM_CORE.dm_core_fact_opportunity_dev.id_primary_Opportunity = MARKETINGOPERATIONS.SFDC.OPPORTUNITY.Id
  LEFT JOIN MARKETINGOPERATIONS.SFDC.USER as OppJER
    ON MARKETINGOPERATIONS.SFDC.OPPORTUNITY.JOURNEY_ENGAGEMENT_REP_C = OppJER.Id
  LEFT JOIN MARKETINGOPERATIONS.REF_TABLES.ML_DM_REF_USERTYPE
    ON CASE WHEN MARKETINGOPERATIONS.DM_CORE.dm_core_dim_opportunity_dev.id_OpportunityLDR IS NOT NULL
    THEN MARKETINGOPERATIONS.DM_CORE.dm_core_dim_opportunity_dev.id_OpportunityLDR
       ELSE MARKETINGOPERATIONS.SFDC.OPPORTUNITY.JOURNEY_ENGAGEMENT_REP_C END = MARKETINGOPERATIONS.REF_TABLES.ML_DM_REF_USERTYPE.ID
WHERE dm_core_fact_opportunity_dev.DATE_OPPCREATED_DATE >= '2018-02-19'
  AND dm_core_fact_opportunity_dev.m_IsMarketingSourced = 1
  AND dm_core_dim_opportunity_dev.dim_geo_OpportunityRegion = 'North America'
  AND dm_core_dim_opportunity_dev.dim_OpportunityBusinessType = 'New logo'
  AND dm_core_fact_opportunity_dev.m_IsValidForReporting = 1
  AND dm_core_fact_opportunity_dev.m_AmountOfConvertedOppLicenseEquivalency > 0.000000

;


----EMEA

SELECT dm_core_fact_opportunity_dev.id_primary_Opportunity
  ,MARKETINGOPERATIONS.DM_CORE.dm_core_dim_accountlead_dev.DIM_STATEPROVINCE
  ,dm_core_dim_opportunity_dev.dim_geo_OpportunitySubSubRegion
  ,dm_core_dim_opportunity_dev.dim_geo_OpportunitySubRegion
  ,dm_core_dim_opportunity_dev.dim_geo_OpportunityCountry
  ,MARKETINGOPERATIONS.DM_CORE.DM_CORE_DIM_ACCOUNTLEAD_DEV.DIM_GEO_ACCOUNTSUBREGION AS TableauSubRegion
  ,dm_core_dim_opportunity_dev.dim_OpportunityLDRName
  ,dm_core_fact_opportunity_dev.DATE_OPPCREATED_DATE
  ,dm_core_fact_opportunity_dev.date_OppClosedDate
  ,dm_core_dim_opportunity_dev.dim_OpportunityNumberofAgents
  ,dm_core_dim_opportunity_dev.dim_OpportunityOwnerSegment AS TableauOpportunityOwnerSegment
  ,dm_core_dim_opportunity_dev.dim_OpportunityCreatedByName
  ,dm_core_dim_opportunity_dev.dim_OpportunityOwnerName
  ,dm_core_dim_opportunity_dev.dim_OpportunityOwnerRoleName
  ,dm_core_dim_opportunity_dev.dim_OpportunityName
  ,eopp.CONVERTEDOPPCREATEDBYMAORLDR
  ,elc.CONVERTEDPRIMARYCONTACTCREATEDBYMAORLDR
  ,MARKETINGOPERATIONS.DM_CORE.dm_core_dim_opportunity_dev.DIM_OPPORTUNITYSTAGE_NAME
  ,CONCAT('https://genesys.my.salesforce.com/',dm_core_fact_opportunity_dev.id_primary_Opportunity) AS URL
  ,CASE WHEN dm_core_fact_opportunity_dev.f_Ad_f_IsAuditIssueAccepted = 1 THEN 0
    ELSE GREATEST(dm_core_fact_opportunity_dev.f_Ad_01_MissingPrimary
                  ,dm_core_fact_opportunity_dev.f_Ad_02_BANTIssue
                  ,dm_core_fact_opportunity_dev.f_Ad_03_LDRnoCampaign
                  ,dm_core_fact_opportunity_dev.f_Ad_04_CampaignnoLDR
                  ,dm_core_fact_opportunity_dev.f_Ad_08_BadOwner
                  ,dm_core_fact_opportunity_dev.f_Ad_09_LDROwned
                  ,CASE WHEN dm_core_fact_opportunity_dev.f_Ad_17_ClaimedOpp = 1
                              AND dm_core_dim_opportunity_dev.dim_OpportunityCreatedByName LIKE '%Integration%' THEN 0 ELSE dm_core_fact_opportunity_dev.f_Ad_17_ClaimedOpp END) END AS INVALID_OPP
  ,dm_core_fact_opportunity_dev.f_Ad_01_MissingPrimary AS Missing_Primary_Contact
  ,dm_core_fact_opportunity_dev.f_Ad_02_BANTIssue AS BANT_Issue
  ,dm_core_fact_opportunity_dev.f_Ad_03_LDRnoCampaign AS No_Campaign_Source
  ,dm_core_fact_opportunity_dev.f_Ad_04_CampaignnoLDR AS No_Journey_Team_on_Opportunity
  ,dm_core_fact_opportunity_dev.f_Ad_08_BadOwner AS NonSales_Owner
  ,CASE WHEN dm_core_fact_opportunity_dev.f_Ad_17_ClaimedOpp = 1
                              AND dm_core_dim_opportunity_dev.dim_OpportunityCreatedByName LIKE '%Integration%' THEN 0 ELSE dm_core_fact_opportunity_dev.f_Ad_17_ClaimedOpp END AS Not_Created_by_Journey_Team
  ,dm_core_fact_opportunity_dev.f_Ad_f_IsAuditIssueAccepted AS Exception_Accepted
  ,dm_core_dim_opportunity_dev.DIM_OPPORTUNITYWINLOSS
  ,dm_core_dim_opportunity_dev.dim_OpportunityBusinessType
  ,dm_core_dim_opportunity_dev.dim_PlatformType
  ,dm_core_dim_opportunity_dev.dim_OfferingsType
  ,dm_core_fact_opportunity_dev.date_OppPipelineAddedDate
  ,dm_core_fact_opportunity_dev.m_AmountOfConvertedOppLicenseEquivalency
  ,dm_core_fact_opportunity_dev.m_AmountOfConvertedOppLicenseAmount
  ,dm_core_fact_opportunity_dev.m_AmountOfConvertedOppACV
  ,dm_core_fact_opportunity_dev.m_AmountOfConvertedOppTCV
  ,dm_core_fact_opportunity_dev.m_AmountOfConvertedOppTermACV
  ,dm_core_dim_accountlead_dev.dim_AccountName
  ,dm_core_fact_opportunity_dev.id_Account
  ,CONCAT('https://genesys.my.salesforce.com/',dm_core_fact_opportunity_dev.id_Account) AS ACCOUNT_URL
  ,dm_core_dim_opportunity_dev.dim_geo_OpportunityRegion

----  ,dm_core_fact_opportunity_dev.m_IsPipelineValidation
----  ,dm_core_fact_opportunity_dev.m_IsBookingValidation
----  ,dm_core_fact_opportunity_dev.m_IsValidForReporting
FROM MARKETINGOPERATIONS.DM_CORE.dm_core_fact_opportunity_dev
  JOIN MARKETINGOPERATIONS.DM_CORE.dm_core_dim_opportunity_dev
    ON MARKETINGOPERATIONS.DM_CORE.dm_core_fact_opportunity_dev.id_primary_Opportunity = MARKETINGOPERATIONS.DM_CORE.dm_core_dim_opportunity_dev.id_primary_Opportunity
    LEFT JOIN MARKETINGOPERATIONS.DM_CALC.V_EXT_OPPORTUNITY eopp
  ON MARKETINGOPERATIONS.DM_CORE.DM_CORE_FACT_OPPORTUNITY_DEV.ID_PRIMARY_OPPORTUNITY = eopp.ID
  LEFT JOIN MARKETINGOPERATIONS.DM_CORE.dm_core_dim_accountlead_dev
    ON MARKETINGOPERATIONS.DM_CORE.dm_core_fact_opportunity_dev.id_AccountLead = MARKETINGOPERATIONS.DM_CORE.dm_core_dim_accountlead_dev.id_primary_AccountLead
  LEFT JOIN MARKETINGOPERATIONS.DM_CALC.V_EXT_LEADCONTACT elc
  ON MARKETINGOPERATIONS.DM_CORE.DM_CORE_DIM_ACCOUNTLEAD_DEV.ID_PRIMARY_ACCOUNTLEAD = elc.ACCOUNTLEAD_ID
  LEFT JOIN MARKETINGOPERATIONS.SFDC.OPPORTUNITY
    ON MARKETINGOPERATIONS.DM_CORE.dm_core_fact_opportunity_dev.id_primary_Opportunity = MARKETINGOPERATIONS.SFDC.OPPORTUNITY.Id
  LEFT JOIN MARKETINGOPERATIONS.SFDC.USER as OppJER
    ON MARKETINGOPERATIONS.SFDC.OPPORTUNITY.JOURNEY_ENGAGEMENT_REP_C = OppJER.Id
  LEFT JOIN MARKETINGOPERATIONS.REF_TABLES.ML_DM_REF_USERTYPE
    ON CASE WHEN MARKETINGOPERATIONS.DM_CORE.dm_core_dim_opportunity_dev.id_OpportunityLDR IS NOT NULL
    THEN MARKETINGOPERATIONS.DM_CORE.dm_core_dim_opportunity_dev.id_OpportunityLDR
       ELSE MARKETINGOPERATIONS.SFDC.OPPORTUNITY.JOURNEY_ENGAGEMENT_REP_C END = MARKETINGOPERATIONS.REF_TABLES.ML_DM_REF_USERTYPE.ID
WHERE dm_core_fact_opportunity_dev.DATE_OPPCREATED_DATE >= '2018-02-19'
  AND dm_core_fact_opportunity_dev.m_IsMarketingSourced = 1
  AND dm_core_dim_opportunity_dev.dim_geo_OpportunityRegion = 'EMEA'
  AND dm_core_dim_opportunity_dev.dim_OpportunityBusinessType = 'New logo'
  AND dm_core_fact_opportunity_dev.m_IsValidForReporting = 1
  AND dm_core_fact_opportunity_dev.m_AmountOfConvertedOppLicenseEquivalency > 0.000000
;

----LATAM

SELECT dm_core_fact_opportunity_dev.id_primary_Opportunity
  ,MARKETINGOPERATIONS.DM_CORE.dm_core_dim_accountlead_dev.DIM_STATEPROVINCE
  ,dm_core_dim_opportunity_dev.dim_geo_OpportunitySubSubRegion
  ,dm_core_dim_opportunity_dev.dim_geo_OpportunitySubRegion
  ,dm_core_dim_opportunity_dev.dim_geo_OpportunityCountry
  ,MARKETINGOPERATIONS.DM_CORE.DM_CORE_DIM_ACCOUNTLEAD_DEV.DIM_GEO_ACCOUNTSUBREGION AS TableauSubRegion
  ,dm_core_dim_opportunity_dev.dim_OpportunityLDRName
  ,dm_core_fact_opportunity_dev.DATE_OPPCREATED_DATE
  ,dm_core_fact_opportunity_dev.date_OppClosedDate
  ,dm_core_dim_opportunity_dev.dim_OpportunityNumberofAgents
  ,dm_core_dim_opportunity_dev.dim_OpportunityOwnerSegment AS TableauOpportunityOwnerSegment
  ,dm_core_dim_opportunity_dev.dim_OpportunityCreatedByName
  ,dm_core_dim_opportunity_dev.dim_OpportunityOwnerName
  ,dm_core_dim_opportunity_dev.dim_OpportunityOwnerRoleName
  ,dm_core_dim_opportunity_dev.dim_OpportunityName
  ,eopp.CONVERTEDOPPCREATEDBYMAORLDR
  ,elc.CONVERTEDPRIMARYCONTACTCREATEDBYMAORLDR
  ,MARKETINGOPERATIONS.DM_CORE.dm_core_dim_opportunity_dev.DIM_OPPORTUNITYSTAGE_NAME
  ,CONCAT('https://genesys.my.salesforce.com/',dm_core_fact_opportunity_dev.id_primary_Opportunity) AS URL
  ,CASE WHEN dm_core_fact_opportunity_dev.f_Ad_f_IsAuditIssueAccepted = 1 THEN 0
    ELSE GREATEST(dm_core_fact_opportunity_dev.f_Ad_01_MissingPrimary
                  ,dm_core_fact_opportunity_dev.f_Ad_02_BANTIssue
                  ,dm_core_fact_opportunity_dev.f_Ad_03_LDRnoCampaign
                  ,dm_core_fact_opportunity_dev.f_Ad_04_CampaignnoLDR
                  ,dm_core_fact_opportunity_dev.f_Ad_08_BadOwner
                  ,dm_core_fact_opportunity_dev.f_Ad_09_LDROwned
                  ,CASE WHEN dm_core_fact_opportunity_dev.f_Ad_17_ClaimedOpp = 1
                              AND dm_core_dim_opportunity_dev.dim_OpportunityCreatedByName LIKE '%Integration%' THEN 0 ELSE dm_core_fact_opportunity_dev.f_Ad_17_ClaimedOpp END) END AS INVALID_OPP
  ,dm_core_fact_opportunity_dev.f_Ad_01_MissingPrimary AS Missing_Primary_Contact
  ,dm_core_fact_opportunity_dev.f_Ad_02_BANTIssue AS BANT_Issue
  ,dm_core_fact_opportunity_dev.f_Ad_03_LDRnoCampaign AS No_Campaign_Source
  ,dm_core_fact_opportunity_dev.f_Ad_04_CampaignnoLDR AS No_Journey_Team_on_Opportunity
  ,dm_core_fact_opportunity_dev.f_Ad_08_BadOwner AS NonSales_Owner

  ,CASE WHEN dm_core_fact_opportunity_dev.f_Ad_17_ClaimedOpp = 1
                              AND dm_core_dim_opportunity_dev.dim_OpportunityCreatedByName LIKE '%Integration%' THEN 0 ELSE dm_core_fact_opportunity_dev.f_Ad_17_ClaimedOpp END AS Not_Created_by_Journey_Team
  ,dm_core_fact_opportunity_dev.f_Ad_f_IsAuditIssueAccepted AS Exception_Accepted
  ,dm_core_dim_opportunity_dev.DIM_OPPORTUNITYWINLOSS
  ,dm_core_dim_opportunity_dev.dim_OpportunityBusinessType
  ,dm_core_dim_opportunity_dev.dim_PlatformType
  ,dm_core_dim_opportunity_dev.dim_OfferingsType
  ,dm_core_fact_opportunity_dev.date_OppPipelineAddedDate
  ,dm_core_fact_opportunity_dev.m_AmountOfConvertedOppLicenseEquivalency
  ,dm_core_fact_opportunity_dev.m_AmountOfConvertedOppLicenseAmount
  ,dm_core_fact_opportunity_dev.m_AmountOfConvertedOppACV
  ,dm_core_fact_opportunity_dev.m_AmountOfConvertedOppTCV
  ,dm_core_fact_opportunity_dev.m_AmountOfConvertedOppTermACV
  ,dm_core_dim_accountlead_dev.dim_AccountName
  ,dm_core_fact_opportunity_dev.id_Account
  ,CONCAT('https://genesys.my.salesforce.com/',dm_core_fact_opportunity_dev.id_Account) AS ACCOUNT_URL
  ,dm_core_dim_opportunity_dev.dim_geo_OpportunityRegion

----  ,dm_core_fact_opportunity_dev.m_IsPipelineValidation
----  ,dm_core_fact_opportunity_dev.m_IsBookingValidation
----  ,dm_core_fact_opportunity_dev.m_IsValidForReporting
FROM MARKETINGOPERATIONS.DM_CORE.dm_core_fact_opportunity_dev
  JOIN MARKETINGOPERATIONS.DM_CORE.dm_core_dim_opportunity_dev
    ON MARKETINGOPERATIONS.DM_CORE.dm_core_fact_opportunity_dev.id_primary_Opportunity = MARKETINGOPERATIONS.DM_CORE.dm_core_dim_opportunity_dev.id_primary_Opportunity
    LEFT JOIN MARKETINGOPERATIONS.DM_CALC.V_EXT_OPPORTUNITY eopp
  ON MARKETINGOPERATIONS.DM_CORE.DM_CORE_FACT_OPPORTUNITY_DEV.ID_PRIMARY_OPPORTUNITY = eopp.ID
  LEFT JOIN MARKETINGOPERATIONS.DM_CORE.dm_core_dim_accountlead_dev
    ON MARKETINGOPERATIONS.DM_CORE.dm_core_fact_opportunity_dev.id_AccountLead = MARKETINGOPERATIONS.DM_CORE.dm_core_dim_accountlead_dev.id_primary_AccountLead
  LEFT JOIN MARKETINGOPERATIONS.DM_CALC.V_EXT_LEADCONTACT elc
  ON MARKETINGOPERATIONS.DM_CORE.DM_CORE_DIM_ACCOUNTLEAD_DEV.ID_PRIMARY_ACCOUNTLEAD = elc.ACCOUNTLEAD_ID
  LEFT JOIN MARKETINGOPERATIONS.SFDC.OPPORTUNITY
    ON MARKETINGOPERATIONS.DM_CORE.dm_core_fact_opportunity_dev.id_primary_Opportunity = MARKETINGOPERATIONS.SFDC.OPPORTUNITY.Id
  LEFT JOIN MARKETINGOPERATIONS.SFDC.USER as OppJER
    ON MARKETINGOPERATIONS.SFDC.OPPORTUNITY.JOURNEY_ENGAGEMENT_REP_C = OppJER.Id
  LEFT JOIN MARKETINGOPERATIONS.REF_TABLES.ML_DM_REF_USERTYPE
    ON CASE WHEN MARKETINGOPERATIONS.DM_CORE.dm_core_dim_opportunity_dev.id_OpportunityLDR IS NOT NULL
    THEN MARKETINGOPERATIONS.DM_CORE.dm_core_dim_opportunity_dev.id_OpportunityLDR
       ELSE MARKETINGOPERATIONS.SFDC.OPPORTUNITY.JOURNEY_ENGAGEMENT_REP_C END = MARKETINGOPERATIONS.REF_TABLES.ML_DM_REF_USERTYPE.ID
WHERE dm_core_fact_opportunity_dev.DATE_OPPCREATED_DATE >= '2018-02-19'
  AND dm_core_fact_opportunity_dev.m_IsMarketingSourced = 1
  AND dm_core_dim_opportunity_dev.dim_geo_OpportunityRegion = 'LATAM'
  AND dm_core_dim_opportunity_dev.dim_OpportunityBusinessType = 'New logo'
  AND dm_core_fact_opportunity_dev.m_IsValidForReporting = 1
  AND dm_core_fact_opportunity_dev.m_AmountOfConvertedOppLicenseEquivalency > 0.000000
;

----APAC

SELECT dm_core_fact_opportunity_dev.id_primary_Opportunity
  ,MARKETINGOPERATIONS.DM_CORE.dm_core_dim_accountlead_dev.DIM_STATEPROVINCE
  ,dm_core_dim_opportunity_dev.dim_geo_OpportunitySubSubRegion
  ,dm_core_dim_opportunity_dev.dim_geo_OpportunitySubRegion
  ,dm_core_dim_opportunity_dev.dim_geo_OpportunityCountry
  ,MARKETINGOPERATIONS.DM_CORE.DM_CORE_DIM_ACCOUNTLEAD_DEV.DIM_GEO_ACCOUNTSUBREGION AS TableauSubRegion
  ,dm_core_dim_opportunity_dev.dim_OpportunityLDRName
  ,dm_core_fact_opportunity_dev.DATE_OPPCREATED_DATE
  ,dm_core_fact_opportunity_dev.date_OppClosedDate
  ,dm_core_dim_opportunity_dev.dim_OpportunityNumberofAgents
  ,dm_core_dim_opportunity_dev.dim_OpportunityOwnerSegment AS TableauOpportunityOwnerSegment
  ,dm_core_dim_opportunity_dev.dim_OpportunityCreatedByName
  ,dm_core_dim_opportunity_dev.dim_OpportunityOwnerName
  ,dm_core_dim_opportunity_dev.dim_OpportunityOwnerRoleName
  ,dm_core_dim_opportunity_dev.dim_OpportunityName
  ,eopp.CONVERTEDOPPCREATEDBYMAORLDR
  ,elc.CONVERTEDPRIMARYCONTACTCREATEDBYMAORLDR
  ,MARKETINGOPERATIONS.DM_CORE.dm_core_dim_opportunity_dev.DIM_OPPORTUNITYSTAGE_NAME
  ,CONCAT('https://genesys.my.salesforce.com/',dm_core_fact_opportunity_dev.id_primary_Opportunity) AS URL
  ,CASE WHEN dm_core_fact_opportunity_dev.f_Ad_f_IsAuditIssueAccepted = 1 THEN 0
    ELSE GREATEST(dm_core_fact_opportunity_dev.f_Ad_01_MissingPrimary
                  ,dm_core_fact_opportunity_dev.f_Ad_02_BANTIssue
                  ,dm_core_fact_opportunity_dev.f_Ad_03_LDRnoCampaign
                  ,dm_core_fact_opportunity_dev.f_Ad_04_CampaignnoLDR
                  ,dm_core_fact_opportunity_dev.f_Ad_08_BadOwner
                  ,dm_core_fact_opportunity_dev.f_Ad_09_LDROwned
                  ,CASE WHEN dm_core_fact_opportunity_dev.f_Ad_17_ClaimedOpp = 1
                              AND dm_core_dim_opportunity_dev.dim_OpportunityCreatedByName LIKE '%Integration%' THEN 0 ELSE dm_core_fact_opportunity_dev.f_Ad_17_ClaimedOpp END) END AS INVALID_OPP
  ,dm_core_fact_opportunity_dev.f_Ad_01_MissingPrimary AS Missing_Primary_Contact
  ,dm_core_fact_opportunity_dev.f_Ad_02_BANTIssue AS BANT_Issue
  ,dm_core_fact_opportunity_dev.f_Ad_03_LDRnoCampaign AS No_Campaign_Source
  ,dm_core_fact_opportunity_dev.f_Ad_04_CampaignnoLDR AS No_Journey_Team_on_Opportunity
  ,dm_core_fact_opportunity_dev.f_Ad_08_BadOwner AS NonSales_Owner

  ,CASE WHEN dm_core_fact_opportunity_dev.f_Ad_17_ClaimedOpp = 1
                              AND dm_core_dim_opportunity_dev.dim_OpportunityCreatedByName LIKE '%Integration%' THEN 0 ELSE dm_core_fact_opportunity_dev.f_Ad_17_ClaimedOpp END AS Not_Created_by_Journey_Team
  ,dm_core_fact_opportunity_dev.f_Ad_f_IsAuditIssueAccepted AS Exception_Accepted
  ,dm_core_dim_opportunity_dev.DIM_OPPORTUNITYWINLOSS
  ,dm_core_dim_opportunity_dev.dim_OpportunityBusinessType
  ,dm_core_dim_opportunity_dev.dim_PlatformType
  ,dm_core_dim_opportunity_dev.dim_OfferingsType
  ,dm_core_fact_opportunity_dev.date_OppPipelineAddedDate
  ,dm_core_fact_opportunity_dev.m_AmountOfConvertedOppLicenseEquivalency
  ,dm_core_fact_opportunity_dev.m_AmountOfConvertedOppLicenseAmount
  ,dm_core_fact_opportunity_dev.m_AmountOfConvertedOppACV
  ,dm_core_fact_opportunity_dev.m_AmountOfConvertedOppTCV
  ,dm_core_fact_opportunity_dev.m_AmountOfConvertedOppTermACV
  ,dm_core_dim_accountlead_dev.dim_AccountName
  ,dm_core_fact_opportunity_dev.id_Account
  ,CONCAT('https://genesys.my.salesforce.com/',dm_core_fact_opportunity_dev.id_Account) AS ACCOUNT_URL
  ,dm_core_dim_opportunity_dev.dim_geo_OpportunityRegion

----  ,dm_core_fact_opportunity_dev.m_IsPipelineValidation
----  ,dm_core_fact_opportunity_dev.m_IsBookingValidation
----  ,dm_core_fact_opportunity_dev.m_IsValidForReporting
FROM MARKETINGOPERATIONS.DM_CORE.dm_core_fact_opportunity_dev
  JOIN MARKETINGOPERATIONS.DM_CORE.dm_core_dim_opportunity_dev
    ON MARKETINGOPERATIONS.DM_CORE.dm_core_fact_opportunity_dev.id_primary_Opportunity = MARKETINGOPERATIONS.DM_CORE.dm_core_dim_opportunity_dev.id_primary_Opportunity
    LEFT JOIN MARKETINGOPERATIONS.DM_CALC.V_EXT_OPPORTUNITY eopp
  ON MARKETINGOPERATIONS.DM_CORE.DM_CORE_FACT_OPPORTUNITY_DEV.ID_PRIMARY_OPPORTUNITY = eopp.ID
  LEFT JOIN MARKETINGOPERATIONS.DM_CORE.dm_core_dim_accountlead_dev
    ON MARKETINGOPERATIONS.DM_CORE.dm_core_fact_opportunity_dev.id_AccountLead = MARKETINGOPERATIONS.DM_CORE.dm_core_dim_accountlead_dev.id_primary_AccountLead
  LEFT JOIN MARKETINGOPERATIONS.DM_CALC.V_EXT_LEADCONTACT elc
  ON MARKETINGOPERATIONS.DM_CORE.DM_CORE_DIM_ACCOUNTLEAD_DEV.ID_PRIMARY_ACCOUNTLEAD = elc.ACCOUNTLEAD_ID
  LEFT JOIN MARKETINGOPERATIONS.SFDC.OPPORTUNITY
    ON MARKETINGOPERATIONS.DM_CORE.dm_core_fact_opportunity_dev.id_primary_Opportunity = MARKETINGOPERATIONS.SFDC.OPPORTUNITY.Id
  LEFT JOIN MARKETINGOPERATIONS.SFDC.USER as OppJER
    ON MARKETINGOPERATIONS.SFDC.OPPORTUNITY.JOURNEY_ENGAGEMENT_REP_C = OppJER.Id
  LEFT JOIN MARKETINGOPERATIONS.REF_TABLES.ML_DM_REF_USERTYPE
    ON CASE WHEN MARKETINGOPERATIONS.DM_CORE.dm_core_dim_opportunity_dev.id_OpportunityLDR IS NOT NULL
    THEN MARKETINGOPERATIONS.DM_CORE.dm_core_dim_opportunity_dev.id_OpportunityLDR
       ELSE MARKETINGOPERATIONS.SFDC.OPPORTUNITY.JOURNEY_ENGAGEMENT_REP_C END = MARKETINGOPERATIONS.REF_TABLES.ML_DM_REF_USERTYPE.ID
WHERE dm_core_fact_opportunity_dev.DATE_OPPCREATED_DATE >= '2018-02-19'
  AND dm_core_fact_opportunity_dev.m_IsMarketingSourced = 1
  AND dm_core_dim_opportunity_dev.dim_geo_OpportunityRegion = 'APAC'
  AND dm_core_dim_opportunity_dev.dim_OpportunityBusinessType = 'New logo'
  AND dm_core_fact_opportunity_dev.m_IsValidForReporting = 1
  AND dm_core_fact_opportunity_dev.m_AmountOfConvertedOppLicenseEquivalency > 0.000000
;

----Global New logo

SELECT dm_core_fact_opportunity_dev.id_primary_Opportunity
  ,MARKETINGOPERATIONS.DM_CORE.dm_core_dim_accountlead_dev.DIM_STATEPROVINCE
  ,dm_core_dim_opportunity_dev.dim_geo_OpportunitySubSubRegion
  ,dm_core_dim_opportunity_dev.dim_geo_OpportunitySubRegion
  ,dm_core_dim_opportunity_dev.dim_geo_OpportunityCountry
  ,MARKETINGOPERATIONS.DM_CORE.DM_CORE_DIM_ACCOUNTLEAD_DEV.DIM_GEO_ACCOUNTSUBREGION AS TableauSubRegion
  ,dm_core_dim_opportunity_dev.dim_OpportunityLDRName
  ,dm_core_fact_opportunity_dev.DATE_OPPCREATED_DATE
  ,dm_core_fact_opportunity_dev.date_OppClosedDate
  ,dm_core_dim_opportunity_dev.dim_OpportunityNumberofAgents
  ,dm_core_dim_opportunity_dev.dim_OpportunityOwnerSegment AS TableauOpportunityOwnerSegment
  ,dm_core_dim_opportunity_dev.dim_OpportunityCreatedByName
  ,dm_core_dim_opportunity_dev.dim_OpportunityOwnerName
  ,dm_core_dim_opportunity_dev.dim_OpportunityOwnerRoleName
  ,dm_core_dim_opportunity_dev.dim_OpportunityName
  ,eopp.CONVERTEDOPPCREATEDBYMAORLDR
  ,elc.CONVERTEDPRIMARYCONTACTCREATEDBYMAORLDR
  ,MARKETINGOPERATIONS.DM_CORE.dm_core_dim_opportunity_dev.DIM_OPPORTUNITYSTAGE_NAME
  ,CONCAT('https://genesys.my.salesforce.com/',dm_core_fact_opportunity_dev.id_primary_Opportunity) AS URL
  ,CASE WHEN dm_core_fact_opportunity_dev.f_Ad_f_IsAuditIssueAccepted = 1 THEN 0
    ELSE GREATEST(dm_core_fact_opportunity_dev.f_Ad_01_MissingPrimary
                  ,dm_core_fact_opportunity_dev.f_Ad_02_BANTIssue
                  ,dm_core_fact_opportunity_dev.f_Ad_03_LDRnoCampaign
                  ,dm_core_fact_opportunity_dev.f_Ad_04_CampaignnoLDR
                  ,dm_core_fact_opportunity_dev.f_Ad_08_BadOwner
                  ,dm_core_fact_opportunity_dev.f_Ad_09_LDROwned
                  ,CASE WHEN dm_core_fact_opportunity_dev.f_Ad_17_ClaimedOpp = 1
                              AND dm_core_dim_opportunity_dev.dim_OpportunityCreatedByName LIKE '%Integration%' THEN 0 ELSE dm_core_fact_opportunity_dev.f_Ad_17_ClaimedOpp END) END AS INVALID_OPP
  ,dm_core_fact_opportunity_dev.f_Ad_01_MissingPrimary AS Missing_Primary_Contact
  ,dm_core_fact_opportunity_dev.f_Ad_02_BANTIssue AS BANT_Issue
  ,dm_core_fact_opportunity_dev.f_Ad_03_LDRnoCampaign AS No_Campaign_Source
  ,dm_core_fact_opportunity_dev.f_Ad_04_CampaignnoLDR AS No_Journey_Team_on_Opportunity
  ,dm_core_fact_opportunity_dev.f_Ad_08_BadOwner AS NonSales_Owner

  ,CASE WHEN dm_core_fact_opportunity_dev.f_Ad_17_ClaimedOpp = 1
                              AND dm_core_dim_opportunity_dev.dim_OpportunityCreatedByName LIKE '%Integration%' THEN 0 ELSE dm_core_fact_opportunity_dev.f_Ad_17_ClaimedOpp END AS Not_Created_by_Journey_Team
  ,dm_core_fact_opportunity_dev.f_Ad_f_IsAuditIssueAccepted AS Exception_Accepted
  ,dm_core_dim_opportunity_dev.DIM_OPPORTUNITYWINLOSS
  ,dm_core_dim_opportunity_dev.dim_OpportunityBusinessType
  ,dm_core_dim_opportunity_dev.dim_PlatformType
  ,dm_core_dim_opportunity_dev.dim_OfferingsType
  ,dm_core_fact_opportunity_dev.date_OppPipelineAddedDate
  ,dm_core_fact_opportunity_dev.m_AmountOfConvertedOppLicenseEquivalency
  ,dm_core_fact_opportunity_dev.m_AmountOfConvertedOppLicenseAmount
  ,dm_core_fact_opportunity_dev.m_AmountOfConvertedOppACV
  ,dm_core_fact_opportunity_dev.m_AmountOfConvertedOppTCV
  ,dm_core_fact_opportunity_dev.m_AmountOfConvertedOppTermACV
  ,dm_core_dim_accountlead_dev.dim_AccountName
  ,dm_core_fact_opportunity_dev.id_Account
  ,CONCAT('https://genesys.my.salesforce.com/',dm_core_fact_opportunity_dev.id_Account) AS ACCOUNT_URL
  ,dm_core_dim_opportunity_dev.dim_geo_OpportunityRegion

----  ,dm_core_fact_opportunity_dev.m_IsPipelineValidation
----  ,dm_core_fact_opportunity_dev.m_IsBookingValidation
----  ,dm_core_fact_opportunity_dev.m_IsValidForReporting
FROM MARKETINGOPERATIONS.DM_CORE.dm_core_fact_opportunity_dev
  JOIN MARKETINGOPERATIONS.DM_CORE.dm_core_dim_opportunity_dev
    ON MARKETINGOPERATIONS.DM_CORE.dm_core_fact_opportunity_dev.id_primary_Opportunity = MARKETINGOPERATIONS.DM_CORE.dm_core_dim_opportunity_dev.id_primary_Opportunity
    LEFT JOIN MARKETINGOPERATIONS.DM_CALC.V_EXT_OPPORTUNITY eopp
  ON MARKETINGOPERATIONS.DM_CORE.DM_CORE_FACT_OPPORTUNITY_DEV.ID_PRIMARY_OPPORTUNITY = eopp.ID
  LEFT JOIN MARKETINGOPERATIONS.DM_CORE.dm_core_dim_accountlead_dev
    ON MARKETINGOPERATIONS.DM_CORE.dm_core_fact_opportunity_dev.id_AccountLead = MARKETINGOPERATIONS.DM_CORE.dm_core_dim_accountlead_dev.id_primary_AccountLead
  LEFT JOIN MARKETINGOPERATIONS.DM_CALC.V_EXT_LEADCONTACT elc
  ON MARKETINGOPERATIONS.DM_CORE.DM_CORE_DIM_ACCOUNTLEAD_DEV.ID_PRIMARY_ACCOUNTLEAD = elc.ACCOUNTLEAD_ID
  LEFT JOIN MARKETINGOPERATIONS.SFDC.OPPORTUNITY
    ON MARKETINGOPERATIONS.DM_CORE.dm_core_fact_opportunity_dev.id_primary_Opportunity = MARKETINGOPERATIONS.SFDC.OPPORTUNITY.Id
  LEFT JOIN MARKETINGOPERATIONS.SFDC.USER as OppJER
    ON MARKETINGOPERATIONS.SFDC.OPPORTUNITY.JOURNEY_ENGAGEMENT_REP_C = OppJER.Id
  LEFT JOIN MARKETINGOPERATIONS.REF_TABLES.ML_DM_REF_USERTYPE
    ON CASE WHEN MARKETINGOPERATIONS.DM_CORE.dm_core_dim_opportunity_dev.id_OpportunityLDR IS NOT NULL
    THEN MARKETINGOPERATIONS.DM_CORE.dm_core_dim_opportunity_dev.id_OpportunityLDR
       ELSE MARKETINGOPERATIONS.SFDC.OPPORTUNITY.JOURNEY_ENGAGEMENT_REP_C END = MARKETINGOPERATIONS.REF_TABLES.ML_DM_REF_USERTYPE.Id
WHERE dm_core_fact_opportunity_dev.DATE_OPPCREATED_DATE >= '2018-02-19'
  AND dm_core_fact_opportunity_dev.m_IsMarketingSourced = 1
  AND dm_core_dim_opportunity_dev.dim_OpportunityBusinessType = 'New logo'
  AND dm_core_fact_opportunity_dev.m_IsValidForReporting = 1
;

----Global Non-New logo

SELECT dm_core_fact_opportunity_dev.id_primary_Opportunity
  ,MARKETINGOPERATIONS.DM_CORE.dm_core_dim_accountlead_dev.DIM_STATEPROVINCE
  ,dm_core_dim_opportunity_dev.dim_geo_OpportunitySubSubRegion
  ,dm_core_dim_opportunity_dev.dim_geo_OpportunitySubRegion
  ,dm_core_dim_opportunity_dev.dim_geo_OpportunityCountry
  ,MARKETINGOPERATIONS.DM_CORE.DM_CORE_DIM_ACCOUNTLEAD_DEV.DIM_GEO_ACCOUNTSUBREGION AS TableauSubRegion
  ,dm_core_dim_opportunity_dev.dim_OpportunityLDRName
  ,dm_core_fact_opportunity_dev.DATE_OPPCREATED_DATE
  ,dm_core_fact_opportunity_dev.date_OppClosedDate
  ,dm_core_dim_opportunity_dev.dim_OpportunityNumberofAgents
  ,dm_core_dim_opportunity_dev.dim_OpportunityOwnerSegment AS TableauOpportunityOwnerSegment
  ,dm_core_dim_opportunity_dev.dim_OpportunityCreatedByName
  ,dm_core_dim_opportunity_dev.dim_OpportunityOwnerName
  ,dm_core_dim_opportunity_dev.dim_OpportunityOwnerRoleName
  ,dm_core_dim_opportunity_dev.dim_OpportunityName
  ,eopp.CONVERTEDOPPCREATEDBYMAORLDR
  ,elc.CONVERTEDPRIMARYCONTACTCREATEDBYMAORLDR
  ,MARKETINGOPERATIONS.DM_CORE.dm_core_dim_opportunity_dev.DIM_OPPORTUNITYSTAGE_NAME
  ,CONCAT('https://genesys.my.salesforce.com/',dm_core_fact_opportunity_dev.id_primary_Opportunity) AS URL
  ,CASE WHEN dm_core_fact_opportunity_dev.f_Ad_f_IsAuditIssueAccepted = 1 THEN 0
    ELSE GREATEST(dm_core_fact_opportunity_dev.f_Ad_01_MissingPrimary
                  ,dm_core_fact_opportunity_dev.f_Ad_02_BANTIssue
                  ,dm_core_fact_opportunity_dev.f_Ad_03_LDRnoCampaign
                  ,dm_core_fact_opportunity_dev.f_Ad_04_CampaignnoLDR
                  ,dm_core_fact_opportunity_dev.f_Ad_08_BadOwner
                  ,dm_core_fact_opportunity_dev.f_Ad_09_LDROwned
                  ,CASE WHEN dm_core_fact_opportunity_dev.f_Ad_17_ClaimedOpp = 1
                              AND dm_core_dim_opportunity_dev.dim_OpportunityCreatedByName LIKE '%Integration%' THEN 0 ELSE dm_core_fact_opportunity_dev.f_Ad_17_ClaimedOpp END) END AS INVALID_OPP
  ,dm_core_fact_opportunity_dev.f_Ad_01_MissingPrimary AS Missing_Primary_Contact
  ,dm_core_fact_opportunity_dev.f_Ad_02_BANTIssue AS BANT_Issue
  ,dm_core_fact_opportunity_dev.f_Ad_03_LDRnoCampaign AS No_Campaign_Source
  ,dm_core_fact_opportunity_dev.f_Ad_04_CampaignnoLDR AS No_Journey_Team_on_Opportunity
  ,dm_core_fact_opportunity_dev.f_Ad_08_BadOwner AS NonSales_Owner
  ,CASE WHEN dm_core_fact_opportunity_dev.f_Ad_17_ClaimedOpp = 1
                              AND dm_core_dim_opportunity_dev.dim_OpportunityCreatedByName LIKE '%Integration%' THEN 0 ELSE dm_core_fact_opportunity_dev.f_Ad_17_ClaimedOpp END AS Not_Created_by_Journey_Team
  ,dm_core_fact_opportunity_dev.f_Ad_f_IsAuditIssueAccepted AS Exception_Accepted
  ,dm_core_dim_opportunity_dev.DIM_OPPORTUNITYWINLOSS
  ,dm_core_dim_opportunity_dev.dim_OpportunityBusinessType
  ,dm_core_dim_opportunity_dev.dim_PlatformType
  ,dm_core_dim_opportunity_dev.dim_OfferingsType
  ,dm_core_fact_opportunity_dev.date_OppPipelineAddedDate
  ,dm_core_fact_opportunity_dev.m_AmountOfConvertedOppLicenseEquivalency
  ,dm_core_fact_opportunity_dev.m_AmountOfConvertedOppLicenseAmount
  ,dm_core_fact_opportunity_dev.m_AmountOfConvertedOppACV
  ,dm_core_fact_opportunity_dev.m_AmountOfConvertedOppTCV
  ,dm_core_fact_opportunity_dev.m_AmountOfConvertedOppTermACV
  ,dm_core_dim_accountlead_dev.dim_AccountName
  ,dm_core_fact_opportunity_dev.id_Account
  ,CONCAT('https://genesys.my.salesforce.com/',dm_core_fact_opportunity_dev.id_Account) AS ACCOUNT_URL
  ,dm_core_dim_opportunity_dev.dim_geo_OpportunityRegion

----  ,dm_core_fact_opportunity_dev.m_IsPipelineValidation
----  ,dm_core_fact_opportunity_dev.m_IsBookingValidation
----  ,dm_core_fact_opportunity_dev.m_IsValidForReporting
FROM MARKETINGOPERATIONS.DM_CORE.dm_core_fact_opportunity_dev
  JOIN MARKETINGOPERATIONS.DM_CORE.dm_core_dim_opportunity_dev
    ON MARKETINGOPERATIONS.DM_CORE.dm_core_fact_opportunity_dev.id_primary_Opportunity = MARKETINGOPERATIONS.DM_CORE.dm_core_dim_opportunity_dev.id_primary_Opportunity
    LEFT JOIN MARKETINGOPERATIONS.DM_CALC.V_EXT_OPPORTUNITY eopp
  ON MARKETINGOPERATIONS.DM_CORE.DM_CORE_FACT_OPPORTUNITY_DEV.ID_PRIMARY_OPPORTUNITY = eopp.ID
  LEFT JOIN MARKETINGOPERATIONS.DM_CORE.dm_core_dim_accountlead_dev
    ON MARKETINGOPERATIONS.DM_CORE.dm_core_fact_opportunity_dev.id_AccountLead = MARKETINGOPERATIONS.DM_CORE.dm_core_dim_accountlead_dev.id_primary_AccountLead
  LEFT JOIN MARKETINGOPERATIONS.DM_CALC.V_EXT_LEADCONTACT elc
  ON MARKETINGOPERATIONS.DM_CORE.DM_CORE_DIM_ACCOUNTLEAD_DEV.ID_PRIMARY_ACCOUNTLEAD = elc.ACCOUNTLEAD_ID
  LEFT JOIN MARKETINGOPERATIONS.SFDC.OPPORTUNITY
    ON MARKETINGOPERATIONS.DM_CORE.dm_core_fact_opportunity_dev.id_primary_Opportunity = MARKETINGOPERATIONS.SFDC.OPPORTUNITY.Id
  LEFT JOIN MARKETINGOPERATIONS.SFDC.USER as OppJER
    ON MARKETINGOPERATIONS.SFDC.OPPORTUNITY.JOURNEY_ENGAGEMENT_REP_C = OppJER.Id
  LEFT JOIN MARKETINGOPERATIONS.REF_TABLES.ML_DM_REF_USERTYPE
    ON CASE WHEN MARKETINGOPERATIONS.DM_CORE.dm_core_dim_opportunity_dev.id_OpportunityLDR IS NOT NULL
    THEN MARKETINGOPERATIONS.DM_CORE.dm_core_dim_opportunity_dev.id_OpportunityLDR
       ELSE MARKETINGOPERATIONS.SFDC.OPPORTUNITY.JOURNEY_ENGAGEMENT_REP_C END = MARKETINGOPERATIONS.REF_TABLES.ML_DM_REF_USERTYPE.Id
WHERE dm_core_fact_opportunity_dev.DATE_OPPCREATED_DATE >= '2018-02-19'
  AND dm_core_fact_opportunity_dev.m_IsMarketingSourced = 1
  AND dm_core_dim_opportunity_dev.dim_OpportunityBusinessType != 'New logo'
  AND dm_core_fact_opportunity_dev.m_IsValidForReporting = 1
;

----Global Not Valid for Reporting

SELECT dm_core_fact_opportunity_dev.id_primary_Opportunity
  ,MARKETINGOPERATIONS.DM_CORE.dm_core_dim_accountlead_dev.DIM_STATEPROVINCE
  ,dm_core_dim_opportunity_dev.dim_geo_OpportunitySubSubRegion
  ,dm_core_dim_opportunity_dev.dim_geo_OpportunitySubRegion
  ,dm_core_dim_opportunity_dev.dim_geo_OpportunityCountry
  ,MARKETINGOPERATIONS.DM_CORE.DM_CORE_DIM_ACCOUNTLEAD_DEV.DIM_GEO_ACCOUNTSUBREGION AS TableauSubRegion
  ,dm_core_dim_opportunity_dev.dim_OpportunityLDRName
  ,dm_core_fact_opportunity_dev.DATE_OPPCREATED_DATE
  ,dm_core_fact_opportunity_dev.date_OppClosedDate
  ,dm_core_dim_opportunity_dev.dim_OpportunityNumberofAgents
  ,dm_core_dim_opportunity_dev.dim_OpportunityOwnerSegment AS TableauOpportunityOwnerSegment
  ,dm_core_dim_opportunity_dev.dim_OpportunityCreatedByName
  ,dm_core_dim_opportunity_dev.dim_OpportunityOwnerName
  ,dm_core_dim_opportunity_dev.dim_OpportunityOwnerRoleName
  ,dm_core_dim_opportunity_dev.dim_OpportunityName
  ,eopp.CONVERTEDOPPCREATEDBYMAORLDR
  ,elc.CONVERTEDPRIMARYCONTACTCREATEDBYMAORLDR
  ,MARKETINGOPERATIONS.DM_CORE.dm_core_dim_opportunity_dev.DIM_OPPORTUNITYSTAGE_NAME
  ,CONCAT('https://genesys.my.salesforce.com/',dm_core_fact_opportunity_dev.id_primary_Opportunity) AS URL
  ,CASE WHEN dm_core_fact_opportunity_dev.f_Ad_f_IsAuditIssueAccepted = 1 THEN 0
    ELSE GREATEST(dm_core_fact_opportunity_dev.f_Ad_01_MissingPrimary
                  ,dm_core_fact_opportunity_dev.f_Ad_02_BANTIssue
                  ,dm_core_fact_opportunity_dev.f_Ad_03_LDRnoCampaign
                  ,dm_core_fact_opportunity_dev.f_Ad_04_CampaignnoLDR
                  ,dm_core_fact_opportunity_dev.f_Ad_08_BadOwner
                  ,dm_core_fact_opportunity_dev.f_Ad_09_LDROwned
                  ,CASE WHEN dm_core_fact_opportunity_dev.f_Ad_17_ClaimedOpp = 1
                              AND dm_core_dim_opportunity_dev.dim_OpportunityCreatedByName LIKE '%Integration%' THEN 0 ELSE dm_core_fact_opportunity_dev.f_Ad_17_ClaimedOpp END) END AS INVALID_OPP
  ,dm_core_fact_opportunity_dev.f_Ad_01_MissingPrimary AS Missing_Primary_Contact
  ,dm_core_fact_opportunity_dev.f_Ad_02_BANTIssue AS BANT_Issue
  ,dm_core_fact_opportunity_dev.f_Ad_03_LDRnoCampaign AS No_Campaign_Source
  ,dm_core_fact_opportunity_dev.f_Ad_04_CampaignnoLDR AS No_Journey_Team_on_Opportunity
  ,dm_core_fact_opportunity_dev.f_Ad_08_BadOwner AS NonSales_Owner

  ,CASE WHEN dm_core_fact_opportunity_dev.f_Ad_17_ClaimedOpp = 1
                              AND dm_core_dim_opportunity_dev.dim_OpportunityCreatedByName LIKE '%Integration%' THEN 0 ELSE dm_core_fact_opportunity_dev.f_Ad_17_ClaimedOpp END AS Not_Created_by_Journey_Team
  ,dm_core_fact_opportunity_dev.f_Ad_f_IsAuditIssueAccepted AS Exception_Accepted
  ,dm_core_dim_opportunity_dev.DIM_OPPORTUNITYWINLOSS
  ,dm_core_dim_opportunity_dev.dim_OpportunityBusinessType
  ,dm_core_dim_opportunity_dev.dim_PlatformType
  ,dm_core_dim_opportunity_dev.dim_OfferingsType
  ,dm_core_fact_opportunity_dev.date_OppPipelineAddedDate
  ,dm_core_fact_opportunity_dev.m_AmountOfConvertedOppLicenseEquivalency
  ,dm_core_fact_opportunity_dev.m_AmountOfConvertedOppLicenseAmount
  ,dm_core_fact_opportunity_dev.m_AmountOfConvertedOppACV
  ,dm_core_fact_opportunity_dev.m_AmountOfConvertedOppTCV
  ,dm_core_fact_opportunity_dev.m_AmountOfConvertedOppTermACV
  ,dm_core_dim_accountlead_dev.dim_AccountName
  ,dm_core_fact_opportunity_dev.id_Account
  ,CONCAT('https://genesys.my.salesforce.com/',dm_core_fact_opportunity_dev.id_Account) AS ACCOUNT_URL
  ,dm_core_dim_opportunity_dev.dim_geo_OpportunityRegion

----  ,dm_core_fact_opportunity_dev.m_IsPipelineValidation
----  ,dm_core_fact_opportunity_dev.m_IsBookingValidation
----  ,dm_core_fact_opportunity_dev.m_IsValidForReporting
FROM MARKETINGOPERATIONS.DM_CORE.dm_core_fact_opportunity_dev
  JOIN MARKETINGOPERATIONS.DM_CORE.dm_core_dim_opportunity_dev
    ON MARKETINGOPERATIONS.DM_CORE.dm_core_fact_opportunity_dev.id_primary_Opportunity = MARKETINGOPERATIONS.DM_CORE.dm_core_dim_opportunity_dev.id_primary_Opportunity
    LEFT JOIN MARKETINGOPERATIONS.DM_CALC.V_EXT_OPPORTUNITY eopp
  ON MARKETINGOPERATIONS.DM_CORE.DM_CORE_FACT_OPPORTUNITY_DEV.ID_PRIMARY_OPPORTUNITY = eopp.ID
  LEFT JOIN MARKETINGOPERATIONS.DM_CORE.dm_core_dim_accountlead_dev
    ON MARKETINGOPERATIONS.DM_CORE.dm_core_fact_opportunity_dev.id_AccountLead = MARKETINGOPERATIONS.DM_CORE.dm_core_dim_accountlead_dev.id_primary_AccountLead
  LEFT JOIN MARKETINGOPERATIONS.DM_CALC.V_EXT_LEADCONTACT elc
  ON MARKETINGOPERATIONS.DM_CORE.DM_CORE_DIM_ACCOUNTLEAD_DEV.ID_PRIMARY_ACCOUNTLEAD = elc.ACCOUNTLEAD_ID
  LEFT JOIN MARKETINGOPERATIONS.SFDC.OPPORTUNITY
    ON MARKETINGOPERATIONS.DM_CORE.dm_core_fact_opportunity_dev.id_primary_Opportunity = MARKETINGOPERATIONS.SFDC.OPPORTUNITY.Id
  LEFT JOIN MARKETINGOPERATIONS.SFDC.USER as OppJER
    ON MARKETINGOPERATIONS.SFDC.OPPORTUNITY.JOURNEY_ENGAGEMENT_REP_C = OppJER.Id
  LEFT JOIN MARKETINGOPERATIONS.REF_TABLES.ML_DM_REF_USERTYPE
    ON CASE WHEN MARKETINGOPERATIONS.DM_CORE.dm_core_dim_opportunity_dev.id_OpportunityLDR IS NOT NULL
    THEN MARKETINGOPERATIONS.DM_CORE.dm_core_dim_opportunity_dev.id_OpportunityLDR
       ELSE MARKETINGOPERATIONS.SFDC.OPPORTUNITY.JOURNEY_ENGAGEMENT_REP_C END = MARKETINGOPERATIONS.REF_TABLES.ML_DM_REF_USERTYPE.ID
WHERE dm_core_fact_opportunity_dev.DATE_OPPCREATED_DATE >= '2018-02-19'
  AND dm_core_fact_opportunity_dev.m_IsMarketingSourced = 1
  AND dm_core_fact_opportunity_dev.m_IsValidForReporting = 0
;
