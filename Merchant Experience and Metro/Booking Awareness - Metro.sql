
drop table sandbox.nvp_booking_awareness;
create multiset table sandbox.nvp_booking_awareness as (
select
   a.*,
   b.name as merchant_name,
   b.website,
   SUBSTR(b.salesforce_account_id,1,15) SF_Account_15,
   b.salesforce_account_id SF_Account_18,
   c.merchant_segmentation,
   c.account_owner,
   c.BillingCity as City,
   c.BillingState as State,
   c.BillingCountry as Country,
   op.Opportunity_ID,
   op.Primary_Deal_Services,
   ad.mn_load_date first_live_date
from
(Select merchant_id,
     history_data.JSONExtractValue('$.restricted.bCookie') as bCookie,
     history_data.JSONExtractValue('$.additionalInfo.tool') as tool,
     history_data.JSONExtractValue('$.additionalInfo.reachout') as reachout,
     history_data.JSONExtractValue('$.additionalInfo.take_booking') as take_booking,
     SUBSTR(history_data.JSONExtractValue('$.additionalInfo.timestamp'),1,10) as time_survey_filled
    from sb_merchant_experience.history_event
    where event_type = 'BOOKING_SURVEY') as a
left join
     user_edwprod.dim_merchant as b on a.merchant_id = b.merchant_uuid
left join
     (select Account_ID_18,
            BillingCity,
            BillingCountry,
            BillingState,
            sfa.Merchant_Segmentation__c merchant_segmentation,
            max(full_name) account_owner
        from dwh_base_sec_view.sf_account sfa
        left join
             user_groupondw.dim_sf_person sfp on sfa.ownerid = sfp.person_id
        group by 1,2,3,4,5
        ) as c on b.salesforce_account_id = c.Account_ID_18
left join
    (select
         sf1.Opportunity_ID,
         sf1.AccountId,
         sf1.Primary_Deal_Services,
         sf2.deal_uuid deal_uuid,
         sf1.CreatedDate----later if needed
         from dwh_base_sec_view.sf_opportunity_1 sf1
         join user_edwprod.sf_opportunity_2 sf2 on sf1.id = sf2.id
         group by 1,2,3,4,5
     ) as op on c.Account_ID_18 = op.AccountID
left join
     (select
          deal_uuid,
          min(cast(load_date as date)) mn_load_date
      from user_groupondw.active_deals
      where
         sold_out = 'false'
         and available_qty > 0
      group by 1
     )  as ad on op.deal_uuid = ad.deal_uuid
) with data no primary index;

drop table sandbox.nvp_booking_awareness;
create multiset table sandbox.nvp_booking_awareness as (
select
   a.*,
   b.name as merchant_name,
   b.website,
   SUBSTR(b.salesforce_account_id,1,15) SF_Account_15,
   c.merchant_segmentation,
   c.account_owner,
   c.BillingCity as City,
   c.BillingState as State,
   c.BillingCountry as Country,
   op.Opportunity_ID,
   op.Primary_Deal_Services,
   ad.mn_load_date first_live_date
from
(Select merchant_id,
     history_data.JSONExtractValue('$.restricted.bCookie') as bCookie,
     history_data.JSONExtractValue('$.additionalInfo.tool') as tool,
     history_data.JSONExtractValue('$.additionalInfo.reachout') as reachout,
     history_data.JSONExtractValue('$.additionalInfo.take_booking') as take_booking,
     SUBSTR(history_data.JSONExtractValue('$.additionalInfo.timestamp'),1,10) as time_survey_filled
    from sb_merchant_experience.history_event
    where event_type = 'BOOKING_SURVEY') as a
left join
     user_edwprod.dim_merchant as b on a.merchant_id = b.merchant_uuid
left join
     (select Account_ID_18,
            BillingCity,
            BillingCountry,
            BillingState,
            sfa.Merchant_Segmentation__c merchant_segmentation,
            max(full_name) account_owner
        from dwh_base_sec_view.sf_account sfa
        left join
             user_groupondw.dim_sf_person sfp on sfa.ownerid = sfp.person_id
        group by 1,2,3,4,5
        ) as c on b.salesforce_account_id = c.Account_ID_18
left join
    (select
         sf1.Opportunity_ID,
         sf1.AccountId,
         sf1.Primary_Deal_Services,
         sf2.deal_uuid deal_uuid,
         sf1.CreatedDate----later if needed
         from dwh_base_sec_view.sf_opportunity_1 sf1
         join user_edwprod.sf_opportunity_2 sf2 on sf1.id = sf2.id
         group by 1,2,3,4,5
     ) as op on c.Account_ID_18 = op.AccountID
left join
     (select
          deal_uuid,
          min(cast(load_date as date)) mn_load_date
      from user_groupondw.active_deals
      where
         sold_out = 'false'
         and available_qty > 0
      group by 1
     )  as ad on op.deal_uuid = ad.deal_uuid
) with data no primary index
;



select
         sf1.Opportunity_ID,
         sf1.AccountId,
         sf1.Primary_Deal_Services,
         sf2.deal_uuid deal_uuid,
         sf1.CreatedDate,
         sf1.dwh_created_at,
         sf2.dwh_created_at,
         sf2.CreatedDate
         from dwh_base_sec_view.sf_opportunity_1 sf1
         join user_edwprod.sf_opportunity_2 sf2 on sf1.id = sf2.id

--------Trials

select Account_ID_18, count(distinct Merchant_Segmentation__c) cn from dwh_base_sec_view.sf_account group by Account_ID_18 having cn >1;

SELECT full_name from user_groupondw.dim_sf_person sfp;

Select merchant_id,
     history_data.JSONExtractValue('$.restricted.bCookie') as bCookie,
     history_data.JSONExtractValue('$.additionalInfo.tool') as tool,
     history_data.JSONExtractValue('$.additionalInfo.reachout') as reachout,
     history_data.JSONExtractValue('$.additionalInfo.take_booking') as take_booking,
     SUBSTR(history_data.JSONExtractValue('$.additionalInfo.timestamp'),1,10) as survey_fill_date
    from sb_merchant_experience.history_event
    where event_type = 'BOOKING_SURVEY'
    and history_data.JSONExtractValue('$.additionalInfo.timestamp') is not NULL;

select * from sb_merchant_experience.history_event where event_type = 'BOOKING_SURVEY'

----------OLD QUERY
drop table sandbox.nvp_booking_awareness;create table sandbox.nvp_booking_awareness as (
select
   a.*,
   b.name as merchant_name,
   b.website,
   SUBSTR(b.salesforce_account_id,1,15) SF_Account_15,
   c.BillingCity as City,
   c.BillingState as State,
   c.BillingCountry as Country,
   e.pds_cat_name,
   c.Subcategory_v3
from
(Select merchant_id,
     history_data.JSONExtractValue('$.restricted.bCookie') as bCookie,
     history_data.JSONExtractValue('$.additionalInfo.tool') as tool,
     history_data.JSONExtractValue('$.additionalInfo.reachout') as reachout,
     history_data.JSONExtractValue('$.additionalInfo.take_booking') as take_booking,
     history_data.JSONExtractValue('$.additionalInfo.timestamp') as time_survey_filled
    from sb_merchant_experience.history_event
    where event_type = 'BOOKING_SURVEY') as a
left join
     user_edwprod.dim_merchant as b on a.merchant_id = b.merchant_uuid
left join
     (select Account_ID_18, BillingCity, BillingCountry, BillingState, Services_Offered, Subcategory_v3 from dwh_base_sec_view.sf_account) as c on b.salesforce_account_id = c.Account_ID_18
left join
(select
       history_data.JSONExtractValue('$.additionalInfo.deal.merchantId') merchant_id,
       max(history_data.JSONExtractValue('$.additionalInfo.deal.primaryDealServiceId')) PDS_service_ID
    from sb_merchant_experience.history_event
    where event_type = 'DRAFT_DEAL_CREATION'
    group by history_data.JSONExtractValue('$.additionalInfo.deal.merchantId')) as d on a.merchant_id = d.merchant_id
left join user_dw.v_dim_pds_grt_map as e on d.PDS_service_ID = e.pds_cat_id
) with data;grant select on sandbox.nvp_booking_awareness to public

----------ROUGH WORK

select
  count(1)
  from
(Select merchant_id,
     history_data.JSONExtractValue('$.restricted.bCookie') as bCookie,
     history_data.JSONExtractValue('$.additionalInfo.tool') as tool,
     history_data.JSONExtractValue('$.additionalInfo.reachout') as reachout,
     history_data.JSONExtractValue('$.additionalInfo.take_booking') as take_booking
    from sb_merchant_experience.history_event()
    where event_type = 'BOOKING_SURVEY') as a;

(select
     distinct
          history_data.JSONExtractValue('$.additionalInfo.deal.merchantId') merchant_id,
          history_data.JSONExtractValue('$.additionalInfo.deal.primaryDealServiceId') PDS_service_ID
      from sb_merchant_experience.history_event
      where event_type = 'DRAFT_DEAL_CREATION') as a
left join user_dw.v_dim_pds_grt_map as b on a.PDS_service_ID = b.pds_cat_id;



{"restricted": {"bCookie": "89b6c1a8-2dd2-9a06-741f-87bdd48343ce", "ipAddress": "108.16.236.246", "requestId": "63d9d4b1-fbf5-4061-9ff8-036e30f9ef16"}, "additionalInfo": {"reachout": "No thanks.", "timestamp": "2020-11-24T16:31:08.354Z", "take_booking": "No, but maybe in future", "merchant_uuid": "7aaec662-0a1f-4f50-b3d8-0327136694e2"}}

{"restricted":
    {"bCookie": "9988a748-4a90-ae9d-8875-46e26a140067",
     "ipAddress": "68.8.224.43",
     "requestId": "c46616e0-2f84-4a4f-a616-e02f84da4fcb"},
     "additionalInfo":
             {"deal":
	                  {"id": "829b1a0d-6e8c-4792-a6b2-3b48aebd1066",
	                   "images": [{"id": "img.grouponcdn.com/deal/6whGX1o4avncfmdvePP4/Ri-2228x1671"}],
	                   "products": [{"inventoryProduct":
	                                                   {"prices": {"unitPrice": {"amount": 2000, "decimals": 2, "currencyCode": "USD"},
	                                                               "unitValue": {"amount": 4200, "decimals": 2, "currencyCode": "USD"},
	                                                               "unitBuyPrice": {"amount": 1040, "decimals": 2, "currencyCode": "USD"},
	                                                               "discountValue": {"amount": 47, "decimals": 2, "currencyCode": "USD"}},
	                                                               "buyerMax": 4, "maxPledges": 50, "expiresInDays": 180, "userMaxWindow": 30},
	                                                    "localizedContents": [{"title": "Pet boarding for 2 nights", "locale": "en_US"}]},
	                                {"inventoryProduct": {"prices": {"unitPrice": {"amount": 1000, "decimals": 2, "currencyCode": "USD"},
	                                                                 "unitValue": {"amount": 2100, "decimals": 2, "currencyCode": "USD"},
	                                                                 "unitBuyPrice": {"amount": 520, "decimals": 2, "currencyCode": "USD"},
	                                                                 "discountValue": {"amount": 47, "decimals": 2, "currencyCode": "USD"}},
	                                                                 "buyerMax": 4, "maxPledges": 50, "expiresInDays": 180, "userMaxWindow": 30},
	                                                     "localizedContents": [{"title": "Pet boarding for 1 night", "locale": "en_US"}]},
	                                {"inventoryProduct": {"prices": {"unitPrice": {"amount": 3000, "decimals": 2, "currencyCode": "USD"},
	                                                                 "unitValue": {"amount": 6300, "decimals": 2, "currencyCode": "USD"},
	                                                                 "unitBuyPrice": {"amount": 1560, "decimals": 2, "currencyCode": "USD"},
	                                                                 "discountValue": {"amount": 47, "decimals": 2, "currencyCode": "USD"}},
	                                                                 "buyerMax": 4, "maxPledges": 50, "expiresInDays": 180, "userMaxWindow": 30},
	                                                      "localizedContents": [{"title": "Pet boarding for 3 nights", "locale": "en_US"}]},
	                                {"inventoryProduct": {"prices": {"unitPrice": {"amount": 2500, "decimals": 2, "currencyCode": "USD"},
	                                                                 "unitValue": {"amount": 5000, "decimals": 2, "currencyCode": "USD"},
	                                                                 "unitBuyPrice": {"amount": 1300, "decimals": 2, "currencyCode": "USD"},
	                                                                 "discountValue": {"amount": 50, "decimals": 2, "currencyCode": "USD"}},
	                                                                 "buyerMax": 4, "maxPledges": 50, "expiresInDays": 180, "userMaxWindow": 30},
	                                                      "localizedContents": [{"title": "$50 voucher towards veterinary services", "locale": "en_US"}]}],
	                   "merchantId": "4bb12a54-da29-4c62-8cdf-71602d0effc4",
	                   "templateId": "53ec01cc-3ee1-42b9-e5b9-84ec7bbb8816",
	                   "availableAt": "2019-02-19T00:49:09.963Z",
	                   "localizedContents": [{"title": "Up to ${max_discount_percent} Off at San Diego Cat bnb",
	                                         "locale": "en_US", "descriptor": "Up to ${max_discount_percent} Off at San Diego Cat bnb",
	                                         "emailSubject": "Up to ${max_discount_percent} Off at San Diego Cat bnb",
	                                         "shortDescriptor": "Kennel / Boarding",
	                                         "redemptionInstructions": "1. Pull up Groupon with our mobile app (or print it out). \n2. Present voucher upon arrival. \n3. Enjoy! "}],
	                   "primaryDealServiceId": "08787e82-87d4-4f6a-a0f8-c264ce009147",
	                   "consumerContractTerms": []},
                 "places": [{"name": "San Diego Cat bnb", "phone": "619-736-1439", "region": "CA", "country": "US", "locality": "San Diego", "postcode": "92104", "streetAddress": "South Park"}],
                 "merchant": {"website": "SanDiegoCatbnb.com"}}}


{"restricted": {"bCookie": "9988a748-4a90-ae9d-8875-46e26a140067", "ipAddress": "68.8.224.43", "requestId": "c46616e0-2f84-4a4f-a616-e02f84da4fcb"}, "additionalInfo": {"deal": {"id": "829b1a0d-6e8c-4792-a6b2-3b48aebd1066", "images": [{"id": "img.grouponcdn.com/deal/6whGX1o4avncfmdvePP4/Ri-2228x1671"}], "products": [{"inventoryProduct": {"prices": {"unitPrice": {"amount": 2000, "decimals": 2, "currencyCode": "USD"}, "unitValue": {"amount": 4200, "decimals": 2, "currencyCode": "USD"}, "unitBuyPrice": {"amount": 1040, "decimals": 2, "currencyCode": "USD"}, "discountValue": {"amount": 47, "decimals": 2, "currencyCode": "USD"}}, "buyerMax": 4, "maxPledges": 50, "expiresInDays": 180, "userMaxWindow": 30}, "localizedContents": [{"title": "Pet boarding for 2 nights", "locale": "en_US"}]}, {"inventoryProduct": {"prices": {"unitPrice": {"amount": 1000, "decimals": 2, "currencyCode": "USD"}, "unitValue": {"amount": 2100, "decimals": 2, "currencyCode": "USD"}, "unitBuyPrice": {"amount": 520, "decimals": 2, "currencyCode": "USD"}, "discountValue": {"amount": 47, "decimals": 2, "currencyCode": "USD"}}, "buyerMax": 4, "maxPledges": 50, "expiresInDays": 180, "userMaxWindow": 30}, "localizedContents": [{"title": "Pet boarding for 1 night", "locale": "en_US"}]}, {"inventoryProduct": {"prices": {"unitPrice": {"amount": 3000, "decimals": 2, "currencyCode": "USD"}, "unitValue": {"amount": 6300, "decimals": 2, "currencyCode": "USD"}, "unitBuyPrice": {"amount": 1560, "decimals": 2, "currencyCode": "USD"}, "discountValue": {"amount": 47, "decimals": 2, "currencyCode": "USD"}}, "buyerMax": 4, "maxPledges": 50, "expiresInDays": 180, "userMaxWindow": 30}, "localizedContents": [{"title": "Pet boarding for 3 nights", "locale": "en_US"}]}, {"inventoryProduct": {"prices": {"unitPrice": {"amount": 2500, "decimals": 2, "currencyCode": "USD"}, "unitValue": {"amount": 5000, "decimals": 2, "currencyCode": "USD"}, "unitBuyPrice": {"amount": 1300, "decimals": 2, "currencyCode": "USD"}, "discountValue": {"amount": 50, "decimals": 2, "currencyCode": "USD"}}, "buyerMax": 4, "maxPledges": 50, "expiresInDays": 180, "userMaxWindow": 30}, "localizedContents": [{"title": "$50 voucher towards veterinary services", "locale": "en_US"}]}], "merchantId": "4bb12a54-da29-4c62-8cdf-71602d0effc4", "templateId": "53ec01cc-3ee1-42b9-e5b9-84ec7bbb8816", "availableAt": "2019-02-19T00:49:09.963Z", "localizedContents": [{"title": "Up to ${max_discount_percent} Off at San Diego Cat bnb", "locale": "en_US", "descriptor": "Up to ${max_discount_percent} Off at San Diego Cat bnb", "emailSubject": "Up to ${max_discount_percent} Off at San Diego Cat bnb", "shortDescriptor": "Kennel / Boarding", "redemptionInstructions": "1. Pull up Groupon with our mobile app (or print it out). \n2. Present voucher upon arrival. \n3. Enjoy! "}], "primaryDealServiceId": "08787e82-87d4-4f6a-a0f8-c264ce009147", "consumerContractTerms": []}, "places": [{"name": "San Diego Cat bnb", "phone": "619-736-1439", "region": "CA", "country": "US", "locality": "San Diego", "postcode": "92104", "streetAddress": "South Park"}], "merchant": {"website": "SanDiegoCatbnb.com"}}}
{"restricted": {"bCookie": "59d9e060-3737-45cc-99e0-60373775cc89", "cCookie": null, "ipAddress": "24.171.26.58", "requestId": "96c9a3a7-f041-40a3-aa00-f575b20c75ba"}, "additionalInfo": {"request": {"name": "Grand Treatment Beauty Boutique", "website": "www.grandtreatmentbeauty.com", "contacts": [{"role": "full_access", "email": "rose@grandeventstl.com", "phone": "(314) 769-9603", "lastName": "Capone", "firstName": "Rose", "servicesAndTools": false, "promotionsAndEvents": false, "surveysAndSatisfaction": false}], "writeups": null, "locations": [{"city": "Saint Louis", "state": "MO", "country": "US", "postcode": "63104", "streetAddress": "2249 Nebraska Avenue"}], "treatment": true, "application": "merchantCenter", "utmCampaign": "Other", "noOfLocations": 1, "skipValidation": false, "businessCategory": ["bfb94a7c-1eb4-4aed-885c-c34f45ac7604", "48fec51f-e100-4503-9ffa-4680b1c3d1b7", "2f8e868e-41f3-40be-899a-358d5a3319b9"], "businessVertical": "Local", "webToLeadChannel": null, "customRedemptionCodes": null, "billingCountrySameAsFeatureCountry": null, "businessAddressAResidentialAddress": null}, "response": {"contact": {"id": "1a3e0fc6-ba71-11ea-a510-0242ac120002", "lastName": "Capone", "firstName": "Rose", "merchants": [{"id": "581f54e0-4806-4bcc-96bf-6d005fbd4509", "name": "Grand Treatment Beauty Boutique", "roles": {"merchantCenter": ["full_access"]}}], "emailAddresses": [{"address": "rose@grandeventstl.com"}], "primaryEmailAddress": "rose@grandeventstl.com"}, "freshRegistration": false}}}
{"restricted": {"bCookie": "bad5e5ce-46a3-56cb-843b-fdcdcfd61f2d", "ipAddress": "100.37.49.47", "requestId": "a5bc2125-e5d0-4dea-8d6a-73a4f4142f91"}, "additionalInfo": {"tool": "Digital calendar (Google Calendar, iCal, others)", "reachout": "No thanks.", "timestamp": "2020-11-18T00:45:50.095Z", "take_booking": "Yes", "merchant_uuid": "0714514d-6432-42d8-b972-a616bc1f6983"}}

{"restricted": {}, "additionalInfo": {"request": {"name": "JP Impressions", "website": "http://jpimpressions.com/", "contacts": [{"role": "full_access", "email": "info.jpimpressions@gmail.com", "phone": "(408) 298-7532", "lastName": "Pratt", "firstName": "John", "servicesAndTools": false, "promotionsAndEvents": false, "surveysAndSatisfaction": false}], "locations": [{"city": "San Jose", "state": "AL", "country": "US", "postcode": "95113", "streetAddress": "367 S 1st St."}], "application": "merchantCenter", "utmCampaign": "merchant-blog-how-to-sell-post", "noOfLocations": 1, "businessCategory": "03c81dea-3a6a-4f6b-9b72-1b4ff655d9e0"}, "response": {"id": "bb6965b9-be93-4906-999b-cbe393143e04", "name": "JP Impressions", "website": "http://jpimpressions.com/", "writeups": [{"type": "Traditional", "facet": "Main", "title": "JP Impressions", "locale": "en_US", "html_text": "<p></p>", "facet_type_id": "04212a50-dbfc-1037-bf7b-9b2d44a6a26e"}], "permalink": "jp-impressions", "place_ids": [], "created_at": 1571077980000, "updated_at": 1571077987000, "version_id": "042521f0-dbfc-1037-bcef-4c1d2d7f8193", "feature_country": "US", "extended_attributes": {}, "redemption_processes": [], "salesforce_account_id": "0013c00001pZKm3AAG", "page_self_service_status": "UNKNOWN"}}}



{"restricted": {"bCookie": "489c308d-e9b5-f433-7344-f9d9cb379e87", "ipAddress": "47.222.230.104", "requestId": "558791b6-3a5f-4e22-b5a2-d2d89d30f133"}, "additionalInfo": {"tool": "Pen & paper calendar (offline)", "reachout": "No thanks.", "timestamp": "2020-11-20T07:50:08.315Z", "take_booking": "Yes", "merchant_uuid": "bfeaad3b-6f07-434c-b303-3435f8106549"}}

{"restricted": {"bCookie": "32664f24-fb1b-8107-3272-83d602d484da", "ipAddress": "173.18.114.71", "requestId": "bda9dfd9-6c71-4c96-aded-71708800b999"}, "additionalInfo": {"tool": "Massage Book", "reachout": "Yes put me in touch with my Account Manager.", "timestamp": "2020-11-30T22:55:33.375Z", "take_booking": "Yes", "merchant_uuid": "d8bdef05-aa8a-4009-a1df-1b9d7bb3a539"}}


CREATE SET TABLE sb_merchant_experience.history_event ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      history_id CHAR(36) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
      event_type VARCHAR(256) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
      event_type_id CHAR(36) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
      event_date TIMESTAMP(6),
      user_type VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
      user_id CHAR(36) CHARACTER SET LATIN NOT CASESPECIFIC,
      client_id VARCHAR(256) CHARACTER SET LATIN NOT CASESPECIFIC,
      system_id VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
      device_id CHAR(36) CHARACTER SET LATIN NOT CASESPECIFIC,
      merchant_id CHAR(36) CHARACTER SET LATIN NOT CASESPECIFIC,
      deal_id CHAR(36) CHARACTER SET LATIN NOT CASESPECIFIC,
      history_data JSON(62000) INLINE LENGTH 29232 CHARACTER SET UNICODE)
PRIMARY INDEX ( history_id );



show table edwprod.dim_merchants_unity;

CREATE SET TABLE edwprod.dim_merchants_unity ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      merchant_uuid VARCHAR(256) CHARACTER SET LATIN NOT CASESPECIFIC,
      name VARCHAR(512) CHARACTER SET UNICODE NOT CASESPECIFIC,
      website VARCHAR(512) CHARACTER SET UNICODE NOT CASESPECIFIC,
      salesforce_account_id VARCHAR(256) CHARACTER SET LATIN NOT CASESPECIFIC,
      feature_country INTEGER,
      created_at TIMESTAMP(0),
      updated_at TIMESTAMP(0),
      dwh_created_at TIMESTAMP(0),
      valid_start_dt TIMESTAMP(0),
      valid_end_dt TIMESTAMP(0),
      description_exists INTEGER,
      description_word_count INTEGER,
      process_key BIGINT,
      dwh_active INTEGER)
PRIMARY INDEX ( merchant_uuid );


select * from dwh_base_sec_view.sf_account;

REPLACE VIEW dwh_base_sec_view.sf_account AS
/******************************
** Name: dwh_base_sec_view.sf_account
** Desc: Direct view on base tables
** C2 data NULL valued for GDPR
** Author: ttosun@groupon.com
** Date: 2018-04-25
**************************
** Change History
**************************
** Date Author Description
** ---------- --------- ------------------------------------
*******************************/
LOCK ROW FOR ACCESS
SELECT
Id
,dwh_valid_from
,dwh_valid_before
,dwh_created_at
,Account_ID_18
,Account_Imported_From
,Account_Manager
,null as Account_Number --nulled for GDPR
,Account_Status
,Acct_Owner_Change_Date
,Acct_Status_Change_Date
,Acct_Status_Override
,null as Agreement_Bank_Detail --nulled for GDPR
,null as Approved_for_Case_Stu --nulled for GDPR
,null as Approved_for_PR_Contact --nulled for GDPR
,Avg_Gen_Rvw_Score_Outof5
,Avg_Rvw_Score_Outof5
,null as Bank_Name --nulled for GDPR
,Best_Of
,Best_of_Division
,Best_Of_Eligibility
,BillingCity
,BillingCountry
,BillingPostalCode
,BillingStreet
,Bookings_Target
,Business_Development
,Business_Phone
,Campaign_Name
,Category_v3
,CF_Upload
,Chain_Name
,Cloned_from_Accountname
,Coffee_First_Clicked
,Coffee_Pressed_At
,Coffee_Pressed_By
,Commission_First_Feature
,Company_Legal_Name
,Company_Type
,Competitor_Features
,ConnectionReceivedId
,ConnectionSentId
,Created_Date_Custom
,CreatedById
,CreatedDate
,null as Credit_Check --nulled for GDPR
,Credit_Check_Date
,CS_Merchant_Point_Person
,CurrencyIsoCode
,Current_POS_Cost
,Current_POS_Units
,Current_Processor
,Current_Scheduling_Tool
,Custom_Merchant_Photo
,Custom_Photo_Email_Pr
,Customer_Referral_Fro
,Date_Account_Went_Inactive
,Date_of_Hotness
,Date_Removed_from_MP
,Deal_Creation_Process
,Deal_Source
,Deassignment_Warning
,Decision_made_at_Fran
,Descion_made_at_Corpo
,null as Direct_Deposit_Opt_In --nulled for GDPR
,Division
,DNR_Reason
,null as Dropship_Blacklist --nulled for GDPR
,DSM
,EchoSign_Locale
,Editorial_Manager
,Eligible_for_Refeature
,Eligible_for_Scheduler
,Escalation_Status
,Evaluate_Merchant_Status
,Expedia_Chain
,Expedia_Number_of_Rooms
,Exp_Ovrl_Satisf_Ratng
,Expedia_Star_Rating
,Exposure_Risk
,Family_Channel_Eligib
,Feature_Country
,First_Competitor_Feature
,First_Groupon_Feature_Date
,G1_Rejected_Travel_Ac
,G1_Services_Override_
,Get_Featured
,Getaways_ID
,Getaways_Merch_Region
,Getaways_Sales_Region
,Getaways_Sales_RegionQL2
,Google_Streetview_Rat
,Groupon_Chamber_of_Co
,Groupon_DM
,Groupon_Scheduler_Strength
,Has_Banking_Object
,has_child
,High_Risk_Subcategory
,Home_Channel_Eligible
,Hot_Lead
,Hotel_Star_Rating
,Inbound_Team
,IsDeleted
,Last_Phone_Bank_Call
,Last_Voucher_Sold_Date
,LastActivityDate
,LastModifiedById
,LastModifiedDate
,Lead_Quality
,Lead_Researcher
,Lead_Source_Custom
,LG_Duplicate
,Locking_Reason
,Marketing_Internal_Us
,Marquee_Merchant
,MasterRecordId
,Member_Status
,Merchant_Attributes
,Merchant_Nutshell_Loa
,Merchant_Partner_Progress
,Merchant_Payment_Rejected_Date
,Merchant_Permalink
,Merchant_Profile_Load
,Merchant_Tier
,Merchant_Value
,Merchant_will_be_a_re
,Most_Recent_Competito
,Most_Recent_Expiry_Date
,Most_Recent_Feature
,Most_Recent_Last_Close_Date
,Most_Recent_QL_Resolver
,MQS
,Name
,Named_accounts
,National_Account
,National_Vertical
,Nominated_for_MP
,Not_Likely_to_Refeatu
,NPSID
,Number_of_Features
,Number_of_Total_Opptys
,of_Locations
,One_Feature_Only
,Online_Booking_Opt_Out_Reason
,Other_Phone
,Overall_Status
,Override_Acct_Transfe
,OwnerId
,Parent_Relationship
,ParentId
,Partner_Number
,Partner_Reviews
,Partner_Website
,Pay_On_Redemption_Onl
,Payment_Preference
,Payments_Activation_Date
,Payments_Close_Date
,Payments_Handoff_Made
,Payments_Interested
,Payments_Lead_Source
,Payments_Manager
,Payments_Prdt_Rejectd_Rsns
,Payments_Product_Sales_Status
,Phone
,Date_of_Incorporation
,Pinned
,Pitched_by_Living_Soc
,POS_Interest
,POS_Lead_Gen_Speciali
,POS_Lead_Source
,POS_Manager
,PPSS_Contract_Sent_Date_Time
,PPSS_Needs_Follow_Up
,PPSS_Negotiatng_Prc_Date_Time
,PPSS_Non_Interested_Date_Time
,Preferred_Contact_Language
,Previous_Account_Owner
,Previous_Human_Owner
,Property_Decision_Making
,Prospect_Quality_Scor
,Proxy_Company
,QL2_ID
,QL_Do_Not_Deassign
,QL_Do_Not_Deassign_Reason
,QL_Flag_Status
,Quality_of_Location
,Quote_Approved_for_Ex
,Reason_for_Hotness
,Red_Flagged
,Referred_by_Acct_Owner
,Region
,Rej_Groupon_not_intersd
,Remove_from_MP
,Research_Ranking
,Research_Ranking_Date
,Reserve_Offered
,Result_of_Merchant_Su
,ROI_First_Clicked
,ROI_Viewed_At
,ROI_Viewed_By
,ROI_Viewed_By_Merchant_At
,Sales_Intelligence_An
,Scheduler_Account_Active_Date
,Scheduler_Account_Ref
,Scheduler_Account_Ref_Date
,Scheduler_Invite_Sent
,Scheduler_Setup_Type
,Scheduler_Specialist
,Send_Nets_on_Manifest
,Services_Offered
,Sinai_Merchant
,Sinai_Terms_Date
,Sourced_Date
,Subcategory_v3
,Subdivision
,SystemModstamp
,null as Tax_Identification_Nu --nulled for GDPR
,Ticketmaster_Venue
,TIN_Status
,TIN_Suspect
,TMC_wave
,Top_100_Venue
,Top_20_Account
,Top_Merchant_Campaign
,Total_GP
,Total_Revenue
,Total_Units_Sold
,Travel_Account_Score
,Travel_Account_Type
,Travel_Channel_Eligib
,Type__c
,Update_Do_Not_Use
,VAT
,VAT_code
,VAT_Exemption
,null as VAT_Number --nulled for GDPR
,Village_Vine
,Website
,Yellow_Fields
,Colorguard_Notes__c
,BillingState
,Merchant_Segmentation__c
,partition_id
,dwh_active
,CURRENT_TIMESTAMP(3) AS dwh_modified_at
from edwprod.sf_account;
