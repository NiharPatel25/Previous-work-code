
select distinct
   cast('close' as varchar(64)) category,
   trunc(cast(close_date as date), 'iw')+6  date_week, 
   campaign_name, 
   campaign_type, 
   campaign_group_lvl1_1, 
   campaign_group_lvl1_2, 
   campaign_paid_category, 
   mktg_txny_version, 
   mktg_country,
   mktg_test_division,
   mktg_traffic_source,
   mktg_audience,
   mktg_sem_type,
   mktg_platform,
   mktg_creative,
   country_code,
   case when country_code = 'US' then 'NAM' else 'INTL' end region,
   metal, 
   account_id
from np_temp_acc_cl_lnch
where close_order = 1 and country_code in ( 'US', 'CA')
and trunc(cast(close_date as date), 'iw')+6 > cast('2022-10-01' as date)
;

select distinct
   cast('launch' as varchar(64)) category,
   trunc(cast(launch_date as date), 'iw')+6 date_week,
   campaign_name, 
   campaign_type, 
   campaign_group_lvl1_1, 
   campaign_group_lvl1_2, 
   campaign_paid_category, 
   mktg_txny_version, 
   mktg_country,
   mktg_test_division,
   mktg_traffic_source,
   mktg_audience,
   mktg_sem_type,
   mktg_platform,
   mktg_creative,
   country_code,
   case when country_code = 'US' then 'NAM' else 'INTL' end region,
   metal, 
   account_id
from np_temp_acc_cl_lnch
where launch_order = 1 and country_code in ( 'US', 'CA')
and trunc(cast(close_date as date), 'iw')+6 > cast('2022-10-01' as date);

select * 
from sandbox.pai_lead_opp_mtd_attrib 
where account_id = '0013c00001xrQGzAAM'
AND country_code in ('GB', 'UK', 'US', 'AU', 'PL', 'FR', 'IT', 'AE', 'DE', 'ES', 'CA')
      and (LOWER(leadsource) = 'metro - self service' or (dmapi_flag = 1 and close_order = 1))
;

select * from sandbox.pai_leads where ConvertedAccountId  = '0013c00001xrQGzAAM';
select * from sandbox.pai_opp_mtd_attrib where account_id = '0013c00001xrQGzAAM';

select * 
from sandbox.pai_lead_opp_mtd_attrib 
where account_id = '0013c00001xrQGzAAM';

---0013c0000231JOQAA2

select * from np_temp_acc_cl_lnch where account_id = '0013c00001xrQGzAAM';
select * from sandbox.pai_lead_opp_mtd_attrib where account_id = '0013c0000231esrAAA';
select * from sandbox.np_merch_lead_asgnmt where account_id = '0013c00002312JsAAI';
select * from sandbox.pai_opp_mtd_attrib where account_id = '0013c00001xrQGzAAM';
select * from sandbox.pai_leads where ConvertedAccountId  = '0013c00001xrQGzAAM';
select * from user_groupondw.sf_lead 
WHERE lower(leadsource) like any ('%mia%','%metro%') AND convertedaccountid = '0013c00002312JsAAI'
order by CAST(createddate AS DATE) desc;

select * from user_groupondw.sf_lead 
WHERE lower(leadsource) like any ('%mia%','%metro%') AND convertedaccountid = '0013c00002312JsAAI'
order by CAST(createddate AS DATE) desc;



CASE
      WHEN (dl.convertedaccountid IS NOT NULL OR (ma.dmapi_flag = 1 AND ma.close_order = 1)) AND LOWER(ma.opportunity_name) NOT LIKE '%dmapi%' THEN 'Metro Lead - Sales Team Close'
      WHEN (dl.convertedaccountid IS NOT NULL OR (ma.dmapi_flag = 1 AND ma.close_order = 1)) AND ma.close_order > 1 THEN 'Existing Metro'
      WHEN (dl.convertedaccountid IS NOT NULL OR (ma.dmapi_flag = 1 AND ma.close_order = 1)) AND ma.close_order = 1 THEN 'New Metro'
      WHEN day_prev_owner.mtd_attribution IS NOT NULL THEN day_prev_owner.mtd_attribution
      ELSE close_owner.mtd_attribution
END mtd_attribution,

drop table np_temp_acc_cl_lnch;
create multiset volatile table np_temp_acc_cl_lnch as 
(select 
   CloseDate close_date, 
   launch_date launch_date,
   c.campaign_name, 
   c.campaign_group_lvl2 campaign_type, 
   c.campaign_group_lvl1_1, 
   c.campaign_group_lvl1_2, 
   c.campaign_paid_category, 
   c.mktg_txny_version, 
   c.mktg_country,
   case when c.campaign_paid_category = 1 then c.mktg_test_division else 'Non Paid Re-Launch Campaign' end mktg_test_division,
   c.mktg_traffic_source,
   c.mktg_audience,
   c.mktg_sem_type,
   c.mktg_platform,
   c.mktg_creative,
   c.country_code,
   case when lower(c.metal) in ('silver', 'gold', 'sliver', 'platinum') then 'S+' else 'B-' end metal,
   account_id,
   close_order, 
   launch_order, 
   mtd_attribution
from sandbox.pai_lead_opp_mtd_attrib as c
where country_code in ('GB', 'UK', 'US', 'AU', 'PL', 'FR', 'IT', 'AE', 'DE', 'ES', 'CA')
      and lower(mtd_attribution) like '%metro%'
      and c.grt_l1_cat_name = 'L1 - Local'
      and c.por_relaunch = 0
) with data on commit preserve rows;


drop table sandbox.np_ld_cl_lnch_wk;
create multiset table sandbox.np_ld_cl_lnch_wk as (
select 
  cast('lead' as varchar(64)) category,
  trunc(cast(lead_create_date as date), 'iw')+6 date_week,
  a.campaign_name, 
  a.campaign_group_lvl2 campaign_type, 
  a.campaign_group_lvl1_1, 
  a.campaign_group_lvl1_2, 
  a.campaign_paid_category, 
  a.mktg_txny_version, 
  a.mktg_country,
  case when a.campaign_paid_category = 1 then a.mktg_test_division else 'Non Paid Re-Launch Campaign' end mktg_test_division,
  a.mktg_traffic_source,
  a.mktg_audience,
  a.mktg_sem_type,
  a.mktg_platform,
  a.mktg_creative,
  a.country_code, 
  case when a.country_code = 'US' then 'NAM' else 'INTL' end region,
  case when lower(sfa.merchant_segmentation__c) in ('silver', 'gold', 'sliver', 'platinum') then 'S+' else 'B-' end metal,
  count(distinct ConvertedAccountId) leads
from 
sandbox.pai_leads as a
left join dwh_base_sec_view.sf_account sfa on sfa.account_id_18 = a.convertedaccountid
where trunc(cast(lead_create_date as date), 'iw')+6 < trunc(cast(current_date as date), 'iw')+6 
group by 
1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
union all 
select 
   cast('close' as varchar(64)) category,
   trunc(cast(close_date as date), 'iw')+6  date_week, 
   campaign_name, 
   campaign_type, 
   campaign_group_lvl1_1, 
   campaign_group_lvl1_2, 
   campaign_paid_category, 
   mktg_txny_version, 
   mktg_country,
   mktg_test_division,
   mktg_traffic_source,
   mktg_audience,
   mktg_sem_type,
   mktg_platform,
   mktg_creative,
   country_code,
   case when country_code = 'US' then 'NAM' else 'INTL' end region,
   metal, 
   count(distinct account_id) as metric
from np_temp_acc_cl_lnch
where close_order = 1 and trunc(cast(close_date as date), 'iw')+6 < trunc(cast(current_date as date), 'iw')+6 
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
union all 
select 
   cast('launch' as varchar(64)) category,
   trunc(cast(launch_date as date), 'iw')+6 date_week,
   campaign_name, 
   campaign_type, 
   campaign_group_lvl1_1, 
   campaign_group_lvl1_2, 
   campaign_paid_category, 
   mktg_txny_version, 
   mktg_country,
   mktg_test_division,
   mktg_traffic_source,
   mktg_audience,
   mktg_sem_type,
   mktg_platform,
   mktg_creative,
   country_code,
   case when country_code = 'US' then 'NAM' else 'INTL' end region,
   metal, 
   count(distinct account_id) as metric
from np_temp_acc_cl_lnch
where launch_order = 1 and trunc(cast(launch_date as date), 'iw')+6 < trunc(cast(current_date as date), 'iw')+6 
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
) with data;



drop table sandbox.np_ld_cl_lnch_mnth;
create multiset table sandbox.np_ld_cl_lnch_mnth as (
select 
  cast('lead' as varchar(64)) category,
  cast(lead_create_date as date) - EXTRACT(DAY FROM cast(lead_create_date as date)) + 1 date_month,
  a.campaign_name, 
  a.campaign_group_lvl2 campaign_type, 
  a.campaign_group_lvl1_1, 
  a.campaign_group_lvl1_2, 
  a.campaign_paid_category, 
  a.mktg_txny_version, 
  a.mktg_country,
  case when a.campaign_paid_category = 1 then a.mktg_test_division else 'Non Paid Re-Launch Campaign' end mktg_test_division,
  a.mktg_traffic_source,
  a.mktg_audience,
  a.mktg_sem_type,
  a.mktg_platform,
  a.mktg_creative,
  a.country_code, 
  case when a.country_code = 'US' then 'NAM' else 'INTL' end region,
  case when lower(sfa.merchant_segmentation__c) in ('silver', 'gold', 'sliver', 'platinum') then 'S+' else 'B-' end metal,
  count(distinct ConvertedAccountId) leads
from 
sandbox.pai_leads as a
left join dwh_base_sec_view.sf_account sfa on sfa.account_id_18 = a.convertedaccountid
group by 
1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
union all 
select 
   cast('close' as varchar(64)) category,
   cast(close_date as date) - EXTRACT(DAY FROM cast(close_date as date)) + 1 date_month,
   campaign_name, 
   campaign_type, 
   campaign_group_lvl1_1, 
   campaign_group_lvl1_2, 
   campaign_paid_category, 
   mktg_txny_version, 
   mktg_country,
   mktg_test_division,
   mktg_traffic_source,
   mktg_audience,
   mktg_sem_type,
   mktg_platform,
   mktg_creative,
   country_code,
   case when country_code = 'US' then 'NAM' else 'INTL' end region,
   metal, 
   count(distinct account_id) as metric
from np_temp_acc_cl_lnch
where close_order = 1
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
union all 
select 
   cast('launch' as varchar(64)) category,
   cast(launch_date as date) - EXTRACT(DAY FROM cast(launch_date as date)) + 1 date_month,
   campaign_name, 
   campaign_type, 
   campaign_group_lvl1_1, 
   campaign_group_lvl1_2, 
   campaign_paid_category, 
   mktg_txny_version, 
   mktg_country,
   mktg_test_division,
   mktg_traffic_source,
   mktg_audience,
   mktg_sem_type,
   mktg_platform,
   mktg_creative,
   country_code,
   case when country_code = 'US' then 'NAM' else 'INTL' end region,
   metal, 
   count(distinct account_id) as metric
from np_temp_acc_cl_lnch
where launch_order = 1
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
) with data;




select * from sandbox.np_ld_cl_lnch_wk;
select * from sandbox.np_ld_cl_lnch_mnth;

------leads



select
    sf.createddate as dt
    , campaign_name__c
    , campaign_type
    , coalesce(campaign_group, 'Direct / Referral / Other') as campaign_group
    , pd_mm_relaunch_flag
    , mktg_txny_version
    , mktg_country
    , case when pd_mm_relaunch_flag = 1 then mktg_test_division
        else 'Non Paid Re-Launch Campaign' end mktg_test_division
    , mktg_traffic_source
    , mktg_audience
    , mktg_sem_type
    , mktg_platform
    , mktg_creative
    , sf.highest_touch
    , case when sf.country_code = 'GB' then 'UK' else sf.country_code end feature_country
    , case when lower(sf.acct_metal) in ('silver', 'gold', 'sliver', 'platinum') then 'S+' else 'B-' end metal
    , count(distinct sf.accountid) leads
    , count(distinct case when lower(sf.acct_metal) in ('silver', 'gold', 'platinum') then sf.accountid end) s_plus_leads
  from sm_w2l_mktg_acct_attrib sf
  group by 
  sf.createddate
    , campaign_name__c
    , campaign_type
    , coalesce(campaign_group, 'Direct / Referral / Other')
    , pd_mm_relaunch_flag
    , mktg_txny_version
    , mktg_country
    , case when pd_mm_relaunch_flag = 1 then mktg_test_division
        else 'Non Paid Re-Launch Campaign' end
    , mktg_traffic_source
    , mktg_audience
    , mktg_sem_type
    , mktg_platform
    , mktg_creative
    , sf.highest_touch
    , case when sf.country_code = 'GB' then 'UK' else sf.country_code end
    , case when lower(sf.acct_metal) in ('silver', 'gold', 'sliver', 'platinum') then 'S+' else 'B-' end;
   
   
   
   
-----------------------CLOSES 
WHEN (dl.convertedaccountid IS NOT NULL OR (ma.dmapi_flag = 1 AND ma.close_order = 1)) AND LOWER(ma.opportunity_name) NOT LIKE '%dmapi%' THEN 'Metro Lead - Sales Team Close'
WHEN (dl.convertedaccountid IS NOT NULL OR (ma.dmapi_flag = 1 AND ma.close_order = 1)) AND ma.close_order > 1 THEN 'Existing Metro'
WHEN (dl.convertedaccountid IS NOT NULL OR (ma.dmapi_flag = 1 AND ma.close_order = 1)) AND ma.close_order = 1 THEN 'New Metro'
   
select * from sandbox.pai_lead_opp_mtd_attrib 
where dmapi_flag = 1 
and LOWER(opportunity_name) NOT LIKE '%dmapi%';



select 
   trunc(launch_date, 'iw') + 6 launch_week, 
   case when LOWER(leadsource) = 'metro - self service' and lower(opportunity_name) not like '%dmapi%' then 'Metro Lead - Sales Team Close'
        when LOWER(leadsource) = 'metro - self service'
   count(distinct account_id) total_accs
from sandbox.pai_lead_opp_mtd_attrib as c
where country_code in ( 'US', 'CA')
      and c.grt_l1_cat_name = 'L1 - Local'
      and c.launch_order = 1
      and c.launch_date is not null
      and c.por_relaunch = 0
      and (LOWER(leadsource) = 'metro - self service' or (dmapi_flag = 1 and close_order = 1))
group by 1
order by 1 desc;
   
select * from sandbox.pai_lead_opp_mtd_attrib;
---case when c.feature_country = 'US' then 'NAM' else 'INTL' end region




select 
*
from sandbox.pai_lead_opp_mtd_attrib as c
where
    country_code in ('GB', 'UK', 'US', 'AU', 'PL', 'FR', 'IT', 'AE', 'DE', 'ES', 'CA')
    and c.grt_l1_cat_name = 'L1 - Local'
    and c.close_order = 1
    and c.por_relaunch = 0
    and LOWER(leadsource) = 'metro - self service';
   
select account_id_18, count(1) cnts, count(distinct Merchant_Segmentation__c) xyz  
from dwh_base_sec_view.sf_account 
group by 1 
having cnts > 1;

-----close 
c.feature_country in ('GB', 'UK', 'US', 'AU', 'PL', 'FR', 'IT', 'AE', 'DE', 'ES', 'CA')
      and c.grt_l1_cat_name = 'L1 - Local'
      and (lower(c.mtd_attribution) like '%metro%' or lower(c.mtd_attribution_intl) like '%metro%')
      and c.close_order = 1
      and c.por_relaunch = 0
      
-----launch
where c.feature_country in ('GB', 'UK', 'US', 'AU', 'PL', 'FR', 'IT', 'AE', 'DE', 'ES', 'CA')
      and c.grt_l1_cat_name = 'L1 - Local'
      and (lower(c.mtd_attribution) like '%metro%' or lower(c.mtd_attribution_intl) like '%metro%')
      and c.launch_order = 1
      and c.launch_date is not null
      and c.por_relaunch = 0
   
   
 drop table if exists grp_gdoop_sup_analytics_db.sm_w2l_closes;
 create table sm_w2l_closes stored as orc as
  select
    c.close_date as dt
    , campaign_name__c
    , campaign_type
    , coalesce(campaign_group, 'Direct / Referral / Other') as campaign_group
    , pd_mm_relaunch_flag
    , mktg_txny_version
    , mktg_country
    , case when mkt.pd_mm_relaunch_flag = 1 then mkt.mktg_test_division
        else 'Non Paid Re-Launch Campaign' end mktg_test_division
    , mktg_traffic_source
    , mktg_audience
    , mktg_sem_type
    , mktg_platform
    , mktg_creative
    , case when c.feature_country = 'GB' then 'UK' else c.feature_country end feature_country
    , mkt.highest_touch
    , case when lower(mkt.acct_metal) in ('silver', 'gold', 'sliver', 'platinum') then 'S+' else 'B-' end metal
    , count(distinct c.accountid) closes
  from grp_gdoop_sup_analytics_db.jc_merchant_mtd_attrib c
  left join grp_gdoop_sup_analytics_db.sm_w2l_mktg_acct_attrib mkt
    on c.accountid = mkt.accountid
    where c.feature_country in ('GB', 'UK', 'US', 'AU', 'PL', 'FR', 'IT', 'AE', 'DE', 'ES', 'CA')
      and c.grt_l1_cat_name = 'L1 - Local'
      and (lower(c.mtd_attribution) like '%metro%' or lower(c.mtd_attribution_intl) like '%metro%')
      and c.close_order = 1
      and c.por_relaunch = 0
  group by 
     c.close_date
    , campaign_name__c
    , campaign_type
    , coalesce(campaign_group, 'Direct / Referral / Other')
    , pd_mm_relaunch_flag
    , mktg_txny_version
    , mktg_country
    , case when mkt.pd_mm_relaunch_flag = 1 then mkt.mktg_test_division
        else 'Non Paid Re-Launch Campaign' end
    , mktg_traffic_source
    , mktg_audience
    , mktg_sem_type
    , mktg_platform
    , mktg_creative
    , case when c.feature_country = 'GB' then 'UK' else c.feature_country end
    , mkt.highest_touch
    , case when lower(mkt.acct_metal) in ('silver', 'gold', 'sliver', 'platinum') then 'S+' else 'B-' end


  select
    c.launch_date as dt
    , campaign_name__c
    , campaign_type
    , coalesce(campaign_group, 'Direct / Referral / Other') as campaign_group
    , pd_mm_relaunch_flag
    , mktg_txny_version
    , mktg_country
    , case when mkt.pd_mm_relaunch_flag = 1 then mkt.mktg_test_division
        else 'Non Paid Re-Launch Campaign' end mktg_test_division
    , mktg_traffic_source
    , mktg_audience
    , mktg_sem_type
    , mktg_platform
    , mktg_creative
    , case when c.feature_country = 'GB' then 'UK' else c.feature_country end feature_country
    , mkt.highest_touch
    , case when lower(mkt.acct_metal) in ('silver', 'gold', 'sliver', 'platinum') then 'S+' else 'B-' end metal
    , count(distinct c.accountid) launches
  from grp_gdoop_sup_analytics_db.jc_merchant_mtd_attrib c
  left join grp_gdoop_sup_analytics_db.sm_w2l_mktg_acct_attrib mkt
    on c.accountid = mkt.accountid
    where c.feature_country in ('GB', 'UK', 'US', 'AU', 'PL', 'FR', 'IT', 'AE', 'DE', 'ES', 'CA')
      and c.grt_l1_cat_name = 'L1 - Local'
      and (lower(c.mtd_attribution) like '%metro%' or lower(c.mtd_attribution_intl) like '%metro%')
      and c.launch_order = 1
      and c.launch_date is not null
      and c.por_relaunch = 0
  group by 
     c.launch_date
    , campaign_name__c
    , campaign_type
    , coalesce(campaign_group, 'Direct / Referral / Other')
    , pd_mm_relaunch_flag
    , mktg_txny_version
    , mktg_country
    , case when mkt.pd_mm_relaunch_flag = 1 then mkt.mktg_test_division
        else 'Non Paid Re-Launch Campaign' end
    , mktg_traffic_source
    , mktg_audience
    , mktg_sem_type
    , mktg_platform
    , mktg_creative
    , case when c.feature_country = 'GB' then 'UK' else c.feature_country end
    , mkt.highest_touch
    , case when lower(mkt.acct_metal) in ('silver', 'gold', 'sliver', 'platinum') then 'S+' else 'B-' end 
  
    
    
    
    
  