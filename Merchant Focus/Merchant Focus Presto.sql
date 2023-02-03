select 
    max(dt)
from 
   grp_gdoop_sup_analytics_db.jc_w2l_funnel_events
;

select * from grp_gdoop_bizops_db.np_temp_launch;

select 
   * 
from grp_gdoop_sup_analytics_db.sm_w2l_closes where dt = '2020-02-19';



SELECT
     *
FROM grp_gdoop_pde.junohourly
WHERE 
  eventdate = '2022-12-02'
  and rawpagetype like '%dashboard%'
  and lower(pageapp) = 'ios-mobile-merchant'
  AND country ='US'
  and userbrowserid = '3C037D6D-B680-4CBA-9E81-57DEEB6BE638'
  limit 100
;

select distinct dt from grp_gdoop_sup_analytics_db.sm_w2l_mia_traffic order by dt;

select distinct dt from grp_gdoop_sup_analytics_db.sm_w2l_leads order by dt

select distinct week from grp_gdoop_sup_analytics_db.smm_w2l_kpis order by week;

select * from grp_gdoop_bizops_db.sm_w2l_mktg_acct_attrib;

select 
week_end
from 
(select 
  week_end,
  count(distinct report_mth) xyz
from grp_gdoop_sup_analytics_db.np_temp_sheet_import
group by 1) as fin
where xyz > 1
order by 1 desc;

select distinct * from grp_gdoop_sup_analytics_db.np_temp_sheet_import where week_end = '2022-10-02 23:59:59.000';


select * from grp_gdoop_bizops_db.np_temp_close;

select * 
from grp_gdoop_bizops_db.np_temp_launch 
where launch_week >= '2022-11-01';

select * from grp_gdoop_bizops_db.sm_ss_unique_bcookie_utm;

select * from grp_gdoop_bizops_db.sm_ss_bcookies;

select distinct campaign_group  from grp_gdoop_sup_analytics_db.sm_w2l_leads;

select distinct campaign_group from grp_gdoop_sup_analytics_db.sm_w2l_mia_traffic;

select distinct campaign_group  from grp_gdoop_sup_analytics_db.smm_w2l_kpis;

select max(event_time) from grp_gdoop_sup_analytics_db.jc_w2l_funnel_events;

    date_sub(next_day(launch_date, 'MON'), 1)

    , campaign_name__c
    , campaign_type
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
    , mkt.highest_touch
    
    
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
 
    
select 
   fin.*
from 
(select 
    deal_uuid, 
    count(distinct campaign_name__c) xyz 
from grp_gdoop_bizops_db.np_temp_launch
group by deal_uuid) as fin
where xyz > 1;

select campaign_name__c, campaign_group  from grp_gdoop_bizops_db.np_temp_launch where deal_uuid = '74e318a9-a504-404b-bfcf-7521fc39aebc';
select * from grp_gdoop_sup_analytics_db.sm_w2l_mktg_acct_attrib where accountid = '0013c00001nEdE0AAK';

select * 
from grp_gdoop_bizops_db.np_temp_launch 
where launch_week >= '2022-10-01';

drop table grp_gdoop_bizops_db.np_temp_close;
create table grp_gdoop_bizops_db.np_temp_close stored as orc as
  select
    date_sub(next_day(c.close_date, 'MON'), 1) launch_week
    , c.close_date as dt
    , case when c.feature_country = 'GB' then 'UK' else c.feature_country end feature_country
    , c.accountid
    , c.deal_uuid
    , mtd_attribution
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
    , mkt.highest_touch
  from grp_gdoop_sup_analytics_db.jc_merchant_mtd_attrib c
  left join grp_gdoop_sup_analytics_db.sm_w2l_mktg_acct_attrib mkt
    on c.accountid = mkt.accountid
    where c.feature_country in ( 'US', 'CA')
      and c.grt_l1_cat_name = 'L1 - Local'
      and (lower(c.mtd_attribution) like '%metro%' or lower(c.mtd_attribution_intl) like '%metro%')
      and c.close_order = 1
      and c.por_relaunch = 0;


drop table grp_gdoop_bizops_db.np_temp_launch;
create table grp_gdoop_bizops_db.np_temp_launch stored as orc as
  select
    date_sub(next_day(c.launch_date, 'MON'), 1) launch_week
    ,c.launch_date as dt
    , case when c.feature_country = 'GB' then 'UK' else c.feature_country end feature_country
    , c.accountid
    , c.deal_uuid
    , mtd_attribution
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
    , mkt.highest_touch
from 
grp_gdoop_sup_analytics_db.jc_merchant_mtd_attrib c
left join grp_gdoop_sup_analytics_db.sm_w2l_mktg_acct_attrib mkt on c.accountid = mkt.accountid
    where c.feature_country in ( 'US', 'CA')
      and c.grt_l1_cat_name = 'L1 - Local'
      and (lower(c.mtd_attribution) like '%metro%' or lower(c.mtd_attribution_intl) like '%metro%')
      and c.launch_order = 1
      and c.launch_date is not null
      and c.por_relaunch = 0;
     
create table sm_w2l_launches stored as orc as
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
  from jc_merchant_mtd_attrib c
  left join sm_w2l_mktg_acct_attrib mkt
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
    , case when lower(mkt.acct_metal) in ('silver', 'gold', 'sliver', 'platinum') then 'S+' else 'B-' end;

     
  
select 
distinct 
launch_week, 
dt, 
feature_country, 
accountid, 
deal_uuid
from  
grp_gdoop_bizops_db.np_temp_launch
;


select * from grp_gdoop_bizops_db.np_temp_launch where accountid = '0013c0000222n4zAAA';

select account_id, dmapi_flag, close_order 
from grp_gdoop_sup_analytics_db.jc_merchant_deal_order 
where 
account_id in ('0013c000021QaL5AAK'
,'0013c0000222n4zAAA'
,'0013c00002234RWAAY'
,'0013c000021QAZJAA4'
,'0013c0000222qOiAAI'
,'0013c0000222zq0AAA'
,'0013c00002231UzAAI' )
;



------need to break down this logic and understand how its classified into metro 

CASE
             WHEN (dl.convertedaccountid IS NOT NULL OR (ma.dmapi_flag = 1 AND ma.close_order = 1)) AND LOWER(ma.opportunity_name) NOT LIKE '%dmapi%' THEN 'Metro Lead - Sales Team Close'
             WHEN (dl.convertedaccountid IS NOT NULL OR (ma.dmapi_flag = 1 AND ma.close_order = 1)) AND ma.close_order > 1 THEN 'Existing Metro'
             WHEN (dl.convertedaccountid IS NOT NULL OR (ma.dmapi_flag = 1 AND ma.close_order = 1)) AND ma.close_order = 1 THEN 'New Metro'
             WHEN day_prev_owner.mtd_attribution IS NOT NULL THEN day_prev_owner.mtd_attribution
             ELSE close_owner.mtd_attribution
         END mtd_attribution,
         
         


     join user_dw.v_dim_day dd
on date_format(c.week,'yyyy-MM-dd') = dd.day_rw
join user_dw.v_dim_week wk
on dd.week_key = wk.week_key

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
     
     
select * from grp_gdoop_sup_analytics_db.sm_w2l_mktg_acct_attrib limit 5;
     
     
select 
*
from
(select 
deal_uuid, 
count(1) xyz 
from grp_gdoop_sup_analytics_db.jc_merchant_mtd_attrib
group by 1) as f 
where xyz > 1;


select 
distinct 
a.*
from grp_gdoop_sup_analytics_db.jc_merchant_mtd_attrib as a
where accountid ='001C000001l50BnIAI';

select 
feature_country,
deal_uuid,
close_order
from grp_gdoop_sup_analytics_db.jc_merchant_mtd_attrib where accountid = '0013c00001nljhQAAQ'
group by 1,2,3
order by 3

select 
* 
from grp_gdoop_sup_analytics_db.jc_merchant_mtd_attrib
where deal_uuid ='db540bca-8790-4d01-ae16-7ebeb786da52';

select 
   
  from grp_gdoop_sup_analytics_db.jc_merchant_mtd_attrib c
    on c.accountid = mkt.accountid
    where c.feature_country in ('GB', 'UK', 'US', 'AU', 'PL', 'FR', 'IT', 'AE', 'DE', 'ES', 'CA')
      and c.grt_l1_cat_name = 'L1 - Local'
      and (lower(c.mtd_attribution) like '%metro%' or lower(c.mtd_attribution_intl) like '%metro%')
      and c.close_order = 1
      and c.por_relaunch = 0


