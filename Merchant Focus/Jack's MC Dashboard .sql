 join grp_gdoop_bizops_db.pai_merchants pm on a.merchant_uuid = pm.merchant_uuid--a.accountid = pm.account_id
  left join grp_gdoop_sup_analytics_db.jc_w2l_mktg_acct_attrib mkt on mkt.accountid = pm.account_id
  left join grp_gdoop_bizops_db.sg_merchant_event_agg_v pma on pma.merchant_id = pm.merchant_uuid
      --and pma.event_date between a.start_date and date_add(a.start_date, 30)
  join user_dw.v_dim_day dd on date_format(a.sent_date,'yyyy-MM-dd') = dd.day_rw
  join user_dw.v_dim_week wk on dd.week_key = wk.week_key
; 

select * from grp_gdoop_sup_analytics_db.sm_w2l_closes where mktg_sem_type is null limit 5;
--------------SG'S CODE

create table grp_gdoop_bizops_db.np_lc_mnth_tmp1 stored as orc as
select
        sfe.merchant_uuid
        , emailname as utm_campaign
        , journeyname
        , emailname
        , case when journeyname like'%MM_LeadNurture%' then 'Lifecycle_LeadNurture'
            when journeyname like'%MM_ONBOARDING%' then 'Lifecycle_Onboarding'
            when journeyname like'%MM_Retention%' then 'Lifecycle_Retention'
            when journeyname like'%IMC%' then 'IMC'
            when journeyname like'MM_%' then 'Other'
            Else 'Unknown'
        end as Journey_Category
        , case when emailname like '%_NA_%' then 'North America'
        when emailname like '%_UK_%' then 'United Kingdom'
        when emailname like '%_GB_%' then 'United Kingdom'
        when emailname like '%_FR_%' then 'France'
        when emailname like '%_IT_%' then 'Italy'
        when emailname like '%_DE_%' then 'Germany'
        when emailname like '%_AU_%' then 'Australia'
        when emailname like '%_PL_%' then 'Poland'
        when emailname like '%_ES_%' then 'Spain'
        when emailname like '%_AE_%' then 'United Arab Emirates'
        when emailname like '%_NL_%' then 'Netherlands'
        when emailname like '%_BE_%' then 'Belgium'
        when emailname like '%_CA_%' then 'Canada'
        when emailname like '%_US_%' then 'North America'
        when emailname like '%_IE_%' then 'Ireland'
        when emailname like '%_INTL_%' then 'International'
        Else 'Other'
        end as Engagement_Country
        , date_format(sentdate, 'yyyy-MM-dd') sent_date
        , date_format(firstopendate, 'yyyy-MM-dd') open_date
        , date_format(firstclickdate, 'yyyy-MM-dd') click_date
        , date_format(mc.first_visit, 'yyyy-MM-dd') visit_date
      from grp_gdoop_bizops_db.sfmc_emailengagement sfe
      left join (
        select merchant_uuid, utm_campaign, min(eventdate) first_visit
        from grp_gdoop_bizops_db.pai_merchant_center_visits
        where utm_campaign is not null
        group by merchant_uuid,utm_campaign
      ) mc on sfe.merchant_uuid = mc.merchant_uuid and mc.utm_campaign = sfe.emailname
      where lower(sfe.journeyname) not like '%newsletter%' and lower(sfe.journeyname) not like 'op_%';

SET hive.auto.convert.join=false;
SET mapred.reduce.tasks=503;
SET mapreduce.job.reduces=503;
SET mapreduce.input.fileinputformat.split.minsize=1;
SET mapreduce.input.fileinputformat.split.maxsize=10000000;
SET tez.grouping.min-size=1;
SET tez.grouping.max-size=10000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.merge.size.per.task=32000000;
set hive.exec.reducers.bytes.per.reducer=5000000;

---sujith
select distinct merchant_uuid as merchant_id
      , case when dmapi_flag = 1 then 'ss_deal_close'
          else 'rep_deal_close'
        end as event
      , close_date as event_date
      , null as sub_event_detail
    from grp_gdoop_sup_analytics_db.jc_merchant_mtd_attrib a
    join grp_gdoop_bizops_db.pai_merchants pm on a.accountid = pm.account_id
    where close_date >= '2021-01-01' 
    and merchant_uuid is not null;

----jack 
grp_gdoop_sup_analytics_db.jc_merchant_mtd_attrib c
  left join (
      select deal_id
      from grp_gdoop_sup_analytics_db.metro_history_event
      where event_type = 'DRAFT_DEAL_CREATION'
        and get_json_object(history_data, '$.additionalInfo.deal.clonedFrom') is not null
    ) dupe
    on dupe.deal_id = c.deal_uuid
where 
 and c.grt_l1_cat_name = 'L1 - Local'
    and c.por_relaunch = 0
    and c.dmapi_flag = 1
    and c.close_date > '2020-01-01'

    
select * from grp_gdoop_sup_analytics_db.jc_merchant_mtd_attrib where accountid  = '001C000001637s7IAA';
select * from grp_gdoop_sup_analytics_db.metro_history_event; 
select 
     JSON_EXTRACT_SCALAR(history_data, '$.additionalInfo.deal.clonedFrom'),
     a.*
from grp_gdoop_sup_analytics_db.metro_history_event as a
where event_type = 'DRAFT_DEAL_CREATION';

select deal_id
from grp_gdoop_sup_analytics_db.metro_history_event
where event_type = 'DRAFT_DEAL_CREATION'
and get_json_object(history_data, '$.additionalInfo.deal.clonedFrom') is not null

select * from grp_gdoop_sup_analytics_db.metro_history_event where deal_id = 'b74bcffa-01ea-4065-a58e-5cc17f21d22e';





select * from grp_gdoop_bizops_db.np_lc_mnth_tmp1 where merchant_uuid = 'd627ba03-076e-66b9-a7f4-f00a3b7c5a98';
select * from grp_gdoop_bizops_db.sg_merchant_event_agg_v 
where merchant_id = 'd627ba03-076e-66b9-a7f4-f00a3b7c5a98' and event_date >= '2022-09-01' and event = 'ss_deal_close';
select * from grp_gdoop_bizops_db.np_lifecycle_monthly_deals_tmp;

select * from grp_gdoop_bizops_db.np_metro_mtd_closes_dl where accounts_closed = '001C000001637s7IAA';
select * from grp_gdoop_bizops_db.pai_merchants where substr(account_id, 1,15) = '001C000001637s7';
select * from grp_gdoop_bizops_db.pai_merchants where merchant_uuid = 'd2b263a2-aed8-11e1-8e54-00259060b612';
select * from grp_gdoop_bizops_db.pai_deals where merchant_uuid = '966f63aa-a7db-48cc-bcbf-30583b67d220';
select * from grp_gdoop_bizops_db.pai_deals where opportunity_id = '001C00000162nrG';


select 
   *
from 
(select 
c.*, 
case when dupe.deal_id is not null then 1 else 0 end cloned_deals
from grp_gdoop_sup_analytics_db.jc_merchant_mtd_attrib c
left join (
      select deal_id,
             JSON_EXTRACT_SCALAR(history_data, '$.additionalInfo.deal.clonedFrom') cloned
      from grp_gdoop_sup_analytics_db.metro_history_event
      where event_type = 'DRAFT_DEAL_CREATION'
        and JSON_EXTRACT_SCALAR(history_data, '$.additionalInfo.deal.clonedFrom') is not null
    ) dupe on dupe.deal_id = c.deal_uuid
where 
     c.feature_country in ('GB', 'UK', 'US', 'AU', 'PL', 'FR', 'IT', 'AE', 'DE', 'ES', 'CA')
    and c.grt_l1_cat_name = 'L1 - Local'
    and c.por_relaunch = 0
    and c.dmapi_flag = 1
    and c.close_date > '2020-01-01') xyz 
   where cloned_deals = 1;

select 
    deal_close_opens
 from grp_gdoop_bizops_db.np_lifecycle_monthly_deals_tmp 
 where start_of_month = '2022-09-01' 
 group by deal_close_opens;

select 
   b.merchant_uuid, 
   a.accounts_closed,
   wk
from grp_gdoop_bizops_db.np_metro_mtd_closes_dl as a 
left join grp_gdoop_bizops_db.pai_merchants as b on a.accounts_closed = b.account_id
where substr(wk, 1, 4) = '2022'
and substr(wk, 6,2) in ('09', '10', '11') 
group by b.merchant_uuid, wk, a.accounts_closed

select 
   count(distinct a.merchant_uuid) merchants_closed, 
   count(distinct b.deal_close_opens) deal_close_opens
from 
(select 
   b.merchant_uuid
from grp_gdoop_bizops_db.np_metro_mtd_closes_dl as a 
left join grp_gdoop_bizops_db.pai_merchants as b on a.accounts_closed = b.account_id
where substr(wk, 1, 4) = '2022'
and substr(wk, 6,2) in ('09', '10', '11') 
group by b.merchant_uuid) as a 
left join
(select 
    deal_close_opens
 from grp_gdoop_bizops_db.np_lifecycle_monthly_deals_tmp 
 where start_of_month = '2022-09-01' 
 group by deal_close_opens) as b on a.merchant_uuid = b.deal_close_opens;





create table grp_gdoop_bizops_db.np_lifecycle_monthly_deals_tmp stored as orc as
select
distinct 
 year(cast(sent_date as date)) as Year
,month(cast(sent_date as date)) as month
,case when month(cast(sent_date as date)) = 1 then 'January'
      when month(cast(sent_date as date)) = 2 then 'February'
      when month(cast(sent_date as date)) = 3 then 'March'
      when month(cast(sent_date as date)) = 4 then 'April'
      when month(cast(sent_date as date)) = 5 then 'May'
      when month(cast(sent_date as date)) = 6 then 'June'
      when month(cast(sent_date as date)) = 7 then 'July'
      when month(cast(sent_date as date)) = 8 then 'August'
      when month(cast(sent_date as date)) = 9 then 'September'
      when month(cast(sent_date as date)) = 10 then 'October'
      when month(cast(sent_date as date)) = 11 then 'November'
      when month(cast(sent_date as date)) = 12 then 'December'
end as MTH
,concat(substr(sent_date,1,8),'01') as start_of_month
,a.utm_campaign
, a.journeyname
, a.emailname
, a.Journey_Category
, case when mkt.accountid is not null then 'Metro' else pm.acct_owner end as account_owner
, case when pm.current_metal_segment in ('Silver', 'Gold', 'Platinum') then 'S+' else 'B-' end as metal_group
, case when sent_date < first_launch_date then 'Pre-Feature' else 'Post-Feature' end as new_v_existing
, pm.l2 as vertical
, a.Engagement_Country
, a.merchant_uuid merchants_sent
, case when a.open_date is not null then a.merchant_uuid end merchants_open
, case when a.click_date is not null then a.merchant_uuid end merchants_click
, case when a.visit_date is not null then a.merchant_uuid end merchants_visit
, case when pma.event IN ('ss_deal_start') and pma.event_date between a.open_date and date_add(a.open_date, 360) then pma.merchant_id end deal_start_opens
, case when pma.event IN ('ss_deal_start') and pma.event_date between a.click_date and date_add(a.click_date, 360) then pma.merchant_id end deal_start_clicks
, case when pma.event IN ('ss_deal_start') and pma.event_date between a.visit_date and date_add(a.visit_date, 360) then pma.merchant_id end deal_start_visits
, case when pma.event IN ('ss_deal_close') and pma.event_date between a.open_date and date_add(a.open_date, 360) then pma.merchant_id end deal_close_opens
, case when pma.event IN ('ss_deal_close') and pma.event_date between a.click_date and date_add(a.click_date, 360) then pma.merchant_id end deal_close_clicks
, case when pma.event IN ('ss_deal_close') and pma.event_date between a.visit_date and date_add(a.visit_date, 360) then pma.merchant_id end deal_close_visits
, case when pma.event IN ('ss_deal_launch') and pma.event_date between a.open_date and date_add(a.open_date, 360) then pma.merchant_id end deal_launch_opens
, case when pma.event IN ('ss_deal_launch') and pma.event_date between a.click_date and date_add(a.click_date, 360) then pma.merchant_id end deal_launch_clicks
, case when pma.event IN ('ss_deal_launch') and pma.event_date between a.visit_date and date_add(a.visit_date, 360) then pma.merchant_id end deal_launch_visits
  from 
  grp_gdoop_bizops_db.np_lc_mnth_tmp1 as a 
  join grp_gdoop_bizops_db.pai_merchants pm on a.merchant_uuid = pm.merchant_uuid
  left join (select accountid from grp_gdoop_sup_analytics_db.jc_w2l_mktg_acct_attrib group by accountid) mkt on mkt.accountid = pm.account_id
  left join grp_gdoop_bizops_db.sg_merchant_event_agg_v pma on pma.merchant_id = pm.merchant_uuid
  join user_dw.v_dim_day dd on date_format(a.sent_date,'yyyy-MM-dd') = dd.day_rw
  join user_dw.v_dim_week wk on dd.week_key = wk.week_key
  where pm.l1 = 'Local'


---------------USERS 

select * from grp_gdoop_sup_analytics_db.jc_merchant_mtd_attrib where accountid = '001C0000017iLwDIAU' order by close_recency;
select * from grp_gdoop_sup_analytics_db.jc_cb_funnel_agg;

drop table if exists grp_gdoop_sup_analytics_db.jc_metro_mtd_mc_users;
create table grp_gdoop_sup_analytics_db.jc_metro_mtd_mc_users stored as orc as
  select
    date_format(wk.week_end,'yyyy-MM-dd') wk
    , case when mc.country_code in ('US','CA') and mtd.mtd_attribution = 'BD' then 'MD'
        when mc.country_code in ('US','CA') then mtd.mtd_attribution
        when mc.country_code <> 'US' then mtd.mtd_attribution_intl
        when mtd.accountid is null and mkt.accountid is not null then 'New Metro'
      end mtd_attribution
    , mtd.vertical
    , case when mc.country_code in ('UK' ,'GB') then 'GB' else mc.country_code end page_country
    , pm.current_metal_segment as metal_at_close
    , coalesce(mkt.campaign_group, 'Direct / Referral / Other') as campaign_group
    , case 
	    when mkt.pd_mm_relaunch_flag = 1 then mkt.mktg_test_division
        else 'Non Paid Re-Launch Campaign' end mktg_test_division
    , mkt.pd_mm_relaunch_flag
    , case 
	    when t_o.tier = 1 then 'All Offers'
        when t_o.tier = 2 then 'All Deals'
        when t_o.tier = 3 then 'Mix Deals/Offers'
        else 'Non-TO'
      end to_tier
    , cast(null as bigint) as deal_last_touch
    , cast(null as bigint) as deal_last_touch_group
    , count(distinct mc.merchant_uuid) mc_users
  from grp_gdoop_bizops_db.pai_merchant_center_visits mc
  join grp_gdoop_bizops_db.pai_merchants pm on mc.merchant_uuid = pm.merchant_uuid
  left join (select * from grp_gdoop_sup_analytics_db.jc_merchant_mtd_attrib where close_recency = 1) mtd  on mtd.accountid = pm.account_id
  left join grp_gdoop_sup_analytics_db.jc_w2l_mktg_acct_attrib mkt on mkt.accountid = pm.account_id
  left join grp_gdoop_sup_analytics_db.eh_to_closes t_o on t_o.deal_uuid = mtd.deal_uuid
  join user_dw.v_dim_day dd on date_format(mc.eventdate, 'yyyy-MM-dd') = dd.day_rw
  join user_dw.v_dim_week wk on dd.week_key = wk.week_key
  where mc.country_code in ('GB', 'UK', 'US', 'AU', 'PL', 'FR', 'IT', 'AE', 'DE', 'ES', 'CA')
  group by 1,2,3,4,5,6,7,8,9,10,11
;

select * from grp_gdoop_sup_analytics_db.jc_merchant_mtd_attrib;
select * from grp_gdoop_sup_analytics_db.jc_w2l_mktg_acct_attrib where accountid = '0013c000021GTFoAAO';
select * from grp_gdoop_sup_analytics_db.jc_ss_mm_deal_attrib;



-----------START
select 
   distinct 
      merchant_id, 'ss_deal_start' as event, 
      date_format(event_date,'yyyy-MM-dd') as event_date, 
      null as sub_event_detail
from grp_gdoop_sup_analytics_db.metro_history_event
where event_type = 'DRAFT_DEAL_CREATION'
      and event_date >= '2021-01-01'
      and merchant_id is not null

      
drop table if exists grp_gdoop_sup_analytics_db.jc_metro_deal_starts;
create table grp_gdoop_sup_analytics_db.jc_metro_deal_starts stored as orc as
  select
    date_format(wk.week_end,'yyyy-MM-dd') wk
    , case when cb.country in ('US','CA') and mtd.mtd_attribution = 'BD' then 'MD'
        when cb.country in ('US','CA') then mtd.mtd_attribution
        when cb.country <> 'US' then mtd.mtd_attribution_intl
        when mtd.accountid is null and mkt.accountid is not null then 'New Metro'
      end mtd_attribution
    , grt.grt_l2_cat_name as vertical
    , case when cb.country in ('UK' ,'GB') then 'GB' else cb.country end page_country
    , pm.current_metal_segment as metal_at_close
    , coalesce(campaign_group, 'Direct / Referral / Other') as campaign_group
    , case when mkt.pd_mm_relaunch_flag = 1 then mkt.mktg_test_division
        else 'Non Paid Re-Launch Campaign' end mktg_test_division
    , mkt.pd_mm_relaunch_flag
    , case when t_o.tier = 1 then 'All Offers'
        when t_o.tier = 2 then 'All Deals'
        when t_o.tier = 3 then 'Mix Deals/Offers'
        else 'Non-TO'
      end to_tier
    , cast(null as bigint) as deal_last_touch
    , cast(null as bigint) as deal_last_touch_group
    , count(distinct cb.deal_id) deal_starts
  from (
        select
          distinct deal_id
          , event_date
          , merchant_id as merchant_uuid
          , get_json_object(history_data, '$.additionalInfo.places[0].country') country
          , get_json_object(he.history_data, '$.additionalInfo.deal.primaryDealServiceId') pds_cat_id
        from grp_gdoop_sup_analytics_db.metro_history_event he
        where event_type = 'DRAFT_DEAL_CREATION'
    ) cb
  join grp_gdoop_bizops_db.pai_merchants pm on cb.merchant_uuid = pm.merchant_uuid
  join user_dw.v_dim_pds_grt_map grt on grt.pds_cat_id = cb.pds_cat_id
  left join (
      select deal_id
      from grp_gdoop_sup_analytics_db.metro_history_event
      where event_type = 'DRAFT_DEAL_CREATION'
        and get_json_object(history_data, '$.additionalInfo.deal.clonedFrom') is not null
    ) dupe
    on dupe.deal_id = cb.deal_id
  left join (select * from grp_gdoop_sup_analytics_db.jc_merchant_mtd_attrib where close_recency = 1) mtd 
    on mtd.accountid = pm.account_id
  left join grp_gdoop_sup_analytics_db.jc_w2l_mktg_acct_attrib   on mkt.accountid = pm.account_id
  left join grp_gdoop_sup_analytics_db.eh_to_closes t_o on t_o.deal_uuid = mtd.deal_uuid
  join user_dw.v_dim_day dd on date_format(cb.event_date, 'yyyy-MM-dd') = dd.day_rw
  join user_dw.v_dim_week wk on dd.week_key = wk.week_key
  where cb.country in ('GB', 'UK', 'US', 'AU', 'PL', 'FR', 'IT', 'AE', 'DE', 'ES', 'CA')
    and dupe.deal_id is null
  group by 1,2,3,4,5,6,7,8,9,10,11
;

---------CLOSE
select distinct merchant_uuid as merchant_id
      , case when dmapi_flag = 1 then 'ss_deal_close'
          else 'rep_deal_close'
        end as event
      , close_date as event_date
      , null as sub_event_detail
    from grp_gdoop_sup_analytics_db.jc_merchant_mtd_attrib a
    join grp_gdoop_bizops_db.pai_merchants pm on a.accountid = pm.account_id
    where close_date >= '2021-01-01' 
    and merchant_uuid is not null;

select * from grp_gdoop_sup_analytics_db.jc_merchant_mtd_attrib where merchant_uuid = 'd700c7eb-8a67-4b6d-bd78-32a38e57963b'

 and c.grt_l1_cat_name = 'L1 - Local'
    and c.por_relaunch = 0
    and c.dmapi_flag = 1
    and c.close_date > '2020-01-01'

select * from grp_gdoop_sup_analytics_db.jc_merchant_mtd_attrib;

drop table if exists grp_gdoop_sup_analytics_db.jc_metro_mtd_closes;
create table grp_gdoop_sup_analytics_db.jc_metro_mtd_closes stored as orc as
  select
    date_format(wk.week_end,'yyyy-MM-dd') wk
    , case when c.feature_country in ('US', 'CA') and c.mtd_attribution = 'BD' and c.dmapi_flag = 1 then 'MD'
        when c.feature_country in ('US', 'CA') then c.mtd_attribution
        when c.feature_country <> 'US' then c.mtd_attribution_intl
      end mtd_attribution
    , c.vertical
    , c.feature_country
    , da.metal_at_close
    , coalesce(campaign_group, 'Direct / Referral / Other') as campaign_group
    , case when mkt.pd_mm_relaunch_flag = 1 then mkt.mktg_test_division
        else 'Non Paid Re-Launch Campaign' end mktg_test_division
    , mkt.pd_mm_relaunch_flag
    , case when t_o.tier = 1 then 'All Offers'
        when t_o.tier = 2 then 'All Deals'
        when t_o.tier = 3 then 'Mix Deals/Offers'
        else 'Non-TO'
      end to_tier
    , coalesce(mda.last_touch_campaign, 'Other') as deal_last_touch
    , coalesce(mda.last_touch_campaign_group, 'Other') as deal_last_touch_group
    , count(distinct c.deal_uuid) deals_closed
    , count(distinct c.accountid) accounts_closed
  from grp_gdoop_sup_analytics_db.jc_merchant_mtd_attrib c
  left join (
      select deal_id
      from grp_gdoop_sup_analytics_db.metro_history_event
      where event_type = 'DRAFT_DEAL_CREATION'
        and get_json_object(history_data, '$.additionalInfo.deal.clonedFrom') is not null
    ) dupe
    on dupe.deal_id = c.deal_uuid
  left join grp_gdoop_sup_analytics_db.rev_mgmt_deal_attributes da on c.deal_uuid = da.deal_id
  left join grp_gdoop_sup_analytics_db.jc_w2l_mktg_acct_attrib mkt on mkt.accountid = c.accountid
  left join grp_gdoop_sup_analytics_db.eh_to_closes t_o on t_o.deal_uuid = c.deal_uuid
  left join grp_gdoop_sup_analytics_db.jc_ss_mm_deal_attrib mda on mda.deal_uuid = c.deal_uuid
  join user_dw.v_dim_day dd on date_format(c.close_date, 'yyyy-MM-dd') = dd.day_rw
  join user_dw.v_dim_week wk on dd.week_key = wk.week_key
  where c.feature_country in ('GB', 'UK', 'US', 'AU', 'PL', 'FR', 'IT', 'AE', 'DE', 'ES', 'CA')
    and c.grt_l1_cat_name = 'L1 - Local'
    and c.por_relaunch = 0
    and c.dmapi_flag = 1
    and c.close_date > '2020-01-01'
    and dupe.deal_id is null
  group by 1,2,3,4,5,6,7,8,9,10,11
;

select 
*
from grp_gdoop_sup_analytics_db.metro_history_event


--REPLICA CLOSE

select * from grp_gdoop_bizops_db.np_lifecycle_monthly_deals_tmp;

select * from grp_gdoop_bizops_db.np_metro_mtd_closes_dl;

drop table grp_gdoop_bizops_db.np_metro_mtd_closes_dl;
create table grp_gdoop_bizops_db.np_metro_mtd_closes_dl stored as orc as
  select
    distinct 
    date_format(wk.week_end,'yyyy-MM-dd') wk
    , case when c.feature_country in ('US', 'CA') and c.mtd_attribution = 'BD' and c.dmapi_flag = 1 then 'MD'
        when c.feature_country in ('US', 'CA') then c.mtd_attribution
        when c.feature_country <> 'US' then c.mtd_attribution_intl
      end mtd_attribution
    , c.vertical
    , c.feature_country
    , da.metal_at_close
    , coalesce(campaign_group, 'Direct / Referral / Other') as campaign_group
    , case when mkt.pd_mm_relaunch_flag = 1 then mkt.mktg_test_division
        else 'Non Paid Re-Launch Campaign' end mktg_test_division
    , mkt.pd_mm_relaunch_flag
    , case when t_o.tier = 1 then 'All Offers'
        when t_o.tier = 2 then 'All Deals'
        when t_o.tier = 3 then 'Mix Deals/Offers'
        else 'Non-TO'
      end to_tier
    , coalesce(mda.last_touch_campaign, 'Other') as deal_last_touch
    , coalesce(mda.last_touch_campaign_group, 'Other') as deal_last_touch_group
    , c.deal_uuid deals_closed
    , c.accountid accounts_closed
  from grp_gdoop_sup_analytics_db.jc_merchant_mtd_attrib c
  left join (
      select deal_id
      from grp_gdoop_sup_analytics_db.metro_history_event
      where event_type = 'DRAFT_DEAL_CREATION'
        and get_json_object(history_data, '$.additionalInfo.deal.clonedFrom') is not null
    ) dupe
    on dupe.deal_id = c.deal_uuid
  left join grp_gdoop_sup_analytics_db.rev_mgmt_deal_attributes da
    on c.deal_uuid = da.deal_id
  left join grp_gdoop_sup_analytics_db.jc_w2l_mktg_acct_attrib mkt
    on mkt.accountid = c.accountid
  left join grp_gdoop_sup_analytics_db.eh_to_closes t_o
    on t_o.deal_uuid = c.deal_uuid
  left join grp_gdoop_sup_analytics_db.jc_ss_mm_deal_attrib mda
    on mda.deal_uuid = c.deal_uuid
  join user_dw.v_dim_day dd
    on date_format(c.close_date, 'yyyy-MM-dd') = dd.day_rw
  join user_dw.v_dim_week wk
    on dd.week_key = wk.week_key
  where c.feature_country in ('GB', 'UK', 'US', 'AU', 'PL', 'FR', 'IT', 'AE', 'DE', 'ES', 'CA')
    and c.grt_l1_cat_name = 'L1 - Local'
    and c.por_relaunch = 0
    and c.dmapi_flag = 1
    and c.close_date > '2020-01-01'
    and dupe.deal_id is null
;





----LAUNCHES

drop table if exists grp_gdoop_sup_analytics_db.jc_metro_mtd_launches;
create table grp_gdoop_sup_analytics_db.jc_metro_mtd_launches stored as orc as
  select
    date_format(wk.week_end,'yyyy-MM-dd') wk
    , case when c.feature_country in ('US', 'CA') and c.mtd_attribution = 'BD' and c.dmapi_flag = 1 then 'MD'
        when c.feature_country in ('US', 'CA') then c.mtd_attribution
        when c.feature_country <> 'US' then c.mtd_attribution_intl
      end mtd_attribution
    , c.vertical
    , c.feature_country
    , da.metal_at_close
    , coalesce(campaign_group, 'Direct / Referral / Other') as campaign_group
    , case when mkt.pd_mm_relaunch_flag = 1 then mkt.mktg_test_division
        else 'Non Paid Re-Launch Campaign' end mktg_test_division
    , mkt.pd_mm_relaunch_flag
    , case when t_o.tier = 1 then 'All Offers'
        when t_o.tier = 2 then 'All Deals'
        when t_o.tier = 3 then 'Mix Deals/Offers'
        else 'Non-TO'
      end to_tier
    , coalesce(mda.last_touch_campaign, 'Other') as deal_last_touch
    , coalesce(mda.last_touch_campaign_group, 'Other') as deal_last_touch_group
    , count(distinct c.deal_uuid) deals_launched
    , count(distinct c.accountid) accounts_launched
  from grp_gdoop_sup_analytics_db.jc_merchant_mtd_attrib c
  left join grp_gdoop_sup_analytics_db.rev_mgmt_deal_attributes da on c.deal_uuid = da.deal_id
  left join grp_gdoop_sup_analytics_db.jc_w2l_mktg_acct_attrib mkt on mkt.accountid = c.accountid
  left join grp_gdoop_sup_analytics_db.eh_to_closes t_o on t_o.deal_uuid = c.deal_uuid
  left join grp_gdoop_sup_analytics_db.jc_ss_mm_deal_attrib mda on mda.deal_uuid = c.deal_uuid
  join user_dw.v_dim_day dd on date_format(c.launch_date, 'yyyy-MM-dd') = dd.day_rw
  join user_dw.v_dim_week wk on dd.week_key = wk.week_key
  where c.feature_country in ('GB', 'UK', 'US', 'AU', 'PL', 'FR', 'IT', 'AE', 'DE', 'ES', 'CA')
    and c.grt_l1_cat_name = 'L1 - Local'
    and c.por_relaunch = 0
    and c.dmapi_flag = 1
    and c.launch_date is not null
    and c.launch_date > '2020-01-01'
  group by 1,2,3,4,5,6,7,8,9,10,11
;

drop table if exists grp_gdoop_sup_analytics_db.jc_metro_mtd;
create table grp_gdoop_sup_analytics_db.jc_metro_mtd stored as orc as
  select
    coalesce(a.wk, b.wk, c.wk, d.wk) week
    , coalesce(a.vertical, b.vertical, c.vertical, d.vertical) vertical
    , coalesce(a.mtd_attribution, b.mtd_attribution, c.mtd_attribution, d.mtd_attribution) mtd_attribution
    , coalesce(a.page_country, b.feature_country, c.feature_country, d.page_country) feature_country
    , coalesce(a.metal_at_close, b.metal_at_close, c.metal_at_close, d.metal_at_close) rev_management_metal_at_close
    , coalesce(a.campaign_group, b.campaign_group, c.campaign_group, d.campaign_group, 'Direct / Referral / Other') campaign_group
    , coalesce(a.mktg_test_division, b.mktg_test_division, c.mktg_test_division, d.mktg_test_division) mktg_test_division
    , coalesce(a.pd_mm_relaunch_flag, b.pd_mm_relaunch_flag, c.pd_mm_relaunch_flag, d.pd_mm_relaunch_flag, 0) pd_mm_relaunch_flag
    , coalesce(a.to_tier, b.to_tier, c.to_tier, d.to_tier) to_tier
    , coalesce(a.deal_last_touch, b.deal_last_touch, c.deal_last_touch, d.deal_last_touch) deal_last_touch
    , coalesce(a.deal_last_touch_group, b.deal_last_touch_group, c.deal_last_touch_group, d.deal_last_touch_group) deal_last_touch_group
    , sum(mc_users) mc_users
    , sum(deal_starts) deal_starts
    , sum(deals_closed) deals_closed
    , sum(accounts_closed) accounts_closed
    , sum(deals_launched) deals_launched
    , sum(accounts_launched) accounts_launched
  from grp_gdoop_sup_analytics_db.jc_metro_mtd_mc_users a
  full outer join grp_gdoop_sup_analytics_db.jc_metro_deal_starts d
    on a.wk = d.wk
      and a.vertical = d.vertical
      and a.mtd_attribution = d.mtd_attribution
      and a.page_country = d.page_country
      and a.metal_at_close = d.metal_at_close
      and a.campaign_group = d.campaign_group
      and a.mktg_test_division = d.mktg_test_division
      and a.pd_mm_relaunch_flag = d.pd_mm_relaunch_flag
      and a.to_tier = d.to_tier
      and a.deal_last_touch = d.deal_last_touch
      and a.deal_last_touch_group = d.deal_last_touch_group
  full outer join grp_gdoop_sup_analytics_db.jc_metro_mtd_closes b
    on a.wk = b.wk
      and a.vertical = b.vertical
      and a.mtd_attribution = b.mtd_attribution
      and a.page_country = b.feature_country
      and a.metal_at_close = b.metal_at_close
      and a.campaign_group = b.campaign_group
      and a.mktg_test_division = b.mktg_test_division
      and a.pd_mm_relaunch_flag = b.pd_mm_relaunch_flag
      and a.to_tier = b.to_tier
      and a.deal_last_touch = b.deal_last_touch
      and a.deal_last_touch_group = b.deal_last_touch_group
  full outer join grp_gdoop_sup_analytics_db.jc_metro_mtd_launches c
  on a.wk = c.wk
    and a.vertical = c.vertical
    and a.mtd_attribution = c.mtd_attribution
    and a.page_country = c.feature_country
    and a.metal_at_close = c.metal_at_close
    and a.campaign_group = c.campaign_group
    and a.mktg_test_division = c.mktg_test_division
    and a.pd_mm_relaunch_flag = c.pd_mm_relaunch_flag
    and a.to_tier = c.to_tier
    and a.deal_last_touch = c.deal_last_touch
    and a.deal_last_touch_group = c.deal_last_touch_group
  where coalesce(a.wk, b.wk, c.wk, d.wk) < current_date
  group by 1,2,3,4,5,6,7,8,9,10,11
;
