-----------------------------CODE FOR COMPARISON - ENGAGEMENT MERCHANT DETAIL 
---jack's code

---ss_deal_start --- grp_gdoop_sup_analytics_db.metro_history_event
---ss_deal_close --- grp_gdoop_sup_analytics_db.jc_merchant_mtd_attrib 
---ss_deal_launch --- grp_gdoop_bizops_db.pai_deals




set hive.exec.dynamic.partition.mode=nonstrict;set hive.exec.max.dynamic.partitions=2048;
set hive.exec.max.dynamic.partitions.pernode=256;set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;set hive.auto.convert.join.noconditionaltask.size=100000000;
set hive.cbo.enable=true;set hive.stats.fetch.column.stats=true;set hive.stats.fetch.partition.stats=true;
set hive.merge.tezfiles=true;set hive.merge.smallfiles.avgsize=128000000;set hive.merge.size.per.task=128000000;
set hive.tez.container.size=8192;set hive.tez.java.opts=-Xmx6000M;set hive.groupby.orderby.position.alias=true;
set hive.exec.parallel=true;add jar hdfs:///user/grp_gdoop_marketing_analytics/mktg-hive-udf.jar;
add jar hdfs:///user/grp_gdoop_marketing_analytics/scala-library-2.11.6.jar;add jar hdfs:///user/grp_gdoop_marketing_analytics/traffic-source-lib_2.11-1.0.3.jar;
create temporary function TrafficSource as 'com.groupon.marketing.analytics.hive.udf.TrafficSourceUDF';



drop table if exists grp_gdoop_bizops_db.sg_lifecycle_monthly_deals_v purge;
create table grp_gdoop_bizops_db.sg_lifecycle_monthly_deals_v stored as orc as
select
 year(cast(sent_date as date)) as Year
,month(cast(sent_date as date)) as month
,case 
	when month(cast(sent_date as date)) = 1 then 'January'
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
,count(distinct a.merchant_uuid) merchants_sent
,count(distinct case when a.open_date is not null then a.merchant_uuid end) merchants_open
,count(distinct case when a.click_date is not null then a.merchant_uuid end) merchants_click
,count(distinct case when a.visit_date is not null then a.merchant_uuid end) merchants_visit
,count(distinct case when pma.event IN ('ss_deal_start') and pma.event_date between a.open_date and date_add(a.open_date, 360) then pma.merchant_id end) deal_start_merchant_open
,count(distinct case when pma.event IN ('ss_deal_start') and pma.event_date between a.click_date and date_add(a.click_date, 360) then pma.merchant_id end) deal_start_merchant_click
,count(distinct case when pma.event IN ('ss_deal_start') and pma.event_date between a.visit_date and date_add(a.visit_date, 360) then pma.merchant_id end) deal_start_merchant_visit
,count(distinct case when pma.event IN ('ss_deal_close') and pma.event_date between a.open_date and date_add(a.open_date, 360) then pma.merchant_id end) deal_close_merchant_open
,count(distinct case when pma.event IN ('ss_deal_close') and pma.event_date between a.click_date and date_add(a.click_date, 360) then pma.merchant_id end) deal_close_merchant_click
,count(distinct case when pma.event IN ('ss_deal_close') and pma.event_date between a.visit_date and date_add(a.visit_date, 360) then pma.merchant_id end) deal_close_merchant_visit
,count(distinct case when pma.event IN ('ss_deal_launch') and pma.event_date between a.open_date and date_add(a.open_date, 360) then pma.merchant_id end) deal_launch_merchant_open
,count(distinct case when pma.event IN ('ss_deal_launch') and pma.event_date between a.click_date and date_add(a.click_date, 360) then pma.merchant_id end) deal_launch_merchant_click
,count(distinct case when pma.event IN ('ss_deal_launch') and pma.event_date between a.visit_date and date_add(a.visit_date, 360) then pma.merchant_id end) deal_launch_merchant_visit
,count(distinct case when pma.event IN ('ss_deal_start') and pma.event_date between a.open_date and date_add(a.open_date, 360) then pma.merchant_id end) deal_start_opens
,count(distinct case when pma.event IN ('ss_deal_start') and pma.event_date between a.click_date and date_add(a.click_date, 360) then pma.merchant_id end) deal_start_clicks
,count(distinct case when pma.event IN ('ss_deal_start') and pma.event_date between a.visit_date and date_add(a.visit_date, 360) then pma.merchant_id end) deal_start_visits
,count(distinct case when pma.event IN ('ss_deal_close') and pma.event_date between a.open_date and date_add(a.open_date, 360) then pma.merchant_id end) deal_close_opens
,count(distinct case when pma.event IN ('ss_deal_close') and pma.event_date between a.click_date and date_add(a.click_date, 360) then pma.merchant_id end) deal_close_clicks
,count(distinct case when pma.event IN ('ss_deal_close') and pma.event_date between a.visit_date and date_add(a.visit_date, 360) then pma.merchant_id end) deal_close_visits
,count(distinct case when pma.event IN ('ss_deal_launch') and pma.event_date between a.open_date and date_add(a.open_date, 360) then pma.merchant_id end) deal_launch_opens
,count(distinct case when pma.event IN ('ss_deal_launch') and pma.event_date between a.click_date and date_add(a.click_date, 360) then pma.merchant_id end) deal_launch_clicks
,count(distinct case when pma.event IN ('ss_deal_launch') and pma.event_date between a.visit_date and date_add(a.visit_date, 360) then pma.merchant_id end) deal_launch_visits
  from 
(select
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
      where lower(sfe.journeyname) not like '%newsletter%' and lower(sfe.journeyname) not like 'op_%'
) a
  join grp_gdoop_bizops_db.pai_merchants pm on a.merchant_uuid = pm.merchant_uuid--a.accountid = pm.account_id
  left join grp_gdoop_sup_analytics_db.jc_w2l_mktg_acct_attrib mkt on mkt.accountid = pm.account_id
  left join grp_gdoop_bizops_db.sg_merchant_event_agg_v pma on pma.merchant_id = pm.merchant_uuid
      --and pma.event_date between a.start_date and date_add(a.start_date, 30)
  join user_dw.v_dim_day dd on date_format(a.sent_date,'yyyy-MM-dd') = dd.day_rw
  join user_dw.v_dim_week wk on dd.week_key = wk.week_key
  where pm.l1 = 'Local'
  group by 
        year(cast(sent_date as date)) 
        ,month(cast(sent_date as date))
   ,case
    when month(cast(sent_date as date)) = 1 then 'January'
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
    when month(cast(sent_date as date)) = 12 then 'December' end
,concat(substr(sent_date,1,8),'01')
,a.journeyname
,a.utm_campaign
,a.emailname
,a.Journey_Category
,case when mkt.accountid is not null then 'Metro' else pm.acct_owner end 
,case when pm.current_metal_segment in ('Silver', 'Gold', 'Platinum') then 'S+' else 'B-' end 
,case when sent_date < first_launch_date then 'Pre-Feature' else 'Post-Feature' end 
,pm.l2 
,a.Engagement_Country




-------------------------------------------------------WEEK OVER WEEK DASHBOARD

sg_merchant_event_agg_v

set hive.exec.dynamic.partition.mode=nonstrict;set hive.exec.max.dynamic.partitions=2048;
set hive.exec.max.dynamic.partitions.pernode=256;set hive.auto.convert.join=true;set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=100000000;set hive.cbo.enable=true;set hive.stats.fetch.column.stats=true;
set hive.stats.fetch.partition.stats=true;set hive.merge.tezfiles=true;set hive.merge.smallfiles.avgsize=128000000;
set hive.merge.size.per.task=128000000;set hive.tez.container.size=8192;set hive.tez.java.opts=-Xmx6000M;set hive.groupby.orderby.position.alias=true;
set hive.exec.parallel=true;

add jar hdfs:///user/grp_gdoop_marketing_analytics/mktg-hive-udf.jar;
add jar hdfs:///user/grp_gdoop_marketing_analytics/scala-library-2.11.6.jar;
add jar hdfs:///user/grp_gdoop_marketing_analytics/traffic-source-lib_2.11-1.0.3.jar;
create temporary function TrafficSource as 'com.groupon.marketing.analytics.hive.udf.TrafficSourceUDF';



drop table if exists grp_gdoop_bizops_db.sg_merchant_event_agg_v;
create table grp_gdoop_bizops_db.sg_merchant_event_agg_v stored as orc as
  select *
  from (
    select distinct merchant_id, 'ss_deal_start' as event, date_format(event_date,'yyyy-MM-dd') as event_date, null as sub_event_detail
    from grp_gdoop_sup_analytics_db.metro_history_event
    where event_type = 'DRAFT_DEAL_CREATION'
      and event_date >= '2021-01-01'
      and merchant_id is not null
    union all
    select distinct merchant_uuid as merchant_id
      , case when dmapi_flag = 1 then 'ss_deal_close'
          else 'rep_deal_close'
        end as event
      , close_date as event_date
      , null as sub_event_detail
    from grp_gdoop_sup_analytics_db.jc_merchant_mtd_attrib a
    join grp_gdoop_bizops_db.pai_merchants pm on a.accountid = pm.account_id
    where close_date >= '2021-01-01' and merchant_uuid is not null
union all
    select distinct b.merchant_uuid as merchant_id, 'ss_draft_work' as event, date_format(a.dt, 'yyyy-MM-dd') as event_date, null as sub_event_detail
    from grp_gdoop_sup_analytics_db.jc_cb_funnel_agg a
    join grp_gdoop_sup_analytics_db.jc_cb_deal_starts b on a.deal_uuid = b.deal_uuid
    where images + option_text + highlights + about_business + fineprint
      + redemption_locations + launch_date + bank_tax_info + contract > 0
      and a.dt >= '2021-01-01' and b.merchant_uuid is not null
union all
    select distinct merchant_uuid as merchant_id
      , case when is_self_serve = 1 then 'ss_deal_launch'
          else 'rep_deal_launch'
        end as event
      , launch_date as event_date
      , null as sub_event_detail
    from grp_gdoop_bizops_db.pai_deals
    where launch_date >= '2021-01-01' and merchant_uuid is not null
union all
    -- select distinct merchant_uuid, pause_date as event_date
    --   , case when paused_source = 'deal_estate' then 'de_deal_pause'
    --       when paused_source = 'mdm' then 'mdm_deal_pause'
    --     end as event
    -- from grp_gdoop_bizops_db.jj_paused_deals_all_v
    -- where pause_date >= '2021-01-01'
    --
    -- union all
    select distinct merchant_uuid as merchant_id, 'merchant_live' as event, load_date as event_date, null as sub_event_detail
    from user_groupondw.active_deals ad
    join grp_gdoop_bizops_db.pai_deals pd on ad.deal_uuid = pd.deal_uuid
    where load_date >= '2021-01-01' and merchant_uuid is not null
    union all
select distinct merchant_id as merchant_id
    ,case when event_type in ('PRE_VETTING_UPDATE') then 'pre_vetting_submit'
        when event_type in ('POST_VETTING_STARTED') then 'post_vetting_started'
        end as event
    , date_format(event_date,'yyyy-MM-dd') as event_date, null as sub_event_detail
    from grp_gdoop_sup_analytics_db.metro_history_event
    where event_type in ('PRE_VETTING_UPDATE','POST_VETTING_STARTED') and merchant_id is not null
union all
    select distinct merchant_uuid as merchant_id
      , case when mdm_case_flag = 1 then 'mdm_support_case_opened' when mdm_case_flag = 0 then 'other_support_case_opened' end as event
      , event_date
      , null as sub_event_detail
    from (
        select distinct account_id, date_format(opened_date, 'yyyy-MM-dd') as event_date, mdm_case_flag from grp_gdoop_sup_analytics_db.jc_case_raw  a
        where a.account_id is not null and a.channel = 'Local' and opened_date >= '2021-01-01' and (issue_category not in ('Deal Pause', 'Deal Stop Request') or issue_details <> 'Deal Stop Request')
      ) a
    join grp_gdoop_bizops_db.pai_merchants b on a.account_id = b.account_id
    where merchant_uuid is not null
union all
    select distinct merchant_uuid as merchant_id, 'mc_visit' as event, date_format(eventdate, 'yyyy-MM-dd') as event_date, null as sub_event_detail
    from grp_gdoop_bizops_db.pai_merchant_center_visits
    where eventdate >= '2021-01-01' and merchant_uuid is not null
union all
    select distinct e.merchant_uuid as merchant_id, 'ma_recommendation_accepted' as event, date_format(tr.created_at, 'yyyy-MM-dd') as event_date, null as sub_event_detail
    from grp_gdoop_bizops_db.ma_recommendations_tracker tr
    join grp_gdoop_bizops_db.ma_entities e
      on e.entity_uuid = tr.entity_uuid
    where lower(delete_event_type)='actioned' and e.merchant_uuid is not null
union all
    select distinct merchant_id, event_type as event, date_format(create_datetime, 'yyyy-MM-dd') as event_date, null as sub_event_detail
    from grp_gdoop_bizops_db.np_mc_sssl_tracking
    where date_format(create_datetime, 'yyyy-MM-dd') >= '2021-01-01'
    union all 
    select distinct merchant_id, event, event_date, sub_event_detail
    from  (
    select distinct merchant_uuid as merchant_id
    , 'ss_deal_edits' as event
    , date_format(cd.created_at, 'yyyy-MM-dd') as event_date
    ,case 
    when upper(attribute_id) = 'IMAGES' and original_value <> '{}' then 'deal_image'
    when attribute_id = 'DESCRIPTION' then 'description'
    when upper(attribute_id) = 'END_DATE' and original_value <> '{}' then 'end_date'
    when upper(attribute_id) = 'START_DATE' and original_value <> '{}' then 'start_date'
    when upper(attribute_id) = 'HIGHLIGHTS' and original_value <> '{}' then 'highlights'
    when upper(attribute_id) = 'REDEMPTION_LOCATIONS' then 'redemption_location'
    when upper(attribute_id) = 'CONSUMER_CONTRACT_TERMS' then 'fine_print'
    when attribute_id = 'PRODUCTS' and original_value = '{}' then 'new_opition'
    end as sub_event_detail
        from  grp_gdoop_sup_analytics_db.merchant_self_service_change_details cd
        join  grp_gdoop_sup_analytics_db.merchant_self_service_changes c
        on cd.change_uuid = c.change_uuid
        where cd.created_at >= "2021-01-01"
union all
    select distinct merchant_uuid as merchant_id
    , 'ss_deal_edits' as event
    , date_format(cd.created_at, 'yyyy-MM-dd') as event_date
    ,case 
    when attribute_id = 'PRODUCTS' and get_json_object(changed_value, '$.products.discount') <> get_json_object(original_value, '$.discount.value') then 'discount_percent'
    end as sub_event_detail
    from  grp_gdoop_sup_analytics_db.merchant_self_service_change_details cd
        join  grp_gdoop_sup_analytics_db.merchant_self_service_changes c
        on cd.change_uuid = c.change_uuid
        where cd.created_at >= "2021-01-01"
        and attribute_id = 'PRODUCTS'
union all
    select distinct merchant_uuid as merchant_id
    , 'ss_deal_edits' as event
    , date_format(cd.created_at, 'yyyy-MM-dd') as event_date
    ,case 
    when attribute_id = 'PRODUCTS' and lower(get_json_object(changed_value, '$.products.isReordered')) = 'true' then 'reorder'
    end as sub_event_detail
    from  grp_gdoop_sup_analytics_db.merchant_self_service_change_details cd
        join  grp_gdoop_sup_analytics_db.merchant_self_service_changes c
        on cd.change_uuid = c.change_uuid
        where cd.created_at >= "2021-01-01"
        and attribute_id = 'PRODUCTS'
union all
    select distinct merchant_uuid as merchant_id
    , 'ss_deal_edits' as event
    , date_format(cd.created_at, 'yyyy-MM-dd') as event_date
    ,case 
    when attribute_id = 'PRODUCTS' and lower(get_json_object(changed_value, '$.products.isActive')) = 'false' and lower(get_json_object(original_value, '$.isActive.value')) = 'true'then 'deal_paused'
    when attribute_id = 'PRODUCTS' and lower(get_json_object(changed_value, '$.products.isActive')) = 'true' and lower(get_json_object(original_value, '$.isActive.value')) = 'false'then 'deal_unpaused'
    end as sub_event_detail
    from  grp_gdoop_sup_analytics_db.merchant_self_service_change_details cd
        join  grp_gdoop_sup_analytics_db.merchant_self_service_changes c
        on cd.change_uuid = c.change_uuid
        where cd.created_at >= "2021-01-01"
        and attribute_id = 'PRODUCTS'
union all
    select distinct merchant_uuid as merchant_id
    , 'ss_deal_edits' as event
    , date_format(cd.created_at, 'yyyy-MM-dd') as event_date
    ,case 
    when attribute_id = 'PRODUCTS' and get_json_object(changed_value, '$.products.discountedPrice') > get_json_object(original_value, '$.discountedPrice.value') then 'voucher_price_increase'   
    when attribute_id = 'PRODUCTS' and get_json_object(changed_value, '$.products.discountedPrice') < get_json_object(original_value, '$.discountedPrice.value') then 'voucher_price_decrease'
    end as sub_event_detail
    from  grp_gdoop_sup_analytics_db.merchant_self_service_change_details cd
        join  grp_gdoop_sup_analytics_db.merchant_self_service_changes c
        on cd.change_uuid = c.change_uuid
        where cd.created_at >= "2021-01-01"
        and attribute_id = 'PRODUCTS'
union all
    select distinct merchant_uuid as merchant_id
    , 'ss_deal_edits' as event
    , date_format(cd.created_at, 'yyyy-MM-dd') as event_date
    ,case 
    when attribute_id = 'PRODUCTS' and get_json_object(changed_value, '$.products.maxVoucher') > get_json_object(original_value, '$.maxVoucher.value') then 'voucher_cap_increase'
    when attribute_id = 'PRODUCTS' and get_json_object(changed_value, '$.products.maxVoucher') < get_json_object(original_value, '$.maxVoucher.value') then 'voucher_cap_decrease'
    end as sub_event_detail
    from  grp_gdoop_sup_analytics_db.merchant_self_service_change_details cd
        join  grp_gdoop_sup_analytics_db.merchant_self_service_changes c
        on cd.change_uuid = c.change_uuid
        where cd.created_at >= "2021-01-01"
        and attribute_id = 'PRODUCTS'
    ) a
union all 
    select distinct merchant_uuid, 'bt_booking_created' as Event, load_date as event_date, null as sub_event_detail
    from (
    select pai.merchant_uuid,act.deal_uuid,load_date
    from grp_gdoop_bizops_db.sh_bt_active_deals_log act
    join grp_gdoop_bizops_db.pai_deals pai
    on act.deal_uuid = pai.deal_uuid
    where coalesce(new_bt_opt_in_date,load_date) > "2021-01-01"
    ) a
union all
    select merch_uuid, 'tpis_booking_created' as Event, launch_date as event_date, null as sub_event_detail
    from grp_gdoop_bizops_db.pai_deals a
    join  grp_gdoop_bizops_db.bzops_booking_deals b on a.deal_uuid = b.product_uuid
    where inv_service_id = 'tpis' and exclude_flag = 0
    and launch_date > "2021-01-01"
  ) t
  
  jc_cb_funnel_agg
  
  
set hive.exec.dynamic.partition.mode=nonstrict;set hive.exec.max.dynamic.partitions=2048;
set hive.exec.max.dynamic.partitions.pernode=256;set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;set hive.auto.convert.join.noconditionaltask.size=100000000;
set hive.cbo.enable=true;set hive.stats.fetch.column.stats=true;set hive.stats.fetch.partition.stats=true;set hive.merge.tezfiles=true;s
et hive.merge.smallfiles.avgsize=128000000;set hive.merge.size.per.task=128000000;set hive.tez.container.size=8192;
set hive.tez.java.opts=-Xmx6000M;set hive.groupby.orderby.position.alias=true;set hive.exec.parallel=true;
add jar hdfs:///user/grp_gdoop_marketing_analytics/mktg-hive-udf.jar;add jar hdfs:///user/grp_gdoop_marketing_analytics/scala-library-2.11.6.jar;
add jar hdfs:///user/grp_gdoop_marketing_analytics/traffic-source-lib_2.11-1.0.3.jar;

create temporary function TrafficSource as 'com.groupon.marketing.analytics.hive.udf.TrafficSourceUDF';

SELECT * FROM grp_gdoop_bizops_db.sfmc_emailengagement LIMIT 10;

drop table if exists grp_gdoop_bizops_db.sg_lifecycle_merchant_journeys_v purge;
create table grp_gdoop_bizops_db.sg_lifecycle_merchant_journeys_v stored as orc as
select
    'Email Sends' as Entity_type
    ,a.utm_campaign 
    , a.journeyname
    , a.emailname
    , a.Journey_Category
    , case when mkt.accountid is not null then 'Metro' else pm.acct_owner end as account_owner
    , case when pm.current_metal_segment in ('Silver', 'Gold', 'Platinum') then 'S+' else 'B-' end  as metal_grp
    , case when sent_date < first_launch_date then 'Pre-Feature' else 'Post-Feature' end as new_v_existing
    , pm.l2 as Vertical   
    , a.Engagement_Country
    , date_format(wk.week_end, 'yyyy-MM-dd') week_end_send
    ,'' as event
    ,'' as sub_event_detail
    , count(distinct a.merchant_uuid) merchants_sent
    , count(distinct case when a.open_date is not null then a.merchant_uuid end) merchants_open
    , count(distinct case when a.click_date is not null then a.merchant_uuid end) merchants_click
    , count(distinct case when a.visit_date is not null then a.merchant_uuid end) merchants_visit
    , 0 d1_merchant_actions_open
    , 0 d7_merchant_actions_open
    , 0 d14_merchant_actions_open
    , 0 d21_merchant_actions_open
    , 0 d30_merchant_actions_open
    , 0 d90_merchant_actions_open
    , 0 d180_merchant_actions_open
    , 0 d360_merchant_actions_open
    , 0 d1_merchant_actions_click
    , 0 d7_merchant_actions_click
    , 0 d14_merchant_actions_click
    , 0 d21_merchant_actions_click
    , 0 d30_merchant_actions_click
    , 0 d90_merchant_actions_click
    , 0 d180_merchant_actions_click
    , 0 d360_merchant_actions_click
    , 0 d1_merchant_actions_visit
    , 0 d7_merchant_actions_visit
    , 0 d14_merchant_actions_visit
    , 0 d21_merchant_actions_visit
    , 0 d30_merchant_actions_visit
    , 0 d90_merchant_actions_visit
    , 0 d180_merchant_actions_visit
    , 0 d360_merchant_actions_visit
  from (
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
        when emailname like '%_US_%' then 'United States'
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
      where lower(sfe.journeyname) not like '%newsletter%' and lower(sfe.journeyname) not like 'op_%'
    ) a
  join grp_gdoop_bizops_db.pai_merchants pm on a.merchant_uuid = pm.merchant_uuid--a.accountid = pm.account_id
  left join grp_gdoop_sup_analytics_db.jc_w2l_mktg_acct_attrib mkt on mkt.accountid = pm.account_id
      --and pma.event_date between a.start_date and date_add(a.start_date, 30)
  join user_dw.v_dim_day dd
    on date_format(a.sent_date,'yyyy-MM-dd') = dd.day_rw
  join user_dw.v_dim_week wk
    on dd.week_key = wk.week_key
  where pm.l1 = 'Local'
  group by a.utm_campaign
    , a.journeyname
    , a.emailname
    , Journey_Category
    , case when mkt.accountid is not null then 'Metro' else pm.acct_owner end 
    , case when pm.current_metal_segment in ('Silver', 'Gold', 'Platinum') then 'S+' else 'B-' end 
    , case when sent_date < first_launch_date then 'Pre-Feature' else 'Post-Feature' end 
    , pm.l2 
    , Engagement_Country
    , date_format(wk.week_end, 'yyyy-MM-dd')
union all
select
    'Merchant Actions' as Entity_type
	,a.utm_campaign
    , a.journeyname
    , a.emailname
    , a.Journey_Category
    , case when mkt.accountid is not null then 'Metro' else pm.acct_owner end as account_owner
    , case when pm.current_metal_segment in ('Silver', 'Gold', 'Platinum') then 'S+' else 'B-' end as metal_group
    , case when sent_date < first_launch_date then 'Pre-Feature' else 'Post-Feature' end as new_v_existing
    , pm.l2 as vertical
    , a.Engagement_Country
    , date_format(wk.week_end, 'yyyy-MM-dd') week_end_send
    , pma.event
    , pma.sub_event_detail
    , count(distinct a.merchant_uuid) merchants_sent
    , count(distinct case when a.open_date is not null then a.merchant_uuid end) merchants_open
    , count(distinct case when a.click_date is not null then a.merchant_uuid end) merchants_click
    , count(distinct case when a.visit_date is not null then a.merchant_uuid end) merchants_visit
    , count(distinct case when pma.event_date between a.open_date and date_add(a.open_date, 1) then pma.merchant_id end) d1_merchant_actions_open
    , count(distinct case when pma.event_date between a.open_date and date_add(a.open_date, 7) then pma.merchant_id end) d7_merchant_actions_open
    , count(distinct case when pma.event_date between a.open_date and date_add(a.open_date, 14) then pma.merchant_id end) d14_merchant_actions_open
    , count(distinct case when pma.event_date between a.open_date and date_add(a.open_date, 21) then pma.merchant_id end) d21_merchant_actions_open
    , count(distinct case when pma.event_date between a.open_date and date_add(a.open_date, 30) then pma.merchant_id end) d30_merchant_actions_open
    , count(distinct case when pma.event_date between a.open_date and date_add(a.open_date, 90) then pma.merchant_id end) d90_merchant_actions_open
    , count(distinct case when pma.event_date between a.open_date and date_add(a.open_date, 180) then pma.merchant_id end) d180_merchant_actions_open
    , count(distinct case when pma.event_date between a.open_date and date_add(a.open_date, 360) then pma.merchant_id end) d360_merchant_actions_open
    , count(distinct case when pma.event_date between a.click_date and date_add(a.click_date, 1) then pma.merchant_id end) d1_merchant_actions_click
    , count(distinct case when pma.event_date between a.click_date and date_add(a.click_date, 7) then pma.merchant_id end) d7_merchant_actions_click
    , count(distinct case when pma.event_date between a.click_date and date_add(a.click_date, 14) then pma.merchant_id end) d14_merchant_actions_click
    , count(distinct case when pma.event_date between a.click_date and date_add(a.click_date, 21) then pma.merchant_id end) d21_merchant_actions_click
    , count(distinct case when pma.event_date between a.click_date and date_add(a.click_date, 30) then pma.merchant_id end) d30_merchant_actions_click
    , count(distinct case when pma.event_date between a.click_date and date_add(a.click_date, 90) then pma.merchant_id end) d90_merchant_actions_click
    , count(distinct case when pma.event_date between a.click_date and date_add(a.click_date, 180) then pma.merchant_id end) d180_merchant_actions_click
    , count(distinct case when pma.event_date between a.click_date and date_add(a.click_date, 360) then pma.merchant_id end) d360_merchant_actions_click
    , count(distinct case when pma.event_date between a.visit_date and date_add(a.visit_date, 1) then pma.merchant_id end) d1_merchant_actions_visit
    , count(distinct case when pma.event_date between a.visit_date and date_add(a.visit_date, 7) then pma.merchant_id end) d7_merchant_actions_visit
    , count(distinct case when pma.event_date between a.visit_date and date_add(a.visit_date, 14) then pma.merchant_id end) d14_merchant_actions_visit
    , count(distinct case when pma.event_date between a.visit_date and date_add(a.visit_date, 21) then pma.merchant_id end) d21_merchant_actions_visit
    , count(distinct case when pma.event_date between a.visit_date and date_add(a.visit_date, 30) then pma.merchant_id end) d30_merchant_actions_visit
    , count(distinct case when pma.event_date between a.visit_date and date_add(a.visit_date, 90) then pma.merchant_id end) d90_merchant_actions_visit
    , count(distinct case when pma.event_date between a.visit_date and date_add(a.visit_date, 180) then pma.merchant_id end) d180_merchant_actions_visit
    , count(distinct case when pma.event_date between a.visit_date and date_add(a.visit_date, 360) then pma.merchant_id end) d360_merchant_actions_visit

  from (
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
        when emailname like '%_US_%' then 'United States'
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
      where lower(sfe.journeyname) not like '%newsletter%' and lower(sfe.journeyname) not like 'op_%'
    ) a
  join grp_gdoop_bizops_db.pai_merchants pm on a.merchant_uuid = pm.merchant_uuid--a.accountid = pm.account_id
  left join grp_gdoop_sup_analytics_db.jc_w2l_mktg_acct_attrib mkt on mkt.accountid = pm.account_id
  left join grp_gdoop_bizops_db.sg_merchant_event_agg_v pma on pma.merchant_id = pm.merchant_uuid
      --and pma.event_date between a.start_date and date_add(a.start_date, 30)
  join user_dw.v_dim_day dd
    on date_format(a.sent_date,'yyyy-MM-dd') = dd.day_rw
  join user_dw.v_dim_week wk
    on dd.week_key = wk.week_key
  where pm.l1 = 'Local'
  group by 
        a.journeyname
     ,a.utm_campaign
     ,a.emailname
     ,a.Journey_Category
    , case when mkt.accountid is not null then 'Metro' else pm.acct_owner end 
    , case when pm.current_metal_segment in ('Silver', 'Gold', 'Platinum') then 'S+' else 'B-' end 
    , case when sent_date < first_launch_date then 'Pre-Feature' else 'Post-Feature' end 
    , pm.l2 
    , a.Engagement_Country
    , date_format(wk.week_end, 'yyyy-MM-dd')
    ,pma.event
    ,pma.sub_event_detail;




drop table if exists grp_gdoop_bizops_db.np_mm_email_base;
create table grp_gdoop_bizops_db.np_mm_email_base stored as orc as 
   select
      a.*
    , case when mkt.accountid is not null then 'Metro' else pm.acct_owner end as account_owner
    , case when pm.current_metal_segment in ('Silver', 'Gold', 'Platinum') then 'S+' else 'B-' end as metal_group
    , case when a.sent_date < first_launch_date then 'Pre-Feature' else 'Post-Feature' end as new_v_existing
    , pm.l2 as vertical
    , date_format(wk.week_end, 'yyyy-MM-dd') week_end_send
  from (
      select
        sfe.merchant_uuid
        , journeyname
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
        , min(date_format(sentdate, 'yyyy-MM-dd')) sent_date
        , min(date_format(firstopendate, 'yyyy-MM-dd')) open_date
        , min(date_format(firstclickdate, 'yyyy-MM-dd')) click_date
        , min(date_format(mc.first_visit, 'yyyy-MM-dd')) visit_date
      from grp_gdoop_bizops_db.sfmc_emailengagement sfe
      left join (
        select merchant_uuid, utm_campaign, min(eventdate) first_visit
        from grp_gdoop_bizops_db.pai_merchant_center_visits
        where utm_campaign is not null
        group by merchant_uuid,utm_campaign
      ) mc on sfe.merchant_uuid = mc.merchant_uuid and mc.utm_campaign = sfe.emailname
      where lower(sfe.journeyname) not like '%newsletter%' and lower(sfe.journeyname) not like 'op_%'
      group by 
          sfe.merchant_uuid
        , journeyname
        , case when journeyname like'%MM_LeadNurture%' then 'Lifecycle_LeadNurture'
            when journeyname like'%MM_ONBOARDING%' then 'Lifecycle_Onboarding'
            when journeyname like'%MM_Retention%' then 'Lifecycle_Retention'
            when journeyname like'%IMC%' then 'IMC'
            when journeyname like'MM_%' then 'Other'
            Else 'Unknown'
        end
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
        end
    ) a
join grp_gdoop_bizops_db.pai_merchants pm on a.merchant_uuid = pm.merchant_uuid--a.accountid = pm.account_id
left join (select accountid from grp_gdoop_sup_analytics_db.jc_w2l_mktg_acct_attrib group by accountid) mkt on mkt.accountid = pm.account_id
join user_dw.v_dim_day dd on date_format(a.sent_date,'yyyy-MM-dd') = dd.day_rw
join user_dw.v_dim_week wk on dd.week_key = wk.week_key
where pm.l1 = 'Local'
;


drop table grp_gdoop_bizops_db.np_mm_financial_base;
create table grp_gdoop_bizops_db.np_mm_financial_base stored as orc as
select 
    a.*
from 
(select 
   date_format(order_date,'yyyy-MM-dd') order_date, 
   merchant_uuid,
   deal_uuid,
   sum(auth_nob_loc * coalesce(approved_avg_exchange_rate,1)) nob, 
   sum(auth_nor_loc * coalesce(approved_avg_exchange_rate,1)) nor
from edwprod.fact_gbl_transactions as fgt
join user_groupondw.dim_day dd on dd.day_rw = fgt.order_date
join (select
                  currency_from,
                  currency_to,
                  fx_neutral_exchange_rate,
                  approved_avg_exchange_rate,
                  period_key
               from user_groupondw.gbl_fact_exchange_rate
                  where currency_to = 'USD'
                  group by
                  currency_from,
                  currency_to,
                  fx_neutral_exchange_rate,
                  approved_avg_exchange_rate,
                  period_key
                  ) er on fgt.currency_code = er.currency_from and dd.month_key  = er.period_key
where 
   cast(order_date as date) >= cast('2021-01-01' as date)
   and action = 'authorize'
group by 
   date_format(order_date,'yyyy-MM-dd'), 
   merchant_uuid, deal_uuid) as a 
join 
(select 
   merchant_uuid, 
   date_format(min(sent_date),'yyyy-MM-dd') min_sent_date 
from grp_gdoop_bizops_db.np_mm_email_base
    group by merchant_uuid) as b on a.merchant_uuid = b.merchant_uuid
where a.order_date >= date_add(b.min_sent_date, -30)
;




drop table grp_gdoop_bizops_db.np_mm_fin_tabl;
create table grp_gdoop_bizops_db.np_mm_fin_tabl stored as orc as 
with main as 
(select 
*
from 
(select 
  a.*, 
  order_date, 
  case when cast(order_date as date) < cast(sent_date as date) then 'performance before email' else 'performance after email' end performance_cat,
  b.deal_uuid,
  b.merchant_uuid as merchant_uuid_b,
  nob, 
  nor
from
grp_gdoop_bizops_db.np_mm_email_base as a
left join grp_gdoop_bizops_db.np_mm_financial_base as b on a.merchant_uuid = b.merchant_uuid) as fin
where (order_date >= date_add(sent_date, -30) or order_date is null)
)
select
   journeyname,
   journey_category,
   engagement_country,
   account_owner,
   metal_group,
   new_v_existing,
   vertical,
   week_end_send, 
   performance_cat,
   'within_0_days' within_x_days,
   sum(case when abs(DATEDIFF(order_date, sent_date)) = 0 then nob end) nob, 
   sum(case when abs(DATEDIFF(order_date, sent_date)) = 0 then nor end) nor, 
   count(distinct case when abs(DATEDIFF(order_date, sent_date)) = 0 then merchant_uuid_b end) merch, 
   count(distinct case when abs(DATEDIFF(order_date, sent_date)) = 0 then deal_uuid end) deal, 
   count( case when abs(DATEDIFF(order_date, sent_date)) = 0 then order_date end) ord
from main 
group by 
 journeyname,
   journey_category,
   engagement_country,
   account_owner,
   metal_group,
   new_v_existing,
   vertical,
   week_end_send,
   performance_cat
UNION ALL 
select
   journeyname,
   journey_category,
   engagement_country,
   account_owner,
   metal_group,
   new_v_existing,
   vertical,
   week_end_send, 
   performance_cat,
   'within_7_days' within_x_days,
   sum(case when abs(DATEDIFF(order_date, sent_date)) <= 7 then nob end) nob, 
   sum(case when abs(DATEDIFF(order_date, sent_date)) <= 7 then nor end) nor, 
   count(distinct case when abs(DATEDIFF(order_date, sent_date)) <= 7 then merchant_uuid_b end) merch, 
   count(distinct case when abs(DATEDIFF(order_date, sent_date)) <= 7 then deal_uuid end)  deal, 
   count( case when abs(DATEDIFF(order_date, sent_date)) <= 7 then order_date end) ord
from main 
group by 
 journeyname,
   journey_category,
   engagement_country,
   account_owner,
   metal_group,
   new_v_existing,
   vertical,
   week_end_send,
   performance_cat
UNION ALL 
select
   journeyname,
   journey_category,
   engagement_country,
   account_owner,
   metal_group,
   new_v_existing,
   vertical,
   week_end_send, 
   performance_cat,
   'within_14_days' within_x_days,
   sum(case when abs(DATEDIFF(order_date, sent_date)) <= 14 then nob end) nob, 
   sum(case when abs(DATEDIFF(order_date, sent_date)) <= 14 then nor end) nor, 
   count(distinct case when abs(DATEDIFF(order_date, sent_date)) <= 14 then merchant_uuid_b end) merch, 
   count(distinct case when abs(DATEDIFF(order_date, sent_date)) <= 14 then deal_uuid end) deal, 
   count( case when abs(DATEDIFF(order_date, sent_date)) <= 14 then order_date end) ord
from main 
group by 
 journeyname,
   journey_category,
   engagement_country,
   account_owner,
   metal_group,
   new_v_existing,
   vertical,
   week_end_send,
   performance_cat
UNION ALL 
select
   journeyname,
   journey_category,
   engagement_country,
   account_owner,
   metal_group,
   new_v_existing,
   vertical,
   week_end_send, 
   performance_cat,
   'within_21_days' within_x_days,
   sum(case when abs(DATEDIFF(order_date, sent_date)) <= 21 then nob end) nob, 
   sum(case when abs(DATEDIFF(order_date, sent_date)) <= 21 then nor end) nor, 
   count(distinct case when abs(DATEDIFF(order_date, sent_date)) <= 21 then merchant_uuid_b end) merch, 
   count(distinct case when abs(DATEDIFF(order_date, sent_date)) <= 21 then deal_uuid end) deal, 
   count( case when abs(DATEDIFF(order_date, sent_date)) <= 21 then order_date end) ord 
from main 
group by 
 journeyname,
   journey_category,
   engagement_country,
   account_owner,
   metal_group,
   new_v_existing,
   vertical,
   week_end_send,
   performance_cat
UNION ALL 
select
   journeyname,
   journey_category,
   engagement_country,
   account_owner,
   metal_group,
   new_v_existing,
   vertical,
   week_end_send,
   performance_cat,
   'within_30_days' within_x_days,
   sum(case when abs(DATEDIFF(order_date, sent_date)) <= 30 then nob end) nob, 
   sum(case when abs(DATEDIFF(order_date, sent_date)) <= 30 then nor end) nor, 
   count(distinct case when abs(DATEDIFF(order_date, sent_date)) <= 30 then merchant_uuid_b end) merch, 
   count(distinct case when abs(DATEDIFF(order_date, sent_date)) <= 30 then deal_uuid end) deal, 
   count( case when abs(DATEDIFF(order_date, sent_date)) <= 30 then order_date end) ord
from main 
group by 
 journeyname,
   journey_category,
   engagement_country,
   account_owner,
   metal_group,
   new_v_existing,
   vertical,
   week_end_send,
   performance_cat
UNION ALL 
select
   journeyname,
   journey_category,
   engagement_country,
   account_owner,
   metal_group,
   new_v_existing,
   vertical,
   week_end_send,
   performance_cat,
   'within_90_days' within_x_days,
   sum(case when abs(DATEDIFF(order_date, sent_date)) <= 90 then nob end) nob, 
   sum(case when abs(DATEDIFF(order_date, sent_date)) <= 90 then nor end) nor, 
   count(distinct case when abs(DATEDIFF(order_date, sent_date)) <= 90 then merchant_uuid_b end) merch, 
   count(distinct case when abs(DATEDIFF(order_date, sent_date)) <= 90 then deal_uuid end) deal, 
   count( case when abs(DATEDIFF(order_date, sent_date)) <= 90 then order_date end) ord
from main 
where performance_cat = 'performance after email' 
group by 
 journeyname,
   journey_category,
   engagement_country,
   account_owner,
   metal_group,
   new_v_existing,
   vertical,
   week_end_send,
   performance_cat
UNION ALL 
select
   journeyname,
   journey_category,
   engagement_country,
   account_owner,
   metal_group,
   new_v_existing,
   vertical,
   week_end_send,
   performance_cat,
   'within_180_days' within_x_days,
   sum(case when abs(DATEDIFF(order_date, sent_date)) <= 180 then nob end) nob, 
   sum(case when abs(DATEDIFF(order_date, sent_date)) <= 180 then nor end) nor, 
   count(distinct case when abs(DATEDIFF(order_date, sent_date)) <= 180 then merchant_uuid_b end) merch, 
   count(distinct case when abs(DATEDIFF(order_date, sent_date)) <= 180 then deal_uuid end) deal, 
   count( case when abs(DATEDIFF(order_date, sent_date)) <= 180 then order_date end) ord 
from main 
where performance_cat = 'performance after email' 
group by 
 journeyname,
   journey_category,
   engagement_country,
   account_owner,
   metal_group,
   new_v_existing,
   vertical,
   week_end_send,
   performance_cat
UNION ALL 
select
   journeyname,
   journey_category,
   engagement_country,
   account_owner,
   metal_group,
   new_v_existing,
   vertical,
   week_end_send,
   performance_cat,
   'total' within_x_days,
   sum(nob) nob, 
   sum(nor) nor, 
   count(distinct merchant_uuid_b) merch, 
   count(distinct deal_uuid ) deal, 
   count(order_date) ord
from main 
where performance_cat = 'performance after email' 
group by 
 journeyname,
   journey_category,
   engagement_country,
   account_owner,
   metal_group,
   new_v_existing,
   vertical,
   week_end_send,
   performance_cat
;
   
   
   
   
   
   
   
drop table grp_gdoop_bizops_db.np_mm_fin_tabl2;
create table grp_gdoop_bizops_db.np_mm_fin_tabl2 stored as orc as 
with main as
(
select
   journeyname,
   journey_category,
   engagement_country,
   account_owner,
   metal_group,
   new_v_existing,
   vertical,
   week_end_send, 
   performance_cat,
   sum(case when abs(DATEDIFF(order_date, sent_date)) = 0 then nob end) nob_within_0_days, 
   sum(case when abs(DATEDIFF(order_date, sent_date)) <= 7 then nob end) nob_within_7_days,
   sum(case when abs(DATEDIFF(order_date, sent_date)) <= 14 then nob end) nob_within_14_days,
   sum(case when abs(DATEDIFF(order_date, sent_date)) <= 21 then nob end) nob_within_21_days,
   sum(case when abs(DATEDIFF(order_date, sent_date)) <= 30 then nob end) nob_within_30_days,
   sum(case when abs(DATEDIFF(order_date, sent_date)) <= 90 and performance_cat = 'performance after email' then nob end) nob_within_90_days,
   sum(case when abs(DATEDIFF(order_date, sent_date)) <= 180 and performance_cat = 'performance after email' then nob end) nob_within_180_days,
   sum(case when performance_cat = 'performance after email' then nob end) nob_exceeding_180_days,
   sum(case when abs(DATEDIFF(order_date, sent_date)) = 0 then nor end) nor_within_0_days, 
   sum(case when abs(DATEDIFF(order_date, sent_date)) <= 7 then nor end) nor_within_7_days,
   sum(case when abs(DATEDIFF(order_date, sent_date)) <= 14 then nor end) nor_within_14_days,
   sum(case when abs(DATEDIFF(order_date, sent_date)) <= 21 then nor end) nor_within_21_days,
   sum(case when abs(DATEDIFF(order_date, sent_date)) <= 30 then nor end) nor_within_30_days,
   sum(case when abs(DATEDIFF(order_date, sent_date)) <= 90 and performance_cat = 'performance after email' then nor end) nor_within_90_days,
   sum(case when abs(DATEDIFF(order_date, sent_date)) <= 180 and performance_cat = 'performance after email' then nor end) nor_within_180_days,
   sum( case when performance_cat = 'performance after email' then nor end) nor_exceeding_180_days,
   count(distinct case when abs(DATEDIFF(order_date, sent_date)) = 0 then merchant_uuid_b end) merch_within_0_days, 
   count(distinct case when abs(DATEDIFF(order_date, sent_date)) <= 7 then merchant_uuid_b end) merch_within_7_days,
   count(distinct case when abs(DATEDIFF(order_date, sent_date)) <= 14 then merchant_uuid_b end) merch_within_14_days,
   count(distinct case when abs(DATEDIFF(order_date, sent_date)) <= 21 then merchant_uuid_b end) merch_within_21_days,
   count(distinct case when abs(DATEDIFF(order_date, sent_date)) <= 30 then merchant_uuid_b end) merch_within_30_days,
   count(distinct case when abs(DATEDIFF(order_date, sent_date)) <= 90 and performance_cat = 'performance after email' then merchant_uuid_b end) merch_within_90_days,
   count(distinct case when abs(DATEDIFF(order_date, sent_date)) <= 180 and performance_cat = 'performance after email' then merchant_uuid_b end) merch_within_180_days,
   count(distinct case when performance_cat = 'performance after email' then merchant_uuid_b end ) merch_exceeding_180_days,
   count(distinct case when abs(DATEDIFF(order_date, sent_date)) = 0 then deal_uuid end) deal_within_0_days, 
   count(distinct case when abs(DATEDIFF(order_date, sent_date)) <= 7 then deal_uuid end) deal_within_7_days,
   count(distinct case when abs(DATEDIFF(order_date, sent_date)) <= 14 then deal_uuid end) deal_within_14_days,
   count(distinct case when abs(DATEDIFF(order_date, sent_date)) <= 21 then deal_uuid end) deal_within_21_days,
   count(distinct case when abs(DATEDIFF(order_date, sent_date)) <= 30 then deal_uuid end) deal_within_30_days,
   count(distinct case when abs(DATEDIFF(order_date, sent_date)) <= 90 and performance_cat = 'performance after email' then deal_uuid end) deal_within_90_days,
   count(distinct case when abs(DATEDIFF(order_date, sent_date)) <= 180 and performance_cat = 'performance after email' then deal_uuid end) deal_within_180_days,
   count(distinct case when  performance_cat = 'performance after email' then deal_uuid end) deal_exceeding_180_days,
   count( case when abs(DATEDIFF(order_date, sent_date)) = 0 then order_date end) ord_within_0_days, 
   count( case when abs(DATEDIFF(order_date, sent_date)) <= 7 then order_date end) ord_within_7_days,
   count( case when abs(DATEDIFF(order_date, sent_date)) <= 14 then order_date end) ord_within_14_days,
   count( case when abs(DATEDIFF(order_date, sent_date)) <= 21 then order_date end) ord_within_21_days,
   count( case when abs(DATEDIFF(order_date, sent_date)) <= 30 then order_date end) ord_within_30_days,
   count( case when abs(DATEDIFF(order_date, sent_date)) <= 90 and performance_cat = 'performance after email' then order_date end) ord_within_90_days,
   count( case when abs(DATEDIFF(order_date, sent_date)) <= 180 and performance_cat = 'performance after email' then order_date end) ord_within_180_days,
   count(case when performance_cat = 'performance after email' then order_date end ) ord_exceeding_180_days
from 
(select 
  a.*, 
  order_date, 
  case when cast(order_date as date) < cast(sent_date as date) then 'performance before email' else 'performance after email' end performance_cat,
  b.deal_uuid,
  b.merchant_uuid as merchant_uuid_b,
  nob, 
  nor
from
grp_gdoop_bizops_db.np_mm_email_base as a
left join grp_gdoop_bizops_db.np_mm_financial_base as b on a.merchant_uuid = b.merchant_uuid) as fin
where (order_date >= date_add(sent_date, -30) or order_date is null)
group by 
   journeyname, 
   journey_category, 
   engagement_country, 
   account_owner, 
   metal_group, 
   new_v_existing, 
   vertical, 
   week_end_send, 
   performance_cat)
select
    a.*
from main as a
;

 
union all 
select 
   journeyname,
   journey_category,
   engagement_country,
   account_owner,
   metal_group,
   new_v_existing,
   vertical,
   week_end_send, 
   'difference post - pre' performance_cat,
   sum(case when performance_cat = 'performance after email' then nob_within_0_days end)/sum(case when performance_cat = 'performance before email' then nob_within_0_days end) nob_within_0_days,
   sum(case when performance_cat = 'performance after email' then nob_within_7_days end)/sum(case when performance_cat = 'performance before email' then nob_within_7_days end) nob_within_7_days,
   sum(case when performance_cat = 'performance after email' then nob_within_14_days end)/sum(case when performance_cat = 'performance before email' then nob_within_14_days end) nob_within_14_days,
   sum(case when performance_cat = 'performance after email' then nob_within_21_days end)/sum(case when performance_cat = 'performance before email' then nob_within_21_days end) nob_within_21_days,
   sum(case when performance_cat = 'performance after email' then nob_within_30_days end)/sum(case when performance_cat = 'performance before email' then nob_within_30_days end) nob_within_30_days,
   sum(case when performance_cat = 'performance after email' then nob_within_90_days end)/sum(case when performance_cat = 'performance before email' then nob_within_90_days end) nob_within_90_days,
   sum(case when performance_cat = 'performance after email' then nob_within_180_days end)/sum(case when performance_cat = 'performance before email' then nob_within_180_days end) nob_within_180_days,
   sum(case when performance_cat = 'performance after email' then nob_exceeding_180_days end)/sum(case when performance_cat = 'performance before email' then nob_exceeding_180_days end) nob_exceeding_180_days,
   sum(case when performance_cat = 'performance after email' then nor_within_0_days end)/sum(case when performance_cat = 'performance before email' then nor_within_0_days end) nor_within_0_days, 
   sum(case when performance_cat = 'performance after email' then nor_within_7_days end)/sum(case when performance_cat = 'performance before email' then nor_within_7_days end) nor_within_7_days,
   sum(case when performance_cat = 'performance after email' then nor_within_14_days end)/sum(case when performance_cat = 'performance before email' then nor_within_14_days end) nor_within_14_days,
   sum(case when performance_cat = 'performance after email' then nor_within_21_days end)/sum(case when performance_cat = 'performance before email' then nor_within_21_days end) nor_within_21_days,
   sum(case when performance_cat = 'performance after email' then nor_within_30_days end)/sum(case when performance_cat = 'performance before email' then nor_within_30_days end) nor_within_30_days,
   sum(case when performance_cat = 'performance after email' then nor_within_90_days end)/sum(case when performance_cat = 'performance before email' then nor_within_90_days end) nor_within_90_days,
   sum(case when performance_cat = 'performance after email' then nor_within_180_days end)/sum(case when performance_cat = 'performance before email' then nor_within_180_days end) nor_within_180_days,
   sum(case when performance_cat = 'performance after email' then nor_exceeding_180_days end)/sum(case when performance_cat = 'performance before email' then nor_exceeding_180_days end) nor_exceeding_180_days,
   sum(case when performance_cat = 'performance after email' then merch_within_0_days end)/sum(case when performance_cat = 'performance before email' then merch_within_0_days end) merch_within_0_days, 
   sum(case when performance_cat = 'performance after email' then merch_within_7_days end)/sum(case when performance_cat = 'performance before email' then merch_within_7_days end) merch_within_7_days,
   sum(case when performance_cat = 'performance after email' then merch_within_14_days end)/sum(case when performance_cat = 'performance before email' then merch_within_14_days end) merch_within_14_days,
   sum(case when performance_cat = 'performance after email' then merch_within_21_days end)/sum(case when performance_cat = 'performance before email' then merch_within_21_days end) merch_within_21_days,
   sum(case when performance_cat = 'performance after email' then merch_within_30_days end)/sum(case when performance_cat = 'performance before email' then merch_within_30_days end) merch_within_30_days,
   sum(case when performance_cat = 'performance after email' then merch_within_90_days end)/sum(case when performance_cat = 'performance before email' then merch_within_90_days end) merch_within_90_days,
   sum(case when performance_cat = 'performance after email' then merch_within_180_days end)/sum(case when performance_cat = 'performance before email' then merch_within_180_days end) merch_within_180_days,
   sum(case when performance_cat = 'performance after email' then merch_exceeding_180_days end)/sum(case when performance_cat = 'performance before email' then merch_exceeding_180_days end) merch_exceeding_180_days,
   sum(case when performance_cat = 'performance after email' then deal_within_0_days end)/sum(case when performance_cat = 'performance before email' then deal_within_0_days end) deal_within_0_days, 
   sum(case when performance_cat = 'performance after email' then deal_within_7_days end)/sum(case when performance_cat = 'performance before email' then deal_within_7_days end) deal_within_7_days,
   sum(case when performance_cat = 'performance after email' then deal_within_14_days end)/sum(case when performance_cat = 'performance before email' then deal_within_14_days end) deal_within_14_days,
   sum(case when performance_cat = 'performance after email' then deal_within_21_days end)/sum(case when performance_cat = 'performance before email' then deal_within_21_days end) deal_within_21_days,
   sum(case when performance_cat = 'performance after email' then deal_within_30_days end)/sum(case when performance_cat = 'performance before email' then deal_within_30_days end) deal_within_30_days,
   sum(case when performance_cat = 'performance after email' then deal_within_90_days end)/sum(case when performance_cat = 'performance before email' then deal_within_90_days end) deal_within_90_days,
   sum(case when performance_cat = 'performance after email' then deal_within_180_days end)/sum(case when performance_cat = 'performance before email' then deal_within_180_days end) deal_within_180_days,
   sum(case when performance_cat = 'performance after email' then deal_exceeding_180_days end) /sum(case when performance_cat = 'performance before email' then deal_exceeding_180_days end) deal_exceeding_180_days,
   sum(case when performance_cat = 'performance after email' then ord_within_0_days end)/sum(case when performance_cat = 'performance before email' then ord_within_0_days end) ord_within_0_days, 
   sum(case when performance_cat = 'performance after email' then ord_within_7_days end)/sum(case when performance_cat = 'performance before email' then ord_within_7_days end) ord_within_7_days,
   sum(case when performance_cat = 'performance after email' then ord_within_14_days end)/sum(case when performance_cat = 'performance before email' then ord_within_14_days end) ord_within_14_days,
   sum(case when performance_cat = 'performance after email' then ord_within_21_days end)/sum(case when performance_cat = 'performance before email' then ord_within_21_days end) ord_within_21_days,
   sum(case when performance_cat = 'performance after email' then ord_within_30_days end)/sum(case when performance_cat = 'performance before email' then ord_within_30_days end) ord_within_30_days,
   sum(case when performance_cat = 'performance after email' then ord_within_90_days end)/sum(case when performance_cat = 'performance before email' then ord_within_90_days end) ord_within_90_days,
   sum(case when performance_cat = 'performance after email' then ord_within_180_days end)/sum(case when performance_cat = 'performance before email' then ord_within_180_days end) ord_within_180_days,
   sum(case when performance_cat = 'performance after email' then ord_exceeding_180_days end)/sum(case when performance_cat = 'performance before email' then ord_exceeding_180_days end) ord_exceeding_180_days
from main as b 
group by 
 journeyname,
   journey_category,
   engagement_country,
   account_owner,
   metal_group,
   new_v_existing,
   vertical,
   week_end_send
   
create table grp_gdoop_bizops_db.np_mm_fin_tabl (
   financial_category string,
   utm_campaign string, 
   journeyname string, 
   emailname string, 
   journey_category string, 
   engagement_country string, 
   account_owner string, 
   metal_group string, 
   new_v_existing string, 
   vertical string, 
   week_end_send string, 
   performance_cat string,
   within_0_days float, 
   within_7_days float,
   within_14_days float,
   within_21_days float,
   within_30_days float,
   within_90_days float,
   within_180_days float,
   exceeding_180_days float
)
stored as orc
tblproperties ("orc.compress"="SNAPPY");

CREATE TEMPORARY TABLE grp_gdoop_bizops_db.np_mm_main as 
select 
  a.*, 
  order_date, 
  case when order_date < sent_date then 'performance before email' else 'performance after email' end performance_cat,
  b.merchant_uuid as merchant_uuid_b,
  deal_uuid, 
  nob, 
  nor
from
grp_gdoop_bizops_db.np_mm_email_base as a
left join grp_gdoop_bizops_db.np_mm_financial_base as b on a.merchant_uuid = b.merchant_uuid

with main as
(select 
  a.*, 
  order_date, 
  case when order_date < sent_date then 'performance before email' else 'performance after email' end performance_cat,
  b.merchant_uuid as merchant_uuid_b,
  deal_uuid, 
  nob, 
  nor
from
grp_gdoop_bizops_db.np_mm_email_base as a
left join grp_gdoop_bizops_db.np_mm_financial_base as b on a.merchant_uuid = b.merchant_uuid
)


with main as
(select 
  a.*, 
  order_date, 
  case when order_date < sent_date then 'performance before email' else 'performance after email' end performance_cat,
  b.merchant_uuid as merchant_uuid_b,
  deal_uuid, 
  nob, 
  nor
from
grp_gdoop_bizops_db.np_mm_email_base as a
left join grp_gdoop_bizops_db.np_mm_financial_base as b on a.merchant_uuid = b.merchant_uuid
)
select * from main;



   case when DATEDIFF(order_date, sent_date) <=7 and DATEDIFF(order_date, sent_date) >=7 and  then 'less than 7 days'
        when DATEDIFF(order_date, sent_date) < 0 and DATEDIFF(order_date, sent_date) >= -7 and  then 'less than 7 days'
        
        when abs(DATEDIFF(order_date, sent_date)) <=14  then 'less than 14 days'
        when abs(DATEDIFF(order_date, sent_date)) <= 21  then 'less than 21 days'
        when abs(DATEDIFF(order_date, sent_date)) <= 30  then 'less than 30 days'
        
        when abs(DATEDIFF(order_date, sent_date)) <= 90  then 'less than 90 days'
        when abs(DATEDIFF(order_date, sent_date)) <= 180  then 'less than 180 days'
        when abs(DATEDIFF(order_date, sent_date)) > 180  then 'more than 90 days'
        end
---when joining here above merchant will either have an order date or they wont

drop table grp_gdoop_bizops_db.np_mm_fin_tabl_grn;
create table grp_gdoop_bizops_db.np_mm_fin_tabl_grn stored as orc as
select 
   merchant_uuid, 
   utm_campaign, 
   journeyname, 
   emailname, 
   journey_category, 
   engagement_country, 
   account_owner, 
   metal_group, 
   new_v_existing, 
   vertical, 
   week_end_send, 
   case when order_date < sent_date then 'performance before email' else 'performance after email' end performance_cat,
   case when abs(DATEDIFF(order_date, sent_date)) <=7  then 'less than 7 days'
        when abs(DATEDIFF(order_date, sent_date)) <=14  then 'less than 14 days'
        when abs(DATEDIFF(order_date, sent_date)) <= 21  then 'less than 21 days'
        when abs(DATEDIFF(order_date, sent_date)) <= 30  then 'less than 30 days'
        when abs(DATEDIFF(order_date, sent_date)) <= 90  then 'less than 90 days'
        when abs(DATEDIFF(order_date, sent_date)) <= 180  then 'less than 180 days'
        when abs(DATEDIFF(order_date, sent_date)) > 180  then 'more than 90 days'
        end days_pre_post_since_email,
   count(distinct deal_uuid) deals_with_orders, 
   sum(nob) nob, 
   sum(nor) nor
from 
(select 
  a.*, 
  order_date, 
  deal_uuid, 
  nob, 
  nor
from 
grp_gdoop_bizops_db.np_mm_email_base as a 
left join grp_gdoop_bizops_db.np_mm_financial_base as b on a.merchant_uuid = b.merchant_uuid
) fin
where (order_date >= date_add(sent_date, -30) or order_date is null)
group by 
   merchant_uuid,
   utm_campaign, 
   journeyname, 
   emailname, 
   journey_category, 
   engagement_country, 
   account_owner, 
   metal_group, 
   new_v_existing, 
   vertical, 
   week_end_send,
   case when order_date < sent_date then 'performance before email' else 'performance after email' end,
   case when abs(DATEDIFF(order_date, sent_date)) <=7  then 'less than 7 days'
        when abs(DATEDIFF(order_date, sent_date)) <=14  then 'less than 14 days'
        when abs(DATEDIFF(order_date, sent_date)) <= 21  then 'less than 21 days'
        when abs(DATEDIFF(order_date, sent_date)) <= 30  then 'less than 30 days'
        when abs(DATEDIFF(order_date, sent_date)) <= 90  then 'less than 90 days'
        when abs(DATEDIFF(order_date, sent_date)) <= 180  then 'less than 180 days'
        when abs(DATEDIFF(order_date, sent_date)) > 180  then 'more than 90 days'
        end
;



  