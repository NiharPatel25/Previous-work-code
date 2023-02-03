select * from grp_gdoop_bizops_db.np_mc_sssl_tracking;

select a.user_uuid, b.merchant_uuid from prod_groupondw.user_bcookie_mapping as a 
join grp_gdoop_bizops_db.pai_merchants as b on a.user_uuid = b.merchant_uuid
limit 100;

select * from edwprod.agg_gbl_traffic;
select * from edwprod.agg_gbl_traffic_fin_deal;

select * from grp_gdoop_bizops_db.np_sl_drecommender;

---------------------------------------------------------------------------------------------------------------------------REDEMPTIONS


insert overwrite table grp_gdoop_bizops_db.np_redemption_orders
select order_uuid, 
       order_id, 
       case when usage_state_id = 2 then 1 else 0 end redeemed, 
       usage_date, 
       redeem_date
  from (
    select 
        t.order_uuid,
        t.order_id,
        max(usage_state_id) usage_state_id,
        min(usage_date) usage_date,
        min(case when usage_state_id = 2 then substr(last_modified,1,10) end) redeem_date
    from 
        user_edwprod.fact_gbl_transactions t
    join dwh_base.vouchers as v on t.parent_order_uuid = v.billing_id and t.deal_uuid = v.deal_id
    where 
    t.action = 'authorize'
    and
    t.country_id <> '235'
    and cast(t.order_date as date) > cast('2020-01-01' as date)
    group by 
        t.order_uuid,
        t.order_id
union
    select 
        t.order_uuid, 
        t.order_id,
        max(case when (customer_redeemed = 1 or merchant_redeemed = 1) then 2 else 0 end) usage_state_id,
        min(customer_redeemed_at) usage_date,
        min(case when (customer_redeemed = 1 or merchant_redeemed = 1) then substr(updated_at,1,10) end) redeem_date
    from 
        user_edwprod.fact_gbl_transactions t
    join user_gp.camp_membership_coupons v on t.user_uuid = v.purchaser_consumer_id  and t.order_id = v.order_id
    where 
    t.action = 'authorize'
    and
    t.country_id = '235'
    and cast(t.order_date as date) > cast('2020-01-01' as date)
    group by 
       t.order_uuid, 
       t.order_id
) a
;


select * from grp_gdoop_bizops_db.np_impressions_sl;


--------------------------------------------------------------------------------------------------------------------------------------impressions and citrus

select * from grp_gdoop_bizops_db.np_impressions_sl;

drop table grp_gdoop_bizops_db.np_impressions_sl;
create table grp_gdoop_bizops_db.np_impressions_sl (
      deal_landing string,
      search_query string,
      position string,
      ogp float,
      nor float,
      nob float,
      dv_time string,
      bcookie string,
      order_uuid string,
      dealuuid string,
      clientplatform string,
      aogcampaignid string,
      extrainfo string,
      page string
)partitioned by (eventdate string) 
stored as orc
tblproperties ("orc.compress"="SNAPPY");

drop table grp_gdoop_bizops_db.np_impressions_sl_app;
create table grp_gdoop_bizops_db.np_impressions_sl_app  (
      deal_landing string,
      search_query string,
      position string,
      ogp float,
      nor float,
      nob float,
      dv_time string,
      bcookie string,
      order_uuid string,
      dealuuid string,
      clientplatform string,
      aogcampaignid string,
      extrainfo string,
      page string
)partitioned by (eventdate string) 
stored as orc
tblproperties ("orc.compress"="SNAPPY");



drop table grp_gdoop_bizops_db.np_sl_all_deals;
create table grp_gdoop_bizops_db.np_sl_all_deals  (
      dealuuid string
)partitioned by (eventdate string) 
stored as orc
tblproperties ("orc.compress"="SNAPPY");





-----QUERIES
select * from grp_gdoop_bizops_db.np_impressions_sl where eventdate >= '2022-02-01';

select eventdate from grp_gdoop_bizops_db.np_impressions_sl group by eventdate order by eventdate desc;

insert overwrite table grp_gdoop_bizops_db.np_sl_all_deals partition (eventdate)
select 
    fin.dealuuid, fin.eventdate
from 
(select  dealuuid, eventdate
 from ai_reporting.sl_imp_clicks 
 where aogcampaignid is not null
 and eventdate >= date_add(current_date, -30)
UNION
 select  dealuuid, eventdate
 from ai_reporting.sl_imp_clicks_app 
 where aogcampaignid is not null
 and eventdate >= date_add(current_date, -30)
) as fin
group by fin.dealuuid, fin.eventdate;
   


select * from grp_gdoop_bizops_db.np_impressions_sl;

insert overwrite table grp_gdoop_bizops_db.np_impressions_sl partition (eventdate)
   select 
      case when cl.aogcampaignid is not null then 'Sponsored' else 'Organic' end deal_landing,
      trim(lower(regexp_replace(search_query,'\\+',' '))) search_query,
      position,
      ogp,
      nor,
      nob,
      dv_time,
      bcookie,
      order_uuid,
      cl.dealuuid,
      clientplatform,
      aogcampaignid,
      '' extrainfo,
      case when rawpagetype = 'browse/deals/index' and fullurl like '%context=local%' or fullurl like '%category=%' then 'browse'
           when rawpagetype in ('browse/deals/index') and search_query is not null  and search_query!='' then 'search'
           when rawpagetype in ('homepage' ,'homepage/index', 'featured/deals/index') then 'homepage'
           when rawpagetype in ( 'browse/deals/index') then 'browse' --'featured'
           when rawpagetype in ('nearby/deals/index', --featured? or local only 
                                 'goods/browse/index',
                                 'goods/index',
                                 'giftshop/deals/show',
                                 'giftshop/deals/index',
                                  'channels/show',
                                  'beautynow_promoted',
                                  'beautynow_salon',
                                  'beautynow_appointment_receipt',
                                  'beautynow_SELECT_appointment_time',
                                  'beautynow_SELECT_service') then 'browse'
           when rawpagetype like '%-%-%-%' then 'occasions'
           else rawpagetype
           end page,
        cl.eventdate
     from ai_reporting.sl_imp_clicks cl 
     join (select dealuuid, min(eventdate) min_eventdate, max(eventdate) max_eventdate 
           from grp_gdoop_bizops_db.np_sl_all_deals
           group by dealuuid
           ) dl on cl.dealuuid = dl.dealuuid 
     where 
        cl.eventdate >= date_add(current_date, -30) 
        and cast(cl.eventdate as date) >= cast(dl.min_eventdate as date) 
        and cast(cl.eventdate as date) <= cast(dl.max_eventdate as date);

select  eventdate, position, count(1) total 
from grp_gdoop_bizops_db.np_impressions_sl_app 
where cast(position as integer) <= 0 
group by eventdate, position 
order by eventdate desc, position;
       

 insert overwrite table grp_gdoop_bizops_db.np_impressions_sl_app partition (eventdate)
   select 
          case when cl.aogcampaignid is not null then 'Sponsored' else 'Organic' end deal_landing,
          trim(lower(regexp_replace(search_query,'\\+',' '))) search_query,  
          cast(cast(position as int )-1 as string) position ,
          ogp,
          nor,
          nob,
          dv_time,
          bcookie,
          order_uuid,
          cl.dealuuid,
          clientplatform,
          aogcampaignid,
          extrainfo,
          case when search_query is not null and search_query!='' and search_query!='All+Deals' then 'search' 
               when (get_json_object(lower(extrainfo), '$.type' ) is null and get_json_object(extrainfo, '$.tabName' ) ='home_tab') then 'homepage' ---or channel='all'
               when rawpagetype in ('wolfhound_mobile_page', 'GlobalSearchResult', 'MapLedSearch') then 'browse'
               else 'other' end page,
          cl.eventdate
     from ai_reporting.sl_imp_clicks_app  cl 
     join (select dealuuid, min(eventdate) min_eventdate, max(eventdate) max_eventdate 
           from grp_gdoop_bizops_db.np_sl_all_deals
           group by dealuuid
           ) dl on cl.dealuuid = dl.dealuuid
     where 
        cl.eventdate >= date_add(current_date, -30)
        and cast(cl.eventdate as date) >= cast(dl.min_eventdate as date)
        and cast(cl.eventdate as date) <= cast(dl.max_eventdate as date);
       
       
{"attributionId":"00000000-0000-0000-0000-000005493554",
"card_search_uuid":"00000000-0000-0000-0000-000005493554","deal_status":"available",
"hasTopRatedBadge":false,"idfv":"2D849B2F-3C36-4C87-B576-65F1EA1B27DD","IsCLO":"off","isD3CExperimentOn":true,
"isD3CExperimentOn_3PIP":true,"isD3CExperimentOn_Booking":true,"isD3CExperimentOn_Goods":false,
"offerType":"SALE","oneClickClaimEnabled":"false","rating":"5.000000",
"screen_instance_id":"8F13DE8C-2716-4AC3-8A58-3946BA1DE693_1648066220935","tabName":"home_tab","UMSText":"10+ viewed today"}

select *from ai_reporting.sl_imp_clicks_app where rawpagetype in ('wolfhound_mobile_page', 'GlobalSearchResult', 'MapLedSearch') and clientplatform = 'android';



------------------------------------------------add partition 

drop table grp_gdoop_bizops_db.np_citrus_sl;
create table grp_gdoop_bizops_db.np_citrus_sl  (
      source string,
      dealuuid string,
      position string, 
      platform string,
      search_keyword string,
      page string,
      citrus_page string, 
      total_cpc float, 
      citrus_impressions float, 
      citrus_clicks float
)partitioned by (report_date string) 
stored as orc
tblproperties ("orc.compress"="SNAPPY");
    
insert overwrite table grp_gdoop_bizops_db.np_citrus_sl  partition (report_date)
select 
     'citrus' source,
     deal_id dealuuid,
     case when page='SEARCH_ONLY' then cast(position_in_pod as int)*cast(position_in_pod as int)-1 
          when page='BROAD_DISPLAY' and get_json_object(translate(regexp_replace(translate(sponsored_product_filters,'[','{'),':','":"'),']','}'), '$.category_id')  is null then 3
          when page='BROAD_DISPLAY' and get_json_object(translate(regexp_replace(translate(sponsored_product_filters,'[','{'),':','":"'),']','}'), '$.category_id')  is not null then cast(position_in_pod as int)*cast(position_in_pod as int)-1
          when page='CATEGORY_ONLY' then cast(position_in_pod as int)*cast(position_in_pod as int)-1
          when page='CATEGORY_AND_SEARCH' then cast(position_in_pod as int)*cast(position_in_pod as int)-1
          end position, 
     get_json_object(translate(regexp_replace(translate(sponsored_product_filters,'[','{'),':','":"'),']','}'), '$.platform') platform,
     case when search_keyword != '' then  search_keyword end search_keyword,
     case when page='SEARCH_ONLY' then 'search'
          when page='CATEGORY_ONLY' then 'browse'
          when page='BROAD_DISPLAY' and get_json_object(translate(regexp_replace(translate(sponsored_product_filters,'[','{'),':','":"'),']','}'), '$.category_id')  is null then 'homepage'
          when page='BROAD_DISPLAY' and get_json_object(translate(regexp_replace(translate(sponsored_product_filters,'[','{'),':','":"'),']','}'), '$.category_id')  is not null then 'browse'
          end page,
     page citrus_page, 
     sum(revenue) total_cpc, 
     sum(impressions) citrus_impressions, 
     sum(clicks) citrus_clicks,
     report_date
FROM  ad_reporting_na_prod.citrus_master_report_citrus_51_v1 cit
where report_date > date_add(current_date, -30)
group by report_date,
     deal_id ,
     case when page='SEARCH_ONLY' then cast(position_in_pod as int)*cast(position_in_pod as int)-1
          when page='BROAD_DISPLAY' and get_json_object(translate(regexp_replace(translate(sponsored_product_filters,'[','{'),':','":"'),']','}'), '$.category_id')  is null then 3
          when page='BROAD_DISPLAY' and get_json_object(translate(regexp_replace(translate(sponsored_product_filters,'[','{'),':','":"'),']','}'), '$.category_id')  is not null then cast(position_in_pod as int)*cast(position_in_pod as int)-1
          when page='CATEGORY_ONLY' then cast(position_in_pod as int)*cast(position_in_pod as int)-1
          when page='CATEGORY_AND_SEARCH' then cast(position_in_pod as int)*cast(position_in_pod as int)-1 end , 
     get_json_object(translate(regexp_replace(translate(sponsored_product_filters,'[','{'),':','":"'),']','}'), '$.platform') ,
     case when search_keyword != '' then  search_keyword end ,
     case when page='SEARCH_ONLY' then 'search'
          when page='CATEGORY_ONLY' then 'browse'
          when page='BROAD_DISPLAY' and get_json_object(translate(regexp_replace(translate(sponsored_product_filters,'[','{'),':','":"'),']','}'), '$.category_id')  is null then 'homepage'
          when page='BROAD_DISPLAY' and get_json_object(translate(regexp_replace(translate(sponsored_product_filters,'[','{'),':','":"'),']','}'), '$.category_id')  is not null then 'browse' end ,
     page;
    



-----------------------------------------------------------------------------AGG Table 

select distinct eventdate from grp_gdoop_bizops_db.np_sl_performance_base order by eventdate desc;
select * from grp_gdoop_bizops_db.np_sl_performance_base;
    
create table grp_gdoop_bizops_db.np_sl_performance_base (
      dealuuid string,
      deal_permalink string,
      account_id string, 
      account_name string,
      merch_segmentation string,
      l1 string,
      l2 string,
      coupon int,
      deal_rev float, 
      deal_rev_same_day float, 
      deal_rev_30_day float, 
      deal_rev_120_day float, 
      deal_rev_all_red float,
      sli_impressions float, 
      sli_clicks float,
      total_impression_aog float,
      orders_sold float,
      deal_rev_org float, 
      deal_rev_same_day_org float, 
      deal_rev_30_day_org float, 
      deal_rev_120_day_org float, 
      deal_rev_all_red_org float,
      sli_impressions_org float, 
      sli_clicks_org float,
      total_impression_aog_org float,
      orders_sold_org float,
      total_groupon_rev float,
      citrus_impressions float,
      citrus_clicks float
)partitioned by (eventdate string) 
stored as orc
tblproperties ("orc.compress"="SNAPPY");

select * from grp_gdoop_bizops_db.np_sl_performance_base;

insert overwrite table grp_gdoop_bizops_db.np_sl_performance_base partition (eventdate)
select 
   sli2.dealuuid,
   gdl.deal_permalink,
   sf.account_id, 
   sf.account_name,
   sf.merch_segmentation,
   gdl.l1,
   gdl.l2,
   case when gdl.deal_permalink like '%cpn%' then 1 else 0 end coupon,
   sum(deal_rev) deal_rev, 
   sum(red_same_day) deal_rev_same_day, 
   sum(red_30) deal_rev_30_day, 
   sum(red_120) deal_rev_120_day, 
   sum(all_reds) deal_rev_all_red,
   sum(total_impressions) sli_impressions, 
   sum(total_clicks) sli_clicks, 
   sum(total_impression_aog) total_impression_aog,
   sum(orders_sold) orders_sold,
   sum(deal_rev_org) deal_rev_org, 
   sum(red_same_day_org) deal_rev_same_day_org, 
   sum(red_30_org) deal_rev_30_day_org,
   sum(red_120_org) deal_rev_120_day_org,
   sum(all_reds_org) deal_rev_all_red_org,
   sum(total_impressions_org) sli_impressions_org, 
   sum(total_clicks_org) sli_clicks_org, 
   sum(total_impression_aog_org) total_impression_aog,
   sum(orders_sold_org) orders_sold_org,
   sum(total_cpc) total_groupon_rev,
   sum(citrus_impressions) citrus_impressions,
   sum(citrus_clicks) citrus_clicks,
   sli2.eventdate
   from 
(select 
        eventdate,  
        dealuuid, 
        sum(case when deal_landing = 'Sponsored' then nob - nor end) deal_rev, 
        sum(case when deal_landing = 'Sponsored' and redeem_date <= cast(eventdate as date) then (nob-nor) when deal_landing = 'Sponsored' and (nob-nor) is not null then 0 end) red_same_day, 
        sum(case when deal_landing = 'Sponsored' and redeem_date <= date_add(cast(eventdate as date), 30) then (nob-nor) when deal_landing = 'Sponsored' and (nob-nor) is not null then 0 end) red_30,
        sum(case when deal_landing = 'Sponsored' and redeem_date <= date_add(cast(eventdate as date), 120) then (nob-nor) when deal_landing = 'Sponsored' and (nob-nor) is not null then 0 end) red_120, -----redemption 120 days doesn't work when updating oly last 30 days. 
        sum(case when deal_landing = 'Sponsored' and reds.order_uuid is not null then nob-nor when (nob-nor) is not null then 0 end) all_reds,
        count(case when deal_landing = 'Sponsored' then 1 end) total_impressions, 
        count(distinct case when deal_landing = 'Sponsored' then aogcampaignid end) total_impression_aog,
        count(distinct case when deal_landing = 'Sponsored' then dv_time end) total_clicks, 
        count(distinct case when deal_landing = 'Sponsored' then sli.order_uuid end) orders_sold, 
        sum(case when deal_landing = 'Organic' then nob - nor end) deal_rev_org, 
        sum(case when deal_landing = 'Organic' and redeem_date <= cast(eventdate as date) then (nob-nor) when deal_landing = 'Organic' and (nob-nor) is not null then 0 end) red_same_day_org, 
        sum(case when deal_landing = 'Organic' and redeem_date <= date_add(cast(eventdate as date), 30) then (nob-nor) when deal_landing = 'Organic' and (nob-nor) is not null then 0 end) red_30_org,
        sum(case when deal_landing = 'Organic' and redeem_date <= date_add(cast(eventdate as date), 120) then (nob-nor) when deal_landing = 'Organic' and (nob-nor) is not null then 0 end) red_120_org,
        sum(case when deal_landing = 'Organic' and reds.order_uuid is not null then nob-nor when deal_landing = 'Organic' and (nob-nor) is not null then 0 end) all_reds_org,
        count(case when deal_landing = 'Organic' then 1 end) total_impressions_org, 
        count(distinct case when deal_landing = 'Organic' then aogcampaignid end) total_impression_aog_org,
        count(distinct case when deal_landing = 'Organic' then dv_time end) total_clicks_org, 
        count(distinct case when deal_landing = 'Organic' then sli.order_uuid end) orders_sold_org
   from 
        (select * from grp_gdoop_bizops_db.np_impressions_sl
         where eventdate >= date_add(current_date, -30)
        union all
         select * from grp_gdoop_bizops_db.np_impressions_sl_app
         where eventdate >= date_add(current_date, -30)) as sli 
   left join 
           (select 
               order_uuid,
               redeem_date
            from grp_gdoop_bizops_db.np_redemption_orders
            where redeemed = 1
            group by order_uuid, redeem_date
           ) reds on sli.order_uuid = reds.order_uuid
      group by 
         eventdate, 
         dealuuid) sli2 
left join 
      (select 
           report_date, 
           dealuuid, 
           sum(total_cpc) total_cpc, 
           sum(citrus_impressions) citrus_impressions, 
           sum(citrus_clicks) citrus_clicks
           from grp_gdoop_bizops_db.np_citrus_sl
           group by report_date, dealuuid) cit on sli2.dealuuid = cit.dealuuid and  sli2.eventdate = cit.report_date
left join 
          (select deal_id, 
                  max(grt_l1_cat_name) l1, 
                  max(grt_l2_cat_name) l2, 
                  max(country_code) country_code,
                  max(deal_permalink) deal_permalink
                  from user_edwprod.dim_gbl_deal_lob 
                  group by deal_id) as gdl on sli2.dealuuid = gdl.deal_id
left join (
        select d.deal_uuid, 
               max(m.account_id) account_id, 
               max(m.acct_owner) account_owner, 
               max(sfa.name) account_name, 
               max(merchant_segmentation__c) merch_segmentation
        from grp_gdoop_bizops_db.pai_deals d 
        join grp_gdoop_bizops_db.pai_merchants m on d.merchant_uuid = m.merchant_uuid
        join dwh_base_sec_view.sf_account sfa on m.account_id = sfa.id
            group by d.deal_uuid
    ) sf on sli2.dealuuid = sf.deal_uuid
 group by 
     sli2.eventdate,
     sli2.dealuuid,
     sf.account_id, 
     sf.account_name,
     sf.merch_segmentation,
     gdl.l1,
     gdl.l2,
     gdl.deal_permalink,
     case when gdl.deal_permalink like '%cpn%' then 1 else 0 end
;


    
drop table grp_gdoop_bizops_db.np_sl_roas_final_merchlvl2;
create table grp_gdoop_bizops_db.np_sl_roas_final_merchlvl2 stored as orc as 
select 
    fin2.*, 
    case when 
         (orders_sold is null or orders_sold = 0) then 'no orders received'
         when orders_sold <= 10 then 'a.<= 10 orders received'
         when orders_sold <= 20 then 'b.<= 15 orders received'
         when orders_sold <= 30 then 'c.<= 30 orders received'
         when orders_sold >30 then 'd. more than 30 orders received' end orders_cohort,
    case when red_rate_30 is null then 'a. no orders made for clicks'
        when red_rate_30 <= 0.15 then 'b.<= 15% redemption rate'
        when red_rate_30 <= 0.25 then 'c.<= 25% redemption rate'
        when red_rate_30 <= 0.35 then 'd.<= 35% redemption rate'
        when red_rate_30 <= 0.50 then 'e.<= 50% redemption rate'
        when red_rate_30 <= 0.70 then 'f.<= 70% redemption rate'
        else 'g.>70% redemption rate' end revenue_redeemed_cat,
    case when red_rate_30_org is null then 'a. no orders made for clicks'
        when red_rate_30_org <= 0.15 then 'b.<= 15% redemption rate'
        when red_rate_30_org <= 0.25 then 'c.<= 25% redemption rate'
        when red_rate_30_org <= 0.35 then 'd.<= 35% redemption rate'
        when red_rate_30_org <= 0.50 then 'e.<= 50% redemption rate'
        when red_rate_30_org <= 0.70 then 'f.<= 70% redemption rate'
        else 'g.>70% redemption rate' end revenue_redeemed_cat_org,
    case 
        when potential_roas = 0 then 'x.no orders received yet'
        when potential_roas <= 1 then 'a. less than 1'
        when potential_roas <= 2 then 'b. less than 2'
        when potential_roas <= 3 then 'c. less than 3'
        when potential_roas <= 4 then 'd. less than 4'
        when potential_roas is not null then 'e. more than 4'end potential_roas_category, 
    case 
        when potential_roas = 0 then 'x.no orders received yet'
        when potential_30_day_roas = 0 then 'y.no redemptions made'
        when potential_30_day_roas <= 1 then 'a. less than 1'
        when potential_30_day_roas <= 2 then 'b. less than 2'
        when potential_30_day_roas <= 3 then 'c. less than 3'
        when potential_30_day_roas <= 4 then 'd. less than 4'
        when potential_30_day_roas is not null then 'e. more than 4' end days_30_roas_category, 
    case 
        when potential_roas = 0 then 'x.no orders received yet'
        when potential_120_day_roas = 0 then 'y.no redemptions made'
        when potential_120_day_roas <= 1 then 'a. less than 1'
        when potential_120_day_roas <= 2 then 'b. less than 2'
        when potential_120_day_roas <= 3 then 'c. less than 3'
        when potential_120_day_roas <= 4 then 'd. less than 4'
        when potential_120_day_roas is not null then 'e. more than 4' end days_120_roas_category
   from 
(select 
    fin.*,
     case when merch_rev is not null then merch_rev_30_day/merch_rev end red_rate_30, 
     case when merch_rev_org is not null then merch_rev_30_day_org/merch_rev_org end red_rate_30_org,
     case when (total_groupon_rev is not null and total_groupon_rev <> 0) then (case when merch_rev is null then 0 else merch_rev end)/total_groupon_rev end potential_roas, 
     case when (total_groupon_rev is not null and total_groupon_rev <> 0) then (case when merch_rev_30_day is null then 0 else merch_rev_30_day end)/total_groupon_rev end potential_30_day_roas,
     case when (total_groupon_rev is not null and total_groupon_rev <> 0) then (case when merch_rev_120_day is null then 0 else merch_rev_120_day end)/total_groupon_rev end potential_120_day_roas
    from 
    (select
           'monthly_cut' dashboard_cut,
           date_add(last_day(add_months(eventdate, -1)),1) date_start_end, 
           account_id, 
           account_name,
           max(merch_segmentation) merch_segmentation,
           max(l1) l1,
           max(l2) l2,
           max(coupon) coupon,
           sum(deal_rev) merch_rev, 
           sum(deal_rev_same_day) merch_rev_same_day, 
           sum(deal_rev_30_day) merch_rev_30_day, 
           sum(deal_rev_120_day) merch_rev_120_day, 
           sum(deal_rev_all_red) merch_rev_all_red,
           sum(sli_impressions) sli_impressions, 
           sum(total_impression_aog) total_impression_aog,
           sum(sli_clicks) sli_clicks, 
           sum(orders_sold) orders_sold,
           sum(deal_rev_org) merch_rev_org, 
           sum(deal_rev_same_day_org) merch_rev_same_day_org,
           sum(deal_rev_30_day_org) merch_rev_30_day_org,
           sum(deal_rev_120_day_org) merch_rev_120_day_org,
           sum(deal_rev_all_red_org) merch_rev_all_red_org,
           sum(sli_impressions_org) sli_impressions_org,
           sum(total_impression_aog_org) total_impression_aog_org,
           sum(sli_clicks_org) sli_clicks_org,
           sum(orders_sold_org) orders_sold_org,
           count(distinct eventdate) number_of_days,
           sum(total_groupon_rev) total_groupon_rev,
           sum(citrus_impressions) citrus_impressions,
           sum(citrus_clicks) citrus_clicks
           from
       grp_gdoop_bizops_db.np_sl_performance_base 
       group by date_add(last_day(add_months(eventdate, -1)),1), account_id, account_name) as fin
 ) as fin2
UNION
select 
    fin2.*, 
    case when 
         (orders_sold is null or orders_sold = 0) then 'no orders received'
         when orders_sold <= 10 then 'a.<= 10 orders received'
         when orders_sold <= 20 then 'b.<= 15 orders received'
         when orders_sold <= 30 then 'c.<= 30 orders received'
         when orders_sold >30 then 'd. more than 30 orders received' end orders_cohort,
    case when red_rate_30 is null then 'a. no orders made for clicks'
        when red_rate_30 <= 0.15 then 'b.<= 15% redemption rate'
        when red_rate_30 <= 0.25 then 'c.<= 25% redemption rate'
        when red_rate_30 <= 0.35 then 'd.<= 35% redemption rate'
        when red_rate_30 <= 0.50 then 'e.<= 50% redemption rate'
        when red_rate_30 <= 0.70 then 'f.<= 70% redemption rate'
        else 'g.>70% redemption rate' end revenue_redeemed_cat,
    case when red_rate_30_org is null then 'a. no orders made for clicks'
        when red_rate_30_org <= 0.15 then 'b.<= 15% redemption rate'
        when red_rate_30_org <= 0.25 then 'c.<= 25% redemption rate'
        when red_rate_30_org <= 0.35 then 'd.<= 35% redemption rate'
        when red_rate_30_org <= 0.50 then 'e.<= 50% redemption rate'
        when red_rate_30_org <= 0.70 then 'f.<= 70% redemption rate'
        else 'g.>70% redemption rate' end revenue_redeemed_cat_org,
    case 
        when potential_roas = 0 then 'x.no orders received yet'
        when potential_roas <= 1 then 'a. less than 1'
        when potential_roas <= 2 then 'b. less than 2'
        when potential_roas <= 3 then 'c. less than 3'
        when potential_roas <= 4 then 'd. less than 4'
        when potential_roas is not null then 'e. more than 4'end potential_roas_category, 
    case 
        when potential_roas = 0 then 'x.no orders received yet'
        when potential_30_day_roas = 0 then 'y.no redemptions made'
        when potential_30_day_roas <= 1 then 'a. less than 1'
        when potential_30_day_roas <= 2 then 'b. less than 2'
        when potential_30_day_roas <= 3 then 'c. less than 3'
        when potential_30_day_roas <= 4 then 'd. less than 4'
        when potential_30_day_roas is not null then 'e. more than 4' end days_30_roas_category, 
    case 
        when potential_roas = 0 then 'x.no orders received yet'
        when potential_120_day_roas = 0 then 'y.no redemptions made'
        when potential_120_day_roas <= 1 then 'a. less than 1'
        when potential_120_day_roas <= 2 then 'b. less than 2'
        when potential_120_day_roas <= 3 then 'c. less than 3'
        when potential_120_day_roas <= 4 then 'd. less than 4'
        when potential_120_day_roas is not null then 'e. more than 4' end days_120_roas_category
   from 
(select 
    fin.*,
     case when merch_rev is not null then merch_rev_30_day/merch_rev end red_rate_30, 
     case when merch_rev_org is not null then merch_rev_30_day_org/merch_rev_org end red_rate_30_org,
     case when (total_groupon_rev is not null and total_groupon_rev <> 0) then (case when merch_rev is null then 0 else merch_rev end)/total_groupon_rev end potential_roas, 
     case when (total_groupon_rev is not null and total_groupon_rev <> 0) then (case when merch_rev_30_day is null then 0 else merch_rev_30_day end)/total_groupon_rev end potential_30_day_roas,
     case when (total_groupon_rev is not null and total_groupon_rev <> 0) then (case when merch_rev_120_day is null then 0 else merch_rev_120_day end)/total_groupon_rev end potential_120_day_roas
    from 
    (select
           'weekly_cut' dashboard_cut,
           date_sub(next_day(eventdate, 'MON'), 1) date_start_end,
           account_id, 
           account_name,
           max(merch_segmentation) merch_segmentation,
           max(l1) l1,
           max(l2) l2,
           max(coupon) coupon,
           sum(deal_rev) merch_rev, 
           sum(deal_rev_same_day) merch_rev_same_day, 
           sum(deal_rev_30_day) merch_rev_30_day, 
           sum(deal_rev_120_day) merch_rev_120_day, 
           sum(deal_rev_all_red) merch_rev_all_red,
           sum(sli_impressions) sli_impressions, 
           sum(total_impression_aog) total_impression_aog,
           sum(sli_clicks) sli_clicks, 
           sum(orders_sold) orders_sold,
           sum(deal_rev_org) merch_rev_org, 
           sum(deal_rev_same_day_org) merch_rev_same_day_org,
           sum(deal_rev_30_day_org) merch_rev_30_day_org,
           sum(deal_rev_120_day_org) merch_rev_120_day_org,
           sum(deal_rev_all_red_org) merch_rev_all_red_org,
           sum(sli_impressions_org) sli_impressions_org,
           sum(total_impression_aog_org) total_impression_aog_org,
           sum(sli_clicks_org) sli_clicks_org,
           sum(orders_sold_org) orders_sold_org,
           count(distinct eventdate) number_of_days,
           sum(total_groupon_rev) total_groupon_rev,
           sum(citrus_impressions) citrus_impressions,
           sum(citrus_clicks) citrus_clicks
           from
       grp_gdoop_bizops_db.np_sl_performance_base
       group by date_sub(next_day(eventdate, 'MON'), 1), account_id, account_name) as fin
 ) as fin2
;



/*
left join (
        select deal_uuid, 
               max(merchant_segmentation__c) merch_segmentation
        from user_edwprod.opportunity_1 o1
        join user_edwprod.opportunity_2 o2 on o1.id = o2.id
        join user_edwprod.sf_account sfa on o1.accountid = sfa.id
        group by deal_uuid
    ) sf on sli2.deal_uuid = sf.deal_uuid*/




select * from grp_gdoop_bizops_db.np_sl_roas_final_merchlvl2 
where sli_impressions >= 30 and dashboard_cut = 'monthly_cut' and date_start_end >= '2021-04-01';


select 
        eventdate,  
        search_query,
        sum(nob - nor) deal_rev, 
        count(1) total_impressions, 
        count(distinct dv_time ) total_clicks, 
        count(distinct order_uuid ) orders_sold
   from 
        (select * from grp_gdoop_bizops_db.np_impressions_sl
         where deal_landing = 'Organic' and search_query is not null
        union all
         select * from grp_gdoop_bizops_db.np_impressions_sl_app
         where deal_landing = 'Sponsored' and search_query is not null) as fin
         group by eventdate, search_query

------------------------------------------------------------------------------------------------CITRUS SPEND AND IMPRESSIONS

drop table grp_gdoop_bizops_db.np_citrus_merch_spend_sl;

create table grp_gdoop_bizops_db.np_citrus_merch_spend_sl stored as orc as 
select 
   date_add(last_day(add_months(report_date, -1)),1) month_start, 
   doe.merchant_uuid,
   sf.merchant_name,
   sf.merch_segmentation,
   case when sf.merch_segmentation in ('Platinum', 'Silver', 'Gold') then 'S+' else 'B-' end m_segmentation,
   max(gdl.l1) l1,
   max(gdl.l2) l2,
   max(gdl.country_code) country_code, 
   sum(total_cpc) groupon_revenue, 
   sum(citrus_impressions) citrus_impressions, 
   sum(citrus_clicks) citrus_clicks
from 
grp_gdoop_bizops_db.np_citrus_sl as a 
left join 
          (select deal_id, 
                  max(grt_l1_cat_name) l1, 
                  max(grt_l2_cat_name) l2, 
                  max(country_code) country_code
                  from user_edwprod.dim_gbl_deal_lob 
                  group by deal_id) as gdl on a.dealuuid = gdl.deal_id
left join 
          (select product_uuid, max(merchant_uuid) merchant_uuid from user_edwprod.dim_offer_ext group by product_uuid) as doe on a.dealuuid = doe.product_uuid
left join (
          select 
             a.merchant_uuid, 
             max(a.name) merchant_name,
             max(merchant_segmentation__c) merch_segmentation
          from 
             user_edwprod.dim_merchant as a
          left join dwh_base_sec_view.sf_account as b on a.salesforce_account_id = b.id
          group by a.merchant_uuid) sf on doe.merchant_uuid = sf.merchant_uuid
group by 
   date_add(last_day(add_months(report_date, -1)),1), 
   a.dealuuid,
   doe.merchant_uuid,
   sf.merchant_name,
   sf.merch_segmentation,
   case when sf.merch_segmentation in ('Platinum', 'Silver', 'Gold') then 'S+' else 'B-' end;


------------------------------------------------------------------------------------------------PRE POST COMPARISON
  
  
drop table grp_gdoop_bizops_db.np_sl_min_deal;
create table grp_gdoop_bizops_db.np_sl_min_deal stored as orc as
select 
     done.*, 
     case when live_days_on_sl <= diff_btw_sl_min_groupon_min and live_days_on_sl < 30 then live_days_on_sl
          when diff_btw_sl_min_groupon_min <= live_days_on_sl and diff_btw_sl_min_groupon_min < 30 then diff_btw_sl_min_groupon_min
          else 30
          end number_of_days_comparison
from
(select 
     fin.*, 
     datediff(to_date(fin.max_live_date), to_date(fin.min_launch_date)) live_days_on_sl, 
     fin2.min_live_on_groupon, 
     datediff(to_date(fin.min_launch_date), to_date(fin2.min_live_on_groupon)) diff_btw_sl_min_groupon_min
from 
(select
     a.dealuuid,
     max(b.merchant_uuid) merchant_uuid,
     cast(min(eventdate) as date) min_launch_date, 
     cast(max(eventdate) as date) max_live_date
  from grp_gdoop_bizops_db.np_sl_all_deals as a
  left join grp_gdoop_bizops_db.pai_deals as b on a.dealuuid = b.deal_uuid
  group by a.dealuuid) as fin
left join 
(select deal_uuid, min(load_date) min_live_on_groupon from user_groupondw.active_deals group by deal_uuid) as fin2 on fin.dealuuid = fin2.deal_uuid
) as done
  ;



 
insert overwrite table grp_gdoop_bizops_db.np_impressions_sl_pre
   select
      case when cl.aogcampaignid is not null then 'Sponsored' else 'Organic' end deal_landing,
      trim(lower(regexp_replace(search_query,'\\+',' '))) search_query,
      position,
      ogp,
      nor,
      nob,
      dv_time,
      bcookie,
      order_uuid,
      cl.dealuuid,
      clientplatform,
      aogcampaignid,
      '' extrainfo,
      case when rawpagetype = 'browse/deals/index' and fullurl like '%context=local%' or fullurl like '%category=%' then 'browse'
           when rawpagetype in ('browse/deals/index') and search_query is not null  and search_query!='' then 'search'
           when rawpagetype in ('homepage' ,'homepage/index', 'featured/deals/index') then 'homepage'
           when rawpagetype in ( 'browse/deals/index') then 'browse' --'featured'
           when rawpagetype in ('nearby/deals/index', --featured? or local only
                                 'goods/browse/index',
                                 'goods/index',
                                 'giftshop/deals/show',
                                 'giftshop/deals/index',
                                  'channels/show',
                                  'beautynow_promoted',
                                  'beautynow_salon',
                                  'beautynow_appointment_receipt',
                                  'beautynow_SELECT_appointment_time',
                                  'beautynow_SELECT_service') then 'browse'
           when rawpagetype like '%-%-%-%' then 'occasions'
           else rawpagetype
           end page,
        cl.eventdate
     from ai_reporting.sl_imp_clicks cl
     join (select dealuuid, min_launch_date
           from grp_gdoop_bizops_db.np_sl_min_deal
           ) dl on cl.dealuuid = dl.dealuuid
     where
        cast(cl.eventdate as date) < cast(dl.min_launch_date as date);
       



insert overwrite table grp_gdoop_bizops_db.np_impressions_slapp_pre
   select
          case when cl.aogcampaignid is not null then 'Sponsored' else 'Organic' end deal_landing,
          trim(lower(regexp_replace(search_query,'\\+',' '))) search_query,
          cast(cast(position as int )-1 as string) position ,
          ogp,
          nor,
          nob,
          dv_time,
          bcookie,
          order_uuid,
          cl.dealuuid,
          clientplatform,
          aogcampaignid,
          extrainfo,
          case when search_query is not null and search_query!='' and search_query!='All+Deals' then 'search'
               when (get_json_object(lower(extrainfo), '$.type' ) is null and get_json_object(extrainfo, '$.tabName' ) ='home_tab') then 'homepage' ---or channel='all'
               when rawpagetype in ('wolfhound_mobile_page', 'GlobalSearchResult', 'MapLedSearch') then 'browse'
               else 'other' end page,
          cl.eventdate
     from ai_reporting.sl_imp_clicks_app  cl
     join (select dealuuid, min_launch_date
           from grp_gdoop_bizops_db.np_sl_min_deal
           ) dl on cl.dealuuid = dl.dealuuid
     where
        cast(cl.eventdate as date) < cast(dl.min_launch_date as date);




/*      
drop table grp_gdoop_bizops_db.np_sl_tab_perf_comp;
create table grp_gdoop_bizops_db.np_sl_tab_perf_comp stored as orc as 
select
     dl_sl.min_launch_date,
     dl_sl.max_live_date,
     dl_sl.live_days_on_sl,
     dl_sl.min_live_on_groupon, 
     dl_sl.number_of_days_comparison,
     dl_sl.diff_btw_sl_min_groupon_min,
     dl_sl.merchant_uuid,
     fin.dealuuid,
     fin.position,
     fin.eventdate,
     fin.deal_landing,
     fin.timeline,
     count(1) impressions,
     count(distinct aogcampaignid) impressions_aog,
     count(distinct dv_time) clicks,
     count(distinct bcookie) distinct_users, 
     sum(nob-nor) merch_revenue, 
     count(distinct order_uuid) total_orders   
from
   grp_gdoop_bizops_db.np_sl_min_deal as dl_sl
left join
  (select a.*, 'post' timeline
    from grp_gdoop_bizops_db.np_impressions_sl as a
  union all
  select a.*, 'post' timeline
    from grp_gdoop_bizops_db.np_impressions_sl_app as a
  union all
   select a.*, 'pre' timeline
    from grp_gdoop_bizops_db.np_impressions_sl_pre as a
         where cast(eventdate as date) >= cast('2020-01-01' as date)
     union all
  select a.*, 'pre' timeline
    from grp_gdoop_bizops_db.np_impressions_slapp_pre as a
          where cast(eventdate as date) >= cast('2020-01-01' as date)
          ) as fin on fin.dealuuid = dl_sl.dealuuid
group by 
    fin.eventdate,
    fin.deal_landing,
    fin.timeline,
    dl_sl.min_launch_date,
    dl_sl.max_live_date,
    dl_sl.live_days_on_sl,
    dl_sl.min_live_on_groupon, 
    dl_sl.diff_btw_sl_min_groupon_min,
    dl_sl.number_of_days_comparison,
    fin.dealuuid,
    dl_sl.merchant_uuid,
    fin.position
;*/


insert overwrite table grp_gdoop_bizops_db.np_sl_tab_preinput
select 
       dealuuid,
       min_launch_date, 
       max_live_date,
       min_live_on_groupon,
       live_days_on_sl,
       number_of_days_comparison,
       deal_landing, 
       timeline,
       sum(impressions) total_imps,
       sum(clicks) total_clicks,
       sum(merch_revenue) merch_revenue,
       sum(total_orders) total_orders
from 
(select 
     dl_sl.min_launch_date,
     dl_sl.max_live_date,
     dl_sl.live_days_on_sl,
     dl_sl.min_live_on_groupon, 
     dl_sl.number_of_days_comparison,
     dl_sl.diff_btw_sl_min_groupon_min,
     dl_sl.merchant_uuid,
     dl_sl.dealuuid,
     fin.position,
     fin.eventdate,
     fin.deal_landing,
     fin.timeline,
     count(1) impressions,
     count(distinct aogcampaignid) impressions_aog,
     count(distinct dv_time) clicks,
     count(distinct bcookie) distinct_users,
     sum(nob-nor) merch_revenue,
     count(distinct order_uuid) total_orders
from
grp_gdoop_bizops_db.np_sl_min_deal as dl_sl
left join 
(select a.*, 'pre' timeline
    from grp_gdoop_bizops_db.np_impressions_sl_pre as a
         where cast(eventdate as date) >= cast('2020-01-01' as date)
               and cast(position as int) <= 20 
     union all
  select a.*, 'pre' timeline
    from grp_gdoop_bizops_db.np_impressions_slapp_pre as a
          where cast(eventdate as date) >= cast('2020-01-01' as date)
                and cast(position as int) <= 20
 ) as fin on fin.dealuuid = dl_sl.dealuuid
group by 
     dl_sl.min_launch_date,
     dl_sl.max_live_date,
     dl_sl.live_days_on_sl,
     dl_sl.min_live_on_groupon, 
     dl_sl.number_of_days_comparison,
     dl_sl.diff_btw_sl_min_groupon_min,
     dl_sl.merchant_uuid,
     dl_sl.dealuuid,
     fin.position,
     fin.eventdate,
     fin.deal_landing,
     fin.timeline) as fin 
where
     cast(eventdate as date) >= date_add(cast(min_launch_date as date), - number_of_days_comparison)
group by 
     dealuuid,
     min_launch_date, 
     max_live_date,
     min_live_on_groupon,
     live_days_on_sl,
     number_of_days_comparison,
     deal_landing, 
     timeline
;



insert overwrite table grp_gdoop_bizops_db.np_sl_tab_postinput 
select 
       dealuuid,
       min_launch_date, 
       max_live_date,
       min_live_on_groupon,
       live_days_on_sl,
       number_of_days_comparison,
       deal_landing, 
       timeline,
       sum(impressions) total_imps,
       sum(clicks) total_clicks,
       sum(merch_revenue) merch_revenue,
       sum(total_orders) total_orders
from 
(select 
     dl_sl.min_launch_date,
     dl_sl.max_live_date,
     dl_sl.live_days_on_sl,
     dl_sl.min_live_on_groupon, 
     dl_sl.number_of_days_comparison,
     dl_sl.diff_btw_sl_min_groupon_min,
     dl_sl.merchant_uuid,
     dl_sl.dealuuid,
     fin.eventdate,
     fin.deal_landing,
     fin.timeline,
     count(1) impressions,
     count(distinct aogcampaignid) impressions_aog,
     count(distinct dv_time) clicks,
     count(distinct bcookie) distinct_users,
     sum(nob-nor) merch_revenue,
     count(distinct order_uuid) total_orders
from
grp_gdoop_bizops_db.np_sl_min_deal as dl_sl
left join 
(select a.*, 'post' timeline
    from grp_gdoop_bizops_db.np_impressions_sl as a
    where cast(position as int) <= 20
  union all
  select a.*, 'post' timeline
    from grp_gdoop_bizops_db.np_impressions_sl_app as a 
    where cast(position as int) <= 20
 ) as fin on fin.dealuuid = dl_sl.dealuuid
group by 
   dl_sl.min_launch_date,
     dl_sl.max_live_date,
     dl_sl.live_days_on_sl,
     dl_sl.min_live_on_groupon, 
     dl_sl.number_of_days_comparison,
     dl_sl.diff_btw_sl_min_groupon_min,
     dl_sl.merchant_uuid,
     dl_sl.dealuuid,
     fin.eventdate,
     fin.deal_landing,
     fin.timeline) as fin 
where
     cast(eventdate as date) <= date_add(cast(min_launch_date as date), number_of_days_comparison)
group by 
     dealuuid,
     min_launch_date, 
     max_live_date,
     min_live_on_groupon,
     live_days_on_sl,
     number_of_days_comparison,
     deal_landing, 
     timeline
;



drop table grp_gdoop_bizops_db.np_sl_tab_comp_input;
create table grp_gdoop_bizops_db.np_sl_tab_comp_input stored as orc as 
select 
       fin.dealuuid,
       b.deal_permalink, 
       c.merchant_uuid,
       d.merchant_name,
       fin.min_launch_date,
       fin.min_live_on_groupon,
       fin.live_days_on_sl,
       fin.number_of_days_comparison,
       sum(case when timeline = 'pre' then total_imps end)  total_imps_pre, 
       sum(case when timeline = 'post' and deal_landing = 'Organic' then total_imps end) total_imps_post_org, 
       sum(case when timeline = 'post' and deal_landing = 'Sponsored' then total_imps end) total_imps_post_sl,
       sum(case when timeline = 'pre' then total_clicks end)  total_clicks_pre, 
       sum(case when timeline = 'post' and deal_landing = 'Organic' then total_clicks end) total_clicks_post_org, 
       sum(case when timeline = 'post' and deal_landing = 'Sponsored' then total_clicks end) total_clicks_post_sl,
       sum(case when timeline = 'pre' then merch_revenue end)  merch_revenue_pre, 
       sum(case when timeline = 'post' and deal_landing = 'Organic' then merch_revenue end) merch_revenue_post_org, 
       sum(case when timeline = 'post' and deal_landing = 'Sponsored' then merch_revenue end) merch_revenue_post_sl,
       sum(case when timeline = 'pre' then total_orders end)  total_orders_pre, 
       sum(case when timeline = 'post' and deal_landing = 'Organic' then total_orders end) total_orders_post_org, 
       sum(case when timeline = 'post' and deal_landing = 'Sponsored' then total_orders end) total_orders_post_sl
from
(
select * from grp_gdoop_bizops_db.np_sl_tab_preinput
union all 
select * from grp_gdoop_bizops_db.np_sl_tab_postinput
) as fin
left join (select deal_id, max(deal_permalink) deal_permalink from user_edwprod.dim_gbl_deal_lob group by deal_id) as b on fin.dealuuid = b.deal_id
left join grp_gdoop_bizops_db.pai_deals as c on fin.dealuuid = c.deal_uuid
left join grp_gdoop_bizops_db.pai_merchants as d on c.merchant_uuid = d.merchant_uuid
group by 
    fin.dealuuid,
    b.deal_permalink,
    fin.min_launch_date,
    fin.min_live_on_groupon,
    fin.live_days_on_sl,
    fin.number_of_days_comparison,
    c.merchant_uuid,
    d.merchant_name
;
  
select 
    * 
from 
(select merchant_uuid, count(1) cnz from grp_gdoop_bizops_db.pai_merchants group by 1)
where cnz > 1;

select * from grp_gdoop_bizops_db.nvp_to_temp_availablity;


----------------------------------------------------------------------------------------------------------------------------------------------------------------SELF SERVE FUNNEL 
select * from grp_gdoop_bizops_db.np_sl_drecommender;

insert overwrite table grp_gdoop_bizops_db.np_ss_sl_user_granular
select 
    consumerid, 
    merchantid, 
    consumeridsource,
    rawpagetype,
    ROW_NUMBER () over (partition by merchantid, consumerid, consumeridsource, eventdate order by eventtime asc) row_num_rank,
    eventdate
from 
(select
    consumerid, 
    merchantid, 
    consumeridsource,
    rawpagetype,
    min(eventtime) eventtime,
    eventdate
from
    grp_gdoop_pde.junoHourly
where
    eventdate >= '2021-07-15'
    and clientplatform in ('web','Touch')
    and eventDestination = 'other'
    and event = 'merchantPageView'
    and country = 'US'
    and pageapp = 'sponsored-campaign-itier'
    and merchantid is not null
group by consumerid, merchantid, consumeridsource, rawpagetype, eventdate) as fin
;


insert overwrite table grp_gdoop_bizops_db.np_sssl_tab_login2
select 
    date_sub(next_day(a.eventdate, 'MON'), 1) eventweek, 
    case when a.row_num_rank = b.max_interaction then 1 else 0 end max_interaction,
    c.l2,
    a.rawpagetype, 
    a.row_num_rank,
    count(1) distinct_logins
from grp_gdoop_bizops_db.np_ss_sl_user_granular as a 
left join 
    (select 
         eventdate,
         merchantid, 
         consumerid,
         max(row_num_rank) max_interaction
    from grp_gdoop_bizops_db.np_ss_sl_user_granular
    group by 
         eventdate, 
         merchantid,
         consumerid) as b on a.eventdate = b.eventdate and a.merchantid = b.merchantid and coalesce(a.consumerid,'apple') = COALESCE(b.consumerid, 'apple')
left join grp_gdoop_bizops_db.pai_merchants as c on a.merchantid = c.merchant_uuid
group by 
     date_sub(next_day(a.eventdate, 'MON'), 1), 
     case when a.row_num_rank = b.max_interaction then 1 else 0 end,
     c.l2,
     a.rawpagetype, 
     a.row_num_rank
       
select * from grp_gdoop_bizops_db.np_ss_sl_user_granular;

------------------------------------------------------------------------------------------------------------------------------------------------MERCHANT DROP OFF TARGETTING

          
        
        
        
        
        
---------------------------------------------------------------------------------------------------------------------------------------------------DASHBOARD input

     

create multiset volatile table np_inv_price as (
select 
           inv_product_uuid, 
           price_value, 
           contract_buy_price/100 contract_buy_price, 
           contract_sell_price/100 contract_sell_price
        from
         (select inv_product_uuid, 
                 updated_ts,
                 ROW_NUMBER() over(partition by inv_product_uuid order by updated_ts desc) row_num, 
                price_value, 
                contract_buy_price, 
                contract_sell_price
           from user_edwprod.dim_deal_inv_product) as f 
           where row_num = 1 
) with data on commit preserve rows;


create multiset volatile table np_temp_avg_price as (
select 
        product_uuid, 
        avg(coalesce(a.contract_sell_price, b.contract_sell_price)) contract_sell_price,  
        avg(coalesce(a.contract_buy_price, b.contract_buy_price)) contract_buy_price 
     from user_edwprod.dim_offer_ext as a 
     left join np_inv_price as b on a.inv_product_uuid = b.inv_product_uuid
       group by 1
) with data on commit preserve rows;


drop table sandbox.np_sl_supplier_l3_daily;
create table sandbox.np_sl_supplier_l3_daily as (
select 
     report_date,
     team_cohort,
     l1 l1, 
     l2 l2, 
     l3 l3, 
     campaign_sub_type,
     sum(impressions) impressions,
     sum(clicks) clicks,
     sum(conversions) conversions,
     sum(unit_sales) unit_sales,
     sum(sales_revenue) sales_revenue,
     sum(total_adspend) total_adspend,
     sum(groupon_deal_revenue) groupon_deal_revenue
     from
(select a.*,
       c.l1, c.l2, c.l3,
       d.team_cohort,
       b.contract_sell_price , 
       b.contract_buy_price , 
       (b.contract_sell_price - b.contract_buy_price)/NULLIFZERO(b.contract_sell_price) groupon_margin, 
       case when sales_revenue * groupon_margin is null then 0 else sales_revenue * groupon_margin end groupon_deal_revenue
from 
     (select 
         cast(report_date as date)  report_date, 
         supplier_id,
         supplier_name,
         sku product_code, 
         campaign_sub_type,
         sum(impressions) impressions,
         sum(clicks) clicks,
         sum(conversions) conversions,
         sum(unitsales) unit_sales,
         sum(sales_revenue) sales_revenue,
         sum(adspend) total_adspend
     from sandbox.np_sl_ad_snapshot as a 
     group by 1,2,3,4,5) as a
left join 
   np_temp_avg_price as b on b.product_uuid = a.product_code
left join 
     (select deal_id, max(grt_l1_cat_name) l1, max(grt_l2_cat_name) l2, max(grt_l3_cat_name) l3  from user_edwprod.dim_gbl_deal_lob group by 1) as c on a.product_code = c.deal_id
left join sandbox.np_sl_supp_mapping as d on a.supplier_name = d.supplier_name
     ) fin group by 1,2,3,4,5,6) with data;




------------------------OLD 
  
select * from dwh_base_sec_view.sf_account; 

insert overwrite table grp_gdoop_bizops_db.np_sl_deal_performance_base
select 
   date_add(last_day(add_months(eventdate, -1)),1) month_start, 
   sli2.dealuuid,
   gdl.deal_permalink,
   doe.merchant_uuid,
   sf.merch_segmentation,
   sf.merchant_name,
   gdl.l1,
   gdl.l2,
   case when gdl.deal_permalink like '%cpn%' then 1 else 0 end coupon,
   sum(deal_rev) deal_rev, 
   sum(red_same_day) deal_rev_same_day, 
   sum(red_30) deal_rev_30_day, 
   sum(red_120) deal_rev_120_day, 
   sum(all_reds) deal_rev_all_red,
   sum(total_cpc) total_groupon_rev,
   sum(citrus_impressions) citrus_impressions,
   sum(citrus_clicks) citrus_clicks,
   sum(total_impressions) sli_impressions, 
   sum(total_clicks) sli_clicks, 
   sum(total_impression_aog) total_impression_aog,
   sum(orders_sold) orders_sold
   from 
(select 
        eventdate,  
        dealuuid, 
        sum(nob - nor) deal_rev, 
        sum(case when redeem_date <= cast(eventdate as date) then (nob-nor) when (nob-nor) is not null then 0 end) red_same_day, 
        sum(case when redeem_date <= date_add(cast(eventdate as date), 30) then (nob-nor) when (nob-nor) is not null then 0 end) red_30,
        sum(case when redeem_date <= date_add(cast(eventdate as date), 120) then (nob-nor) when (nob-nor) is not null then 0 end) red_120,
        sum(case when reds.order_uuid is not null then nob-nor when (nob-nor) is not null then 0 end) all_reds,
        count(1) total_impressions, 
        count(distinct aogcampaignid) total_impression_aog,
        count(distinct dv_time) total_clicks, 
        count(distinct sli.order_uuid) orders_sold
   from grp_gdoop_bizops_db.np_impressions_sl sli 
   left join 
           (select 
               order_uuid,
               redeem_date
            from grp_gdoop_bizops_db.np_redemption_orders
            where redeemed = 1
            group by order_uuid, redeem_date
           ) reds on sli.order_uuid = reds.order_uuid
      group by 
         eventdate, 
         dealuuid) sli2 
left join 
(select 
  report_date, 
  dealuuid, 
  sum(total_cpc) total_cpc, 
  sum(citrus_impressions) citrus_impressions, 
  sum(citrus_clicks) citrus_clicks
  from grp_gdoop_bizops_db.np_citrus_sl
 group by report_date, dealuuid) cit on sli2.dealuuid = cit.dealuuid and  sli2.eventdate = cit.report_date
left join 
          (select deal_id, 
                  max(grt_l1_cat_name) l1, 
                  max(grt_l2_cat_name) l2, 
                  max(country_code) country_code,
                  max(deal_permalink) deal_permalink
                  from user_edwprod.dim_gbl_deal_lob 
                  group by deal_id) as gdl on sli2.dealuuid = gdl.deal_id
left join 
          (select product_uuid, max(merchant_uuid) merchant_uuid from user_edwprod.dim_offer_ext group by product_uuid) as doe on sli2.dealuuid = doe.product_uuid
left join (
          select 
             a.merchant_uuid, 
             max(a.name) merchant_name,
             max(merchant_segmentation__c) merch_segmentation
          from 
             user_edwprod.dim_merchant as a
          left join dwh_base_sec_view.sf_account as b on a.salesforce_account_id = b.id
          group by a.merchant_uuid) sf on doe.merchant_uuid = sf.merchant_uuid
 group by 
     date_add(last_day(add_months(eventdate, -1)),1),
     sli2.dealuuid,
     doe.merchant_uuid,
     sf.merch_segmentation,
     sf.merchant_name,
     gdl.l1,
     gdl.l2,
     gdl.deal_permalink,
     case when gdl.deal_permalink like '%cpn%' then 1 else 0 end
    ;

   
   
drop table grp_gdoop_bizops_db.np_sl_roas_final_deallvl2;
create table grp_gdoop_bizops_db.np_sl_roas_final_deallvl2 stored as orc as
select 
    fin2.*, 
    case when red_rate_30 is null then 'a. no orders made'
        when red_rate_30 <= 0.15 then 'b.<= 15% redemption rate'
        when red_rate_30 <= 0.25 then 'c.<= 25% redemption rate'
        when red_rate_30 <= 0.35 then 'd.<= 35% redemption rate'
        when red_rate_30 <= 0.50 then 'e.<= 50% redemption rate'
        when red_rate_30 <= 0.70 then 'f.<= 70% redemption rate'
        else 'g.>70% redemption rate' end revenue_redeemed_cat,
    case 
        when potential_roas = 0 then 'x.no orders received yet'
        when potential_roas <= 1 then 'a. less than 1'
        when potential_roas <= 2 then 'b. less than 2'
        when potential_roas <= 3 then 'c. less than 3'
        when potential_roas <= 4 then 'd. less than 4'
        when potential_roas is not null then 'e. more than 4'end potential_roas_category, 
    case 
        when potential_roas = 0 then 'x.no orders received yet'
        when potential_30_day_roas = 0 then 'y.no redemptions made'
        when potential_30_day_roas <= 1 then 'a. less than 1'
        when potential_30_day_roas <= 2 then 'b. less than 2'
        when potential_30_day_roas <= 3 then 'c. less than 3'
        when potential_30_day_roas <= 4 then 'd. less than 4'
        when potential_30_day_roas is not null then 'e. more than 4' end days_30_roas_category, 
    case 
        when potential_roas = 0 then 'x.no orders received yet'
        when potential_120_day_roas = 0 then 'y.no redemptions made'
        when potential_120_day_roas <= 1 then 'a. less than 1'
        when potential_120_day_roas <= 2 then 'b. less than 2'
        when potential_120_day_roas <= 3 then 'c. less than 3'
        when potential_120_day_roas <= 4 then 'd. less than 4'
        when potential_120_day_roas is not null then 'e. more than 4' end days_120_roas_category
   from 
(select 
    fin.*,
     case when deal_rev is not null then deal_rev_30_day/deal_rev end red_rate_30, 
     case when (total_groupon_rev is not null and total_groupon_rev <> 0) then (case when deal_rev is null then 0 else deal_rev end)/total_groupon_rev end potential_roas, 
     case when (total_groupon_rev is not null and total_groupon_rev <> 0) then (case when deal_rev_30_day is null then 0 else deal_rev_30_day end)/total_groupon_rev end potential_30_day_roas,
     case when (total_groupon_rev is not null and total_groupon_rev <> 0) then (case when deal_rev_120_day is null then 0 else deal_rev_120_day end)/total_groupon_rev end potential_120_day_roas
    from 
    grp_gdoop_bizops_db.np_sl_deal_performance_base fin 
) as fin2
;


drop table grp_gdoop_bizops_db.np_sl_roas_final2;
create table grp_gdoop_bizops_db.np_sl_roas_final2 stored as orc as
select 
   month_start, 
   l1,
   l2,
   sum(deal_rev) all_merch_rev, 
   sum(deal_rev_same_day) all_merch_rev_same_day, 
   sum(deal_rev_30_day) all_merch_rev_30_day, 
   sum(deal_rev_120_day) all_merch_rev_120_day, 
   sum(deal_rev_all_red) all_merch_rev_all_red,
   sum(total_groupon_rev) all_total_groupon_rev,
   sum(citrus_impressions) all_citrus_impressions,
   sum(citrus_clicks) all_citrus_clicks, 
   sum(sli_impressions) all_sli_impressions, 
   sum(total_impression_aog) all_total_impression_aog,
   sum(sli_clicks) all_sli_clicks, 
   sum(orders_sold) orders_sold
   from 
     grp_gdoop_bizops_db.np_sl_deal_performance_base
group by 
  month_start,
  l1,l2
;


