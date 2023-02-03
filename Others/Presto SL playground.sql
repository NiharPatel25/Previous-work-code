select 
     case when total_cpc > 0 then 1 else 0 end impressions_cat, 
     sum(citrus_clicks)
from grp_gdoop_bizops_db.np_citrus_sl_bid where quarter = 'Q2'
group by 1;

select date_start_end,cleared_bid_floor, sum(citrus_clicks) from grp_gdoop_bizops_db.np_sl_bidfloor_tableau 
where dashboard_cut = 'monthly_cut'
group by 1,2
order by 1, 2;


select sum(clicks), sum(impressions) from ad_reporting_na_prod.citrus_master_report_citrus_51_v1 where report_date >= '2021-07-01' and report_date < '2021-08-01';
select sum(clicks), sum(impressions) from ad_reporting_na_prod.citrus_master_report_citrus_51_v1 where report_date >= '2021-08-01';

select report_month, sum(citrus_impressions) from grp_gdoop_bizops_db.np_citrus_data_match group by 1 order by 1;

select 
    date_start_end, sum(sli_impressions) 
   from grp_gdoop_bizops_db.np_sl_roas_final_merchlvl2 
   where dashboard_cut = 'monthly_cut' group by 1 order by 1;

select report_month, placement, sum(citrus_impressions) from grp_gdoop_bizops_db.np_citrus_data_match 
where placement <> 'email' group by 1,2 order by 1,2;

select 
    report_month, sum(citrus_impressions)
 from 
(select a.*, case when placement = 'email' then 1 else 0 end cond 
 from grp_gdoop_bizops_db.np_citrus_data_match as a)
 where cond = 0
 group by 1
order by 1;

select * from ad_reporting_na_prod.citrus_master_report_citrus_51_v1;

drop table grp_gdoop_bizops_db.np_citrus_data_match;
create table grp_gdoop_bizops_db.np_citrus_data_match stored as orc as 
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
     get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(sponsored_product_filters,'[','{"'),':','":"'),', ','", "'),']','"}'), '$.placement') placement2,
     get_json_object(translate(regexp_replace(translate(sponsored_product_filters,'[','{'),':','":"'),']','}'), '$.placement') placement,
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
     date_add(last_day(add_months(report_date, -1)),1) report_month
FROM  ad_reporting_na_prod.citrus_master_report_citrus_51_v1 cit
group by date_add(last_day(add_months(report_date, -1)),1),
     deal_id ,
     get_json_object(translate(regexp_replace(translate(sponsored_product_filters,'[','{'),':','":"'),']','}'), '$.placement'),
     get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(sponsored_product_filters,'[','{"'),':','":"'),', ','", "'),']','"}'), '$.placement'),
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

    
    
select 
    report_month, cond, sum(citrus_impressions)
 from 
(select a.*, case when placement = 'email' then 1 else 0 end 
             cond 
 from grp_gdoop_bizops_db.np_citrus_data_match as a)
 group by 1,2
order by 1,2;

select 
    report_month, email_filter, sum(citrus_impressions)
 from 
(select a.*
 from grp_gdoop_bizops_db.np_citrus_data_match as a)
 group by 1,2
order by 1,2;


select * from ad_reporting_na_prod.citrus_master_report_citrus_51_v1 where report_date >= '2021-08-01';
case when sponsored_product_filters like '%email%' then 1 else 0 end email_present,

drop table grp_gdoop_bizops_db.np_citrus_data_match;
create table grp_gdoop_bizops_db.np_citrus_data_match stored as orc as 
select 
     'citrus' source,
     deal_id dealuuid,
     get_json_object(translate(regexp_replace(translate(sponsored_product_filters,'[','{'),':','":"'),']','}'), '$.platform') platform,
     get_json_object(regexp_replace(translate(regexp_replace(translate(sponsored_product_filters,'[','{"'),':','":"'),',','","'),']','"}'), '$.placement') placement2,
     get_json_object(translate(regexp_replace(translate(sponsored_product_filters,'[','{'),':','":"'),']','}'), '$.placement') placement,
     case when sponsored_product_filters like '%email%' then 1 else 0 end email_filter,
     page citrus_page, 
     sum(revenue) total_cpc, 
     sum(impressions) citrus_impressions, 
     sum(clicks) citrus_clicks,
     date_add(last_day(add_months(report_date, -1)),1) report_month
FROM  ad_reporting_na_prod.citrus_master_report_citrus_51_v1 cit
group by date_add(last_day(add_months(report_date, -1)),1),
     deal_id ,
     get_json_object(translate(regexp_replace(translate(sponsored_product_filters,'[','{'),':','":"'),']','}'), '$.placement'),
     get_json_object(regexp_replace(translate(regexp_replace(translate(sponsored_product_filters,'[','{"'),':','":"'),',','","'),']','"}'), '$.placement'),
     get_json_object(translate(regexp_replace(translate(sponsored_product_filters,'[','{'),':','":"'),']','}'), '$.platform') ,
     case when sponsored_product_filters like '%email%' then 1 else 0 end,
     page;    
    
select * from grp_gdoop_bizops_db.np_citrus_data_match where email_filter = 1;
select * from ad_reporting_na_prod.citrus_master_report_citrus_51_v1 
where deal_id = 'a6037ec7-b4c6-4acd-927f-1953f82b44dd' and page = 'BROAD_DISPLAY' and report_date > '2021-05-01'
and sponsored_product_filters like '%email%';




select 
 distinct xyz
 from 
(select 
     json_extract_scalar(replace(replace(replace(sponsored_product_filters,'[','{'),':','":"'),']','}'), '$.placement') xyz 
from ad_reporting_na_prod.citrus_master_report_citrus_51_v1) where xyz is not null;

select 
distinct * from 
(select json_extract_scalar(replace(replace(replace(replace(sponsored_product_filters,'[','{"'),':','":"'),', ','", "'),']','"}'), '$.placement') xyz 
from ad_reporting_na_prod.citrus_master_report_citrus_51_v1) where xyz is not null;

select 

get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(sponsored_product_filters,'[','{"'),':','":"'),', ','", "'),']','"}'), '$.placement') placement,

select * from grp_gdoop_bizops_db.np_citrus_data_match;

select distinct quarter from ai_reporting.category_bid_floors;
select * from ai_reporting.search_bid_floors;
select * from ai_reporting.search_bid_floors;
select * from ai_reporting.category_bid_floors;
SELECT * FROM grp_gdoop_bizops_db.np_citrus_sl_bid;

cast((page_bid_cpc - page_bid_floor) as float)/page_bid_floor

select 
     fin2.*, 
     cast((page_bid_cpc - page_bid_floor) as double)/page_bid_floor
from 
(select 
       fin.*, 
       case when page_bid is null then 0.5 else page_bid end page_bid_floor, 
       case when page_bid is null then 1 else 0 end page_bid_is_null
   from 
       (select 
            a.*, 
            coalesce(cast(b.page_bid as double), cast(c.page_bid as double)) page_bid,
            total_cpc/citrus_clicks page_bid_cpc
        from grp_gdoop_bizops_db.np_citrus_sl_bid as a 
        left join 
             ai_reporting.search_bid_floors as b on a.bid_join_final = b.search_query and a.quarter = b.quarter
        left join 
             ai_reporting.category_bid_floors as c on a.bid_join_final = c.cat_uuid and a.quarter = c.quarter) as fin) as fin2;
           
         

create table grp_gdoop_bizops_db.np_ss_sl_interaction  (
      merchantid string,
      consumeridsource string, 
      rawpagetype string
)partitioned by (eventdate string) 
stored as orc
tblproperties ("orc.compress"="SNAPPY");


insert overwrite table grp_gdoop_bizops_db.np_ss_sl_interaction partition (eventdate)
select
distinct 
    merchantid, 
    consumeridsource,
    rawpagetype, 
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
    and consumeridsource is not null
    and merchantid is not null
;


select report_month, count(distinct deal_id) from grp_gdoop_bizops_db.np_sl_deals_transaction group by 1;


select * from edwprod.agg_gbl_traffic_fin_deal;


select
distinct 
    consumeridsource
from
    grp_gdoop_pde.junoHourly
where
    eventdate >= '2021-07-15'
    and clientplatform in ('web','Touch')
    and eventDestination = 'other'
    and event = 'merchantPageView'
    and country = 'US'
    and pageapp = 'sponsored-campaign-itier';
    
   
   
select distinct consumeridsource
from
	grp_gdoop_pde.junoHourly
where
	eventdate between '2021-07-15' and '2021-07-28'
	and clientplatform in ('web', 'Touch')
	and eventDestination = 'other'
	and event = 'merchantPageView'
	and country='US'
	and consumeridsource is not null;


select
*
from
	grp_gdoop_pde.junoHourly
where
	eventdate between '2021-07-15' and '2021-07-28'
	and clientplatform in ('web',
	'Touch')
	and eventDestination = 'other'
	and event = 'merchantPageView'
	and country = 'US'
	and pageapp = 'sponsored-campaign-itier'
	and consumeridsource in ('internal_user')
    and merchantid is not null;

	

select * from ai_reporting.CITRUS_MASTER_REPORT_CITRUS_363_v1;


select
    consumerid, 
    merchantid, 
    consumeridsource,
    rawpagetype,
    count(1) total_entries,
    eventdate
from
    grp_gdoop_pde.junoHourly
where
    eventdate >= date_sub(current_date, 5) 
    and clientplatform in ('web','Touch')
    and eventDestination = 'other'
    and event = 'merchantPageView'
    and country = 'US'
    and pageapp = 'sponsored-campaign-itier'
group by 
    consumerid, 
    merchantid, 
    consumeridsource,
    rawpagetype,
    eventdate;