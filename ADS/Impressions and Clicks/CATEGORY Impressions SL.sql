SELECT 
   a.*, 
   get_json_object(lower(extrainfo), '$.type') 
FROM grp_gdoop_bizops_db.np_impressions_sl_app as a 
where eventdate = '2021-03-18';

-----------------------------------------IMPRESSIONS FOR APP

np_temp_sl_cat_imps
np_temp_sl_cat_pageviews
np_temp_sl_cat_aggthree

drop table grp_gdoop_bizops_db.np_temp_sl_cat_imps;
create table grp_gdoop_bizops_db.np_temp_sl_cat_imps  (
      eventtime string,
      bcookie string,
      extrainfo string, 
      dealuuid string, 
      position int,
      sponsored int, 
      platform string
)partitioned by (eventdate string) 
stored as orc
tblproperties ("orc.compress"="SNAPPY");

drop table grp_gdoop_bizops_db.np_temp_sl_cat_andr;
create table grp_gdoop_bizops_db.np_temp_sl_cat_andr  (
      eventtime string,
      bcookie string,
      extrainfo string, 
      dealuuid string, 
      position int,
      sponsored int, 
      platform string,
      rawpagetype string
)partitioned by (eventdate string) 
stored as orc
tblproperties ("orc.compress"="SNAPPY");

drop table grp_gdoop_bizops_db.np_temp_sl_cat_pageviews;
create table grp_gdoop_bizops_db.np_temp_sl_cat_pageviews  (
      eventtime string,
      bcookie string,
      extrainfo string, 
      category string, 
      event_end_time string, 
      platform string
)partitioned by (eventdate string) 
stored as orc
tblproperties ("orc.compress"="SNAPPY");

insert overwrite table grp_gdoop_bizops_db.np_temp_sl_cat_imps partition (eventdate)
select 
   eventtime,
   bcookie, 
   extrainfo, 
   dealuuid,
   position,
   case when get_json_object(extrainfo, '$.sponsoredAdId') is not null then 1 else 0 end sponsored, 
   lower(trim(regexp_replace(clientplatform,'\\t|\\n|\\r|\\u0001', ''))) platform, 
   eventdate
from grp_gdoop_pde.junoHourly 
where 
eventdestination = 'dealImpression'
and event = 'dealImpression'
and eventdate >= date_add(current_date, -18) AND eventdate <= date_add(current_date, -2)
and rawevent = 'GRP2'
and lower(trim(regexp_replace(clientplatform,'\\t|\\n|\\r|\\u0001', ''))) in ('iphone', 'android')
and country in ('US','CA')
and platform = 'mobile'
and bcookie is not null 
and bcookie <> ''
and lower(trim(bcookie)) <> 'null'
and lower(brand) = 'groupon' 
and bcookie not in ('android_automation_test' ,'ios_automation_test','android_perf_test_nightly','ios_perf_test_nightly')
and lower(trim(extrainfo)) like '%categories_tab%' 
;





insert overwrite table grp_gdoop_bizops_db.np_temp_sl_cat_pageviews partition (eventdate)
select
   eventtime, 
   bcookie,
   extrainfo,
   get_json_object(lower(extrainfo), '$.screen_name') category,
   lead(eventtime) over (partition by bcookie order by eventtime) event_end_time,
   lower(trim(regexp_replace(clientplatform,'\\t|\\n|\\r|\\u0001', ''))) platform,
   eventdate
from grp_gdoop_pde.junoHourly 
where 
eventdestination = 'searchBrowseView'
and event = 'genericPageView'
and eventdate >= date_add(current_date, -18) AND eventdate <= date_add(current_date, -2)
and lower(trim(regexp_replace(clientplatform,'\\t|\\n|\\r|\\u0001', ''))) in ('iphone', 'android')
and country in ('US','CA')
and platform = 'mobile'
and bcookie is not null 
and bcookie <> ''
and lower(trim(bcookie)) <> 'null'
and lower(brand) = 'groupon' 
and bcookie not in ('android_automation_test' ,'ios_automation_test','android_perf_test_nightly','ios_perf_test_nightly')
and get_json_object(lower(extrainfo), '$.screen_name') is not null
;

drop table grp_gdoop_bizops_db.np_temp_sl_cat_aggthree;
CREATE TABLE grp_gdoop_bizops_db.np_temp_sl_cat_aggthree stored AS orc AS  
select 
     a.eventdate,
     a.eventtime,
     a.bcookie, 
     a.extrainfo, 
     a.dealuuid,
     a.position,
     a.sponsored,
     a.platform,
     max(b.category) category
     ---max(coalesce(b.category, c.category)) category
from 
   grp_gdoop_bizops_db.np_temp_sl_cat_imps as a 
   left join (select * from  grp_gdoop_bizops_db.np_temp_sl_cat_pageviews) as b  on a.bcookie = b.bcookie and a.eventdate = b.eventdate and a.platform = b.platform
   where 
       a.eventdate >= date_add(current_date, -56) AND a.eventdate <= date_add(current_date, -2)
       and a.eventtime >= b.eventtime 
       and a.eventtime <= b.event_end_time
   group by 
   a.eventdate,
   a.eventtime,
   a.bcookie, 
   a.extrainfo,
   a.dealuuid,
   a.position,
   a.sponsored, 
   a.platform
   ;
  
-----------------------------------------------------------------------------------------ALLLLL

drop table grp_gdoop_bizops_db.np_sl_temp_imps_all;
create table grp_gdoop_bizops_db.np_sl_temp_imps_all stored as orc as 
select
cast(position as int )-1 position,
date_sub(next_day(eventdate, 'MON'), 1) weekly_day,
sponsored, 
case when sponsored = 1 then two.team_cohort else 'none' end team_cohort,
case when category like '%things to do%' then 'TTD'
when category like '%food & drink%'  then 'FND'
when category like '%beauty & spas%' then 'HBW'
when category like '%local%' then 'other - local' 
else 'others - browse'
end category_page,
platform,
count(distinct concat(bcookie, dealuuid, eventdate)) impressions
from grp_gdoop_bizops_db.np_temp_sl_cat_aggthree as one
left join (select a.sku, max(b.team_cohort) team_cohort
           from grp_gdoop_bizops_db.np_sl_ad_snapshot as a
           left join (select * from grp_gdoop_bizops_db.np_sl_supp_mapping where team_cohort <> 'Test') as b on a.supplier_name = b.supplier_name
           group by a.sku) as two on one.dealuuid = two.sku
where position <= 21
group by 
position, 
sponsored, 
date_sub(next_day(eventdate, 'MON'), 1),
case when category like '%things to do%' then 'TTD'
when category like '%food & drink%'  then 'FND'
when category like '%beauty & spas%' then 'HBW'
when category like '%local%' then 'other - local' 
else 'others - browse'
end,
platform,
case when sponsored = 1 then two.team_cohort else 'none' end
union all
select 
   cast(position as int) position, 
   date_sub(next_day(eventdate, 'MON'), 1) weekly_day,
   sponsored, 
   case when sponsored = 1 then two.team_cohort else 'none' end team_cohort,
   case when fullurl like '%things-to-do%' then 'TTD'
        when fullurl like '%food-and-drink%'  then 'FND'
        when fullurl like '%beauty-and-spas%' then 'HBW'
        when fullurl like '%local%' then 'other - local' 
        else 'others - browse'
        end category_page,
   clientplatform,
   count(distinct concat(bcookie, eventdate, dealuuid)) impressions
from 
(select 
      case when cl.aogcampaignid is not null then 1 else 0 end sponsored,
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
        rawpagetype,
        fullurl,
        cl.eventdate
     from ai_reporting.sl_imp_clicks cl
     where eventdate >= date_add(current_date, -56) and eventdate <= date_add(current_date, -2)
) as fin 
left join (select a.sku, max(b.team_cohort) team_cohort
           from grp_gdoop_bizops_db.np_sl_ad_snapshot as a
           left join (select * from grp_gdoop_bizops_db.np_sl_supp_mapping where team_cohort <> 'Test') as b on a.supplier_name = b.supplier_name
           group by a.sku) as two on fin.dealuuid = two.sku
where fin.page = 'browse' and cast(position as int) <= 20
group by 
   position, 
   date_sub(next_day(eventdate, 'MON'), 1),
   sponsored, 
   case when sponsored = 1 then two.team_cohort else 'none' end,
   case when fullurl like '%things-to-do%' then 'TTD'
        when fullurl like '%food-and-drink%'  then 'FND'
        when fullurl like '%beauty-and-spas%' then 'HBW'
        when fullurl like '%local%' then 'other - local' 
        else 'others - browse'
        end,
   clientplatform
;

-----------------------------------------IMPRESSIONS FOR WEB
drop table grp_gdoop_bizops_db.np_temp_sl_cat_imps_web;
create table grp_gdoop_bizops_db.np_temp_sl_cat_imps_web  (
      event_time int,
      bcookie string,
      wc_data_json string, 
      dealuuid string, 
      widget_content_position int,
      sponsored int, 
      platform string
)partitioned by (event_date string) 
stored as orc
tblproperties ("orc.compress"="SNAPPY");



insert overwrite table grp_gdoop_bizops_db.np_temp_sl_cat_imps_web partition (event_date)
select 
   event_time,
   user_browser_id, 
   wc_data_json, 
   deal_uuid,
   widget_content_position,
   case when lower(wc_data_json) like '%sponsored%' then 1 else 0 end sponsored,
   lower(platform) platform,
   dt
from prod_groupondw.bld_widget_contents 
where lower(platform) in ('desktop', 'touch')
and dt >= '2022-03-01' AND dt <= '2022-03-07'
and user_browser_id <> '' 
and user_browser_id is not null
and lower(page_country) IN ('us', 'ca')
and lower(page_hostname) like '%groupon%'
and lower(page_type) like '%browse%'
and lower(page_type) is not like '%query=%'
;


drop table grp_gdoop_bizops_db.np_temp_sl_cat_imps_web2;
create table grp_gdoop_bizops_db.np_temp_sl_cat_imps_web2  (
      eventtime int,
      bcookie string,
      extrainfo string, 
      dealuuid string, 
      position int,
      sponsored int,
      rawpagetype string,
      platform string
)partitioned by (event_date string) 
stored as orc
tblproperties ("orc.compress"="SNAPPY");


drop table grp_gdoop_bizops_db.np_temp_sl_cat_imps_web2;
create table grp_gdoop_bizops_db.np_temp_sl_cat_imps_web2 stored as orc as 
select 
   eventtime,
   bcookie, 
   extrainfo, 
   dealuuid,
   position,
   lower(trim(regexp_replace(clientplatform,'\\t|\\n|\\r|\\u0001', ''))) platform, 
   rawpagetype,
   eventdate,
   clientplatform,
   sponsoredAdId,
   fullurl,
   case when fullurl like '%query=%' then str_to_map(translate(fullurl,'?','&'),'&','=')['query'] end  search_query
from grp_gdoop_pde.junoHourly 
where 
eventdestination = 'dealImpression'
and lower(brand) = 'groupon' 
and eventdate >= '2022-03-01' and eventdate <= '2022-03-21'
and clientplatform in ('Touch','web')
and bcookie is not null 
and bcookie <> ''
and lower(trim(bcookie)) <> 'null'
and country in ('US','CA')
and sponsoredAdId is not null;




  
  

--  left join (select * from  grp_gdoop_bizops_db.np_temp_sl_cat_pageviews where category like '%browse | browse categories%' and event_end_time is null) as c
 --       on a.bcookie = c.bcookie and a.eventdate = c.eventdate and a.eventtime >= c.eventtime
   
   
   