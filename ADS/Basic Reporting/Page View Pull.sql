drop table grp_gdoop_bizops_db.np_sl_pagev_temp;
create table grp_gdoop_bizops_db.np_sl_pagev_temp (
    platform string,
    page_views int,
    unique_page_views int
) partitioned by (dt string)
stored as orc
tblproperties ("orc.compress"="SNAPPY");


select a.*
from grp_gdoop_bizops_db.np_temp_fgt_div as a 
join 
(select * from grp_gdoop_bizops_db.pai_deals where country_code = 'US') as b on a.deal_uuid = b.deal_uuid;

select * from grp_gdoop_bizops_db.np_temp_fgt_div where permalink = 'the-sculpt-pod-3';

drop table grp_gdoop_bizops_db.np_temp_fgt_div;
create table grp_gdoop_bizops_db.np_temp_fgt_div stored as orc as 
select 
    a.deal_uuid, 
    b.permalink, 
    transaction_division,
    count(distinct parent_order_id) total_parent_orders, 
    sum(transaction_qty) transaction_quantity,
    sum(auth_nob_loc) NOB
from edwprod.fact_gbl_transactions as a 
left join grp_gdoop_bizops_db.pai_deals as b on a.deal_uuid = b.deal_uuid 
where a.action = 'authorize' and order_date >= '2022-04-16'
group by 
    a.deal_uuid, 
    b.permalink, 
    transaction_division
;

select * from edwprod.fact_gbl_transactions;

create table grp_gdoop_bizops_db.np_sl_pagev_temp2 (
    platform string,
    page_views int,
    unique_page_views int
) partitioned by (dt string)
stored as orc
tblproperties ("orc.compress"="SNAPPY");
 
select * from grp_gdoop_bizops_db.pai_deals;

select * from grp_gdoop_bizops_db.np_sl_pagev_temp 
union all 
select * from grp_gdoop_bizops_db.np_sl_pagev_temp2

insert overwrite table grp_gdoop_bizops_db.np_sl_pagev_temp partition (dt)
select platform, count(concat(bcookie, dt)) as page_views, count(distinct concat(bcookie, dt)) as unique_page_views, dt
from prod_groupondw.bld_events
where dt >= date_sub(CURRENT_DATE, 15)  
and lower(platform) in ('desktop', 'touch')
and lower(page_country) in ('us')
and lower(event) in ('pageview')
and lower(trim(page_type)) like '%homepage/index%'
and lower(page_hostname) like '%groupon%'
and bcookie is not null
and bcookie <> ''
and lower(trim(bcookie)) <> 'null'
and bot_flag = '0'
and internal_ip_ind = '0'
group by platform, dt;

insert overwrite table grp_gdoop_bizops_db.np_sl_pagev_temp2 partition (dt)
select lower(trim(regexp_replace(clientplatform,'\\t|\\n|\\r|\\u0001', ''))) as clientplatform, 
count(concat(bcookie, eventdate)) as page_views, count(distinct concat(bcookie, eventdate)) as unique_page_views, 
eventdate
from grp_gdoop_pde.junohourly
where eventdate >= date_sub(CURRENT_DATE, 15) 
and lower(platform) = 'mobile'
and lower(trim(regexp_replace(clientplatform,'\\t|\\n|\\r|\\u0001', ''))) in ('android', 'iphone')
and lower(country) in ('us')
and rawevent = 'GRP14'
and lower(trim(REGEXP_REPLACE(pageid,'\\t|\\n|\\r|\\u0001', ' '))) in ('home_tab','home_page','featured', 'featuredrapifragment')
and lower(event) in ('genericpageview')
and lower(eventdestination) in ('searchbrowseview')
and bcookie is not null
and bcookie <> ''
and lower(brand) = 'groupon'
and lower(trim(bcookie)) <> 'null'
and bcookie not in ('android_automation_test' ,'ios_automation_test','android_perf_test_nightly','ios_perf_test_nightly')
and lower(trim(useragent)) not like '%bot%'
and lower(trim(useragent)) not like '%crawler%'
and lower(trim(useragent)) not like '%search%'
and lower(trim(useragent)) not like '%spider%'
and lower(trim(useragent)) not like '%spyder%'
and lower(trim(coalesce(pageid,'x'))) <> 'splash'
group by eventdate, lower(trim(regexp_replace(clientplatform,'\\t|\\n|\\r|\\u0001', '')))

select * from sandbox.rev_mgmt_deal_funnel;

----------------

select count(1) from grp_gdoop_bizops_db.np_sl_deals_imps_tran_date;

select * from grp_gdoop_bizops_db.np_sl_carousel_temp
union all 
select * from grp_gdoop_bizops_db.np_sl_carousel_temp2


drop table grp_gdoop_bizops_db.np_sl_carousel_temp;
create table grp_gdoop_bizops_db.np_sl_carousel_temp (
    platform string,
    unique_impressions int
) partitioned by (dt string)
stored as orc
tblproperties ("orc.compress"="SNAPPY");


create table grp_gdoop_bizops_db.np_sl_carousel_temp2 (
    platform string,
    unique_impressions int
) partitioned by (dt string)
stored as orc
tblproperties ("orc.compress"="SNAPPY");


insert overwrite table grp_gdoop_bizops_db.np_sl_carousel_temp partition (dt)
select platform, count(distinct concat(user_browser_id, dt)) as unique_impressions, dt
from prod_groupondw.bld_widget_contents 
where lower(platform) in ('desktop', 'touch')
and dt >= date_sub(CURRENT_DATE, 15) 
and user_browser_id <> '' 
and user_browser_id is not null
and lower(page_country) IN ('us', 'ca')
and lower(page_hostname) like '%groupon%'
and lower(page_type) like '%homepage%'
and lower(widget_content_name) like '%crosschannel_homepage_sponsored_carousel%'
and lower(widget_content_type) = 'compound'
group by dt, platform;



insert overwrite table grp_gdoop_bizops_db.np_sl_carousel_temp2 partition (dt)
select clientplatform, count(distinct concat(bcookie, eventdate)) as unique_impressions, eventdate
from grp_gdoop_pde.junoHourly 
where eventdate >= date_sub(CURRENT_DATE, 15) 
and lower(trim(regexp_replace(clientplatform,'\\t|\\n|\\r|\\u0001', ''))) in ('iphone', 'android')
and country in ('US','CA')
and platform = 'mobile'
and bcookie is not null 
and bcookie <> ''
and lower(trim(bcookie)) <> 'null'
and lower(brand) = 'groupon' 
and bcookie not in ('android_automation_test' ,'ios_automation_test','android_perf_test_nightly','ios_perf_test_nightly')
and case when 'userAgent' like '%bot%' or useragent like '%crawler%' or useragent like '%search%' or useragent like '%spider%' or useragent like '%spyder%' then 1 else 0 end = 0
and lower(trim(extrainfo)) like '%crosschannel_homepage_sponsored_carousel%' 
and lower(impressiontype) = 'collection_card_impression'
and lower(eventdestination) = 'genericimpression'
and lower(event) = 'genericimpression'
group by 
eventdate, clientplatform






select 
bcookie,
extrainfo,
eventdate,
position
from grp_gdoop_pde.junoHourly
where eventdate >= '2022-04-01'
and lower(eventdestination) = 'genericimpression'
and lower(event) = 'genericimpression'
and country in ('US','CA')
--and lower(trim(consumerid)) = '18010e36-415c-11ea-8ecc-0242ac120002'
and bcookie = '7178083F-BCB6-D8B1-919B-9D41C8625973'
and lower(trim(extrainfo)) like '%crosschannel_homepage_sponsored_carousel%' 
and get_json_object(extrainfo, '$.sponsoredAdId') is not null;

select *
from grp_gdoop_pde.junoHourly 
where eventdate >= '2022-04-01' and eventdate <= '2022-04-19'
and lower(trim(regexp_replace(clientplatform,'\\t|\\n|\\r|\\u0001', ''))) in ('iphone', 'android')
and country in ('US','CA')
and platform = 'mobile'
and bcookie is not null 
and bcookie <> ''
and lower(trim(bcookie)) <> 'null'
and lower(brand) = 'groupon' 
and bcookie not in ('android_automation_test' ,'ios_automation_test','android_perf_test_nightly','ios_perf_test_nightly')
and case when 'userAgent' like '%bot%' or useragent like '%crawler%' or useragent like '%search%' or useragent like '%spider%' or useragent like '%spyder%' then 1 else 0 end = 0
and lower(trim(extrainfo)) like '%crosschannel_homepage_sponsored_carousel%' 
and lower(impressiontype) = 'collection_card_impression'
and lower(eventdestination) = 'genericimpression'
and lower(event) = 'genericimpression'
and bcookie = '7178083F-BCB6-D8B1-919B-9D41C8625973'
;


select 
*
from grp_gdoop_pde.junoHourly
where eventdate >= '2022-04-01'
and country in ('US','CA')
--and lower(trim(consumerid)) = '18010e36-415c-11ea-8ecc-0242ac120002'
and bcookie = '7178083F-BCB6-D8B1-919B-9D41C8625973'
and sponsoredAdId is not null;


