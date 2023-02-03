

------------------------------------------------------------------------------Widget Errors

create table grp_gdoop_bizops_db.nvp_bh_user_max_widgets3 (
    bcookie string,
    consumerid string,
    deal_uuid string,
    l2 string,
    l3 string,
    platform string,
    country_code string
) partitioned by (dt string)
stored as orc
tblproperties ("orc.compress"="SNAPPY");

insert overwrite table grp_gdoop_bizops_db.nvp_bh_user_max_widgets3 partition (dt)
select
    bwc.bcookie,
    bwc.consumerid,
    bev.deal_uuid,
    bev.l2,
    bev.l3,
    sf.platform,
    sf.country_code,
    bwc.dt
from 
(select
        eventdate dt,
        bcookie,
        consumerid,
        parentpageid, 
        clientplatform
    from grp_gdoop_pde.junoHourly
    where lower(widgetname) in ('cartmessage','usermessage')
    and lower(widgetcontenttype) = 'inventory_product_declined'
    and cast(eventdate as date) between date_sub(current_date,5) and date_sub(current_date,1) 
    and lower(platform) in ('web','desktop','touch')
) bwc
join (
    select
        event_date,
        unique_visitors as bcookie,
        max(cookie_first_country_code) country_code,
        max(cookie_first_sub_platform) platform
    from user_groupondw.gbl_traffic_superfunnel
    where cast(event_date as date) between date_sub(current_date,5) and date_sub(current_date,1)
    group by event_date, unique_visitors
) sf on lower(bwc.bcookie) = lower(sf.bcookie) and bwc.dt = sf.event_date
left join (
    select
        page_id,
        dt,
        deal_uuid,
        gdl.grt_l2_cat_description l2,
        gdl.grt_l3_cat_description l3
    from user_groupondw.bld_events bev
    left join user_edwprod.dim_gbl_deal_lob gdl on bev.deal_uuid = gdl.deal_id
    where event = 'pageview'
    and cast(dt as date) between date_sub(current_date,5) and date_sub(current_date,1) 
) bev on bwc.parentpageid = bev.page_id and bwc.dt = bev.dt;

--------------------------------------------------------------------------------------------USER MAX AGG

between date_sub(current_date,5) and date_sub(current_date,1)

create table grp_gdoop_bizops_db.nvp_user_max_widgets_agg3 (
    platform string,
    deal_uuid string,
    l2 string, 
    l3 string,
    uniq_attempts int
) partitioned by (dt string)
stored as orc
tblproperties ("orc.compress"="SNAPPY");

insert overwrite table grp_gdoop_bizops_db.nvp_user_max_widgets_agg3 partition (dt)
select
platform,
deal_uuid,
l2, 
l3, 
count(distinct concat(bcookie, case when length(deal_uuid) = 36 then deal_uuid end)) uniq_attempts,
dt
from grp_gdoop_bizops_db.nvp_bh_user_max_widgets3
where 
cast(dt as date) between date_sub(current_date,10) and date_sub(current_date,1)
group by 
platform,
dt, 
deal_uuid, 
l2, 
l3;


select 
c.dt,
c.platform,
sum(c.uniq_attempts) user_max_errors
from
(select  
      deal_uuid,
      lower(platform) platform,
      cast(dt as date) dt, 
      sum(uniq_attempts) uniq_attempts
      from grp_gdoop_bizops_db.nvp_user_max_widgets_agg3 group by deal_uuid, lower(platform), cast(dt as date)
      ) c 
group by dt, platform;


------------------------------------------------------------------------------------------- REPEAT PURCHASERS
create table grp_gdoop_bizops_db.nvp_bh_purchasers stored as orc as
    select user_uuid, deal_uuid, min(order_date) first_purchase_date
    from user_edwprod.fact_gbl_transactions
    group by user_uuid, deal_uuid;
   
drop table grp_gdoop_bizops_db.nvp_bh_udv_optimus;

create table grp_gdoop_bizops_db.nvp_um_repeat_udv (
    platform string,
    l1 string,
    l2 string,
    deal_uuid string,
    uniq_rpt_views int
) partitioned by (event_date string)
stored as orc
tblproperties ("orc.compress"="SNAPPY");

insert overwrite table grp_gdoop_bizops_db.nvp_um_repeat_udv partition (event_date)
select 
     sf.platform, 
     gdl.grt_l1_cat_name l1,
     gdl.grt_l2_cat_name l2, 
     gdl.deal_id deal_uuid,
     count(distinct concat(sf.bcookie, case when length(sf.deal_uuid) = 36 then sf.deal_uuid end)) uniq_rpt_views,
     sf.event_date
     from grp_gdoop_bizops_db.nvp_bh_purchasers as p
     join (
       select
           x.event_date,
           x.unique_visitors as bcookie,
           y.user_uuid,
           x.deal_uuid,
           max(x.cookie_first_country_code) country_code,
           max(x.cookie_first_sub_platform) platform
        from user_groupondw.gbl_traffic_superfunnel_deal x
             left join prod_groupondw.user_bcookie_mapping y on x.event_date = y.event_date and x.unique_visitors = y.bcookie
        where cast(x.event_date as date) >=  cast('2020-07-15' as date)
        group by x.event_date, x.unique_visitors, y.user_uuid, x.deal_uuid
        ) sf 
        on p.user_uuid = sf.user_uuid and p.deal_uuid = sf.deal_uuid
     join user_edwprod.dim_gbl_deal_lob gdl on p.deal_uuid = gdl.deal_id
     where 
    cast(sf.event_date as date) > cast(p.first_purchase_date as date)
   group by sf.platform, gdl.grt_l1_cat_name, gdl.grt_l2_cat_name, gdl.deal_id, sf.event_date;

-------------------------------------------------------------------------------------------TOTAL UDV

drop table grp_gdoop_bizops_db.nvp_deal_udv;
create table grp_gdoop_bizops_db.nvp_deal_udv (
    deal_uuid string,
    platform string,
    country_code string,
    udv int, 
    dv int
) partitioned by (event_date string)
stored as orc
tblproperties ("orc.compress"="SNAPPY");


insert overwrite table grp_gdoop_bizops_db.nvp_deal_udv partition (event_date)
select 
    deal_uuid,
    cookie_first_sub_platform platform,
    cookie_first_country_code country_code,
    sum(unique_deal_views) udv, 
    sum(deal_views) dv,
    event_date
from
user_groupondw.gbl_traffic_superfunnel_deal
where cast(event_date as date) >= cast('2020-07-13' as date)
group by event_date, deal_uuid, cookie_first_sub_platform, cookie_first_country_code;

-----------------------------------------------------------------------------------------------------DIVISIONS
drop table grp_gdoop_bizops_db.nvp_user_max_divs;
create table grp_gdoop_bizops_db.nvp_user_max_divs stored as orc as
select m.deal_uuid,
max(d.division) division
from user_edwprod.deal_division_map m
join (
select division_uuid,
max(division_name) division
from user_groupondw.dim_lat_lng_loc_map
group by division_uuid
) d on d.division_uuid = m.division_id
group by m.deal_uuid;

--------------------------------------------------------------------------------------------------

drop table grp_gdoop_bizops_db.nvp_bh_user_max_widgets4;
create table grp_gdoop_bizops_db.nvp_bh_user_max_widgets4 (
    bcookie string,
    consumerid string,
    deal_uuid string,
    l2 string,
    l3 string,
    bld_platform string,
    sf_sub_platform string,
    country_code string,
    event_time string
) partitioned by (dt string)
stored as orc
tblproperties ("orc.compress"="SNAPPY");



insert overwrite table grp_gdoop_bizops_db.nvp_bh_user_max_widgets4 partition (dt)
select
    bwc.bcookie,
    bwc.consumerid,
    bev.deal_uuid,
    bev.l2,
    bev.l3,
    bwc.platform bld_platform,
    sf.platform sf_sub_platform,
    sf.country_code,
    bwc.event_time,
    bwc.dt
from (
    select
        dt,
        user_browser_id bcookie,
        user_uuid consumerid,
        event_time,
        parent_page_id, 
        platform
    from user_groupondw.bld_widget_contents
    where lower(widget_name) in ('cartmessage','usermessage')
    and lower(wc_data_json) = 'inventory_product_declined'
    and cast(dt as date) between cast('2020-09-01' as date) and cast('2020-09-05' as date)
    and lower(platform) in ('web','desktop','touch')
) bwc
join (
    select
        event_date,
        unique_visitors as bcookie,
        max(cookie_first_country_code) country_code,
        max(cookie_first_sub_platform) platform
    from user_groupondw.gbl_traffic_superfunnel
    where cast(event_date as date) between cast('2020-09-01' as date) and cast('2020-09-05' as date)
    group by event_date, unique_visitors
) sf on lower(bwc.bcookie) = lower(sf.bcookie) and bwc.dt = sf.event_date
left join (
    select
        page_id,
        dt,
        deal_uuid,
        gdl.grt_l2_cat_name l2,
        gdl.grt_l3_cat_name l3
    from user_groupondw.bld_events bev
    left join user_edwprod.dim_gbl_deal_lob gdl on bev.deal_uuid = gdl.deal_id
    where event = 'pageview'
    and cast(dt as date) between cast('2020-09-01' as date) and cast('2020-09-05' as date)
) bev on bwc.parent_page_id = bev.page_id and bwc.dt = bev.dt;


-----------------------------------------------------------------------------------------------BLD Repeat Visitors
drop table grp_gdoop_bizops_db.nvp_bh_um_udv4;
create table grp_gdoop_bizops_db.nvp_bh_um_udv4 (
    platform string,
    l1 string,
    l2 string,
    deal_uuid string,
    bcookie string,
    repeat_purchaser int
) partitioned by (dt string)
stored as orc
tblproperties ("orc.compress"="SNAPPY");

drop table grp_gdoop_bizops_db.nvp_bh_um_udv4;

insert overwrite table grp_gdoop_bizops_db.nvp_bh_um_udv4 partition (dt)
select
        lower(e.platform) platform,
        gdl.grt_l1_cat_name l1,
        gdl.grt_l2_cat_name l2, 
        e.deal_uuid,
        e.bcookie,
        case when p.deal_uuid is not null and cast(e.dt as date) > cast(p.first_purchase_date as date) then 1 end repeat_purchaser,
        e.dt
    from  
        user_groupondw.bld_events e
    left join grp_gdoop_bizops_db.nvp_bh_purchasers p on p.user_uuid = e.user_uuid and p.deal_uuid = e.deal_uuid
    join user_edwprod.dim_gbl_deal_lob gdl on p.deal_uuid = gdl.deal_id
    where 
       cast(e.dt as date) between cast('2020-09-01' as date) and cast('2020-09-05' as date)
    and e.event = 'pageview'
    and e.page_type = 'deals/show'
    group by 
    lower(e.platform), 
    gdl.grt_l1_cat_name, 
    gdl.grt_l2_cat_name,
    e.deal_uuid,
    e.dt, e.bcookie, 
    case when p.deal_uuid is not null and cast(e.dt as date) > cast(p.first_purchase_date as date) then 1 end;

select * from user_groupondw.bld_events where cast(e.dt as date) = cast('2020-09-01' as date);
------------------------------------------------------------------------------------------


   
drop table grp_gdoop_bizops_db.nvp_usermaxerror_tab;
create table grp_gdoop_bizops_db.nvp_usermaxerror_tab (
    platform string,
    l2 string,
    udv int, 
    repeat_views int,
    user_max_errors int
) partitioned by (dt string)
stored as orc
tblproperties ("orc.compress"="SNAPPY");



insert overwrite table grp_gdoop_bizops_db.nvp_usermaxerror_tab partition (dt)
select 
    COALESCE(a.platform, b.platform) platform, 
    COALESCE(a.l2, b.l2) l2,
    sum(a.udv) udv, 
    sum(a.repeat_views) repeat_views, 
    sum(b.user_max_error) user_max_errors,
    COALESCE(a.dt, b.dt) dt
from
(select 
    lower(platform) platform, 
    l2, 
    deal_uuid, 
    count(distinct concat(bcookie, case when length(deal_uuid) = 36 then deal_uuid end)) udv, 
    count(distinct case when repeat_purchaser = 1 then concat(bcookie, case when length(deal_uuid) = 36 then deal_uuid end) end) repeat_views, 
    dt
  from 
    grp_gdoop_bizops_db.nvp_bh_um_udv4
  where 
   cast(dt as date) between cast('2020-09-01' as date) and cast('2020-09-05' as date)
  group by lower(platform), l2, deal_uuid, dt) as a 
full join
(select
    lower(bld_platform) platform,
    l2, 
    deal_uuid,
    count(distinct concat(bcookie, case when length(deal_uuid) = 36 then deal_uuid end)) user_max_error, 
    dt
  from grp_gdoop_bizops_db.nvp_bh_user_max_widgets4
  where 
   cast(dt as date) between cast('2020-09-01' as date) and cast('2020-09-05' as date)
group by 
   lower(bld_platform),
   dt, deal_uuid,
   l2) as b
 on a.platform = b.platform and a.l2 = b.l2 and a.deal_uuid = b.deal_uuid and a.dt = b.dt
 
 group by COALESCE(a.platform, b.platform), COALESCE(a.l2, b.l2), COALESCE(a.dt, b.dt);



-------------------------------------------------------------------------------------------TRASH

drop table grp_gdoop_bizops_db.nvp_user_agg_junotrial;
create table grp_gdoop_bizops_db.nvp_user_agg_junotrial stored as orc as
select
platform,
deal_uuid,
l2,
l3,
count(distinct concat(bcookie, case when length(deal_uuid) = 36 then deal_uuid end)) uniq_attempts,
dt
from grp_gdoop_bizops_db.nvp_bh_user_max_widgets3
where 
cast(dt as date) between cast('2020-07-13' as date) and cast('2020-07-23' as date)
group by 
platform,
dt,
deal_uuid, 
l2,
l3;



create table grp_gdoop_bizops_db.nvp_user_agg_bldtrial stored as orc as
select
platform,
deal_uuid,
l2, 
l3, 
count(distinct concat(bcookie, case when length(deal_uuid) = 36 then deal_uuid end)) uniq_attempts,
dt
from grp_gdoop_bizops_db.nvp_bh_user_max_widgets2
where 
cast(dt as date) between cast('2020-07-13' as date) and cast('2020-07-23' as date)
group by 
platform,
dt, 
deal_uuid, 
l2, 
l3;



-----    -----


select 
c.dt,
platform, 
sum(c.uniq_attempts) user_max_errors
from
(select  
      deal_uuid,
      lower(platform) platform,
      cast(dt as date) dt, 
      sum(uniq_attempts) uniq_attempts
      from grp_gdoop_bizops_db.nvp_user_agg_junotrial group by deal_uuid, lower(platform), cast(dt as date)
      ) c 
group by dt, platform;


