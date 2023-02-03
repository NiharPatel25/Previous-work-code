

-----Tableau Input



select
a.deal_uuid,
a.l2,
sum(uniq_attempts) user_max,
sum(uniq_rpt_views) rpt_view, 
sum(three_wk_uk) udv
from
(select deal_uuid, l2, sum(uniq_attempts) uniq_attempts
from grp_gdoop_bizops_db.nvp_user_max_widgets_agg 
group by deal_uuid, l2) a
left join 
(select deal_uuid, sum(uniq_rpt_views) uniq_rpt_views 
from grp_gdoop_bizops_db.nvp_bh_udv_useruuid2 
where dt between '2020-07-16' and '2020-08-04' group by deal_uuid) b on a.deal_uuid = b.deal_uuid
left join grp_gdoop_bizops_db.nvp_user_max_avg c on a.deal_uuid = c.deal_id
group by l2, a.deal_uuid order by user_max desc
limit 1000;



----



select * from user_edwprod.agg_gbl_traffic_deal limit 5;



create table grp_gdoop_bizops_db.nvp_widget_tab stored as orc as
select 
a.deal_uuid, 
a.dt, 
sum(uniq_attempts) user_max_error, 
sum(uniq_rpt_views) total_rpt_views
from (select dt, deal_uuid, sum(uniq_rpt_views) uniq_rpt_views from grp_gdoop_bizops_db.nvp_bh_udv_useruuid2 group by dt, deal_uuid) b 
right join grp_gdoop_bizops_db.nvp_user_max_widgets_agg a
on a.deal_uuid = b.deal_uuid and a.dt = b.dt
group by a.deal_uuid, a.dt;



------EARLY WORK


create table grp_gdoop_bizops_db.nvp_bh_user_max_widgets (
    bcookie string,
    consumerid string,
    deal_uuid string,
    l2 string,
    l3 string,
    platform string,
    event_time string
) partitioned by (dt string)
stored as orc
tblproperties ("orc.compress"="SNAPPY");


insert into grp_gdoop_bizops_db.nvp_bh_user_max_widgets partition (dt)
select
    bwc_c.bcookie,
    bwc_c.consumerid,
    bev.deal_uuid,
    bev.l2,
    bev.l3,
    bwc_c.platform,
    bwc_c.event_time,
    bwc_c.dt
from 
(select 
    bwc.bcookie,
    bwc.consumerid,
    bwc.platform,
    bwc.event_time,
    bwc.dt, 
    bwc.parent_page_id
from
   (   select
        dt,
        user_browser_id bcookie,
        user_uuid consumerid,
        event_time,
        parent_page_id,
        platform
      from user_groupondw.bld_widget_contents
        where lower(widget_name) in ('cartmessage','usermessage')
        and lower(widget_content_type) = 'inventory_product_declined'
        and dt between date_sub(current_date,18) and date_sub(current_date,16)
        and lower(platform) in ('web','desktop','touch')
       and page_country = 'US'
   ) bwc
   left join (
      select 
       trim(cookie_b) cookie_b, 
       event_date 
     from 
       marketing_analytics_dev_db.dim_bot_cookies 
      where country_code = 'US' and event_date between date_sub(current_date,18) and date_sub(current_date,16)
      group by cookie_b, event_date
   ) mac on lower(bwc.bcookie) = lower(mac.cookie_b) and bwc.dt = mac.event_date
   where mac.cookie_b is null) as bwc_c
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
    and page_country = 'US'
    and dt between date_sub(current_date,18) and date_sub(current_date,16)
    group by page_id, dt, deal_uuid, gdl.grt_l2_cat_description, gdl.grt_l3_cat_description
) bev on bwc_c.parent_page_id = bev.page_id and bwc_c.dt = bev.dt;

       
select
        dt,
        user_browser_id bcookie,
        user_uuid consumerid,
        event_time,
        parent_page_id
    from user_groupondw.bld_widget_contents
    where lower(widget_name) in ('cartmessage','usermessage')
    and lower(widget_content_type) = 'inventory_product_declined'
    and dt between date_sub(current_date,21) and date_sub(current_date,19)
    and lower(platform) in ('web','desktop','touch')
    
    
-----
drop table grp_gdoop_bizops_db.nvp_user_max_widgets_agg;

create table grp_gdoop_bizops_db.nvp_user_max_widgets_agg stored as orc as
select 
dt, 
deal_uuid, 
l2, 
l3, 
count(distinct concat(bcookie, case when length(deal_uuid) = 36 then deal_uuid end)) uniq_attempts
from grp_gdoop_bizops_db.nvp_bh_user_max_widgets 
group by 
dt, 
deal_uuid, 
l2, 
l3;

create table grp_gdoop_bizops_db.nvp_widget_tab stored as orc as
select 
a.deal_uuid, 
a.dt, 
sum(uniq_attempts) user_max_error, 
sum(uniq_rpt_views) total_rpt_views
from (select dt, deal_uuid, sum(uniq_rpt_views) uniq_rpt_views from grp_gdoop_bizops_db.nvp_bh_udv_useruuid2 group by dt, deal_uuid) b 
right join grp_gdoop_bizops_db.nvp_user_max_widgets_agg a
on a.deal_uuid = b.deal_uuid and a.dt = b.dt
group by a.deal_uuid, a.dt;


/* date_sub(next_day(dt, 'MON'), 1) ASSIGNS IT TO THE LAST SUNDAY */

------
drop table grp_gdoop_bizops_db.nvp_bh_user_max_widgets2;


select dt, date_sub(next_day(dt, 'MON'), 1)
from grp_gdoop_bizops_db.nvp_user_max_widgets_agg2 group by dt, date_sub(next_day(dt, 'MON'), 1) order by dt;

/*create table grp_gdoop_bizops_db.nvp_bh_user_max_widgets2 (
    bcookie string,
    consumerid string,
    deal_uuid string,
    l2 string,
    l3 string,
    platform string,
    event_time string
) partitioned by (dt string)
stored as orc
tblproperties ("orc.compress"="SNAPPY");

------
drop table grp_gdoop_bizops_db.nvp_bh_user_max_widgets2;

create table grp_gdoop_bizops_db.nvp_bh_user_max_widgets (
    bcookie string,
    consumerid string,
    deal_uuid string,
    l2 string,
    l3 string,
    platform string,
    event_time string
) partitioned by (dt string)
stored as orc
tblproperties ("orc.compress"="SNAPPY");



insert into grp_gdoop_bizops_db.nvp_bh_user_max_widgets2 partition (dt)
select
    bwc.bcookie,
    bwc.consumerid,
    bev.deal_uuid,
    bev.l2,
    bev.l3,
    bwc.platform,
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
    and lower(widget_content_type) = 'inventory_product_declined'
    and dt between date_sub(current_date,3) and date_sub(current_date,1)
    and lower(platform) in ('web','desktop','touch')
    and page_country = 'US'
) bwc
join (
    select
        event_date,
        unique_visitors as bcookie
    from user_groupondw.gbl_traffic_superfunnel
    where event_date between date_sub(current_date,3) and date_sub(current_date,1) 
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
    and page_country = 'US'
    and dt between date_sub(current_date,3) and date_sub(current_date, 1)
    group by page_id, dt, deal_uuid, gdl.grt_l2_cat_description, gdl.grt_l3_cat_description
) bev on bwc.parent_page_id = bev.page_id and bwc.dt = bev.dt;




select
        dt,
        user_browser_id bcookie,
        user_uuid consumerid,
        event_time,
        parent_page_id, 
        platform
    from user_groupondw.bld_widget_contents
    where lower(widget_name) in ('cartmessage','usermessage')
    and lower(widget_content_type) = 'inventory_product_declined'
    and dt between date_sub(current_date,3) and date_sub(current_date,1)
    and lower(platform) in ('web','desktop','touch')
    and page_country = 'US' limit 5; */
   
select * from user_groupondw.bld_widget_contents 
where dt = '2020-08-08' and lower(widget_name) in ('cartmessage','usermessage');


select page_country, count(1) from user_groupondw.bld_widget_contents 
where dt = '2020-08-11' and lower(widget_name) in ('cartmessage','usermessage') 
group by page_country; 


select * from user_edwprod.agg_gbl_traffic_deal limit 5;


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


