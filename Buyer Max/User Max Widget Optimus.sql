create table grp_gdoop_bizops_db.nvp_user_max_avg stored as orc as
select 
    deal_id,
    sum(uniq_deal_views) three_wk_uk
from user_edwprod.agg_gbl_traffic_deal
where report_date between '2020-07-16' and '2020-08-04'
group by deal_id;


/*create table grp_gdoop_bizops_db.nvp_bh_udv_optimus_mnth stored as orc as
select 
deal_uuid, 
month_of_dt, 
year_of_dt,
total_uniq_rpt_views,
ROW_NUMBER() over(partition by year_of_dt, month_of_dt order by total_uniq_rpt_views desc) rank_of_dealsqtr
from 
(select 
deal_uuid, 
month_of_dt, 
year_of_dt,
total_uniq_rpt_views
from
   (select 
   deal_uuid,
   month(dt) month_of_dt,
   year(dt) year_of_dt,
   sum(uniq_rpt_views) total_uniq_rpt_views
   from 
   grp_gdoop_bizops_db.nvp_bh_udv_useruuid2 group by deal_uuid, month(dt), year(dt)) fina
) finb
;



---user max from bcookie and user_uuid from bcookie but using user_uuid



drop table grp_gdoop_bizops_db.nvp_bh_udv_optimus_tab;
create table grp_gdoop_bizops_db.nvp_bh_udv_optimus_tab stored as orc as
select 
b.deal_uuid deals_usermax,
b.l2, b.l3,
a.deal_uuid deals_udv,
b.month_of_dt, 
b.year_of_dt, 
a.total_uniq_rpt_views,
a.rank_of_dealsqtr rank_udv,
b.total_uniq_attempts,
b.rank_of_dealsqtr rank_user_max,
b.deal_not_active
from grp_gdoop_bizops_db.nvp_user_uuid_udv2_topm as a
right join grp_gdoop_bizops_db.nvp_user_max_topm_deals as b 
on a.deal_uuid = b.deal_uuid and a.month_of_dt = b.month_of_dt and a.year_of_dt = b.year_of_dt;
*/

------
select dt from grp_gdoop_bizops_db.nvp_bh_user_max_widgets2 group by dt order by dt;


create table grp_gdoop_bizops_db.nvp_bh_user_max_widgets2 (
    bcookie string,
    consumerid string,
    deal_uuid string,
    l2 string,
    l3 string,
    platform string,
    country_code string,
    event_time string
) partitioned by (dt string)
stored as orc
tblproperties ("orc.compress"="SNAPPY");

create table grp_gdoop_bizops_db.nvp_buymax_trial stored as orc as
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
    and cast(dt as date) between cast('2019-03-01' as date) and cast('2019-03-04' as date)
    and lower(platform) in ('web','desktop','touch')

insert overwrite table grp_gdoop_bizops_db.nvp_bh_user_max_widgets2 partition (dt)
select
    bwc.bcookie,
    bwc.consumerid,
    bev.deal_uuid,
    bev.l2,
    bev.l3,
    bwc.platform,
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
    and cast(dt as date) between cast('2020-09-13' as date) and cast('2020-09-15' as date)
    and lower(platform) in ('web','desktop','touch')
) bwc
join (
    select
        event_date,
        unique_visitors as bcookie,
        max(cookie_first_country_code) country_code
    from user_groupondw.gbl_traffic_superfunnel
    where cast(event_date as date) between cast('2020-09-13' as date) and cast('2020-09-15' as date)
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
    and cast(dt as date) between cast('2020-09-13' as date) and cast('2020-09-15' as date)
) bev on bwc.parent_page_id = bev.page_id and bwc.dt = bev.dt;




------USER MAX AGG



create table grp_gdoop_bizops_db.nvp_user_max_widgets_agg2 (
    platform string,
    deal_uuid string,
    l2 string, 
    l3 string,
    uniq_attempts int
) partitioned by (dt string)
stored as orc
tblproperties ("orc.compress"="SNAPPY");

drop table grp_gdoop_bizops_db.nvp_user_max_widgets_agg2;

insert overwrite table grp_gdoop_bizops_db.nvp_user_max_widgets_agg2 partition (dt)
select
platform,
deal_uuid,
l2, 
l3, 
count(distinct concat(bcookie, case when length(deal_uuid) = 36 then deal_uuid end)) uniq_attempts,
dt
from grp_gdoop_bizops_db.nvp_bh_user_max_widgets2
where 
cast(dt as date) between date_sub(current_date,10) and date_sub(current_date,1)
group by 
platform,
dt, 
deal_uuid, 
l2, 
l3;

/* For Day wise entry
 * 
insert overwrite table grp_gdoop_bizops_db.nvp_user_max_widgets_agg2 partition (dt)
select
platform,
deal_uuid,
l2, 
l3, 
count(distinct concat(bcookie, case when length(deal_uuid) = 36 then deal_uuid end)) uniq_attempts,
dt
from grp_gdoop_bizops_db.nvp_bh_user_max_widgets2
where 
cast(dt as date) = cast('2019-09-14' as date)
group by 
platform,
dt, 
deal_uuid, 
l2, 
l3;

insert overwrite table grp_gdoop_bizops_db.nvp_user_max_widgets_agg2 partition (dt)
select
platform,
deal_uuid,
l2, 
l3, 
count(distinct concat(bcookie, case when length(deal_uuid) = 36 then deal_uuid end)) uniq_attempts,
dt
from grp_gdoop_bizops_db.nvp_bh_user_max_widgets2
group by 
platform,
dt, 
deal_uuid, 
l2, 
l3;
*/

------


-------------------------------------------------------------------------------------REPEAT PURCHASERS
----grp_gdoop_bizops_db.nvp_bh_udv_optimus_trash 

drop table grp_gdoop_bizops_db.nvp_bh_purchasers;
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


------------------------------------------------------------------------------------------TOTAL UDV


  
drop table grp_gdoop_bizops_db.nvp_deal_udv;
create table grp_gdoop_bizops_db.nvp_deal_udv stored as orc as
select
   report_date,
   platform,
   deal_id,
   grt_l1_cat_name, 
   grt_l2_cat_name,
   max(country_code) country_code,
   sum(uniq_deal_views) udv
from user_edwprod.agg_gbl_traffic_deal
where length(deal_id) = 36
group by report_date, deal_id, grt_l1_cat_name, grt_l2_cat_name, platform
;

select * from grp_gdoop_bizops_db.nvp_deal_udv;


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

--------- TABLEAU ----date_sub(next_day(dt, 'MON'), 1)




drop table grp_gdoop_bizops_db.nvp_user_max_tab2;

create table grp_gdoop_bizops_db.nvp_user_max_tab2 stored as orc as
select 
a.dt,
a.country_code,
a.platform,
a.l1, a.l2,
sum(a.udv) udv, sum(b.uniq_rpt_views) repeat_views, sum(c.uniq_attempts) user_max_errors,
d.division
from
(select 
      lower(platform) platform,
      cast(report_date as date) dt,
      deal_id,
      grt_l1_cat_name l1, 
      grt_l2_cat_name l2,
      country_code,
      sum(udv) udv
from grp_gdoop_bizops_db.nvp_deal_udv 
where report_date >= '2020-07-01'and lower(platform) <> 'app' 
group by lower(platform), cast(report_date as date), deal_id, grt_l1_cat_name, grt_l2_cat_name, country_code) a
left join
(select 
      deal_uuid, 
      lower(platform) platform,
      cast(dt as date) dt,
      sum(uniq_rpt_views) uniq_rpt_views
      from grp_gdoop_bizops_db.nvp_bh_udv_optimus group by deal_uuid, lower(platform), cast(dt as date)
      ) b on a.deal_id = b.deal_uuid and a.platform = b.platform and a.dt = b.dt
left join
(select  
      deal_uuid,
      lower(platform) platform,
      cast(dt as date) dt, 
      sum(uniq_attempts) uniq_attempts
      from grp_gdoop_bizops_db.nvp_user_max_widgets_agg2 group by deal_uuid, lower(platform), cast(dt as date)
      ) c on a.deal_id = c.deal_uuid and a.platform = c.platform and a.dt = c.dt
left join
grp_gdoop_bizops_db.nvp_user_max_divs d on a.deal_id = d.deal_uuid
group by a.dt, a.country_code, a.platform, a.l1, a.l2, d.division;



------
/*select * from 
(select 
      platform,
      report_date,
      deal_id,
      grt_l1_cat_name l1, 
      grt_l2_cat_name l2,
      country_code,
      udv
from grp_gdoop_bizops_db.nvp_deal_udv where report_date >= '2020-07-01') a
left join
(select  
      deal_uuid,
      platform,
      dt, 
      uniq_attempts 
      from grp_gdoop_bizops_db.nvp_user_max_widgets_agg2 
) c on a.deal_id = c.deal_uuid and a.report_date = c.dt and a.platform = c.platform
where uniq_attempts is not null;*/


-----DISTINCT DEAL COUNT AGG PULL


drop table grp_gdoop_bizops_db.nvp_user_max_tab3;
create table grp_gdoop_bizops_db.nvp_user_max_tab3 stored as orc as
select 
'day' granularity,
a.dt,
a.country_code,
a.platform,
count(distinct a.deal_id) deal_distinct_udv,
count(distinct b.deal_uuid) deal_distinct_rpt
from 
(select 
      case when lower(platform) = 'app' then 'app' 
           when lower(platform) = 'touch' then 'touch' 
           when lower(platform) = 'web'  then 'desktop' end platform,
      cast(report_date as date) dt,
      deal_id,
      country_code
from grp_gdoop_bizops_db.nvp_deal_udv where report_date >= '2020-07-01' group by case when lower(platform) = 'app' then 'app' when lower(platform) = 'touch' then 'touch' when lower(platform) = 'web'  then 'desktop' end, cast(report_date as date), deal_id, country_code) a
left join
(select 
      deal_uuid, 
      lower(platform) platform,
      cast(dt as date) dt
      from grp_gdoop_bizops_db.nvp_bh_udv_optimus group by deal_uuid, lower(platform), cast(dt as date)
      ) b on a.deal_id = b.deal_uuid and a.platform = b.platform and a.dt = b.dt group by a.dt, a.country_code, a.platform
UNION
select 
'week' granularity,
a.dt,
a.country_code,
a.platform,
count(distinct a.deal_id) deal_distinct_udv,
count(distinct b.deal_uuid) deal_distinct_rpt
from 
(select 
      case when lower(platform) = 'app' then 'app' 
           when lower(platform) = 'touch' then 'touch' 
           when lower(platform) = 'web'  then 'desktop' end platform,
      cast(date_sub(next_day(cast(report_date as date), 'MON'), 1) as date) dt,
      deal_id,
      country_code
from grp_gdoop_bizops_db.nvp_deal_udv where report_date >= '2020-07-01' group by case when lower(platform) = 'app' then 'app' when lower(platform) = 'touch' then 'touch' when lower(platform) = 'web'  then 'desktop' end, date_sub(next_day(cast(report_date as date), 'MON'), 1), deal_id, country_code) a
left join
(select 
      deal_uuid, 
      lower(platform) platform,
      cast(date_sub(next_day(cast(dt as date), 'MON'), 1) as date) dt
      from grp_gdoop_bizops_db.nvp_bh_udv_optimus group by deal_uuid, lower(platform), date_sub(next_day(cast(dt as date), 'MON'), 1)
      ) b on a.deal_id = b.deal_uuid and a.platform = b.platform and a.dt = b.dt group by a.dt, a.country_code, a.platform
UNION
select 
'monthly' granularity,
a.dt,
a.country_code,
a.platform,
count(distinct a.deal_id) deal_distinct_udv, 
count(distinct b.deal_uuid) deal_distinct_rpt
from 
(select 
      case when lower(platform) = 'app' then 'app' 
           when lower(platform) = 'touch' then 'touch' 
           when lower(platform) = 'web'  then 'desktop' end platform,
      cast(last_day(cast(report_date as date)) as date) dt,
      deal_id,
      country_code
from grp_gdoop_bizops_db.nvp_deal_udv where report_date >= '2020-07-01' group by case when lower(platform) = 'app' then 'app' when lower(platform) = 'touch' then 'touch' when lower(platform) = 'web'  then 'desktop' end, last_day(cast(report_date as date)), deal_id, country_code) a
left join
(select 
      deal_uuid, 
      lower(platform) platform,
      cast(last_day(cast(dt as date)) as date) dt
      from grp_gdoop_bizops_db.nvp_bh_udv_optimus group by deal_uuid, lower(platform), last_day(cast(dt as date))
      ) b on a.deal_id = b.deal_uuid and a.platform = b.platform and a.dt = b.dt group by a.dt, a.country_code, a.platform;


select report_date, last_day(report_date) from grp_gdoop_bizops_db.nvp_deal_udv limit 5;
     
-----date wise agg


/*
drop table grp_gdoop_bizops_db.nvp_user_max_tab4;
create table grp_gdoop_bizops_db.nvp_user_max_tab4 stored as orc as
select 
country_code,
division,
report_date, 
platform, 
l1, 
l2, 
sum(udv) udvs, sum(repeat_views) repeat_views, sum(user_max_errors) user_max_error
from 
grp_gdoop_bizops_db.nvp_user_max_tab2
group by 
country_code,
report_date, 
platform,
division,
l1, 
l2
;
*/



-------


/*
create table grp_gdoop_bizops_db.nvp_user_max_tab2 stored as orc as
select 
a.deal_uuid,
a.l2,
a.mnth_dt,
a.year_dt,
b.uniq_rpt_views,
a.uniq_max_errors,
c.udv
from
(select deal_uuid, l2, month(cast(dt as date)) mnth_dt, year(cast(dt as date)) year_dt, sum(uniq_attempts) uniq_max_errors
from grp_gdoop_bizops_db.nvp_user_max_widgets_agg2 
group by deal_uuid, l2, month(cast(dt as date)), year(cast(dt as date))) a
left join
(select deal_uuid, month(cast(dt as date)) mnth_dt, year(cast(dt as date)) year_dt, sum(uniq_rpt_views) uniq_rpt_views 
from grp_gdoop_bizops_db.nvp_bh_udv_optimus group by deal_uuid, month(cast(dt as date)), year(cast(dt as date))) b 
on a.deal_uuid = b.deal_uuid and a.mnth_dt = b.mnth_dt and a.year_dt = b.year_dt
left join 
(select report_month, report_year, deal_id, avg(udv) over (partition by report_month, report_year) udv from grp_gdoop_bizops_db.nvp_deal_udv) c 
on a.mnth_dt = c.report_month and a.year_dt = c.report_year and a.deal_uuid = c.deal_id;



select report_month, report_year, sum(udv) udv, count(deal_id) deal_id, sum(udv)/count(deal_id) from grp_gdoop_bizops_db.nvp_deal_udv group by report_month, report_year;


-----







-----



drop table grp_gdoop_bizops_db.nvp_deal_udv;
create table grp_gdoop_bizops_db.nvp_deal_udv stored as orc as
select
   report_date,
   country_code,
   platform,
   deal_id,
   grt_l1_cat_name, 
   grt_l2_cat_name,
   sum(uniq_deal_views) udv
from user_edwprod.agg_gbl_traffic_deal
where length(deal_id) = 36
group by report_date, country_code, deal_id, grt_l1_cat_name, grt_l2_cat_name, platform
;



----
55120347-9a3f-e9eb-4fb9-f63b068f6de6

*/