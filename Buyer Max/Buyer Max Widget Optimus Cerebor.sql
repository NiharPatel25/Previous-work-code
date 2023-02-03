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
    sf.platform,
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
    and cast(dt as date) between date_sub(current_date,4) and date_sub(current_date,1)
    and lower(platform) in ('web','desktop','touch')
) bwc
join (
    select
        event_date,
        unique_visitors as bcookie,
        max(cookie_first_country_code) country_code,
        max(cookie_first_sub_platform) platform
    from user_groupondw.gbl_traffic_superfunnel
    where cast(event_date as date) between date_sub(current_date,4) and date_sub(current_date,1)
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
    and cast(dt as date) between date_sub(current_date,4) and date_sub(current_date,1)
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
  cast(dt as date) between date_sub(current_date,12) and date_sub(current_date,1)
group by 
  platform,
  dt, 
  deal_uuid, 
  l2, 
  l3;



-------------------------------------------------------------------------------------REPEAT PURCHASERS
----grp_gdoop_bizops_db.nvp_bh_udv_optimus_trash 

drop table if exists grp_gdoop_bizops_db.nvp_bh_purchasers;
create table grp_gdoop_bizops_db.nvp_bh_purchasers stored as orc as
    select bcookie, deal_uuid, min(order_date) first_purchase_date
    from user_edwprod.fact_gbl_transactions
    group by bcookie, deal_uuid;



drop table grp_gdoop_bizops_db.nvp_um_repeat_udv;
create table grp_gdoop_bizops_db.nvp_um_repeat_udv (
    platform string,
    deal_uuid string,
    uniq_rpt_views int,
    udv int
) partitioned by (event_date string)
stored as orc
tblproperties ("orc.compress"="SNAPPY");



insert overwrite table grp_gdoop_bizops_db.nvp_um_repeat_udv partition (event_date)
   select 
     lower(sf.platform) platform, 
     sf.deal_uuid,
     count(distinct concat(sf.cookie_b, case when length(sf.deal_uuid) = 36 then sf.deal_uuid end)) uniq_rpt_views,
     sum(unique_deal_views) udv,
     sf.event_date
   from grp_gdoop_bizops_db.nvp_bh_purchasers as p
   join 
      (select
           x.event_date,
           x.cookie_b,
           x.deal_uuid,
           x.cookie_first_sub_platform platform, 
           x.unique_deal_views
       from user_groupondw.gbl_traffic_superfunnel_deal x
       where cast(x.event_date as date) >= date_sub(current_date,12)
       ) sf
        on p.bcookie = sf.cookie_b and p.deal_uuid = sf.deal_uuid
     where 
    cast(sf.event_date as date) >= cast(p.first_purchase_date as date)
   group by lower(sf.platform), sf.event_date, sf.deal_uuid;



create table grp_gdoop_bizops_db.nvp_um_rptudv_trial stored as orc as
select
           x.event_date,
           x.unique_visitors as bcookie,
           x.deal_uuid,
           x.cookie_first_sub_platform platform,
           x.cookie_first_platform,
           x.unique_deal_views
       from user_groupondw.gbl_traffic_superfunnel_deal x
       where cast(x.event_date as date) >=  cast('2020-07-13' as date);
 

      

------------------------------------------------------------------------------------------TOTAL UDV


drop table grp_gdoop_bizops_db.nvp_deal_udv;
create table grp_gdoop_bizops_db.nvp_deal_udv (
    deal_uuid string,
    platform string,
    uniq_rpt_views int,
    udv int, 
    dv int
) partitioned by (event_date string)
stored as orc
tblproperties ("orc.compress"="SNAPPY");



insert overwrite table grp_gdoop_bizops_db.nvp_deal_udv partition (event_date)
select 
    deal_uuid,
    lower(cookie_first_sub_platform) platform,
    count(distinct concat(cookie_b, case when length(deal_uuid) = 36 then deal_uuid end)) unique_repeat_views,
    sum(unique_deal_views) udv,
    sum(deal_views) dv,
    event_date
from
user_groupondw.gbl_traffic_superfunnel_deal
where cast(event_date as date) >= date_sub(current_date,12)
group by event_date, deal_uuid, lower(cookie_first_sub_platform);





/*
 * 
 * 
 * 
select * from user_groupondw.gbl_traffic_superfunnel_deal
where deal_uuid is not null and unique_visitors is not null and unique_deal_views <> 1
and cast(event_date as date) =  cast('2020-07-13' as date);

select distinct 
    cookie_first_platform, 
    cookie_first_sub_platform
from user_groupondw.gbl_traffic_superfunnel_deal
where cast(event_date as date) =  cast('2020-07-13' as date);


select distinct 
    cookie_first_platform, 
    cookie_first_sub_platform
from user_groupondw.gbl_traffic_superfunnel
where cast(event_date as date) =  cast('2020-07-13' as date);

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
*/


-----date_sub(next_day(dt, 'MON'), 1)
-----------------------------------------------------------------------------------------------------DIVISIONS
drop table if exists grp_gdoop_bizops_db.nvp_user_max_divs;
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



--------------------------------------------------------------------------------------------------------- TABLEAU 



drop table grp_gdoop_bizops_db.nvp_user_max_tab2;

create table grp_gdoop_bizops_db.nvp_user_max_tab2 stored as orc as
select 
a.dt,
a.country_code,
a.platform,
a.l1, a.l2,
sum(a.udv) udv, 
sum(b.uniq_rpt_views) repeat_views, 
sum(c.uniq_attempts) user_max_errors,
d.division
from
   (select 
      x.deal_uuid,
      y.country_code,
      lower(x.platform) platform,
      cast(x.event_date as date) dt,
      sum(x.uniq_rpt_views) udv, -----this is not repeat deal view but all dealviews.
      y.l2, y.l3,y.l1
     from grp_gdoop_bizops_db.nvp_deal_udv x
       left join 
       (select 
          distinct 
           deal_id,
           grt_l1_cat_name l1, 
           grt_l2_cat_name l2, 
           grt_l2_cat_name l3, 
           country_code
         from user_edwprod.dim_gbl_deal_lob
      ) y on x.deal_uuid = y.deal_id 
      group by x.deal_uuid, y.country_code, lower(x.platform), cast(x.event_date as date), y.l2, y.l3,y.l1) a
full join
     (select 
        deal_uuid, 
        lower(platform) platform,
        cast(event_date as date) dt,
        sum(uniq_rpt_views) uniq_rpt_views -------this is repeat deal views. 
      from grp_gdoop_bizops_db.nvp_um_repeat_udv group by deal_uuid, lower(platform), cast(event_date as date)
      ) b on a.deal_uuid = b.deal_uuid and a.platform = b.platform and a.dt = b.dt
full join
     (select  
        deal_uuid,
        lower(platform) platform,
        cast(dt as date) dt, 
        sum(uniq_attempts) uniq_attempts
      from grp_gdoop_bizops_db.nvp_user_max_widgets_agg2 group by deal_uuid, lower(platform), cast(dt as date)
      ) c on a.deal_uuid = c.deal_uuid and a.platform = c.platform and a.dt = c.dt
left join
      grp_gdoop_bizops_db.nvp_user_max_divs d on a.deal_uuid = d.deal_uuid
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


----------------------------------------------------------------------------------------------------------------DISTINCT DEAL COUNT AGG PULL


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