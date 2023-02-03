

create table sh_bh_purchasers stored as orc as
    select bcookie, deal_uuid, min(order_date) first_purchase_date
    from user_edwprod.fact_gbl_transactions
    group by 1,2
;

select * from user_edwprod.fact_gbl_transactions limit 5;
----
create table grp_gdoop_bizops_db.nvp_model_fin_info stored as orc as

		from 
(select 
parent_order_uuid,
order_uuid orders,
bcookie, 
deal_uuid
from 
user_edwprod.fact_gbl_transactions where country_id = 235) as a
join 
(bcookie, 
deal_uuid, 
first_purchase_date
from 
grp_gdoop_bizops_db.sh_bh_purchasers 
) as pre on a.bcookie = pre.bcookie and a.deal_uuid = pre.deal_uuid

			group by f.user_uuid, f.parent_order_uuid, f.merchant_uuid, f.deal_uuid) as fin_) final_ group by deal_uuid,purchase_case,order_cancelled_;


select * from user_groupondw.bld_widget_contents limit 100;

select * from user_edwprod.agg_gbl_traffic_deal where platform = 'touch' and sub_platform = 'iphone' limit 5;

------- Only repeat filter

drop table grp_gdoop_bizops_db.nvp_bh_repeat_dealviews;

create table grp_gdoop_bizops_db.nvp_bh_repeat_dealviews (
    platform string,
    l1 string,
    raw_rpt_views int,
    uniq_rpt_views int
) partitioned by (dt string)
stored as orc
tblproperties ("orc.compress"="SNAPPY");

insert into grp_gdoop_bizops_db.nvp_bh_repeat_dealviews partition (dt)
select
        platform,
        gdl.grt_l1_cat_name l1,
        count(1) raw_rpt_views,
        count(distinct concat(e.bcookie, case when length(e.deal_uuid) = 36 then e.deal_uuid end)) uniq_rpt_views,
        dt
    from grp_gdoop_bizops_db.sh_bh_purchasers p
    join user_groupondw.bld_events e on p.bcookie = e.bcookie and p.deal_uuid = e.deal_uuid
    join user_edwprod.dim_gbl_deal_lob gdl on p.deal_uuid = gdl.deal_id
    where e.dt between '2020-01-01' and '2020-06-30'
    and cast(e.dt as date) > cast(p.first_purchase_date as date)
    and e.event = 'pageview'
    and e.page_country = 'US'
    and e.page_type = 'deals/show'
    group by platform, gdl.grt_l1_cat_name, dt;
    
   
-----MY CODE
------Deals showing max repeat visitors
drop table grp_gdoop_bizops_db.nvp_bh_udv_withdeals;


create table grp_gdoop_bizops_db.nvp_bh_udv_withdeals (
    platform string,
    l1 string,
    l2 string,
    deal_uuid string,
    raw_rpt_views int,
    uniq_rpt_views int
) partitioned by (dt string)
stored as orc
tblproperties ("orc.compress"="SNAPPY");

insert into grp_gdoop_bizops_db.nvp_bh_udv_withdeals partition (dt)
select
        platform,
        gdl.grt_l1_cat_name l1,
        gdl.grt_l2_cat_name l2, 
        gdl.deal_id deal_uuid,
        count(1) raw_rpt_views,
        count(distinct concat(e.bcookie, case when length(e.deal_uuid) = 36 then e.deal_uuid end)) uniq_rpt_views,
        e.dt
    from grp_gdoop_bizops_db.sh_bh_purchasers p
    join user_groupondw.bld_events e on p.bcookie = e.bcookie and p.deal_uuid = e.deal_uuid
    join user_edwprod.dim_gbl_deal_lob gdl on p.deal_uuid = gdl.deal_id
    where e.dt between '2020-01-01' and '2020-06-30'
    and cast(e.dt as date) > cast(p.first_purchase_date as date)
    and e.event = 'pageview'
    and e.page_country = 'US'
    and e.page_type = 'deals/show'
    group by platform, gdl.grt_l1_cat_name, gdl.grt_l2_cat_name, dt, gdl.deal_id;
   
   
----------SPECIAL RANKING

drop table grp_gdoop_bizops_db.nvp_user_max_udv_top2;
create table grp_gdoop_bizops_db.nvp_user_max_udv_top2 stored as orc as
select 
deal_uuid, 
quarter, 
total_uniq_rpt_views,
rank_of_dealsqtr,
max(rank_of_dealsqtr) over(partition by quarter) * 0.10 ten_percent
from
(select 
deal_uuid, 
quarter,
total_uniq_rpt_views,
ROW_NUMBER() over(partition by quarter order by total_uniq_rpt_views desc) rank_of_dealsqtr
from 
(select 
deal_uuid, 
quarter, 
sum(uniq_rpt_views) total_uniq_rpt_views
from
(select 
deal_uuid,
case when dt between '2018-08-01' and '2018-12-31' then 'Q42018'
     when dt between '2019-01-01' and '2019-03-31' then 'Q12019'
     when dt between '2019-04-01' and '2019-06-30' then 'Q22019'
     when dt between '2019-07-01' and '2019-09-30' then 'Q32019'
     when dt between '2019-10-01' and '2019-12-31' then 'Q42019'
     when dt between '2020-01-01' and '2020-03-31' then 'Q12020'
     when dt between '2020-04-01' and '2020-06-30' then 'Q22020'
     end quarter,
uniq_rpt_views
from 
grp_gdoop_bizops_db.nvp_bh_udv_withdeals2) fina
group by deal_uuid, quarter) finb) finc
;
-----------------

drop table grp_gdoop_bizops_db.nvp_user_max_udv_top;
create table grp_gdoop_bizops_db.nvp_user_max_udv_top stored as orc as
select 
deal_uuid, 
quarter,
total_uniq_rpt_views,
ROW_NUMBER() over(partition by quarter order by total_uniq_rpt_views desc) rank_of_dealsqtr
from 
(select 
deal_uuid, 
quarter, 
sum(uniq_rpt_views) total_uniq_rpt_views
from
(select 
deal_uuid,
case when dt between '2018-08-01' and '2018-12-31' then 'Q42018'
     when dt between '2019-01-01' and '2019-03-31' then 'Q12019'
     when dt between '2019-04-01' and '2019-06-30' then 'Q22019'
     when dt between '2019-07-01' and '2019-09-30' then 'Q32019'
     when dt between '2019-10-01' and '2019-12-31' then 'Q42019'
     when dt between '2020-01-01' and '2020-03-31' then 'Q12020'
     when dt between '2020-04-01' and '2020-06-30' then 'Q22020'
     end quarter,
uniq_rpt_views
from 
grp_gdoop_bizops_db.nvp_bh_udv_withdeals) fina
group by deal_uuid, quarter) finb
;


create table grp_gdoop_bizops_db.nvp_user_max_udv_topm stored as orc as
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
total_uniq_rpt_views, 
case when max_load_date is null then NULL when day(max_load_date) < 30 then 1 else 0 end deal_not_active
from
   (select 
   deal_uuid,
   month(dt) month_of_dt,
   year(dt) year_of_dt,
   sum(uniq_rpt_views) total_uniq_rpt_views
   from 
   grp_gdoop_bizops_db.nvp_bh_udv_withdeals group by deal_uuid, month(dt), year(dt)) fina
left join 
   (select deal_uuid, month(load_date) month_, year(load_date) year_, max(load_date) max_load_date 
   from user_groupondw.active_deals 
   group by deal_uuid, month(load_date), year(load_date)) b on fina.deal_uuid = b.deal_uuid and fina.month_of_dt = b.month_ and fina.year_of_dt = b.year_
) finb
;



create table grp_gdoop_bizops_db.nvp_bh_udv_alldeals stored as orc as
select 
    platform, 
    quarter, 
    dt, 
    l1, 
    l2, 
    top_ten, 
    sum(uniq_rpt_views) as uniq_rpt_views
from
(select 
    a.platform,
    a.deal_uuid,
    a.quarter,
    a.dt, 
    a.l1, 
    a.l2,
    case when b.deal_uuid is not null then 1 else 0 end top_ten,
    uniq_rpt_views
    from
(select 
    platform,
    deal_uuid,
    l1,
    l2, 
    dt,
    case when dt between '2018-08-01' and '2018-12-31' then 'Q42018'
     when dt between '2019-01-01' and '2019-03-31' then 'Q12019'
     when dt between '2019-04-01' and '2019-06-30' then 'Q22019'
     when dt between '2019-07-01' and '2019-09-30' then 'Q32019'
     when dt between '2019-10-01' and '2019-12-31' then 'Q42019'
     when dt between '2020-01-01' and '2020-03-31' then 'Q12020'
     when dt between '2020-04-01' and '2020-06-30' then 'Q22020'
     end quarter,
     uniq_rpt_views
    from 
     grp_gdoop_bizops_db.nvp_bh_udv_withdeals) as a
left join
(select
    deal_uuid,
    quarter 
    from grp_gdoop_bizops_db.nvp_user_max_udv_top2 where rank_of_dealsqtr <=ten_percent) as b on a.deal_uuid = b.deal_uuid and a.quarter = b.quarter
) fin
group by platform, quarter, dt, l1, l2, top_ten; 
   





-------User max table with deals


drop table grp_gdoop_bizops_db.nvp_bh_user_max_all;
create table grp_gdoop_bizops_db.nvp_bh_user_max_all (
    platform string,
    deal_uuid string, 
    l2 string, 
    l3 string,
    raw_total int,
    uniq_bcookies_deal int,
    uniq_attempts int
) partitioned by (dt string)
stored as orc
tblproperties ("orc.compress"="SNAPPY");

insert into grp_gdoop_bizops_db.nvp_bh_user_max_all partition (dt)
    select
        rep_res.platform,
        rep_res.deal_uuid,
        gdl.grt_l2_cat_description l2,
        gdl.grt_l3_cat_description l3,
        count(1) raw_total,
        count(distinct bcookie) uniq_bcookies_deal,
        count(distinct concat(bcookie, case when length(deal_uuid) = 36 then deal_uuid end)) uniq_attempts,
        dt
    from user_groupondw.bld_events rep_res
    left join user_edwprod.dim_gbl_deal_lob gdl on rep_res.deal_uuid = gdl.deal_id
    where dt between '2020-01-01' and '2020-06-30'
    and event = 'pageview'
    and page_country = 'US'
    and page_url like '%user_max_exceeded%'
    group by dt, platform, rep_res.deal_uuid, gdl.grt_l2_cat_description, gdl.grt_l3_cat_description;


grp_gdoop_bizops_db.nvp_user_max_topm_deals
drop table grp_gdoop_bizops_db.nvp_user_max_topm_deals;
create table grp_gdoop_bizops_db.nvp_user_max_topm_deals stored as orc as
select 
deal_uuid, 
month_of_dt, 
year_of_dt,
l2, l3,
deal_not_active,
total_uniq_attempts,
ROW_NUMBER() over(partition by year_of_dt, month_of_dt order by total_uniq_attempts desc) rank_of_dealsqtr
from 
(select 
fina.deal_uuid, 
fina.month_of_dt, 
fina.year_of_dt,
fina.l2, fina.l3,
fina.total_uniq_attempts,
case when max_load_date is null then NULL when day(max_load_date) < 30 then 1 else 0 end deal_not_active
from
   (select 
   deal_uuid,
   l2, l3,
   month(dt) month_of_dt,
   year(dt) year_of_dt,
   sum(uniq_attempts) total_uniq_attempts
   from 
   grp_gdoop_bizops_db.nvp_bh_user_max_all group by month(dt), year(dt), deal_uuid, l2, l3) fina
left join 
   (select deal_uuid, month(load_date) month_, year(load_date) year_, max(load_date) max_load_date 
   from user_groupondw.active_deals
   group by deal_uuid, month(load_date), year(load_date)) b on fina.deal_uuid = b.deal_uuid and fina.month_of_dt = b.month_ and fina.year_of_dt = b.year_
   ) finb
;

drop table grp_gdoop_bizops_db.nvp_bh_user_max_alldeals;
create table grp_gdoop_bizops_db.nvp_bh_user_max_alldeals stored as orc as
select 
    platform, 
    quarter, 
    dt, 
    l2,
    l3,
    top_ten, 
    sum(uniq_attempts) as uniq_attempts
from
(select 
    a.platform,
    a.deal_uuid,
    a.quarter,
    a.dt, 
    a.l2,
    a.l3,
    case when b.deal_uuid is not null then 1 else 0 end top_ten,
    uniq_attempts
    from
(select 
    platform,
    deal_uuid,
    l2,
    l3,
    dt,
    case when dt between '2018-08-01' and '2018-12-31' then 'Q42018'
     when dt between '2019-01-01' and '2019-03-31' then 'Q12019'
     when dt between '2019-04-01' and '2019-06-30' then 'Q22019'
     when dt between '2019-07-01' and '2019-09-30' then 'Q32019'
     when dt between '2019-10-01' and '2019-12-31' then 'Q42019'
     when dt between '2020-01-01' and '2020-03-31' then 'Q12020'
     when dt between '2020-04-01' and '2020-06-30' then 'Q22020'
     end quarter,
     uniq_attempts
    from 
     grp_gdoop_bizops_db.nvp_bh_user_max_all) as a
left join
(select
    deal_uuid,
    quarter 
    from grp_gdoop_bizops_db.nvp_user_max_udv_top2 where rank_of_dealsqtr <=ten_percent) as b on a.deal_uuid = b.deal_uuid and a.quarter = b.quarter
) fin
group by platform, quarter, dt, l2,l3, top_ten;
   
   

grp_gdoop_bizops_db.nvp_user_max_topm_deals

drop table grp_gdoop_bizops_db.nvp_user_max_tableau_conversion;
create table grp_gdoop_bizops_db.nvp_user_max_tableau_conversion stored as orc as
select 
b.deal_uuid deals_usermax,
a.deal_uuid deals_udv,
b.month_of_dt, 
b.year_of_dt, 
a.total_uniq_rpt_views,
a.rank_of_dealsqtr rank_udv,
b.total_uniq_attempts,
b.rank_of_dealsqtr rank_user_max,
b.deal_not_active
from grp_gdoop_bizops_db.nvp_user_max_udv_topm as a
right join grp_gdoop_bizops_db.nvp_user_max_topm_deals as b 
on a.deal_uuid = b.deal_uuid and a.month_of_dt = b.month_of_dt and a.year_of_dt = b.year_of_dt;

------------
------

------

drop table grp_gdoop_bizops_db.nvp_bh_udv_withdeals2;
create table grp_gdoop_bizops_db.nvp_bh_udv_withdeals2 (
    platform string,
    l1 string,
    l2 string,
    deal_uuid string,
    raw_rpt_views int,
    uniq_rpt_views int
) partitioned by (dt string)
stored as orc
tblproperties ("orc.compress"="SNAPPY");

insert into grp_gdoop_bizops_db.nvp_bh_udv_withdeals2 partition (dt)
select
        platform,
        gdl.grt_l1_cat_name l1,
        gdl.grt_l2_cat_name l2, 
        gdl.deal_id deal_uuid,
        count(1) raw_rpt_views,
        count(distinct concat(e.bcookie, case when length(e.deal_uuid) = 36 then e.deal_uuid end)) uniq_rpt_views,
        e.dt
    from grp_gdoop_bizops_db.sh_bh_purchasers p
    join user_groupondw.bld_events e on p.bcookie = e.bcookie and p.deal_uuid = e.deal_uuid
    join user_edwprod.dim_gbl_deal_lob gdl on p.deal_uuid = gdl.deal_id
    where e.dt between '2020-01-01' and '2020-06-30'
    and e.event = 'pageview'
    and e.page_country = 'US'
    and e.page_type = 'deals/show'
    group by platform, gdl.grt_l1_cat_name, gdl.grt_l2_cat_name, dt, gdl.deal_id;


create table grp_gdoop_bizops_db.nvp_user_max_udv_topm2 stored as orc as
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
   grp_gdoop_bizops_db.nvp_bh_udv_withdeals2 group by deal_uuid, month(dt), year(dt)) fina
) finb
;


create table grp_gdoop_bizops_db.nvp_user_max_tableau_conversion3 stored as orc as
select 
b.deal_uuid deals_usermax,
a.deal_uuid deals_udv,
b.month_of_dt, 
b.year_of_dt, 
a.total_uniq_rpt_views,
a.rank_of_dealsqtr rank_udv,
b.total_uniq_attempts,
b.rank_of_dealsqtr rank_user_max,
b.deal_not_active
from grp_gdoop_bizops_db.nvp_user_max_udv_topm2 as a
right join grp_gdoop_bizops_db.nvp_user_max_topm_deals as b 
on a.deal_uuid = b.deal_uuid and a.month_of_dt = b.month_of_dt and a.year_of_dt = b.year_of_dt;

create table grp_gdoop_bizops_db.nvp_user_max_allconversion3 stored as orc as
select 
b.deal_uuid deals_usermax,
a.deal_uuid deals_udv,
b.month_of_dt, 
b.year_of_dt, 
a.total_uniq_rpt_views,
a.rank_of_dealsqtr rank_udv,
b.total_uniq_attempts,
b.rank_of_dealsqtr rank_user_max,
b.deal_not_active
from grp_gdoop_bizops_db.nvp_user_max_udv_topm2 as a
full join grp_gdoop_bizops_db.nvp_user_max_topm_deals as b 
on a.deal_uuid = b.deal_uuid and a.month_of_dt = b.month_of_dt and a.year_of_dt = b.year_of_dt;
--------
create table grp_gdoop_bizops_db.nvp_bh_udv_alldeals2 stored as orc as
select 
    platform, 
    quarter, 
    dt, 
    l1, 
    l2, 
    top_ten, 
    sum(uniq_rpt_views) as uniq_rpt_views
from
(select 
    a.platform,
    a.deal_uuid,
    a.quarter,
    a.dt, 
    a.l1, 
    a.l2,
    case when b.deal_uuid is not null then 1 else 0 end top_ten,
    uniq_rpt_views
    from
(select 
    platform,
    deal_uuid,
    l1,
    l2, 
    dt,
    case when dt between '2018-08-01' and '2018-12-31' then 'Q42018'
     when dt between '2019-01-01' and '2019-03-31' then 'Q12019'
     when dt between '2019-04-01' and '2019-06-30' then 'Q22019'
     when dt between '2019-07-01' and '2019-09-30' then 'Q32019'
     when dt between '2019-10-01' and '2019-12-31' then 'Q42019'
     when dt between '2020-01-01' and '2020-03-31' then 'Q12020'
     when dt between '2020-04-01' and '2020-06-30' then 'Q22020'
     end quarter,
     uniq_rpt_views
    from 
     grp_gdoop_bizops_db.nvp_bh_udv_withdeals2) as a
left join
(select
    deal_uuid,
    quarter 
    from grp_gdoop_bizops_db.nvp_user_max_udv_top2 where rank_of_dealsqtr <=ten_percent) as b on a.deal_uuid = b.deal_uuid and a.quarter = b.quarter
) fin
group by platform, quarter, dt, l1, l2, top_ten;




---------- USER_UUID BASED USER MAX and UDV

drop table grp_gdoop_bizops_db.nvp_bh_user_max_all;
create table grp_gdoop_bizops_db.nvp_bh_user_uuid_max_all (
    platform string,
    deal_uuid string, 
    l2 string, 
    l3 string,
    raw_total int,
    uniq_bcookies_deal int,
    uniq_attempts int
) partitioned by (dt string)
stored as orc
tblproperties ("orc.compress"="SNAPPY");

insert into grp_gdoop_bizops_db.nvp_bh_user_uuid_max_all partition (dt)
    select
        rep_res.platform,
        rep_res.deal_uuid,
        gdl.grt_l2_cat_description l2,
        gdl.grt_l3_cat_description l3,
        count(1) raw_total,
        count(distinct user_uuid) uniq_bcookies_deal,
        count(distinct concat(user_uuid, case when length(deal_uuid) = 36 then deal_uuid end)) uniq_attempts,
        dt
    from user_groupondw.bld_events rep_res
    left join user_edwprod.dim_gbl_deal_lob gdl on rep_res.deal_uuid = gdl.deal_id
    where dt between '2020-01-01' and '2020-06-30'
    and event = 'pageview'
    and page_country = 'US'
    and page_url like '%user_max_exceeded%'
    group by dt, platform, rep_res.deal_uuid, gdl.grt_l2_cat_description, gdl.grt_l3_cat_description;

create table grp_gdoop_bizops_db.nvp_bh_purchasers stored as orc as
    select user_uuid, deal_uuid, min(order_date) first_purchase_date
    from user_edwprod.fact_gbl_transactions
    group by user_uuid, deal_uuid
;

create table grp_gdoop_bizops_db.nvp_user_max_uuid_topm stored as orc as
select 
deal_uuid,
l2, l3,
month_of_dt, 
year_of_dt,
deal_not_active,
total_uniq_attempts,
ROW_NUMBER() over(partition by year_of_dt, month_of_dt order by total_uniq_attempts desc) rank_of_dealsqtr
from 
(select 
fina.deal_uuid, 
fina.l2, 
fina.l3,
fina.month_of_dt, 
fina.year_of_dt,
fina.total_uniq_attempts,
case when max_load_date is null then NULL when day(max_load_date) < 30 then 1 else 0 end deal_not_active
from
   (select 
   deal_uuid,
   l2,l3,
   month(dt) month_of_dt,
   year(dt) year_of_dt,
   sum(uniq_attempts) total_uniq_attempts
   from 
   grp_gdoop_bizops_db.nvp_bh_user_uuid_max_all group by month(dt), year(dt), deal_uuid, l2, l3) fina
left join 
   (select deal_uuid, month(load_date) month_, year(load_date) year_, max(load_date) max_load_date 
   from user_groupondw.active_deals
   group by deal_uuid, month(load_date), year(load_date)) b on fina.deal_uuid = b.deal_uuid and fina.month_of_dt = b.month_ and fina.year_of_dt = b.year_
   ) finb
;


------


select * from user_edwprod.fact_gbl_transactions limit 5;

create table grp_gdoop_bizops_db.nvp_bh_udv_useruuid (
    platform string,
    l1 string,
    l2 string,
    deal_uuid string,
    raw_rpt_views int,
    uniq_rpt_views int
) partitioned by (dt string)
stored as orc
tblproperties ("orc.compress"="SNAPPY");

insert into grp_gdoop_bizops_db.nvp_bh_udv_useruuid partition (dt)
select
        platform,
        gdl.grt_l1_cat_name l1,
        gdl.grt_l2_cat_name l2, 
        gdl.deal_id deal_uuid,
        count(1) raw_rpt_views,
        count(distinct concat(e.user_uuid, case when length(e.deal_uuid) = 36 then e.deal_uuid end)) uniq_rpt_views,
        e.dt
    from grp_gdoop_bizops_db.nvp_bh_purchasers p
    join user_groupondw.bld_events e on p.user_uuid = e.user_uuid and p.deal_uuid = e.deal_uuid
    join user_edwprod.dim_gbl_deal_lob gdl on p.deal_uuid = gdl.deal_id
    where e.dt between '2018-0-01' and '2019-05-31'
    and e.event = 'pageview'
    and e.page_country = 'US'
    and e.page_type = 'deals/show'
    group by platform, gdl.grt_l1_cat_name, gdl.grt_l2_cat_name, dt, gdl.deal_id;

   

create table grp_gdoop_bizops_db.nvp_user_uuid_udv_topm stored as orc as
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
   grp_gdoop_bizops_db.nvp_bh_udv_useruuid group by deal_uuid, month(dt), year(dt)) fina
) finb
;

   
drop table grp_gdoop_bizops_db.nvp_user_max_tableau_conversion2;
create table grp_gdoop_bizops_db.nvp_user_max_tableau_conversion2 stored as orc as
select 
b.deal_uuid deals_usermax,
a.deal_uuid deals_udv,
b.month_of_dt, 
b.year_of_dt, 
a.total_uniq_rpt_views,
a.rank_of_dealsqtr rank_udv,
b.total_uniq_attempts,
b.rank_of_dealsqtr rank_user_max,
b.deal_not_active
from grp_gdoop_bizops_db.nvp_user_uuid_udv_topm as a
right join grp_gdoop_bizops_db.nvp_user_max_uuid_topm as b 
on a.deal_uuid = b.deal_uuid and a.month_of_dt = b.month_of_dt and a.year_of_dt = b.year_of_dt;


-------with user_uuid >

drop table grp_gdoop_bizops_db.nvp_bh_udv_useruuid2;

create table grp_gdoop_bizops_db.nvp_bh_udv_useruuid2 (
    platform string,
    l1 string,
    l2 string,
    deal_uuid string,
    raw_rpt_views int,
    uniq_rpt_views int
) partitioned by (dt string)
stored as orc
tblproperties ("orc.compress"="SNAPPY");



insert into grp_gdoop_bizops_db.nvp_bh_udv_useruuid2 partition (dt)
select
        platform,
        gdl.grt_l1_cat_name l1,
        gdl.grt_l2_cat_name l2, 
        gdl.deal_id deal_uuid,
        count(1) raw_rpt_views,
        count(distinct concat(e.bcookie, case when length(e.deal_uuid) = 36 then e.deal_uuid end)) uniq_rpt_views,
        e.dt
    from grp_gdoop_bizops_db.nvp_bh_purchasers p
    join user_groupondw.bld_events e on p.user_uuid = e.user_uuid and p.deal_uuid = e.deal_uuid
    join user_edwprod.dim_gbl_deal_lob gdl on p.deal_uuid = gdl.deal_id
    where e.dt > '2020-07-01'
    and cast(e.dt as date) > cast(p.first_purchase_date as date)
    and e.event = 'pageview'
    and e.page_country = 'US'
    and e.page_type = 'deals/show'
    group by platform, gdl.grt_l1_cat_name, gdl.grt_l2_cat_name, dt, gdl.deal_id;
   
   

create table grp_gdoop_bizops_db.nvp_user_uuid_udv2_topm stored as orc as
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



drop table grp_gdoop_bizops_db.nvp_user_max_tableau_conversion4;
create table grp_gdoop_bizops_db.nvp_user_max_tableau_conversion4 stored as orc as
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



-------with user_uuid >= 

drop table grp_gdoop_bizops_db.nvp_bh_udv_useruuid3;

create table grp_gdoop_bizops_db.nvp_bh_udv_useruuid3 (
    platform string,
    l1 string,
    l2 string,
    deal_uuid string,
    raw_rpt_views int,
    user_uuid int
) partitioned by (dt string)
stored as orc
tblproperties ("orc.compress"="SNAPPY");

insert into grp_gdoop_bizops_db.nvp_bh_udv_useruuid3 partition (dt)
select
        platform,
        gdl.grt_l1_cat_name l1,
        gdl.grt_l2_cat_name l2, 
        gdl.deal_id deal_uuid,
        count(1) raw_rpt_views,
        user_uuid,
        e.dt
    from grp_gdoop_bizops_db.nvp_bh_purchasers p
    join user_groupondw.bld_events e on p.user_uuid = e.user_uuid and p.deal_uuid = e.deal_uuid
    join user_edwprod.dim_gbl_deal_lob gdl on p.deal_uuid = gdl.deal_id
    where e.dt between '2019-06-01' and '2019-12-31'
    and cast(e.dt as date) >= cast(p.first_purchase_date as date)
    and e.event = 'pageview'
    and e.page_country = 'US'
    and e.page_type = 'deals/show';

   

create table grp_gdoop_bizops_db.nvp_user_uuid_udv2_topm stored as orc as
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


create table grp_gdoop_bizops_db.nvp_user_max_tableau_conversion4 stored as orc as
select 
b.deal_uuid deals_usermax,
a.deal_uuid deals_udv,
b.month_of_dt, 
b.year_of_dt, 
a.total_uniq_rpt_views,
a.rank_of_dealsqtr rank_udv,
b.total_uniq_attempts,
b.rank_of_dealsqtr rank_user_max,
b.deal_not_active
from grp_gdoop_bizops_db.nvp_user_uuid_udv2_topm as a
right join grp_gdoop_bizops_db.nvp_user_max_uuid_topm as b 
on a.deal_uuid = b.deal_uuid and a.month_of_dt = b.month_of_dt and a.year_of_dt = b.year_of_dt;