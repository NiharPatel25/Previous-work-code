
drop table nvp_agg_dealview_nst
nvp_bh_purchasers

----------------------------------------grp_gdoop_bizops_db.nvp_all_yrs_txns
-----------------------------------------------------------------------------Creating Empty tables NAM
drop table if exists grp_gdoop_bizops_db.nvp_agg_dealview_bhnam;
drop table if exists grp_gdoop_bizops_db.nvp_agg_otherview_bhnam;
drop table if exists grp_gdoop_bizops_db.nvp_agg_dealview_nstnam;
drop table if exists grp_gdoop_bizops_db.nvp_agg_otherview_nstnam;


create table grp_gdoop_bizops_db.nvp_agg_dealview_bh_nam90  (
    eventdate string,
    user_uuid string,
    platform string,
    deal_uuid string,
    l1 string,
    l2 string
)partitioned by (redeem_date_mnth int, redeem_date_yr int) 
stored as orc
tblproperties ("orc.compress"="SNAPPY");

create table grp_gdoop_bizops_db.nvp_agg_otherview_bh_nam90  (
    eventdate string,
    user_uuid string,
    platform string,
    deal_uuid string,
    l1 string,
    l2 string
)partitioned by (redeem_date_mnth int, redeem_date_yr int) 
stored as orc
tblproperties ("orc.compress"="SNAPPY");


create table grp_gdoop_bizops_db.nvp_agg_dealview_nst_nam90  (
    eventdate string,
    user_uuid string,
    platform string,
    deal_uuid string,
    l1 string,
    l2 string
)partitioned by (redeem_date_mnth int, redeem_date_yr int) stored as orc
tblproperties ("orc.compress"="SNAPPY");


create table grp_gdoop_bizops_db.nvp_agg_otherview_nst_nam90  (
    eventdate string,
    user_uuid string,
    platform string,
    deal_uuid string,
    l1 string,
    l2 string
)partitioned by (redeem_date_mnth int, redeem_date_yr int) stored as orc
tblproperties ("orc.compress"="SNAPPY");


--------------------------------------------------------------------------------------------
select * from grp_gdoop_bizops_db.nvp_agg_dealview_bh_nam90;

insert overwrite table grp_gdoop_bizops_db.nvp_agg_dealview_bh_nam90 partition (redeem_date_mnth, redeem_date_yr)
select event_date, user_uuid,platform, deal_uuid, l1,l2,redeem_date_mnth, redeem_date_yr from 
(select
            a.user_uuid,
            b.dt event_date,
            case when lower(b.platform) = 'web' then 'desktop' else lower(b.platform) end as platform,
            b.deal_uuid,
            gbl.grt_l1_cat_name l1,
            gbl.grt_l2_cat_name l2,
            a.redeem_date_mnth,
            a.redeem_date_yr
from grp_gdoop_bizops_db.nvp_all_yrs_txns a
join user_groupondw.bld_events b on lower(a.user_uuid) = lower(b.user_uuid) and b.page_country = a.country_code
join 
   (select
        deal_id,
        grt_l1_cat_name, 
        grt_l2_cat_name 
        from user_edwprod.dim_gbl_deal_lob 
        group by deal_id, grt_l1_cat_name, grt_l2_cat_name) gbl on lower(b.deal_uuid) = lower(gbl.deal_id)
where cast(b.dt as date) between cast('2020-07-01' as date) and cast('2020-11-06' as date)
and cast(b.dt as date) between date_add(a.first_redeem_date,1) and date_add(a.first_redeem_date,90)
and a.redeem_date_yr = 2020 and a.redeem_date_mnth = 7
and a.country_code in ('US','CA')
and b.bot_flag = 0
and lower(b.platform) in ('web','desktop','touch') ) fin_ 
group by event_date, user_uuid, redeem_date_mnth, redeem_date_yr, platform, deal_uuid, l1, l2;


select * from grp_gdoop_bizops_db.nvp_agg_otherview_bh_nam90;
insert overwrite table grp_gdoop_bizops_db.nvp_agg_otherview_bh_nam90 partition (redeem_date_mnth, redeem_date_yr)
select event_date, user_uuid, platform, deal_uuid, l1,l2, redeem_date_mnth, redeem_date_yr from 
(select
            a.user_uuid,
            b.dt event_date,
            case when lower(b.platform) = 'web' then 'desktop' else lower(b.platform) end as platform,
            null as deal_uuid,
            null as l1,
            null as l2,
            a.redeem_date_mnth,
            a.redeem_date_yr
from grp_gdoop_bizops_db.nvp_all_yrs_txns a
join user_groupondw.bld_events b on lower(a.user_uuid) = lower(b.user_uuid) and b.page_country = a.country_code
where cast(b.dt as date) between cast('2020-07-01' as date) and cast('2020-11-06' as date)
and cast(b.dt as date) between date_add(a.first_redeem_date,1) and date_add(a.first_redeem_date,90)
and a.redeem_date_yr = 2020 and a.redeem_date_mnth = 7
and a.country_code in ('US','CA')
and b.bot_flag = 0
and lower(b.platform) in ('web','desktop','touch') ) fin_ group by event_date, user_uuid, platform, redeem_date_mnth, redeem_date_yr, deal_uuid, l1, l2;



insert overwrite table grp_gdoop_bizops_db.nvp_agg_dealview_nst_nam90 partition (redeem_date_mnth, redeem_date_yr)
select event_date, user_uuid, platform, deal_uuid, l1,l2, redeem_date_mnth, redeem_date_yr from
(select
         c.user_uuid,
         d.eventdate event_date,
         case when lower(d.platform) in ('ipad','iphone') then 'ios' else lower(d.platform) end as platform,
         d.deal_uuid,
         gbl_2.grt_l1_cat_name l1,
         gbl_2.grt_l2_cat_name l2,
         c.redeem_date_mnth,
         c.redeem_date_yr
from grp_gdoop_bizops_db.nvp_all_yrs_txns c
join user_groupondw.m_raw_dealview d on lower(c.user_uuid) = lower(d.consumer_id) and d.countrycode = c.country_code
join (select 
         deal_id, 
         grt_l1_cat_name, 
         grt_l2_cat_name 
         from user_edwprod.dim_gbl_deal_lob 
         group by deal_id, grt_l1_cat_name, grt_l2_cat_name) gbl_2 on lower(d.deal_uuid) = lower(gbl_2.deal_id)
where cast(d.eventdate as date) between cast('2020-07-01' as date) and cast('2020-11-03' as date)
and c.redeem_date_yr = 2020 and c.redeem_date_mnth = 7
and cast(d.eventdate as date) between date_add(c.first_redeem_date,1) and date_add(c.first_redeem_date,90)
and c.country_code in ('US','CA')
) fin_ group by event_date, user_uuid, platform, deal_uuid, l1, l2, redeem_date_mnth, redeem_date_yr;



insert overwrite table grp_gdoop_bizops_db.nvp_agg_otherview_nst_nam90 partition (redeem_date_mnth, redeem_date_yr)
select event_date, user_uuid, platform, deal_uuid, l1,l2 , redeem_date_mnth, redeem_date_yr from
(select 
       c.user_uuid,
       d.eventdate event_date,
       case when lower(d.platform) in ('ipad','iphone') then 'ios' else lower(d.platform) end as platform,
       null as deal_uuid,
       null as l1,
       null as l2,
       c.redeem_date_mnth,
       c.redeem_date_yr
from grp_gdoop_bizops_db.nvp_all_yrs_txns c
  join user_groupondw.m_raw_pageview d on lower(c.user_uuid) = lower(d.consumer_id) and d.countrycode = c.country_code
   where cast(d.eventdate as date) between cast('2020-07-01' as date) and cast('2020-11-05' as date)
     and cast(d.eventdate as date) between date_add(c.first_redeem_date,1) and date_add(c.first_redeem_date,90)
     and c.redeem_date_yr = 2020 and c.redeem_date_mnth = 7
     and c.country_code in ('US','CA')
) fin_ group by event_date, user_uuid, platform, deal_uuid, l1, l2, redeem_date_mnth, redeem_date_yr;



drop table if exists grp_gdoop_bizops_db.nvp_all_traffic_purch_nam90;
create table grp_gdoop_bizops_db.nvp_all_traffic_purch_nam90 stored as orc as
select * from (
select * from grp_gdoop_bizops_db.nvp_agg_dealview_bh_nam90
UNION ALL
select * from grp_gdoop_bizops_db.nvp_agg_otherview_bh_nam90
UNION ALL
select * from grp_gdoop_bizops_db.nvp_agg_dealview_nst_nam90
UNION ALL
select * from grp_gdoop_bizops_db.nvp_agg_otherview_nst_nam90
) fin_;


----- Local Inventory


drop table if exists grp_gdoop_bizops_db.nvp_local_inventory;
create table grp_gdoop_bizops_db.nvp_local_inventory stored as orc as
select distinct deal_id, grt_l2_cat_name ltwo, country_code, country_id 
from user_edwprod.dim_gbl_deal_lob 
where grt_l1_cat_name = 'L1 - Local';



-----LOOKS AT ALL UDV NAM


drop table if exists grp_gdoop_bizops_db.nvp_purch_trial_one_nam90;
create table grp_gdoop_bizops_db.nvp_purch_trial_one_nam90 stored as orc as
select 
    a.country_code,
    a.booked_frst_ord,
    a.l2,
    a.redeem_date_yr as year_of_purch,
    a.redeem_date_mnth as month_of_purch,
    count(distinct a.user_uuid) distinct_users,
    count(distinct case when c.user_rating in ('HH','HM') then a.user_uuid end) three_star_user,
    count(distinct case when c.user_rating in ('MH','MM','HL') then a.user_uuid end) two_star_user,
    count(distinct case when c.user_rating in ('LH','LM','LL','ML') then a.user_uuid end) one_star_user,
    count(distinct b.user_uuid) repeat_visitors,
    count(distinct concat(b.eventdate, b.user_uuid)) uv,
    count(distinct case when b.deal_uuid is not null then concat(b.eventdate, b.user_uuid) end) udvva,
    count(distinct case when b.deal_uuid is not null then concat(b.eventdate, b.user_uuid, b.deal_uuid) end) udva
from
    grp_gdoop_bizops_db.nvp_all_yrs_txns as a
    left join grp_gdoop_bizops_db.nvp_all_traffic_purch_nam90 as b
               on a.user_uuid = b.user_uuid and a.redeem_date_yr = b.redeem_date_yr and a.redeem_date_mnth = b.redeem_date_mnth
    left join grp_gdoop_bizops_db.nvp_pf_allyrtxns_star as c on a.user_uuid = c.user_uuid AND a.first_redeem_date = c.first_redeem_date
where a.country_code in ('US','CA') and a.redeem_date_mnth in (6,7) and a.redeem_date_yr = 2020
group by 
    a.country_code, a.booked_frst_ord, a.l2, a.redeem_date_yr, a.redeem_date_mnth;

   

-----LOOKS AT LOCAL UDV

drop table if exists grp_gdoop_bizops_db.nvp_purch_trial_two_nam90;
create table grp_gdoop_bizops_db.nvp_purch_trial_two_nam90 stored as orc as
select 
a.country_code, 
a.booked_frst_ord, 
a.l2, 
a.redeem_date_yr as year_of_purch,
a.redeem_date_mnth as month_of_purch,
count(distinct case when r.deal_uuid is not null then concat(r.eventdate, r.user_uuid) end) udvv,
count(distinct case when r.deal_uuid is not null then concat(r.eventdate, r.user_uuid, r.deal_uuid) end) udv
from grp_gdoop_bizops_db.nvp_all_yrs_txns as a
left join 
     (select x.*
       from grp_gdoop_bizops_db.nvp_all_traffic_purch_nam90 x
       join grp_gdoop_bizops_db.nvp_local_inventory y on x.deal_uuid = y.deal_id
      ) as r on a.user_uuid = r.user_uuid and a.redeem_date_yr = r.redeem_date_yr and a.redeem_date_mnth = r.redeem_date_mnth
where a.country_code in ('US','CA') and a.redeem_date_mnth in (6,7) and a.redeem_date_yr = 2020
group by a.country_code, a.booked_frst_ord, a.l2, a.redeem_date_yr, a.redeem_date_mnth;

-----ALL LOCAL TRANSACTIONS


drop table if exists grp_gdoop_bizops_db.nvp_purch_trial_three_nam90;
create table grp_gdoop_bizops_db.nvp_purch_trial_three_nam90 stored as orc as
select 
a.country_code, 
a.booked_frst_ord,
a.l2,
a.redeem_date_yr as year_of_purch,
a.redeem_date_mnth as month_of_purch, 
count(distinct a.user_uuid) distinct_users, 
count(distinct b.user_uuid) repeat_purchaser,
count(distinct case when c.user_rating in ('HH','HM') and cast(b.order_date as date) between date_add(cast(a.first_redeem_date as date), 1) and date_add(cast(a.first_redeem_date as date), 90) then b.parent_order_uuid end) three_star_day_order,
count(distinct case when c.user_rating in ('MH','MM','HL') and cast(b.order_date as date) between date_add(cast(a.first_redeem_date as date), 1) and date_add(cast(a.first_redeem_date as date), 90) then b.parent_order_uuid end) two_star_day_order,
count(distinct case when c.user_rating in ('LH','LM','LL','ML') and cast(b.order_date as date) between date_add(cast(a.first_redeem_date as date), 1) and date_add(cast(a.first_redeem_date as date), 90) then b.parent_order_uuid end) one_star_day_order,
count(distinct case when cast(b.order_date as date) between date_add(cast(a.first_redeem_date as date), 1) and date_add(cast(a.first_redeem_date as date), 90) then b.parent_order_uuid end) ninety_day_order
from grp_gdoop_bizops_db.nvp_all_yrs_txns as a
     left join grp_gdoop_bizops_db.nvp_all_local_txns as b on a.user_uuid = b.user_uuid
     left join grp_gdoop_bizops_db.nvp_pf_allyrtxns_star as c on a.user_uuid = c.user_uuid AND a.first_redeem_date = c.first_redeem_date
where a.country_code in ('US','CA') and a.redeem_date_mnth in (6,7) and a.redeem_date_yr = 2020
group by a.country_code, a.booked_frst_ord, a.l2, a.redeem_date_yr, a.redeem_date_mnth;

  
drop table if exists grp_gdoop_bizops_db.nvp_purch_freq_final_nam90;
create table grp_gdoop_bizops_db.nvp_purch_freq_final_nam90 stored as orc as 
select
   a.country_code,
   a.booked_frst_ord,
   a.l2,
   a.year_of_purch,
   a.month_of_purch,
   a.distinct_users distinct_user_1,
   a.three_star_user,
   a.two_star_user,
   a.one_star_user,
   a.repeat_visitors,
   a.uv,a.udvva,a.udva,
   b.udvv,b.udv,
   c.ninety_day_order,
   c.three_star_day_order,
   c.two_star_day_order,
   c.one_star_day_order,
   c.distinct_users as distinct_user_2,
   c.repeat_purchaser ----need to change this.
from grp_gdoop_bizops_db.nvp_purch_trial_one_nam90 a
  left join grp_gdoop_bizops_db.nvp_purch_trial_two_nam90 b 
         on a.booked_frst_ord = b.booked_frst_ord and a.l2 = b.l2 and a.year_of_purch = b.year_of_purch and a.month_of_purch = b.month_of_purch and a.country_code = b.country_code
  left join grp_gdoop_bizops_db.nvp_purch_trial_three_nam90 c 
         on a.booked_frst_ord = c.booked_frst_ord and a.l2 = c.l2 and a.year_of_purch = c.year_of_purch and a.month_of_purch = c.month_of_purch and a.country_code = c.country_code;


----90 day order comes from c
        
------------------------STAGE 2 NAM

drop table if exists grp_gdoop_bizops_db.nvp_purch_freq_bt_final_agg_nam90;
create table grp_gdoop_bizops_db.nvp_purch_freq_bt_final_agg_nam90 stored as orc as 
select
    v.country_code, 
    v.booked_frst_ord, 
    v.l2,
    v.year_of_purch,
    v.month_of_purch,
    v.udv,
    v.udv_notbt_eligible,
    v.udv_bt_eligible,
    t.ninety_day_order,
    t.units_total,
    t.units_notbt_eligible,
    t.three_star_units_notbt_eligible,
    t.two_star_units_notbt_eligible,
    t.one_star_units_notbt_eligible,
    t.units_bt_eligible,
    t.three_star_units_bt_eligible,
    t.two_star_units_bt_eligible,
    t.one_star_units_bt_eligible
from
(select 
    a.country_code, 
    a.booked_frst_ord, 
    a.l2, 
    a.redeem_date_yr as year_of_purch,
    a.redeem_date_mnth as month_of_purch,
    count(distinct case when r.deal_uuid is not null then concat(r.eventdate, r.user_uuid, r.deal_uuid) end) udv,
    count(distinct case when r.deal_uuid is not null and r.bt_eligible = 0 then concat(r.eventdate, r.user_uuid, r.deal_uuid) end) udv_notbt_eligible,
    count(distinct case when r.deal_uuid is not null and r.bt_eligible = 1 then concat(r.eventdate, r.user_uuid, r.deal_uuid) end) udv_bt_eligible
 from grp_gdoop_bizops_db.nvp_all_yrs_txns as a
     left join 
    (select x.*, case when z.deal_uuid2 is not null then 1 else 0 end as bt_eligible 
         from grp_gdoop_bizops_db.nvp_all_traffic_purch_nam90 x
         join grp_gdoop_bizops_db.nvp_local_inventory y on x.deal_uuid = y.deal_id ----This is the same filter for local used to calculate UDV. including retail. 
         left join 
         (select 
             load_date, 
             deal_uuid deal_uuid2
          from 
           grp_gdoop_bizops_db.sh_bt_active_deals_log 
          where product_is_active_flag = 1 and partner_inactive_flag = 0 and is_bookable = 1 and load_date >= '2020-05-01'
          ) z on x.deal_uuid = z.deal_uuid2 and x.eventdate = z.load_date
     ) as r on a.user_uuid = r.user_uuid and a.redeem_date_yr = r.redeem_date_yr and a.redeem_date_mnth = r.redeem_date_mnth
    where a.country_code in ('US','CA') and a.redeem_date_mnth in (6,7) and a.redeem_date_yr = 2020
    group by a.country_code, a.booked_frst_ord, a.l2, a.redeem_date_mnth, a.redeem_date_yr
   ) as v
left join
(select 
    a.country_code, 
    a.booked_frst_ord,
    a.l2,
    a.redeem_date_yr as year_of_purch,
    a.redeem_date_mnth as month_of_purch,
    count(distinct case when cast(b.order_date as date) between date_add(cast(a.first_redeem_date as date), 1) and date_add(cast(a.first_redeem_date as date), 90) then b.parent_order_uuid end) ninety_day_order,
    sum(case when cast(b.order_date as date) between date_add(cast(a.first_redeem_date as date), 1) and date_add(cast(a.first_redeem_date as date), 90) then b.units end) as units_total,
    sum(case when cast(b.order_date as date) >= date_add(cast(a.first_redeem_date as date), 1) and cast(b.order_date as date) <= date_add(cast(a.first_redeem_date as date), 90) and bt_eligible = 1 then b.units end) as units_bt_eligible,
    sum(case when cast(b.order_date as date) >= date_add(cast(a.first_redeem_date as date), 1) and cast(b.order_date as date) <= date_add(cast(a.first_redeem_date as date), 90) and bt_eligible = 1 and c.user_rating in ('HH','HM') then b.units end) as three_star_units_bt_eligible,
    sum(case when cast(b.order_date as date) >= date_add(cast(a.first_redeem_date as date), 1) and cast(b.order_date as date) <= date_add(cast(a.first_redeem_date as date), 90) and bt_eligible = 1 and c.user_rating in ('MH','MM','HL') then b.units end) as two_star_units_bt_eligible,
    sum(case when cast(b.order_date as date) >= date_add(cast(a.first_redeem_date as date), 1) and cast(b.order_date as date) <= date_add(cast(a.first_redeem_date as date), 90) and bt_eligible = 1 and c.user_rating in ('LH','LM','LL','ML') then b.units end) as one_star_units_bt_eligible,
    sum(case when cast(b.order_date as date) >= date_add(cast(a.first_redeem_date as date), 1) and cast(b.order_date as date) <= date_add(cast(a.first_redeem_date as date), 90) and bt_eligible = 0 then b.units end) as units_notbt_eligible,
    sum(case when cast(b.order_date as date) >= date_add(cast(a.first_redeem_date as date), 1) and cast(b.order_date as date) <= date_add(cast(a.first_redeem_date as date), 90) and bt_eligible = 0 and c.user_rating in ('HH','HM') then b.units end) as three_star_units_notbt_eligible,
    sum(case when cast(b.order_date as date) >= date_add(cast(a.first_redeem_date as date), 1) and cast(b.order_date as date) <= date_add(cast(a.first_redeem_date as date), 90) and bt_eligible = 0 and c.user_rating in ('MH','MM','HL') then b.units end) as two_star_units_notbt_eligible,
    sum(case when cast(b.order_date as date) >= date_add(cast(a.first_redeem_date as date), 1) and cast(b.order_date as date) <= date_add(cast(a.first_redeem_date as date), 90) and bt_eligible = 0 and c.user_rating in ('LH','LM','LL','ML') then b.units end) as one_star_units_notbt_eligible
   from grp_gdoop_bizops_db.nvp_all_yrs_txns a
   left join 
   (select x.*, case when z.deal_uuid2 is not null then 1 else 0 end bt_eligible 
        from grp_gdoop_bizops_db.nvp_all_local_txns x 
        left join 
     (select 
         load_date, 
         deal_uuid deal_uuid2
       from 
         grp_gdoop_bizops_db.sh_bt_active_deals_log 
       where product_is_active_flag = 1 and partner_inactive_flag = 0 and is_bookable = 1 and load_date >= '2020-05-01'
     ) z on x.deal_uuid = z.deal_uuid2 and x.order_date = z.load_date
     ) b on a.user_uuid = b.user_uuid
   left join grp_gdoop_bizops_db.nvp_pf_allyrtxns_star as c on a.user_uuid = c.user_uuid AND a.first_redeem_date = c.first_redeem_date
   where a.country_code in ('US','CA') and a.redeem_date_mnth in(6,7) and a.redeem_date_yr = 2020
   group by a.country_code, a.booked_frst_ord, a.l2, a.redeem_date_mnth, a.redeem_date_yr
) as t on v.country_code = t.country_code and v.booked_frst_ord = t.booked_frst_ord and v.l2 = t.l2 and v.year_of_purch = t.year_of_purch and v.month_of_purch = t.month_of_purch;



------------TAB 1
drop table grp_gdoop_bizops_db.nvp_purch_freq_fin_bt_down_nam90;
create table grp_gdoop_bizops_db.nvp_purch_freq_fin_bt_down_nam90 stored as orc as 
select
     v.country_code, 
     v.booked_frst_ord, 
     v.l2,
     v.ltwo, 
     v.year_of_purch, 
     v.month_of_purch,
     v.udv,
     v.udv_notbt_eligible,v.udv_bt_eligible,
     v.repeat_uniql2_visitor,
     t.units_total,
     t.units_notbt_eligible,
     t.units_bt_eligible
from
(select 
      a.country_code, 
      a.booked_frst_ord, 
      a.l2, 
      r.ltwo, 
      a.redeem_date_yr as year_of_purch,
      a.redeem_date_mnth as month_of_purch,
      count(distinct case when r.deal_uuid is not null then concat(r.eventdate, r.user_uuid, r.deal_uuid) end) udv,
      count(distinct case when r.deal_uuid is not null and r.bt_eligible = 0 then concat(r.eventdate, r.user_uuid, r.deal_uuid) end) udv_notbt_eligible,
      count(distinct case when r.deal_uuid is not null and r.bt_eligible = 1 then concat(r.eventdate, r.user_uuid, r.deal_uuid) end) udv_bt_eligible,
      count(distinct r.user_uuid) repeat_uniql2_visitor
    from grp_gdoop_bizops_db.nvp_all_yrs_txns as a
    left join 
    (select 
         x.*, 
         y.ltwo, 
         case when z.deal_uuid2 is not null then 1 else 0 end as bt_eligible
       from grp_gdoop_bizops_db.nvp_all_traffic_purch_nam90 x
       join grp_gdoop_bizops_db.nvp_local_inventory y on x.deal_uuid = y.deal_id
       left join 
       (select 
          load_date, 
          deal_uuid deal_uuid2
        from 
        grp_gdoop_bizops_db.sh_bt_active_deals_log 
        where product_is_active_flag = 1 and partner_inactive_flag = 0 and is_bookable = 1 and load_date >= '2020-05-01'
        ) z on x.deal_uuid = z.deal_uuid2 and x.eventdate = z.load_date
     ) as r on a.user_uuid = r.user_uuid and a.redeem_date_yr = r.redeem_date_yr and a.redeem_date_mnth = r.redeem_date_mnth
     where a.country_code in ('US','CA') and a.redeem_date_mnth in (6,7) and a.redeem_date_yr = 2020
     group by a.country_code, a.booked_frst_ord, a.l2, r.ltwo, a.redeem_date_yr, a.redeem_date_mnth
) as v
left join
(select 
     a.country_code, 
     a.booked_frst_ord,
     a.l2,
     b.ltwo,
     a.redeem_date_yr as year_of_purch,
     a.redeem_date_mnth as month_of_purch,
     sum(case when cast(b.order_date as date) between date_add(cast(a.first_redeem_date as date), 1) and date_add(cast(a.first_redeem_date as date), 90) then b.units end) as units_total,
     sum(case when cast(b.order_date as date) >= date_add(cast(a.first_redeem_date as date), 1) and cast(b.order_date as date) <= date_add(cast(a.first_redeem_date as date), 90) and bt_eligible = 1 then b.units end) as units_bt_eligible,
     sum(case when cast(b.order_date as date) >= date_add(cast(a.first_redeem_date as date), 1) and cast(b.order_date as date) <= date_add(cast(a.first_redeem_date as date), 90) and bt_eligible = 0 then b.units end) as units_notbt_eligible
     from grp_gdoop_bizops_db.nvp_all_yrs_txns a
     left join 
        (select x.*, y.ltwo, case when z.deal_uuid2 is not null then 1 else 0 end bt_eligible 
            from grp_gdoop_bizops_db.nvp_all_local_txns x
            left join grp_gdoop_bizops_db.nvp_local_inventory y on x.deal_uuid = y.deal_id
            left join 
           (select 
              load_date, 
              deal_uuid deal_uuid2 
              from  grp_gdoop_bizops_db.sh_bt_active_deals_log 
              where product_is_active_flag = 1 and partner_inactive_flag = 0 and is_bookable = 1 and load_date >= '2020-05-01'
            ) z on x.deal_uuid = z.deal_uuid2 and x.order_date = z.load_date
         ) b on a.user_uuid = b.user_uuid
       where a.country_code in ('US','CA') and a.redeem_date_mnth in (6,7) and a.redeem_date_yr = 2020
       group by a.country_code, a.booked_frst_ord, a.l2, b.ltwo, a.redeem_date_yr, a.redeem_date_mnth
) as t on v.country_code = t.country_code and v.booked_frst_ord = t.booked_frst_ord and v.l2 = t.l2 and v.ltwo = t.ltwo and v.year_of_purch = t.year_of_purch and v.month_of_purch = t.month_of_purch;



--------------------TAB 2

drop table grp_gdoop_bizops_db.nvp_engagement_purch_basic_tableau_nam90;
create table grp_gdoop_bizops_db.nvp_engagement_purch_basic_tableau_nam90 stored as orc as
select 
   a.country_code, 
   a.booked_frst_ord, 
   a.l2,
   a.year_of_purch,
   a.month_of_purch,
   a.distinct_user_1,
   a.repeat_visitors,
   a.three_star_user,
   a.two_star_user,
   a.one_star_user,
   a.ninety_day_order,
   a.three_star_day_order,
   a.two_star_day_order,
   a.one_star_day_order,
   a.uv,a.udvva,a.udva,a.udvv,a.udv,
   a.distinct_user_2,
   a.repeat_purchaser,
   b.udv udv_from_b,
   b.udv_notbt_eligible,
   b.udv_bt_eligible,
   b.ninety_day_order ninety_day_order_from_b,
    b.units_total,
    b.units_notbt_eligible,
    b.three_star_units_notbt_eligible,
    b.two_star_units_notbt_eligible,
    b.one_star_units_notbt_eligible,
    b.units_bt_eligible,
    b.three_star_units_bt_eligible,
    b.two_star_units_bt_eligible,
    b.one_star_units_bt_eligible
from 
   grp_gdoop_bizops_db.nvp_purch_freq_final_nam90 as a 
left join 
   grp_gdoop_bizops_db.nvp_purch_freq_bt_final_agg_nam90 as b 
   on a.country_code = b.country_code 
   and a.booked_frst_ord = b.booked_frst_ord 
   and a.l2 = b.l2 
   and a.year_of_purch = b.year_of_purch 
   and a.month_of_purch = b.month_of_purch
  order by country_code, booked_frst_ord;
  
 

 
--------------------------------------------------------------------------------
 
c.user_rating in ('HH','HM')
c.user_rating in ('MH','MM','HL')
c.user_rating in ('LH','LM','LL','ML')

create table grp_gdoop_bizops_db.nvp_frequency_statsig_nam90 stored as orc as
select 
    a.country_code, 
    a.booked_frst_ord,
    a.l2,
    a.redeem_date_yr as year_of_purch,
    a.redeem_date_mnth as month_of_purch,
    a.user_uuid,
    case when c.user_rating in ('HH','HM') then '3 star' when c.user_rating in ('MH','MM','HL') then '2 star' when c.user_rating in ('LH','LM','LL','ML') then '1 star' else 'WTF' end as star_rating,
    count(distinct case when cast(b.order_date as date) between date_add(cast(a.first_redeem_date as date), 1) and date_add(cast(a.first_redeem_date as date), 90) then b.parent_order_uuid end) ninety_day_order,
    sum(case when cast(b.order_date as date) between date_add(cast(a.first_redeem_date as date), 1) and date_add(cast(a.first_redeem_date as date), 90) then b.units end) as units_total,
    sum(case when cast(b.order_date as date) >= date_add(cast(a.first_redeem_date as date), 1) and cast(b.order_date as date) <= date_add(cast(a.first_redeem_date as date), 90) and bt_eligible = 1 then b.units end) as units_bt_eligible,
    sum(case when cast(b.order_date as date) >= date_add(cast(a.first_redeem_date as date), 1) and cast(b.order_date as date) <= date_add(cast(a.first_redeem_date as date), 90) and bt_eligible = 0 then b.units end) as units_notbt_eligible
   from grp_gdoop_bizops_db.nvp_all_yrs_txns a
   left join 
   (select x.*, case when z.deal_uuid2 is not null then 1 else 0 end bt_eligible 
        from grp_gdoop_bizops_db.nvp_all_local_txns x 
        left join 
     (select 
         load_date, 
         deal_uuid deal_uuid2
       from 
         grp_gdoop_bizops_db.sh_bt_active_deals_log 
       where product_is_active_flag = 1 and partner_inactive_flag = 0 and is_bookable = 1 and load_date >= '2020-05-01'
     ) z on x.deal_uuid = z.deal_uuid2 and x.order_date = z.load_date
     ) b on a.user_uuid = b.user_uuid
   left join grp_gdoop_bizops_db.nvp_pf_allyrtxns_star as c on a.user_uuid = c.user_uuid AND a.first_redeem_date = c.first_redeem_date
   where a.country_code in ('US','CA') and a.redeem_date_mnth = 6 and a.redeem_date_yr = 2020
   group by a.country_code, a.booked_frst_ord, a.l2, a.redeem_date_mnth, a.redeem_date_yr, a.user_uuid,case when c.user_rating in ('HH','HM') then '3 star' when c.user_rating in ('MH','MM','HL') then '2 star' when c.user_rating in ('LH','LM','LL','ML') then '1 star' else 'WTF' end
   ;
   
  
select 
  VAR_POP(units_total) variance_overall,
  VAR_POP(case when star_rating = '3 star' then units_total) variance_3star
from 
  grp_gdoop_bizops_db.nvp_frequency_statsig_nam90;
 
 

  