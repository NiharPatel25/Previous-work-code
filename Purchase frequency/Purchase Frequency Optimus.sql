drop table nvp_agg_dealview_nst;
nvp_deal_udv
-----TXNS

select * from grp_gdoop_bizops_db.sh_bt_txns where order_date = '2020-03-31';

select * from grp_gdoop_bizops_db.sh_bt_txns;

---GRABBING USERS WHO REDEEMED IN ANY PARTICULAR MONTH AND IF THEY BOOKED DURING THEIR FIRST ORDER
--first_redeem_date is the first redeem date in that particular month

drop table if exists grp_gdoop_bizops_db.nvp_all_yrs_txns;
create table grp_gdoop_bizops_db.nvp_all_yrs_txns stored as orc as
select 
    user_uuid, 
    country_code country_code,
    first_redeem_date,
    redeem_date_yr,
    redeem_date_mnth,
    max(booked) booked_frst_ord,
    max(l2) l2
from 
    (select 
           t.user_uuid, 
           t.parent_order_uuid,
           t.deal_uuid, 
           t.country_code, 
           t.booked, 
           t.order_date, 
           t.redeem_date,
           t.first_redeem_date,
           t.redeem_date_mnth,
           t.redeem_date_yr,
           case when t.redeem_date = t.first_redeem_date then 1 else 0 end accepted_redeemed_ord, 
           gbl.l2 l2
      from 
      (select user_uuid, 
                 parent_order_uuid, 
                 deal_uuid,
                 country_code, 
                 booked, 
                 cast(order_date as date) order_date,
                 cast(redeem_date as date) redeem_date,
                 month(cast(redeem_date as date)) redeem_date_mnth, 
                 year(cast(redeem_date as date)) redeem_date_yr, 
                 min(cast(redeem_date as date)) over(partition by country_code, user_uuid, year(cast(redeem_date as date)), month(cast(redeem_date as date))) first_redeem_date
                 from grp_gdoop_bizops_db.rt_bt_txns
                 WHERE cast(redeem_date as date) >= cast('2018-07-01' as date)
             ) as t
       join 
       (select parent_order_uuid, is_zero_amount
            from user_edwprod.fact_gbl_transactions
       where is_zero_amount = 0
       group by parent_order_uuid, is_zero_amount) tx on tx.parent_order_uuid = t.parent_order_uuid
      join
      (select 
          deal_id, 
          grt_l2_cat_name l2
       from 
          user_edwprod.dim_gbl_deal_lob 
         where grt_l1_cat_name = 'L1 - Local' and grt_l2_cat_name <> 'L2 - Retail' 
         group by deal_id, grt_l2_cat_name) gbl on gbl.deal_id = t.deal_uuid
       join 
       (select 
          product_uuid
        from 
          user_edwprod.dim_offer_ext 
        where inventory_service_name <> 'tpis' 
        group by product_uuid) c on t.deal_uuid = c.product_uuid
        group by 
        t.user_uuid, t.deal_uuid, t.country_code, t.booked, t.order_date, t.redeem_date, t.first_redeem_date,t.parent_order_uuid, gbl.l2, t.redeem_date_yr, t.redeem_date_mnth) fin_ 
       where accepted_redeemed_ord = 1 group by user_uuid, country_code, first_redeem_date, redeem_date_yr, redeem_date_mnth;

----------RATINGS DATA
      
drop table grp_gdoop_bizops_db.nvp_pf_allyrtxns_star;
create table grp_gdoop_bizops_db.nvp_pf_allyrtxns_star stored as orc as
select 
 a.user_uuid,
 a.country_code,
 a.first_redeem_date,
 case when b.recency_segment in ( 'acquisition' ,'sub_activation') then 'activation'
      when b.recency_segment in ('reactivation') then 'reactivation'
      when b.recency_segment in ('current_purchasers','Recent First Purchasers') and b.frequency_segment in ('order_cnt_1') then 'LH'
      when b.recency_segment in ('current_purchasers') and b.frequency_segment in ('order_cnt_2-4') then 'MH'
      when b.recency_segment in ('current_purchasers') and b.frequency_segment in ('order_cnt_5-10','order_cnt_11-20','order_cnt_20+') then 'HH'
      when b.recency_segment in ('lapsed_first_purchasers','lapsed_current_purchasers') and  b.frequency_segment in ('order_cnt_1') then 'LM'
      when b.recency_segment in ('lapsed_current_purchasers') and  b.frequency_segment in ('order_cnt_2-4') then 'MM'
      when b.recency_segment in ('lapsed_current_purchasers') and  b.frequency_segment in ('order_cnt_5-10','order_cnt_11-20','order_cnt_20+') then 'HM'
      when b.recency_segment in ('pre-attrition','retention') and b.frequency_segment in ('order_cnt_1') then 'LL'
      when b.recency_segment in ('pre-attrition','retention') and b.frequency_segment  in ('order_cnt_2-4') then 'ML'
      when b.recency_segment in ('pre-attrition','retention') and  b.frequency_segment in ('order_cnt_5-10','order_cnt_11-20','order_cnt_20+') then 'HL'
      else 'WTF' end as user_rating
from 
   grp_gdoop_bizops_db.nvp_all_yrs_txns as a
left join 
   user_groupondw.agg_user_ord_seg_day_na as b on a.user_uuid = b.user_uuid
where
   cast(b.valid_date_end as date) >= cast('2018-07-01' as date)
   and cast(a.first_redeem_date as date) >= cast(b.valid_date_start as date)
   and cast(a.first_redeem_date as date) <= cast(b.valid_date_end as date)
group by 
 a.user_uuid,
 a.country_code,
 a.first_redeem_date,
 case when b.recency_segment in ( 'acquisition' ,'sub_activation') then 'activation'
      when b.recency_segment in ('reactivation') then 'reactivation'
      when b.recency_segment in ('current_purchasers','Recent First Purchasers') and b.frequency_segment in ('order_cnt_1') then 'LH'
      when b.recency_segment in ('current_purchasers') and b.frequency_segment in ('order_cnt_2-4') then 'MH'
      when b.recency_segment in ('current_purchasers') and b.frequency_segment in ('order_cnt_5-10','order_cnt_11-20','order_cnt_20+') then 'HH'
      when b.recency_segment in ('lapsed_first_purchasers','lapsed_current_purchasers') and  b.frequency_segment in ('order_cnt_1') then 'LM'
      when b.recency_segment in ('lapsed_current_purchasers') and  b.frequency_segment in ('order_cnt_2-4') then 'MM'
      when b.recency_segment in ('lapsed_current_purchasers') and  b.frequency_segment in ('order_cnt_5-10','order_cnt_11-20','order_cnt_20+') then 'HM'
      when b.recency_segment in ('pre-attrition','retention') and b.frequency_segment in ('order_cnt_1') then 'LL'
      when b.recency_segment in ('pre-attrition','retention') and b.frequency_segment  in ('order_cnt_2-4') then 'ML'
      when b.recency_segment in ('pre-attrition','retention') and  b.frequency_segment in ('order_cnt_5-10','order_cnt_11-20','order_cnt_20+') then 'HL'
      else 'WTF' end;

------FINANCE 



drop table if exists grp_gdoop_bizops_db.nvp_all_local_txns;
create table grp_gdoop_bizops_db.nvp_all_local_txns stored as orc as
select 
    t.user_uuid, 
    t.country_code, 
    t.parent_order_uuid, 
    t.deal_uuid,
    t.order_date, 
    t.units, 
    t.nob
from 
    (select 
       user_uuid, 
       country_code, 
       parent_order_uuid, 
       deal_uuid, 
       cast(order_date as date) order_date, 
       units, 
       nob
     from grp_gdoop_bizops_db.rt_bt_txns 
       where cast(order_date as date) >= cast('2018-07-01' as date)) t
     join 
    (select parent_order_uuid, is_zero_amount
      from user_edwprod.fact_gbl_transactions
     where is_zero_amount = 0
     group by parent_order_uuid, is_zero_amount) tx on tx.parent_order_uuid = t.parent_order_uuid
    join
    (select 
       deal_id
     from user_edwprod.dim_gbl_deal_lob 
     where grt_l1_cat_name = 'L1 - Local' and grt_l2_cat_name <> 'L2 - Retail' 
     group by deal_id) gbl on gbl.deal_id = t.deal_uuid
    join 
    (select 
      product_uuid
     from 
      user_edwprod.dim_offer_ext 
     where inventory_service_name <> 'tpis' 
     group by product_uuid) c on t.deal_uuid = c.product_uuid;


-----60 Days UDV

--Creating Empty tables
drop table if exists grp_gdoop_bizops_db.nvp_agg_dealview_bh2018;

drop table if exists grp_gdoop_bizops_db.nvp_agg_dealview_bh;
drop table if exists grp_gdoop_bizops_db.nvp_agg_otherview_bh;
drop table if exists grp_gdoop_bizops_db.nvp_agg_dealview_nst;
drop table if exists grp_gdoop_bizops_db.nvp_agg_otherview_nst;

create table grp_gdoop_bizops_db.nvp_agg_dealview_bh  (
    eventdate string,
    user_uuid string,
    platform string,
    deal_uuid string,
    l1 string,
    l2 string
)partitioned by (redeem_date_mnth int, redeem_date_yr int) 
stored as orc
tblproperties ("orc.compress"="SNAPPY");

create table grp_gdoop_bizops_db.nvp_agg_otherview_bh  (
    eventdate string,
    user_uuid string,
    platform string,
    deal_uuid string,
    l1 string,
    l2 string
)partitioned by (redeem_date_mnth int, redeem_date_yr int) 
stored as orc
tblproperties ("orc.compress"="SNAPPY");


create table grp_gdoop_bizops_db.nvp_agg_dealview_nst  (
    eventdate string,
    user_uuid string,
    platform string,
    deal_uuid string,
    l1 string,
    l2 string
)partitioned by (redeem_date_mnth int, redeem_date_yr int) stored as orc
tblproperties ("orc.compress"="SNAPPY");


create table r  (
    eventdate string,
    user_uuid string,
    platform string,
    deal_uuid string,
    l1 string,
    l2 string
)partitioned by (redeem_date_mnth int, redeem_date_yr int) stored as orc
tblproperties ("orc.compress"="SNAPPY");



---grp_gdoop_bizops_db.nvp_all_yrs_txns;--- UDV info is collected only for Intl Countries. 
----date_sub(next_day(dt, 'MON'), 1)
---date_sub(current_date,1)
---insert into grp_gdoop_bizops_db.nvp_agg_dealview_bh
---grp_gdoop_bizops_db.nvp_pf_temp_copy;

---date_sub(next_day(dt, 'MON'), 1)



insert overwrite table grp_gdoop_bizops_db.nvp_agg_dealview_bh partition (redeem_date_mnth, redeem_date_yr)
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
where cast(b.dt as date) >= date_sub(CURRENT_DATE, 120)
and cast(b.dt as date) between date_add(a.first_redeem_date,1) and date_add(a.first_redeem_date,60)
and a.redeem_date_yr = year(date_sub(CURRENT_DATE, 90)) and a.redeem_date_mnth = month(date_sub(CURRENT_DATE, 90))
and a.country_code <> 'US' and a.country_code <> 'CA'
and b.bot_flag = 0
and lower(b.platform) in ('web','desktop','touch') ) fin_ group by event_date, user_uuid, redeem_date_mnth, redeem_date_yr, platform, deal_uuid, l1, l2;




------

insert overwrite table grp_gdoop_bizops_db.nvp_agg_otherview_bh partition (redeem_date_mnth, redeem_date_yr)
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
where cast(b.dt as date) >= date_sub(CURRENT_DATE, 120)
and cast(b.dt as date) between date_add(a.first_redeem_date,1) and date_add(a.first_redeem_date,60)
and a.redeem_date_yr = year(date_sub(CURRENT_DATE, 90)) and a.redeem_date_mnth = month(date_sub(CURRENT_DATE, 90))
and a.country_code <> 'US' and a.country_code <> 'CA'
and b.bot_flag = 0
and lower(b.platform) in ('web','desktop','touch') ) fin_ group by event_date, user_uuid, platform, redeem_date_mnth, redeem_date_yr, deal_uuid, l1, l2;



-----

insert overwrite table grp_gdoop_bizops_db.nvp_agg_dealview_nst partition (redeem_date_mnth, redeem_date_yr)
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
where cast(d.eventdate as date) between date_sub(CURRENT_DATE, 120) and date_sub(CURRENT_DATE, 30)
and cast(d.eventdate as date) between date_add(c.first_redeem_date,1) and date_add(c.first_redeem_date,60)
and c.redeem_date_yr = year(date_sub(CURRENT_DATE, 90)) and c.redeem_date_mnth = month(date_sub(CURRENT_DATE, 90))
and c.country_code <> 'US' and c.country_code <> 'CA'
) fin_ group by event_date, user_uuid, platform, deal_uuid, l1, l2, redeem_date_mnth, redeem_date_yr;


insert overwrite table grp_gdoop_bizops_db.nvp_agg_dealview_nst partition (redeem_date_mnth, redeem_date_yr)
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
where cast(d.eventdate as date) between cast('2020-07-01' as date) and cast('2020-10-05' as date)
and cast(d.eventdate as date) between date_add(c.first_redeem_date,1) and date_add(c.first_redeem_date,60)
and c.redeem_date_yr = 2020 and c.redeem_date_mnth = 7
and c.country_code <> 'US' and c.country_code <> 'CA'
) fin_ group by event_date, user_uuid, platform, deal_uuid, l1, l2, redeem_date_mnth, redeem_date_yr;

------------------------------------------------------------
insert overwrite table grp_gdoop_bizops_db.nvp_agg_otherview_nst partition (redeem_date_mnth, redeem_date_yr)
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
   where cast(d.eventdate as date) between date_sub(CURRENT_DATE, 120) and date_sub(CURRENT_DATE, 30)
     and cast(d.eventdate as date) between date_add(c.first_redeem_date,1) and date_add(c.first_redeem_date,60)
     and c.redeem_date_yr = year(date_sub(CURRENT_DATE, 90)) and c.redeem_date_mnth = month(date_sub(CURRENT_DATE, 90))
     and c.country_code <> 'US' and c.country_code <> 'CA'
) fin_ group by event_date, user_uuid, platform, deal_uuid, l1, l2, redeem_date_mnth, redeem_date_yr;



------Aggregating it all together

drop table if exists grp_gdoop_bizops_db.nvp_all_traffic_purch;

create table grp_gdoop_bizops_db.nvp_all_traffic_purch stored as orc as
select * from (
select * from grp_gdoop_bizops_db.nvp_agg_dealview_bh
UNION ALL
select * from grp_gdoop_bizops_db.nvp_agg_otherview_bh
UNION ALL
select * from grp_gdoop_bizops_db.nvp_agg_dealview_nst
UNION ALL
select * from grp_gdoop_bizops_db.nvp_agg_otherview_nst
) fin_;



------Local Inventory


drop table if exists grp_gdoop_bizops_db.nvp_local_inventory;
create table grp_gdoop_bizops_db.nvp_local_inventory stored as orc as
select distinct deal_id, grt_l2_cat_name ltwo, country_code, country_id 
from user_edwprod.dim_gbl_deal_lob 
where grt_l1_cat_name = 'L1 - Local';


----- STAGE 1


-----LOOKS AT ALL UDV
drop table if exists grp_gdoop_bizops_db.nvp_purch_trial_one;
create table grp_gdoop_bizops_db.nvp_purch_trial_one stored as orc as
select 
    a.country_code, 
    a.booked_frst_ord, 
    a.l2, 
    a.redeem_date_yr as year_of_purch,
    a.redeem_date_mnth as month_of_purch,
    count(distinct a.user_uuid) distinct_users,
    count(distinct b.user_uuid) repeat_visitors, 
    count(distinct concat(b.eventdate, b.user_uuid)) uv, 
    count(distinct case when b.deal_uuid is not null then concat(b.eventdate, b.user_uuid) end) udvva,
    count(distinct case when b.deal_uuid is not null then concat(b.eventdate, b.user_uuid, b.deal_uuid) end) udva
from 
    grp_gdoop_bizops_db.nvp_all_yrs_txns as a 
    left join grp_gdoop_bizops_db.nvp_all_traffic_purch as b 
on a.user_uuid = b.user_uuid and a.redeem_date_yr = b.redeem_date_yr and a.redeem_date_mnth = b.redeem_date_mnth
where a.country_code <> 'US' and a.country_code <> 'CA'
group by a.country_code, a.booked_frst_ord, a.l2, a.redeem_date_yr, a.redeem_date_mnth;

-----LOOKS AT LOCAL UDV
drop table if exists grp_gdoop_bizops_db.nvp_purch_trial_two;
create table grp_gdoop_bizops_db.nvp_purch_trial_two stored as orc as
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
       from grp_gdoop_bizops_db.nvp_all_traffic_purch x
       join grp_gdoop_bizops_db.nvp_local_inventory y on x.deal_uuid = y.deal_id
      ) as r on a.user_uuid = r.user_uuid and a.redeem_date_yr = r.redeem_date_yr and a.redeem_date_mnth = r.redeem_date_mnth
where a.country_code <> 'US' and a.country_code <> 'CA'
group by a.country_code, a.booked_frst_ord, a.l2, a.redeem_date_yr, a.redeem_date_mnth;

-----ALL LOCAL TRANSACTIONS
drop table if exists grp_gdoop_bizops_db.nvp_purch_trial_three;
create table grp_gdoop_bizops_db.nvp_purch_trial_three stored as orc as
select 
a.country_code, 
a.booked_frst_ord,
a.l2,
a.redeem_date_yr as year_of_purch,
a.redeem_date_mnth as month_of_purch, 
count(distinct a.user_uuid) distinct_users, 
count(distinct b.user_uuid) repeat_purchaser, -----Need to add the repeat purchaser criteria here. Its wrong. 
count(distinct case when cast(b.order_date as date) between date_add(cast(a.first_redeem_date as date), 1) and date_add(cast(a.first_redeem_date as date), 60) then b.parent_order_uuid end) sixty_day_order
from grp_gdoop_bizops_db.nvp_all_yrs_txns a
     left join grp_gdoop_bizops_db.nvp_all_local_txns b on a.user_uuid = b.user_uuid
where a.country_code <> 'US' and a.country_code <> 'CA'
group by a.country_code, a.booked_frst_ord, a.l2, a.redeem_date_yr, a.redeem_date_mnth;



drop table if exists grp_gdoop_bizops_db.nvp_purch_freq_final;
create table grp_gdoop_bizops_db.nvp_purch_freq_final stored as orc as 
select
   a.country_code,
   a.booked_frst_ord,
   a.l2,
   a.year_of_purch,
   a.month_of_purch,
   a.distinct_users distinct_user_1,
   a.repeat_visitors,
   a.uv,a.udvva,a.udva,b.udvv,b.udv,
   c.sixty_day_order,
   c.distinct_users as distinct_user_2,
   c.repeat_purchaser
from grp_gdoop_bizops_db.nvp_purch_trial_one a
  left join grp_gdoop_bizops_db.nvp_purch_trial_two b 
         on a.booked_frst_ord = b.booked_frst_ord and a.l2 = b.l2 and a.year_of_purch = b.year_of_purch and a.month_of_purch = b.month_of_purch and a.country_code = b.country_code
  left join grp_gdoop_bizops_db.nvp_purch_trial_three c 
         on a.booked_frst_ord = c.booked_frst_ord and a.l2 = c.l2 and a.year_of_purch = c.year_of_purch and a.month_of_purch = c.month_of_purch and a.country_code = c.country_code;


---------
/*
drop table grp_gdoop_bizops_db.nvp_purch_freq_final;
create table grp_gdoop_bizops_db.nvp_purch_freq_final stored as orc as 
select
u.country_code, 
u.booked_frst_ord, 
u.l2,
u.year_of_purch,
u.month_of_purch,
u.distinct_users distinct_user_1,
u.repeat_visitors,
u.uv,u.udvva,u.udva,v.udvv,v.udv,
t.sixty_day_order,
t.distinct_users as distinct_user_2,
t.repeat_purchaser
from
	(select 
		a.country_code, 
		a.booked_frst_ord, 
		a.l2, 
		a.redeem_date_yr as year_of_purch,
		a.redeem_date_mnth as month_of_purch,
		count(distinct a.user_uuid) distinct_users,
		count(distinct b.user_uuid) repeat_visitors, 
		count(distinct concat(b.eventdate, b.user_uuid)) uv, 
		count(distinct case when b.deal_uuid is not null then concat(b.eventdate, b.user_uuid) end) udvva,
		count(distinct case when b.deal_uuid is not null then concat(b.eventdate, b.user_uuid, b.deal_uuid) end) udva
	from grp_gdoop_bizops_db.nvp_all_yrs_txns as a 
		left join grp_gdoop_bizops_db.nvp_all_traffic_purch as b 
		on a.user_uuid = b.user_uuid and a.redeem_date_yr = b.redeem_date_yr and a.redeem_date_mnth = b.redeem_date_mnth
	where a.country_code <> 'US' and a.country_code <> 'CA'
	group by a.country_code, a.booked_frst_ord, a.l2, a.redeem_date_yr, a.redeem_date_mnth
	) as u
left join
	(select 
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
			from grp_gdoop_bizops_db.nvp_all_traffic_purch x
			join grp_gdoop_bizops_db.nvp_local_inventory y on x.deal_uuid = y.deal_id
		) as r on a.user_uuid = r.user_uuid and a.redeem_date_yr = r.redeem_date_yr and a.redeem_date_mnth = r.redeem_date_mnth
		where a.country_code <> 'US' and a.country_code <> 'CA'
		group by a.country_code, a.booked_frst_ord, a.l2, a.redeem_date_yr, a.redeem_date_mnth
	) as v on u.country_code = v.country_code and u.booked_frst_ord = v.booked_frst_ord and u.l2 = v.l2 and u.year_of_purch = v.year_of_purch and u.month_of_purch = v.month_of_purch
left join
	(select 
		a.country_code, 
		a.booked_frst_ord,
		a.l2,
		a.redeem_date_yr as year_of_purch,
	    a.redeem_date_mnth as month_of_purch, 
		count(distinct a.user_uuid) distinct_users, 
		count(distinct b.user_uuid) repeat_purchaser,
		count(distinct case when cast(b.order_date as date) between date_add(cast(a.first_redeem_date as date), 1) and date_add(cast(a.first_redeem_date as date), 60) then b.parent_order_uuid end) sixty_day_order
	from grp_gdoop_bizops_db.nvp_all_yrs_txns a
	left join grp_gdoop_bizops_db.nvp_all_local_txns b on a.user_uuid = b.user_uuid
	where a.country_code <> 'US' and a.country_code <> 'CA'
	group by a.country_code, a.booked_frst_ord, a.l2, a.redeem_date_yr, a.redeem_date_mnth
	) as t on u.country_code = t.country_code and u.booked_frst_ord = t.booked_frst_ord and u.l2 = t.l2 and u.year_of_purch = t.year_of_purch and u.month_of_purch = t.month_of_purch;





drop table grp_gdoop_bizops_db.nvp_purch_freq_final;
create table grp_gdoop_bizops_db.nvp_purch_freq_final stored as orc as 
select
     u.country_code, 
     u.booked_frst_ord, 
     u.l2,
     u.year_of_purch,
     u.month_of_purch,
     u.distinct_users distinct_user_1,
     u.repeat_visitors,
     u.uv,u.udvva,u.udva,v.udvv,v.udv,
     t.sixty_day_order,
     t.distinct_users as distinct_user_2,
     t.repeat_purchaser
from
     (select 
        a.country_code, 
        a.booked_frst_ord, 
        a.l2, 
        a.redeem_date_yr as year_of_purch,
        a.redeem_date_mnth as month_of_purch,
        count(distinct a.user_uuid) distinct_users,
        count(distinct b.user_uuid) repeat_visitors, 
        count(distinct concat(b.eventdate, b.user_uuid)) uv, 
        count(distinct case when b.deal_uuid is not null then concat(b.eventdate, b.user_uuid) end) udvva,
        count(distinct case when b.deal_uuid is not null then concat(b.eventdate, b.user_uuid, b.deal_uuid) end) udva
      from grp_gdoop_bizops_db.nvp_all_yrs_txns as a 
      left join grp_gdoop_bizops_db.nvp_all_traffic_purch as b 
      on a.user_uuid = b.user_uuid and a.redeem_date_yr = b.redeem_date_yr and a.redeem_date_mnth = b.redeem_date_mnth
      where a.country_code <> 'US' and a.country_code <> 'CA'
      group by a.country_code, a.booked_frst_ord, a.l2, a.redeem_date_yr, a.redeem_date_mnth
      ) as u
    left join
     (select 
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
        from grp_gdoop_bizops_db.nvp_all_traffic_purch x
        join grp_gdoop_bizops_db.nvp_local_inventory y on x.deal_uuid = y.deal_id
        ) as r on a.user_uuid = r.user_uuid and a.redeem_date_yr = r.redeem_date_yr and a.redeem_date_mnth = r.redeem_date_mnth
        where a.country_code <> 'US' and a.country_code <> 'CA'
        group by a.country_code, a.booked_frst_ord, a.l2, a.redeem_date_yr, a.redeem_date_mnth
        ) as v on u.country_code = v.country_code and u.booked_frst_ord = v.booked_frst_ord and u.l2 = v.l2 and u.year_of_purch = v.year_of_purch and u.month_of_purch = v.month_of_purch
     left join
       (select 
         a.country_code, 
         a.booked_frst_ord,
         a.l2,
         a.redeem_date_yr as year_of_purch,
         a.redeem_date_mnth as month_of_purch, 
         count(distinct a.user_uuid) distinct_users, 
         count(distinct b.user_uuid) repeat_purchaser,
         count(distinct case when cast(b.order_date as date) between date_add(cast(a.first_redeem_date as date), 1) and date_add(cast(a.first_redeem_date as date), 60) then b.parent_order_uuid end) sixty_day_order
        from grp_gdoop_bizops_db.nvp_all_yrs_txns a
        left join grp_gdoop_bizops_db.nvp_all_local_txns b on a.user_uuid = b.user_uuid
        where a.country_code <> 'US' and a.country_code <> 'CA'
        group by a.country_code, a.booked_frst_ord, a.l2, a.redeem_date_yr, a.redeem_date_mnth
       ) as t on u.country_code = t.country_code and u.booked_frst_ord = t.booked_frst_ord and u.l2 = t.l2 and u.year_of_purch = t.year_of_purch and u.month_of_purch = t.month_of_purch;
*/
-----STAGE 2

drop table if exists grp_gdoop_bizops_db.nvp_purch_freq_bt_final_agg;

create table grp_gdoop_bizops_db.nvp_purch_freq_bt_final_agg stored as orc as 
select
    v.country_code, 
    v.booked_frst_ord, 
    v.l2,
    v.year_of_purch,
    v.month_of_purch,
    v.udv,
    v.udv_notbt_eligible,
    v.udv_bt_eligible,
    t.sixty_day_order,
    t.units_total,
    t.units_notbt_eligible,
    t.units_bt_eligible
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
         from grp_gdoop_bizops_db.nvp_all_traffic_purch x
         join grp_gdoop_bizops_db.nvp_local_inventory y on x.deal_uuid = y.deal_id ----This is the same filter for local used to calculate UDV. including retail. 
         left join 
         (select 
             load_date, 
             deal_uuid deal_uuid2
          from 
           grp_gdoop_bizops_db.sh_bt_active_deals_log 
          where product_is_active_flag = 1 and partner_inactive_flag = 0 and is_bookable = 1 and load_date >= '2018-07-01'
          ) z on x.deal_uuid = z.deal_uuid2 and x.eventdate = z.load_date
     ) as r on a.user_uuid = r.user_uuid and a.redeem_date_yr = r.redeem_date_yr and a.redeem_date_mnth = r.redeem_date_mnth
    where a.country_code <> 'US' and a.country_code <> 'CA'
    group by a.country_code, a.booked_frst_ord, a.l2, a.redeem_date_mnth, a.redeem_date_yr
   ) as v
left join
(select 
    a.country_code, 
    a.booked_frst_ord,
    a.l2,
    a.redeem_date_yr as year_of_purch,
    a.redeem_date_mnth as month_of_purch,
    count(distinct case when cast(b.order_date as date) between date_add(cast(a.first_redeem_date as date), 1) and date_add(cast(a.first_redeem_date as date), 60) then b.parent_order_uuid end) sixty_day_order,
    sum(case when cast(b.order_date as date) between date_add(cast(a.first_redeem_date as date), 1) and date_add(cast(a.first_redeem_date as date), 60) then b.units end) as units_total,
    sum(case when cast(b.order_date as date) >= date_add(cast(a.first_redeem_date as date), 1) and cast(b.order_date as date) <= date_add(cast(a.first_redeem_date as date), 60) and bt_eligible = 1 then b.units end) as units_bt_eligible,
    sum(case when cast(b.order_date as date) >= date_add(cast(a.first_redeem_date as date), 1) and cast(b.order_date as date) <= date_add(cast(a.first_redeem_date as date), 60) and bt_eligible = 0 then b.units end) as units_notbt_eligible
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
       where product_is_active_flag = 1 and partner_inactive_flag = 0 and is_bookable = 1 and load_date >= '2018-07-01'
     ) z on x.deal_uuid = z.deal_uuid2 and x.order_date = z.load_date
     ) b on a.user_uuid = b.user_uuid
   where a.country_code <> 'US' and a.country_code <> 'CA'
   group by a.country_code, a.booked_frst_ord, a.l2, a.redeem_date_mnth, a.redeem_date_yr
) as t on v.country_code = t.country_code and v.booked_frst_ord = t.booked_frst_ord and v.l2 = t.l2 and v.year_of_purch = t.year_of_purch and v.month_of_purch = t.month_of_purch;


----Tableau 1


drop table if exists grp_gdoop_bizops_db.nvp_purch_freq_fin_bt_down;
create table grp_gdoop_bizops_db.nvp_purch_freq_fin_bt_down stored as orc as 
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
       from grp_gdoop_bizops_db.nvp_all_traffic_purch x
       join grp_gdoop_bizops_db.nvp_local_inventory y on x.deal_uuid = y.deal_id
       left join 
       (select 
          load_date, 
          deal_uuid deal_uuid2
        from 
        grp_gdoop_bizops_db.sh_bt_active_deals_log 
        where product_is_active_flag = 1 and partner_inactive_flag = 0 and is_bookable = 1 and load_date >= '2018-07-01'
        ) z on x.deal_uuid = z.deal_uuid2 and x.eventdate = z.load_date
       ) as r on a.user_uuid = r.user_uuid and a.redeem_date_yr = r.redeem_date_yr and a.redeem_date_mnth = r.redeem_date_mnth
      where a.country_code <> 'US' and a.country_code <> 'CA'
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
     sum(case when cast(b.order_date as date) between date_add(cast(a.first_redeem_date as date), 1) and date_add(cast(a.first_redeem_date as date), 60) then b.units end) as units_total,
     sum(case when cast(b.order_date as date) >= date_add(cast(a.first_redeem_date as date), 1) and cast(b.order_date as date) <= date_add(cast(a.first_redeem_date as date), 60) and bt_eligible = 1 then b.units end) as units_bt_eligible,
     sum(case when cast(b.order_date as date) >= date_add(cast(a.first_redeem_date as date), 1) and cast(b.order_date as date) <= date_add(cast(a.first_redeem_date as date), 60) and bt_eligible = 0 then b.units end) as units_notbt_eligible
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
              where product_is_active_flag = 1 and partner_inactive_flag = 0 and is_bookable = 1 and load_date >= '2018-07-01'
              ) z on x.deal_uuid = z.deal_uuid2 and x.order_date = z.load_date
          ) b on a.user_uuid = b.user_uuid
        where a.country_code <> 'US' and a.country_code <> 'CA'
        group by a.country_code, a.booked_frst_ord, a.l2, b.ltwo, a.redeem_date_yr, a.redeem_date_mnth
) as t on v.country_code = t.country_code and v.booked_frst_ord = t.booked_frst_ord and v.l2 = t.l2 and v.ltwo = t.ltwo and v.year_of_purch = t.year_of_purch and v.month_of_purch = t.month_of_purch;



----TABLEAU_2


drop table if exists grp_gdoop_bizops_db.nvp_engagement_purch_basic_tableau;
create table grp_gdoop_bizops_db.nvp_engagement_purch_basic_tableau stored as orc as
select 
   a.country_code, 
   a.booked_frst_ord, 
   a.l2,
   a.year_of_purch,
   a.month_of_purch,
   a.distinct_user_1,
   a.repeat_visitors,
   a.uv,a.udvva,a.udva,a.udvv,a.udv,
   a.sixty_day_order,
   a.distinct_user_2,
   a.repeat_purchaser,
   b.udv udv_from_b,
   b.udv_notbt_eligible,
   b.udv_bt_eligible,
   b.sixty_day_order sixty_day_order_from_b,
   b.units_total,
   b.units_notbt_eligible,
   b.units_bt_eligible
from 
   grp_gdoop_bizops_db.nvp_purch_freq_final as a 
left join 
   grp_gdoop_bizops_db.nvp_purch_freq_bt_final_agg as b 
   on a.country_code = b.country_code 
   and a.booked_frst_ord = b.booked_frst_ord 
   and a.l2 = b.l2 
   and a.year_of_purch = b.year_of_purch 
   and a.month_of_purch = b.month_of_purch
  order by country_code, booked_frst_ord;


select * from grp_gdoop_bizops_db.rt_bt_txns limit 5;


SELECT min(cast(load_date as date)) FROM grp_gdoop_bizops_db.sh_bt_active_deals_log; 

select x.*, y.ltwo, case when z.deal_uuid2 is not null then 1 else 0 end as bt_eligible
from grp_gdoop_bizops_db.nvp_all_traffic_purch x
join grp_gdoop_bizops_db.nvp_local_inventory y on x.deal_uuid = y.deal_id
left join 
	(select 
	load_date, 
	deal_uuid deal_uuid2
	from 
	grp_gdoop_bizops_db.sh_bt_active_deals_log 
	where product_is_active_flag = 1 and partner_inactive_flag = 0 and is_bookable = 1 and load_date >= '2018-07-01'
	) z on x.deal_uuid = z.deal_uuid2 and x.eventdate = z.load_date;