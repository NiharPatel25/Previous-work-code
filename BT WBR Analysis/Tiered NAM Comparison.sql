-----min booked at rt_bt_txns

use grp_gdoop_bizops_db;
drop table nvp_bt_bookings;
create table nvp_bt_bookings stored as orc as
select parent_order_uuid, min_booked_at
from (
  select
        v.billing_id as parent_order_uuid,
        min(b.created_at) min_booked_at
    from dwh_base.vouchers v
    join grp_gdoop_bizops_db.sh_bt_bookings_rebuild b
      on b.voucher_code = v.voucher_code and b.security_code = v.security_code
    where is_a_groupon_booking = 1 and (lower(booked_by) in ('customer', 'api') or booked_by is null)
    group by v.billing_id
  union
    select parent_order_uuid, min(b.created_at) min_booked_at
    from user_gp.camp_membership_coupons v
join grp_gdoop_bizops_db.sh_bt_bookings_rebuild b on b.voucher_code = v.code and b.user_uuid = v.purchaser_consumer_id
join user_edwprod.fact_gbl_transactions t on t.user_uuid = v.purchaser_consumer_id  and t.order_id = v.order_id
where b.country_id = 'US' and is_a_groupon_booking = 1 and (lower(booked_by) in ('customer', 'api') or booked_by is null)
group by parent_order_uuid
) a
;



drop table nvp_bt_reds;
create table nvp_bt_reds stored as orc as
select parent_order_uuid, usage_state_id, usage_date, redeem_date
  from (
    select billing_id as parent_order_uuid,
        max(usage_state_id) usage_state_id,
        min(usage_date) usage_date,
        min(case when usage_state_id = 2 then substr(last_modified,1,10) end) redeem_date
    from dwh_base.vouchers
    group by billing_id
union
    select parent_order_uuid,
    max(case when customer_redeemed = 1 then 2 else 0 end) usage_state_id,
    min(customer_redeemed_at) usage_date,
    min(case when customer_redeemed = 1 then substr(updated_at,1,10) end) redeem_date
    from user_gp.camp_membership_coupons v
    join user_edwprod.fact_gbl_transactions t on t.user_uuid = v.purchaser_consumer_id  and t.order_id = v.order_id
    where t.country_id = '235'
    group by parent_order_uuid
) a
;



drop table if exists grp_gdoop_bizops_db.nvp_bt_first_txns;
create table grp_gdoop_bizops_db.nvp_bt_first_txns stored as orc as 
select 
    t.user_uuid, 
    min(case when length(min_booked_at) > 10 then substr(min_booked_at,1,10) end) min_book_date,
    min(case when usage_state_id = 2 then substr(redeem_date,1,10) end) min_redeem_date
    from 
    user_edwprod.fact_gbl_transactions t
      join nvp_bt_bookings b on t.parent_order_uuid = b.parent_order_uuid
      join nvp_bt_reds r on t.parent_order_uuid = r.parent_order_uuid
    where is_zero_amount = 0 and usage_state_id = 2
    group by t.user_uuid
;
   

insert overwrite table grp_gdoop_bizops_db.nvp_deals_tiered_market
select deal_uuid
       from dwh_base_sec_view.opportunity_1 o1
       join dwh_base_sec_view.opportunity_2 o2 on o1.id = o2.id
       where division in ('Long Island','Seattle','Detroit','Denver')
       group by deal_uuid;
   
------------------------------




drop table if exists grp_gdoop_bizops_db.nvp_bt_first_txns;
create table grp_gdoop_bizops_db.nvp_bt_first_txns stored as orc as
select
user_uuid,
min(cast(book_date as date)) min_book_date,
min(cast(redeem_date as date)) min_redeem_date
from grp_gdoop_bizops_db.rt_bt_txns
where redeemed = 1
and booked = 1
group by user_uuid
;

drop table grp_gdoop_bizops_db.nvp_bt_tiered_nam;
create table grp_gdoop_bizops_db.nvp_bt_tiered_nam stored as orc as
select
  sum(units) units,
  booked,
  redeemed,
  case when a.deal_uuid is not null then 1 else 0 end bt_eligible,
  case when cast(b.order_date as date) > cast(bt_f.min_redeem_date as date) and a.deal_uuid is not null then 1 else 0 end bt_prev_user,
  gdl.country_code country_code,
  e.economic_area,
  gdl.grt_l2_cat_name l2,
  gdl.grt_l3_cat_name l3,
  order_date,
  book_date,
  redeem_date,
  case when tie.deal_uuid is not null then 1 else 0 end tiered_market
from grp_gdoop_bizops_db.rt_bt_txns b
left join (
  select
    load_date,
    deal_uuid
  from grp_gdoop_bizops_db.sh_bt_active_deals_log
  where
    partner_inactive_flag = 0
    and product_is_active_flag = 1
    group by load_date, deal_uuid
  ) a on a.deal_uuid = b.deal_uuid and a.load_date = b.order_date
left join user_edwprod.dim_gbl_deal_lob gdl on gdl.deal_id = b.deal_uuid
left join user_groupondw.gbl_dim_country c on gdl.country_code = c.country_iso_code_2
left join user_groupondw.gbl_dim_economic_area e on c.economic_area_id = e.economic_area_id
left join grp_gdoop_bizops_db.nvp_deals_tiered_market tie on b.deal_uuid = tie.deal_uuid
left join grp_gdoop_bizops_db.nvp_bt_first_txns bt_f on b.user_uuid = bt_f.user_uuid
where
  grt_l1_cat_name = 'L1 - Local'
group by
  booked,
  redeemed,
  case when a.deal_uuid is not null then 1 else 0 end,
  gdl.country_code,
  e.economic_area,
  gdl.grt_l2_cat_name,
  gdl.grt_l3_cat_name,
  order_date,
  book_date,
  redeem_date,
  case when tie.deal_uuid is not null then 1 else 0 end,
  case when cast(b.order_date as date) > cast(bt_f.min_redeem_date as date) and a.deal_uuid is not null then 1 else 0 end
 ;

-----------------------------------

drop table grp_gdoop_bizops_db.nvp_tiered_nam_deepdive;
create table grp_gdoop_bizops_db.nvp_tiered_nam_deepdive stored as orc as
select
  sum(a.units) units,
  sum(a.bt_eligible_units)bt_eligible_units,
  sum(b.bookings_units) bookings,
  sum(c.bookings_redeemed_units) bookings_redeemed,
  sum(c.bt_eligible_txns_redeemed_units) bt_eligible_txns_redeemed,
  a.country_code,
  a.l2,
  a.l3,
  a.tiered_market,
  a.bt_prev_user
from (
  select
    sum(units) units,
    sum(case when bt_eligible = 1 then units end) bt_eligible_units,
    country_code,
    l2,
    l3,
    order_date,
    a.tiered_market,
    bt_prev_user
  from grp_gdoop_bizops_db.nvp_bt_tiered_nam a
  where cast(order_date as date) between cast('2020-10-01' as date) and cast('2020-12-31' as date)
  group by country_code, l2, l3, order_date,a.tiered_market,a.bt_prev_user) a
left join (
  select
    sum(case when booked = 1 then units end) bookings_units,
    country_code,
    l2,
    l3,
    book_date,
    a.tiered_market,
    bt_prev_user
  from grp_gdoop_bizops_db.nvp_bt_tiered_nam a
    where cast(book_date as date) between cast('2020-10-01' as date) and cast('2020-12-31' as date)
  group by country_code,l2, l3, book_date, a.tiered_market, a.bt_prev_user) b
  on a.l2 = b.l2 and a.l3 = b.l3 and a.country_code = b.country_code and a.tiered_market = b.tiered_market and a.order_date = b.book_date and a.bt_prev_user = b.bt_prev_user
left join (
  select
    sum(case when bt_eligible = 1 and booked = 1 then units end) bookings_redeemed_units,
    sum(case when bt_eligible = 1 then units end) bt_eligible_txns_redeemed_units,
    country_code,
    l2,
    l3,
    redeem_date,
    a.tiered_market, 
    bt_prev_user
  from grp_gdoop_bizops_db.nvp_bt_tiered_nam a
  where cast(redeem_date as date) between cast('2020-10-01' as date) and cast('2020-12-31' as date)
  group by country_code,l2, l3, redeem_date, tiered_market, bt_prev_user) c
  on a.l2 = c.l2 and a.l3 = c.l3 and a.country_code = c.country_code and a.tiered_market = c.tiered_market and a.order_date = c.redeem_date and a.bt_prev_user = c.bt_prev_user
group by a.country_code, a.tiered_market, a.l2, a.l3, a.bt_prev_user;


------------------------------------------------------------Supply info



drop table grp_gdoop_bizops_db.nvp_tiered_nam_sup_com;
create table grp_gdoop_bizops_db.nvp_tiered_nam_sup_com stored as orc as
select
    date_sub(next_day(ad.load_date, 'MON'), 1) week_end,
    count(distinct case when g.deal_uuid is not null then g.deal_uuid end) bt_eligible,
    count(distinct ad.deal_uuid) all_deals,
    gdl.grt_l2_cat_name l2,
    gdl.grt_l3_cat_name l3,
    gdl.country_code,
    case when tie.deal_uuid is not null then 1 else 0 end tiered_market
from (
    SELECT
        deal_uuid,
        load_date
   from prod_groupondw.active_deals
    WHERE
        sold_out = 'false'
        and available_qty > 0
        and load_date >= cast('2019-01-01' as date)
    group by deal_uuid, load_date) ad
left join (
    SELECT
        load_date,
        deal_uuid
    from grp_gdoop_bizops_db.sh_bt_active_deals_log
    WHERE
        partner_inactive_flag = 0
        and product_is_active_flag = 1
        and load_date >= cast('2019-01-01' as date)
    group by load_date, deal_uuid) g
    on g.deal_uuid = ad.deal_uuid
    and ad.load_date = g.load_date
left join user_edwprod.dim_gbl_deal_lob gdl on gdl.deal_id = ad.deal_uuid
left join user_groupondw.gbl_dim_country c on gdl.country_code = c.country_iso_code_2
left join user_groupondw.gbl_dim_economic_area e on c.economic_area_id = e.economic_area_id
left join grp_gdoop_bizops_db.nvp_deals_tiered_market tie on ad.deal_uuid = tie.deal_uuid
where
     grt_l1_cat_name = 'L1 - Local'
group by
     gdl.grt_l2_cat_name,
     gdl.grt_l3_cat_name,
     gdl.country_code,
     date_sub(next_day(ad.load_date, 'MON'), 1),
     case when tie.deal_uuid is not null then 1 else 0 end;

    
drop table grp_gdoop_bizops_db.nvp_tiered_nam_sup_com2;
create table grp_gdoop_bizops_db.nvp_tiered_nam_sup_com2 stored as orc as 
select * from grp_gdoop_bizops_db.nvp_tiered_nam_sup_com where week_end in ('2020-10-04', '2021-01-03');
    
drop table grp_gdoop_bizops_db.nvp_tiered_nam_sup_dash;
create table grp_gdoop_bizops_db.nvp_tiered_nam_sup_dash stored as orc as 
select 
    a.*, 
    b.all_deals prev_all_deals, 
    b.bt_eligible prev_bt_eligible, 
    c.all_deals new_all_deals, 
    c.bt_eligible new_bt_eligible
    from 
    (select 
         sum(units) units,
         sum(bt_eligible_units)bt_eligible_units,
         sum(bookings) bookings,
         sum(bookings_redeemed) bookings_redeemed,
         sum(bt_eligible_txns_redeemed) bt_eligible_txns_redeemed,
         country_code,
         l2,
         l3,
         tiered_market
       from
       grp_gdoop_bizops_db.nvp_tiered_nam_deepdive
       group by 
         country_code, l2, l3, tiered_market) as a
    left join grp_gdoop_bizops_db.nvp_tiered_nam_sup_com as b on a.l2 = b.l2 and a.l3 = b.l3 and a.country_code = b.country_code and a.tiered_market = b.tiered_market and b.week_end = '2020-10-04'
    left join grp_gdoop_bizops_db.nvp_tiered_nam_sup_com as c on a.l2 = c.l2 and a.l3 = c.l3 and a.country_code = c.country_code and a.tiered_market = c.tiered_market and c.week_end = '2021-01-03';


    
----------------------Day wise supply comparison
drop table grp_gdoop_bizops_db.nvp_tiered_nam_sup_day;
create table grp_gdoop_bizops_db.nvp_tiered_nam_sup_day stored as orc as
select
    ad.load_date,
    count(distinct case when g.deal_uuid is not null then g.deal_uuid end) bt_eligible,
    count(distinct ad.deal_uuid) all_deals,
    gdl.grt_l2_cat_name l2,
    gdl.grt_l3_cat_name l3,
    gdl.country_code,
    case when tie.deal_uuid is not null then 1 else 0 end tiered_market
from (
    SELECT
        deal_uuid,
        load_date
   from prod_groupondw.active_deals
    WHERE
        sold_out = 'false'
        and available_qty > 0
        and load_date >= cast('2019-01-01' as date)
    group by deal_uuid, load_date) ad
left join (
    SELECT
        load_date,
        deal_uuid
    from grp_gdoop_bizops_db.sh_bt_active_deals_log
    WHERE
        partner_inactive_flag = 0
        and product_is_active_flag = 1
        and load_date >= cast('2019-01-01' as date)
    group by load_date, deal_uuid) g
    on g.deal_uuid = ad.deal_uuid
    and ad.load_date = g.load_date
left join user_edwprod.dim_gbl_deal_lob gdl on gdl.deal_id = ad.deal_uuid
left join user_groupondw.gbl_dim_country c on gdl.country_code = c.country_iso_code_2
left join user_groupondw.gbl_dim_economic_area e on c.economic_area_id = e.economic_area_id
left join grp_gdoop_bizops_db.nvp_deals_tiered_market tie on ad.deal_uuid = tie.deal_uuid
where
     grt_l1_cat_name = 'L1 - Local'
group by
     gdl.grt_l2_cat_name,
     gdl.grt_l3_cat_name,
     gdl.country_code,
     ad.load_date,
     case when tie.deal_uuid is not null then 1 else 0 end;
    
    
    
drop table grp_gdoop_bizops_db.nvp_tiered_nam_daywise;
create table grp_gdoop_bizops_db.nvp_tiered_nam_daywise stored as orc as 
select 
   a.*,
   c.bookings_redeemed_units, 
   c.bt_eligible_txns_redeemed_units,
   b.bt_eligible, 
   b.all_deals
   from
(select
    sum(units) units,
    sum(case when bt_eligible = 1 then units end) bt_eligible_units,
    country_code,
    l2,
    l3,
    order_date,
    tiered_market
  from grp_gdoop_bizops_db.nvp_bt_tiered_nam
  group by country_code, l2, l3, order_date,tiered_market) as a
left join (
  select
    sum(case when bt_eligible = 1 and booked = 1 then units end) bookings_redeemed_units,
    sum(case when bt_eligible = 1 then units end) bt_eligible_txns_redeemed_units,
    country_code,
    l2,
    l3,
    redeem_date,
    a.tiered_market
  from grp_gdoop_bizops_db.nvp_bt_tiered_nam a
  where redeem_date is not null
  group by country_code,l2, l3, redeem_date, tiered_market) c
  on a.l2 = c.l2 and a.l3 = c.l3 and a.country_code = c.country_code and a.tiered_market = c.tiered_market and a.order_date = c.redeem_date
join 
(select 
    load_date,
    bt_eligible,
    all_deals,
    l2,
    l3,
    country_code,
    tiered_market
from 
grp_gdoop_bizops_db.nvp_tiered_nam_sup_day) 
as b on a.tiered_market = b.tiered_market and b.load_date = a.order_date and a.l2 = b.l2 and a.country_code = b.country_code and a.l3 = b.l3





----------------------BT Eligible units compared to previous bt users
    
drop table grp_gdoop_bizops_db.nvp_tiered_nam_all;
create table grp_gdoop_bizops_db.nvp_tiered_nam_all stored as orc as
select
  sum(a.units) units,
  sum(a.bt_eligible_units)bt_eligible_units,
  sum(b.bookings_units) bookings,
  sum(c.bookings_redeemed_units) bookings_redeemed,
  sum(c.bt_eligible_txns_redeemed_units) bt_eligible_txns_redeemed,
  a.country_code,
  a.l2,
  a.l3,
  a.tiered_market,
  a.bt_prev_user, 
  a.order_date
from (
  select
    sum(units) units,
    sum(case when bt_eligible = 1 then units end) bt_eligible_units,
    country_code,
    l2,
    l3,
    order_date,
    a.tiered_market,
    bt_prev_user
  from grp_gdoop_bizops_db.nvp_bt_tiered_nam a
  group by country_code, l2, l3, order_date,a.tiered_market,a.bt_prev_user) a
left join (
  select
    sum(case when booked = 1 then units end) bookings_units,
    country_code,
    l2,
    l3,
    book_date,
    a.tiered_market,
    bt_prev_user
  from grp_gdoop_bizops_db.nvp_bt_tiered_nam a
  where book_date is not null
  group by country_code,l2, l3, book_date, a.tiered_market, a.bt_prev_user) b
  on a.l2 = b.l2 and a.l3 = b.l3 and a.country_code = b.country_code and a.tiered_market = b.tiered_market and a.order_date = b.book_date and a.bt_prev_user = b.bt_prev_user
left join (
  select
    sum(case when bt_eligible = 1 and booked = 1 then units end) bookings_redeemed_units,
    sum(case when bt_eligible = 1 then units end) bt_eligible_txns_redeemed_units,
    country_code,
    l2,
    l3,
    redeem_date,
    a.tiered_market, 
    bt_prev_user
  from grp_gdoop_bizops_db.nvp_bt_tiered_nam a
  where redeem_date is not null
  group by country_code,l2, l3, redeem_date, tiered_market, bt_prev_user) c
  on a.l2 = c.l2 and a.l3 = c.l3 and a.country_code = c.country_code and a.tiered_market = c.tiered_market and a.order_date = c.redeem_date and a.bt_prev_user = c.bt_prev_user
group by a.country_code, a.tiered_market, a.l2, a.l3, a.bt_prev_user, a.order_date;

