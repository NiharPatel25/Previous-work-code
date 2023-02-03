drop table if exists grp_gdoop_bizops_db.rt_bt_funnel2;
create table grp_gdoop_bizops_db.rt_bt_funnel2 stored as orc as
select
  count (distinct parent_order_uuid) txns,
  sum(units) units,
  booked,
  redeemed,
  case when a.deal_uuid is not null then 1 else 0 end bt_eligible,
  gdl.country_code country_code,
  e.economic_area,
  platform,
  grt_l2_cat_name l2,
  grt_l3_cat_name l3,
  order_date,
  book_date,
  redeem_date,
  sum(nor) nor,
  sum(nob) nob
from grp_gdoop_bizops_db.rt_bt_txns b
left join (
  select load_date, deal_uuid
  from grp_gdoop_bizops_db.sh_bt_active_deals_log
  where partner_inactive_flag = 0 and product_is_active_flag = 1
  group by load_date, deal_uuid
) a on a.deal_uuid = b.deal_uuid and a.load_date = b.order_date
left join user_edwprod.dim_gbl_deal_lob gdl on gdl.deal_id = b.deal_uuid
join user_groupondw.gbl_dim_country c on gdl.country_code = c.country_iso_code_2
join user_groupondw.gbl_dim_economic_area e on c.economic_area_id = e.economic_area_id
  where grt_l1_cat_name = 'L1 - Local'
group by
booked,
redeemed,
case when a.deal_uuid is not null then 1 else 0 end,
gdl.country_code,
e.economic_area,
platform,
grt_l2_cat_name,
grt_l3_cat_name,
order_date,
book_date,
redeem_date;



drop table grp_gdoop_bizops_db.rt_bt_reds_booked;
create table grp_gdoop_bizops_db.rt_bt_reds_booked stored as orc as
select
sum( case when bt_eligible = 1 and booked = 1 then units end) bookings_redeemed_units,
sum(case when bt_eligible = 1 then units end) bt_eligible_txns_redeemed_units,
economic_area,
country_code,
l2,
l3,
redeem_date
from grp_gdoop_bizops_db.rt_bt_funnel2 a
where redeem_date >= '2019-02-01'
and country_code is not null
group by economic_area,country_code, l2, l3, redeem_date
;


select 
   COALESCE(bookings_redeemed_units, 0) bookings_redeemed_units, 
   COALESCE(bt_eligible_txns_redeemed_units, 0) bt_eligible_txns_redeemed_units, 
   economic_area, 
   country_code,
   l2, 
   cast(redeem_date as date) redeemed_date, 
   year(cast(redeem_date as date)) year_red_date, 
   month(cast(redeem_date as date)) month_red_date
from grp_gdoop_bizops_db.rt_bt_reds_booked
where country_code = 'US';
   