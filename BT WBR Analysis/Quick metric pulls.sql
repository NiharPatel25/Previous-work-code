-----DEEP Dive WBR Numbers

use grp_gdoop_bizops_db;
drop table grp_gdoop_bizops_db.nvp_bt_funnel2;
create table grp_gdoop_bizops_db.nvp_bt_funnel2 stored as orc as
select
  sum(units) units,
  booked,
  redeemed,
  case when a.deal_uuid is not null then 1 else 0 end bt_eligible,
  b.deal_uuid,
  gdl.country_code country_code,
  e.economic_area,
  grt_l2_cat_name l2,
  order_date,
  book_date,
  redeem_date,
  date_sub(next_day(order_date, 'MON'), 1) week_end_ord, 
  date_sub(next_day(redeem_date, 'MON'), 1) week_end_red,
  date_sub(next_day(book_date, 'MON'), 1) week_end_book
from grp_gdoop_bizops_db.rt_bt_txns b
left join (
  select load_date, deal_uuid
  from grp_gdoop_bizops_db.sh_bt_active_deals_log
  where 
        partner_inactive_flag = 0 
        and product_is_active_flag = 1
        and load_date >= '2020-08-01'
  group by load_date, deal_uuid
) a on a.deal_uuid = b.deal_uuid and a.load_date = b.order_date
left join user_edwprod.dim_gbl_deal_lob gdl on gdl.deal_id = b.deal_uuid
join user_groupondw.gbl_dim_country c on gdl.country_code = c.country_iso_code_2
join user_groupondw.gbl_dim_economic_area e on c.economic_area_id = e.economic_area_id
  where grt_l1_cat_name = 'L1 - Local' and order_date >= '2021-01-01'
group by
booked,
redeemed,
case when a.deal_uuid is not null then 1 else 0 end,
gdl.country_code,
e.economic_area,
grt_l2_cat_name,
order_date,
book_date,
redeem_date, 
b.deal_uuid,
date_sub(next_day(order_date, 'MON'), 1),
date_sub(next_day(redeem_date, 'MON'), 1),
date_sub(next_day(book_date, 'MON'), 1)
;

select 
   a.deal_uuid, 
   a.units_this_week, 
   b.units_last_week
   from
(select 
   deal_uuid,
   sum(units) units_this_week
from grp_gdoop_bizops_db.nvp_bt_funnel2
where week_end_ord = cast('2021-03-14' as date)
and country_code = 'US'
and l2 = 'L2 - Things to Do - Leisure'
group by deal_uuid) as a 
left join 
(select 
   deal_uuid,
   sum(units) units_last_week
from grp_gdoop_bizops_db.nvp_bt_funnel2
where week_end_ord = cast('2021-03-07' as date)
and country_code = 'US'
and l2 = 'L2 - Things to Do - Leisure'
group by deal_uuid) as b on a.deal_uuid = b.deal_uuid

select * from grp_gdoop_bizops_db.nvp_bt_funnel2;


drop table grp_gdoop_bizops_db.nvp_bookedbt_deals_trial;
create table grp_gdoop_bizops_db.nvp_bookedbt_deals_trial stored as orc as
select 
   date_sub(next_day(b.book_date, 'MON'), 1) book_date, 
   b.deal_uuid,
   xyz.state,
   xyz.market,
   sum(b.units) total_units
from 
    grp_gdoop_bizops_db.nvp_bt_funnel2 as b
left join
(select 
deal_id, 
account_name, 
state, 
mkt_permalink, 
market
from
grp_gdoop_revenue_management_db.rev_mgmt_deal_attributes
where grt_l1_cat_name = 'L1 - Local') as xyz on b.deal_uuid = xyz.deal_id 
where b.country_code = 'US'
group by b.deal_uuid, date_sub(next_day(b.book_date, 'MON'), 1), xyz.state, xyz.market;

select * from grp_gdoop_bizops_db.nvp_bt_funnel2;

create table grp_gdoop_bizops_db.nvp_bt_rebooking_miss_fin (
    user_uuid string,
    merchant_uuid string,
    country_name string,
    order_date string,
    possible_inv_product_uuid string,
    possible_merch_product_uuid string,
    possible_offer_description string
) partitioned by (dt string)
stored as orc
tblproperties ("orc.compress"="SNAPPY");

use grp_gdoop_bizops_db;
drop table nvp_bt_funnel2;
create table nvp_bt_funnel2 stored as orc as
select
  sum(units) units,
  sum(case when extra.user_uuid is not null then units else 0 end) extra_missing_rebooked,
  booked,
  redeemed,
  case when a.deal_uuid is not null then 1 else 0 end bt_eligible,
  b.deal_uuid,
  gdl.country_code country_code,
  e.economic_area,
  grt_l2_cat_name l2,
  b.order_date,
  book_date,
  redeem_date
from rt_bt_txns b
left join (
  select load_date, deal_uuid
  from grp_gdoop_bizops_db.sh_bt_active_deals_log
  where partner_inactive_flag = 0 and product_is_active_flag = 1
  group by load_date, deal_uuid
) a on a.deal_uuid = b.deal_uuid and a.load_date = b.order_date
left join user_edwprod.dim_gbl_deal_lob gdl on gdl.deal_id = b.deal_uuid
left join (select distinct user_uuid, order_date from grp_gdoop_bizops_db.nvp_bt_rebooking_miss_fin) extra on b.user_uuid = extra.user_uuid and b.order_date = extra.order_date
join user_groupondw.gbl_dim_country c on gdl.country_code = c.country_iso_code_2
join user_groupondw.gbl_dim_economic_area e on c.economic_area_id = e.economic_area_id
  where grt_l1_cat_name = 'L1 - Local' and b.order_date >= '2020-01-01'
group by
booked,
redeemed,
case when a.deal_uuid is not null then 1 else 0 end,
gdl.country_code,
e.economic_area,
grt_l2_cat_name,
b.order_date,
book_date,
redeem_date, 
b.deal_uuid
;
drop table grp_gdoop_bizops_db.nvp_bookedbt_deals_trial;
create table grp_gdoop_bizops_db.nvp_bookedbt_deals_trial stored as orc as
select 
   date_sub(next_day(book_date, 'MON'), 1) book_date, 
   deal_uuid,
   country_code, 
   sum(units) total_units, 
   sum(extra_missing_rebooked) missingbooked
   from 
 grp_gdoop_bizops_db.nvp_bt_funnel2
group by deal_uuid, date_sub(next_day(book_date, 'MON'), 1), country_code;


select * from grp_gdoop_bizops_db.nvp_bookedbt_deals_trial;



-----LOCATION WISE Booked Units
use grp_gdoop_bizops_db;
drop table nvp_bt_funnel2;
create table nvp_bt_funnel2 stored as orc as
select
  sum(units) units,
  booked,
  redeemed,
  gdl.country_code country_code,
  xyz.market,
  e.economic_area,
  grt_l2_cat_name l2,
  order_date,
  book_date,
  redeem_date, 
  xyz.state
from rt_bt_txns b
left join user_edwprod.dim_gbl_deal_lob gdl on gdl.deal_id = b.deal_uuid
left join
	(select 
				deal_id, 
				account_name, 
				state, 
				mkt_permalink, 
				market
				from 
				grp_gdoop_revenue_management_db.rev_mgmt_deal_attributes
				where grt_l1_cat_name = 'L1 - Local') xyz on b.deal_uuid = xyz.deal_id 
join user_groupondw.gbl_dim_country c on gdl.country_code = c.country_iso_code_2
join user_groupondw.gbl_dim_economic_area e on c.economic_area_id = e.economic_area_id
  where grt_l1_cat_name = 'L1 - Local' and b.country_code = 'US'
group by
booked,
redeemed,
gdl.country_code,
e.economic_area,
grt_l2_cat_name,
order_date,
book_date,
redeem_date,
xyz.market, 
xyz.state;

select sum(units) units,
sum(case when booked = 1 then units end) booked_units,
market,
state, 
country_code,
order_date, 
week_of_year_num, 
month_of_year_num, 
quarter_of_year_num, 
b.year_key
from nvp_bt_funnel2 a
  join prod_groupondw.dim_day b on b.day_rw = a.order_date
  join prod_groupondw.dim_week c on b.week_key = c.week_key
  join prod_groupondw.dim_month d on b.month_key = d.month_key
  join prod_groupondw.dim_quarter e on b.quarter_key = e.quarter_key
  where c.year_key in (2019, 2020)
  group by country_code, order_date, week_of_year_num, month_of_year_num, quarter_of_year_num, b.year_key, market , state order by year_key desc, month_of_year_num desc, week_of_year_num desc;
  
-------TRASH LOCATION WISE
select 
	deal_uuid, 
	state, 
	count(distinct parent_order_uuid) count_
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
				where cast(order_date as date) >= cast('2020-06-07' as date) and cast(order_date as date) <= cast('2020-06-15' as date) 
				and country_code in ('US', 'CA')
				and booked = 1) a 		
	join 
	(select 
				deal_id, 
				account_name, 
				state, 
				mkt_permalink, 
				market
				from 
				grp_gdoop_revenue_management_db.rev_mgmt_deal_attributes
				where grt_l2_cat_name = 'L2 - Health / Beauty / Wellness') b on a.deal_uuid = b.deal_id 
	group by deal_uuid, state
	order by count_ desc;
	


select 
	state, 
	count(distinct parent_order_uuid) count_
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
				where cast(order_date as date) >= cast('2020-06-07' as date) and cast(order_date as date) <= cast('2020-06-15' as date) 
				and country_code in ('US', 'CA')
				and booked = 1) a 		
	join 
	(select 
				deal_id, 
				account_name, 
				state, 
				mkt_permalink, 
				market
				from 
				grp_gdoop_revenue_management_db.rev_mgmt_deal_attributes
				) b on a.deal_uuid = b.deal_id 
	group by state
	order by count_ desc;


select * from grp_gdoop_revenue_management_db.rev_mgmt_deal_attributes;


select 
				user_uuid, 
				country_code, 
				parent_order_uuid, 
				deal_uuid, 
				cast(order_date as date) order_date, 
				units, 
				nob
			from grp_gdoop_bizops_db.rt_bt_txns 
				where cast(order_date as date) >= cast('2020-06-07' as date) and cast(order_date as date) <= cast('2020-06-15' as date) 
				and country_code in ('US', 'CA');