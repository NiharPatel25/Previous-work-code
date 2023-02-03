
----Extracting all INFO FOR Frequency

select min(eventdate), max(eventdate) from grp_gdoop_bizops_db.nvp_agg_dealview_bh2018 limit 5;

select * from
(select * from grp_gdoop_bizops_db.nvp_purch_freq_fin3_2018
union
select * from grp_gdoop_bizops_db.nvp_purch_freq_fin3_2019) fin_ order by country_code, booked_frst_ord;

select * from
(select * from grp_gdoop_bizops_db.nvp_purch_freq_fin_bt_2018
union
select * from grp_gdoop_bizops_db.nvp_purch_freq_fin_bt_2019) fin_ order by country_code, booked_frst_ord;

select * from
(select * from grp_gdoop_bizops_db.nvp_purch_freq_fin_bt2_2018
union
select * from grp_gdoop_bizops_db.nvp_purch_freq_fin_bt2_2019) fin_ order by country_code, booked_frst_ord;


SELECT booked_frst_ord, count(distinct user_uuid), count(user_uuid) from grp_gdoop_bizops_db.nvp_all_yrs_txns where redeem_date_yr = 2018 and redeem_date_mnth = 12 and country_code <> 'US' and country_code <> 'CA' group by booked_frst_ord;
SELECT booked_frst_ord, sum(distinct_user_1) from grp_gdoop_bizops_db.nvp_purch_freq_final where year_of_purch = 2018 and month_of_purch = 12 and country_code <> 'US' and country_code <> 'CA' group by booked_frst_ord;
grp_gdoop_bizops_db.nvp_purch_freq_final

select * from grp_gdoop_bizops_db.nvp_purch_trial_one;
select booked_frst_ord, sum(distinct_users) from grp_gdoop_bizops_db.nvp_purch_trial_one where year_of_purch = 2018 and month_of_purch = 12 and country_code <> 'US' and country_code <> 'CA' group by booked_frst_ord;;

select a.booked_frst_ord, sum(a.distinct_users) from grp_gdoop_bizops_db.nvp_purch_trial_one a
	left join grp_gdoop_bizops_db.nvp_purch_trial_two b
	on a.booked_frst_ord = b.booked_frst_ord and a.l2 = b.l2 and a.year_of_purch = b.year_of_purch and a.month_of_purch = b.month_of_purch and a.country_code = b.country_code
	left join grp_gdoop_bizops_db.nvp_purch_trial_three c on a.booked_frst_ord = c.booked_frst_ord and a.l2 = c.l2 and a.year_of_purch = c.year_of_purch and a.month_of_purch = c.month_of_purch and a.country_code = c.country_code
where a.year_of_purch = 2018 and a.month_of_purch = 12 and a.country_code <> 'US' and a.country_code <> 'CA' group by a.booked_frst_ord;

select count(*) from grp_gdoop_bizops_db.nvp_purch_trial_three;

-----Deals
select * from grp_gdoop_bizops_db.nvp_local_inventory limit 5;
select '2019', count(deal_uuid), sum(bt_active) from grp_gdoop_bizops_db.bt_deals_info_2018;
select '2018', count(deal_uuid), sum(bt_active) from grp_gdoop_bizops_db.bt_deals_info_2019;

-----Quater based frequency


select
	booked_2018,
	booked_2019,
	count(distinct user_uuid) total_users,
	sum(total_orders_2018) orders_2018,
	sum(total_orders_2019) orders_2019,
	sum(total_units_2018) units_2018,
	sum(total_units_2019) units_2019
from
(select
	fina.user_uuid,
	fina.total_orders_2018,
	fina.booked_2018,
	fina.total_units_2018,
	finb.total_orders_2019,
	finb.booked_2019,
	finb.total_units_2019
from
(select
	x.user_uuid,
	count(distinct(x.parent_order_uuid)) total_orders_2018,
	sum(x.units) total_units_2018,
	max(x.booked) booked_2018
	from
	grp_gdoop_bizops_db.nvp_2018qtr x
	join
	(select a.user_uuid from grp_gdoop_bizops_db.nvp_2018qtr a
		join grp_gdoop_bizops_db.nvp_2019qtr b on a.user_uuid = b.user_uuid
		where a.redeem_date is not null and b.redeem_date is not null
		and a.redeem_date >= '2018-10-01' and a.redeem_date <= '2018-12-31'
		group by a.user_uuid) as base on x.user_uuid = base.user_uuid
	where x.country_code <> 'CA' and x.country_code <> 'US'
	group by x.user_uuid) fina
join
(select
	y.user_uuid,
	count(distinct(y.parent_order_uuid)) total_orders_2019,
	sum(y.units) total_units_2019,
	max(y.booked) booked_2019
	from
	grp_gdoop_bizops_db.nvp_2019qtr y
	join
	(select a.user_uuid from grp_gdoop_bizops_db.nvp_2018qtr a
		join grp_gdoop_bizops_db.nvp_2019qtr b on a.user_uuid = b.user_uuid
		where a.redeem_date is not null and b.redeem_date is not null
		and a.redeem_date >= '2019-10-01' and a.redeem_date <= '2019-12-31'
		group by a.user_uuid) as base on y.user_uuid = base.user_uuid
	where y.country_code <> 'CA' and y.country_code <> 'US'
	group by y.user_uuid) finb on fina.user_uuid = finb.user_uuid) fin_ group by booked_2019, booked_2018 order by booked_2018 desc, booked_2019 desc;



-------REDEEMED AND THEN PURCHASED METHOD 1



select
full_1.booked_2018,
full_1.booked_2019,
full_1.total_users_2018,
full_1.total_orders_2018,
full_1.total_units_2018,
full_2.total_users_2019,
full_2.total_orders_2019,
full_2.total_units_2019
from
(select
base.booked_2018,
base.booked_2019,
count(distinct base.user_uuid) total_users_2018,
count(distinct(x.parent_order_uuid)) total_orders_2018,
sum(x.units) total_units_2018
from
(select
	a.user_uuid, max(a.booked) booked_2018, max(b.booked) booked_2019
	from
	grp_gdoop_bizops_db.nvp_2018qtr_red a
	join grp_gdoop_bizops_db.nvp_2019qtr_red b on a.user_uuid = b.user_uuid and a.country_code <> 'US' and a.country_code <> 'CA' and a.grt_l2_cat_name = 'L2 - Food & Drink' and b.grt_l2_cat_name = 'L2 - Food & Drink' group by a.user_uuid) base
left join
	grp_gdoop_bizops_db.nvp_2018qtr as x on base.user_uuid = x.user_uuid and x.country_code <> 'CA' and x.country_code <> 'US' and x.grt_l2_cat_name = 'L2 - Food & Drink'
group by base.booked_2018, base.booked_2019) full_1
join
(select
base2.booked_2018,
base2.booked_2019,
count(distinct base2.user_uuid) total_users_2019,
count(distinct(x.parent_order_uuid)) total_orders_2019,
sum(x.units) total_units_2019
from
(select
	a.user_uuid, max(a.booked) booked_2018, max(b.booked) booked_2019
	from
	grp_gdoop_bizops_db.nvp_2018qtr_red a
	join grp_gdoop_bizops_db.nvp_2019qtr_red b on a.user_uuid = b.user_uuid and a.country_code <> 'US' and a.country_code <> 'CA' and a.grt_l2_cat_name = 'L2 - Food & Drink' and b.grt_l2_cat_name = 'L2 - Food & Drink' group by a.user_uuid) base2
left join
	grp_gdoop_bizops_db.nvp_2019qtr as x on base2.user_uuid = x.user_uuid and x.country_code <> 'CA' and x.country_code <> 'US' and x.grt_l2_cat_name = 'L2 - Food & Drink'
group by base2.booked_2018, base2.booked_2019) full_2 on full_1.booked_2018 = full_2.booked_2018 and full_1.booked_2019 = full_2.booked_2019 order by booked_2018 desc, booked_2019 desc;


select booked_2018, booked_2019, count(distinct user_uuid)
from
(select
	a.user_uuid, max(a.booked) booked_2018, max(b.booked) booked_2019
	from
	grp_gdoop_bizops_db.nvp_2018qtr_red a
	join grp_gdoop_bizops_db.nvp_2019qtr_red b on a.user_uuid = b.user_uuid and a.country_code <> 'US' and a.country_code <> 'CA' and a.grt_l2_cat_name = 'L2 - Food & Drink' and b.grt_l2_cat_name = 'L2 - Food & Drink' group by a.user_uuid) fin
group by booked_2018, booked_2019;

-------METHOD 2



select
	booked_2018,
	booked_2019,
	count(distinct user_uuid) total_users,
	sum(total_orders_2018) orders_2018,
	sum(total_orders_2019) orders_2019,
	sum(total_units_2018) units_2018,
	sum(total_units_2019) units_2019
from
(select
	fina.user_uuid,
	fina.total_orders_2018,
	fina.booked_2018,
	fina.total_units_2018,
	finb.total_orders_2019,
	finb.booked_2019,
	finb.total_units_2019
from
(select
	base.user_uuid,
	base.booked booked_2018,
	count(distinct(x.parent_order_uuid)) total_orders_2018,
	sum(x.units) total_units_2018
	from
	(select
		a.user_uuid, max(a.booked) booked
		from
		grp_gdoop_bizops_db.nvp_2018qtr_red a
		join grp_gdoop_bizops_db.nvp_2019qtr_red b on a.user_uuid = b.user_uuid and a.country_code <> 'US' and a.country_code <> 'CA' and a.grt_l2_cat_name = 'L2 - Health / Beauty / Wellness' and b.grt_l2_cat_name = 'L2 - Health / Beauty / Wellness'
		group by a.user_uuid) as base
	left join
	grp_gdoop_bizops_db.nvp_2018qtr x on x.user_uuid = base.user_uuid and x.country_code <> 'CA' and x.country_code <> 'US' and x.grt_l2_cat_name = 'L2 - Health / Beauty / Wellness'
	group by base.user_uuid, base.booked) fina
left join
(select
	base.user_uuid,
	base.booked booked_2019,
	count(distinct(y.parent_order_uuid)) total_orders_2019,
	sum(y.units) total_units_2019
	from
	(select b.user_uuid, max(b.booked) booked
		from
		grp_gdoop_bizops_db.nvp_2018qtr_red a
		join grp_gdoop_bizops_db.nvp_2019qtr_red b on a.user_uuid = b.user_uuid and a.country_code <> 'US' and a.country_code <> 'CA' and a.grt_l2_cat_name = 'L2 - Health / Beauty / Wellness' and b.grt_l2_cat_name = 'L2 - Health / Beauty / Wellness'
		group by b.user_uuid) as base
	left join grp_gdoop_bizops_db.nvp_2019qtr y on y.user_uuid = base.user_uuid and y.country_code <> 'CA' and y.country_code <> 'US' and y.grt_l2_cat_name = 'L2 - Health / Beauty / Wellness'
	group by base.user_uuid, base.booked) finb on fina.user_uuid = finb.user_uuid) fin_ group by booked_2019, booked_2018 order by booked_2018 desc, booked_2019 desc;



----VARIANCE POP


select
	count(user_uuid) total_users,
	sum(total_units_2018) total_units_2018,
	sum(total_units_2019) total_units_2019,
	var_pop(total_units_2018) variance_2018,
	var_pop(total_units_2019) variance_2019
from
(select
	fina.user_uuid,
	fina.total_orders_2018,
	fina.booked_2018,
	fina.total_units_2018,
	finb.total_orders_2019,
	finb.booked_2019,
	finb.total_units_2019
from
(select
	base.user_uuid,
	base.booked booked_2018,
	count(distinct(x.parent_order_uuid)) total_orders_2018,
	sum(x.units) total_units_2018
	from
	(select
		a.user_uuid, max(a.booked) booked
		from
		grp_gdoop_bizops_db.nvp_2018qtr_red a
		join grp_gdoop_bizops_db.nvp_2019qtr_red b on a.user_uuid = b.user_uuid and a.country_code <> 'US' and a.country_code <> 'CA'
		group by a.user_uuid) as base
	left join
	grp_gdoop_bizops_db.nvp_2018qtr x on x.user_uuid = base.user_uuid and x.country_code <> 'CA' and x.country_code <> 'US'
	group by base.user_uuid, base.booked) fina
full join
(select
	base.user_uuid,
	base.booked booked_2019,
	count(distinct(y.parent_order_uuid)) total_orders_2019,
	sum(y.units) total_units_2019
	from
	(select b.user_uuid, max(b.booked) booked
		from
		grp_gdoop_bizops_db.nvp_2018qtr_red a
		join grp_gdoop_bizops_db.nvp_2019qtr_red b on a.user_uuid = b.user_uuid and a.country_code <> 'US' and a.country_code <> 'CA'
		group by b.user_uuid) as base
	left join grp_gdoop_bizops_db.nvp_2019qtr y on y.user_uuid = base.user_uuid and y.country_code <> 'CA' and y.country_code <> 'US'
	group by base.user_uuid, base.booked) finb on fina.user_uuid = finb.user_uuid) final_ where final_.booked_2018 = 0 and final_.booked_2019 = 1;


select
	count(*)
	from
(select
		a.user_uuid, max(a.booked) booked
		from
		grp_gdoop_bizops_db.nvp_2018qtr_red a
		join grp_gdoop_bizops_db.nvp_2019qtr_red b on a.user_uuid = b.user_uuid and a.country_code <> 'US' and a.country_code <> 'CA'
		group by a.user_uuid)


----Redemption Order

select
	order_of_redemption,
	'2019' as year_of_purch,
	sum(total_units) units_redeemed,
	sum(total_booked) units_booked,
	count(distinct user_uuid) total_users,
	count(distinct case when total_booked > 0 then user_uuid end) booked_users,
	count(distinct case when total_booked = 0 then user_uuid end) non_booked_users,
	count(distinct case when total_booked > 0 and booked_during_first_booking = 1 and cast(redeem_date as date) > cast(first_booked_red as date) then user_uuid end) previously_used_bt_booked,
	count(distinct case when total_booked = 0 and booked_during_first_booking = 1 and cast(redeem_date as date) > cast(first_booked_red as date) then user_uuid end) previously_used_bt_non_booked
	from
	(select
	fina.user_uuid,
	fina.country_code,
	fina.redeem_date,
	fina.total_units,
	fina.total_booked,
	finb.first_booked_red,
	case when finb.user_uuid is not null then 1 else 0 end booked_during_first_booking,
	row_number() over(partition by fina.user_uuid order by cast(redeem_date as date) asc) order_of_redemption
	from
	(select
		x.user_uuid,
		x.country_code,
		redeem_date,
		sum(booked) total_booked,
		sum(units) total_units
	from
		grp_gdoop_bizops_db.nvp_2019qtr x
	  join
		(select a.user_uuid
		from grp_gdoop_bizops_db.nvp_2018qtr a
		join grp_gdoop_bizops_db.nvp_2019qtr b on a.user_uuid = b.user_uuid
		where a.redeem_date is not null and b.redeem_date is not null group by a.user_uuid) y on x.user_uuid = y.user_uuid
	where x.country_code <> 'US' and x.country_code <> 'CA' and redeem_date is not null group by x.user_uuid, x.country_code, x.redeem_date) as fina
	left join
	(select * from grp_gdoop_bizops_db.nvp_user_min_red group by user_uuid, first_booked_red) as finb on fina.user_uuid = finb.user_uuid) final_ group by order_of_redemption order by order_of_redemption;


select
	fina.user_uuid,
	fina.country_code,
	fina.redeem_date,
	fina.total_units,
	fina.total_booked,
	row_number() over(partition by fina.user_uuid order by cast(redeem_date as date)) order_of_redemption
	from
	(select
		x.user_uuid,
		x.country_code,
		redeem_date,
		sum(booked) total_booked,
		sum(units) total_units
	from
		grp_gdoop_bizops_db.nvp_2018qtr x
	  join
		(select a.user_uuid
		from grp_gdoop_bizops_db.nvp_2018qtr a
		join grp_gdoop_bizops_db.nvp_2019qtr b on a.user_uuid = b.user_uuid
		where a.redeem_date is not null and b.redeem_date is not null group by a.user_uuid) y on x.user_uuid = y.user_uuid
	where x.country_code <> 'US' and x.country_code <> 'CA' and redeem_date is not null and redeem_date >= '2018-10-01' and redeem_date <= '2018-12-31' group by x.user_uuid, x.country_code, x.redeem_date) fina order by order_of_redemption desc;


select * from grp_gdoop_bizops_db.rt_bt_txns where user_uuid = '426d479b-a12a-4261-b9f1-91389e2350f3' and order_date >= '2018-10-01' and order_date <= '2018-12-31';

-----Quater flow




-----
select count(user_uuid),count(distinct user_uuid), count(distinct concat(cast(user_uuid as varchar), cast(deal_uuid as varchar), cast(first_redeem_date as varchar))) number_of_redemption, count(distinct concat(cast(user_uuid as varchar), cast(first_redeem_date as varchar))) number_of_redeemer_day from grp_gdoop_bizops_db.nvp_2019_txns_one where accepted_redeemed_ord = 1 and rank_for_ordering = 1 and country_code <> 'US' and country_code <> 'CA';

select *
from
(select user_uuid, count(user_uuid) count_ from grp_gdoop_bizops_db.nvp_2019_txns_one where accepted_redeemed_ord = 1 and rank_for_ordering = 1 and country_code <> 'US' and country_code <> 'CA' group by user_uuid) fin_
where count_ > 1

select * from (select user_uuid, count(user_uuid) count_ from grp_gdoop_bizops_db.nvp_2019_txns where country_code <> 'US' and country_code <> 'CA' group by user_uuid) where count_ >1;

select count(user_uuid), count(distinct user_uuid) from grp_gdoop_bizops_db.nvp_2018_txns where country_code <> 'US' and country_code <> 'CA';


select * from grp_gdoop_bizops_db.nvp_2019_all order by user_uuid, eventdate, deal_uuid limit 10000;
select * from grp_gdoop_bizops_db.nvp_2019_all where user_uuid = 'a573dd85-a973-4a8d-a4f5-550546f693ba' order by eventdate;

select count(distinct concat(eventdate, user_uuid, deal_uuid)) from grp_gdoop_bizops_db.nvp_2019_all where user_uuid = 'a573dd85-a973-4a8d-a4f5-550546f693ba' and deal_uuid is not null;


select * from prod_groupondw.user_entity_attribute_02 where consumer_id = 'a573dd85-a973-4a8d-a4f5-550546f693ba';







--------TRASH
select * from grp_gdoop_bizops_db.nvp_agg_dealview_bh2018;
select * from grp_gdoop_bizops_db.nvp_all_yrs_txns where country_code = 'US' order by user_uuid;

select * from grp_gdoop_bizops_db.nvp_2019three x
left join
grp_gdoop_bizops_db.rt_bt_inventory y on x.deal_uuid = y.deal_id where deal_id is null;

select * from grp_gdoop_bizops_db.rt_bt_inventory where deal_id = '501bd116-afde-4e33-b94b-d12a31d21ddd'

select
	a.country_code,
	a.booked_frst_ord,
	a.l2,
	'2019' as year_of_purch,
	count(distinct a.user_uuid) distinct_users,
	count(distinct concat_ws(b.eventdate, b.user_uuid)) uv,
	count(distinct case when deal_uuid is not null then concat_ws(b.eventdate, b.user_uuid) end) udvv,
	count(distinct case when deal_uuid is not null then concat_ws(b.eventdate, b.user_uuid, b.deal_uuid) end) udv
from grp_gdoop_bizops_db.nvp_2019_txns a
left join grp_gdoop_bizops_db.nvp_2019_all b on a.user_uuid = b.user_uuid
where a.country_code <> 'US' and a.country_code <> 'CA'
group by a.country_code, a.booked_frst_ord, a.l2;


select * from grp_gdoop_bizops_db.rt_bt_txns limit 5;
select * from grp_gdoop_bizops_db.rt_bt_inventory;


select * from grp_gdoop_bizops_db.nvp_2018three;
select * from grp_gdoop_bizops_db.nvp_bt_first_txns;

select * from grp_gdoop_bizops_db.nvp_purch_freq_fin;
select * from grp_gdoop_bizops_db.nvp_2019_txns;
select * from grp_gdoop_bizops_db.nvp_2018three;
select * from grp_gdoop_bizops_db.nvp_2019_txns_one where user_uuid = '8e68316a-6c48-4c10-bd13-b865d9283531';
select * from grp_gdoop_bizops_db.nvp_agg_dealview_bh2019;

select * from grp_gdoop_bizops_db.nvp_2019_all;


-----
select * from sandbox.rev_mgmt_deal_attributes;

select * from grp_gdoop_revenue_management_db.rev_mgmt_deal_attributes;
