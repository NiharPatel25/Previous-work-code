create table grp_gdoop_bizops_db.np_bt_orders stored as orc as
select * from(
select 
			x.parent_order_uuid parent_order_uuid, 
			x.order_uuid order_uuid,
			x.order_id order_id, 
			x.order_date order_date, 
			x.unified_deal_option_id deal_id,
			x.deal_uuid deal_uuid, 
			x.user_uuid fgt_user_uuid, 
			x.country_id country_id, 
			z.country country_code,
			case when z.is_bookable = 1 and z.partner_inactive_flag = 0 and z.product_is_active_flag = 1 then 1 else 0 end is_bt_purc,
			z.deal_uuid bookable
			from user_edwprod.fact_gbl_transactions x
			left join user_groupondw.gbl_dim_country y on x.country_id = y.country_key
			inner join grp_gdoop_bizops_db.sh_bt_active_deals_log z on x.deal_uuid = z.deal_uuid and cast(x.order_date as date) = cast(z.load_date as date) and y.country_iso_code_2 = z.country
			where x.order_date >= '2020-01-01' and x.order_date <'2020-02-01' and x.action = 'authorize' and z.load_date >= '2020-01-01'
		) as uni;
	
drop table grp_gdoop_bizops_db.np_bt_orders;

create table grp_gdoop_bizops_db.np_cancelled stored as orc as
select * from(
select 
			parent_order_uuid parent_order_uuid, 
			order_uuid order_uuid,
			order_id order_id, 
			is_canceled, 
			is_order_canceled
			from user_edwprod.fact_gbl_transactions
			where is_canceled = 1 or is_order_canceled = 1 and order_date >= '2020-01-01'
		) as uni;



drop table grp_gdoop_bizops_db.np_cancelled;
/* BT Backbone table*/

drop table nihpatel_db.np_bt_cancel_cnt2;

create table np_bt_cancel_cnt2 stored as orc as
select * from 
(select 
	fgt.country_id country_id,
	fgt.country_code country_code,
	fgt.parent_order_uuid parent_order_uuid,
	fgt.order_uuid order_uuid,
	fgt.order_date order_date,
	fgt.bookable bookable, 
	fgt.is_bookable is_bookable, 
	fgt.partner_inactive_flag partner_inactive_flag, 
	fgt.product_is_active_flag product_is_active_flag,
	fgt.deal_uuid deal_uuid,
	fgt.user_uuid user_uuid,
	b.voucher_code voucher_cd, 
	b.security_code security_cd, 
	b.merchant_uuid merchant_uuid, 
	b.user_uuid user_uuid2, 
	b.checked_in checked_in, 
	b.booked_by booked_by, 
	b.cancelled_by cancelled_by,
	b.participants_per_coupon participant_unit, 
	b.start_time start_time, 
	b.end_time end_time, 
	b.created_at created_at, 
	b.deleted_at deleted_at, 
	b.state state
	from
	(select 
	        parent_order_uuid, 
			order_uuid,
			order_date, 
			deal_id,
			deal_uuid,
			fgt_user_uuid user_uuid, 
			country_id, 
			country_code,
			bookable, 
			is_bookable, 
			partner_inactive_flag, 
			product_is_active_flag
			from grp_gdoop_bizops_db.np_bt_orders where country_id<>235 and country_id <> 40
			) as fgt
	left join 
	(select 
		voucher_code, 
		security_code,
		billing_id parent_order_uuid,
		deal_id deal_id, 
		country_id
		from dwh_base.vouchers where date(created_at)>='2020-01-01' and country_id <> 235 and country_id <> 40
	) as v on fgt.parent_order_uuid = v.parent_order_uuid and fgt.deal_id = v.deal_id and fgt.country_id = v.country_id
	left join 
	(select 
		merchant_uuid, 
		voucher_code,
		security_code, 
		deal_uuid, 
		booking_id,
		country_id,
		user_uuid,
		checked_in,
		participants_per_coupon,
		booked_by booked_by, 
		cancelled_by,
		start_time, 
		end_time, 
		created_at, 
		deleted_at,
		state
		from grp_gdoop_bizops_db.sh_bt_bookings_rebuild where date(created_at) >= '2020-01-01' and country_id <> 'US' and country_id <> 'CA'
	) as b on v.voucher_code = b.voucher_code and v.security_code = b.security_code
 UNION
	select 
		fgt2.country_id country_id,
		fgt2.country_code country_code,
		fgt2.parent_order_uuid parent_order_uuid,
		fgt2.order_uuid order_uuid,
		fgt2.order_date order_date,
		fgt2.bookable bookable, 
		fgt2.is_bookable is_bookable, 
		fgt2.partner_inactive_flag partner_inactive_flag, 
		fgt2.product_is_active_flag product_is_active_flag,
		fgt2.deal_uuid deal_uuid,
		fgt2.user_uuid user_uuid,
		b2.voucher_code voucher_cd, 
		b2.security_code security_cd, 
		b2.merchant_uuid merchant_uuid,
		b2.user_uuid user_uuid2, 
		b2.checked_in checked_in, 
		b2.booked_by booked_by, 
		b2.cancelled_by cancelled_by,
		b2.participants_per_coupon participant_unit, 
		b2.start_time start_time, 
		b2.end_time end_time, 
		b2.created_at created_at, 
		b2.deleted_at deleted_at, 
		b2.state state
	from
		(select 
			parent_order_uuid, 
			order_uuid,
			order_id,
			order_date, 
			deal_id,
			deal_uuid,
			fgt_user_uuid user_uuid, 
			country_id, 
			country_code,
			bookable, 
			is_bookable, 
			partner_inactive_flag, 
			product_is_active_flag
			from grp_gdoop_bizops_db.np_bt_orders where country_id = 235) as fgt2 
		left join 
		(select 
			code as voucher_code, 
			purchaser_consumer_id purchaser_consumer_id,
			order_id order_id
			from user_gp.camp_membership_coupons where date(created_at)>='2020-01-01' 
		) as v2 on fgt2.user_uuid = v2.purchaser_consumer_id  and fgt2.order_id = v2.order_id 
		left join 
		(select 
			merchant_uuid, 
			voucher_code,
			security_code, 
			deal_uuid, 
			booking_id,
			country_id,
			user_uuid,
			checked_in,
			participants_per_coupon,
			booked_by booked_by, 
			cancelled_by cancelled_by,
			start_time, 
			end_time, 
			created_at, 
			deleted_at,
			state
			from grp_gdoop_bizops_db.sh_bt_bookings_rebuild where date(created_at) >= '2020-01-01' and country_id = 'US'
		) as b2 on v2.voucher_code = b2.voucher_code and v2.purchaser_consumer_id = b2.user_uuid
) as uni
;

select * from user_gp.camp_membership_coupons limit 5;

		
/*BACKBONE*/

drop table grp_gdoop_bizops_db.np_bt_backbone2;

create table grp_gdoop_bizops_db.np_bt_backbone2 stored as orc as
select * from 
(select b.*, c.statec mx_state, c.created_atc mx_created_at, c.cancelled_by cancelled_by, c.mx_start_time mx_start_time, e.booked_by booked_by, e.created_ate mn_created_at, e.mn_start_time mn_start_time, f.created_atf first_ord_date ,d.l1, d.l2, d.l3, can.is_canceled is_canceled, can.is_order_canceled is_order_canceled
from 
	(select a.parent_order_uuid parent_order_uuid, 
			a.order_uuid order_uuid, 
			a.order_date order_Date, 
			a.deal_uuid deal_uuid, 
			a.voucher_cd voucher_cd, 
			a.security_cd security_cd, 
			a.merchant_uuid merchant_uuid, 
			a.is_bookable is_bookable, 
			a.partner_inactive_flag partner_inactive_flag, 
			a.product_is_active_flag product_is_active_flag, 
			a.user_uuid user_uuid,
			a.country_code country_code,
			max(a.row_num) mx, 
			min(a.row_num) mn,
			min(order_rank) first_ord,
			sum(merchant_cancel) merchant_cancel
	from
				(select 
					 country_code, 
					 merchant_uuid,
					 parent_order_uuid, 
					 order_uuid,
					 order_date, 
					 deal_uuid, 
					 voucher_cd, 
					 security_cd, 
					 is_bookable, 
					 partner_inactive_flag, 
					 product_is_active_flag,
					 user_uuid,
					 ROW_NUMBER() over(partition by parent_order_uuid, voucher_cd, security_cd order by created_at asc) as row_num, 
					 ROW_NUMBER() over(partition by parent_order_uuid order by created_at asc) as order_rank,
					 case when cancelled_by = 'merchant' then 1 else 0 end merchant_cancel
				from nihpatel_db.np_bt_cancel_cnt2) as a
				group by a.parent_order_uuid, a.order_uuid, a.order_date, a.deal_uuid, a.voucher_cd, a.security_cd, a.merchant_uuid, a.is_bookable, a.partner_inactive_flag, a.product_is_active_flag, a.user_uuid, a.country_code) as b
	left join 
			(select 
					 parent_order_uuid parent_order_uuidc,
					 voucher_cd voucher_cdc, 
					 security_cd security_cdc, 
					 created_at created_atc,
					 state statec,
					 cancelled_by cancelled_by,
					 start_time mx_start_time,
					 ROW_NUMBER() over(partition by parent_order_uuid, voucher_cd, security_cd order by created_at asc) as row_numc 
				from nihpatel_db.np_bt_cancel_cnt2) as c
			on b.voucher_cd = c.voucher_cdc and b.security_cd = c.security_cdc and b.parent_order_uuid = c.parent_order_uuidc and b.mx = c.row_numc
	left join 
			(select 
					parent_order_uuid parent_order_uuide, 
					voucher_cd voucher_cde, 
					 security_cd security_cde, 
					 created_at created_ate,
					 booked_by booked_by,
					 start_time mn_start_time,
					 ROW_NUMBER() over(partition by parent_order_uuid, voucher_cd, security_cd order by created_at asc) as row_nume
				from nihpatel_db.np_bt_cancel_cnt2) as e
			on b.voucher_cd = e.voucher_cde and b.security_cd = e.security_cde and b.parent_order_uuid = e.parent_order_uuide and b.mn = e.row_nume
	left join 
			(select 
					 parent_order_uuid parent_order_uuidf,
					 order_uuid order_uuidf,
					 created_at created_atf,
					 ROW_NUMBER() over(partition by parent_order_uuid order by created_at asc) as order_rankf
				from nihpatel_db.np_bt_cancel_cnt2) as f
			on b.parent_order_uuid = f.parent_order_uuidf and b.order_uuid = f.order_uuidf and b.first_ord = f.order_rankf
	left join 
			(select 
					deal_id deal_uuid, 
					country_code country_code,  
					country_id country_id,
					grt_l1_cat_name l1, 
					grt_l2_cat_name l2, 
					grt_l3_cat_name l3
			from user_edwprod.dim_gbl_deal_lob
			) as d 
			on b.deal_uuid = d.deal_uuid and b.country_code = d.country_code  
	left join 
			(select 
				parent_order_uuid, 
				order_uuid, 
				is_canceled, 
				is_order_canceled
				from grp_gdoop_bizops_db.np_cancelled 
			) as can on b.parent_order_uuid = can.parent_order_uuid and b.order_uuid = can.order_uuid
) as uni;



select parent_order_uuid, order_uuid, count(*) from grp_gdoop_bizops_db.np_cancelled group by parent_order_uuid, order_uuid;
-------------------------------------------------------------------------------------------------------

drop table grp_gdoop_bizops_db.np_bt_rebooking_trend;

create table grp_gdoop_bizops_db.np_bt_rebooking_trend stored as orc as
select a.*, b.parent_orderx, b.order_uuidx, c.l1, c.l2, c.l3
from 
(select * from nihpatel_db.np_bt_cancel_cnt2) as a 
left join 
(select 
		parent_order_uuid parent_orderx, 
		order_uuid order_uuidx
		from grp_gdoop_bizops_db.np_cancelled 
) as b on a.parent_order_uuid = b.parent_orderx and a.order_uuid = b.order_uuidx
left join
(select 
		deal_id deal_uuid, 
		country_code country_code,  
		country_id country_id,
		grt_l1_cat_name l1, 
		grt_l2_cat_name l2, 
		grt_l3_cat_name l3
from user_edwprod.dim_gbl_deal_lob
) as c
on a.deal_uuid = c.deal_uuid and a.country_code = c.country_code ;



--------------------------------------------------- Engagement of Filter, 

create table m_bt_filter_eng stored as orc as
select platform, eventdate, clicktype, count(*) as clicks
from user_groupondw.m_raw_click
where eventdate between '2020-02-07' and '2020-02-14'
  and lower(platform) in ('iphone', 'android') --android or iphone
  and countrycode in ('UK')
  and clicktype in ('booking_date_filter_option', 'booking_time_filter_option') 
group by  platform, eventdate, clicktype; 

drop table m_bt_filter_eng;

create table m_bt_filter_eng stored as orc as
select 
country_code, 
country_id, 
economic_area, 
extrainfo,
get_json_object(extrainfo,'$.option_name') as obj
from user_groupondw.m_raw_click
where eventdate between '2020-02-07' and '2020-02-14' 
and countrycode in ('UK') 
and clicktype in ('booking_date_filter_option', 'booking_time_filter_option');

create table np_bt_filter_eng stored as orc as
select *, get_json_object(extrainfo,'$.option_name') as obj from m_bt_filter_eng; 

select * From user_groupondw.m_raw_click limit 5;

create table np_traffic stored as orc as
select report_date, sub_platform, uniq_visitors, bounce_visitors
from edwprod.agg_gbl_traffic 
where report_date between '2020-02-07' and '2020-02-14';

drop table np_traffic;
create table np_traffic stored as orc as
select event_date, cookie_first_sub_platform, count(distinct cookie_b)
from user_groupondw.gbl_traffic_superfunnel
where cookie_first_country_code = 'UK' and event_date between '2020-02-07' and '2020-02-14'
group by event_date, cookie_first_sub_platform;




select * from user_groupondw.m_raw_click where eventdate between '2020-02-07' and '2020-02-14' limit 5;



-------------------------------------------------- PURCHASE FREQUENCY
drop table grp_gdoop_bizops_db.nvp_bt_txns;

create table grp_gdoop_bizops_db.nvp_bt_txns stored as orc as
select 
	user_uuid, 
	parent_order_uuid, 
	country_code, 
	platform, 
	booked, 
	redeemed, 
	is_refunded, 
	is_expired, 
	ROW_NUMBER() over(partition by user_uuid, year(order_date), month(order_date) order by order_date) month_rank,
	ROW_NUMBER() over(partition by user_uuid order by order_date) overall_rank
from 
	(select 
		user_uuid user_uuid, 
		parent_order_uuid parent_order_uuid, 
		country_code, 
		platform, 
		booked, 
		redeemed, 
		is_refunded, 
		is_expired, 
		cast(order_date as date) order_date
	from grp_gdoop_bizops_db.rt_bt_txns where order_date >= '2019-01-01') as a;


drop table grp_gdoop_bizops_db.nvp_uniq_user;
create table grp_gdoop_bizops_db.nvp_uniq_user stored as orc as
select 
	t.user_uuid user_uuid, 
	year(cast(order_date as date)) year_,
	month(cast(order_date as date)) month_,
	t.country_code country_cd,
	min(cast(order_date as date)) months_frst_ord,
	min(t.booked) min_booked,
	max(t.booked) max_booked, 
	sum(t.nob) sum_nob, 
	sum(t.nor) sum_nor, 
	sum(t.units),
	min(gdl.grt_l2_cat_name) l2,
	count(*)
	from grp_gdoop_bizops_db.rt_bt_txns as t
	left join user_edwprod.dim_gbl_deal_lob gdl on gdl.deal_id = t.deal_uuid and gdl.country_code = t.country_code
where month(t.order_date) = 1 or month(t.order_date) = 2 and year(t.order_date) = 2019 or year(t.order_date) = 2020
group by user_uuid, year(cast(order_date as date)), month(cast(order_date as date)), t.country_code;


drop table grp_gdoop_bizops_db.nvp_uniq_user;
create table grp_gdoop_bizops_db.nvp_uniq_user stored as orc as
select 
	year(cast(order_date as date)) as purchase_year,
	month(cast(order_date as date)) as purchase_month, 
	country_code as country_code,
	platform as platform,
	count(distinct user_uuid) as customers, 
	count(parent_order_uuid) as orders_purchased,
	sum(units) as total_units, 
	sum(booked) as orders_booked, 
	sum(redeemed) as total_redemptions,
	sum(is_refunded) as total_refunds,
	sum(nob) as sum_nob,
	sum(nor) as sum_nor
from grp_gdoop_bizops_db.rt_bt_txns
	where cast(order_date as date) >= '2019-01-01'
	group by year(cast(order_date as date)), month(cast(order_date as date)), country_code, platform;



create table grp_gdoop_bizops_db.nvp_uniq_user_cat stored as orc as
select 
	year(cast(t.order_date as date)) as purchase_year,
	month(cast(t.order_date as date)) as purchase_month, 
	t.country_code as country_code,
	t.platform as platform,
	min(gdl.l2) l2,
	count(distinct t.user_uuid) as customers, 
	count(t.parent_order_uuid) as orders_purchased,
	sum(t.units) as total_units, 
	sum(t.booked) as orders_booked, 
	sum(t.redeemed) as total_redemptions,
	sum(t.is_refunded) as total_refunds,
	sum(t.nob) as sum_nob,
	sum(t.nor) as sum_nor
from 
	(select 
		user_uuid, 
		deal_uuid,
		order_date, 
		country_code country_code, 
		platform, 
		parent_order_uuid, 
		units, 
		booked, 
		redeemed, 
		is_refunded, 
		nob, 
		nor
	from
		grp_gdoop_bizops_db.rt_bt_txns
		where cast(order_date as date) >= '2019-01-01') as t
    join 
	(select deal_id, 
	    grt_l2_cat_name l2, 
	    country_code country_cd 
	    from user_edwprod.dim_gbl_deal_lob) gdl on gdl.deal_id = t.deal_uuid and gdl.country_cd = t.country_code
	group by year(cast(t.order_date as date)), month(cast(t.order_date as date)), l2 ,country_code, platform;
