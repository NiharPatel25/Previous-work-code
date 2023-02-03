/* creating table */

/* MAIN QUERY*/

/* creating table : np_bt_cancel_cnt : Modify this one before setting it up on Optimus */

select * from grp_gdoop_bizops_db.np_bt_cancel_cnt limit 5;

select * from dwh_base.vouchers limit 5;

create table np_bt_cancel_cnt stored as orc as
select * from 
(select 
	fgt.country_id country_id,
	fgt.order_uuid order_uuid,
	fgt.order_date order_date, 
	fgt.parent_order_uuid parent_order_uuid,
	b.voucher_code voucher_cd, 
	b.security_code security_cd, 
	b.merchant_uuid merchant_uuid, 
	b.deal_uuid deal_uuid,
	b.user_uuid user_uuid, 
	b.checked_in checked_in, 
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
			country_id,
			user_uuid fgt_user_uuid,
			order_date
			from user_edwprod.fact_gbl_transactions where order_date >= '2019-01-01' and action = 'authorize' and country_id<>235) as fgt
	inner join 
	(select 
		voucher_code, 
		security_code,
		billing_id parent_order_uuid,
		country_id
		from dwh_base.vouchers where date(created_at)>='2019-01-01' and country_id <> 235
	) as v on fgt.parent_order_uuid = v.parent_order_uuid and fgt.country_id = v.country_id
	inner join 
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
		start_time, 
		end_time, 
		created_at, 
		deleted_at,
		state
		from grp_gdoop_bizops_db.sh_bt_bookings_rebuild where date(created_at) >= '2019-01-01' and country_id <> 'US'
	) as b on v.voucher_code = b.voucher_code and v.security_code = b.security_code
 UNION
	select 
		fgt2.country_id country_id,
		fgt2.order_uuid order_uuid,
		fgt2.order_date order_date, 
		fgt2.parent_order_uuid parent_order_uuid, 
		b2.voucher_code voucher_cd, 
		b2.security_code security_cd, 
		b2.merchant_uuid merchant_uuid, 
		b2.deal_uuid deal_uuid,
		b2.user_uuid user_uuid, 
		b2.checked_in checked_in, 
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
			country_id,
			user_uuid fgt_user_uuid,
			order_date
			from user_edwprod.fact_gbl_transactions where order_date >= '2019-01-01' and action = 'authorize' and (country_id = 40) or (country_id = 235)) as fgt2 
		inner join 
		(select 
			code as voucher_code, 
			purchaser_consumer_id purchaser_consumer_id,
			order_id order_id
			from user_gp.camp_membership_coupons where date(created_at)>='2019-01-01' 
		) as v2 on fgt2.fgt_user_uuid = v2.purchaser_consumer_id  and fgt2.order_id = v2.order_id 
		inner join 
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
			start_time, 
			end_time, 
			created_at, 
			deleted_at,
			state
			from grp_gdoop_bizops_db.sh_bt_bookings_rebuild where date(created_at) >= '2019-01-01' and (country_id = 'US') or (country_id = 'CA')
		) as b2 on v2.voucher_code = b2.voucher_code and v2.purchaser_consumer_id = b2.user_uuid
) as uni
;




/*
 *  Table directly feeding into the Tableau Dashboard 
 */
create table grp_gdoop_bizops_db.np_bt_cust_cnt stored as orc as
select * from 
(select b.*, c.state, c.created_at, d.l1, d.l2, d.l3, e.deal_uuid bookable_on_purchase
from 
	(select a.parent_order_uuid, a.order_uuid, a.merchant_uuid merchant_uuid, a.country_id country_id, a.user_uuid user_uuid, a.deal_uuid deal_uuid, a.voucher_cd voucher_cd, a.security_cd security_cd, a.order_date order_date ,max(a.row_num) mx, min(a.row_num) mn
			from
				(select 
					 country_id, 
					 merchant_uuid,
					 parent_order_uuid, 
					 order_uuid,
					 order_date,
					 deal_uuid, 
					 voucher_cd, 
					 security_cd, 
					 user_uuid,
					 dense_rank() over(partition by user_uuid, voucher_cd, security_cd order by created_at) as row_num 
				from grp_gdoop_bizops_db.np_bt_cancel_cnt) a
				group by a.merchant_uuid, a.country_id, a.user_uuid, a.deal_uuid, a.voucher_cd, a.security_cd, a.order_date) as b
	left join 
			(select 
					 voucher_cd, 
					 security_cd, 
					 user_uuid,
					 created_at,
					 state,
					 dense_rank() over(partition by user_uuid, voucher_cd, security_cd order by created_at) as row_num 
				from grp_gdoop_bizops_db.np_bt_cancel_cnt) as c
			on b.voucher_cd = c.voucher_cd and b.security_cd = c.security_cd and b.user_uuid = c.user_uuid and b.mx = c.row_num
	left join 
			(select 
					deal_id deal_uuid, 
					country_code country_code, 
					country_id country_id, ;
					grt_l1_cat_name l1, 
					grt_l2_cat_name l2, 
					grt_l3_cat_name l3
			from user_edwprod.dim_gbl_deal_lob
			) as d 
			on b.deal_uuid = d.deal_uuid and b.country_id = d.country_id
	left join 
			(select 
					deal_uuid, 
					load_date, 
					country
			from grp_gdoop_bizops_db.sh_bt_active_deals_log where load_date >= '2019-01-01' and is_bookable = 1 and partner_inactive_flag = 0 and product_is_active_flag = 1
			) e 
			on b.deal_uuid = e.deal_uuid and b.order_date = e.load_date and d.country_code = e.country
		   
) as uni
;

create table grp_gdoop_bizops_db.np_bt_orders stored as orc as
select * from(
select 
			x.parent_order_uuid parent_order_uuid, 
			x.order_uuid order_uuid,
			x.country_id country_id, 
			z.country country_code,
			z.deal_uuid bookable, 
			z.is_bookable is_bookable, 
			z.partner_inactive_flag partner_inactive_flag, 
			z.product_is_active_flag product_is_active_flag
			from user_edwprod.fact_gbl_transactions x
			left join user_groupondw.gbl_dim_country y on x.country_id = y.country_key
			inner join grp_gdoop_bizops_db.sh_bt_active_deals_log z on x.deal_uuid = z.deal_uuid and cast(x.order_date as date) = cast(z.load_date as date) and y.country_iso_code_2 = z.country
			where cast(x.order_date as date) >= '2019-01-01' and x.action = 'authorize' and z.load_date >= '2019-01-01'
		) as uni;


create table np_bt_cancel_cnt stored as orc as
select * from 
(select 
	fgt.country_id country_id,
	fgt.order_uuid order_uuid,
	fgt.order_date order_date, 
	fgt.parent_order_uuid parent_order_uuid,
	b.voucher_code voucher_cd, 
	b.security_code security_cd, 
	b.merchant_uuid merchant_uuid, 
	b.deal_uuid deal_uuid,
	b.user_uuid user_uuid, 
	b.checked_in checked_in, 
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
			country_id,
			user_uuid fgt_user_uuid,
			order_date
			from user_edwprod.fact_gbl_transactions where order_date >= '2019-01-01' and action = 'authorize' and country_id<>235
			
			) as fgt
	inner join 
	(select 
		voucher_code, 
		security_code,
		billing_id parent_order_uuid,
		country_id
		from dwh_base.vouchers where date(created_at)>='2019-01-01' and country_id <> 235
	) as v on fgt.parent_order_uuid = v.parent_order_uuid and fgt.country_id = v.country_id
	inner join 
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
		start_time, 
		end_time, 
		created_at, 
		deleted_at,
		state
		from grp_gdoop_bizops_db.sh_bt_bookings_rebuild where date(created_at) >= '2019-01-01' and country_id <> 'US'
	) as b on v.voucher_code = b.voucher_code and v.security_code = b.security_code
 UNION
	select 
		fgt2.country_id country_id,
		fgt2.order_uuid order_uuid,
		fgt2.order_date order_date, 
		fgt2.parent_order_uuid parent_order_uuid, 
		b2.voucher_code voucher_cd, 
		b2.security_code security_cd, 
		b2.merchant_uuid merchant_uuid, 
		b2.deal_uuid deal_uuid,
		b2.user_uuid user_uuid, 
		b2.checked_in checked_in, 
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
			country_id,
			user_uuid fgt_user_uuid,
			order_date
			from user_edwprod.fact_gbl_transactions where order_date >= '2019-01-01' and action = 'authorize' and (country_id = 40) or (country_id = 235)) as fgt2 
		inner join 
		(select 
			code as voucher_code, 
			purchaser_consumer_id purchaser_consumer_id,
			order_id order_id
			from user_gp.camp_membership_coupons where date(created_at)>='2019-01-01' 
		) as v2 on fgt2.fgt_user_uuid = v2.purchaser_consumer_id  and fgt2.order_id = v2.order_id 
		inner join 
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
			start_time, 
			end_time, 
			created_at, 
			deleted_at,
			state
			from grp_gdoop_bizops_db.sh_bt_bookings_rebuild where date(created_at) >= '2019-01-01' and (country_id = 'US') or (country_id = 'CA')
		) as b2 on v2.voucher_code = b2.voucher_code and v2.purchaser_consumer_id = b2.user_uuid
) as uni
;
	
		

select * from user_edwprod.fact_gbl_transactions where parent_order_uuid = '1033d1fd-4e2c-49f4-a68c-9f12e5ecf6d9';


select * from user_gp.camp_membership_coupons where year(created_at) >= '2019' limit 5;
select * from dwh_base.vouchers where year(created_at) >= '2019' limit 5;

select * from dwh_base.vouchers limit 5;