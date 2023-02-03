
select count(distinct parent_order_uuid,order_uuid), count(distinct parent_order_uuid,deal_uuid) from (select * from nihpatel_db.np_bt_cancel_cnt) as uni;

select count(distinct parent_order_uuid,order_uuid), count(distinct parent_order_uuid,deal_uuid)
from
(select 
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
		b2.voucher_code voucher_cd, 
		b2.security_code security_cd, 
		b2.merchant_uuid merchant_uuid,
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
			order_date, 
			deal_id,
			deal_uuid,
			fgt_user_uuid, 
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
			order_id order_id,
			merchant_id merchant_uuid
			from user_gp.camp_membership_coupons where date(created_at)>='2019-01-01' 
		) as v2 on fgt2.fgt_user_uuid = v2.purchaser_consumer_id  and fgt2.order_id = v2.order_id 
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
			start_time,
			end_time, 
			created_at, 
			deleted_at,
			state
			from grp_gdoop_bizops_db.sh_bt_bookings_rebuild where date(created_at) >= '2019-01-01' and country_id = 'US'
		) as b2 on v2.voucher_code = b2.voucher_code and v2.purchaser_consumer_id = b2.user_uuid and v2.merchant_uuid = b2.merchant_uuid) as uni
	;
	

/* trying with USA And canada included


create table np_bt_cancel_cnt stored as orc as
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
	b.voucher_code voucher_cd, 
	b.security_code security_cd, 
	b.merchant_uuid merchant_uuid, 
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
			order_date, 
			deal_id,
			deal_uuid,
			fgt_user_uuid, 
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
		from dwh_base.vouchers where date(created_at)>='2019-01-01' and country_id <> 235 and country_id <> 40
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
		start_time, 
		end_time, 
		created_at, 
		deleted_at,
		state
		from grp_gdoop_bizops_db.sh_bt_bookings_rebuild where date(created_at) >= '2019-01-01' and country_id <> 'US' and country_id <> 'CA'
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
		b2.voucher_code voucher_cd, 
		b2.security_code security_cd, 
		b2.merchant_uuid merchant_uuid,
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
			order_date, 
			deal_id,
			deal_uuid,
			fgt_user_uuid, 
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
			from user_gp.camp_membership_coupons where date(created_at)>='2019-01-01' 
		) as v2 on fgt2.fgt_user_uuid = v2.purchaser_consumer_id  and fgt2.order_id = v2.order_id 
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
			start_time, 
			end_time, 
			created_at, 
			deleted_at,
			state
			from grp_gdoop_bizops_db.sh_bt_bookings_rebuild where date(created_at) >= '2019-01-01' and country_id = 'US'
		) as b2 on v2.voucher_code = b2.voucher_code and v2.purchaser_consumer_id = b2.user_uuid

) as uni
;
*/


