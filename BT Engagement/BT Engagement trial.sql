select b.*, c.state 
from 
	(select merchant_uuid merchant_uuid, user_uuid user_uuid, deal_uuid deal_uuid, voucher_code voucher_code, security_code security_code, max(row_num) mx, min(row_num) mn
	from (select booking_id, 
				 country_id, 
				 merchant_uuid,
				 deal_option_uuid, 
				 deal_uuid, 
				 voucher_code, 
				 security_code, 
				 user_uuid,
				 dense_rank() over(partition by voucher_code, security_code order by created_at) as row_num 
			from tmp2) a
		group by a.merchant_uuid, a.user_uuid, a.deal_uuid, a.voucher_code, a.security_code) as b
	left join  
	(select
		 deal_uuid, 
		 voucher_code, 
		 security_code,
		 state,
		 start_time, 
		 end_time,
		 created_at, 
		 deleted_at,
		 dense_rank() over(partition by voucher_code, security_code order by created_at) as row_num 
	from tmp2) c
on b.deal_uuid = c.deal_uuid and b.voucher_code = c.voucher_code and b.security_code = c.security_code and b.mx = c.row_num





CREATE TEMPORARY TABLE tmp2 AS select * from grp_gdoop_bizops_db.sh_bt_bookings_rebuild where user_uuid = '00003811-a002-494a-8c1e-f75f599fd3a3' and deal_uuid = 'd5eab2a1-8952-4c9d-8038-efbd336c93a3';

select *, DENSE_RANK() over(order by created_at ) from grp_gdoop_bizops_db.sh_bt_bookings_rebuild where user_uuid = '00003811-a002-494a-8c1e-f75f599fd3a3' and deal_uuid = 'd5eab2a1-8952-4c9d-8038-efbd336c93a3';

SELECT *, ROW_NUMBER() over(partition by voucher_code, security_code order by created_at) FROM tmp2;

/*windowed function rows preceding */

select b.user_uuid, b.deal_uuid, b.voucher_code, b.security_code, count(booking_id) count_
from grp_gdoop_bizops_db.sh_bt_bookings_rebuild b where year(created_at) >= '2019' group by b.user_uuid, b.deal_uuid, b.voucher_code, b.security_code;

select * from tmp2;
/* creating tableau table*/

select * from tmp2 where min(created_at)> '2019-10-08' group by deal_uuid, voucher_code, security_code;

select * from user_edwprod.fact_gbl_transactions limit 3;

select 
		distinct(country_id)
		from dwh_base.vouchers limit 30;

select distinct(country_id) from 	
	(select 
			parent_order_uuid, 
			order_uuid,
			order_id order_id,
			country_id,
			user_uuid fgt_user_uuid,
			order_date
			from user_edwprod.fact_gbl_transactions where order_date >= '2019-01-01' and action = 'authorize') as fgt 
		inner join 
		(select 
			purchaser_consumer_id purchaser_consumer_id,
			order_id order_id
			from user_gp.camp_membership_coupons where date(created_at)>='2019-01-01'
		) as v on fgt.fgt_user_uuid = v.purchaser_consumer_id  and fgt.order_id = v.order_id limit 15;


select * from user_edwprod.fact_gbl_transactions limit 5;
