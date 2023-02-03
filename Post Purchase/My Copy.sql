
----step 1
drop table grp_gdoop_bizops_db.nvp_bt_bookings_stage;
create table grp_gdoop_bizops_db.nvp_bt_bookings_stage stored as orc as
select * from grp_gdoop_bizops_db.sh_bt_bookings_rebuild
where to_date(start_time) >= '2018-01-01' and to_date(start_time) < current_date;

insert overwrite table grp_gdoop_bizops_db.ab_bt_bookings_stage
select * from grp_gdoop_bizops_db.sh_bt_bookings_rebuild
where to_date(start_time) >= '2020-01-01' and to_date(start_time) < current_date

----------------------------------------------------------------------------------------------------step 2
delete from sandbox.nvp_bt_bookings_stage;
------Abi's
delete from sandbox.ab_bt_bookings_stage


----------------------------------------------------------------------------------------------------step 3
select * from grp_gdoop_bizops_db.nvp_bt_bookings_stage;

------Abi's
select * from grp_gdoop_bizops_db.ab_bt_bookings_stage

----------------------------------------------------------------------------------------------------step 4

delete from sandbox.nvp_bt_bookings_final;insert into sandbox.nvp_bt_bookings_final
select
booking_id,
country_id,
checked_in,
merchant_uuid,
cda_number,
deal_option_uuid,
deal_uuid,
voucher_code,
security_code,
participants_per_coupon,
is_a_groupon_booking,
booked_by,
cancelled_by,
user_uuid,
cast(start_time as timestamp(6) format 'yyyy-mm-dd-hh.mi.ss.s(6)'),
cast(end_time as timestamp(6) format 'yyyy-mm-dd-hh.mi.ss.s(6)'),
cast(created_at as timestamp(6) format 'yyyy-mm-dd-hh.mi.ss.s(6)'),
state,
cast(deleted_at as timestamp(6) format 'yyyy-mm-dd-hh.mi.ss.s(6)')
from sandbox.nvp_bt_bookings_stage


------------------------------------------------------------------------------------------------------Step 5

create volatile multiset table us_reds2 as (
	select 
		membership_coupon_id, status, id
	from user_gp.redemptions
	qualify row_number() over(partition by membership_coupon_id order by id desc) = 1
) with data primary index (membership_coupon_id) on commit preserve rows;collect stats on us_reds2 column(membership_coupon_id);delete from sandbox.nvp_chkins_stage_0;
insert into sandbox.nvp_chkins_stage_0
sel
	l.grt_l2_cat_name,
	b.country_id,
	b.checked_in,
	b.merchant_uuid,
	b.deal_uuid,
	concat(b.voucher_code,b.security_code) vsc_code,
	booked_by,
	cancelled_by,
	cast(b.start_time as date) as appointment_date,
	cast(b.created_at as date) as book_date,
	cast(b.deleted_at as date) as cancel_date,
	cast(coalesce(v.usage_date,coalesce(c.customer_redeemed_at,c.merchant_redeemed_at)) as date)
		as redemption_date,
	state as book_state,
	coalesce(v.billing_id,cast(c.order_id as varchar(64))) as order_id,
	lower(case -- redemption origin
		when v.usage_state_id = 2 then redeem_origin_name
		when r.status = 'redeemed' then rs.source_type
		else 'non-redeemed' end) as redemption_origin,
	case -- redemption status
		when coalesce(concat(v.voucher_code,v.security_code),concat(c.code,
			c.merchant_redemption_code)) is not null and state = 'confirmed' 
			and (v.usage_state_id = 2 or c.customer_redeemed = 1 or c.merchant_redeemed = 1)
			then 1 else 0 end as redemption_status
from sandbox.nvp_bt_bookings_final b
-- voucher joins
left join dwh_base_sec_view.vouchers v on v.voucher_code = b.voucher_code 
  and v.security_code = b.security_code and dwh_active = 1
left join user_gp.camp_membership_coupons c on c.code = b.voucher_code
	and cast(c.merchant_redemption_code as varchar(64)) = b.security_code
-- attributes
left join user_edwprod.dim_gbl_deal_lob l on l.deal_id = b.deal_uuid
-- redemptions
left join dwh_base_sec_view.voucher_redeem_origins o 
	on v.redeem_origin_id = o.redeem_origin_id
left join us_reds2 r on r.membership_coupon_id = c.id
left join user_gp.redemption_sources rs on rs.redemption_id = r.id
where 
	is_a_groupon_booking = 1
	and b.merchant_uuid <> '886b6b3c-8298-33cf-4f5c-3210505ded00' -- BT Test Account
	--and concat(b.voucher_code,b.security_code) = 'LG-NYSL-MGL5-6Z92-3B9L15987252'
	and cast(b.start_time as date) between '2020-01-01' and current_date - 1;
drop table us_reds2;delete from sandbox.nvp_chkins_stage_1;insert into sandbox.nvp_chkins_stage_1
sel
	b.*,
	case when book_state = 'confirmed' then
		row_number() over(partition by b.vsc_code,book_state order by book_date desc)
		end as confirm_row
from sandbox.nvp_chkins_stage_0 b;collect stats on sandbox.nvp_chkins_stage_1 column(appointment_date,merchant_uuid,deal_uuid);



------Abi's

create volatile multiset table us_reds as (
	select 
		membership_coupon_id, status, id
	from user_gp.redemptions
	qualify row_number() over(partition by membership_coupon_id order by id desc) = 1
) with data primary index (membership_coupon_id) on commit preserve rows;collect stats on us_reds column(membership_coupon_id);delete from sandbox.ab_chkins_stage_0;insert into sandbox.ab_chkins_stage_0
sel
	l.grt_l2_cat_name,
	b.country_id,
	b.checked_in,
	b.merchant_uuid,
	b.deal_uuid,
	concat(b.voucher_code,b.security_code) vsc_code,
	booked_by,
	cancelled_by,
	cast(b.start_time as date) as appointment_date,
	cast(b.created_at as date) as book_date,
	cast(b.deleted_at as date) as cancel_date,
	cast(coalesce(v.usage_date,coalesce(c.customer_redeemed_at,c.merchant_redeemed_at)) as date)
		as redemption_date,
	state as book_state,
	coalesce(v.billing_id,cast(c.order_id as varchar(64))) as order_id,
	lower(case -- redemption origin
		when v.usage_state_id = 2 then redeem_origin_name
		when r.status = 'redeemed' then rs.source_type
		else 'non-redeemed' end) as redemption_origin,
	case -- redemption status
		when coalesce(concat(v.voucher_code,v.security_code),concat(c.code,
			c.merchant_redemption_code)) is not null and state = 'confirmed' 
			and (v.usage_state_id = 2 or c.customer_redeemed = 1 or c.merchant_redeemed = 1)
			then 1 else 0 end as redemption_status
from sandbox.ab_bt_bookings_final b
-- voucher joins
left join dwh_base_sec_view.vouchers v on v.voucher_code = b.voucher_code 
  and v.security_code = b.security_code and dwh_active = 1
left join user_gp.camp_membership_coupons c on c.code = b.voucher_code
	and cast(c.merchant_redemption_code as varchar(64)) = b.security_code
-- attributes
left join user_edwprod.dim_gbl_deal_lob l on l.deal_id = b.deal_uuid
-- redemptions
left join dwh_base_sec_view.voucher_redeem_origins o 
	on v.redeem_origin_id = o.redeem_origin_id
left join us_reds r on r.membership_coupon_id = c.id
left join user_gp.redemption_sources rs on rs.redemption_id = r.id
where 
	is_a_groupon_booking = 1
	and b.merchant_uuid <> '886b6b3c-8298-33cf-4f5c-3210505ded00' -- BT Test Account
	--and concat(b.voucher_code,b.security_code) = 'LG-NYSL-MGL5-6Z92-3B9L15987252'
	and cast(b.start_time as date) between '2020-01-01' and current_date - 1;drop table us_reds;delete from sandbox.ab_chkins_stage_1;insert into sandbox.ab_chkins_stage_1
sel
	b.*,
	case when book_state = 'confirmed' then
		row_number() over(partition by b.vsc_code,book_state order by book_date desc)
		end as confirm_row
from sandbox.ab_chkins_stage_0 b;collect stats on sandbox.ab_chkins_stage_1 column(appointment_date,merchant_uuid,deal_uuid)



----------------------------------------------------------------------------------------------------------Step 6


delete from sandbox.nvp_chkins_stage_2;insert into sandbox.nvp_chkins_stage_2
sel
	b.*,
	case 
		when coalesce(f.parent_order_uuid,f2.order_id) is not null then 1 
		else 0 end as fgt_flag,
	coalesce(f.parent_order_uuid,f2.parent_order_uuid) as fgt_order_uuid,
	coalesce(f.order_date,f2.order_date) as order_date,
	case when confirm_row = 1 then
		(coalesce(f.capture_nob_loc * coalesce(approved_avg_exchange_rate,1),f2.capture_nob_loc) )
		else 0 end as nob,
	case when confirm_row = 1 then
		(coalesce(f.capture_nor_loc * coalesce(approved_avg_exchange_rate,1), f2.capture_nor_loc))
		else 0 end as nor
from sandbox.nvp_chkins_stage_1 b
-- voucher to txn join
left join user_edwprod.fact_gbl_transactions f on f.parent_order_uuid = b.order_id
	and f.action = 'capture' and b.deal_uuid = f.deal_uuid
left join user_edwprod.fact_gbl_transactions f2 on f2.order_id = b.order_id
	and f2.deal_uuid = b.deal_uuid
	and f2.action = 'capture'
-- fxn
left join (
	select
		currency_from,
		approved_avg_exchange_rate
	from user_groupondw.gbl_fact_exchange_rate
	where 
		currency_to = 'USD'
		and period_key = 202006
	group by 1,2) fx on f.currency_code = fx.currency_from;collect stats on sandbox.nvp_chkins_stage_2 column(appointment_date,merchant_uuid,deal_uuid)

	

------Abi's
delete from sandbox.ab_chkins_stage_2;insert into sandbox.ab_chkins_stage_2
sel
	b.*,
	case 
		when coalesce(f.parent_order_uuid,f2.order_id) is not null then 1 
		else 0 end as fgt_flag,
	coalesce(f.parent_order_uuid,f2.parent_order_uuid) as fgt_order_uuid,
	coalesce(f.order_date,f2.order_date) as order_date,
	case when confirm_row = 1 then
		(coalesce(f.capture_nob_loc * coalesce(approved_avg_exchange_rate,1),f2.capture_nob_loc) )
		else 0 end as nob,
	case when confirm_row = 1 then
		(coalesce(f.capture_nor_loc * coalesce(approved_avg_exchange_rate,1), f2.capture_nor_loc))
		else 0 end as nor
from sandbox.ab_chkins_stage_1 b
-- voucher to txn join
left join user_edwprod.fact_gbl_transactions f on f.parent_order_uuid = b.order_id
	and f.action = 'capture' and b.deal_uuid = f.deal_uuid
left join user_edwprod.fact_gbl_transactions f2 on f2.order_id = b.order_id
	and f2.deal_uuid = b.deal_uuid
	and f2.action = 'capture'
-- fxn
left join (
	select
		currency_from,
		approved_avg_exchange_rate
	from user_groupondw.gbl_fact_exchange_rate
	where 
		currency_to = 'USD'
		and period_key = 202006
	group by 1,2) fx on f.currency_code = fx.currency_from;collect stats on sandbox.ab_chkins_stage_2 column(appointment_date,merchant_uuid,deal_uuid)
-------------------------------------------------------------------------------------------------------step 7
	
	
delete from sandbox.nvp_chkins_final;insert into sandbox.nvp_chkins_final
sel
	b.*,
	case when f.action = 'refund' and f.parent_order_uuid is not null 
		and confirm_row = 1 then 1 else 0 end as refund_flag,
	case when f2.action = 'cancel' and f2.parent_order_uuid is not null 
		and confirm_row = 1 then 1 else 0 end as cancel_flag
from sandbox.nvp_chkins_stage_2 b
left join user_edwprod.fact_gbl_transactions f on b.fgt_order_uuid = f.parent_order_uuid
	and f.action = 'refund' and confirm_row = 1
left join user_edwprod.fact_gbl_transactions f2 on b.fgt_order_uuid = f2.parent_order_uuid
	and f2.action = 'cancel' and confirm_row = 1;collect stats on sandbox.nvp_chkins_final column(appointment_date,merchant_uuid,deal_uuid);grant select on sandbox.nvp_chkins_final to public
	
	
------Abi's
delete from sandbox.ab_chkins_final;insert into sandbox.ab_chkins_final
sel
	b.*,
	case when f.action = 'refund' and f.parent_order_uuid is not null 
		and confirm_row = 1 then 1 else 0 end as refund_flag,
	case when f2.action = 'cancel' and f2.parent_order_uuid is not null 
		and confirm_row = 1 then 1 else 0 end as cancel_flag
from sandbox.ab_chkins_stage_2 b
left join user_edwprod.fact_gbl_transactions f on b.fgt_order_uuid = f.parent_order_uuid
	and f.action = 'refund' and confirm_row = 1
left join user_edwprod.fact_gbl_transactions f2 on b.fgt_order_uuid = f2.parent_order_uuid
	and f2.action = 'cancel' and confirm_row = 1;collect stats on sandbox.ab_chkins_final column(appointment_date,merchant_uuid,deal_uuid);

grant select on sandbox.ab_chkins_final to public



------------------------------------------------------------------------------------------------------step 8
	
delete from sandbox.nvp_chkins_final_agg;insert into sandbox.nvp_chkins_final_agg
sel
	grt_l2_cat_name,
	country_id,
	checked_in,
--	merchant_uuid,
--	deal_uuid,
	booked_by,
	cancelled_by,
	trunc(appointment_date,'iw')+6 as appt_week,
	trunc(book_date,'iw')+6 as book_week,
	trunc(cancel_date,'iw')+6 as cancel_week,
	trunc(redemption_date,'iw')+6 as redeem_week,
	trunc(order_date,'iw')+6 as order_week,
	book_state,
	redemption_origin,
	redemption_status,
	fgt_flag,
	refund_flag,
	cancel_flag,
	sum(coalesce(nob,0)) as nob,
	sum(coalesce(nor,0)) as nor,
	count(1) as bookings
from sandbox.nvp_chkins_final
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16

------Abi's

delete from sandbox.ab_chkins_final_agg;insert into sandbox.ab_chkins_final_agg
sel
	grt_l2_cat_name,
	country_id,
	checked_in,
--	merchant_uuid,
--	deal_uuid,
	booked_by,
	cancelled_by,
	trunc(appointment_date,'iw')+6 as appt_week,
	trunc(book_date,'iw')+6 as book_week,
	trunc(cancel_date,'iw')+6 as cancel_week,
	trunc(redemption_date,'iw')+6 as redeem_week,
	trunc(order_date,'iw')+6 as order_week,
	book_state,
	redemption_origin,
	redemption_status,
	fgt_flag,
	refund_flag,
	cancel_flag,
	sum(coalesce(nob,0)) as nob,
	sum(coalesce(nor,0)) as nor,
	count(1) as bookings
from sandbox.ab_chkins_final
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
