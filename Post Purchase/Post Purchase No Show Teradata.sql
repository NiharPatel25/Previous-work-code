describe table sandbox.ab_bt_bookings_stage


----step 1
drop table grp_gdoop_bizops_db.nvp_bt_bookings_stage;
create table grp_gdoop_bizops_db.nvp_bt_bookings_stage stored as orc as
select * from grp_gdoop_bizops_db.sh_bt_bookings_rebuild
where to_date(start_time) >= '2018-01-01' and to_date(start_time) < current_date;

insert overwrite table grp_gdoop_bizops_db.ab_bt_bookings_stage
select * from grp_gdoop_bizops_db.sh_bt_bookings_rebuild
where to_date(start_time) >= '2020-01-01' and to_date(start_time) < current_date

select * from sandbox.sh_bt_active_deals_log where deal_uuid = '7e13332c-4de2-4f6f-a0af-caea804efd8f';
'4a615e10-63d3-49f3-af3b-e4effef2318d'
----------------------------------------------------------------------------------------------------step 2

delete from sandbox.nvp_bt_bookings_stage;


------Abi's

delete from sandbox.ab_bt_bookings_stage;

select * from sandbox.ab_bt_bookings_stage;

----------------------------------------------------------------------------------------------------step 3
select * from grp_gdoop_bizops_db.nvp_bt_bookings_stage;

------Abi's
select * from grp_gdoop_bizops_db.ab_bt_bookings_stage

----------------------------------------------------------------------------------------------------step 4

delete from sandbox.nvp_bt_bookings_final;
insert into sandbox.nvp_bt_bookings_final
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

------Abi's
show table sandbox.ab_bt_bookings_final;

delete from sandbox.ab_bt_bookings_final;
insert into sandbox.ab_bt_bookings_final
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
from sandbox.ab_bt_bookings_stage;


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
drop table us_reds2;
delete from sandbox.nvp_chkins_stage_1;
insert into sandbox.nvp_chkins_stage_1
sel
	b.*,
	case when book_state = 'confirmed' then
		row_number() over(partition by b.vsc_code,book_state order by book_date desc)
		end as confirm_row
from sandbox.nvp_chkins_stage_0 b;
collect stats on sandbox.nvp_chkins_stage_1 column(appointment_date,merchant_uuid,deal_uuid);



------Abi's


create volatile multiset table us_reds as (
	select 
		membership_coupon_id, status, id
	from user_gp.redemptions
	qualify row_number() over(partition by membership_coupon_id order by id desc) = 1
) with data primary index (membership_coupon_id) on commit preserve rows;
collect stats on us_reds column(membership_coupon_id);


show table sandbox.ab_chkins_stage_0;
delete from sandbox.ab_chkins_stage_0;
insert into sandbox.ab_chkins_stage_0
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
	when
		coalesce(concat(v.voucher_code,v.security_code), concat(c.code, c.merchant_redemption_code)) is not null and state = 'confirmed' 
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
left join dwh_base_sec_view.voucher_redeem_origins o on v.redeem_origin_id = o.redeem_origin_id
left join us_reds r on r.membership_coupon_id = c.id
left join user_gp.redemption_sources rs on rs.redemption_id = r.id
where
	is_a_groupon_booking = 1
	and b.merchant_uuid <> '886b6b3c-8298-33cf-4f5c-3210505ded00' -- BT Test Account
	--and concat(b.voucher_code,b.security_code) = 'LG-NYSL-MGL5-6Z92-3B9L15987252'
	and cast(b.start_time as date) between '2020-01-01' and current_date - 1;



select 
      redemption_origin,
      redemption_status,
      count(1)
   from sandbox.ab_chkins_stage_0 
   where country_id = 'US' 
   and appointment_date > '2020-09-13' and appointment_date <= '2020-09-20'
  group by redemption_origin, redemption_status
 order by redemption_origin, redemption_status;

select distinct source_type from user_gp.redemption_sources;

select 
      redemption_origin,
      redemption_status,
      count(1)
   from sandbox.ab_chkins_stage_test
   where country_id = 'US' 
   and appointment_date > '2020-09-13' and appointment_date <= '2020-09-20'
  group by redemption_origin, redemption_status
  order by redemption_origin, redemption_status;

drop table sandbox.ab_chkins_stage_test;
create multiset table sandbox.ab_chkins_stage_test as(
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
	cast(coalesce(c.customer_redeemed_at,c.merchant_redeemed_at) as date) as redemption_date,
	state as book_state,
	cast(c.order_id as varchar(64)) as order_id,
	lower(rs.source_type) as redemption_origin,
	case -- redemption status 
	when
		concat(c.code, c.merchant_redemption_code) is not null and state = 'confirmed' 
		    and (c.customer_redeemed = 1 or c.merchant_redeemed = 1)
			then 1 else 0 end as redemption_status
from sandbox.ab_bt_bookings_final b
-- voucher joins
left join user_gp.camp_membership_coupons c on c.code = b.voucher_code and cast(c.merchant_redemption_code as varchar(64)) = b.security_code
-- attributes
left join user_edwprod.dim_gbl_deal_lob l on l.deal_id = b.deal_uuid
-- redemptions
left join us_reds r on r.membership_coupon_id = c.id
left join user_gp.redemption_sources rs on rs.redemption_id = r.id
where
	is_a_groupon_booking = 1
	and b.merchant_uuid <> '886b6b3c-8298-33cf-4f5c-3210505ded00' -- BT Test Account
	--and concat(b.voucher_code,b.security_code) = 'LG-NYSL-MGL5-6Z92-3B9L15987252'
	and cast(b.start_time as date) between '2020-08-01' and current_date - 1) with data;






show table sandbox.ab_chkins_stage_1;

drop table us_reds;
delete from sandbox.ab_chkins_stage_1;
insert into sandbox.ab_chkins_stage_1
sel
	b.*,
	case when book_state = 'confirmed' then
		row_number() over(partition by b.vsc_code,book_state order by book_date desc)
		end as confirm_row
from sandbox.ab_chkins_stage_0 b;
collect stats on sandbox.ab_chkins_stage_1 column(appointment_date,merchant_uuid,deal_uuid)

select * from sandbox.ab_chkins_stage_1;

----------------------------------------------------------------------------------------------------------Step 6


delete from sandbox.nvp_chkins_stage_2;
insert into sandbox.nvp_chkins_stage_2
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


delete from sandbox.ab_chkins_stage_2;

insert into sandbox.ab_chkins_stage_2
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
	group by 1,2) fx on f.currency_code = fx.currency_from;



collect stats on sandbox.ab_chkins_stage_2 column(appointment_date,merchant_uuid,deal_uuid)


select * from sandbox.ab_chkins_stage_1
where appointment_date >= '2020-10-11'
and book_state = 'confirmed'
and checked_in = 'no-show'
and country_id <> 'US'
and redemption_status = 0;



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
	and f2.action = 'cancel' and confirm_row = 1;



collect stats on sandbox.nvp_chkins_final column(appointment_date,merchant_uuid,deal_uuid);
grant select on sandbox.nvp_chkins_final to public;

------Abi's


drop table sandbox.ab_chkins_final;
CREATE MULTISET TABLE sandbox.ab_chkins_final ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      grt_l2_cat_name VARCHAR(36) CHARACTER SET LATIN NOT CASESPECIFIC,
      country_id VARCHAR(8) CHARACTER SET LATIN NOT CASESPECIFIC,
      checked_in VARCHAR(16) CHARACTER SET LATIN NOT CASESPECIFIC,
      merchant_uuid VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
      deal_uuid VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
      vsc_code VARCHAR(128) CHARACTER SET LATIN NOT CASESPECIFIC,
      booked_by VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
      cancelled_by VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
      appointment_date DATE FORMAT 'yyyy-mm-dd',
      book_date DATE FORMAT 'yyyy-mm-dd',
      cancel_date DATE FORMAT 'yyyy-mm-dd',
      redemption_date DATE FORMAT 'yyyy-mm-dd',
      book_state VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
      order_id VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
      redemption_origin VARCHAR(256) CHARACTER SET UNICODE CASESPECIFIC,
      redemption_status BYTEINT,
      confirm_row INTEGER,
      fgt_flag BYTEINT,
      fgt_order_uuid VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
      order_date DATE FORMAT 'yyyy-mm-dd',
      nob FLOAT,
      nor FLOAT,
      refund_flag BYTEINT,
      cancel_flag BYTEINT,
      new_bt_opt_in INTEGER,
      merch_bt_login INTEGER
      )
PRIMARY INDEX ( merchant_uuid ,deal_uuid ,appointment_date );


-----------Merchant Last Login Information

drop table sandbox.nvp_ns_merchant_lastlog;

CREATE MULTISET TABLE sandbox.nvp_ns_merchant_lastlog ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      merchant_uuid VARCHAR(64) CHARACTER SET UNICODE,
      created_at VARCHAR(64) CHARACTER SET UNICODE,
      name VARCHAR(64) CHARACTER SET UNICODE,
      booking_solution VARCHAR(64) CHARACTER SET UNICODE,
      last_login VARCHAR(64) CHARACTER SET UNICODE,
      inactive VARCHAR(64) CHARACTER SET UNICODE,
      new_bt_opt_in VARCHAR(64) CHARACTER SET UNICODE,
      new_merchant VARCHAR(64) CHARACTER SET UNICODE,
      country VARCHAR(64) CHARACTER SET UNICODE,
      no_show_rate VARCHAR(64) CHARACTER SET UNICODE,
      has_live_deals_flags VARCHAR(64) CHARACTER SET UNICODE,
      live_deals_count VARCHAR(64) CHARACTER SET UNICODE,
      live_deals_non_sold_out_count VARCHAR(64) CHARACTER SET UNICODE)
NO PRIMARY INDEX;

----------------------------main query



create volatile multiset table dl_opt_in as (
	select 
		deal_uuid,
		max(cast(substr(new_bt_opt_in_date, 1, 10) as DATE)) max_optin_date
	from sandbox.sh_bt_active_deals_log
	where new_bt_opt_in_date is not null
	group by deal_uuid
) with data on commit preserve rows;
collect stats on us_reds column(membership_coupon_id);

delete from sandbox.ab_chkins_final;
insert into sandbox.ab_chkins_final
sel
	b.*,
	case when f.action = 'refund' and f.parent_order_uuid is not null 
		and confirm_row = 1 then 1 else 0 end as refund_flag,
	case when f2.action = 'cancel' and f2.parent_order_uuid is not null 
		and confirm_row = 1 then 1 else 0 end as cancel_flag, 
	case when b.book_date >= dl.max_optin_date then 1 else 0 end as new_bt_opt_in, 
	case when ml.merchant_uuid is not null then 1 else 0 end as merch_bt_login
from sandbox.ab_chkins_stage_2 b
left join user_edwprod.fact_gbl_transactions f on b.fgt_order_uuid = f.parent_order_uuid
	and f.action = 'refund' and confirm_row = 1
left join user_edwprod.fact_gbl_transactions f2 on b.fgt_order_uuid = f2.parent_order_uuid
	and f2.action = 'cancel' and confirm_row = 1
left join dl_opt_in as dl on b.deal_uuid = dl.deal_uuid
left join sandbox.nvp_ns_merchant_lastlog as ml on b.merchant_uuid = ml.merchant_uuid and last_login is not null;



collect stats on sandbox.ab_chkins_final column(appointment_date,merchant_uuid,deal_uuid);
grant select on sandbox.ab_chkins_final to public;



------------------------------------------------------------------------------------------------------step 8



delete from sandbox.nvp_chkins_final_agg;
insert into sandbox.nvp_chkins_final_agg
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
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16;



--------------------------Abi's
drop table sandbox.ab_chkins_final_agg;

CREATE MULTISET TABLE sandbox.ab_chkins_final_agg ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      grt_l2_cat_name VARCHAR(36) CHARACTER SET LATIN NOT CASESPECIFIC,
      country_id VARCHAR(8) CHARACTER SET LATIN NOT CASESPECIFIC,
      checked_in VARCHAR(16) CHARACTER SET LATIN NOT CASESPECIFIC,
      booked_by VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
      cancelled_by VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
      appointment_week DATE FORMAT 'yyyy-mm-dd',
      book_week DATE FORMAT 'yyyy-mm-dd',
      cancel_week DATE FORMAT 'yyyy-mm-dd',
      redemption_week DATE FORMAT 'yyyy-mm-dd',
      order_week DATE FORMAT 'yyyy-mm-dd',
      book_state VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
      redemption_origin VARCHAR(256) CHARACTER SET UNICODE CASESPECIFIC,
      redemption_status BYTEINT,
      fgt_flag BYTEINT,
      refund_flag BYTEINT,
      cancel_flag BYTEINT,
      nob FLOAT,
      nor FLOAT,
      bookings INTEGER,
      new_bt_opt_flag BYTEINT,
      merch_bt_login_flag BYTEINT)
PRIMARY INDEX ( grt_l2_cat_name ,appointment_week);
grant select on sandbox.ab_chkins_final_agg to public;

delete from sandbox.ab_chkins_final_agg;
insert into sandbox.ab_chkins_final_agg
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
	count(1) as bookings,
	new_bt_opt_in,
    merch_bt_login
from sandbox.ab_chkins_final
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,20,21
;

select * from sandbox.ab_chkins_final_agg 
where appointment_week = '2020-09-20' and country_id = 'US';



select 
    book_state, 
    checked_in, 
    redemption_status, 
    sum(bookings)
from sandbox.ab_chkins_final_agg 
where appointment_week = '2020-10-11'
and country_id <> 'US'
group by 1,2,3
order by 1,2,3
;



select 
   *
from sandbox.ab_chkins_stage_2
where checked_in = 'unknown' 
and redemption_status = 0 
and book_state = 'confirmed'
and trunc(book_date,'iw')+6 >= '2020-10-11'
and country_id <> 'US';


create volatile multiset table nvp_no_show_temp as (
sel
	grt_l2_cat_name,
	country_id,
	checked_in,
    merchant_uuid,
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
	count(1) as bookings,
	new_bt_opt_in,
    merch_bt_login
from sandbox.ab_chkins_final
where cast(appointment_date as date) >= cast('2020-10-01' as date)
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,21,22
) with data on commit preserve rows;

create volatile multiset table nvp_merch_new_onboarded as (
select 
b.merchant_uuid, 
min(a.mn_date) mn_datenew
from 
  (select deal_uuid, min(load_date) mn_date from sandbox.sh_bt_active_deals_log group by deal_uuid
  ) as a
  left join 
  (select 
             product_uuid, 
             max(merchant_uuid) merchant_uuid 
           from user_edwprod.dim_offer_ext 
           group by product_uuid) as b on a.deal_uuid = b.product_uuid
group by b.merchant_uuid
having mn_datenew >= '2020-09-18') with data on commit preserve rows;


create volatile multiset table nvp_merch_new_onfinal as (
select 
   a.*, 
   case when b.merchant_uuid is not null then 1 else 0 end new_merchant
   from 
   nvp_no_show_temp as a
   left join 
   nvp_merch_new_onboarded as b on a.merchant_uuid = b.merchant_uuid) with data on commit preserve ROWS;
  

select 
    new_merchant, 
    sum(case when book_state = 'confirmed' then bookings end) appointments,
    sum(case when book_state = 'confirmed' and checked_in = 'no-show' and redemption_status = 0 then bookings end) no_show
from nvp_merch_new_onfinal
where booked_by not in ('admin', 'merchant') 
and appt_week = '2020-10-25'
and country_id in ('US', 'CA')
and refund_flag = 0
group by new_merchant;


select * from nvp_merch_new_onfinal;
-----------------------------------------------------------------------------------



