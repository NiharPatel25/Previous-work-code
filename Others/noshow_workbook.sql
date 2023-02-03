------Abi's


create volatile multiset table us_reds as (
	select 
		membership_coupon_id, status, id
	from user_gp.redemptions
	qualify row_number() over(partition by membership_coupon_id order by id desc) = 1
) with data primary index (membership_coupon_id) on commit preserve rows;collect stats on us_reds column(membership_coupon_id);

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
	and cast(b.start_time as date) between '2020-01-01' and current_date - 1;

show table sandbox.ab_chkins_stage_1;

drop table us_reds;delete from sandbox.ab_chkins_stage_1;
insert into sandbox.ab_chkins_stage_1
sel
	b.*,
	case when book_state = 'confirmed' then
		row_number() over(partition by b.vsc_code,book_state order by book_date desc)
		end as confirm_row
from sandbox.ab_chkins_stage_0 b;
collect stats on sandbox.ab_chkins_stage_1 column(appointment_date,merchant_uuid,deal_uuid)



select * from sandbox.ab_chkins_stage_1
where appointment_date >= '2020-10-11'
and book_state = 'confirmed'
and checked_in = 'no-show'
and country_id <> 'US'
and redemption_status = 0;
--------------

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