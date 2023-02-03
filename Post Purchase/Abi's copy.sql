CREATE MULTISET TABLE sandbox.nvp_bt_bookings_stage ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      booking_id INTEGER,
      country_id VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
      checked_in VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
      merchant_uuid VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
      cda_number VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
      deal_option_uuid VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
      deal_uuid VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
      voucher_code VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
      security_code VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
      participants_per_coupon INTEGER,
      is_a_groupon_booking INTEGER,
      booked_by VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
      cancelled_by VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
      user_uuid VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
      start_time VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
      end_time VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
      created_at VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
      state VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
      deleted_at VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC)
NO PRIMARY INDEX ;



-----

CREATE MULTISET TABLE sandbox.nvp_bt_bookings_final ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      booking_id INTEGER,
      country_id VARCHAR(8) CHARACTER SET LATIN NOT CASESPECIFIC,
      checked_in VARCHAR(16) CHARACTER SET LATIN NOT CASESPECIFIC,
      merchant_uuid VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
      cda_number VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
      deal_option_uuid VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
      deal_uuid VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
      voucher_code VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
      security_code VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
      participants_per_coupon INTEGER,
      is_a_groupon_booking INTEGER,
      booked_by VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
      cancelled_by VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
      user_uuid VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
      start_time TIMESTAMP(6),
      end_time TIMESTAMP(6),
      created_at TIMESTAMP(6),
      state VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
      deleted_at TIMESTAMP(6))
PRIMARY INDEX ( country_id ,checked_in ,state );


------

show table sandbox.ab_chkins_stage_2;

CREATE MULTISET TABLE sandbox.nvp_chkins_stage_2 ,NO FALLBACK ,
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
      nor FLOAT)
PRIMARY INDEX ( merchant_uuid ,deal_uuid ,appointment_date );



-----


CREATE MULTISET TABLE sandbox.nvp_chkins_final ,NO FALLBACK ,
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
      cancel_flag BYTEINT)
PRIMARY INDEX ( merchant_uuid ,deal_uuid ,appointment_date );


-----


CREATE MULTISET TABLE sandbox.nvp_chkins_final_agg ,NO FALLBACK ,
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
      bookings INTEGER)
PRIMARY INDEX ( grt_l2_cat_name ,appointment_week );



--------


CREATE MULTISET TABLE sandbox.nvp_chkins_stage_0 ,NO FALLBACK ,
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
      redemption_status BYTEINT)
PRIMARY INDEX ( merchant_uuid ,deal_uuid ,appointment_date );



------


show table sandbox.ab_chkins_stage_1;


CREATE MULTISET TABLE sandbox.nvp_chkins_stage_1 ,NO FALLBACK ,
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
      confirm_row INTEGER)
PRIMARY INDEX ( merchant_uuid ,deal_uuid ,appointment_date );



------Step 4

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
from sandbox.nvp_bt_bookings_stage;


-----
select min(created_at), max(created_at), min(start_time), max(start_time) from sandbox.nvp_bt_bookings_final;

select * from sandbox.nvp_bt_bookings_final WHERE EXTRACT(YEAR FROM start_time) = 2018;

select * from sandbox.nvp_bt_bookings_final; 

select * from sandbox.nvp_chkins_stage_0;


-----Step 5

drop table vouchers;
create volatile multiset table vouchers as (
    select 
        voucher_code, 
        security_code, 
        redeem_origin_id, 
        usage_state_id, 
        cast(usage_date as date) as usage_date, 
        billing_id
    from dwh_base_sec_view.vouchers
    where dwh_active = 1
    and cast(created_at as date) >= '2018-01-01'
    and voucher_code is not null
) with data on commit preserve rows;
collect stats on vouchers column(voucher_code,usage_date);


drop table us_red2;
create volatile multiset table us_reds2 as (
	select 
		membership_coupon_id, status, id
	from user_gp.redemptions
	qualify row_number() over(partition by membership_coupon_id order by id desc) = 1
) with data primary index (membership_coupon_id) on commit preserve rows;
collect stats on us_reds2 column(membership_coupon_id);


create volatile multiset table coupons as (
	select 
		code, 
		cast(merchant_redemption_code as varchar(64)) security_code, 
		merchant_redeemed_at, 
		customer_redeemed_at, 
		customer_redeemed,
		merchant_redeemed,
		cast(order_id as varchar(64)) order_id, 
		id
	from user_gp.camp_membership_coupons
) with data on commit preserve rows;
collect stats on coupons column(id);

grant insert on sandbox.nvp_bt_bookings_final to ub_bizops;

select * from sandbox.nvp_chkins_stage_0;


delete from sandbox.nvp_chkins_stage_0;
insert into sandbox.nvp_chkins_stage_0
select
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
	coalesce(v.billing_id,c.order_id) as order_id,
	lower(case -- redemption origin
		when v.usage_state_id = 2 then redeem_origin_name
		when r.status = 'redeemed' then rs.source_type
		else 'non-redeemed' end) as redemption_origin,
	case -- redemption status
		when coalesce(concat(v.voucher_code,v.security_code),concat(c.code,
			c.security_code)) is not null and state = 'confirmed' 
			and (v.usage_state_id = 2 or c.customer_redeemed = 1 or c.merchant_redeemed = 1)
			then 1 else 0 end as redemption_status
from sandbox.nvp_bt_bookings_final b
-- voucher joins
left join sandbox.ab_vouchers_temp v on v.voucher_code = b.voucher_code 
  and v.security_code = b.security_code
left join coupons c on c.code = b.voucher_code
	and c.security_code = b.security_code
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
	and cast(b.start_time as date) between '2018-01-01' and '2018-04-30';



select * from sandbox.ab_chkins_stage_0;

-----adding 2020


insert into sandbox.nvp_chkins_stage_0
select * from sandbox.ab_chkins_stage_0;



------Step 5.2


delete from sandbox.nvp_chkins_stage_1;
insert into sandbox.nvp_chkins_stage_1
sel
	b.*,
	case when book_state = 'confirmed' then
		row_number() over(partition by b.vsc_code,book_state order by book_date desc)
		end as confirm_row
from sandbox.nvp_chkins_stage_0 b;collect stats on sandbox.nvp_chkins_stage_1 column(appointment_date,merchant_uuid,deal_uuid);

----Step 6


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
	group by 1,2) fx on f.currency_code = fx.currency_from;
collect stats on sandbox.nvp_chkins_stage_2 column(appointment_date,merchant_uuid,deal_uuid);


-----step 7 


delete from sandbox.nvp_chkins_final;
insert into sandbox.nvp_chkins_final
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
grant select on sandbox.nvp_chkins_final to public



drop table sandbox.nvp_merc_red_tableau;
create multiset table sandbox.nvp_merc_red_tableau as
(select 
	*
from sandbox.nvp_chkins_final a 
	where a.book_state = 'confirmed'
	and a.fgt_flag = 1
	and a.refund_flag = 0) with data;


-----ADDING AND IMPORTING TEST MERCHANTS

drop table sandbox.nvp_ns_test_merchants;
create multiset table sandbox.nvp_ns_test_merchants
(
deal_link varchar(100) character set unicode, 
country_cd varchar(100) character set unicode,
vertical varchar(100) character set unicode,
notes varchar(100) character set unicode, 
deal_option varchar(100) character set unicode,
deal_uuid varchar(100) character set unicode
);

select distinct b.merchant_uuid
from sandbox.nvp_ns_test_merchants a
left join user_edwprod.dim_offer_ext b on a.deal_uuid = b.product_uuid

select * from sandbox.ab_chkins_stage_2;

delete from sandbox.ab_chkins_final;
insert into sandbox.ab_chkins_final
sel
	b.*,
	case when f.action = 'refund' and f.parent_order_uuid is not null 
		and confirm_row = 1 then 1 else 0 end as refund_flag,
	case when f2.action = 'cancel' and f2.parent_order_uuid is not null 
		and confirm_row = 1 then 1 else 0 end as cancel_flag
from 
	(select x.*
          from 
            sandbox.ab_chkins_stage_2 x 
            left join 
            (select distinct v.merchant_uuid
            from sandbox.nvp_ns_test_merchants u
            left join user_edwprod.dim_offer_ext v on u.deal_uuid = v.product_uuid) y on x.merchant_uuid = y.merchant_uuid
            where y.merchant_uuid is null) as b
left join user_edwprod.fact_gbl_transactions f on b.fgt_order_uuid = f.parent_order_uuid
	and f.action = 'refund' and confirm_row = 1
left join user_edwprod.fact_gbl_transactions f2 on b.fgt_order_uuid = f2.parent_order_uuid
	and f2.action = 'cancel' and confirm_row = 1;


collect stats on sandbox.ab_chkins_final column(appointment_date,merchant_uuid,deal_uuid);
grant select on sandbox.nvp_ns_test_merchants to public

select * from sandbox.ab_chkins_final;

------
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
	and f2.action = 'cancel' and confirm_row = 1;collect stats on sandbox.ab_chkins_final column(appointment_date,merchant_uuid,deal_uuid);grant select on sandbox.ab_chkins_final to public
