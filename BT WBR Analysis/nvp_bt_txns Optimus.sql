
-----Source copy of sh_bt_bookings_rebuild in teradata
------USE THIS sandbox.sh_bt_bookings_rebuild;

/*CREATE MULTISET TABLE sandbox.nvp_bt_bookings_rebuild ,NO FALLBACK ,
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
NO PRIMARY INDEX ;*/



-----source copy for sh_bt_active_deals_log

select * from sandbox.sh_bt_bookings_rebuild;
select * from sandbox.nvp_bt_txns where parent_order_uuid = 'a2cf88ad-1933-4433-9500-245a187818b8';


CREATE MULTISET TABLE sandbox.nvp_bt_active_deals_log_kpi ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
       deal_uuid VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
       inventory_product_uuid	VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
       gapp_enabled	integer,
       is_bookable	integer,
       partner_inactive_flag	integer,
       product_is_active_flag integer,
       new_bt_opt_in_flag integer,
       new_bt_opt_in_date VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
       is_multisession_flag integer,
       is_multiagenda_flag integer,
       country	VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
       sold_out VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
       load_date VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC
     )
NO PRIMARY INDEX ;

select * from grp_gdoop_bizops_db.sh_bt_active_deals_log where to_date(load_date) >= cast('2020-01-01' as date);
select * from grp_gdoop_bizops_db.sh_bt_bookings_rebuild where to_date(created_at) >= '2020-01-01';
select * from grp_gdoop_bizops_db.sh_bt_bookings_rebuild
where cast(substr(created_at,1,10) as date) >= cast('2020-01-01' as date);

-----hive

select * from grp_gdoop_bizops_db.sh_bt_bookings_rebuild where to_date(created_at) >= '2020-01-01';



drop table if exists grp_gdoop_bizops_db.nvp_bt_bookings_imp;
create table grp_gdoop_bizops_db.nvp_bt_bookings_imp stored as orc as;
create table sandbox.nvp_bt_bookings_imp as (
select
     l.grt_l1_cat_name,
     l.grt_l2_cat_name,
     l.grt_l3_cat_name,
     b.country_id,
     b.checked_in,
     b.merchant_uuid,
     b.deal_uuid,
     concat(b.voucher_code,b.security_code) vsc_code,
     booked_by,
     cancelled_by,
     cast(SUBSTRING(b.start_time,1,10) as date) as appointment_date,
     cast(SUBSTRING(b.created_at,1,10) as date) as book_date,
     cast(SUBSTRING(b.deleted_at,1,10) as date) as cancel_date,
     cast(coalesce(SUBSTRING(v.usage_date, 1,10),coalesce(SUBSTRING(c.customer_redeemed_at,1,10) , SUBSTRING(c.merchant_redeemed_at,1,10))) as date) as redemption_date,
     state as book_state,
     coalesce(v.billing_id,cast(c.order_id as varchar(64))) as order_id,
     case -- redemption status
        when
        coalesce(concat(v.voucher_code,v.security_code), concat(c.code, c.merchant_redemption_code)) is not null and state = 'confirmed'
        and (v.usage_state_id = 2 or c.customer_redeemed = 1 or c.merchant_redeemed = 1)
        then 1 else 0 end as redemption_status
from sandbox.nvp_bt_bookings_rebuild b
-- voucher joins
left join dwh_base_sec_view.vouchers v on v.voucher_code = b.voucher_code and v.security_code = b.security_code and dwh_active = 1
left join user_gp.camp_membership_coupons c on c.code = b.voucher_code and cast(c.merchant_redemption_code as varchar(64)) = b.security_code
-- attributes
-- redemptions
where
    is_a_groupon_booking = 1
    and b.merchant_uuid <> '886b6b3c-8298-33cf-4f5c-3210505ded00' -- BT Test Account
    --and concat(b.voucher_code,b.security_code) = 'LG-NYSL-MGL5-6Z92-3B9L15987252'
     ) with data;





--Bookings--

select * from dwh_base_sec_view.vouchers;
drop table nihpatel.nvp_bookings_coupons;

create volatile multiset table nvp_bookings_vouchers as(
select parent_order_uuid, min_booked_at
from (
  select
        v.billing_id as parent_order_uuid,
        min(cast(b.created_at as timestamp)) min_booked_at
    from dwh_base_sec_view.vouchers v
          join sandbox.sh_bt_bookings_rebuild b on b.voucher_code = v.voucher_code and b.security_code = v.security_code
    where is_a_groupon_booking = 1
          and (lower(booked_by) in ('customer', 'api') or booked_by is null)
          and cast(v.created_at as date) >= cast('2019-01-01' as date)
    group by v.billing_id
  ) a
) with data on commit preserve rows;


create volatile multiset table nvp_booking_coupons_tp as
(select
     cast(b.created_at as timestamp) created_at,
     v.purchaser_consumer_id,
     v.order_id,
     b.deal_uuid
    from user_gp.camp_membership_coupons v
         join sandbox.sh_bt_bookings_rebuild b on b.voucher_code = v.code and b.security_code = cast(v.merchant_redemption_code as varchar(64))
    where b.country_id = 'US'
         and is_a_groupon_booking = 1
         and (lower(booked_by) in ('customer', 'api') or booked_by is null)
         and cast(v.created_at as date) >= cast('2019-01-01' as date)
 )with data on commit preserve rows;



create volatile multiset table nvp_bookings_coupons as(
select
  f.parent_order_uuid,
  min(cast(tp.created_at as date)) min_booked_at
from
   nvp_booking_coupons_tp tp
   join
   user_edwprod.fact_gbl_transactions f on f.order_id = cast(tp.order_id as varchar(64)) and f.action = 'capture' and f.user_uuid = tp.purchaser_consumer_id
   group by f.parent_order_uuid
) with data on commit preserve rows;


drop table sandbox.nvp_bt_bookings;

create multiset table sandbox.nvp_bt_bookings as (
select
    cast(parent_order_uuid as varchar(64)) parent_order_uuid,
    cast(min_booked_at as date) min_booked_at
    from nihpatel.nvp_bookings_coupons
union
select
    cast(parent_order_uuid as varchar(64)) parent_order_uuid,
    cast(min_booked_at as date) min_booked_at
    from nihpatel.nvp_bookings_vouchers
) with data

select * from sandbox.nvp_bt_bookings;
--Redemptions---


drop table sandbox.nvp_bt_reds;

drop table sandbox.nvp_bt_reds;
create multiset table sandbox.nvp_bt_reds as
(
select parent_order_uuid, usage_state_id, usage_date, redeem_date
  from
  (select cast(billing_id as varchar(64)) as parent_order_uuid,
        cast(max(usage_state_id) as integer) usage_state_id,
        cast(min(usage_date) as date) usage_date,
        cast(min(case when usage_state_id = 2 then last_modified end) as date) redeem_date
    from dwh_base_sec_view.vouchers
    where cast(created_at as date) >= cast('2019-01-01' as date)
    group by billing_id
union
    select
        cast(parent_order_uuid as varchar(64)) as parent_order_uuid,
        cast(max(case when customer_redeemed = 1 then 2 else 0 end) as integer) usage_state_id,
        cast(min(customer_redeemed_at) as date) usage_date,
        cast(min(case when customer_redeemed = 1 then updated_at end) as date) redeem_date
    from user_gp.camp_membership_coupons v
    join user_edwprod.fact_gbl_transactions f on f.user_uuid = v.purchaser_consumer_id  and cast(f.order_id as varchar(64))= cast(v.order_id as varchar(64))
    where f.country_id = '235' and cast(v.created_at as date) >= cast('2019-01-01' as date)
    group by parent_order_uuid) a
) with data

select
top 5 a.deal_id
from dwh_base_sec_view.vouchers a
  join user_edwprod.fact_gbl_transactions b
  on a.deal_id = b.deal_uuid
 where length(a.deal_id) > 10;


---------is_expired

drop table sandbox.nvp_bt_expired;

create multiset table sandbox.nvp_bt_expired as (
select billing_id,
        is_expired
  from
        (select
                billing_id,
                case when current_date >= valid_before then 1 else 0 end is_expired
            from dwh_base_sec_view.vouchers
            group by
                billing_id,
                case when current_date >= valid_before then 1 else 0 end
        union
          select
             parent_order_uuid billing_id,
             expired_yn is_expired
          from
               (select  
                   f.parent_order_uuid,
                   expired_yn
                from user_groupondw.acctg_red_voucher_base vb
                join user_gp.camp_membership_coupons cp on cp.code = vb.code and cp.purchaser_consumer_id = vb.user_uuid
                join user_edwprod.fact_gbl_transactions f on f.user_uuid = cp.purchaser_consumer_id  and f.order_id = cast(cp.order_id as varchar(64))
                group by f.parent_order_uuid, expired_yn
                ) v
              group by  parent_order_uuid, expired_yn) a
 ) with data



--- trafficsource ---

---too much to process
create multiset table sandbox.nvp_bt_traffic_source as (
    select deal_uuid, event_date, traffic_source, platform, cookie_b unique_deal_views
    from (
      select
      deal_uuid,
      event_date,
      cookie_b,
      max(cookie_first_traf_source) traffic_source,
      max(cookie_first_sub_platform) platform
      from user_groupondw.gbl_traffic_superfunnel_deal
      where event_date >= '2018-01-01'
      group by deal_uuid, event_date, cookie_b
    ) t
    group by deal_uuid, event_date, traffic_source, platform, cookie_b) with data
    ;


---------rt_bt_txns

drop table sandbox.nvp_bt_txns;
create multiset table sandbox.nvp_bt_txns as (
select
  fgt.deal_uuid deal_uuid,
  fgt.parent_order_uuid parent_order_uuid,
  fgt.user_uuid user_uuid,
  max(c.country_iso_code_2) country_code,
  max(case when b.parent_order_uuid is not null then 1 else 0 end) booked,
  max(case when r.usage_state_id = 2 then 1 else 0 end) redeemed,
  max(case when (refund_amount_loc * coalesce(approved_avg_exchange_rate,1) <> 0) then 1 else 0 end) is_refunded,
  max(ex.is_expired) is_expired,
  min(transaction_date) order_date,
  min(min_booked_at) book_date,
  min(usage_date) usage_date,
  min(case when usage_state_id = 2 then redeem_date end) redeem_date,
  sum(case when action = 'authorize' then transaction_qty end) units,
  sum(capture_nob_loc * coalesce(approved_avg_exchange_rate,1)) nob,
  sum(capture_nor_loc * coalesce(approved_avg_exchange_rate,1)) nor
from user_edwprod.fact_gbl_transactions fgt
      left join sandbox.nvp_bt_bookings b on fgt.parent_order_uuid = b.parent_order_uuid
      left join sandbox.nvp_bt_reds r on fgt.parent_order_uuid = r.parent_order_uuid
      left join sandbox.nvp_bt_expired ex on ex.billing_id = fgt.parent_order_uuid
      join user_groupondw.dim_day dd on dd.day_rw = fgt.order_date
      join (select
                  currency_from,
                  currency_to,
                  fx_neutral_exchange_rate,
                  approved_avg_exchange_rate,
                  period_key
               from user_groupondw.gbl_fact_exchange_rate
                  where currency_to = 'USD'
                  group by
                  currency_from,
                  currency_to,
                  fx_neutral_exchange_rate,
                  approved_avg_exchange_rate,
                  period_key
                  ) er on fgt.currency_code = er.currency_from and dd.month_key  = er.period_key
      join user_groupondw.gbl_dim_country c on fgt.country_id = c.country_key
where fgt.order_date >= cast('2019-01-01' as date) and is_zero_amount = 0
group by
    fgt.deal_uuid,
    fgt.parent_order_uuid,
    fgt.user_uuid
   ) with data;
