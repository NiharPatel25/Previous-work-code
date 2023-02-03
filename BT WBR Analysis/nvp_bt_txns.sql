
-----Source copy of sh_bt_bookings_rebuild in teradata
------USE THIS sandbox.sh_bt_bookings_rebuild;

CREATE MULTISET TABLE sandbox.nvp_bt_bookings_rebuild ,NO FALLBACK ,
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

-----source copy for sh_bt_active_deals_log

select * from sandbox.sh_bt_bookings_rebuild;



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

select * from grp_gdoop_bizops_db.sh_bt_bookings_rebuild;



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

SELECT a.deal_id, b.order_id, c.merchant_redeemed
FROM sb_rmaprod.vw_rev_mgmt_gbl_deal_attributes a
JOIN user_edwprod.fact_gbl_transactions b
     on a.deal_id = b.deal_uuid
JOIN user_gp.camp_membership_coupons c
     on cast(b.order_id as varchar(64))  = cast(c.order_id as varchar(64))
WHERE b.order_date BETWEEN '2022-11-01' AND '2022-11-30'
AND is_bookable = '1'
GROUP BY 1,2,3;

select max(last_update) from sandbox.sh_bt_bookings_rebuild;

select max(load_date) from sandbox.sh_bt_active_deals_log;


--Bookings--


----intl 
drop table nvp_bookings_vouchers;
create volatile multiset table nvp_bookings_vouchers as(
select parent_order_uuid, min_booked_at, BAP
from (
  select
        v.billing_id as parent_order_uuid,
        min(cast(b.created_at as timestamp)) min_booked_at, 
        max(case when booked_by = 'api' and substr(b.created_at,1,10) = substr(cast(v.created_at as varchar(50)),1,10) then 1 end) BAP
    from dwh_base_sec_view.vouchers v
          join sandbox.sh_bt_bookings_rebuild b on b.voucher_code = v.voucher_code and b.security_code = v.security_code
    where is_a_groupon_booking = 1
          and (lower(booked_by) in ('customer', 'api') or booked_by is null)
          and cast(v.created_at as date) >= cast('2019-01-01' as date)
    group by v.billing_id
  ) a
) with data on commit preserve rows;

----US
create volatile multiset table nvp_booking_coupons_tp as
(select
     cast(b.created_at as timestamp) created_at,
     v.purchaser_consumer_id,
     v.order_id,
     b.deal_uuid,
     case when booked_by = 'api' and substr(b.created_at,1,10) = substr(cast(v.created_at as varchar(50)),1,10) then 1 end BAP
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
  min(cast(tp.created_at as date)) min_booked_at, 
  max(BAP) BAP
from
   nvp_booking_coupons_tp tp
   join
   user_edwprod.fact_gbl_transactions f on f.order_id = cast(tp.order_id as varchar(64)) and f.action = 'capture' and f.user_uuid = tp.purchaser_consumer_id
   group by f.parent_order_uuid
) with data on commit preserve rows;

---final parents orders booked
drop table sandbox.nvp_bt_bookings;
create multiset table sandbox.nvp_bt_bookings as (
select
    cast(parent_order_uuid as varchar(64)) parent_order_uuid,
    cast(min_booked_at as date) min_booked_at,
    BAP
    from nihpatel.nvp_bookings_coupons
union
select
    cast(parent_order_uuid as varchar(64)) parent_order_uuid,
    cast(min_booked_at as date) min_booked_at,
    BAP
    from nihpatel.nvp_bookings_vouchers
) with data;





--Redemptions---



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

select * from sandbox.nvp_bt_reds where parent_order_uuid = 'aa4932da-90c1-4660-a61b-0167d97f7dd8';
select * from sandbox.nvp_bt_bookings where parent_order_uuid = 'aa4932da-90c1-4660-a61b-0167d97f7dd8';
select * from dwh_base_sec_view.vouchers where billing_id = 'aa4932da-90c1-4660-a61b-0167d97f7dd8';
select * from sandbox.sh_bt_bookings_rebuild where voucher_code = 'VS-TJZ4-4SYJ-XPPL-NW5S' and security_code = 'E2FE4Y496R'


---------is_expired

drop table sandbox.nvp_bt_expired;
create multiset table sandbox.nvp_bt_expired as (
select billing_id,
        max(is_expired) is_expired
  from
        (select
                billing_id,
                max(case when current_date >= valid_before then 1 else 0 end) is_expired
            from dwh_base_sec_view.vouchers
            where cast(created_at as date) >= cast('2019-01-01' as date)
            group by
                billing_id
        union
          select
             parent_order_uuid billing_id,
             max(expired_yn) is_expired
          from
               (select  
                   f.parent_order_uuid,
                   expired_yn
                from user_groupondw.acctg_red_voucher_base vb
                join user_gp.camp_membership_coupons cp on cp.code = vb.code and cp.purchaser_consumer_id = vb.user_uuid
                join user_edwprod.fact_gbl_transactions f on f.user_uuid = cp.purchaser_consumer_id  and f.order_id = cast(cp.order_id as varchar(64))
                where cast(cp.created_at as date) >= cast('2019-01-01' as date)
                ) v
              group by  parent_order_uuid) a
         group by billing_id
 ) with data

select * from dwh_base_sec_view.vouchers where billing_id = '94663883-9335-4a7a-b7a4-b4759be42b35';

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

drop table nvp_bt_temp;
create multiset volatile table nvp_bt_temp as 
(select 
   a.parent_order_uuid, 
   a.order_id,
   sum(total_estimated_ogp_loc) ogp
from 
   user_edwprod.fact_gbl_ogp_transactions as a 
   join user_edwprod.fact_gbl_transactions as b on a.parent_order_uuid = b.parent_order_uuid and a.order_id = b.order_id and b."action" = 'authorize'
   where a.order_date >= cast('2020-01-01' as date) and a."action" = 'authorize'
   group by 1,2) with data on commit preserve rows;
   
drop table sandbox.nvp_bt_txns;
create multiset table sandbox.nvp_bt_txns as (
select
  fgt.deal_uuid deal_uuid,
  fgt.parent_order_uuid parent_order_uuid,
  fgt.user_uuid user_uuid,
  max(c.country_iso_code_2) country_code,
  max(case when b.parent_order_uuid is not null then 1 else 0 end) booked,
  max(b.BAP) bap,
  max(case when r.usage_state_id = 2 then 1 else 0 end) redeemed,
  max(case when (refund_amount_loc * coalesce(approved_avg_exchange_rate,1) <> 0) then 1 else 0 end) is_refunded,
  max(ex.is_expired) is_expired,
  min(transaction_date) order_date,
  min(min_booked_at) book_date,
  min(usage_date) usage_date,
  min(case when usage_state_id = 2 then redeem_date end) redeem_date,
  sum(case when action = 'authorize' then transaction_qty end) units,
  sum(capture_nob_loc * coalesce(approved_avg_exchange_rate,1)) nob,
  sum(capture_nor_loc * coalesce(approved_avg_exchange_rate,1)) nor, 
  sum(ogp.ogp * coalesce(approved_avg_exchange_rate,1)) ogp
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
      left join nvp_bt_temp as ogp on fgt.order_id  = ogp.order_id and fgt.parent_order_uuid  = ogp.parent_order_uuid and fgt.action = 'authorize'
where fgt.order_date >= cast('2020-01-01' as date) and is_zero_amount = 0
group by
    fgt.deal_uuid,
    fgt.parent_order_uuid,
    fgt.user_uuid
   ) with data;





select 
   country_code,
   bookable,
   count(distinct deal_uuid) deals,
   count(distinct parent_order_uuid) total_orders, 
   count(distinct user_uuid) total_users,
   sum(ogp) ogp
from 
(select 
   a.deal_uuid, 
   a.parent_order_uuid,
   a.user_uuid, 
   a.country_code,
   a.booked,
   a.ogp,
   case when b.deal_uuid is not null then 1 else 0 end bookable, 
   c.l2
from sandbox.nvp_bt_txns as a 
left join 
    (select 
      load_date,
      deal_uuid
      from sandbox.sh_bt_active_deals_log 
      where is_bookable = 1 and product_is_active_flag = 1 and partner_inactive_flag = 0) as b on a.order_date  = b.load_date and a.deal_uuid = b.deal_uuid
join (select * from sandbox.pai_deals where l2 in ('TTD - Leisure','HBW','F&D')) as c on a.deal_uuid  = c.deal_uuid
where year(a.order_date) = 2022 and month(a.order_date) >= 01 and month(a.order_date) <= 03
) as fin
group by 1,2
order by 1,2
;

select sum(ogp)
from sandbox.nvp_bt_txns as a
join (select * from sandbox.pai_deals where l2 in ('TTD - Leisure','HBW','F&D')) as c on a.deal_uuid  = c.deal_uuid
where year(a.order_date) = 2022 and month(a.order_date) = 01
;

select * from np_temp_bookable;

drop table np_temp_bookable;
create multiset volatile table np_temp_bookable as (
select
   user_uuid, 
   max(country_code) country_code,
   max(order_date) order_date,
   max(deal_uuid) deal_uuid,
   max(booked) booked,
   max(bookable) bookable,
   max(redeemed) redeemed,
   sum(ogp) ogp
from
(select 
   a.*,
   case when b.deal_uuid is not null then 1 else 0 end bookable, 
   DENSE_RANK () over(partition by user_uuid order by order_date asc) ranked
from 
   sandbox.nvp_bt_txns as a 
   left join 
    (select 
      load_date,
      deal_uuid
      from sandbox.sh_bt_active_deals_log 
      where is_bookable = 1 and product_is_active_flag = 1 and partner_inactive_flag = 0) as b on a.order_date  = b.load_date and a.deal_uuid = b.deal_uuid
   join (select * from sandbox.pai_deals where l2 in ('TTD - Leisure','HBW','F&D')) as c on a.deal_uuid  = c.deal_uuid
   where year(a.order_date) = 2022 and month(a.order_date) = 01
) as fin 
where ranked = 1
group by 1) with data on commit preserve rows
;



create multiset volatile table np_temp_purch as (
select 
   a.*, 
   b.parent_order_uuid, 
   b.order_date order_date_two,
   b.ogp ogp_two
from np_temp_bookable as a 
left join 
    (select a.* 
      from sandbox.nvp_bt_txns as a
      join (select * from sandbox.pai_deals where l2 in ('TTD - Leisure','HBW','F&D')) as c on a.deal_uuid  = c.deal_uuid
      where order_date >= cast('2022-01-01' as date) and order_date < cast('2022-04-01' as date)
      ) as b on a.user_uuid = b.user_uuid and b.order_date > a.order_date and b.order_date <= a.order_date + 60
) with data on commit preserve rows
;

drop table np_temp_purch;
select * from np_temp_purch qualify ROW_NUMBER () over(partition by parent_order_uuid order by order_date) = 2;
select * from np_temp_purch where parent_order_uuid = '00117202-68e9-47d1-bbb1-0399f3dfbdef';
select * from sandbox.nvp_bt_txns where parent_order_uuid = '6e7b1268-c4af-44b1-ba50-82c0425c2a3e';
select * from np_temp_bookable where user_uuid = 'e1a85114-f0d2-11e1-a608-00259060afbc';


select parent_order_uuid, count(distinct country_code) contri from np_temp_purch group by 1 having contri > 1;

select
   a.*,
   b.total_users,
   b.total_initial_ogp, 
   b.total_initial_orders,
   a.additional_ogp + b.total_initial_ogp total_ogp
from 
 (select 
  bookable, 
  country_code,
  count(distinct deal_uuid) total_deals,
  count(distinct parent_order_uuid) total_additional_orders, 
  count(distinct case when parent_order_uuid is not null then user_uuid end) user_repurchased, 
  sum(ogp_two) additional_ogp
from np_temp_purch
group by 1,2) as a 
left join 
  (select bookable, 
         country_code, 
         count(distinct user_uuid) total_users, 
         sum(ogp) total_initial_ogp,
         count(distinct user_uuid) total_initial_orders 
  from np_temp_bookable
         group by 1,2
  )
         as b on a.bookable = b.bookable and a.country_code = b.country_code
order by 2,1
;


select 
   redeemed, 
   booked,
   count(distinct user_uuid) total_users, 
   count(distinct case when parent_order_uuid is not null then user_uuid end) user_repurchased, 
   count(distinct parent_order_uuid) total_additional_orders
from np_temp_purch
group by 1,2
order by 1 desc,2;

select 
  bookable, 
  count(distinct deal_uuid) total_deals,
  count(distinct parent_order_uuid) total_additional_orders, 
  count(distinct case when parent_order_uuid is not null then user_uuid end) user_repurchased, 
  sum(ogp_two) additional_ogp
from np_temp_purch
group by 1

select * from sandbox.nvp_bt_txns where redeemed  = 0 and booked = 1;

select a.country_code , count(distinct user_uuid), count(distinct a.deal_uuid)
from sandbox.nvp_bt_txns as a 
join (select * from sandbox.pai_deals where l2 in ('TTD - Leisure','HBW','F&D')) as c on a.deal_uuid  = c.deal_uuid
where year(order_date) = 2021
group by 1;

select 
  count(distinct a.deal_uuid) count_of_deals
from 
   user_groupondw.active_deals as a
join (select * from sandbox.pai_deals where l2 in ('TTD - Leisure','HBW','F&D')) as c on a.deal_uuid  = c.deal_uuid and c.l1 = 'Local'
where year(load_date) = 2021
      and available_qty > 0 
      and sold_out = 'false'
;

select 
  count(distinct a.deal_uuid) count_of_deals
from 
   (select 
      load_date,
      deal_uuid
      from sandbox.sh_bt_active_deals_log 
      where 
        is_bookable = 1 
        and product_is_active_flag = 1 
        and partner_inactive_flag = 0
        and year(load_date) = 2021 
) as a
join (select * from sandbox.pai_deals where l2 in ('TTD - Leisure','HBW','F&D')) as c on a.deal_uuid  = c.deal_uuid and c.l1 = 'Local';

-----ACTIVE DEALS
select 
  c.country_code,
  c.l2,
  count(distinct a.deal_uuid) count_of_deals
from 
   user_groupondw.active_deals as a
join (select * from sandbox.pai_deals where l2 in ('TTD - Leisure','HBW','F&D')) as c on a.deal_uuid  = c.deal_uuid and c.l1 = 'Local'
where year(load_date) = 2022 
      and month(load_date) = 01 
      and available_qty > 0 
      and sold_out = 'false'
group by 1,2
order by 1,2
;

select 
  c.country_code,
  c.l2,
  count(distinct a.deal_uuid) count_of_deals
from 
   (select 
      load_date,
      deal_uuid
      from sandbox.sh_bt_active_deals_log 
      where 
        is_bookable = 1 
        and product_is_active_flag = 1 
        and partner_inactive_flag = 0
        and year(load_date) = 2022 
        and month(load_date) = 01) as a
join (select * from sandbox.pai_deals where l2 in ('TTD - Leisure','HBW','F&D')) as c on a.deal_uuid  = c.deal_uuid and c.l1 = 'Local'
group by 1,2
order by 1,2
;





select * from sandbox.pai_deals;