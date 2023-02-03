drop table if exists grp_gdoop_bizops_db.nvp_bt_prepurchase_st1;
CREATE table grp_gdoop_bizops_db.nvp_bt_prepurchase_st1 stored as orc as
select 
  *
from
(  select
       t.parent_order_uuid,
       t.order_date, 
       v.created_at voucher_created,
       case when v.customer_redeemed+v.merchant_redeemed >= 1 and b.state = 'confirmed' then 1 else 0 end voucher_redemption,
       b.*,
       gdl.grt_l1_cat_name,
       gdl.grt_l2_cat_name,
       gdl.grt_l3_cat_name,
       row_number() over (partition by parent_order_uuid order by cast(b.created_at as date) )rownumasc
     from user_gp.camp_membership_coupons v
     join grp_gdoop_bizops_db.sh_bt_bookings_rebuild b on b.voucher_code = v.code and b.security_code = cast(v.merchant_redemption_code as varchar(64))
     join (select 
               parent_order_uuid, 
               user_uuid, 
               order_id, 
               order_date 
           from user_edwprod.fact_gbl_transactions
           where action = 'authorize') t on t.user_uuid = v.purchaser_consumer_id  and t.order_id = v.order_id
     join 
         (select distinct 
               deal_id, 
               grt_l1_cat_name, 
               grt_l2_cat_name, 
               grt_l3_cat_name 
             from user_edwprod.dim_gbl_deal_lob) as gdl on gdl.deal_id = b.deal_uuid
     where
          b.country_id = 'US' and b.is_a_groupon_booking = 1
UNION
   select
      v.billing_id as parent_order_uuid,
      t.order_date,
      v.created_at voucher_created,
      case when v.usage_state_id = 2 and b.state = 'confirmed' then 1 else 0 end voucher_redemption,
      b.*, 
      gdl.grt_l1_cat_name,
      gdl.grt_l2_cat_name,
      gdl.grt_l3_cat_name,
      row_number() over (partition by parent_order_uuid order by cast(b.created_at as date)) rownumasc
    from dwh_base.vouchers v
    join grp_gdoop_bizops_db.sh_bt_bookings_rebuild b on b.voucher_code = v.voucher_code and b.security_code = v.security_code
    join (select 
             parent_order_uuid, 
             order_date 
          from user_edwprod.fact_gbl_transactions 
          where action = 'authorize'
          group by parent_order_uuid, order_date) as t on t.parent_order_uuid = v.billing_id
    join (select 
              distinct 
               deal_id, 
               grt_l1_cat_name, 
               grt_l2_cat_name, 
               grt_l3_cat_name 
            from 
               user_edwprod.dim_gbl_deal_lob) as gdl on gdl.deal_id = b.deal_uuid
         where
         b.is_a_groupon_booking = 1
) a;



drop table if exists grp_gdoop_bizops_db.nvp_bt_prepurchase_st1b;
CREATE table grp_gdoop_bizops_db.nvp_bt_prepurchase_st1b stored as orc as
select 
  *
from
(  select
       t.parent_order_uuid,
       t.order_date, 
       v.created_at voucher_created,
       case when v.customer_redeemed+v.merchant_redeemed >= 1 and b.state = 'confirmed' then 1 else 0 end voucher_redemption,
       b.*,
       gdl.grt_l1_cat_name,
       gdl.grt_l2_cat_name,
       gdl.grt_l3_cat_name,
       row_number() over (partition by parent_order_uuid order by cast(b.created_at as date) )rownumasc
     from user_gp.camp_membership_coupons v
        join grp_gdoop_bizops_db.sh_bt_bookings_rebuild b on b.voucher_code = v.code and b.user_uuid = v.purchaser_consumer_id
        join user_edwprod.fact_gbl_transactions t on t.user_uuid = v.purchaser_consumer_id  and t.order_id = v.order_id
        join user_edwprod.dim_gbl_deal_lob  as gdl on gdl.deal_id = t.unified_deal_id
     where
          b.country_id = 'US' and b.is_a_groupon_booking = 1 and action = 'authorize'
UNION
   select
      v.billing_id as parent_order_uuid,
      t.order_date,
      v.created_at voucher_created,
      case when v.usage_state_id = 2 and b.state = 'confirmed' then 1 else 0 end voucher_redemption,
      b.*, 
      gdl.grt_l1_cat_name,
      gdl.grt_l2_cat_name,
      gdl.grt_l3_cat_name,
      row_number() over (partition by parent_order_uuid order by cast(b.created_at as date)) rownumasc
    from dwh_base.vouchers v
    join grp_gdoop_bizops_db.sh_bt_bookings_rebuild b on b.voucher_code = v.voucher_code and b.security_code = v.security_code
    join user_edwprod.fact_gbl_transactions t on t.parent_order_uuid = v.billing_id
    join user_edwprod.dim_gbl_deal_lob  as gdl on gdl.deal_id = t.unified_deal_id
    where
         b.is_a_groupon_booking = 1 and action = 'authorize'
) a;

------Pre Purchase



drop table if exists grp_gdoop_bizops_db.nvp_bt_prepurchase_st2a;
CREATE table grp_gdoop_bizops_db.nvp_bt_prepurchase_st2a stored as orc as
select 
    country_id,
    case when country_id in ('US', 'CA') then 'NAM' else 'INTL' end as econ_region,
    grt_l2_cat_name,
    grt_l3_cat_name,
    cast(SUBSTRING(created_at,1,10) as date) voucher_first_book_date,
    count(DISTINCT 
          case when booked_by = 'api' and day(cast(voucher_created as timestamp)-cast(created_at as timestamp)) 
          between -1 and 1 then concat(voucher_code, security_code) end) total_api_same_day,
    count(DISTINCT concat(voucher_code, security_code)) total_bookings
from grp_gdoop_bizops_db.nvp_bt_prepurchase_st1
join grp_gdoop_bizops_db.jw_day_week_end we on cast(SUBSTRING(created_at,1,10) as date) = cast(we.day_rw as date)
where 
   grt_l1_cat_name = 'L1 - Local'
   and rownumasc = 1
group by 
    country_id,
    case when country_id in ('US', 'CA') then 'NAM' else 'INTL' end,
    grt_l2_cat_name,
    grt_l3_cat_name,
    cast(SUBSTRING(created_at,1,10) as date);


CREATE table grp_gdoop_bizops_db.nvp_bt_prepurchase_st2ao stored as orc as
select 
    country_id,
    case when country_id in ('US', 'CA') then 'NAM' else 'INTL' end as econ_region,
    grt_l2_cat_name,
    grt_l3_cat_name,
    cast(SUBSTRING(order_date,1,10) as date) order_date,
    count(DISTINCT 
          case when booked_by = 'api' and day(cast(voucher_created as timestamp)-cast(created_at as timestamp)) 
          between -1 and 1 then concat(voucher_code, security_code) end) total_api_same_day,
    count(DISTINCT concat(voucher_code, security_code)) total_bookings
from grp_gdoop_bizops_db.nvp_bt_prepurchase_st1
join grp_gdoop_bizops_db.jw_day_week_end we on cast(SUBSTRING(created_at,1,10) as date) = cast(we.day_rw as date)
where 
   grt_l1_cat_name = 'L1 - Local'
   and rownumasc = 1
group by 
    country_id,
    case when country_id in ('US', 'CA') then 'NAM' else 'INTL' end,
    grt_l2_cat_name,
    grt_l3_cat_name,
    cast(SUBSTRING(order_date,1,10) as date);
   



CREATE table grp_gdoop_bizops_db.nvp_bt_prepurchase_st2ab stored as orc as
select 
    country_id,
    case when country_id in ('US', 'CA') then 'NAM' else 'INTL' end as econ_region,
    grt_l2_cat_name,
    grt_l3_cat_name,
    cast(SUBSTRING(created_at,1,10) as date) voucher_first_book_date,
    count(DISTINCT 
          case when booked_by = 'api' and day(cast(voucher_created as timestamp)-cast(created_at as timestamp)) 
          between -1 and 1 then concat(voucher_code, security_code) end) total_api_same_day,
    count(DISTINCT concat(voucher_code, security_code)) total_bookings
from grp_gdoop_bizops_db.nvp_bt_prepurchase_st1b
join grp_gdoop_bizops_db.jw_day_week_end we on cast(SUBSTRING(created_at,1,10) as date) = cast(we.day_rw as date)
where
   grt_l1_cat_name = 'L1 - Local'
   and rownumasc = 1
group by 
    country_id,
    case when country_id in ('US', 'CA') then 'NAM' else 'INTL' end,
    grt_l2_cat_name,
    grt_l3_cat_name,
    cast(SUBSTRING(created_at,1,10) as date);
   
   
grp_gdoop_bizops_db.nvp_bt_prepurchase_st1b;



-------------------------------------------------------------------------------------------------------------NO SHOW

       
       
drop table if exists grp_gdoop_bizops_db.nvp_bt_prepurchase_st1;
CREATE table grp_gdoop_bizops_db.nvp_bt_prepurchase_st1 stored as orc as
select 
  *
from
(  select
       t.parent_order_uuid,
       t.order_date, 
       c.created_at voucher_created,
       case when concat(c.code, c.merchant_redemption_code) is not null and (c.customer_redeemed = 1 or c.merchant_redeemed = 1) and b.state = 'confirmed' then 1 else 0 end voucher_redemption,
       b.country_id,
       b.checked_in,
       b.merchant_uuid,
       b.deal_uuid,
       concat(b.voucher_code,b.security_code) vsc_code,
       b.booked_by,
       b.cancelled_by,
       cast(SUBSTRING(b.start_time,1,10) as date) as appointment_date,
       cast(SUBSTRING(b.created_at,1,10) as date) as book_date,
       cast(SUBSTRING(b.deleted_at,1,10) as date) as cancel_date,
       cast(coalesce(SUBSTRING(c.customer_redeemed_at,1,10) , SUBSTRING(c.merchant_redeemed_at,1,10)) as date) as redemption_date,
       gdl.grt_l1_cat_name,
       gdl.grt_l2_cat_name,
       gdl.grt_l3_cat_name,
       row_number() over (partition by parent_order_uuid order by cast(b.created_at as date) )rownumasc
     from user_gp.camp_membership_coupons c
     join grp_gdoop_bizops_db.sh_bt_bookings_rebuild b on b.voucher_code = c.code and b.security_code = cast(c.merchant_redemption_code as varchar(64))
     left join (select 
               distinct
               parent_order_uuid, 
               user_uuid, 
               order_id, 
               order_date 
           from user_edwprod.fact_gbl_transactions
           where action = 'authorize') t on t.user_uuid = c.purchaser_consumer_id  and t.order_id = c.order_id
     join 
         (select distinct 
               deal_id, 
               grt_l1_cat_name, 
               grt_l2_cat_name, 
               grt_l3_cat_name 
             from user_edwprod.dim_gbl_deal_lob) as gdl on gdl.deal_id = b.deal_uuid
     where
          b.country_id = 'US' and b.is_a_groupon_booking = 1
UNION
    select
      v.billing_id as parent_order_uuid,
      t.order_date,
      v.created_at voucher_created,
      case when concat(v.voucher_code,v.security_code) is not null and v.usage_state_id = 2 and b.state = 'confirmed' then 1 else 0 end voucher_redemption,
      b.country_id,
      b.checked_in,
      b.merchant_uuid,
      b.deal_uuid,
      concat(b.voucher_code,b.security_code) vsc_code,
      b.booked_by,
      b.cancelled_by,
      cast(SUBSTRING(b.start_time,1,10) as date) as appointment_date,
      cast(SUBSTRING(b.created_at,1,10) as date) as book_date,
      cast(SUBSTRING(b.deleted_at,1,10) as date) as cancel_date,
      cast(SUBSTRING(v.usage_date, 1,10) as date) as redemption_date,
      gdl.grt_l1_cat_name,
      gdl.grt_l2_cat_name,
      gdl.grt_l3_cat_name,
      row_number() over (partition by parent_order_uuid order by cast(b.created_at as date)) rownumasc
    from dwh_base.vouchers v
    join grp_gdoop_bizops_db.sh_bt_bookings_rebuild b on b.voucher_code = v.voucher_code and b.security_code = v.security_code
    left join (select 
             parent_order_uuid, 
             order_date 
          from user_edwprod.fact_gbl_transactions 
          where action = 'authorize'
          group by parent_order_uuid, order_date) as t on t.parent_order_uuid = v.billing_id
    join (select 
              distinct 
               deal_id, 
               grt_l1_cat_name, 
               grt_l2_cat_name, 
               grt_l3_cat_name 
            from 
               user_edwprod.dim_gbl_deal_lob) as gdl on gdl.deal_id = b.deal_uuid
         where
         b.is_a_groupon_booking = 1 and v.dwh_active = 1
) a;



create table grp_gdoop_bizops_db.nvp_bt_prepurchase_noshow stored as orc as
select 
   b.*, 
   case when state = 'confirmed' then row_number() over(partition by vsc_code , state order by appointment_date desc) end as confirm_row
from
grp_gdoop_bizops_db.nvp_bt_prepurchase_st1 b
;


drop table if exists grp_gdoop_bizops_db.nvp_bt_prepurchase_st2b;
CREATE table grp_gdoop_bizops_db.nvp_bt_prepurchase_st2b stored as orc as
select 
    country_id,
    case when country_id in ('US', 'CA') then 'NAM' else 'INTL' end as econ_region,
    grt_l2_cat_name,
    grt_l3_cat_name,
    cast(SUBSTRING(start_time,1,10) as date) appointment_date, 
    count(DISTINCT
          case when state = 'confirmed'
          then concat(voucher_code, security_code) end) total_appointments,
    count(DISTINCT case 
             when state = 'confirmed' and cast(SUBSTRING(start_time,1,10) as date) >= cast('2020-05-11' as date) and checked_in = 'no-show' and voucher_redemption = 0 then concat(voucher_code, security_code)
             when state = 'confirmed' and cast(SUBSTRING(start_time,1,10) as date) < cast('2020-05-11' as date) and checked_in = 'unknown' and voucher_redemption = 0 then concat(voucher_code, security_code)
             end) no_show
from grp_gdoop_bizops_db.nvp_bt_prepurchase_noshow
where 
   grt_l1_cat_name = 'L1 - Local'
   and cast(SUBSTRING(start_time,1,10) as date) <= CURRENT_DATE
group by 
    country_id,
    case when country_id in ('US', 'CA') then 'NAM' else 'INTL' end,
    grt_l2_cat_name,
    grt_l3_cat_name,
    cast(SUBSTRING(start_time,1,10) as date);
   
   






DROP TABLE grp_gdoop_bizops_db.nvp_bt_noshow;
create table grp_gdoop_bizops_db.nvp_bt_noshow stored as orc as
select
     l.grt_l2_cat_name,
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
from grp_gdoop_bizops_db.sh_bt_bookings_rebuild b
-- voucher joins
left join dwh_base.vouchers v on v.voucher_code = b.voucher_code and v.security_code = b.security_code and dwh_active = 1
left join user_gp.camp_membership_coupons c on c.code = b.voucher_code and cast(c.merchant_redemption_code as varchar(64)) = b.security_code
-- attributes
left join user_edwprod.dim_gbl_deal_lob l on l.deal_id = b.deal_uuid
-- redemptions
where
    is_a_groupon_booking = 1
    and b.merchant_uuid <> '886b6b3c-8298-33cf-4f5c-3210505ded00' -- BT Test Account
    --and concat(b.voucher_code,b.security_code) = 'LG-NYSL-MGL5-6Z92-3B9L15987252'
    ;


create table grp_gdoop_bizops_db.nvp_bt_noshow2 stored as orc as
select
b.*,
case when book_state = 'confirmed' then
row_number() over(partition by b.vsc_code,book_state order by book_date desc)
end as confirm_row
from grp_gdoop_bizops_db.nvp_bt_noshow b;




select 
   country_id,
    case when country_id in ('US', 'CA') then 'NAM' else 'INTL' end as econ_region,
    grt_l2_cat_name,
    appointment_date, 
    count(distinct vsc_code) appointments,  
    count(case when appointment_date >= cast('2020-05-11' as date) and checked_in = 'no-show' and voucher_redemption = 0 then vsc_code 
               when appointment_date < cast('2020-05-11' as date) and checked_in = 'unknown' and voucher_redemption = 0 then vsc_code end) no_show_appointments
   from 
   grp_gdoop_bizops_db.nvp_bt_noshow2
   where confirm_row = 1
   group by 
   country_id,
    case when country_id in ('US', 'CA') then 'NAM' else 'INTL' end as econ_region,
    grt_l2_cat_name,
    appointment_date;




drop table if exists grp_gdoop_bizops_db.nvp_bt_prepurchase_noshow_trial;
create table  grp_gdoop_bizops_db.nvp_bt_prepurchase_noshow_trial stored as orc as
select
   date_sub(next_day(appointment_date, 'MON'), 1) appointment_week,
   econ_region,
   country_id, 
   grt_l2_cat_name,
   grt_l3_cat_name,
   sum(total_appointments) total_appointments, 
   sum(no_show) no_show
from
  grp_gdoop_bizops_db.nvp_bt_prepurchase_st2b
group by 
   econ_region,
   country_id, 
   grt_l2_cat_name,
   grt_l3_cat_name,
   date_sub(next_day(appointment_date, 'MON'), 1)
   ;
  
select 
    appointment_week, 
    sum(total_appointments) appointments, 
    sum(no_show)
   from grp_gdoop_bizops_db.nvp_bt_prepurchase_noshow_trial 
where country_id <> "US"
group by appointment_week
order by appointment_week desc;



-------------------------------------------------------PREVIOUS VERSION

drop table if exists grp_gdoop_bizops_db.nvp_bt_prepurchase_st1;
CREATE table grp_gdoop_bizops_db.nvp_bt_prepurchase_st1 stored as orc as
select 
  *
from
(  select
       t.parent_order_uuid,
       t.order_date, 
       v.created_at voucher_created,
       case when v.customer_redeemed+v.merchant_redeemed >= 1 and b.state = 'confirmed' then 1 else 0 end voucher_redemption,
       b.*,
       gdl.grt_l1_cat_name,
       gdl.grt_l2_cat_name,
       gdl.grt_l3_cat_name,
       row_number() over (partition by parent_order_uuid order by b.created_at) rownumasc
     from user_gp.camp_membership_coupons v
     join grp_gdoop_bizops_db.sh_bt_bookings_rebuild b on b.voucher_code = v.code and b.security_code = cast(v.merchant_redemption_code as varchar(64))
     join (select 
               parent_order_uuid, 
               user_uuid, 
               order_id, 
               order_date 
           from user_edwprod.fact_gbl_transactions
           where action = 'authorize') t on t.user_uuid = v.purchaser_consumer_id  and t.order_id = v.order_id
     join 
         (select distinct 
               deal_id, 
               grt_l1_cat_name, 
               grt_l2_cat_name, 
               grt_l3_cat_name 
             from user_edwprod.dim_gbl_deal_lob) as gdl on gdl.deal_id = b.deal_uuid
     join grp_gdoop_bizops_db.jw_day_week_end we on cast(SUBSTRING(v.created_at,1,10) as date) = cast(we.day_rw as date)
     where
          b.is_a_groupon_booking = 1 -- and (lower(booked_by) in ('customer', 'api') or booked_by is null) 
UNION
   select
      v.billing_id as parent_order_uuid,
      t.order_date,
      v.created_at voucher_created,
      case when v.usage_state_id = 2 and b.state = 'confirmed' then 1 else 0 end voucher_redemption,
      b.*, 
      gdl.grt_l1_cat_name,
      gdl.grt_l2_cat_name,
      gdl.grt_l3_cat_name,
      row_number() over (partition by parent_order_uuid order by b.created_at) rownumasc
    from dwh_base.vouchers v
    join grp_gdoop_bizops_db.sh_bt_bookings_rebuild b on b.voucher_code = v.voucher_code and b.security_code = v.security_code
    join (select 
             parent_order_uuid, 
             order_date 
          from user_edwprod.fact_gbl_transactions 
          where action = 'authorize'
          group by parent_order_uuid, order_date) as t on t.parent_order_uuid = v.billing_id
    join (select 
              distinct 
               deal_id, 
               grt_l1_cat_name, 
               grt_l2_cat_name, 
               grt_l3_cat_name 
            from 
               user_edwprod.dim_gbl_deal_lob) as gdl on gdl.deal_id = b.deal_uuid
    join grp_gdoop_bizops_db.jw_day_week_end as we on cast(SUBSTRING(v.created_at,1,10) as date) = cast(we.day_rw as date)
     where
         b.is_a_groupon_booking = 1
) a;



------Pre Purchase



drop table if exists grp_gdoop_bizops_db.nvp_bt_prepurchase_st2a;
CREATE table grp_gdoop_bizops_db.nvp_bt_prepurchase_st2a stored as orc as
select 
    country_id,
    case when country_id in ('US', 'CA') then 'NAM' else 'INTL' end as econ_region,
    grt_l2_cat_name,
    grt_l3_cat_name,
    cast(SUBSTRING(created_at,1,10) as date) voucher_first_book_date,
    count(DISTINCT 
          case when booked_by = 'api' and day(cast(voucher_created as timestamp)-cast(created_at as timestamp)) 
          between -1 and 1 then concat(voucher_code, security_code) end) total_api_same_day,
    count(DISTINCT concat(voucher_code, security_code)) total_bookings
from grp_gdoop_bizops_db.nvp_bt_prepurchase_st1
where 
   grt_l1_cat_name = 'L1 - Local'
   and rownumasc = 1
group by 
    country_id,
    case when country_id in ('US', 'CA') then 'NAM' else 'INTL' end,
    grt_l2_cat_name,
    grt_l3_cat_name,
    cast(SUBSTRING(created_at,1,10) as date);



-------------------------------------------------------------------------------------------------------------NO SHOW

select * from user_edwprod.fact_gbl_transactions limit 5;

drop table if exists grp_gdoop_bizops_db.nvp_bt_prepurchase_st2b;
CREATE table grp_gdoop_bizops_db.nvp_bt_prepurchase_st2b stored as orc as
select 
    country_id,
    case when country_id in ('US', 'CA') then 'NAM' else 'INTL' end as econ_region,
    grt_l2_cat_name,
    grt_l3_cat_name,
    cast(SUBSTRING(start_time,1,10) as date) appointment_date, 
    count(DISTINCT
          case when state = 'confirmed'
          then concat(voucher_code, security_code) end) total_appointments,
    count(DISTINCT case 
             when state = 'confirmed' and cast(SUBSTRING(start_time,1,10) as date) >= cast('2020-05-11' as date) and checked_in = 'no-show' and voucher_redemption = 0 then concat(voucher_code, security_code)
             when state = 'confirmed' and cast(SUBSTRING(start_time,1,10) as date) < cast('2020-05-11' as date) and checked_in = 'unknown' and voucher_redemption = 0 then concat(voucher_code, security_code)
             end) no_show
from grp_gdoop_bizops_db.nvp_bt_prepurchase_st1 st1
where 
   grt_l1_cat_name = 'L1 - Local'
   and cast(SUBSTRING(start_time,1,10) as date) <= CURRENT_DATE
group by 
    country_id,
    case when country_id in ('US', 'CA') then 'NAM' else 'INTL' end,
    grt_l2_cat_name,
    grt_l3_cat_name,
    cast(SUBSTRING(start_time,1,10) as date);


drop table if exists grp_gdoop_bizops_db.nvp_bt_prepurchase_noshow_trial;
create table  grp_gdoop_bizops_db.nvp_bt_prepurchase_noshow_trial stored as orc as
select
   date_sub(next_day(appointment_date, 'MON'), 1) appointment_week,
   econ_region,
   country_id, 
   grt_l2_cat_name,
   grt_l3_cat_name,
   sum(total_appointments) total_appointments, 
   sum(no_show) no_show
from
  grp_gdoop_bizops_db.nvp_bt_prepurchase_st2b
group by 
   econ_region,
   country_id, 
   grt_l2_cat_name,
   grt_l3_cat_name,
   date_sub(next_day(appointment_date, 'MON'), 1)
   ;
  
select 
    appointment_week, 
    sum(total_appointments) appointments, 
    sum(no_show)
   from grp_gdoop_bizops_db.nvp_bt_prepurchase_noshow_trial 
where country_id <> "US"
group by appointment_week
order by appointment_week desc;


