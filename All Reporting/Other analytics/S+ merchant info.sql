drop table sh_fgt_ord;
create volatile table sh_fgt_ord as (
    sel fgt.*,
    gdl.grt_l2_cat_name
    from user_edwprod.fact_gbl_transactions fgt
    join (
        sel deal_uuid,
            load_date
        from sandbox.sh_bt_active_deals_log_v4
        where product_is_active_flag = 1
        and partner_inactive_flag = 0
        group by 1,2
    ) l on fgt.deal_uuid = l.deal_uuid and fgt.order_date = l.load_date
    join user_edwprod.dim_gbl_deal_lob gdl on fgt.deal_uuid = gdl.deal_id
    where 
    order_date >= cast('2021-01-01' as date) and order_date <= cast('2021-02-28' as date)
    and gdl.country_code = 'US'
    and gdl.grt_l1_cat_name = 'L1 - Local'
) with data primary index (order_id, action) on commit preserve rows;


drop table sh_fgt_ord;

select 
fgt.grt_l2_cat_name,
metal_at_close, 
case when metal_at_close in ('Silver', 'Gold', 'Platinum') then 'S+' else 'B-' end metal_category, 
count(distinct order_id) orders_bookable, 
count(distinct deal_uuid) total_deals
from 
sh_fgt_ord fgt
left join sandbox.rev_mgmt_deal_attributes mat on fgt.deal_uuid = mat.deal_id
where action = 'authorize'
group by 1,2,3
order by 1,3;



select 
    l2, 
    metal_category, 
    count(distinct deal_uuid) deals, 
    count(distinct merchant_uuid) merchants
    from
(select 
   l.deal_uuid,
   merch.merchant_uuid,
   max(gdl.grt_l2_cat_name) l2,
   max(metal_at_close) metal, 
   max(case when metal_at_close in ('Silver', 'Gold', 'Platinum') then 'S+' else 'B-' end) metal_category
   from 
    (
        sel deal_uuid,
            load_date
        from sandbox.sh_bt_active_deals_log_v4
        where product_is_active_flag = 1
        and partner_inactive_flag = 0
        AND load_date >= cast('2021-01-01' as date) and load_date <= cast('2021-02-28' as date)
        group by 1,2
    ) l 
    join user_edwprod.dim_gbl_deal_lob gdl on l.deal_uuid = gdl.deal_id
    left join (select product_uuid, max(merchant_uuid) merchant_uuid from user_edwprod.dim_offer_ext group by 1) merch on l.deal_uuid = merch.product_uuid
    left join sandbox.rev_mgmt_deal_attributes mat on l.deal_uuid = mat.deal_id
    where 
    gdl.country_code = 'US'
    and gdl.grt_l1_cat_name = 'L1 - Local'
   group by 1, 2) fin 
  group by 1,2 order by 1,2;


;
 
select 
    l2, 
    metal_category,  
    count(distinct bt_deals) bt_deals,
    count(distinct case when bt_deals is not null then merchant_uuid end) merchants_bt,
    count(distinct all_deals) deals, 
    count(distinct merchant_uuid) all_merchants
    from
(select
   a.deal_uuid all_deals, 
   l.deal_uuid bt_deals,
   merch.merchant_uuid,
   max(gdl.grt_l2_cat_name) l2,
   max(metal_at_close) metal, 
   max(case when metal_at_close in ('Silver', 'Gold', 'Platinum') then 'S+' else 'B-' end) metal_category
   from 
   ( select deal_uuid, load_date 
     from user_groupondw.active_deals 
     where AVAILABLE_QTY > 0 
     AND load_date >= cast('2021-01-01' as date) and load_date <= cast('2021-02-28' as date)) as a
   left join
    (
        sel deal_uuid,
            load_date
        from sandbox.sh_bt_active_deals_log_v4
        where product_is_active_flag = 1
        and partner_inactive_flag = 0
        AND load_date >= cast('2021-01-01' as date) and load_date <= cast('2021-02-28' as date)
        group by 1,2
    ) l on a.deal_uuid = l.deal_uuid and a.load_date = l.load_date
    join user_edwprod.dim_gbl_deal_lob gdl on a.deal_uuid = gdl.deal_id
    left join (select product_uuid, max(merchant_uuid) merchant_uuid from user_edwprod.dim_offer_ext group by 1) merch on a.deal_uuid = merch.product_uuid
    left join sandbox.rev_mgmt_deal_attributes mat on a.deal_uuid = mat.deal_id
    where 
    gdl.country_code = 'US'
    and gdl.grt_l1_cat_name = 'L1 - Local'
   group by 1, 2,3) fin 
  group by 1,2 order by 1,2;
  
-----------------
drop table nvp_appointments_ord;
create volatile multiset table nvp_appointments_ord as (
select 
    deal_uuid, 
    min_appointment_date, 
    count(distinct order_id) orders_booked
    from
(select cmc1.order_id,
        bn.deal_uuid, 
        min(cast(cmc1.created_at as date)) created_at, 
        min(coalesce(merchant_redeemed_at, customer_redeemed_at)) min_redeem_date, 
        min(cast(substr(bn.created_at, 1,10) as date)) min_book_at,
        min(cast(substr(start_time, 1,10) as date)) min_appointment_date
 from user_gp.camp_membership_coupons cmc1
      join sandbox.sh_bt_bookings_rebuild bn on cmc1.code = bn.voucher_code and cast(cmc1.merchant_redemption_code as varchar(50)) = bn.security_code
      group by 1,2
    ) cmc
   group by 1,2
) with data on commit preserve rows;



drop table nvp_deal_avail;
create volatile multiset table nvp_deal_avail as (
select 
   l.deal_uuid,
   mat.metal_at_close, 
   case when mat.metal_at_close in ('Silver', 'Gold', 'Platinum') then 'S+' else 'B-' end metal_category, 
   gdl.grt_l2_cat_name, 
   count(l.deal_uuid) days_live, 
   sum(day_available_total) available_days, 
   sum(orders_booked) orders_booked_appointments,
   sum(case when orders_booked >= 1 then 1 else 0 end) days_with_appointments
from
(sel deal_uuid,
     load_date
 from sandbox.sh_bt_active_deals_log_v4
 where product_is_active_flag = 1
 and partner_inactive_flag = 0
 and load_date <= cast('2021-02-28' as date) and load_date >= cast('2021-01-01' as date)
 group by 1,2) l
join user_edwprod.dim_gbl_deal_lob gdl on l.deal_uuid = gdl.deal_id
left join 
  (select 
     *
   from 
   sandbox.nvp_everyday_avail) avail on l.deal_uuid = avail.deal_uuid and l.load_date = cast(avail.load_date as date)
left join sandbox.rev_mgmt_deal_attributes mat on l.deal_uuid = mat.deal_id
left join nvp_appointments_ord nao on l.deal_uuid = nao.deal_uuid and l.load_date = cast(nao.min_appointment_date as date)
where 
    gdl.country_code = 'US'
and gdl.grt_l1_cat_name = 'L1 - Local'
group by 1,2,3,4) with data on commit preserve rows;


select 
     *
from 
sandbox.nvp_everyday_avail;  


select 
grt_l2_cat_name,
metal_category, 
metal_at_close, 
count(distinct deal_uuid) total_deals,
sum(days_live) total_days, 
sum(available_days) days_with_availability,
sum(orders_booked_appointments) orders_booked_appointments,
sum(days_with_appointments) days_with_appointments
from 
nvp_deal_avail
group by 1,2,3
order by 1,3;



select 
   gdl.grt_l2_cat_name, 
   case when mat.metal_at_close in ('Silver', 'Gold', 'Platinum') then 'S+' else 'B-' end metal_category,
   sum(orders_booked) order_booked
from 
nvp_appointments_ord l
join user_edwprod.dim_gbl_deal_lob gdl on l.deal_uuid = gdl.deal_id
left join sandbox.rev_mgmt_deal_attributes mat on l.deal_uuid = mat.deal_id
where 
    gdl.country_code = 'US'
and gdl.grt_l1_cat_name = 'L1 - Local'
and min_appointment_date <= cast('2021-02-28' as date) and min_appointment_date >= cast('2021-01-01' as date)
group by 1,2
order by 1,2 desc
;



----------------------------
drop table sandbox.nvp_everyday_avail;

CREATE MULTISET TABLE sandbox.nvp_everyday_avail ,NO FALLBACK ,
      NO BEFORE JOURNAL,
      NO AFTER JOURNAL,
      CHECKSUM = DEFAULT,
      DEFAULT MERGEBLOCKRATIO,
      MAP = TD_MAP1
      (
        deal_uuid VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
        country	VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
        load_date VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
        day_available integer,
        day_available_total integer
      )
 NO PRIMARY INDEX ;


drop table grp_gdoop_bizops_db.nvp_everyday_avail;
create table grp_gdoop_bizops_db.nvp_everyday_avail stored as orc as
select
      avail.deal_uuid,
      avail.country ,
      avail.reference_date,
      max(case when gss_available_minutes > 0 then 1 end) day_available, 
      max(case when gss_total_minutes > 0 then 1 end) day_available_total
from
  (select *
       from grp_gdoop_bizops_db.jk_bt_availability_gbl_2
       where
       report_date >= cast('2020-09-01' as date)
       and days_delta <= 15
   ) avail
WHERE country= 'US' AND CAST(reference_date AS date) <= date_sub(next_day(CURRENT_DATE, 'MON'), 1)
and CAST(reference_date AS date) >= cast('2021-01-01' as date)
group by 
    avail.deal_uuid,
    avail.country ,
    avail.reference_date
;
 




