grant all on sandbox.sh_bt_retention_view to abautista with grant option

--------------------------------CATEGORY BREAKDOWN
select * from sh_bt_launch_dates;
drop table sh_bt_launch_dates;
create volatile table sh_bt_launch_dates as (
    sel deal_uuid,
        max(has_gcal) has_gcal,
        min(load_date) launch_date,
        max(load_date) max_load_date
    from sandbox.sh_bt_active_deals_log_v4 ad
    where product_is_active_flag = 1
    and partner_inactive_flag = 0
    having launch_date >= cast('2020-10-01' as date) and launch_date < cast('2021-03-01' as date)
    group by 1
) with data unique primary index (deal_uuid) on commit preserve rows;

drop table sh_bt_retention_30;
create volatile table sh_bt_retention_30 as (
    sel ad.deal_uuid,
        max(case when ad.load_date >= cast(ld.launch_date as date) + interval '31' day then 1 else 0 end) live_after_30_days
    from sandbox.sh_bt_active_deals_log ad
    join sh_bt_launch_dates ld on ad.deal_uuid = ld.deal_uuid
    where product_is_active_flag = 1
    and partner_inactive_flag = 0
    group by 1
) with data unique primary index (deal_uuid) on commit preserve rows;

drop table sh_bt_still_live_30;
create volatile table sh_bt_still_live_30 as (
    sel ad.deal_uuid
    from user_groupondw.active_deals ad
    join sh_bt_launch_dates ld on ad.deal_uuid = ld.deal_uuid
    where ad.load_date >= cast(ld.launch_date as date) + interval '31' day
    and ad.sold_out = 'false'
    group by 1
) with data unique primary index (deal_uuid) on commit preserve rows;

drop table sh_booking_solution;
create volatile table sh_booking_solution as (
    select o2.deal_uuid,
        max(sfa.id) sf_account_id,
        max(case when lower(sfa.scheduler_setup_type) in ('pen & paper','none') then 'pen & paper'
            when sfa.scheduler_setup_type is null then 'no data'
            else 'some booking tool'
            end) current_booking_solution,
        max(sfa.scheduler_setup_type) detailed_booking_solution,
        max(sfa.name) account_name,
        max(company_type) company_type
    from dwh_base_sec_view.sf_opportunity_1 o1
    join dwh_base_sec_view.sf_opportunity_2 o2 on o1.id = o2.id
    join dwh_base_sec_view.sf_account sfa on o1.accountid = sfa.id
    group by o2.deal_uuid
) with data unique primary index (deal_uuid) on commit preserve rows;


drop table sh_fgt_ord;
create volatile table sh_fgt_ord as (
    sel fgt.*,
        d.max_load_date
    from user_edwprod.fact_gbl_transactions fgt
    join sh_bt_launch_dates d on fgt.deal_uuid = d.deal_uuid
    where order_date >= launch_date and order_date <= launch_date + 31
) with data primary index (order_id, action) on commit preserve rows;


drop table sh_booked;
create volatile table sh_booked as (
    sel f.order_id,
        max(case when bn.voucher_code is not null then 1 else 0 end) booked_bt,
        max(case when bn.booked_by = 'api' and substr(bn.created_at,1,10) = substr(cast(cmc.created_at as varchar(50)),1,10) then 1 else 0 end) prepurchase_bookings
    from sh_fgt_ord f
    join user_gp.camp_membership_coupons cmc on f.order_id = cast(cmc.order_id as varchar(64))
    left join sandbox.sh_bt_bookings_rebuild bn on cmc.code = bn.voucher_code and cast(cmc.merchant_redemption_code as varchar(50)) = bn.security_code
    group by 1
) with data unique primary index (order_id) on commit preserve rows;

drop table sh_appointments;
create volatile table sh_appointments as (
select
   a.deal_uuid,
   count(distinct booking_id) total_appointments,
   count(distinct case when state = 'cancelled' then booking_id end) cancelled_appointments,
   count(distinct case when state = 'confirmed' then booking_id end) confirmed_appointments
   from
   sh_bt_launch_dates as a
   left join sandbox.sh_bt_bookings_rebuild as bn
             on a.deal_uuid = bn.deal_uuid
             and cast(substr(created_at, 1,10) as date) >= cast(launch_date as date)
             and cast(substr(created_at, 1,10) as date) <= cast(launch_date as date) + 31
             and bn.country_id = 'US'
   group by 1
  )with data unique primary index (deal_uuid) on commit preserve rows;



drop table sandbox.sh_bt_retention_view_temp_2;
create table sandbox.sh_bt_retention_view_temp_2 as (
    sel cast(dm.month_start as date) report_mth,
        case when gdl.country_code in ('US','CA') then 'NAM' else 'INTL' end region,
        gdl.grt_l2_cat_description,
        ret_v.pds_cat_name,
        case when lower(sbs.current_booking_solution) = 'pen & paper' then 'p&p' else 'other' end booking_type,
        ld.has_gcal,
        case when has_gcal = 1 then 'gcal' else booking_type end OKR_grouping,
        count(distinct ld.deal_uuid) n_deals_launched_live30,
        count(distinct case when live_after_30_days = 1 then ld.deal_uuid end) n_deals_retained,
        sum(transaction_qty) units_sold,
        sum(case when booked_bt = 1 then transaction_qty end) bt_units_sold,
        sum(case when live_after_30_days = 1 then transaction_qty end) units_live_30_days,
        sum(case when live_after_30_days = 1 and booked_bt = 1 then transaction_qty end) units_live_30_days_bt
    from sh_bt_launch_dates ld
    join user_groupondw.dim_day dd on ld.launch_date = dd.day_rw
    join user_groupondw.dim_month dm on dd.month_key = dm.month_key
    join user_edwprod.dim_gbl_deal_lob gdl on ld.deal_uuid = gdl.deal_id
    join sh_bt_still_live_30 l30 on ld.deal_uuid = l30.deal_uuid
    join sh_booking_solution sbs on ld.deal_uuid = sbs.deal_uuid
    left join sh_bt_retention_30 r30 on ld.deal_uuid = r30.deal_uuid
    left join user_dw.v_dim_pds_grt_map ret_v on gdl.pds_cat_id = ret_v.pds_cat_id
    left join sh_fgt_ord as ord on ld.deal_uuid = ord.deal_uuid and ord.action = 'authorize'
    left join sh_booked as bk on ord.order_id = bk.order_id
    where gdl.grt_l2_cat_description in ('F&D','HBW','TTD - Leisure')
    and not (gdl.grt_l2_cat_description = 'F&D' and gdl.country_code in ('US','CA'))
    and launch_date >= '2020-07-01'
    group by
        1,2,3,4,5,6
) with data no primary index;




drop table sandbox.sh_bt_retention_view_temp_3;
create table sandbox.sh_bt_retention_view_temp_3 as
(   sel cast(dm.month_start as date) report_mth,
        ld.deal_uuid,
        case when gdl.country_code in ('US','CA') then 'NAM' else 'INTL' end region,
        gdl.grt_l2_cat_description,
        ret_v.pds_cat_name,
        case when lower(sbs.current_booking_solution) = 'pen & paper' then 'p&p' else 'other' end booking_type,
        ld.has_gcal,
        case when has_gcal = 1 then 'gcal' else booking_type end OKR_grouping,
        case when l30.deal_uuid is not null then 1 else 0 end deals_live,
        case when r30.live_after_30_days = 1 then 1 else 0 end n_deals_retained,
        CAST(ld.max_load_date AS DATE) - CAST(launch_date AS DATE) num_of_days_before_leaving_bt,
        sum(transaction_qty) units_sold,
        sum(case when bk.booked_bt = 1 then transaction_qty end) booked_units,
        case
            when (units_sold is null or units_sold = 0) then 'a.no units sold'
            when units_sold <= 5 then 'b.<= 5 units sold'
            when units_sold <= 10 then 'c.<= 10 units sold'
            when units_sold <= 15 then 'd. <= 15 units sold'
            else 'e.> 15 units sold'
            end units_cohort,
        total_appointments,
        cancelled_appointments,
        confirmed_appointments,
        coalesce(dal.num_dow_total, dal2.num_dow_total) num_dow_total,
        case when total_appointments = 0 or total_appointments is null then 'a.<= no appointments sold'
             when total_appointments <= 5 then 'b.<= 5 appointments sold'
             when total_appointments <= 10 then 'c.<= 10 appointments sold'
             when total_appointments <= 15 then 'd.<= 15 appointments sold'
             when total_appointments <= 20 then 'e.<= 20 appointments sold'
             else 'f.> 20 appointments sold'
             end  appointment_cohort
    from sh_bt_launch_dates ld
    join user_groupondw.dim_day dd on ld.launch_date = dd.day_rw
    join user_groupondw.dim_month dm on dd.month_key = dm.month_key
    join user_edwprod.dim_gbl_deal_lob gdl on ld.deal_uuid = gdl.deal_id
    left join sh_bt_still_live_30 l30 on ld.deal_uuid = l30.deal_uuid ----based on active
    join sh_booking_solution sbs on ld.deal_uuid = sbs.deal_uuid
    left join sh_bt_retention_30 r30 on ld.deal_uuid = r30.deal_uuid ---based on bt
    left join user_dw.v_dim_pds_grt_map ret_v on gdl.pds_cat_id = ret_v.pds_cat_id
    left join sh_fgt_ord as ord on ld.deal_uuid = ord.deal_uuid and ord.action = 'authorize'
    left join sh_booked as bk on ord.order_id = bk.order_id
    left join sh_appointments as apt on ld.deal_uuid = apt.deal_uuid
    left join (
      sel deal_uuid,
          load_week,
          num_dow,
          num_dow_total
          from sandbox.nvp_weekwise_avail
          qualify row_number() over (partition by deal_uuid order by load_week desc) = 1
    ) dal on ld.deal_uuid = dal.deal_uuid
    left join (
      sel deal_uuid,
          country,
          load_week,
          num_dow,
          num_dow_total
          from sandbox.nvp_weekwise_avail_intl
          qualify row_number() over (partition by deal_uuid, country order by load_week desc) = 1
    ) dal2 on ld.deal_uuid = dal2.deal_uuid and gdl.country_code = dal2.country
    where gdl.grt_l2_cat_description in ('F&D','HBW','TTD - Leisure')
    and not (gdl.grt_l2_cat_description = 'F&D' and gdl.country_code in ('US','CA'))
    and launch_date >= '2020-07-01'
    group by 1,2,3,4,5,6,7,8,9,10,11,15,16,17,18
) with data no primary index;


select * from sandbox.sh_bt_retention_view_temp_3;


select * from  sh_bt_paused_reason where deal_uuid = 'f90de4b0-52e4-4a0a-b1a3-e42708e0744e'



select max(load_date) from sandbox.sh_bt_active_deals_log where deal_uuid = 'f90de4b0-52e4-4a0a-b1a3-e42708e0744e' and is_bookable = 1
-------------------------------------------Highest booked TTD PDS's


-------------------------------------------% of deals live
drop table sh_bt_launch_dates;
create volatile table sh_bt_launch_dates as (
    sel deal_uuid,
        max(has_gcal) has_gcal,
        min(load_date) launch_date,
        max(load_date) max_load_date
    from sandbox.sh_bt_active_deals_log_v4 ad
    where product_is_active_flag = 1
    and partner_inactive_flag = 0
    and load_date >= '2019-04-01'
    group by 1
) with data unique primary index (deal_uuid) on commit preserve rows;

drop table np_new_old_deal;
create volatile table np_new_old_deal as (
select 
      a.load_date, 
      a.deal_uuid, 
      gdl.grt_l2_cat_description,
      ret_v.pds_cat_name,
      b.launch_date,
      case when cast(a.load_date as date) <= cast(b.launch_date as date) + 31 then 1 else 0 end new_deal
from 
      sandbox.sh_bt_active_deals_log as a 
      left join sh_bt_launch_dates as b on a.deal_uuid = b.deal_uuid
      join user_edwprod.dim_gbl_deal_lob gdl on a.deal_uuid = gdl.deal_id AND gdl.country_code = 'US'
      left join
       user_dw.v_dim_pds_grt_map ret_v on gdl.pds_cat_id = ret_v.pds_cat_id
where a.is_bookable = 1 
      and a.product_is_active_flag = 1 
      and a.partner_inactive_flag = 0 
      and a.country = 'US'
      and cast(a.load_date as date)>= cast('2020-10-01' as date) and cast(a.load_date as date) < cast('2021-03-01' as date)
) with data on commit preserve rows;

select * from np_new_old_deal;

drop table sandbox.np_pds_deal;
create multiset table sandbox.np_pds_deal as (
select 
   load_date, 
   grt_l2_cat_description, 
   pds_cat_name,
   count(distinct a.deal_uuid) total_deals, 
   count(distinct case when new_deal = 1 then a.deal_uuid end) new_deals, 
   count(distinct apt.booking_id) total_appointments, 
   count(distinct case when new_deal = 1 then apt.booking_id end) new_deals_appointments
from np_new_old_deal as a 
    left join sandbox.sh_bt_bookings_rebuild as apt on a.deal_uuid = apt.deal_uuid and cast(substr(apt.created_at, 1,10) as date) = cast(a.load_date as date)
   group by 1,2,3)
with data;

drop table sandbox.np_pds_deal2;
create multiset table sandbox.np_pds_deal2 as (
select 
     a.l2,
     a.pds,
     average_deals_live,
     apt.total_appointments,
     cast(apt.total_appointments as float)/average_deals_live appointments_per_deals_live, 
     cancelled_appointments, 
     confirmed_appointments
     from
     (select 
         grt_l2_cat_description l2, 
         pds_cat_name pds,
         count(distinct concat(deal_uuid,load_date))/count(distinct load_date) average_deals_live
      from np_new_old_deal
      group by 1,2) as a 
   left join 
      (select 
          gdl.grt_l2_cat_description l2,
          pds_cat_name pds,
          count(distinct booking_id) total_appointments,
          count(distinct case when state = 'cancelled' then booking_id end) cancelled_appointments,
          count(distinct case when state = 'confirmed' then booking_id end) confirmed_appointments
       from
           sandbox.sh_bt_bookings_rebuild as a
       join 
           user_edwprod.dim_gbl_deal_lob gdl on a.deal_uuid = gdl.deal_id AND gdl.country_code = 'US'
       left join
           user_dw.v_dim_pds_grt_map ret_v on gdl.pds_cat_id = ret_v.pds_cat_id
       where
           cast(substr(created_at, 1,10) as date) >= cast('2020-10-01' as date)
           and cast(substr(created_at, 1,10) as date) < cast('2021-03-01' as date)
       group by 1,2
      ) as apt on a.l2 = apt.l2 and a.pds = apt.pds
)with data;


select * from sandbox.np_pds_deal2;

select * from sandbox.sh_bt_bookings_rebuild 
where deal_uuid = '925e9265-bef4-4071-93cc-639ada199f1c';




---------------------------------------------- PDS's With most appointments
------QUESTION: Are we adding to PDS's that have low appointments





drop table sh_appointments;
create volatile table sh_appointments as (
select
   a.deal_uuid,
   gdl.country_code,
   gdl.grt_l2_cat_description,
   ret_v.pds_cat_name,
   count(distinct booking_id) total_appointments,
   count(distinct case when state = 'cancelled' then booking_id end) cancelled_appointments,
   count(distinct case when state = 'confirmed' then booking_id end) confirmed_appointments
   from
      sandbox.sh_bt_bookings_rebuild a
    join 
         user_edwprod.dim_gbl_deal_lob gdl on a.deal_uuid = gdl.deal_id AND gdl.country_code = 'US'
    left join
       user_dw.v_dim_pds_grt_map ret_v on gdl.pds_cat_id = ret_v.pds_cat_id
    where
        cast(substr(a.created_at, 1,10) as date) >= cast('2020-10-01' as date) 
          and cast(substr(a.created_at, 1,10) as date) < cast('2021-03-01' as date)
   group by 1,2,3,4
  )with data unique primary index (deal_uuid) on commit preserve rows;



drop table sh_fgt_pds;
create multiset volatile table sh_fgt_pds as (
    sel
        gdl.grt_l2_cat_description,
        ret_v.pds_cat_name, 
        sum(transaction_qty)  units
    from user_edwprod.fact_gbl_transactions fgt
    join user_edwprod.dim_gbl_deal_lob gdl on fgt.deal_uuid = gdl.deal_id and gdl.country_code = 'US'
    join
       (select
          product_uuid
        from
          user_edwprod.dim_offer_ext
        where inventory_service_name <> 'tpis'
        group by product_uuid) c on fgt.deal_uuid = c.product_uuid
    left join user_dw.v_dim_pds_grt_map ret_v on gdl.pds_cat_id = ret_v.pds_cat_id
    where 
       order_date >= cast('2020-10-01' as date) 
       and order_date < cast('2021-03-01' as date)
       and fgt.action = 'authorize'
    group by 1,2
) with data on commit preserve rows;


select 
   coalesce(a.grt_l2_cat_description, b.grt_l2_cat_description) l2,
   coalesce(a.pds_cat_name, b.pds_cat_name) pds, 
   sum(total_appointments) appointments, 
   sum(cancelled_appointments) cancelled_appointments, 
   sum(confirmed_appointments) confirmed_appointments, 
   sum(units) units
from 
   (select grt_l2_cat_description, 
        pds_cat_name,
       count(distinct deal_uuid) TTD_deals_live,
       count(distinct case when total_appointments is not null then deal_uuid end) TTD_deals_with_appointments,
       sum(total_appointments) total_appointments, 
       sum(cancelled_appointments) cancelled_appointments, 
       sum(confirmed_appointments) confirmed_appointments
       from 
       sh_appointments group by 1,2) as a 
   full join sh_fgt_pds as b on a.grt_l2_cat_description = b.grt_l2_cat_description and a.pds_cat_name = b.pds_cat_name
  group by 1,2
  order by 1,2

  
select 
     grt_l2_cat_description,
     pds_cat_name, 
     count(distinct deal_uuid) deals_launched_bt, 
     count(distinct case when deals_live = 0 then deal_uuid end) active_on
     count(distinct case when deals_live = 1 and n_deals_retained = 0 then deal_uuid end ) active_on_groupon,
     count(distinct case when n_deals_retained = 1 then deal_uuid end) bt_retained_30_days
from sandbox.sh_bt_retention_view_temp_3;



drop table sh_booked_pds;
create volatile table sh_booked_pds as (
    sel f.order_id,
        max(case when bn.voucher_code is not null then 1 else 0 end) booked_bt,
        max(case when bn.booked_by = 'api' and substr(bn.created_at,1,10) = substr(cast(cmc.created_at as varchar(50)),1,10) then 1 else 0 end) prepurchase_bookings
    from sh_fgt_pds f
    join user_gp.camp_membership_coupons cmc on f.order_id = cast(cmc.order_id as varchar(64))
    left join sandbox.sh_bt_bookings_rebuild bn on cmc.code = bn.voucher_code and cast(cmc.merchant_redemption_code as varchar(50)) = bn.security_code
    group by 1
) with data unique primary index (order_id) on commit preserve rows;



drop table all_pds_ttd;

create volatile table all_pds_ttd as (
select
    a.*,
    b.booked_bt,
    b.prepurchase_bookings
from
   sh_fgt_pds as a
   left join
   sh_booked_pds as b on a.order_id = b.order_id
   where a.action = 'authorize') with data on commit preserve rows;

select * from all_pds_ttd where booked_bt is null;

select
   pds_cat_name,
   sum(transaction_qty) units,
   sum(case when booked_bt = 1 then transaction_qty end) booked_units,
   sum(case when booked_bt = 0 then transaction_qty end) non_booked_units
from all_pds_ttd
where booked_bt is not null
group by 1;



select 
   pds_cat_name, 
   count(distinct product_uuid)
from 
(select 
 a.product_uuid, 
 b.grt_l2_cat_description,
 ret_v.pds_cat_name
from sandbox.bzops_booking_deals as a 
join
   (sel ad.deal_uuid
    from user_groupondw.active_deals ad
    where cast(load_date as date) = current_date - 2
    group by 1) a2 on a.product_uuid = a2.deal_uuid
join user_edwprod.dim_gbl_deal_lob as b on a.product_uuid = b.deal_id
    left join user_dw.v_dim_pds_grt_map ret_v on b.pds_cat_id = ret_v.pds_cat_id
where a.country_id = 235  and b.grt_l2_cat_description = 'TTD - Leisure') as fin 
group by 1
order by 2 desc;




-------------------------------Retention Curve


drop table sh_bt_launch_dates;
create volatile table sh_bt_launch_dates as (
    sel deal_uuid,
        max(has_gcal) has_gcal,
        cast(min(load_date) as date) launch_date
    from sandbox.sh_bt_active_deals_log_v4 ad
    where product_is_active_flag = 1
    and partner_inactive_flag = 0
    and load_date >= '2020-07-01'
    group by 1
) with data unique primary index (deal_uuid) on commit preserve rows;


create volatile table sh_booking_solution as (
    select o2.deal_uuid,
        max(sfa.id) sf_account_id,
        max(case when lower(sfa.scheduler_setup_type) in ('pen & paper','none') then 'pen & paper'
            when sfa.scheduler_setup_type is null then 'no data'
            else 'some booking tool'
            end) current_booking_solution,
        max(sfa.scheduler_setup_type) detailed_booking_solution,
        max(sfa.name) account_name,
        max(company_type) company_type
    from dwh_base_sec_view.sf_opportunity_1 o1
    join dwh_base_sec_view.sf_opportunity_2 o2 on o1.id = o2.id
    join dwh_base_sec_view.sf_account sfa on o1.accountid = sfa.id
    group by o2.deal_uuid
) with data unique primary index (deal_uuid) on commit preserve rows;

drop table sandbox.nvp_temp_retention_curve;
create table sandbox.nvp_temp_retention_curve as (
select
   ld.deal_uuid,
   ld.has_gcal,
   case when gdl.country_code in ('US','CA') then 'NAM' else 'INTL' end region,
   gdl.grt_l2_cat_description,
   case when lower(sbs.current_booking_solution) = 'pen & paper' then 'p&p' else 'other' end booking_type,
   cast(ld.launch_date as date) launch_date,
   ad.load_date,
   cast(ad.load_date as date) - cast(ld.launch_date as date) num_days,
   case when ad.load_date is not null then 1 else 0 end deal_active,
   case when bt_d.load_date is not null then 1 else 0 end deal_active_on_bt
from
   sh_bt_launch_dates as ld
   join user_edwprod.dim_gbl_deal_lob gdl on ld.deal_uuid = gdl.deal_id
   join sh_booking_solution sbs on ld.deal_uuid = sbs.deal_uuid
   left join
   (select deal_uuid, load_date, sold_out
          from user_groupondw.active_deals
          group by 1,2,3)
   as ad on ad.deal_uuid = ld.deal_uuid and ad.sold_out = 'false' and ad.load_date >= ld.launch_date
   left join
   (select deal_uuid, load_date
        from sandbox.sh_bt_active_deals_log
        where product_is_active_flag = 1
        and partner_inactive_flag = 0) as bt_d on ad.deal_uuid = bt_d.deal_uuid and ad.load_date = bt_d.load_date
        ) with data unique primary index(deal_uuid, load_date)
   ;


select count(1) from sandbox.nvp_temp_retention_curve;
