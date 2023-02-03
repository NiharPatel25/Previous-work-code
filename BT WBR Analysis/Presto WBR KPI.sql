
select * from grp_gdoop_bizops_db.nvp_bt_funnel2;


--------------------------------------------------------------------------------Attrition_Booking_correlation
select count(*) from grp_gdoop_bizops_db.nvp_bt_merchant_attrition where mx_bt_merch_load_date < mn_merch_load_date;
select count(*) from grp_gdoop_bizops_db.nvp_bt_merchant_attrition where mn_bt_merch_load_date < mn_merch_load_date;
select count(*) from grp_gdoop_bizops_db.nvp_bt_merchant_attrition;
select * from grp_gdoop_bizops_db.nvp_bt_merchant_attrition where mn_merch_load_date > mn_bt_merch_load_date;------61 cases like these


select min(mn_bt_merch_load_date) from grp_gdoop_bizops_db.nvp_bt_merchant_attrition;;

select count(1) from grp_gdoop_bizops_db.sh_bt_bookings_rebuild; 

--------------------------------------------------------------------------------Pre Purchase

select 
   a.econ_region, 
   a.order_date, 
   sum(a.total_api_same_day) total_same_day, 
   sum(a.total_bookings) total_bookings
from grp_gdoop_bizops_db.nvp_bt_prepurchase_st2ao as a
group by 
   a.econ_region, 
   a.order_date
;

select 
   country_id,
    case when country_id in ('US', 'CA') then 'NAM' else 'INTL' end as econ_region,
    grt_l2_cat_name,
    appointment_date, 
    count(distinct vsc_code) appointments,  
    count(distinct case when appointment_date >= cast('2020-05-11' as date) and checked_in = 'no-show' and redemption_status = 0 then vsc_code 
               when appointment_date < cast('2020-05-11' as date) and checked_in = 'unknown' and redemption_status = 0 then vsc_code end) no_show_appointments
   from 
   grp_gdoop_bizops_db.nvp_bt_noshow2
   where confirm_row = 1
   group by 
   country_id,
    case when country_id in ('US', 'CA') then 'NAM' else 'INTL' end,
    grt_l2_cat_name,
    appointment_date;

   select * from grp_gdoop_bizops_db.nvp_bt_prepurchase_st1;
--------------------------------------------------------------------------------No SHOW
select 
   a.*, 
   cast(a.no_show as double)/a.appointments no_show_rate
   from
(select 
    appointment_week, 
    sum(total_appointments) appointments, 
    sum(no_show) no_show
   from grp_gdoop_bizops_db.nvp_bt_prepurchase_noshow_trial 
where country_id <> 'US'
group by appointment_week) as a
order by appointment_week desc
;

select * from grp_gdoop_bizops_db.nvp_bt_noshow;


select 
   country_id,
    case when country_id in ('US', 'CA') then 'NAM' else 'INTL' end as econ_region,
    grt_l2_cat_name,
    appointment_date, 
    count(distinct vsc_code) appointments,  
    count(case when appointment_date >= cast('2020-05-11' as date) and checked_in = 'no-show' and voucher_redemption = 0 then vsc_code 
               when appointment_date < cast('2020-05-11' as date) and checked_in = 'unknown' and voucher_redemption = 0 then vsc_code end) no_show_appointments
   from 
   grp_gdoop_bizops_db.nvp_bt_prepurchase_st1 
   where appointment_date > cast('2020-05-11' as date)
   group by 
   country_id,
    case when country_id in ('US', 'CA') then 'NAM' else 'INTL' end,
    grt_l2_cat_name,
    appointment_date;


select * from grp_gdoop_bizops_db.nvp_bt_prepurchase_st2b;
--------------------------------------------------------------------------------availability


select * from grp_gdoop_bizops_db.nvp_bt_availability;



--------------------------------------------------------------------------------median/avg time

select a.* from 
(select 
   econ_region, 
   wbr_week, 
   percentile(book_diff, 0.5)
from grp_gdoop_bizops_db.nvp_bt_usermd_time_st2 
where book_diff is not null
group by econ_region, wbr_week) as a
where wbr_week >= date_sub(CURRENT_DATE, 60);


select * from grp_gdoop_bizops_db.nvp_bt_useravg_time where cast(wbr_week as date) >= date_sub(CURRENT_DATE, 60)

--------------------------------------------------------------------------------Attrition base
   
   
select 
    mn_load_week, 
    country_code, 
    l1,
    l2,
    count(distinct merchant_uuid) merchant_onboarded,
    count(distinct case when bt_attrited = 1 then merchant_uuid end) merch_left_bt,
    count(distinct case when merchant_stayed_more_than_7_days = 1 then merchant_uuid end) merch_stayed_on_groupon
from grp_gdoop_bizops_db.nvp_wbr_7day_bt_attrition
where mn_load_week >= '2020-08-01'
group by mn_load_week, country_code,l1,l2
order by mn_load_week, country_code;



select 
   mn_load_week, 
    country_code,
    merch_onboarded_bt, 
    merch_left_bt,
    cast(merch_left_bt as double)/merch_onboarded_bt attrition_rate
from
(select 
    mn_load_week, 
    country_code, 
    count(distinct merchant_uuid) merch_onboarded_bt,
    count(distinct case when bt_attrited = 1 then merchant_uuid end) merch_left_bt
from grp_gdoop_bizops_db.nvp_wbr_7day_bt_attrition
group by mn_load_week, country_code
order by mn_load_week desc, country_code);





--------------------------------------------------------------------------------Units base
select 
    COALESCE(sum(units), 0) units,
    COALESCE(sum(bt_eligible_units), 0) bt_eligible_units,
    COALESCE(sum(prev_btpurchaser_units), 0) prev_btpurchaser_units,
    COALESCE(sum(bookings), 0) bookings,
    COALESCE(sum(bookings_redeemed), 0) bookings_redeemed,
    COALESCE(sum(bt_eligible_txns_redeemed), 0) bt_eligible_txns_redeemed,
    COALESCE(sum(same_day_bookings), 0) prepurchase_bookings,
    COALESCE(sum(bookings_b)) total_bookings,
    COALESCE(sum(total_appointments)) total_appointments,
    COALESCE(sum(no_show)) no_show,
    a.country_code,
    a.l2,
    a.l3,
    a.wbr_week,
    a.cy_week
from grp_gdoop_bizops_db.nvp_bt_units a
where cast(a.wbr_week as date) >= date_sub(CURRENT_DATE, 60)
group by 
    a.country_code,
    a.l2, a.l3, a.wbr_week, a.cy_week
   limit 5;


>= date_sub(CURRENT_DATE, 60)


--------------------------------------------------------------------------------merchant base


select * from grp_gdoop_bizops_db.nvp_bt_supply;

select 
   wbr_week, 
   cy_week, 
   sum(bt_eligible) bt_eligible_deals, 
   sum(all_deals) all_local_deals, 
   sum(to_bt_eligible) to_bt_eligible_deals, 
   sum(to_all_deals) to_all_deals
from grp_gdoop_bizops_db.nvp_bt_supply_temp where country_code = 'US'
group by wbr_week, cy_week;


-------------------------------------------------------------------------------jw_bt_marketplace
--% BT ELIGIBLE TXNS
select 
   wbr_week, cy_week, cast(bookings_redeemed as double)/bt_eligible_txns_redeemed per_eligible_booked, bookings_redeemed, bt_eligible_txns_redeemed
from
(select 
   cast(wbr_week as date) as wbr_week,
   cast(cy_week as date) cy_week, 
   sum(bookings_redeemed) bookings_redeemed,
   sum(bt_eligible_txns_redeemed) bt_eligible_txns_redeemed
from grp_gdoop_bizops_db.jw_bt_marketplace_view
where country_code <> 'US' and country_code <> 'CA'
group by 
   cast(wbr_week as date),
   cast(cy_week as date)
) as a 
where wbr_week >= cast('2020-01-01' as date)
order by wbr_week
;
---% units bookable 

select 
     wbr_week, 
     sum(units) total_units_sold, 
     sum(bt_eligible_units) bt_units
from grp_gdoop_bizops_db.jw_bt_marketplace_view
where country_code = 'US' and l2 = 'L2 - Health / Beauty / Wellness' and cast(wbr_week as date) > cast('2020-06-01' as date)
group by wbr_week
order by cast(wbr_week as date);


select * from grp_gdoop_bizops_db.nvp_bt_funnel2;
-------


select 
   a.*, 
   b.*, 
   (cast(bookings_redeemed_units_b as double)/bt_eligible_txns_redeemed_units_b)- (cast(a.bookings_redeemed_units as double)/bt_eligible_txns_redeemed_units) xyz, 
   cast(bookings_redeemed_units_b as double)/bt_eligible_txns_redeemed_units_b abc
   from
(select 
   deal_uuid,
   country_code,
   sum(case when bt_eligible = 1 and booked = 1 then units end) bookings_redeemed_units,
   sum(case when bt_eligible = 1 then units end) bt_eligible_txns_redeemed_units
from grp_gdoop_bizops_db.nvp_bt_funnel2
where cast(week_end_red as date) = cast('2021-02-07' as date)
and country_code = 'UK'
group by deal_uuid, country_code) as a 
full join 
(select 
   deal_uuid, 
   country_code,
   sum(case when bt_eligible = 1 and booked = 1 then units end) bookings_redeemed_units_b,
   sum(case when bt_eligible = 1 then units end) bt_eligible_txns_redeemed_units_b
from grp_gdoop_bizops_db.nvp_bt_funnel2
where cast(week_end_red as date) = cast('2021-02-21' as date)
and country_code = 'UK'
group by deal_uuid, country_code) as b on a.deal_uuid = b.deal_uuid and a.country_code = b.country_code
order by abc asc, xyz;



select 
   coalesce(a.deal_uuid, b.deal_uuid), 
   a.units_this_week, 
   b.units_last_week
   from
(select 
   deal_uuid,
   sum(units) units_this_week
from grp_gdoop_bizops_db.nvp_bt_funnel2
where cast(week_end_ord as date) = cast('2021-03-21' as date)
and country_code = 'US'
and l2 = 'L2 - Things to Do - Leisure' 
and booked = 1
group by deal_uuid) as a 
full join 
(select 
   deal_uuid,
   sum(units) units_last_week
from grp_gdoop_bizops_db.nvp_bt_funnel2
where cast(week_end_ord as date) = cast('2021-03-14' as date)
and country_code = 'US'
and l2 = 'L2 - Things to Do - Leisure'
and booked = 1
group by deal_uuid) as b on a.deal_uuid = b.deal_uuid
order by units_this_week desc


select 
   sum(units) units_last_week
from grp_gdoop_bizops_db.nvp_bt_funnel2
where cast(week_end_ord as date) = cast('2021-03-07' as date)
and country_code = 'US'
and l2 = 'L2 - Things to Do - Leisure'
and bt_eligible = 1;


select 
   a.*, 
   b.*, 
   bookings_b - bookings_a xyz
   from
(select 
   deal_uuid,
   country_code,
   sum(case when booked = 1 then units end) bookings_a
from grp_gdoop_bizops_db.nvp_bt_funnel2
where cast(week_end_book as date) = cast('2021-01-31' as date)
and economic_area = 'EMEA'
group by deal_uuid, country_code) as a 
left join 
(select 
   deal_uuid, 
   country_code,
   sum(case when booked = 1 then units end) bookings_b
from grp_gdoop_bizops_db.nvp_bt_funnel2
where cast(week_end_book as date) = cast('2021-02-07' as date)
and economic_area = 'EMEA'
group by deal_uuid, country_code) as b on a.deal_uuid = b.deal_uuid and a.country_code = b.country_code
order by xyz desc;

select 
   deal_uuid,
   cast(week_end_book as date) xyz,
   country_code,
   sum(case when booked = 1 then units end) bookings_a
from grp_gdoop_bizops_db.nvp_bt_funnel2
where 
economic_area = 'EMEA'
where deal_uuid = '0b4d8e47-3979-4fa8-ae9e-171e7fa13bea'
group by deal_uuid, country_code, cast(week_end_book as date)
order by xyz desc

--------------------------------------------------------------------------------
select 
  booked_frst_ord,
  VAR_POP(units_total) variance_overall,
  VAR_POP(case when star_rating = '3 star' then units_total end) variance_3star,
  VAR_POP(case when star_rating = '2 star' then units_total end) variance_2star,
  VAR_POP(case when star_rating = '1 star' then units_total end) variance_1star,
  sum(units_total), 
  count(distinct user_uuid)
from 
  grp_gdoop_bizops_db.nvp_frequency_statsig_nam90
  where country_code ='US' and l2 = 'L2 - Health / Beauty / Wellness'
  group by booked_frst_ord;

select booked_frst_ord, sum(distinct_users) from grp_gdoop_bizops_db.nvp_purch_trial_one_nam90 group by booked_frst_ord;



select 
   econ_region, 
   wbr_week, 
   percentile(book_diff, 0.5)
from grp_gdoop_bizops_db.nvp_bt_usermd_time_st2 
group by econ_region, wbr_week 
order by econ_region, wbr_week;

select * from grp_gdoop_bizops_db.nvp_bt_usermd_time_st2;



--------------------------------------------------------------------------------
select 
a.deal_uuid,
a.state,
total_units, 
total_units2,
total_units-total_units2 diff
from 
(select deal_uuid, state, sum(total_units) total_units
from grp_gdoop_bizops_db.nvp_bookedbt_deals_trial 
where book_date = '2020-10-18'
group by deal_uuid, state) as a 
left join 
(select deal_uuid, sum(total_units) total_units2
from grp_gdoop_bizops_db.nvp_bookedbt_deals_trial 
where book_date = '2020-10-25'
group by deal_uuid
) as b on a.deal_uuid = b.deal_uuid
order by diff desc
;

select * from grp_gdoop_bizops_db.nvp_bookedbt_deals_trial;
select min(reference_date), max(reference_date) from grp_gdoop_bizops_db.jk_bt_availability_gbl;

select * from grp_gdoop_bizops_db.jk_bt_availability_gbl where deal_uuid = 'bfff40d5-53a0-443d-83ff-3c8574e5d0ed' and deal_option_uuid = '9c05a8a2-7448-459a-bee9-50e2721f3be7' and reference_date = cast('2020-06-18' as date);

select 
      * 
from grp_gdoop_bizops_db.nvp_booked_parentorders_trial 
where order_date > '2020-08-01';

select 
      *
from grp_gdoop_bizops_db.sh_bt_txns;

select
     *
from grp_gdoop_bizops_db.nvp_all_yrs_txns
where country_code in ('US','CA')
;