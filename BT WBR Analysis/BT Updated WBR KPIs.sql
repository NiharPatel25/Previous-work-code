----looks at if a merchant has atleast one deal bookable. If a merch has one bookable and one non bookable it is still a bookable merchant

--------------------------------------------------------------------------------------------------------BOOKABLE MERCHANTS
--------------------------------------------------------------------------------------------GCAL Integration

drop table if exists grp_gdoop_bizops_db.nvp_wbr_merch_booked;
create table grp_gdoop_bizops_db.nvp_wbr_merch_booked stored as orc as
select
    cast(b.order_date as date) order_date,
    cast(b.book_date as date) book_date,
    b.parent_order_uuid,
    merchant_uuid
from grp_gdoop_bizops_db.rt_bt_txns as b
left join (
    select
         product_uuid product_uuid,
         max(merchant_uuid) merchant_uuid
    from user_edwprod.dim_offer_ext
    where inv_product_uuid <> '-1'
    group by product_uuid) merch on b.deal_uuid = merch.product_uuid
group by
   book_date,
   order_date,
   merchant_uuid,
   parent_order_uuid
;



drop table grp_gdoop_bizops_db.nvp_bt_supply;
create table grp_gdoop_bizops_db.nvp_bt_supply stored as orc as
select
    we.wbr_week,
    we.cy_week,
    count(distinct case when ad.load_date = we.wbr_week and g.deal_uuid is not null then g.deal_uuid end) bt_eligible,
    count(distinct case when ad.load_date = we.wbr_week then ad.deal_uuid end) all_deals,
    count(distinct case when ad.load_date = we.wbr_week and g.deal_uuid is not null then merch.merchant_uuid end) bt_merchants,
    count(distinct case when ad.load_date = we.wbr_week then merch.merchant_uuid end) all_merchants,
    count(distinct case when ad.load_date = we.wbr_week and g.deal_uuid is not null and has_gcal = 1 then merch.merchant_uuid end) gcal_merchants,
    count(distinct mb.merchant_uuid) merchant_with_bookings,
    count(distinct case when ad.load_date = we.wbr_week and g.deal_uuid is not null and to_d.deal_uuid is not null then g.deal_uuid end) to_bt_eligible,
    count(distinct case when ad.load_date = we.wbr_week and to_d.deal_uuid is not null then ad.deal_uuid end) to_all_deals,
    gdl.grt_l2_cat_name l2,
    gdl.grt_l2_cat_name l3,
    gdl.country_code,
    geo.geo_locale
from (
    select
        deal_uuid,
        load_date
     from prod_groupondw.active_deals
    where
       sold_out = 'false'
       and available_qty > 0
       and cast(load_date as date) >= date_sub(CURRENT_DATE, 60)
    group by deal_uuid, load_date) ad
left join (
    select
        load_date,
        deal_uuid
    from grp_gdoop_bizops_db.sh_bt_active_deals_log
    where
        partner_inactive_flag = 0
        and product_is_active_flag = 1
        and cast(load_date as date) >= date_sub(CURRENT_DATE, 60)
    group by load_date, deal_uuid) g
    on g.deal_uuid = ad.deal_uuid and ad.load_date = g.load_date
left join (
    select
         product_uuid product_uuid,
         max(merchant_uuid) merchant_uuid
    from user_edwprod.dim_offer_ext
    where inv_product_uuid <> '-1'
    group by product_uuid) merch on ad.deal_uuid = merch.product_uuid
join
    (select
        distinct
          day_rw,
          wbr_week,
          cy_week
     from grp_gdoop_bizops_db.jw_day_week_end
     ) we on ad.load_date = day_rw
left join
     (select
           merchant_uuid,
           book_date
        from grp_gdoop_bizops_db.nvp_wbr_merch_booked
        where book_date is not null
        group by merchant_uuid, book_date
         ) mb on merch.merchant_uuid = mb.merchant_uuid and ad.load_date = mb.book_date
left join user_edwprod.dim_gbl_deal_lob gdl on gdl.deal_id = ad.deal_uuid
left join user_groupondw.gbl_dim_country c on gdl.country_code = c.country_iso_code_2
left join user_groupondw.gbl_dim_economic_area e on c.economic_area_id = e.economic_area_id
left join grp_gdoop_bizops_db.jw_deals_geo_locale geo on ad.deal_uuid = geo.deal_uuid
left join
     (select
        load_date,
        deal_uuid,
        has_gcal
      from grp_gdoop_bizops_db.sh_bt_active_deals_log_v4
      where partner_inactive_flag = 0
      and product_is_active_flag = 1
      and cast(load_date as date) >= date_sub(CURRENT_DATE, 60)
  group by load_date, deal_uuid, has_gcal) gcal on gcal.deal_uuid = ad.deal_uuid and ad.load_date = gcal.load_date
left join
     (select deal_uuid from marketing_analytics_dev_db.rev_mgmt_tiered_offerings group by deal_uuid) to_d on to_d.deal_uuid = ad.deal_uuid
where
    grt_l1_cat_name = 'L1 - Local'
group by we.wbr_week, we.cy_week, gdl.grt_l2_cat_name, gdl.grt_l3_cat_name, gdl.country_code, geo.geo_locale;


----previous join
left join
     (select
        merchant_uuid
      from grp_gdoop_bizops_db.sh_bt_google_accounts
      group by merchant_uuid) gcal on merch.merchant_uuid = gcal.merchant_uuid

------------------------------------------------------------------------------------------------Attrition (Change it to country wise)
select * from user_edwprod.dim_gbl_deal_lob;


drop table if exists grp_gdoop_bizops_db.nvp_wbr_7day_bt_attrition;
create table grp_gdoop_bizops_db.nvp_wbr_7day_bt_attrition stored as orc as
select
  attrition.merchant_uuid,
  attrition.l1,
  attrition.l2,
  attrition.country_code,
  attrition.mn_merch_load_date,
  attrition.mx_merch_load_date,
  attrition.mn_bt_load_date,
  attrition.mx_bt_load_date,
  we.wbr_week mn_load_week,
  case when datediff(attrition.mx_merch_load_date, attrition.mn_bt_load_date) >= 7 then 1 else 0 end merchant_stayed_more_than_7_days,
  case when datediff(attrition.mx_bt_load_date, attrition.mn_bt_load_date) < 7 then 1 else 0 end bt_attrited
from
  (select
      e.merchant_uuid,
      gdl.l1,
      gdl.l2,
      gdl.country_code,
      min(mn_load_date) mn_merch_load_date,
      max(mx_load_date) mx_merch_load_date,
      min(mn_bt_load_date) mn_bt_load_date,
      max(mx_bt_load_date) mx_bt_load_date
    from
       (select
          deal_uuid,
          min(cast(load_date as date)) mn_load_date,
          max(cast(load_date as date)) mx_load_date
       from prod_groupondw.active_deals
      where
         sold_out = 'false'
         and available_qty > 0
         and cast(load_date as date) >= cast('2018-01-01' as date)
        group by deal_uuid) ad
    left join
       (select
              deal_uuid,
              min(cast(load_date as date)) mn_bt_load_date,
              max(cast(load_date as date)) mx_bt_load_date
          from grp_gdoop_bizops_db.sh_bt_active_deals_log
          where product_is_active_flag = 1 and is_bookable = 1 and partner_inactive_flag = 0 and cast(load_date as date) >= cast('2018-01-01' as date)
          group by deal_uuid) bt on bt.deal_uuid = ad.deal_uuid
    left join
         (select
              distinct
              product_uuid,
              merchant_uuid
          from user_edwprod.dim_offer_ext) as e on ad.deal_uuid = e.product_uuid
    join (select
              deal_id,
              max(grt_l1_cat_name) l1,
              max(grt_l2_cat_name) l2,
              max(grt_l2_cat_name) l3,
              max(country_code) country_code
              from user_edwprod.dim_gbl_deal_lob
              group by deal_id
              ) gdl on ad.deal_uuid = gdl.deal_id
    group by e.merchant_uuid, bt.deal_uuid, gdl.l1, gdl.l2, gdl.country_code) as attrition
  join
  grp_gdoop_bizops_db.jw_day_week_end we on attrition.mn_bt_load_date = we.day_rw and we.date_cut = 'ytd'
;



drop table if exists grp_gdoop_bizops_db.nvp_wbr_7day_bt_attrition_temp;
create table grp_gdoop_bizops_db.nvp_wbr_7day_bt_attrition_temp stored as orc as
select
    mn_load_week,
    country_code,
    l1,
    l2,
    count(distinct merchant_uuid) merch_onboarded_bt,
    count(distinct case when merchant_stayed_more_than_7_days = 1 then merchant_uuid end) merchant_stayed_more_than_7_days,
    count(distinct case when bt_attrited = 1 and merchant_stayed_more_than_7_days = 1 then merchant_uuid end) stayed_groupon_left_bt
from grp_gdoop_bizops_db.nvp_wbr_7day_bt_attrition
group by
    mn_load_week, country_code,l1,l2
order by
    mn_load_week, country_code;

select * from grp_gdoop_bizops_db.nvp_wbr_7day_bt_attrition_temp;

-----for future addition of active deals for attrition

/*(select
         deal_uuid,
         min(cast(load_date as date)) mn_load_date,
         max(cast(load_date as date)) mx_load_date
    from user_groupondw.active_deals
    where sold_out = 'false'
    and available_qty > 0
    group by deal_uuid, load_date
) ad on t.deal_uuid = ad.deal_uuid and ad.load_date = t.load_date
left join
(select
            deal_uuid,
            min(cast(load_date as date)) mn_bt_load_date,
            max(cast(load_date as date)) mx_bt_load_date
      from grp_gdoop_bizops_db.sh_bt_active_deals_log
      where product_is_active_flag = 1 and is_bookable = 1 and partner_inactive_flag = 0
      group by deal_uuid) bd on ad.
*/



-----------------------------------------------------------------------------------------------------------First BT Redemption

drop table if exists grp_gdoop_bizops_db.nvp_bt_first_txns;
create table grp_gdoop_bizops_db.nvp_bt_first_txns stored as orc as
select
user_uuid,
min(cast(book_date as date)) min_book_date,
min(cast(redeem_date as date)) min_redeem_date
from grp_gdoop_bizops_db.rt_bt_txns
where redeemed = 1
and booked = 1
group by user_uuid
;


-----------------------------------------------------------------------------------------------------------nvp_bt_funnel


drop table if exists grp_gdoop_bizops_db.nvp_bt_funnel;
create table grp_gdoop_bizops_db.nvp_bt_funnel stored as orc as
select
    sum(units) units,
    b.booked,
    b.redeemed,
    case when a.deal_uuid is not null then 1 else 0 end bt_eligible,
    case when cast(b.order_date as date) > cast(bt_f.min_redeem_date as date) and a.deal_uuid is not null then 1 else 0 end bt_prev_user,
    ---case when datediff(cast(b.redeem_date as date),cast(b.order_date as date)) <= 1 then 1 else 0 end same_day_redemption,
    gdl.country_code country_code,
    e.economic_area,
    geo.geo_locale,
    gdl.grt_l2_cat_name l2,
    gdl.grt_l3_cat_name l3,
    b.order_date,
    b.book_date,
    b.redeem_date
from grp_gdoop_bizops_db.rt_bt_txns b
left join (
    select
        load_date,
        deal_uuid
    from grp_gdoop_bizops_db.sh_bt_active_deals_log
    where
        partner_inactive_flag = 0
        and product_is_active_flag = 1
    group by load_date, deal_uuid
    ) a on a.deal_uuid = b.deal_uuid and a.load_date = b.order_date
left join user_edwprod.dim_gbl_deal_lob gdl on gdl.deal_id = b.deal_uuid
left join user_groupondw.gbl_dim_country c on gdl.country_code = c.country_iso_code_2
left join user_groupondw.gbl_dim_economic_area e on c.economic_area_id = e.economic_area_id
left join grp_gdoop_bizops_db.jw_deals_geo_locale geo on b.deal_uuid = geo.deal_uuid
left join grp_gdoop_bizops_db.nvp_bt_first_txns bt_f on b.user_uuid = bt_f.user_uuid
where
    grt_l1_cat_name = 'L1 - Local'
group by
    booked,
    redeemed,
    case when a.deal_uuid is not null then 1 else 0 end,
    case when cast(b.order_date as date) > cast(bt_f.min_redeem_date as date) and a.deal_uuid is not null then 1 else 0 end,
    ----case when datediff(cast(b.redeem_date as date),cast(b.order_date as date)) <= 1 then 1 else 0 end,
    gdl.country_code,
    e.economic_area,
    geo.geo_locale,
    gdl.grt_l2_cat_name,
    gdl.grt_l3_cat_name,
    order_date,
    book_date,
   redeem_date;



---------------------------------------------------------------------------------------------------------PRE PURCHASE BOOKINGS

drop table if exists grp_gdoop_bizops_db.nvp_bt_prepurchase_st1;
CREATE table grp_gdoop_bizops_db.nvp_bt_prepurchase_st1 stored as orc as
select
  *
from
(  select
       t.parent_order_uuid,
       t.order_date,
       v.created_at voucher_created,
       case when concat(v.code, v.merchant_redemption_code) is not null and (v.customer_redeemed = 1 or v.merchant_redeemed = 1) and b.state = 'confirmed' then 1 else 0 end voucher_redemption,
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
      case when concat(v.voucher_code,v.security_code) is not null and v.usage_state_id = 2 and b.state = 'confirmed' then 1 else 0 end voucher_redemption,
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



drop table if exists grp_gdoop_bizops_db.nvp_bt_prepurchase_st2;
CREATE table grp_gdoop_bizops_db.nvp_bt_prepurchase_st2 stored as orc as
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


-------------------------------------------------------------------------------------------------------------NO SHOW

drop table if exists grp_gdoop_bizops_db.nvp_bt_noshow;
create table grp_gdoop_bizops_db.nvp_bt_noshow stored as orc as
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

drop table if exists grp_gdoop_bizops_db.nvp_bt_noshow2;
create table grp_gdoop_bizops_db.nvp_bt_noshow2 stored as orc as
select
  country_id,
  case when country_id in ('US', 'CA') then 'NAM' else 'INTL' end as econ_region,
  grt_l1_cat_name,
  grt_l2_cat_name,
  grt_l3_cat_name,
  appointment_date,
  count(1) total_appointments,
  sum(case when book_state = 'confirmed' then 1 end) confirmed_appointments,
  sum(case when appointment_date >= cast('2020-05-11' as date) and checked_in = 'no-show' and book_state = 'confirmed' and redemption_status = 0 then 1
               when appointment_date < cast('2020-05-11' as date) and checked_in = 'unknown' and book_state = 'confirmed' and redemption_status = 0 then 1 end) no_show_appointments
  from
  grp_gdoop_bizops_db.nvp_bt_noshow
  group by
    country_id,
    case when country_id in ('US', 'CA') then 'NAM' else 'INTL' end,
    grt_l1_cat_name,
    grt_l2_cat_name,
    grt_l3_cat_name,
    appointment_date;



-----------------------------------------------------------------------------------------------------------nvp_bt_units
drop table if exists grp_gdoop_bizops_db.nvp_bt_units;
create table grp_gdoop_bizops_db.nvp_bt_units stored as orc as
select
    sum(a.units) units,
    sum(a.bt_eligible_units)bt_eligible_units,
    sum(a.prev_btpurchaser_units) prev_btpurchaser_units,
    sum(b.bookings_units) bookings,
    sum(c.bookings_redeemed_units) bookings_redeemed,
    sum(c.bt_eligible_txns_redeemed_units) bt_eligible_txns_redeemed,
    sum(d.total_api_same_day) same_day_bookings,
    sum(d.total_bookings) bookings_b,
    sum(e.total_appointments) total_appointments,
    sum(e.confirmed_appointments) confirmed_appointments,
    sum(e.no_show_appointments) no_show,
    a.country_code,
    a.l2,
    a.l3,
    a.order_date as report_date,
    a.wbr_week,
    a.cy_week
from (
    select
        sum(units) units,
        sum(case when bt_eligible = 1 then units end) bt_eligible_units,
        sum(case when bt_eligible = 1 and bt_prev_user = 1 then units end) prev_btpurchaser_units,
        country_code,
        l2,
        l3,
        order_date,
        wbr_week,
        cy_week
    from grp_gdoop_bizops_db.nvp_bt_funnel a
    join grp_gdoop_bizops_db.jw_day_week_end we on a.order_date = we.day_rw
    group by country_code, l2, l3, order_date, wbr_week, cy_week) a
left join (
    select
        sum(case when booked = 1 then units end) bookings_units,
        country_code,
        l2,
        l3,
        book_date
    from grp_gdoop_bizops_db.nvp_bt_funnel a
    join grp_gdoop_bizops_db.jw_day_week_end we on a.book_date = we.day_rw
    group by country_code, l2, l3, book_date) b
    on a.l2 = b.l2 and a.l3 = b.l3 and a.country_code = b.country_code and a.order_date = b.book_date
left join (
    select
        sum(case when bt_eligible = 1 and booked = 1 then units end) bookings_redeemed_units,
        sum(case when bt_eligible = 1 then units end) bt_eligible_txns_redeemed_units,
        country_code,
        l2,
        l3,
        redeem_date
    from grp_gdoop_bizops_db.nvp_bt_funnel a
    join grp_gdoop_bizops_db.jw_day_week_end we on a.redeem_date = we.day_rw
    group by country_code, l2, l3, redeem_date) c
    on a.l2 = c.l2 and a.l3 = c.l3 and a.country_code = c.country_code and a.order_date = c.redeem_date
left join
    grp_gdoop_bizops_db.nvp_bt_prepurchase_st2 d on a.order_date = d.voucher_first_book_date and a.country_code = d.country_id and a.l3 = d.grt_l3_cat_name and a.l2 = d.grt_l2_cat_name
left join
    grp_gdoop_bizops_db.nvp_bt_noshow2 e on a.order_date = e.appointment_date and a.country_code = e.country_id and a.l3 = e.grt_l3_cat_name and a.l2 = e.grt_l2_cat_name
group by a.country_code, a.l2, a.l3, a.order_date, a.wbr_week, a.cy_week;



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
    COALESCE(sum(confirmed_appointments)) confirmed_appointments,
    COALESCE(sum(no_show)) no_show,
    a.country_code,
    a.l2,
    a.l3,
    a.wbr_week,
    a.cy_week
from grp_gdoop_bizops_db.nvp_bt_units a
where a.wbr_week > '2020-08-15'
group by
    a.country_code,
    a.l2, a.l3, a.wbr_week, a.cy_week;



---------------------------------------------------------------------------------------------------------MEDIAN TIME

-----NEED TO UNDERSTAND L2 Breakdown

select a.* from
(select
   econ_region,
   wbr_week,
   percentile(book_diff, 0.5)
from grp_gdoop_bizops_db.nvp_bt_usermd_time_st2
where book_diff is not null
group by econ_region, wbr_week) as a
where wbr_week >= date_sub(CURRENT_DATE, 60);

drop table if exists grp_gdoop_bizops_db.nvp_bt_usermd_time;
create table grp_gdoop_bizops_db.nvp_bt_usermd_time stored as orc as
select
x.user_uuid,
--x.deal_uuid,
--gdl.l1,
--gdl.l2,
x.country_code,
cast(x.book_date as date) book_date,
ROW_NUMBER() over (partition by x.country_code, x.user_uuid order by cast(x.book_date as date) desc) rank_order
from
  (select
      user_uuid,
      --deal_uuid,
      country_code,
      book_date
      --sum(units) units
     from grp_gdoop_bizops_db.rt_bt_txns
     where booked = 1 and cast(book_date as date) >= cast('2018-01-01' as date)
     group by
      user_uuid,
      country_code,
      --deal_uuid,
      book_date) as x
;
/*left join
  (select
         deal_id,
         max(grt_l1_cat_name) l1,
         max(grt_l2_cat_name) l2
     from user_edwprod.dim_gbl_deal_lob
     group by deal_id
  ) gdl on x.deal_uuid = gdl.deal_id*/

002e557e-2536-11e2-82c0-00259069d5fe

drop table if exists grp_gdoop_bizops_db.nvp_bt_usermd_time_st2;
create table grp_gdoop_bizops_db.nvp_bt_usermd_time_st2 stored as orc as
select
    a.country_code,
    case when a.country_code in ('US', 'CA') then 'NAM' else 'INTL' end as econ_region,
    a.user_uuid,
    date_sub(next_day(cast(a.book_date as date), 'MON'), 1) as wbr_week,
    a.book_date as new_date,
    b.book_date as previous_date,
    a.rank_order as rank_order1,
    b.rank_order as rank_order2,
    --a.l2 latest_cat_l2,
    --b.l2 previous_cat_l2,
    case when b.book_date is not null then datediff(a.book_date, b.book_date) end as book_diff
   from
     (select
         country_code,
         user_uuid,
         book_date,
         rank_order
         --l2,
         --units
      from
      grp_gdoop_bizops_db.nvp_bt_usermd_time
     ) as a
   left join
     (select
         user_uuid,
         book_date,
         rank_order
         --l2
      from
      grp_gdoop_bizops_db.nvp_bt_usermd_time
     ) as b on a.user_uuid = b.user_uuid and a.rank_order = b.rank_order-1

select
   econ_region,
   wbr_week,
   percentile(book_diff, 0.5)
from grp_gdoop_bizops_db.nvp_bt_usermd_time_st2
where wbr_week >= cast('2020-08-01' as date) and book_diff is not null
group by econ_region, wbr_week
order by econ_region, wbr_week;


drop table if exists grp_gdoop_bizops_db.nvp_bt_useravg_time;
create table grp_gdoop_bizops_db.nvp_bt_useravg_time stored as orc as
select * from
(
select
     country_code,
     wbr_week,
     latest_cat_l2 l2,
     count(distinct user_uuid) total_users_booked,
     count(distinct case when book_diff <= 30 and latest_cat_l2 = previous_cat_l2 then user_uuid end) less_than_30,
     count(distinct case when book_diff <= 60 and latest_cat_l2 = previous_cat_l2 then user_uuid end) less_than60,
     count(distinct case when book_diff <= 90 and latest_cat_l2 = previous_cat_l2 then user_uuid end) less_than90
from grp_gdoop_bizops_db.nvp_bt_usermd_time_st2
group by latest_cat_l2, wbr_week, country_code
UNION
select
    country_code,
    wbr_week,
    'all' as l2,
    count(distinct user_uuid) total_users_booked,
    count(distinct case when book_diff <= 30 then user_uuid end) less_than_30,
    count(distinct case when book_diff <= 60 then user_uuid end) less_than_60,
    count(distinct case when book_diff <= 90 then user_uuid end) less_than_90
from
   grp_gdoop_bizops_db.nvp_bt_usermd_time_st2
group by wbr_week, country_code
) as a;


---------------------------------------------------------------------------------------------------------AVAILABILITY


drop table if exists grp_gdoop_bizops_db.nvp_bt_availability;
create table grp_gdoop_bizops_db.nvp_bt_availability stored as orc as
select
       country,
       a.week week_end,
       gdl.grt_l2_cat_description, gdl.grt_l3_cat_description,
       count(distinct deal_uuid) cnt,
       count(distinct case when  max_avail = 0 then deal_uuid end) no_avail,
       count(distinct case when num_dow >=3 then deal_uuid end) more_than_3_same_day_availability,
       count(distinct case when bookings > 0  then deal_uuid end) bookings_gt_zero,
       count(distinct case when bookings >= 5  then deal_uuid end) bookings_gte_five,
       count(distinct case when num_dow_avail_booked > 0  then deal_uuid end) dow_full_booked_gt_zero
from
     (select
         avail.deal_uuid,
         country,
         date_sub(next_day(avail.reference_date, 'MON'), 1) as week,
         max(gss_total_availability) max_avail,
         count(distinct case when gss_total_availability > 0 then date_format(reference_date,'E') end) num_dow,
         sum(gbk_morning +gbk_noon + gbk_afternoon + gbk_evening) bookings,
         count(distinct
                 case when gss_total_availability > 0
                      and (gbk_morning+ gbk_noon +gbk_afternoon + gbk_evening) * avail_taken_per_booking >= gss_total_availability
                      then date_format(reference_date,'E') end) as num_dow_avail_booked
          from
             (select *,
                  row_number() over(partition by merchant_uuid, deal_uuid,deal_option_uuid,calendar_uuid,
                        reference_date order by days_delta) update_order
                        from grp_gdoop_bizops_db.jk_bt_availability_gbl
                        where reference_date < current_date
                    ) avail
          join
             (select
                   groupon_real_deal_uuid as deal_uuid,
                   groupon_deal_uuid as deal_option_uuid,
                 (case when min(participants_per_coupon) OVER (PARTITION BY groupon_real_deal_uuid)= 0 then 1
                      else participants_per_coupon/ min(participants_per_coupon)  OVER (PARTITION BY groupon_real_deal_uuid) end
                  ) as avail_taken_per_booking
                   from
                   grp_gdoop_bizops_db.sh_bt_deals
                )  deals on deals.deal_uuid = avail.deal_uuid and deals.deal_option_uuid = avail.deal_option_uuid
           join
              (
              select
                  ad.deal_uuid,
                  ad.load_date
               from
              (
                 select
                    deal_uuid,
                    load_date
                  from prod_groupondw.active_deals
                  where
                     sold_out = 'false'
                     and available_qty > 0
                     and cast(load_date as date) >= date_sub(CURRENT_DATE, 60)
                  group by deal_uuid, load_date) ad
               join (
                  select
                     load_date,
                     deal_uuid
                   from grp_gdoop_bizops_db.sh_bt_active_deals_log
                   where
                      partner_inactive_flag = 0
                      and product_is_active_flag = 1
                      and cast(load_date as date) >= date_sub(CURRENT_DATE, 60)
                   group by load_date, deal_uuid) g
                       on g.deal_uuid = ad.deal_uuid and ad.load_date = g.load_date
                       ) bt_deals on avail.deal_uuid = bt_deals.deal_uuid and avail.reference_date = cast(bt_deals.load_date as date)
            where update_order  = 1
            group by avail.deal_uuid, country , date_sub(next_day(avail.reference_date, 'MON'), 1) ---gdl.grt_l2_cat_description, gdl.grt_l3_cat_description
       )a
       join  edwprod.dim_gbl_deal_lob gdl on gdl.deal_id = a.deal_uuid -- and gdl.country_code = a.country
       group by country , a.week ,gdl.grt_l2_cat_description, gdl.grt_l3_cat_description;








-----Reference tables


date_sub(next_day(dt, 'MON'), 1)
(select
	deal_uuid,
	country,
	min(load_date) load_date
	from grp_gdoop_bizops_db.sh_bt_active_deals_log
	where is_bookable = 1
	and partner_inactive_flag = 0
	and product_is_active_flag = 1
	and load_date ='2020-07-26'
	group by deal_uuid, country) as bo on bo.deal_uuid = mp.product_uuid and all_d.country_code = bo.country

(select
	deal_id,
	grt_l2_cat_name l2
	from
	user_edwprod.dim_gbl_deal_lob
	where grt_l1_cat_name = 'L1 - Local'
	group by deal_id, grt_l2_cat_name) local_d on mp.product_uuid = local_d.deal_id

(select
	merchant_uuid,
	product_uuid product_uuid
	from user_edwprod.dim_offer_ext
	where inv_product_uuid <> '-1' and contract_sell_price > 0 and groupon_value <> 0
	group by product_uuid, merchant_uuid
) as mp;

create table grp_gdoop_bizops_db.nvp_bt_supply_demo stored as orc as
(select
    product_uuid product_uuid,
    max(merchant_uuid) dis_merch
    from user_edwprod.dim_offer_ext
    where inv_product_uuid <> '-1'
    group by product_uuid)


    join (select
       deal_id
       from user_edwprod.dim_gbl_deal_lob
       where grt_l2_cat_name = 'L2 - Health / Beauty / Wellness') b on a.deal_uuid = b.deal_id
