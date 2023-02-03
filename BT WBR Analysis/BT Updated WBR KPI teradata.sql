

----------BT SUPPLY



create volatile multiset table nvp_wbr_merch_booked as
  (
    select
        cast(b.order_date as date) order_date,
        cast(b.book_date as date) book_date,
        b.parent_order_uuid,
        merchant_uuid
    from sandbox.nvp_bt_txns as b
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
  ) with data on commit preserve rows;


  drop table grp_gdoop_bizops_db.nvp_bt_supply;
  create multiset table sandbox.nvp_bt_supply as(
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
       from user_groupondw.active_deals
      where
         sold_out = 'false'
         and available_qty > 0
         and cast(load_date as date) >= current_date - 60
      group by deal_uuid, load_date) ad
  left join (
      select
          load_date,
          deal_uuid
      from sandbox.sh_bt_active_deals_log
      where
          partner_inactive_flag = 0
          and product_is_active_flag = 1
          and cast(load_date as date) >= current_date - 60
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
       from sandbox.nvp_day_week_end
       ) we on ad.load_date = day_rw
  left join
       (select
             merchant_uuid,
             book_date
          from nvp_wbr_merch_booked
          where book_date is not null
          group by merchant_uuid, book_date
           ) mb on merch.merchant_uuid = mb.merchant_uuid and ad.load_date = mb.book_date
  left join user_edwprod.dim_gbl_deal_lob gdl on gdl.deal_id = ad.deal_uuid
  left join user_groupondw.gbl_dim_country c on gdl.country_code = c.country_iso_code_2
  left join user_groupondw.gbl_dim_economic_area e on c.economic_area_id = e.economic_area_id
  left join sandbox.nvp_deals_geo_locale geo on ad.deal_uuid = geo.deal_uuid
  left join
       (select
          load_date,
          deal_uuid,
          has_gcal
        from sandbox.sh_bt_active_deals_log_v4
        where partner_inactive_flag = 0
        and product_is_active_flag = 1
        and cast(load_date as date) >= current_date - 60
    group by load_date, deal_uuid, has_gcal) gcal on gcal.deal_uuid = ad.deal_uuid and ad.load_date = gcal.load_date
  left join
       (select deal_uuid from sandbox.rev_mgmt_tiered_offerings group by deal_uuid) to_d on to_d.deal_uuid = ad.deal_uuid
  where
      grt_l1_cat_name = 'L1 - Local'
  group by we.wbr_week, we.cy_week, gdl.grt_l2_cat_name, gdl.grt_l3_cat_name, gdl.country_code, geo.geo_locale
) with data no primary index



---------

drop table if exists grp_gdoop_bizops_db.nvp_bt_usermd_time;
create volatile multiset table nvp_bt_usermd_time as (
select
    x.user_uuid,
    x.deal_uuid,
    gdl.l1,
    gdl.l2,
    x.country_code,
    cast(x.book_date as date) book_date,
    ROW_NUMBER() over (partition by x.country_code, x.user_uuid order by cast(x.book_date as date) desc) rank_order_user,
    ROW_NUMBER() over (partition by x.country_code, x.deal_uuid order by cast(x.book_date as date) desc) rank_order_deal
from
   (select user_uuid,
      deal_uuid,
      country_code,
      book_date
     from sandbox.nvp_bt_txns
     where booked = 1 and book_date >= '2020-01-01'
     group by user_uuid, country_code, deal_uuid, book_date
   ) as x
left join
   (select
         deal_id,
         max(grt_l1_cat_name) l1,
         max(grt_l2_cat_name) l2
     from user_edwprod.dim_gbl_deal_lob
     group by deal_id
   ) gdl on x.deal_uuid = gdl.deal_id
 ) with data on commit preserve rows
;

drop table if exists grp_gdoop_bizops_db.nvp_bt_usermd_time_st2;

drop table sandbox.nvp_bt_usermd_time_st2;

create multiset table sandbox.nvp_bt_usermd_time_st2 as
(select
    a.country_code,
    case when a.country_code in ('US', 'CA') then 'NAM' else 'INTL' end as econ_region,
    a.user_uuid,
    a.book_date as new_date,
    b.book_date as previous_date,
    a.rank_order as rank_order1,
    b.rank_order as rank_order2,
    a.l2 latest_cat_l2,
    b.l2 previous_cat_l2,
    trunc(a.book_date,'iw')+6 as wbr_week,
    a.book_date - b.book_date as book_diff
   from
     (select
         md.country_code,
         md.user_uuid,
         md.book_date,
         md.rank_order,
         md.l2
      from
      nvp_bt_usermd_time md
     ) as a
   join
     (select
         user_uuid,
         book_date,
         rank_order,
         l2
      from
      nvp_bt_usermd_time
     ) as b on a.user_uuid = b.user_uuid and a.rank_order = b.rank_order-1
) with data;

----avg bookings per month of any user..
select
     wbr_week,
     latest_cat_l2,
     count(distinct case when book_diff <= 30 and latest_cat_l2 = previous_cat_l2 then user_uuid end) book_diff,
     count(distinct case when book_diff <= 60 and latest_cat_l2 = previous_cat_l2 then user_uuid end) book_diff,
     count(distinct case when book_diff <= 90 and latest_cat_l2 = previous_cat_l2 then user_uuid end) book_diff,
     count(distinct case when book_diff > 90 and latest_cat_l2 = previous_cat_l2 then user_uuid end) book_diff
from sandbox.nvp_bt_usermd_time_st2
group by latest_cat_l2, wbr_week
order by wbr_week, latest_cat_l2;



create multiset table sandbox.nvp_bt_dealuuid_time_st2 as
(select
    a.country_code,
    case when a.country_code in ('US', 'CA') then 'NAM' else 'INTL' end as econ_region,
    a.deal_uuid,
    a.book_date as new_date,
    b.book_date as previous_date,
    a.rank_order_deal as rank_order1,
    b.rank_order_deal as rank_order2,
    a.l2 latest_cat_l2,
    b.l2 previous_cat_l2,
    trunc(a.book_date,'iw')+6 as wbr_week,
    a.book_date - b.book_date as book_diff
   from
     (select
         md.country_code,
         md.deal_uuid,
         md.book_date,
         md.rank_order_deal,
         md.l2
      from
      nvp_bt_usermd_time md
     ) as a
   join
     (select
         deal_uuid,
         country_code,
         book_date,
         rank_order_deal,
         l2
      from
      nvp_bt_usermd_time
     ) as b on a.deal_uuid = b.deal_uuid and a.country_code = b.country_code and a.rank_order_deal = b.rank_order_deal-1
) with data;


select
     latest_cat_l2,
     country_code,
     count(distinct case when book_diff <= 30 and latest_cat_l2 = previous_cat_l2 then user_uuid end) book_diff
from sandbox.nvp_bt_usermd_time_st2
where wbr_week = '2020-11-08'
group by latest_cat_l2
order by latest_cat_l2;

----deals rebooking time

select
   wbr_week,
   deal_uuid,
   avg(book_diff)
from sandbox.nvp_bt_dealuuid_time_st2;


----------BOOKINGS CONVERSIONS


create volatile multiset table nvp_bt_userbt_conv as (
select
    x.parent_order_uuid,
    x.user_uuid,
    x.deal_uuid,
    gdl.l1,
    gdl.l2,
    x.country_code,
    cast(x.book_date as date) book_date,
    ROW_NUMBER() over (partition by x.country_code, x.user_uuid order by cast(x.book_date as date) desc) rank_order_user,
    ROW_NUMBER() over (partition by x.country_code, x.deal_uuid order by cast(x.book_date as date) desc) rank_order_deal
from
   (select user_uuid,
      parent_order_uuid,
      deal_uuid,
      country_code,
      book_date
     from sandbox.nvp_bt_txns
     where booked = 1 and book_date >= '2020-01-01'
     group by user_uuid, country_code, deal_uuid, book_date, parent_order_uuid
   ) as x
left join
   (select
         deal_id,
         max(grt_l1_cat_name) l1,
         max(grt_l2_cat_name) l2
     from user_edwprod.dim_gbl_deal_lob
     group by deal_id
   ) gdl on x.deal_uuid = gdl.deal_id
 ) with data on commit preserve rows
;
----trunc(a.book_date,'iw')+6

select
   country_code,
   l2,
   trunc(book_date,'iw')+6 wbr_week,
   count(distinct concat(parent_order_uuid, deal_uuid)) distinct_books,
   count(distinct user_uuid) users

from nvp_bt_userbt_conv
