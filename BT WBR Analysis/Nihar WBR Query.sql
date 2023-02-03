-----------JOSEPH QUERIES
-------WEEK RELATED QUERY

select * from dwh_base_sec_view.opportunity_1;
select * from dwh_base_sec_view.opportunity_2;


create table grp_gdoop_bizops_db.nvp_week_end2 stored as orc as
select
     cast(ly.report_date as date) wbr_week,
     cast(cy.report_date as date) cy_week
  from
  (select
       *
      from
   (select cast(substr(week_end,1,10) as date) report_date,
        ROW_NUMBER () over(partition by year(cast(substr(week_end,1,10) as date)) order by cast(substr(week_end,1,10) as date)) row_
   from user_groupondw.dim_week) as a
   where report_date >= date_sub(current_date, 465) and report_date <= current_date
   ) ly
  join
  (select
    *
    from
    (select cast(substr(week_end,1,10) as date) report_date,
            ROW_NUMBER () over(partition by year(cast(substr(week_end,1,10) as date)) order by cast(substr(week_end,1,10) as date)) row_
            from user_groupondw.dim_week) as a
     where report_date >= date_sub(current_date, 90) and report_date <= current_date
   )cy
  on cy.row_ = ly.row_ and year(cy.report_date) = year(ly.report_date)+1
UNION
  select
      cast(ly.report_date as date) wbr_week,
      cast(cy.report_date as date) cy_week
  from
  (select
     *
     from
     (select cast(substr(week_end,1,10) as date) report_date,
             ROW_NUMBER () over(partition by year(cast(substr(week_end,1,10) as date)) order by cast(substr(week_end,1,10) as date)) row_
      from user_groupondw.dim_week) as a
      where report_date >= date_sub(current_date, 90) and report_date <= current_date
  ) ly
   join
   (select
     *
    from
      (select cast(substr(week_end,1,10) as date) report_date,
              ROW_NUMBER () over(partition by year(cast(substr(week_end,1,10) as date)) order by cast(substr(week_end,1,10) as date)) row_
              from user_groupondw.dim_week) as a
    where report_date >= date_sub(current_date,90) and report_date <= current_date
  ) cy
  on cy.row_ = ly.row_ and year(cy.report_date) = year(ly.report_date);



 
--------------------UPDATED WEEK_Day
drop table if exists grp_gdoop_bizops_db.nvp_week_end2;
create table grp_gdoop_bizops_db.nvp_week_end2 stored as orc as
    select
        cast(substr(b.week_end,1,10) as date) wbr_week,
        cast(substr(a.week_end,1,10) as date) cy_week
    from user_groupondw.dim_week as a
        join user_groupondw.dim_week as b on a.last_year_same_week_key = b.week_key
    where
        cast(substr(a.week_end,1,10) as date) <= date_sub(current_date, 1)
        and
        (cast(substr(a.week_end,1,10) as date) >= date_sub(current_date, 45) or year(cast(substr(a.week_end,1,10) as date)) = year(current_date))
    union
    select
        cast(substr(week_end,1,10) as date) wbr_week,
        cast(substr(week_end,1,10) as date) cy_week
    from user_groupondw.dim_week
    where
        cast(substr(week_end,1,10) as date) <= date_sub(current_date, 1)
        and
        (cast(substr(week_end,1,10) as date) >= date_sub(current_date, 45) or year(cast(substr(week_end,1,10) as date)) = year(current_date))
;



drop table if exists grp_gdoop_bizops_db.nvp_day_week_end2;
    create table grp_gdoop_bizops_db.nvp_day_week_end2 stored as orc as
    select
        case when (we.wbr_week >= date_sub(current_date,45) or year(we.wbr_week) = year(current_date))
             then cast('ytd' as varchar(8))
             else cast('lytd' as varchar(8)) end as date_cut,
        dy.day_rw,
        cast(substr(dw.week_end,1,10) as date) as wbr_week,
        we.cy_week
    from user_groupondw.dim_day dy
    join user_groupondw.dim_week dw on dy.week_key = dw.week_key
    join grp_gdoop_bizops_db.nvp_week_end2 we on cast(substr(dw.week_end,1,10) as date) = we.wbr_week
;

select * from grp_gdoop_bizops_db.nvp_week_end2 order by wbr_week;

-----GEOLOCALE

use grp_gdoop_bizops_db;

drop table if exists grp_gdoop_bizops_db.nvp_deals_geo_locale;


insert overwrite table grp_gdoop_bizops_db.nvp_deals_geo_locale
select
    gdl.deal_id as deal_uuid,
    coalesce(da.state,da.country,c.country_name_en) as geo_locale
from user_edwprod.dim_gbl_deal_lob gdl
left join
    (select distinct
        da.deal_id,
        coalesce(da.state,'Missing') as state,
        da.country
    from grp_gdoop_sup_analytics_db.rev_mgmt_deal_attributes da
    where
        da.country = 'USA') da on gdl.deal_id = da.deal_id
left join user_groupondw.gbl_dim_country c on gdl.country_code = c.country_iso_code_2
where
    gdl.grt_l1_cat_name = 'L1 - Local'
;

insert overwrite table grp_gdoop_bizops_db.nvp_deals_tiered_market
select deal_uuid
       from dwh_base_sec_view.opportunity_1 o1
       join dwh_base_sec_view.opportunity_2 o2 on o1.id = o2.id
       where division in ('Long Island','Seattle','Detroit','Denver')
       group by deal_uuid;

------MAIN code

drop table if exists grp_gdoop_bizops_db.nvp_bt_funnel_dash;

insert overwrite table grp_gdoop_bizops_db.nvp_bt_funnel_dash
select
  sum(units) units,
  booked,
  redeemed,
  case when a.deal_uuid is not null then 1 else 0 end bt_eligible,
  gdl.country_code country_code,
  e.economic_area,
  geo.geo_locale,
  gdl.grt_l2_cat_name l2,
  gdl.grt_l3_cat_name l3,
  order_date,
  book_date,
  redeem_date,
  case when tie.deal_uuid is not null then 1 else 0 end tiered_market
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
left join grp_gdoop_bizops_db.nvp_deals_geo_locale geo on b.deal_uuid = geo.deal_uuid
left join grp_gdoop_bizops_db.nvp_deals_tiered_market tie on b.deal_uuid = tie.deal_uuid
where
  grt_l1_cat_name = 'L1 - Local'
group by
  booked,
  redeemed,
  case when a.deal_uuid is not null then 1 else 0 end,
  gdl.country_code,
  e.economic_area,
  geo.geo_locale,
  gdl.grt_l2_cat_name,
  gdl.grt_l3_cat_name,
  order_date,
  book_date,
  redeem_date,
  case when tie.deal_uuid is not null then 1 else 0 end
 ;


select sum(units) from grp_gdoop_bizops_db.rt_bt_txns as a
join grp_gdoop_bizops_db.nvp_day_week_end2 as b on cast(a.book_date as date) = cast(b.day_rw as date) and b.wbr_week = cast('2021-01-03' as date)
join user_edwprod.dim_gbl_deal_lob gdl on gdl.deal_id = a.deal_uuid and gdl.grt_l1_cat_name = 'L1 - Local'
where a.country_code = 'US';


insert overwrite table grp_gdoop_bizops_db.nvp_bt_units_dash
select
  sum(a.units) units,
  sum(a.bt_eligible_units)bt_eligible_units,
  sum(b.bookings_units) bookings,
  sum(c.bookings_redeemed_units) bookings_redeemed,
  sum(c.bt_eligible_txns_redeemed_units) bt_eligible_txns_redeemed,
  a.country_code,
  a.geo_locale,
  a.l2,
  a.l3,
  a.order_date as report_date,
  a.wbr_week,
  a.cy_week,
  a.tiered_market
from ((
  select
    sum(units) units,
    sum(case when bt_eligible = 1 then units end) bt_eligible_units,
    country_code,
    geo_locale,
    l2,
    l3,
    order_date,
    wbr_week,
    cy_week,
    a.tiered_market
  from grp_gdoop_bizops_db.nvp_bt_funnel_dash a
  join grp_gdoop_bizops_db.nvp_day_week_end2 we on a.order_date = we.day_rw
  group by country_code, geo_locale, l2, l3, order_date, wbr_week, cy_week,a.tiered_market) a
left join (
  select
    sum(case when booked = 1 then units end) bookings_units,
    country_code,
    geo_locale,
    l2,
    l3,
    book_date,
    a.tiered_market
  from grp_gdoop_bizops_db.nvp_bt_funnel_dash a
  join grp_gdoop_bizops_db.nvp_day_week_end2 we on a.book_date = we.day_rw
  group by country_code, geo_locale, l2, l3, book_date, a.tiered_market) b
  on a.l2 = b.l2 and a.l3 = b.l3 and a.country_code = b.country_code and a.geo_locale = b.geo_locale and a.tiered_market = b.tiered_market and a.order_date = b.book_date
left join (
  select
    sum(case when bt_eligible = 1 and booked = 1 then units end) bookings_redeemed_units,
    sum(case when bt_eligible = 1 then units end) bt_eligible_txns_redeemed_units,
    country_code,
    geo_locale,
    l2,
    l3,
    redeem_date,
    a.tiered_market
  from grp_gdoop_bizops_db.nvp_bt_funnel_dash a
  join grp_gdoop_bizops_db.nvp_day_week_end2 we on a.redeem_date = we.day_rw
  group by country_code, geo_locale, l2, l3, redeem_date, tiered_market) c
  on a.l2 = c.l2 and a.l3 = c.l3 and a.country_code = c.country_code and a.geo_locale = c.geo_locale and a.tiered_market = c.tiered_market and a.order_date = c.redeem_date)
group by a.country_code, a.geo_locale, a.tiered_market, a.l2, a.l3, a.order_date, a.wbr_week, a.cy_week;


insert overwrite table grp_gdoop_bizops_db.nvp_bt_supply_dash
select
    we.wbr_week,
    we.cy_week,
    count(distinct case when g.deal_uuid is not null then g.deal_uuid end) bt_eligible,
    count(distinct ad.deal_uuid) all_deals,
    gdl.grt_l2_cat_name l2,
    gdl.grt_l2_cat_name l3,
    gdl.country_code,
    geo.geo_locale,
    case when tie.deal_uuid is not null then 1 else 0 end tiered_market
from (
    SELECT
        deal_uuid,
        load_date
   from prod_groupondw.active_deals
    WHERE
        sold_out = 'false'
        and available_qty > 0
        and load_date >= '2019-01-01'
    group by deal_uuid, load_date) ad
left join (
    SELECT
        load_date,
        deal_uuid
    from grp_gdoop_bizops_db.sh_bt_active_deals_log
    WHERE
        partner_inactive_flag = 0
        and product_is_active_flag = 1
    group by load_date, deal_uuid) g
    on g.deal_uuid = ad.deal_uuid
    and ad.load_date = g.load_date
left join user_edwprod.dim_gbl_deal_lob gdl on gdl.deal_id = ad.deal_uuid
left join user_groupondw.gbl_dim_country c on gdl.country_code = c.country_iso_code_2
left join user_groupondw.gbl_dim_economic_area e on c.economic_area_id = e.economic_area_id
join
    (SELECT
         distinct wbr_week, cy_week
     from grp_gdoop_bizops_db.nvp_day_week_end2) we on ad.load_date = we.wbr_week
left join grp_gdoop_bizops_db.nvp_deals_geo_locale geo on ad.deal_uuid = geo.deal_uuid
left join grp_gdoop_bizops_db.nvp_deals_tiered_market tie on ad.deal_uuid = tie.deal_uuid
where
     grt_l1_cat_name = 'L1 - Local'
group by
     we.wbr_week,
     we.cy_week,
     gdl.grt_l2_cat_name,
     gdl.grt_l3_cat_name,
     gdl.country_code,
     geo.geo_locale,
     case when tie.deal_uuid is not null then 1 else 0 end;



insert overwrite table grp_gdoop_bizops_db.nvp_bt_marketplace_view_dash
select
    cast('original data' as varchar(25)) dash_selector,
    year(coalesce(a.wbr_week,f.wbr_week)) yr,
    coalesce(a.wbr_week,f.wbr_week) as wbr_week,
    coalesce(a.cy_week,f.cy_week) as cy_week,
    coalesce(a.l2,f.l2) as l2,
    coalesce(a.l3,f.l3) as l3,
    coalesce(a.country_code,f.country_code) as country_code,
    coalesce(a.geo_locale,f.geo_locale) as geo_locale,
    coalesce(a.tiered_market, f.tiered_market) as tiered_market,
    sum(a.bt_eligible) as bt_eligible,
    sum(a.all_deals) as all_deals,
    sum(coalesce(f.units,0)) as units,
    sum(coalesce(f.bt_eligible_units,0)) as bt_eligible_units,
    sum(coalesce(f.bookings,0)) as bookings,
    sum(coalesce(f.bookings_redeemed,0)) as bookings_redeemed,
    sum(coalesce(f.bt_eligible_txns_redeemed,0)) as bt_eligible_txns_redeemed
from (
    SELECT
        wbr_week,
        cy_week,
        l2,
        l3,
        country_code,
        geo_locale,
        tiered_market,
        sum(bt_eligible) bt_eligible,
        sum(all_deals) all_deals
    from grp_gdoop_bizops_db.nvp_bt_supply_dash
    group by
        wbr_week,
        cy_week,
        l2,
        l3,
        country_code,
        geo_locale,
        tiered_market
    ) a
full join (
    SELECT
        wbr_week,
        cy_week,
        l2,
        l3,
        country_code,
        geo_locale,
        tiered_market,
        sum(units) units,
        sum(bt_eligible_units) bt_eligible_units,
        sum(bookings) bookings,
        sum(bookings_redeemed) bookings_redeemed,
        sum(bt_eligible_txns_redeemed) bt_eligible_txns_redeemed
    from grp_gdoop_bizops_db.nvp_bt_units_dash
    group by
        wbr_week,
        cy_week,
        l2,
        l3,
        country_code,
        geo_locale,
        tiered_market
    ) f
    on a.wbr_week = f.wbr_week
    and a.cy_week = f.cy_week
    and a.l2 = f.l2
    and a.l3 = f.l3
    and a.country_code = f.country_code
    and a.geo_locale = f.geo_locale
    and a.tiered_market = f.tiered_market
group by
    coalesce(a.wbr_week,f.wbr_week),
    coalesce(a.cy_week,f.cy_week),
    coalesce(a.l2,f.l2),
    coalesce(a.l3,f.l3),
    coalesce(a.country_code,f.country_code),
    coalesce(a.geo_locale,f.geo_locale),
    coalesce(a.tiered_market, f.tiered_market);


create table grp_gdoop_bizops_db.nvp_bt_marketplace_view_dash_temp stored as orc as
select * from grp_gdoop_bizops_db.nvp_bt_marketplace_view_dash;
------------grp_gdoop_bizops_db.nvp_bt_marketplace_ytd


drop table grp_gdoop_bizops_db.nvp_bt_marketplace_ytd;
create table grp_gdoop_bizops_db.nvp_bt_marketplace_ytd  (
    dash_selector string,
    yr int,
    wbr_week date,
    cy_week date,
    l2 string,
    l3 string,
    country_code string,
    geo_locale string,
    tiered_market int,
    bt_eligible int,
    all_deals int,
    units int,
    bt_eligible_units int,
    bookings int,
    bookings_redeemed int,
    bt_eligible_txns_redeemed int
) stored as orc
tblproperties ("orc.compress"="SNAPPY");


insert overwrite table grp_gdoop_bizops_db.nvp_bt_marketplace_ytd
select
cast('YTD dash' as varchar(25)) dash_selector,
a.yr,
null as wbr_week,
null as cy_week,
a.l2,
a.l3,
a.country_code,
a.geo_locale,
a.tiered_market,
b.bt_eligible,
b.all_deals,
a.units,
a.bt_eligible_units,
a.bookings,
a.bookings_redeemed,
a.bt_eligible_txns_redeemed
from
(select
   year(wbr_week) yr,
   l2, l3, country_code, geo_locale, tiered_market,
   sum(units) units,
   sum(bt_eligible_units) bt_eligible_units,
   sum(bookings) bookings,
   sum(bookings_redeemed) bookings_redeemed,
   sum(bt_eligible_txns_redeemed) bt_eligible_txns_redeemed
from
grp_gdoop_bizops_db.nvp_bt_marketplace_view_dash
where year(cy_week) = year(current_date)
group by
year(wbr_week),
l2, l3, country_code, geo_locale, tiered_market) as a
left join
(select
   year(wbr_week) yr,
   l2, l3, country_code, geo_locale, tiered_market,
   sum(bt_eligible) bt_eligible,
   sum(all_deals) all_deals
from
   grp_gdoop_bizops_db.nvp_bt_marketplace_view_dash as a
   join
   (select max(cy_week) max_cy_week from grp_gdoop_bizops_db.nvp_day_week_end2) as b on a.cy_week = b.max_cy_week
   where year(cy_week) = year(current_date)
   group by year(wbr_week),
   l2, l3, country_code, geo_locale, tiered_market) as b
on
  a.yr = b.yr
  and a.l2 = b.l2
  and a.l3 = b.l3
  and a.country_code = b.country_code
  and a.geo_locale = b.geo_locale
  and a.tiered_market = b.tiered_market
;



------------------------CREATE TABLE


drop table if exists grp_gdoop_bizops_db.nvp_bt_marketplace_view_fin;
create table grp_gdoop_bizops_db.nvp_bt_marketplace_view_fin stored as orc as
select * from grp_gdoop_bizops_db.nvp_bt_marketplace_view_dash
union
select * from grp_gdoop_bizops_db.nvp_bt_marketplace_ytd;



-------------
drop table grp_gdoop_bizops_db.nvp_wbr_pds;
create table grp_gdoop_bizops_db.nvp_wbr_pds stored as orc as
select
    date_sub(next_day(a.load_date, 'MON'), 1) week_ending,
    pds.pds_cat_name,
    count(distinct a.deal_uuid) deals
from (select deal_uuid, cast(load_date as date) load_date
      from grp_gdoop_bizops_db.sh_bt_active_deals_log
      where product_is_active_flag = 1 and partner_inactive_flag = 0 and cast(load_date as date) >= '2020-07-13'
      ) a
join ( select
        deal_uuid,
        cast(load_date as date) load_date
    from prod_groupondw.active_deals
    where
        sold_out = 'false'
        and available_qty > 0
        and load_date >= '2019-01-01'
    group by deal_uuid, load_date
   ) ad on a.deal_uuid = ad.deal_uuid and a.load_date = ad.load_date
join
  (select
      deal_id,
      pds_cat_id
     from user_edwprod.dim_gbl_deal_lob
     where grt_l2_cat_name = 'L2 - Health / Beauty / Wellness' and country_code = 'US') b on a.deal_uuid = b.deal_id
left join
   (select distinct
      pds_cat_id,
      pds_cat_name
     from user_dw.v_dim_pds_grt_map) pds on b.pds_cat_id = pds.pds_cat_id
group by date_sub(next_day(a.load_date, 'MON'), 1), pds.pds_cat_name;

select * from grp_gdoop_bizops_db.nvp_wbr_pds;



;
