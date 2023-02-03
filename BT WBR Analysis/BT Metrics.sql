-----UNITS RAW

use grp_gdoop_bizops_db;
drop table rt_bt_funnel2;
create table rt_bt_funnel2 stored as orc as
select
  sum(units) units,
  booked,
  redeemed,
  case when a.deal_uuid is not null then 1 else 0 end bt_eligible,
  gdl.country_code country_code,
  e.economic_area,
  grt_l2_cat_name l2,
  order_date,
  book_date,
  redeem_date
from rt_bt_txns b
left join (
  select load_date, deal_uuid
  from grp_gdoop_bizops_db.sh_bt_active_deals_log
  where partner_inactive_flag = 0 and product_is_active_flag = 1
  group by load_date, deal_uuid
) a on a.deal_uuid = b.deal_uuid and a.load_date = b.order_date
left join user_edwprod.dim_gbl_deal_lob gdl on gdl.deal_id = b.deal_uuid
join user_groupondw.gbl_dim_country c on gdl.country_code = c.country_iso_code_2
join user_groupondw.gbl_dim_economic_area e on c.economic_area_id = e.economic_area_id
  where grt_l1_cat_name = 'L1 - Local'
group by
booked,
redeemed,
case when a.deal_uuid is not null then 1 else 0 end,
gdl.country_code,
e.economic_area,
grt_l2_cat_name,
order_date,
book_date,
redeem_date;



select sum(a.units) units, 
sum(a.bt_eligible_units)bt_eligible_units, 
sum(b.bookings_units) bookings, 
sum(c.bookings_redeemed_units) bookings_redeemed, 
sum(c.bt_eligible_txns_redeemed_units) bt_eligible_txns_redeemed,
a.country_code,
a.l2,
a.week_of_year_num,
a.month_of_year_num,
a.quarter_of_year_num, 
a.year_key
from ((
  select sum(units) units, sum(case when bt_eligible = 1 then units end) bt_eligible_units, country_code, l2, order_date, week_of_year_num, month_of_year_num, quarter_of_year_num, b.year_key from grp_gdoop_bizops_db.rt_bt_funnel2 a
  join prod_groupondw.dim_day b on b.day_rw = a.order_date
  join prod_groupondw.dim_week c on b.week_key = c.week_key
  join prod_groupondw.dim_month d on b.month_key = d.month_key
  join prod_groupondw.dim_quarter e on b.quarter_key = e.quarter_key
  where c.year_key in (2019, 2020)
  group by country_code, l2, order_date, week_of_year_num, month_of_year_num, quarter_of_year_num, b.year_key) a
  left join (
    select sum(case when booked = 1 then units end) bookings_units, country_code, l2, book_date, week_of_year_num, month_of_year_num, quarter_of_year_num, b.year_key from grp_gdoop_bizops_db.rt_bt_funnel2 a
    join prod_groupondw.dim_day b on b.day_rw = a.book_date
    join prod_groupondw.dim_week c on b.week_key = c.week_key
    join prod_groupondw.dim_month d on b.month_key = d.month_key
    join prod_groupondw.dim_quarter e on b.quarter_key = e.quarter_key
    where c.year_key in (2019, 2020)
    group by country_code, l2, book_date, week_of_year_num, month_of_year_num, quarter_of_year_num, b.year_key) b on a.l2 = b.l2 and a.country_code = b.country_code and a.order_date = b.book_date and a.week_of_year_num = b.week_of_year_num and a.month_of_year_num = b.month_of_year_num and a.quarter_of_year_num = b.quarter_of_year_num and b.year_key = a.year_key
 left join (
    select sum( case when bt_eligible = 1 and booked = 1 then units end) bookings_redeemed_units, sum(case when bt_eligible = 1 then units end) bt_eligible_txns_redeemed_units, country_code, l2, redeem_date, week_of_year_num, month_of_year_num, quarter_of_year_num, b.year_key from grp_gdoop_bizops_db.rt_bt_funnel2 a
    join prod_groupondw.dim_day b on b.day_rw = a.redeem_date
    join prod_groupondw.dim_week c on b.week_key = c.week_key
    join prod_groupondw.dim_month d on b.month_key = d.month_key
    join prod_groupondw.dim_quarter e on b.quarter_key = e.quarter_key
    where c.year_key in (2019, 2020)
    group by country_code, l2, redeem_date, week_of_year_num, month_of_year_num, quarter_of_year_num, b.year_key) c on a.l2 = c.l2 and a.country_code = c.country_code and a.order_date = c.redeem_date and a.week_of_year_num = c.week_of_year_num and a.month_of_year_num = c.month_of_year_num and a.quarter_of_year_num = c.quarter_of_year_num and c.year_key = a.year_key )
  group by a.country_code, a.l2,a.week_of_year_num, a.month_of_year_num, a.quarter_of_year_num, a.year_key

------Inventory

select * from grp_gdoop_bizops_db.rt_bt_inventory_wbr;
 
drop table  grp_gdoop_bizops_db.rt_bt_inventory_wbr;



create table grp_gdoop_bizops_db.rt_bt_inventory_wbr stored as orc as
    select year(load_date), load_date, date_type, bt_eligible, all_deals, l2, country_code
    from (
    select week_start as load_date,
          'week' as date_type,
          count(distinct case when g.deal_uuid is not null then g.deal_uuid end) bt_eligible,
          count(distinct ad.deal_uuid) all_deals,
          gdl.grt_l2_cat_name l2,
          gdl.country_code
          from (
              select deal_uuid, load_date
              from prod_groupondw.active_deals
              where sold_out = 'false' and available_qty > 0
              and load_date >= '2019-02-01'
              group by deal_uuid, load_date
          ) ad
          left join (
            select load_date, deal_uuid
            from grp_gdoop_bizops_db.sh_bt_active_deals_log
            where partner_inactive_flag = 0 and product_is_active_flag = 1
            group by load_date, deal_uuid
          ) g on g.deal_uuid = ad.deal_uuid and ad.load_date = g.load_date
          join (select distinct deal_uuid from edwprod.deal_merch_product where inv_service_id = 'vis') v on v.deal_uuid = ad.deal_uuid
          left join user_edwprod.dim_gbl_deal_lob gdl on gdl.deal_id = ad.deal_uuid
          join user_groupondw.gbl_dim_country c on gdl.country_code = c.country_iso_code_2
          join user_groupondw.gbl_dim_economic_area e on c.economic_area_id = e.economic_area_id
          join prod_groupondw.dim_day bh on bh.day_rw = ad.load_date
          join prod_groupondw.dim_week cd on bh.week_key = cd.week_key and cd.week_start = cast(ad.load_date as timestamp)
          where grt_l1_cat_name = 'L1 - Local'
          group by week_start, 'week', gdl.grt_l2_cat_name, gdl.country_code
      union
      select month_start as load_date,
            'month' as date_type,
            count(distinct case when g.deal_uuid is not null then g.deal_uuid end) bt_eligible,
            count(distinct ad.deal_uuid) all_deals,
            gdl.grt_l2_cat_name,
            gdl.country_code
            from (
                select deal_uuid, load_date
                from prod_groupondw.active_deals
                where sold_out = 'false' and available_qty > 0
                and load_date >= '2019-02-01'
                group by deal_uuid, load_date
            ) ad
            left join (
              select load_date, deal_uuid
              from grp_gdoop_bizops_db.sh_bt_active_deals_log
              where partner_inactive_flag = 0 and product_is_active_flag = 1
              group by load_date, deal_uuid
            ) g on g.deal_uuid = ad.deal_uuid and ad.load_date = g.load_date
            join (select distinct deal_uuid from edwprod.deal_merch_product where inv_service_id = 'vis') v on v.deal_uuid = ad.deal_uuid
            left join user_edwprod.dim_gbl_deal_lob gdl on gdl.deal_id = ad.deal_uuid
            join user_groupondw.gbl_dim_country c on gdl.country_code = c.country_iso_code_2
            join user_groupondw.gbl_dim_economic_area e on c.economic_area_id = e.economic_area_id
            join prod_groupondw.dim_day bh on bh.day_rw = ad.load_date
            join prod_groupondw.dim_month d on bh.month_key = d.month_key and d.month_start = cast(ad.load_date as timestamp)
            where grt_l1_cat_name = 'L1 - Local'
            group by month_start, 'month', gdl.grt_l2_cat_name, gdl.country_code
    union
      select quarter_start as load_date,
            'quarter' as date_type,
            count(distinct case when g.deal_uuid is not null then g.deal_uuid end) bt_eligible,
            count(distinct ad.deal_uuid) all_deals,
            gdl.grt_l2_cat_name l2,
            gdl.country_code
            from (
                select deal_uuid, load_date
                from prod_groupondw.active_deals
                where sold_out = 'false' and available_qty > 0
                and load_date >= '2019-02-01'
                group by deal_uuid, load_date
            ) ad
            left join (
              select load_date, deal_uuid
              from grp_gdoop_bizops_db.sh_bt_active_deals_log
              where partner_inactive_flag = 0 and product_is_active_flag = 1
              group by load_date, deal_uuid
            ) g on g.deal_uuid = ad.deal_uuid and ad.load_date = g.load_date
            join (select distinct deal_uuid from edwprod.deal_merch_product where inv_service_id = 'vis') v on v.deal_uuid = ad.deal_uuid
            left join user_edwprod.dim_gbl_deal_lob gdl on gdl.deal_id = ad.deal_uuid
            join user_groupondw.gbl_dim_country c on gdl.country_code = c.country_iso_code_2
            join user_groupondw.gbl_dim_economic_area ed on c.economic_area_id = ed.economic_area_id
            join prod_groupondw.dim_day bh on bh.day_rw = ad.load_date
            join prod_groupondw.dim_quarter e on bh.quarter_key = e.quarter_key and e.quarter_start = cast(ad.load_date as timestamp)
            where grt_l1_cat_name = 'L1 - Local'
            group by quarter_start, 'quarter', gdl.grt_l2_cat_name, gdl.country_code
    ) a
    group by year(load_date), load_date, date_type, bt_eligible, all_deals, l2, country_code;


   

--supply---
drop table  grp_gdoop_bizops_db.rt_bt_supply_marketplace;
​
create table grp_gdoop_bizops_db.rt_bt_supply_marketplace stored as orc as
    select year(load_date) year, load_date, date_type, bt_eligible, all_deals, l2, country_code
    from (
    select week_start as load_date,
          'week' as date_type,
          count(distinct case when g.deal_uuid is not null then g.deal_uuid end) bt_eligible,
          count(distinct ad.deal_uuid) all_deals,
          gdl.grt_l2_cat_name l2,
          gdl.country_code
          from (
              select deal_uuid, load_date
              from prod_groupondw.active_deals
              where sold_out = 'false' and available_qty > 0
              and load_date >= '2019-02-01'
              group by deal_uuid, load_date
          ) ad
          left join (
            select load_date, deal_uuid
            from grp_gdoop_bizops_db.sh_bt_active_deals_log
            where partner_inactive_flag = 0 and product_is_active_flag = 1
            group by load_date, deal_uuid
          ) g on g.deal_uuid = ad.deal_uuid and ad.load_date = g.load_date
          left join user_edwprod.dim_gbl_deal_lob gdl on gdl.deal_id = ad.deal_uuid
          join user_groupondw.gbl_dim_country c on gdl.country_code = c.country_iso_code_2
          join user_groupondw.gbl_dim_economic_area e on c.economic_area_id = e.economic_area_id
          join prod_groupondw.dim_day bh on bh.day_rw = ad.load_date
          join prod_groupondw.dim_week cd on bh.week_key = cd.week_key and cd.week_start = cast(ad.load_date as timestamp)
          where grt_l1_cat_name = 'L1 - Local'
          group by week_start, 'week', gdl.grt_l2_cat_name, gdl.country_code
      union
      select month_start as load_date,
            'month' as date_type,
            count(distinct case when g.deal_uuid is not null then g.deal_uuid end) bt_eligible,
            count(distinct ad.deal_uuid) all_deals,
            gdl.grt_l2_cat_name,
            gdl.country_code
            from (
                select deal_uuid, load_date
                from prod_groupondw.active_deals
                where sold_out = 'false' and available_qty > 0
                and load_date >= '2019-02-01'
                group by deal_uuid, load_date
            ) ad
            left join (
              select load_date, deal_uuid
              from grp_gdoop_bizops_db.sh_bt_active_deals_log
              where partner_inactive_flag = 0 and product_is_active_flag = 1
              group by load_date, deal_uuid
            ) g on g.deal_uuid = ad.deal_uuid and ad.load_date = g.load_date
            left join user_edwprod.dim_gbl_deal_lob gdl on gdl.deal_id = ad.deal_uuid
            join user_groupondw.gbl_dim_country c on gdl.country_code = c.country_iso_code_2
            join user_groupondw.gbl_dim_economic_area e on c.economic_area_id = e.economic_area_id
            join prod_groupondw.dim_day bh on bh.day_rw = ad.load_date
            join prod_groupondw.dim_month d on bh.month_key = d.month_key and d.month_start = cast(ad.load_date as timestamp)
            where grt_l1_cat_name = 'L1 - Local'
            group by month_start, 'month', gdl.grt_l2_cat_name, gdl.country_code
    union
      select quarter_start as load_date,
            'quarter' as date_type,
            count(distinct case when g.deal_uuid is not null then g.deal_uuid end) bt_eligible,
            count(distinct ad.deal_uuid) all_deals,
            gdl.grt_l2_cat_name l2,
            gdl.country_code
            from (
                select deal_uuid, load_date
                from prod_groupondw.active_deals
                where sold_out = 'false' and available_qty > 0
                and load_date >= '2019-02-01'
                group by deal_uuid, load_date
            ) ad
            left join (
              select load_date, deal_uuid
              from grp_gdoop_bizops_db.sh_bt_active_deals_log
              where partner_inactive_flag = 0 and product_is_active_flag = 1
              group by load_date, deal_uuid
            ) g on g.deal_uuid = ad.deal_uuid and ad.load_date = g.load_date
            left join user_edwprod.dim_gbl_deal_lob gdl on gdl.deal_id = ad.deal_uuid
            join user_groupondw.gbl_dim_country c on gdl.country_code = c.country_iso_code_2
            join user_groupondw.gbl_dim_economic_area ed on c.economic_area_id = ed.economic_area_id
            join prod_groupondw.dim_day bh on bh.day_rw = ad.load_date
            join prod_groupondw.dim_quarter e on bh.quarter_key = e.quarter_key and e.quarter_start = cast(ad.load_date as timestamp)
            where grt_l1_cat_name = 'L1 - Local'
            group by quarter_start, 'quarter', gdl.grt_l2_cat_name, gdl.country_code
    ) a
    group by year(load_date), load_date, date_type, bt_eligible, all_deals, l2, country_code;

