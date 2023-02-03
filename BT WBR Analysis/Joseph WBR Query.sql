-----------JOSEPH QUERIES
-------WEEK RELATED QUERY
SET hive.cli.print.header = true;SET hive.default.fileformat = Orc;SET hive.groupby.orderby.position.alias = true;SET hive.exec.dynamic.partition.mode = nonstrict;SET mapred.task.timeout = 1200000;SET hive.tez.container.size = 12288;SET hive.tez.java.opts =-Xmx9000M;SET hive.exec.max.dynamic.partitions.pernode = 19000;SET hive.exec.max.dynamic.partitions = 19000;SET hive.auto.convert.join.noconditionaltask.size = 3862953984;set hive.limit.query.max.table.partition = 5000;use grp_gdoop_bizops_db;insert overwrite table grp_gdoop_bizops_db.jw_week_end
select
	ly.report_date as wbr_week,
	cy.report_date as cy_week
from
	(select
		cast('we_ty' as varchar(8)) as date_cut,
		to_date(dw.week_end) as report_date,
		row_number() over(order by week_end desc) as row_
	from user_groupondw.dim_week dw
	where
		to_date(dw.week_end) between '2020-01-01' and date_sub(current_date,1)) cy
join
	(select
		cast('we_ly' as varchar(8)) as date_cut,
		to_date(dw.week_end) as report_date,
		row_number() over(order by week_end desc) as row_
	from user_groupondw.dim_week dw
	where
		to_date(dw.week_end) between '2019-01-01' and date_sub(current_date,365)) ly
on cy.row_ = ly.row_

union all

select
	cy2.report_date as wbr_week,
	cy.report_date as cy_week
from
	(select
		cast('we_ty' as varchar(8)) as date_cut,
		to_date(dw.week_end) as report_date,
		row_number() over(order by week_end desc) as row_
	from user_groupondw.dim_week dw
	where
		to_date(dw.week_end) between '2020-01-01' and date_sub(current_date,1)) cy
join
	(select
		cast('we_ty' as varchar(8)) as date_cut,
		to_date(dw.week_end) as report_date,
		row_number() over(order by week_end desc) as row_
	from user_groupondw.dim_week dw
	where
		to_date(dw.week_end) between '2020-01-01' and date_sub(current_date,1)) cy2
on cy.row_ = cy2.row_;


insert overwrite table grp_gdoop_bizops_db.jw_day_week_end
select
	case
		when year(day_rw) = 2020 then cast('ytd' as varchar(8))
		else cast('lytd' as varchar(8))
	end as date_cut,
	dy.day_rw,
	to_date(dw.week_end) as wbr_week,
	we.cy_week
from user_groupondw.dim_day dy
join user_groupondw.dim_week dw on dy.week_key = dw.week_key
join grp_gdoop_bizops_db.jw_week_end we on to_date(dw.week_end) = we.wbr_week
where
	dy.day_rw between '2019-01-01' and date_sub(current_date,365)
	or dy.day_rw between '2020-01-01' and date_sub(current_date,1);




insert overwrite table grp_gdoop_bizops_db.nvp_week_end2
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



insert overwrite table grp_gdoop_bizops_db.nvp_day_week_end2
  select
    case when we.wbr_week >= date_sub(current_date,90)
              then cast('ytd' as varchar(8))
         else cast('lytd' as varchar(8)) end as date_cut,
    dy.day_rw,
    cast(substr(dw.week_end,1,10) as date) as wbr_week,
    we.cy_week
  from user_groupondw.dim_day dy
  join user_groupondw.dim_week dw on dy.week_key = dw.week_key
  join grp_gdoop_bizops_db.nvp_week_end2 we on cast(substr(dw.week_end,1,10) as date) = we.wbr_week
;

-----GEOLOCALE

use grp_gdoop_bizops_db;
insert overwrite table grp_gdoop_bizops_db.jw_deals_geo_locale
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


------MAIN code

use grp_gdoop_bizops_db;
insert overwrite table grp_gdoop_bizops_db.jw_bt_funnel
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
	redeem_date
from rt_bt_txns b
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
	redeem_date;


insert overwrite table grp_gdoop_bizops_db.jw_bt_units
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
	a.cy_week
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
		cy_week
	from grp_gdoop_bizops_db.jw_bt_funnel a
	join grp_gdoop_bizops_db.jw_day_week_end we on a.order_date = we.day_rw
	group by country_code, geo_locale, l2, l3, order_date, wbr_week, cy_week) a
left join (
	select
		sum(case when booked = 1 then units end) bookings_units,
		country_code,
		geo_locale,
		l2,
		l3,
		book_date
	from grp_gdoop_bizops_db.jw_bt_funnel a
	join grp_gdoop_bizops_db.jw_day_week_end we on a.book_date = we.day_rw
	group by country_code, geo_locale, l2, l3, book_date) b
	on a.l2 = b.l2 and a.l3 = b.l3 and a.country_code = b.country_code and a.geo_locale = b.geo_locale and a.order_date = b.book_date
left join (
	select
		sum(case when bt_eligible = 1 and booked = 1 then units end) bookings_redeemed_units,
		sum(case when bt_eligible = 1 then units end) bt_eligible_txns_redeemed_units,
		country_code,
		geo_locale,
		l2,
		l3,
		redeem_date
	from grp_gdoop_bizops_db.jw_bt_funnel a
	join grp_gdoop_bizops_db.jw_day_week_end we on a.redeem_date = we.day_rw
	group by country_code, geo_locale, l2, l3, redeem_date) c
	on a.l2 = c.l2 and a.l3 = c.l3 and a.country_code = c.country_code and a.geo_locale = c.geo_locale and a.order_date = c.redeem_date)
group by a.country_code, a.geo_locale, a.l2, a.l3, a.order_date, a.wbr_week, a.cy_week;




insert overwrite table grp_gdoop_bizops_db.jw_bt_supply
select
	we.wbr_week,
	we.cy_week,
	count(distinct case when g.deal_uuid is not null then g.deal_uuid end) bt_eligible,
	count(distinct ad.deal_uuid) all_deals,
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
		and load_date >= '2019-01-01'
	group by deal_uuid, load_date) ad
left join (
	select
		load_date,
		deal_uuid
	from grp_gdoop_bizops_db.sh_bt_active_deals_log
	where
		partner_inactive_flag = 0
		and product_is_active_flag = 1
	group by load_date, deal_uuid) g
	on g.deal_uuid = ad.deal_uuid
	and ad.load_date = g.load_date
left join user_edwprod.dim_gbl_deal_lob gdl on gdl.deal_id = ad.deal_uuid
left join user_groupondw.gbl_dim_country c on gdl.country_code = c.country_iso_code_2
left join user_groupondw.gbl_dim_economic_area e on c.economic_area_id = e.economic_area_id
join
	(select
		distinct wbr_week, cy_week
	from grp_gdoop_bizops_db.jw_day_week_end) we on ad.load_date = we.wbr_week
left join grp_gdoop_bizops_db.jw_deals_geo_locale geo on ad.deal_uuid = geo.deal_uuid
where
	grt_l1_cat_name = 'L1 - Local'
group by we.wbr_week, we.cy_week, gdl.grt_l2_cat_name, gdl.grt_l3_cat_name, gdl.country_code, geo.geo_locale;



insert overwrite table grp_gdoop_bizops_db.jw_bt_marketplace_view
select
	coalesce(a.wbr_week,f.wbr_week) as wbr_week,
	coalesce(a.cy_week,f.cy_week) as cy_week,
	coalesce(a.l2,f.l2) as l2,
	coalesce(a.l3,f.l3) as l3,
	coalesce(a.country_code,f.country_code) as country_code,
	coalesce(a.geo_locale,f.geo_locale) as geo_locale,
	sum(a.bt_eligible) as bt_eligible,
	sum(a.all_deals) as all_deals,
	sum(coalesce(f.units,0)) as units,
	sum(coalesce(f.bt_eligible_units,0)) as bt_eligible_units,
	sum(coalesce(f.bookings,0)) as bookings,
	sum(coalesce(f.bookings_redeemed,0)) as bookings_redeemed,
	sum(coalesce(f.bt_eligible_txns_redeemed,0)) as bt_eligible_txns_redeemed
from (
	select
		wbr_week,
		cy_week,
		l2,
		l3,
		country_code,
		geo_locale,
		sum(bt_eligible) bt_eligible,
		sum(all_deals) all_deals
	from grp_gdoop_bizops_db.jw_bt_supply
	group by 1,2,3,4,5,6
	) a
full join (
	select
		wbr_week,
		cy_week,
		l2,
		l3,
		country_code,
		geo_locale,
		sum(units) units,
		sum(bt_eligible_units) bt_eligible_units,
		sum(bookings) bookings,
		sum(bookings_redeemed) bookings_redeemed,
		sum(bt_eligible_txns_redeemed) bt_eligible_txns_redeemed
	from grp_gdoop_bizops_db.jw_bt_units
	group by 1,2,3,4,5,6
	) f
	on a.wbr_week = f.wbr_week
	and a.cy_week = f.cy_week
	and a.l2 = f.l2
	and a.l3 = f.l3
	and a.country_code = f.country_code
	and a.geo_locale = f.geo_locale
group by 1,2,3,4,5,6






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
