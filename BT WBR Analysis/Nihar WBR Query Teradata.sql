


-------jw_week ---OLD
/*
drop table sandbox.nvp_week_end;
create multiset table sandbox.nvp_week_end as (
select
    cast(ly.report_date as date) wbr_week,
    cast(cy.report_date as date) cy_week
from
(select
  *
  from
  (select "week_end" report_date,
		ROW_NUMBER () over(partition by year("week_end") order by "week_end") row_
from user_groupondw.dim_week) as a
where report_date >= current_date - 465 and report_date <= current_date
) ly
join
(select
  *
  from
(select "week_end" report_date,
		ROW_NUMBER () over(partition by year("week_end") order by "week_end") row_
from user_groupondw.dim_week) as a
where report_date >= current_date - 90 and report_date <= current_date)
cy
on cy.row_ = ly.row_ and year(cy.report_date) = year(ly.report_date)+1

UNION

select
    cast(ly.report_date as date) wbr_week,
    cast(cy.report_date as date) cy_week
from
(select
  *
  from
  (select "week_end" report_date,
		ROW_NUMBER () over(partition by year("week_end") order by "week_end") row_
from user_groupondw.dim_week) as a
where report_date >= current_date - 90 and report_date <= current_date
) ly
join
(select
  *
  from
(select "week_end" report_date,
		ROW_NUMBER () over(partition by year("week_end") order by "week_end") row_
from user_groupondw.dim_week) as a
where report_date >= current_date - 90 and report_date <= current_date)
cy
on cy.row_ = ly.row_ and year(cy.report_date) = year(ly.report_date)
) with data no primary index;

create table sandbox.nvp_day_week_end as (
select
	dy.day_rw,
	cast(dw.week_end as date) as wbr_week,
	we.cy_week
from user_groupondw.dim_day dy
join user_groupondw.dim_week dw on dy.week_key = dw.week_key
join sandbox.nvp_week_end we on cast(dw.week_end as date) = we.wbr_week
) with data primary index (day_rw)


select * from user_groupondw.dim_day;
select * from user_groupondw.dim_week;*/

select * from user_groupondw.dim_week;


--------------------UPDATED WEEK_Day
drop table sandbox.nvp_week_end2;
create multiset table sandbox.nvp_week_end2 as 
(select
	    cast(b."week_end" as date) wbr_week,
	    cast(a."week_end" as date) cy_week
	    from user_groupondw.dim_week as a
	    join user_groupondw.dim_week as b on a.last_year_same_week_key = b.week_key
	where
	    cast(a."week_end" as date) <= current_date - 1
	    and
	    (cast (a."week_end" as date) >= (current_date - 45) or year(cast(a."week_end" as date)) = year(current_date))
union
select
	    cast("week_end" as date) wbr_week,
	    cast("week_end" as date) cy_week
	    from user_groupondw.dim_week
	 where
	    cast("week_end" as date) <= current_date - 1
	    and
	    (cast("week_end" as date) >= (current_date - 45) or year(cast("week_end" as date)) = year(current_date))
	) with data no primary index;

drop table sandbox.nvp_day_week_end2;
create multiset table sandbox.nvp_day_week_end2 as
(select
	    case when (we.wbr_week >= (current_date - 45) or year(we.wbr_week) = year(current_date))
	              then cast('ytd' as varchar(8))
	         else cast('lytd' as varchar(8)) end as date_cut,
	    dy.day_rw,
	    cast(dw."week_end" as date) as wbr_week,
	    we.cy_week
	  from user_groupondw.dim_day dy
	  join user_groupondw.dim_week dw on dy.week_key = dw.week_key
	  join sandbox.nvp_week_end2 we on cast(dw."week_end" as date) = we.wbr_week
) with data no primary index;


select * from sandbox.nvp_day_week_end2 order by day_rw;

select 
*
from 
sandbox.nvp_bt_txns as a 
join 
sandbox.nvp_day_week_end2 as b on a.book_date = b.day_rw and b.wbr_week = cast('2021-01-03' as date)
where
country_code = 'US'
order by parent_order_uuid
;

select 
order_date, 
count(distinct parent_order_uuid), 
sum(units), 
sum(case when booked = 1 then units end) booked_units
from 
sandbox.nvp_bt_txns 
where order_date > cast('2020-12-01' as date)
and country_code = 'US'
group by order_date 
order by order_date
;

-----------import nvp_bt_txns
-----------jw_deal_geo_locale

create multiset table sandbox.nvp_deals_geo_locale as (
select
	gdl.deal_id as deal_uuid,
	coalesce(da.state,da.country,c.country_name_en) as geo_locale
from user_edwprod.dim_gbl_deal_lob gdl
left join
	(select distinct
		da.deal_id,
		coalesce(da.state,'Missing') as state,
		da.country
	from sandbox.rev_mgmt_deal_attributes da
	where
		da.country = 'USA') da on gdl.deal_id = da.deal_id
left join user_groupondw.gbl_dim_country c on gdl.country_code = c.country_iso_code_2
where
	gdl.grt_l1_cat_name = 'L1 - Local'
) with data no primary index;


