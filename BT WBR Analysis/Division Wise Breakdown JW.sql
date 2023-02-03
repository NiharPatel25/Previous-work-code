---Division Wise Breakdown

select
wbr_week,
cy_week,
country_code,
geo_locale,
sum(bt_eligible) bookable_deals
from grp_gdoop_bizops_db.jw_bt_marketplace_view
where 
cast(wbr_week as date) >= cast('2020-07-19' as date)
and 
l2 = 'L2 - Health / Beauty / Wellness' 
and country_code = 'US'
group by
wbr_week,
cy_week,
country_code, 
geo_locale
order by
cast(wbr_week as date);

-----