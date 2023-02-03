select 
a.deal_uuid, 
total_units, 
total_units2,
total_units-total_units2 diff
from 
(select deal_uuid, sum(total_units) total_units
from grp_gdoop_bizops_db.nvp_bookedbt_deals_trial 
where book_date = '2020-10-11'
group by deal_uuid) as a 
left join 
(select deal_uuid, sum(total_units) total_units2
from grp_gdoop_bizops_db.nvp_bookedbt_deals_trial 
where book_date = '2020-10-18'
group by deal_uuid
) as b on a.deal_uuid = b.deal_uuid
order by diff desc
;

select book_date, sum(total_units) total_units, sum(missingbooked)
from grp_gdoop_bizops_db.nvp_bookedbt_deals_trial
where book_date is not null
and country_code <> 'US'
group by book_date
order by book_date desc;

select count(*) from grp_gdoop_bizops_db.nvp_bt_rebooking_miss_fin;

