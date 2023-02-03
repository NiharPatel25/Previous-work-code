---balance data

create table grp_gdoop_bizops_db.nvp_trial2 stored as orc as
select * from grp_gdoop_sup_analytics_db.eh_to_merch_loc_agg limit 5;

select date_add(CURRENT_DATE, -1);
select current_date;

select * from grp_gdoop_sup_analytics_db.eh_to_merch_loc_agg;

---balance data so far looks at one day at a time for the last day. 

/*
 * 
 * 
 Metrics to add:
1) Unrestricted Service Nodes = merchant_service_locations_unres / merchant_service_locations

2) Unrestricted Option Nodes = merchant_option_locations_unres / merchant_option_locations

3) Unrestricted Option Nodes - Tier 2 = merchant_option_locations_unres_tier2 / merchant_option_locations_tier2

 */

select
a.report_wk,
a.vertical,
case when a.variant in ('Deals+Offers','DealsUnrestricted+Offers') then 'V1|V2' when a.variant = 'BAU' then 'BAU' else 'V3' end as variant,
coalesce(sum(a.merchants),0) as merchants,
coalesce(sum(a.merchant_locations),0) as merchant_locations,
coalesce(sum(a.merchant_services),0) as merchant_services,
coalesce(sum(a.merchant_options),0) as merchant_options,
coalesce(sum(a.merchant_services_unres),0) as merchant_services_unres,
coalesce(sum(a.merchant_services_booking),0) as merchant_services_booking,
coalesce(sum(a.merchant_services_tier2),0) as merchant_services_tier2,
coalesce(sum(a.merchant_services_tier3),0) as merchant_services_tier3,
coalesce(sum(a.merchant_services_tier1),0) as merchant_services_tier1, 
coalesce(sum(a.merchant_service_locations),0) as merchant_services_loc,
coalesce(sum(a.merchant_option_locations),0) as merchant_option_loc, 
coalesce(sum(a.merchant_services_takeout),0) as merchant_service_takeout, 
coalesce(sum(a.merchant_options_tier1),0) as merchant_options_tier1,
coalesce(sum(a.merchant_options_tier2),0) as merchant_options_tier2,
coalesce(sum(a.merchant_service_locations_unres),0) as merchant_service_locations_unres,
coalesce(sum(a.merchant_option_locations_unres),0) as merchant_option_locations_unres,
coalesce(sum(a.merchant_option_locations_unres_tier2),0) as merchant_option_locations_unres_tier2,
coalesce(sum(a.merchant_option_locations_tier2),0) as merchant_option_locations_tier2
from grp_gdoop_sup_analytics_db.eh_to_merch_loc_agg a
where (a.report_date = a.report_wk or a.report_date = date_add(CURRENT_DATE, -1))
and a.report_date >= '2020-07-19'
and a.variant not in ('COVID')
and a.vertical in ('F&D','HBW','TTD','H&A')
group by a.report_wk,
a.vertical,
case when a.variant in ('Deals+Offers','DealsUnrestricted+Offers') then 'V1|V2' when a.variant = 'BAU' then 'BAU' else 'V3' end;

select * from grp_gdoop_sup_analytics_db.eh_greenlist_detail;

/*
select CURRENT_DATE;

select 
a.report_wk, 
a.vertical,
case when a.variant in ('Deals+Offers','DealsUnrestricted+Offers') then 'V1|V2' else 'V3' end as variant,
case when a.report_date = a.report_wk then a.merchants when 
from 
(select *
from grp_gdoop_sup_analytics_db.eh_to_merch_loc_agg a
where a.report_date = a.report_wk
and a.report_date >= '2020-07-19'
and a.variant not in ('BAU','COVID')
and a.vertical in ('F&D','HBW','TTD','H&A')
UNION ALL
select *
from grp_gdoop_sup_analytics_db.eh_to_merch_loc_agg a
where a.report_date = date_add(CURRENT_DATE, -1)
and a.report_date >= '2020-07-19'
and a.variant not in ('BAU','COVID')
and a.vertical in ('F&D','HBW','TTD','H&A')
) a;

select max(report_wk), max(report_date)  from grp_gdoop_sup_analytics_db.eh_to_merch_loc_agg;*/
----md DATA: OR a.report_date = date_add(CURRENT_DATE, -2)

---THIS ONE IS BUILT WEEKLY AND THE BASE TABLE AGGREGATES WEEKLY BASIS. 


-----md data

create table grp_gdoop_bizops_db.nvp_to_md_trial stored as orc as
select
date_sub(next_day(a.report_date, 'MON'), 1) report_date,
a.vertical,
case when a.variant in ('Deals+Offers','DealsUnrestricted+Offers','Variant 1','Variant 2') then 'V1|V2' else 'V3' end as variant,
a.merchant_type,
a.target_live,
a.top_account_flag,
a.merchants_added_svc,
a.merchants_added_option,
case
when a.merchant_type = 'Active - Not Live' and a.merchants > 0 then 1
when a.merchant_type = 'Active - Live' and a.merchants_added_svc > 0 then 1
when lower(a.pitched) like '%yes%' and a.pitch_dmc_date <= a.report_date then 1
else 0
end as is_pitched,
sum(a.merchants_start) as merchants_start,
sum(a.services_start) as services_start,
sum(a.merchants) as merchants,
sum(a.services_booking) as services_booking,
sum(a.services_tier_2) as services_tier_2,
sum(a.services_tier_3) as services_tier_3,
sum(a.services_tier_1) as services_tier_1,
sum(a.services) as services,
sum(a.options) as options,
sum(a.unrest_services) as unrest_services, 
sum(merchant_service_locations) merchant_service_loc, 
sum(merchant_option_locations) merchant_option_loc,
sum(merchants_incl_non_target) as merchants_incl_non_target,
sum(services_takeout) as services_takeout, 
sum(merchant_locations) as merchant_locations
from grp_gdoop_sup_analytics_db.eh_to_md_scorecard a
where a.variant not in ('BAU','COVID')
and a.vertical in ('F&D','HBW','TTD','H&A')
group by date_sub(next_day(a.report_date, 'MON'), 1),
a.vertical,
case when a.variant in ('Deals+Offers','DealsUnrestricted+Offers','Variant 1','Variant 2') then 'V1|V2' else 'V3' end,
a.merchant_type,
a.target_live,
a.top_account_flag,
a.merchants_added_svc,
a.merchants_added_option,
case
when a.merchant_type = 'Active - Not Live' and a.merchants > 0 then 1
when a.merchant_type = 'Active - Live' and a.merchants_added_svc > 0 then 1
when lower(a.pitched) like '%yes%' and a.pitch_dmc_date <= a.report_date then 1
else 0
end;


select *
from grp_gdoop_bizops_db.nvp_to_md_trial
where vertical = 'H&A';


