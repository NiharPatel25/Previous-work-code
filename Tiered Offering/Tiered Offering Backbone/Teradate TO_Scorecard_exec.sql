
----bd closes
----sandbox.eh_to_closes
-----Logic using all possible scenario
select * from sandbox.eh_to_closes;


select
a.close_wk,
case when a.vertical = 'TTD - Leisure' then 'TTD' else a.vertical end as vertical,
case when a.variant in ('1','2') then 'V1|V2' 
     when a.variant = '3' then 'V3' 
     when 
         a.market in 
         ('chicago',
	       'washington-dc',
	       'north-jersey',
	       'philadelphia',
	       'boston',
	       'baltimore',
	       'portland',
	       'stlouis') then 'BAU' end as variant,
count(distinct a.account_id) as merchants,
count(distinct a.opportunity_id) as deals,
count(distinct a.opportunity_id) as services,
sum(a.options) as options,
sum(case when a.bookable_at_close = 1 then 1 else 0 end) as services_booking,
sum(case when a.unrestricted_flag = 1 then 1 else 0 end) as services_unrestricted,
sum(case when a.tier = 2 then 1 else 0 end) as services_tier2,
sum(case when a.tier = 3 then 1 else 0 end) as services_tier3,
sum(case when a.tier = 1 then 1 else 0 end) as services_tier1, 
sum(launched_flag) as launched_deals
from sandbox.eh_to_closes a
where a.merchant_type in ('New','Inactive','lead gen','New/Inactive')
and a.vertical in ('F&D','HBW','TTD - Leisure', 'H&A')
group by 1,2,3;



-----2020-09-06
----looking only for locations that are live. 



drop table nvp_to_inv_redloc_mn;
create volatile multiset table nvp_to_inv_redloc_mn as (
select 
        a.product_id,
        count(distinct a.redemption_location_id) redemption_locations
      from 
      (select 
          product_id, 
          redemption_location_id, 
          SUBSTR(load_key,1, 8) load_key 
        from 
        user_edwprod.deal_inv_prd_redloc) a
      join
       (select 
         product_id, 
         max(SUBSTR(load_key,1, 8)) max_load_key 
        from user_edwprod.deal_inv_prd_redloc group by product_id
       ) b on a.product_id = b.product_id and a.load_key = b.max_load_key
       group by 1) with data primary index (product_id) on commit preserve rows;
collect stats on nvp_to_inv_redloc_mn column(product_id);


create volatile multiset table nvp_to_inv_redloc_main as (
select 
     a.product_uuid deal_uuid, 
     sum(redemption_locations) locations_opt
   from user_edwprod.dim_offer_ext a
   join 
     nvp_to_inv_redloc_mn c on a.inv_product_uuid = c.product_id
   group by 1
) with data primary index (deal_uuid) on commit preserve rows;


select
a.close_wk,
case when a.vertical = 'TTD - Leisure' then 'TTD' else a.vertical end as vertical,
case when a.variant in ('1','2') then 'V1|V2' else 'V3' end as variant,
count(distinct a.account_id) as merchants,
count(distinct a.opportunity_id) as deals,
count(distinct a.opportunity_id) as services,
sum(a.options) as options,
sum(case when a.bookable_at_close = 1 then 1 else 0 end) as services_booking,
sum(case when a.unrestricted_flag = 1 then 1 else 0 end) as services_unrestricted,
sum(case when a.tier = 2 then 1 else 0 end) as services_tier2,
sum(case when a.tier = 3 then 1 else 0 end) as services_tier3,
sum(case when a.tier = 1 then 1 else 0 end) as services_tier1, 
count(distinct case when a.launched_flag = 1 then a.opportunity_id end) as launched_deals,
coalesce(sum(ddl.locations_ser),0) merchant_services_loc, 
coalesce(sum(ddl2.locations_opt),0) merchant_options_loc
from sandbox.eh_to_closes a
left join
 (select
    dd.uuid as deal_uuid,
    count(distinct ddl.deal_location_key) as locations_ser
  from user_groupondw.dim_deal_location ddl
  left join user_groupondw.dim_deal dd on dd.deal_key = ddl.deal_key
    group by 1) ddl on ddl.deal_uuid = a.deal_uuid
left join 
  nvp_to_inv_redloc_main ddl2 on ddl2.deal_uuid = a.deal_uuid
where a.variant in ('1','2','3')
and a.merchant_type in ('New','Inactive','lead gen','New/Inactive')
and a.vertical in ('F&D','HBW','TTD - Leisure', 'H&A')
group by 1,2,3
;



/*
---spot check


select
a.close_wk,
case when a.vertical = 'TTD - Leisure' then 'TTD' else a.vertical end as vertical,
case when a.variant in ('1','2') then 'V1|V2' else 'V3' end as variant,
a.account_id as merchants,
a.opportunity_id as deals,
a.opportunity_id as services,
sum(a.options) as options,
sum(case when a.bookable_at_close = 1 then 1 else 0 end) as services_booking,
sum(case when a.unrestricted_flag = 1 then 1 else 0 end) as services_unrestricted,
sum(case when a.tier = 2 then 1 else 0 end) as services_tier2,
sum(case when a.tier = 3 then 1 else 0 end) as services_tier3,
sum(case when a.tier = 1 then 1 else 0 end) as services_tier1, 
count(distinct case when a.launched_flag = 1 then a.opportunity_id end) as launched_deals, 
sum(ddl.locations_ser) merchant_services_loc, 
sum(ddl2.locations_opt) merchant_options_loc
from sandbox.eh_to_closes a
left join
 (select
    dd.uuid as deal_uuid,
    count(distinct ddl.deal_location_key) as locations_ser
  from user_groupondw.dim_deal_location ddl
  left join user_groupondw.dim_deal dd on dd.deal_key = ddl.deal_key
    group by 1) ddl on ddl.deal_uuid = a.deal_uuid
left join 
  nvp_to_inv_redloc_main ddl2 on ddl2.deal_uuid = a.deal_uuid
where a.variant in ('1','2','3')
and a.merchant_type in ('New','Inactive','lead gen','New/Inactive')
and a.vertical in ('F&D','HBW','TTD - Leisure', 'H&A')
and a.Account_id = '001C000000yWQ23IAG'
group by 1,2,3,4,5,6
;

/*0013c00001sKdZFAA0,0013c00001pYGRrAAO

select * from sandbox.eh_to_closes where Account_id = '001C000000yWQ23IAG';

select
    *
  from user_groupondw.dim_deal_location ddl
  left join user_groupondw.dim_deal dd on dd.deal_key = ddl.deal_key
  where dd.uuid = '46a51c03-0d27-4d86-8ab5-5ee330c09260';

 
select 
     a.product_uuid deal_uuid, 
     a.inv_product_uuid,
     sum(redemption_locations) locations_opt
   from user_edwprod.dim_offer_ext a
   join 
     nvp_to_inv_redloc_mn c on a.inv_product_uuid = c.product_id
   where a.product_uuid = '6cc73e7a-42a5-4300-b247-407c95dd3461'
   group by 1,2;

select 
        a.product_id,
        x.product_uuid,
        a.redemption_location_id redemption_locations
      from 
      (select 
          product_id, 
          redemption_location_id, 
          SUBSTR(load_key,1, 8) load_key 
        from 
        user_edwprod.deal_inv_prd_redloc) a
      join
       (select 
         product_id, 
         max(SUBSTR(load_key,1, 8)) max_load_key 
        from user_edwprod.deal_inv_prd_redloc group by product_id
       ) b on a.product_id = b.product_id and a.load_key = b.max_load_key
       left join 
       user_edwprod.dim_offer_ext as x on x.inv_product_uuid = a.product_id
       where x.product_uuid = '46a51c03-0d27-4d86-8ab5-5ee330c09260'

       
select * from user_edwprod.deal_inv_prd_redloc;
select opportunity_id, count(DISTINCT deal_key) from user_groupondw.dim_deal group by opportunity_id;

select deal_key, count(deal_location_id), count(distinct deal_location_id) from user_groupondw.dim_deal_location group by deal_key;

select opportunity_id, count(distinct inventory_product_id), count(inventory_product_id) from dwh_base_sec_view.sf_multi_deal group by opportunity_id;
select * from user_edwprod.dim_offer_ext;
select * from user_groupondw.dim_deal_location where deal_key = 45679463;
select * from user_groupondw.dim_deal;

count(distinct concat( option_id, lat, lon)




select
a.close_wk,
case when a.vertical = 'TTD - Leisure' then 'TTD' else a.vertical end as vertical,
case when a.variant in ('1','2') then 'V1|V2' else 'V3' end as variant,
count(distinct a.account_id) as merchants,
count(distinct a.opportunity_id) as deals,
count(distinct a.opportunity_id) as services,
sum(a.options) as options,
sum(case when a.bookable_at_close = 1 then 1 else 0 end) as services_booking,
sum(case when a.unrestricted_flag = 1 then 1 else 0 end) as services_unrestricted,
sum(case when a.tier = 2 then 1 else 0 end) as services_tier2,
sum(case when a.tier = 3 then 1 else 0 end) as services_tier3,
sum(case when a.tier = 1 then 1 else 0 end) as services_tier1, 
count(distinct case when a.launched_flag = 1 then a.opportunity_id end) as launched_deals, 
sum(a.launched_flag) 
from sandbox.eh_to_closes a
where a.variant in ('1','2','3')
and a.merchant_type in ('New','Inactive','lead gen','New/Inactive')
and a.vertical in ('F&D','HBW','TTD - Leisure', 'H&A')
group by 1,2,3;
*/

----bd_productivity
----sandbox.jl_nick_bd_supply


select
cast(dw.week_end as date format 'yyyy-mm-dd') as report_wk,
case when a.vertical = 'services' then 'H&A' else a.vertical end as sales_vertical,
case when a.team in ('Team - TO DealsOffers','Team - TO DealsUnrestrictedOffers') then 'V1|V2' else 'V3' end as variant,
count(distinct a.emplid) as headcount,
count(distinct a.emplid || a.working_day) as rep_days,
sum(a.total_touches) as total_touches,
sum(a.appointments_held) as appts_held
from sandbox.jl_nick_bd_supply a
left join user_groupondw.dim_day dy on dy.day_rw = a.date_rw
left join user_groupondw.dim_week dw on dw.week_key = dy.week_key
where a.team in ('Team - TO DealsOffers','Team - TO DealsUnrestrictedOffers','Team - TO Deals')
group by 1,2,3;



-----bd_launches

case when a.variant in ('1','2') then 'V1|V2' 
when a.variant = '3' then 'V3' 
when 
a.variant = '4' and 
a.market in 
('chicago',
	'washington-dc',
	'north-jersey',
	'philadelphia',
	'boston',
	'baltimore',
	'portland',
	'stlouis'
	) then 'BAU' end as variant,

drop table nvp_to_bd_merchloc;

create volatile multiset table nvp_to_bd_merchloc as (
select
a.launch_wk,
case when a.vertical = 'TTD - Leisure' then 'TTD' else a.vertical end as vertical,
case when a.variant in ('1','2') then 'V1|V2' 
     when a.variant = '3' then 'V3' 
     when 
         a.market in 
         ('chicago',
	       'washington-dc',
	       'north-jersey',
	       'philadelphia',
	       'boston',
	       'baltimore',
	       'portland',
	       'stlouis') then 'BAU' end as variant,
count(distinct concat(a.Account_id, ddl2.longitude, ddl2.latitude)) merchant_loc
from sandbox.eh_to_closes a
left join 
  (select 
     dd.uuid as deal_uuid,
     longitude, 
     latitude
  from user_groupondw.dim_deal dd
  left join dwh_base_sec_view.sf_multi_deal sf on dd.opportunity_id = sf.opportunity_id
  left join user_groupondw.dim_deal_location ddl on dd.deal_key = ddl.deal_key
  group by 1,2,3
  ) ddl2 on ddl2.deal_uuid = a.deal_uuid
where 
a.merchant_type in ('New','Inactive','lead gen','New/Inactive')
and a.vertical in ('F&D','HBW','TTD - Leisure', 'H&A')
and a.launch_wk is not null
and a.launch_wk >= '2020-07-19'
group by 1,2,3) with data on commit preserve rows;



create volatile multiset table nvp_to_bd_launch_1 as (
select
a.launch_wk,
case when a.vertical = 'TTD - Leisure' then 'TTD' else a.vertical end as vertical,
case when a.variant in ('1','2') then 'V1|V2' 
     when a.variant = '3' then 'V3' 
     when 
         a.market in 
         ('chicago',
	       'washington-dc',
	       'north-jersey',
	       'philadelphia',
	       'boston',
	       'baltimore',
	       'portland',
	       'stlouis') then 'BAU' end as variant,
count(distinct a.account_id) as merchants,
count(distinct a.opportunity_id) as deals,
count(distinct a.opportunity_id) as services,
sum(a.options) as options,
sum(case when a.bookable_at_close = 1 then 1 else 0 end) as services_booking,
sum(case when a.unrestricted_flag = 1 then 1 else 0 end) as services_unrestricted,
sum(case when a.tier = 2 then 1 else 0 end) as services_tier2,
sum(case when a.tier = 3 then 1 else 0 end) as services_tier3,
sum(case when a.tier = 1 then 1 else 0 end) as services_tier1, 
sum(a.is_takeout_delivery) is_takeout_delivery,
sum(ddl.locations_ser) merchant_services_loc, 
sum(ddl2.locations_opt) merchant_options_loc
from sandbox.eh_to_closes a
left join
 (select
    dd.uuid as deal_uuid,
    count(distinct ddl.deal_location_key) as locations_ser
  from user_groupondw.dim_deal_location ddl
  left join user_groupondw.dim_deal dd on dd.deal_key = ddl.deal_key
    group by 1) ddl on ddl.deal_uuid = a.deal_uuid
left join 
  (select 
     dd.uuid as deal_uuid,
     count(distinct concat(inventory_product_id, longitude, latitude)) locations_opt
  from user_groupondw.dim_deal dd
  left join dwh_base_sec_view.sf_multi_deal sf on dd.opportunity_id = sf.opportunity_id
  left join user_groupondw.dim_deal_location ddl on dd.deal_key = ddl.deal_key
  group by 1
  ) ddl2 on ddl2.deal_uuid = a.deal_uuid
where a.merchant_type in ('New','Inactive','lead gen','New/Inactive')
and a.vertical in ('F&D','HBW','TTD - Leisure', 'H&A')
and a.launch_wk is not null
and a.launch_wk >= '2020-07-19'
group by 1,2,3) with data on commit preserve rows;


select 
a.*, 
b.merchant_loc
from 
nvp_to_bd_launch_1 as a
left join 
nvp_to_bd_merchloc as b 
on a.launch_wk = b.launch_wk 
and a.vertical = b.vertical 
and a.variant = b.variant
order by 1,2,3;

/*
select 
from 
sandbox.eh_to_closes a
left join 
(select 
     dd.uuid as deal_uuid,
     longitude, 
     latitude
  from user_groupondw.dim_deal dd
  left join dwh_base_sec_view.sf_multi_deal sf on dd.opportunity_id = sf.opportunity_id
  left join user_groupondw.dim_deal_location ddl on dd.deal_key = ddl.deal_key
  group by 1
) ddl2 on ddl2.deal_uuid = a.deal_uuid;
 
 
select 
a.*, 
ml.merch_loc
from 
  (select 
      launch_wk,
      case when vertical = 'TTD - Leisure' then 'TTD' else vertical end as vertical,
      case when variant in ('1','2') then 'V1|V2' else 'V3' end as variant,
      account_id
    from 
    sandbox.eh_to_closes group by 1,2,3,4) as a
left join 
  (select 
     op.Accountid account_id, 
	 count(distinct concat(ddl.longitude, ddl.latitude)) merch_loc
    from user_groupondw.dim_deal dd
    left join dwh_base_sec_view.opportunity_1 op on dd.opportunity_id = op.Opportunity_ID
    left join user_groupondw.dim_deal_location ddl on dd.deal_key = ddl.deal_key
    group by 1) ml on a.account_id = ml.account_id
;

select Account_id from sandbox.eh_to_closes;

select * from dwh_base_sec_view.opportunity_1 where Opportunity_ID = '006C000000xq0iZ';

*/



