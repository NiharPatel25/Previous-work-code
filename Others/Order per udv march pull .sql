
select * from sandbox.sh_bt_bookings_rebuild where country_id = 'US' and start_time >= '2021-04-01';

select
a.tablename,
sum (currentperm)/1024**3 as current_GB
from dbc.allspace a
join dbc.tables t on a.databasename = t.databasename and a.tablename = t.tablename
where creatorname = 'nihpatel'
group by 1
order by 2 desc


select * from np_march_reports;

drop table np_march_reports;
create volatile multiset table np_march_reports as (
select
    deal_id,
    case when sbt.deal_uuid is not null then 1 else 0 end bookable,
    sum(uniq_deal_views) deal_views,
    sum(transactions) all_transaction,
    sum(transactions_qty) units, 
    sum(parent_orders_qty) parent_orders_qty
from
   user_edwprod.agg_gbl_traffic_fin_deal a 
   left join sandbox.sh_bt_active_deals_log_v4 sbt 
        on a.deal_id = sbt.deal_uuid 
        and cast(a.report_date as date) = cast(sbt.load_date as date) 
        and sbt.product_is_active_flag = 1 
        and sbt.partner_inactive_flag = 0
where  
    report_date >= cast('2021-03-01' as date) 
    and report_date <= cast('2021-03-31' as date) 
    and country_code = 'US'
    and grt_l2_cat_name = 'L2 - Health / Beauty / Wellness'
    and deal_id <> 'feb48d63-3f9f-4ec3-8fe4-6d5e9ca0dbea'
    group by 1,2
) with data on commit preserve rows
;


create volatile table np_active_deals as (
    sel ad.deal_uuid
    from sandbox.sh_bt_active_deals_log ad
    where product_is_active_flag = 1
    and partner_inactive_flag = 0
    and load_date >= cast('2021-03-01' as date) 
    and load_date <= cast('2021-03-31' as date)
    group by 1
) with data unique primary index (deal_uuid) on commit preserve rows;


drop table np_to_merch;
create volatile table np_to_merch as (
select deal_uuid
       from dwh_base_sec_view.opportunity_1 o1
       join dwh_base_sec_view.opportunity_2 o2 on o1.id = o2.id
       where division in ('Long Island','Seattle','Detroit','Denver', 'Dallas', 'Fort Worth')
       group by deal_uuid) 
      with data unique primary index (deal_uuid) on commit preserve rows;

-------------------------------------------------------------
drop table np_availability;
create volatile table np_availability as (
sel deal_uuid,
    report_weekdate,
          load_week,
          num_dow, 
          num_dow_total,
          completely_blocked_days, 
          completely_paused_days
      from sandbox.nvp_weekstart_avail
          where load_week >= cast('2021-03-01' as date) and report_weekdate <= cast('2021-03-31' as date) and country = 'US'
          qualify ROW_NUMBER () over(partition by deal_uuid order by report_weekdate) = 1
) with data unique primary index (deal_uuid) on commit preserve rows;

create volatile table np_availability as (
sel deal_uuid,
          avg(num_dow) num_dow
      from sandbox.nvp_weekstart_avail
          where load_week >= cast('2021-03-01' as date) and report_weekdate <= cast('2021-03-31' as date) and country = 'US'
          group by 1
) with data unique primary index (deal_uuid) on commit preserve rows;

----------------------------------------------------------------


drop table np_offers;
create volatile table np_offers as (
sel distinct deal_uuid from sandbox.rev_mgmt_tiered_offerings where deal_tier = 1 group by 1) 
with data unique primary index(deal_uuid) on commit preserve rows

create volatile table sh_booking_solution as (
    select o2.deal_uuid,
        max(o1.id) opp_id,
        max(sfa.id) account_id,
        max(case when lower(sfa.scheduler_setup_type) in ('pen & paper','none') then 'pen & paper'
            when sfa.scheduler_setup_type is null then 'no data'
            else 'some booking tool'
            end) current_booking_solution,
        max(sfa.scheduler_setup_type) detailed_booking_solution,
        max(sfa.name) account_name,
        max(company_type) company_type,
        max(o1.division) division,
        max(sfp.full_name) account_owner, 
        max(metal_at_close) metal_segmentation
    from dwh_base_sec_view.sf_opportunity_1 o1
    join dwh_base_sec_view.sf_opportunity_2 o2 on o1.id = o2.id
    join dwh_base_sec_view.sf_account sfa on o1.accountid = sfa.id
    left join user_groupondw.dim_sf_person sfp on sfa.ownerid = sfp.person_id
    left join sandbox.rev_mgmt_deal_attributes mat on o2.deal_uuid = mat.deal_id
    group by o2.deal_uuid
) with data unique primary index (deal_uuid) on commit preserve rows;

select * from sh_booking_solution where deal_uuid = '7cdc564b-8e19-4c11-8446-8b004f08a77c';
select * from dwh_base_sec_view.sf_opportunity_2 where deal_uuid = '7cdc564b-8e19-4c11-8446-8b004f08a77c';
--------Analytics pull

drop table sandbox.np_deals_conversion;
create multiset table sandbox.np_deals_conversion as (
select 
    * 
    from
(select 
    a.deal_id,
    a.bookable,
    coalesce(deal_tier,0 ) deal_tier ,
    case when bs.deal_uuid is not null then 1 else 0 end bs_solutions,
    case when dream.account_id is not null then 1 else 0 end dream_deal,
    case when offers.deal_uuid is not null then 1 else 0 end offers,
    case when b.deal_uuid is not null then 'TO market' else 'Non - TO market' end market_type, 
    case when a.bookable = 0 then 'a.non bookable'
         when c.deal_uuid is null then 'b.no data for availability'
         when c.num_dow is null or round(c.num_dow) = 0 then 'c.no availability'
         when round(c.num_dow) >0 then 'd.has availability' end deal_category,
    case when a.bookable = 1 then c.num_dow end num_dow,
    round(case when a.bookable = 1 then c.num_dow end) round_num_dow,
    sum(a.deal_views) deal_views,
    sum(a.all_transaction) all_transaction
from
    np_march_reports as a 
    left join sh_booking_solution bs on a.deal_id = bs.deal_uuid
    left join sandbox.ar_hbw_dream_merchants as dream on bs.account_id = dream.account_id
    left join np_to_merch as b on a.deal_id = b.deal_uuid
    left join np_availability as c on a.deal_id = c.deal_uuid
    left join np_offers as offers on a.deal_id = offers.deal_uuid
    left join (
        sel deal_uuid, max(deal_tier) deal_tier
        from sandbox.rev_mgmt_tiered_offerings
        group by 1
    ) rmto on a.deal_id = rmto.deal_uuid
    group by 1,2,3,4,5,6,7,8,9,10) fin
) with data
;



select bookable, deal_tier, bs_solutions, dream_deal, offers, market_type, deal_category, round_num_dow, sum(deal_views) deal_views, sum(all_transaction) all_transaction
from sandbox.np_deals_conversion
group by 1,2,3,4,5,6,7,8;


select deal_id, count(1) cnz from sandbox.np_deals_conversion where bookable = 0 group by 1 having cnz > 1;
select * from sandbox.np_deals_conversion;








select 
    * 
    from
(select 
    a.bookable,
    case when offers.deal_uuid is not null then 1 else 0 end offers,
    case when b.deal_uuid is not null then 'TO market' else 'Non - TO market' end market_type, 
    case when a.bookable = 0 then 'a.non bookable'
         when c.deal_uuid is null then 'b.no data for availability'
         when c.num_dow is null or c.num_dow = 0 then 'c.no availability'
         when c.num_dow >0 then 'd.has availability' end deal_category,
    sum(a.deal_views) deal_views,
    sum(a.all_transaction) all_transaction
from 
    np_march_reports as a 
    join sh_booking_solution bs on a.deal_id = bs.deal_uuid
    join sandbox.ar_hbw_dream_merchants as dream on bs.account_id = dream.account_id
    left join np_to_merch as b on a.deal_id = b.deal_uuid
    left join np_availability as c on a.deal_id = c.deal_uuid
    left join np_offers as offers on a.deal_id = offers.deal_uuid
    group by 1,2,3,4
    ) fin where offers = 0 
    order by 1,3,4


select 
    case when dream.account_id is not null then 1 else 0 end dream_deal, 
    
   from 
   np_active_deals as a 
   join sh_booking_solution bs on a.deal_id = bs.deal_uuid
   left join sandbox.ar_hbw_dream_merchants as dream on bs.account_id = dream.account_id
   left join np_to_merch as b on a.deal_id = b.deal_uuid
   left join np_offers as offers on a.deal_id = offers.deal_uuid

    
    
    
select 
    a.deal_id,
    a.bookable,
    case when b.deal_uuid is not null then 'TO market' else 'Non - TO market' end market_type, 
    case when a.bookable = 0 then 'a.non bookable'
         when c.deal_uuid is null then 'b.no data for availability'
         when c.num_dow is null or c.num_dow = 0 then 'c.no availability'
         when c.num_dow >0 then 'd.has availability' end deal_category,
    case when a.bookable = 0 then null
         when c.deal_uuid is null then null
         else c.num_dow end availability_num_of_days_per_week,
    sum(a.deal_views) deal_views,
    sum(a.all_transaction) all_transaction
from 
    np_march_reports as a 
    left join np_to_merch as b on a.deal_id = b.deal_uuid
    left join np_availability as c on a.deal_id = c.deal_uuid
    group by 1,2,3,4, 5
    order by 1,2,3,4;
    
   

   
   