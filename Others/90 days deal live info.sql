drop table sh_bt_launch_dates;
create volatile table sh_bt_launch_dates as (
    sel deal_uuid,
        max(has_gcal) has_gcal,
        cast(min(load_date) as date) launch_date
    from sandbox.sh_bt_active_deals_log_v4 ad
    where product_is_active_flag = 1
    and partner_inactive_flag = 0
    and country = 'US'
    and load_date >= '2019-04-01'
    group by 1
) with data unique primary index (deal_uuid) on commit preserve rows;



drop table np_deals_live_in_2021;
create volatile table np_deals_live_in_2021 as (
    sel deal_uuid,
        cast(max(load_date) as date) max_date_2021
    from sandbox.sh_bt_active_deals_log_v4 ad
    where product_is_active_flag = 1
    and partner_inactive_flag = 0
    and country = 'US'
    and load_date >= cast('2021-01-01' as date)
    group by 1
) with data unique primary index (deal_uuid) on commit preserve rows;

drop table np_deals_live_90days;
create volatile table np_deals_live_90days as (
select 
   deal_uuid, 
   max_date_2021, 
   launch_date, 
   xyz
   from 
(select 
   a.deal_uuid, 
   a.max_date_2021 - b.launch_date as xyz, 
   a.max_date_2021, 
   b.launch_date
   from np_deals_live_in_2021 as a 
   join sh_bt_launch_dates as b on a.deal_uuid = b.deal_uuid) fin where xyz >= 90
) with data unique primary index (deal_uuid) on commit preserve rows;


drop table np_txns;
create volatile table np_txns as (
    sel b.deal_uuid,
        b.max_date_2021, 
        b.launch_date, 
        b.xyz,
        sum(case when a.report_date >= cast(b.launch_date as date)
            then net_transactions_qty - zdo_net_transactions_qty end) units_lifetime, 
        case when units_lifetime is null or units_lifetime = 0 then 'a.no units sold'
             when units_lifetime <= 10 then 'b.<= 10 units sold'
             when units_lifetime <= 20 then 'c.11 - 20 units sold'
             when units_lifetime <= 30 then 'd.21 - 30 units sold'
             when units_lifetime <= 40 then 'e.31 - 40 units sold'
             when units_lifetime <= 50 then 'f.41 - 50 units sold'
             else 'g.> 50 units sold' end units_sold_bucket, 
        case when b.xyz = 90 then 'a.live for 90 days'
             when b.xyz <= 95 then 'b.live for 91-95 days'
             when b.xyz <= 100 then 'c.live for 96-100 days'
             when b.xyz <= 115 then 'd.live for 101-115 days'
             when b.xyz <= 125 then 'e.live for 116-125 days'
             when b.xyz > 125 then 'f.live for >125 days'
             end days_live_category
    from np_deals_live_90days as b 
       left join user_edwprod.agg_gbl_financials_deal as a on a.deal_id = b.deal_uuid
    where report_date >= '2020-01-01'
    group by 1,2,3,4
) with data unique primary index (deal_uuid) on commit preserve rows;

select units_sold_bucket, days_live_category, count(distinct deal_uuid) from np_txns group by 1,2;




--------------------
drop table np_deals_live_now;
create volatile table np_deals_live_now as (
    sel deal_uuid, 
        merchant_uuid,
        cast(max(load_date) as date) load_date
    from sandbox.sh_bt_active_deals_log_v4 ad
    left join (select product_uuid, max(merchant_uuid) merchant_uuid from user_edwprod.dim_offer_ext group by 1) as b on ad.deal_uuid = b.product_uuid
    where product_is_active_flag = 1
    and partner_inactive_flag = 0
    and country = 'US'
    and load_date >= current_date - 3
    group by 1,2
) with data unique primary index (deal_uuid) on commit preserve rows;

create volatile table sh_bt_launch_dates as (
    sel deal_uuid,
        max(has_gcal) has_gcal,
        cast(min(load_date) as date) launch_date
    from sandbox.sh_bt_active_deals_log_v4 ad
    where product_is_active_flag = 1
    and partner_inactive_flag = 0
    and country = 'US'
    and load_date >= '2019-04-01'
    group by 1
) with data unique primary index (deal_uuid) on commit preserve rows;

drop table np_deals_live_delta;
create volatile table np_deals_live_delta as (
select 
   deal_uuid,
   merchant_uuid,
   load_date,
   launch_date,
   xyz
   from
(select
   a.deal_uuid,
   a.merchant_uuid,
   a.load_date - b.launch_date as xyz,
   a.load_date, 
   b.launch_date
   from np_deals_live_now as a 
   join sh_bt_launch_dates as b on a.deal_uuid = b.deal_uuid) fin
) with data unique primary index (deal_uuid) on commit preserve rows;


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


drop table np_f_txns;
create volatile table np_f_txns as (
    sel b.deal_uuid,
        b.load_date, 
        b.launch_date, 
        b.xyz,
        case when bs.current_booking_solution = 'pen & paper' then 1 else 0 end pen_and_paper_merch,
        sum(case when a.report_date >= cast(b.launch_date as date)
            then net_transactions_qty - zdo_net_transactions_qty end) units_lifetime, 
        case when units_lifetime is null or units_lifetime = 0 then 'a.no units sold'
             when units_lifetime <= 10 then 'b.<= 10 units sold'
             when units_lifetime <= 20 then 'c.11 - 20 units sold'
             when units_lifetime <= 30 then 'd.21 - 30 units sold'
             when units_lifetime <= 40 then 'e.31 - 40 units sold'
             when units_lifetime <= 50 then 'f.41 - 50 units sold'
             else 'g.> 50 units sold' end units_sold_bucket, 
        case when b.xyz <= 5 or xyz is null then 'a.live for <=5 days'
             when b.xyz <= 14 then 'b.live for 6 - 14 days'
             when b.xyz <= 24 then 'c.live for 15 - 24 days'
             when b.xyz <= 49 then 'd.live for 25 - 49 days'
             when b.xyz <= 59 then 'e.live for 50 - 59 days'
             when b.xyz <= 89 then 'f.live for 60 - 89 days'
             when b.xyz <= 99 then 'g.live for 90-99 days'
             when b.xyz <= 114 then 'h.live for 100-114 days'
             when b.xyz <= 125 then 'i.live for 115-125 days'
             when b.xyz > 125 then 'j.live for >125 days'
             end days_live_category
    from np_deals_live_delta as b 
      left join (select * from user_edwprod.agg_gbl_financials_deal where report_date >= '2019-01-01') as a on a.deal_id = b.deal_uuid  
      left join sh_booking_solution as bs on a.deal_id = bs.deal_uuid
    group by 1,2,3,4,5
) with data unique primary index (deal_uuid) on commit preserve rows;


select units_sold_bucket, pen_and_paper_merch,  days_live_category, count(distinct deal_uuid) from np_f_txns group by 1,2,3;


-------------------------------------------------

select count(distinct merchant_uuid) from np_deals_live_now;


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


drop table np_merch_live_now;
create volatile table np_merch_live_now as (
    sel 
        merchant_uuid,
        max(case when bs.current_booking_solution = 'pen & paper' then 1 else 0 end) pen_and_paper_merch,
        cast(max(load_date) as date) load_date
    from sandbox.sh_bt_active_deals_log_v4 ad
    left join (select product_uuid, max(merchant_uuid) merchant_uuid from user_edwprod.dim_offer_ext group by 1) as b on ad.deal_uuid = b.product_uuid
    left join sh_booking_solution bs on ad.deal_uuid = bs.deal_uuid
    where product_is_active_flag = 1
    and partner_inactive_flag = 0
    and country = 'US'
    and load_date >= current_date - 3
    group by 1
) with data unique primary index (merchant_uuid) on commit preserve rows;


drop table sh_mc_launch_dates;
create volatile table sh_mc_launch_dates as (
    sel merchant_uuid,
        max(has_gcal) has_gcal,
        cast(min(load_date) as date) launch_date
    from sandbox.sh_bt_active_deals_log_v4 ad
    left join (select product_uuid, max(merchant_uuid) merchant_uuid from user_edwprod.dim_offer_ext group by 1) as b on ad.deal_uuid = b.product_uuid
    where product_is_active_flag = 1
    and partner_inactive_flag = 0
    and country = 'US'
    and load_date >= '2019-04-01'
    group by 1
) with data unique primary index (merchant_uuid) on commit preserve rows;



drop table np_account_live_delta;
create volatile table np_account_live_delta as (
select 
   merchant_uuid,
   pen_and_paper_merch,
   load_date,
   launch_date,
   xyz
   from 
(select 
   a.merchant_uuid,
   a.pen_and_paper_merch,
   a.load_date - b.launch_date as xyz, 
   a.load_date, 
   b.launch_date
   from np_merch_live_now as a 
   join sh_mc_launch_dates as b on a.merchant_uuid = b.merchant_uuid) fin
) with data unique primary index (merchant_uuid) on commit preserve rows;


drop table np_txns_init;
create multiset volatile table np_txns_init as (
select *       
       from user_edwprod.agg_gbl_financials_deal as a
        left join (select product_uuid, 
                          max(merchant_uuid) merchant_uuid 
                          from user_edwprod.dim_offer_ext 
                          group by 1) 
             as b on a.deal_id = b.product_uuid
        where report_date >= '2019-01-01') with data on commit preserve rows;


drop table np_f_txns;
create volatile table np_f_txns as (
    sel c.merchant_uuid,
        c.pen_and_paper_merch,
        c.load_date load_date, 
        c.launch_date launch_date, 
        c.xyz,
        sum(case when fin.report_date >= cast(c.launch_date as date)
            then net_transactions_qty - zdo_net_transactions_qty end) units_lifetime, 
        case when units_lifetime is null or units_lifetime = 0 then 'a.no units sold'
             when units_lifetime <= 10 then 'b.<= 10 units sold'
             when units_lifetime <= 20 then 'c.11 - 20 units sold'
             when units_lifetime <= 30 then 'd.21 - 30 units sold'
             when units_lifetime <= 40 then 'e.31 - 40 units sold'
             when units_lifetime <= 50 then 'f.41 - 50 units sold'
             else 'g.> 50 units sold' end units_sold_bucket, 
        case when c.xyz <= 5 or c.xyz is null then 'a.live for <=5 days'
             when c.xyz <= 14 then 'b.live for 6 - 14 days'
             when c.xyz <= 24 then 'c.live for 15 - 24 days'
             when c.xyz <= 49 then 'd.live for 25 - 49 days'
             when c.xyz <= 59 then 'e.live for 50 - 59 days'
             when c.xyz <= 89 then 'f.live for 60 - 89 days'
             when c.xyz <= 99 then 'g.live for 90-99 days'
             when c.xyz <= 114 then 'h.live for 100-114 days'
             when c.xyz <= 125 then 'i.live for 115-125 days'
             when c.xyz > 125 then 'j.live for >125 days'
             end days_live_category
    from 
     np_account_live_delta as c 
     left join np_txns_init as fin on fin.merchant_uuid = c.merchant_uuid
    group by 1,2,3,4,5
) with data unique primary index (merchant_uuid) on commit preserve rows;


drop table np_f_txns2;
create volatile table np_f_txns2 as (
    sel c.merchant_uuid,
        c.pen_and_paper_merch,
        c.load_date load_date, 
        c.launch_date launch_date, 
        c.xyz,
        sum(case when fin.report_date >= cast(c.launch_date as date) and fin.report_date <= cast(c.launch_date as date) + 30
            then net_transactions_qty - zdo_net_transactions_qty end) units_30_days, 
        case when units_30_days > 10 then 1 else 0 end sold_more_than_10_units,
        case when c.xyz >= 30 then 1 else 0 end live_more_than_30_days
    from 
     np_account_live_delta as c 
     left join np_txns_init as fin on fin.merchant_uuid = c.merchant_uuid
    group by 1,2,3,4,5,8
) with data unique primary index (merchant_uuid) on commit preserve rows;




select sold_more_than_10_units, live_more_than_30_days, pen_and_paper_merch, count(distinct merchant_uuid) from np_f_txns2 group by 1,2,3 order by 3,2,1;

select units_sold_bucket, pen_and_paper_merch ,days_live_category, count(distinct merchant_uuid) from np_f_txns group by 1,2, 3 ;
