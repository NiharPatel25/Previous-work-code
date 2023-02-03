-----------------------------NPS
drop table sh_bt_launch_dates;


create volatile table sh_booking_solution as (
    select b.merchant_uuid,
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
    left join  user_edwprod.dim_offer_ext as b on o2.deal_uuid = b.product_uuid
    group by b.merchant_uuid
) with data unique primary index (merchant_uuid) on commit preserve rows;

drop table sandbox.nvp_nps_us;
create multiset table sandbox.nvp_nps_us as (
select  
        identity_col merchant_uuid,
        country_code,
        /*split_part(split_part(nps, 'category'': ''', 2), '''', 1) category,*/
        split_part(split_part(split_part(nps, 'category": ', 2), ',',1), '}',1) category,
        cast(split_part(split_part(nps, 'score'': ', 2), ',', 1) as int) score,
        split_part(split_part(nps, 'reason'': ', 2), ',', 1) reason,
        cast(case when length(split_part(split_part(nps, 'time'': ''', 2), 'T', 1)) = 10 
                  then split_part(split_part(nps, 'time'': ''', 2), 'T', 1) end as date) event_date, 
        account_id, 
        account_name,
        detailed_booking_solution,
        metal_segmentation
   from sandbox.sh_nps_survey_data_final as a
   left join sh_booking_solution as b on a.identity_col = b.merchant_uuid
   where country_code = 'US_BT'
) with data;


create volatile table sh_bt_launch_dates as (
    sel deal_uuid,
        max(has_gcal) has_gcal,
        min(load_date) launch_date
    from sandbox.sh_bt_active_deals_log_v4 ad
    where product_is_active_flag = 1
    and partner_inactive_flag = 0
    and load_date >= '2019-04-01'
    group by 1
) with data unique primary index (deal_uuid) on commit preserve rows;

create volatile table nvp_gcal_merch as (
select 
   b.merchant_uuid, 
   max(c.grt_l2_cat_description) l2, 
   max(has_gcal) has_gcal, 
   min(gcal_launch_date) gcal_launch_date, 
   max(b.pds_cat_id) pds_cat_id,
   max(ret_v.pds_cat_name) pds_catname
   from 
(select 
   deal_uuid, 
   max(has_gcal) has_gcal,
   min(case when has_gcal = 1 then load_date end) gcal_launch_date
   from 
   sandbox.sh_bt_active_deals_log_v4 
   where product_is_active_flag = 1
   and partner_inactive_flag = 0
   group by 1) as a 
   join 
    user_edwprod.dim_offer_ext as b on a.deal_uuid = b.product_uuid
   join user_edwprod.dim_gbl_deal_lob as c on a.deal_uuid = c.deal_id and c.country_code = 'US'
   left join user_dw.v_dim_pds_grt_map ret_v on c.pds_cat_id = ret_v.pds_cat_id
    group by 1
) with data unique primary index(merchant_uuid) on commit preserve rows;


create volatile table merchant_appointments as (
select merchant_uuid, 
       count(distinct booking_id) bookings_received
from sandbox.sh_bt_bookings_rebuild
     where cast(substr(created_at,1,10) as date) >= current_date - 60
group by 1) with data unique primary index(merchant_uuid) on commit preserve rows;

create volatile table merchant_units_sold as (
select 
    merchant_uuid, 
    count(distinct parent_order_uuid) orders
    from 
    user_edwprod.fact_gbl_transactions as a 
    where action = 'authorize'
         and a.order_date >= current_date - 60
    group by 1
) with data unique primary index(merchant_uuid) on commit preserve rows;




drop table sandbox.np_nps_tableau_input;
create table sandbox.np_nps_tableau_input as (
select 
    fin.*,
    row_number() over(partition by merchant_uuid order by event_date) fent, 
    case when bookings_received is null or bookings_received = 0 then 'a. No bookings received in last 60 days'
         when bookings_received <= 5 then 'b. <= 5 bookings received'
         when bookings_received <= 10 then 'c. 6 - 10 bookings received'
         when bookings_received <= 15 then 'd. 11 - 15 bookings received'
         when bookings_received >15 then 'e. > 15 bookings received'
         end appointments_received_category, 
    case when orders is null or bookings_received = 0 then 'a. No orders received in last 60 days'
         when orders <= 5 then 'b. <= 5 orders received'
         when orders <= 10 then 'c. 6 - 10 orders received'
         when orders <= 15 then 'd. 11 - 15 orders received'
         when orders >15 then 'e. > 15 orders received'
         end orders_received_category
from 
(select 
     a.event_date,
     trunc(a.event_date, 'iw') + 6 event_week,
     b.gcal_launch_date,
     case when cast(a.event_date as date) > cast(gcal_launch_date as date) then 'a.survey after gcal launched' 
          when cast(a.event_date as date) <= cast(gcal_launch_date as date) then 'b.survey before gcal launched'
          when cast(a.event_date as date) is not null then 'c.no gcal merchant' 
          end gcal_category,
     a.merchant_uuid, 
     a.account_id, 
     a.account_name,
     a.detailed_booking_solution,
     a.metal_segmentation,
     b.l2, 
     a.score, 
     a.category, 
     a.reason, 
     b.pds_catname,
     b.has_gcal,
     case when b.merchant_uuid is null or event_date is null then 1 else 0 end ignore_data, 
     case when bss.merchant_uuid is not null then 1 else 0 end bss_merchant, 
     act.acct_owner,
     appts.bookings_received, 
     units.orders
from sandbox.nvp_nps_us as a 
left join nvp_gcal_merch as b on a.merchant_uuid = b.merchant_uuid
left join (select merchant_uuid from sandbox.nvp_bss_funnel where onboarded = 1) as bss on a.merchant_uuid = bss.merchant_uuid
left join (select merchant_uuid ,max(acct_owner) acct_owner from sandbox.sh_acct_and_opp_owner group by 1) as act on a.merchant_uuid = act.merchant_uuid
left join merchant_appointments appts on a.merchant_uuid = appts.merchant_uuid
left join merchant_units_sold units on a.merchant_uuid = units.merchant_uuid
) as fin
) with data
;

select * from sandbox.sh_acct_and_opp_owner;
select * from sandbox.np_nps_tableau_input;


select 
     gcal_category, 
     count(distinct case when category = '"promoter"' then merchant_uuid end) promoters,
     count(distinct case when category = '"detractor"' then merchant_uuid end) detractor, 
     count(distinct case when category = '"passive"' then merchant_uuid end) passive
     from 
     sandbox.np_nps_tableau_input 
     where 
       event_date <= cast('2021-03-31' as date) and event_date >= cast('2021-01-01' as date)
     group by 1;

select 
     acct_owner, 
     count(distinct case when category = '"promoter"' then merchant_uuid end) promoters,
     count(distinct case when category = '"detractor"' then merchant_uuid end) detractor, 
     count(distinct case when category = '"passive"' then merchant_uuid end) passive
     from 
     sandbox.np_nps_tableau_input 
     where 
       event_date <= cast('2021-03-31' as date) and event_date >= cast('2021-01-01' as date)
     group by 1;


select 
     appointments_received_category, 
     count(distinct case when category = '"promoter"' then merchant_uuid end) promoters,
     count(distinct case when category = '"detractor"' then merchant_uuid end) detractor, 
     count(distinct case when category = '"passive"' then merchant_uuid end) passive
     from 
     sandbox.np_nps_tableau_input 
     where 
       event_date <= cast('2021-03-31' as date) and event_date >= cast('2021-01-01' as date)
     group by 1
     order by 1;
    
    
    


---------------------------------ANALYTICS





select 
   gcal_category,
   sum(promoters), 
   sum(detractor), 
   sum(passive), 
   sum(total_reviews)
from 
(select 
   gcal_category,
   count(distinct case when category = '"promoter"' then merchant_uuid end) promoters,
   count(distinct case when category = '"detractor"' then merchant_uuid end) detractor, 
   count(distinct case when category = '"passive"' then merchant_uuid end) passive, 
   promoters + detractor + passive total_reviews
   from 
(select 
     a.event_date,
     b.gcal_launch_date,
     case when a.event_date > gcal_launch_date then 'a.survey after gcal launched' 
          when a.event_date <= gcal_launch_date then 'b.survey before gcal launched'
          when a.event_date is not null then 'c.no gcal merchant' 
          end gcal_category,
     a.merchant_uuid, 
     b.l2,
     case when b.merchant_uuid is null or event_date is null then 1 else 0 end ignore_data, 
     a.score, 
     a.category, 
     a.reason
from sandbox.nvp_nps_us as a 
left join nvp_gcal_merch as b on a.merchant_uuid = b.merchant_uuid
where a.event_date <= cast('2021-03-31' as date) and event_date >= cast('2021-01-01' as date)
) as a 
group by 1) as a 
group by 1
order by 1;




select 
   gcal_category,
   sum(promoters), 
   sum(detractor), 
   sum(passive), 
   sum(total_reviews)
from 
(select 
   gcal_category,
   count(distinct case when category = '"promoter"' then merchant_uuid end) promoters,
   count(distinct case when category = '"detractor"' then merchant_uuid end) detractor, 
   count(distinct case when category = '"passive"' then merchant_uuid end) passive, 
   promoters + detractor + passive total_reviews
   from 
(select 
     a.event_date,
     b.gcal_launch_date,
     case when a.event_date > gcal_launch_date then 'a.survey after gcal launched' 
          when a.event_date <= gcal_launch_date then 'b.survey before gcal launched'
          when a.event_date is not null then 'c.no gcal merchant' 
          end gcal_category,
     a.merchant_uuid, 
     b.l2,
     case when b.merchant_uuid is null or event_date is null then 1 else 0 end ignore_data, 
     a.score, 
     a.category, 
     a.reason
from sandbox.nvp_nps_us as a 
left join nvp_gcal_merch as b on a.merchant_uuid = b.merchant_uuid
where a.event_date <= cast('2021-03-31' as date) and event_date >= cast('2021-01-01' as date)
) as a 
group by 1) as a 
group by 1
order by 1;



-----------------------------NPS  trials

drop table nvp_nps;

create volatile multiset table nvp_nps as 
(select
   country_code, 
   cast(
       case when length(split_part(split_part(nps, 'time": ', 2), ',',1)) = 26
       then 
       substr(split_part(split_part(nps, 'time": ', 2), ',',1), 2,10) end as date) record_date, 
   cast(
       case 
       when length(split_part(split_part(nps, 'score": ', 2), ',',1)) < 5 
       then split_part(split_part(nps, 'score": ', 2), ',',1) end as int) score, 
   split_part(split_part(split_part(nps, 'category": ', 2), ',',1), '}',1) category
from sandbox.sh_nps_survey_data_final
) with data on commit preserve rows;


select * from sandbox.sh_nps_survey_data_final;

select 
    year(record_date) yr, 
    month(record_date) mnth, 
    category,
    count(1) counts,
    avg(score) avg_nps
from nvp_nps
where country_code = 'US_BT'
GROUP BY 1,2,3
order by 1,2,3
;

select 
    cast(month_start as date) month_start, 
    count(distinct case when category = '"promoter"' then merchant_uuid end) promoters,
    count(distinct case when category = '"detractor"' then merchant_uuid end) detractor, 
    count(distinct case when category = '"passive"' then merchant_uuid end) passive, 
    promoters + detractor + passive total_reviews, 
    cast(promoters as float)/total_reviews proms, 
    cast(detractor as float)/total_reviews dets, 
    proms-dets NPS
from sandbox.nvp_nps_us nps
    join user_groupondw.dim_day dd on nps.event_date = dd.day_rw
    join user_groupondw.dim_month dm on dd.month_key = dm.month_key
    group by 
    1
   order by 1;


--------TRIALS

select 
substr(
substr(split_part(split_part(split_part(nps, 'category": ', 2), ',',1), '}',1), 2),1,
length(substr(split_part(split_part(split_part(nps, 'category": ', 2), ',',1), '}',1), 2))) from sandbox.sh_nps_survey_data_final;

select 
split_part(split_part(split_part(nps, 'category": ', 2), ',',1), '}',1)
from 
sandbox.sh_nps_survey_data_final;

select 
substr(
CAST(split_part(split_part(nps, 'category": ', 2), ',',1) AS varchar(64)), 1,
length(split_part(split_part(nps, 'category": ', 2), ',',1))-5)
from sandbox.sh_nps_survey_data_final;




-------
{'time': '2020-07-29T00:34:24.000Z', 
'reason': 'the advertisement', 
'respondent_id': 197040655, 
'response_uri': '/reports/187220/browse?response_id=197040655', 
'score': 10, 
'category': 'promoter'}


{"score": 6, 
"reason": "", 
"respondent_id": 139676791, 
"time": "2018-05-09T22:08:21.000Z", 
"category": "detractor", 
"response_uri": "/reports/187220/browse?response_id=139676791"}