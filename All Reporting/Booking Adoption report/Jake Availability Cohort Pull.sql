select report_weekdate, load_week, deal_uuid from sandbox.nvp_weekstart_avail where deal_uuid is not null order by 3,1 desc;

create volatile table sh_bt_deals as (
    sel a.deal_uuid,
        sf.opportunity_id,
        sf.account_name,
        sf.account_owner,
        sf.account_id,
        cast(bt_launch_date as date) bt_launch_date,
        sf.division,
        max(has_gcal) has_gcal, 
        gdl.grt_l2_cat_description l2
    from sandbox.sh_bt_active_deals_log_v4 a
    join user_edwprod.dim_gbl_deal_lob gdl on a.deal_uuid = gdl.deal_id
    left join (
        sel deal_uuid,
            min(load_date) bt_launch_date
        from sandbox.sh_bt_active_deals_log
        where product_is_active_flag = 1
        and partner_inactive_flag = 0
        group by 1
        ) l on a.deal_uuid = l.deal_uuid
    left join (
        sel deal_uuid, max(o1.id) opportunity_id, max(o1.division) division, max(sfa.name) account_name, max(full_name) account_owner, max(o1.accountid) account_id
           from user_edwprod.sf_opportunity_1 o1
           join user_edwprod.sf_opportunity_2 o2 on o1.id = o2.id
           join user_edwprod.sf_account sfa on o1.accountid = sfa.id
           left join user_groupondw.dim_sf_person sfp on sfa.ownerid = sfp.person_id
        group by 1
        ) sf on a.deal_uuid = sf.deal_uuid
    where product_is_active_flag = 1
    and partner_inactive_flag = 0
    and is_bookable = 1
    and sold_out = 'false'
    and load_date between cast('2021-03-01' as date) and cast('2021-03-31' as date)
    and gdl.country_code = 'US'
    and gdl.grt_l1_cat_name = 'L1 - Local'
    group by 1,2,3,4,5,6,7,9
) with data unique primary index (deal_uuid) on commit preserve rows;

drop table np_availability_avg;
create volatile table np_availability_avg as (
select 
    deal_uuid, 
    country, 
    average(num_dow) num_dow_avg, 
    average(num_dow_total) num_dow_total_avg
from sandbox.nvp_weekstart_avail
    where cast(load_week as date) >= cast('2021-03-01' as date) and cast(load_week as date) <= cast('2021-03-31' as date)
group by 1,2
) with data unique primary index (deal_uuid, country) on commit preserve rows;



create multiset table sandbox.np_march_deal_availability as (
select 
    a.*, 
    case when b.deal_uuid is null then 'a. availability setup/data issue'
         when num_dow_total_avg < 1 then 'b. 0 capacity next week'
         when num_dow_avg < 1 then 'c. 0 days availability next week'
         when bdd.avg_booking_delay > 24 then 'd. booking leadtime >24 hours'
         when num_dow_avg < 4 then 'e. less than 4 days available next week'
         else 'f. No availability issue'
    end availability_cohort,
    case when b.deal_uuid is null then 'Availability - None'
         when num_dow_total_avg < 1 then 'Availability - None'
         when num_dow_avg < 1 then 'Availability - filled'
         when bdd.avg_booking_delay > 24 then 'Long Lead time'
         when num_dow_avg < 4 then 'Availability - Low'
         else 'No availability issue' end availability_cohort2
    from 
       sh_bt_deals as a 
       left join np_availability_avg as b on a.deal_uuid = b.deal_uuid and b.country = 'US'
       left join sandbox.sh_dream_booking_delay_deal bdd on a.deal_uuid = bdd.deal_uuid
) with data unique primary index (deal_uuid)
;


select availability_cohort2, availability_cohort, count(1) from sandbox.np_march_deal_availability group by 1,2 order by 2;