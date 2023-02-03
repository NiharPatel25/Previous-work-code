select count(distinct deal_uuid) from sh_bt_deals where region = 'INTL';

drop table sh_bt_deals;
create volatile table sh_bt_deals as (
    sel a.deal_uuid,
        gdl.country_code,
        case when gdl.country_code = 'US' then 'NAM' else 'INTL' end region,
        gdl.grt_l2_cat_description l2,
        sf.opportunity_id,
        account_name,
        account_owner,
        account_id,
        cast(bt_launch_date as date) bt_launch_date,
        division,
        max(has_gcal) has_gcal
    from sandbox.sh_bt_active_deals_log a
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
    and a.sold_out = 'false'
    and load_date  = current_date - 3
    and gdl.grt_l1_cat_name = 'L1 - Local'
    group by 1,2,3,4,5,6,7,8,9,10
) with data unique primary index (deal_uuid) on commit preserve rows;

drop table sandbox.nvp_future_earliest_availability;
create multiset table sandbox.nvp_future_earliest_availability as (
select 
    a.*,
    case when b.deal_uuid is null then 'no availability_data' 
         when earliest_availability  < trunc(current_date, 'iw')+6 then 'available this week' 
         when earliest_availability  <= trunc(current_date, 'iw')+6 + 7 then 'available within a week' 
         when earliest_availability  <= trunc(current_date, 'iw')+6 + 14 then 'availablity within 2 weeks'
         when earliest_availability  <= trunc(current_date, 'iw')+6 + 30 then 'availability within a month'
         when earliest_availability  <= trunc(current_date, 'iw')+6 + 61 then 'availability within 2 months'
         when earliest_availability  <= trunc(current_date, 'iw')+6 + 91 then 'availability within 3 months'
         else 'no availability within 3 months' end 
         availability_cohort
from 
     sh_bt_deals as a 
     left join 
     (select deal_uuid, country, earliest_availability, latest_refresh_date
             from sandbox.nvp_future_avail 
             ) as b on a.deal_uuid = b.deal_uuid and a.country_code = b.country
      group by  1,2,3,4,5,6,7,8,9,10,11,12
) with data;

select * from sandbox.nvp_future_avail;

select 
country_code, 
region, 
l2,
availability_cohort, 
count(distinct deal_uuid) count_of_deals
from 
sandbox.nvp_future_earliest_availability
group by 1,2,3,4
order by 1,2,3,4




-----hive

drop table grp_gdoop_bizops_db.nvp_temp_future;
create table grp_gdoop_bizops_db.nvp_temp_future stored as orc as
select
    deal_uuid,
    country,
    date_sub(next_day(reference_date, 'MON'), 1) load_week,
    count(distinct day_available) num_dow, 
    count(distinct day_available_total) num_dow_total
from
(select
      avail.deal_uuid,
      avail.country ,
      avail.reference_date,
      case when gss_available_minutes > 0 then date_format(reference_date,'E') end day_available, 
      case when gss_total_minutes > 0 then date_format(reference_date,'E') end day_available_total
from
  (select *
       from grp_gdoop_bizops_db.jk_bt_availability_gbl_2
       where
       report_date >= date_sub(current_date,30)
   ) avail
 ) fin
 group by deal_uuid, country, date_sub(next_day(reference_date, 'MON'), 1)
 ;



----teradata

drop table sandbox.nvp_weekwise_avail;

CREATE MULTISET TABLE sandbox.nvp_future_avail ,NO FALLBACK ,
      NO BEFORE JOURNAL,
      NO AFTER JOURNAL,
      CHECKSUM = DEFAULT,
      DEFAULT MERGEBLOCKRATIO,
      MAP = TD_MAP1
      (
        deal_uuid VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
        country	VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
        load_week	VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
        num_dow integer, 
        num_dow_total integer
      )
 NO PRIMARY INDEX ;
 



