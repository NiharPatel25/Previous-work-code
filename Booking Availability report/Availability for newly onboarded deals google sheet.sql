create volatile multiset table nvp_bt_deals as 
(select 
     deal_uuid,
     min(load_date) mn_bt_launch
from sandbox.sh_bt_active_deals_log_v4
where 
    is_bookable = 1 and product_is_active_flag = 1 and partner_inactive_flag = 0
    group by 1)
    with data on commit preserve rows;
;

drop table nvp_bt_deals_availability_0;
create volatile table nvp_bt_deals_availability_0 as (
select 
   a.deal_uuid,  
   a.launch_week,
   cast(dal.load_week as date) load_week_avail,
   dal.num_dow,
   max(case when day_available = 'Mon' then 1 end) Mon, 
   max(case when day_available = 'Tue' then 1 end) Tue, 
   max(case when day_available = 'Wed' then 1 end) Wed, 
   max(case when day_available = 'Thu' then 1 end) Thu, 
   max(case when day_available = 'Fri' then 1 end) Fri, 
   max(case when day_available = 'Sat' then 1 end) Sat, 
   max(case when day_available = 'Sun' then 1 end) Sun
   from 
   (select 
      deal_uuid, 
      trunc(cast(mn_bt_launch as date),'iw')+6 launch_week
    from 
    nvp_bt_deals) as a 
left join 
   (sel deal_uuid,
        load_week,
        count(distinct day_available) num_dow
    from sandbox.nvp_weekday_avail
    group by 1,2
    ) dal on a.deal_uuid = dal.deal_uuid and a.launch_week + 7 = cast(dal.load_week as date)
left join 
   (sel deal_uuid,
        load_week,
        day_available
    from sandbox.nvp_weekday_avail
    group by 1,2,3
    ) dal2 on a.deal_uuid = dal2.deal_uuid and a.launch_week + 7 = cast(dal2.load_week as date)
where a.launch_week > current_date - 45
group by 1,2,3,4
) 
with data primary index(deal_uuid) on commit preserve rows;



drop table sandbox.nvp_bt_deals_availability;
create multiset table sandbox.nvp_bt_deals_availability as (
select 
    launch_week,
    load_week_avail, 
    case when num_dow is null then 'availability not setup'
         when num_dow = 0 then '0 day''s available'
         when num_dow = 1 then '1 day available'
         when num_dow = 2 then '2 days available'
         when num_dow = 3 then '3 days available'
         when num_dow = 4 then '4 days available'
         when num_dow >= 5 then '>= 5 days available'
    end as availability_cohort, 
    count(distinct deal_uuid) deals_launched, 
    sum(Mon) Mon, 
    sum(Tue) Tue, 
    sum(Wed) Wed, 
    sum(Thu) Thu, 
    sum(Fri) Fri, 
    sum(Sat) Sat, 
    sum(Sun) Sun
    from 
         nihpatel.nvp_bt_deals_availability_0 as a 
    group by 1,2,3) with data;
    
select * from sandbox.nvp_bt_deals_availability order by launch_week desc, availability_cohort;


----------CREATING DATA

CREATE MULTISET TABLE sandbox.nvp_weekday_avail ,NO FALLBACK ,
      NO BEFORE JOURNAL,
      NO AFTER JOURNAL,
      CHECKSUM = DEFAULT,
      DEFAULT MERGEBLOCKRATIO,
      MAP = TD_MAP1
      (
        deal_uuid VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
        country	VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
        load_week	VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
        day_available VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC
      )
 NO PRIMARY INDEX ;
 


drop table grp_gdoop_bizops_db.nvp_temp_weekday_avail;
create table grp_gdoop_bizops_db.nvp_temp_weekday_avail stored as orc as
select
    deal_uuid,
    country,
    date_sub(next_day(reference_date, 'MON'), 1) load_week,
    reference_date,
    day_available
from
(select
      avail.deal_uuid,
      avail.country ,
      avail.reference_date,
      case when gss_available_minutes > 0 then date_format(reference_date,'E') end day_available
from
  (select *
       from grp_gdoop_bizops_db.jk_bt_availability_gbl_2
       where
       report_date >= cast('2020-09-01' as date)
       and days_delta <= 8
   ) avail
   WHERE country= 'US'
 ) fin
 group by deal_uuid, country, date_sub(next_day(reference_date, 'MON'), 1), day_available, reference_date
 ;



select 