-----------------------------------------------WoW Progression

select a.availability_cohort2, b.availability_cohort2, count(1) from 
(select 
     *
from sandbox.nvp_hbw_booking_status_deal2 
where load_week = trunc(current_date - 7, 'iw')+6
      and availability_cohort2 = 'No availability issue'
      ) as a 
left join 
(select 
     *
from sandbox.nvp_hbw_booking_status_deal2 
where load_week = trunc(current_date - 14, 'iw')+6
) as b on a.deal_uuid = b.deal_uuid
group by 1,2



-----------------------------------------------consecutively 4 Weeks 
select 
action_cohort2, 
case when (has_availability_setup = 0 or latest_availability_not_data  = 1) then 'no latest avail. data or setup issue' else num_dow_total end availability, 
count(distinct deal_uuid) deal_count
from 
sandbox.nvp_hbw_booking_status_deal
group by 1,2
order by 1,2;


select count(distinct account_id)
from sandbox.nvp_hbw_booking_status_deal
where availability_cohort2 in ('Availability - None', 'Availability - None at all');

select 
   a.*
from
(select 
     *
from sandbox.nvp_hbw_booking_status_deal2 
where load_week = trunc(current_date - 7, 'iw')+6
      and availability_cohort2 in ('Availability - None', 'Availability - None at all')
      and focus_merchant = 1) as a 
join 
(select 
     *
from sandbox.nvp_hbw_booking_status_deal2 
where load_week = trunc(current_date - 14, 'iw')+6
      and availability_cohort2 in ('Availability - None', 'Availability - None at all')
      and focus_merchant = 1
) as b on a.deal_uuid = b.deal_uuid
join 
(select 
     *
from sandbox.nvp_hbw_booking_status_deal2 
where load_week = trunc(current_date - 21, 'iw')+6
      and availability_cohort2 in ('Availability - None', 'Availability - None at all')
      and focus_merchant = 1 
) as c on a.deal_uuid = c.deal_uuid
join 
(select 
     *
from sandbox.nvp_hbw_booking_status_deal2 
where load_week = trunc(current_date - 28, 'iw')+6
      and availability_cohort2 in ('Availability - None', 'Availability - None at all')
      and focus_merchant = 1
) as d on a.deal_uuid = d.deal_uuid


select * from sandbox.nvp_hbw_booking_status_deal2;
------------------------------------------------------------------NO AVAILABILITY 4 WEEKS (REMOVED FOCUS MERCHANT CRITERIA


drop table sandbox.nvp_agg_reporting;
CREATE MULTISET TABLE sandbox.nvp_agg_reporting
     (
      report_date date,
      report_type VARCHAR(64) CHARACTER SET UNICODE,
      deal_uuid VARCHAR(64) CHARACTER SET UNICODE,
      opportunity_id VARCHAR(64) CHARACTER SET UNICODE,
      account_name VARCHAR(64) CHARACTER SET UNICODE,
      account_owner VARCHAR(64) CHARACTER SET UNICODE,
      account_id VARCHAR(64) CHARACTER SET UNICODE,
      bt_launch_date date,
      units_sold_t30 INTEGER,
      availability_cohort2 VARCHAR(64) CHARACTER SET UNICODE,
      metal_category VARCHAR(64) CHARACTER SET UNICODE,
      is_dream_account VARCHAR(64) CHARACTER SET UNICODE,
      top_account VARCHAR(64) CHARACTER SET UNICODE,
      has_gcal VARCHAR(64) CHARACTER SET UNICODE,
      focus_merchant INTEGER,
      sold_more_than_3_units INTEGER,
      no_availability INTEGER,
      no_availability_more_than_4_weeks VARCHAR(64) CHARACTER SET UNICODE)
NO PRIMARY INDEX;


DROP TABLE nvp_4_week_no_avail;
create volatile multiset table nvp_4_week_no_avail as 
(select 
   a.*
from
(select 
     *
from sandbox.nvp_hbw_booking_status_deal2 
where load_week = trunc(current_date - 7, 'iw')+6
      and availability_cohort2 = 'Availability - None') as a 
join 
(select 
     *
from sandbox.nvp_hbw_booking_status_deal2 
where load_week = trunc(current_date - 14, 'iw')+6
      and availability_cohort2 = 'Availability - None'
) as b on a.deal_uuid = b.deal_uuid
join 
(select 
     *
from sandbox.nvp_hbw_booking_status_deal2 
where load_week = trunc(current_date - 21, 'iw')+6
      and availability_cohort2 = 'Availability - None'
) as c on a.deal_uuid = c.deal_uuid
join 
(select 
     *
from sandbox.nvp_hbw_booking_status_deal2 
where load_week = trunc(current_date - 28, 'iw')+6
      and availability_cohort2 = 'Availability - None'
) as d on a.deal_uuid = d.deal_uuid) with data on commit preserve rows;



delete from sandbox.nvp_agg_reporting where report_date = trunc(current_date - 7, 'iw')+6;

INSERT INTO sandbox.nvp_agg_reporting 
SELECT * FROM (
SELECT 
   cast(trunc(current_date - 7, 'iw')+6 as date) report_date,
   'deals live' report_type,
   a.deal_uuid, 
   a.opportunity_id, 
   a.account_name, 
   a.account_owner, 
   a.account_id, 
   cast(a.bt_launch_date as date) bt_launch_date, 
   cast(a.units_sold_t30 as integer) units_sold_t30,
   a.availability_cohort2,
   a.metal_category,
   cast(a.is_dream_account as varchar(64)) is_dream_account,
   cast(a.top_account as varchar(64)) top_account,
   cast(a.has_gcal as varchar(64)) has_gcal,
   case when (a.top_account = 1 or a.is_dream_account = 'Yes' or a.has_gcal = 1) then 1 else 0 end focus_merchant,
   case when a.units_sold_t30 > 3 then 1 else 0 end sold_more_than_3_units,
   case when a.availability_cohort2 = 'Availability - None' then 1 else 0 end no_availability,
   case when b.deal_uuid is not null then '1' else '0' end no_availability_more_than_4_weeks
from sandbox.nvp_hbw_booking_status_deal as a 
     left join nvp_4_week_no_avail as b on a.deal_uuid = b.deal_uuid
UNION
select
   cast(trunc(current_date - 7, 'iw')+6 as date) report_date,
   'deals lost' report_type, 
   deal_uuid, 
   opp_id,
   account_name, 
   account_owner,
   account_id, 
   cast(bt_launch_date as date) bt_launch_date,
   cast(units_30_optout as integer) as units_sold_t30,
   availability_cohort2, 
   metal_category, 
   cast(dream_account as varchar(64)) is_deam_account, 
   cast(top_account as varchar(64)) top_account, 
   cast(is_gcal as varchar(64)) is_gcal, 
   case when (top_account = 1 or dream_account = 'Yes' or is_gcal = 1) then 1 else 0 end focus_merchant,
   case when units_30_optout > 3 then 1 else 0 end sold_more_than_3_units,
   case when availability_cohort2 = 'Availability - None' then 1 else 0 end no_availability,
   'Not Applicable' as no_availability_more_than_4_weeks
from sandbox.sh_bt_pause_reasons_wbr
where opt_out_week = trunc(current_date - 7, 'iw')+6
) as fin ;


select * from sandbox.nvp_agg_reporting order by report_date desc, account_id;


select * from sandbox.sh_bt_pause_reasons_wbr;