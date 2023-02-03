
------------------------------------------------------------30 DAY ANALYTICS



-------------------Days given for availability filled bucket

select action_cohort2, num_dow_total, count(distinct deal_uuid)
from sandbox.nvp_hbw_booking_status_deal
group by 1,2
order by 1.2;

select num_dow_total, count(distinct deal_uuid)
from sandbox.nvp_hbw_booking_status_deal where action_cohort2 = 'Availability - filled' and is_dream_account = 'Yes' and units_sold_t30 >3
group by 1
order by 1;


--------------------Capacity buckets

---might need modification
select 
    case when hours >= 0 and hours <= 25 then '<= 25 hrs/week'
    when hours > 25 and hours <= 50 then '<= 50 hrs/week'
    when hours > 50 and hours <= 75 then '<= 75 hrs/week'
    when hours > 75 and hours <= 100 then '<= 100 hrs/week'
    when hours <= 125 then '<= 125 hrs/week'
    end hours_cohort, 
    num_dow_total,
    count(distinct deal_uuid) total_deals
from 
(select 
   a.deal_uuid,
   num_dow_total, 
   cast(gss_total_minutes as float)/60 hours
from 
sandbox.nvp_hbw_booking_status_deal as a 
left join 
  (select * from sandbox.nvp_capacity_avail 
   qualify row_number() over (partition by deal_uuid order by load_week desc) = 1) as b on a.deal_uuid = b.deal_uuid
where 
a.action_cohort2 = 'Availability - filled'
and units_sold_t30 > 3) as fin
group by 1,2
order by 1,2
;



drop table grp_gdoop_bizops_db.nvp_temp_capacity_avail;
create table grp_gdoop_bizops_db.nvp_temp_capacity_avail stored as orc as
select
    deal_uuid,
    country,
    date_sub(next_day(reference_date, 'MON'), 1) load_week,
    sum(gss_total_minutes) gss_total_minutes
from
(select
      avail.deal_uuid,
      avail.country,
      avail.reference_date,
      max(gss_total_minutes) gss_total_minutes
from
  (select *
       from grp_gdoop_bizops_db.jk_bt_availability_gbl_2
       where
       report_date >= cast('2020-09-01' as date)
       and days_delta <= 10
   ) avail
   WHERE country= 'US' AND CAST(reference_date AS date) <= date_sub(next_day(CURRENT_DATE, 'MON'), 1)
   and CAST(reference_date AS date) >= date_sub(next_day(CURRENT_DATE, 'MON'), 30)
   group by 
      avail.deal_uuid,
      avail.country ,
      avail.reference_date
) fin
 group by deal_uuid, country, date_sub(next_day(reference_date, 'MON'), 1)
 ;



CREATE MULTISET TABLE sandbox.nvp_capacity_avail ,NO FALLBACK ,
      NO BEFORE JOURNAL,
      NO AFTER JOURNAL,
      CHECKSUM = DEFAULT,
      DEFAULT MERGEBLOCKRATIO,
      MAP = TD_MAP1
      (
        deal_uuid VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
        country	VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
        load_week	VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
        gss_total_minutes integer
      )
 NO PRIMARY INDEX ;
-------------------% Units booked analysis


drop table sandbox.nvp_hbw_booking_temp;
create table sandbox.nvp_hbw_booking_temp as (
 select t_.*,
          case when (has_availability_setup = 0 or latest_availability_not_data  = 1) then 'a. availability setup/data issue'
               when num_dow < 1 then 'b. 0 days available next week'
               when avg_booking_delay > 24 then 'c. booking leadtime >24 hours'
               when num_dow < 4 then 'd. less than 4 days available next week'
               when pct_units_booked_pre < .1 then '10% units pre purchase booked'
               when pct_units_booked_pre < .2 then '20% units pre purchase booked'
               when pct_units_booked_pre < .3 then '30% units pre purchase booked'
               when pct_units_booked_pre < .45 then '45% units pre purchase booked'
               when pct_units_booked_pre < .5 then '50% units pre purchase booked'
               when pct_units_booked_pre < .6 then '60% units pre purchase booked'
               when pct_units_booked_pre < .7 then '70% units pre purchase booked'
               when pct_units_booked_pre < 1 then '100 % units pre purchase booked'
               when booked_refund_rate > .25 then 'g. high refunds on booked vouchers'
               else 'h. no action needed'
           end action_cohort,
          case when (has_availability_setup = 0 or latest_availability_not_data  = 1) then 'Availability - None'
               when num_dow < 1 then 'Availability - None'
               when avg_booking_delay > 24 then 'Long Lead time'
               when num_dow < 4 then 'Availability - Low'
               when pct_units_booked_pre < .6 then 'Low Bookings'
               when booked_refund_rate > .25 then 'High Refunds'
               else 'No Action'
           end action_cohort2
      from (
          sel d.*,
              u.top_pds,
              u.units_sold as units_sold_t30,
              u.units_sold_booked_bt as units_sold_bt_t30,
              u.units_prepurchase_booked as units_prepurchase_booked_t30,
              cast(u.units_prepurchase_booked as dec(18,3)) / cast(nullifzero(u.units_sold) as dec(18,3)) pct_units_booked_pre,
              cast(bdd.avg_booking_delay as dec(18,3)) avg_booking_delay,
              r.booked_refund_rate
          from sh_bt_deals d
          left join sandbox.sh_dream_booking_delay_deal bdd on d.deal_uuid = bdd.deal_uuid
          left join sh_units u on d.deal_uuid = u.deal_uuid
          left join sh_booked_refunds r on d.deal_uuid = r.deal_uuid
      ) as t_
  ) with data unique primary index (deal_uuid);
  
 
 
select 
   action_cohort, 
   count(distinct deal_uuid) deal_count
from sandbox.nvp_hbw_booking_temp
where units_sold_t30 > 3
group by 1 order by 1;
;


select * from sandbox.nvp_hbw_booking_temp where units_sold_t30 >3;
select sum(units_sold_t30), sum(units_sold_bt_t30), sum(units_prepurchase_booked_t30) from sandbox.nvp_hbw_booking_temp where units_sold_t30>2;



