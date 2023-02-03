--------------------------------------------------------Redemption demand
drop table sh_fgt_all;
create volatile table sh_fgt_all as (
    sel fgt.*
    from user_edwprod.fact_gbl_transactions fgt 
    where 
       order_date >= cast('2021-01-01' as date)
       and 
       order_date <= cast('2021-01-31' as date)
       and country_id = 235
) with data primary index (order_id, action) on commit preserve rows;


drop table nvp_redeem_date;
create multiset volatile table nvp_redeem_date as 
(sel 
    distinct 
    f.order_id,
    cmc.order_id ord2,
    f.deal_uuid, 
    gdl.grt_l3_cat_name l3,
    f.order_date,
    cmc.created_at,
    cmc.min_redeem_date, 
    cast(cmc.min_redeem_date as date) - cast(f.order_date as date) ord_red_diff, 
    cmc.min_book_at,
    cast(cmc.min_book_at as date) - cast(f.order_date as date) ord_book_diff,
    cmc.min_appointment_date, 
    cast(cmc.min_appointment_date as date) - cast(cmc.min_book_at as date) appointment_book_diff
from 
    (select order_id, deal_uuid, order_date from sh_fgt_all group by 1,2,3) f
    join 
    (select order_id,
            min(cast(cmc1.created_at as date)) created_at, 
            min(coalesce(merchant_redeemed_at, customer_redeemed_at)) min_redeem_date, 
            min(cast(substr(bn.created_at, 1,10) as date)) min_book_at,
            min(cast(substr(start_time, 1,10) as date)) min_appointment_date
            from user_gp.camp_membership_coupons cmc1
            left join sandbox.sh_bt_bookings_rebuild bn on cmc1.code = bn.voucher_code and cast(cmc1.merchant_redemption_code as varchar(50)) = bn.security_code
            where cmc1.created_at >= cast('2020-01-01' as date)
            group by 1
    ) cmc on f.order_id = cast(cmc.order_id as varchar(64))
    join user_edwprod.dim_gbl_deal_lob gdl on f.deal_uuid = gdl.deal_id
    where 
    gdl.country_code = 'US'
    and gdl.grt_l1_cat_name = 'L1 - Local'
    and gdl.grt_l2_cat_description = 'HBW'
) with data on commit preserve rows
    ;


create volatile table nvp_bookable as (
select 
   distinct deal_uuid 
   from sandbox.sh_bt_active_deals_log_v4 
   where 
       cast(load_date as date) >= cast('2021-01-01' as date)
       and 
       cast(load_date as date) <= cast('2021-01-31' as date)
       and partner_inactive_flag = 0 and product_is_active_flag = 1
)with data primary index (deal_uuid) on commit preserve rows;
   
select
   case when b.deal_uuid is not null then 1 else 0 end bookable, 
   case when ord_red_diff is null then 'g. unredeemed'
   when ord_red_diff = 0 then 'a.same day red'
   when ord_red_diff = 1 then 'b.next day red'
   when ord_red_diff <= 3 then 'c.<= 3 days red'
   when ord_red_diff <= 7 then 'd.<= 7 days red'
   when ord_red_diff <= 14 then 'e.<= 14 days red'
   else 'f.15+ days' end red_category, 
   count(distinct order_id) orders_tot
from
   nvp_redeem_date as a 
   left join nvp_bookable as b on a.deal_uuid = b.deal_uuid
  group by 1, 2
  order by 1, 2;
 
 
select
   l3, 
   case 
   when appointment_book_diff = 0 then 'a.same day appointment'
   when appointment_book_diff = 1 then 'b.next day appointment'
   when appointment_book_diff <= 3 then 'c.<= 3 days appointment'
   when appointment_book_diff <= 7 then 'd.<= 7 days appointment'
   when appointment_book_diff <= 14 then 'e.<= 14 days appointment'
   when appointment_book_diff <= 30 then 'f.<= 30 days appointment'
   else 'g.30+ appointment' end red_category, 
   count(distinct order_id) orders_tot
from
   nvp_redeem_date as a 
   where appointment_book_diff is not null
  group by 1,2
  order by 1,2;
 
select
   gdl.grt_l3_cat_name, 
   avg(avg_booking_delay) avg_booking_delay
from sandbox.sh_dream_booking_delay_deal as f
join user_edwprod.dim_gbl_deal_lob gdl on f.deal_uuid = gdl.deal_id
    where 
    gdl.country_code = 'US'
    and gdl.grt_l1_cat_name = 'L1 - Local'
    and gdl.grt_l2_cat_description = 'HBW'
    group by 1;


select 
    l3, 
    case when td_day_of_week(cast(min_redeem_date as date)) = 1 then 'Sun'
         when td_day_of_week(cast(min_redeem_date as date)) = 2 then 'Mon'
         when td_day_of_week(cast(min_redeem_date as date)) = 3 then 'Tue'
         when td_day_of_week(cast(min_redeem_date as date)) = 4 then 'Wed'
         when td_day_of_week(cast(min_redeem_date as date)) = 5 then 'Thu'
         when td_day_of_week(cast(min_redeem_date as date)) = 6 then 'Fri'
         when td_day_of_week(cast(min_redeem_date as date)) = 7 then 'Sat'
         end day_of_the_week,
     extract(hour from cast(min_redeem_date as timestamp format 'yyyy-mm-dd.hh.mi.ss')) hour_of_day, 
        count(distinct order_id) orders_tot
from
   nvp_redeem_date
group by 1,2,3;


-------------------------------------------------------Appointment demand

drop table sandbox.sh_booked_appt;
create multiset table sandbox.sh_booked_appt as (
    sel f.order_id,
       f.deal_uuid, 
       cmc.merchant_redemption_code, 
       cmc.code voucher_code,
       cmc.created_at voucher_created_at,
       bn.booking_id,
       f.order_date,
       bn.start_time,
       bn.end_time,
       bn.created_at booking_created_at,
       bn.state, 
       bn.voucher_code bk_voucher_code
    from (select order_id, deal_uuid, order_date from sh_fgt_ord group by 1,2, 3) f
    left join user_gp.camp_membership_coupons cmc on f.order_id = cast(cmc.order_id as varchar(64))
    left join sandbox.sh_bt_bookings_rebuild bn on cmc.code = bn.voucher_code and cast(cmc.merchant_redemption_code as varchar(50)) = bn.security_code
) with data;

drop table nvp_appts_avail;
create volatile multiset table nvp_appts_avail as 
(select 
    a.deal_uuid, 
    a.units_sold_t30,
    c.l3,
    a.action_cohort, 
    b.order_id, 
    b.order_date, 
    b.booking_created_at,
    b.booking_id,
    b.start_time, 
    b.end_time, 
    b.state, 
    b.bk_voucher_code
from 
sandbox.nvp_hbw_booking_status_deal as a 
left join 
sandbox.sh_booked_appt as b on a.deal_uuid = b.deal_uuid
left join 
   (select deal_id, max(grt_l3_cat_description) l3 from user_edwprod.dim_gbl_deal_lob group by 1)
    as c on a.deal_uuid = c.deal_id
) with data on commit preserve rows;

select * from nvp_appts_avail;

drop table sandbox.nvp_booking_days_time;
create multiset table sandbox.nvp_booking_days_time as (
select 
    l3,
    case when td_day_of_week(cast(substr(start_time, 1,10) as date)) = 1 then 'Sun'
         when td_day_of_week(cast(substr(start_time, 1,10) as date)) = 2 then 'Mon'
         when td_day_of_week(cast(substr(start_time, 1,10) as date)) = 3 then 'Tue'
         when td_day_of_week(cast(substr(start_time, 1,10) as date)) = 4 then 'Wed'
         when td_day_of_week(cast(substr(start_time, 1,10) as date)) = 5 then 'Thu'
         when td_day_of_week(cast(substr(start_time, 1,10) as date)) = 6 then 'Fri'
         when td_day_of_week(cast(substr(start_time, 1,10) as date)) = 7 then 'Sat'
         end day_of_the_week,
     extract(hour from cast(start_time as timestamp format 'yyyy-mm-dd.hh.mi.ss')) hour_of_day,
    count(distinct booking_id) bookings, 
    action_cohort
from 
    nvp_appts_avail
where units_sold_t30 >2
     group by 1,2,3,5
) with data;



------------
drop table nvp_availability_days_time;
create multiset volatile table nvp_availability_days_time as
(select 
    deal_uuid, 
    deal_option_uuid,
    min_start_time,
    max_end_time,
    country,
    reference_date, 
    cast(substr(min_start_time, 1,2) as int) min_start_time_hr, 
    cast(substr(min_start_time, 3,4) as int) min_start_time_min,
    cast(substr(max_end_time, 1,2) as int) max_start_time_hr, 
    cast(substr(max_end_time, 3,4) as int) max_start_time_min, 
    (max_start_time_hr * 60)+ max_start_time_min - (min_start_time_hr * 60) - min_start_time_min time_available,
    gss_total_availability, 
    gss_available_minutes, 
    gss_total_minutes,
    trunc(cast(reference_date as date), 'iw')+6 load_week
from 
    sandbox.nvp_availability_slots) with data on commit preserve rows;
;


drop table nvp_availability_days_time2;

create multiset volatile table nvp_availability_days_time2 as
(select 
    * 
    from 
(select 
    a.*, 
    row_number() over (partition by deal_uuid order by load_week desc) arrange, 
    rank() over (partition by deal_uuid order by load_week desc) arrange2
    from 
(select 
     deal_uuid, 
     load_week, 
     reference_date, 
     min(min_start_time_hr) min_start_time_hr, 
     min(min_start_time_min) min_start_time_min, 
     max(max_start_time_hr) max_start_time_hr, 
     max(max_start_time_min) max_start_time_min,
     max(time_available) time_available, 
     max(gss_total_availability) total_availability, 
     max(gss_available_minutes) available_minutes, 
     max(gss_total_minutes) total_minutes
 from 
     nvp_availability_days_time
     where load_week <= trunc(current_date, 'iw') + 6
     group by 1,2,3) as a) as fin
     where arrange2 = 1
     ) with data on commit preserve rows
     ;

drop table nvp_supply_avail;
create multiset volatile table nvp_supply_avail as
(select 
    a.deal_uuid, 
    a.units_sold_t30,
    c.l3,
    a.action_cohort, 
    b.reference_date,
    b.min_start_time_hr,
    b.min_start_time_min,
    b.max_start_time_hr, 
    b.max_start_time_min, 
    b.time_available, 
    b.total_availability, 
    b.available_minutes, 
    b.total_minutes, 
    case when td_day_of_week(cast(substr(reference_date, 1,10) as date)) = 1 then 'Sun'
         when td_day_of_week(cast(substr(reference_date, 1,10) as date)) = 2 then 'Mon'
         when td_day_of_week(cast(substr(reference_date, 1,10) as date)) = 3 then 'Tue'
         when td_day_of_week(cast(substr(reference_date, 1,10) as date)) = 4 then 'Wed'
         when td_day_of_week(cast(substr(reference_date, 1,10) as date)) = 5 then 'Thu'
         when td_day_of_week(cast(substr(reference_date, 1,10) as date)) = 6 then 'Fri'
         when td_day_of_week(cast(substr(reference_date, 1,10) as date)) = 7 then 'Sat'
         end day_of_the_week,
    cast(b.available_minutes as float)/nullifzero(b.time_available) ratio,
    case when min_start_time_hr = 1 then (60 - min_start_time_min)*ratio 
         when 1 > min_start_time_hr and 1 < max_start_time_hr then 60*ratio
         when max_start_time_hr = 1 then max_start_time_min*ratio
         end as "1", 
    case when min_start_time_hr = 2 then (60 - min_start_time_min)*ratio 
         when 2 > min_start_time_hr and 2 < max_start_time_hr then 60*ratio
         when max_start_time_hr = 2 then max_start_time_min*ratio
         end "2", 
    case when min_start_time_hr = 3 then (60 - min_start_time_min)*ratio 
         when 3 > min_start_time_hr and 3 < max_start_time_hr then 60*ratio
         when max_start_time_hr = 3 then max_start_time_min*ratio
         end "3", 
    case when min_start_time_hr = 4 then (60 - min_start_time_min)*ratio 
         when 4 > min_start_time_hr and 4 < max_start_time_hr then 60*ratio
         when max_start_time_hr = 4 then max_start_time_min*ratio
         end "4", 
    case when min_start_time_hr = 5 then (60 - min_start_time_min)*ratio 
         when 5 > min_start_time_hr and 5 < max_start_time_hr then 60*ratio
         when max_start_time_hr = 5 then max_start_time_min*ratio
         end "5",
    case when min_start_time_hr = 6 then (60 - min_start_time_min)*ratio 
         when 6 > min_start_time_hr and 6 < max_start_time_hr then 60*ratio
         when max_start_time_hr = 6 then max_start_time_min*ratio
         end "6", 
    case when min_start_time_hr = 7 then (60 - min_start_time_min)*ratio 
         when 7 > min_start_time_hr and 7 < max_start_time_hr then 60*ratio
         when max_start_time_hr = 7 then max_start_time_min*ratio
         end "7", 
    case when min_start_time_hr = 8 then (60 - min_start_time_min)*ratio 
         when 8 > min_start_time_hr and 8 < max_start_time_hr then 60*ratio
         when max_start_time_hr = 8 then max_start_time_min*ratio
         end "8", 
    case when min_start_time_hr = 9 then (60 - min_start_time_min)*ratio 
         when 9 > min_start_time_hr and 9 < max_start_time_hr then 60*ratio
         when max_start_time_hr = 9 then max_start_time_min*ratio
         end "9", 
    case when min_start_time_hr = 10 then (60 - min_start_time_min)*ratio 
         when 10 > min_start_time_hr and 10 < max_start_time_hr then 60*ratio
         when max_start_time_hr = 10 then max_start_time_min*ratio
         end "10", 
    case when min_start_time_hr = 11 then (60 - min_start_time_min)*ratio 
         when 11 > min_start_time_hr and 11 < max_start_time_hr then 60*ratio
         when max_start_time_hr = 11 then max_start_time_min*ratio
         end "11", 
    case when min_start_time_hr = 12 then (60 - min_start_time_min)*ratio 
         when 12 > min_start_time_hr and 12 < max_start_time_hr then 60*ratio
         when max_start_time_hr = 12 then max_start_time_min*ratio
         end "12", 
    case when min_start_time_hr = 13 then (60 - min_start_time_min)*ratio 
         when 13 > min_start_time_hr and 13 < max_start_time_hr then 60*ratio
         when max_start_time_hr = 13 then max_start_time_min*ratio
         end "13", 
    case when min_start_time_hr = 14 then (60 - min_start_time_min)*ratio 
         when 14 > min_start_time_hr and 14 < max_start_time_hr then 60*ratio
         when max_start_time_hr = 14 then max_start_time_min*ratio
         end "14", 
    case when min_start_time_hr = 15 then (60 - min_start_time_min)*ratio 
         when 15 > min_start_time_hr and 15 < max_start_time_hr then 60*ratio
         when max_start_time_hr = 15 then max_start_time_min*ratio
         end "15", 
    case when min_start_time_hr = 16 then (60 - min_start_time_min)*ratio 
         when 16 > min_start_time_hr and 16 < max_start_time_hr then 60*ratio
         when max_start_time_hr = 16 then max_start_time_min*ratio
         end "16", 
    case when min_start_time_hr = 17 then (60 - min_start_time_min)*ratio 
         when 17 > min_start_time_hr and 17< max_start_time_hr then 60*ratio
         when max_start_time_hr = 17 then max_start_time_min*ratio
         end "17", 
    case when min_start_time_hr = 18 then (60 - min_start_time_min)*ratio 
         when 18 > min_start_time_hr and 18 < max_start_time_hr then 60*ratio
         when max_start_time_hr = 18 then max_start_time_min*ratio
         end "18", 
    case when min_start_time_hr = 19 then (60 - min_start_time_min)*ratio 
         when 19 > min_start_time_hr and 19 < max_start_time_hr then 60*ratio
         when max_start_time_hr = 19 then max_start_time_min*ratio
         end "19", 
    case when min_start_time_hr = 20 then (60 - min_start_time_min)*ratio 
         when 20 > min_start_time_hr and 20 < max_start_time_hr then 60*ratio
         when max_start_time_hr = 20 then max_start_time_min*ratio
         end "20", 
    case when min_start_time_hr = 21 then (60 - min_start_time_min)*ratio 
         when 21 > min_start_time_hr and 21 < max_start_time_hr then 60*ratio
         when max_start_time_hr = 21 then max_start_time_min*ratio
         end "21",
    case when min_start_time_hr = 22 then (60 - min_start_time_min)*ratio 
         when 22 > min_start_time_hr and 22 < max_start_time_hr then 60*ratio
         when max_start_time_hr = 22 then max_start_time_min*ratio
         end "22", 
    case when min_start_time_hr = 23 then (60 - min_start_time_min)*ratio 
         when 23 > min_start_time_hr and 23 < max_start_time_hr then 60*ratio
         when max_start_time_hr = 23 then max_start_time_min*ratio
         end "23", 
    case when min_start_time_hr = 24 then (60 - min_start_time_min)*ratio 
         when 24 > min_start_time_hr and 24 < max_start_time_hr then 60*ratio
         when max_start_time_hr = 24 then max_start_time_min*ratio
         end "24"
from
sandbox.nvp_hbw_booking_status_deal as a
left join nvp_availability_days_time2 as b on a.deal_uuid = b.deal_uuid
left join 
   (select deal_id, max(grt_l3_cat_description) l3 from user_edwprod.dim_gbl_deal_lob group by 1)
    as c on a.deal_uuid = c.deal_id
   ) with data on commit preserve rows;
  
  

    
  
select 
   action_cohort, 
   l3, 
   day_of_the_week, 
   sum("1") "1", 
   sum("2") "2", 
   sum("3") "3", 
   sum("4") "4", 
   sum("5") "5", 
   sum("6") "6", 
   sum("7") "7", 
   sum("8") "8", 
   sum("9") "9", 
   sum("10") "10",
   sum("11") "11",
   sum("12") "12",
   sum("13") "13",
   sum("14") "14",
   sum("15") "15", 
   sum("16") "16", 
   sum("17") "17", 
   sum("18") "18", 
   sum("19") "19", 
   sum("20") "20", 
   sum("21") "21", 
   sum("22") "22", 
   sum("23") "23", 
   sum("24") "24"
from nvp_supply_avail
where units_sold_t30 > 2
group by 1,2,3;

--------------
select * from sandbox.nvp_availability_slots;

select cast(max_end_time as timestamp format 'hh.mi.ss') from sandbox.nvp_availability_slots;

------------------------------------CREATING AVAILABILITY REQUIREMENT TABLE
create table grp_gdoop_bizops_db.nvp_temp_availability_slot stored as orc as
select * from  grp_gdoop_bizops_db.jk_bt_availability_gbl_2
where report_date = cast('2021-02-15' as date)
   and days_delta < 7;



CREATE MULTISET TABLE sandbox.nvp_availability_slots ,NO FALLBACK ,
      NO BEFORE JOURNAL,
      NO AFTER JOURNAL,
      CHECKSUM = DEFAULT,
      DEFAULT MERGEBLOCKRATIO,
      MAP = TD_MAP1
      (
        deal_uuid VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
        deal_option_uuid VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
        country	VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
        reference_date VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
        min_start_time VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC, 
        max_end_time VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC, 
        gss_total_availability integer, 
        gss_available_minutes integer,
        gss_total_minutes integer
      )
NO PRIMARY INDEX ;



drop table if exists grp_gdoop_bizops_db.nvp_availability_slots;
create table grp_gdoop_bizops_db.nvp_availability_slots stored as orc as
select
      avail.deal_uuid,
      avail.deal_option_uuid, 
      avail.country,
      avail.reference_date,
      min(avail.gss_min_start_time) min_start_time,
      max(avail.gss_max_end_time) max_end_time, 
      max(avail.gss_total_availability) gss_total_availability,
      max(avail.gss_available_minutes) gss_available_minutes, 
      max(avail.gss_total_minutes) gss_total_minutes
from
  (select *
       from grp_gdoop_bizops_db.jk_bt_availability_gbl_2
       where
       report_date >= cast('2020-09-01' as date)
       and days_delta <= 8
   ) avail
   WHERE country= 'US'
group by 
   avail.deal_uuid,
   avail.deal_option_uuid, 
   avail.country,
   avail.reference_date
;

    
-------




select order_id, deal_uuid, count(distinct merchant_redemption_code) cnz
from sandbox.sh_booked_appt
group by 1,2
having cnz >1
order by 2 desc
;
select * from user_gp.camp_membership_coupons where order_id = 1428282113;
select * from sandbox.sh_booked_appt where order_id = '1434436115';

select 
    f.order_id, 
    f.deal_uuid, 
    f.units,
    cmc.*
from (select order_id, deal_uuid, sum(transaction_qty) units from sh_fgt_ord group by 1,2) f
    left join user_gp.camp_membership_coupons cmc on f.order_id = cast(cmc.order_id as varchar(64))
   order by deal_uuid, f.order_id, code;

    
