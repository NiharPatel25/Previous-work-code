----------CREATING MY VERSION OF availability BASED ON REFEREENCE DATE and going back to 14 days only.


-----hive ------EARLIEST AVAILABILITY

drop table if exists grp_gdoop_bizops_db.nvp_temp_future;
create table grp_gdoop_bizops_db.nvp_temp_future stored as orc as
select
    deal_uuid,
    country,
    min(case when day_available is not null then reference_date end) earliest_availability,
    max(report_date) latest_refresh_date,
    count(distinct case when day_available is not null then reference_date end) total_available_days
from
(select
      avail.deal_uuid,
      avail.country,
      avail.report_date,
      avail.reference_date,
      case when gss_available_minutes > 0 then date_format(reference_date,'E') end day_available
from
   (select *
       from grp_gdoop_bizops_db.jk_bt_availability_gbl_2
       where
       cast(report_date as date) >= date_sub(next_day(current_date, 'MON'), 10) and cast(reference_date as date) >= cast(date_sub(next_day(current_date, 'MON'), 7) as date)
   ) avail
 ) fin
group by
   deal_uuid,
   country;


----teradata ---- EARLIEST AVAILABILITY

drop table sandbox.nvp_future_avail;
CREATE MULTISET TABLE sandbox.nvp_future_avail ,NO FALLBACK ,
      NO BEFORE JOURNAL,
      NO AFTER JOURNAL,
      CHECKSUM = DEFAULT,
      DEFAULT MERGEBLOCKRATIO,
      MAP = TD_MAP1
      (
        deal_uuid VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
        country	VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
        earliest_availability	VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC, 
        latest_refresh_date VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
        total_available_days integer
      )
 NO PRIMARY INDEX ;

select * from sandbox.nvp_future_avail;

--------------------------------------------------------------Week start
drop table grp_gdoop_bizops_db.nvp_availability_deals;
create table grp_gdoop_bizops_db.nvp_availability_deals stored as orc as
select 
   distinct 
   reference_date,
   deal_uuid,
   case when days_delta <= 7 and gss_available_minutes > 0 then deal_uuid end deals_both, 
   case when days_delta <= 7 then deal_uuid end deals_days_delta
   from 
grp_gdoop_bizops_db.jk_bt_availability_gbl
where 
    report_date >= cast('2021-04-01' as date);

  
select * from user_edwprod.dim_gbl_deal_lob;
   
   
drop table grp_gdoop_bizops_db.nvp_temp_availablity_ref2;
create table grp_gdoop_bizops_db.nvp_temp_availablity_ref2 stored as orc as
select
     deal_uuid, 
     country, 
     report_week,
     load_week,
     count(distinct day_available) num_dow, 
     count(distinct day_available_total) num_dow_total, 
     count(distinct completely_blocked_days) completely_blocked_days,
     count(distinct completely_paused_days) completely_paused_days
from 
(select 
       avail.deal_uuid, 
       country,
       date_sub(next_day(report_date, 'MON'), 1) report_week,
       report_date,
       date_sub(next_day(reference_date, 'MON'), 1) load_week,
       reference_date,
       case when gss_available_minutes > 0 then date_format(reference_date,'E') end day_available, 
       case when gss_total_minutes > 0  then date_format(reference_date,'E') end day_available_total, 
       case when gss_total_minutes > 0 and gss_blocked_min >= gss_total_minutes then date_format(reference_date,'E') end completely_blocked_days,
       case when gss_total_minutes > 0 and gss_number_of_slots - gss_number_of_paused_slots = 0 then date_format(reference_date,'E') end completely_paused_days
   from (select *
          from grp_gdoop_bizops_db.jk_bt_availability_gbl
          where 
            report_date >= cast('2020-09-01' as date)
          and days_delta > 0 
          and days_delta <= 7
     ) avail
     WHERE 
     date_sub(next_day(reference_date, 'MON'), 1) <> date_sub(next_day(CURRENT_DATE, 'MON'), 1)
     ) fin
where fin.report_date = date_sub(fin.load_week, 7)
group by deal_uuid, country, report_week, load_week
UNION 
select
     deal_uuid, 
     country, 
     report_week,
     load_week,
     count(distinct day_available) num_dow, 
     count(distinct day_available_total) num_dow_total, 
     count(distinct completely_blocked_days) completely_blocked_days,
     count(distinct completely_paused_days) completely_paused_days
from 
(select 
       avail.deal_uuid, 
       country,
       date_sub(next_day(report_date, 'MON'), 1) report_week,
       report_date,
       date_sub(next_day(reference_date, 'MON'), 1) load_week,
       reference_date,
       case when gss_available_minutes > 0 then date_format(reference_date,'E') end day_available, 
       case when gss_total_minutes > 0 then date_format(reference_date,'E') end day_available_total,
       case when gss_total_minutes > 0 and gss_blocked_min >= gss_total_minutes then date_format(reference_date,'E') end completely_blocked_days,
       case when gss_total_minutes > 0 and gss_number_of_slots - gss_number_of_paused_slots = 0 then date_format(reference_date,'E') end completely_paused_days
   from (select *
          from grp_gdoop_bizops_db.jk_bt_availability_gbl
          where 
            cast(report_date as date) >= date_sub(next_day(current_date, 'MON'), 10)
          and days_delta <= 10
     ) avail
     WHERE  
     date_sub(next_day(reference_date, 'MON'), 1) = date_sub(next_day(CURRENT_DATE, 'MON'), 1)
     ) fin
where fin.report_week = date_sub(fin.load_week, 7)
group by deal_uuid, country, report_week, load_week
;


 
          
drop table sandbox.nvp_weekstart_avail;
CREATE MULTISET TABLE sandbox.nvp_weekstart_avail ,NO FALLBACK ,
      NO BEFORE JOURNAL,
      NO AFTER JOURNAL,
      CHECKSUM = DEFAULT,
      DEFAULT MERGEBLOCKRATIO,
      MAP = TD_MAP1
      (
        deal_uuid VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
        country	VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
        report_weekdate	VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
        load_week	VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
        num_dow integer, 
        num_dow_total integer, 
        completely_blocked_days integer, 
        completely_paused_days integer
      )
 NO PRIMARY INDEX ;

--------------------------------------------------------------NAM Weekwise avail
-----hive

drop table grp_gdoop_bizops_db.nvp_temp_availablity_ref;
create table grp_gdoop_bizops_db.nvp_temp_availablity_ref stored as orc as
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
      case when gss_total_minutes > 0 and gss_number_of_slots - gss_number_of_paused_slots <> 0 then date_format(reference_date,'E') end day_available_total
from
  (select *
       from grp_gdoop_bizops_db.jk_bt_availability_gbl_2
       where
       report_date >= cast('2020-09-01' as date)
       and days_delta <= 10
   ) avail
   WHERE country= 'US' AND CAST(reference_date AS date) <= date_sub(next_day(CURRENT_DATE, 'MON'), 1)
 ) fin
 group by deal_uuid, country, date_sub(next_day(reference_date, 'MON'), 1)
 ;



----teradata

drop table sandbox.nvp_weekwise_avail;

CREATE MULTISET TABLE sandbox.nvp_weekwise_avail ,NO FALLBACK ,
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


--------------------------------------------------------------INTERNATIONAL
-----hive

drop table grp_gdoop_bizops_db.nvp_temp_availablity_ref_intl;
create table grp_gdoop_bizops_db.nvp_temp_availablity_ref_intl stored as orc as
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
      case when gss_total_minutes > 0 and gss_number_of_slots - gss_number_of_paused_slots <> 0 then date_format(reference_date,'E') end day_available_total
from
  (select *
       from grp_gdoop_bizops_db.jk_bt_availability_gbl_2
       where
       report_date >= cast('2020-09-01' as date)
       and days_delta <= 10
   ) avail
   WHERE country <> 'US' AND CAST(reference_date AS date) <= date_sub(next_day(CURRENT_DATE, 'MON'), 1)
 ) fin
 group by deal_uuid, country, date_sub(next_day(reference_date, 'MON'), 1)
 ;





----teradata intl

drop table sandbox.nvp_weekwise_avail_intl;

CREATE MULTISET TABLE sandbox.nvp_weekwise_avail_intl ,NO FALLBACK ,
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




------------------------------------------------






select max(load_week) from sandbox.nvp_weekstart_avail;

drop table grp_gdoop_bizops_db.nvp_temp_availablity_ref3;
create table grp_gdoop_bizops_db.nvp_temp_availablity_ref3 stored as orc as
select 
       avail.deal_uuid, 
       country,
       date_sub(next_day(report_date, 'MON'), 1) report_week,
       report_date,
       date_sub(next_day(reference_date, 'MON'), 1) load_week,
       reference_date,
       case when gss_available_minutes > 0 then date_format(reference_date,'E') end day_available, 
       case when gss_total_minutes > 0 then date_format(reference_date,'E') end day_available_total, 
       row_number() over (partition by deal_uuid, country, date_sub(next_day(reference_date, 'MON'), 1) order by report_date desc) latest
   from (select *
          from grp_gdoop_bizops_db.jk_bt_availability_gbl
          where 
            report_date >= cast('2020-09-01' as date)
          and days_delta > 0 
          and days_delta <= 10
     ) avail
     WHERE country = 'US' ; 



-----Previous query active deal options

drop table if exists grp_gdoop_bizops_db.nvp_bt_active_deals_log;create table grp_gdoop_bizops_db.nvp_bt_active_deals_log stored as orc as
select
    d.groupon_real_deal_uuid as deal_uuid,
    d.groupon_deal_uuid,
    max(d.gapp_enabled) gapp_enabled,
    max(pr.is_bookable) is_bookable,
    max(pa.inactive) partner_inactive_flag,
    max(pr.is_active) product_is_active_flag,
    max(pa.new_bt_opt_in) new_bt_opt_in_flag,
    max(pa.new_bt_opt_in_date) new_bt_opt_in_date,
    max(d.multicoupon_validity_enabled) is_multisession_flag,
    max(pa.multi_agenda_enabled) is_multiagenda_flag,
    max(d.country) country,
    max(ad.sold_out) sold_out,
    ad.load_date
from grp_gdoop_bizops_db.sh_bt_deals d
join grp_gdoop_bizops_db.sh_bt_partners pa on d.partners_id = pa.id and d.country = pa.country
join grp_gdoop_bizops_db.sh_bt_products pr on d.products_id = pr.id and d.country = pr.country
join user_groupondw.active_deals ad on d.groupon_real_deal_uuid = ad.deal_uuid
where ad.load_date = date_sub(current_date,1)
group by
    d.groupon_real_deal_uuid,
    d.groupon_deal_uuid,
    ad.load_date;



drop table if exists grp_gdoop_bizops_db.nvp_to_temp_availablity;create table grp_gdoop_bizops_db.nvp_to_temp_availablity stored as orc as
select * from
(select 
  fin_2.*, 
  row_number() over(partition by deal_uuid,deal_option_uuid order by load_week desc) update_order
  from 
(select
    deal_uuid,
    deal_option_uuid,
    country,
    date_sub(next_day(reference_date, 'MON'), 1) load_week,
    count(distinct day_available) num_dow
from
(select
      avail.deal_uuid,
      avail.deal_option_uuid,
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
   WHERE country= 'US' AND CAST(reference_date AS date) <= date_sub(next_day(CURRENT_DATE, 'MON'), 1)
 ) fin
 group by deal_uuid, country, date_sub(next_day(reference_date, 'MON'), 1), deal_option_uuid) fin_2
) fin_3 
where update_order = 1




---------
---------Jake's query adds this in sandbox.jk_bt_deal_max_avail_v3

select avail.deal_uuid, country ,report_date
, max(case when days_delta = 0 then gss_total_availability end)  max_avail
, count(distinct case when gss_total_availability > 0 then date_format(reference_date,'EEEE' ) end ) num_dow
from (select *
		from grp_gdoop_bizops_db.jk_bt_availability_gbl
		 where report_date >= cast('2020-09-01' as date)
		 and days_delta < 7
		) avail
WHERE country= 'US'
group by avail.deal_uuid, country ,report_date




----------------------------------
-----This table is created by Sam's query
sandbox.sh_bt_active_deals_log_v4