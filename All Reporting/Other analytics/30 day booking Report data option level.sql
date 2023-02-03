--------Hive
drop table if exists grp_gdoop_bizops_db.nvp_to_temp_availablity;


drop table if exists grp_gdoop_bizops_db.nvp_to_temp_availablity;
create table grp_gdoop_bizops_db.nvp_to_temp_availablity stored as orc as
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
;



/*
create table grp_gdoop_bizops_db.nvp_to_temp_availablity stored as orc as
select * from
(select
    fin_0.*,
    row_number() over(partition by deal_uuid,deal_option_uuid order by report_date desc) update_order
    from
 (select
       avail.deal_uuid,
       avail.deal_option_uuid,
       avail.country ,
       avail.report_date,
       max(case when days_delta = 0 then gss_total_availability end)  max_avail,
       count(distinct case when gss_total_availability > 0 then date_format(reference_date,'E' ) end ) num_dow
 from
   (select *
        from grp_gdoop_bizops_db.jk_bt_availability_gbl
        where
        report_date >= cast('2020-09-01' as date)
        and days_delta < 7
    ) avail
 join
      (select
            groupon_real_deal_uuid as deal_uuid,
            groupon_deal_uuid as deal_option_uuid,
            (case when min(participants_per_coupon) OVER (PARTITION BY groupon_real_deal_uuid)= 0 then 1
            else participants_per_coupon/ min(participants_per_coupon)  OVER (PARTITION BY groupon_real_deal_uuid) end
            ) as avail_taken_per_booking
       from
          grp_gdoop_bizops_db.sh_bt_deals
   )  deals on deals.deal_uuid = avail.deal_uuid and deals.deal_option_uuid = avail.deal_option_uuid
   WHERE country= 'US'
   group by avail.deal_uuid, country ,report_date, avail.deal_option_uuid)
  fin_0) as
fin_1 where update_order = 1;*/



------TD
drop table sandbox.nvp_inv_availability;
CREATE MULTISET TABLE sandbox.nvp_inv_availability,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
       deal_uuid VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
       deal_option_id	VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
       country	VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
       report_date	VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
       num_dow integer,
       update_order integer
     )
NO PRIMARY INDEX ;



-----Hive
drop table grp_gdoop_bizops_db.nvp_bt_active_deals_log;

create table grp_gdoop_bizops_db.nvp_bt_active_deals_log stored as orc as
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
    ad.load_date
;


------TD


drop table sandbox.nvp_bt_active_deals_log;
CREATE MULTISET TABLE sandbox.nvp_bt_active_deals_log ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
       deal_uuid VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
       deal_option_id	VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
       gapp_enabled	integer,
       is_bookable	integer,
       partner_inactive_flag	integer,
       product_is_active_flag integer,
       new_bt_opt_in_flag integer,
       new_bt_opt_in_date VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
       is_multisession_flag integer,
       is_multiagenda_flag integer,
       country	VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
       sold_out VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
       load_date VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC
     )
NO PRIMARY INDEX ;






----------------------------Old trials


drop table sandbox.nvp_bt_deals;
CREATE MULTISET TABLE sandbox.sh_bt_deals ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
       id	integer,
       partners_id	integer,
       products_id	integer,
       groupon_real_deal_uuid	VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
       inventory_product_uuid VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
       gapp_enabled	integer,
       gapp_registered	VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
       gapp_updated	VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
       gapp_deleted	VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
       gapp_status	VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
       country	VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC
     )
NO PRIMARY INDEX ;
------------------------------------
select *
from sandbox.jk_bt_deal_max_avail_v3
where deal_uuid = '196e7136-c3c7-429f-b26a-1944fc58dc02'
and report_date = '2020-09-22'
order by report_date;

select * from sandbox.jk_bt_deal_max_avail_v3
where
deal_uuid = 'd84c19bc-57e6-4a01-8ece-788e11235077'
order by report_date desc;


select
	inv_product_uuid,
	sum(transaction_qty)
from
user_edwprod.fact_gbl_transactions
where deal_uuid = 'cfcab58c-f8cd-49e5-8358-9c354101575a'
and action = 'capture'
and order_date >= cast('2020-09-01' as date)
group by inv_product_uuid
;

select * from user_edwprod.dim_offer_ext where inv_product_uuid = '84c24a67-f07a-46f1-88ab-5de2c64c50c3';



select * from user_dw.v_fact_gbl_transactions sample 5;




--------sh_bt_deals


id	integer,
partners_id	integer,
products_id	integer,
groupon_deal_id	VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
groupon_deal_uuid	VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
groupon_permalink	VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
groupon_real_deal_uuid	VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
groupon_location_uuid	VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
validity_start	VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
validity_end	VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
duration	integer,
amount_sold	integer,
amount_session	integer,
deal_name	VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
city	VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
redemption_locations	VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
capping_day	integer,
capping_week	integer,
capping_month	integer,
cancellation_period	integer,
calendar_view	integer,
capping_monday	integer,
capping_tuesday	integer,
capping_wednesday	integer,
capping_thursday	integer,
capping_friday	integer,
capping_saturday	integer,
capping_sunday	integer,
booking_delay	integer,
bookings_max_per_coupon	integer,
booking_till	VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
capacity	integer,
comment_required	integer,
comment_heading	VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
participants_per_coupon	integer,
applied_sessions	VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
applied_days_per_session	VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
is_g2_deal	integer,
is_btos_deal	integer,
show_warning_message	integer,
warning_message	VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
accept_redeemed_vouchers	integer,
arrival_days	VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
validity_dates	VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
multicoupon_after_session_reminder_enabled	integer,
multicoupon_ending_validity_reminder_enabled	integer,
multicoupon_ending_validity_reminder_days	integer,
multicoupon_validity_enabled	integer,
multicoupon_validity_days	integer,
multicoupon_minimum_interval_enabled	integer,
multicoupon_minimum_interval_days	integer,
customers_agenda_choice_enabled	integer,
ts_btos_update	VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
image_small_url	VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
ts_created	VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
ts_modfied	VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
created_by_ssu	integer,
ssu_opportunity_id	VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
inventory_product_uuid	VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
inventory_service_id	VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
gapp_enabled	integer,
gapp_registered	VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
gapp_updated	VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
gapp_deleted	VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
gapp_status	VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
country	VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC
