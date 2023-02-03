select * from sh_bt_deals;
drop table sh_bt_deals;
create volatile table sh_bt_deals as (
    sel a.deal_uuid,
        a2.deal_option_id unified_deal_option_id,
        a2.gapp_enabled,
        sf.opportunity_id,
        sf.account_name,
        sf.account_owner,
        sf.account_id,
        cast(l.bt_launch_date as date) bt_launch_date,
        sf.division,
        a.has_gcal,
        case when a2.is_bookable = 1 and a2.partner_inactive_flag = 0 and a2.product_is_active_flag = 1 then 1 else 0 end bookable,
        dal.num_dow,
        dal.num_dow_total,
        dal.load_week,
        dal.completely_blocked_days,
        dal.completely_paused_days,
        case when dal.num_dow_total is not null then 1 else 0 end has_availability_setup, ----this is redundant now
        case when cast(new_avail.latest_refresh_date as date) < trunc(current_date, 'iw') - 3 or new_avail.deal_uuid is null then 1 end latest_availability_not_data, 
        new_avail.deal_option_uuid data_check, 
        cast(new_avail.earliest_availability as date) earliest_availability, 
        case when top_a.account_id is not null then 1 else 0 end top_account, 
        case when sf.division in ('Long Island','Seattle','Detroit','Denver', 'Dallas', 'Fort Worth') then 1 else 0 end to_market
    from sandbox.sh_bt_active_deals_log_v4 as a
    join sandbox.nvp_bt_active_deals_log as a2 on a.deal_uuid = a2.deal_uuid
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
    left join
        (select
           *
           from sandbox.nvp_inv_latest_week_avail 
           where country = 'US'
        ) dal on a.deal_uuid = dal.deal_uuid and a2.deal_option_id = dal.deal_option_uuid
    left join sandbox.nvp_future_avail_inv as new_avail on a.deal_uuid = new_avail.deal_uuid AND a2.deal_option_id = new_avail.deal_option_uuid and new_avail.country = 'US'
    left join sandbox.avb_top_accts_mar2021 as top_a on sf.account_id = top_a.account_id
    where a.product_is_active_flag = 1
    and a.partner_inactive_flag = 0
    and a.is_bookable = 1
    and a.sold_out = 'false'
    and a.load_date = current_date-2
    and gdl.country_code = 'US'
    and gdl.grt_l1_cat_name = 'L1 - Local'
    and gdl.grt_l2_cat_description = 'HBW'
) with data unique primary index (deal_uuid, unified_deal_option_id) on commit preserve rows;



create volatile table sh_fgt_book as (
    sel fgt.*
    from user_edwprod.fact_gbl_transactions fgt
    join
       (select deal_uuid,
               unified_deal_option_id,
               bt_launch_date
         from sh_bt_deals group by 1,2,3
         ) d on fgt.deal_uuid = d.deal_uuid and (fgt.unified_deal_option_id is null or fgt.unified_deal_option_id = d.unified_deal_option_id)
    where order_date >= case when d.bt_launch_date > current_date-30 then d.bt_launch_date else current_date-30 end
) with data primary index (order_id, action) on commit preserve rows;



create volatile table sh_booked as (
    sel f.order_id,
        max(case when bn.voucher_code is not null then 1 else 0 end) booked_bt, 
        max(case when bn.booked_by = 'api' and substr(bn.created_at,1,10) = substr(cast(cmc.created_at as varchar(50)),1,10) then 1 else 0 end) prepurchase_bookings
    from sh_fgt_book f
    join user_gp.camp_membership_coupons cmc on f.order_id = cast(cmc.order_id as varchar(64))
    left join sandbox.sh_bt_bookings_rebuild bn on cmc.code = bn.voucher_code and cast(cmc.merchant_redemption_code as varchar(50)) = bn.security_code
    group by 1
) with data unique primary index (order_id) on commit preserve rows;



drop table sh_inv_pds;
create volatile table sh_inv_pds as (
    sel doe.merch_product_uuid, max(pds.pds_cat_name) pds, max(inv_product_uuid) inv_product_uuid
    from user_edwprod.dim_offer_ext doe
    join user_dw.v_dim_pds_grt_map pds on doe.pds_cat_id = pds.pds_cat_id
    group by 1
) with data unique primary index(merch_product_uuid) on commit preserve rows;

drop table sh_units;
create volatile table sh_units as (
    sel deal_uuid,
        unified_deal_option_id,
        pds, ------no need for row num stuff because we are doing a max() above by 
        sum(units_sold) units_sold,
        sum(case when booked_bt = 1 then units_sold else 0 end) units_sold_booked_bt,
        sum(case when prepurchase_bookings = 1 then units_sold else 0 end) units_prepurchase_booked
    from (
            sel m.deal_uuid,
                m.unified_deal_option_id,
                pds.pds,
                b.booked_bt,
                prepurchase_bookings,
                sum(fgt.transaction_qty) units_sold
            from sh_bt_deals m
            join sh_fgt_book fgt on m.deal_uuid = fgt.deal_uuid and (fgt.unified_deal_option_id is null or m.unified_deal_option_id = fgt.unified_deal_option_id)
            join sh_inv_pds pds on fgt.unified_deal_option_id = pds.merch_product_uuid
            left join sh_booked b on fgt.order_id = b.order_id
            where fgt.action = 'authorize'
              and fgt.is_zero_amount = 0
              and fgt.is_order_canceled = 0
              and fgt.order_date >= current_date-30
              and fgt.order_date >= m.bt_launch_date
            group by 1,2,3,4,5
    ) t
    group by 1,2,3
) with data unique primary index (deal_uuid, unified_deal_option_id) on commit preserve rows;


drop table sh_refunds;
create volatile table sh_refunds as (
    sel order_id
    from user_edwprod.fact_gbl_transactions
    where action = 'refund'
    and order_date >= '2020-01-01'
    group by 1
) with data unique primary index (order_id) on commit preserve rows;

drop table sh_booked_refunds;
create volatile table sh_booked_refunds as (
    sel f.deal_uuid,
        f.unified_deal_option_id,
        count(distinct b.order_id) booked_txns_inv,
        count(distinct r.order_id) booked_refunds_inv,
        cast(booked_refunds_inv as dec(18,3)) / cast(booked_txns_inv as dec(18,3)) booked_refund_rate_inv
    from sh_booked b
    join (sel order_id, max(deal_uuid) deal_uuid, max(unified_deal_option_id) unified_deal_option_id from sh_fgt_book group by 1) f on b.order_id = f.order_id
    left join sh_refunds r on b.order_id = r.order_id
    where booked_bt = 1
    group by 1,2
) with data unique primary index (deal_uuid, unified_deal_option_id) on commit preserve rows;



create table sandbox.nvp_hbw_booking_inv_status_deal as (
 select t_.*,
          case when units_sold_t30 > 3 and (has_availability_setup = 0 or latest_availability_not_data = 1) then 'a. availability setup/data issue'
               when units_sold_t30 > 3 and num_dow_total < 1 then 'b. No capacity setup'
               when units_sold_t30 > 3 and num_dow < 1 then 'c. 0 days available - blocked/paused'
               when units_sold_t30 > 3 and avg_booking_delay > 24 then 'd. booking leadtime >24 hours'
               when units_sold_t30 > 3 and num_dow < 4 then 'e. less than 4 days available next week'
               when units_sold_t30 > 3 and pct_units_booked_pre < .2 then 'f. less than 20% units prepurchase booked'
               when units_sold_t30 > 3 and booked_refund_rate_inv > .25 then 'g. high refunds on booked vouchers'
               when units_sold_t30 > 3 then 'h. no action needed'
               when (units_sold_t30 is null or units_sold_t30 = 0) then 'x. 0 units sold'
               when units_sold_t30 <= 3 then 'y. <= 3 units sold'
           end action_cohort,
          case when units_sold_t30 > 3 and (has_availability_setup = 0 or latest_availability_not_data  = 1) then 'Availability - data problem'
               when units_sold_t30 > 3 and num_dow_total < 1 then 'Availability - None'
               when units_sold_t30 > 3 and num_dow < 1 then 'Availability - None'
               when units_sold_t30 > 3 and avg_booking_delay > 24 then 'Long Lead time'
               when units_sold_t30 > 3 and num_dow < 4 then 'Availability - Low'
               when units_sold_t30 > 3 and pct_units_booked_pre < .2 then 'Low Prepurchase Bookings'
               when units_sold_t30 > 3 and booked_refund_rate_inv > .25 then 'High Refunds'
               when units_sold_t30 > 3 and units_sold_t30 > 3 then 'No Action'
               when (units_sold_t30 is null or units_sold_t30 = 0) then 'x. 0 units sold'
               when units_sold_t30 <= 3 then 'y. <= 3 units sold'
          end action_cohort2, 
          case when (has_availability_setup = 0 or latest_availability_not_data  = 1) then 'a. availability setup/data issue'
               when num_dow_total < 1 then 'b. No capacity setup'
               when num_dow < 1 then 'c. 0 days available - blocked/paused'
               when avg_booking_delay > 24 then 'd. booking leadtime >24 hours'
               when num_dow < 4 then 'e. less than 4 days available next week'
               else 'f. No availability issue'
           end availability_cohort,
           case when (has_availability_setup = 0 or latest_availability_not_data  = 1) then 'Availability - data problem'
             when num_dow_total < 1 then 'Availability - None'
             when num_dow < 1 then 'Availability - None'
             when avg_booking_delay > 24 then 'Long Lead time'
             when num_dow < 4 then 'Availability - Low'
             else 'No availability issue'
           end availability_cohort2,
          case when data_check is null then 'no availability_data' 
               when earliest_availability is null then 'no availability at all'
               when earliest_availability  <= trunc(current_date, 'iw')+6 then 'availability this week' 
               when earliest_availability  <= trunc(current_date, 'iw')+6 + 7 then 'availability next week' 
               when earliest_availability  <= trunc(current_date, 'iw')+6 + 14 then 'availability within 2 weeks'
               when earliest_availability  <= trunc(current_date, 'iw')+6 + 30 then 'availability within a month'
               when earliest_availability  <= trunc(current_date, 'iw')+6 + 61 then 'availability within 2 months'
               when earliest_availability  <= trunc(current_date, 'iw')+6 + 91 then 'availability within 3 months'
          else 'no availability within 3 months' end 
          earliest_availability_cohort,
          mat.metal_at_close, 
         case when mat.metal_at_close in ('Silver', 'Gold', 'Platinum') then 'S+' else 'B-' end metal_category, 
         case when dream.account_id is not null then 'Yes' else 'No' end is_dream_account
         from
(sel d.*,
      u.pds,
      u.units_sold as units_sold_t30,
      u.units_sold_booked_bt as units_sold_bt_t30,
      u.units_prepurchase_booked,
      cast(u.units_prepurchase_booked as dec(18,3)) / cast(nullifzero(u.units_sold) as dec(18,3)) pct_units_booked_pre,
      cast(u.units_sold_booked_bt as dec(18,3)) / cast(nullifzero(u.units_sold) as dec(18,3)) pct_units_booked,
      cast(bdd.avg_booking_delay as dec(18,3)) avg_booking_delay,
      booked_refund_rate_inv
  from sh_bt_deals d
  left join sandbox.sh_dream_booking_delay_deal bdd on d.deal_uuid = bdd.deal_uuid
  left join sh_units u on d.deal_uuid = u.deal_uuid and d.unified_deal_option_id = u.unified_deal_option_id
  left join sh_booked_refunds r on d.deal_uuid = r.deal_uuid and d.unified_deal_option_id = r.unified_deal_option_id
) as t_
      left join sandbox.rev_mgmt_deal_attributes mat on t_.deal_uuid = mat.deal_id
      left join sandbox.ar_hbw_dream_merchants dream on t_.account_id = dream.account_id
) with data unique primary index (deal_uuid, unified_deal_option_id);
  

select * from sandbox.nvp_hbw_booking_inv_status_deal;





----teradata ---- EARLIEST AVAILABILITY-----option level

drop table sandbox.nvp_future_avail_inv;
CREATE MULTISET TABLE sandbox.nvp_future_avail_inv ,NO FALLBACK ,
      NO BEFORE JOURNAL,
      NO AFTER JOURNAL,
      CHECKSUM = DEFAULT,
      DEFAULT MERGEBLOCKRATIO,
      MAP = TD_MAP1
      (
        deal_uuid VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
        deal_option_uuid VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
        country	VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
        earliest_availability	VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC, 
        latest_refresh_date VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
        total_available_days integer
      )
 NO PRIMARY INDEX ;

-----hive ------EARLIEST AVAILABILITY------option level

drop table if exists grp_gdoop_bizops_db.nvp_temp_future_inv;
create table grp_gdoop_bizops_db.nvp_temp_future_inv stored as orc as
select
    deal_uuid,
    deal_option_uuid,
    country,
    min(case when day_available is not null then reference_date end) earliest_availability,
    max(report_date) latest_refresh_date,
    count(distinct case when day_available is not null then reference_date end) total_available_days
from
(select
      avail.deal_uuid,
      avail.deal_option_uuid,
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
   deal_uuid, deal_option_uuid,
   country;




---------------------TD --------availability option level
drop table sandbox.nvp_weekstart_avail;
CREATE MULTISET TABLE sandbox.nvp_inv_latest_week_avail ,NO FALLBACK ,
      NO BEFORE JOURNAL,
      NO AFTER JOURNAL,
      CHECKSUM = DEFAULT,
      DEFAULT MERGEBLOCKRATIO,
      MAP = TD_MAP1
      (
        deal_uuid VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
        deal_option_uuid VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
        country	VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
        report_weekdate	VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
        load_week	VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
        num_dow integer, 
        num_dow_total integer, 
        completely_blocked_days integer, 
        completely_paused_days integer
      )
 NO PRIMARY INDEX ;



create table grp_gdoop_bizops_db.np_bt_inv_availability stored as orc as
select
     deal_uuid, 
     deal_option_uuid,
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
       avail.deal_option_uuid,
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
group by deal_uuid,deal_option_uuid, country, report_week, load_week;



-----Hive --------option level deal log
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


