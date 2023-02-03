select has_availability_setup, latest_availability_not_data, count(distinct deal_uuid) from sh_bt_deals group by 1,2;

select * from sandbox.nvp_future_avail;

drop table sh_bt_deals;
drop table sh_fgt_ord;
drop table sh_booked;
drop table sh_inv_pds;
drop table sh_units;
drop table sh_refunds;
drop table sh_booked_refunds;




create volatile table sh_bt_deals as (
    sel a.deal_uuid,
        sf.opportunity_id,
        account_name,
        account_owner,
        sf.account_id,
        cast(bt_launch_date as date) bt_launch_date,
        division,
        has_gcal,
        dal.num_dow,
        dal.num_dow_total,
        dal.load_week,
        dal.completely_blocked_days,
        dal.completely_paused_days,
        case when dal.num_dow_total is not null then 1 else 0 end has_availability_setup, ----this is redundant now
        case when cast(new_avail.latest_refresh_date as date) < trunc(current_date, 'iw') - 3 or new_avail.deal_uuid is null then 1 end latest_availability_not_data, 
        new_avail.deal_uuid data_check, 
        cast(new_avail.earliest_availability as date) earliest_availability, 
        case when top_a.account_id is not null then 1 else 0 end top_account, 
        case when sf.division in ('Long Island','Seattle','Detroit','Denver', 'Dallas', 'Fort Worth') then 1 else 0 end to_market,
        total_available_days total_days_available_90
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
    left join (
      sel deal_uuid,
          load_week,
          num_dow, 
          num_dow_total,
          completely_blocked_days, 
          completely_paused_days
          from sandbox.nvp_weekstart_avail
          where report_weekdate >= trunc(current_date, 'iw') - 3 and country = 'US'
    ) dal on a.deal_uuid = dal.deal_uuid
    left join sandbox.nvp_future_avail as new_avail on a.deal_uuid = new_avail.deal_uuid and new_avail.country = 'US'
    left join sandbox.avb_top_accts_mar2021 as top_a on sf.account_id = top_a.account_id
    where product_is_active_flag = 1
    and partner_inactive_flag = 0
    and is_bookable = 1
    and sold_out = 'false'
    and load_date = current_date-2
    and gdl.country_code = 'US'
    and gdl.grt_l1_cat_name = 'L1 - Local'
    and gdl.grt_l2_cat_description = 'HBW'
) with data unique primary index (deal_uuid) on commit preserve rows;



create volatile table sh_fgt_ord as (
    sel fgt.*
    from user_edwprod.fact_gbl_transactions fgt
    join sh_bt_deals d on fgt.deal_uuid = d.deal_uuid
    where order_date >= current_date-30
    and order_date >= d.bt_launch_date
) with data primary index (order_id, action) on commit preserve rows;


create volatile table sh_booked as (
    sel f.order_id,
        max(case when bn.voucher_code is not null then 1 else 0 end) booked_bt, 
        max(case when bn.booked_by = 'api' and substr(bn.created_at,1,10) = substr(cast(cmc.created_at as varchar(50)),1,10) then 1 else 0 end) prepurchase_bookings
    from sh_fgt_ord f
    join user_gp.camp_membership_coupons cmc on f.order_id = cast(cmc.order_id as varchar(64))
    left join sandbox.sh_bt_bookings_rebuild bn on cmc.code = bn.voucher_code and cast(cmc.merchant_redemption_code as varchar(50)) = bn.security_code
    group by 1
) with data unique primary index (order_id) on commit preserve rows;



create volatile table sh_inv_pds as (
    sel doe.inv_product_uuid, max(pds.pds_cat_name) pds
    from user_edwprod.dim_offer_ext doe
    join user_dw.v_dim_pds_grt_map pds on doe.pds_cat_id = pds.pds_cat_id
    group by 1
) with data unique primary index (inv_product_uuid) on commit preserve rows;


create volatile table sh_units as (
    sel
        deal_uuid,
        min(case when pds_rank = 1 then pds end) top_pds,
        sum(units_sold) units_sold,
        sum(case when booked_bt = 1 then units_sold else 0 end) units_sold_booked_bt, 
        sum(case when prepurchase_bookings = 1 then units_sold else 0 end) units_prepurchase_booked
    from (
        sel t.*,
            row_number() over (partition by deal_uuid order by units_sold desc) pds_rank
        from (
            sel m.deal_uuid,
                pds.pds,
                booked_bt,
                prepurchase_bookings,
                sum(fgt.transaction_qty) units_sold
            from sh_bt_deals m
            join sh_fgt_ord fgt on m.deal_uuid = fgt.deal_uuid
            join sh_inv_pds pds on fgt.inv_product_uuid = pds.inv_product_uuid
            left join sh_booked b on fgt.order_id = b.order_id
            where fgt.action = 'authorize'
            and fgt.is_zero_amount = 0
            and fgt.is_order_canceled = 0
            and fgt.order_date >= m.bt_launch_date
            group by 1,2,3,4
        ) t
    ) t
    group by 1
) with data unique primary index (deal_uuid) on commit preserve rows;


create volatile table sh_refunds as (
    sel order_id
    from user_edwprod.fact_gbl_transactions
    where action = 'refund'
    and order_date >= '2020-01-01'
    group by 1
) with data unique primary index (order_id) on commit preserve rows;


create volatile table sh_booked_refunds as (
    sel f.deal_uuid,
        count(distinct b.order_id) booked_txns,
        count(distinct r.order_id) booked_refunds,
        cast(booked_refunds as dec(18,3)) / cast(booked_txns as dec(18,3)) booked_refund_rate
    from sh_booked b
    join (sel order_id, max(deal_uuid) deal_uuid from sh_fgt_ord group by 1) f on b.order_id = f.order_id
    left join sh_refunds r on b.order_id = r.order_id
    where booked_bt = 1
    group by 1
) with data unique primary index (deal_uuid) on commit preserve rows;


create volatile table sh_booked_refunds_all as (
    sel f.deal_uuid,
        count(distinct b.order_id) booked_txns_all,
        count(distinct r.order_id) booked_refunds_all,
        cast(booked_refunds_all as dec(18,3)) / cast(booked_txns_all as dec(18,3)) booked_refund_rate_all
    from sh_booked b
    join (sel order_id, max(deal_uuid) deal_uuid from sh_fgt_ord group by 1) f on b.order_id = f.order_id
    left join sh_refunds r on b.order_id = r.order_id
    group by 1
) with data unique primary index (deal_uuid) on commit preserve rows;


create volatile multiset table np_conversion_report as (
select
    deal_id,
    sum(uniq_deal_views) deal_views,
    sum(transactions) all_transaction,
    sum(case when sbt.deal_uuid is not null then uniq_deal_views else 0 end) bookable_deal_views,
    sum(case when sbt.deal_uuid is not null then transactions else 0 end) bookable_transactions
from
   user_edwprod.agg_gbl_traffic_fin_deal a 
   left join sandbox.sh_bt_active_deals_log_v4 sbt 
        on a.deal_id = sbt.deal_uuid 
        and cast(a.report_date as date) = cast(sbt.load_date as date) 
        and sbt.product_is_active_flag = 1 
        and sbt.partner_inactive_flag = 0
where  
    report_date >= current_date-30
    group by 1
) with data on commit preserve rows
;   

drop table sandbox.nvp_hbw_booking_status_deal;
create table sandbox.nvp_hbw_booking_status_deal as (
 select t_.*,
          case when units_sold_t30 > 3 and (has_availability_setup = 0 or latest_availability_not_data = 1) then 'a. availability setup/data issue'
               when units_sold_t30 > 3 and earliest_availability is null then 'b.No availability at all'
               when units_sold_t30 > 3 and num_dow_total < 1 then 'c. No capacity setup'
               when units_sold_t30 > 3 and num_dow < 1 then 'd. 0 days available - blocked/paused'
               when units_sold_t30 > 3 and avg_booking_delay > 24 then 'e. booking leadtime >24 hours'
               when units_sold_t30 > 3 and num_dow < 4 then 'f. less than 4 days available next week'
               when units_sold_t30 > 3 and pct_units_booked_pre < .2 then 'g. less than 20% units prepurchase booked'
               when units_sold_t30 > 3 and booked_refund_rate > .25 then 'h. high refunds on booked vouchers'
               when units_sold_t30 > 3 then 'i. no action needed'
               when (units_sold_t30 is null or units_sold_t30 = 0) then 'x. 0 units sold'
               when units_sold_t30 <= 3 then 'y. <= 3 units sold'
           end action_cohort,
          case when units_sold_t30 > 3 and (has_availability_setup = 0 or latest_availability_not_data  = 1) then 'Availability - data problem'
               when units_sold_t30 > 3 and earliest_availability is null then 'Availability - None at all'
               when units_sold_t30 > 3 and num_dow_total < 1 then 'Availability - None'
               when units_sold_t30 > 3 and num_dow < 1 then 'Availability - None'
               when units_sold_t30 > 3 and avg_booking_delay > 24 then 'Long Lead time'
               when units_sold_t30 > 3 and num_dow < 4 then 'Availability - Low'
               when units_sold_t30 > 3 and pct_units_booked_pre < .2 then 'Low Prepurchase Bookings'
               when units_sold_t30 > 3 and booked_refund_rate > .25 then 'High Refunds'
               when units_sold_t30 > 3 and units_sold_t30 > 3 then 'No Action'
               when (units_sold_t30 is null or units_sold_t30 = 0) then 'x. 0 units sold'
               when units_sold_t30 <= 3 then 'y. <= 3 units sold'
          end action_cohort2, 
          case when (has_availability_setup = 0 or latest_availability_not_data  = 1) then 'a. availability setup/data issue'
               when earliest_availability is null then 'b.No availability at all'
               when num_dow_total < 1 then 'c. No capacity setup'
               when num_dow < 1 then 'd. 0 days available - blocked/paused'
               when avg_booking_delay > 24 then 'e. booking leadtime >24 hours'
               when num_dow < 4 then 'f. less than 4 days available next week'
               else 'g. No availability issue'
           end availability_cohort,
           case when (has_availability_setup = 0 or latest_availability_not_data  = 1) then 'Availability - data problem'
             when earliest_availability is null then 'Availability - None at all'
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
         case when dream.account_id is not null then 'Yes' else 'No' end is_dream_account, 
         case when data_check is null then 'no availability data'
              when total_days_available_90 = 0 then 'No Availability at all'
              when total_days_available_90 <= 10 then 'less than or equal to 10 days available'
              when total_days_available_90 <= 20 then '11 - 20 days available'
              when total_days_available_90 <= 30 then '21 - 30 days available'
              when total_days_available_90 <= 40 then '31 - 40 days available'
              when total_days_available_90 <= 50 then '41 - 50 days available'
              when total_days_available_90 <= 60 then '51 - 60 days available'
              when total_days_available_90 <= 90 then '61 - 90 days available'
              end ninety_day_availability_cohort
      from 
      (sel d.*,
              u.top_pds,
              u.units_sold as units_sold_t30,
              u.units_sold_booked_bt as units_sold_bt_t30,
              u.units_prepurchase_booked as units_prepurchase_booked_t30,
              cast(u.units_prepurchase_booked as dec(18,3)) / cast(nullifzero(u.units_sold) as dec(18,3)) pct_units_booked_pre,
              cast(u.units_sold_booked_bt as dec(18,3)) / cast(nullifzero(u.units_sold) as dec(18,3)) pct_units_booked,
              cast(bdd.avg_booking_delay as dec(18,3)) avg_booking_delay,
              r.booked_refund_rate,
              cr.deal_views,
              cr.all_transaction,
              cr.bookable_deal_views,
              cr.bookable_transactions, 
              rall.booked_txns_all, 
              rall.booked_refunds_all
          from sh_bt_deals d
          left join sandbox.sh_dream_booking_delay_deal bdd on d.deal_uuid = bdd.deal_uuid
          left join sh_units u on d.deal_uuid = u.deal_uuid
          left join sh_booked_refunds r on d.deal_uuid = r.deal_uuid
          left join sh_booked_refunds_all rall on d.deal_uuid = rall.deal_uuid
          left join np_conversion_report cr on d.deal_uuid = cr.deal_id
      ) as t_
      left join sandbox.rev_mgmt_deal_attributes mat on t_.deal_uuid = mat.deal_id
      left join sandbox.ar_hbw_dream_merchants dream on t_.account_id = dream.account_id
  ) with data unique primary index (deal_uuid);

grant sel on sandbox.nvp_hbw_booking_status_deal to public;



drop table sandbox.nvp_hbw_booking_status_account;
create table sandbox.nvp_hbw_booking_status_account as (
      sel t.*,
          case when units_sold_t30 > 3 and (has_availability_setup = 0 or latest_availability_not_data  = 1) then 'a. availability setup/data issue'
               when units_sold_t30 > 3 and earliest_availability is null then 'b.No availability at all'
               when units_sold_t30 > 3 and num_dow_total < 1 then 'c. No capacity setup'
               when units_sold_t30 > 3 and num_dow < 1 then 'd. 0 days available - blocked/paused'
               when units_sold_t30 > 3 and avg_booking_delay > 24 then 'e. booking leadtime >24 hours'
               when units_sold_t30 > 3 and num_dow < 4 then 'f. less than 4 days available next week'
               when units_sold_t30 > 3 and pct_units_booked_pre < .3 then 'g. less than 20% units prepurchase booked'
               when units_sold_t30 > 3 and booked_refund_rate > .25 then 'h. high refunds on booked vouchers'
               when units_sold_t30 > 3 then 'i. no action needed'
               when (units_sold_t30 is null or units_sold_t30 = 0) then 'x. 0 units sold'
               when units_sold_t30 <= 3 then 'y. <= 3 units sold'
           end action_cohort,
          case when units_sold_t30 > 3 and (has_availability_setup = 0 or latest_availability_not_data  = 1) then 'Availability - data problem'
               when units_sold_t30 > 3 and earliest_availability is null then 'Availability - None at all'
               when units_sold_t30 > 3 and num_dow_total < 1 then 'Availability - None'
               when units_sold_t30 > 3 and num_dow < 1 then 'Availability - None'
               when units_sold_t30 > 3 and avg_booking_delay > 24 then 'Long Lead time'
               when units_sold_t30 > 3 and num_dow < 4 then 'Availability - Low'
               when units_sold_t30 > 3 and pct_units_booked_pre < .2 then 'Low Prepurchase Bookings'
               when units_sold_t30 > 3 and booked_refund_rate > .25 then 'High Refunds'
               when units_sold_t30 > 3 then 'No Action'
               when (units_sold_t30 is null or units_sold_t30 = 0) then 'x. 0 units sold'
               when units_sold_t30 <= 3 then 'y. <= 3 units sold'
           end action_cohort2,
           case when (has_availability_setup = 0 or latest_availability_not_data  = 1) then 'a. availability setup/data issue'
               when earliest_availability is null then 'b.No availability at all'
               when num_dow_total < 1 then 'c. No capacity setup'
               when num_dow < 1 then 'd. 0 days available - blocked/paused'
               when avg_booking_delay > 24 then 'e. booking leadtime >24 hours'
               when num_dow < 4 then 'f. less than 4 days available next week'
               else 'g. No availability issue'
           end availability_cohort,
           case when (has_availability_setup = 0 or latest_availability_not_data  = 1) then 'Availability - data problem'
             when earliest_availability is null then 'Availability - None at all'
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
      from (
          sel d.account_id,
              max(d.account_name) account_name,
              max(d.account_owner) rep_name,
              min(d.bt_launch_date) bt_launch_date,
              max(d.deal_uuid) sample_deal_uuid,
              max(d.has_gcal) has_gcal,
              max(d.division) division,
              count(distinct d.deal_uuid) n_deals_bookable,
              max(u.top_pds) pds,
              sum(u.units_sold) as units_sold_t30,
              sum(u.units_sold_booked_bt) as units_sold_bt_t30,
              sum(u.units_prepurchase_booked) as units_prepurchase_booked_t30,
              cast(units_prepurchase_booked_t30 as dec(18,3)) / cast(nullifzero(units_sold_t30) as dec(18,3)) pct_units_booked_pre,
              cast(units_sold_bt_t30 as dec(18,3)) / cast(nullifzero(units_sold_t30) as dec(18,3)) pct_units_booked,
              cast(sum(r.booked_refunds) as dec(18,3)) / cast(nullifzero(sum(r.booked_txns)) as dec(18,3)) booked_refund_rate,
              cast(avg(bdd.avg_booking_delay) as dec(18,3)) avg_booking_delay,
              max(d.num_dow) num_dow,
              max(d.num_dow_total) num_dow_total,
              max(d.completely_blocked_days) completely_blocked_days,
              max(d.completely_paused_days) completely_paused_days,
              max(has_availability_setup) has_availability_setup,
              max(latest_availability_not_data) latest_availability_not_data, 
              min(earliest_availability) earliest_availability, 
              max(data_check) data_check,
              max(top_account) top_account, 
              max(to_market) to_market
          from sh_bt_deals d
          left join sandbox.sh_dream_booking_delay_deal bdd on d.deal_uuid = bdd.deal_uuid
          left join sh_units u on d.deal_uuid = u.deal_uuid
          left join sh_booked_refunds r on d.deal_uuid = r.deal_uuid
          group by 1
      ) t
      left join (select account_id, max(metal_at_close) metal_at_close from sandbox.rev_mgmt_deal_attributes group by 1) mat on t.account_id = mat.account_id
      left join sandbox.ar_hbw_dream_merchants dream on t.account_id = dream.account_id
      ) with data unique primary index (account_id);

grant sel on sandbox.nvp_hbw_booking_status_account to public;



select * from sandbox.nvp_hbw_booking_status_deal;

select * from sandbox.nvp_hbw_booking_status_account;
----------------------------------------- < 3 Units sold deal


