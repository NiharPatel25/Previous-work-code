
select * from user_edwprod.dim_gbl_deal_lob where deal_id = '9ba6f070-423c-4a9f-b4df-54cdedb85e2c';
------------AVG BOOKING DELAY DATA PULL

delete from sandbox.sh_dream_booking_delay_deal_intl;

select groupon_real_deal_uuid, a.country, avg(a.booking_delay) avg_booking_delay
from grp_gdoop_bizops_db.sh_bt_products a
join grp_gdoop_bizops_db.sh_bt_partners b on a.partners_id = b.id
join grp_gdoop_bizops_db.sh_bt_deals c on a.id = c.products_id and b.id = c.partners_id
where a.country <> 'US'
and b.booking_solution = 'BasicV2'
and b.inactive = 0
group by groupon_real_deal_uuid, a.country


drop table sandbox.sh_dream_booking_delay_deal_intl;
CREATE MULTISET TABLE sandbox.sh_dream_booking_delay_deal_intl ,NO FALLBACK ,
      NO BEFORE JOURNAL,
      NO AFTER JOURNAL,
      CHECKSUM = DEFAULT,
      DEFAULT MERGEBLOCKRATIO,
      MAP = TD_MAP1
      (
        deal_uuid VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
        country	VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
		avg_booking_delay float
      )
 NO PRIMARY INDEX ;
grant sel on sandbox.sh_dream_booking_delay_deal_intl to public;

---------------------------------------------------------------

drop table sandbox.np_covid_closure;
CREATE MULTISET TABLE sandbox.np_covid_closure ,NO FALLBACK ,
      NO BEFORE JOURNAL,
      NO AFTER JOURNAL,
      CHECKSUM = DEFAULT,
      DEFAULT MERGEBLOCKRATIO,
      MAP = TD_MAP1
      (
        merchant_uuid VARCHAR(64) CHARACTER SET UNICODE,
        account_id	VARCHAR(64) CHARACTER SET UNICODE,
		country VARCHAR(64) CHARACTER SET UNICODE,
		start_day VARCHAR(64) CHARACTER SET UNICODE,
		end_day VARCHAR(64) CHARACTER SET UNICODE, 
		has_live_deals_flag VARCHAR(64) CHARACTER SET UNICODE, 
		live_deals_count VARCHAR(64) CHARACTER SET UNICODE
      )
 NO PRIMARY INDEX ;


CREATE MULTISET TABLE sandbox.np_slot_extended ,NO FALLBACK ,
      NO BEFORE JOURNAL,
      NO AFTER JOURNAL,
      CHECKSUM = DEFAULT,
      DEFAULT MERGEBLOCKRATIO,
      MAP = TD_MAP1
      (
        deal_uuid VARCHAR(64) CHARACTER SET UNICODE,
        country	VARCHAR(64) CHARACTER SET UNICODE,
		opportunity_id VARCHAR(64) CHARACTER SET UNICODE,
		g2_slot_not_created VARCHAR(64) CHARACTER SET UNICODE,
		action_to_take VARCHAR(64) CHARACTER SET UNICODE
      )
 NO PRIMARY INDEX;



select * from sandbox.np_covid_closure;

-----why are there deals with earliest availability in there with 

cast(concat(substr(start_day,7,4), '-' ,substr(start_day,4,2), '-' , substr(start_day,1,2)) as date) start_date,
   cast(concat(substr(end_day,7,4), '-' ,substr(end_day,4,2), '-' , substr(end_day,1,2)) as date) end_date

--------------- MAIN DATA 1
drop table sh_bt_deals;
drop table sh_fgt_ord;
drop table sh_booked;
drop table sh_inv_pds;
drop table sh_units;
drop table sh_refunds;
drop table sh_booked_refunds;
drop table nihpatel.np_covid_closure_temp;
select * from np_covid_closure_temp;
select * from sandbox.np_covid_closure;

create volatile table np_covid_closure_temp as (
select 
   a.merchant_uuid, 
   b.product_uuid,
   account_id, 
   country,
   cast(concat(split_part(start_day, '-', 3) ,'-', lpad(split_part(start_day, '-', 2),2,'0'), '-', lpad(split_part(start_day, '-', 1),2,'0')) as date) start_date,
   cast(concat(split_part(end_day, '-', 3) ,'-', lpad(split_part(end_day, '-', 2),2,'0'), '-', lpad(split_part(end_day, '-', 1),2,'0')) as date) end_date
from (select merchant_uuid, account_id, country, oreplace(start_day, '/', '-') start_day, oreplace(end_day, '/', '-') end_day from sandbox.np_covid_closure) as a 
left join 
    (select product_uuid, max(merchant_uuid) merchant_uuid from user_edwprod.dim_offer_ext group by 1) as b on a.merchant_uuid = b.merchant_uuid
) with data on commit preserve rows;



create volatile table sh_bt_deals as (
    sel a.deal_uuid,
        gdl.grt_l2_cat_name,
        gdl.country_code,
        sf.opportunity_id,
        account_name,
        account_owner,
        account_id,
        cast(bt_launch_date as date) bt_launch_date,
        division,
        has_gcal,
        dal.num_dow,
        dal.num_dow_total,
        dal.load_week,
        dal.completely_blocked_days,
        dal.completely_paused_days,
        case when dal.num_dow_total is not null then 1 else 0 end has_availability_setup,
        case when 
               cast(new_avail.latest_refresh_date as date) < trunc(current_date, 'iw') - 3  or new_avail.deal_uuid is null 
               then 1 end latest_availability_not_data,----this accounts for both availability setup or latest data not available 
        new_avail.deal_uuid data_check,
        cast(new_avail.earliest_availability as date) earliest_availability, 
        case when cov.product_uuid is not null then 1 else 0 end covid_paused,
        case when slot.deal_uuid is not null then 1 else 0 end slot_needs_extension
    from sandbox.sh_bt_active_deals_log_v4 a
    join user_edwprod.dim_gbl_deal_lob gdl on a.deal_uuid = gdl.deal_id
    left join (
        sel deal_uuid,
            min(load_date) bt_launch_date
        from sandbox.sh_bt_active_deals_log_v4
        where product_is_active_flag = 1
        and partner_inactive_flag = 0
        group by 1
    ) l on a.deal_uuid = l.deal_uuid
    left join (
        sel deal_uuid, 
            max(o1.id) opportunity_id, 
            max(o1.division) division, 
            max(sfa.name) account_name, 
            max(full_name) account_owner, 
            max(o1.accountid) account_id
        from user_edwprod.sf_opportunity_1 o1
        join user_edwprod.sf_opportunity_2 o2 on o1.id = o2.id
        join user_edwprod.sf_account sfa on o1.accountid = sfa.id
        left join user_groupondw.dim_sf_person sfp on sfa.ownerid = sfp.person_id
        group by 1
    ) sf on a.deal_uuid = sf.deal_uuid
    left join (
      sel deal_uuid,
          load_week,
          country,
          num_dow, 
          num_dow_total,
          completely_blocked_days,
          completely_paused_days
          from sandbox.nvp_weekstart_avail
          where report_weekdate >= trunc(current_date, 'iw') - 3 and country <> 'US'
    ) dal on a.deal_uuid = dal.deal_uuid and gdl.country_code = dal.country
    left join sandbox.nvp_future_avail as new_avail on a.deal_uuid = new_avail.deal_uuid and new_avail.country = gdl.country_code and new_avail.country <> 'US'
    left join 
         (select product_uuid, 
                 country
           from np_covid_closure_temp 
                 where current_date - 3 >= start_date and current_date - 3 <= end_date
                 group by 1,2
          ) cov on a.deal_uuid = cov.product_uuid and cov.country = gdl.country_code
    left join (select deal_uuid, country from sandbox.np_slot_extended group by 1,2) slot on a.deal_uuid = slot.deal_uuid and slot.country = gdl.country_code
    where a.product_is_active_flag = 1
    and a.partner_inactive_flag = 0
    and a.is_bookable = 1
    and a.sold_out = 'false'
    and a.load_date = current_date-2
    and gdl.country_code <> 'US'
    and gdl.grt_l1_cat_name = 'L1 - Local'
) with data unique primary index (deal_uuid, country_code) on commit preserve rows;



create volatile table sh_fgt_ord as (
    sel fgt.*, 
        cntry.country_iso_code_2 country_code
    from user_edwprod.fact_gbl_transactions fgt
    join dwh_base_sec_view.country cntry on fgt.country_id = cntry.country_id
    join sh_bt_deals d on fgt.deal_uuid = d.deal_uuid and cntry.country_iso_code_2 = d.country_code
    where order_date >= current_date-30
    and order_date >= d.bt_launch_date
) with data primary index (order_id, action) on commit preserve rows;


   
create volatile table sh_booked as (
    sel f.order_id,
        max(case when bn.voucher_code is not null and bn.deal_uuid = f.deal_uuid then 1 else 0 end) booked_bt, 
        max(case when bn.booked_by = 'api' and substr(bn.created_at,1,10) = substr(cast(v.created_at as varchar(50)),1,10) and bn.deal_uuid = f.deal_uuid then 1 else 0 end) prepurchase_bookings
    from sh_fgt_ord f
    join dwh_base_sec_view.vouchers v on f.parent_order_uuid = cast(v.billing_id as varchar(64))
    left join sandbox.sh_bt_bookings_rebuild bn on v.voucher_code = bn.voucher_code and v.security_code = bn.security_code
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
        country_code,
        min(case when pds_rank = 1 then pds end) top_pds,
        sum(units_sold) units_sold,
        sum(case when booked_bt = 1 then units_sold else 0 end) units_sold_booked_bt, 
        sum(case when prepurchase_bookings = 1 then units_sold else 0 end) units_prepurchase_booked
    from (
        sel t.*,
            row_number() over (partition by deal_uuid, country_code order by units_sold desc) pds_rank
        from (
            sel m.deal_uuid,
                m.country_code,
                pds.pds,
                booked_bt,
                prepurchase_bookings,
                sum(fgt.transaction_qty) units_sold
            from sh_bt_deals m
            join sh_fgt_ord fgt on m.deal_uuid = fgt.deal_uuid and m.country_code = fgt.country_code
            join sh_inv_pds pds on fgt.inv_product_uuid = pds.inv_product_uuid
            left join sh_booked b on fgt.order_id = b.order_id
            where fgt.action = 'authorize'
            and fgt.is_zero_amount = 0
            and fgt.is_order_canceled = 0
            and fgt.order_date >= m.bt_launch_date
            group by 1,2,3,4,5
        ) t
    ) t
    group by 1,2
) with data unique primary index (deal_uuid, country_code) on commit preserve rows;



create volatile table sh_refunds as (
    sel order_id
    from user_edwprod.fact_gbl_transactions
    where action = 'refund'
    and order_date >= '2020-01-01'
    group by 1
) with data unique primary index (order_id) on commit preserve rows;



create volatile table sh_booked_refunds as (
    sel f.deal_uuid,
        f.country_code,
        count(distinct b.order_id) booked_txns,
        count(distinct r.order_id) booked_refunds,
        cast(booked_refunds as dec(18,3)) / cast(booked_txns as dec(18,3)) booked_refund_rate
    from sh_booked b
    join (sel order_id, max(deal_uuid) deal_uuid, max(country_code) country_code from sh_fgt_ord group by 1) f on b.order_id = f.order_id
    left join sh_refunds r on b.order_id = r.order_id
    where booked_bt = 1
    group by 1,2
) with data unique primary index (deal_uuid, country_code) on commit preserve rows;


drop table sandbox.nvp_booking_status_deal_intl;
create table sandbox.nvp_booking_status_deal_intl as (
 select t_.*,
          case when units_sold_t30 > 3 and (has_availability_setup = 0 or latest_availability_not_data = 1) then 'a. availability setup/data issue'
               when units_sold_t30 > 3 and earliest_availability is null then 'b.No availability at all'
               when units_sold_t30 > 3 and num_dow_total < 1 then 'c. No capacity setup' ----has availability in the latest week since we are checking it after latest availability is verified
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
          case when mat.metal_at_close in ('Silver', 'Gold', 'Platinum') then 'S+' else 'B-' end metal_category
      from 
      (sel d.*,
              u.top_pds,
              u.units_sold as units_sold_t30,
              u.units_sold_booked_bt as units_sold_bt_t30,
              u.units_prepurchase_booked as units_prepurchase_booked_t30,
              cast(u.units_prepurchase_booked as dec(18,3)) / cast(nullifzero(u.units_sold) as dec(18,3)) pct_units_booked_pre,
              cast(u.units_sold_booked_bt as dec(18,3)) / cast(nullifzero(u.units_sold) as dec(18,3)) pct_units_booked,
              cast(bdd.avg_booking_delay as dec(18,3)) avg_booking_delay,
              r.booked_refund_rate
          from sh_bt_deals d
          left join sandbox.sh_dream_booking_delay_deal_intl bdd on d.deal_uuid = bdd.deal_uuid and d.country_code = bdd.country
          left join sh_units u on d.deal_uuid = u.deal_uuid and d.country_code = u.country_code
          left join sh_booked_refunds r on d.deal_uuid = r.deal_uuid and d.country_code = r.country_code
      ) as t_
      left join (
         sel deal_uuid,
             max(lower(sda.merchant_seg_at_closed_won)) metal_at_close
         from user_edwprod.sf_opportunity_1 o1
         join user_edwprod.sf_opportunity_2 o2 on o1.id = o2.id
         join user_edwprod.sf_account sfa on o1.accountid = sfa.id
         join dwh_base_sec_view.sf_deal_attribute sda on o1.deal_attribute = sda.id
         group by 1
        )  mat on t_.deal_uuid = mat.deal_uuid
  ) with data unique primary index (deal_uuid,country_code);

grant sel on sandbox.nvp_booking_status_deal_intl to public;



drop table sandbox.nvp_booking_status_account_intl;
create table sandbox.nvp_booking_status_account_intl as (
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
           case when mat.metal_at_close in ('Silver', 'Gold', 'Platinum') then 'S+' else 'B-' end metal_category
      from (
          sel d.account_id,
              d.country_code,
              max(d.account_name) account_name,
              max(d.account_owner) rep_name,
              min(d.bt_launch_date) bt_launch_date,
              max(d.deal_uuid) sample_deal_uuid,
              max(d.has_gcal) has_gcal,
              max(d.division) division,
              count(distinct d.deal_uuid) n_deals_bookable,
              max(u.top_pds) pds,
              max(grt_l2_cat_name) l2,
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
              max(covid_paused) covid_paused, 
              max(slot_needs_extension) slot_needs_extension
          from sh_bt_deals d
          left join sandbox.sh_dream_booking_delay_deal_intl bdd on d.deal_uuid = bdd.deal_uuid and d.country_code = bdd.country
          left join sh_units u on d.deal_uuid = u.deal_uuid and d.country_code = u.country_code
          left join sh_booked_refunds r on d.deal_uuid = r.deal_uuid and d.country_code = r.country_code
          group by 1,2
      ) t
      left join (
         sel o1.accountid account_id,
             max(lower(sda.merchant_seg_at_closed_won)) metal_at_close
         from user_edwprod.sf_opportunity_1 o1
         join user_edwprod.sf_opportunity_2 o2 on o1.id = o2.id
         join user_edwprod.sf_account sfa on o1.accountid = sfa.id
         join dwh_base_sec_view.sf_deal_attribute sda on o1.deal_attribute = sda.id
         group by 1
        )  mat on t.account_id = mat.account_id
  ) with data unique primary index (account_id, country_code);

grant sel on sandbox.nvp_booking_status_account_intl to public;




select * from sandbox.nvp_booking_status_account_intl;
select * from sandbox.nvp_booking_status_deal_intl;

select * from sandbox.nvp_future_avail where deal_uuid = '467cb6ed-3388-bd6a-eae5-74b17f3af9bb';

select * from (
select 
    a.*, 
    case when cov.product_uuid is not null then 1 else 0 end cov_pause1, 
    cov.end_date, 
    cov.start_date, 
    case when cov.start_date <= current_date - 3 and  current_date - 3 <= cov.end_date then 1 else 0 end cov_pause2, 
    case when (has_availability_setup = 0  or latest_availability_not_data  = 1) then 1 else 0 end availability_issue,
    case when cov.start_date > current_date - 10 then 1 else 0 end availability_data_older_than_deals
from
    sh_bt_deals as a
left join np_covid_closure_temp cov 
    on a.deal_uuid = cov.product_uuid) as fin where cov_pause2 = 1 and availability_issue = 0 and availability_data_older_than_deals = 0;
   
   
select availability_issue, 
       cov_pause2, 
       count(distinct deal_uuid), 
       count(distinct case when availability_data_older_than_deals = 0 and end_date <= current_date + 10 then deal_uuid end) from (
select 
    a.*, 
    case when cov.product_uuid is not null then 1 else 0 end cov_pause1, 
    cov.end_date, 
    cov.start_date, 
    case when cov.start_date <= current_date - 3 and  current_date - 3 <= cov.end_date then 1 else 0 end cov_pause2, 
    case when (has_availability_setup = 0  or latest_availability_not_data  = 1) then 1 else 0 end availability_issue, 
    case when current_date - 10 < cov.start_date then 1 else 0 end availability_data_older_than_deals
from
    sh_bt_deals as a
left join np_covid_closure_temp cov 
    on a.deal_uuid = cov.product_uuid) as fin  group by 1,2;
   
   
   
   
