
-----------------------------END OF WEEK DATE
drop table sh_bt_deals;
drop table sh_fgt_ord;
drop table sh_booked;
drop table sh_inv_pds;
drop table sh_units;
drop table sh_refunds;
drop table sh_booked_refunds;


create volatile table sh_bt_deals as (
    sel
        a.load_week,
        a.deal_uuid,
        sf.opportunity_id,
        sf.account_name,
        sf.account_owner,
        sf.account_id,
        cast(l.bt_launch_date as date) bt_launch_date,
        sf.division,
        a.has_gcal,
        dal.num_dow,
        dal.num_dow_total,
        dal.load_week ld_week,
        case when dal.num_dow_total is not null then 1 else 0 end availability_data_ok, ---this could be a setup issue or it could be that latest data is not available since those deals wont have anything linked for the next week
        new_avail.earliest_availability,
        case when top_a.account_id is not null then 1 else 0 end top_account
    from
        (select 
           deal_uuid,
           trunc(cast(load_date as date),'iw')+6 as load_week,
           max(has_gcal) has_gcal
        from sandbox.sh_bt_active_deals_log_v4
        where product_is_active_flag = 1
             and partner_inactive_flag = 0
             and is_bookable = 1
             and sold_out = 'false'
             and trunc(cast(load_date as date),'iw')+6 >= current_date - 35
             and trunc(cast(load_date as date),'iw')+6 = cast(load_date as date)
        group by 1,2
        ) as a
    join user_edwprod.dim_gbl_deal_lob gdl on a.deal_uuid = gdl.deal_id
    left join
       (sel deal_uuid,
            min(load_date) bt_launch_date
        from sandbox.sh_bt_active_deals_log
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
          num_dow, 
          num_dow_total
          from sandbox.nvp_weekstart_avail
          where country = 'US'
    ) dal on a.deal_uuid = dal.deal_uuid and a.load_week + 7 = dal.load_week
    left join sandbox.nvp_future_avail as new_avail on a.deal_uuid = new_avail.deal_uuid and new_avail.country = 'US'
    left join sandbox.avb_top_accts_mar2021 as top_a on sf.account_id = top_a.account_id
    where
        gdl.country_code = 'US'
    and gdl.grt_l1_cat_name = 'L1 - Local'
    and gdl.grt_l2_cat_description = 'HBW'
) with data unique primary index (deal_uuid, load_week) on commit preserve rows;



create volatile table sh_fgt_ord as (
    sel fgt.*
    from user_edwprod.fact_gbl_transactions fgt
    join (select deal_uuid, load_week ,min(bt_launch_date) bt_launch_date from sh_bt_deals group by 1,2) d
          on fgt.deal_uuid = d.deal_uuid and trunc(fgt.order_date,'iw')+6 = d.load_week
    where order_date >= d.bt_launch_date
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
        load_week,
        deal_uuid,
        min(case when pds_rank = 1 then pds end) top_pds,
        sum(units_sold) units_sold,
        sum(case when booked_bt = 1 then units_sold else 0 end) units_sold_booked_bt, 
        sum(case when prepurchase_bookings = 1 then units_sold else 0 end) units_prepurchase_booked
    from (
        sel t.*,
            row_number() over (partition by load_week, deal_uuid order by units_sold desc) pds_rank
        from (
            sel m.deal_uuid,
                m.load_week,
                pds.pds,
                booked_bt,
                prepurchase_bookings, 
                sum(fgt.transaction_qty) units_sold
            from sh_bt_deals m
            join sh_fgt_ord fgt on m.deal_uuid = fgt.deal_uuid and m.load_week = trunc(fgt.order_date,'iw')+6
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
) with data unique primary index (deal_uuid, load_week) on commit preserve rows;


create volatile table sh_refunds as (
    sel order_id
    from user_edwprod.fact_gbl_transactions
    where action = 'refund'
    and order_date >= '2020-01-01'
    group by 1
) with data unique primary index (order_id) on commit preserve rows;


create volatile table sh_booked_refunds as (
    sel f.deal_uuid,
        f.order_week,
        count(distinct b.order_id) booked_txns,
        count(distinct r.order_id) booked_refunds,
        cast(booked_refunds as dec(18,3)) / cast(booked_txns as dec(18,3)) booked_refund_rate
    from sh_booked b
    join (sel order_id, trunc(order_date,'iw')+6 order_week, max(deal_uuid) deal_uuid from sh_fgt_ord group by 1,2) f on b.order_id = f.order_id
    left join sh_refunds r on b.order_id = r.order_id
    where booked_bt = 1
    group by 1,2
) with data unique primary index (deal_uuid, order_week) on commit preserve rows;


drop table sandbox.nvp_hbw_booking_status_deal2;
create table sandbox.nvp_hbw_booking_status_deal2 as
(sel t.*,
        case
             when availability_data_ok  = 0 then 'a. availability setup/data issue'
             when num_dow_total < 1 then 'b. No capacity setup'
             when num_dow < 1 then 'c. 0 days available - blocked/paused'
             when avg_booking_delay > 24 then 'd. booking leadtime >24 hours'
             when num_dow < 4 then 'e. less than 4 days available next week'
             else 'f. No availability issue'
         end availability_cohort, 
        case
             when availability_data_ok  = 0 then 'Availability - data problem'
             when num_dow_total < 1 then 'Availability - None'
             when num_dow < 1 then 'Availability - None'
             when avg_booking_delay > 24 then 'Long Lead time'
             when num_dow < 4 then 'Availability - Low'
             else 'No availability issue'
         end availability_cohort2, 
         mat.metal_at_close, 
         case when mat.metal_at_close in ('Silver', 'Gold', 'Platinum') then 'S+' else 'B-' end metal_category, 
         case when dream.account_id is not null then 'Yes' else 'No' end is_dream_account, 
         case when units_sold_7w >3 then 1 else 0 end more_than_3_units_sold, 
         case when (top_account = 1 or is_dream_account = 'Yes' or has_gcal = 1) then 1 else 0 end focus_merchant
    from (
        sel d.*,
            u.top_pds,
            u.units_sold as units_sold_7w,
            u.units_sold_booked_bt as units_sold_booked_7bt,
            u.units_prepurchase_booked as units_prepurchase_booked_7bt,
            cast(u.units_prepurchase_booked as dec(18,3)) / cast(nullifzero(u.units_sold) as dec(18,3)) pct_units_booked_pre,
            cast(u.units_sold_booked_bt as dec(18,3)) / cast(nullifzero(u.units_sold) as dec(18,3)) pct_units_booked,
            cast(bdd.avg_booking_delay as dec(18,3)) avg_booking_delay,
            r.booked_refund_rate
        from sh_bt_deals d
        left join sandbox.sh_dream_booking_delay_deal bdd on d.deal_uuid = bdd.deal_uuid
        left join sh_units u on d.deal_uuid = u.deal_uuid and d.load_week = u.load_week
        left join sh_booked_refunds r on d.deal_uuid = r.deal_uuid and d.load_week = r.order_week
    ) t
    left join sandbox.rev_mgmt_deal_attributes mat on t.deal_uuid = mat.deal_id
    left join sandbox.ar_hbw_dream_merchants dream on t.account_id = dream.account_id
) with data unique primary index (deal_uuid, load_week);

grant sel on sandbox.nvp_hbw_booking_status_deal2 to public;




drop table sandbox.nvp_hbw_booking_status_account2;
create table sandbox.nvp_hbw_booking_status_account2 as (
    sel t.*,
        case 
             when availability_data_ok  = 0 then 'a. availability setup/data issue'
             when num_dow_total < 1 then 'b. No capacity setup'
             when num_dow < 1 then 'c. 0 days available - blocked/paused'
             when avg_booking_delay > 24 then 'd. booking leadtime >24 hours'
             when num_dow < 4 then 'e. less than 4 days available next week'
             else 'f. No availability issue'
         end availability_cohort,  
         case 
             when availability_data_ok  = 0 then 'Availability - data problem'
             when num_dow_total < 1 then 'Availability - None'
             when num_dow < 1 then 'Availability - None'
             when avg_booking_delay > 24 then 'Long Lead time'
             when num_dow < 4 then 'Availability - Low'
             else 'No availability issue'
         end availability_cohort2, 
         mat.metal_at_close, 
         case when mat.metal_at_close in ('Silver', 'Gold', 'Platinum') then 'S+' else 'B-' end metal_category, 
         case when dream.account_id is not null then 'Yes' else 'No' end is_dream_account, 
         case when units_sold_7w >3 then 1 else 0 end more_than_3_units_sold, 
         case when (top_account = 1 or is_dream_account = 'Yes' or has_gcal = 1) then 1 else 0 end focus_merchant
    from (
        sel d.account_id,
            d.load_week,
            max(d.account_name) account_name,
            max(d.account_owner) rep_name,
            min(d.bt_launch_date) bt_launch_date,
            max(d.deal_uuid) sample_deal_uuid,
            max(d.has_gcal) has_gcal,
            max(d.division) division,
            count(distinct d.deal_uuid) n_deals_bookable,
            max(u.top_pds) pds,
            sum(u.units_sold) as units_sold_7w,
            sum(u.units_sold_booked_bt) as units_sold_booked_7bt,
            sum(u.units_prepurchase_booked) as units_prepurchase_booked_7bt,
            cast(units_prepurchase_booked_7bt as dec(18,3)) / cast(nullifzero(units_sold_7w) as dec(18,3)) pct_units_booked_pre,
            cast(units_sold_booked_7bt as dec(18,3)) / cast(nullifzero(units_sold_7w) as dec(18,3)) pct_units_booked,
            cast(sum(r.booked_refunds) as dec(18,3)) / cast(nullifzero(sum(r.booked_txns)) as dec(18,3)) booked_refund_rate,
            cast(avg(bdd.avg_booking_delay) as dec(18,3)) avg_booking_delay,
            max(d.num_dow) num_dow,
            max(d.num_dow_total) num_dow_total,
            max(availability_data_ok) availability_data_ok,
            min(earliest_availability) earliest_availability, 
            max(top_account) top_account
        from sh_bt_deals d
        left join sandbox.sh_dream_booking_delay_deal bdd on d.deal_uuid = bdd.deal_uuid
        left join sh_units u on d.deal_uuid = u.deal_uuid and d.load_week = u.load_week
        left join sh_booked_refunds r on d.deal_uuid = r.deal_uuid and d.load_week = r.order_week
        group by 1,2
    ) t
    left join (select account_id, max(metal_at_close) metal_at_close from sandbox.rev_mgmt_deal_attributes group by 1) mat on t.account_id = mat.account_id
    left join sandbox.ar_hbw_dream_merchants dream on t.account_id = dream.account_id
    ) with data unique primary index (account_id, load_week);

grant sel on sandbox.nvp_hbw_booking_status_account2 to public;


select * from sandbox.nvp_hbw_booking_status_deal2;
select * from sandbox.nvp_hbw_booking_status_account2;
 -----restriction on number of weeks we are pulling


drop table sh_bt_deals;

create volatile table sh_bt_deals as (
    sel
        a.load_week,
        a.deal_uuid,
        sf.opportunity_id,
        sf.account_name,
        sf.account_owner,
        sf.account_id,
        cast(l.bt_launch_date as date) bt_launch_date,
        sf.division,
        a.has_gcal,
        dal.num_dow,
        dal.num_dow_total,
        dal.load_week ld_week,
        case when dal.num_dow_total is null then 1 else 0 end availability_issue
    from
        (select  
           deal_uuid,
           trunc(cast(load_date as date),'iw')+6 as load_week,
           max(has_gcal) has_gcal
        from sandbox.sh_bt_active_deals_log_v4
        where product_is_active_flag = 1
             and partner_inactive_flag = 0
             and trunc(cast(load_date as date),'iw')+6 >= current_date - 45
        group by 1,2
        ) as a
    join user_edwprod.dim_gbl_deal_lob gdl on a.deal_uuid = gdl.deal_id
    left join
       (sel deal_uuid,
            min(load_date) bt_launch_date
        from sandbox.sh_bt_active_deals_log
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
          num_dow, 
          num_dow_total
          from sandbox.nvp_weekwise_avail
    ) dal on a.deal_uuid = dal.deal_uuid and a.load_week + 7 = dal.load_week
    where
    gdl.country_code = 'US'
    and gdl.grt_l1_cat_name = 'L1 - Local'
    and gdl.grt_l2_cat_description = 'HBW'
) with data unique primary index (deal_uuid, load_week) on commit preserve rows;



create volatile table sh_fgt_ord as (
    sel fgt.*
    from user_edwprod.fact_gbl_transactions fgt
    join (select deal_uuid, load_week ,min(bt_launch_date) bt_launch_date from sh_bt_deals group by 1,2) d
          on fgt.deal_uuid = d.deal_uuid and trunc(fgt.order_date,'iw')+6 = d.load_week
    where order_date >= d.bt_launch_date
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
        load_week,
        deal_uuid,
        min(case when pds_rank = 1 then pds end) top_pds,
        sum(units_sold) units_sold,
        sum(case when booked_bt = 1 then units_sold else 0 end) units_sold_booked_bt, 
        sum(case when prepurchase_bookings = 1 then units_sold else 0 end) units_prepurchase_booked
    from (
        sel t.*,
            row_number() over (partition by load_week, deal_uuid order by units_sold desc) pds_rank
        from (
            sel m.deal_uuid,
                m.load_week,
                pds.pds,
                booked_bt,
                prepurchase_bookings, 
                sum(fgt.transaction_qty) units_sold
            from sh_bt_deals m
            join sh_fgt_ord fgt on m.deal_uuid = fgt.deal_uuid and m.load_week = trunc(fgt.order_date,'iw')+6
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
) with data unique primary index (deal_uuid, load_week) on commit preserve rows;


create volatile table sh_refunds as (
    sel order_id
    from user_edwprod.fact_gbl_transactions
    where action = 'refund'
    and order_date >= '2020-01-01'
    group by 1
) with data unique primary index (order_id) on commit preserve rows;


create volatile table sh_booked_refunds as (
    sel f.deal_uuid,
        f.order_week,
        count(distinct b.order_id) booked_txns,
        count(distinct r.order_id) booked_refunds,
        cast(booked_refunds as dec(18,3)) / cast(booked_txns as dec(18,3)) booked_refund_rate
    from sh_booked b
    join (sel order_id, trunc(order_date,'iw')+6 order_week, max(deal_uuid) deal_uuid from sh_fgt_ord group by 1,2) f on b.order_id = f.order_id
    left join sh_refunds r on b.order_id = r.order_id
    where booked_bt = 1
    group by 1,2
) with data unique primary index (deal_uuid, order_week) on commit preserve rows;


drop table sandbox.nvp_hbw_booking_status_deal2;
create table sandbox.nvp_hbw_booking_status_deal2 as
(sel t.*,
        case
             when availability_issue  = 1 then 'a. availability setup/data issue'
             when num_dow_total < 1 then 'b. 0 capacity for the deal'
             when num_dow < 1 then 'c. 0 days availability next week'
             when avg_booking_delay > 24 then 'd. booking leadtime >24 hours'
             when num_dow < 4 then 'e. less than 4 days available next week'
             else 'f. No availability issue'
         end action_cohort, 
        case
             when availability_issue  = 1 then 'Availability - None'
             when num_dow_total < 1 then 'Availability - None'
             when num_dow < 1 then 'Availability - filled'
             when avg_booking_delay > 24 then 'Long Lead time'
             when num_dow < 4 then 'Availability - Low'
             else 'No availability issue'
         end action_cohort2, 
         mat.metal_at_close, 
         case when mat.metal_at_close in ('Silver', 'Gold', 'Platinum') then 'S+' else 'B-' end metal_category, 
         case when dream.account_id is not null then 'Yes' else 'No' end is_dream_account, 
         case when units_sold_7w >3 then 1 else 0 end more_than_3_units_sold, 
         case when (metal_category = 'S+' or is_dream_account = 'Yes' or has_gcal = 1) then 1 else 0 end focus_merchant
    from (
        sel d.*,
            u.top_pds,
            u.units_sold as units_sold_7w,
            u.units_sold_booked_bt as units_sold_booked_7bt,
            u.units_prepurchase_booked as units_prepurchase_booked_7bt,
            cast(u.units_prepurchase_booked as dec(18,3)) / cast(nullifzero(u.units_sold) as dec(18,3)) pct_units_booked_pre,
            cast(u.units_sold_booked_bt as dec(18,3)) / cast(nullifzero(u.units_sold) as dec(18,3)) pct_units_booked,
            cast(bdd.avg_booking_delay as dec(18,3)) avg_booking_delay,
            r.booked_refund_rate
        from sh_bt_deals d
        left join sandbox.sh_dream_booking_delay_deal bdd on d.deal_uuid = bdd.deal_uuid
        left join sh_units u on d.deal_uuid = u.deal_uuid and d.load_week = u.load_week
        left join sh_booked_refunds r on d.deal_uuid = r.deal_uuid and d.load_week = r.order_week
    ) t
    left join sandbox.rev_mgmt_deal_attributes mat on t.deal_uuid = mat.deal_id
    left join sandbox.ar_hbw_dream_merchants dream on t.account_id = dream.account_id
) with data unique primary index (deal_uuid, load_week);

grant sel on sandbox.nvp_hbw_booking_status_deal2 to public;




drop table sandbox.nvp_hbw_booking_status_account2;
create table sandbox.nvp_hbw_booking_status_account2 as (
    sel t.*,
        case 
             when availability_issue  = 1 then 'a. availability setup/data issue'
             when num_dow_total < 1 then 'b. 0 capacity for the deal'
             when num_dow < 1 then 'c. 0 days availability next week'
             when avg_booking_delay > 24 then 'd. booking leadtime >24 hours'
             when num_dow < 4 then 'e. less than 4 days available next week'
             else 'f. No availability issue'
         end action_cohort,  
         case 
             when availability_issue  = 1 then 'Availability - None'
             when num_dow_total < 1 then 'Availability - None'
             when num_dow < 1 then 'Availability - filled'
             when avg_booking_delay > 24 then 'Long Lead time'
             when num_dow < 4 then 'Availability - Low'
             else 'No availability issue'
         end action_cohort2, 
         mat.metal_at_close, 
         case when mat.metal_at_close in ('Silver', 'Gold', 'Platinum') then 'S+' else 'B-' end metal_category, 
         case when dream.account_id is not null then 'Yes' else 'No' end is_dream_account, 
         case when units_sold_7w >3 then 1 else 0 end more_than_3_units_sold, 
         case when (metal_category = 'S+' or is_dream_account = 'Yes' or has_gcal = 1) then 1 else 0 end focus_merchant
    from (
        sel d.account_id,
            d.load_week,
            max(d.account_name) account_name,
            max(d.account_owner) rep_name,
            min(d.bt_launch_date) bt_launch_date,
            max(d.deal_uuid) sample_deal_uuid,
            max(d.has_gcal) has_gcal,
            max(d.division) division,
            count(distinct d.deal_uuid) n_deals_bookable,
            max(u.top_pds) pds,
            sum(u.units_sold) as units_sold_7w,
            sum(u.units_sold_booked_bt) as units_sold_booked_7bt,
            sum(u.units_prepurchase_booked) as units_prepurchase_booked_7bt,
            cast(units_prepurchase_booked_7bt as dec(18,3)) / cast(nullifzero(units_sold_7w) as dec(18,3)) pct_units_booked_pre,
            cast(units_sold_booked_7bt as dec(18,3)) / cast(nullifzero(units_sold_7w) as dec(18,3)) pct_units_booked,
            cast(sum(r.booked_refunds) as dec(18,3)) / cast(nullifzero(sum(r.booked_txns)) as dec(18,3)) booked_refund_rate,
            cast(avg(bdd.avg_booking_delay) as dec(18,3)) avg_booking_delay,
            max(d.num_dow) num_dow,
            max(d.num_dow_total) num_dow_total,
            max(availability_issue) availability_issue
        from sh_bt_deals d
        left join sandbox.sh_dream_booking_delay_deal bdd on d.deal_uuid = bdd.deal_uuid
        left join sh_units u on d.deal_uuid = u.deal_uuid and d.load_week = u.load_week
        left join sh_booked_refunds r on d.deal_uuid = r.deal_uuid and d.load_week = r.order_week
        group by 1,2
    ) t
    left join (select account_id, max(metal_at_close) metal_at_close from sandbox.rev_mgmt_deal_attributes group by 1) mat on t.account_id = mat.account_id
    left join sandbox.ar_hbw_dream_merchants dream on t.account_id = dream.account_id
    ) with data unique primary index (account_id, load_week);

grant sel on sandbox.nvp_hbw_booking_status_account2 to public;




select * from sandbox.nvp_hbw_booking_status_deal2;
select * from sandbox.nvp_hbw_booking_status_account2 where units_sold_7w >3




--------






select 
   a.*
from
(select 
     *
from sandbox.nvp_hbw_booking_status_deal2 
where 
     load_week = trunc(current_date - 7, 'iw')+6
      and deal_uuid in ('5dc8f9bc-0b85-4a96-83a9-de27d0a51b03', 'dd3de0ff-0b5f-4e21-b6e0-57fe1247da38')) as a 
left join 
(select 
     *
from sandbox.nvp_hbw_booking_status_deal2 
where units_sold_7w >3
      and load_week = trunc(current_date - 14, 'iw')+6
      and action_cohort2 = 'Availability - None'
) as b on a.deal_uuid = b.deal_uuid


select * from sandbox.sh_bt_active_deals_log 
where deal_uuid = '5dc8f9bc-0b85-4a96-83a9-de27d0a51b03'
order by load_date desc;

-----------------------------

/*
 * 
 * case when units_sold_7w >3 and availability_issue  = 1 then 'a. availability setup/data issue'
             when units_sold_7w >3 and num_dow_total < 1 then 'b. 0 capacity for the deal'
             when units_sold_7w >3 and num_dow < 1 then 'c. 0 days availability next week'
             when units_sold_7w >3 and avg_booking_delay > 24 then 'd. booking leadtime >24 hours'
             when units_sold_7w >3 and num_dow < 4 then 'e. less than 4 days available next week'
             when units_sold_7w >3 and pct_units_booked_pre < .2 then 'f. less than 20% units prepurchase booked'
             when units_sold_7w >3 and booked_refund_rate > .25 then 'g. high refunds on booked vouchers'
             when units_sold_7w >3 then 'h. no action needed'
             when (units_sold_7w is null or units_sold_7w = 0) then 'x. 0 units sold'
             when units_sold_7w <= 2 then 'y. <= 2 units sold'
             when units_sold_7w <= 3 then 'z. 3 units sold'
         end action_cohort, 
        case when units_sold_7w >3 and availability_issue  = 1 then 'Availability - None'
             when units_sold_7w >3 and num_dow_total < 1 then 'Availability - None'
             when units_sold_7w >3 and num_dow < 1 then 'Availability - filled'
             when units_sold_7w >3 and avg_booking_delay > 24 then 'Long Lead time'
             when units_sold_7w >3 and num_dow < 4 then 'Availability - Low'
             when units_sold_7w >3 and pct_units_booked < .2 then 'Low Prepurchase Bookings'
             when units_sold_7w >3 and booked_refund_rate > .25 then 'High Refunds'
             when units_sold_7w >3 then 'No Action'
             when (units_sold_7w is null or units_sold_7w = 0) then 'x. 0 units sold'
             when units_sold_7w <= 2 then 'y. <= 2 units sold'
             when units_sold_7w <= 3 then 'z. 3 units sold'
         end action_cohort2, 
         
         
         case when units_sold_7w >3 and availability_issue  = 1 then 'a. availability setup/data issue'
             when units_sold_7w >3 and num_dow_total < 1 then 'b. 0 capacity for the deal'
             when units_sold_7w >3 and num_dow < 1 then 'c. 0 days availability next week'
             when units_sold_7w >3 and avg_booking_delay > 24 then 'd. booking leadtime >24 hours'
             when units_sold_7w >3 and num_dow < 4 then 'e. less than 4 days available next week'
             when units_sold_7w >3 and pct_units_booked_pre < .2 then 'f. less than 20% units prepurchase booked'
             when units_sold_7w >3 and booked_refund_rate > .25 then 'g. high refunds on booked vouchers'
             when units_sold_7w >3 then 'h. no action needed'
             when (units_sold_7w is null or units_sold_7w = 0) then 'x. 0 units sold'
             when units_sold_7w <= 2 then 'y. <= 2 units sold'
             when units_sold_7w <= 3 then 'z. 3 units sold'
         end action_cohort, 
        case when units_sold_7w >3 and availability_issue  = 1 then 'Availability - None'
             when units_sold_7w >3 and num_dow_total < 1 then 'Availability - None'
             when units_sold_7w >3 and num_dow < 1 then 'Availability - filled'
             when units_sold_7w >3 and avg_booking_delay > 24 then 'Long Lead time'
             when units_sold_7w >3 and num_dow < 4 then 'Availability - Low'
             when units_sold_7w >3 and pct_units_booked_pre < .2 then 'Low Prepurchase Bookings'
             when units_sold_7w >3 and booked_refund_rate > .25 then 'High Refunds'
             when units_sold_7w >3 then 'No Action'
             when (units_sold_7w is null or units_sold_7w = 0) then 'x. 0 units sold'
             when units_sold_7w <= 2 then 'y. <= 2 units sold'
             when units_sold_7w <= 3 then 'z. 3 units sold'
         end action_cohort2,
 * 
 */

select 
   a.deal_uuid, 
   a.bt_launch_date, 
   a.units_sold_booked_7bt, 
   a.account_id, 
   a.account_name, 
   a.action_cohort,
   c.deal_uuid, 
   c.action_cohort2
from
(select 
     *
from sandbox.nvp_hbw_booking_status_deal2 
where units_sold_7w >3
      and load_week = '2021-03-07'
      and action_cohort2 = 'Availability - None') as a 
left join 
(select 
     *
from sandbox.nvp_hbw_booking_status_deal2 
where units_sold_7w >3
      and load_week = '2021-02-28'
) as c on a.deal_uuid = c.deal_uuid;



select * 
from sandbox.sh_bt_active_deals_log 
where deal_uuid = '0a93aa79-45d0-4970-bf41-c9470b303a4c' 
order by load_date desc;



/*create volatile table sh_booked_red as (
    sel f.order_id,
        sum(case when f.action = 'authorize' then f.transaction_qty end ) units,
        max(case when bn.voucher_code is not null then 1 else 0 end) booked_bt,
        max(customer_redeemed) redeemed,
        cast(min(case when customer_redeemed = 1 then updated_at end) as date) redeem_date
    from sh_fgt_ord f
    join user_gp.camp_membership_coupons cmc on f.order_id = cast(cmc.order_id as varchar(64))
    left join sandbox.sh_bt_bookings_rebuild bn on cmc.code = bn.voucher_code and cast(cmc.merchant_redemption_code as varchar(50)) = bn.security_code
    group by 1
) with data unique primary index (order_id) on commit preserve rows;


(select
   m.deal_uuid
   m.load_week,
   pds.pds,
   sum(fgt.transaction_qty) units_sold
   from sh_bt_deals m
   join sh_fgt_ord fgt on m.deal_uuid = fgt.deal_uuid and m.load_week = trunc(fgt.order_date,'iw')+6
   join sh_inv_pds pds on fgt.inv_product_uuid = pds.inv_product_uuid)
   where fgt.action = 'authorize'
   and fgt.is_zero_amount = 0
   and fgt.is_order_canceled = 0)
(select
      m.deal_uuid
      m.load_week,
      pds.pds,
      sum(fgt.transaction_qty) units_sold
  from sh_bt_deals m
  join sh_booked_red red on m.deal_uuid = red.deal_uuid and m.load_week = trunc(red.redeem_date,'iw')+6)
*/
