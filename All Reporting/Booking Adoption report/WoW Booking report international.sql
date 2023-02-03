drop table sandbox.sh_bt_deals;
drop table sh_fgt_ord;
drop table sh_booked;
drop table sh_inv_pds;
drop table sh_units;
drop table sh_refunds;
drop table sh_booked_refunds;


drop table sandbox.sh_bt_deals_intl_wow;
create table sandbox.sh_bt_deals_intl_wow as (
    sel
        a.load_week,
        a.deal_uuid,
        gdl.grt_l2_cat_name,
        gdl.country_code,
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
        case when dal.num_dow_total is not null then 1 else 0 end availability_data_ok
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
             and trunc(cast(load_date as date),'iw')+6 >= current_date - 30
             and trunc(cast(load_date as date),'iw')+6 = cast(load_date as date)
        group by 1,2
        ) as a
    join user_edwprod.dim_gbl_deal_lob gdl on a.deal_uuid = gdl.deal_id
    left join
       (sel deal_uuid,
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
          country,
          load_week,
          num_dow, 
          num_dow_total
          from sandbox.nvp_weekstart_avail
          where country <> 'US'
    ) dal on a.deal_uuid = dal.deal_uuid and a.load_week + 7 = dal.load_week and gdl.country_code = dal.country
    where
    gdl.country_code <> 'US'
    and gdl.grt_l1_cat_name = 'L1 - Local'
) with data unique primary index (deal_uuid, load_week, country_code);


---order is definitely looked in the past and availability in future

drop table sandbox.sh_fgt_ord_intl_wow;
create table sandbox.sh_fgt_ord_intl_wow as (
    sel fgt.*, 
        cntry.country_iso_code_2 country_code
    from user_edwprod.fact_gbl_transactions fgt
    join dwh_base_sec_view.country cntry on fgt.country_id = cntry.country_id
    join (select deal_uuid, load_week , country_code ,min(bt_launch_date) bt_launch_date from sandbox.sh_bt_deals_intl_wow group by 1,2,3) d 
          on fgt.deal_uuid = d.deal_uuid 
          and cntry.country_iso_code_2 = d.country_code 
          and trunc(fgt.order_date,'iw')+6 = d.load_week
    where order_date >= d.bt_launch_date
) with data primary index (order_id, action);


create volatile table sh_booked as (
    sel f.order_id,
        max(case when bn.voucher_code is not null and bn.deal_uuid = f.deal_uuid then 1 else 0 end) booked_bt, 
        max(case when bn.booked_by = 'api' and substr(bn.created_at,1,10) = substr(cast(v.created_at as varchar(50)),1,10) and bn.deal_uuid = f.deal_uuid then 1 else 0 end) prepurchase_bookings
    from sandbox.sh_fgt_ord_intl_wow f
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
        load_week,
        deal_uuid,
        country_code,
        min(case when pds_rank = 1 then pds end) top_pds,
        sum(units_sold) units_sold,
        sum(case when booked_bt = 1 then units_sold else 0 end) units_sold_booked_bt, 
        sum(case when prepurchase_bookings = 1 then units_sold else 0 end) units_prepurchase_booked
    from (
        sel t.*,
            row_number() over (partition by load_week, deal_uuid order by units_sold desc) pds_rank
        from (
            sel m.deal_uuid,
                m.country_code,
                m.load_week,
                pds.pds,
                booked_bt,
                prepurchase_bookings, 
                sum(fgt.transaction_qty) units_sold
            from sandbox.sh_bt_deals_intl_wow m
            join sandbox.sh_fgt_ord_intl_wow fgt on m.deal_uuid = fgt.deal_uuid and m.load_week = trunc(fgt.order_date,'iw')+6 and m.country_code = fgt.country_code
            join sh_inv_pds pds on fgt.inv_product_uuid = pds.inv_product_uuid
            left join sh_booked b on fgt.order_id = b.order_id
            where fgt.action = 'authorize'
            and fgt.is_zero_amount = 0
            and fgt.is_order_canceled = 0
            and fgt.order_date >= m.bt_launch_date
            group by 1,2,3,4,5,6
        ) t
    ) t
    group by 1,2,3
) with data unique primary index (deal_uuid, load_week, country_code) on commit preserve rows;



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
        f.order_week,
        count(distinct b.order_id) booked_txns,
        count(distinct r.order_id) booked_refunds,
        cast(booked_refunds as dec(18,3)) / cast(booked_txns as dec(18,3)) booked_refund_rate
    from sh_booked b
    join (sel order_id, 
              trunc(order_date,'iw')+6 order_week, 
              max(deal_uuid) deal_uuid, 
              max(country_code) country_code 
          from sandbox.sh_fgt_ord_intl_wow group by 1,2) f 
          on b.order_id = f.order_id
    left join sh_refunds r on b.order_id = r.order_id
    where booked_bt = 1
    group by 1,2,3
) with data unique primary index (deal_uuid, country_code ,order_week) on commit preserve rows;



drop table sandbox.nvp_booking_status_deal2_intl;
create table sandbox.nvp_booking_status_deal2_intl as
(sel t.*,
        case when availability_data_ok  = 0 then 'a. availability setup/data issue'
             when num_dow_total < 1 then 'b. No capacity setup'
             when num_dow < 1 then 'c. 0 days available - blocked/paused'
             when avg_booking_delay > 24 then 'd. booking leadtime >24 hours'
             when num_dow < 4 then 'e. less than 4 days available next week'
             else 'f. No availability issue'
         end availability_cohort, 
        case when availability_data_ok  = 0 then 'Availability - data problem'
             when num_dow_total < 1 then 'Availability - None'
             when num_dow < 1 then 'Availability - None'
             when avg_booking_delay > 24 then 'Long Lead time'
             when num_dow < 4 then 'Availability - Low'
             else 'No availability issue'
         end availability_cohort2, 
         mat.metal_at_close, 
         case when mat.metal_at_close in ('Silver', 'Gold', 'Platinum') then 'S+' else 'B-' end metal_category,  
         case when units_sold_7w >3 then 1 else 0 end more_than_3_units_sold
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
        from sandbox.sh_bt_deals_intl_wow d
        left join sandbox.sh_dream_booking_delay_deal_intl bdd on d.deal_uuid = bdd.deal_uuid and d.country_code = bdd.country
        left join sh_units u on d.deal_uuid = u.deal_uuid and d.load_week = u.load_week and d.country_code = u.country_code
        left join sh_booked_refunds r on d.deal_uuid = r.deal_uuid and d.load_week = r.order_week and d.country_code = r.country_code
    ) t
    left join (
         sel deal_uuid,
             max(lower(sda.merchant_seg_at_closed_won)) metal_at_close
         from user_edwprod.sf_opportunity_1 o1
         join user_edwprod.sf_opportunity_2 o2 on o1.id = o2.id
         join user_edwprod.sf_account sfa on o1.accountid = sfa.id
         join dwh_base_sec_view.sf_deal_attribute sda on o1.deal_attribute = sda.id
         group by 1
        )  mat on t.deal_uuid = mat.deal_uuid
) with data unique primary index (deal_uuid, load_week, country_code);

grant sel on sandbox.nvp_booking_status_deal2_intl to public;



drop table sandbox.nvp_booking_status_account2_intl;
create table sandbox.nvp_booking_status_account2_intl as (
    sel t.*,
        case when availability_data_ok  = 0 then 'a. availability setup/data issue'
             when num_dow_total < 1 then 'b. No capacity setup'
             when num_dow < 1 then 'c. 0 days available - blocked/paused'
             when avg_booking_delay > 24 then 'd. booking leadtime >24 hours'
             when num_dow < 4 then 'e. less than 4 days available next week'
             else 'f. No availability issue'
         end action_cohort, 
        case when availability_data_ok  = 0 then 'Availability - data problem'
             when num_dow_total < 1 then 'Availability - None'
             when num_dow < 1 then 'Availability - None'
             when avg_booking_delay > 24 then 'Long Lead time'
             when num_dow < 4 then 'Availability - Low'
             else 'No availability issue'
         end action_cohort2, 
         mat.metal_at_close, 
         case when mat.metal_at_close in ('Silver', 'Gold', 'Platinum') then 'S+' else 'B-' end metal_category,
         case when units_sold_7w >3 then 1 else 0 end more_than_3_units_sold
    from (
        sel d.account_id,
            d.load_week,
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
            sum(u.units_sold) as units_sold_7w,
            sum(u.units_sold_booked_bt) as units_sold_booked_7bt,
            sum(u.units_prepurchase_booked) as units_prepurchase_booked_7bt,
            cast(units_prepurchase_booked_7bt as dec(18,3)) / cast(nullifzero(units_sold_7w) as dec(18,3)) pct_units_booked_pre,
            cast(units_sold_booked_7bt as dec(18,3)) / cast(nullifzero(units_sold_7w) as dec(18,3)) pct_units_booked,
            cast(sum(r.booked_refunds) as dec(18,3)) / cast(nullifzero(sum(r.booked_txns)) as dec(18,3)) booked_refund_rate,
            cast(avg(bdd.avg_booking_delay) as dec(18,3)) avg_booking_delay,
            max(d.num_dow) num_dow,
            max(d.num_dow_total) num_dow_total,
            max(availability_data_ok) availability_data_ok
        from sandbox.sh_bt_deals_intl_wow d
        left join sandbox.sh_dream_booking_delay_deal_intl bdd on d.deal_uuid = bdd.deal_uuid and d.country_code = bdd.country
        left join sh_units u on d.deal_uuid = u.deal_uuid and d.load_week = u.load_week and d.country_code = u.country_code
        left join sh_booked_refunds r on d.deal_uuid = r.deal_uuid and d.load_week = r.order_week and d.country_code = r.country_code
        group by 1,2,3
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
) with data unique primary index (account_id, load_week, country_code);

grant sel on sandbox.nvp_booking_status_account2_intl to public;



select * from sandbox.nvp_booking_status_account2_intl;
select * from sandbox.nvp_booking_status_deal2_intl;
