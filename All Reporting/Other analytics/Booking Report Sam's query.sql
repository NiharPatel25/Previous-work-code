----------update_booking_delay

select merchant_uuid, avg(a.booking_delay) avg_booking_delay
from grp_gdoop_bizops_db.sh_bt_products a
join grp_gdoop_bizops_db.sh_bt_partners b on a.partners_id = b.id
where a.country = 'US'
and b.booking_solution = 'BasicV2'
and b.inactive = 0
group by merchant_uuid




----------middle step (deleted)

create volatile table sh_dream_launch_dates as (
    sel merchant_uuid, min(cast(launch_date as date)) launch_date
    from sandbox.sh_dream_launch_dates_ld a
    join user_edwprod.dim_merchant m on a.account_id = m.salesforce_account_id
    where char_length(account_id) > 5
    group by 1
) with data unique primary index (merchant_uuid) on commit preserve rows;

create volatile table sh_dream_mercs as (
    select a.*, m.merchant_uuid
    from sandbox.avb_to_hbw_dream_accts a
    join user_edwprod.dim_merchant m on a.account_id = m.salesforce_account_id
    where all_dream = 'TRUE'
    qualify row_number() over (partition by merchant_uuid order by account_name) = 1
) with data unique primary index (merchant_uuid) on commit preserve rows;

create volatile table sh_dream_options_stg as (
    sel doe.inv_product_uuid,
        doe.product_uuid as deal_uuid,
        m.merchant_uuid,
        sf.division,
        pds.pds_cat_name pds,
        pds.grt_l2_cat_description l2,
        pds.grt_l3_cat_description l3,
        pds.grt_l4_cat_description l4,
        pds.grt_l5_cat_description l5,
        u.nat_freq_days
    from sh_dream_mercs m
    join user_edwprod.dim_offer_ext doe on m.merchant_uuid = doe.merchant_uuid
    join user_dw.v_dim_pds_grt_map pds on doe.pds_cat_id = pds.pds_cat_id
    left join sandbox.sh_hbw_nat_freq_upl u on pds.pds_cat_name = u.pds
    left join (
        select deal_uuid, max(division) division
        from user_edwprod.sf_opportunity_1 o1
        join user_edwprod.sf_opportunity_2 o2 on o1.id = o2.id
        group by 1
    ) sf on doe.product_uuid = sf.deal_uuid
) with data unique primary index (inv_product_uuid) on commit preserve rows;

create volatile table sh_dream_options as (
    sel o.*
    from (
        sel o.deal_uuid
        from (sel distinct deal_uuid, merchant_uuid from sh_dream_options_stg) o
        join sh_dream_launch_dates ld on o.merchant_uuid = ld.merchant_uuid
        join user_groupondw.active_deals ad on o.deal_uuid = ad.deal_uuid and ad.load_date >= '2020-09-01'
        where ad.load_date >= coalesce(ld.launch_date,cast('2020-10-01' as date))
        group by 1
    ) l
    join sh_dream_options_stg o on l.deal_uuid = o.deal_uuid
) with data unique primary index (inv_product_uuid) on commit preserve rows;


create volatile table sh_dream_live as (
    sel ad.deal_uuid,
        min(ad.load_date) first_dt,
        max(ad.load_date) last_dt,
        case when last_dt >= current_date-2 then 1 else 0 end still_live
    from user_groupondw.active_deals ad
    join (sel distinct deal_uuid from sh_dream_options) d on ad.deal_uuid = d.deal_uuid
    group by 1
) with data unique primary index (deal_uuid) on commit preserve rows;


drop table sandbox.sh_dream_merchants;

create table sandbox.sh_dream_merchants as (
    sel o.merchant_uuid,
        m.account_id,
        m.account_name,
        m.rep_name,
        ld.launch_date,
        max(l.still_live) still_live,
        max(div.division) division,
        count(distinct case when still_live = 1 then pds end) pdses_live,
        count(distinct case when still_live = 1 then inv_product_uuid end) options_live,
        count(distinct case when still_live = 1 then o.deal_uuid end) deals_live
    from sh_dream_options o
    join sh_dream_mercs m on o.merchant_uuid = m.merchant_uuid
    join (
        sel merchant_uuid,
            division,
            count(1) n_options,
            row_number() over (partition by merchant_uuid order by n_options desc) rownumdesc
        from sh_dream_options
        where division in ('Seattle','Denver','Long Island','Detroit')
        group by 1,2
    ) div on o.merchant_uuid = div.merchant_uuid
    left join sh_dream_launch_dates ld on o.merchant_uuid = ld.merchant_uuid
    left join sh_dream_live l on l.deal_uuid = o.deal_uuid
    group by 1,2,3,4,5
) with data unique primary index (merchant_uuid);grant sel on sandbox.sh_dream_merchants to public
;


-----------------------------ACCOUNT


/* what we need

x    deal_uuid
x    opportunity_id
x    account_name
x    rep_name
x    bt_launch_date
    action_cohort
x    link_to_deal
x    division
x    top_pds
x    has_gcal
x    units_sold_t30
x    pct_units_booked
x    has_availability
x    n_dow_available
x    avg_booking_delay
*/

create volatile table sh_bt_deals as (
    sel a.deal_uuid,
        sf.opportunity_id,
        account_name,
        account_owner,
        account_id,
        cast(bt_launch_date as date) bt_launch_date,
        division,
        has_gcal
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
    where product_is_active_flag = 1
    and partner_inactive_flag = 0
    and load_date = current_date-2
    and gdl.country_code = 'US'
    and gdl.grt_l1_cat_name = 'L1 - Local'
    and gdl.grt_l2_cat_description = 'HBW'
) with data unique primary index (deal_uuid) on commit preserve rows;


create volatile table sh_fgt_book as (
    sel fgt.*
    from user_edwprod.fact_gbl_transactions fgt
    join sh_bt_deals d on fgt.deal_uuid = d.deal_uuid
    where order_date >= case when d.bt_launch_date > current_date-30 then d.bt_launch_date else current_date-30 end
) with data primary index (order_id, action) on commit preserve rows;


create volatile table sh_booked as (
    sel f.order_id,
        max(case when bn.voucher_code is not null then 1 else 0 end) booked_bt
    from sh_fgt_book f
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
    sel deal_uuid,
        min(case when pds_rank = 1 then pds end) top_pds,
        sum(units_sold) units_sold,
        sum(case when booked_bt = 1 then units_sold else 0 end) units_sold_booked_bt
    from (
        sel t.*,
            row_number() over (partition by deal_uuid order by units_sold desc) pds_rank
        from (
            sel m.deal_uuid,
                pds.pds,
                booked_bt,
                sum(fgt.transaction_qty) units_sold
            from sh_bt_deals m
            join sh_fgt_book fgt on m.deal_uuid = fgt.deal_uuid
            join sh_inv_pds pds on fgt.inv_product_uuid = pds.inv_product_uuid
            left join sh_booked b on fgt.order_id = b.order_id
            where fgt.action = 'authorize'
            and fgt.is_zero_amount = 0
            and fgt.is_order_canceled = 0
            and fgt.order_date >= case when m.bt_launch_date > current_date-30 then m.bt_launch_date else current_date-30 end
            group by 1,2,3
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
    join (sel order_id, max(deal_uuid) deal_uuid from sh_fgt_book group by 1) f on b.order_id = f.order_id
    left join sh_refunds r on b.order_id = r.order_id
    where booked_bt = 1
    group by 1
) with data unique primary index (deal_uuid) on commit preserve rows;drop table sandbox.sh_hbw_booking_status_account;

create table sandbox.sh_hbw_booking_status_account as (
    sel t.*,
        case when num_dow is null then 'a. no availability setup'
             when booked_refund_rate > .25 then 'b. high refunds on booked vouchers'
             when avg_booking_delay > 24 then 'c. booking leadtime >24 hours'
             when num_dow < 5 then 'd. less than 5 days available next week'
             when pct_units_booked < .3 then 'e. less than 30% units booked'
             else 'f. no action needed'
         end action_cohort
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
        cast(sum(u.units_sold_booked_bt) as dec(18,3)) / cast(nullifzero(units_sold_t30) as dec(18,3)) pct_units_booked,
        cast(sum(r.booked_refunds) as dec(18,3)) / cast(nullifzero(sum(r.booked_txns)) as dec(18,3)) booked_refund_rate,
        cast(avg(bdd.avg_booking_delay) as dec(18,3)) avg_booking_delay,
        max(dal.num_dow) num_dow,
        max(case when dal.deal_uuid is not null then 1 else 0 end) has_availability
    from sh_bt_deals d
    left join sandbox.sh_dream_booking_delay_deal bdd on d.deal_uuid = bdd.deal_uuid
    left join (
        sel deal_uuid,
            report_date,
            days_in_next_7 num_dow
        from sandbox.jk_bt_deal_max_avail_v3
        qualify row_number() over (partition by deal_uuid order by report_date desc) = 1
    ) dal on d.deal_uuid = dal.deal_uuid
    left join sh_units u on d.deal_uuid = u.deal_uuid
    left join sh_booked_refunds r on d.deal_uuid = r.deal_uuid
    group by 1
    ) t
) with data unique primary index (account_id);grant sel on sandbox.sh_hbw_booking_status_account to public


sel * from sandbox.sh_hbw_booking_status_account where units_sold_t30 > 3
--------------------------DEAL



/* what we need

x    deal_uuid
x    opportunity_id
x    account_name
x    rep_name
x    bt_launch_date
    action_cohort
x    link_to_deal
x    division
x    top_pds
x    has_gcal
x    units_sold_t30
x    pct_units_booked
x    has_availability
x    n_dow_available
x    avg_booking_delay
*/

create volatile table sh_bt_deals as (
    sel a.deal_uuid,
        sf.opportunity_id,
        account_name,
        account_owner,
        cast(bt_launch_date as date) bt_launch_date,
        division,
        has_gcal
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
        sel deal_uuid, max(o1.id) opportunity_id, max(o1.division) division, max(sfa.name) account_name, max(full_name) account_owner
        from user_edwprod.sf_opportunity_1 o1
        join user_edwprod.sf_opportunity_2 o2 on o1.id = o2.id
        join user_edwprod.sf_account sfa on o1.accountid = sfa.id
        left join user_groupondw.dim_sf_person sfp on sfa.ownerid = sfp.person_id
        group by 1
    ) sf on a.deal_uuid = sf.deal_uuid
    where product_is_active_flag = 1
    and partner_inactive_flag = 0
    and load_date = current_date-2
    and gdl.country_code = 'US'
    and gdl.grt_l1_cat_name = 'L1 - Local'
    and gdl.grt_l2_cat_description = 'HBW'
) with data unique primary index (deal_uuid) on commit preserve rows;


create volatile table sh_fgt_book as (
    sel fgt.*
    from user_edwprod.fact_gbl_transactions fgt
    join sh_bt_deals d on fgt.deal_uuid = d.deal_uuid
    where order_date >= case when d.bt_launch_date > current_date-30 then d.bt_launch_date else current_date-30 end
) with data primary index (order_id, action) on commit preserve rows;


create volatile table sh_booked as (
    sel f.order_id,
        max(case when bn.voucher_code is not null then 1 else 0 end) booked_bt
    from sh_fgt_book f
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
    sel deal_uuid,
        min(case when pds_rank = 1 then pds end) top_pds,
        sum(units_sold) units_sold,
        sum(case when booked_bt = 1 then units_sold else 0 end) units_sold_booked_bt
    from (
        sel t.*,
            row_number() over (partition by deal_uuid order by units_sold desc) pds_rank
        from (
            sel m.deal_uuid,
                pds.pds,
                booked_bt,
                sum(fgt.transaction_qty) units_sold
            from sh_bt_deals m
            join sh_fgt_book fgt on m.deal_uuid = fgt.deal_uuid
            join sh_inv_pds pds on fgt.inv_product_uuid = pds.inv_product_uuid
            left join sh_booked b on fgt.order_id = b.order_id
            where fgt.action = 'authorize'
            and fgt.is_zero_amount = 0
            and fgt.is_order_canceled = 0
            and fgt.order_date >= case when m.bt_launch_date > current_date-30 then m.bt_launch_date else current_date-30 end
            group by 1,2,3
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
    join (sel order_id, max(deal_uuid) deal_uuid from sh_fgt_book group by 1) f on b.order_id = f.order_id
    left join sh_refunds r on b.order_id = r.order_id
    where booked_bt = 1
    group by 1
) with data unique primary index (deal_uuid) on commit preserve rows;



drop table sandbox.sh_hbw_booking_status_deal;
create table sandbox.sh_hbw_booking_status_deal as (
    sel t.*,
        case when num_dow is null then 'a. no availability setup'
             when booked_refund_rate > .25 then 'b. high refunds on booked vouchers'
             when avg_booking_delay > 24 then 'c. booking leadtime >24 hours'
             when num_dow < 5 then 'd. less than 5 days available next week'
             when pct_units_booked < .3 then 'e. less than 30% units booked'
             else 'f. no action needed'
         end action_cohort
    from (
        sel d.*,
            u.top_pds,
            u.units_sold as units_sold_t30,
            cast(u.units_sold_booked_bt as dec(18,3)) / cast(nullifzero(u.units_sold) as dec(18,3)) pct_units_booked,
            cast(bdd.avg_booking_delay as dec(18,3)) avg_booking_delay,
            dal.num_dow,
            case when dal.deal_uuid is not null then 1 else 0 end has_availability,
            r.booked_refund_rate
        from sh_bt_deals d
        left join sandbox.sh_dream_booking_delay_deal bdd on d.deal_uuid = bdd.deal_uuid
        left join (
            sel deal_uuid,
                report_date,
                days_in_next_7 num_dow
            from sandbox.jk_bt_deal_max_avail_v3
            qualify row_number() over (partition by deal_uuid order by report_date desc) = 1
        ) dal on d.deal_uuid = dal.deal_uuid
        left join sh_units u on d.deal_uuid = u.deal_uuid
        left join sh_booked_refunds r on d.deal_uuid = r.deal_uuid
    ) t
) with data unique primary index (deal_uuid);grant sel on sandbox.sh_hbw_booking_status_deal to public



sel * from sandbox.sh_hbw_booking_status_deal where units_sold_t30 > 3
