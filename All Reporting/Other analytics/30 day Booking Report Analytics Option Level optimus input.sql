case when dal.num_dow is not null then 1 else 0 end has_availability_setup,
select * from sandbox.nvp_inv_availability;
select * from sandbox.nvp_inv_availability;


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
        case when dal.num_dow is not null then 1 else 0 end has_availability_setup,
        case when dal.report_date <= current_date - 10 then 1 end latest_availability_not_data
    from sandbox.sh_bt_active_deals_log_v4 as a
    left join sandbox.nvp_bt_active_deals_log as a2 on a.deal_uuid = a2.deal_uuid
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
        from sandbox.nvp_inv_availability) dal on a.deal_uuid = dal.deal_uuid and a2.deal_option_id = dal.deal_option_id
    where a.product_is_active_flag = 1
    and a.partner_inactive_flag = 0
    and a.load_date = current_date-2
    and gdl.country_code = 'US'
    and gdl.grt_l1_cat_name = 'L1 - Local'
    and gdl.grt_l2_cat_description = 'HBW'
) with data unique primary index (deal_uuid, unified_deal_option_id) on commit preserve rows;



create volatile table sh_bt_deals2 as (
  select
    deal_uuid,
    opportunity_id,
    account_name,
    account_owner,
    account_id,
    bt_launch_date,
    division,
    has_gcal,
    count(distinct unified_deal_option_id) total_options,
    count(distinct case when bookable = 1 then unified_deal_option_id else null end) bookable_options,
    count(distinct case when num_dow >=5 then unified_deal_option_id else null end) options_greater_than5,
    count(distinct case when num_dow >=3 then unified_deal_option_id else null end) options_greater_than3,
    count(distinct case when num_dow >=1 then unified_deal_option_id else null end) options_greater_than1,
    max(has_availability_setup) has_availability_setup,
    max(latest_availability_not_data) latest_availability_not_data,
    max(gapp_enabled) gapp_enabled,
    avg(num_dow) avg_available_days
  from sh_bt_deals
  group by 1,2,3,4,5,6,7,8
) with data unique primary index (deal_uuid) on commit preserve rows;


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
        max(case when bn.voucher_code is not null then 1 else 0 end) booked_bt
    from sh_fgt_book f
    join user_gp.camp_membership_coupons cmc on f.order_id = cast(cmc.order_id as varchar(64))
    left join sandbox.sh_bt_bookings_rebuild bn on cmc.code = bn.voucher_code and cast(cmc.merchant_redemption_code as varchar(50)) = bn.security_code
    group by 1
) with data unique primary index (order_id) on commit preserve rows;


create volatile table sh_booked_cancelled_by as (
    sel
        order_id,
        cancelled_by
    from
    (select
            f.order_id,
            bn.cancelled_by,
            ROW_NUMBER() over(partition by f.order_id order by cast(substr(bn.created_at, 1,10) as date) desc) ord
        from sh_fgt_book f
        join user_gp.camp_membership_coupons cmc on f.order_id = cast(cmc.order_id as varchar(64))
        join sandbox.sh_bt_bookings_rebuild bn
           on cmc.code = bn.voucher_code
           and cast(cmc.merchant_redemption_code as varchar(50)) = bn.security_code
           and bn.state = 'cancelled'
        ) fin where fin.ord = 1
) with data unique primary index (order_id) on commit preserve rows;


create volatile table sh_inv_pds as (
    sel doe.merch_product_uuid, max(pds.pds_cat_name) pds, max(inv_product_uuid) inv_product_uuid
    from user_edwprod.dim_offer_ext doe
    join user_dw.v_dim_pds_grt_map pds on doe.pds_cat_id = pds.pds_cat_id
    group by 1
) with data unique primary index(merch_product_uuid) on commit preserve rows;


create volatile table sh_units as (
    sel deal_uuid,
        unified_deal_option_id,
        pds,
        sum(units_sold) units_sold,
        sum(case when booked_bt = 1 then units_sold else 0 end) units_sold_booked_bt
    from (
            sel m.deal_uuid,
                m.unified_deal_option_id,
                pds.pds,
                b.booked_bt,
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
            group by 1,2,3,4
    ) t
    group by 1,2,3
) with data unique primary index (deal_uuid, unified_deal_option_id) on commit preserve rows;


create volatile table sh_refunds as (
    sel order_id
    from user_edwprod.fact_gbl_transactions
    where action = 'refund'
    and order_date >= '2020-01-01'
    group by 1
) with data unique primary index (order_id) on commit preserve rows;


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


create volatile table nvp_hbw_booking_status_inv as (
  sel d.*,
      u.pds,
      u.units_sold as units_sold_t30,
      u.units_sold_booked_bt as units_sold_bt_t30,
      cast(bdd.avg_booking_delay as dec(18,3)) avg_booking_delay,
      r.booked_txns_inv,
      r.booked_refunds_inv
  from sh_bt_deals d
  left join sandbox.sh_dream_booking_delay_deal bdd on d.deal_uuid = bdd.deal_uuid
  left join sh_units u on d.deal_uuid = u.deal_uuid and d.unified_deal_option_id = u.unified_deal_option_id
  left join sh_booked_refunds r on d.deal_uuid = r.deal_uuid and d.unified_deal_option_id = r.unified_deal_option_id
) with data unique primary index(deal_uuid, unified_deal_option_id) on commit preserve rows;


create volatile table nvp_hbw_report_deal as (
select
   a.*,
   b.avg_booking_delay,
   min(case when b.pds_rank = 1 then pds end) pds_most_selling,
   sum(booked_txns_inv) booked_txns,
   sum(booked_refunds_inv) booked_refunds_inv,
   sum(units_sold_t30) units_sold_t30,
   sum(units_sold_bt_t30) units_sold_bt_t30,
   sum(case when num_dow >=5 then units_sold_t30 else null end) units_sold_5avail,
   sum(case when num_dow >=3 then units_sold_t30 else null end) units_sold_3avail,
   sum(case when num_dow >=1 then units_sold_t30 else null end) units_sold_1avail
   from
   sh_bt_deals2 as a
   left join
   (sel t.*,
       row_number() over (partition by deal_uuid order by units_sold_t30 desc) pds_rank
   from
   nvp_hbw_booking_status_inv as t) as b on a.deal_uuid = b.deal_uuid
   group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18)
with data unique primary index (deal_uuid) on commit preserve rows
;

drop table sandbox.nvp_hbw_booking_status_deal;
create table sandbox.nvp_hbw_booking_status_deal as (
select
	fin.*,
	case
        when (has_availability_setup = 0 or latest_availability_not_data  = 1) then 'a availability setup/data issue'
        when avg_available_days < 1 then 'b. 0 days available next week'
        when avg_booking_delay > 24 then 'c. booking leadtime >24 hours'
        when avg_available_days < 4 then 'd. less than 4 days available next week'
        when pct_units_booked < .3 then 'e. less than 30% units booked'
        when pct_units_booked <.6  then 'f. less than 60% units booked'
        when booked_refund_rate > .25 then 'g. high refunds on booked vouchers'
        else 'h. no action needed' end action_cohort,
    case when (has_availability_setup = 0 or latest_availability_not_data  = 1) then 'Availability - None'
             when avg_available_days < 1 then 'Availability - None'
             when avg_booking_delay > 24 then 'Long Lead time'
             when avg_available_days < 4 then 'Availability - Low'
             when pct_units_booked < .6 then 'Low Bookings'
             when booked_refund_rate > .25 then 'High Refunds'
             else 'No Action'
         end action_cohort2
     from
(select
  a.*,
  cast(units_sold_bt_t30 as dec(18,3)) / cast(nullifzero(units_sold_t30) as dec(18,3)) pct_units_booked,
  cast(booked_refunds_inv as dec(18,3)) / cast(nullifzero(booked_txns) as dec(18,3)) booked_refund_rate
  from
  nvp_hbw_report_deal as a) as fin
) with data unique primary index (deal_uuid);

create volatile table nvp_hbw_report_account as (
select
        a.account_id,
        max(a.account_name) account_name,
        max(a.account_owner) account_owner,
        min(a.bt_launch_date) bt_launch_date,
        max(a.deal_uuid) sample_deal_uuid,
        max(a.has_gcal) has_gcal,
        max(a.division) max_division,
        max(a.has_availability_setup) has_availability_setup,
        max(a.latest_availability_not_data) latest_availability_not_data,
        count(distinct a.deal_uuid) n_deals_bookable,
        min(case when b.pds_rank = 1 then pds end) top_pds,
        sum(b.booked_txns_inv) booked_txns,
        sum(b.booked_refunds_inv) booked_refunds_inv,
        sum(b.units_sold_t30) units_sold_t30,
        sum(b.units_sold_bt_t30) units_sold_bt_t30,
        sum(case when b.num_dow >=5 then b.units_sold_t30 else null end) units_sold_5avail,
        sum(case when b.num_dow >=3 then b.units_sold_t30 else null end) units_sold_3avail,
        sum(case when b.num_dow >=1 then b.units_sold_t30 else null end) units_sold_1avail,
        avg(avg_booking_delay) avg_booking_delay,
        sum(total_options*avg_available_days) sum_available_days,
        sum(total_options) total_options,
        count(distinct concat(a.account_name,a.account_owner)) d_accounts, 
        max(avg_available_days) avg_available_days
       from
         sh_bt_deals2 as a
       left join
       (sel t.*,
            row_number() over (partition by account_id order by units_sold_t30 desc) pds_rank
        from
        nvp_hbw_booking_status_inv as t) as b on a.deal_uuid = b.deal_uuid
        group by 1
        )
with data unique primary index (account_id) on commit preserve rows
;



drop table sandbox.nvp_hbw_booking_status_account;
create table sandbox.nvp_hbw_booking_status_account as (
select
	fin.*,
	case
  when (has_availability_setup = 0 or latest_availability_not_data  = 1) then 'a availability setup/data issue'
          when avg_available_days < 1 then 'b. 0 days available next week'
          when avg_booking_delay > 24 then 'c. booking leadtime >24 hours'
          when avg_available_days < 4 then 'd. less than 4 days available next week'
          when pct_units_booked < .3 then 'e. less than 30% units booked'
          when pct_units_booked <.6 then 'f. less than 60% units booked'
          when booked_refund_rate > .25 then 'g. high refunds on booked vouchers'
          else 'h. no action needed' end action_cohort,
     case when (has_availability_setup = 0 or latest_availability_not_data  = 1) then 'Availability - None'
             when avg_available_days < 1 then 'Availability - None'
             when avg_booking_delay > 24 then 'Long Lead time'
             when avg_available_days < 4 then 'Availability - Low'
             when pct_units_booked < .6 then 'Low Bookings'
             when booked_refund_rate > .25 then 'High Refunds'
             else 'No Action'
         end action_cohort2
     from
(select
  a.*,
  cast(units_sold_bt_t30 as dec(18,3)) / cast(nullifzero(units_sold_t30) as dec(18,3)) pct_units_booked,
  cast(booked_refunds_inv as dec(18,3)) / cast(nullifzero(booked_txns) as dec(18,3)) booked_refund_rate
  from
  nvp_hbw_report_account as a) as fin
) with data unique primary index (account_id);



select * from sandbox.nvp_hbw_booking_status_deal where units_sold_t30 >3;
select * from sandbox.nvp_hbw_booking_status_account where units_sold_t30 > 3;


----------Actual Account Available days average


/*  when has_availability_setup = 0 then 'a. no availability setup'
       when latest_availability_not_data  = 1 then 'b.latest availability data not available'*/


create volatile table nvp_hbw_report_account as (
    select
       fin.account_id,
       fin.account_name,
       fin.account_owner,
       fin.bt_launch_date,
       fin.sample_deal_uuid,
       fin.has_gcal,
       fin.max_division,
       fin.has_availability_setup,
       fin.latest_availability_not_data,
       fin.n_deals_bookable,
       fin.top_pds,
       fin.booked_txns,
       fin.booked_refunds_inv,
       fin.units_sold_t30,
       fin.units_sold_bt_t30,
       fin.units_sold_5avail,
       fin.units_sold_3avail,
       fin.units_sold_1avail,
       fin.avg_booking_delay,
       sum_available_days/(total_options*d_accounts) avg_available_days
     from
    (select
        a.account_id,
        max(a.account_name) account_name,
        max(a.account_owner) account_owner,
        min(a.bt_launch_date) bt_launch_date,
        max(a.deal_uuid) sample_deal_uuid,
        max(a.has_gcal) has_gcal,
        max(a.division) max_division,
        max(a.has_availability_setup) has_availability_setup,
        max(a.latest_availability_not_data) latest_availability_not_data,
        count(distinct a.deal_uuid) n_deals_bookable,
        min(case when b.pds_rank = 1 then pds end) top_pds,
        sum(b.booked_txns_inv) booked_txns,
        sum(b.booked_refunds_inv) booked_refunds_inv,
        sum(b.units_sold_t30) units_sold_t30,
        sum(b.units_sold_bt_t30) units_sold_bt_t30,
        sum(case when b.num_dow >=5 then b.units_sold_t30 else null end) units_sold_5avail,
        sum(case when b.num_dow >=3 then b.units_sold_t30 else null end) units_sold_3avail,
        sum(case when b.num_dow >=1 then b.units_sold_t30 else null end) units_sold_1avail,
        avg(avg_booking_delay) avg_booking_delay,
        sum(total_options*avg_available_days) sum_available_days,
        sum(total_options) total_options,
        count(distinct concat(a.account_name,a.account_owner)) d_accounts
       from
         sh_bt_deals2 as a
       left join
       (sel t.*,
            row_number() over (partition by account_id order by units_sold_t30 desc) pds_rank
        from
        nvp_hbw_booking_status_inv as t) as b on a.deal_uuid = b.deal_uuid
        group by 1) as fin
        )
with data unique primary index (account_id) on commit preserve rows
;

----------refund reasons report

create multiset table sandbox.sh_booked_refund_reasons as (
sel
   f.deal_uuid,
   f.unified_deal_option_id,
   b.order_id,
   ar.reason,
   ar.comments
   from
   sh_booked b
   join (sel order_id, max(deal_uuid) deal_uuid, max(unified_deal_option_id) unified_deal_option_id from sh_fgt_book group by 1) f on b.order_id = f.order_id
   join sandbox.nvp_hbw_booking_status_deal tp on f.deal_uuid = tp.deal_uuid and tp.action_cohort = 'c. high refunds on booked vouchers'
   left join
       sh_refunds r on b.order_id = r.order_id ----only refunded and booked vouchers included
   left join
       user_gp.order_audit_records ar on ar.order_id = r.order_id
   where b.booked_bt = 1
) with data;

drop table sandbox.nvp_high_refund_reasons;
create multiset table sandbox.nvp_high_refund_reasons as
(select
    reason,
    comments,
    count(distinct order_id) orders_refunded
from
sh_booked_refund_reasons
group by 1,2) with data;

-----bookable

drop table sandbox.sh_booked_refund_reasons_bookable;
create multiset table sandbox.sh_booked_refund_reasons_bookable as (
sel
   f.deal_uuid,
   b.booked_bt,
   f.unified_deal_option_id,
   b.order_id,
   case when r.order_id is not null then 1 else 0 end refunded,
   ar.reason,
   ar.comments
   from
   sh_booked b
   join (sel order_id, max(deal_uuid) deal_uuid, max(unified_deal_option_id) unified_deal_option_id from sh_fgt_book group by 1) f on b.order_id = f.order_id
   left join
       sh_refunds r on b.order_id = r.order_id ----only refunded and booked vouchers included
   left join
       user_gp.order_audit_records ar on ar.order_id = r.order_id
) with data;

select booked_bt, refunded , count(distinct order_id)
from sandbox.sh_booked_refund_reasons_bookable as f
join sandbox.nvp_hbw_booking_status_deal tp on f.deal_uuid = tp.deal_uuid and tp.action_cohort = 'c. high refunds on booked vouchers'
group by 1,2 order by 1,2;

drop table sandbox.nvp_high_refund_reasons_bookable;
create multiset table sandbox.nvp_high_refund_reasons_bookable as
(select
    reason,
    comments,
    count(distinct order_id) orders_refunded
from
sandbox.sh_booked_refund_reasons_bookable
where booked_bt = 1 and refund = 1
group by 1,2) with data;

select * from sandbox.nvp_high_refund_reasons_bookable order by orders_refunded desc;


select
    reason,
    comments,
    count(distinct order_id) orders_refunded
from
sandbox.sh_booked_refund_reasons_bookable as f
join sandbox.nvp_hbw_booking_status_deal tp on f.deal_uuid = tp.deal_uuid and tp.action_cohort = 'c. high refunds on booked vouchers'
where booked_bt = 1 and refunded = 1
group by 1,2
order by orders_refunded desc;

-----All refund reasons
drop table nvp_all_refund;
create multiset volatile table nvp_all_refund as (
select order_id
from user_edwprod.fact_gbl_transactions as a
join user_edwprod.dim_gbl_deal_lob gdl on a.deal_uuid = gdl.deal_id
where action = 'refund'
    and gdl.country_code = 'US'
    and gdl.grt_l1_cat_name = 'L1 - Local'
    and gdl.grt_l2_cat_description = 'HBW'
    and order_date > CURRENT_DATE - 30
) with data on commit preserve rows


create multiset volatile table nvp_all_authorize as (
select order_id
from user_edwprod.fact_gbl_transactions as a
join user_edwprod.dim_gbl_deal_lob gdl on a.deal_uuid = gdl.deal_id
where action = 'authorize'
    and gdl.country_code = 'US'
    and gdl.grt_l1_cat_name = 'L1 - Local'
    and gdl.grt_l2_cat_description = 'HBW'
    and order_date > CURRENT_DATE - 30
) with data on commit preserve rows

select
   count(distinct a.order_id) total_orders,
   count(distinct b.order_id) refunded_orders
from nvp_all_authorize as a
left join nvp_all_refund as b on a.order_id = b.order_id
;

select
     ar.reason,
     ar.comments,
     count(distinct r.order_id) refunded_orders
  from
      nvp_all_refund r
       left join
       user_gp.order_audit_records ar on ar.order_id = r.order_id
   group by 1,2
   order by refunded_orders desc;


------old
select * from sandbox.nvp_hbw_booking_status_deal;


drop table sandbox.nvp_hbw_booking_status_deal;
create table sandbox.nvp_hbw_booking_status_deal as (
    sel t.*,
        case when (gapp_enabled <> 1 or gapp_enabled is null) then 'a.not gapp enabled'
             when num_dow is null then 'b. no availability setup'
             when booked_refund_rate > .25 then 'c. high refunds on booked vouchers'
             when avg_booking_delay > 24 then 'd. booking leadtime >24 hours'
             when num_dow < 5 then 'e. less than 5 days available next week'
             when pct_units_booked < .3 then 'f. less than 30% units booked'
             else 'g. no action needed'
         end action_cohort
    from (
        sel d.*,
            u.pds,
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
        left join sh_units u on d.deal_uuid = u.deal_uuid and d.inventory_product_uuid = u.inventory_product_uuid
        left join sh_booked_refunds r on d.deal_uuid = r.deal_uuid and d.inventory_product_uuid = r.inv_product_uuid
    ) t
) with data unique primary index (deal_uuid, inventory_product_uuid);
grant sel on sandbox.nvp_hbw_booking_status_deal to public;



-----------------------------------

drop table sandbox.nvp_hbw_booking_status_deal;
create table sandbox.nvp_hbw_booking_status_deal as (
    sel t.*,
        case when (gapp_enabled <> 1 or gapp_enabled is null) then 'a.not gapp enabled'
             when num_dow is null then 'b. no availability setup'
             when booked_refund_rate > .25 then 'c. high refunds on booked vouchers'
             when avg_booking_delay > 24 then 'd. booking leadtime >24 hours'
             when num_dow < 5 then 'e. less than 5 days available next week'
             when pct_units_booked < .3 then 'f. less than 30% units booked'
             else 'g. no action needed'
         end action_cohort
    from (
        sel d.*,
            u.pds,
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
        left join sh_units u on d.deal_uuid = u.deal_uuid and d.inventory_product_uuid = u.inventory_product_uuid
        left join sh_booked_refunds r on d.deal_uuid = r.deal_uuid and d.inventory_product_uuid = r.inv_product_uuid
    ) t
) with data unique primary index (deal_uuid, inventory_product_uuid);
grant sel on sandbox.nvp_hbw_booking_status_deal to public;
