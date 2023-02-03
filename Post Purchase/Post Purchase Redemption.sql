/* get booked parent order from cerebro */
create table grp_gdoop_bizops_db.nvp_booked_parentorders_trial stored as orc as
select 
     parent_order_uuid
from grp_gdoop_bizops_db.sh_bt_txns
where order_date >= '2020-01-01'
and booked = 1
group by parent_order_uuid;

select 
     parent_order_uuid
from grp_gdoop_bizops_db.rt_bt_txns
where order_date >= '2020-01-01'
and booked = 1
group by parent_order_uuid;

delete from sandbox.sh_booked_parent_orders;
create table sandbox.sh_booked_parent_orders (
    parent_order_uuid varchar(255)
) unique primary index (parent_order_uuid);



/* replicate red dash */

--------------------------------------------------------------------------------------------------------------SH_FGT_SUB

create volatile table sh_fgt_sub as (
    sel deal_uuid, 
    parent_order_uuid, 
    order_id, 
    min(case when action = 'refund' then transaction_date end) refund_date, 
    min(case when action = 'authorize' then order_date end) order_date_
    from user_edwprod.fact_gbl_transactions
    where deal_uuid is not null
    and order_date between '2018-01-01' and current_date
    group by 1,2,3
) with data primary index (parent_order_uuid, order_id) on commit preserve rows;


--------------------------------------------------------------------------------------------------------------NAM Vouchers
create volatile table sh_rvb_sub as (
    sel a.*
    from user_groupondw.acctg_red_voucher_base a
    where cast (created_at as date) between '2019-01-01' and '2019-06-30'
    and external_yn = 0
    qualify row_number() over (partition by id order by created_at desc) = 1
) with data primary index (order_id) on commit preserve rows;


insert into sh_rvb_sub
    sel a.*
    from user_groupondw.acctg_red_voucher_base a
    where cast (created_at as date) between '2019-07-01' and '2019-12-31'
    and external_yn = 0
    qualify row_number() over (partition by id order by created_at desc) = 1
;

insert into sh_rvb_sub
    sel a.*
    from user_groupondw.acctg_red_voucher_base a
    where cast (created_at as date) between '2018-01-01' and '2018-06-30'
    and external_yn = 0
    qualify row_number() over (partition by id order by created_at desc) = 1
;

insert into sh_rvb_sub
    sel a.*
    from user_groupondw.acctg_red_voucher_base a
    where cast (created_at as date) between '2018-07-01' and '2018-12-31'
    and external_yn = 0
    qualify row_number() over (partition by id order by created_at desc) = 1
;

insert into sh_rvb_sub
    sel a.*
    from user_groupondw.acctg_red_voucher_base a
    where cast (created_at as date) between '2020-01-01' and '2020-06-30'
    and external_yn = 0
    qualify row_number() over (partition by id order by created_at desc) = 1
;

insert into sh_rvb_sub
    sel a.*
    from user_groupondw.acctg_red_voucher_base a
    where cast (created_at as date) between '2020-07-01' and current_date
    and external_yn = 0
    qualify row_number() over (partition by id order by created_at desc) = 1
;

--------------------------------------------------------------------------------------------------------------INTL Vouchers and redemptions


create volatile table sh_intl_reds as (
    sel grt_l2_cat_description l2,
        grt_l3_cat_description l3,
        gdl.country_code,
        cast('INTL' as varchar(4)) as region,
        is_por,
        case when bpo.parent_order_uuid is not null then 1 else 0 end is_booked,
        case when adl.deal_uuid is not null then 1 else 0 end is_bookable,
        cast(null as smallint) is_viewed,
        cast(created_at as date) order_date,
        cast(refund_date as date) as refund_date,
        cast(usage_date as date) redeem_date,
        cast(valid_before as date) expiration_date,
        count(distinct voucher_code || security_code) as vouchers
    from 
       (select a.*
    	from dwh_base_sec_view.vouchers a
    	where cast (created_at as date) between '2018-01-01' and current_date - 1
    	qualify row_number() over (partition by voucher_code , security_code order by last_modified desc) =1
        ) v
    join sh_fgt_sub fgt on v.billing_id = fgt.parent_order_uuid
    left join sandbox.sh_bt_active_deals_log_v4 adl
        on fgt.deal_uuid = adl.deal_uuid
        and fgt.order_date_ = adl.load_date
        and adl.partner_inactive_flag = 0
        and adl.product_is_active_flag = 1
    left join sandbox.sh_booked_parent_orders bpo on fgt.parent_order_uuid = bpo.parent_order_uuid
    join user_edwprod.dim_gbl_deal_lob as gdl on gdl.deal_id = fgt.deal_uuid
    left join (
    	sel deal_uuid,
            max(case when lower(payment_terms) in ('redemption system','payment on redemption (0%)','on redemption') then 1 else 0 end) is_por
    	from user_edwprod.sf_opportunity_2 op2
    	left join user_edwprod.sf_opportunity_1 op on op.opportunity_id = op2.opportunity_id
    	group by 1
        ) por on por.deal_uuid = fgt.deal_uuid
    where gdl.grt_l1_cat_name = 'L1 - Local'
    and gdl.grt_l2_cat_description in ('TTD - Leisure','HBW','F&D','H&A')
    and gdl.country_code not in ('US','CA')
    group by 1,2,3,4,5,6,7,8,9,10,11,12
) with data no primary index on commit preserve rows;



--------------------------------------------------------------------------------------------------------------NAM Redemptions
create volatile table sh_nam_reds as (
    sel grt_l2_cat_description l2,
        grt_l3_cat_description l3,
        gdl.country_code,
        cast('NAM' as varchar(4)) as region,
        is_por,
        case when bpo.parent_order_uuid is not null then 1 else 0 end is_booked,
        case when adl.deal_uuid is not null then 1 else 0 end is_bookable,
        printed_yn as is_viewed,
        cast(order_date as date) order_date,
        cast(refund_date as date) as refund_date,
        cast(redeemed_at as date) redeem_date,
        cast(expires_at as date) expiration_date,
        count(distinct voucher_barcode_id || code) as vouchers
    from sh_rvb_sub v
    join sh_fgt_sub fgt on cast(v.order_id as varchar(64)) = fgt.order_id
    left join sandbox.sh_bt_active_deals_log_v4 adl
        on fgt.deal_uuid = adl.deal_uuid
        and fgt.order_date_ = adl.load_date
        and adl.partner_inactive_flag = 0
        and adl.product_is_active_flag = 1
    left join sandbox.sh_booked_parent_orders bpo on fgt.parent_order_uuid = bpo.parent_order_uuid
    join user_edwprod.dim_gbl_deal_lob as gdl on gdl.deal_id = fgt.deal_uuid
    left join (
        sel deal_uuid,
            max(case when lower(payment_terms) in ('redemption system','payment on redemption (0%)','on redemption') then 1 else 0 end) is_por
        from user_edwprod.sf_opportunity_2 op2
        left join user_edwprod.sf_opportunity_1 op on op.opportunity_id = op2.opportunity_id
        group by 1
    ) por on por.deal_uuid = fgt.deal_uuid
    where gdl.grt_l1_cat_name = 'L1 - Local'
    and gdl.grt_l2_cat_description in ('TTD - Leisure','HBW','F&D','H&A')
    and gdl.country_code in ('US','CA')
    group by 1,2,3,4,5,6,7,8,9,10,11,12
) with data no primary index on commit preserve rows;


--------------------------------------------------------------------------------------------------------------ALL Redemptions
create volatile table sh_reds as (
    sel * from sh_intl_reds
    union
    sel * from sh_nam_reds
) with data no primary index on commit preserve rows;




--------------------------------------------------------------------------------------------------------------Tableau Input


drop table sandbox.sh_cumul_reds_bk;
create table sandbox.sh_cumul_reds_bk as 
(sel td_month_begin(order_date) purchase_mth,
        l2,
        l3,
        is_por,
        is_booked,
        is_bookable,
        is_viewed,
        region,
        country_code,
        case when refund_date is not null then 'a. refunded'
             when redeem_date is not null then 'b. redeemed'
             when expiration_date < current_date then 'c. breakage'
             when expiration_date >= current_date then 'd. outstanding'
             end redemption_state,
        redeem_date - order_date days_to_redeem,
        sum(vouchers) vouchers
    from sh_reds
    group by 1,2,3,4,5,6,7,8,9,10,11
) with data no primary index;
grant sel on sandbox.sh_cumul_reds_bk to public;



/*
select 
    is_bookable, 
    l2,
    L3, 
    sum(case when redemption_state = 'a. refunded' then vouchers end) refunded,
    sum(case when redemption_state = 'b. redeemed' then vouchers end) redeemed,
    sum(case when redemption_state = 'c. breakage' then vouchers end) broken_exp,
    sum(case when redemption_state = 'd. outstanding' then vouchers end) outstanding_unused,
    sum(vouchers) total_vouchers
from sandbox.sh_cumul_reds_bk 
where purchase_mth = '2019-10-01' 
and country_code in ('BE', 'DE', 'ES', 'FR', 'IE', 'NL', 'PL', 'UK')
group by 1,2,3
order by 1,2,3;*/


------------------------------------------------------------------------------------------Redemption Rate 

---------------------30 day redemption Rate

select * from sandbox.sh_cumul_reds_bk where purchase_mth > '2020-07-01' and is_booked = 1;

drop table sandbox.nvp_cum_red_breakdown;
create table sandbox.nvp_cum_red_breakdown as(
SELECT 
   * 
FROM
(
select 
    purchase_mth,
    l2, 
    l3,
    region,
    country_code,
    is_bookable,
    cast('30' as varchar(5)) Cumulative_Upto_X_Days,
    sum(case when redemption_state = 'b. redeemed' and days_to_redeem >= 0 and days_to_redeem <= 30 then vouchers end) redeemed_vouchers,
    sum(vouchers) all_vouchers, 
    sum(case when redemption_state = 'b. redeemed' and days_to_redeem >= 0 and days_to_redeem <= 30 and is_booked = 1 then vouchers end) booked_redeemed_vouchers,
    sum(case when is_booked = 1 then vouchers end) all_booked_vouchers
from sandbox.sh_cumul_reds_bk
group by 1,2,3,4,5,6
UNION
select 
    purchase_mth,
    l2,
    l3,
    region,
    country_code,
    is_bookable,
    '60' Cumulative_Upto_X_Days,
    sum(case when redemption_state = 'b. redeemed' and days_to_redeem >= 0 and days_to_redeem <= 60 then vouchers end) redeemed_vouchers,
    sum(vouchers) all_vouchers,
    sum(case when redemption_state = 'b. redeemed' and days_to_redeem >= 0 and days_to_redeem <= 60 and is_booked = 1 then vouchers end) booked_redeemed_vouchers,
    sum(case when is_booked = 1 then vouchers end) all_booked_vouchers
from sandbox.sh_cumul_reds_bk
group by 1,2,3,4,5,6
UNION
select
    purchase_mth,
    l2,
    l3,
    region,
    country_code,
    is_bookable,
    '90' Cumulative_Upto_X_Days,
    sum(case when redemption_state = 'b. redeemed' and days_to_redeem >= 0 and days_to_redeem <= 90 then vouchers end) redeemed_vouchers,
    sum(vouchers) all_vouchers,
    sum(case when redemption_state = 'b. redeemed' and days_to_redeem >= 0 and days_to_redeem <= 90 and is_booked = 1 then vouchers end) booked_redeemed_vouchers,
    sum(case when is_booked = 1 then vouchers end) all_booked_vouchers
from sandbox.sh_cumul_reds_bk
group by 1,2,3,4,5,6
UNION
select 
    purchase_mth, 
    l2, 
    l3,
    region,
    country_code,
    is_bookable,
    '120' Cumulative_Upto_X_Days,
    sum(case when redemption_state = 'b. redeemed' and days_to_redeem >= 0 and days_to_redeem <= 120 then vouchers end) redeemed_vouchers,
    sum(vouchers) all_vouchers, 
    sum(case when redemption_state = 'b. redeemed' and days_to_redeem >= 0 and days_to_redeem <= 120 and is_booked = 1 then vouchers end) booked_redeemed_vouchers,
    sum(case when is_booked = 1 then vouchers end) all_booked_vouchers
from sandbox.sh_cumul_reds_bk
group by 1,2,3,4,5,6
) AS final_table
) with data no primary index
;
grant sel on sandbox.nvp_cum_red_breakdown to public;

drop table sandbox.nvp_cum_red_breakdown;


select 
is_bookable,
sum(redeemed_vouchers), 
sum(all_vouchers),
sum(booked_redeemed_vouchers), 
sum(all_booked_vouchers)
from sandbox.nvp_cum_red_breakdown 
where purchase_mth = '2019-10-01' and region = 'INTL' and Cumulative_Upto_X_Days = '30'
group by is_bookable;



select * from sandbox.sh_cumul_reds_bk where purchase_mth >= '2020-08-01';