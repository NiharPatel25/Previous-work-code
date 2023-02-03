


drop table sh_bt_launch_dates;
create volatile table sh_bt_launch_dates as (
sel deal_uuid,
        max(has_gcal) has_gcal,
        min(load_date) launch_date
    from sandbox.sh_bt_active_deals_log_v4 ad
    where product_is_active_flag = 1
    and partner_inactive_flag = 0
    and load_date >= '2019-04-01'
    group by 1
) with data unique primary index (deal_uuid) on commit preserve rows;

select * from sandbox.sh_bt_active_deals_log_v4;

create volatile table sh_bt_retention_30 as (
    sel ad.deal_uuid,
        max(case when ad.load_date >= cast(ld.launch_date as date) + interval '31' day then 1 else 0 end) live_after_30_days
    from sandbox.sh_bt_active_deals_log ad
    join sh_bt_launch_dates ld on ad.deal_uuid = ld.deal_uuid
    where product_is_active_flag = 1
    and partner_inactive_flag = 0
    group by 1
) with data unique primary index (deal_uuid) on commit preserve rows;

create volatile table sh_bt_still_live_30 as (
    sel ad.deal_uuid
    from user_groupondw.active_deals ad
    join sh_bt_launch_dates ld on ad.deal_uuid = ld.deal_uuid
    where ad.load_date >= cast(ld.launch_date as date) + interval '31' day
    and ad.sold_out = 'false'
    group by 1
) with data unique primary index (deal_uuid) on commit preserve rows;

drop table sh_booking_solution;
create volatile table sh_booking_solution as (
    select o2.deal_uuid,
        max(sfa.id) sf_account_id,
        max(case when lower(sfa.scheduler_setup_type) in ('pen & paper','none') then 'pen & paper'
            when sfa.scheduler_setup_type is null then 'no data'
            else 'some booking tool'
            end) current_booking_solution,
        max(sfa.scheduler_setup_type) detailed_booking_solution,
        max(sfa.name) account_name,
        max(company_type) company_type
    from dwh_base_sec_view.sf_opportunity_1 o1
    join dwh_base_sec_view.sf_opportunity_2 o2 on o1.id = o2.id
    join dwh_base_sec_view.sf_account sfa on o1.accountid = sfa.id
    group by o2.deal_uuid
) with data unique primary index (deal_uuid) on commit preserve rows;







create volatile multiset table np_five_plus_ret as (
select 
    a.deal_uuid, 
    sum(case when b.order_date >= a.launch_date and b.order_date <= a.launch_date + 30 then transaction_qty end) units_sold, 
    count(distinct case when b.order_date >= a.launch_date and b.order_date <= a.launch_date + 30 then order_id end ) orders_places
from sh_bt_launch_dates as a
    left join user_edwprod.fact_gbl_transactions as b on a.deal_uuid = b.deal_uuid and b.action = 'authorize'
    group by 1
) with data on commit preserve rows;



drop table sandbox.sh_bt_retention_view;
create table sandbox.sh_bt_retention_view as (
    sel cast(dm.month_start as date) report_mth,
        case when gdl.country_code in ('US','CA') then 'NAM' else 'INTL' end region,
        gdl.grt_l2_cat_description,
        case when lower(sbs.current_booking_solution) = 'pen & paper' then 'p&p' else 'other' end booking_type,
        ld.has_gcal,
        count(distinct ld.deal_uuid) n_deals_launched_live30,
        count(distinct case when live_after_30_days = 1 then ld.deal_uuid end) n_deals_retained,
        cast(n_deals_retained as dec(18,3)) / cast(n_deals_launched_live30 as dec(18,3)) pct_retained, 
        count(distinct case when units.units_sold >= 5 then ld.deal_uuid end) five_units_deals_live_30,
        count(distinct case when units.units_sold >= 5  and live_after_30_days = 1 then ld.deal_uuid end)five_units_deals_retained,
        cast(five_units_deals_retained as dec(18,3)) / NULLIFZERO(cast(five_units_deals_live_30 as dec(18,3))) five_pct_retained
    from sh_bt_launch_dates ld
    join user_groupondw.dim_day dd on ld.launch_date = dd.day_rw
    join user_groupondw.dim_month dm on dd.month_key = dm.month_key
    join user_edwprod.dim_gbl_deal_lob gdl on ld.deal_uuid = gdl.deal_id
    join sh_bt_still_live_30 l30 on ld.deal_uuid = l30.deal_uuid
    join sh_booking_solution sbs on ld.deal_uuid = sbs.deal_uuid
    left join sh_bt_retention_30 r30 on ld.deal_uuid = r30.deal_uuid
    left join np_five_plus_ret units on ld.deal_uuid = units.deal_uuid
    where gdl.grt_l2_cat_description in ('F&D','HBW','TTD - Leisure')
    and not (gdl.grt_l2_cat_description = 'F&D' and gdl.country_code in ('US','CA'))
    and launch_date >= '2019-04-01'
    group by 1,2,3,4,5
) with data no primary index;

select * from sandbox.nvp_bss_funnel where onboarded = 1;


-----------------------------------------------OKR
sel cast(dm.month_start as date) report_mth,
        gdl.grt_l2_cat_description,
        ld.has_gcal,
        case when gdl.country_code in ('US','CA') then 'NAM' else 'INTL' end region,
        count(distinct ld.deal_uuid) n_deals_launched_live30,
        count(distinct case when live_after_30_days = 1 then ld.deal_uuid end) n_deals_retained,
        cast(n_deals_retained as dec(18,3)) / cast(n_deals_launched_live30 as dec(18,3)) pct_retained, 
        count(distinct case when units.units_sold >= 5 then ld.deal_uuid end) five_units_deals_live_30,
        count(distinct case when units.units_sold >= 5  and live_after_30_days = 1 then ld.deal_uuid end)five_units_deals_retained,
        cast(five_units_deals_retained as dec(18,3)) / NULLIFZERO(cast(five_units_deals_live_30 as dec(18,3))) five_pct_retained
    from sh_bt_launch_dates ld
    join user_groupondw.dim_day dd on ld.launch_date = dd.day_rw
    join user_groupondw.dim_month dm on dd.month_key = dm.month_key
    join user_edwprod.dim_gbl_deal_lob gdl on ld.deal_uuid = gdl.deal_id
    join sh_bt_still_live_30 l30 on ld.deal_uuid = l30.deal_uuid
    join sh_booking_solution sbs on ld.deal_uuid = sbs.deal_uuid
    left join (select * from sandbox.nvp_bss_funnel where onboarded = 1) bss on ld.deal_uuid = bss.deal_uuid
    left join sh_bt_retention_30 r30 on ld.deal_uuid = r30.deal_uuid
    left join np_five_plus_ret units on ld.deal_uuid = units.deal_uuid
    where gdl.grt_l2_cat_description in ('F&D','HBW','TTD - Leisure')
    and not (gdl.grt_l2_cat_description = 'F&D' and gdl.country_code in ('US','CA'))
    and launch_date >= '2021-01-01'
    and region = 'NAM'
    group by 1,2,3,4
    order by 1,2,3



----------------------------------------BSS
    
sel cast(dm.month_start as date) report_mth,
        case when gdl.country_code in ('US','CA') then 'NAM' else 'INTL' end region,
        gdl.grt_l2_cat_description,
        case when lower(sbs.current_booking_solution) = 'pen & paper' then 'p&p' else 'other' end booking_type,
        ld.has_gcal,
        case when bss.deal_uuid is not null then 1 else 0 end bss_deals,
        count(distinct ld.deal_uuid) n_deals_launched_live30,
        count(distinct case when live_after_30_days = 1 then ld.deal_uuid end) n_deals_retained,
        cast(n_deals_retained as dec(18,3)) / cast(n_deals_launched_live30 as dec(18,3)) pct_retained, 
        count(distinct case when units.units_sold >= 5 then ld.deal_uuid end) five_units_deals_live_30,
        count(distinct case when units.units_sold >= 5  and live_after_30_days = 1 then ld.deal_uuid end)five_units_deals_retained,
        cast(five_units_deals_retained as dec(18,3)) / NULLIFZERO(cast(five_units_deals_live_30 as dec(18,3))) five_pct_retained
    from sh_bt_launch_dates ld
    join user_groupondw.dim_day dd on ld.launch_date = dd.day_rw
    join user_groupondw.dim_month dm on dd.month_key = dm.month_key
    join user_edwprod.dim_gbl_deal_lob gdl on ld.deal_uuid = gdl.deal_id
    join sh_bt_still_live_30 l30 on ld.deal_uuid = l30.deal_uuid
    join sh_booking_solution sbs on ld.deal_uuid = sbs.deal_uuid
    left join (select * from sandbox.nvp_bss_funnel where onboarded = 1) bss on ld.deal_uuid = bss.deal_uuid
    left join sh_bt_retention_30 r30 on ld.deal_uuid = r30.deal_uuid
    left join np_five_plus_ret units on ld.deal_uuid = units.deal_uuid
    where gdl.grt_l2_cat_description in ('F&D','HBW','TTD - Leisure')
    and not (gdl.grt_l2_cat_description = 'F&D' and gdl.country_code in ('US','CA'))
    and launch_date >= '2021-03-01'
    and region = 'NAM'
    group by 1,2,3,4,5,6
    order by  3,bss_deals desc

---------------------------------Retention analytics

    
    
drop table nvp_deal_retention;
create multiset volatile table nvp_deal_retention as (
    sel cast(dm.month_start as date) report_mth,
        ld.deal_uuid, 
        case when gdl.country_code in ('US','CA') then 'NAM' else 'INTL' end region,
        gdl.grt_l2_cat_description,
        case when lower(sbs.current_booking_solution) = 'pen & paper' then 'p&p' else 'other' end booking_type,
        ld.has_gcal,
        case when live_after_30_days = 1 then 1 else 0 end n_deals_retained
    from sh_bt_launch_dates ld
    join user_groupondw.dim_day dd on ld.launch_date = dd.day_rw
    join user_groupondw.dim_month dm on dd.month_key = dm.month_key
    join user_edwprod.dim_gbl_deal_lob gdl on ld.deal_uuid = gdl.deal_id
    join sh_bt_still_live_30 l30 on ld.deal_uuid = l30.deal_uuid
    join sh_booking_solution sbs on ld.deal_uuid = sbs.deal_uuid
    left join sh_bt_retention_30 r30 on ld.deal_uuid = r30.deal_uuid
    where gdl.grt_l2_cat_description in ('F&D','HBW','TTD - Leisure')
    and not (gdl.grt_l2_cat_description = 'F&D' and gdl.country_code in ('US','CA'))
    and launch_date >= '2019-04-01'
) with data no primary index on commit preserve rows;

drop table sh_bt_paused_reason;
create volatile table sh_bt_paused_reason as (
    sel deal_uuid, max(o2.booking_pause_reason__c) booking_pause_reason, max(sfa.id) account_id
    from user_edwprod.sf_opportunity_2 o2
    join user_edwprod.sf_opportunity_1 o1 on o2.id = o1.id
    join user_edwprod.sf_account sfa on o1.accountid = sfa.id
    group by 1
    having booking_pause_reason is not null
) with data unique primary index (deal_uuid) on commit preserve rows;


select 
  grt_l2_cat_description,
  booking_pause_reason, 
  booking_type,
  has_gcal,
  count(distinct deal_uuid) deals
  from 
(select 
  a.*, 
  b.booking_pause_reason
  from 
    nvp_deal_retention as a 
    left join sh_bt_paused_reason as b on a.deal_uuid = b.deal_uuid
   where a.report_mth = '2021-04-01'
   and n_deals_retained = 0) as fin
  group by 1,2,3,4
  order by 1,2;
 
 
 
 
 
    
    

select 
    cast(dm.month_start as date) report_mth,
    case when gdl.country_code in ('US','CA') then 'NAM' else 'INTL' end region,
    ld.has_gcal, 
    count(distinct ld.deal_uuid) all_deals, 
    count(distinct case when l30.deal_uuid is not null then ld.deal_uuid end) retained_after_30, 
    count(distinct case when r30.deal_uuid is not null then ld.deal_uuid end) retained_on_bt
from sh_bt_launch_dates ld
    join user_groupondw.dim_day dd on ld.launch_date = dd.day_rw
    join user_groupondw.dim_month dm on dd.month_key = dm.month_key  
    join user_edwprod.dim_gbl_deal_lob gdl on ld.deal_uuid = gdl.deal_id
    left join sandbox.jrg_all_deal_pauses psr on ld.deal_uuid = psr.deal_uuid
    left join sh_bt_still_live_30 l30 on ld.deal_uuid = l30.deal_uuid
    left join sh_bt_retention_30 r30 on ld.deal_uuid = r30.deal_uuid and r30.live_after_30_days = 1
    where region = 'NAM' and has_gcal = 1
    group by 1,2,3
    order by 1 desc;
    
select deal_uuid, count(1) cnz from sandbox.jrg_all_deal_pauses group by 1 having cnz >1;
select * from sandbox.jrg_all_deal_pauses where deal_uuid = '5126e97f-1cd8-4327-849f-7e6b96a2563e'
    
sel ld.launch_date,
    ld.deal_uuid,
    live_after_30_days retained,
    current_date - launch_date
from sh_bt_launch_dates ld
join sh_bt_still_live_30 l30 on ld.deal_uuid = l30.deal_uuid
join sh_booking_solution sbs on ld.deal_uuid = sbs.deal_uuid
left join sh_bt_retention_30 r30 on ld.deal_uuid = r30.deal_uuid
where ld.has_gcal = 0
and sbs.current_booking_solution = 'pen & paper'
and ld.launch_date >= '2020-10-01'
order by launch_date;

select * from nvp_deal_retention;




 
 


-------------TABLE DISTRO 
drop table sh_bt_paused_reason;
create volatile table sh_bt_paused_reason as (
    sel deal_uuid, max(o2.booking_pause_reason__c) booking_pause_reason, max(sfa.id) account_id
    from user_edwprod.sf_opportunity_2 o2
    join user_edwprod.sf_opportunity_1 o1 on o2.id = o1.id
    join user_edwprod.sf_account sfa on o1.accountid = sfa.id
    group by 1
    having booking_pause_reason is not null
) with data unique primary index (deal_uuid) on commit preserve rows
;
drop table sh_launch_date;
create volatile table sh_launch_date as (
    sel deal_uuid,
        min(load_date) launch_date
    from user_groupondw.active_deals
    group by 1
) with data unique primary index (deal_uuid) on commit preserve rows;

drop table sh_booking_solution;
create volatile table sh_booking_solution as (
    select o2.deal_uuid,
        max(sfa.id) sf_account_id,
        max(case when lower(sfa.scheduler_setup_type) in ('pen & paper','none') then 'pen & paper'
            when sfa.scheduler_setup_type is null then 'no data'
            else 'some booking tool'
            end) current_booking_solution,
        max(sfa.scheduler_setup_type) detailed_booking_solution,
        max(sfa.name) account_name,
        max(company_type) company_type
    from dwh_base_sec_view.sf_opportunity_1 o1
    join dwh_base_sec_view.sf_opportunity_2 o2 on o1.id = o2.id
    join dwh_base_sec_view.sf_account sfa on o1.accountid = sfa.id
    group by o2.deal_uuid
) with data unique primary index (deal_uuid) on commit preserve rows;

create volatile table sh_txns as (
    sel deal_id,
        sum(net_transactions_qty - zdo_net_transactions_qty) units
    from user_edwprod.agg_gbl_financials_deal
    where report_date >= '2020-01-01'
    group by 1
) with data unique primary index (deal_id) on commit preserve rows
;

create volatile table sh_opt_out_notes as (
    sel deal_uuid,
        Product_Opt_Out_Notes__c product_opt_out_notes,
        st.createddate,
        row_number() over (partition by deal_uuid order by st.createddate desc) rownumdesc
    from user_edwprod.sf_task st
    join user_edwprod.sf_opportunity_1 o1 on st.accountid = o1.accountid
    join user_edwprod.sf_opportunity_2 o2 on o1.id = o2.id
    where Product_Opt_Out_Notes__c is not null
    qualify rownumdesc = 1
) with data unique primary index (deal_uuid) on commit preserve rows
;

drop table sh_opt_out_deals;
create volatile table sh_opt_out_deals as (
    sel dw.week_end (date) opt_out_week,
        region,
        country_code,
        l2,
        l3,
        booking_pause_reason,
        deal_uuid,
        sum(units) units_sold_2020
    from (
        sel ad.deal_uuid,
            min(load_date) opt_out_date,
            p.booking_pause_reason,
            grt_l2_cat_description l2,
            grt_l3_cat_description l3,
            case when gdl.country_code = 'US' then 'NAM' else 'INTL' end region,
            gdl.country_code,
            units
        from sandbox.sh_bt_active_deals_log ad
        left join sh_bt_paused_reason p on ad.deal_uuid = p.deal_uuid
        join user_edwprod.dim_gbl_deal_lob gdl on ad.deal_uuid = gdl.deal_id
        left join sh_txns t on ad.deal_uuid = t.deal_id
        where partner_inactive_flag = 1 or product_is_active_flag = 0
        group by 1,3,4,5,6,7,8
    ) t
    join user_dw.v_dim_day dd on t.opt_out_date = dd.day_rw
    join user_dw.v_dim_week dw on dd.week_key = dw.week_key
    group by 1,2,3,4,5,6,7
) with data unique primary index (deal_uuid) on commit preserve rows
;

create volatile table sh_deal_merc_map as (
    sel product_uuid,
        max(merchant_uuid) merchant_uuid
    from user_edwprod.dim_offer_ext
    group by 1
) with data unique primary index (product_uuid) on commit preserve rows;

create volatile table sh_gcal as (
    sel deal_uuid
    from sandbox.sh_bt_active_deals_log_v4
    where has_gcal = 1
    group by 1
) with data unique primary index (deal_uuid) on commit preserve rows;

/* nam hbw query */
sel opt_out_week,
    region,
    case when l2 in ('F&D','HBW') then l2 else 'Other' end l2,
    l3,
    booking_pause_reason,
    bs.current_booking_solution,
    od.deal_uuid,
    units_sold_2020
from sh_opt_out_deals od
left join sh_gcal g on od.deal_uuid = g.deal_uuid
left join sh_booking_solution bs on od.deal_uuid = bs.deal_uuid
join sandbox.sh_bt_active_deals_log adl on od.deal_uuid = adl.deal_uuid
where opt_out_week between current_date-28 and current_date
and adl.product_is_active_flag = 1
and adl.partner_inactive_flag = 0
and od.region = 'NAM'
and od.l2 = 'HBW'
group by 1,2,3,4,5,6,7,8
;


/* distro of nam hbw deals live */
sel grt_l3_cat_description,
    current_booking_solution,
    count(distinct adl.deal_uuid)
from sandbox.sh_bt_active_deals_log adl
left join sh_gcal g on adl.deal_uuid = g.deal_uuid
join user_edwprod.dim_gbl_deal_lob gdl on adl.deal_uuid = gdl.deal_id
join (
    sel cast(dw.week_end as date) wk
    from user_groupondw.dim_day dd
    join user_groupondw.dim_week dw on dd.week_key = dw.week_key
    where wk between current_date-7 and current_date
) dw on adl.load_date = dw.wk
join sh_booking_solution sbs on adl.deal_uuid = sbs.deal_uuid
where product_is_active_flag = 1
and partner_inactive_flag = 0
and adl.country = 'US'
and gdl.grt_l2_cat_description = 'HBW'
group by 1,2;


/* intl query */
sel opt_out_week,
    od.country_code,
    case when l2 in ('F&D','HBW') then l2 else 'Other' end l2,
    l3,
    case when booking_pause_reason is null then 'None' else booking_pause_reason end booking_pause_reason,
    bs.current_booking_solution,
    od.deal_uuid,
    units_sold_2020
from sh_opt_out_deals od
left join sh_booking_solution bs on od.deal_uuid = bs.deal_uuid
join sandbox.sh_bt_active_deals_log adl on od.deal_uuid = adl.deal_uuid
where opt_out_week between current_date-28 and current_date
and adl.product_is_active_flag = 1
and adl.partner_inactive_flag = 0
and od.region = 'INTL'
group by 1,2,3,4,5,6,7,8
;



/* overall pause reasons */
sel gdl.grt_l2_cat_description,
    gdl.country_code,
    pd.pause_reason,
    count(distinct ad.deal_uuid)
from (
    sel deal_uuid, max(cast(dw.week_end as date)) last_wk
    from sandbox.sh_bt_active_deals_log ad
    join user_groupondw.dim_day dd on ad.load_date = dd.day_rw
    join user_groupondw.dim_week dw on dd.week_key = dw.week_key
    group by 1
    having last_wk between current_date -7 and current_date
) ad
join user_edwprod.dim_gbl_deal_lob gdl on ad.deal_uuid = gdl.deal_id
left join user_groupondw.paused_deals pd on ad.deal_uuid = pd.deal_uuid
where gdl.grt_l2_cat_description in ('F&D','HBW','TTD - Live','TTD - Leisure')
group by 1,2,3 order by 1,2,3

/* which deals launched on BT last week */
sel country, l2, count(distinct deal_uuid)
from (
sel ad.country,
    gdl.grt_l2_cat_description l2,
    ad.deal_uuid,
    min(load_date) first_dt
from sandbox.sh_bt_active_deals_log ad
join user_edwprod.dim_gbl_deal_lob gdl on ad.deal_uuid = gdl.deal_id
where partner_inactive_flag = 0
and product_is_active_flag = 1
group by 1,2,3
) t
join user_groupondw.dim_day dd on t.first_dt = dd.day_rw
join user_groupondw.dim_week dw on dd.week_key = dw.week_key
where cast(dw.week_end as date) between current_date-7 and current_date
group by 1,2 order by 1,2;


sel country, count(distinct deal_uuid)
from (
sel ad.country,
    gdl.grt_l2_cat_description l2,
    ad.deal_uuid,
    min(load_date) first_dt
from sandbox.sh_bt_active_deals_log ad
join user_edwprod.dim_gbl_deal_lob gdl on ad.deal_uuid = gdl.deal_id
where partner_inactive_flag = 0
and product_is_active_flag = 1
group by 1,2,3
) t
join user_groupondw.dim_day dd on t.first_dt = dd.day_rw
join user_groupondw.dim_week dw on dd.week_key = dw.week_key
where cast(dw.week_end as date) between current_date-7 and current_date
group by 1 order by 1;


/* which BT deals left Groupon last week */
sel gdl.country_code,
    gdl.grt_l2_cat_description,
    count(distinct t.deal_uuid) n_deals_left
from (
    sel deal_uuid,
        max(load_date) last_dt
    from sandbox.sh_bt_active_deals_log adl
    where product_is_active_flag = 1
    and partner_inactive_flag = 0
    group by 1
) t
join user_groupondw.dim_day dd on t.last_dt = dd.day_rw
join user_groupondw.dim_week dw on dd.week_key = dw.week_key
join user_edwprod.dim_gbl_deal_lob gdl on t.deal_uuid = gdl.deal_id
where cast(dw.week_end as date) between current_date-7 and current_date
group by 1,2 order by 1,2;

sel gdl.country_code,
    count(distinct t.deal_uuid) n_deals_left
from (
    sel deal_uuid,
        max(load_date) last_dt
    from sandbox.sh_bt_active_deals_log adl
    where product_is_active_flag = 1
    and partner_inactive_flag = 0
    group by 1
) t
join user_groupondw.dim_day dd on t.last_dt = dd.day_rw
join user_groupondw.dim_week dw on dd.week_key = dw.week_key
join user_edwprod.dim_gbl_deal_lob gdl on t.deal_uuid = gdl.deal_id
where cast(dw.week_end as date) between current_date-7 and current_date
group by 1 order by 1;

/* which Groupon deals left last week */
sel gdl.country_code,
    gdl.grt_l2_cat_description,
    count(distinct t.deal_uuid) n_deals_left
from (
    sel deal_uuid,
        max(load_date) last_dt
    from user_groupondw.active_deals adl
    group by 1
) t
join user_groupondw.dim_day dd on t.last_dt = dd.day_rw
join user_groupondw.dim_week dw on dd.week_key = dw.week_key
join user_edwprod.dim_gbl_deal_lob gdl on t.deal_uuid = gdl.deal_id
where cast(dw.week_end as date) between current_date-7 and current_date
and gdl.grt_l1_cat_name = 'L1 - Local'
group by 1,2 order by 1,2;

sel gdl.country_code,
    count(distinct t.deal_uuid) n_deals_left
from (
    sel deal_uuid,
        max(load_date) last_dt
    from user_groupondw.active_deals adl
    group by 1
) t
join user_groupondw.dim_day dd on t.last_dt = dd.day_rw
join user_groupondw.dim_week dw on dd.week_key = dw.week_key
join user_edwprod.dim_gbl_deal_lob gdl on t.deal_uuid = gdl.deal_id
where cast(dw.week_end as date) between current_date-7 and current_date
and gdl.grt_l1_cat_name = 'L1 - Local'
group by 1 order by 1;