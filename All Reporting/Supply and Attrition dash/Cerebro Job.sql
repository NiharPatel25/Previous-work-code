USE grp_gdoop_bizops_db;

create TEMPORARY table jk_booking_solution as 
    select o2.deal_uuid,
        max(sfa.id) sf_account_id,
        max(case when lower(sfa.scheduler_setup_type) = 'pen & paper' then 'pen & paper'
            when sfa.scheduler_setup_type is null then 'no data'
            else 'some booking tool'
            end) current_booking_solution,
        max(sfa.name) account_name,
        max(company_type) company_type
    from dwh_base_sec_view.opportunity_1 o1
    join dwh_base_sec_view.opportunity_2 o2 on o1.id = o2.id
    join dwh_base_sec_view.sf_account sfa on o1.accountid = sfa.id
    group by o2.deal_uuid;

----------
   
create temporary table booking_scope_deals as 
select 
t.deal_uuid,to_date(dw.week_end)  as week_end_date,
date_sub(to_date(dw.week_end) , 7) previous_week,
date_add(to_date(dw.week_end ), 7) next_week,
max(is_bookable) as is_bookable,
max(partner_inactive_flag) as partner_inactive_flag,
max(product_is_active_flag) as product_is_active_flag,
max(t.country) as country,
max(case when gdl.country_code = 'US' then 'NAM' else 'INTL' end) region,
max(grt_l1_cat_name) l1,
max(grt_l2_cat_description) l2,
max(grt_l3_cat_description) l3,
max(bs.current_booking_solution) as current_booking_solution, 
max(case when gcal.has_gcal = 1 then 1 else 0 end) gcal_deal
from grp_gdoop_bizops_db.sh_bt_active_deals_log t
join(
    select deal_uuid, load_date
    from user_groupondw.active_deals
    where sold_out = 'false' 
    and available_qty > 0
    and load_date >= '2020-01-01'
    group by deal_uuid, load_date
) ad on t.deal_uuid = ad.deal_uuid and ad.load_date = t.load_date
join user_dw.v_dim_week dw on to_date(dw.week_end ) =t.load_date
join user_edwprod.dim_gbl_deal_lob gdl on gdl.deal_id = t.deal_uuid
join (select deal_uuid, current_booking_solution
     from jk_booking_solution
     )as bs on bs.deal_uuid = t.deal_uuid
left join 
     (select deal_uuid, max(has_gcal) has_gcal from grp_gdoop_bizops_db.sh_bt_active_deals_log_v4 group by deal_uuid) as gcal on t.deal_uuid = gcal.deal_uuid
where partner_inactive_flag = 0 
     and product_is_active_flag = 1
group by t.deal_uuid,to_date(dw.week_end),date_sub(to_date(dw.week_end) , 7) ,date_add(to_date(dw.week_end ), 7);



-------------

drop table grp_gdoop_bizops_db.jk_bt_attrition_agg purge;

create table grp_gdoop_bizops_db.jk_bt_attrition_agg stored as orc as
select
live.week_end_date,
live.region,
live.country,
live.l1,
live.l2,
live.l3,
live.current_booking_solution,
sum(live.accts) live_deals,
sum(adds.accts) added_deals,
sum(lost.accts) lost_deals,
sum(lost.left_bt_accts) left_bt_deals,
sum(lost.left_grpn_accts) left_grpn_deals,
sum(adds.gcal_accts_adds) gcal_added_deals,
sum(lost.gcal_accts_lost) lost_gcal_deals,
sum(lost.gcal_left_bt_accts) gcal_left_bt_deals,
sum(lost.gcal_left_grpn_accts) gcal_left_grpn_deals
from (
select week_end_date,
       region,
       country,
       l1,
       l2,
       l3,
       current_booking_solution,
       count(distinct deal_uuid) accts
from booking_scope_deals 
group by 
       week_end_date, 
       region,
       country, 
       l1, l2, 
       l3, 
       current_booking_solution
) as live
left join (
select 
     bsd.week_end_date,
     bsd.region,
     bsd.country,
     bsd.l1,
     bsd.l2,
     bsd.l3,
     bsd.current_booking_solution,
     count(distinct bsd.deal_uuid) accts, 
     count(distinct case when bsd.gcal_deal = 1 then bsd.deal_uuid end) gcal_accts_adds
from booking_scope_deals bsd
     left join booking_scope_deals bsd2 on cast (bsd.week_end_date as date)= cast(bsd2.next_week as date) and bsd.deal_uuid = bsd2.deal_uuid
     where bsd2.deal_uuid is null
     group by 
     bsd.week_end_date, 
     bsd.region, 
     bsd.country,
     bsd.l1, 
     bsd.l2, 
     bsd.l3, 
     bsd.current_booking_solution
     ) as adds on live.week_end_date = adds.week_end_date and live.region = adds.region and live.country = adds.country
               and live.l1 = adds.l1 and live.l2 = adds.l2 and live.l3 = adds.l3 and live.current_booking_solution = adds.current_booking_solution 
left join 
(select
     date_add (bds.week_end_date ,7) as week_end_date,
     bds.region,
     bds.country,
     bds.l1,
     bds.l2,
     bds.l3,
     bds.current_booking_solution,
     count(distinct bds.deal_uuid) accts,
     count(distinct case when btad.partner_inactive_flag = 1 or btad.product_is_active_flag = 0 then bds.deal_uuid end) left_bt_accts,
     count(distinct case when btad.partner_inactive_flag is null then bds.deal_uuid end) left_grpn_accts, 
     count(distinct case when bds.gcal_deal = 1 then bds.deal_uuid end) gcal_accts_lost,
     count(distinct case when (btad.partner_inactive_flag = 1 or btad.product_is_active_flag = 0) and bds.gcal_deal = 1 then bds.deal_uuid end) gcal_left_bt_accts,
     count(distinct case when btad.partner_inactive_flag is null and bds.gcal_deal = 1 then bds.deal_uuid end) gcal_left_grpn_accts
from booking_scope_deals bds
     left join booking_scope_deals bds3 on cast (bds.week_end_date as date)= cast(bds3.previous_week as date) and bds.deal_uuid = bds3.deal_uuid
     left join grp_gdoop_bizops_db.sh_bt_active_deals_log btad on date_add (bds.week_end_date ,7) = btad.load_date and bds.deal_uuid = btad.deal_uuid
     where bds3.deal_uuid is null
     group by bds.week_end_date, bds.region, bds.country, bds.l1, bds.l2, bds.l3, bds.current_booking_solution
) lost on live.week_end_date = lost.week_end_date and live.region = lost.region and live.country = lost.country
       and live.l1 = lost.l1 and live.l2 = lost.l2 and live.l3 = lost.l3 and live.current_booking_solution = lost.current_booking_solution
group by 
    live.week_end_date,
    live.region,
    live.country,
    live.l1,
    live.l2,
    live.l3,
    live.current_booking_solution


