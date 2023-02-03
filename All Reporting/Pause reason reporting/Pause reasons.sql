

create volatile table sh_bt_paused_reason as (
    sel deal_uuid, max(o2.booking_pause_reason__c) booking_pause_reason, max(sfa.id) account_id
    from user_edwprod.sf_opportunity_2 o2
    join user_edwprod.sf_opportunity_1 o1 on o2.id = o1.id
    join user_edwprod.sf_account sfa on o1.accountid = sfa.id
    group by 1
    having booking_pause_reason is not null
) with data unique primary index (deal_uuid) on commit preserve rows;

select * from sandbox.nvp_agg_reporting order by report_date desc, account_id;

create volatile table sh_launch_date as (
    sel deal_uuid,
        min(load_date) bt_launch_date
    from sandbox.sh_bt_active_deals_log
    where product_is_active_flag = 1
    and partner_inactive_flag = 0
    group by 1
) with data unique primary index (deal_uuid) on commit preserve rows;


create volatile table sh_booking_solution as (
    select o2.deal_uuid,
        max(o1.id) opp_id,
        max(sfa.id) account_id,
        max(case when lower(sfa.scheduler_setup_type) in ('pen & paper','none') then 'pen & paper'
            when sfa.scheduler_setup_type is null then 'no data'
            else 'some booking tool'
            end) current_booking_solution,
        max(sfa.scheduler_setup_type) detailed_booking_solution,
        max(sfa.name) account_name,
        max(company_type) company_type,
        max(o1.division) division,
        max(sfp.full_name) account_owner, 
        max(metal_at_close) metal_segmentation
    from dwh_base_sec_view.sf_opportunity_1 o1
    join dwh_base_sec_view.sf_opportunity_2 o2 on o1.id = o2.id
    join dwh_base_sec_view.sf_account sfa on o1.accountid = sfa.id
    left join user_groupondw.dim_sf_person sfp on sfa.ownerid = sfp.person_id
    left join sandbox.rev_mgmt_deal_attributes mat on o2.deal_uuid = mat.deal_id
    group by o2.deal_uuid
) with data unique primary index (deal_uuid) on commit preserve rows;



create volatile table sh_txns as (
    sel deal_id,
        sum(net_transactions_qty - zdo_net_transactions_qty) units, 
        sum(case when shbt.deal_uuid is not null then net_transactions_qty - zdo_net_transactions_qty end) units_bt, 
        sum(case when a.report_date >= cast(opt_out.opt_out_date as date) - 30 
            then net_transactions_qty - zdo_net_transactions_qty end) units_30_optout,
        sum(case when a.report_date >= cast(opt_out.opt_out_date as date) - 30 
                 and shbt.deal_uuid is not null 
            then net_transactions_qty - zdo_net_transactions_qty end) units_bt_30_optout
    from user_edwprod.agg_gbl_financials_deal as a
    left join (select deal_uuid, load_date
               from sandbox.sh_bt_active_deals_log
               where partner_inactive_flag = 0 or product_is_active_flag = 1
               group by 1,2)
               shbt on a.deal_id = shbt.deal_uuid and a.report_date = cast(shbt.load_date as date)
    left join (select deal_uuid, min(load_date) opt_out_date
               from sandbox.sh_bt_active_deals_log
               where partner_inactive_flag = 1 or product_is_active_flag = 0
               group by 1)
               opt_out on a.deal_id = opt_out.deal_uuid
    where report_date >= '2020-01-01'
    group by 1
) with data unique primary index (deal_id) on commit preserve rows;




create volatile table bt_appointments as (
select 
     a.deal_uuid, 
     sum(case when state = 'confirmed' 
              and cast(substr(created_at, 1,10) as date) >= cast(a.opt_out_date as date) - 30 
              then 1 
              end) appointments_confirmed_30_optout, 
     sum(case when state = 'cancelled' 
              and cast(substr(created_at, 1,10) as date) >= cast(a.opt_out_date as date) - 30 
              then 1 
              end) appointments_cancelled_30_optout
from 
    (select 
       deal_uuid, 
       min(load_date) opt_out_date
     from sandbox.sh_bt_active_deals_log
     where partner_inactive_flag = 1 or product_is_active_flag = 0
     group by 1) as a
     left join sandbox.sh_bt_bookings_rebuild as opt_out on a.deal_uuid = opt_out.deal_uuid
     group by 1
) with data unique primary index (deal_uuid) on commit preserve rows;



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
) with data unique primary index (deal_uuid) on commit preserve rows;


create volatile table sh_l5 as (
    sel doe.product_uuid deal_uuid,
        max(pds.grt_l5_cat_name) l5
    from user_edwprod.dim_offer_ext doe
    join user_dw.v_dim_pds_grt_map pds on doe.pds_cat_id = pds.pds_cat_id
    join user_edwprod.dim_gbl_deal_lob gdl on doe.product_uuid = gdl.deal_id
    where gdl.country_code = 'US'
    group by 1
) with data unique primary index (deal_uuid) on commit preserve rows;




create volatile table sh_opt_out_deals as (
    sel dw.week_end (date) opt_out_week,
        region,
        country_code,
        l2,
        l3,
        l5,
        booking_pause_reason,
        deal_uuid,
        sum(units) units_sold_2020,
        sum(units_bt) units_bt, 
        sum(units_30_optout) units_30_optout,
        sum(units_bt_30_optout) units_bt_30_optout, 
        sum(appointments_confirmed_30_optout) appointments_confirmed_30_optout, 
        sum(appointments_cancelled_30_optout) appointments_cancelled_30_optout
    from 
    (sel    ad.deal_uuid,
            min(load_date) opt_out_date,
            p.booking_pause_reason,
            grt_l2_cat_description l2,
            grt_l3_cat_description l3,
            l.l5,
            case when gdl.country_code = 'US' then 'NAM' else 'INTL' end region,
            gdl.country_code,
            units, 
            units_bt, 
            units_30_optout,
            units_bt_30_optout, 
            appointments_confirmed_30_optout, 
            appointments_cancelled_30_optout
        from sandbox.sh_bt_active_deals_log ad
        left join sh_bt_paused_reason p on ad.deal_uuid = p.deal_uuid
        join user_edwprod.dim_gbl_deal_lob gdl on ad.deal_uuid = gdl.deal_id
        left join sh_txns t on ad.deal_uuid = t.deal_id
        left join bt_appointments apts on ad.deal_uuid = apts.deal_uuid
        left join sh_l5 l on ad.deal_uuid = l.deal_uuid
        where partner_inactive_flag = 1 
              or product_is_active_flag = 0
        group by 1,3,4,5,6,7,8,9,10,11,12,13,14
    ) t
    join user_dw.v_dim_day dd on t.opt_out_date = dd.day_rw
    join user_dw.v_dim_week dw on dd.week_key = dw.week_key
    group by 1,2,3,4,5,6,7,8
) with data unique primary index (deal_uuid) on commit preserve rows;




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

create volatile table nvp_op_rep_cat as(
select
op.opportunity_id,
ros.rep as close_owner,
ros.m1 as close_dsm,
   case
		when ros.team = 'Team - Winbacks' then 'Dedicated'
		when ros.team = 'Getaway' then 'Getaway'
		when ros.title_rw = 'Agent' then 'CS'
		when lower(ros.title_rw) like '%cs rep%' then 'CS'
		when lower(ros.title_rw) like '%GOB%' then 'GOB'
		when ros.title_rw in ('Live Account Manager', 'Live Sales Representative') then  'Live'
		when ros.team = 'Team - FIN' and ros.title_rw = 'Business Development Specialist - FIN' then  'CLO'
		when ros.segment = 'F&D - CLO' then 'CLO'
		when ros.title_rw in ('Strategic Merchant Development Director') then 'Enterprise'
		when lower(ros.title_rw) like '%enterprise%' then 'Enterprise'
		when ros.title_rw like ('Business Development Director%') then 'BDD'
		when ros.title_rw like ('Business Development Manager%') then 'BDM'
		when ros.title_rw like ('Business Development Representative%') then 'BDR'
		when ros.title_rw in ('Merchant Development Director') then 'MDD'
		when ros.title_rw in ('Merchant Development Manager') then 'MDM'
		when ros.title_rw in ('Merchant Development Representative') then 'MDR'
		when ros.title_rw = 'Senior Account Manager' then 'SAM'
		when ros.title_rw in('Merchant Support Place Holder', 'MS Account Manager') then  'MS'
		when lower(ros.title_rw) like 'strategic%' then 'Enterprise'
		when lower(ros.title_rw) like 'national%' then 'Enterprise'
		when lower(ros.title_rw) like 'multi-market%' then 'Enterprise'
		when ros.team = 'Team - Inbound' and ros.title_rw like '%Inbound%' then  'Inbound'
		else ros.title_rw end as rep_grp
from user_groupondw.dim_opportunity op
left join (select distinct ultipro_id, person_key from user_groupondw.dim_sf_person) sfp_opp on op.person_key = sfp_opp.person_key
left join
(
    select
    roster_date,
    emplid,
    rep,
    m1,
    m2,
    m3,
    m4,
    m5,
    team,
    title_rw,
    segment
    from sandbox.ops_roster_all
    qualify row_number() over(partition by roster_date,emplid order by start_date,month_end_date,length(title_rw) desc) = 1
) ros on ros.roster_date = op.close_date and sfp_opp.ultipro_id = ros.emplid
)with data unique primary index (opportunity_id) on commit preserve rows;




create volatile table nvp_action_cohort as 
(select 
   fin.deal_uuid, 
   availability_cohort, 
   availability_cohort2, 
   units_sold_7w, 
   num_dow_total
from 
(select 
   deal_uuid, 
   ROW_NUMBER() over(partition by deal_uuid order by load_week desc) ord,
   availability_cohort, 
   availability_cohort2, 
   units_sold_7w
from sandbox.nvp_hbw_booking_status_deal2) as fin
left join (
      sel deal_uuid,
          load_week,
          num_dow, 
          num_dow_total
          from sandbox.nvp_weekstart_avail
          where country = 'US'
          qualify row_number() over (partition by deal_uuid order by load_week desc) = 1
    ) dal on fin.deal_uuid = dal.deal_uuid
where fin.ord = 1
) with data UNIQUE PRIMARY index (deal_uuid) on commit preserve rows;





drop table sandbox.sh_bt_pause_reasons_wbr;
create table sandbox.sh_bt_pause_reasons_wbr as (
sel distinct
    od.opt_out_week,
    sld.bt_launch_date,
    od.deal_uuid,
    dmm.merchant_uuid,
    bs.opp_id,
    bs.account_id,
    bs.account_name,
    bs.account_owner,
    rep.rep_grp,
    od.l3, 
    od.l5,
    units_sold_2020,
    units_bt,
    units_30_optout,
    units_bt_30_optout,
    od.booking_pause_reason,
    oon.product_opt_out_notes,
    case when g.deal_uuid is not null then 1 else 0 end is_gcal,
    case when sdm.merchant_uuid is not null then 1 else 0 end is_dream,
    bs.current_booking_solution booking_type,
    bs.detailed_booking_solution booking_solution,
    bs.division, 
    bs.metal_segmentation, 
    case when bs.metal_segmentation in ('Silver', 'Gold', 'Platinum') then 'S+' else 'B-' end metal_category, 
    act.availability_cohort, 
    act.availability_cohort2, 
    act.units_sold_7w units_sold_7d,
    act.num_dow_total days_available_before_leaving,
    appointments_confirmed_30_optout,
    appointments_cancelled_30_optout,
    case when top_a.account_id is not null then 1 else 0 end top_account,
    case when dream.account_id is not null then 'Yes' else 'No' end dream_account,
    case 
        when appointments_confirmed_30_optout > 15 then 'a.> 15 appointments confirmed'
        when units_bt_30_optout >= 10 then 'b.>= 10 BT eligible units purchased'
        when act.num_dow_total > 3 then 'c.more than 3 days of availability'
        when appointments_cancelled_30_optout > 15 then 'd. > 15 appointments cancelled'
        when appointments_confirmed_30_optout <= 5 then 'e. <= 5 appointments confirmed'
        else 'no highlights' 
        end
        performance_cohorts
from sh_opt_out_deals od
join sandbox.sh_bt_active_deals_log adl on od.deal_uuid = adl.deal_uuid
join sh_deal_merc_map dmm on od.deal_uuid = dmm.product_uuid
join sh_launch_date sld on od.deal_uuid = sld.deal_uuid
left join sh_gcal g on od.deal_uuid = g.deal_uuid
left join sh_booking_solution bs on od.deal_uuid = bs.deal_uuid
left join sh_opt_out_notes oon on od.deal_uuid = oon.deal_uuid
left join sandbox.sh_dream_merchants sdm on dmm.merchant_uuid = sdm.merchant_uuid
left join nvp_op_rep_cat as rep on substr(bs.opp_id,1,15) = rep.opportunity_id
left join nvp_action_cohort as act on od.deal_uuid = act.deal_uuid
left join sandbox.avb_top_accts_mar2021 as top_a on bs.account_id = top_a.account_id
left join sandbox.ar_hbw_dream_merchants dream on bs.account_id = dream.account_id
where opt_out_week between current_date-28 and current_date
and adl.product_is_active_flag = 1
and adl.partner_inactive_flag = 0
and od.region = 'NAM'
and od.l2 = 'HBW'
) with data no primary index;
grant sel on sandbox.sh_bt_pause_reasons_wbr to public;


 



select * from sandbox.ar_hbw_dream_merchants;



-------ACCOUNT PULLS

select 
    account_id,
    account_name,
    account_owner,
    max(opt_out_week) opt_out_week,
    min(bt_launch_date) bt_launch_date,
    merchant_uuid,
    max(rep_grp) rep_grp,
    max(l3) l3, 
    max(l5) l5,
    sum(units_sold_2020) units_sold_2020,
    sum(units_bt) bt_units_sold,
    max(booking_pause_reason) pause_reasons,
    max(product_opt_out_notes) product_opt_out_notes,
    max(is_gcal) is_gcal,
    max(is_dream) is_dream,
    max(booking_type) booking_type,
    max(booking_solution) booking_solution,
    max(division) division, 
    max(metal_segmentation) metal_segmentation2, 
    case when metal_segmentation2 in ('Silver', 'Gold', 'Platinum') then 'S+' else 'B-' end metal_category, 
    max(availability_cohort) availability_cohort, 
    max(availability_cohort2) availability_cohort2, 
    sum(units_sold_7d) units_sold_7d, 
    max(days_available_before_leaving) days_available_before_leaving, 
    count(distinct deal_uuid) deals_paused
from 
sandbox.sh_bt_pause_reasons_wbr
where opt_out_week = cast('2021-04-04' as date) 
group by 1,2,3,6



-----------------------------------analysis

create volatile table sh_opt_out_deals_analysis as (
    sel dw.week_end (date) opt_out_week,
        region,
        country_code,
        l2,
        l3,
        l5,
        booking_pause_reason,
        deal_uuid,
        sum(units) units_sold_2020,
        sum(units_bt) units_bt, 
        sum(units_bt_30_optout) units_bt_30_optout, 
        sum(appointments_confirmed_30_optout) appointments_confirmed_30_optout, 
        sum(appointments_cancelled_30_optout) appointments_cancelled_30_optout
    from 
    (sel    ad.deal_uuid,
            min(load_date) opt_out_date,
            p.booking_pause_reason,
            grt_l2_cat_description l2,
            grt_l3_cat_description l3,
            l.l5,
            case when gdl.country_code = 'US' then 'NAM' else 'INTL' end region,
            gdl.country_code,
            units, 
            units_bt, 
            units_bt_30_optout, 
            appointments_confirmed_30_optout, 
            appointments_cancelled_30_optout
        from sandbox.sh_bt_active_deals_log ad
        left join sh_bt_paused_reason p on ad.deal_uuid = p.deal_uuid
        join user_edwprod.dim_gbl_deal_lob gdl on ad.deal_uuid = gdl.deal_id
        left join sh_txns t on ad.deal_uuid = t.deal_id
        left join bt_appointments apts on ad.deal_uuid = apts.deal_uuid
        left join sh_l5 l on ad.deal_uuid = l.deal_uuid
        where partner_inactive_flag = 1 
              or product_is_active_flag = 0
              or ad.sold_out = 'true'
        group by 1,3,4,5,6,7,8,9,10,11,12,13
    ) t
    join user_dw.v_dim_day dd on t.opt_out_date = dd.day_rw
    join user_dw.v_dim_week dw on dd.week_key = dw.week_key
    group by 1,2,3,4,5,6,7,8
) with data unique primary index (deal_uuid) on commit preserve rows;

select opt_out_week, count(distinct deal_uuid) from sh_opt_out_deals_analysis where region = 'NAM' group by 1 order by 1 desc;
select opt_out_week, count(distinct deal_uuid) from sh_opt_out_deals where region = 'NAM' group by 1 order by 1 desc;

------------------OLD QUERY


create volatile table sh_bt_paused_reason as (
    sel deal_uuid, max(o2.booking_pause_reason__c) booking_pause_reason, max(sfa.id) account_id
    from user_edwprod.sf_opportunity_2 o2
    join user_edwprod.sf_opportunity_1 o1 on o2.id = o1.id
    join user_edwprod.sf_account sfa on o1.accountid = sfa.id
    group by 1
    having booking_pause_reason is not null
) with data unique primary index (deal_uuid) on commit preserve rows;create volatile table sh_launch_date as (
    sel deal_uuid,
        min(load_date) bt_launch_date
    from sandbox.sh_bt_active_deals_log
    where product_is_active_flag = 1
    and partner_inactive_flag = 0
    group by 1
) with data unique primary index (deal_uuid) on commit preserve rows;create volatile table sh_booking_solution as (
    select o2.deal_uuid,
        max(o1.id) opp_id,
        max(sfa.id) account_id,
        max(case when lower(sfa.scheduler_setup_type) in ('pen & paper','none') then 'pen & paper'
            when sfa.scheduler_setup_type is null then 'no data'
            else 'some booking tool'
            end) current_booking_solution,
        max(sfa.scheduler_setup_type) detailed_booking_solution,
        max(sfa.name) account_name,
        max(company_type) company_type,
        max(o1.division) division,
        max(sfp.full_name) account_owner
    from dwh_base_sec_view.sf_opportunity_1 o1
    join dwh_base_sec_view.sf_opportunity_2 o2 on o1.id = o2.id
    join dwh_base_sec_view.sf_account sfa on o1.accountid = sfa.id
    left join user_groupondw.dim_sf_person sfp on sfa.ownerid = sfp.person_id
    group by o2.deal_uuid
) with data unique primary index (deal_uuid) on commit preserve rows;create volatile table sh_txns as (
    sel deal_id,
        sum(net_transactions_qty - zdo_net_transactions_qty) units
    from user_edwprod.agg_gbl_financials_deal
    where report_date >= '2020-01-01'
    group by 1
) with data unique primary index (deal_id) on commit preserve rows;create volatile table sh_opt_out_notes as (
    sel deal_uuid,
        Product_Opt_Out_Notes__c product_opt_out_notes,
        st.createddate,
        row_number() over (partition by deal_uuid order by st.createddate desc) rownumdesc
    from user_edwprod.sf_task st
    join user_edwprod.sf_opportunity_1 o1 on st.accountid = o1.accountid
    join user_edwprod.sf_opportunity_2 o2 on o1.id = o2.id
    where Product_Opt_Out_Notes__c is not null
    qualify rownumdesc = 1
) with data unique primary index (deal_uuid) on commit preserve rows;create volatile table sh_opt_out_deals as (
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
) with data unique primary index (deal_uuid) on commit preserve rows;create volatile table sh_deal_merc_map as (
    sel product_uuid,
        max(merchant_uuid) merchant_uuid
    from user_edwprod.dim_offer_ext
    group by 1
) with data unique primary index (product_uuid) on commit preserve rows;create volatile table sh_gcal as (
    sel deal_uuid
    from sandbox.sh_bt_active_deals_log_v4
    where has_gcal = 1
    group by 1
) with data unique primary index (deal_uuid) on commit preserve rows;drop table sandbox.sh_bt_pause_reasons_wbr;create table sandbox.sh_bt_pause_reasons_wbr as (
sel distinct
    od.opt_out_week,
    sld.bt_launch_date,
    od.deal_uuid,
    dmm.merchant_uuid,
    bs.opp_id,
    bs.account_id,
    bs.account_name,
    bs.account_owner,
    units_sold_2020,
    od.booking_pause_reason,
    oon.product_opt_out_notes,
    case when g.deal_uuid is not null then 1 else 0 end is_gcal,
    case when sdm.merchant_uuid is not null then 1 else 0 end is_dream,
    bs.current_booking_solution booking_type,
    bs.detailed_booking_solution booking_solution,
    bs.division
from sh_opt_out_deals od
join sandbox.sh_bt_active_deals_log adl on od.deal_uuid = adl.deal_uuid
join sh_deal_merc_map dmm on od.deal_uuid = dmm.product_uuid
join sh_launch_date sld on od.deal_uuid = sld.deal_uuid
left join sh_gcal g on od.deal_uuid = g.deal_uuid
left join sh_booking_solution bs on od.deal_uuid = bs.deal_uuid
left join sh_opt_out_notes oon on od.deal_uuid = oon.deal_uuid
left join sandbox.sh_dream_merchants sdm on dmm.merchant_uuid = sdm.merchant_uuid
where opt_out_week between current_date-28 and current_date
and adl.product_is_active_flag = 1
and adl.partner_inactive_flag = 0
and od.region = 'NAM'
and od.l2 = 'HBW'
) with data no primary index;grant sel on sandbox.sh_bt_pause_reasons_wbr to public