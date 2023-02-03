create volatile table sh_bt_paused_reason as (
    sel deal_uuid, max(o2.booking_pause_reason__c) booking_pause_reason, max(sfa.id) account_id
    from user_edwprod.sf_opportunity_2 o2
    join user_edwprod.sf_opportunity_1 o1 on o2.id = o1.id
    join user_edwprod.sf_account sfa on o1.accountid = sfa.id
    group by 1
    having booking_pause_reason is not null
) with data unique primary index (deal_uuid) on commit preserve rows;


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


select * from user_edwprod.agg_gbl_financials_deal;


drop table sh_txns;
create volatile table sh_txns as (
    sel deal_id,
        sum(net_transactions_qty - zdo_net_transactions_qty) units, 
        sum(case when shbt.deal_uuid is not null then net_transactions_qty - zdo_net_transactions_qty end) units_bt,
        sum(case when a.report_date >= ld.bt_launch_date then net_transactions_qty - zdo_net_transactions_qty end) units_min_bt
    from user_edwprod.agg_gbl_financials_deal as a
    left join (select deal_uuid, load_date 
               from sandbox.sh_bt_active_deals_log
               where partner_inactive_flag = 0 or product_is_active_flag = 1
               group by 1,2)
               shbt on a.deal_id = shbt.deal_uuid and a.report_date = shbt.load_date
    left join sh_launch_date ld on a.deal_id = ld.deal_uuid
    where report_date >= '2020-01-01'
    group by 1
) with data unique primary index (deal_id) on commit preserve rows;

drop table sh_opt_out_notes;
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

drop table sh_l5;
create volatile table sh_l5 as (
    sel doe.product_uuid deal_uuid,
        max(pds.grt_l5_cat_name) l5
    from user_edwprod.dim_offer_ext doe
    join user_dw.v_dim_pds_grt_map pds on doe.pds_cat_id = pds.pds_cat_id
    join user_edwprod.dim_gbl_deal_lob gdl on doe.product_uuid = gdl.deal_id
    where gdl.country_code = 'US'
    group by 1
) with data unique primary index (deal_uuid) on commit preserve rows;


drop table sh_opt_out_deals;
create volatile table sh_opt_out_deals as (
    sel dw.week_end (date) opt_out_week,
        region,
        country_code,
        l2,
        l3,
        l5,
        booking_pause_reason,
        deal_uuid,
        sum(units_bt) units_bt, 
        sum(units_min_bt) units_min_bt
    from (
        sel ad.deal_uuid,
            min(load_date) opt_out_date,
            p.booking_pause_reason,
            grt_l2_cat_description l2,
            grt_l3_cat_description l3,
            l.l5,
            case when gdl.country_code = 'US' then 'NAM' else 'INTL' end region,
            gdl.country_code,
            units, 
            units_bt, 
            units_min_bt
        from sandbox.sh_bt_active_deals_log ad
        left join sh_bt_paused_reason p on ad.deal_uuid = p.deal_uuid
        join user_edwprod.dim_gbl_deal_lob gdl on ad.deal_uuid = gdl.deal_id
        left join sh_txns t on ad.deal_uuid = t.deal_id
        left join sh_l5 l on ad.deal_uuid = l.deal_uuid
        where partner_inactive_flag = 1 or product_is_active_flag = 0
        group by 1,3,4,5,6,7,8,9,10,11
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


drop table sandbox.sh_bt_pause_reasons_temp;
create table sandbox.sh_bt_pause_reasons_temp as (
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
    units_bt units_bt,
    od.booking_pause_reason,
    oon.product_opt_out_notes,
    case when g.deal_uuid is not null then 1 else 0 end is_gcal,
    case when sdm.merchant_uuid is not null then 1 else 0 end is_dream,
    bs.current_booking_solution booking_type,
    bs.detailed_booking_solution booking_solution,
    bs.division, 
    bs.metal_segmentation, 
    case when bs.metal_segmentation in ('Silver', 'Gold', 'Platinum') then 'S+' else 'B-' end metal_category
from sh_opt_out_deals od
join sandbox.sh_bt_active_deals_log adl on od.deal_uuid = adl.deal_uuid
join sh_deal_merc_map dmm on od.deal_uuid = dmm.product_uuid
join sh_launch_date sld on od.deal_uuid = sld.deal_uuid
left join sh_gcal g on od.deal_uuid = g.deal_uuid
left join sh_booking_solution bs on od.deal_uuid = bs.deal_uuid
left join sh_opt_out_notes oon on od.deal_uuid = oon.deal_uuid
left join sandbox.sh_dream_merchants sdm on dmm.merchant_uuid = sdm.merchant_uuid
left join nvp_op_rep_cat as rep on substr(bs.opp_id,1,15) = rep.opportunity_id
where opt_out_week >= cast('2020-01-01' as date)
and adl.product_is_active_flag = 1
and adl.partner_inactive_flag = 0
and od.region = 'NAM'
and od.l2 = 'HBW'
) with data no primary index;
grant sel on sandbox.sh_bt_pause_reasons_wbr to public;


select sum(units_bt) from sandbox.sh_bt_pause_reasons_temp where opt_out_week <= cast('2021-03-01' as date);

select sum(units_bt), sum(units_min_bt) from sandbox.sh_bt_pause_reasons_temp;

select 
    gdl.grt_l2_cat_description,
    sum(units_bt) 
from sh_txns od
   join user_edwprod.dim_gbl_deal_lob gdl on od.deal_id = gdl.deal_id
where 
gdl.country_code = 'US'
group by 1
order by 2 desc;


