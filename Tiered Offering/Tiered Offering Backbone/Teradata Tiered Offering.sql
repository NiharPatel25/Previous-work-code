show table sandbox.avb_to_t600_hbw;

CREATE MULTISET TABLE sandbox.avb_to_t600_hbw ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      row_rank INTEGER,
      account_id VARCHAR(32) CHARACTER SET LATIN NOT CASESPECIFIC)
PRIMARY INDEX ( account_id );


CREATE MULTISET TABLE sandbox.avb_to_bt_options_live ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      account_id VARCHAR(32) CHARACTER SET LATIN NOT CASESPECIFIC,
      total_options_bookable INTEGER)
PRIMARY INDEX ( account_id );


CREATE MULTISET TABLE sandbox.avb_to_bt_options_live2,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      account_id VARCHAR(32) CHARACTER SET LATIN NOT CASESPECIFIC,
      total_options_bookable INTEGER)
PRIMARY INDEX ( account_id );

delete from sandbox.avb_to_bt_options_live;insert into sandbox.avb_to_bt_options_live
sel
    account_id,
    sum(total_options_bookable) as bk_options
from (
    sel * from sandbox.avb_to_bt_options_live2
    union all 
    sel
        m.account_id,
        count(distinct inventory_id) option_live
    from sandbox.hbw_deals_bookings_flags m
    join user_groupondw.fact_active_deals ad on m.deal_uuid = ad.deal_uuid
    where 
        mbo_flag in (1)
        and m.deal_uuid <> '88610046-a287-48e7-8fd3-e5092b5926f1' -- permalink booking-test-co-5'
        and m.report_date = (sel max(report_date) from sandbox.hbw_deals_bookings_flags)
        and m.report_date = ad.load_date
        and sold_out = 'FALSE' and available_qty > 0
    group by 1
) bk_options
group by 1



----BD_FUNNEL

select
trunc(report_date,'iw')+6 time_period,
vertical,
case when  variant = 'Variant 1' then 'V1' 
     when variant = 'Variant 2' then 'V2' 
     when variant = 'Variant 3' then 'V3' end Variant,
account_owner rep_level,
cast(CURRENT_DATE() as date) as_of_date,
sum(leads) leads,
sum(contract_closed+
    leads_flagged+
    leads_hard_burn+
    leads_soft_burn
    -hard_burn_merchant_moving_forward) as leads_completed,
sum(contract_closed
     +leads_hard_burn
     +leads_soft_burn
     -leads_hard_burn_not_interested_due_to_covid
     -leads_hard_burn_restrictions_COVID
     -leads_hard_burn_temp_closed_due_to_COVID
     -hard_burn_merchant_moving_forward) as leads_workable_completed,
sum(leads_actioned) leads_actioned,
sum(leads_solid) leads_solid,
sum(leads_appt_held) leads_appt_held,
sum(contract_sent) contract_sent,
sum(contract_closed) contract_closed
from sandbox.jl_tableau_tiered_offering
where 
    report_date = trunc(report_date,'iw')+6
    and variant in ('Variant 1','Variant 2','Variant 3')
    and account_owner is not null
    and merchant_type in ('New','Inactive','lead gen')
group by 1,2,3,4,5
order by 1;

select * from sandbox.jl_tableau_tiered_offering;

-----BD Close

select
trunc(closedate,'iw')+6 as close_week,
case when vertical = 'TTD - Leisure' then 'TTD' else vertical end vertical,
case 
	when variant = 1 then 'V1'
	when variant = 2 then 'V2'
	when variant = 3 then 'V3'
end as variant,
case 
	when tier = 1 then '1'
	when tier = 2 then '2'
	when tier = 3 then '1 & 2'
	else 'unknown' end as tier,
count(distinct account_id) as merchants_,
count(1) as deals_closed,
sum(case when launched_flag = 1 then 1 else 0 end) as deals_launched,
sum(options) as options_,
average(deal_discount) as discount_,
average(deal_margin) as margin_,
sum(case when unrestricted_flag = 1 then 1 else 0 end) as unrestricted, 
count(distinct case when metal in ('Nickel', 'Bronze',Null) then account_id end) as Nickel_mn,
count(distinct case when metal in ('Silver', 'Gold','Platinum') then account_id end) as Silver_pl, 
count(distinct case when bookable_at_close = 1 then account_id end) as bookable_merchants
from sandbox.eh_to_closes
where
merchant_type in ('New','Inactive','lead gen')
and vertical in ('HBW','TTD - Leisure','F&D')
and variant in (1,2,3)
group by 1,2,3,4;

select distinct metal from sandbox.eh_to_closes;
---md

select
trunc(closedate,'iw')+6 as close_week,
case when vertical = 'TTD - Leisure' then 'TTD' else vertical end vertical,
case 
	when variant = 1 then 'V1'
	when variant = 2 then 'V2'
	when variant = 3 then 'V3'
end as variant,
case 
	when tier = 1 then '1'
	when tier = 2 then '2'
	when tier = 3 then '1 & 2'
	else 'unknown' end as tier,
count(distinct account_id) as merchants_,
count(1) as deals_closed,
sum(case when launched_flag = 1 then 1 else 0 end) as deals_launched,
sum(options) as options_,
average(deal_discount) as discount_,
average(deal_margin) as margin_,
sum(case when unrestricted_flag = 1 then 1 else 0 end) as unrestricted, 
count(distinct case when metal in ('Nickel', 'Bronze',Null) then account_id end) as Nickel_mn,
count(distinct case when metal in ('Silver', 'Gold','Platinum') then account_id end) as Silver_pl, 
count(distinct case when bookable_at_close = 1 then account_id end) as bookable_merchants
from sandbox.eh_to_closes
where
merchant_type in ('Active')
and vertical in ('HBW','TTD - Leisure','F&D')
and variant in (1,2,3)
group by 1,2,3,4;



-------MD NOT LIVE


select
dy.day_rw,
a.vertical,
a.variant,
-- these numbers are cumulative from 7/19
count(distinct a.account_id) as merchants,
count(distinct c.account_id) as winbacks,
sum(coalesce(c.services,0)) as services,
sum(coalesce(c.options,0)) as options,
sum(case when coalesce(c.unrestricted_flag,0) = 1 then coalesce(c.services,0) else 0 end) as unrest_services
from
(
	select
	a.vertical,
	'V' || trim(a.test_group_hierarchy) as variant,
	case when b.account_id is null then 'Active - Not Live' else 'Active - Live' end as merchant_type,
	a.account_id
	from
	(
		select
		*
		from sandbox.eh_greenlist_detail
		where merchant_type = 'Active'
		and test_group_hierarchy in ('1','2','3')
		and vertical in ('F&D','HBW','TTD')
	) a
	left join 
	(
		select
		account_id,
		sum(deals_live) as deals
		from sandbox.sup_analytics_deal_counts_final
		where report_date = '2020-07-19'
		and deals_live > 0
		group by 1
	) b on b.account_id = a.account_id
	where b.account_id is null
	group by 1,2,3,4
) a
left join (select day_rw from user_groupondw.dim_day where day_of_week_num = 7 and day_rw >= '2020-07-26') dy on dy.day_rw >= '2020-07-19'
left join
(
	select
	da.account_id,
	a.load_date as report_date,
	case
	 when COALESCE(dres.new_customer_res, 0) > 0 then 0
     when COALESCE(dres.active_within_res, 0) > 0 then 0
     when COALESCE(dres.num_guests_res, 0) > 0 then 0
     when COALESCE(dres.pref_guests_res, 0) > 0 then 0
     when COALESCE(dores.dt_time_res, 0) > 0 then 0
	 when coalesce(dores.restricted,1) = 0 then 1
	 else 0
	end as unrestricted_flag,
	count(distinct a.deal_uuid) as services,
	count(distinct a.inventory_id) as options
	from user_groupondw.fact_active_deals a
	left join sandbox.rev_mgmt_deal_attributes da on da.deal_id = a.deal_uuid
	left join user_groupondw.dim_deal dd on dd.uuid = da.deal_id
	LEFT JOIN (SELECT DISTINCT uuid AS deal_option_uuid, inventory_product_id FROM user_groupondw.dim_deal_option) ddo on ddo.inventory_product_id = a.inventory_id
	left join sandbox.kg_deal_structure_final dsf on dsf.deal_uuid = a.deal_uuid
	left join 
	(
		select distinct
		deal_option_uuid,
		deal_uuid,
		buyer_max_res,
		dt_time_res,
		buyer_max,
		repurchase_control,
		case when coalesce(buyer_max,0) > 0 and coalesce(repurchase_control,0) > 0 and coalesce(buyer_max,0) >= coalesce(repurchase_control,0) then 0 else 1 end as restricted
		FROM sandbox.temp_to_do_restrictions
		where deal_option_uuid is not null
	) dores on dores.deal_option_uuid = ddo.deal_option_uuid
	LEFT JOIN 
	(
		SELECT
		deal_uuid,
		gen_spend,
		new_customer_res,
		active_within_res,
		repurchase_control_res,
		num_guests_res,
		pref_guests_res
		FROM 
		( 
			SELECT
		       deal_uuid,
		       gen_spend,
		       new_customer_res,
		       active_within_res,
		       repurchase_control_res,
		       num_guests_res,
		       pref_guests_res,
		       ROW_NUMBER() OVER (PARTITION BY deal_uuid ORDER BY deal_option_uuid, buyer_max DESC) AS pick_one
			FROM sandbox.temp_to_do_restrictions
		) a
		WHERE pick_one = 1
	) dres ON a.deal_uuid = dres.deal_uuid
	left join user_groupondw.dim_day dy on dy.day_rw = a.load_date
	where a.load_date >= '2020-07-19'
	and a.sold_out = 'false'
	and dy.day_of_week_num = 7
	and da.account_id is not null
	group by 1,2,3
) c on c.account_id = a.account_id and c.report_date = dy.day_rw
group by 1,2,3
order by 1;

-------LIVE 

select
dy.day_rw,
a.vertical,
a.variant,
-- these numbers are cumulative from 7/19
count(distinct a.account_id) as merchants,
sum(coalesce(c.services,0)) as services,
sum(coalesce(c.options,0)) as options,
sum(case when coalesce(c.unrestricted_flag,0) = 1 then coalesce(c.services,0) else 0 end) as unrest_services
from
(
	select
	a.vertical,
	'V' || trim(a.test_group_hierarchy) as variant,
	case when b.account_id is null then 'Active - Not Live' else 'Active - Live' end as merchant_type,
	a.account_id
	from
	(
		select
		*
		from sandbox.eh_greenlist_detail
		where merchant_type = 'Active'
		and test_group_hierarchy in ('1','2','3')
		and vertical in ('F&D','HBW','TTD')
	) a
	left join 
	(
		select
		account_id,
		sum(deals_live) as deals
		from sandbox.sup_analytics_deal_counts_final
		where report_date = '2020-07-19'
		and deals_live > 0
		group by 1
	) b on b.account_id = a.account_id
	where b.account_id is not null
	group by 1,2,3,4
) a
left join (select day_rw from user_groupondw.dim_day where day_of_week_num = 7 and day_rw >= '2020-07-26') dy on dy.day_rw >= '2020-07-19'
left join
(
	select
	da.account_id,
	a.load_date as report_date,
	case
	 when COALESCE(dres.new_customer_res, 0) > 0 then 0
     when COALESCE(dres.active_within_res, 0) > 0 then 0
     when COALESCE(dres.num_guests_res, 0) > 0 then 0
     when COALESCE(dres.pref_guests_res, 0) > 0 then 0
     when COALESCE(dores.dt_time_res, 0) > 0 then 0
	 when coalesce(dores.restricted,1) = 0 then 1
	 else 0
	end as unrestricted_flag,
	count(distinct a.deal_uuid) as services,
	count(distinct a.inventory_id) as options
	from user_groupondw.fact_active_deals a
	left join sandbox.rev_mgmt_deal_attributes da on da.deal_id = a.deal_uuid
	left join user_groupondw.dim_deal dd on dd.uuid = da.deal_id
	LEFT JOIN (SELECT DISTINCT uuid AS deal_option_uuid, inventory_product_id FROM user_groupondw.dim_deal_option) ddo on ddo.inventory_product_id = a.inventory_id
	left join sandbox.kg_deal_structure_final dsf on dsf.deal_uuid = a.deal_uuid
	left join 
	(
		select distinct
		deal_option_uuid,
		deal_uuid,
		buyer_max_res,
		dt_time_res,
		buyer_max,
		repurchase_control,
		case when coalesce(buyer_max,0) > 0 and coalesce(repurchase_control,0) > 0 and coalesce(buyer_max,0) >= coalesce(repurchase_control,0) then 0 else 1 end as restricted
		FROM sandbox.temp_to_do_restrictions
		where deal_option_uuid is not null
	) dores on dores.deal_option_uuid = ddo.deal_option_uuid
	LEFT JOIN 
	(
		SELECT
		deal_uuid,
		gen_spend,
		new_customer_res,
		active_within_res,
		repurchase_control_res,
		num_guests_res,
		pref_guests_res
		FROM 
		( 
			SELECT
		       deal_uuid,
		       gen_spend,
		       new_customer_res,
		       active_within_res,
		       repurchase_control_res,
		       num_guests_res,
		       pref_guests_res,
		       ROW_NUMBER() OVER (PARTITION BY deal_uuid ORDER BY deal_option_uuid, buyer_max DESC) AS pick_one
			FROM sandbox.temp_to_do_restrictions
		) a
		WHERE pick_one = 1
	) dres ON a.deal_uuid = dres.deal_uuid
	left join user_groupondw.dim_day dy on dy.day_rw = a.load_date
	where a.load_date >= '2020-07-19'
	and a.sold_out = 'false'
	and dy.day_of_week_num = 7
	and da.account_id is not null
	group by 1,2,3
) c on c.account_id = a.account_id and c.report_date = dy.day_rw
group by 1,2,3
order by 1;



-----


select
trunc(closedate,'iw')+6 as close_week,
case when vertical = 'TTD - Leisure' then 'TTD' else vertical end vertical,
case 
	when variant = 1 then 'V1'
	when variant = 2 then 'V2'
	when variant = 3 then 'V3'
end as variant,
case 
	when tier = 1 then '1'
	when tier = 2 then '2'
	when tier = 3 then '1 & 2'
	else 'unknown' end as tier,
count(distinct account_id) as merchants_,
count(1) as deals_closed,
sum(case when launched_flag = 1 then 1 else 0 end) as deals_launched,
sum(options) as options_,
average(deal_discount) as discount_,
average(deal_margin) as margin_,
sum(case when unrestricted_flag = 1 then 1 else 0 end) as unrestricted
from sandbox.eh_to_closes
where
merchant_type in ('New','Inactive','lead gen')
and vertical in ('HBW','TTD - Leisure','F&D')
and variant in (1,2,3)
group by 1,2,3,4;


-----Sales output


select
	trunc(report_date,'iw')+6 week_ending,
	vertical,
	variant,
	account_owner,
	sum(contract_closed+leads_flagged+leads_hard_burn+leads_soft_burn) as leads_completed,
	sum(contract_closed+leads_hard_burn+leads_soft_burn-leads_hard_burn_not_interested_due_to_covid-leads_hard_burn_restrictions_COVID
	  -leads_hard_burn_temp_closed_due_to_COVID) as workable_leads_completed,
	sum(leads_flagged) as leads_flagged,
	sum(contract_closed) as leads_closed
from sandbox.jl_tableau_tiered_offering
where 
	report_date = trunc(report_date,'iw')+6
	and variant in ('Variant 1','Variant 2','Variant 3')
	and account_owner is not null
	and merchant_type in ('New','Inactive','lead gen')
group by 1,2,3,4;

select
	trunc(report_date,'iw')+6 week_ending,
	vertical,
	variant,
	case 
		when rep_title = 'BDD' and rep_previous_role in ('Getaways','Live') then 'BDD Live/GA'
		when rep_title = 'BDM' and rep_previous_role in ('Getaways','Live') then 'BDM Live/GA'
		when rep_title = 'BDR' and rep_previous_role in ('Getaways','Live') then 'BDR Live/GA'
		else rep_title
	end as account_owner,
	sum(contract_closed+leads_flagged+leads_hard_burn+leads_soft_burn) as leads_completed,
	sum(contract_closed+leads_hard_burn+leads_soft_burn-leads_hard_burn_not_interested_due_to_covid-leads_hard_burn_restrictions_COVID
	 -leads_hard_burn_temp_closed_due_to_COVID) as workable_leads_completed,
	sum(leads_flagged) as leads_flagged,
	sum(contract_closed) as leads_closed
from sandbox.jl_tableau_tiered_offering
where 
	report_date = trunc(report_date,'iw')+6
	and variant in ('Variant 1','Variant 2','Variant 3')
	and rep_title is not null
	and rep_title <> 'GOB'
	and merchant_type in ('New','Inactive','lead gen')
group by 1,2,3, 
case 
		when rep_title = 'BDD' and rep_previous_role in ('Getaways','Live') then 'BDD Live/GA'
		when rep_title = 'BDM' and rep_previous_role in ('Getaways','Live') then 'BDM Live/GA'
		when rep_title = 'BDR' and rep_previous_role in ('Getaways','Live') then 'BDR Live/GA'
		else rep_title
	end 
order by week_ending, vertical, variant, account_owner;

----BD FUNNEL COPIED

select
trunc(report_date,'iw')+6 time_period,
vertical,
case when  variant = 'Variant 1' then 'V1' 
     when variant = 'Variant 2' then 'V2' 
     when variant = 'Variant 3' then 'V3' end Variant,
account_owner rep_level,
cast(CURRENT_DATE() as date) as_of_date,
sum(leads) leads,
sum(contract_closed+
    leads_flagged+
    leads_hard_burn+
    leads_soft_burn
    -hard_burn_merchant_moving_forward) as leads_completed,
sum(contract_closed
     +leads_hard_burn
     +leads_soft_burn
     -leads_hard_burn_not_interested_due_to_covid
     -leads_hard_burn_restrictions_COVID
     -leads_hard_burn_temp_closed_due_to_COVID
     -hard_burn_merchant_moving_forward) as leads_workable_completed,
sum(leads_actioned) leads_actioned,
sum(leads_solid) leads_solid,
sum(leads_appt_held) leads_appt_held,
sum(contract_sent) contract_sent,
sum(contract_closed) contract_closed
from sandbox.jl_tableau_tiered_offering
where 
    report_date = trunc(report_date,'iw')+6
    and variant in ('Variant 1','Variant 2','Variant 3')
    and account_owner is not null
    and merchant_type in ('New','Inactive','lead gen')
group by 1,2,3,4,5
order by 1;