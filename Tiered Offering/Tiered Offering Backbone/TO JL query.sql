---------------------------------------------------------------------------- step 0: replace date_action, date_solid and date_appt columns with Eric's new logic
select * from sandbox.ops_glm_supply_funnel_base  where account_id = '001C0000011gWfNIAU';

drop table sandbox.ops_glm_supply_funnel_base;

create multiset table sandbox.ops_glm_supply_funnel_base as (
select  a.*, b. date_of_action_eh, date_of_solid_eh, date_of_appt_eh
from sandbox.ops_glm_supply_funnel as a 
left join 
(
select
account_id,
min(case when Total_Touches >= 1 then day_rw else null end) as date_of_action_eh,
min(case when Solids >= 1 then day_rw else null end) as date_of_solid_eh,
min(case when Appointments_Held >= 1 then day_rw else null end) as date_of_appt_eh
from sandbox.ops_glm_supply_activity
where day_rw >= '2020-07-20'
group by 1
) as b 
on a.account_id = b.account_id 
) with data primary index(account_id);


---------------------------------------------------------------------------- step 1: create soft burn logic

drop table sandbox.jl_soft_burn;

select * from sandbox.jl_soft_burn where account_id = '001C000001ehHQEIA2';

create multiset table sandbox.jl_soft_burn as (
select distinct 
account_id, 
rep_emplid, 
report_date as soft_burn_date
from
(
				select  account_id, 
				rep_emplid, 
				report_date,
				assign_date, 
				days_assigned, 
				total_touches,
				sum(total_touches) over(partition by account_id, rep_emplid  order by report_date rows between unbounded preceding and current row) as touches_cumulative
				from
				(
						select distinct 
						a.account_id,
						rep_emplid,
						a.report_date, 
						b.assign_date, 
						a.report_date - b.assign_date as days_assigned,
						c.total_touches

						from
						(
						select
						a.*,
						dy.day_rw as report_date
						from sandbox.eh_greenlist_detail a
						left join user_groupondw.dim_day dy on dy.day_rw between '2020-07-20' and current_date - 1
						) as a
						left join 
						(
						select * 
						from sandbox.ops_glm_supply_funnel_base 
						where date_of_action_eh >= '2020-07-20' 
						) as b
						on a.account_id = b.account_id
						left join sandbox.ops_glm_supply_activity as c
						on  b.account_id = c.account_id and b.rep_emplid = c.emplid and c.day_rw = a.report_date
				) as base
) as main
where days_assigned >= 14 and touches_cumulative >= 8
qualify row_number() over(partition by account_id, rep_emplid order by report_date asc) = 1
) with data primary index (account_id, soft_burn_date);

---------------------------------------------------------------------------- step 2: add soft burn info to Nick's table as a new table 

select* from sandbox.jl_ops_glm_supply_funnel where account_id = '001C000001ehHQEIA2';

drop table sandbox.jl_ops_glm_supply_funnel;

create multiset table sandbox.jl_ops_glm_supply_funnel as (
select a.*, b.soft_burn_date
from sandbox.ops_glm_supply_funnel_base as a 
left join sandbox.jl_soft_burn as b
on a.account_id  = b.account_id and a.rep_emplid = b.rep_emplid
) with data primary index (account_id, rep_title);


---------------------------------------------------------------------------step 2.1: day level for greenlist 

create multiset table sandbox.jl_gl_days as (
select
a.*,
dy.day_rw as report_date
from sandbox.eh_greenlist_detail a
left join user_groupondw.dim_day dy on dy.day_rw between '2020-07-20' and current_date - 1
) with data primary index(account_id, report_date);


---------------------------------------------------------------------------step 2.2 Rep and DSM for each account 

--drop table sandbox.jl_rep_dsm;
select * from sandbox.jl_rep_dsm;

create multiset table sandbox.jl_rep_dsm as (
select a.*, b.DSM
from
(
select distinct account_id, rep_title as account_owner, rep_name--, m1 as DSM
from sandbox.jl_ops_glm_supply_funnel 
qualify row_number() over(partition by account_id, rep_name order by  assign_date desc) = 1
) as a
join 
(
select distinct rep_name, m1 as DSM
from sandbox.jl_ops_glm_supply_funnel 
qualify row_number() over(partition by rep_name order by  assign_date desc) = 1
) as b
on a.rep_name = b.rep_name
) with data primary index (account_id);



---------------------------------------------------------------------------- step 3: create cumulative base table 

select * from sandbox.jl_tiered_base where hard_burn_merchant_moving_forward_ = 1;
drop table sandbox.jl_tiered_base;

create multiset table sandbox.jl_tiered_base as (
select 
a.report_date, 
a.account_id,
a.vertical, 
a.division as market,
case when a.test_group_hierarchy = '1' then 'Variant 1' 
when a.test_group_hierarchy = '2' then 'Variant 2'
when a.test_group_hierarchy = '3' then 'Variant 3'
when a.test_group_hierarchy = '5' then 'COVID'
else 'BAU' end as variant,
a.metal,
a.msa_flag,
a.merchant_type, 
c.account_owner,
c.rep_name,
c.DSM,
sum(case when b.assign_date <= a.report_date then 1 else 0 end) as assign_, 
sum(case when b.date_of_action_eh <= a.report_date and  b.date_of_action_eh >= '2020-07-20' then 1 else 0 end) as actioned_,
sum(case when b.date_of_solid_eh  <= a.report_date and  b.date_of_solid_eh >= '2020-07-20' then 1 else 0 end) as solid_,
sum(case when b.date_of_appt_eh <= a.report_date and  b.date_of_appt_eh >= '2020-07-20' then 1 else 0 end) as appt_held_,
sum(case when b.date_of_cs <= a.report_date and b.date_of_cs >= '2020-07-20' then 1 else 0 end) as contract_sent_,
sum(case when b.date_closed <= a.report_date and b.date_closed>= '2020-07-20' then 1 else 0 end) as contract_closed_,
sum(case when b.flag_date <= a.report_date and b.flag_date >= '2020-07-20' then 1 else 0 end) as flagged_,
sum(case when b.road_block_date <= a.report_date and b.road_block_date>= '2020-07-20' then 1 else 0 end) as hard_burn_,
sum(case when b.road_block_date <= a.report_date and b.road_block_date>= '2020-07-20' and road_block = 'Not interested due to COVID' then 1 else 0 end) as hard_burn_not_interested_due_to_COVID_,
sum(case when b.road_block_date <= a.report_date and b.road_block_date>= '2020-07-20' and road_block = 'Restrictions - Peak/Off Peak - COVID-related capacity' then 1 else 0 end) as hard_burn_restrictions_COVID_,
sum(case when b.road_block_date <= a.report_date and b.road_block_date>= '2020-07-20' and road_block = 'Temp closed due to COVID' then 1 else 0 end) as hard_burn_temp_closed_due_to_COVID_,
sum(case when b.road_block_date <= a.report_date and b.road_block_date>= '2020-07-20' and road_block = 'Merchant Moving Forward - Working on Details' then 1 else 0 end) as hard_burn_merchant_moving_forward_,
sum(case when b.soft_burn_date <= a.report_date and b.soft_burn_date>= '2020-07-20' then 1 else 0 end) as soft_burn_
from
sandbox.jl_gl_days as a

left join 

(
select *
from sandbox.jl_ops_glm_supply_funnel 
/*where --date_actioned >= '2020-07-20'
rep_team like '%Team - TO%'*/
) as b
on a.account_id = b.account_id

left join
sandbox.jl_rep_dsm as c
on b.account_id = c.account_id
group by 1,2,3,4,5,6,7, 8, 9, 10, 11
) with data primary index (report_date, account_id, vertical, market, variant, metal, msa_flag, account_owner, rep_name, DSM);





---------------------------------------------------------------------------- step 4: tier info
drop table sandbox.jl_tier_info;

select* from sandbox.jl_tier_info where account_id = '0013c00001pbjM2AAI';

create multiset table sandbox.jl_tier_info as (
select distinct op_close_date, account_id, 1 as tier_2_flag
from sandbox.sup_analytics_dim_deal_tier
where op_close_date >= '2020-07-20' and tier >= 2 
) with data primary index(op_close_date, account_id, tier_2_flag);


---------------------------------------------------------------------------- step 5: remove duplicates and add tier_2_flag
select * from sandbox.jl_tiered_no_dup where account_id = '0013c00001pbjM2AAI';

drop table sandbox.jl_tiered_no_dup;

create multiset table sandbox.jl_tiered_no_dup as (
select distinct a.*,
case when a.contract_closed_=1 and b.tier_2_flag = 1 then 1 else 0 end as new_tier_2_flag
from
(
select
report_date, 
account_id,
vertical, 
market,
variant,
metal,
msa_flag,
merchant_type, 
account_owner,
rep_name,
DSM,
case when assign_ > 0 then 1 else 0 end as assign_,
case when actioned_ > 0 then 1 else 0 end as actioned_,
case when solid_ > 0 then 1 else 0 end as solid_,
case when appt_held_ > 0 then 1 else 0 end as appt_held_,
case when contract_sent_ > 0 then 1 else 0 end as contract_sent_,
case when contract_closed_ > 0 then 1 else 0 end as contract_closed_,
case when flagged_ > 0 and hard_burn_ = 0 and contract_closed_ = 0  then 1 else 0 end as flagged_,   --actions hierarchy 
case when hard_burn_ > 0 and contract_closed_ = 0 then 1 else 0 end as hard_burn_, --actions hierarchy 
case when hard_burn_not_interested_due_to_COVID_ >0 and contract_closed_ = 0 then 1 else 0 end as hard_burn_not_interested_due_to_COVID_,
case when hard_burn_restrictions_COVID_ >0 and contract_closed_ = 0 then 1 else 0 end as hard_burn_restrictions_COVID_,
case when hard_burn_temp_closed_due_to_COVID_ >0  and contract_closed_ = 0 then 1 else 0 end as hard_burn_temp_closed_due_to_COVID_,
case when hard_burn_merchant_moving_forward_ >0  and contract_closed_ = 0 then 1 else 0 end as hard_burn_merchant_moving_forward_,
case when soft_burn_ > 0 and flagged_ = 0 and hard_burn_ = 0 and contract_closed_ = 0  then 1 else 0 end as soft_burn_  --actions hierarchy 

from sandbox.jl_tiered_base

--where account_id = '0013c00001pbjM2AAI'
)as a
left join sandbox.jl_tier_info as b
on a.account_id = b.account_id 
) with data primary index (report_date, account_id, vertical, market, variant, metal, msa_flag, merchant_type, account_owner);


---------------------------------------------------------------------------- step 6: data final step
select * from sandbox.jl_tiered_offering_final where report_date = '2020-07-21';
grant sel on sandbox.jl_tiered_offering_final to public;
drop table sandbox.jl_tiered_offering_final;

create multiset table sandbox.jl_tiered_offering_final as (
select base.*,
cast(dwk.week_end as date) as week_end_date,
cast(dmo.month_end as date) as month_end_date,
cast(dqr.quarter_end as date) as quarter_end_date
from
(
select 
report_date,
vertical, 
market,
variant,
metal,
msa_flag,
merchant_type, 
account_owner, 
rep_name,
DSM,
count(distinct account_id) as leads,
sum(assign_) as leads_assigned,
sum(actioned_) as leads_actioned, 
sum(solid_) as leads_solid, 
sum(appt_held_) as leads_appt_held,
sum(contract_sent_) as contract_sent,
sum(contract_closed_) as contract_closed,
sum(flagged_) as leads_flagged, 
sum(hard_burn_) as leads_hard_burn,
sum(hard_burn_not_interested_due_to_COVID_) as leads_hard_burn_not_interested_due_to_COVID,
sum(hard_burn_restrictions_COVID_) as leads_hard_burn_restrictions_COVID,
sum(hard_burn_temp_closed_due_to_COVID_) as leads_hard_burn_temp_closed_due_to_COVID,
sum(hard_burn_merchant_moving_forward_) as hard_burn_merchant_moving_forward, 
sum(soft_burn_) as leads_soft_burn,
sum(new_tier_2_flag) as closed_tier_2
from
sandbox.jl_tiered_no_dup as main
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
) as base 
left join user_dw.v_dim_day as ddy
on ddy.day_rw = base.report_date
left join user_dw.v_dim_week as dwk 
on dwk.week_key = ddy.week_key 
left join user_dw.v_dim_month as dmo 
on dmo.month_key = ddy.month_key 
left join user_dw.v_dim_quarter as dqr
on dqr.quarter_key = ddy.quarter_key 
) with data;



---------------------------------------------------------------------------- step 7: tableau final step

select* from  sandbox.jl_tableau_tiered_offering where report_date = '2020-07-21';
drop table  sandbox.jl_tableau_tiered_offering;
grant sel on sandbox.jl_tableau_tiered_offering to public;

create multiset table sandbox.jl_tableau_tiered_offering as (
select a.*, 
case when week_end_date = report_date then week_end_date else null end as new_week_end_date,
case when month_end_date = report_date then month_end_date else null end as new_month_end_date,
case when quarter_end_date = report_date then quarter_end_date else null end as new_quarter_end_date
from sandbox.jl_tiered_offering_final as a 
) with data;