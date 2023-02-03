select * from grp_gdoop_sup_analytics_db.tiered_offerings_base_table;
select * from grp_gdoop_revenue_management_db.rev_mgmt_deal_attributes;

select * from grp_gdoop_sup_analytics_db.eh_ls_convenience_scr_deal_final_20191126;

CREATE TABLE grp_gdoop_sup_analytics_db.eh_to_md_scorecard
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY")
AS
select
a.report_date,
a.vertical,
a.variant,
a.target_live,
a.merchant_type,
coalesce(a.account_id,jl.account_id) as account_id,
jl.team_flag,
jl.last_activity,
jl.dmc_date,
jl.call_disposition,
jl.road_block,
jl.current_status,
case when t.account_id is null then 0 else 1 end as top_account_flag,
ros.emplid,
sfp.full_name as account_owner,
ros.team,
ros.title_rw,
case 
	when sfa.ownerid = '005C000000455ZvIAI' then 'Merchant Support'
	when sfa.ownerid = '005C000000Aaro9IAB' then 'Merchant Support'
	when sfa.ownerid = '00580000001YaJIAA0' then 'Merchant Support'
	when ros.team = 'Getaway' then 'Getaways'
	when ros.title_rw in ('Live Account Manager', 'Live Sales Representative') then  'Live'
	when ros.team = 'Team - Inbound' then  'Inbound'
	when ros.title_rw like ('Business Development%') then 'BD'
	when ros.title_rw like ('Merchant Ambassador%') then 'BD'
	when ros.title_rw like ('%Merchant Development%') then  'MD'
	when ros.title_rw in( 'Merchant Support Place Holder', 'MS Account Manager' ) then  'Merchant Support'
	when lower(ros.title_rw) like ('national%') then 'Enterprise'
	when lower(ros.title_rw) like ('enterprise%') then 'Enterprise'
	when lower(ros.title_rw) like ('multi-market%') then 'Enterprise'
	else 'NA'
end as rep_role,
sum(a.merchants_start) as merchants_start,
sum(a.services_start) as services_start,
sum(a.options_start) as options_start,
sum(a.merchant_service_locations_start) as merchant_service_locations_start,
sum(a.merchant_option_locations_start) as merchant_option_locations_start,
sum(a.merchants) as merchants,
sum(a.services_booking) as services_booking,
sum(a.services_tier_2) as services_tier_2,
sum(a.services_tier_3) as services_tier_3,
sum(a.services_tier_1) as services_tier_1,
sum(a.services) as services,
sum(a.options) as options,
sum(a.merchant_service_locations) as merchant_service_locations,
sum(a.merchant_option_locations) as merchant_option_locations,
sum(a.unrest_services) as unrest_services,
sum(case when a.services > a.services_start then 1 else 0 end) as merchants_added_svc
from
(
	select
	dy.day_rw as report_date,
	a.vertical,
	a.variant,
	a.account_id,
	a.target_live,
	case when a.services > 0 then 'Active - Live' else 'Active - Not Live' end as merchant_type, 
	sum(a.services) as services_start,
	sum(a.options) as options_start,
	sum(a.merchant_service_locations) as merchant_service_locations_start,
	sum(a.merchant_option_locations) as merchant_option_locations_start,
	count(distinct a.account_id) as merchants_start,
	count(distinct c.account_id) as merchants,
	sum(case when coalesce(c.bookable_flag,0) = 1 then coalesce(c.services,0) else 0 end) as services_booking,
	sum(case when coalesce(c.tier,0) = 2 then coalesce(c.services,0) else 0 end) as services_tier_2,
	sum(case when coalesce(c.tier,0) = 3 then coalesce(c.services,0) else 0 end) as services_tier_3,
	sum(case when coalesce(c.tier,0) = 1 then coalesce(c.services,0) else 0 end) as services_tier_1,
	sum(coalesce(c.services,0)) as services,
	sum(coalesce(c.options,0)) as options,
	sum(coalesce(c.merchant_service_locations,0)) as merchant_service_locations,
	sum(coalesce(c.merchant_option_locations,0)) as merchant_option_locations,
	sum(case when coalesce(c.unrestricted_flag,0) = 1 then coalesce(c.services,0) else 0 end) as unrest_services
	from
	(
		select
		a.vertical,
		a.test_group as variant,
		a.account_id,
		max(coalesce(b.target_flag,0)) as target_live,
		sum(coalesce(b.services,0)) as services,
		sum(coalesce(b.options,0)) as options,
		sum(coalesce(b.merchant_service_locations,0)) as merchant_service_locations,
		sum(coalesce(b.merchant_option_locations,0)) as merchant_option_locations
		from
		(
			select
			*
			from grp_gdoop_sup_analytics_db.eh_greenlist_detail
			where merchant_type = 'Active'
			and test_group_hierarchy in ('1','2','3')
			and vertical in ('F&D','H&A','HBW','TTD')
		) a
		left join 
		(
			select
			da.account_id,
			gl.vertical,
			case 
			 when gl.vertical = a.vertical and a.division is null then 1
			 when gl.vertical = a.vertical and gl.test_group = a.test_group then 1
			 else 0
			end as target_flag,
			COUNT(DISTINCT CONCAT(a.account_id, a.pds_cat_id)) AS services,
			COUNT(DISTINCT CONCAT(a.account_id, a.deal_option_uuid)) AS options,
			COUNT(DISTINCT CONCAT(a.account_id, a.pds_cat_id, coalesce(a.deal_lat_rounded,1), coalesce(a.deal_lng_rounded,1))) AS merchant_service_locations,
			COUNT(DISTINCT CONCAT(a.account_id, a.deal_option_uuid, coalesce(a.deal_lat_rounded,1), coalesce(a.deal_lng_rounded,1))) AS merchant_option_locations
			from grp_gdoop_sup_analytics_db.tiered_offerings_base_table a
			left outer join (select distinct deal_id, account_id from grp_gdoop_revenue_management_db.rev_mgmt_deal_attributes) da on da.deal_id = a.deal_uuid
			LEFT OUTER JOIN (SELECT DISTINCT account_id, division, test_group, vertical FROM grp_gdoop_sup_analytics_db.eh_greenlist_detail) gl ON gl.account_id = da.account_id
			where report_date = '2020-07-19'
			group by da.account_id,
			gl.vertical,
			case 
			 when gl.vertical = a.vertical and a.division is null then 1
			 when gl.vertical = a.vertical and gl.test_group = a.test_group then 1
			 else 0
			end
		) b on b.account_id = a.account_id
		group by a.vertical,
		a.test_group,
		a.account_id
	) a
	left join 
	(
		select
		day_rw
		from user_groupondw.dim_day
		where (day_of_week_num = 7 or day_rw = date_sub(current_date,1))
		and day_rw between '2020-07-19' and date_sub(current_date,1)
	) dy on dy.day_rw >= '2020-07-19'
	left join
	(
		select
		da.account_id,
		a.report_date,
		case
		 when dt.tier in ('1','3') then 1
		 when COALESCE(dres.new_customer_res, 0) > 0 then 0
	     when COALESCE(dres.active_within_res, 0) > 0 then 0
	     when COALESCE(dres.num_guests_res, 0) > 0 then 0
	     when COALESCE(dres.pref_guests_res, 0) > 0 then 0
	     when COALESCE(dores.dt_time_res, 0) > 0 then 0
		 when coalesce(dores.restricted,1) = 0 then 1
		 else 0
		end as unrestricted_flag,
		dt.tier,
		case when abd.deal_uuid is null then 0 else 1 end as bookable_flag,
		COUNT(DISTINCT CONCAT(a.account_id, a.pds_cat_id)) AS services,
		COUNT(DISTINCT CONCAT(a.account_id, a.deal_option_uuid)) AS options,
		COUNT(DISTINCT CONCAT(a.account_id, a.pds_cat_id, coalesce(a.deal_lat_rounded,1), coalesce(a.deal_lng_rounded,1))) AS merchant_service_locations,
		COUNT(DISTINCT CONCAT(a.account_id, a.deal_option_uuid, coalesce(a.deal_lat_rounded,1), coalesce(a.deal_lng_rounded,1))) AS merchant_option_locations
		from grp_gdoop_sup_analytics_db.tiered_offerings_base_table a
		left outer join (select distinct deal_id, account_id from grp_gdoop_revenue_management_db.rev_mgmt_deal_attributes) da on da.deal_id = a.deal_uuid
		left join grp_gdoop_sup_analytics_db.kg_deal_structure_final dsf on dsf.deal_uuid = a.deal_uuid
		left join grp_gdoop_bizops_db.avb_bookable_deals abd on abd.deal_uuid = a.deal_uuid and abd.report_date = a.report_date
		left join
		(
			select distinct 
			deal_uuid,
			tier
			from grp_gdoop_sup_analytics_db.sup_analytics_dim_deal_tier
			where is_current = 1
		) dt on dt.deal_uuid = a.deal_uuid
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
			FROM grp_gdoop_sup_analytics_db.temp_to_do_restrictions
			where deal_option_uuid is not null
		) dores on dores.deal_option_uuid = a.deal_option_uuid
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
				FROM grp_gdoop_sup_analytics_db.temp_to_do_restrictions
			) a
			WHERE pick_one = 1
		) dres ON a.deal_uuid = dres.deal_uuid
		left join user_groupondw.dim_day dy on dy.day_rw = a.report_date
		where a.report_date >= '2020-07-19'
		and da.account_id is not null
		group by da.account_id,
		a.report_date,
		case
		 when dt.tier in ('1','3') then 1
		 when COALESCE(dres.new_customer_res, 0) > 0 then 0
	     when COALESCE(dres.active_within_res, 0) > 0 then 0
	     when COALESCE(dres.num_guests_res, 0) > 0 then 0
	     when COALESCE(dres.pref_guests_res, 0) > 0 then 0
	     when COALESCE(dores.dt_time_res, 0) > 0 then 0
		 when coalesce(dores.restricted,1) = 0 then 1
		 else 0
		end,
		dt.tier,
		case when abd.deal_uuid is null then 0 else 1 end
	) c on c.account_id = a.account_id and c.report_date = dy.day_rw
	group by dy.day_rw,
	a.vertical,
	a.variant,
	a.account_id,
	a.target_live,
	case when a.services > 0 then 'Active - Live' else 'Active - Not Live' end
) a
full outer join grp_gdoop_sup_analytics_db.jl_md_final jl on jl.account_id = a.account_id
left join (select distinct account_id from grp_gdoop_bizops_db.to_vertical_accounts) t on t.account_id = coalesce(a.account_id,jl.account_id)
left outer join dwh_base_sec_view.sf_account sfa on sfa.account_id_18 = coalesce(a.account_id,jl.account_id)
left join user_groupondw.dim_sf_person sfp on sfp.person_id = sfa.ownerid
left join 
(
	select
	a.*
	from
	(
		select
		roster_date,
		emplid,
		team,
		title_rw,
		row_number() over(partition by roster_date,emplid order by length(title_rw) desc) as pick_one
		from grp_gdoop_sup_analytics_db.ops_roster_all
		where roster_date = date_sub(current_date,1)
	) a
	where pick_one = 1
) ros on sfp.ultipro_id = ros.emplid
group by a.report_date,
a.vertical,
a.variant,
a.target_live,
a.merchant_type,
coalesce(a.account_id,jl.account_id),
jl.team_flag,
jl.last_activity,
jl.dmc_date,
jl.call_disposition,
jl.road_block,
jl.current_status,
case when t.account_id is null then 0 else 1 end,
ros.emplid,
sfp.full_name,
ros.team,
ros.title_rw,
case 
	when sfa.ownerid = '005C000000455ZvIAI' then 'Merchant Support'
	when sfa.ownerid = '005C000000Aaro9IAB' then 'Merchant Support'
	when sfa.ownerid = '00580000001YaJIAA0' then 'Merchant Support'
	when ros.team = 'Getaway' then 'Getaways'
	when ros.title_rw in ('Live Account Manager', 'Live Sales Representative') then  'Live'
	when ros.team = 'Team - Inbound' then  'Inbound'
	when ros.title_rw like ('Business Development%') then 'BD'
	when ros.title_rw like ('Merchant Ambassador%') then 'BD'
	when ros.title_rw like ('%Merchant Development%') then  'MD'
	when ros.title_rw in( 'Merchant Support Place Holder', 'MS Account Manager' ) then  'Merchant Support'
	when lower(ros.title_rw) like ('national%') then 'Enterprise'
	when lower(ros.title_rw) like ('enterprise%') then 'Enterprise'
	when lower(ros.title_rw) like ('multi-market%') then 'Enterprise'
	else 'NA'
end