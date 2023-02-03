

create volatile table sh_booking_solution as (
	select
		o2.deal_uuid,
		max(sfa.id) sf_account_id,
		max(case
				when lower(sfa.scheduler_setup_type) = 'pen & paper' then 'pen & paper'
				when sfa.scheduler_setup_type is null then 'no data'
				else trim(lower(sfa.scheduler_setup_type))
			end) current_booking_solution,
		max(sfa.name) account_name,
		max(company_type) company_type
	from dwh_base_sec_view.opportunity_1 o1
	join dwh_base_sec_view.opportunity_2 o2 on o1.id = o2.id
	join dwh_base_sec_view.sf_account sfa on o1.accountid = sfa.id
	group by 1
) with data unique primary index (deal_uuid) on commit preserve rows;

create volatile table sh_bt_opt_out_reason as (
	select
		accountid,
		product_opt_out_reason__c as product_opt_out_reason,
		dwh_created_at
	from dwh_base_sec_view.sf_task
	where
		product_opt_out_reason__c is not null
	qualify row_number() over (partition by accountid order by dwh_created_at desc) = 1
) with data unique primary index (accountid) on commit preserve rows;

create volatile table jw_booking_solution_opt_out as (
	select
		deal_uuid,
		current_booking_solution,
		company_type,
		product_opt_out_reason
	from
		sh_booking_solution a
	left join sh_bt_opt_out_reason b on a.sf_account_id = b.accountid
) with data unique primary index (deal_uuid) on commit preserve rows;

create volatile table jw_deals_live as (
	select
		ad.load_date,
		ad.deal_uuid,
		case when bta.deal_uuid is not null then '1' else '0' end as bt_flag,
		case when dmp.inv_service_id = 'tpis' then '1' else '0' end as tpis_flag,
		coalesce(bso.current_booking_solution,'-1') current_booking_solution,
		coalesce(bso.company_type,'-1') company_type,
		coalesce(bso.product_opt_out_reason,'-1') product_opt_out_reason,
		max(grt_l2_cat_description)  L2,
		max(grt_l3_cat_description)  L3,
		max(grt_l4_cat_description)  L4,
		max(grt_l5_cat_description)  L5,
		max(case when ad.country_code in ('US') then 'NAM' else 'INTL' end) region,
		max(case when ad.country_code in ('US','DE','IT','FR','UK') then ad.country_code else 'Other INTL' end) country
	from user_groupondw.active_deals ad
	left join
		(select distinct
			dmp.deal_uuid,
			dmp.inv_service_id,
			dmp.primary_dealservice_cat_id
		from user_edwprod.deal_merch_product dmp) dmp on ad.deal_uuid = dmp.deal_uuid
	left join
		(select distinct
			pds_cat_id,
			grt_l2_cat_description,
			grt_l3_cat_description,
			grt_l4_cat_description,
			grt_l5_cat_description
		from user_dw.v_dim_pds_grt_map) pds on dmp.primary_dealservice_cat_id = pds.pds_cat_id
	left join jw_booking_solution_opt_out bso on bso.deal_uuid = ad.deal_uuid
	left join
		(select distinct
			bta.load_date,
			bta.deal_uuid
		from sandbox.sh_bt_active_deals_log bta
		where
			bta.is_bookable = '1'
			and bta.partner_inactive_flag = '0'
			and bta.product_is_active_flag = '1') bta on bta.load_date = ad.load_date and bta.deal_uuid = ad.deal_uuid
	where
		ad.sold_out = 'false'
		and pds.grt_l2_cat_description in ('F&D','HBW','TTD - Leisure')
		and ad.load_date = (
			select distinct
				cast(month_start as date) as month_start_date
			from user_dw.v_dim_month a
			where
				month_key in (
					select distinct 
						month_key 
					from user_dw.v_dim_day a 
					where 
						day_rw = (current_date - 1)))
	group by 1,2,3,4,5,6,7 ) with data unique primary index (load_date, deal_uuid) on commit preserve rows;
	
create
	volatile table
		jw_financial as (
		SELECT
			f.deal_id ,
			f.report_date ,
			case when bta.deal_uuid is not null then '1' else '0' end as bt_flag,
			case when dmp.inv_service_id = 'tpis' then '1' else '0' end as tpis_flag,
			coalesce(bso.current_booking_solution,'-1') current_booking_solution,
			coalesce(bso.company_type,'-1') company_type,
			coalesce(bso.product_opt_out_reason,'-1') product_opt_out_reason,
			max(grt_l2_cat_description)  L2,
			max(grt_l3_cat_description)  L3,
			max(grt_l4_cat_description)  L4,
			max(grt_l5_cat_description)  L5,
			max(case when f.country_code in ('US') then 'NAM' else 'INTL' end) region,
			max(case when f.country_code in ('US','DE','IT','FR','UK') then f.country_code else 'Other INTL' end) country,
			sum(f.transactions_qty) - sum(f.zdo_transactions_qty) as units,
			avg(addr.addressable_wt) as addressable_wt
		FROM user_edwprod.agg_gbl_financials_deal f
		left join
			(select distinct
				dmp.deal_uuid,
				dmp.inv_service_id,
				dmp.primary_dealservice_cat_id
			from user_edwprod.deal_merch_product dmp) dmp on f.deal_id = dmp.deal_uuid
		left join
			(select distinct
				pds_cat_id,
				grt_l2_cat_description,
				grt_l3_cat_description,
				grt_l4_cat_description,
				grt_l5_cat_description
			from user_dw.v_dim_pds_grt_map) pds on dmp.primary_dealservice_cat_id = pds.pds_cat_id
		left join sandbox.sh_pds_addressability addr on pds.pds_cat_id = addr.pds_cat_id
		left join jw_booking_solution_opt_out bso on bso.deal_uuid = f.deal_id
		left join
			(select distinct
				bta.load_date,
				bta.deal_uuid
			from sandbox.sh_bt_active_deals_log bta
			where
				bta.is_bookable = '1'
				and bta.partner_inactive_flag = '0'
				and bta.product_is_active_flag = '1') bta on bta.load_date = f.report_date and bta.deal_uuid = f.deal_id
		JOIN user_groupondw.dim_day dd ON dd.day_rw = f.report_date
		JOIN (
			SELECT
				currency_from ,
				currency_to ,
				fx_neutral_exchange_rate ,
				approved_avg_exchange_rate ,
				period_key
			FROM
				user_groupondw.gbl_fact_exchange_rate
			WHERE
				currency_to = 'USD'
			GROUP BY
				1,2,3,4,5 ) er ON
			f.currency_code = er.currency_from
			AND dd.month_key = er.period_key
		WHERE
			f.report_date between ('2019-01-01') and ('2019-12-31')
			and pds.grt_l2_cat_description in ('F&D','HBW','TTD - Leisure')
		GROUP BY
			1,2,3,4,5,6,7 ) with data unique primary index (deal_id,
		report_date,
		bt_flag) on
		commit preserve rows;
		
create
	volatile table
		jw_ogp as (
		SELECT
			ogp.deal_id,
			ogp.report_date ,
			case when bta.deal_uuid is not null then '1' else '0' end as bt_flag,
			case when dmp.inv_service_id = 'tpis' then '1' else '0' end as tpis_flag,
			coalesce(bso.current_booking_solution,'-1') current_booking_solution,
			coalesce(bso.company_type,'-1') company_type,
			coalesce(bso.product_opt_out_reason,'-1') product_opt_out_reason,
			max(grt_l2_cat_description)  L2,
			max(grt_l3_cat_description)  L3,
			max(grt_l4_cat_description)  L4,
			max(grt_l5_cat_description)  L5,
			max(case when ogp.country_code in ('US') then 'NAM' else 'INTL' end) region,
			max(case when ogp.country_code in ('US','DE','IT','FR','UK') then ogp.country_code else 'Other INTL' end) country,
			sum((ogp_auth_loc + ogp_estimated_refund_loc) * coalesce(er.fx_neutral_exchange_rate, 1)) ogp
		FROM user_edwprod.agg_gbl_ogp_financials_deal ogp
		left join
			(select distinct
				dmp.deal_uuid,
				dmp.inv_service_id,
				dmp.primary_dealservice_cat_id
			from user_edwprod.deal_merch_product dmp) dmp on ogp.deal_id = dmp.deal_uuid
		left join
			(select distinct
				pds_cat_id,
				grt_l2_cat_description,
				grt_l3_cat_description,
				grt_l4_cat_description,
				grt_l5_cat_description
			from user_dw.v_dim_pds_grt_map) pds on dmp.primary_dealservice_cat_id = pds.pds_cat_id
		left join jw_booking_solution_opt_out bso on bso.deal_uuid = ogp.deal_id
		left join
			(select distinct
				bta.load_date,
				bta.deal_uuid
			from sandbox.sh_bt_active_deals_log bta
			where
				bta.is_bookable = '1'
				and bta.partner_inactive_flag = '0'
				and bta.product_is_active_flag = '1') bta on bta.load_date = ogp.report_date and bta.deal_uuid = ogp.deal_id
		JOIN user_groupondw.dim_day dd ON dd.day_rw = ogp.report_date
		JOIN (
			SELECT
				currency_from ,
				currency_to ,
				fx_neutral_exchange_rate ,
				approved_avg_exchange_rate ,
				period_key
			FROM
				user_groupondw.gbl_fact_exchange_rate
			WHERE
				currency_to = 'USD'
			GROUP BY
				1,2,3,4,5 ) er ON
			ogp.currency_code = er.currency_from
			AND dd.month_key = er.period_key
		WHERE
			ogp.report_date between ('2019-01-01') and ('2019-12-31')
			and pds.grt_l2_cat_description in ('F&D','HBW','TTD - Leisure')
		GROUP BY
			1,2,3,4,5,6,7 ) with data unique primary index (deal_id,
		report_date,
		bt_flag) on
		commit preserve rows;
		
create
	volatile table
		jw_dims as (
		SELECT
			deal_id,
			report_date,
			bt_flag,
			tpis_flag,
			current_booking_solution,
			company_type,
			product_opt_out_reason,
			L2,
			L3,
			L4,
			L5,
			region,
			country
		FROM
			(
			SELECT
				deal_id,
				report_date,
				bt_flag,
				tpis_flag,
				current_booking_solution,
				company_type,
				product_opt_out_reason,
				L2,
				L3,
				L4,
				L5,
				region,
				country
			FROM
				jw_financial
		UNION ALL
			SELECT
				deal_id,
				report_date,
				bt_flag,
				tpis_flag,
				current_booking_solution,
				company_type,
				product_opt_out_reason,
				L2,
				L3,
				L4,
				L5,
				region,
				country
			FROM
				jw_ogp ) a
		group by
			1,2,3,4,5,6,7,8,9,10,11,12,13 ) with data unique primary index (deal_id,
			report_date,
			bt_flag,
			tpis_flag,
			current_booking_solution,
			company_type,
			product_opt_out_reason,
			L2,
			L3,
			L4,
			L5,
			region,
			country) on
		commit preserve rows;
		
	create volatile table jw_gtm_metrics as (
	select
		d.deal_id,
		d.report_date,
		d.bt_flag,
		d.tpis_flag,
		d.current_booking_solution,
		d.company_type,
		d.product_opt_out_reason,
		d.L2,
		d.L3,
		d.L4,
		d.L5,
		d.region,
		d.country,
		f.units,
		f.addressable_wt,
		ogp.ogp
	from
		jw_dims d
	left join jw_financial f on
		d.deal_id = f.deal_id
		and d.report_date = f.report_date
		and d.bt_flag = f.bt_flag
		and d.tpis_flag = f.tpis_flag
		and d.current_booking_solution = f.current_booking_solution
		and d.company_type = f.company_type
		and d.product_opt_out_reason = f.product_opt_out_reason
		and d.L2 = f.L2
		and d.L3 = f.L3
		and d.L4 = f.L4
		and d.L5 = f.L5
		and d.region = f.region
		and d.country = f.country
	left join jw_ogp ogp on
		d.deal_id = ogp.deal_id
		and d.report_date = ogp.report_date
		and d.bt_flag = ogp.bt_flag
		and d.tpis_flag = ogp.tpis_flag
		and d.current_booking_solution = ogp.current_booking_solution
		and d.company_type = ogp.company_type
		and d.product_opt_out_reason = ogp.product_opt_out_reason
		and d.L2 = ogp.L2
		and d.L3 = ogp.L3
		and d.L4 = ogp.L4
		and d.L5 = ogp.L5
		and d.region = ogp.region
		and d.country = ogp.country) with data no primary index on commit preserve rows;--Active Deals, Units, OGP

create volatile table duo as (
	select
		coalesce(ad.region,f.region) as region,
		coalesce(ad.bt_flag,f.bt_flag) as bt_flag,
		coalesce(ad.tpis_flag,f.tpis_flag) as tpis_flag,
		coalesce(ad.L2,f.L2) as L2,
		coalesce(ad.L3,f.L3) as L3,
		coalesce(ad.L4,f.L4) as L4,
		coalesce(ad.L5,f.L5) as L5,
		coalesce(ad.current_booking_solution,f.current_booking_solution) as current_booking_solution,
		coalesce(ad.company_type,f.company_type) as company_type,
		coalesce(ad.product_opt_out_reason,f.product_opt_out_reason) as product_opt_out_reason,
		count(distinct ad.deal_uuid) as live_deals,
		avg(coalesce(addressable_wt,null)) as addressable_wt,
		sum(coalesce(units,0)) as units,
		sum(coalesce(ogp,0)) as ogp
	from (
		select
			region,
			bt_flag,
			tpis_flag,
			L2,
			L3,
			L4,
			L5,
			current_booking_solution,
			company_type,
			product_opt_out_reason,
			deal_uuid
		from jw_deals_live
		group by 1,2,3,4,5,6,7,8,9,10,11) ad
	full join (
		select
			region,
			bt_flag,
			tpis_flag,
			L2,
			L3,
			L4,
			L5,
			current_booking_solution,
			company_type,
			product_opt_out_reason,
			deal_id,
			avg(addressable_wt) as addressable_wt,
			sum(units) as units,
			sum(ogp) as ogp
		from jw_gtm_metrics
		group by 1,2,3,4,5,6,7,8,9,10,11) f
		on ad.region = f.region
		and ad.bt_flag = f.bt_flag
		and ad.tpis_flag = f.tpis_flag
		and ad.L2 = f.L2
		and ad.L3 = f.L3
		and ad.L4 = f.L4
		and ad.L5 = f.L5
		and ad.deal_uuid = f.deal_id
		and ad.current_booking_solution = f.current_booking_solution
		and ad.company_type = f.company_type
		and ad.product_opt_out_reason = f.product_opt_out_reason
	group by 1,2,3,4,5,6,7,8,9,10
) with data no primary index on commit preserve rows;--attrition
--Dates for Last Month & Prior Month

create volatile table jw_months as (
	select distinct
		cast(month_start as date) as month_start_date,
		cast(month_end as date) as month_end_date
	from user_dw.v_dim_month a
	where
		month_key in (
			select distinct 
				month_key 
			from user_dw.v_dim_day a 
			where 
				day_rw between (current_date - 40) and (current_date - 5))
) with data no primary index on commit preserve rows;--Prior month Live BT Deals


create volatile table jw_prior_month_bt_deals as (
	select distinct
		trunc(cast(load_date as date),'MM') as prior_month,
		deal_uuid,
		(case when gdl.country_code = 'US' then 'NAM' else 'INTL' end) region,
		max(pds.grt_l2_cat_description) L2,
		max(pds.grt_l3_cat_description) L3,
		max(pds.grt_l4_cat_description) L4,
		max(pds.grt_l5_cat_description) L5
	from sandbox.sh_bt_active_deals_log_v3 a
	left join user_edwprod.dim_gbl_deal_lob gdl on gdl.deal_id = a.deal_uuid
	left join
		(select distinct
			pds_cat_id,
			grt_l2_cat_description,
			grt_l3_cat_description,
			grt_l4_cat_description,
			grt_l5_cat_description
		from user_dw.v_dim_pds_grt_map) pds on gdl.pds_cat_id = pds.pds_cat_id
	where
		pds.grt_l2_cat_description in ('F&D','HBW','TTD - Leisure') and
		is_bookable = '1' and 
		partner_inactive_flag = '0' and 
		product_is_active_flag = '1' and
		load_date between 
			(select min(month_start_date) from jw_months)
		and
			(select min(month_end_date) from jw_months)
	group by 1,2,3
) with data no primary index on commit preserve rows;--Last month Live BT Deals

create volatile table jw_last_month_bt_deals as (
	select distinct
		trunc(cast(load_date as date),'MM') as last_month,
		deal_uuid,
		(case when gdl.country_code = 'US' then 'NAM' else 'INTL' end) region,
		max(pds.grt_l2_cat_description) L2,
		max(pds.grt_l3_cat_description) L3,
		max(pds.grt_l4_cat_description) L4,
		max(pds.grt_l5_cat_description) L5
	from sandbox.sh_bt_active_deals_log_v3 a
	left join user_edwprod.dim_gbl_deal_lob gdl on gdl.deal_id = a.deal_uuid
	left join
		(select distinct
			pds_cat_id,
			grt_l2_cat_description,
			grt_l3_cat_description,
			grt_l4_cat_description,
			grt_l5_cat_description
		from user_dw.v_dim_pds_grt_map) pds on gdl.pds_cat_id = pds.pds_cat_id
	where
		pds.grt_l2_cat_description in ('F&D','HBW','TTD - Leisure') and
		is_bookable = '1' and 
		partner_inactive_flag = '0' and 
		product_is_active_flag = '1' and
		load_date between
			(select	max(month_start_date) from jw_months)
		and
			(select max(month_end_date) from jw_months)
	group by 1,2,3
) with data no primary index on commit preserve rows;--Last month Live Groupon Deals

create volatile table jw_last_month_gr_deals as (
	select distinct
		trunc(cast(load_date as date),'MM') as last_month,
		deal_uuid,
		(case when gdl.country_code = 'US' then 'NAM' else 'INTL' end) region,
		max(pds.grt_l2_cat_description) L2,
		max(pds.grt_l3_cat_description) L3,
		max(pds.grt_l4_cat_description) L4,
		max(pds.grt_l5_cat_description) L5
	from sandbox.sh_bt_active_deals_log_v3 a
	left join user_edwprod.dim_gbl_deal_lob gdl on gdl.deal_id = a.deal_uuid
	left join
		(select distinct
			pds_cat_id,
			grt_l2_cat_description,
			grt_l3_cat_description,
			grt_l4_cat_description,
			grt_l5_cat_description
		from user_dw.v_dim_pds_grt_map) pds on gdl.pds_cat_id = pds.pds_cat_id
	where
		pds.grt_l2_cat_description in ('F&D','HBW','TTD - Leisure') and
		load_date between
			(select	max(month_start_date) from jw_months)
		and
			(select max(month_end_date) from jw_months)
	group by 1,2,3
) with data no primary index on commit preserve rows;

create volatile table gtm_attr as (
	select
		region,
		L2,
		L3,
		L4,
		L5,
		trunc(cast(current_date - 16 as date),'MM') as report_month,
		sum(Live_BT_Deal_prior_month_flag) as prior_month_BT_deals,
		sum(Live_BT_Deal_last_month_flag) as still_on_BT_deals,
		sum(Live_GR_Deal_last_month_flag) as still_on_GR_deals
	from (
		select 
			a.region,
			a.L2,
			a.L3,
			a.L4,
			a.L5,
			a.deal_uuid, 
			case
				when a.prior_month is null then '0'
				else '1'
			end as Live_BT_Deal_prior_month_flag, 
			case 
				when b.last_month is null then '0'
				else '1'
			end as Live_BT_Deal_last_month_flag,
			case 
				when c.last_month is null then '0'
				else '1'
			end as Live_GR_Deal_last_month_flag
		from jw_prior_month_bt_deals a
		left join jw_last_month_bt_deals b on a.deal_uuid = b.deal_uuid 
		left join jw_last_month_gr_deals c on a.deal_uuid = c.deal_uuid) a
	group by 1,2,3,4,5,6
) with data no primary index on commit preserve rows;

drop table sandbox.jw_gtm_view2;

create table sandbox.jw_gtm_view2 as (
	select
		coalesce(a.region,m.region) region,
		coalesce(a.L2,m.L2) L2,
		coalesce(a.L3,m.L3) L3,
		coalesce(a.L4,m.L4) L4,
		coalesce(a.L5,m.L5) L5,
		coalesce(m.tpis_flag,'0') tpis_flag,
		coalesce(m.current_booking_solution,'-1') current_booking_solution,
		coalesce(m.company_type,'-1') company_type,
		coalesce(m.product_opt_out_reason,'-1') product_opt_out_reason,
		(coalesce(m.live_deals,0)) deals_live,
		(coalesce(case when bt_flag = '1' then live_deals else 0 end,0)) bt_deals_live,
		(coalesce(m.addressable_wt,null)) addressable_wt,
		(coalesce(m.units,0)) as units_2019,
		(coalesce(case when bt_flag = '1' then m.units else 0 end,0)) bt_units_2019,
		(coalesce(m.ogp,0)) as ogp_2019,
		(coalesce(case when bt_flag = '1' then m.ogp else 0 end,0)) bt_ogp_2019,
		sum(coalesce(a.prior_month_BT_deals,0)) prior_month_BT_deals,
		sum(coalesce(a.still_on_BT_deals,0)) still_on_BT_deals,
		sum(coalesce(a.still_on_GR_deals,0)) still_on_GR_deals
	FROM
		gtm_attr a
	full join duo m on
		a.region = m.region
		and a.L2 = m.L2
		and a.L3 = m.L3
		and a.L4 = m.L4
		and a.L5 = m.L5
	group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
) with data no primary index;grant select on sandbox.jw_gtm_view2 to public;

grant all on sandbox.jw_gtm_view2 to abautista with grant option;

drop table sh_booking_solution;drop table sh_bt_opt_out_reason;
drop table jw_booking_solution_opt_out;drop table jw_deals_live;
drop table jw_financial;
drop table jw_ogp;drop table jw_dims;drop table jw_gtm_metrics;
drop table duo;drop table jw_months;drop table jw_prior_month_bt_deals;
drop table jw_last_month_bt_deals;
drop table jw_last_month_gr_deals;drop table gtm_attr