select account_id, count(distinct deal_uuid) xyz from sandbox.jl_new_groupon group by Account_id;

select account_id, count(distinct division) xyz from sandbox.eh_greenlist_detail;


select * from sandbox.eh_to_closes;


left join
(
    select distinct 
    opportunity_id,
    tier
    from sandbox.sup_analytics_dim_deal_tier
    where is_current = 1 and deal_uuid is not null
) dt on dt.opportunity_id = substr(ng.opp_id,1,15)


select * from sandbox.sup_analytics_dim_deal_tier_stg where opportunity_id = '0063c00001FS8Wm' and is_current = 1;

select * from sandbox.sup_analytics_dim_deal_tier_stg where opportunity_id = '0063c00001FT3hX';

select * from sandbox.sup_analytics_dim_deal_tier;

select * from sandbox.jl_tiered_base;

select * from sandbox.cc_restrictions_dash;

select * from sandbox.deal_structure_field_jl;

select * from dwh_base_sec_view.sf_multi_deal;

select * from sandbox.eh_ls_convenience_scr_deal_final_20191126;

select * from sandbox.kg_deal_option_title limit 5;

select * from user_edwprod.deal_inv_prd_redloc;

select * from user_edwprod.deal_redemption_location;

select * from user_

select * from user_groupondw.dim_deal_option;
----deal structure


SELECT 
a.rep, -- 
a.team_0,
a.DSM, -- 
a.RSD, 
a.Team,
a.Account_name, --
a.account_id,  
a.opp_id, -- 
a.deal_uuid,
a.pds AS PDS,
a.division,
a.merchant_seg_at_closed_won, --
a.closedate,
a.repurchase AS Repurchase_control,
a.New_Client_Window,
a.cc_fee,
a.options,
a.POR,
a.buyer_max,
a.multi_voucher,
a.voucher_type_online,
a.gen_spend,
CASE WHEN dynamic_pricing = 'true' THEN 1 ELSE 0 END AS DP_ILS_Flag,
CASE
  WHEN expiration_type = 'Relative to Purchase' THEN 'Reatlive to Purchase'
  WHEN expiration_type='Fixed' THEN 'Fixed' 
  ELSE 'NA'
END AS Expiration_flag,
CASE WHEN repurchase BETWEEN 1 AND 365 THEN 1 ELSE 0 END Repurchase_Flag,
a.vertical
FROM 
(
	SELECT 
	ros.rep, -- 
	ros.team as team_0, 
	ros.m1 AS DSM, -- 
	ros.m2 as RSD, 
	CASE
            WHEN vm.l1_name = 'L1 - Shopping' THEN 'Goods'
            WHEN sfa.National_Vertical = 'CPG' THEN 'Retail'
            WHEN sfa.National_Vertical = 'Customizable' THEN 'Retail'
            WHEN sfa.National_Vertical = 'Multi-Channel Retail' THEN 'Retail'
            WHEN sfa.National_Vertical = 'Online' THEN 'Retail'
            WHEN sfa.subcategory_v3 = 'Jumping' THEN 'TTD'
            WHEN sfa.services_offered LIKE ANY ('%Consultant - Nutritional / Weight-Loss%','%Consultant - Counselor / Therapist%','%Gymnastics%','%Beauty School%','%Personal Chef%','%Personal Stylist%','%Rehabilitation Center%','%Boxing / Kickboxing - Training%') THEN 'HBW'
            WHEN sfa.subcategory_v3 IN ('Tattoo & Piercing','Workout & Fitness') THEN 'HBW'
            WHEN sfa.subcategory_v3 IN ('Boat Rental','Sporting Rental','Winery / Distillery / Brewery','Consulting Services') THEN 'TTD'
            WHEN sfa.subcategory_v3 IN ('Professional Photography, Photo Printing, Framing','Subscriptions') THEN 'Retail'
            WHEN sfa.subcategory_v3 LIKE 'Courses%' THEN 'TTD'
            WHEN sfa.category_v3 LIKE '%Food%' THEN 'F&D'
            WHEN sfa.category_v3 LIKE '%Beauty%' THEN 'HBW'
            WHEN sfa.category_v3 LIKE '%Leisure%' THEN 'TTD'
            WHEN sfa.category_v3 = 'Tickets' THEN 'TTD'
            WHEN sfa.category_v3 = 'Services' THEN 'H&A'
            WHEN sfa.category_v3 = 'Shopping' THEN 'Retail'
            WHEN sfa.category_v3 = 'Goods' THEN 'Retail'
        ELSE 'Other'
        END AS vertical_raw,
	CASE 
WHEN vertical_raw = 'TTD' AND dop.partner_deal_source IS NULL THEN 'TTD - Leisure'
WHEN vertical_raw = 'TTD' THEN 'TTD - Live'
ELSE vertical_raw
        END AS vertical,
	case 
		when ea.account_id is not null then 'Enterprise'
		when ros.team = 'Getaway' then 'Getaway'
		when ros.title_rw in ('Business Development Director - Outside', 'Business Development Manager - Outside',  'Business Development Manager' , 'Business Development Director', 'Business Development Director - Outside','Business Development Representative - Outside' ,'Business Development Representative')then 'BD'
		when ros.title_rw in ('Merchant Development Director','Merchant Development Manager', 'Merchant Development Representative','Strategic Merchant Development Director') then  'MD'
		when ros.title_rw in('Merchant Support Place Holder', 'MS Account Manager' ) then  'Merchant Support'
		when ros.team = 'Team - Inbound' then  'Inbound'
		when ros.title_rw in ('Live Account Manager', 'Live Sales Representative') then  'Live'
		when ros.segment = 'F&D - CLO' then  'CLO'
		else 'NA'
	end as team, 
	sfa.name Account_name, -- 
	sfa.id Account_id, 
	o.id as opp_id, -- 
	o2.deal_uuid,
	CASE WHEN dop.txnmy_pds_v3_name IS NOT NULL THEN dop.txnmy_pds_v3_name else o.Primary_Deal_Services end as pds,
	o.division, 
	sda.merchant_seg_at_closed_won, --                               
	o.closedate, 
	str.Repurchase_window AS repurchase,
	str.New_Client_Window, 
	COALESCE(o.cc_fee,0) AS cc_fee,
	o.expiration_type,
	o2.dynamic_pricing,
	CASE WHEN dop.payment_terms = 'Redemption System' THEN 1 ELSE 0 END AS POR,
	op.buyer_max,
	CASE WHEN dop.redeem_at='Online' THEN 1 ELSE 0 END AS voucher_type_online,
	dm.multi_voucher,
	case when coalesce(gs.gen_spend_cnt,0) > 0 then 'Gen Spend' else 'Price For Service' end as gen_spend,
	COUNT(distinct md.id) options
	FROM dwh_base_sec_view.opportunity_1 o                       
	LEFT JOIN dwh_base_sec_view.sf_opportunity_2 o2 ON o2.id = o.id                    
	LEFT JOIN user_dw.v_dim_deal D ON d.opportunity_id = o.id
	LEFT JOIN 
	 (
	  SELECT
	  deal_key,
	  MIN(max_per_pledge) AS buyer_max
	  FROM user_groupondw.dim_deal_option
	  GROUP BY 1
	 ) op ON op.deal_key= D.deal_key
	LEFT JOIN 
	(
	  SELECT 
DISTINCT opportunity_key,
max(CASE WHEN voucher_units_per_option > 1 THEN 1 ELSE 0 END) AS multi_voucher
FROM user_groupondw.dim_multi_deal   
group by 1
	) dm ON dm. opportunity_key = d.opportunity_key
	LEFT JOIN user_groupondw.dim_opportunity dop ON d.opportunity_key= dop.opportunity_key 
	INNER JOIN dwh_base_sec_view.sf_account sfa ON sfa.account_id_18 = o.accountid                                      
	INNER JOIN user_dw.v_dim_sf_deal_attribute sda ON sda.id = o.deal_attribute                              
	INNER JOIN user_groupondw.dim_sf_person sfp ON sfp.person_id = o.ownerid
	JOIN 
	(
	select *
	from sandbox.ops_roster_all ros 
	 qualify row_number() over(partition by roster_date,emplid order by start_date,month_end_date,length(title_rw) desc) = 1
	 ) as ros
	ON ros.roster_date = o.closedate AND ros.emplid = sfp.ultipro_id
	left join user_dw.v_dim_merchant_account ea on ea.account_id=sfa.account_id_18 and ea.account_type = 'Enterprise'
	LEFT JOIN dwh_base_sec_view.sf_multi_deal md ON md.opportunity_id = o.id                
	LEFT JOIN sandbox.deal_structure_gen_spend gs on gs.deal_uuid = o2.deal_uuid	                
	LEFT JOIN sandbox.kg_deal_structure_final str ON str.deal_uuid = o2.deal_uuid
	LEFT JOIN sandbox.eh_vertical_map_deal vm ON vm.deal_key = d.deal_key AND vm.platform_key = 1
	WHERE 
	o.stagename = 'Closed Won'
	AND (dop.txnmy_pds_v3_name IS NOT NULL OR o.Primary_Deal_Services IS NOT NULL)
/*	AND (YEAR(o.closedate) = YEAR(current_date) OR YEAR(o.closedate) = YEAR(current_date)-1 OR o.closedate between current_date-14 and current_date-1) */
	GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 , 17, 18 , 19 , 20 , 21, 22, 23, 24,25)
	a;



-----cc_restrictions_dash

sel
dc.*,
case when ds.Tm_Dt_Res=1 or dow.dow_restriction=1 then 1 else 0 end as Tm_combo,   
case when dow.one_person_restriction = 1 then 1 else 0 end as one_person_restriction, 
ds.New_Clients_Res, 
case when tpis.deal_uuid is not null then 1 else 0 end as TPIS,
o.permalink as "perm", 
ds.Tm_Dt_Res,
ds.Add_Fee_Res,
ds.Menu_Ser_Res,
ds.Repurchase_Res,
ds.Holiday_Res,
ds.Appointment_Res,
r.service_rank,
case when t.account_id is not null then 'Yes' else 'No' end Top_100_flag,
case when dc.vertical in ('HBW', 'TTD - Leisure') then coalesce (ht.freq_goal, 0)
when dc.vertical in ('F&D', 'F&D - CLO') then 12
else 1 end as freq_goal,
cast (case when Repurchase_control>=1 then 360/Repurchase_control else 0  end as decimal (4,2)) as repurchase_times
from sandbox.deal_structure_field_jl dc
left join user_dw.v_dim_pds_grt_map mp on dc.pds=mp.pds_cat_name
left join sandbox.buyermax_hbw_TTD ht on ht.pds_cat_name= mp.pds_cat_name and ht.grt_l6_cat_name=mp.grt_l6_cat_name and ht.grt_l5_cat_name=mp.grt_l5_cat_name and ht.grt_l4_cat_name=mp.grt_l4_cat_name and ht.grt_l3_cat_name=mp.grt_l3_cat_name and ht.grt_l2_cat_name=mp.grt_l2_cat_name and ht.grt_l1_cat_name=mp.grt_l1_cat_name 
left join sandbox.kg_deal_structure_final ds on dc.deal_uuid=ds.deal_uuid
left join (
 SELECT
        deal_uuid,
        CASE WHEN min_no_people = 1 THEN 1 ELSE 0 END AS one_person_restriction,
        CASE WHEN mon_ind > 0 AND tue_ind > 0 AND wed_ind > 0 AND thu_ind > 0 AND fri_ind > 0 AND sat_ind > 0 AND sun_ind > 0 THEN 0 ELSE 1 END AS dow_restriction
 FROM
 (
  SELECT
        deal_uuid,
        MIN(no_of_people) AS min_no_people,
        SUM(mon_ind) AS mon_ind,
        SUM(tue_ind) AS tue_ind,
        SUM(wed_ind) AS wed_ind,
        SUM(thu_ind) AS thu_ind,
        SUM(fri_ind) AS fri_ind,
        SUM(sat_ind) AS sat_ind,
        SUM(sun_ind) AS sun_ind
  FROM sandbox.kg_deal_option_title
  GROUP BY deal_uuid
 ) a
) dow
ON dc.deal_uuid = dow.deal_uuid
left join sandbox.kg_acct_serv_rank r on r.account_id=dc.account_id and r.service_id=mp.pds_cat_id
left join (sel distinct account_id from sandbox.eh_top_accounts_retention2 where (vertical_rank_nob<=100 or vertical_rank_act_react<=100) 
/*AND qtr IN (td_quarter_begin(current_date), td_quarter_begin(current_date)-1,td_quarter_begin(current_date)-2,td_quarter_begin(current_date)-3)*/)  t on dc.account_id=t.account_id 
LEFT JOIN (SELECT DISTINCT deal_uuid FROM user_edwprod.deal_merch_product WHERE inv_service_id = 'tpis') tpis on tpis.deal_uuid=dc.deal_uuid
JOIN dwh_base_sec_view.sf_opportunity_2 o2 ON o2.deal_uuid = dc.deal_uuid  
JOIN dwh_base_sec_view.opportunity_1 o ON o2.id = o.id;

-------new_groupon

select 
a.*, 
b.launch_date, 
case 
when (b.launch_date is not null AND b.launch_date > a.closedate)  then 'launched'
when (c.status = 'closed' and (b.launch_date is null OR b.launch_date > a.closedate)) then 'closed'
else c.status
end as status,
--d.por, 
d.dp_ils_flag as dp_eligible, 
coalesce(d.vfm, o2.promotional_adj_pct/100) as vfm, 
d.voucher_flag as booking, 
coalesce(da.credit_card_fee_pct, a.cc_fee/100) as credit_card_fee_pct,
e.deal_discount, 
e.grpn_margin,
case when dc.efgi_flag = 0 then 0 else 1 end as od_eligible
from sandbox.cc_restrictions_dash as a
LEFT JOIN 
(
select deal_uuid,
min(load_date) launch_date
FROM user_groupondw.active_deals A
GROUP BY 1
) as b 
on a.deal_uuid = b.deal_uuid and b.launch_date > a.closedate
LEFT JOIN
user_groupondw.dim_deal as c
on a.deal_uuid = c.uuid
left join sandbox.eh_deal_structure as d 
on a.deal_uuid = d.deal_uuid
left join sandbox.rev_mgmt_deal_attributes as da
on a.deal_uuid = da.deal_id
left join sandbox.sup_analytics_deal_counts_final as dc
on a.deal_uuid = dc.deal_uuid and a.closedate = dc.report_date
left join sandbox.jl_average_option as e
on a.deal_uuid = e.deal_uuid
left join dwh_base_sec_view.sf_opportunity_2 as o2
on a.deal_uuid = o2.deal_uuid;

-----

select * from sandbox.eh_to_closes;

insert into sandbox.eh_to_closes
select
ng.account_id,
ng.opp_id as opportunity_id,
ng.deal_uuid,
case when da.start_date is null then 0 else 1 end as launched_flag,
dt.tier,
gl.test_group_hierarchy as variant,
gl.division as market,
gl.metal as metal,
gl.merchant_type,
gl.msa_flag,
gl.vertical as gl_vertical,
coalesce(da.vertical,ng.vertical) as vertical,
da.pds_cat_id,
coalesce(da.pds_name,ng.PDS) as pds,
da.grt_l3_cat_name,
da.grt_l4_cat_name,
da.start_date,
ddl.locations,
ng.rep,
ng.team_0,
ng.DSM,
ng.RSD,
--ng.team,
--ng.Account_name,
ng.Merchant_Seg_at_Closed_Won,
ng.CloseDate,
ng.Repurchase_control,
ng.New_Client_Window,
ng.cc_fee,
ng.options,
ng.POR,
ng.buyer_max,
ng.multi_voucher,
ng.voucher_type_online,
ng.TPIS as is_3pip,
ng."perm" as deal_permalink,
ng.gen_spend,
ng.DP_ILS_Flag,
ng.Expiration_flag,
ng.Repurchase_Flag,
COALESCE(dres.num_guests_res, 0) as num_guests_res,
COALESCE(dres.new_customer_res, 0) as new_customer_res,
COALESCE(dres.pref_guests_res, 0) as pref_guests_res,
COALESCE(dres.active_within_res, 0) as active_within_res,
COALESCE(dores.dt_time_res, 0) as dt_time_res,
case when dores.deal_uuid is null then 1 else 0 end as repurchase_res,
case when op.opportunity_name like ('%*CD*%') then 1 else 0 end as is_takeout_delivery,
ng.Add_Fee_Res,
ng.Menu_Ser_Res,
ng.Holiday_Res,
ng.Appointment_Res,
ng.service_rank,
ng.Top_100_flag,
ng.freq_goal,
ng.repurchase_times,
ng.launch_date,
ng.status,
ng.dp_eligible,
ng.od_eligible,
ng.vfm,
ng.booking,
dis.deal_discount,
dis.deal_margin,
dis.deal_margin + (coalesce(ng.cc_fee,0) / 100) as deal_margin_incl_cc,
case
 when dt.tier in ('1','3') then 1
 when dores.deal_uuid is null then 0
 when COALESCE(dres.new_customer_res, 0) > 0 then 0
 when COALESCE(dres.active_within_res, 0) > 0 then 0
 when COALESCE(dres.num_guests_res, 0) > 0 then 0
 when COALESCE(dres.pref_guests_res, 0) > 0 then 0
 when COALESCE(dores.dt_time_res, 0) > 0 then 0
 else 1
end as unrestricted_flag,
coalesce(btf.bookable_at_close,'0') as bookable_at_close,
cast(dw.week_end as date format 'yyyy-mm-dd') as close_wk,
td_month_end(ng.closedate) as close_mth,
td_quarter_end(ng.closedate) as close_qtr,
cast(dwl.week_end as date format 'yyyy-mm-dd') as launch_wk,
td_month_end(da.start_date) as launch_mth,
td_quarter_end(da.start_date) as launch_qtr
from (select * from sandbox.jl_new_groupon where closedate >= '2020-07-13') ng
left join
(
	select distinct 
	opportunity_id,
	tier
	from sandbox.sup_analytics_dim_deal_tier
	where is_current = 1
) dt on dt.opportunity_id = substr(ng.opp_id,1,15)
left join sandbox.eh_greenlist_detail gl on gl.account_id = ng.account_id
left join sandbox.rev_mgmt_deal_attributes da on da.deal_id = ng.deal_uuid
left join
(
	select
	dd.uuid as deal_uuid,
	count(distinct ddl.deal_location_key) as locations
	from user_groupondw.dim_deal_location ddl
	left join user_groupondw.dim_deal dd on dd.deal_key = ddl.deal_key
	group by 1
) ddl on ddl.deal_uuid = ng.deal_uuid
left join
(
	select
	opportunity_id,
	option_id,
	sell_price,
	CAST(CASE WHEN sell_price = 0.0 THEN 0.0 ELSE grpn_share/sell_price END AS DECIMAL(10,2)) AS deal_margin,
	CAST(CASE WHEN original_price = 0.0 THEN 0.0 ELSE discount/original_price END AS DECIMAL(10,2)) AS deal_discount
	FROM 
	(
		SELECT
		opportunity_id,
		option_id,
		original_price,
		sell_price,
		buy_price,
		CAST((original_price - sell_price) AS DECIMAL(20,2)) AS discount,
		CAST((sell_price - buy_price) AS DECIMAL(20,2)) AS grpn_share
		FROM 
		(
			SELECT
			smd.opportunity_id,
			smd.id AS option_id,
			CAST(smd.unit_value AS DECIMAL(20,2)) AS original_price,
			CAST(smd.unit_sell_price AS DECIMAL(20,2)) as sell_price,
			CAST(smd.unit_buy_price AS DECIMAL(20,2)) as buy_price,
			ROW_NUMBER() OVER(PARTITION BY smd.opportunity_id ORDER BY CAST(smd.unit_sell_price AS DECIMAL(20,2))) AS rnk
			FROM dwh_base_sec_view.sf_multi_deal smd
		) a
		WHERE rnk = 1
	) b
	qualify row_number() over(partition by opportunity_id order by sell_price asc) = 1
) dis on dis.opportunity_id = ng.opp_id
left join user_groupondw.dim_opportunity op on op.opportunity_id = substr(ng.opp_id,1,15)
left join user_groupondw.dim_day dy on dy.day_rw = ng.closedate
left join user_groupondw.dim_day dyl on dyl.day_rw = da.start_date
left join user_groupondw.dim_week dw on dw.week_key = dy.week_key
left join user_groupondw.dim_week dwl on dwl.week_key = dyl.week_key
left join 
(
	select
	deal_uuid,
	max(dt_time_res) as dt_time_res,
	max(buyer_max_res) as buyer_max_res
	FROM sandbox.temp_to_do_restrictions
	where deal_option_uuid is not null
	and coalesce(buyer_max,0) > 0
	and coalesce(repurchase_control,0) > 0
	and coalesce(buyer_max,0) >= coalesce(repurchase_control,0)
	group by 1
) dores on dores.deal_uuid = ng.deal_uuid
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
) dres on ng.deal_uuid = dres.deal_uuid
left join
(
	select
		deal_uuid,
		case 
			when bt_at_close = '1' then '1'
			when mbo_flag = '1' then '1'
			when tpis_flag = '1' then '1'
			else '0'
		end as bookable_at_close
	from 
	(
		select
			ng.deal_uuid,
			case
				when abs(cast(b.bt_date as date format 'YYYY-MM-DD') - cast(ng.launch_date as date format 'YYYY-MM-DD')) <= 7 then '1'
				else '0'
			end as bt_at_close,
			coalesce(c.mbo_flag,'0') as mbo_flag,
			coalesce(d.tpis_flag,'0') as tpis_flag
		from
		(select distinct deal_uuid,launch_date from sandbox.jl_new_groupon where closedate >= '2020-07-13') ng
		left join (
			select
				deal_uuid,
				'1' as bt_flag,
				min(load_date) bt_date
			from sandbox.sh_bt_active_deals_log 
			where
				partner_inactive_flag = '0'
				and product_is_active_flag = '1'
				and load_date >= '2020-07-13'
			group by 1,2) b on b.deal_uuid = ng.deal_uuid
		left join (
			select
				mbo.deal_uuid,
				'1' as mbo_flag
			from (
				select
					deal_uuid,
					max(report_date) as last_dt
				from sandbox.hbw_deals_bookings_flags
				where
					report_date >= '2019-12-01'
					and mbo_flag = 1
				group by deal_uuid) mbo
			left join
				(
				select
					deal_uuid,
					max(report_date) as out_date
				from sandbox.hbw_deals_bookings_flags
				where
					mbo_flag = 0
				group by deal_uuid) optout on mbo.deal_uuid = optout.deal_uuid
			where
				last_dt > coalesce(out_date,cast('2019-01-01' as date format 'YYYY-MM-DD'))) c on c.deal_uuid = ng.deal_uuid
		left join (
			select
				product_uuid as deal_uuid,
				'1' as tpis_flag
			from sandbox.bzops_booking_deals a
			left join (
				select
					distinct deal_id
				from
					user_gp.clo_offers)c on
				c.deal_id = a.product_uuid
			where
				c.deal_id is null
				and a.merchant_name not in ('Groupon To Go & BeautyNow & CLO (testing account)',
						'Groupon Select',
						'Grubhub',
						'giftango',
						'Gratafy',
						'Vagaro',
						'Vacation Express USA Corp (Grandchild)',
						'Epitourean',
						'Buffalo Wild Wings',
						'Uber',
						'TTD OPS Test Account - TEST ACCOUNT')) d on d.deal_uuid = ng.deal_uuid	
	) a
) btf on btf.deal_uuid = ng.deal_uuid
where case 
when op.opportunity_name like ('%*POR RL W1*%') then 0
when op.opportunity_name like ('%*POR Wave 2a RL*%') then 0
when op.opportunity_name like ('%*POR Wave 2b RL*%') then 0
when op.opportunity_name like ('%*POR ULA RL*%') then 0
when op.opportunity_name like ('%*POR Wave 3a RL*%') then 0
when op.opportunity_name like ('%*POR Wave 3b RL*%') then 0
when op.opportunity_name like ('%*POR Wave 3c RL*%') then 0
when op.opportunity_name like ('%*POR Wave 3d RL*%') then 0
when op.opportunity_name like ('%*POR Wave 3e RL*%') then 0
else 1
end = 1
--) with data primary index (opportunity_id)
;


select * from sandbox.tiered_offerings_base_table;