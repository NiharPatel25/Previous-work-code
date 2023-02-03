


-------------------------------------------MERCHANT ADVISOR



-----BUILD FUNNEL

drop table sandbox.jrg_comp_rec;
create  table sandbox.jrg_comp_rec as (
select 
	yu.created_at
	, yu.entity_uuid
	, yu.template_id
	, case when yu.template_id=1 then 'Add Option'
 			when yu.template_id=2 then 'Change Discount'
 			when yu.template_id=3 then 'Change Voucher Cap'
 			when yu.template_id=4 then 'Upload Photos'
 			when yu.template_id=5 then 'Change Purchase Frequency'
 			when yu.template_id=6 then 'Change Purchase Limit'
            when yu.template_id=8 then 'Unpause Campaign'
 			when yu.template_id=9 then 'Add Attributes'
            when yu.template_id=10 then 'Build Campaign'
            when yu.template_id=11 then 'Increase Voucher Cap'
 			when yu.template_id=12 then 'Extend Campaign'
            when yu.template_id=13 then 'Relaunch Campaign'
            when yu.template_id=14 then 'Reply to Feedback'
            when yu.template_id=15 then 'Add Option'
			when yu.template_id=17 then 'Sponsor Campaign'
			when yu.template_id=21 then 'Learn More'
 			else 'Other' end recommendation_type --- other should never appear if it does that means there is a new recommendation type
 	, yu.recommendation_id
 	, e.entity_type
 	, e.merchant_uuid 
 	, case when e.entity_type ='DEAL' then yu.entity_uuid
 			when e.entity_type ='INVENTORY_PRODUCT' then opp.deal_uuid
 			when e.entity_type ='PLACE' then pl.deal_uuid 
 			end deal_id
 	, m.current_metal_segment
 	, m.country_code
	, m.acct_owner as merchant_acct_owner
 	, case when m.country_code in ('US', 'CA') then 'NAM' else 'INTL' end region
 	, m.division
 	, m.l2 as vertical
 	, m.l4 
 	, (case when mtd.account_owner in ('New Metro','Existing Metro') then 'Metro'
   	 		when mtd.account_owner in ('BD','MD', 'BD/MD') then 'MD'
   	 		when mtd.account_owner in ('Inbound','Existing MS') then 'MS'
   	 		else 'Other' end) acct_owner
 	, tr.created_at actioned_date
 	, tr.delete_event_type
from (
		select 
			recommendation_id
			, entity_uuid
			, template_id
			, created_at
		from sb_merchant_experience.recommendations
				UNION ALL
		select 
			recommendation_id
			, entity_uuid
			, template_id
			, initial_creation as created_at
		from sb_merchant_experience.recommendations_tracker-- holds only completed recommendations. when a recommendation is completed it leaves sb_merchant_experience.recommendations and comes to this table
		where recommendation_id not in (select distinct recommendation_id from sb_merchant_experience.recommendations) -- sometimes the recommendation will still be in the other table dues to refresh schedule but we dont want any dupes so we take that out
		and initial_creation is not null
) yu
join sb_merchant_experience.merchant_advisor_entities e on e.entity_uuid = yu.entity_uuid ---what is this? --just using this to get merchant uuid?
left join sb_merchant_experience.recommendations_tracker tr on tr.recommendation_id = yu.recommendation_id
left join (select distinct
				inv_product_uuid
				, product_uuid deal_uuid
			from user_edwprod.dim_offer_ext 
			group by 1,2
			) opp
	on opp.inv_product_uuid = yu.entity_uuid
left join (
			select 
				 max(tt.product_uuid) deal_uuid
				, p.place_uuid
			from user_edwprod.dim_places p
			join user_edwprod.dim_merchant_places_map m
				on m.place_uuid = p.place_uuid
			left join user_edwprod.deal_inv_prd_redloc rl
				on rl.redemption_location_id = p.place_uuid
			left join user_edwprod.dim_offer_ext tt
				on rl.product_id =tt.inv_product_uuid
			group by 2
			) pl 
	on pl.place_uuid = yu.entity_uuid
join sandbox.pai_merchants m 
	on m.merchant_uuid = e.merchant_uuid
left  join (
				select
        			deal_uuid
        			, max(case when feature_country = 'US' and mtd_attribution = 'BD' and dmapi_flag = 1 then 'MD'
        			  			when feature_country = 'US' then mtd_attribution
        			  			when feature_country <> 'US' then mtd_attribution_intl end) account_owner
 				from sandbox.jc_merchant_mtd_attrib def
        		group by deal_uuid
    ) mtd
	on mtd.deal_uuid = deal_id  
)with data unique primary index (recommendation_id);


create volatile table jrg_live_merc as (
select
	b.merchant_uuid
	--, a.deal_uuid
    , max(load_date) last_date
	, min(load_date) launch_date
    from user_groupondw.active_deals a
    join sandbox.pai_deals b
    	on b.deal_uuid = a.deal_uuid
    where load_date > '2021-06-01'
   group by 1
  )with data unique primary index (merchant_uuid) on commit preserve rows;
  
 ---for live merchants after june 2021. doing a cross join for every day that is there to every merchant. to see upto which date merchant was show the reccommendation before actioned on
create VOLATILE table jrg_dates as (
select 
yu.day_rw
, yu.merchant_uuid
, ty.recommendation_id
from (
		select  distinct
			r.merchant_uuid
			, dd.day_rw
		from jrg_comp_rec  r
		cross join user_groupondw.dim_day dd
		where dd.day_rw between date '2021-06-20' and current_date
) yu
join (
		select 
			merchant_uuid
			, recommendation_id
			, min(created_at) created_at
			, max(actioned_date) actioned_date
		from jrg_comp_rec
		group by 1,2
) ty 
	on ty.merchant_uuid = yu.merchant_uuid
join jrg_live_merc dd on dd.merchant_uuid = yu.merchant_uuid
where  cast(ty.created_at as date ) <= day_rw  ---similar to greated than created date
and day_rw<= cast(coalesce(ty.actioned_date, current_date) as date )
and day_rw between dd.launch_date and dd.last_date
)with data unique primary index (recommendation_id, merchant_uuid, day_rw) on commit preserve rows;



--table imported in the first dash 
drop table sandbox.jrg_weekly_ma;
create table sandbox.jrg_weekly_ma as (
select 
	ma.* , case when coalesce(fgt.units_sold,0) = 0 then 0 else 1 end sold_units 
from (
select 
	coalesce(hu.report_wk ,lu.report_wk) report_week
	, coalesce(hu.report_mth,lu.report_mth) report_mth
	, coalesce(hu.country_code, lu.country_code) country
	, coalesce(hu.region, lu.region) region
	, coalesce(hu.division, lu.division) division
	, coalesce(hu.acct_owner, lu.acct_owner) acct_owner
 	, coalesce(hu.metal, lu.metal) metal
 	, coalesce(hu.vertical, lu.vertical) vertical 
 	, coalesce(hu.l4, lu.l4) l4
 	, coalesce(hu.merchant_uuid, lu.merchant_uuid) merchant_uuid
 	, coalesce(hu.recommendation_type,lu.recommendation_type) recommendation_type
 	, coalesce(hu.recommendation_id,lu.recommendation_id) recommendation_id
 	, n_open_recs
 	, n_recs_completed
	, n_recs_accepted
	, n_recs_deleted
	, options_added
	, case when coalesce(n_recs_completed,0)> 0 then 1 else recs_seen end recs_seen
from (
		select 
			coalesce(a.report_wk ,b.report_wk) report_wk
			, coalesce(a.report_mth,b.report_mth) report_mth
			, coalesce(a.country_code, b.country_code) country_code
			, coalesce(a.region, b.region) region
			, coalesce(a.division, b.division) division
			, coalesce(a.acct_owner, b.acct_owner) acct_owner
 			, coalesce(a.metal, b.metal) metal
 			, coalesce(a.vertical, b.vertical) vertical 
 			, coalesce(a.l4, b.l4) l4
 			, coalesce(a.merchant_uuid, b.merchant_uuid) merchant_uuid
 			, coalesce(a.recommendation_type, b.recommendation_type) recommendation_type
 			, coalesce(a.recommendation_id, b.recommendation_id)recommendation_id
 			, n_open_recs
 			, n_recs_completed
			, n_recs_accepted
			, n_recs_deleted
			, options_added
		from ( 
				select 
					cast(dw.week_end as date) as report_wk
					, trunc(ggg.day_rw, 'RM') report_mth
					, country_code
					, region
					, case when merchant_acct_owner='House' then acct_owner
							when coalesce(merchant_acct_owner,'GONE')='GONE' then acct_owner
							else merchant_acct_owner end acct_owner
					, division
					, a.vertical 
					, a.l4
					, case when lower(current_metal_segment) in ('silver','gold','platinum') then 's+' else 'b-' end  as metal
					, a.merchant_uuid
					, recommendation_type
					, a.recommendation_id
					, count(distinct case when ggg.day_rw between cast(created_at as date) and coalesce(cast(actioned_date as date), date '2999-01-01') then a.recommendation_id end) n_open_recs
				from jrg_comp_rec a
				join jrg_dates ggg 
					on ggg.merchant_uuid = a.merchant_uuid
					and ggg.recommendation_id = a.recommendation_id
				join user_groupondw.dim_day dd
					on ggg.day_rw = dd.day_rw
				join user_groupondw.dim_week dw
					on dd.week_key = dw.week_key
				group by 1,2,3,4,5,6,7,8,9,10,11,12
			) a
		full outer join (
							select 
								cast(dw.week_end as date) as report_wk
								, trunc(a.actioned_date,'RM') report_mth
								, country_code
								, region
								, case when merchant_acct_owner='House' then acct_owner
										when coalesce(merchant_acct_owner,'GONE')='GONE' then acct_owner
										else merchant_acct_owner end acct_owner
								, division
								, a.vertical
								, a.l4
								, case when lower(current_metal_segment) in ('silver','gold','platinum') then 's+' else 'b-' end as metal
								, a.merchant_uuid
								, recommendation_type
								, recommendation_id
								, case when recommendation_type = 'Add Option' then inv.options_added end as options_added
								, count(distinct case when actioned_date is not null then recommendation_id end) n_recs_completed
								, count(distinct case when delete_event_type='ACTIONED' then recommendation_id end) n_recs_accepted
								, count(distinct case when delete_event_type='DELETED' then recommendation_id end) n_recs_deleted
							from jrg_comp_rec a
							join user_groupondw.dim_day dd
								on (a.actioned_date) = dd.day_rw
							join user_groupondw.dim_week dw
								on dd.week_key = dw.week_key
							left join (
											select 
												i.first_live_date
												, i.deal_uuid
												, d.merchant_uuid
												, count(distinct i.inv_product_uuid) options_added
											from sandbox.pai_options i
											join sandbox.pai_deals d 
												on d.deal_uuid = i.deal_uuid
											group by 1,2,3
										) inv 
								on inv.deal_uuid = case when entity_type ='DEAL' then a.entity_uuid end
								and inv.first_live_date between a.actioned_date - interval '1' day and a.actioned_date + interval '1' day
							group by 1,2,3,4,5,6,7,8,9,10,11,12,13
			
	) b
		on b.report_wk = a.report_wk
		and a.report_mth = b.report_mth
		and b.country_code = a.country_code
		and b.region = a.region
		and b.acct_owner = a.acct_owner
		and b.metal = a.metal
		and b.division = a.division
		and b.merchant_uuid = a.merchant_uuid
		and b.recommendation_type = b.recommendation_type
		and b.recommendation_id = a.recommendation_id
		and b.vertical = a.vertical
		and b.l4 = a.l4
) hu
full outer join (
					select 
						cast(dw.week_end as date) as report_wk
						, trunc(cast(ti.report_date as date), 'RM') report_mth
						, mm.country_code
						, region
						, case when merchant_acct_owner='House' then acct_owner
									when coalesce(merchant_acct_owner,'GONE')='GONE' then acct_owner
									else merchant_acct_owner end acct_owner
						, division
						, mm.vertical
						, mm.l4
						, case when lower(current_metal_segment) in ('silver','gold','platinum') then 's+' else 'b-' end as metal
						, ti.merchant_uuid
						, ti.recommendation_type
						, ti.recommendation_id						
						, count(distinct ti.recommendation_id) recs_seen
					from (
							select 
								report_date
								, merchant_uuid
								, recommendation_id 
								, recommendation_type
							from sandbox.jrg_ma_hp 
								union all 
							select 
								report_date
								, merchant_uuid
								, recommendation_id 
								, recommendation_type
							from sandbox.jrg_ma_ad
								union all 
							select
								a.event_date as report_date 
								, a.merchant_uuid
								, case when cast(a.event_date as date) between b.created_at and b.actioned_date then b.recommendation_id end recommendation_id
								, case when cast(a.event_date as date) between b.created_at and b.actioned_date then b.recommendation_type end recommendation_type
							from sandbox.jrg_inline_rec a 
							left join jrg_comp_rec b 
								on b.merchant_uuid = a.merchant_uuid
						) ti 
						join jrg_comp_rec mm 
 							on mm.recommendation_id = ti.recommendation_id
 						join user_groupondw.dim_day dd
							on (ti.report_date) = dd.day_rw
						join user_groupondw.dim_week dw
							on dd.week_key = dw.week_key
						where ti.recommendation_id not in (select distinct recommendation_id from sb_merchant_experience.recommendations_tracker where initial_creation is null)
						and ti.recommendation_id is not null
					group by 1,2,3,4,5,6,7,8,9,10,11,12
				) lu
		on lu.report_wk = hu.report_wk
		and lu.report_mth = hu.report_mth
		and lu.country_code = hu.country_code
		and lu.region = hu.region
		and lu.acct_owner = hu.acct_owner
		and lu.metal = hu.metal
		and lu.division = hu.division
		and lu.merchant_uuid = hu.merchant_uuid
		and lu.recommendation_type = hu.recommendation_type
		and lu.recommendation_id = hu.recommendation_id
		and lu.vertical = hu.vertical
		and lu.l4 = hu.l4
	) ma 
left join (
			select
				cast(dw.week_end as date) report_wk
  				, merchant_uuid
  				, sum(transaction_qty) units_sold
  				, sum(auth_nob_loc) nob
			from user_edwprod.fact_gbl_transactions a
			join user_groupondw.dim_day dd
				on a.order_date = dd.day_rw
			join user_groupondw.dim_week dw
				on dd.week_key = dw.week_key
			where action = 'authorize'
			and is_order_canceled = 0
			and is_zero_amount = 0
			and user_brand_affiliation ='groupon'
			and platform_key = 1
			and order_date between date'2021-06-12' and current_date
			group by 1,2
		) fgt
	on fgt.report_wk = ma.report_week
	and fgt.merchant_uuid = ma.merchant_uuid
)with data primary index(report_week, country, region, acct_owner, metal, division, merchant_uuid,recommendation_type, recommendation_id);

grant select on sandbox.jrg_weekly_ma to public


----------NEW FUNNEL


drop table sandbox.jrg_comp_rec;create  table sandbox.jrg_comp_rec as (
select 
	yu.created_at
	, yu.entity_uuid
	, yu.template_id
	, case when yu.template_id=1 then 'Add Option'
 			when yu.template_id=2 then 'Change Discount'
 			when yu.template_id=3 then 'Change Voucher Cap'
 			when yu.template_id=4 then 'Upload Photos'
 			when yu.template_id=5 then 'Change Purchase Frequency'
 			when yu.template_id=6 then 'Change Purchase Limit'
            when yu.template_id=8 then 'Unpause Campaign'
 			when yu.template_id=9 then 'Add Attributes'
            when yu.template_id=10 then 'Build Campaign'
            when yu.template_id=11 then 'Increase Voucher Cap'
 			when yu.template_id=12 then 'Extend Campaign'
            when yu.template_id=13 then 'Relaunch Campaign'
            when yu.template_id=14 then 'Reply to Feedback'
            when yu.template_id=15 then 'Add Option'
			when yu.template_id=17 then 'Sponsor Campaign'
			when yu.template_id=21 then 'Learn More'
 			else 'Other' end recommendation_type --- other should never appear if it does that means there is a new recommendation type
 	, yu.recommendation_id
 	, e.entity_type
 	, e.merchant_uuid 
 	, case when e.entity_type ='DEAL' then yu.entity_uuid
 			when e.entity_type ='INVENTORY_PRODUCT' then opp.deal_uuid
 			when e.entity_type ='PLACE' then pl.deal_uuid 
 			end deal_id
 	, m.current_metal_segment
 	, m.country_code
	, m.acct_owner as merchant_acct_owner
 	, case when m.country_code in ('US', 'CA') then 'NAM' else 'INTL' end region
 	, m.division
 	, m.l2 as vertical
 	, m.l4 
 	, (case when mtd.account_owner in ('New Metro','Existing Metro') then 'Metro'
   	 		when mtd.account_owner in ('BD','MD', 'BD/MD') then 'MD'
   	 		when mtd.account_owner in ('Inbound','Existing MS') then 'MS'
   	 		else 'Other' end) acct_owner
 	, tr.created_at actioned_date
 	, tr.delete_event_type
from (
		select 
			recommendation_id
			, entity_uuid
			, template_id
			, created_at
		from sb_merchant_experience.recommendations
				UNION ALL
		select 
			recommendation_id
			, entity_uuid
			, template_id
			, initial_creation as created_at
		from sb_merchant_experience.recommendations_tracker-- holds only completed recommendations. when a recommendation is completed it leaves sb_merchant_experience.recommendations and comes to this table
		where recommendation_id not in (select distinct recommendation_id from sb_merchant_experience.recommendations) -- sometimes the recommendation will still be in the other table dues to refresh schedule but we dont want any dupes so we take that out
		and initial_creation is not null
) yu
join sb_merchant_experience.merchant_advisor_entities e 
	on e.entity_uuid = yu.entity_uuid
left join sb_merchant_experience.recommendations_tracker tr 
	on tr.recommendation_id = yu.recommendation_id
left join (
			select distinct
				inv_product_uuid
				, product_uuid deal_uuid
			from user_edwprod.dim_offer_ext 
			group by 1,2
			) opp
	on opp.inv_product_uuid = yu.entity_uuid
left join (
			select 
				 max(tt.product_uuid) deal_uuid
				, p.place_uuid
			from user_edwprod.dim_places p
			join user_edwprod.dim_merchant_places_map m
				on m.place_uuid = p.place_uuid
			left join user_edwprod.deal_inv_prd_redloc rl
				on rl.redemption_location_id = p.place_uuid
			left join user_edwprod.dim_offer_ext tt
				on rl.product_id =tt.inv_product_uuid
			group by 2
			) pl 
	on pl.place_uuid = yu.entity_uuid
join sandbox.pai_merchants m 
	on m.merchant_uuid = e.merchant_uuid
left  join (
				select
        			deal_uuid
        			, max(case when feature_country = 'US' and mtd_attribution = 'BD' and dmapi_flag = 1 then 'MD'
        			  			when feature_country = 'US' then mtd_attribution
        			  			when feature_country <> 'US' then mtd_attribution_intl end) account_owner
 				from sandbox.jc_merchant_mtd_attrib def
        		group by deal_uuid
    ) mtd
	on mtd.deal_uuid = deal_id  
)with data unique primary index (recommendation_id);create volatile table jrg_live_merc as (
select
	b.merchant_uuid
	--, a.deal_uuid
    , max(load_date) last_date
	, min(load_date) launch_date
    from user_groupondw.active_deals a
    join sandbox.pai_deals b
    	on b.deal_uuid = a.deal_uuid
    where load_date > '2021-06-01'
   group by 1
  )with data unique primary index (merchant_uuid) on commit preserve rows;create VOLATILE table jrg_dates as (
select 
yu.day_rw
, yu.merchant_uuid
, ty.recommendation_id
from (
		select  distinct
			r.merchant_uuid
			, dd.day_rw
		from jrg_comp_rec  r
		cross join user_groupondw.dim_day dd
		where dd.day_rw between date '2021-06-20' and current_date
) yu
join (
		select 
			merchant_uuid
			, recommendation_id
			, min(created_at) created_at
			, max(actioned_date) actioned_date
		from jrg_comp_rec
		group by 1,2
) ty 
	on ty.merchant_uuid = yu.merchant_uuid
join jrg_live_merc dd 
	on dd.merchant_uuid = yu.merchant_uuid
where  cast(ty.created_at as date ) <= day_rw
and day_rw<= cast(coalesce(ty.actioned_date, current_date) as date )
and day_rw between dd.launch_date and dd.last_date
)with data unique primary index (recommendation_id, merchant_uuid, day_rw) on commit preserve rows;drop table sandbox.jrg_weekly_ma;create table sandbox.jrg_weekly_ma as (
select 
	ma.* , case when coalesce(fgt.units_sold,0) = 0 then 0 else 1 end sold_units 
from (
select 
	coalesce(hu.report_wk ,lu.report_wk) report_week
	, coalesce(hu.report_mth,lu.report_mth) report_mth
	, coalesce(hu.country_code, lu.country_code) country
	, coalesce(hu.region, lu.region) region
	, coalesce(hu.division, lu.division) division
	, coalesce(hu.acct_owner, lu.acct_owner) acct_owner
 	, coalesce(hu.metal, lu.metal) metal
 	, coalesce(hu.vertical, lu.vertical) vertical 
 	, coalesce(hu.l4, lu.l4) l4
 	, coalesce(hu.merchant_uuid, lu.merchant_uuid) merchant_uuid
 	, coalesce(hu.recommendation_type,lu.recommendation_type) recommendation_type
 	, coalesce(hu.recommendation_id,lu.recommendation_id) recommendation_id
 	, n_open_recs
 	, n_recs_completed
	, n_recs_accepted
	, n_recs_deleted
	, options_added
	, case when coalesce(n_recs_completed,0)> 0 then 1 else recs_seen end recs_seen
from (
		select 
			coalesce(a.report_wk ,b.report_wk) report_wk
			, coalesce(a.report_mth,b.report_mth) report_mth
			, coalesce(a.country_code, b.country_code) country_code
			, coalesce(a.region, b.region) region
			, coalesce(a.division, b.division) division
			, coalesce(a.acct_owner, b.acct_owner) acct_owner
 			, coalesce(a.metal, b.metal) metal
 			, coalesce(a.vertical, b.vertical) vertical 
 			, coalesce(a.l4, b.l4) l4
 			, coalesce(a.merchant_uuid, b.merchant_uuid) merchant_uuid
 			, coalesce(a.recommendation_type, b.recommendation_type) recommendation_type
 			, coalesce(a.recommendation_id, b.recommendation_id)recommendation_id
 			, n_open_recs
 			, n_recs_completed
			, n_recs_accepted
			, n_recs_deleted
			, options_added
		from ( 
				select 
					cast(dw.week_end as date) as report_wk
					, trunc(ggg.day_rw, 'RM') report_mth
					, country_code
					, region
					, case when merchant_acct_owner='House' then acct_owner
							when coalesce(merchant_acct_owner,'GONE')='GONE' then acct_owner
							else merchant_acct_owner end acct_owner
					, division
					, a.vertical 
					, a.l4
					, case when lower(current_metal_segment) in ('silver','gold','platinum') then 's+' else 'b-' end  as metal
					, a.merchant_uuid
					, recommendation_type
					, a.recommendation_id
					, count(distinct case when ggg.day_rw between cast(created_at as date) and coalesce(cast(actioned_date as date), date '2999-01-01') then a.recommendation_id end) n_open_recs
				from jrg_comp_rec a
				join jrg_dates ggg 
					on ggg.merchant_uuid = a.merchant_uuid
					and ggg.recommendation_id = a.recommendation_id
				join user_groupondw.dim_day dd
					on ggg.day_rw = dd.day_rw
				join user_groupondw.dim_week dw
					on dd.week_key = dw.week_key
				group by 1,2,3,4,5,6,7,8,9,10,11,12
			) a
		full outer join (
							select 
								cast(dw.week_end as date) as report_wk
								, trunc(a.actioned_date,'RM') report_mth
								, country_code
								, region
								, case when merchant_acct_owner='House' then acct_owner
										when coalesce(merchant_acct_owner,'GONE')='GONE' then acct_owner
										else merchant_acct_owner end acct_owner
								, division
								, a.vertical
								, a.l4
								, case when lower(current_metal_segment) in ('silver','gold','platinum') then 's+' else 'b-' end as metal
								, a.merchant_uuid
								, recommendation_type
								, recommendation_id
								, case when recommendation_type = 'Add Option' then inv.options_added end as options_added
								, count(distinct case when actioned_date is not null then recommendation_id end) n_recs_completed
								, count(distinct case when delete_event_type='ACTIONED' then recommendation_id end) n_recs_accepted
								, count(distinct case when delete_event_type='DELETED' then recommendation_id end) n_recs_deleted
							from jrg_comp_rec a
							join user_groupondw.dim_day dd
								on (a.actioned_date) = dd.day_rw
							join user_groupondw.dim_week dw
								on dd.week_key = dw.week_key
							left join (
											select 
												i.first_live_date
												, i.deal_uuid
												, d.merchant_uuid
												, count(distinct i.inv_product_uuid) options_added
											from sandbox.pai_options i
											join sandbox.pai_deals d 
												on d.deal_uuid = i.deal_uuid
											group by 1,2,3
										) inv 
								on inv.deal_uuid = case when entity_type ='DEAL' then a.entity_uuid end
								and inv.first_live_date between a.actioned_date - interval '1' day and a.actioned_date + interval '1' day
							group by 1,2,3,4,5,6,7,8,9,10,11,12,13
			
	) b
		on b.report_wk = a.report_wk
		and a.report_mth = b.report_mth
		and b.country_code = a.country_code
		and b.region = a.region
		and b.acct_owner = a.acct_owner
		and b.metal = a.metal
		and b.division = a.division
		and b.merchant_uuid = a.merchant_uuid
		and b.recommendation_type = b.recommendation_type
		and b.recommendation_id = a.recommendation_id
		and b.vertical = a.vertical
		and b.l4 = a.l4
) hu
full outer join (
					select 
						cast(dw.week_end as date) as report_wk
						, trunc(cast(ti.report_date as date), 'RM') report_mth
						, mm.country_code
						, region
						, case when merchant_acct_owner='House' then acct_owner
									when coalesce(merchant_acct_owner,'GONE')='GONE' then acct_owner
									else merchant_acct_owner end acct_owner
						, division
						, mm.vertical
						, mm.l4
						, case when lower(current_metal_segment) in ('silver','gold','platinum') then 's+' else 'b-' end as metal
						, ti.merchant_uuid
						, ti.recommendation_type
						, ti.recommendation_id						
						, count(distinct ti.recommendation_id) recs_seen
					from (
							select 
								report_date
								, merchant_uuid
								, recommendation_id 
								, recommendation_type
							from sandbox.jrg_ma_hp 
								union all 
							select 
								report_date
								, merchant_uuid
								, recommendation_id 
								, recommendation_type
							from sandbox.jrg_ma_ad
								union all 
							select
								a.event_date as report_date 
								, a.merchant_uuid
								, case when cast(a.event_date as date) between b.created_at and b.actioned_date then b.recommendation_id end recommendation_id
								, case when cast(a.event_date as date) between b.created_at and b.actioned_date then b.recommendation_type end recommendation_type
							from sandbox.jrg_inline_rec a 
							left join jrg_comp_rec b 
								on b.merchant_uuid = a.merchant_uuid
						) ti 
						join jrg_comp_rec mm 
 							on mm.recommendation_id = ti.recommendation_id
 						join user_groupondw.dim_day dd
							on (ti.report_date) = dd.day_rw
						join user_groupondw.dim_week dw
							on dd.week_key = dw.week_key
						where ti.recommendation_id not in (select distinct recommendation_id from sb_merchant_experience.recommendations_tracker where initial_creation is null)
						and ti.recommendation_id is not null
					group by 1,2,3,4,5,6,7,8,9,10,11,12
				) lu
		on lu.report_wk = hu.report_wk
		and lu.report_mth = hu.report_mth
		and lu.country_code = hu.country_code
		and lu.region = hu.region
		and lu.acct_owner = hu.acct_owner
		and lu.metal = hu.metal
		and lu.division = hu.division
		and lu.merchant_uuid = hu.merchant_uuid
		and lu.recommendation_type = hu.recommendation_type
		and lu.recommendation_id = hu.recommendation_id
		and lu.vertical = hu.vertical
		and lu.l4 = hu.l4
	) ma 
left join (
			select
				cast(dw.week_end as date) report_wk
  				, merchant_uuid
  				, sum(transaction_qty) units_sold
  				, sum(auth_nob_loc) nob
			from user_edwprod.fact_gbl_transactions a
			join user_groupondw.dim_day dd
				on a.order_date = dd.day_rw
			join user_groupondw.dim_week dw
				on dd.week_key = dw.week_key
			where action = 'authorize'
			and is_order_canceled = 0
			and is_zero_amount = 0
			and user_brand_affiliation ='groupon'
			and platform_key = 1
			and order_date between date'2021-06-12' and current_date
			group by 1,2
		) fgt
	on fgt.report_wk = ma.report_week
	and fgt.merchant_uuid = ma.merchant_uuid
)with data primary index(report_week, country, region, acct_owner, metal, division, merchant_uuid,recommendation_type, recommendation_id);grant select on sandbox.jrg_weekly_ma to public

-----------------PERFORMANCE BASE

create VOLATILE table jrg_dates as (
select 
yu.day_rw
, yu.merchant_uuid
, ty.recommendation_id
from (
		select  distinct
			r.merchant_uuid
			, dd.day_rw
		from jrg_comp_rec  r
		cross join user_groupondw.dim_day dd
		where dd.day_rw between date '2021-06-20' and current_date
) yu
join (
		select 
			merchant_uuid
			, recommendation_id
			, min(created_at) created_at
			, max(actioned_date) actioned_date
		from jrg_comp_rec
		group by 1,2
) ty 
	on ty.merchant_uuid = yu.merchant_uuid
where  cast(ty.created_at as date ) <= day_rw
and day_rw<= cast(coalesce(ty.actioned_date, current_date) as date )
)with data unique primary index (recommendation_id, merchant_uuid, day_rw) on commit preserve rows;drop table sandbox.jrg_weekly_ma_b;create table sandbox.jrg_weekly_ma_b as (
select 
	coalesce(hu.report_wk ,lu.report_wk) report_week
	, coalesce(hu.report_mth ,lu.report_mth) report_mth
	, coalesce(hu.country_code, lu.country_code) country
	, coalesce(hu.region, lu.region) region
	, coalesce(hu.division, lu.division) division
	, coalesce(hu.acct_owner, lu.acct_owner) acct_owner
 	, coalesce(hu.metal, lu.metal) metal
 	, coalesce(hu.merchant_uuid, lu.merchant_uuid) merchant_uuid
 	, coalesce(hu.recommendation_type,lu.recommendation_type) recommendation_type
 	, coalesce(hu.recommendation_id,lu.recommendation_id) recommendation_id
 	, c.deal_id as deal_uuid
 	, n_open_recs
 	, n_recs_completed
	, n_recs_accepted
	, n_recs_deleted
	, case when coalesce(n_recs_completed,0)> 0 then 1 else recs_seen end recs_seen
from (
		select 
			coalesce(a.report_wk ,b.report_wk) report_wk
			, coalesce(a.report_mth ,b.report_mth) report_mth
			, coalesce(a.country_code, b.country_code) country_code
			, coalesce(a.region, b.region) region
			, coalesce(a.division, b.division) division
			, coalesce(a.acct_owner, b.acct_owner) acct_owner
 			, coalesce(a.metal, b.metal) metal
 			, coalesce(a.merchant_uuid, b.merchant_uuid) merchant_uuid
 			, coalesce(a.recommendation_type, b.recommendation_type) recommendation_type
 			, coalesce(a.recommendation_id, b.recommendation_id) recommendation_id
 			, n_open_recs
 			, n_recs_completed
			, n_recs_accepted
			, n_recs_deleted
		from ( 
				select 
					cast(dw.week_end as date) as report_wk
					, trunc(ggg.day_rw, 'RM') as report_mth
					, country_code
					, region
					, case when merchant_acct_owner='House' then acct_owner
								when coalesce(merchant_acct_owner,'GONE')='GONE' then acct_owner
								else merchant_acct_owner end acct_owner
					, division
					, case when lower(current_metal_segment) in ('silver','gold','platinum') then 's+' else 'b-' end  as metal
					, a.merchant_uuid
					, recommendation_type
					, a.recommendation_id
					, count(distinct case when ggg.day_rw between cast(created_at as date) and coalesce(cast(actioned_date as date), date '2999-01-01') then a.recommendation_id end) n_open_recs
				from jrg_comp_rec a
				join jrg_dates ggg 
					on ggg.merchant_uuid = a.merchant_uuid
					and ggg.recommendation_id = a.recommendation_id
				join user_groupondw.dim_day dd
					on ggg.day_rw = dd.day_rw
				join user_groupondw.dim_week dw
					on dd.week_key = dw.week_key
				group by 1,2,3,4,5,6,7,8,9,10
			) a
		full outer join (
							select 
								cast(dw.week_end as date) as report_wk
								, trunc(actioned_date, 'RM') report_mth
								, country_code
								, region
								, case when merchant_acct_owner='House' then acct_owner
											when coalesce(merchant_acct_owner,'GONE')='GONE' then acct_owner
											else merchant_acct_owner end acct_owner
								, division
								, case when lower(current_metal_segment) in ('silver','gold','platinum') then 's+' else 'b-' end as metal
								, merchant_uuid
								, recommendation_type
								, recommendation_id
								, count(distinct case when actioned_date is not null then recommendation_id end) n_recs_completed
								, count(distinct case when delete_event_type='ACTIONED' then recommendation_id end) n_recs_accepted
								, count(distinct case when delete_event_type='DELETED' then recommendation_id end) n_recs_deleted
							from jrg_comp_rec a
							join user_groupondw.dim_day dd
								on (a.actioned_date) = dd.day_rw
							join user_groupondw.dim_week dw
								on dd.week_key = dw.week_key
							group by 1,2,3,4,5,6,7,8,9,10
			
	) b
		on b.report_wk = a.report_wk
		and b.country_code = a.country_code
		and b.region = a.region
		and b.acct_owner = a.acct_owner
		and b.metal = a.metal
		and b.division = a.division
		and b.merchant_uuid = a.merchant_uuid
		and b.recommendation_type = b.recommendation_type
		and b.recommendation_id = a.recommendation_id
	    --and b.deal_id = a.deal_id 
	    and b.report_mth = a.report_mth
) hu
full outer join (
					select 
						cast(dw.week_end as date) as report_wk
						, trunc(cast(ti.report_date as date), 'RM') report_mth
						, mm.country_code
						, region
						, case when merchant_acct_owner='House' then acct_owner
									when coalesce(merchant_acct_owner,'GONE')='GONE' then acct_owner
									else merchant_acct_owner end acct_owner
						, division
						, case when lower(current_metal_segment) in ('silver','gold','platinum') then 's+' else 'b-' end as metal
						, ti.merchant_uuid
						, ti.recommendation_type
						, ti.recommendation_id	
						, count(distinct ti.recommendation_id) recs_seen
					from (
							select 
								report_date
								, merchant_uuid
								, recommendation_id 
								, recommendation_type
							from sandbox.jrg_ma_hp 
								union all 
							select 
								report_date
								, merchant_uuid
								, recommendation_id 
								, recommendation_type
							from sandbox.jrg_ma_ad
								union all 
							select
								a.event_date as report_date 
								, a.merchant_uuid
								, case when cast(a.event_date as date) between b.created_at and b.actioned_date then b.recommendation_id end recommendation_id
								, case when cast(a.event_date as date) between b.created_at and b.actioned_date then b.recommendation_type end recommendation_type
							from sandbox.jrg_inline_rec a 
							left join jrg_comp_rec b 
								on b.merchant_uuid = a.merchant_uuid
						) ti 
						join jrg_comp_rec mm 
 							on mm.recommendation_id = ti.recommendation_id
 						join user_groupondw.dim_day dd
							on (ti.report_date) = dd.day_rw
						join user_groupondw.dim_week dw
							on dd.week_key = dw.week_key
						where ti.recommendation_id not in (select distinct recommendation_id from sb_merchant_experience.recommendations_tracker where initial_creation is null)
						and ti.recommendation_id is not null
					group by 1,2,3,4,5,6,7,8,9,10
				) lu
		on lu.report_wk = hu.report_wk
		and lu.country_code = hu.country_code
		and lu.region = hu.region
		and lu.acct_owner = hu.acct_owner
		and lu.metal = hu.metal
		and lu.division = hu.division
		and lu.merchant_uuid = hu.merchant_uuid
		and lu.recommendation_type = hu.recommendation_type
		and lu.recommendation_id = hu.recommendation_id
		and lu.report_mth = hu.report_mth
		--and lu.deal_id = hu.deal_id
  join jrg_comp_rec c 
    on coalesce(hu.recommendation_id,lu.recommendation_id) = c.recommendation_id
)with data primary index(report_week, country, region, acct_owner, metal, division, merchant_uuid,recommendation_type, recommendation_id);grant select on sandbox.jrg_weekly_ma_b to public;drop table sandbox.jrg_fin_v;create table sandbox.jrg_fin_v as (
select    
coalesce(yu.report_date,lo.report_date) report_date,
          coalesce(yu.deal_id,lo.deal_id) deal_id,
          units_sold,
          nob,
          ogp,
          orders,
          udv
from (
	sel coalesce(t.report_date,o.report_date) report_date,
          coalesce(t.deal_id,o.deal_id) deal_id,
          units_sold,
          nob,
          ogp,
          orders
      from (
          sel report_date,
              deal_id,
              sum(net_transactions_qty - zdo_net_transactions_qty) units_sold,
              sum(nob_loc * coalesce(er.approved_avg_exchange_rate,1)) nob,
              sum(net_transactions - zdo_net_transactions) orders
          from user_edwprod.agg_gbl_financials_deal f
          join user_groupondw.dim_day dd on dd.day_rw = f.report_date
          join (
              sel currency_from,
                  currency_to,
                  fx_neutral_exchange_rate,
                  approved_avg_exchange_rate,
                  period_key
              from user_groupondw.gbl_fact_exchange_rate
              where currency_to = 'USD'
              group by 1,2,3,4,5
          ) er on f.currency_code = er.currency_from and dd.month_key = er.period_key
          group by 1,2
      ) t
      full outer join (
          sel report_date,
              deal_id,
              sum(ogp_loc * coalesce(er.approved_avg_exchange_rate,1)) ogp
          from user_edwprod.agg_gbl_ogp_financials_deal f
          join user_groupondw.dim_day dd on dd.day_rw = f.report_date
          join (
              sel currency_from,
                  currency_to,
                  fx_neutral_exchange_rate,
                  approved_avg_exchange_rate,
                  period_key
              from user_groupondw.gbl_fact_exchange_rate
              where currency_to = 'USD'
              group by 1,2,3,4,5
          ) er on f.currency_code = er.currency_from and dd.month_key = er.period_key
          group by 1,2
      ) o on t.report_date = o.report_date and t.deal_id = o.deal_id
      group by 1,2,3,4,5,6
      ) yu
   full outer join (
   				select 
   				report_date
   				, deal_id
   				, sum(uniq_deal_view_visitors) udv
   				from user_edwprod.agg_gbl_traffic_deal
   				group by 1,2
   ) lo on yu.report_date = lo.report_date and yu.deal_id = lo.deal_id
  ) with data unique primary index (report_date, deal_id);grant select on sandbox.jrg_fin_v to public
  
  
  ------performance DASH
  
  create volatile table jrg_groups as (
select
	report_mth
	, deal_uuid
	, count(distinct case when n_recs_accepted>0 then recommendation_id end) accepted
from sandbox.jrg_weekly_ma_b
where coalesce(deal_uuid,'')<>''
group by 1,2
) with data unique primary index (report_mth, deal_uuid) on commit preserve rows;create volatile table jrg_test as (
select
	trunc(a.actioned_date, 'RM') report_mth
	, a.merchant_uuid
	, pd.deal_uuid
	, a.recommendation_type
	, pd.l2
	, pd.l3
	, lower(pd.division) division
	, case when lower(pd.metal_at_close) in ('silver','gold','platinum') then 's+' else 'b-' end metal
	, case when bb.asp <= 25 then '$0 - $25'
   	 		when bb.asp > 25 and bb.asp <= 50 then '$25 - $50'
   	 		when bb.asp >50 and bb.asp <= 100 then '$51 - $100'
   	 		when bb.asp > 100 and bb.asp <=200 then '$101 - $200'
   	 		when bb.asp > 200 then '$201 <'
   	 		end discounted_price_bucket
	, (trunc(actioned_date)) acceptance_date
	, row_number() over (partition by trunc(a.actioned_date, 'RM'), pd.deal_uuid  order by trunc(actioned_date) desc) option_rank
from sandbox.jrg_comp_rec a
join sandbox.pai_deals pd
	on pd.deal_uuid = a.deal_id
left join (
 			select
      			product_uuid as deal_uuid
      			, count(distinct inv_product_uuid) num_options
      			, avg(doe.contract_sell_price) asp
    		from user_edwprod.dim_offer_ext doe
    		join sandbox.pai_deals b
     			on b.deal_uuid = doe.product_uuid
    		group by 1
) bb
	on bb.deal_uuid = a.deal_id
where delete_event_type='ACTIONED'
qualify option_rank=1
)with data unique primary index (report_mth, deal_uuid) on commit preserve rows;create volatile table jrg_control as (
select
	trunc(ad.load_date, 'RM') report_mth
	, pd.deal_uuid
	, pd.merchant_uuid
	, pd.l2
	, pd.l3
	, lower(pd.division) division
	, case when lower(pd.metal_at_close) in ('silver','gold','platinum') then 's+' else 'b-' end metal
	, case when bb.asp <= 25 then '$0 - $25'
   	 		when bb.asp > 25 and bb.asp <= 50 then '$25 - $50'
   	 		when bb.asp >50 and bb.asp <= 100 then '$51 - $100'
   	 		when bb.asp > 100 and bb.asp <=200 then '$101 - $200'
   	 		when bb.asp > 200 then '$201 <'
   	 		end discounted_price_bucket
from user_groupondw.active_deals ad
join sandbox.pai_deals pd
	on pd.deal_uuid = ad.deal_uuid
join jrg_groups jr
	on jr.deal_uuid = ad.DEAL_UUID
	and trunc(ad.load_date, 'RM')= jr.report_mth
left join (
 			select
      			product_uuid as deal_uuid
      			, count(distinct inv_product_uuid) num_options
      			, avg(doe.contract_sell_price) asp
    		from user_edwprod.dim_offer_ext doe
    		join sandbox.pai_deals b
     			on b.deal_uuid = doe.product_uuid
    		group by 1
) bb
	on bb.deal_uuid = ad.deal_uuid
where load_date>= '2021-06-15'
and pd.l1='Local'
and pd.country_code = 'US'
and jr.accepted=0
) with data unique primary index (report_mth, deal_uuid) on commit preserve rows;create volatile table jrg_basis as (
select
	ts.report_mth
	, ts.l2
	, ts.l3
	, ts.division
	, ts.metal
	, ts.discounted_price_bucket
	, ts.deal_uuid
	, c.deal_uuid as control_deal_uuid
	, ts.acceptance_date
	, ts.recommendation_type
from jrg_test ts
join jrg_control c
	on 	c.l2 = ts.l2
	and c.l3 = ts.l3
	and c.division = ts.division
	and c.metal = ts.metal
	and c.discounted_price_bucket = ts.discounted_price_bucket
	and ts.report_mth = c.report_mth
)with data no primary index on commit preserve rows;create VOLATILE table jrg_base_test as (
select
	report_mth
	, l2
	, l3
	, division
	, metal
	, discounted_price_bucket
	, acceptance_date
	, recommendation_type
	, count(distinct deal_uuid) control_deals
	, sum(pre_units_30) pre_units_30
	, sum(pre_udv_30) pre_udv_30
	, sum(post_units_30) post_units_30
	, sum(post_udv_30) post_udv_30
	, sum(pre_nob_30) pre_nob_30
	, sum(post_nob_30) post_nob_30
	, sum(pre_ogp_30) pre_ogp_30
	, sum(post_ogp_30) post_ogp_30

	, sum(pre_units_90) pre_units_90
	, sum(pre_udv_90) pre_udv_90
	, sum(post_units_90) post_units_90
	, sum(post_udv_90) post_udv_90
	, sum(pre_nob_90) pre_nob_90
	, sum(post_nob_90) post_nob_90
	, sum(pre_ogp_90) pre_ogp_90
	, sum(post_ogp_90) post_ogp_90

	, sum(pre_units_7) pre_units_7
	, sum(pre_udv_7) pre_udv_7
	, sum(post_units_7) post_units_7
	, sum(post_udv_7) post_udv_7
	, sum(pre_nob_7) pre_nob_7
	, sum(post_nob_7) post_nob_7
	, sum(pre_ogp_7) pre_ogp_7
	, sum(post_ogp_7) post_ogp_7
from (
		select
			a.*
			, coalesce(sum(case when v.report_date between a.acceptance_date - interval '30' day and a.acceptance_date then v.units_sold end),0) pre_units_30
			, coalesce(sum(case when v.report_date between a.acceptance_date - interval '30' day and a.acceptance_date then v.udv end),0) pre_udv_30
			, coalesce(sum(case when v.report_date between a.acceptance_date  and a.acceptance_date + interval '30' day  then v.units_sold end),0) post_units_30
			, coalesce(sum(case when v.report_date between a.acceptance_date  and a.acceptance_date + interval '30' day  then v.udv end),0) post_udv_30
			, coalesce(sum(case when v.report_date between a.acceptance_date - interval '30' day and a.acceptance_date then v.nob end),0) pre_nob_30
			, coalesce(sum(case when v.report_date between a.acceptance_date  and a.acceptance_date + interval '30' day  then v.nob end),0) post_nob_30
			, coalesce(sum(case when v.report_date between a.acceptance_date - interval '30' day and a.acceptance_date then v.ogp end),0) pre_ogp_30
			, coalesce(sum(case when v.report_date between a.acceptance_date  and a.acceptance_date + interval '30' day  then v.ogp end),0) post_ogp_30

			, coalesce(sum(case when v.report_date between a.acceptance_date - interval '90' day and a.acceptance_date then v.units_sold end),0) pre_units_90
			, coalesce(sum(case when v.report_date between a.acceptance_date - interval '90' day and a.acceptance_date then v.udv end),0) pre_udv_90
			, coalesce(sum(case when v.report_date between a.acceptance_date  and a.acceptance_date + interval '90' day  then v.units_sold end),0) post_units_90
			, coalesce(sum(case when v.report_date between a.acceptance_date  and a.acceptance_date + interval '90' day  then v.udv end),0) post_udv_90
			, coalesce(sum(case when v.report_date between a.acceptance_date - interval '90' day and a.acceptance_date then v.nob end),0) pre_nob_90
			, coalesce(sum(case when v.report_date between a.acceptance_date  and a.acceptance_date + interval '90' day  then v.nob end),0) post_nob_90
			, coalesce(sum(case when v.report_date between a.acceptance_date - interval '90' day and a.acceptance_date then v.ogp end),0) pre_ogp_90
			, coalesce(sum(case when v.report_date between a.acceptance_date  and a.acceptance_date + interval '90' day  then v.ogp end),0) post_ogp_90

			, coalesce(sum(case when v.report_date between a.acceptance_date - interval '7' day and a.acceptance_date then v.units_sold end),0) pre_units_7
			, coalesce(sum(case when v.report_date between a.acceptance_date - interval '7' day and a.acceptance_date then v.udv end),0) pre_udv_7
			, coalesce(sum(case when v.report_date between a.acceptance_date  and a.acceptance_date + interval '7' day  then v.units_sold end),0) post_units_7
			, coalesce(sum(case when v.report_date between a.acceptance_date  and a.acceptance_date + interval '7' day  then v.udv end),0) post_udv_7
			, coalesce(sum(case when v.report_date between a.acceptance_date - interval '7' day and a.acceptance_date then v.nob end),0) pre_nob_7
			, coalesce(sum(case when v.report_date between a.acceptance_date  and a.acceptance_date + interval '7' day  then v.nob end),0) post_nob_7
			, coalesce(sum(case when v.report_date between a.acceptance_date - interval '7' day and a.acceptance_date then v.ogp end),0) pre_ogp_7
			, coalesce(sum(case when v.report_date between a.acceptance_date  and a.acceptance_date + interval '7' day  then v.ogp end),0) post_ogp_7
		from (
				select distinct
					report_mth
					, l2
					, l3
					, division
					, metal
					, discounted_price_bucket
					, deal_uuid
					, acceptance_date
					, recommendation_type
				from jrg_basis
			) a
		left join sandbox.jrg_fin_v v
			on v.deal_id = a.deal_uuid
		group by 1,2,3,4,5,6,7,8,9
	) b
group by 1,2,3,4,5,6,7,8
)with data no primary index on commit preserve rows;create volatile table jrg_control_base as (
select
	report_mth
	, l2
	, l3
	, division
	, metal
	, discounted_price_bucket
	, acceptance_date
	, recommendation_type
	, count(distinct control_deal_uuid) control_deals
	, sum(pre_units_30) pre_units_30
	, sum(pre_udv_30) pre_udv_30
	, sum(post_units_30) post_units_30
	, sum(post_udv_30) post_udv_30
	, sum(pre_nob_30) pre_nob_30
	, sum(post_nob_30) post_nob_30
	, sum(pre_ogp_30) pre_ogp_30
	, sum(post_ogp_30) post_ogp_30

	, sum(pre_units_90) pre_units_90
	, sum(pre_udv_90) pre_udv_90
	, sum(post_units_90) post_units_90
	, sum(post_udv_90) post_udv_90
	, sum(pre_nob_90) pre_nob_90
	, sum(post_nob_90) post_nob_90
	, sum(pre_ogp_90) pre_ogp_90
	, sum(post_ogp_90) post_ogp_90

	, sum(pre_units_7) pre_units_7
	, sum(pre_udv_7) pre_udv_7
	, sum(post_units_7) post_units_7
	, sum(post_udv_7) post_udv_7
	, sum(pre_nob_7) pre_nob_7
	, sum(post_nob_7) post_nob_7
	, sum(pre_ogp_7) pre_ogp_7
	, sum(post_ogp_7) post_ogp_7
from (
		select
			a.*
			, coalesce(sum(case when v.report_date between a.acceptance_date - interval '30' day and a.acceptance_date then v.units_sold end),0) pre_units_30
			, coalesce(sum(case when v.report_date between a.acceptance_date - interval '30' day and a.acceptance_date then v.udv end),0) pre_udv_30
			, coalesce(sum(case when v.report_date between a.acceptance_date  and a.acceptance_date + interval '30' day  then v.units_sold end),0) post_units_30
			, coalesce(sum(case when v.report_date between a.acceptance_date  and a.acceptance_date + interval '30' day  then v.udv end),0) post_udv_30
			, coalesce(sum(case when v.report_date between a.acceptance_date - interval '30' day and a.acceptance_date then v.nob end),0) pre_nob_30
			, coalesce(sum(case when v.report_date between a.acceptance_date  and a.acceptance_date + interval '30' day  then v.nob end),0) post_nob_30
			, coalesce(sum(case when v.report_date between a.acceptance_date - interval '30' day and a.acceptance_date then v.ogp end),0) pre_ogp_30
			, coalesce(sum(case when v.report_date between a.acceptance_date  and a.acceptance_date + interval '30' day  then v.ogp end),0) post_ogp_30

			, coalesce(sum(case when v.report_date between a.acceptance_date - interval '90' day and a.acceptance_date then v.units_sold end),0) pre_units_90
			, coalesce(sum(case when v.report_date between a.acceptance_date - interval '90' day and a.acceptance_date then v.udv end),0) pre_udv_90
			, coalesce(sum(case when v.report_date between a.acceptance_date  and a.acceptance_date + interval '90' day  then v.units_sold end),0) post_units_90
			, coalesce(sum(case when v.report_date between a.acceptance_date  and a.acceptance_date + interval '90' day  then v.udv end),0) post_udv_90
			, coalesce(sum(case when v.report_date between a.acceptance_date - interval '90' day and a.acceptance_date then v.nob end),0) pre_nob_90
			, coalesce(sum(case when v.report_date between a.acceptance_date  and a.acceptance_date + interval '90' day  then v.nob end),0) post_nob_90
			, coalesce(sum(case when v.report_date between a.acceptance_date - interval '90' day and a.acceptance_date then v.ogp end),0) pre_ogp_90
			, coalesce(sum(case when v.report_date between a.acceptance_date  and a.acceptance_date + interval '90' day  then v.ogp end),0) post_ogp_90

			, coalesce(sum(case when v.report_date between a.acceptance_date - interval '7' day and a.acceptance_date then v.units_sold end),0) pre_units_7
			, coalesce(sum(case when v.report_date between a.acceptance_date - interval '7' day and a.acceptance_date then v.udv end),0) pre_udv_7
			, coalesce(sum(case when v.report_date between a.acceptance_date  and a.acceptance_date + interval '7' day  then v.units_sold end),0) post_units_7
			, coalesce(sum(case when v.report_date between a.acceptance_date  and a.acceptance_date + interval '7' day  then v.udv end),0) post_udv_7
			, coalesce(sum(case when v.report_date between a.acceptance_date - interval '7' day and a.acceptance_date then v.nob end),0) pre_nob_7
			, coalesce(sum(case when v.report_date between a.acceptance_date  and a.acceptance_date + interval '7' day  then v.nob end),0) post_nob_7
			, coalesce(sum(case when v.report_date between a.acceptance_date - interval '7' day and a.acceptance_date then v.ogp end),0) pre_ogp_7
			, coalesce(sum(case when v.report_date between a.acceptance_date  and a.acceptance_date + interval '7' day  then v.ogp end),0) post_ogp_7
		from (
				select distinct
					report_mth
					, l2
					, l3
					, division
					, metal
					, discounted_price_bucket
					, control_deal_uuid
					, acceptance_date
					,recommendation_type
				from jrg_basis
			) a
		left join sandbox.jrg_fin_v v
			on v.deal_id = a.control_deal_uuid
		group by 1,2,3,4,5,6,7,8,9
	) b
group by 1,2,3,4,5,6,7,8
)with data no primary index on commit preserve rows;drop table sandbox.jrg_ma_performance;create table sandbox.jrg_ma_performance as (
select
	ts.report_mth
	, ts.l2
	, ts.l3
	, ts.division
	, ts.metal
	, ts.discounted_price_bucket
	, ts.acceptance_date
	, ts.recommendation_type
	, ts.pre_units_30
	, ts.pre_udv_30
	, ts.post_units_30
	, ts.post_udv_30
	, ts.pre_nob_30
	, ts.post_nob_30
	, ts.pre_ogp_30
	, ts.post_ogp_30

	, ts.pre_units_90
	, ts.pre_udv_90
	, ts.post_units_90
	, ts.post_udv_90
	, ts.pre_nob_90
	, ts.post_nob_90
	, ts.pre_ogp_90
	, ts.post_ogp_90

	, ts.pre_units_7
	, ts.pre_udv_7
	, ts.post_units_7
	, ts.post_udv_7
	, ts.pre_nob_7
	, ts.post_nob_7
	, ts.pre_ogp_7
	, ts.post_ogp_7

	, c.pre_units_30 control_pre_units_30
	, c.pre_udv_30 control_pre_udv_30
	, c.post_units_30 control_post_units_30
	, c.post_udv_30 control_post_udv_30
	, c.pre_nob_30 control_pre_nob_30
	, c.post_nob_30 control_post_nob_30
	, c.pre_ogp_30 control_pre_ogp_30
	, c.post_ogp_30 control_post_ogp_30

	, c.pre_units_90 control_pre_units_90
	, c.pre_udv_90 control_pre_udv_90
	, c.post_units_90 control_post_units_90
	, c.post_udv_90 control_post_udv_90
	, c.pre_nob_90 control_pre_nob_90
	, c.post_nob_90 control_post_nob_90
	, c.pre_ogp_90 control_pre_ogp_90
	, c.post_ogp_90 control_post_ogp_90

	, c.pre_units_7 control_pre_units_7
	, c.pre_udv_7 control_pre_udv_7
	, c.post_units_7 control_post_units_7
	, c.post_udv_7 control_post_udv_7
	, c.pre_nob_7 control_pre_nob_7
	, c.post_nob_7 control_post_nob_7
	, c.pre_ogp_7 control_pre_ogp_7
	, c.post_ogp_7 control_post_ogp_7
from jrg_base_test ts
join jrg_control_base c
	on 	c.l2 = ts.l2
	and c.l3 = ts.l3
	and c.division = ts.division
	and c.metal = ts.metal
	and c.discounted_price_bucket = ts.discounted_price_bucket
	and ts.report_mth = c.report_mth
	and ts.acceptance_date = c.acceptance_date
	and ts.recommendation_type = c.recommendation_type
) with data unique primary index(report_mth, l2, l3, division, metal, discounted_price_bucket, acceptance_date,recommendation_type);grant select on sandbox.jrg_ma_performance to public;grant select on sandbox.jrg_comp_rec to public
---------------------------------------------------------------------------------------------------
/********************************************************************************
this for the recommendation
*********************************************************************************/

select
	trunc(a.actioned_date, 'RM') accepted_month
	, a.recommendation_type
	, b.n_merchants total_merchants
	, b.n_deals total_deals
	, count(distinct a.merchant_uuid) n_merchants
	, count(distinct a.deal_id) n_deals
	, count(distinct a.recommendation_id) n_recs
from sandbox.jrg_comp_rec a
left join  (
			select
				trunc(actioned_date, 'RM') accepted_month
				, count(distinct merchant_uuid) n_merchants
				, count(distinct deal_id) n_deals
			from sandbox.jrg_comp_rec
			where delete_event_type='ACTIONED'
			group by 1
		) b
	on b.accepted_month = trunc(a.actioned_date, 'RM')
where delete_event_type='ACTIONED'
group by 1,2,3,4