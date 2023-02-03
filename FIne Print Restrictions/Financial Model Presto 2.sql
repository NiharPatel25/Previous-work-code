select * from grp_gdoop_bizops_db.nvp_model_fin_info3;

select
	new_client_restriction_seen, 
	purchase_case, 
	l3, 
	avg(min_discount) avg_min_discount, 
	avg(max_discount) avg_max_discount, 
	min(min_discount) min_discount_seen, 
	max(max_discount) max_discount_seen,
	count(distinct deal_uuid) count_deals,
	sum(count_udv) sum_udv2,
	sum(count_d_parent_order_uuid) count_of_d_parent_order,
	sum(sum_nob_loc_USD) sum_nob_usd,
	sum(sum_ogp_loc_usd) sum_ogp_loc_usd
	from
	(select 
		a.deal_uuid,
		a.country_cd,
		a.country_id,
		a.l1,
		a.l2,
		a.l3,
		a.avg_sell_price,
		a.min_discount, 
		a.max_discount, 
		a.new_client_restriction_seen,
		b.sum_udv,
		b.count_udv,
		b.count_bcook_udv,
		b.purchase_case,
		c.count_d_parent_order_uuid,
		c.sum_nob_loc_USD,
		c.sum_nor_loc_USD,
		c.sum_ogp_loc_usd
	from
			(select 
				deal_uuid, 
				country_cd, 
				country_id, 
				l1,
				l2,
				l3,
				avg_sell_price, 
				min_discount, 
				max_discount, 
				new_client_restriction_seen
				from grp_gdoop_bizops_db.nvp_model_new_client_fil
			) as a 
			left join
			(select 
				deal_uuid, 
				purchase_case, 
				sum_udv,
				count_udv,
				count_bcook_udv
				from 
				grp_gdoop_bizops_db.nvp_model_deal_info2) as b on a.deal_uuid = b.deal_uuid
			left join 
			(select 
				deal_uuid, 
				purchase_case, 
				count_d_parent_order_uuid,
				sum_nob_loc_USD,
				sum_nor_loc_USD,
				sum_ogp_loc_usd
			from
			grp_gdoop_bizops_db.nvp_model_fin_info3 where order_cancelled_ = 0
			) as c on a.deal_uuid = c.deal_uuid and b.purchase_case = c.purchase_case) fin where country_cd = 'US' and l2 = 'L2 - Health / Beauty / Wellness' group by new_client_restriction_seen, purchase_case, l3  
			order by new_client_restriction_seen desc, purchase_case;
			
		
select distinct order_cancelled_ from grp_gdoop_bizops_db.nvp_model_fin_info3;

select 
	new_client_restriction_seen,
	l3,
	count(deal_uuid) count_of_deals
from grp_gdoop_bizops_db.nvp_model_new_client_fil 
where country_cd = 'US' and l2 = 'L2 - Health / Beauty / Wellness'
group by l3, new_client_restriction_seen order by new_client_restriction_seen desc;


