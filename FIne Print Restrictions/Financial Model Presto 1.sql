select * from user_groupondw.user_bcookie_mapping;


select
	new_client_restriction_seen, 
	purchase_case, 
	l3, 
	order_cancelled_,
	avg(min_discount) avg_min_discount, 
	avg(max_discount) avg_max_discount, 
	min(min_discount) min_discount_seen, 
	max(max_discount) max_discount_seen,
	count(distinct deal_uuid) count_deals,
	sum(count_udv) sum_udv2,
	sum(count_d_parent_order_uuid) count_of_d_parent_order,
	sum(sum_nob_loc_USD) sum_nob_usd,
	sum(sum_ogp_loc_usd) sum_ogp_loc_usd,
	sum(sum_activation) sum_actm
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
		c.sum_ogp_loc_usd,
		c.sum_activation,
		c.order_cancelled_
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
				sum_ogp_loc_usd,
				sum_activation,
				order_cancelled_
			from
			grp_gdoop_bizops_db.nvp_model_fin_info3
			) as c on a.deal_uuid = c.deal_uuid and b.purchase_case = c.purchase_case) fin where country_cd = 'US' and l2 = 'L2 - Health / Beauty / Wellness' group by new_client_restriction_seen, purchase_case, l3 ,order_cancelled_ order by new_client_restriction_seen desc, purchase_case;
	
select
	deal_attrited, 
	new_client_restriction_seen, 
	purchase_case, 
	l3, 
	order_cancelled_,
	avg(min_discount) avg_min_discount, 
	avg(max_discount) avg_max_discount, 
	min(min_discount) min_discount_seen, 
	max(max_discount) max_discount_seen,
	count(distinct deal_uuid) count_deals,
	sum(count_udv) sum_udv2,
	sum(count_d_parent_order_uuid) count_of_d_parent_order,
	sum(sum_nob_loc_USD) sum_nob_usd,
	sum(sum_ogp_loc_usd) sum_ogp_loc_usd,
	sum(sum_activation) sum_actm
	from
	(select 
		a.deal_uuid,
		a.country_cd,
		a.country_id,
		a.l1,
		a.l2,
		a.l3,
		a.l4, 
		a.l5, 
		a.avg_sell_price,
		a.min_discount, 
		a.max_discount, 
		a.new_client_restriction_seen,
		a.deal_attrited, 
		b.sum_udv,
		b.count_udv,
		b.count_bcook_udv,
		b.purchase_case,
		c.count_d_parent_order_uuid,
		c.sum_nob_loc_USD,
		c.sum_nor_loc_USD,
		c.sum_ogp_loc_usd,
		c.sum_activation,
		c.order_cancelled_
	from
			(select 
				deal_uuid, 
				country_cd, 
				country_id, 
				l1,
				l2,
				l3,
				l4, 
				l5, 
				avg_sell_price, 
				min_discount, 
				max_discount, 
				deal_attrited, 
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
				grp_gdoop_bizops_db.nvp_model_deal_info) as b on a.deal_uuid = b.deal_uuid
			left join 
			(select 
				deal_uuid, 
				purchase_case, 
				count_d_parent_order_uuid,
				sum_nob_loc_USD,
				sum_nor_loc_USD,
				sum_ogp_loc_usd,
				sum_activation,
				order_cancelled_
			from
			grp_gdoop_bizops_db.nvp_model_fin_info 
			) as c on a.deal_uuid = c.deal_uuid and b.purchase_case = c.purchase_case) fin where country_cd = 'US' and l2 = 'L2 - Health / Beauty / Wellness' group by new_client_restriction_seen, purchase_case, l3 ,order_cancelled_, deal_attrited order by deal_attrited asc, new_client_restriction_seen desc, purchase_case;

-----Cancelled Orders	
select
	deal_attrited,
	new_client_restriction_seen, 
	purchase_case, 
	l3, 
	order_cancelled_,
	avg(min_discount) avg_min_discount, 
	avg(max_discount) avg_max_discount, 
	min(min_discount) min_discount_seen, 
	max(max_discount) max_discount_seen,
	count(distinct deal_uuid) count_deals,
	sum(count_d_parent_order_uuid) count_of_d_parent_order,
	sum(sum_nob_loc_USD) sum_nob_usd,
	sum(sum_ogp_loc_usd) sum_ogp_loc_usd,
	sum(sum_activation) sum_actm
	from
	(select 
		a.deal_uuid,
		a.country_cd,
		a.country_id,
		a.l1,
		a.l2,
		a.l3,
		a.l4,
		a.l5,
		a.deal_attrited,
		a.avg_sell_price,
		a.min_discount, 
		a.max_discount, 
		a.new_client_restriction_seen,
		b.purchase_case,
		c.count_d_parent_order_uuid,
		c.sum_nob_loc_USD,
		c.sum_nor_loc_USD,
		c.sum_ogp_loc_usd,
		c.sum_activation,
		c.order_cancelled_
	from
			(select 
				deal_uuid, 
				country_cd, 
				country_id, 
				l1,
				l2,
				l3,
				l4,
				l5,
				avg_sell_price, 
				min_discount, 
				max_discount, 
				deal_attrited,
				new_client_restriction_seen
				from grp_gdoop_bizops_db.nvp_model_new_client_fil
			) as a 
			left join
			(select 
				deal_uuid, 
				purchase_case
				from 
				grp_gdoop_bizops_db.nvp_model_deal_info) as b on a.deal_uuid = b.deal_uuid
			left join 
			(select 
				deal_uuid, 
				purchase_case, 
				count_d_parent_order_uuid,
				sum_nob_loc_USD,
				sum_nor_loc_USD,
				sum_ogp_loc_usd,
				sum_activation,
				order_cancelled_
			from
			grp_gdoop_bizops_db.nvp_model_fin_info 
			) as c on a.deal_uuid = c.deal_uuid and b.purchase_case = c.purchase_case) fin where country_cd = 'US' and l2 = 'L2 - Health / Beauty / Wellness' group by new_client_restriction_seen, purchase_case, l3 ,order_cancelled_, deal_attrited having order_cancelled_ = 0 order by deal_attrited, order_cancelled_, new_client_restriction_seen desc, purchase_case;

		
	
--- TRANSACTION category breakdown

select
	deal_attrited,
	new_client_restriction_seen, 
	purchase_case,
	transaction_purchase_cat,
	count(deal_uuid) count_deals,
	sum(sum_nob_loc_USD) sum_nob_usd,
	sum(sum_ogp_loc_usd) sum_ogp_loc_usd,
	sum(sum_nor_loc_USD) sum_nor_usd
	from
	(select 
		a.deal_uuid,
		a.country_cd,
		a.country_id,
		a.l1,
		a.l2,
		a.l3,
		a.l4,
		a.l5,
		a.deal_attrited,
		a.avg_sell_price,
		a.min_discount, 
		a.max_discount, 
		a.new_client_restriction_seen,
		c.purchase_case,
		c.transaction_purchase_cat,
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
				l4,
				l5,
				avg_sell_price, 
				min_discount, 
				max_discount,
				deal_attrited,
				new_client_restriction_seen
				from grp_gdoop_bizops_db.nvp_model_new_client_fil
			) as a 
			left join 
			(select 
				deal_uuid, 
				purchase_case, 
				transaction_purchase_cat,
				sum_nob_loc_USD,
				sum_nor_loc_USD,
				sum_ogp_loc_usd
			from
			grp_gdoop_bizops_db.nvp_model_fin_info4
			) as c on a.deal_uuid = c.deal_uuid) fin where country_cd = 'US' and l2 = 'L2 - Health / Beauty / Wellness' group by new_client_restriction_seen, transaction_purchase_cat, purchase_case, deal_attrited order by deal_attrited, new_client_restriction_seen;

		
-----CHECKING DEAL TYPES

select 
	l3,
	max(row_),
	sum(sum_ogp_loc_usd)
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
		b.count_udv,
		b.count_bcook_udv,
		b.purchase_case,
		c.count_d_parent_order_uuid,
		c.sum_ogp_loc_usd,
		c.order_cancelled_,
		row_number() over(partition by a.l3 order by c.sum_ogp_loc_usd desc) row_
	from
			(select 
				deal_uuid, 
				country_cd, 
				country_id, 
				l1,
				l2,
				l3,
				l4,
				l5,
				avg_sell_price, 
				min_discount, 
				max_discount, 
				deal_attrited,
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
				grp_gdoop_bizops_db.nvp_model_deal_info) as b on a.deal_uuid = b.deal_uuid
			left join 
			(select 
				deal_uuid, 
				purchase_case, 
				count_d_parent_order_uuid,
				sum_nob_loc_USD,
				sum_nor_loc_USD,
				sum_ogp_loc_usd,
				sum_activation,
				order_cancelled_
			from
			grp_gdoop_bizops_db.nvp_model_fin_info 
			) as c on a.deal_uuid = c.deal_uuid and b.purchase_case = c.purchase_case where a.country_cd = 'US' and a.l3 = 'L3 - Massage' and b.purchase_case = 'old_client') fin_ group by l3;

		
--L3 - High End
--L3 - Fitness
--L3 - Massage
--

select * from grp_gdoop_bizops_db.nvp_model_new_client_fil where deal_uuid = '0d1f8e55-7676-4d16-b192-087e916f7d0c';
	
------STAT SIG TEST VALU
--- 
select * from grp_gdoop_bizops_db.nvp_model_fin_info2;
select * from grp_gdoop_bizops_db.nvp_model_new_client_fil;
select count(deal_uuid) from grp_gdoop_bizops_db.nvp_model_new_client_fil where country_cd = 'US' and l2 = 'L2 - Health / Beauty / Wellness';

select 
			a.deal_uuid,
			country_cd, 
			l3,
			metal,
			division,
			sum(count_udv) udv,
			sum(c.sum_ogp_loc_usd) sum_ogp,
			sum(c.sum_nob_loc_USD) sum_nob,
			sum(c.count_d_parent_order_uuid) sum_ord
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
				account_id, 
				metal, 
				division,
				new_client_restriction_seen
				from grp_gdoop_bizops_db.nvp_model_new_client_fil where new_client_restriction_seen = 1) as a 
		left join 
		(select 
				deal_uuid, 
				purchase_case, 
				sum_udv,
				count_udv,
				count_bcook_udv
				from grp_gdoop_bizops_db.nvp_model_deal_info) as b on a.deal_uuid = b.deal_uuid 
		left join
		(select 
				deal_uuid, 
				purchase_case, 
				count_d_parent_order_uuid,
				sum_nob_loc_USD,
				sum_nor_loc_USD,
				sum_ogp_loc_usd,
				sum_activation,
				order_cancelled_
			from
			grp_gdoop_bizops_db.nvp_model_fin_info2
			) as c on a.deal_uuid = c.deal_uuid and b.purchase_case = c.purchase_case
		where country_cd = 'US' and l2 = 'L2 - Health / Beauty / Wellness' group by a.deal_uuid, country_cd, l3, metal,division;

	
select 
		b.purchase_case, 
		sum(b.count_udv) udv,
		sum(c.sum_ogp_loc_usd) sum_ogp,
		sum(c.sum_nob_loc_USD) sum_nob,
		sum(c.count_d_parent_order_uuid) sum_ord,
		sum(d.transaction_qty) transaction_qty
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
				account_id, 
				metal, 
				division,
				new_client_restriction_seen
				from grp_gdoop_bizops_db.nvp_model_new_client_fil where new_client_restriction_seen = 1) as a 
		left join 
		(select 
				deal_uuid, 
				purchase_case, 
				sum_udv,
				count_udv,
				count_bcook_udv
				from grp_gdoop_bizops_db.nvp_model_deal_info) as b on a.deal_uuid = b.deal_uuid 
		left join
		(select 
				deal_uuid, 
				purchase_case, 
				count_d_parent_order_uuid,
				sum_nob_loc_USD,
				sum_nor_loc_USD,
				sum_ogp_loc_usd,
				sum_activation,
				order_cancelled_
			from
			grp_gdoop_bizops_db.nvp_model_fin_info2
			) as c on a.deal_uuid = c.deal_uuid and b.purchase_case = c.purchase_case
		left join
		(select 
				deal_uuid, 
				purchase_case, 
				transaction_qty
			from
			grp_gdoop_bizops_db.nvp_model_fin_info4
			) as d on a.deal_uuid = d.deal_uuid and b.purchase_case = d.purchase_case
		where country_cd = 'US' and l2 = 'L2 - Health / Beauty / Wellness' group by b.purchase_case;
	
	
	
	
select 		
			a.deal_uuid, 
			a.country_cd, 
			l3,
			metal,
			a.division, 
			sum(count_udv) udv,
			sum(c.sum_ogp_loc_usd) sum_ogp,
			sum(c.sum_nob_loc_USD) sum_nob,
			sum(c.count_d_parent_order_uuid) sum_ord
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
				account_id, 
				metal, 
				division,
				new_client_restriction_seen
				from grp_gdoop_bizops_db.nvp_model_new_client_fil where new_client_restriction_seen = 1 and division is not null) as a 
		inner join 
		(select division from grp_gdoop_bizops_db.nvp_stat__new_repeat) as div on div.division = a.division
		left join 
		(select 
				deal_uuid, 
				purchase_case, 
				sum_udv,
				count_udv,
				count_bcook_udv
				from grp_gdoop_bizops_db.nvp_model_deal_info) as b on a.deal_uuid = b.deal_uuid 
		left join
		(select 
				deal_uuid, 
				purchase_case, 
				count_d_parent_order_uuid,
				sum_nob_loc_USD,
				sum_nor_loc_USD,
				sum_ogp_loc_usd,
				sum_activation,
				order_cancelled_
			from
			grp_gdoop_bizops_db.nvp_model_fin_info2
			) as c on a.deal_uuid = c.deal_uuid and b.purchase_case = c.purchase_case
		where country_cd = 'US' and l2 = 'L2 - Health / Beauty / Wellness' group by a.deal_uuid,l3,a.country_cd, metal, a.division order by sum_nob desc;
	
	
--- transaction qty all 

select 
			a.deal_uuid,
			l3,
			country_cd, 
			metal,
			division,
			sum(c.transaction_qty) transaction_qty
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
				account_id, 
				metal, 
				division,
				new_client_restriction_seen
				from grp_gdoop_bizops_db.nvp_model_new_client_fil where new_client_restriction_seen = 1) as a 
		left join 
		(select 
				deal_uuid, 
				purchase_case
				from grp_gdoop_bizops_db.nvp_model_deal_info) as b on a.deal_uuid = b.deal_uuid 
		left join
		(select 
				deal_uuid, 
				purchase_case, 
				transaction_qty
			from
			grp_gdoop_bizops_db.nvp_model_fin_info4
			) as c on a.deal_uuid = c.deal_uuid and b.purchase_case = c.purchase_case
		where a.country_cd = 'US' and a.l2 = 'L2 - Health / Beauty / Wellness' group by a.deal_uuid, country_cd,l3, metal, division;

/*a.deal_uuid, country_cd, country_id, l1,l2,l3,l4,l5,avg_sell_price, min_discount, max_discount, new_client_restriction_seen*/	

-----diff in diff

select 
	division, 
	count(distinct deal_uuid) total_deals,
	sum(transaction_qty) transaction_qty,
	sum(transaction_qty*transaction_qty) tran_sq
from 
(select 	
			a.deal_uuid,
			a.division,
			sum(c.transaction_qty) transaction_qty
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
				account_id, 
				metal, 
				division,
				new_client_restriction_seen
				from grp_gdoop_bizops_db.nvp_model_new_client_fil where new_client_restriction_seen = 1 and division is not null) as a 
		inner join 
		(select division from grp_gdoop_bizops_db.nvp_stat__new_repeat) as div on div.division = a.division
		left join 
		(select 
				deal_uuid, 
				purchase_case
				from grp_gdoop_bizops_db.nvp_model_deal_info) as b on a.deal_uuid = b.deal_uuid 
		left join
		(select 
				deal_uuid, 
				purchase_case, 
				transaction_qty
			from
			grp_gdoop_bizops_db.nvp_model_fin_info4
			) as c on a.deal_uuid = c.deal_uuid and b.purchase_case = c.purchase_case
		where a.country_cd = 'US' and a.l2 = 'L2 - Health / Beauty / Wellness' group by a.division, a.deal_uuid) group by division;


select 
	division,
	count(distinct deal_uuid),
	sum(udv) sum_udv,
	sum(sum_ogp) sum_ogp, 
	sum(sum_nob) sum_nob, 
	sum(sum_ord) sum_ord, 
	sum(udv*udv) sum_udv, 
	sum(sum_ogp*sum_ogp) sum_ogp_sq, 
	sum(sum_nob*sum_nob) sum_nob_sq, 
	sum(sum_ord*sum_ord) sum_ord_sq
from 
(select 		
			a.deal_uuid, 
			a.country_cd, 
			l3,
			metal,
			a.division, 
			sum(count_udv) udv,
			sum(c.sum_ogp_loc_usd) sum_ogp,
			sum(c.sum_nob_loc_USD) sum_nob,
			sum(c.count_d_parent_order_uuid) sum_ord
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
				account_id, 
				metal, 
				division,
				new_client_restriction_seen
				from grp_gdoop_bizops_db.nvp_model_new_client_fil where new_client_restriction_seen = 1 and division is not null) as a 
		inner join 
		(select division from grp_gdoop_bizops_db.nvp_stat__new_repeat) as div on div.division = a.division
		left join 
		(select 
				deal_uuid, 
				purchase_case, 
				sum_udv,
				count_udv,
				count_bcook_udv
				from grp_gdoop_bizops_db.nvp_model_deal_info) as b on a.deal_uuid = b.deal_uuid 
		left join
		(select 
				deal_uuid, 
				purchase_case, 
				count_d_parent_order_uuid,
				sum_nob_loc_USD,
				sum_nor_loc_USD,
				sum_ogp_loc_usd,
				sum_activation,
				order_cancelled_
			from
			grp_gdoop_bizops_db.nvp_model_fin_info2
			) as c on a.deal_uuid = c.deal_uuid and b.purchase_case = c.purchase_case
		where country_cd = 'US' and l2 = 'L2 - Health / Beauty / Wellness' group by a.deal_uuid,l3,a.country_cd, metal, a.division) group by division order by sum_nob desc;	
	
-----TRASH

	
select 
				count(deal_uuid)
				from grp_gdoop_bizops_db.nvp_model_new_client_fil where new_client_restriction_seen = 1 and division is not null and country_cd = 'US' and l2 = 'L2 - Health / Beauty / Wellness';	
	
select
	new_client_restriction_seen, 
	purchase_case, 
	l3,
	avg(min_discount) avg_min_discount, 
	avg(max_discount) avg_max_discount, 
	min(min_discount) min_discount_seen, 
	max(max_discount) max_discount_seen,
	count(distinct deal_uuid) count_deals,
	sum(count_d_parent_order_uuid) count_of_d_parent_order,
	sum(sum_nob_loc_USD) sum_nob_usd,
	sum(sum_ogp_loc_usd) sum_ogp_loc_usd,
	sum(sum_activation) sum_act
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
		c.purchase_case,
		c.count_d_parent_order_uuid,
		c.sum_nob_loc_USD,
		c.sum_nor_loc_USD,
		c.sum_ogp_nob_loc_usd,
		c.sum_ogp_nor_loc_usd,
		c.sum_ogp_loc_usd,
		c.sum_ogp_total_estimate_loc_usd,
		c.sum_activation
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
				count_d_parent_order_uuid,
				sum_nob_loc_USD,
				sum_nor_loc_USD,
				sum_ogp_nob_loc_usd,
				sum_ogp_nor_loc_usd,
				sum_ogp_loc_usd,
				sum_ogp_total_estimate_loc_usd,
				sum_activation
			from
			grp_gdoop_bizops_db.nvp_model_fin_info2
			) as c on a.deal_uuid = c.deal_uuid) fin where country_cd = 'US' and l2 = 'L2 - Health / Beauty / Wellness' group by new_client_restriction_seen, purchase_case, l3 order by new_client_restriction_seen, purchase_case;
		
		
		
		
		
select count(deal_uuid),sum(count_d_parent_order_uuid) from grp_gdoop_bizops_db.nvp_model_fin_info2 limit 5;

select 
l3,
	new_client_restriction_seen, 
	count(distinct deal_uuid),
	count(deal_uuid)
from grp_gdoop_bizops_db.nvp_model_new_client_fil 
where l2 = 'L2 - Health / Beauty / Wellness' and country_cd = 'US' group by new_client_restriction_seen,l3 order by new_client_restriction_seen;

select 
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
				from grp_gdoop_bizops_db.nvp_model_new_client_fil where min_discount < 0 and country_cd = 'US';

select 
a.new_client_restriction_seen,
count(deal_uuid), 
count(distinct merchant_uuid)
from 
(select * from grp_gdoop_bizops_db.nvp_model_new_client_fil) as a 
left join
(select product_uuid, merchant_uuid
from grp_gdoop_bizops_db.nvp_model_deal_merch_info
group by product_uuid, merchant_uuid) as b  on a.deal_uuid = b.product_uuid where a.l2 = 'L2 - Health / Beauty / Wellness' and a.country_cd = 'US' group by a.new_client_restriction_seen;


select count(product_uuid), count(distinct product_uuid), count(merchant_uuid), count(distinct merchant_uuid) from grp_gdoop_bizops_db.nvp_model_deal_merch_info;

select * from grp_gdoop_bizops_db.nvp_model_cancel_info;
select 
			purchase_case, 
			sum(sum_activation), 
			count()
			from
			grp_gdoop_bizops_db.nvp_model_fin_info2 group by purchase_case;
		
		/*		case when a.min_groupon_value = 0 then 0 else (a.min_groupon_value - a.min_contract_sell)/a.min_groupon_value end as min_discount, 
		case when a.max_groupon_value = 0 then 0 else (a.max_groupon_value - a.max_contract_sell)/a.max_groupon_value end as max_discount,*/


select 
     case when repetition = 1 then 1 when repetition = 2 then 2 when repetition = 3 then 3 else 4 end repetition,
     sum(nob_usd) total_nob, 
     sum(nor_usd) total_nor,
     sum(ogp_usd) total_ogp
from grp_gdoop_bizops_db.nvp_model_var_price2
group by 
     case when repetition = 1 then 1 when repetition = 2 then 2 when repetition = 3 then 3 else 4 end
order by repetition;




select * from grp_gdoop_bizops_db.nvp_model_var_price3 where user_uuid = '00013d3e-f50a-11e1-a62f-00259060b244';



select * from grp_gdoop_bizops_db.nvp_model_var_price2;
