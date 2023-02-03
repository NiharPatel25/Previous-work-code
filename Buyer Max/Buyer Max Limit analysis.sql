
-----
drop table grp_gdoop_bizops_db.nvp_buyermax_info; 
create table grp_gdoop_bizops_db.nvp_buyermax_info stored as orc as
select 
	a.user_uuid,
	a.deal_uuid,
	a.inv_product_uuid,
	a.merchant_uuid,
	c.buyer_max, 
	cast(min(order_date) as date) first_purchase_of_deal, 
	sum(transaction_qty) units_purchased
from 
(select * 
	from user_edwprod.fact_gbl_transactions 
	where country_id = 235 and action = 'authorize' and cast(order_date as date) >= cast('2019-04-01' as date) and cast(order_date as date) <= cast('2019-09-30' as date)
) as a
join
(select 
	deal_id as deal_uuid
	from
	user_edwprod.dim_gbl_deal_lob where grt_l1_cat_name = 'L1 - Local'
	) as b on a.deal_uuid = b.deal_uuid
join 
(select 
	deal_uuid,
	max(buyer_max) buyer_max
	from 
	grp_gdoop_sup_analytics_db.eh_deal_structure
	where voucher_flag = "voucher"
	group by deal_uuid
	) as c on a.deal_uuid = c.deal_uuid
group by a.user_uuid, a.deal_uuid, a.inv_product_uuid, a.merchant_uuid, c.buyer_max;




create table grp_gdoop_bizops_db.nvp_buyermax_allinfo stored as orc as
select 
	user_uuid, 
	deal_uuid, 
	max(buyer_max_hit) buyer_max_hit
from 
	(select
	user_uuid,
	deal_uuid,
	inv_product_uuid, 
	merchant_uuid, 
	buyer_max, 
	first_purchase_of_deal,
	units_purchased, 
	case when buyer_max is not null and units_purchased >= buyer_max then 1 else 0 end buyer_max_hit
		from 
	grp_gdoop_bizops_db.nvp_buyermax_info) as fin_ group by user_uuid, deal_uuid;

select * from grp_gdoop_bizops_db.nvp_buyermax_info;

-----

create table grp_gdoop_bizops_db.nvp_buyermax_udv stored as orc as
select 
	b.user_uuid, 
	c.buyer_max_hit, 
	count(distinct a.udvisitor_date_deal_id_udv) count_udv, 
	count(distinct a.bcook_date_deal_id_udv) count_bcook_udv, 
	sum(unique_deal_views) sum_udv, 
	count(bcook_date_udvv) count_udvv
from 
	(select 
		edw_modified_ts, 
		cookie_b,
		deal_uuid,
		cast(event_date as date) event_date,  
		unique_deal_views,
		CONCAT(cast(unique_dv_visitors as string),'+', cast(event_date as string),'+', cast(deal_uuid as string)) udvisitor_date_deal_id_udv,
		CONCAT(cast(cookie_b as string), '+', cast(event_date as string),'+', cast(deal_uuid as string)) bcook_date_deal_id_udv, 
		CONCAT(cast(cookie_b as string), '+',cast(event_date as string)) as bcook_date_udvv
	from 
		user_groupondw.gbl_traffic_superfunnel_deal where cast(event_date as date) >= cast('2019-10-01' as date) and cast(event_date as date) <= cast('2020-03-31'as date) and deal_uuid is not null) a 
	join 
	(select 
		bcookie,
		user_uuid,
		cast(event_date as date) event_date2
	from 
		user_groupondw.user_bcookie_mapping where cast(event_date as date) >= cast('2019-10-01' as date) and cast(event_date as date) <= cast('2020-03-31'as date)) as b on a.cookie_b = b.bcookie and a.event_date = b.event_date2
	join
	(select
		user_uuid, 
		deal_uuid, 
		buyer_max_hit
	from
		grp_gdoop_bizops_db.nvp_buyermax_allinfo) as c on b.user_uuid = c.user_uuid and a.deal_uuid = c.deal_uuid
	group by b.user_uuid, c.buyer_max_hit;

create table grp_gdoop_bizops_db.nvp_buyermax_udv_deal stored as orc as
select 
	b.user_uuid, 
	c.buyer_max_hit,
	d.ltwo, 
	d.lthree,
	count(distinct a.udvisitor_date_deal_id_udv) count_udv, 
	count(distinct a.bcook_date_deal_id_udv) count_bcook_udv, 
	sum(unique_deal_views) sum_udv, 
	count(bcook_date_udvv) count_udvv
from 
	(select 
		edw_modified_ts, 
		cookie_b,
		deal_uuid,
		cast(event_date as date) event_date,  
		unique_deal_views,
		CONCAT(cast(unique_dv_visitors as string),'+', cast(event_date as string),'+', cast(deal_uuid as string)) udvisitor_date_deal_id_udv,
		CONCAT(cast(cookie_b as string), '+', cast(event_date as string),'+', cast(deal_uuid as string)) bcook_date_deal_id_udv, 
		CONCAT(cast(cookie_b as string), '+',cast(event_date as string)) as bcook_date_udvv
	from 
	user_groupondw.gbl_traffic_superfunnel_deal where cast(event_date as date) >= cast('2019-10-01' as date) and cast(event_date as date) <= cast('2020-03-31'as date) and deal_uuid is not null) a 
join
	(select
		bcookie,
		user_uuid,
		cast(event_date as date) event_date2
	from 
	user_groupondw.user_bcookie_mapping where cast(event_date as date) >= cast('2019-10-01' as date) and cast(event_date as date) <= cast('2020-03-31'as date)) as b on a.cookie_b = b.bcookie and a.event_date = b.event_date2
join
	(select
		user_uuid, 
		deal_uuid, 
		buyer_max_hit
	from
		grp_gdoop_bizops_db.nvp_buyermax_allinfo) as c on b.user_uuid = c.user_uuid and a.deal_uuid = c.deal_uuid
join
	(select 
		deal_id as deal_uuid,
		grt_l2_cat_name ltwo, 
		grt_l3_cat_name lthree
	from
		user_edwprod.dim_gbl_deal_lob where grt_l1_cat_name = 'L1 - Local'
	) as d on a.deal_uuid = d.deal_uuid
group by b.user_uuid, c.buyer_max_hit, d.ltwo, d.lthree;


-------TRASH

	
/*create table grp_gdoop_bizops_db.nvp_buyermax_info2 stored as orc as
select 
	a.user_uuid,
	a.deal_uuid,
	a.inv_product_uuid,
	a.merchant_uuid,
	cast(min(order_date) as date) first_purchase_of_deal, 
	sum(transaction_qty) units_purchased
from 
(select * 
	from user_edwprod.fact_gbl_transactions 
	where country_id = 235 and action = 'authorize' and cast(order_date as date) >= cast('2019-04-01' as date) and cast(order_date as date) <= cast('2019-09-30' as date)
) as a
join
(select 
	deal_id as deal_uuid
	from
	user_edwprod.dim_gbl_deal_lob where grt_l1_cat_name = 'L1 - Local'
	) as b on a.deal_uuid = b.deal_uuid
join 
(select 
	product_uuid,
	inv_product_uuid, 
	merchant_uuid
	from 
	user_edwprod.dim_offer_ext 
	where inventory_service_name <> 'tpis' 
	group by product_uuid, merchant_uuid, inv_product_uuid
	) as c on a.inv_product_uuid = c.inv_product_uuid
group by a.user_uuid, a.deal_uuid, a.inv_product_uuid, a.merchant_uuid;*/


select report_date, last_day(report_date) from grp_gdoop_bizops_db.nvp_deal_udv where report_date = '2020-02-02' limit 5;

select * from user_edwprod.agg_gbl_traffic_deal limit 5;




