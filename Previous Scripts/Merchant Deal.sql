create multiset volatile table division as
(select
				distinct division_uuid,
				division_name,
				country_name
			from
				user_groupondw.dim_lat_lng_loc_map)  with data on commit preserve rows;



create MULTISET table sandbox.np_deal_merch_ as (
select d.*, c.* from
(select  b.deal_uuid deal_uuid, a.product_uuid product_uuid, case when (a.groupon_value-a.contract_sell_price)/a.groupon_value < 0 then 0 else (a.groupon_value-a.contract_sell_price)/a.groupon_value end  discount_ , a.merchant_uuid merchant_uuidone, b.country_code country_code, x.lone, x.ltwo, x.lthree, y.division_id, z.division_name, z.country_name, cl.deal_id from
(select 
	deal_uuid deal_uuid, 
	case when country_code = 'GB' then 'UK' else country_code end country_code
	from user_groupondw.active_deals where load_date = CURRENT_DATE-1 and available_qty > 0 and sold_out = 'false' group by deal_uuid, country_code) as b
join 
(select 
	merchant_uuid, 
	product_uuid product_uuid, 
	groupon_value, 
	contract_sell_price
	from user_edwprod.dim_offer_ext  where inv_product_uuid <> '-1' and contract_sell_price > 0 and groupon_value <> 0
) as a on a.product_uuid = b.deal_uuid
join
(select 
	deal_id as deal_uuid, 
	country_code as country_code,  
	country_id as country_id,
	grt_l1_cat_name as lone, 
	grt_l2_cat_name as ltwo, 
	grt_l3_cat_name as lthree
from user_edwprod.dim_gbl_deal_lob where grt_l1_cat_name = 'L1 - Local') x on b.deal_uuid = x.deal_uuid and b.country_code = x.country_code
left join 
( select division_id, deal_uuid from user_edwprod.deal_division_map) y on a.product_uuid = y.deal_uuid
left join 
(select division_uuid, division_name, country_name from division) z on z.division_uuid = y.division_id
left join 
(select distinct deal_id from user_gp.clo_offers) cl on cl.deal_id = b.deal_uuid
) as c
join 
(select 
	merchant_uuid merchant_uuid, 
	name merchant_name, 
	feature_country country_id
	from user_edwprod.dim_merchant
) as d on c.merchant_uuidone = d.merchant_uuid) with data;

create table sandbox.np_deal_merch as (select * from deal_merch_) with data;



select * from sandbox.np_deal_merch_;
drop table sandbox.deal_merch_;


create table sandbox.np_deal_merch as 
(select merchant_uuid, merchant_name, division_name, ltwo l2, count(distinct product_uuid) deals_live, min(discount_)*100 min_discount, max(discount_)*100 max_discount, country_code, country_name from deal_merch_  where country_code <> 'JP' and deal_id is null group by merchant_uuid, merchant_name, division_name, ltwo, country_code, country_name);


REGEXP_REPLACE
