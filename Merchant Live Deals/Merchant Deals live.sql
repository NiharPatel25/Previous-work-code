select * from user_groupondw.active_deals;
select * from user_edwprod.dim_merchant limit 5;
select * from user_edwprod.dim_offer_ext limit 5;

/*Everything after march 2020*/

CREATE TEMPORARY TABLE deals_ext as 
select 
	merchant_uuid, 
	product_uuid product_uuid, 
	((groupon_value - contract_sell_price)/groupon_value)*100 discount_percentage
	from user_edwprod.dim_offer_ext 
	where inv_product_uuid <> -1;




CREATE TEMPORARY TABLE act_deals as
select 
	deal_uuid deal_uuid, 
	country_code country_code
	from user_groupondw.active_deals where year(load_date) = '2020' and month(load_date) = '03' and available_qty > 0 and sold_out = 'false' group by deal_uuid, country_code;


create TEMPORARY table merchant_info as 
select 
	merchant_uuid merchant_uuid, 
	name merchant_name, 
	feature_country country_id
	from user_edwprod.dim_merchant;




drop table np_merch;



create table np_merch stored as orc as 
select * from (
select d.*, c.* from
(select a.product_uuid product_uuid, case when a.groupon_value = 0 then 0 else (a.groupon_value-a.contract_sell_price)/a.groupon_value end discount_, a.merchant_uuid merchant_uuidone, b.deal_uuid deal_uuid, b.country_code country_code, x.lone, x.ltwo, x.lthree, y.division_id from
(select 
	merchant_uuid, 
	product_uuid product_uuid, 
	groupon_value, 
	contract_sell_price
	from user_edwprod.dim_offer_ext 
) as a
join
(select 
	deal_uuid deal_uuid, 
	country_code country_code
	from user_groupondw.active_deals where year(load_date) = '2020' and month(load_date) = '03' and available_qty > 0 and sold_out = 'false' group by deal_uuid, country_code) as b on a.product_uuid = b.deal_uuid
join
(select 
	deal_id as deal_uuid, 
	country_code as country_code,  
	country_id as country_id,
	grt_l1_cat_name as lone, 
	grt_l2_cat_name as ltwo, 
	grt_l3_cat_name as lthree
from user_edwprod.dim_gbl_deal_lob where grt_l1_cat_name = 'L1 - Local') x on a.product_uuid = x.deal_uuid
left join 
( select division_id, deal_uuid from user_edwprod.deal_division_map) y on a.product_uuid = y.deal_uuid
) as c
join 
(select 
	merchant_uuid merchant_uuid, 
	name merchant_name, 
	feature_country country_id
	from user_edwprod.dim_merchant
) as d on c.merchant_uuidone = d.merchant_uuid) as uni;





select * from np_merch limit 5;



select merchant_uuid, count(deal_uuid), min(discount_), max(discount_) from np_merch group by merchant_uuid;



select * from user_edwprod.deal_division_map limit 5;
select * from user_groupondw.dim_deal where uuid = '00013e56-955c-434a-89f1-95ee3658acf2';
select * from user_groupondw.dim_division;
select * from user_edwprod.deal_division_map_unity limit 5;
select * from edwprod.ods_deal_division limit 5;





select merchant_uuid, count(deal_uuid) from 
(select merchant_uuid, merchant_name, deal_uuid, discount_ from np_merch) as mp 
left join
(select division_id, deal_uuid from user_edwprod.deal_division_map) as y on mp.deal_uuid = y.deal_uuid
left join
(select division_uuid, division_name from user_groupondw.dim_lat_lng_loc_map) as k on y.division_id = k.division_uuid
group by merchant_uuid where merchant_uuid = 'cac4794c-2387-cf20-17bd-e6da58f31786';




select mp.merchant_uuid, count(mp.deal_uuid),y.division_name from 
from np_merch as mp 
left join
user_edwprod.deal_division_map as y on mp.deal_uuid = y.deal_uuid
left join
user_groupondw.dim_lat_lng_loc_map as k on y.division_id = k.division_uuid
where mp.merchant_uuid = 'cac4794c-2387-cf20-17bd-e6da58f31786' group by mp.merchant_uuid, y.division_name ;













drop table merc

create table merc as 
select * from (
select d.*, c.* from
(select a.product_uuid product_uuid, (a.contract_sell_price - a.groupon_value/a.contract_sell_price) calc, a.merchant_uuid merchant_uuidone, b.deal_uuid deal_uuid, b.country_code country_code, x.l1, x.l2, x.l3, y.division_id  from
(select 
	merchant_uuid, 
	product_uuid product_uuid, 
	groupon_value, 
	contract_sell_price
	from user_edwprod.dim_offer_ext 
) as a
join
(select 
	deal_uuid deal_uuid, 
	country_code country_code
	from user_groupondw.active_deals where year(load_date) = '2020' and month(load_date) = '03' and available_qty > 0 and sold_out = 'false' group by deal_uuid, country_code) as b 
on a.product_uuid = b.deal_uuid
join
(select 
				    deal_id deal_uuid, 
					country_code country_code,  
					country_id country_id,
					grt_l1_cat_name l1, 
					grt_l2_cat_name l2, 
					grt_l3_cat_name l3
			from user_edwprod.dim_gbl_deal_lob where  grt_l1_cat_name = 'L1 - Local') x on a.product_uuid = x.deal_uuid
left join 
( select division_id, deal_uuid from user_edwprod.deal_division_map) y on a.product_uuid = y.deal_uuid
) as c
join 
(
select 
	merchant_uuid merchant_uuid, 
	name merchant_name, 
	feature_country country_id
	from user_edwprod.dim_merchant
) as d on c.merchant_uuidone = d.merchant_uuid group by) as uni;


select * from merc;


select division_uuid, division_name from user_groupondw.dim_lat_lng_loc_map;


select * from user_edwprod.dim_division;

select count(*) from user_groupondw.dim_lat_lng_loc_map;