
drop table sandbox.nvp_model_deals2;
create MULTISET table sandbox.nvp_model_deals2 as
(select a.*, 
	c.fine_print, 
	lower(c.fine_print) fine_print_c, 
	c.locale lang_loc,
	d.min_contract_sell, 
	d.max_contract_sell, 
	d.min_groupon_value, 
	d.max_groupon_value,
	atri.deal_uuid3, 
	case when atri.deal_uuid3 is not null then 1 else 0 end deal_attrited ---1 means the deal is not attrited and is still there after whatsoever month
From 
		(select
			deal_id as deal_uuid, 
			grt_l1_cat_name as lone, 
			grt_l2_cat_name as ltwo, 
			grt_l3_cat_name as lthree,
			min(country_code) as country_code,  
			min(country_id) as country_id,
			count(deal_id) as count_
		from user_edwprod.dim_gbl_deal_lob where grt_l1_cat_name = 'L1 - Local' group by 1,2,3,4 having count_ = 1) as a
	inner join 
		(select 
			distinct deal_uuid deal_uuid1, 
			case when country_code = 'GB' then 'UK' when country_code = 'QC' then 'CA' else country_code end country_code
		from 
			user_groupondw.active_deals 
			where cast(load_date as date) >= cast('2019-09-01' as date) and cast(load_date as date) <= cast('2019-09-30' as date) and available_qty > 0 and sold_out = 'false' 
			group by deal_uuid1, country_code
		) as b on b.deal_uuid1 = a.deal_uuid and a.country_code = b.country_code
	left join 
		(select 
			distinct
			deal_uuid deal_uuid2, 
			locale, 
			fine_print 
		from user_gp.deal_localized_contents 
		) as c on a.deal_uuid = c.deal_uuid2
	left join
		(select product_uuid,
				min(groupon_value) min_groupon_value,
				max(groupon_value) max_groupon_value,
				min(contract_sell_price) min_contract_sell,
				max(contract_sell_price) max_contract_sell
		from user_edwprod.dim_offer_ext 
		group by product_uuid) as d on a.deal_uuid = d.product_uuid
	left join
	(select 
			distinct deal_uuid deal_uuid3, 
			case when country_code = 'GB' then 'UK' when country_code = 'QC' then 'CA' else country_code end country_code
		from 
			user_groupondw.active_deals 
			where cast(load_date as date) = cast('2019-10-15' as date) and available_qty > 0 and sold_out = 'false' 
			group by deal_uuid3, country_code
		) as atri on atri.deal_uuid3 = a.deal_uuid and atri.country_code = a.country_code
) with data;



select * from sandbox.nvp_model_new_client2 where deal_uuid = 'c2cd1166-c824-4c02-ab34-fd31c3505c2b';

drop table sandbox.nvp_model_new_client2;
create MULTISET table sandbox.nvp_model_new_client2 as(
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
			min_contract_sell, 
			max_contract_sell, 
			deal_attrited,
			max(new_client_restriction) new_client_restriction_seen
		from 
			(select 
						dd.deal_uuid deal_uuid, 
						dd.country_code country_cd, 
						dd.country_id country_id, 
						dd.lone l1, 
						dd.ltwo l2, 
						dd.lthree l3,
						dd.fine_print_c fine_print_c,
						dd.min_contract_sell, 
						dd.max_contract_sell, 
						dd.min_discount,
						dd.max_discount,
						dd.deal_attrited,
						case when dd.avg_sell_price < 25 then '<25' when dd.avg_sell_price >=25 and dd.avg_sell_price < 50 then '25-50' when dd.avg_sell_price >= 50 then '>=50' else null end avg_sell_price,
						case when typ_a.ind = 1 then 1 else 0 end new_client_restriction
			from 
								(select 
									deal_uuid, 
									country_code,
									country_id, 
									lone, 
									ltwo, 
									lthree, 
									fine_print_c,
									(min_contract_sell + max_contract_sell)/2 avg_sell_price,
									case when min_groupon_value = 0 then 0 else cast((min_groupon_value - min_contract_sell)*100/min_groupon_value as int) end as min_discount, 
									case when max_groupon_value = 0 then 0 else cast((max_groupon_value - max_contract_sell)*100/max_groupon_value as int) end as max_discount,
									min_contract_sell, 
									max_contract_sell,
									deal_attrited
								from 
									sandbox.nvp_model_deals2) dd
							left join 
								(select 
									text_res,
									text_l3,
									text_l2,
									text_l1,
									restriction_type,
									country,
									1 AS ind
								from 
									sandbox.nvp_model_restriction where restriction_type = 'New Clients Restrictions'
									) typ_a on REGEXP_SUBSTR(dd.fine_print_c, lower(typ_a.text_res), 1, 1, 'i') = lower(typ_a.text_res) and dd.country_code = typ_a.country
					) fin_ 
		group by 1,2,3,4,5,6,7,8,9,10,11,12
) with data;


select * from sandbox.nvp_model_new_client2;

select * from sandbox.nvp_model_deals2; ---L2 - Health / Beauty / Wellness

select * from (select deal_uuid, count(*) count_ from sandbox.nvp_model_deals2 where country_code = 'US' and ltwo = 'L2 - Health / Beauty / Wellness' group by deal_uuid) as a where count_ >1;
select count(deal_uuid), count(distinct deal_uuid) from sandbox.nvp_model_new_client2 where country_cd = 'US' and l2 = 'L2 - Health / Beauty / Wellness';
select country_code, count(deal_uuid), count(distinct deal_uuid) from sandbox.nvp_model_deals2 group by country_code;
select country_cd, count(deal_uuid), count(distinct deal_uuid) from sandbox.nvp_model_new_client2 group by country_cd;

-------

SELECT COUNT(deal_uuid), count(distinct deal_uuid) from sandbox.nvp_model_deals where country_code = 'US'; 
select deal_uuid, count_ from (select deal_uuid, count(*) count_ from sandbox.nvp_model_deals where country_code = 'AU' group by deal_uuid) a where a.count_ >1;
select * from sandbox.nvp_model_deals where deal_uuid = '74fe33d7-3d3f-473c-b742-5f951592080a';
select country_code, count(deal_uuid), count(distinct deal_uuid) from sandbox.nvp_model_deals group by country_code;

drop table sandbox.nvp_model_deals;
create MULTISET table sandbox.nvp_model_deals as
(select a.*, 
	c.fine_print, 
	lower(c.fine_print) fine_print_c, 
	c.locale lang_loc,
	d.min_contract_sell, 
	d.max_contract_sell, 
	d.min_groupon_value, 
	d.max_groupon_value,
	g.Accountid,
	g.division,
	h.metal
From 
		(select
			deal_id as deal_uuid, 
			grt_l1_cat_name as lone, 
			grt_l2_cat_name as ltwo, 
			grt_l3_cat_name as lthree,
			min(country_code) as country_code,  
			min(country_id) as country_id,
			count(deal_id) as count_
		from user_edwprod.dim_gbl_deal_lob where grt_l1_cat_name = 'L1 - Local' group by 1,2,3,4 having count_ = 1) as a
	inner join 
		(select 
			distinct deal_uuid deal_uuid1, 
			case when country_code = 'GB' then 'UK' when country_code = 'QC' then 'CA' else country_code end country_code
		from 
			user_groupondw.active_deals 
			where cast(load_date as date) >= cast('2019-04-01' as date) and cast(load_date as date) <= cast('2019-05-31' as date) and available_qty > 0 and sold_out = 'false' 
			group by deal_uuid1, country_code
		) as b on b.deal_uuid1 = a.deal_uuid and a.country_code = b.country_code
	left join 
		(select 
			distinct
			deal_uuid deal_uuid2, 
			locale, 
			fine_print 
		from user_gp.deal_localized_contents 
		) as c on a.deal_uuid = c.deal_uuid2
	left join
		(select product_uuid,
				min(merchant_uuid) merchant_uuid,
				min(groupon_value) min_groupon_value,
				max(groupon_value) max_groupon_value,
				min(contract_sell_price) min_contract_sell,
				max(contract_sell_price) max_contract_sell
		from user_edwprod.dim_offer_ext 
		group by product_uuid) as d on a.deal_uuid = d.product_uuid
	left join 
		(
		select deal_uuid, 
				max(id) id
		from user_edwprod.sf_opportunity_2 group by 1
		) f on f.deal_uuid = a.deal_uuid
	left join 
		(
		select 
			ID, 
			max(Accountid) Accountid, 
			max(division) division
		from user_edwprod.sf_opportunity_1 group by 1
		)as g on f.id = g.ID
	left join 
		(
		select 
			Account_ID_18 account_id, 
			max(Merchant_Segmentation__c) metal
		from 
		user_edwprod.sf_account group by 1
		) as h on g.Accountid = h.account_id
) with data;

select distinct count_ from (select deal_uuid, count(*) count_ from sandbox.nvp_model_deals where country_code = 'US' and ltwo = 'L2 - Health / Beauty / Wellness' group by deal_uuid) as a;

SELECT count(*), count(distinct deal_uuid) from sandbox.nvp_model_deals where country_code = 'US' and ltwo = 'L2 - Health / Beauty / Wellness';

		
	/*
select 
	count(a.deal_uuid),
	count(distinct a.deal_uuid)
from 
(select 
			deal_id as deal_uuid, 
			grt_l1_cat_name as lone, 
			grt_l2_cat_name as ltwo, 
			grt_l3_cat_name as lthree,
			min(country_code) as country_code,  
			min(country_id) as country_id, 
			count(deal_id) as count_
		from user_edwprod.dim_gbl_deal_lob where grt_l1_cat_name = 'L1 - Local' group by 1,2,3,4 having count_ = 1) as a
	inner join 
		(select 
			distinct deal_uuid deal_uuid1, 
			case when country_code = 'GB' then 'UK' when country_code = 'QC' then 'CA' else country_code end country_code
		from 
			user_groupondw.active_deals 
			where cast(load_date as date) >= cast('2019-09-01' as date) and cast(load_date as date) <= cast('2019-09-30' as date) and available_qty > 0 and sold_out = 'false' 
			group by deal_uuid1, country_code
		) as b on b.deal_uuid1 = a.deal_uuid and a.country_code = b.country_code;
	
select count(a.deal_uuid), count(distinct a.deal_uuid) from 
		(select 
			deal_id as deal_uuid, 
			grt_l1_cat_name as lone, 
			grt_l2_cat_name as ltwo, 
			grt_l3_cat_name as lthree,
			country_code as country_code,  
			country_id as country_id
		from user_edwprod.dim_gbl_deal_lob where grt_l1_cat_name = 'L1 - Local') as a
	inner join 
		(select 
			distinct deal_uuid deal_uuid1, 
			case when country_code = 'GB' then 'UK' when country_code = 'QC' then 'CA' else country_code end country_code
		from 
			user_groupondw.active_deals 
			where cast(load_date as date) >= cast('2019-09-01' as date) and cast(load_date as date) <= cast('2019-09-30' as date) and available_qty > 0 and sold_out = 'false' 
			group by deal_uuid1, country_code
		) as b on b.deal_uuid1 = a.deal_uuid and a.country_code = b.country_code;




drop table sandbox.nvp_x_model;
create MULTISET table sandbox.nvp_x_model as
(select distinct(d.merchant_uuid) merchant_uuid_start
	from 
	(select
			deal_id as deal_uuid, 
			grt_l1_cat_name as lone, 
			grt_l2_cat_name as ltwo, 
			grt_l3_cat_name as lthree,
			min(country_code) as country_code,  
			min(country_id) as country_id,
			count(deal_id) as count_
		from user_edwprod.dim_gbl_deal_lob where grt_l2_cat_name = 'L2 - Health / Beauty / Wellness' and country_code = 'US' group by 1,2,3,4 having count_ = 1) as a
	inner join 
		(select 
			distinct deal_uuid deal_uuid1, 
			case when country_code = 'GB' then 'UK' when country_code = 'QC' then 'CA' else country_code end country_code,
			max(load_date) max_load
		from 
			user_groupondw.active_deals 
			where cast(load_date as date) >= cast('2019-01-01' as date) and cast(load_date as date) <= cast('2019-01-31' as date) and available_qty > 0 and sold_out = 'false' 
			group by deal_uuid1, country_code
		) as b on b.deal_uuid1 = a.deal_uuid and a.country_code = b.country_code
	left join 
		(select product_uuid,
				merchant_uuid
		from user_edwprod.dim_offer_ext 
		group by product_uuid,merchant_uuid) as d on a.deal_uuid = d.product_uuid) with data;

	
	
	
drop table sandbox.nvp_y_model;
create multiset table sandbox.nvp_y_model as
(select distinct(d.merchant_uuid) merchant_uuid_end
	from 
	(select
			deal_id as deal_uuid, 
			grt_l1_cat_name as lone, 
			grt_l2_cat_name as ltwo, 
			grt_l3_cat_name as lthree,
			min(country_code) as country_code,  
			min(country_id) as country_id,
			count(deal_id) as count_
		from user_edwprod.dim_gbl_deal_lob where grt_l2_cat_name = 'L2 - Health / Beauty / Wellness' and country_code = 'US' group by 1,2,3,4 having count_ = 1) as a
	inner join 
		(select 
			distinct deal_uuid deal_uuid1, 
			case when country_code = 'GB' then 'UK' when country_code = 'QC' then 'CA' else country_code end country_code,
			max(load_date) max_load
		from 
			user_groupondw.active_deals 
			where cast(load_date as date) = cast('2019-12-01' as date) and available_qty > 0 and sold_out = 'false' 
			group by deal_uuid1, country_code
		) as b on b.deal_uuid1 = a.deal_uuid and a.country_code = b.country_code
	left join 
		(select product_uuid,
				merchant_uuid
		from user_edwprod.dim_offer_ext 
		group by product_uuid,merchant_uuid) as d on a.deal_uuid = d.product_uuid) with data


select count(merchant_uuid_start),
		sum(merchant_didnt_attrit)
from 
(select 
	x.merchant_uuid_start,
	case when y.merchant_uuid_end is not null then 1 else 0 end merchant_didnt_attrit
	from 
(select * from sandbox.nvp_x_model) as  x
left join 
(select * from sandbox.nvp_y_model)as y on x.merchant_uuid_start = y.merchant_uuid_end )as fin_
		

		
		
	*/


---------
select * from sandbox.nvp_model_restriction;

drop table sandbox.nvp_model_restriction;

create multiset table sandbox.nvp_model_restriction
(text_res varchar(100) character set unicode, 
text_res_eng_exact_translation varchar(100) character set unicode,
text_l3 varchar(100) character set unicode, 
text_l2 varchar(100) character set unicode,
text_l1 varchar(100) character set unicode,
restriction_type varchar(100) character set unicode,
country varchar(100) character set unicode
);

---------


drop table sandbox.nvp_model_new_client;
create MULTISET table sandbox.nvp_model_new_client as(
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
			min_contract_sell, 
			max_contract_sell, 
			account_id, 
			metal,
			division,
			max(new_client_restriction) new_client_restriction_seen
		from 
			(select 
						dd.deal_uuid deal_uuid, 
						dd.country_code country_cd, 
						dd.country_id country_id, 
						dd.lone l1, 
						dd.ltwo l2, 
						dd.lthree l3,  
						dd.fine_print_c fine_print_c, 
						dd.min_contract_sell, 
						dd.max_contract_sell, 
						dd.min_discount,
						dd.max_discount,
						dd.account_id, 
						dd.division,
						dd.metal,
						case when dd.avg_sell_price < 25 then '<25' when dd.avg_sell_price >=25 and dd.avg_sell_price < 50 then '25-50' when dd.avg_sell_price >= 50 then '>=50' else null end avg_sell_price,
						case when typ_a.ind = 1 then 1 else 0 end new_client_restriction
			from 
								(select 
									deal_uuid, 
									country_code,
									country_id, 
									lone, 
									ltwo, 
									lthree, 
									fine_print_c,
									(min_contract_sell + max_contract_sell)/2 avg_sell_price,
									case when min_groupon_value = 0 then 0 else cast((min_groupon_value - min_contract_sell)*100/min_groupon_value as int) end as min_discount, 
									case when max_groupon_value = 0 then 0 else cast((max_groupon_value - max_contract_sell)*100/max_groupon_value as int) end as max_discount,
									min_contract_sell, 
									max_contract_sell,
									Accountid account_id, 
									division, 
									metal metal
								from 
									sandbox.nvp_model_deals) dd
							left join 
								(select 
									text_res,
									text_l3,
									text_l2,
									text_l1,
									restriction_type,
									country,
									1 AS ind
								from 
									sandbox.nvp_model_restriction where restriction_type = 'New Clients Restrictions'
									) typ_a on REGEXP_SUBSTR(dd.fine_print_c, lower(typ_a.text_res), 1, 1, 'i') = lower(typ_a.text_res) and dd.country_code = typ_a.country
					) fin_ 
		group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
) with data;

select count(*), count(distinct deal_uuid) from sandbox.nvp_model_new_client where country_cd = 'US' and l2 = 'L2 - Health / Beauty / Wellness' and new_client_restriction_seen = 1;
select count(*), count(distinct deal_uuid) from sandbox.nvp_model_new_client where country_cd = 'US';

select new_client_restriction_seen, count(deal_uuid) from sandbox.nvp_model_new_client where l2 = 'L2 - Health / Beauty / Wellness' and country_cd = 'US' or country_cd = 'CA' group by new_client_restriction_seen;

---------TRASH


select count(deal_uuid), count(distinct deal_uuid) from sandbox.nvp_model_new_client;
select country_cd, count(deal_uuid), count(distinct deal_uuid) from sandbox.nvp_model_new_client group by country_cd;


select * from sandbox.nvp_model_new_client where deal_uuid = '31e168b8-4776-46f4-ae4d-6c7c9bb512d6';
select * from user_edwprod.dim_gbl_deal_lob where deal_id = '31e168b8-4776-46f4-ae4d-6c7c9bb512d6';

----

select new_client_restriction_seen, 
		count(deal_uuid) total_deals, 
		avg(min_contract_sell), 
		avg(max_contract_sell), 
		min(min_contract_sell), 
		max(max_contract_sell)
from sandbox.nvp_model_new_client where country_cd = 'US' and l2 = 'L2 - Health / Beauty / Wellness' and min_contract_sell <> 0 group by new_client_restriction_seen ;

select * from sandbox.nvp_model_deals where deal_uuid = 'c7cb95f9-80a0-b433-5898-7e49cd730ee4';



----
select top 5 * from user_groupondw.gbl_traffic_superfunnel_deal;

select TOP 5 a.order_id order_id1, 
			b.order_id order_id2, 
			a.parent_order_uuid parent1, 
			b.parent_order_uuid parent2, 
			a."action" actiona, 
			b."action" actionb 
from user_edwprod.fact_gbl_transactions a
left join user_edwprod.fact_gbl_ogp_transactions b 
on a.order_id = b.order_id and a.parent_order_uuid = b.parent_order_uuid 
where a.order_date = '2019-01-01' and b.order_date = '2019-01-01' and a."action" = 'authorize' and b."action" = 'authorize';

select top 5 * from user_edwprod.fact_gbl_ogp_transactions;
select 
		deal_id as deal_uuid
		from
		user_edwprod.dim_gbl_deal_lob where grt_l1_cat_name = 'L1 - Local';

select 
			parent_order_uuid, 
			count(distinct order_uuid), 
			count(order_uuid), 
			count(distinct deal_uuid), 
			count(deal_uuid)
			from 
			user_edwprod.fact_gbl_transactions a inner join user_edwprod.dim_gbl_deal_lob b on a.deal_uuid = b.deal_id
			where a.action = 'authorize' and cast(a.order_date as date) >= cast('2019-09-01' as date) and cast(a.order_date as date)<=cast('2019-09-30' as date) and b.grt_l1_cat_name = 'L1 - Local' and b.country_code = 'US' group by a.parent_order_uuid;
			
select transaction_qty, count(parent_order_uuid)
from user_edwprod.fact_gbl_transactions a inner join user_edwprod.dim_gbl_deal_lob b on a.deal_uuid = b.deal_id
where a.action = 'authorize' and a.order_uuid <> '-1' and cast(order_date as date) >= cast('2019-09-01' as date) and cast(order_date as date)<=cast('2019-09-30' as date) and b.grt_l1_cat_name = 'L1 - Local' and b.grt_l2_cat_name = 'L2 - Health / Beauty / Wellness' and b.country_code = 'US' and (transaction_qty = 0 or transaction_qty > 5 or transaction_qty is null) group by 1;


select * 
from user_edwprod.fact_gbl_transactions a inner join user_edwprod.dim_gbl_deal_lob b on a.deal_uuid = b.deal_id
where a.action = 'authorize' and a.order_uuid <> '-1' and cast(a.order_date as date) >= '2018-09-01' and a.is_zero_amount<> 1 and transaction_qty = 20 and b.grt_l1_cat_name = 'L1 - Local' and b.grt_l2_cat_name = 'L2 - Health / Beauty / Wellness';



select count(distinct parent_order_uuid) from user_edwprod.fact_gbl_transactions where action = 'authorize' and order_uuid <> '-1' and transaction_qty = 0 and cast(order_date as date) >= '2018-09-01' and is_zero_amount <> 1;


------- FGT TRASACTION QUANTITY ANALYSIS
SELECT * FROM sandbox.nvp_model_user_info;
drop table sandbox.nvp_model_user_info;
create multiset table sandbox.nvp_model_user_info as
(select 
	user_uuid, 
	merchant_uuid, 
	cast(min(order_date) as date) first_purchase
from user_edwprod.fact_gbl_transactions where action = 'authorize' and cast(order_date as date) >= '2018-09-01' group by user_uuid, merchant_uuid) with data

--Transaction quantity


select 
count(parent_order_uuid), 
sum(is_activation)
from
(select 
		a.parent_order_uuid, 
		max(a.is_activation) is_activation
		from 
			user_edwprod.fact_gbl_transactions a
		inner join 
			user_edwprod.dim_gbl_deal_lob b on a.deal_uuid = b.deal_id
where a.action = 'authorize' 
	and a.order_uuid <> '-1' 
	and cast(a.order_date as date) >= cast('2019-09-01' as date) 
	and cast(a.order_date as date)<=cast('2019-09-30' as date)
	and b.grt_l2_cat_name = 'L2 - Health / Beauty / Wellness'
	and b.country_code = 'US' 
group by 1) as fin_;

select * from user_edwprod.fact_gbl_transactions;

select
	deal_uuid, 
	purchase_case,
	count(parent_order_uuid) count_parent_order_uuid,
	count(distinct parent_order_uuid) count_d_parent_order_uuid, 
	sum(activation) sum_activation, 
	sum(nob_loc_USD) sum_nob_loc_USD, 
	sum(nor_loc_USD) sum_nor_loc_USD, 
	sum(ogp_nob_loc_usd) sum_ogp_nob_loc_usd,
	sum(ogp_nor_loc_usd) sum_ogp_nor_loc_usd, 
	sum(ogp_loc_usd) sum_ogp_loc_usd, 
	sum(ogp_total_estimate_loc_usd) sum_ogp_total_estimate_loc_usd
	from 
		(SELECT 
			user_uuid,
			parent_order_uuid,
			merchant_uuid, 
			deal_uuid, 
			order_date_,
			first_purchase_,
			user_uuid_m,
			activation,
			nob_loc_USD,
			nor_loc_USD,
			ogp_nob_loc_usd,
			ogp_nor_loc_usd,
			ogp_loc_usd,
			ogp_total_estimate_loc_usd, 
			case when user_uuid_m is not null and first_purchase_ < order_date_ then 'old_client' else 'first_time_client' end as purchase_case
		FROM 
		(select 
			f.user_uuid user_uuid,
			f.parent_order_uuid parent_order_uuid,
			f.merchant_uuid merchant_uuid, 
			f.deal_uuid deal_uuid, 
			min(f.order_date) order_date_,
			min(pre.first_purchase) first_purchase_,
			min(pre.user_uuid) user_uuid_m,
			max(f.is_activation) activation,
			sum(f.auth_nob_loc*e.fx_neutral_exchange_rate) nob_loc_USD,
			sum(f.auth_nor_loc*e.fx_neutral_exchange_rate) nor_loc_USD,
			sum(ogp.ogp_nob_loc*e.fx_neutral_exchange_rate) ogp_nob_loc_usd,
			sum(ogp.ogp_nor_loc*e.fx_neutral_exchange_rate) ogp_nor_loc_usd,
			sum(ogp.ogp_loc*e.fx_neutral_exchange_rate) ogp_loc_usd,
			sum(ogp.total_estimated_ogp_loc*e.fx_neutral_exchange_rate) ogp_total_estimate_loc_usd
		from 
				(select 
					country_id,
					order_uuid orders,
					parent_order_uuid, 
					order_id,
					cast(order_date as date) order_date,
					deal_uuid,
					merchant_uuid, 
					user_uuid,
					currency_code,
					auth_nob_loc,
					auth_nor_loc, 
					is_activation, 
					case when transaction_qty = 0 then '0 units' when transaction_qty = 1 then 'single unit' when transaction_qty = 2 then '2 units' when transaction_qty > 2 and transaction_qty<= 5 then 'between 2 and 5 units' else '>5 units' end transaction_qty_cat,
					transaction_qty
				from 
					user_edwprod.fact_gbl_transactions where action = 'authorize' and order_uuid <> '-1' and cast(order_date as date) >= cast('2019-09-01' as date) and cast(order_date as date)<=cast('2019-09-30' as date) and is_zero_amount <> 1 and transaction_qty >=0
					) as f
			left join 
				(select 
					distinct fx_neutral_exchange_rate,
					currency_from,
					period_key 
				from 
				user_groupondw.gbl_fact_exchange_rate where currency_to ='USD'
				) as e on f.currency_code=e.currency_from  and regexp_replace(substr(f.order_date,0,8),'-','') =e.period_key
			left join 
				(
				 select 
				 user_uuid, 
				 merchant_uuid,
				 cast(first_purchase as date) first_purchase
				 from 
				 sandbox.nvp_model_user_info 
				) as pre on f.user_uuid = pre.user_uuid and f.merchant_uuid = pre.merchant_uuid
			left join 
				(
				select 
				parent_order_uuid, 
				order_id, 
				ogp_nob_loc,
				ogp_nor_loc,
				ogp_loc, 
				total_estimated_ogp_loc
				from 
				user_edwprod.fact_gbl_ogp_transactions where action = 'authorize' and cast(order_date as date) >= cast('2019-09-01' as date) and cast(order_date as date)<=cast('2019-10-05' as date)
				) as ogp on f.parent_order_uuid = ogp.parent_order_uuid and f.order_id = ogp.order_id
			group by f.user_uuid, f.parent_order_uuid, f.merchant_uuid, f.deal_uuid) as fin_) final_ group by deal_uuid,purchase_case
-----
			
create volatile table sh_deal_dims as (
    sel deal_uuid,
        case when country_code in ('US','CA') then 'NAM' else 'INTL' end region,
        country_code,
        l2,
        merchant_uuid_,
        cast(dm_deal.month_start as date) launch_mth,
        cast(dm_merc.month_start as date) merchant_launch_mth
    from (
        sel doe.product_uuid as deal_uuid,
            max(gdl.country_code) country_code,
            max(grt_l2_cat_description) l2,
            max(doe.merchant_uuid) merchant_uuid_,
            min(ad.load_date) launch_date,
            min(launch_date) over (partition by merchant_uuid_) merchant_launch_date
        from user_edwprod.dim_offer_ext doe
        join user_groupondw.active_deals ad on doe.product_uuid = ad.deal_uuid
        join user_edwprod.dim_gbl_deal_lob gdl on doe.product_uuid = gdl.deal_id
        left join user_gp.clo_offers clo on doe.product_uuid = clo.deal_id
        where doe.inventory_service_name in ('vis','voucher')
        and gdl.grt_l1_cat_name = 'L1 - Local'
        and gdl.grt_l2_cat_description in ('HBW','F&D','TTD - Leisure')
        and ad.sold_out = 'false'
        and clo.deal_id is null
        and doe.merchant_uuid not in ('bb9dca9a-c592-11e7-abc4-cec278b6b50a','fb793c06-2637-4b84-95d1-08fb7dbbe5bc')
        group by 1
    ) t
    join user_groupondw.dim_day dd_deal on t.launch_date = dd_deal.day_rw
    join user_groupondw.dim_month dm_deal on dd_deal.month_key = dm_deal.month_key
    join user_groupondw.dim_day dd_merc on t.merchant_launch_date = dd_merc.day_rw
    join user_groupondw.dim_month dm_merc on dd_merc.month_key = dm_merc.month_key
) with data unique primary index (deal_uuid) on commit preserve rows;