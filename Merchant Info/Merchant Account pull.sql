drop table grp_gdoop_bizops_db.nvp_all_merchant_info;

select count(*), count(distinct active_merchant_uuid) from grp_gdoop_bizops_db.nvp_active_merchant_info;

drop table grp_gdoop_bizops_db.nvp_active_merchant_info;
create table grp_gdoop_bizops_db.nvp_active_merchant_info stored as orc as
select
 a.active_merchant_uuid, 
 a.salesforce_account_id, 
 a.country_code,
 count(distinct a.active_deals) total_active_deals,
 count(distinct a.active_bt_deals) total_active_bt_deals,
 case when sum(units_sold) > 0 then 1 else 0 end flag_unit_sold
from
(select 
mp.merchant_uuid active_merchant_uuid,
sf.salesforce_account_id,
all_d.country_code,
all_d.deal_uuid active_deals, 
bo.deal_uuid active_bt_deals,
bo.load_date merchant_deal_load_date
from 
(select 
	merchant_uuid, 
	product_uuid product_uuid
	from user_edwprod.dim_offer_ext  where inv_product_uuid <> '-1' and contract_sell_price > 0 and groupon_value <> 0 and inventory_service_name <> 'tpis' 
	group by product_uuid, merchant_uuid
) as mp
join 
(select 
	deal_uuid, 
	salesforce_id, 
	case when country_code = 'GB' then 'UK' else country_code end country_code, 
	load_date
	from user_groupondw.active_deals where sold_out = 'false' and available_qty >0 and load_date = '2020-07-26'
) as all_d on mp.product_uuid = all_d.deal_uuid
join 
(select 
	deal_id, 
	grt_l2_cat_name l2
	from 
	user_edwprod.dim_gbl_deal_lob 
	where grt_l1_cat_name = 'L1 - Local'
	group by deal_id, grt_l2_cat_name) local_d on mp.product_uuid = local_d.deal_id
left join
(select 
	deal_uuid,
	country, 
	min(load_date) load_date 
	from grp_gdoop_bizops_db.sh_bt_active_deals_log
	where is_bookable = 1 and partner_inactive_flag = 0 and product_is_active_flag = 1 and load_date ='2020-07-26'
	group by deal_uuid, country) as bo on bo.deal_uuid = mp.product_uuid and all_d.country_code = bo.country
left join 
(select 
	merchant_uuid, 
	max(salesforce_account_id) salesforce_account_id
	from user_edwprod.dim_merchant group by merchant_uuid
	) as sf on mp.merchant_uuid = sf.merchant_uuid) as a
left join
(select deal_uuid, country_code, sum(units) units_sold from grp_gdoop_bizops_db.rt_bt_txns 
	where order_date >= '2018-07-01' group by deal_uuid, country_code) as b on a.active_deals = b.deal_uuid and a.country_code = b.country_code
group by 
 a.active_merchant_uuid, 
 a.salesforce_account_id, 
 a.country_code
;

select * from user_edwprod.dim_offer_ext where merchant_uuid = '040d04b4-9f8b-4d2d-b7dc-4b9f7e8eeae9';
select * from user_edwprod.dim_merchant where merchant_uuid = '040d04b4-9f8b-4d2d-b7dc-4b9f7e8eeae9';

select * from (select merchant_uuid, min(salesforce_account_id), count(distinct salesforce_account_id) count_ from user_edwprod.dim_merchant group by merchant_uuid) fin where count_ >1;

select * from user_edwprod.dim_merchant;
select * from user_edwprod.dim_offer_ext;
select * from user_edwprod.dim_gbl_deal_lob;
select * from user_groupondw.gbl_dim_deal;
select * from user_groupondw.active_deals;