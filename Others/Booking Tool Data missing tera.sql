

drop table sandbox.nvp_bt_rebooking_miss;
CREATE MULTISET TABLE sandbox.nvp_bt_rebooking_miss ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      reservation_id VARCHAR(64) CHARACTER SET UNICODE,
      formatted_reservation_uuid VARCHAR(64) CHARACTER SET UNICODE,
      formatted_user_uuid VARCHAR(64) CHARACTER SET UNICODE,
      blank VARCHAR(64) CHARACTER SET UNICODE,
      formatted_voucher_uuid VARCHAR(64) CHARACTER SET UNICODE,
      internal_status VARCHAR(64) CHARACTER SET UNICODE,
      merchant_uuid VARCHAR(64) CHARACTER SET UNICODE,
      start_datetime VARCHAR(64) CHARACTER SET UNICODE,
      country_name VARCHAR(64) CHARACTER SET UNICODE,
      merchant_feature_name VARCHAR(64) CHARACTER SET UNICODE,
      contract_id VARCHAR(64) CHARACTER SET UNICODE,
      cda_number VARCHAR(64) CHARACTER SET UNICODE,
      deal_description VARCHAR(64) CHARACTER SET UNICODE,
      retry_status VARCHAR(64) CHARACTER SET UNICODE
      )
NO PRIMARY INDEX;



/*drop table nvp_bt_rebooking_miss_temp;
create volatile table nvp_bt_rebooking_miss_temp as 
(select 
      reservation_id,
      formatted_reservation_uuid,
      formatted_user_uuid,
      blank,
      formatted_voucher_uuid,
      internal_status,
      merchant_uuid,
      cast(substr(start_datetime, 1, 10) as date) start_datetime,
      country_name,
      merchant_feature_name,
      contract_id,
      cda_number,
      deal_description,
      retry_status
 from 
      sandbox.nvp_bt_rebooking_miss) with data on commit preserve rows;

select count(*) from sandbox.nvp_bt_rebooking_miss;*/

drop table sandbox.nvp_bt_rebooking_miss_fin;




create multiset table sandbox.nvp_bt_rebooking_miss_fin as
(select 
   a.formatted_user_uuid user_uuid,
   a.merchant_uuid,
   a.country_name,
   b.order_date, 
   b.inv_product_uuid possible_inv_product_uuid, 
   b.merch_product_uuid possible_merch_product_uuid,
   c.offer_description possible_offer_description
from 
     sandbox.nvp_bt_rebooking_miss as a
left join
	(select 
	    parent_order_uuid, 
	    order_uuid, 
	    user_uuid, 
	    merchant_uuid,
	    inv_product_uuid,
	    merch_product_uuid,
	    order_date, 
	    row_number() over(partition by user_uuid, merchant_uuid order by order_date desc) rank_of_date
	from user_edwprod.fact_gbl_transactions
	where 
	    order_date >= '2019-01-01'
	    and action = 'capture'
	) 
	as b on a.formatted_user_uuid = b.user_uuid and a.merchant_uuid = b.merchant_uuid and b.rank_of_date = 1
left join user_edwprod.dim_offer_ext as c on b.inv_product_uuid = c.inv_product_uuid and b.merch_product_uuid = c.merch_product_uuid
) with data;

select * from sandbox.nvp_bt_rebooking_miss_fin;
select * from sandbox.nvp_bt_rebooking_miss;


