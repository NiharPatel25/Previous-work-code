----importing deals with restrictions
hdfs://cerebro-namenode/user/nihpatel/financial_model/Financial_model_1.csv;
/*	deal_uuid string, 
	country_cd string, 
	country_id int, 
	l1 string, 
	l2 string, 
	l3 string, 
	avg_sell_price string,
	min_discount int, 
	max_discount int, 
	min_contract_sell int, 
	max_contract_sell int, 
	deal_attrited int,
	new_client_restriction_seen int*/
--for the second table

/*deal_uuid, 
    	country_cd, 
    	cast(country_id as int) country_id, 
    	l1,
    	l2,
    	l3,
    	avg_sell_price,
		cast(min_discount as int) as min_discount, 
		cast(max_discount as int) as max_discount, 
		cast(min_contract_sell as int) as min_contract_sell, 
		cast(max_contract_sell as int) as max_contract_sell,
		account_id as account_id, 
		division as division, 
		metal as metal, 
    	cast(new_client_restriction_seen as int) as new_client_restriction_seen*/

/*deal_uuid string, 
	country_cd string, 
	country_id int, 
	l1 string, 
	l2 string, 
	l3 string, 
	avg_sell_price string,
	min_discount int, 
	max_discount int, 
	min_contract_sell int, 
	max_contract_sell int, 
	account_id string, 
	metal string, 
	division string,
	new_client_restriction_seen int*/



---------

use grp_gdoop_bizops_db;
drop table if exists grp_gdoop_bizops_db.nvp_model_new_client;
create external table grp_gdoop_bizops_db.nvp_model_new_client (
	deal_uuid string, 
	country_cd string, 
	country_id int, 
	l1 string, 
	l2 string, 
	l3 string, 
	avg_sell_price string,
	min_discount int, 
	max_discount int, 
	min_contract_sell int, 
	max_contract_sell int, 
	deal_attrited int,
	new_client_restriction_seen int
)  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES
(
    "separatorChar" = ",",
    "quoteChar"     = "\""
)
location 'hdfs://cerebro-namenode/user/nihpatel/fin_model_update'
;

select * from grp_gdoop_bizops_db.nvp_model_new_client;

drop table if exists grp_gdoop_bizops_db.nvp_model_new_client_fil;
create table grp_gdoop_bizops_db.nvp_model_new_client_fil
    stored as orc
    as
    select 
    	deal_uuid, 
		country_cd, 
		cast(country_id as int) country_id, 
		l1, 
		l2, 
		l3 , 
		avg_sell_price,
		cast(min_discount as int) min_discount, 
		cast(max_discount as int) max_discount, 
		cast(min_contract_sell as int) min_contract_sell, 
		cast(max_contract_sell as int) max_contract_sell, 
		cast(deal_attrited as int) deal_attrited,
		cast(new_client_restriction_seen as int) new_client_restriction_seen
   	from grp_gdoop_bizops_db.nvp_model_new_client;

select * from grp_gdoop_bizops_db.nvp_model_new_client_fil;


-----------financial table

create table grp_gdoop_bizops_db.nvp_model_user_info stored as orc as
select 
	user_uuid, 
	merchant_uuid, 
	cast(min(order_date) as date) first_purchase
from user_edwprod.fact_gbl_transactions where action = 'authorize' and cast(order_date as date) >= '2018-09-01' group by user_uuid, merchant_uuid;

----------
create table grp_gdoop_bizops_db.nvp_model_cancel_info stored as orc as
select 
	parent_order_uuid,
	order_uuid
from user_edwprod.fact_gbl_transactions where action = 'refund' and cast(order_date as date) >= '2019-09-01' and cast(order_date as date) < '2020-01-01';

select distinct action from user_edwprod.fact_gbl_transactions;

-----------udv breakdown
select a.deal_uuid, a.event_date, a.unique_dv_visitors, CONCAT(cast(a.unique_dv_visitors as string),'+', cast(a.event_date as string),'+', cast(a.deal_uuid as string)) result_ from user_groupondw.gbl_traffic_superfunnel_deal a where cast(event_date as date) >= cast('2019-09-01' as date) and cast(event_date as date) <= cast('2019-09-30'as date) and deal_uuid is not null limit 5;
select * from user_groupondw.gbl_traffic_superfunnel_deal limit 5;
select * from user_groupondw.user_bcookie_mapping limit 5;

create table grp_gdoop_bizops_db.nvp_model_deal_info stored as orc as
select 
	deal_uuid, 
	purchase_case, 
	sum(udv_int) sum_udv,
	count(distinct udvisitor_date_deal_id_udv) count_udv,
	count(distinct bcook_date_deal_id_udv) count_bcook_udv,
	count(distinct bcook_date_udvv) count_bcook_udvv
from 
(select 
	a.deal_uuid deal_uuid,
	case when e.user_uuid is not null and e.first_purchase < a.event_date then 'old_client' else 'first_time_client' end purchase_case, 
	a.unique_deal_views udv_int,
	a.udvisitor_date_deal_id_udv as udvisitor_date_deal_id_udv,
	a.bcook_date_deal_id_udv as bcook_date_deal_id_udv,
	a.bcook_date_udvv as bcook_date_udvv
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
		user_groupondw.gbl_traffic_superfunnel_deal where cast(event_date as date) >= cast('2019-09-01' as date) and cast(event_date as date) <= cast('2019-09-30'as date) and deal_uuid is not null
	) a 
	inner join 
	(select 
		deal_id as deal_uuid
		from
		user_edwprod.dim_gbl_deal_lob where grt_l1_cat_name = 'L1 - Local'
	) as b on a.deal_uuid = b.deal_uuid
	left join 
	(select 
		product_uuid, 
		max(merchant_uuid) as merchant_uuid
		from 
		user_edwprod.dim_offer_ext group by product_uuid
	) as c on a.deal_uuid = c.product_uuid
	left join 
	( select 
		bcookie,
		user_uuid,
		cast(event_date as date) event_date2
		from 
		user_groupondw.user_bcookie_mapping where cast(event_date as date) >= cast('2019-09-01' as date) and cast(event_date as date) <= cast('2019-09-30'as date)
	) as d on a.cookie_b = d.bcookie and a.event_date = d.event_date2
	left join 
	(select 
		user_uuid, 
		merchant_uuid,
		cast(first_purchase as date) first_purchase
		from
		grp_gdoop_bizops_db.nvp_model_user_info
	) as e on d.user_uuid = e.user_uuid and c.merchant_uuid = e.merchant_uuid) fin_ group by deal_uuid, purchase_case;



------deal transactions 
-----we have not eliminated L1 here



DROP TABLE IF EXISTS grp_gdoop_bizops_db.nvp_model_fin_info;
create table grp_gdoop_bizops_db.nvp_model_fin_info stored as orc as
select
	deal_uuid, 
	purchase_case,
	order_cancelled_,
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
			order_cancelled_,
			case when user_uuid_m is not null and first_purchase_ < order_date_ then 'old_client' else 'first_time_client' end as purchase_case
		FROM 
		(select 
			f.user_uuid user_uuid,
			f.parent_order_uuid parent_order_uuid,
			f.merchant_uuid merchant_uuid, 
			f.deal_uuid deal_uuid, 
			max(f.order_cancelled_info) order_cancelled_,
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
					case when b.order_uuid_ is not null then 1 else 0 end order_cancelled_info
				from 
					user_edwprod.fact_gbl_transactions as a 
				left join 
					(select parent_order_uuid parent_order_uuid_, order_uuid order_uuid_ 
						from grp_gdoop_bizops_db.nvp_model_cancel_info 
						group by parent_order_uuid, order_uuid) as b on a.parent_order_uuid = b.parent_order_uuid_ and a.order_uuid = b.order_uuid_
				where a.action = 'authorize' and a.order_uuid <> '-1' and cast(a.order_date as date) >= cast('2019-04-01' as date) and cast(a.order_date as date)<=cast('2019-05-31' as date)
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
				 grp_gdoop_bizops_db.nvp_model_user_info 
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
				user_edwprod.fact_gbl_ogp_transactions where action = 'authorize' and cast(order_date as date) >= cast('2019-04-01' as date) and cast(order_date as date)<=cast('2019-05-05' as date)
				) as ogp on f.parent_order_uuid = ogp.parent_order_uuid and f.order_id = ogp.order_id
			group by f.user_uuid, f.parent_order_uuid, f.merchant_uuid, f.deal_uuid) as fin_) final_ group by deal_uuid,purchase_case,order_cancelled_;


-----

DROP TABLE IF EXISTS grp_gdoop_bizops_db.nvp_model_fin_info5;
create table grp_gdoop_bizops_db.nvp_model_fin_info5 stored as orc as
select
	deal_uuid, 
	purchase_case,
	sum(transaction_qty) transaction_qty
	from 
		(SELECT 
			user_uuid,
			parent_order_uuid,
			merchant_uuid, 
			deal_uuid, 
			order_date_,
			first_purchase_,
			user_uuid_m,
			transaction_qty, 
			case when user_uuid_m is not null and first_purchase_ < order_date_ then 'old_client' else 'first_time_client' end as purchase_case
		FROM 
		(select 
			f.user_uuid user_uuid,
			f.parent_order_uuid parent_order_uuid,
			f.merchant_uuid merchant_uuid, 
			f.deal_uuid deal_uuid, 
			sum(transaction_qty) transaction_qty, 
			min(f.order_date) order_date_,
			min(pre.first_purchase) first_purchase_,
			min(pre.user_uuid) user_uuid_m
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
					transaction_qty
				from 
					user_edwprod.fact_gbl_transactions where action = 'authorize' and order_uuid <> '-1' and cast(order_date as date) >= cast('2019-09-01' as date) and cast(order_date as date)<=cast('2019-09-30' as date)
					) as f
			left join 
				(
				 select 
				 user_uuid, 
				 merchant_uuid,
				 cast(first_purchase as date) first_purchase
				 from 
				 grp_gdoop_bizops_db.nvp_model_user_info 
				) as pre on f.user_uuid = pre.user_uuid and f.merchant_uuid = pre.merchant_uuid
			group by f.user_uuid, f.parent_order_uuid, f.merchant_uuid, f.deal_uuid) as fin_) final_ group by deal_uuid,purchase_case;


-------- counting merchants
drop table grp_gdoop_bizops_db.nvp_model_deal_merch_info;
create table grp_gdoop_bizops_db.nvp_model_deal_merch_info stored as orc as
select product_uuid, min(merchant_uuid) merchant_uuid
from user_edwprod.dim_offer_ext 
group by product_uuid;
		
------ CALCULATIONS

select 
sum(count_d_parent_order_uuid)
from
(select 
		a.deal_uuid, 
		a.count_d_parent_order_uuid
		from 
			grp_gdoop_bizops_db.nvp_model_fin_info2 a
		inner join 
			user_edwprod.dim_gbl_deal_lob b on a.deal_uuid = b.deal_id
	and b.grt_l2_cat_name = 'L2 - Health / Beauty / Wellness'
	and b.country_code = 'US') as fin_;





select 
a.*, 
b.merchant_uuid
from 
(select * from grp_gdoop_bizops_db.nvp_model_new_client_fil) as a 
left join
(select product_uuid, merchant_uuid 
from user_edwprod.dim_offer_ext 
group by product_uuid, merchant_uuid) as b  on a.deal_uuid = b.product_uuid;

select 
a.new_client_restriction_seen,
count(a.deal_uuid),
count(b.merchant_uuid)
from 
(select * from grp_gdoop_bizops_db.nvp_model_new_client_fil) as a 
left join
(select product_uuid, merchant_uuid 
from user_edwprod.dim_offer_ext 
group by product_uuid, merchant_uuid) as b  on a.deal_uuid = b.product_uuid group by a.new_client_restriction_seen;
		
-------
drop table grp_gdoop_bizops_db.nvp_stat__new_repeat;
create table grp_gdoop_bizops_db.nvp_stat__new_repeat stored as orc as
select 
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
				from grp_gdoop_bizops_db.nvp_model_new_client_fil where new_client_restriction_seen = 1 and division is not null) as a 
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
		where country_cd = 'US' and l2 = 'L2 - Health / Beauty / Wellness' group by division order by sum_nob desc limit 50;

select
	new_client_restriction_seen, 
	purchase_case, 
	min(min_groupon_value), 
	max(max_groupon_value),
	min(min_contract_sell), 
	max()
	count(distinct deal_uuid) count_deals,
	sum(count_udv) sum_udv2,
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

		a.new_client_restriction_seen,
		b.sum_udv,
		b.count_udv,
		b.count_bcook_udv,
		b.purchase_case,
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
				cast(min_contract_sell as float) min_contract_sell, 
				cast(max_contract_sell as float) max_contract_sell, 
				cast(min_groupon_value as float) min_groupon_value, 
				cast(max_groupon_value as float) max_groupon_value, 
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
				sum_ogp_nob_loc_usd,
				sum_ogp_nor_loc_usd,
				sum_ogp_loc_usd,
				sum_ogp_total_estimate_loc_usd,
				sum_activation
			from
			grp_gdoop_bizops_db.nvp_model_fin_info2
			) as c on a.deal_uuid = c.deal_uuid and b.purchase_case = c.purchase_case) fin where country_cd = 'US' and l2 = 'L2 - Health / Beauty / Wellness' group by new_client_restriction_seen, purchase_case order by new_client_restriction_seen, purchase_case;

		
-----ADDITIONAL USER INFO
drop table grp_gdoop_bizops_db.nvp_model_var_price;



create table grp_gdoop_bizops_db.nvp_model_var_price stored as orc as
select
    deal_uuid,
    purchase_case,
    transaction_purchase_cat,
    sum(nob_loc_USD) sum_nob_loc_USD,
    sum(nor_loc_USD) sum_nor_loc_USD,
    sum(ogp_loc_usd) sum_ogp_loc_usd
    from
      (SELECT
            user_uuid,
            parent_order_uuid,
            merchant_uuid,
            deal_uuid,
            order_date_,
            first_purchase_,
            user_uuid_m,
            nob_loc_USD,
            nor_loc_USD,
            ogp_loc_usd,
            case when transaction_qty = 4 then '4 unit' when transaction_qty = 3 then '3 unit' when transaction_qty = 1 then '1' when transaction_qty = 2 then '2 unit' when transaction_qty = 0 then '0 units' when transaction_qty > 4 then 'more than 4 units' end transaction_purchase_cat,
            case when user_uuid_m is not null and first_purchase_ < order_date_ then 'old_client' else 'first_time_client' end as purchase_case
       FROM 
        (select 
            f.user_uuid user_uuid,
            f.parent_order_uuid parent_order_uuid,
            f.merchant_uuid merchant_uuid, 
            f.deal_uuid deal_uuid, 
            sum(transaction_qty) transaction_qty, 
            min(f.order_date) order_date_,
            min(pre.first_purchase) first_purchase_,
            min(pre.user_uuid) user_uuid_m,
            sum(f.auth_nob_loc*e.fx_neutral_exchange_rate) nob_loc_USD,
            sum(f.auth_nor_loc*e.fx_neutral_exchange_rate) nor_loc_USD,
            sum(ogp.ogp_loc*e.fx_neutral_exchange_rate) ogp_loc_usd
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
                  transaction_qty
               from 
                  user_edwprod.fact_gbl_transactions where action = 'authorize' and order_uuid <> '-1' and cast(order_date as date) >= cast('2019-09-01' as date) and cast(order_date as date)<=cast('2019-09-30' as date)
            ) as f
          left join 
            (select distinct fx_neutral_exchange_rate,
              currency_from,
              period_key 
              from 
              user_groupondw.gbl_fact_exchange_rate where currency_to ='USD'
            ) as e on f.currency_code=e.currency_from  and regexp_replace(substr(f.order_date,0,8),'-','') = e.period_key
          left join 
            (select 
              user_uuid, 
              merchant_uuid,
               cast(first_purchase as date) first_purchase
               from 
               grp_gdoop_bizops_db.nvp_model_user_info 
            ) as pre on f.user_uuid = pre.user_uuid and f.merchant_uuid = pre.merchant_uuid
          left join 
            (select 
               parent_order_uuid, 
               order_id, 
               ogp_nob_loc,
               ogp_nor_loc,
               ogp_loc, 
               total_estimated_ogp_loc
               from 
               user_edwprod.fact_gbl_ogp_transactions where action = 'authorize' and cast(order_date as date) >= cast('2019-09-01' as date) and cast(order_date as date)<=cast('2019-10-05' as date)
            ) as ogp on f.parent_order_uuid = ogp.parent_order_uuid and f.order_id = ogp.order_id
            group by f.user_uuid, f.parent_order_uuid, f.merchant_uuid, f.deal_uuid) as fin_) final_ group by deal_uuid,purchase_case,transaction_purchase_cat;

drop table grp_gdoop_bizops_db.nvp_model_var_price2;         
create table grp_gdoop_bizops_db.nvp_model_var_price2 stored as orc as
select 
   repetition, 
   sum(nob_loc_USD) nob_usd, 
   sum(nor_loc_USD) nor_usd, 
   sum(ogp_loc_usd) ogp_usd
   from
  (select 
        user_uuid, 
        parent_order_uuid,
        merchant_uuid,
        order_date,
        ROW_NUMBER() over(partition by merchant_uuid, user_uuid order by order_date asc) repetition,
        nob_loc_USD, 
        nor_loc_USD, 
        ogp_loc_usd
     from
       (select 
            f.user_uuid user_uuid,
            f.parent_order_uuid parent_order_uuid,
            f.merchant_uuid merchant_uuid, 
            f.deal_uuid deal_uuid, 
            cast(f.order_date as date) order_date,
            sum(transaction_qty) transaction_qty, 
            sum(f.auth_nob_loc*e.fx_neutral_exchange_rate) nob_loc_USD,
            sum(f.auth_nor_loc*e.fx_neutral_exchange_rate) nor_loc_USD,
            sum(ogp.ogp_loc*e.fx_neutral_exchange_rate) ogp_loc_usd
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
                  transaction_qty
               from 
                  user_edwprod.fact_gbl_transactions 
                  where action = 'authorize' and order_uuid <> '-1' and cast(order_date as date) >= cast('2019-01-01' as date) and cast(order_date as date)<=cast('2019-09-30' as date)
            ) as f
          left join 
            (select 
                 distinct fx_neutral_exchange_rate,
                 currency_from,
                 period_key 
              from 
                 user_groupondw.gbl_fact_exchange_rate where currency_to ='USD'
            ) as e on f.currency_code=e.currency_from  and regexp_replace(substr(f.order_date,0,8),'-','') = e.period_key
          left join 
            (select 
               parent_order_uuid, 
               order_id, 
               ogp_nob_loc,
               ogp_nor_loc,
               ogp_loc, 
               total_estimated_ogp_loc
               from 
               user_edwprod.fact_gbl_ogp_transactions where action = 'authorize' and cast(order_date as date) >= cast('2019-01-01' as date) and cast(order_date as date)<=cast('2019-10-05' as date)
            ) as ogp on f.parent_order_uuid = ogp.parent_order_uuid and f.order_id = ogp.order_id 
            group by f.user_uuid, f.parent_order_uuid, f.merchant_uuid, f.deal_uuid,cast(f.order_date as date)
          ) fin) final_ where final_.order_date >= cast('2019-09-01' as date) and final_.order_date <=cast('2019-09-30' as date) group by final_.repetition;
            
            
select * from grp_gdoop_bizops_db.nvp_model_var_price2;


create table grp_gdoop_bizops_db.nvp_model_var_price3 stored as orc as
select 
        user_uuid, 
        parent_order_uuid,
        merchant_uuid,
        order_date,
        ROW_NUMBER() over(partition by merchant_uuid, user_uuid order by order_date asc) repetition,
        nob_loc_USD, 
        nor_loc_USD, 
        ogp_loc_usd
     from
       (select 
            f.user_uuid user_uuid,
            f.parent_order_uuid parent_order_uuid,
            f.merchant_uuid merchant_uuid, 
            f.deal_uuid deal_uuid, 
            cast(f.order_date as date) order_date,
            sum(transaction_qty) transaction_qty, 
            sum(f.auth_nob_loc*e.fx_neutral_exchange_rate) nob_loc_USD,
            sum(f.auth_nor_loc*e.fx_neutral_exchange_rate) nor_loc_USD,
            sum(ogp.ogp_loc*e.fx_neutral_exchange_rate) ogp_loc_usd
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
                  transaction_qty
               from 
                  user_edwprod.fact_gbl_transactions 
                  where action = 'authorize' and order_uuid <> '-1' and cast(order_date as date) >= cast('2019-01-01' as date) and cast(order_date as date)<=cast('2019-09-30' as date)
            ) as f
          left join 
            (select 
                 distinct fx_neutral_exchange_rate,
                 currency_from,
                 period_key 
              from 
                 user_groupondw.gbl_fact_exchange_rate where currency_to ='USD'
            ) as e on f.currency_code=e.currency_from  and regexp_replace(substr(f.order_date,0,8),'-','') = e.period_key
          left join 
            (select 
               parent_order_uuid, 
               order_id, 
               ogp_nob_loc,
               ogp_nor_loc,
               ogp_loc, 
               total_estimated_ogp_loc
               from 
               user_edwprod.fact_gbl_ogp_transactions where action = 'authorize' and cast(order_date as date) >= cast('2019-01-01' as date) and cast(order_date as date)<=cast('2019-10-05' as date)
            ) as ogp on f.parent_order_uuid = ogp.parent_order_uuid and f.order_id = ogp.order_id 
            group by f.user_uuid, f.parent_order_uuid, f.merchant_uuid, f.deal_uuid,cast(f.order_date as date)
          ) fin;
