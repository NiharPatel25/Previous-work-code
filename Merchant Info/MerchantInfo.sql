create table sandbox.np_merchant_loc(
need_mapping varchar(5) character set unicode,
agenda_id varchar(5) character set unicode, 
calendar_name varchar(20) character set unicode, 
partner_id varchar(5) character set unicode, 
merchant_uuid varchar(50) character set unicode, 
merchant_name varchar(200) character set unicode, 
location_id varchar(50) character set unicode,
data_ varchar(500) character set unicode,
locality varchar(50) character set unicode,
street_add varchar(400) character set unicode,
country varchar (100) character set unicode
);

select count(*) from sandbox.np_merchant_loc;
drop table sandbox.np_merchant_loc;

drop table Transaction_;

CREATE VOLATILE TABLE Transaction_ as 
(
select a.*, case when b.deal_uuid2 is not null then 1 else 0 end deal_active 
from 
(	Select 
		order_id as order_id,
		order_uuid as order_uuid, 
		parent_order_id as parent_order_id,
		parent_order_uuid as parent_order_uuid,
		country_id as country_id,
		merchant_uuid as merchant_uuid, 
		user_uuid as user_uuid, 
		deal_uuid as deal_uuid,
		auth_nob_loc as nob_loc, 
		auth_nor_loc as nor_loc,
		currency_code as currency_from,
		"action" as action_,
		is_activation as is_activation,
		transaction_date as transaction_date,
		cast(year(transaction_date) as int)*100 + cast(month(transaction_date) as int) as period_key_
	from user_edwprod.fact_gbl_transactions
	where (order_date between CURRENT_DATE - 30 AND CURRENT_DATE) and "action" = 'authorize' and order_uuid <> '-1'
) as a 
left join 
(select 
	x.DEAL_UUID as deal_uuid2,
	x.COUNTRY_CODE as country_code,
	x.load_date as load_date,
	y.country_key as country_id
	FROM user_groupondw.active_deals x
	left join user_groupondw.gbl_dim_country y on x.COUNTRY_CODE = y.country_iso_code_2
	where sold_out = 'false' and available_qty > 0
) as b
on a.deal_uuid = b.deal_uuid2 and a.country_id = b.country_id and a.transaction_date = b.load_date
)
with data on commit preserve rows;




CREATE VOLATILE TABLE conversion_ as 
(select 
	distinct currency_from from_, currency_to to_, fx_neutral_exchange_rate neutral_exg_rate, approved_avg_exchange_rate avg_exchange_rate,period_key per_key 
	from user_groupondw.gbl_fact_exchange_rate 
	where currency_to = 'USD') with data on commit preserve rows;


CREATE VOLATILE TABLE Transaction_ as 
(
select * from 
(select a.*, b.*, case when b.deal_uuid2 is not null then 1 else 0 end deal_active 
from 
    (Select 
		order_id as order_id,
		order_uuid as order_uuid, 
		order_date as order_date,
		parent_order_id as parent_order_id,
		parent_order_uuid as parent_order_uuid,
		country_id as country_id2,
		merchant_uuid as merchant_uuid, 
		user_uuid as user_uuid, 
		deal_uuid as deal_uuid,
		auth_nob_loc as nob_loc, 
		auth_nor_loc as nor_loc,
		currency_code as currency_from,
		"action" as action_,
		is_activation as is_activation,
		transaction_date as transaction_date,
		cast(year(transaction_date) as int)*100 + cast(month(transaction_date) as int) as period_key_
	from user_edwprod.fact_gbl_transactions
	where (order_date between CURRENT_DATE - 30 AND CURRENT_DATE-1) and "action" = 'authorize' and order_uuid <> '-1' and currency_code <> 'CAD'
) as a 
left join 
(select 
	x.DEAL_UUID as deal_uuid2,
	case when x.COUNTRY_CODE = 'GB' THEN 'UK' else x.COUNTRY_CODE end as country_code,
	x.load_date as load_date,
	y.country_key as country_id
	FROM user_groupondw.active_deals x
	left join user_groupondw.gbl_dim_country y on x.COUNTRY_CODE = y.country_iso_code_2
	where sold_out = 'false' and available_qty > 0
) as b on a.deal_uuid = b.deal_uuid2 and a.country_id2 = b.country_id and a.order_date = b.load_date
UNION 
select a2.*, b2.*, case when b2.deal_uuid2 is not null then 1 else 0 end deal_active 
from 
(	Select 
		order_id as order_id,
		order_uuid as order_uuid, 
		order_date as order_date,
		parent_order_id as parent_order_id,
		parent_order_uuid as parent_order_uuid,
		country_id as country_id2,
		merchant_uuid as merchant_uuid, 
		user_uuid as user_uuid, 
		deal_uuid as deal_uuid,
		auth_nob_loc as nob_loc, 
		auth_nor_loc as nor_loc,
		currency_code as currency_from,
		"action" as action_,
		is_activation as is_activation,
		transaction_date as transaction_date,
		cast(year(transaction_date) as int)*100 + cast(month(transaction_date) as int) as period_key_
	from user_edwprod.fact_gbl_transactions
	where (order_date between CURRENT_DATE - 30 AND CURRENT_DATE-1) and "action" = 'authorize' and order_uuid <> '-1' and currency_code = 'CAD'
) as a2 
left join 
(select 
	x.DEAL_UUID as deal_uuid2,
	case when x.COUNTRY_CODE = 'GB' THEN 'UK' else x.COUNTRY_CODE end as country_code,
	x.load_date as load_date,
	y.country_key as country_id
	FROM user_groupondw.active_deals x
	left join user_groupondw.gbl_dim_country y on x.COUNTRY_CODE = y.country_iso_code_2
	where sold_out = 'false' and available_qty > 0
) as b2 on a2.deal_uuid = b2.deal_uuid2 and a2.order_date = b2.load_date
) as uni
)
with data on commit preserve rows;


select T_.merchant_uuid, count(*), sum(T_.nor_loc*conv.avg_exchange_rate) conv_nor
from Transaction_ T_ 
left join conversion_ conv 
on T_.currency_from = conv.from_ and T_.period_key_ = conv.per_key
where T_.deal_active = 1
group by merchant_uuid;



select * from Transaction_;

ee701b9c-29c4-4e8e-adcf-84d619d37da9
fea3cf5c-e91b-11e9-bb51-52548da89807
9c232c2a-17e9-40bd-b874-8ee409807750
2f548cb0-24f2-490c-999a-e2c0d6d0b9d8;


select * from user_groupondw.active_deals where deal_uuid = '9c232c2a-17e9-40bd-b874-8ee409807750';

