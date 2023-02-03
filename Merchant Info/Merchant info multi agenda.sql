







select * from user_groupondw.gbl_fact_exchange_rate limit 5;


CREATE TEMPORARY TABLE conversion_ as 
	select distinct 
		currency_from from_, 
		currency_to to_, 
		fx_neutral_exchange_rate neutral_exg_rate, 
		approved_avg_exchange_rate avg_exchange_rate, 
		period_key per_key 
	 from user_groupondw.gbl_fact_exchange_rate 
	 where currency_to = 'USD';
	
	
select * from conversion_;

drop table conversion_;

CREATE TEMPORARY TABLE Transaction_ as 
	Select 
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
		action as action_,
		is_activation as is_activation,
		transaction_date as transaction_date,
		cast(year(transaction_date) as int)*100 + cast(month(transaction_date) as int) as period_key_
	from user_edwprod.fact_gbl_transactions
	where order_date >= date_sub(CURRENT_DATE, 30) AND order_date < CURRENT_DATE and action = 'authorize' and order_uuid <> '-1'
;



select * from Transaction_;

select T_.merchant_uuid, count(*), sum(T_.nor_loc*conv.avg_exchange_rate) conv_nor
from Transaction_ T_ 
left join conversion_ conv 
on T_.currency_from = conv.from_ and T_.period_key_ = conv.per_key
group by merchant_uuid;



