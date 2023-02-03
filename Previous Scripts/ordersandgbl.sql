/*Table references user_gp.parent_orders (Noram), user_edwprod.parent_orders (intl)*/

SELECT TOP 5 * FROM user_gp.parent_orders;
select top 5 * from user_edwprod.parent_orders;

select top 5 * from user_gp.parent_orders where created_at between CURRENT_DATE - 30 and CURRENT_DATE;
1cf675f8-e43e-11e6-ae9c-a45e60e86e11


select * from (select id, count(*) count_ from user_gp.parent_orders group by id) as f where f.count_ = 2;

CREATE VOLATILE TABLE all_data as 
		(select * from 
			((select
			id as id, 
				uuid as uuid,
				consumer_id as consumer_id,
				purchaser_id as purchaser_id,
				country_code as country_code, 
				guest_order as guest_checkout,
				billing_record_id as billing_record_id,
				created_at as created_at, 
				updated_at as updated_at
			from user_edwprod.parent_orders)
			union ALL
			(select
				id as id, 
				uuid as uuid,
				consumer_id as consumer_id,
				purchaser_id as purchaser_id,
				country_code as country_code, 
				guest_order as guest_checkout,
				billing_record_id as billing_record_id,
				created_at as created_at, 
				updated_at as updated_at
			from user_gp.parent_orders)) as temp 
	) with data on commit PRESERVE rows;

CREATE VOLATILE TABLE Transaction_ as 
(	Select * from user_edwprod.fact_gbl_transactions where "action" = 'authorize'
) with data on commit preserve rows;

select * from user_edwprod.fact_gbl_transactions where "action" = 'authorize';

select top 5 *
from all_data ord
join user_edwprod.fact_gbl_transactions fin
on ord.uuid = fin.parent_order_uuid;

select * from user_edwprod.fact_gbl_transactions where parent_order_uuid = 'cc04fb08-f07c-40a4-8cf2-b100e6b42eaa';
select top 5 * from all_data where uuid = '3c46b33c-3b68-11e3-9e1e-00259069c2f0';

select parent_order_uuid, count(*) count_ from user_edwprod.fact_gbl_transactions  having count_ > 2 group by parent_order_uuid;
