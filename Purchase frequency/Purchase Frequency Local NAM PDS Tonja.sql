

   
drop table nvp_first_transaction;
create volatile multiset table nvp_first_transaction as (
select 
    user_uuid user_uuid_in, 
    deal_uuid deal_uuid_in,
    order_date order_date_in,
    order_uuid order_uuid_in,
    merchant_uuid merchant_uuid_in, 
    country_code country_code_in,
    grt_l2_cat_name l2_in,
    grt_l3_cat_name l3_in, 
    pds_cat_name pds_cat_name_in
    from 
(select 
      a.*,
      ROW_NUMBER() over (partition by user_uuid order by order_date) rank_heirarchy
   from 
   (select
      fgt.*, 
      gdl.country_code,
      gdl.grt_l2_cat_name,
      gdl.grt_l3_cat_name, 
      pds.pds_cat_name
    from
      user_edwprod.fact_gbl_transactions as fgt
      join user_edwprod.dim_gbl_deal_lob gdl on gdl.deal_id = fgt.deal_uuid and gdl.country_code = 'US' and gdl.grt_l1_cat_name = 'L1 - Local'
      left join user_dw.v_dim_pds_grt_map pds on pds.pds_cat_id = gdl.pds_cat_id
    where 
      order_date >= cast('2020-06-01' AS date)
      AND 
      order_date <= cast('2020-06-30' AS date)
      and
      fgt.action = 'authorize'
      and fgt.order_uuid <> '-1') as a
    ) as fin where rank_heirarchy = 1
) with data no primary index on commit preserve rows;

drop table nvp_first_transaction_stage2;
create volatile multiset table nvp_first_transaction_stage2 as 
(select 
         b.*,
         gdl.country_code country_code,
         gdl.grt_l2_cat_name l2,
         gdl.grt_l3_cat_name l3, 
         pds.pds_cat_name
      from
         user_edwprod.fact_gbl_transactions as b
         join user_edwprod.dim_gbl_deal_lob gdl on gdl.deal_id = b.deal_uuid and gdl.country_code = 'US' and gdl.grt_l1_cat_name = 'L1 - Local'
         left join user_dw.v_dim_pds_grt_map pds on pds.pds_cat_id = gdl.pds_cat_id
      where b."action" = 'authorize' and b.order_date >= cast('2020-06-01' AS date) and b.order_date <cast('2020-12-16' AS date) and b.order_uuid <> '-1') 
with data on commit preserve rows;

drop table sandbox.nvp_first_transaction_stage3;
create multiset table sandbox.nvp_first_transaction_stage3 as (
select 
    a.*,
    fgt.deal_uuid, 
    fgt.order_date, 
    fgt.order_uuid, 
    fgt.merchant_uuid,
    fgt.l2, 
    fgt.l3, 
    fgt.pds_cat_name
from nihpatel.nvp_first_transaction as a 
left join
     nihpatel.nvp_first_transaction_stage2 fgt on fgt.order_date <= a.order_date_in + 120 and fgt.order_date > a.order_date_in and a.user_uuid_in = fgt.user_uuid
group by 
     1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
) with data;



select 
   * 
from sandbox.nvp_first_transaction_stage3;


select 
   count(distinct user_uuid_in) total_users, 
   count(distinct case when num >= 4 then user_uuid_in end) revisiting_users, 
   cast(revisiting_users as float)/total_users
   from
(select 
   user_uuid_in,
   count(distinct case when merchant_uuid_in = merchant_uuid then order_uuid end) num
from sandbox.nvp_first_transaction_stage3
   group by 1) as a;

select 
    sum(case when num >= 4 then num end) num2, 
    sum(dis_order_uuid) dis_order_uuid2, 
    cast(num2 as float)/dis_order_uuid2
    from
(select 
   user_uuid_in,
   count(distinct case when merchant_uuid_in = merchant_uuid then order_uuid end) num, 
   count(distinct order_uuid) dis_order_uuid
from sandbox.nvp_first_transaction_stage3
group by 1) xbz



select order_uuid_in, count(order_uuid) ord, count(distinct order_uuid) ord2, case when ord = ord2 then 1 else 0 end jn 
from sandbox.nvp_first_transaction_stage3 
group by order_uuid_in
having jn = 0
;



select pds_cat_id, count(distinct pds_cat_name) sn from user_dw.v_dim_pds_grt_map group by 1 having sn > 1;


