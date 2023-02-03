create table grp_gdoop_bizops_db.nvp_merchant_refund_q stored as orc as
select 
    parent_order_uuid, 
    order_uuid,
    gdl.country_code,
    case when bo.deal_uuid is not null then 1 else 0 end as bookable_order
from user_edwprod.fact_gbl_transactions as a
join user_edwprod.dim_gbl_deal_lob gdl on gdl.deal_id = a.deal_uuid and gdl.grt_l1_cat_name = 'L1 - Local' and country_code <> 'US'
left join 
    (select
          deal_uuid,
          load_date
      from grp_gdoop_bizops_db.sh_bt_active_deals_log
      where product_is_active_flag = 1 and is_bookable = 1 and partner_inactive_flag = 0 
            and cast(load_date as date) >= cast('2019-04-01' as date) 
            and cast(load_date as date) <= cast('2019-06-30' as date)
      group by deal_uuid, load_date) as bo on a.deal_uuid = bo.deal_uuid and a.order_date = bo.load_date
where a.action = 'authorize' 
      and cast(a.order_date as date) >= cast('2019-04-01' as date) 
      and cast(a.order_date as date) <= cast('2019-06-30' as date)
      and a.country_id <> 235
      and a.order_uuid <> '-1'
      ;

create table grp_gdoop_bizops_db.nvp_merchant_refund_q2 stored as orc as
select 
    parent_order_uuid, 
    order_uuid
from user_edwprod.fact_gbl_transactions as a
where a.action = 'refund'
      and a.country_id <> 235
      and a.order_uuid <> '-1'
      group by parent_order_uuid, order_uuid;
      
create table grp_gdoop_bizops_db.nvp_merchant_refund_q3 stored as orc as
select 
   fin.*,
   cast(refunded_orders as double)/total_orders as xyz
   from
(select 
   country_code,
   bookable_order, 
   count(distinct order_uuid) total_orders, 
   count(distinct case when refunded = 1 then order_uuid end) refunded_orders
   from
(select 
   a.*, 
   case when b.order_uuid is not null then 1 else 0 end refunded
   from 
   grp_gdoop_bizops_db.nvp_merchant_refund_q as a 
   left join grp_gdoop_bizops_db.nvp_merchant_refund_q2 as b on a.parent_order_uuid = b.parent_order_uuid and a.order_uuid = b.order_uuid) as fin
   group by country_code, bookable_order
   order by country_code, bookable_order) fin;
   
  
  