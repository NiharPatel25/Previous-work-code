select max(load_date) from sandbox.sh_bt_active_deals_log;
select max(load_date) from user_groupondw.active_deals;
select * from  user_edwprod.dim_gbl_deal_lob;

(select 
     deal_uuid
     from 
     user_groupondw.active_deals 
     where load_date = current_date - 2
     ) as a 


drop table nvp_temp_inventory_pull;
create volatile table nvp_temp_inventory_pull as 
(select 
  a.deal_uuid, 
  case when b.deal_uuid is not null then 1 else 0 end bookable,
  gdl.grt_l2_cat_name l2,
  gdl.grt_l3_cat_name l3,
  gdl.country_code
from
(select 
     deal_uuid as deal_uuid
     from 
     user_groupondw.active_deals 
     where load_date = current_date - 2
     ) as a 
join 
(select 
     deal_id, 
     grt_l2_cat_name,
     grt_l3_cat_name, 
     country_code
     from 
     user_edwprod.dim_gbl_deal_lob
     where grt_l1_cat_name = 'L1 - Local'
) as gdl on gdl.deal_id = a.deal_uuid
left join 
(select 
     deal_uuid
     from 
     sandbox.sh_bt_active_deals_log
     where load_date = current_date - 2
     and product_is_active_flag = 1 and partner_inactive_flag = 0 and is_bookable = 1
) as b on a.deal_uuid = b.deal_uuid
) with data no primary index on commit preserve rows;


create multiset table sandbox.nvp_temp_inventory_pull as (
select 
   country_code, 
   l2, 
   l3,
   count(distinct deal_uuid) total_deals, 
   count(distinct case when bookable = 1 then deal_uuid end) bookable_deals
   from 
   nvp_temp_inventory_pull
   group by 1,2,3
) with data no primary index;
   
drop table sandbox.nvp_temp_inventory_pull;