

select * from grp_gdoop_bizops_db.sh_bt_active_deals_log;


DROP TABLE grp_gdoop_bizops_db.nvp_sr_min_bt_merchant;
create table grp_gdoop_bizops_db.nvp_sr_min_bt_merchant stored as orc as
select 
    e.merchant_uuid, 
    min(mn_load_date) mn_load_date
from (select deal_uuid, min(cast(load_date as date)) mn_load_date 
      from grp_gdoop_bizops_db.sh_bt_active_deals_log
      where product_is_active_flag = 1 and partner_inactive_flag = 0
      group by deal_uuid) a
join 
     (SELECT DISTINCT product_uuid, merchant_uuid FROM user_edwprod.dim_offer_ext) as e on a.deal_uuid = e.product_uuid
group by e.merchant_uuid
;





select 
   a.*, 
   b.mn_load_date
from 
   grp_gdoop_bizops_db.nvp_sr_merchant_onboarding as a
   left join 
   grp_gdoop_bizops_db.nvp_sr_min_bt_merchant as b on a.merchant_uuid = b.merchant_uuid
;


select * from user_groupondw.m_raw_pageview
where eventdate = '2020-09-27'
and consumer_id = '38168f8c-23ed-11e2-86db-00259069d5fe' 
limit 5;