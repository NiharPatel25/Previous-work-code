grp_gdoop_bizops_db.np_sl_ss_list_tableau;
select * from grp_gdoop_bizops_db.avb_aog_sl_l30aggview;

adsnapshot import 



------------------


drop table grp_gdoop_bizops_db.np_target_list_all;
create table grp_gdoop_bizops_db.np_target_list_all stored as orc as 
select
   account_name,
   account_id,
   merchant_uuid,
   permalink,
   dealuuid,
   a.division,
   pds_name,
   grt_l2_cat_name,
   metal_current,
   impressions_t5,
   impressions_t20,
   top3,
   cvr,
   ctr,
   rpc,
   merch_rpc,
   search_imp,
   search_clicks,
   cvr_search,
   rpc_search,
   merch_rpc_search,
   broad_imp,
   broad_clicks,
   cvr_broad,
   rpc_broad,
   merch_rpc_broad,
   cat_imp,
   cat_clicks,
   cvr_cat,
   rpc_cat,
   merch_rpc_cat,
   broad_clicks * 0.50 as grpn_rev_broad,
   cat_clicks * 1.00 as grpn_rev_category,
   search_clicks * 1.00 as grpn_rev_search,
   (broad_clicks * 0.50)+(cat_clicks * 1.00)+(search_clicks * 1.00) as total_ad_revenue,
   merch_rpc_broad * broad_clicks as mx_rev_broad,
   merch_rpc_CAT * CAT_clicks as mx_rev_category,
   merch_rpc_search * search_clicks as mx_rev_search,
   case when broad_clicks = 0 then 0 else ((merch_rpc_broad * broad_clicks) / (broad_clicks * 0.50)) end broad_roas,
   case when cat_clicks = 0 then 0 else ((merch_rpc_CAT * CAT_clicks) / (cat_clicks * 1.00)) end category_roas,
   case when search_clicks = 0 then 0 else ((merch_rpc_search * search_clicks) / (search_clicks * 1.00)) end as search_roas
from grp_gdoop_bizops_db.avb_aog_sl_l30aggview a
join ( -- exclude tpis
   select distinct product_uuid
   from edwprod.dim_offer
   where inventory_service_name <> 'tpis') tpis
   on tpis.product_uuid = a.dealuuid
join (
   select salesforce_account_id, max(feature_country) as country
   from edwprod.dim_merchants_unity
   group by salesforce_account_id
   ) m on m.salesforce_account_id = a.account_id
left join dwh_base_sec_view.sf_account sfa on sfa.id = a.account_id
join prod_groupondw.active_deals ad on ad.deal_uuid = a.dealuuid and ad.load_date = date_sub(current_date,2)
where
   has_sl = 0
   and enterprise_flag = 0
   and grt_l1_cat_name = 'L1 - Local'
   and country = 235
   and dnr_reason is null
;

and grt_l2_cat_name in ('L2 - Things to Do - Leisure', 'L2 - Home & Auto', 'L2 - Health / Beauty / Wellness')





select guid, 
       max(a.id) account_id, 
       max(sfa.name) account_name, 
       max(merchant_segmentation__c) merch_segmentation,
       max(Account_Manager)
     from dwh_base_sec_view.sf_account_2 a
     join dwh_base_sec_view.sf_account sfa on a.id = sfa.id
     group by 1;

account_name ---
current_account_owner
rep_manager
account_id---
permalink---
division---
pds_name
impressions_t5  ----?
impressions_t20 ---
%top3 ---
avg_position2----
ctr---
merchant_uuid---
rpc--
cvr---
merch_rpc---
search_imp
search_clicks
search_cvr
rpc_search
search_clicks
search_cvr
rpc_search
merch_rpc_search----
broad_imps
broad_clicks
broad_cvr
rpc_broad
merch_rpc_broad
cat_imps
cat_clicks
cat_cvr
rpc_cat---
merch_rpc_cat----
metal_current
merchant_permalink
