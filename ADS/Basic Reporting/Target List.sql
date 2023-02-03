

select
   account_name,
   account_id,
   merchant_uuid,
   permalink,
   dealuuid,
   a.division,
   impressions_t20,
   cvr,
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
   group by 1
   ) m on m.salesforce_account_id = a.account_id
left join dwh_base_sec_view.sf_account sfa on sfa.id = a.account_id
join prod_groupondw.active_deals ad on ad.deal_uuid = a.dealuuid and ad.load_date = date_sub(current_date,2)
where
   has_sl = 0
   and enterprise_flag = 0
   and grt_l1_cat_name = 'L1 - Local'
   and country = 235
   and dnr_reason is null)x
;



