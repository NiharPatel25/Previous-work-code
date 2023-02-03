drop table sandbox.np_inv_price_temp1;
create table sandbox.np_inv_price_temp1 as (
select 
           inv_product_uuid, 
           price_value, 
           contract_buy_price/100 contract_buy_price, 
           contract_sell_price/100 contract_sell_price
        from
         (select inv_product_uuid, 
                 updated_ts,
                 ROW_NUMBER() over(partition by inv_product_uuid order by updated_ts desc) row_num, 
                price_value, 
                contract_buy_price, 
                contract_sell_price
           from user_edwprod.dim_deal_inv_product) as f 
           where row_num = 1 
) with data;

create multiset volatile table np_temp_pro_inv as (
select 
    product_uuid, 
    inv_product_uuid, 
    contract_sell_price, 
    contract_buy_price
from user_edwprod.dim_offer_ext
group by 1,2,3,4
) with data on commit preserve rows;

create multiset volatile table np_temp_avg_price as (
select 
        product_uuid, 
        avg(coalesce(a.contract_sell_price, b.contract_sell_price)) contract_sell_price,  
        avg(coalesce(a.contract_buy_price, b.contract_buy_price)) contract_buy_price 
     from np_temp_pro_inv as a 
     left join sandbox.np_inv_price_temp1 as b on a.inv_product_uuid = b.inv_product_uuid
       group by 1
) with data on commit preserve rows;




drop table sandbox.np_ads_vs_roas;
create table sandbox.np_ads_vs_roas as 
(
select 
     supplier_id,
     supplier_name, 
     team_cohort,
     max(l1) l1, 
     max(l2) l2, 
     max(l3) l3, 
     sum(impressions) impressions,
     sum(clicks) clicks,
     sum(conversions) conversions,
     sum(unit_sales) unit_sales,
     sum(sales_revenue) sales_revenue,
     sum(total_adspend) total_adspend,
     sum(groupon_deal_revenue) groupon_deal_revenue, 
     cast(sum(sales_revenue) as float)/NULLIFZERO(sum(total_adspend)) roas,
     sum(overall_impressions) overall_impressions,
     sum(overall_deal_views) overall_deal_views,
     sum(overall_sales_revenue) overall_sales_revenue,
     sum(groupon_overall_deal_revenue) groupon_overall_deal_revenue, 
     avg(contract_sell_price) avg_contract_sell_price, 
     avg(contract_buy_price) avg_contract_buy_price,
     case when roas > 1 then 1 else 0 end roas_more_than_1,
     merchant_uuid
     from
(select a.*,
       c.l1, c.l2, c.l3,
       d.team_cohort,
       b.contract_sell_price , 
       b.contract_buy_price , 
       (b.contract_sell_price - b.contract_buy_price)/NULLIFZERO(b.contract_sell_price) groupon_margin, 
       case when sales_revenue * groupon_margin is null then 0 else sales_revenue * groupon_margin end groupon_deal_revenue,
       overall.total_imps overall_impressions,
       overall.deal_views overall_deal_views,
       overall.nob_usd overall_sales_revenue, 
       case when overall_sales_revenue * groupon_margin is null then 0 else overall_sales_revenue * groupon_margin end groupon_overall_deal_revenue
from 
     (select 
         cast(report_date as date) - EXTRACT(DAY FROM cast(report_date as date)) + 1 report_month, 
         supplier_id,
         supplier_name,
         sku product_code, 
         sum(impressions) impressions,
         sum(clicks) clicks,
         sum(conversions) conversions,
         sum(unitsales) unit_sales,
         sum(sales_revenue) sales_revenue,
         sum(adspend) total_adspend,
         max(merchant_uuid) merchant_uuid
     from sandbox.np_sl_ad_snapshot as a
     left join 
     (       select d.deal_uuid,  
               max(m.acct_owner) account_owner, 
               max(acct_owner_name) acct_owner_name,
               max(contact_full_name) contact_full_name,
               max(m.merchant_uuid) merchant_uuid
        from sandbox.pai_deals d 
        join sandbox.pai_merchants m on d.merchant_uuid = m.merchant_uuid
        group by d.deal_uuid
    ) pai on a.sku = pai.deal_uuid
    left join 
    (     select guid, 
            max(a.id) account_id, 
            max(sfa.name) account_name, 
            max(merchant_segmentation__c) merch_segmentation
     from dwh_base_sec_view.sf_account_2 a
     join dwh_base_sec_view.sf_account sfa on a.id = sfa.id
     group by 1) as g on g.guid = pai.merchant_uuid
     group by 1,2,3,4) as a
left join 
     sandbox.np_sl_deals_imps_trans as overall on a.report_month = cast(overall.report_month as date) and a.product_code = overall.deal_id
left join 
   np_temp_avg_price as b on b.product_uuid = a.product_code
left join 
     (select deal_id, max(grt_l1_cat_name) l1, max(grt_l2_cat_name) l2, max(grt_l3_cat_name) l3  from user_edwprod.dim_gbl_deal_lob group by 1) as c on a.product_code = c.deal_id
left join sandbox.np_sl_supp_mapping as d on a.supplier_name = d.supplier_name
     ) fin group by 1,2,3,22) with data;



drop table sandbox.np_ads_vs_roas_wow;
create table sandbox.np_ads_vs_roas_wow as (
select 
        trunc(cast(report_date as date), 'iw') + 6 report_week,
        a.supplier_name,
        b.team_cohort,
        max(c.l1) l1,
        max(c.l2) l2,
        max(c.l3) l3,
        sum(adspend) adspend,
        sum(sales_revenue) sales_revenue,
        cast(sum(sales_revenue) as float)/NULLIFZERO(sum(adspend)) roas,
        sum(clicks) clicks,
        sum(conversions) conversions,
        cast(sum(conversions) as float)/NULLIFZERO(sum(clicks)) conv_rate
  from sandbox.np_sl_ad_snapshot AS a
  left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name
  left join (select 
                 deal_id, 
                 max(grt_l1_cat_name) l1, 
                 max(grt_l2_cat_name) l2, 
                 max(grt_l3_cat_name) l3  
             from user_edwprod.dim_gbl_deal_lob 
             group by 1) as c on a.sku = c.deal_id
  group by 1,2,3) with data;



drop table sandbox.np_ads_roas_camps;
create table sandbox.np_ads_roas_camps as (
select  
     fin.supplier_id,
     fin.supplier_name, 
     fin.team_cohort,
     fin.l1, 
     fin.l2, 
     fin.l3, 
     fin.impressions,
     fin.clicks,
     fin.conversions,
     fin.unit_sales,
     fin.sales_revenue,
     fin.total_adspend,
     fin.roas,
     fin.avg_contract_sell_price, 
     fin.avg_contract_buy_price,
     fin.roas_more_than_1,
     fin.merchant_uuid,
     camp.max_budget total_budget,
     camp.max_cpc total_max_cpc,
     camp.min_cpc total_min_cpc,
     camp.sum_budget total_sum_budget,
     camp_.max_budget daily_budget,
     camp_.max_cpc daily_max_cpc,
     camp_.min_cpc daily_min_cpc,
     camp_.sum_budget daily_sum_budget,
     camp_2.target_locations
from sandbox.np_ads_vs_roas as fin
left join 
(select 
     pai.merchant_uuid, 
     max(max_cpc) max_cpc, 
     min(max_cpc) min_cpc, 
     max(budget) max_budget,
     sum(budget) sum_budget
from sandbox.np_citrusad_campaigns as a
left join 
     (  select d.deal_uuid,  
               max(m.acct_owner) account_owner, 
               max(acct_owner_name) acct_owner_name,
               max(contact_full_name) contact_full_name,
               max(m.merchant_uuid) merchant_uuid
        from sandbox.pai_deals d 
        join sandbox.pai_merchants m on d.merchant_uuid = m.merchant_uuid
        group by d.deal_uuid
    ) pai on a.product = pai.deal_uuid
where a.spend_type = 'TOTAL'
group by 1
) as camp on fin.merchant_uuid = camp.merchant_uuid
left join 
(select 
     pai.merchant_uuid, 
     max(max_cpc) max_cpc, 
     min(max_cpc) min_cpc, 
     max(budget) max_budget,
     sum(budget) sum_budget
from sandbox.np_citrusad_campaigns as a
left join 
     (       select d.deal_uuid,  
               max(m.acct_owner) account_owner, 
               max(acct_owner_name) acct_owner_name,
               max(contact_full_name) contact_full_name,
               max(m.merchant_uuid) merchant_uuid
        from sandbox.pai_deals d 
        join sandbox.pai_merchants m on d.merchant_uuid = m.merchant_uuid
        group by d.deal_uuid
    ) pai on a.product = pai.deal_uuid
where a.spend_type = 'DAILY'
group by 1
) as camp_ on fin.merchant_uuid = camp_.merchant_uuid
left join 
(select 
     pai.merchant_uuid, 
     max(target_locations) target_locations
from sandbox.np_citrusad_campaigns as a
left join 
     (  select d.deal_uuid,  
               max(m.acct_owner) account_owner, 
               max(acct_owner_name) acct_owner_name,
               max(contact_full_name) contact_full_name,
               max(m.merchant_uuid) merchant_uuid
        from sandbox.pai_deals d 
        join sandbox.pai_merchants m on d.merchant_uuid = m.merchant_uuid
        group by d.deal_uuid
    ) pai on a.product = pai.deal_uuid
where a.spend_type = 'TOTAL'
group by 1
) as camp_2 on fin.merchant_uuid = camp_2.merchant_uuid
) with data;


drop table np_ads_roas_camps_deals;
create volatile table np_ads_roas_camps_deals as 
(select a.*,
       c.l1, c.l2, c.l3,
       d.team_cohort,
       b.contract_sell_price ,
       b.contract_buy_price ,
       (b.contract_sell_price - b.contract_buy_price)/NULLIFZERO(b.contract_sell_price) groupon_margin,
       case when sales_revenue * groupon_margin is null then 0 else sales_revenue * groupon_margin end groupon_deal_revenue,
       overall.total_imps overall_impressions,
       overall.deal_views overall_deal_views,
       overall.parent_orders_qty,
       overall.nob_usd overall_sales_revenue,
       cast(overall.parent_orders_qty as float)/NULLIFZERO(overall.deal_views) conversion_rate,
       case when overall_sales_revenue * groupon_margin is null then 0 else overall_sales_revenue * groupon_margin end groupon_overall_deal_revenue,
       campaign_subtype,
       spend_type,
       budget,
       max_cpc
from 
     (select 
         cast(report_date as date) - EXTRACT(DAY FROM cast(report_date as date)) + 1 report_month, 
         supplier_id,
         supplier_name,
         sku product_code, 
         campaign_id,
         sum(impressions) impressions,
         sum(clicks) clicks,
         sum(conversions) conversions,
         sum(unitsales) unit_sales,
         sum(sales_revenue) sales_revenue,
         sum(adspend) total_adspend,
         max(merchant_uuid) merchant_uuid
     from sandbox.np_sl_ad_snapshot as a
     left join 
     (       select d.deal_uuid,  
               max(m.acct_owner) account_owner, 
               max(acct_owner_name) acct_owner_name,
               max(contact_full_name) contact_full_name,
               max(m.merchant_uuid) merchant_uuid
        from sandbox.pai_deals d 
        join sandbox.pai_merchants m on d.merchant_uuid = m.merchant_uuid
        group by d.deal_uuid
    ) pai on a.sku = pai.deal_uuid
    left join 
    (     select guid, 
            max(a.id) account_id, 
            max(sfa.name) account_name, 
            max(merchant_segmentation__c) merch_segmentation
     from dwh_base_sec_view.sf_account_2 a
     join dwh_base_sec_view.sf_account sfa on a.id = sfa.id
     group by 1) as g on g.guid = pai.merchant_uuid
     group by 1,2,3,4,5) as a
left join 
     sandbox.np_sl_deals_imps_trans as overall on a.report_month = cast(overall.report_month as date) and a.product_code = overall.deal_id
left join 
   np_temp_avg_price as b on b.product_uuid = a.product_code
left join 
     (select deal_id, max(grt_l1_cat_name) l1, max(grt_l2_cat_name) l2, max(grt_l3_cat_name) l3  from user_edwprod.dim_gbl_deal_lob group by 1) as c on a.product_code = c.deal_id
left join sandbox.np_sl_supp_mapping as d on a.supplier_name = d.supplier_name
left join 
     sandbox.np_citrusad_campaigns as camps on a.campaign_id = camps.citrusad_campaign_id
 ) with data on commit preserve rows;



select * from np_ads_roas_camps_deals;

select 
    target_locations,
    count(distinct citrusad_campaign_id) max_campaign_locations
from sandbox.np_citrusad_campaigns
group by 1
order by 2 desc;

select 
    campaign_subtype, 
    count(distinct citrusad_campaign_id)
from sandbox.np_citrusad_campaigns
group by 1
order by 2 desc;


select 
     min(cpc), 
     max(cpc),
     min(conversion_rate),
     max(conversion_rate),
     min(avg_contract_sell_price),
     max(avg_contract_sell_price)
from 
(select 
       a.*, 
       cast(total_adspend as float)/NULLIFZERO(clicks) cpc,
       cast(conversions as float)/NULLIFZERO(clicks) conversion_rate
from sandbox.np_ads_vs_roas as a) xyz;


select 
       a.*, 
       cast(total_adspend as float)/NULLIFZERO(clicks) cpc,
       cast(conversions as float)/NULLIFZERO(clicks) conversion_rate
from sandbox.np_ads_vs_roas as a
order by conversion_rate desc;




select 
      cast(generated_datetime as date) generated_date,
      trunc(generated_datetime, 'iw') + 6 week_date,
      merchant_id, 
      deal_id,
      account_owner,
      acct_owner_name,
      contact_full_name,
      account_name,
      sum(impressioned) impressions,
      sum(clicked) clicks, 
      sum(impression_spend_amount) impressions_spend_amount, 
      sum(total_spend_amount) total_spend_amount
from user_gp.ads_reconciled_report a 
left join 
(       select d.deal_uuid,  
               max(m.acct_owner) account_owner, 
               max(acct_owner_name) acct_owner_name,
               max(contact_full_name) contact_full_name,
               max(m.merchant_uuid) merchant_uuid
        from sandbox.pai_deals d 
        join sandbox.pai_merchants m on d.merchant_uuid = m.merchant_uuid
        group by d.deal_uuid
    ) sf on a.deal_id = sf.deal_uuid
left join 
    (     select guid, 
            max(a.id) account_id, 
            max(sfa.name) account_name, 
            max(merchant_segmentation__c) merch_segmentation
     from dwh_base_sec_view.sf_account_2 a
     join dwh_base_sec_view.sf_account sfa on a.id = sfa.id
     group by 1) as g on g.guid = a.merchant_id
where a.generated_date = '2022-01-05'
group by 1,2,3,4,5,6,7,8 


-----adspend bucket and then their correlation with adsrevenue


