
CREATE MULTISET TABLE sandbox.np_citrus_sl_bid2 ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      report_date VARCHAR(50) CHARACTER SET UNICODE,
      quarter VARCHAR(5) CHARACTER SET UNICODE,
      report_month integer,
      report_year integer,
      deal_id VARCHAR(100) CHARACTER SET UNICODE, 
      page_type VARCHAR(20) CHARACTER SET UNICODE, 
      bid_join VARCHAR(200) CHARACTER SET UNICODE, 
      platform VARCHAR(20) CHARACTER SET UNICODE, 
      country_code VARCHAR(5) CHARACTER SET UNICODE,
      total_cpc decimal(7,2), 
      citrus_impressions integer, 
      citrus_clicks integer, 
      bid_join_final VARCHAR(200) CHARACTER SET UNICODE, 
      type_based_category VARCHAR(20) CHARACTER SET UNICODE,
      l1 VARCHAR(20) CHARACTER SET UNICODE,
      l2 VARCHAR(20) CHARACTER SET UNICODE,
      l3 VARCHAR(20) CHARACTER SET UNICODE
      )
NO PRIMARY INDEX;


select cast(created_at as date), count(*) from user_gp.ads_rcncld_intrmdt_rpt group by 1 order by 1 desc

--------------NUMBER OF NEW ACTIVE MERCHANTS
select distinct consumeridsource from sandbox.np_ss_sl_interaction;


select 
   a.*, 
   trunc(created_date, 'iw') +6 week_date
   from
(select 
    a.merchant_id
    id, 
    a.merchant_name,
    m.account_id,
    min(cast(substr(create_datetime, 1,10) as date)) created_date
from sandbox.np_sponsored_campaign as a
     left join sandbox.pai_merchants as m on a.merchant_id = m.merchant_uuid
     where a.status not in ('DRAFT')
group by 1,2,3
) as a;


CREATE MULTISET TABLE sandbox.citrusad_team_wallet ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO, 
     MAP = TD_MAP1
     (
      wallet_id VARCHAR(50) CHARACTER SET UNICODE,
      merchant_id VARCHAR(50) CHARACTER SET UNICODE,
      balance decimal(9,4), 
      create_datetime VARCHAR(50) CHARACTER SET UNICODE,
      update_datetime VARCHAR(50) CHARACTER SET UNICODE,
      is_archived integer,
      export_datetime VARCHAR(50) CHARACTER SET UNICODE, 
      citrusad_import_date VARCHAR(50) CHARACTER SET UNICODE,
      is_self_serve integer
      )
NO PRIMARY INDEX;

-----------------------------------------------------------ALL MERCHANT CENTER ONBOARDS

   
create volatile table np_merch_catlist as
(select merchant_uuid, 
       max(account_name) account_name,
       max(case when list_category = '300' then 1 else 0 end) free_cred_300, 
       max(case when list_category = '100' then 1 else 0 end) free_cred_100, 
       max(case when list_category = 'handraiser' then 1 else 0 end) handraiser
   from  
       sandbox.np_ss_sl_merch_list
   group by 1) with data on commit preserve rows;
  


create volatile table np_ss_merch_onboarded as (
select 
    merch.*, 
    trunc(created_date, 'iw') + 6 week_date,
    a.freecredit, 
    b.free_cred_300,
    b.free_cred_100, 
    b.handraiser,
    c.target_month,
    d.total_impressions,
    d.total_clicks, 
    d.total_orders, 
    d.total_adspend,
    d.orders_rev,
    d.roas,
    e.number_of_top_ups,
    e.amount_of_top_ups, 
    f.supplier_id, 
    f.supplier_name
from
(select 
    a.merchant_id,
    a.merchant_name,
    m.account_id,
    min(cast(substr(create_datetime, 1,10) as date)) created_date
from sandbox.np_sponsored_campaign as a
     left join sandbox.pai_merchants as m on a.merchant_id = m.merchant_uuid
     where a.status not in ('DRAFT')
group by 1,2,3) as merch
left join sandbox.np_merch_free_credits as a on a.merchant_uuid = merch.merchant_id
left join np_merch_catlist as b on merch.merchant_id = b.merchant_uuid
left join 
     (select a.*, 
           max(COALESCE(live_merchant, was_live_merchant)) merchant_uuid
     from sandbox.np_ss_sl_mnthly_trgtlist as a 
     left join 
          (select 
             account_id, 
             max(case when is_live = 1 then merchant_uuid end) live_merchant, 
             max(case when was_live = 1 then merchant_uuid end) was_live_merchant 
             from sandbox.pai_merchants
             group by 1) as b on a.account_id = b.account_id
      group by 1,2,3,4) as c on merch.merchant_id = c.merchant_uuid
left join 
	   (SELECT 
		      b.merchant_uuid,
		      sum(impressions) total_impressions,
		      sum(clicks) total_clicks,
		      sum(conversions) total_orders,
		      sum(total_spend_amount) total_adspend,
		      sum(price_with_discount) orders_rev,
		      cast(orders_rev as float)/NULLIFZERO(total_adspend) roas
		FROM sandbox.np_ss_performance_met as a
		left join sandbox.pai_deals as b on a.deal_id = b.deal_uuid
		group by 1) as d on merch.merchant_id = d.merchant_uuid
left join 
		(select 
           merchant_id, 
           count(1) number_of_top_ups, 
           sum(amount) amount_of_top_ups
          from sandbox.np_merchant_topup_orders as a
           where event_type='CAPTURE' and event_status='SUCCESS'
           group by 1) as e on merch.merchant_id = e.merchant_id
left join 
  (select 
       merchant_uuid, 
       max(supplier_id) supplier_id, 
       max(supplier_name) supplier_name
  from 
  (select 
    distinct
    a.sku, 
    a.supplier_id, 
    a.supplier_name,
    sf.merchant_uuid, 
    sf.account_id
    from sandbox.np_sl_ad_snapshot a 
    left join 
       (select d.deal_uuid, 
             max(d.l1) l1,
             max(d.l2) l2,
             max(d.l3) l3,
             max(m.merchant_uuid) merchant_uuid,
             max(m.account_id) account_id, 
             max(m.acct_owner) account_owner, 
             max(acct_owner_name) acct_owner_name,
             max(sfa.name) account_name, 
             max(merchant_segmentation__c) merch_segmentation
        from sandbox.pai_deals d 
        join sandbox.pai_merchants m on d.merchant_uuid = m.merchant_uuid
        join dwh_base_sec_view.sf_account sfa on m.account_id = sfa.id
        group by d.deal_uuid
    ) sf on a.sku = sf.deal_uuid
    ) as mg
    group by 1
    ) as f on merch.merchant_id = f.merchant_uuid) with data on commit preserve rows;

   
select 
    week_date, 
    count(1) total_merchants, 
    sum(stumbled) stumbled
from 
(select a.*, 
      case when a.free_cred_300 is null and free_cred_100 is null and handraiser is null and target_month is null then 1 else 0 end stumbled 
from np_ss_merch_onboarded as a
order by week_date desc) as fin 
group by 1
order by week_date desc
;
   


           
select * from sandbox.pai_merchants where is_live = 1;
           
   
select 
     a.*, 
     b.*
from np_ss_merch_onboarded as a 
left join 
   (select merchantid, min(eventdate) eventdate_min from sandbox.np_ss_sl_interaction_agg group by 1) as b on a.merchant_id = b.merchantid
   order by a.week_date
;




select 
     *
from np_ss_merch_onboarded where merchant_uuid = '4af7938c-cef3-11e3-8cea-002590922cb4';

select distinct eventdate
from sandbox.np_ss_sl_interaction_agg
order by 1
;

--------------NUMBER OF CAMPAIGNS CREATED-----AND PAUSED CAMPAIGN REASON

-----One campaign can only have one status
/*select campaign_name, count(distinct status) cnz 
from sandbox.np_sponsored_campaign group by 1 having cnz >1;*/


select 
   fin.*, 
   trunc(created_date, 'iw') +6 camp_week_date, 
   trunc(update_date, 'iw') + 6 update_week_date, 
   case when cast(update_date as date) < cast(last_live_date as date) then 1 else 0 end left_before_leaving_groupon
from 
(select 
    a.merchant_id, 
    a.merchant_name,
    m.account_id,
    a.campaign_name,
    min(cast(substr(create_datetime, 1,10) as date)) created_date, 
    max(cast(substr(update_datetime, 1,10) as date)) update_date,
    max(status) campaign_status, 
    max(status_change_reason) status_change_reason, 
    max(last_live_date) last_live_date, 
    max(is_live) is_live_date
from sandbox.np_sponsored_campaign as a
     left join sandbox.pai_merchants as m on a.merchant_id = m.merchant_uuid
    where a.status not in ('DRAFT')
group by 1,2,3,4) fin 
;


create multiset volatile table np_temp_paused as
(select 
   fin.*, 
   trunc(created_date, 'iw') +6 camp_week_date, 
   trunc(update_date, 'iw') + 6 update_week_date, 
   case when cast(update_date as date) < cast(last_live_date as date) then 1 else 0 end left_before_leaving_groupon
from 
(select 
    a.merchant_id, 
    a.merchant_name,
    m.account_id,
    a.campaign_name,
    min(cast(substr(create_datetime, 1,10) as date)) created_date, 
    max(cast(substr(update_datetime, 1,10) as date)) update_date,
    max(status) campaign_status, 
    max(status_change_reason) status_change_reason, 
    max(last_live_date) last_live_date, 
    max(is_live) is_live_date
from sandbox.np_sponsored_campaign as a
     left join sandbox.pai_merchants as m on a.merchant_id = m.merchant_uuid
    where a.status not in ('DRAFT')
group by 1,2,3,4) fin ) with data on commit preserve rows;

create volatile table np_temp_merch_perf as 
(SELECT 
      merchant_id,
      sum(impressions) total_imps, 
      sum(clicks) total_clicks, 
      sum(conversions) total_ords, 
      sum(total_spend_amount) adspend,
      sum(price_with_discount) orders_rev, 
      cast(total_clicks as float)/NULLIFZERO(total_imps) ss_ctr, 
      cast(total_ords as float)/NULLIFZERO(total_clicks) ss_cnv, 
      cast(orders_rev as float)/NULLIFZERO(adspend) roas,
      cast(adspend as float)/NULLIFZERO(total_clicks) CPC
FROM sandbox.np_ss_performance_met
group by 1) with data on commit preserve rows
;


select 
    a.*,
    b.roas, 
    b.adspend
    from
(select * from np_temp_paused 
   where campaign_status = 'PAUSED' ) as a 
left join np_temp_merch_perf as b on a.merchant_id = b.merchant_id
where status_change_reason = "It's only temporary  I will resume sponsoring later."
ORDER BY update_week_date desc
     ,status_change_reason;

-------------------------PAUSED CAMPAIGN REASONS

select 
    trunc(cast(substr(create_datetime,1,10) as date), 'iw') +6 week_date, 
    feedback_context, 
    reasons, 
    count(merchant_id) merchants, 
    count(distinct merchant_id) distinct_merch_count
from sandbox.np_ss_feedback
group by 1,2,3


--------------SC Merchant 

SELECT * FROM sandbox.np_sponsored_merchants where is_active = 1;


--------------NUMBER OF WALLET TOP UPS AND AMOUNT TOPPED UP



select 
       trunc(cast(substr(create_datetime, 1,10) as date), 'iw') + 6 create_date,
       a.*
from sandbox.np_merchant_topup_orders as a
where event_type='CAPTURE' and event_status='SUCCESS'
;


select * from sandbox.np_merchant_topup_orders;

select 
    account_id, 
    s1.merchant_id,
    sum(amount) as reup_total,
    count(amount) as reup_count,
    min(event_time) as first_mx_card_reup,
    max(event_time) as last_mx_card_reup
    from sandbox.np_merchant_topup_orders as s1
    left join sandbox.pai_merchants as s2
    on s1.merchant_id = s2.merchant_uuid
    where event_type='CAPTURE' 
    and event_status='SUCCESS'
   and account_id = '0013c00001zFkLpAAK'
    group by 1,2;
   
select 
* from 
sandbox.pai_merchants
where account_id = '0013c00001zFkLpAAK';

select guid,
max(a.id) account_id,
max(sfa.name) account_name,
max(merchant_segmentation__c) merch_segmentation
from dwh_base_sec_view.sf_account_2 a
join dwh_base_sec_view.sf_account sfa on a.id = sfa.id
where guid = 'e11b60e9-ac63-464c-b9fd-512287492c7c'
group by 1;


CREATE MULTISET TABLE sandbox.np_ss_feedback ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      id integer,
      merchant_id VARCHAR(64) CHARACTER SET UNICODE,
      feedback_context VARCHAR(64) CHARACTER SET UNICODE,
      context_id VARCHAR(64) CHARACTER SET UNICODE,
      reasons VARCHAR(64) CHARACTER SET UNICODE,
      create_datetime	VARCHAR(50) CHARACTER SET UNICODE
      )
NO PRIMARY INDEX;
grp_gdoop_bizops_db.np_mc_sssl_tracking
--------------REFUNDS

select trunc(cast(substr(create_datetime, 1,10) as date), 'iw') + 6 create_date, a.*  
from sandbox.np_refund_request_orders as a
where status='SUCCEEDED';



select * from sandbox.np_refund_request_orders;
--------------ALL METRICS

SELECT 
      trunc(cast(report_date as date), 'iw')+6 report_week, 
      report_date,
      sum(impressions) total_imps, 
      sum(clicks) total_clicks, 
      sum(conversions) total_ords, 
      sum(total_spend_amount) adspend,
      sum(price_with_discount) orders_rev, 
      cast(total_clicks as float)/NULLIFZERO(total_imps) ss_ctr, 
      cast(total_ords as float)/NULLIFZERO(total_clicks) ss_cnv, 
      cast(orders_rev as float)/NULLIFZERO(adspend) roas,
      cast(adspend as float)/NULLIFZERO(total_clicks) CPC
FROM sandbox.np_ss_performance_met
group by 1,2
order by 1,2;


left join sandbox.pai_deals as b on a.deal_id = b.deal_uuid

select * FROM sandbox.np_ss_performance_met;

create volatile table np_ss_sl_deal as 
(SELECT 
      trunc(cast(report_date as date), 'iw')+6 report_week, 
      deal_id,
      sum(impressions) total_imps, 
      sum(clicks) total_clicks, 
      sum(conversions) total_ords, 
      sum(total_spend_amount) adspend,
      sum(price_with_discount) orders_rev, 
      cast(total_clicks as float)/NULLIFZERO(total_imps) ss_ctr, 
      cast(total_ords as float)/NULLIFZERO(total_clicks) ss_cnv, 
      cast(orders_rev as float)/NULLIFZERO(adspend) roas
FROM sandbox.np_ss_performance_met
group by 1,2
) with data on commit preserve rows;

select report_week, 
       count(deal_id), 
       sum(case when adspend > 1 then 1 else 0 end) has_adspend,
       sum(case when adspend > 100 then 1 else 0 end) has_adspendhundred,
       sum(case when roas > 1 then 1 else 0 end), sum(case when roas > 1.5 then 1 else 0 end) from np_ss_sl_deal group by 1 order by 1;



select 
   a.deal_id, 
   a.roas, 
   b.roas,
   b.roas-a.roas roas_diff
from (select * from np_ss_sl_deal where report_week = '2021-08-29') as a
left join (select * from np_ss_sl_deal where report_week = '2021-10-03') as b on a.deal_id = b.deal_id
order by 4 asc;




SELECT 
      deal_id,
      sum(impressions) total_imps, 
      sum(clicks) total_clicks, 
      sum(conversions) total_ords, 
      sum(total_spend_amount) adspend,
      sum(price_with_discount) orders_rev, 
      cast(total_clicks as float)/NULLIFZERO(total_imps) ss_ctr, 
      cast(total_ords as float)/NULLIFZERO(total_clicks) ss_cnv, 
      cast(orders_rev as float)/NULLIFZERO(adspend) roas
FROM sandbox.np_ss_performance_met
where deal_id = 'c429c8aa-b166-4798-bf59-20a2e670b0f2'
and cast(report_date as date) >= cast('2021-08-01' as date) and cast(report_date as date) <= cast('2021-08-30' as date)
group by 1;




select 
    a.deal_id, 
    case when b.deal_id is not null then 1 else 0 end new_deal, 
    a.adspend,
    a.roas
from (select * from np_ss_sl_deal where report_week = '2021-10-03') as a
left join (select * from np_ss_sl_deal where report_week = '2021-08-29') as b on a.deal_id = b.deal_id
order by 2 desc, 4 desc


--------------CPC

select * from sandbox.np_citrusad_campaigns;

-------------------------------------------------SEARCH LEVEL COMPARISON OF CPC


select distinct type_based_category from sandbox.np_citrus_sl_bid2;

select * from sandbox.sc_ads_cost_metrics where values_of like '%laser';

select a.* from sandbox.np_citrus_sl_bid2 AS a;

create volatile table np_team_search_term as 
(SELECT 
    a.report_year,
    a.quarter, 
    a.page_type, 
    a.l1, a.l2, a.l3,
    a.type_based_category,
    b.team_cohort, 
    sum(a.total_cpc) total_adspend,
    sum(a.citrus_clicks) clicks, 
    sum(a.citrus_impressions) impressions,
    total_adspend/NULLIFZERO(clicks) cpc
from 
   sandbox.np_citrus_sl_bid2 as a 
left join 
np_mapping_value as b on a.deal_id = b.sku
where quarter in ('Q4', 'Q1') and report_year in (2021, 2022)
group by 1,2,3,4,5,6,7,8) with data on commit preserve rows
;

select 
    a.type_based_category, 
    a.total_adspend ent_ads, 
    a.total_clicks ent_clicks, 
    cast(a.total_adspend as float)/a.total_clicks ent_cpc,
    b.total_adspend ss_ads, 
    b.total_clicks ss_clicks,
    cast(b.total_adspend as float)/b.total_clicks ss_cpc,
    c.total_adspend good_ads,
    c.total_clicks good_clicks,
    cast(c.total_adspend as float)/c.total_clicks goods_cpc, 
    a.total_adspend + b.total_adspend + c.total_adspend total_adspend,
    ss_cpc/ent_cpc-1 greater_than_ent,
    ss_cpc/goods_cpc-1 greater_than_goods, 
    case when greater_than_ent > 0 then 1 else 0 end greater_than_ent_case, 
    case when greater_than_goods > 0 then 1 else 0 end greater_than_goods_case
from 
     (select type_based_category, 
             sum(total_adspend) total_adspend, 
             sum(clicks) total_clicks
      from np_team_search_term 
      where team_cohort = 'Enterprise' and page_type = 'Search'
      having total_clicks > 0
      group by 1) as a 
join (select type_based_category, 
             sum(total_adspend) total_adspend, 
             sum(clicks) total_clicks
      from np_team_search_term where team_cohort = 'SelfServe' and page_type = 'Search'
      having total_clicks > 0
      group by 1)  as b on a.type_based_category = b.type_based_category 
join (select type_based_category, 
             sum(total_adspend) total_adspend, 
             sum(clicks) total_clicks
      from np_team_search_term 
      where team_cohort = 'Coupons' and page_type = 'Search'
      having total_clicks > 0
      group by 1) as c on a.type_based_category = c.type_based_category


select 
    a.type_based_category, 
    a.total_adspend ent_ads, 
    a.total_clicks ent_clicks, 
    cast(a.total_adspend as float)/a.total_clicks ent_cpc,
    b.total_adspend ss_ads, 
    b.total_clicks ss_clicks,
    cast(b.total_adspend as float)/b.total_clicks ss_cpc,
    ss_cpc/ent_cpc-1 greater_than_ent,
    case when greater_than_ent > 0 then 1 else 0 end greater_than_ent_case
from 
     (select type_based_category, 
             sum(total_adspend) total_adspend, 
             sum(clicks) total_clicks
      from np_team_search_term 
      where team_cohort = 'Enterprise' and page_type = 'Search'
      having total_clicks > 0
      group by 1) as a 
join (select type_based_category, 
             sum(total_adspend) total_adspend, 
             sum(clicks) total_clicks
      from np_team_search_term where team_cohort = 'SelfServe' and page_type = 'Search'
      having total_clicks > 0
      group by 1)  as b on a.type_based_category = b.type_based_category 

create volatile multiset table np_mapping_value as (
select 
    fin.*, 
    max(fin2.supplier_name) supplier_name
from 
(select 
   a.sku, 
   max(rank_of_cat) rank_of_cat_max,
   case when rank_of_cat_max = 6 then 'Enterprise'
           when rank_of_cat_max = 5 then 'Coupons'
           when rank_of_cat_max = 4 then 'Goods'
           when rank_of_cat_max = 3 then 'SelfServe'
           when rank_of_cat_max = 2 then 'STIAB'
           when rank_of_cat_max = 1 then 'Test'
           end team_cohort
from 
(select 
     sku,
     a.supplier_name,
     b.team_cohort, 
     case when b.team_cohort = 'Enterprise' then 6 
           when b.team_cohort = 'Coupons' then 5
           when b.team_cohort = 'Goods' then 4
           when b.team_cohort = 'SelfServe' then 3
           when b.team_cohort = 'STIAB' then 2 
           when b.team_cohort = 'Test' then 1 
           end rank_of_cat
from sandbox.np_sl_ad_snapshot as a 
left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name
group by 1,2,3
) as a
group by 1) as fin  
left join 
(select 
     sku,
     a.supplier_name,
     b.team_cohort, 
     case when b.team_cohort = 'Enterprise' then 6 
           when b.team_cohort = 'Coupons' then 5
           when b.team_cohort = 'Goods' then 4
           when b.team_cohort = 'SelfServe' then 3
           when b.team_cohort = 'STIAB' then 2 
           when b.team_cohort = 'Test' then 1 
           end rank_of_cat
from sandbox.np_sl_ad_snapshot as a 
left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name
group by 1,2,3
) as fin2 on fin.sku = fin2.sku and fin2.rank_of_cat = fin.rank_of_cat_max
group by 1,2,3) with data on commit preserve rows
;



------------------------------------------ABOVE BID FLOOR
select * from sandbox.np_citrusad_campaigns;
grant select on sandbox.np_citrusad_campaigns
 

select 
*
from 
(select 
    merchant_uuid, 
    sum(case when spend_type = 'DAILY' then budget*30
         when spend_type = 'TOTAL' then budget 
         end) budget
from sandbox.np_citrusad_campaigns as a
left join sandbox.pai_deals as b on a.product = b.deal_uuid
where a.status = 'ACTIVE'
group by 1
) as fin;


select 
    product, 
    sum(case when spend_type = 'DAILY' then budget*30
         when spend_type = 'TOTAL' then budget 
         end) budget
from sandbox.np_citrusad_campaigns as a
left join sandbox.pai_deals as b on a.product = b.deal_uuid
where a.status = 'ACTIVE'
group by 1;



drop table np_cpc_spend;
CREATE VOLATILE MULTISET TABLE np_cpc_spend as (
SELECT * FROM 
(select 
    a.*,
    b.total_adspend,
    b.all_rev,
    b.cpc_actual,
    case when cpc_actual > max_cpc * 1.25 then 1 else 0 end cpc_greater_twentyfive,
    case when cpc_actual > max_cpc * 1.1 then 1 else 0 end cpc_greater_ten,
    case when cpc_actual > max_cpc then 1 else 0 end cpc_greater,
    case when cpc_actual = max_cpc then 1 else 0 end cpc_equal,
    case when cpc_actual < max_cpc then 1 else 0 end cpc_less, 
    case when total_adspend > 0 then 1 else 0 end with_adspend, 
    case when total_adspend > 50 then 1 else 0 end adspend_more_fifty, 
    case when ROAS > 1 then 1 else 0 end camp_roas_greater_than_one
from sandbox.np_citrusad_campaigns as a 
left join 
    (select 
         campaign_id,
         sum(impressions) impressions,
         sum(clicks) total_clicks,
         sum(conversions) conversions,
         sum(unitsales) unit_sales,
         sum(sales_revenue) all_rev,
         sum(adspend) total_adspend, 
         cast(total_adspend as float)/NULLIFZERO(total_clicks) cpc_actual,
         cast(all_rev as float)/NULLIFZERO(total_adspend) ROAS
     from 
     sandbox.np_sl_ad_snapshot 
     group by 1
    )as b on a.citrusad_campaign_id = b.campaign_id
where a.status not in ('DRAFT')
) as fin
) with data on commit preserve rows;




select 
   count(distinct citrusad_campaign_id) campaigns, 
   sum(with_adspend) with_adspend_camp, 
   sum(adspend_more_fifty) fifty_more_camp, 
   sum(cpc_greater_twentyfive) cpc_greater_twentyfive,
   sum(cpc_greater_ten) cpc_greater_ten, 
   sum(cpc_greater) cpc_greater, 
   sum(cpc_equal) cpc_equal, 
   sum(cpc_less) cpc_less
from 
np_cpc_spend
;



select * from sandbox.np_sc_deal_search_terms;
select 
    status, 
    count(distinct citrusad_campaign_id) campaigns, 
    count(1)
from sandbox.np_citrusad_campaigns
group by 1
;
select * from sandbox.np_sc_deal_search_terms;

-----------------------------------PAYING HIGHER THAN BID COMBINED WITH CAMPAIGN

create volatile multiset table np_mapping_value as (
select 
    fin.*, 
    max(fin2.supplier_name) supplier_name
from 
(select 
   a.sku, 
   max(rank_of_cat) rank_of_cat_max,
   case when rank_of_cat_max = 6 then 'Enterprise'
           when rank_of_cat_max = 5 then 'Coupons'
           when rank_of_cat_max = 4 then 'Goods'
           when rank_of_cat_max = 3 then 'SelfServe'
           when rank_of_cat_max = 2 then 'STIAB'
           when rank_of_cat_max = 1 then 'Test'
           end team_cohort
from 
(select 
     sku,
     a.supplier_name,
     b.team_cohort, 
     case when b.team_cohort = 'Enterprise' then 6 
           when b.team_cohort = 'Coupons' then 5
           when b.team_cohort = 'Goods' then 4
           when b.team_cohort = 'SelfServe' then 3
           when b.team_cohort = 'STIAB' then 2 
           when b.team_cohort = 'Test' then 1 
           end rank_of_cat
from sandbox.np_sl_ad_snapshot as a 
left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name
group by 1,2,3
) as a
group by 1) as fin  
left join 
(select 
     sku,
     a.supplier_name,
     b.team_cohort, 
     case when b.team_cohort = 'Enterprise' then 6 
           when b.team_cohort = 'Coupons' then 5
           when b.team_cohort = 'Goods' then 4
           when b.team_cohort = 'SelfServe' then 3
           when b.team_cohort = 'STIAB' then 2 
           when b.team_cohort = 'Test' then 1 
           end rank_of_cat
from sandbox.np_sl_ad_snapshot as a 
left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name
group by 1,2,3
) as fin2 on fin.sku = fin2.sku and fin2.rank_of_cat = fin.rank_of_cat_max
group by 1,2,3) with data on commit preserve rows
;


create multiset volatile table np_citrus_temp as (
select 
    deal_id, 
    type_based_category, 
    l1,
    l2,
    l3, 
    sum(total_cpc) adspend, 
    sum(citrus_clicks) total_clicks, 
    sum(citrus_impressions) total_impressions,
    cast(adspend as float)/NULLIFZERO(total_clicks) cpc_paid, 
    case when c.cpc is not null then 1 else 0 end cpc_available, 
    case when c.cpc is not null then c.cpc else 0.5 end min_cpc_value
from sandbox.np_citrus_sl_bid2 as a 
join np_mapping_value as b on a.deal_id = b.sku 
left join (select * from sandbox.sc_ads_cost_metrics where type_of = 'search_query') as c on lower(a.type_based_category) = lower(c.values_of)
where quarter in ('Q4', 'Q1') 
     and citrus_clicks > 0 
     and report_year in (2021, 2022)
     and page_type =  'Search'
     and b.team_cohort = 'SelfServe'
group by 1,2,3,4,5,10,11
) with data on commit preserve rows
;


create volatile multiset table np_int2 as (
WITH cte (product, search_terms) AS
 (
   SELECT product, search_terms FROM sandbox.np_citrusad_campaigns where campaign_subtype = 'SEARCH_ONLY'
 )
SELECT * 
FROM TABLE
 ( STRTOK_SPLIT_TO_TABLE( cte.product, search_terms, ',') 
   RETURNS ( product varchar(64)  , TokenNum INT , Token VARCHAR (256) CHARACTER SET UNICODE ) 
 ) dt
 ) with data on commit preserve rows
 ;


create multiset volatile table np_deal_min_bid2 as (
select 
    a.product deal_id, 
    count(distinct a.Token) search_terms_dist, 
    min(b.cpc) min_cpc
from np_int2 as a 
left join (select * from sandbox.sc_ads_cost_metrics where type_of = 'search_query') as b on lower(a.Token) = lower(b.values_of)
group by 1
) with data on commit preserve rows
;

create multiset volatile table np_deal_min_bid3 as (
select 
    a.product deal_id, 
    a.Token search_terms_dist,
    case when b.cpc is not null then b.cpc else 0.5 end b.cpc
from np_int2 as a 
left join (select * from sandbox.sc_ads_cost_metrics where type_of = 'search_query') as b on lower(a.Token) = lower(b.values_of)
) with data on commit preserve rows
;


CREATE VOLATILE MULTISET TABLE np_cpc_spend as (
SELECT * FROM 
(select 
    a.*,
    deal.min_cpc,
    deal.search_terms_dist,
    a.max_cpc - deal.min_cpc difference_btw_min_max,
    case when deal.search_terms_dist = 1 then 'a.1 search term'
         when deal.search_terms_dist <= 5 then 'b.less than 5 search term'
         when deal.search_terms_dist <= 10 then 'c.less than 10 search term'
         when deal.search_terms_dist <= 15 then 'd.less than 15 search term'
         when deal.search_terms_dist > 15 then 'e.greater than 15 search term'
         end search_term_case,
    case when max_cpc <= 0.5 then 'a.cpc 0.5 or less'
         when max_cpc <= 0.75 then 'b.cpc 0.75 or less'
         when max_cpc <= 1 then 'c.cpc 1 or less'
         when max_cpc <= 1.25 then 'd.cpc 1.25 or less'
         when max_cpc <= 1.5 then 'e.cpc 1.5 or less'
         when max_cpc <= 1.75 then 'f.cpc 1.75 or less'
         when max_cpc <= 2 then 'g.cpc 2 or less'
         when max_cpc <= 2.25 then 'h.cpc 2.25 or less'
         when max_cpc > 2.25 then 'i.cpc greater than 2.25'
         end max_cpc_case
from sandbox.np_citrusad_campaigns as a 
left join np_deal_min_bid2 as deal on a.product = deal.deal_id
---left join np_deal_min_bid3 as c on a.product = c.deal_id
where a.status not in ('DRAFT') and campaign_subtype = 'SEARCH_ONLY'
) as fin
) with data on commit preserve rows;



drop table sandbox.np_cpc_comp_fin_temp;
CREATE  MULTISET TABLE sandbox.np_cpc_comp_fin_temp as 
(SELECT 
      a.*, 
      (cpc_paid - min_cpc_value)/min_cpc_value per_higher_paid, 
      b.min_cpc, 
      b.max_cpc,
      b.search_term_case,
      b.search_terms_dist,
      b.difference_btw_min_max, 
      case when per_higher_paid < 0 then 'a.less than 0%'
      when per_higher_paid = 0 then 'b.at bid floor 0%'
      when per_higher_paid <= 0.2 then 'c.less than 25%'
      when per_higher_paid <= 0.4 then 'd.less than 40%'
      when per_higher_paid <= 0.6 then 'e.less than 60%'
      when per_higher_paid <= 0.8 then 'f.less than 80%'
      when per_higher_paid <= 1 then 'g.less than 100%'
      when per_higher_paid > 1 then 'h.greater than 100%'
      end cpc_above_bid_floor,
      case when min_cpc_value <= 0.5 then 'a.cpc 0.5 or less'
         when min_cpc_value <= 0.75 then 'b.cpc 0.75 or less'
         when min_cpc_value <= 1 then 'c.cpc 1 or less'
         when min_cpc_value <= 1.25 then 'd.cpc 1.25 or less'
         when min_cpc_value <= 1.5 then 'e.cpc 1.5 or less'
         when min_cpc_value <= 1.75 then 'f.cpc 1.75 or less'
         when min_cpc_value <= 2 then 'g.cpc 2 or less'
         when min_cpc_value <= 2.25 then 'h.cpc 2.25 or less'
         when min_cpc_value > 2.25 then 'i.cpc greater than 2.25'
         end min_cpc_case,
       b.max_cpc_case
FROM np_citrus_temp AS a 
LEFT JOIN np_cpc_spend AS b ON a.deal_id = b.product) with data;


select sum(case when per_higher_paid < 0 then 1 else 0 end), sum(case when per_higher_paid >= 0 then 1 else 0 end) from np_cpc_comp_fin;

-----looking at campaigns performing really bad in roas
-----seeing how many actually go above cpc
-------------------------------------------------------------------------------




drop table sandbox.np_team_sear	ch_term;
create multiset table sandbox.np_team_search_term as 
(SELECT 
    a.report_year,
    a.quarter,
    a.page_type,
    a.deal_id,
    a.l1, a.l2, a.l3,
    a.type_based_category,
    b.team_cohort, 
    sum(a.total_cpc) total_adspend,
    sum(a.citrus_clicks) clicks, 
    sum(a.citrus_impressions) impressions,
    total_adspend/NULLIFZERO(clicks) cpc
from 
   sandbox.np_citrus_sl_bid2 as a 
   left join 
   np_mapping_value as b on a.deal_id = b.sku
where quarter in ('Q4', 'Q1') 
     and report_year in (2021, 2022)
     and page_type =  'Search'
group by 1,2,3,4,5,6,7,8,9) with data





create volatile multiset table np_int2 as (
WITH cte (product, search_terms) AS
 (
   SELECT product, search_terms FROM sandbox.np_citrusad_campaigns where campaign_subtype = 'SEARCH_ONLY'
 )
SELECT * 
FROM TABLE
 ( STRTOK_SPLIT_TO_TABLE( cte.product, search_terms, ',') 
   RETURNS ( product varchar(64)  , TokenNum INT , Token VARCHAR (256) CHARACTER SET UNICODE ) 
 ) dt
 ) with data on commit preserve rows
 ;


create multiset volatile table np_deal_min_bid2 as (
select 
    a.product deal_id, 
    count(distinct a.Token) search_terms_dist, 
    min(b.cpc) min_cpc
from np_int2 as a 
left join (select * from sandbox.sc_ads_cost_metrics where type_of = 'search_query') as b on lower(a.Token) = lower(b.values_of)
group by 1
) with data on commit preserve rows
;








CREATE VOLATILE MULTISET TABLE np_cpc_spend as (
SELECT * FROM 
(select 
    a.*,
    deal.min_cpc,
    deal.search_terms_dist,
    b.total_adspend,
    b.all_rev,
    b.cpc_actual,
    a.max_cpc - deal.min_cpc difference_btw_min_max,
    case when total_adspend > 0 then 1 else 0 end with_adspend,
    case when ROAS > 1 then 1 else 0 end camp_roas_greater_than_one,
    case when deal.search_terms_dist = 1 then 'a.1 search term'
         when deal.search_terms_dist <= 5 then 'b.less than 5 search term'
         when deal.search_terms_dist <= 10 then 'c.less than 10 search term'
         when deal.search_terms_dist <= 15 then 'd.less than 15 search term'
         when deal.search_terms_dist > 15 then 'e.greater than 15 search term'
         end search_term_case,
    case when b.cpc_actual <= 0.5 then 'a.cpc 0.5 or less'
         when b.cpc_actual <= 0.75 then 'b.cpc 0.75 or less'
         when b.cpc_actual <= 1 then 'c.cpc 1 or less'
         when b.cpc_actual <= 1.25 then 'd.cpc 1.25 or less'
         when b.cpc_actual > 1.25 then 'e.cpc greater than 1.25'
         end cpc_case,
         total_clicks
from sandbox.np_citrusad_campaigns as a 
left join np_deal_min_bid2 as deal on a.product = deal.deal_id
left join 
    (select 
         campaign_id,
         sum(impressions) impressions,
         sum(clicks) total_clicks,
         sum(conversions) conversions,
         sum(unitsales) unit_sales,
         sum(sales_revenue) all_rev,
         sum(adspend) total_adspend, 
         cast(total_adspend as float)/NULLIFZERO(total_clicks) cpc_actual,
         cast(all_rev as float)/NULLIFZERO(total_adspend) ROAS
     from 
     sandbox.np_sl_ad_snapshot 
     group by 1
    )as b on a.citrusad_campaign_id = b.campaign_id
where a.status not in ('DRAFT') and campaign_subtype = 'SEARCH_ONLY'
) as fin
) with data on commit preserve rows;









-----------------------------------HIGHER THAN BID FLOOR

drop table sandbox.np_sc_deal_search_terms;
CREATE MULTISET TABLE sandbox.np_sc_deal_search_terms ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      deal_id VARCHAR(225) CHARACTER SET UNICODE,
      search_terms VARCHAR(4640) CHARACTER SET UNICODE
      )
NO PRIMARY INDEX;

select 
  a.deal_id, 
  a.search_terms_dist, 
  b.search_terms_dist search_terms_dist2, 
  case when a.search_terms_dist <> b.search_terms_dist then 1 else 0 end case_search
from np_deal_min_bid as a 
left join np_deal_min_bid2 as b on a.deal_id = b.deal_id
order by 4 desc;


create volatile multiset table np_mapping_value as (
select 
    fin.*, 
    max(fin2.supplier_name) supplier_name
from 
(select 
   a.sku, 
   max(rank_of_cat) rank_of_cat_max,
   case when rank_of_cat_max = 6 then 'Enterprise'
           when rank_of_cat_max = 5 then 'Coupons'
           when rank_of_cat_max = 4 then 'Goods'
           when rank_of_cat_max = 3 then 'SelfServe'
           when rank_of_cat_max = 2 then 'STIAB'
           when rank_of_cat_max = 1 then 'Test'
           end team_cohort
from 
(select 
     sku,
     a.supplier_name,
     b.team_cohort, 
     case when b.team_cohort = 'Enterprise' then 6 
           when b.team_cohort = 'Coupons' then 5
           when b.team_cohort = 'Goods' then 4
           when b.team_cohort = 'SelfServe' then 3
           when b.team_cohort = 'STIAB' then 2 
           when b.team_cohort = 'Test' then 1 
           end rank_of_cat
from sandbox.np_sl_ad_snapshot as a 
left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name
group by 1,2,3
) as a
group by 1) as fin  
left join 
(select 
     sku,
     a.supplier_name,
     b.team_cohort, 
     case when b.team_cohort = 'Enterprise' then 6 
           when b.team_cohort = 'Coupons' then 5
           when b.team_cohort = 'Goods' then 4
           when b.team_cohort = 'SelfServe' then 3
           when b.team_cohort = 'STIAB' then 2 
           when b.team_cohort = 'Test' then 1 
           end rank_of_cat
from sandbox.np_sl_ad_snapshot as a 
left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name
group by 1,2,3
) as fin2 on fin.sku = fin2.sku and fin2.rank_of_cat = fin.rank_of_cat_max
group by 1,2,3) with data on commit preserve rows
;


drop table sandbox.np_team_search_term;
create multiset table sandbox.np_team_search_term as 
(SELECT 
    a.report_year,
    a.quarter,
    a.page_type,
    a.deal_id,
    a.l1, a.l2, a.l3,
    a.type_based_category,
    b.team_cohort, 
    sum(a.total_cpc) total_adspend,
    sum(a.citrus_clicks) clicks, 
    sum(a.citrus_impressions) impressions,
    total_adspend/NULLIFZERO(clicks) cpc
from 
   sandbox.np_citrus_sl_bid2 as a 
   left join 
np_mapping_value as b on a.deal_id = b.sku
where quarter in ('Q4', 'Q1') 
     and report_year in (2021, 2022)
     and page_type =  'Search'
group by 1,2,3,4,5,6,7,8,9) with data
;

select * from sandbox.np_citrus_sl_bid2;

/*create volatile multiset table np_int as (
WITH cte (deal_id, search_terms) AS
 (
   SELECT deal_id, search_terms FROM sandbox.np_sc_deal_search_terms
 )
SELECT * 
FROM TABLE
 ( STRTOK_SPLIT_TO_TABLE( cte.deal_id, search_terms, ',') 
   RETURNS ( deal_id varchar(64)  , TokenNum INT , Token VARCHAR (256) CHARACTER SET UNICODE ) 
 ) dt
 ) with data on commit preserve rows
 ;


create multiset volatile table np_deal_min_bid as (
select 
    a.deal_id, 
    count(distinct a.Token) search_terms_dist, 
    min(b.cpc) min_cpc
from np_int as a 
left join (select * from sandbox.sc_ads_cost_metrics where type_of = 'search_query') as b on lower(a.Token) = lower(b.values_of)
group by 1
) with data on commit preserve rows
;

select max(search_terms_dist) from np_deal_min_bid;
select max(search_terms_dist) from np_deal_min_bid2;*/


create volatile multiset table np_int2 as (
WITH cte (product, search_terms) AS
 (
   SELECT product, search_terms FROM sandbox.np_citrusad_campaigns where campaign_subtype = 'SEARCH_ONLY'
 )
SELECT * 
FROM TABLE
 ( STRTOK_SPLIT_TO_TABLE( cte.product, search_terms, ',') 
   RETURNS ( product varchar(64)  , TokenNum INT , Token VARCHAR (256) CHARACTER SET UNICODE ) 
 ) dt
 ) with data on commit preserve rows
 ;


create multiset volatile table np_deal_min_bid2 as (
select 
    a.product deal_id, 
    count(distinct a.Token) search_terms_dist, 
    min(b.cpc) min_cpc
from np_int2 as a 
left join (select * from sandbox.sc_ads_cost_metrics where type_of = 'search_query') as b on lower(a.Token) = lower(b.values_of)
group by 1
) with data on commit preserve rows
;

select count(1), case when e from np_deal_min_bid2;

select * from np_deal_min_bid2 as a 
left join sandbox.np_citrusad_campaigns as b on a.deal_id = b.product
left join sandbox.pai_deals as c on a.deal_id = c.deal_uuid
where b.status = 'ACTIVE' and a.min_cpc is null
order by search_terms_dist desc
;

select * from sandbox.np_sc_deal_search_terms where deal_id = 'df124592-5520-46d8-81e3-5f8398e9b533';

where min_cpc is null order by search_terms_dist desc;
select * from np_int2 where product = '89afd1da-0332-4678-ab82-3b13c85c0993';
select * from sandbox.np_citrusad_campaigns where citrusad_campaign_id = 'c86982e6-391f-4196-ac33-07e696e37053';

select * from sandbox.sc_ads_cost_metrics where values_of like '%cosco%';
select * from sandbox.np_citrusad_campaigns;

select * from sandbox.np_citrus_sl_bid2;

create multiset volatile table np_deal_min as (
select 
    a.product deal_id, 
    a.Token,
    c.search_terms_dist,
    c.min_cpc
from np_int2 as a 
left join (select * from sandbox.sc_ads_cost_metrics where type_of = 'search_query') as b on lower(a.Token) = lower(b.values_of)
join np_deal_min_bid2 as c on b.cpc = c.min_cpc and a.product = c.deal_id
) with data on commit preserve rows;

drop table np_final_search;
create multiset volatile table np_final_search as (
select 
    a.*, 
    b.search_terms,
    b.cpc_search,
    search_terms_dist, 
    case when search_terms_dist = 1 then 'a.1 search term'
         when search_terms_dist <= 5 then 'b.less than 5 search term'
         when search_terms_dist <= 10 then 'c.less than 10 search term'
         when search_terms_dist <= 15 then 'd.less than 15 search term'
         when search_terms_dist > 15 then 'e.greater than 15 search term'
         end search_term_case, 
     case when c.deal_id is not null then 1 else 0 end in_the_list
from 
(select * from sandbox.np_team_search_term ) as a 
join (select Token search_terms, min(min_cpc) cpc_search from np_deal_min where search_terms_dist = 1 group by 1) as b on a.type_based_category = b.search_terms
left join 
     np_deal_min_bid2 as c on a.deal_id = c.deal_id) with data on commit preserve rows
;

select 
  case when in_the_list = 1 then 'SelfServe' else team_cohort end team_cohort,
  search_terms,
  search_term_case, 
  sum(total_adspend) total_adspend2, 
  sum(clicks) total_clicks, 
  total_adspend2/nullifzero(total_clicks) actual_cpc,
  count(distinct search_terms) search_terms,
  case when actual_cpc <= 0.5 then 'a.cpc 0.5 or less'
         when actual_cpc <= 0.75 then 'b.cpc 0.75 or less'
         when actual_cpc <= 1 then 'c.cpc 1 or less'
         when actual_cpc <= 1.25 then 'd.cpc 1.25 or less'
         when actual_cpc
         > 1.25 then 'e.cpc greater than 1.25'
         end cpc_case, 
  cpc_search,
  sum(impressions) total_imps
from 
 np_final_search
group by 1,2,3,9;



drop table np_cpc_spend;
CREATE VOLATILE MULTISET TABLE np_cpc_spend as (
SELECT * FROM 
(select 
    a.*,
    deal.min_cpc,
    deal.search_terms_dist,
    b.total_adspend,
    b.all_rev,
    b.cpc_actual,
    a.max_cpc - deal.min_cpc difference_btw_min_max,
    case when total_adspend > 0 then 1 else 0 end with_adspend,
    case when ROAS > 1 then 1 else 0 end camp_roas_greater_than_one,
    case when deal.search_terms_dist = 1 then 'a.1 search term'
         when deal.search_terms_dist <= 5 then 'b.less than 5 search term'
         when deal.search_terms_dist <= 10 then 'c.less than 10 search term'
         when deal.search_terms_dist <= 15 then 'd.less than 15 search term'
         when deal.search_terms_dist > 15 then 'e.greater than 15 search term'
         end search_term_case,
    case when b.cpc_actual <= 0.5 then 'a.cpc 0.5 or less'
         when b.cpc_actual <= 0.75 then 'b.cpc 0.75 or less'
         when b.cpc_actual <= 1 then 'c.cpc 1 or less'
         when b.cpc_actual <= 1.25 then 'd.cpc 1.25 or less'
         when b.cpc_actual > 1.25 then 'e.cpc greater than 1.25'
         end cpc_case,
         total_clicks
from sandbox.np_citrusad_campaigns as a 
left join np_deal_min_bid2 as deal on a.product = deal.deal_id
left join 
    (select 
         campaign_id,
         sum(impressions) impressions,
         sum(clicks) total_clicks,
         sum(conversions) conversions,
         sum(unitsales) unit_sales,
         sum(sales_revenue) all_rev,
         sum(adspend) total_adspend, 
         cast(total_adspend as float)/NULLIFZERO(total_clicks) cpc_actual,
         cast(all_rev as float)/NULLIFZERO(total_adspend) ROAS
     from 
     sandbox.np_sl_ad_snapshot 
     group by 1
    )as b on a.citrusad_campaign_id = b.campaign_id
where a.status not in ('DRAFT') and campaign_subtype = 'SEARCH_ONLY'
) as fin
) with data on commit preserve rows;






select OtherID, cs.Value --SplitData
from yourtable
cross apply STRING_SPLIT (Data, ',') cs;


select search_terms, STRTOK(search_terms, ',',2) from sandbox.np_sc_deal_search_terms order by 1;

create volatile multiset table np_int as (
  select  ,search_terms, STRTOK(search_terms, ',',b.integer) 
from sandbox.np_sc_deal_search_terms as a 
cross join 
	(SELECT ROW_NUMBER() OVER (ORDER BY day_rw) AS sr_no
	FROM user_groupondw.dim_day 
	WHERE day_rw BETWEEN CURRENT_DATE - 99 AND CURRENT_DATE) as b 
) with data on commit preserve rows;

select * from np_int;





select * from sandbox.pai_deals where deal_uuid = '95a956e5-f855-4d05-b1d0-9';

REGEXP_SUBSTR(dd.fine_print_c, lower(typ_b.text_res), 1, 1, 'i') = lower(typ_b.text_res)

-----------------------------------EXCEEDING DAILY CAP
drop table np_daily_adspend;
select * from sandbox.np_citrusad_campaigns;

create volatile table np_daily_adspend as (
select 
    b.*, 
    a.max_cpc,
    case when total_adspend = budget then 1 else 0 end spent_daily_budget,
    case when total_adspend > budget then 1 else 0 end spent_greater_budget,
    case when total_adspend < budget then 1 else 0 end spent_less_budget,
    case when total_adspend >= budget then 1 else 0 end spent_greater_or_equal_budget,
    case when total_adspend/budget < 0.25  then 'a.spent less than 25%'
         when total_adspend/budget < 0.5  then 'b.spent less than 50%'
         when total_adspend/budget < 1  then 'c.spent more than 50%'
         when total_adspend/budget = 1  then 'd.spent equal to the budget'
         when total_adspend/budget - 1 < 0.25 then 'e.exceeds budget by < 25%'
         when total_adspend/budget - 1 < 0.5 then 'f.exceeds budget by < 50%'
         when total_adspend/budget - 1 >= 0.5 then 'g.exceeds budget by > 50%'
         end budget_spend_category, 
    case when total_clicks = 1 then 'a. 1 click'
         when total_clicks = 2 then 'b. 2 clicks'
         when total_clicks = 3 then 'c. 3 clicks'
         when total_clicks = 4 then 'd. 4 clicks'
         when total_clicks >= 5 then 'e. 5 or more clicks'
         end clicks_category,
    case when budget <= 1 then 'a.less than $1'
         when budget <= 2 then 'b.less than $2'
         when budget <= 3 then 'c.less than $3'
         when budget <= 4 then 'd.less than $4'
         when budget > 4 then 'e.greater than $4'
         end budget_category,
         budget
from 
(select  
         report_date,
         campaign_id,
         sum(impressions) impressions,
         sum(clicks) total_clicks,
         sum(conversions) conversions,
         sum(unitsales) unit_sales,
         sum(sales_revenue) all_rev,
         sum(adspend) total_adspend, 
         cast(total_adspend as float)/NULLIFZERO(total_clicks) cpc_actual,
         cast(all_rev as float)/NULLIFZERO(total_adspend) ROAS, 
         case when total_adspend > 0 then 1 else 0 end adspend_greater_than_zero
     from 
     sandbox.np_sl_ad_snapshot
     group by 1,2
    )as b 
JOIN 
    sandbox.np_citrusad_campaigns as a on a.citrusad_campaign_id = b.campaign_id
where 
    a.spend_type = 'DAILY'
) with data on commit preserve rows
;

select cast(created_at as date), count(*) from user_gp.ads_rcncld_intrmdt_rpt group by 1 order by 1 desc;



select 
   fin.*, 
   cast(spent_greater_or_equal_budget as float)/total_days_with_adspend percent_days_pass_adspend, 
   case when percent_days_pass_adspend = 0 then 'no days exceeded budget'
        when percent_days_pass_adspend < 0.25 then 'a.less than 25% days'
        when percent_days_pass_adspend < 0.50 then 'b.less than 50% days'
        when percent_days_pass_adspend < 0.75 then 'c.less than 75% days'
        when percent_days_pass_adspend >= 0.75 then 'd.more than 75% days'
        end exceed_adspend_days_percent,
    cast(total_adspend as float)/nullifzero(total_clicks) actual_cpc, 
    case when actual_cpc > max_cpc then 1 else 0 end greater_than_actual_cpc, 
    daily_budget, 
    case when daily_budget <= 1 then 'a.less than $1'
         when daily_budget <= 2 then 'b.less than $2'
         when daily_budget <= 3 then 'c.less than $3'
         when daily_budget <= 4 then 'd.less than $4'
         when daily_budget > 4 then 'e.greater than $4'
         end budget_category
from 
(select 
   campaign_id, 
   max(budget) daily_budget,
   avg(total_adspend) average_adspend_per_campaign_per_day, 
   min(total_adspend) min_adspend_pre_campaign_per_day, 
   max(total_adspend) max_adspend_pre_campaign_per_day,
   count(1) total_days_with_adspend, 
   sum(spent_daily_budget) spent_daily_budget,
   sum(spent_greater_budget) spent_greater_budget, 
   sum(spent_less_budget) spent_less_budget,
   sum(spent_greater_or_equal_budget) spent_greater_or_equal_budget, 
   max(max_cpc) max_cpc,
   sum(total_adspend) total_adspend, 
   sum(total_clicks) total_clicks
from np_daily_adspend
where adspend_greater_than_zero = 1
group by 1) as fin;






select 
    count(distinct campaign_id) total_campaigns,
    count(distinct case when spent_greater_or_equal_budget = 1 then campaign_id end) campaigns_crossed_the_budget,
    sum(adspend_greater_than_zero) has_adspend, 
    sum(spent_daily_budget) has_spend_reached,
    sum(spent_greater_budget) has_spend_greater, 
    sum(spent_less_budget) has_spend_less, 
    sum(spent_greater_or_equal_budget) spent_greater_or_equal_budget
from np_daily_adspend
where adspend_greater_than_zero = 1;

select 
    budget_spend_category,
    sum(adspend_greater_than_zero) has_adspend,
    sum(spent_greater_or_equal_budget) spent_greater_or_equal_budget
from np_daily_adspend
where adspend_greater_than_zero = 1
group by 1
order by 1;

create volatile table np_total_adspend as (
select 
    b.*, 
    a.max_cpc,
    case when total_adspend >= budget then 1 else 0 end spent_total_budget
from 
   (select  
         campaign_id,
         sum(impressions) impressions,
         sum(clicks) total_clicks,
         sum(conversions) conversions,
         sum(unitsales) unit_sales,
         sum(sales_revenue) all_rev,
         sum(adspend) total_adspend, 
         cast(total_adspend as float)/NULLIFZERO(total_clicks) cpc_actual,
         cast(all_rev as float)/NULLIFZERO(total_adspend) ROAS, 
         case when total_adspend > 0 then 1 else 0 end adspend_greater_than_zero
     from 
     sandbox.np_sl_ad_snapshot
     group by 1
    ) as b
JOIN 
    sandbox.np_citrusad_campaigns as a on a.citrusad_campaign_id = b.campaign_id
where 
    a.spend_type = 'TOTAL'
) with data on commit preserve rows
;


select 
    count(distinct campaign_id) total_campaigns,
    count(distinct case when spent_total_budget = 1 then campaign_id end) campaigns_crossed_the_budget,
    sum(adspend_greater_than_zero) has_adspend, 
    sum(spent_total_budget) has_spend_reached
from np_total_adspend;


select 
     a.citrusad_campaign_id, 
     a.budget, 
     a.max_cpc, 
     cast(a.budget as float)/a.max_cpc as average_clicks, 
     cast(average_clicks as integer) int_clicks, 
     int_clicks*30 clicks_per_month, 
     (a.budget*30)/a.max_cpc available_clicks, 
     case when a.max_cpc > a.budget then 'max cpc greater than budget'
          when clicks_per_month <= 50 then 'less than 50 clicks per month'
          when clicks_per_month <= 200 then 'less than 200 clicks per month'
          when clicks_per_month <= 350 then 'less than 350 clicks per month'
          when clicks_per_month <= 500 then 'less than 500 clicks per month'
          when clicks_per_month  > 500 then 'greater than 500 clicks per month'
          end clicks_per_month_cat,
     case when max_cpc <= 0.5 then 'max_cpc <= 0.5' 
          when max_cpc <= 0.75 then 'max_cpc <= 0.75' 
          when max_cpc <= 1 then 'max_cpc <= 1'	
          when max_cpc <= 1.25 then 'max_cpc <= 1.25'	
          when max_cpc <= 1.5 then 'max_cpc <= 1.5'	
          when max_cpc > 1.5 then 'max_cpc > 1.5'
          end max_cpc_cat, 
     b.total_adspend, 
     b.all_rev, 
     b.adspend_greater_than_zero, 
     case when ROAS > 1 then 1 else 0 end roas_more_than_1, 
     case when budget <= 1 then 'a.less than $1'
          when budget <= 2 then 'b.less than $2'
          when budget <= 3 then 'c.less than $3'	
          when budget <= 4 then 'd.less than $4'
          when budget > 4 then 'e.greater than $4'
          end max_cpc_cat
from sandbox.np_citrusad_campaigns as a
left join 
(select  
         campaign_id,
         sum(impressions) impressions,
         sum(clicks) total_clicks,
         sum(conversions) conversions,
         sum(unitsales) unit_sales,
         sum(sales_revenue) all_rev,
         sum(adspend) total_adspend, 
         cast(total_adspend as float)/NULLIFZERO(total_clicks) cpc_actual,
         cast(all_rev as float)/NULLIFZERO(total_adspend) ROAS, 
         case when total_adspend > 0 then 1 else 0 end adspend_greater_than_zero
     from 
     sandbox.np_sl_ad_snapshot
     group by 1
    ) as b on a.citrusad_campaign_id = b.campaign_id
where 
    a.spend_type = 'DAILY';
   

   
   
   
--------------USER INTERACTION



select 
      trunc(cast(eventdate as date), 'iw') + 6 report_week, 
       consumeridsource, 
       rawpagetype, 
       case when b.merchant_id is not null and created_date <= cast(eventdate as date) then 1 else 0 end merchant_has_a_campaign,
       sum(total_entries) page_visits, 
       count(distinct merchantid) distinct_merchant_visits, 
       count(distinct case when merchantid is not null then concat(eventdate, merchantid) end) logins
from sandbox.np_ss_sl_interaction_agg as a 
left join 
    (select merchant_id, min(cast(substr(create_datetime, 1,10) as date)) created_date
            from sandbox.np_sponsored_campaign 
            where status not in ('DRAFT')
            group by 1) as b on a.merchantid = b.merchant_id
     group by 1,2,3,4
;



select 
    fin.eventdate, 
    fin.report_week, 
    fin.rawpagetype1,
    fin.rawpagetype2,
    c.merchantid_type_1,
    fin.merchantid_type_2
from
(select 
    a.eventdate, 
    trunc(cast(a.eventdate as date), 'iw') + 6 report_week,
    a.rawpagetype rawpagetype1, 
    b.rawpagetype rawpagetype2, 
    count(distinct b.merchantid) merchantid_type_2
from
(select 
    eventdate, 
    merchantid, 
    rawpagetype
    from 
    sandbox.np_ss_sl_interaction_agg
    group by 1,2,3) as a 
left join 
(select 
    eventdate, 
    merchantid, 
    rawpagetype
    from 
    sandbox.np_ss_sl_interaction_agg
    group by 1,2,3) as b on a.eventdate = b.eventdate and a.merchantid = b.merchantid and a.rawpagetype <> b.rawpagetype
group by 1,2,3,4) as fin
left join 
(select 
    eventdate, 
    rawpagetype, 
    count(distinct merchantid) merchantid_type_1
    from 
    sandbox.np_ss_sl_interaction_agg
    group by 1,2
) as c on fin.eventdate = c.eventdate and fin.rawpagetype1 = c.rawpagetype;


-------------------------------MERCHANT WHO LANDED IN MC and dont have a 
select 
   a.merchantid, 
   a.first_date_landing_sl, 
   a.drop_off_page,
   case when b.merchant_id is not null then 1 else 0 end merchant_active, 
   b.created_date min_active_date, 
   case when c.merchant_id is not null then 1 else 0 end merchant_has_draft_campaign,
   c.created_date earliest_draft_campaign
from 
(select 
     merchantid, 
     max(case when rawpagetype = 'home' then 1 else 0 end) visited_home,
     min(case when rawpagetype = 'home' then eventdate end) visited_home_date,
     max(case when rawpagetype = 'hub' then 1 else 0 end) visited_hub,
     min(case when rawpagetype = 'hub' then eventdate end) visited_hub_date,
     max(case when rawpagetype = 'set-campaign' then 1 else 0 end) visited_set_campaign,
     min(case when rawpagetype = 'set-campaign' then eventdate end) visited_set_campaign_date,
     max(case when rawpagetype = 'set-locations' then 1 else 0 end) visited_set_location,
     min(case when rawpagetype = 'set-locations' then eventdate end) visited_set_location_date,
     max(case when rawpagetype = 'set-date' then 1 else 0 end) visited_set_date_page,
     min(case when rawpagetype = 'set-date' then eventdate end) visited_set_date_date,
     max(case when rawpagetype = 'set-budget' then 1 else 0 end) visited_set_budget,
     min(case when rawpagetype = 'set-budget' then eventdate end) visited_set_budget_date,
     max(case when rawpagetype = 'add-payment' then 1 else 0 end) visited_add_payment,
     min(case when rawpagetype = 'add-payment' then eventdate end) visited_add_payment_date,
     max(case when rawpagetype = 'review-and-submit' then 1 else 0 end) visited_review,
     min(case when rawpagetype = 'review-and-submit' then eventdate end) visited_review_date,
     max(case when rawpagetype = 'final-page' then 1 else 0 end) visited_final,
     min(case when rawpagetype = 'final-page' then eventdate end) visited_final_date, 
     case when visited_home = 0 then 'never landed in merchant_center'
          when visited_hub = 1 then 
               case when visited_set_campaign = 0 then 'dropped at hub page'
                    when visited_set_location = 0 then 'dropped at set campaign page'
                    when visited_set_date_page = 0 then 'dropped at set location page'
                    when visited_set_budget = 0 then 'dropped at set date page'
                    when visited_add_payment = 0 then 'dropped at budget page'
                    when visited_review = 0 then 'dropped at add payment page'
                    when visited_final = 0 then 'dropped at review page'
                    else 'finished the flow'
                    end 
           else 'landed on merchant center interacted apart from the campaign flow'
           end drop_off_page,
    min(eventdate) first_date_landing_sl
from sandbox.np_ss_sl_interaction_agg group by 1) as a 
left join 
    (select merchant_id, min(cast(substr(create_datetime, 1,10) as date)) created_date
            from sandbox.np_sponsored_campaign 
            where status not in ('DRAFT')
            group by 1) as b on a.merchantid = b.merchant_id
left join 
    (select merchant_id, min(cast(substr(create_datetime, 1,10) as date)) created_date
            from sandbox.np_sponsored_campaign 
            where status = 'DRAFT'
            group by 1) as c on a.merchantid = c.merchant_id



            
-------------------------------QUICK 100 USD FREE CREDIT PULL METRIC 


select 
   a.merchant_uuid, 
   x.supplier_name, 
   x.supplier_id,
   x.sku
from
(select 
    * 
from np_temp_freecred
where freecredit = 100 and merchant_availed_offer = 1) as a 
left join 
(select 
    distinct
    a.sku, 
    a.supplier_id, 
    a.supplier_name,
    sf.merchant_uuid, 
    sf.account_id
from sandbox.np_sl_ad_snapshot a 
left join 
    (select d.deal_uuid, 
            max(d.l1) l1,
            max(d.l2) l2,
            max(d.l3) l3,
            max(m.merchant_uuid) merchant_uuid,
               max(m.account_id) account_id, 
               max(m.acct_owner) account_owner, 
               max(acct_owner_name) acct_owner_name,
               max(sfa.name) account_name, 
               max(merchant_segmentation__c) merch_segmentation
        from sandbox.pai_deals d 
        join sandbox.pai_merchants m on d.merchant_uuid = m.merchant_uuid
        join dwh_base_sec_view.sf_account sfa on m.account_id = sfa.id
        group by d.deal_uuid
    ) sf on a.sku = sf.deal_uuid) as x on a.merchant_uuid = x.merchant_uuid;

   
   
   
----------------------------------------------SS Wallets

select 
	     trunc(cast(c.citrusad_import_date as date), 'iw') + 6 week_start,
	     d.merchant_id,
	     sum(cast(available_balance as float)) available_balance
	from sandbox.np_slad_wlt_blc_htry as c 
	     left join 
	       (select wallet_id, merchant_id 
	         from sandbox.citrusad_team_wallet
	         group by 1,2) as d on c.wallet_id = d.wallet_id
	where 
	   cast(c.citrusad_import_date as date) = trunc(cast(c.citrusad_import_date as date), 'iw') + 6 
	   and c.is_archived = 0
	group by 1,2


select 
      a.*, 
      case when a.is_archived = 1 then CURRENT_DATE - cast(substr(update_datetime,1,10) as date) end number_of_days, 
      case when number_of_days >= 60 then 1 else 0 end greater_than_sixty_days
from sandbox.citrusad_team_wallet as a 
where is_self_serve = 1;

select * from sandbox.np_sl_ad_snapshot;

select * from sandbox.np_slad_wlt_blc_htry;
select * from sandbox.np_ss_performance_met;


select * from sandbox.np_groupon_wallet_sl;
select * from sandbox.np_sl_ad_snapshot;

sandbox.np_groupon_wallet_sl
sandbox.np_sl_ad_snapshot
sandbox.np_sl_lowbudget_actcamp
sandbox.np_sl_ledger
sandbox.np_sl_summary_all_time
sandbox.np_sl_summary_mtd

------------------------------------------------------



/*
create VOLATILE multiset table np_merchant_onboarder as (
select 
   a.*, 
   trunc(created_date, 'iw') +6 week_date
   from
(select 
    a.merchant_id, 
    a.merchant_name,
    m.account_id,
    min(cast(substr(create_datetime, 1,10) as date)) created_date
from sandbox.np_sponsored_campaign as a
     left join sandbox.pai_merchants as m on a.merchant_id = m.merchant_uuid
group by 1,2,3
) as a
) with data on commit preserve rows;



select 
    fin.eventdate,
    'home' page_drop,
    fin.merchantid, 
    case when onb.created_date < cast(fin.eventdate as date)+ 15 then 1 else 0 end onboarded_before_15_days
    from
	(select 
	    min(a.eventdate) eventdate, 
	    a.merchantid
	from
	(select 
	    eventdate, 
	    merchantid, 
	    rawpagetype
	    from 
	    sandbox.np_ss_sl_interaction_agg
	    where rawpagetype = 'home'
	    group by 1,2,3) as a 
	left join 
	(select 
	    eventdate, 
	    merchantid, 
	    rawpagetype
	    from 
	    sandbox.np_ss_sl_interaction_agg
	    where rawpagetype = 'hub'
	    group by 1,2,3) as b on a.eventdate = b.eventdate and a.merchantid = b.merchantid 
	where b.merchantid is null
	group by 2) as fin
left join (select merchant_id, min(created_date) created_date from np_merchant_onboarder group by 1) as onb on fin.merchantid = onb.merchant_id 
UNION 
select 
    fin.eventdate,
    'HUB' page_drop,
    fin.merchantid, 
    case when onb.created_date < cast(fin.eventdate as date)+ 15 then 1 else 0 end onboarded_before_15_days
    from
	(select 
	    min(a.eventdate) eventdate, 
	    a.merchantid
	from
	(select 
	    eventdate, 
	    merchantid, 
	    rawpagetype
	    from 
	    sandbox.np_ss_sl_interaction_agg
	    where rawpagetype = 'hub'
	    group by 1,2,3) as a 
	left join 
	(select 
	    eventdate, 
	    merchantid, 
	    rawpagetype
	    from 
	    sandbox.np_ss_sl_interaction_agg
	    where rawpagetype = 'set-campaign'
	    group by 1,2,3) as b on a.eventdate = b.eventdate and a.merchantid = b.merchantid 
	where b.merchantid is null
	group by 2) as fin
left join (select merchant_id, min(created_date) created_date from np_merchant_onboarder group by 1) as onb on fin.merchantid = onb.merchant_id 
;


select * from user_gp.ads_reconciled_report;


