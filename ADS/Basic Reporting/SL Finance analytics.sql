------TABLES
/*
sandbox.np_groupon_wallet_sl
sandbox.np_sl_ad_snapshot
sandbox.np_sl_lowbudget_actcamp
sandbox.np_sl_ledger
sandbox.np_sl_summary_all_time
sandbox.np_sl_summary_mtd
sandbox.np_sl_supp_mapping
sandbox.np_sl_requests
*/

insert into sandbox.pai_optimus_stats
SELECT logdate, Queryid, sessionID, username,statementtype, statementgroup, appID,starttime,elapsedtime, AMPCPUTime,TotalIOCount, querytext, abortflag, "errorcode", errortext, 
SUBSTR(queryband,instr(queryband,'/',1,5)+1) as col1, SUBSTR(col1,1,instr(col1,'/',1,1)) as col2,  'https://optimus.groupondev.com/#/jobs/edit/' || col2 as OPTIMUS_JOB_link
FROM UsrLib.QryLogVX
where clientID = 'OPTIMUS_APP';

select report_date,
      sum(clicks) tclicks, 
      sum(adspend) tadspend, 
      cast(tadspend as float)/nullifzero(tclicks) cpc 
from sandbox.te_sl_ad_snapshotv2 
where 
    campaign_sub_type = 'BROAD_DISPLAY'
and cast(report_date as date) >= cast('2022-07-01' as date) and cast(report_date as date) <= cast('2022-07-31' as date)
group by 1
order by 1
;

select
      sum(clicks) tclicks, 
      sum(adspend) tadspend, 
      cast(tadspend as float)/nullifzero(tclicks) cpc 
from sandbox.te_sl_ad_snapshotv2 
where 
    campaign_sub_type = 'BROAD_DISPLAY'
and cast(report_date as date) >= cast('2022-07-01' as date) and cast(report_date as date) <= cast('2022-07-31' as date)
;

select 
      b.Placements,
      sum(clicks) tclicks, 
      sum(adspend) tadspend, 
      cast(tadspend as float)/nullifzero(tclicks) cpc 
from sandbox.te_sl_ad_snapshotv2 as a 
join sandbox.te_sl_campaign_atsV2 as b on a.campaign_id = b.Campaign_ID
where 
    campaign_sub_type = 'BROAD_DISPLAY'
and cast(report_date as date) >= cast('2022-07-01' as date) and cast(report_date as date) <= cast('2022-07-31' as date)
group by 1
;

select report_date,
      b.Placements,
      sum(clicks) tclicks, 
      sum(adspend) tadspend, 
      cast(tadspend as float)/nullifzero(tclicks) cpc 
from sandbox.te_sl_ad_snapshotv2 as a 
join sandbox.te_sl_campaign_atsV2 as b on a.campaign_id = b.Campaign_ID
where 
    campaign_sub_type = 'BROAD_DISPLAY'
and cast(report_date as date) >= current_date - 30
group by 1,2
order by 1
;

select * from sandbox.te_sl_campaign_atsV2;
select * from sandbox.te_sl_ad_snapshotv2;


select * from user_edwprod.fact_gbl_transactions where transaction_division  is not null;

show view user_edwprod.fact_gbl_transactions;

select 
    substr(transaction_date, 1, 4) year_of_transaction, 
    substr(transaction_date, 6, 2) month_of_transaction, 
    a.deal_uuid, 
    b.permalink, 
    transaction_division,
    count(distinct parent_order_id) total_parent_orders, 
    sum(transaction_qty) transaction_quantity,
    sum(auth_nob_loc) NOB
from edwprod.fact_gbl_transactions as a 
left join grp_gdoop_bizops_db.pai_deals as b on a.deal_uuid = b.deal_uuid 
where a.action = 'authorize' and order_date >= '2022-01-01'
group by 
substr(transaction_date, 1, 4) , 
    substr(transaction_date, 6, 2) , 
    a.deal_uuid, 
    b.permalink, 
    transaction_division
;

select * from sandbox.np_sl_requests;
select * from sandbox.np_sl_ad_snapshot;
select * from sandbox.np_sl_summary_all_time;

select * from sandbox.sl_search_terms_data;

select campaign_ID, count(Placement_ID) xyz, count(1) xyz2 from sandbox.te_sl_campaign_atsV2 group by 1 having xyz2 > 1;


select 
         report_date,
         cast(report_date as date) - EXTRACT(DAY FROM cast(report_date as date)) + 1 report_month, 
         supplier_id,
         supplier_name,
         sku product_code, 
         sum(impressions) impressions,
         sum(clicks) clicks,
         sum(conversions) conversions,
         sum(unitsales) unit_sales,
         sum(sales_revenue) sales_revenue,
         sum(adspend) total_adspend
from sandbox.np_sl_ad_snapshot where sku in ('1e827ff1-5c1b-40d9-9f83-6c351d1d0874', 'f68ed31e-ec26-4140-92c4-215d3cf4bdb8')
group by 1,2,3,4,5;

select * from user_gp.ads_rcncld_intrmdt_rpt;
select * from sandbox.sl_search_terms_data;

select * from sandbox.np_sl_supp_mapping 
where team_cohort = 'Goods' and lower(supplier_name) not like '%- goods';

grant select on sandbox.np_sc_deal_search_terms to public;
grant select on sandbox.np_groupon_wallet_sl to public;
grant select on sandbox.np_sc_deal_search_terms to public;


grant select on  sandbox.np_merch_free_credits to public; 
grant select on sandbox.np_ss_sl_interaction_agg to public;
grant select on sandbox.np_sponsored_campaign to public;


grant select  on sandbox.np_ss_performance_met to public;

grant select on sandbox.np_sl_supplier_l3_daily to public;
grant select on sandbox.np_sl_requests to public;
grant select on sandbox.np_sl_ss_list_tableau to public;
grant select on sandbox.np_ss_sl_list_rep to public;
sandbox.np_ss_sl_mnthly_trgtlist

grant select on sandbox.np_sponsored_campaign to jclegg;
grant select on sandbox.citrusad_team_wallet to jclegg;
grant select on sandbox.np_merchant_topup_orders to jclegg;

select 
      trunc(cast(report_date as date) , 'iw') + 6 report_week, 
      sku deal_uuid, 
      case when campaign_name like '%mail%' then 1 else 0 end has_email, 
      sum(impressions) impressions, 
      sum(clicks) total_clicks, 
      sum(conversions) orders, 
      cast(orders as float)/NULLIFZERO(total_clicks) conv 
from sandbox.np_sl_ad_snapshot
group by 
     trunc(cast(report_date as date) , 'iw') + 6, 
    case when campaign_name like '%mail%' then 1 else 0 end, 
    sku
order by 1 desc,2;

select * from sandbox.np_sl_requests;

select 
     cast(report_date as date) - EXTRACT(DAY FROM cast(report_date as date)) + 1 report_month,
     placement, 
     sum(impressions) total_imps
from sandbox.np_sl_requests
group by 1,2
order by 1;

select * from sandbox.np_sl_ad_snapshot;

select 
     cast(report_date as date) - EXTRACT(DAY FROM cast(report_date as date)) + 1 report_month,
     placement, 
     sum(total_ads_requested) total_requested,
     sum(ads_served) total_served,
     cast(total_served as float)/total_requested
from sandbox.np_sl_requests
group by 1,2
order by 1;




-------------------------------------------------------------------------------------------------------------------------OVERALL 

select * from sandbox.np_sl_ad_snapshot where supplier_name like '%irate%';

select supplier_name,sku from sandbox.np_sl_ad_snapshot where supplier_name like '%irate%' group by 1,2;

select sum(impressions) impressions, sum(clicks) clicks 
from sandbox.np_sl_ad_snapshot where report_date = '2021-06-01';


select * from sandbox.np_sl_summary_all_time;
select report_date from sandbox.np_sl_ad_snapshot group by 1 order by 1 desc;
select * from sandbox.np_groupon_wallet_sl;

select max(report_date) from sandbox.np_groupon_wallet_sl;

----------------NOTE THIS IS SUPPLIER LOST BASED ON IF THEIR WALLET IS THERE OR NOT ANYMORE. 

drop table sandbox.np_sl_wallet_one;
create table sandbox.np_sl_wallet_one as (
select 
		    'supplier_constant' as supplier_week_cat,
		    a.report_week,
		    a.supplier_name,
		    a.team_cohort, 
		    a.available_balance_sum
		from
		    (select 
		         cast(report_date as date) - 7 prev_week,
		         cast(report_date as date) report_week, 
		         cast(report_date as date) + 7 next_week,
		         a.supplier_name,
		         b.team_cohort,
		         sum(a.available_balance) available_balance_sum
		     from sandbox.np_groupon_wallet_sl as a
		     left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name
		     where 
		         cast(report_date as date) = trunc(cast(report_date as date), 'iw') + 6 
		         and  cast(report_date as date) >= CURRENT_DATE - 40
		         and archived = 'false'
		     group by 1,2,3,4,5) as a
		 join 
		     (select 
		         cast(report_date as date) - 7 prev_week,
		         cast(report_date as date) report_week, 
		         cast(report_date as date) + 7 next_week,
		         a.supplier_name,
		         b.team_cohort,
		         sum(a.available_balance) available_balance_sum
		     from sandbox.np_groupon_wallet_sl as a
		     left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name
		     where 
		         cast(report_date as date) = trunc(cast(report_date as date), 'iw') + 6 
		         and  cast(report_date as date) >= CURRENT_DATE - 40
		         and archived <> 'true'
		     group by 1,2,3,4,5) as b on a.report_week = b.next_week and a.supplier_name = b.supplier_name
) with data;

drop table sandbox.np_sl_wallet_two;
create table sandbox.np_sl_wallet_two as (
select 
		    'supplier_added' as supplier_week_cat,
		    a.report_week,
		    a.supplier_name,
		    a.team_cohort, 
		    a.available_balance_sum
		from
		    (select 
		         cast(report_date as date) - 7 prev_week,
		         cast(report_date as date) report_week, 
		         cast(report_date as date) + 7 next_week,
		         a.supplier_name,
		         b.team_cohort,
		         sum(a.available_balance) available_balance_sum
		     from sandbox.np_groupon_wallet_sl as a
		     left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name
		     where 
		         cast(report_date as date) = trunc(cast(report_date as date), 'iw') + 6 
		         and  cast(report_date as date) >= CURRENT_DATE - 40
		         and archived = 'false'
		     group by 1,2,3,4,5) as a
		left join 
		     (select 
		         cast(report_date as date) - 7 prev_week,
		         cast(report_date as date) report_week, 
		         cast(report_date as date) + 7 next_week,
		         a.supplier_name,
		         b.team_cohort,
		         sum(a.available_balance) available_balance_sum
		     from sandbox.np_groupon_wallet_sl as a
		     left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name
		     where 
		         cast(report_date as date) = trunc(cast(report_date as date), 'iw') + 6 
		         and  cast(report_date as date) >= CURRENT_DATE - 47
		         and archived = 'false'
		     group by 1,2,3,4,5) as b on a.report_week = b.next_week and a.supplier_name = b.supplier_name
		where b.next_week is null
) with data;

drop table sandbox.np_sl_wallet_three;
create table sandbox.np_sl_wallet_three as (
select 
		    'supplier_lost' as supplier_week_cat,
		    a.next_week week_lost_report_week,
		    a.supplier_name,
		    a.team_cohort, 
		    b.available_balance_sum ----null values
		from
		    (select 
		         cast(report_date as date) - 7 prev_week,
		         cast(report_date as date) report_week, 
		         cast(report_date as date) + 7 next_week,
		         a.supplier_name,
		         b.team_cohort,
		         sum(a.available_balance) available_balance_sum
		     from sandbox.np_groupon_wallet_sl as a
		     left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name
		     where 
		         cast(report_date as date) = trunc(cast(report_date as date), 'iw') + 6 
		         and  cast(report_date as date) >= CURRENT_DATE - 47
		         and archived = 'false'
		     group by 1,2,3,4,5) as a
		left join 
		     (select 
		         cast(report_date as date) - 7 prev_week,
		         cast(report_date as date) report_week, 
		         cast(report_date as date) + 7 next_week,
		         a.supplier_name,
		         b.team_cohort,
		         sum(a.available_balance) available_balance_sum
		     from sandbox.np_groupon_wallet_sl as a
		     left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name
		     where 
		         cast(report_date as date) = trunc(cast(report_date as date), 'iw') + 6 
		         and  cast(report_date as date) >= CURRENT_DATE - 40
		         and archived = 'false'
		     group by 1,2,3,4,5) as b on a.report_week = b.prev_week and a.supplier_name = b.supplier_name 
		where b.prev_week is null
) with data;


drop table sandbox.np_sl_wallet_level_spend_agg;
create table sandbox.np_sl_wallet_level_spend_agg as (
select 
    fin.*, 
    sn.adspend, 
    sn.impressions,
    sn.clicks,
    sn.conversions,
    sn.unitsales,
    sn.sales_revenue, 
    case when available_balance_sum <= 0 then 'a.<= 0 USD in the wallet'
          when available_balance_sum <= 250 then 'b.<= 250 USD in the wallet'
          when available_balance_sum <= 500 then 'c.<= 500 USD in the wallet'
          when available_balance_sum <= 1000 then 'd.<= 1000 USD in the wallet'
          when available_balance_sum <= 2000 then 'e.<= 2000 USD in the wallet'
          when available_balance_sum >2000 then 'f.more than 2000 usd in wallet'
          end available_balance_cohort, 
    cast(sales_revenue as float)/NULLIFZERO(adspend) roas, 
    case when roas > 1 then 1 else 0 end roas_more_than_1, 
    case when adspend > 0 then 1 else 0 end merchants_with_adspend,
    cast(available_balance_sum as float)/NULLIFZERO(adspend) weeks_to_finish_balance, 
    case when (available_balance_sum <= 0 or available_balance_sum is null)then 'a. No balance available'
         when (adspend is null or adspend = 0) then 'b. No adspend seen'
         when weeks_to_finish_balance <= 5 then 'c. balance used in <= 5 weeks'
         when weeks_to_finish_balance <= 15 then 'd. balance used in <= 15 weeks'
         when weeks_to_finish_balance <= 25 then 'e. balance used in <= 25 weeks'
         when weeks_to_finish_balance <= 35 then 'f. balance used in <= 35 weeks'
         when weeks_to_finish_balance <= 45 then 'g. balance used in <= 45 weeks'
         when weeks_to_finish_balance > 45 then 'h. balance used in > 45 weeks'
         end weeks_to_finish_bal_cat
from 
		(select * from sandbox.np_sl_wallet_one
		UNION
		select * from sandbox.np_sl_wallet_two
		UNION
		select * from sandbox.np_sl_wallet_three) as fin
left join 
     (select 
            trunc(cast(report_date as date), 'iw') + 6 report_week,
            supplier_name,
            sum(adspend) adspend, 
            sum(impressions) impressions,
            sum(clicks) clicks,
            sum(conversions) conversions,
            sum(unitsales) unitsales,
            sum(sales_revenue) sales_revenue
         from sandbox.np_sl_ad_snapshot
         group by 1,2
     ) as sn on fin.report_week = sn.report_week and fin.supplier_name = sn.supplier_name
) WITH DATA;


select report_week, sum(case when adspend > 0 then 1 else 0 end ) total_supps, sum(case when roas > 1 then 1 else 0 end) roas_greater_than_1
from sandbox.np_sl_wallet_level_spend_agg 
where impressions > 1000 and 
group by 1
order by 1
;



--------------------------------------------------------------------------------------------------------------------------------------------------SUPPLIER L3 AGGREGATED  

------RUN IN HIVE


CREATE MULTISET TABLE sandbox.np_sl_deals_imps_trans ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      report_month VARCHAR(20) CHARACTER SET UNICODE,
      deal_id VARCHAR(64) CHARACTER SET UNICODE, 
      total_imps float,
      all_transaction float,
      units float, 
      parent_orders_qty float,
      deal_views float,
      nob_usd float, 
      nor_usd float
      )
NO PRIMARY INDEX;

drop table if exists grp_gdoop_bizops_db.np_sl_deals_imps_trans;
create table grp_gdoop_bizops_db.np_sl_deals_imps_trans stored as orc as
select 
     imp.*, 
     fin.all_transaction, 
     fin.units, 
     fin.parent_orders_qty, 
     fin.deal_views, 
     fin.nob_usd, 
     fin.nor_usd
from
(select 
      date_add(last_day(add_months(report_date, -1)),1) report_month,
      deal_id, 
      sum(total_impressions) total_imps
     from edwprod.agg_gbl_impressions_deal as a
       join user_groupondw.active_deals as b on a.deal_id = b.deal_uuid and a.report_date = b.load_date and b.SOLD_OUT = 'false'
     where cast(report_date as date) >= cast('2021-01-01' as date)
     group by 
     date_add(last_day(add_months(report_date, -1)),1),
     deal_id) as imp
left join
(select 
       date_add(last_day(add_months(report_date, -1)),1) report_month,
       deal_id,
       sum(transactions) all_transaction,
       sum(transactions_qty) units, 
       sum(parent_orders_qty) parent_orders_qty,
       sum(deal_views) deal_views,
       sum(nob_usd) nob_usd, 
       sum(nor_usd) nor_usd
       from edwprod.agg_gbl_traffic_fin_deal as a
       join user_groupondw.active_deals as b on a.deal_id = b.deal_uuid and a.report_date = b.load_date and b.SOLD_OUT = 'false'
       where cast(a.report_date as date) >= cast('2021-01-01' as date) 
       group by date_add(last_day(add_months(report_date, -1)),1), deal_id) fin on imp.report_month = fin.report_month and imp.deal_id = fin.deal_id
;
-------


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



drop table sandbox.np_supplier_l3_agg;
create table sandbox.np_supplier_l3_agg as (
select 
     report_month,
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
     case when roas > 1 then 1 else 0 end roas_more_than_1
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
         sum(adspend) total_adspend
     from sandbox.np_sl_ad_snapshot as a 
     group by 1,2,3,4) as a
left join 
     sandbox.np_sl_deals_imps_trans as overall on a.report_month = cast(overall.report_month as date) and a.product_code = overall.deal_id
left join 
   np_temp_avg_price as b on b.product_uuid = a.product_code
left join 
     (select deal_id, max(grt_l1_cat_name) l1, max(grt_l2_cat_name) l2, max(grt_l3_cat_name) l3  from user_edwprod.dim_gbl_deal_lob group by 1) as c on a.product_code = c.deal_id
left join sandbox.np_sl_supp_mapping as d on a.supplier_name = d.supplier_name
     ) fin group by 1,2,3,4) with data
    ;
    
   
select 
     qtd_classification,
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
     case when roas > 1 then 1 else 0 end roas_more_than_1
     from
(select a.*,
       c.l1, c.l2, c.l3,
       d.team_cohort,
       b.contract_sell_price , 
       b.contract_buy_price , 
       (b.contract_sell_price - b.contract_buy_price)/NULLIFZERO(b.contract_sell_price) groupon_margin, 
       case when sales_revenue * groupon_margin is null then 0 else sales_revenue * groupon_margin end groupon_deal_revenue
     from 
     (select 
         case when month(cast(report_date as date)) in (1,2,3) then concat('Q1', '-', year(report_date))
              when month(cast(report_date as date)) in (4,5,6) then concat('Q2', '-', year(report_date))
              when month(cast(report_date as date)) in (7,8,9) then concat('Q3', '-', year(report_date))
              when month(cast(report_date as date)) in (10,11,12) then concat('Q4', '-', year(report_date))
              end
              qtd_classification, 
         supplier_id,
         supplier_name,
         sku product_code, 
         sum(impressions) impressions,
         sum(clicks) clicks,
         sum(conversions) conversions,
         sum(unitsales) unit_sales,
         sum(sales_revenue) sales_revenue,
         sum(adspend) total_adspend
     from sandbox.np_sl_ad_snapshot as a 
     group by 1,2,3,4) as a
left join 
   np_temp_avg_price as b on b.product_uuid = a.product_code
left join 
     (select deal_id, max(grt_l1_cat_name) l1, max(grt_l2_cat_name) l2, max(grt_l3_cat_name) l3  from user_edwprod.dim_gbl_deal_lob group by 1) as c on a.product_code = c.deal_id
left join sandbox.np_sl_supp_mapping as d on a.supplier_name = d.supplier_name
WHERE a.total_adspend > 0
     ) fin group by 1,2,3,4;

drop table np_supplier_quaterly;
create volatile multiset table np_supplier_quaterly as (
select 
     year(report_month) year_of_year,
     case when month(report_month) >= 1 and month(report_month) <= 3 then 'Q1'
          when month(report_month) >= 4 and month(report_month) <= 6 then 'Q2'
          when month(report_month) >= 7 and month(report_month) <= 9 then 'Q3'
          when month(report_month) >= 10 and month(report_month) <= 12 then 'Q4' end
          quart_of_year,
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
     case when roas > 1 then 1 else 0 end roas_more_than_1
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
         sum(adspend) total_adspend
     from sandbox.np_sl_ad_snapshot as a 
     group by 1,2,3,4) as a
left join 
     sandbox.np_sl_deals_imps_trans as overall on a.report_month = cast(overall.report_month as date) and a.product_code = overall.deal_id
left join 
   np_temp_avg_price as b on b.product_uuid = a.product_code
left join 
     (select deal_id, max(grt_l1_cat_name) l1, max(grt_l2_cat_name) l2, max(grt_l3_cat_name) l3  from user_edwprod.dim_gbl_deal_lob group by 1) as c on a.product_code = c.deal_id
left join sandbox.np_sl_supp_mapping as d on a.supplier_name = d.supplier_name
     ) fin group by 1,2,3,4,5) with data on commit preserve rows
   


select 
     report_month,
     supplier_name, 
     campaign_name, 
     campaign_id,
     campaign_status,
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
     case when roas > 1 then 1 else 0 end roas_more_than_1
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
         campaign_name, 
         campaign_id,
         campaign_status,
         sku product_code, 
         sum(impressions) impressions,
         sum(clicks) clicks,
         sum(conversions) conversions,
         sum(unitsales) unit_sales,
         sum(sales_revenue) sales_revenue,
         sum(adspend) total_adspend
     from sandbox.np_sl_ad_snapshot as a 
     group by 1,2,3,4,5,6,7) as a
left join 
     sandbox.np_sl_deals_imps_trans as overall on a.report_month = cast(overall.report_month as date) and a.product_code = overall.deal_id
left join 
   np_temp_avg_price as b on b.product_uuid = a.product_code
left join 
     (select deal_id, max(grt_l1_cat_name) l1, max(grt_l2_cat_name) l2, max(grt_l3_cat_name) l3  from user_edwprod.dim_gbl_deal_lob group by 1) as c on a.product_code = c.deal_id
left join sandbox.np_sl_supp_mapping as d on a.supplier_name = d.supplier_name
     ) fin group by 1,2,3,4,5,6
;     
   
select 
         cast(report_date as date) - EXTRACT(DAY FROM cast(report_date as date)) + 1 report_month, 
         supplier_id,
         supplier_name,
         sku product_code, 
         sum(impressions) impressions,
         sum(clicks) clicks,
         sum(conversions) conversions,
         sum(unitsales) unit_sales,
         sum(sales_revenue) sales_revenue,
         sum(adspend) total_adspend
     from sandbox.np_sl_ad_snapshot as a 
     where supplier_id in (select distinct supplier_id from sandbox.np_supplier_l3_agg where report_month >= cast('2021-06-01' as date) and sales_revenue > 0 and overall_sales_revenue = 0)
          and cast(report_date as date) >= cast('2021-06-01' as date)
     group by 1,2,3,4
     order by 2,1;



--------------------------------------------------------------------------------------------------------------------------LAST DAY OF SPEND

drop table np_sl_max_impression_date;
create volatile table np_sl_max_impression_date as     
(select 
         supplier_id,
         supplier_name,
         max(report_date) max_impression_date
     from sandbox.np_sl_ad_snapshot as a 
     where impressions > 0
     group by 1,2
) with data on commit preserve rows;


drop table np_sl_max_spend_date;
create volatile table np_sl_max_spend_date as     
(select 
         supplier_id,
         supplier_name,
         max(report_date) max_spend_date, 
         case when max_spend_date > current_date - 15 then 'adspend in last 15 days'
              when max_spend_date > current_date - 30 then 'adspend in last 30 days'
              when max_spend_date > current_date - 45 then 'adspend in last 45 days'
              when max_spend_date > current_date - 60 then 'adspend in last 60 days'
              else 'has no adspend in last 60 days' end
              last_adspend_day_category
     from sandbox.np_sl_ad_snapshot as a 
     where adspend > 0
     group by 1,2
) with data on commit preserve rows;

drop table np_sl_last_30_days;
create volatile table np_sl_last_30_days as 
(select 
     a.supplier_id, 
     a.supplier_name, 
     a.max_spend_date,
     sum(impressions) impressions,
     sum(clicks) clicks,
     sum(conversions) conversions,
     sum(unitsales) unit_sales,
     sum(sales_revenue) sales_revenue,
     sum(adspend) total_adspend,
     cast(sum(sales_revenue) as float)/NULLIFZERO(sum(adspend)) roas,
     case when roas > 1 then 1 else 0 end roas_more_than_1
from np_sl_max_spend_date as a
left join sandbox.np_sl_ad_snapshot as b on a.supplier_name = b.supplier_name and cast(b.report_date as date) >= cast(a.max_spend_date as date) - 30
group by 1,2,3) with data on commit preserve rows;





select
    a.*, 
    b.max_spend_date, 
    case when b.last_adspend_day_category is null then 'Never had an adspend' else b.last_adspend_day_category end last_adspend_day_category,
    case when b.supplier_name is null then 'merchant has impression but no adspend' else 'merchant has adspend and impression' end impression_adspend_cat,
    case when (f.available_balance_sum <= 0 or f.available_balance_sum is null)then 'a. No balance available'
         when f.available_balance_sum <= 250 then 'b.<= 250 USD in the wallet'
          when f.available_balance_sum <= 500 then 'c.<= 500 USD in the wallet'
          when f.available_balance_sum <= 1000 then 'd.<= 1000 USD in the wallet'
          when f.available_balance_sum <= 2000 then 'e.<= 2000 USD in the wallet'
          when f.available_balance_sum >2000 then 'f.more than 2000 usd in wallet'
         end available_balance_cohort,
    d.team_cohort,
    c.conversions,
    c.unit_sales,
    c.total_adspend,
    c.roas,
    c.roas_more_than_1,
    e.conversions conversoins_last_30_days,
    e.unit_sales unit_sales_last_30_days,
    e.total_adspend adspend_last_30_days,
    e.roas roas_last_30_days,
    e.roas_more_than_1 roas_more_than_1_last_30_days
from 
    np_sl_max_impression_date as a 
    left join np_sl_max_spend_date as b on a.supplier_name = b.supplier_name
    left join 
    (select 
         supplier_id,
         supplier_name,
         sum(impressions) impressions,
         sum(clicks) clicks,
         sum(conversions) conversions,
         sum(unitsales) unit_sales,
         sum(sales_revenue) sales_revenue,
         sum(adspend) total_adspend,
         cast(sum(sales_revenue) as float)/NULLIFZERO(sum(adspend)) roas,
         case when roas > 1 then 1 else 0 end roas_more_than_1
     from sandbox.np_sl_ad_snapshot as a
     group by 1,2
     ) as c on a.supplier_name = c.supplier_name
     left join sandbox.np_sl_supp_mapping as d on a.supplier_name = d.supplier_name
     left join np_sl_last_30_days as e on a.supplier_name = e.supplier_name
     left join 
     (select 
         a.supplier_name,
	     sum(a.available_balance) available_balance_sum
      from sandbox.np_groupon_wallet_sl as a
      where 
	    cast(report_date as date) = trunc(cast(current_date as date), 'iw') -1
	    and archived = 'false'
	    group by 1) as f on a.supplier_name = f.supplier_name
     ;
    
    
----------
    
drop table sandbox.np_supplier_l3_agg;
create table sandbox.np_supplier_l3_agg as (
select 
     report_month,
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
     case when roas > 1 then 1 else 0 end roas_more_than_1
     from
(select a.*,
       c.l1, c.l2, c.l3,
       d.team_cohort,
       b.contract_sell_price , 
       b.contract_buy_price , 
       (b.contract_sell_price - b.contract_buy_price)/NULLIFZERO(b.contract_sell_price) groupon_margin, 
       case when (sales_revenue/nullifzero((1-groupon_margin)))*groupon_margin is null then 0 else (sales_revenue/(1-groupon_margin))*groupon_margin end groupon_deal_revenue,
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
         sum(adspend) total_adspend
     from sandbox.np_sl_ad_snapshot as a 
     group by 1,2,3,4) as a
left join 
     sandbox.np_sl_deals_imps_trans as overall on a.report_month = cast(overall.report_month as date) and a.product_code = overall.deal_id
left join 
   np_temp_avg_price as b on b.product_uuid = a.product_code
left join 
     (select deal_id, max(grt_l1_cat_name) l1, max(grt_l2_cat_name) l2, max(grt_l3_cat_name) l3  from user_edwprod.dim_gbl_deal_lob group by 1) as c on a.product_code = c.deal_id
left join sandbox.np_sl_supp_mapping as d on a.supplier_name = d.supplier_name
     ) fin group by 1,2,3,4) with data;



    
    
--------------------------------------------------------------------------------------------------------------------------DASHBOARD input

     

drop table sandbox.np_inv_price_temp1;
create multiset table sandbox.np_inv_price_temp1 as (
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



drop table sandbox.np_sl_supplier_l3_daily;
create table sandbox.np_sl_supplier_l3_daily as (
select 
     report_date,
     team_cohort,
     l1 l1, 
     l2 l2, 
     l3 l3, 
     campaign_sub_type,
     sum(impressions) impressions,
     sum(clicks) clicks,
     sum(conversions) conversions,
     sum(unit_sales) unit_sales,
     sum(sales_revenue) sales_revenue,
     sum(total_adspend) total_adspend,
     sum(groupon_deal_revenue) groupon_deal_revenue
     from
(select a.*,
       c.l1, c.l2, c.l3,
       d.team_cohort,
       b.contract_sell_price , 
       b.contract_buy_price , 
       (b.contract_sell_price - b.contract_buy_price)/NULLIFZERO(b.contract_sell_price) groupon_margin, 
       case when (sales_revenue/(1-groupon_margin))*groupon_margin is null then 0 else (sales_revenue/(1-groupon_margin))*groupon_margin end groupon_deal_revenue
from 
     (select 
         cast(report_date as date)  report_date, 
         supplier_id,
         supplier_name,
         sku product_code, 
         campaign_sub_type,
         sum(impressions) impressions,
         sum(clicks) clicks,
         sum(conversions) conversions,
         sum(unitsales) unit_sales,
         sum(sales_revenue) sales_revenue,
         sum(adspend) total_adspend
     from sandbox.np_sl_ad_snapshot as a 
     group by 1,2,3,4,5) as a
left join 
   np_temp_avg_price as b on b.product_uuid = a.product_code
left join 
     (select deal_id, max(grt_l1_cat_name) l1, max(grt_l2_cat_name) l2, max(grt_l3_cat_name) l3  from user_edwprod.dim_gbl_deal_lob group by 1) as c on a.product_code = c.deal_id
left join sandbox.np_sl_supp_mapping as d on a.supplier_name = d.supplier_name
     ) fin group by 1,2,3,4,5,6) with data;

     
select 
      cast(report_date as date) - EXTRACT(DAY FROM cast(report_date as date)) + 1 report_month,
      trunc(cast(report_date as date), 'iw') + 6 report_week,
      sum(adrequests) ads_requested,
      sum(total_ads_requested) total_ads_requested,
      sum(ads_served) ads_served,
      sum(impressions) impressions
from sandbox.np_sl_requests
      group by 1,2;


select * from sandbox.SFMC_EmailEngagement;
---------------------------------------------------------------------------------------------------------------------------MAIN ANALYTICS

select sum(impressions), sum(clicks) from sandbox.np_sl_ad_snapshot 
where cast(report_date as date) >= cast('2022-01-01' as date) 
      and cast(report_date as date) <= cast('2022-01-31' as date) 
      and campaign_sub_type = 'BROAD_DISPLAY';
     
select * from nvp_temp_sl_adspend;

create volatile table nvp_temp_sl_adspend as (
select 
        trunc(cast(report_date as date), 'iw') + 6 report_week,
        a.supplier_name,
        b.team_cohort,
        sum(adspend) adspend, 
        sum(impressions) impressions,
        sum(clicks) clicks,
        sum(conversions) conversions,
        sum(unitsales) unitsales,
        sum(sales_revenue) sales_revenue
  from sandbox.np_sl_ad_snapshot AS a
  left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name
  group by 1,2,3
) with data on commit preserve rows
;

select 
* from 
(select 
       report_week, 
       team_cohort,
       'merchants_spending' merchants_category ,  
       count(distinct supplier_name) count_of_merchants 
from nvp_temp_sl_adspend
group by 1,2
where adspend > 0
UNION 
select a.report_week + 7, 
       a.team_cohort,
       'merchants_stopped_spending' merchants_category ,  
       count(distinct a.supplier_name) count_of_merchants 
from (select * from nvp_temp_sl_adspend where adspend > 0)  as a 
left join (select * from nvp_temp_sl_adspend where adspend > 0)  as b on a.report_week+7 = b.report_week and a.supplier_name = b.supplier_name
group by 1,2
where b.supplier_name is null
UNION 
select a.report_week, 
       a.team_cohort,
       'merchants_added_spending' merchants_category ,  
       count(distinct a.supplier_name) count_of_merchants 
from (select * from nvp_temp_sl_adspend where adspend > 0) as a 
left join (select * from nvp_temp_sl_adspend where adspend > 0) as b on a.report_week - 7 = b.report_week and a.supplier_name = b.supplier_name
group by 1,2
where b.supplier_name is null
) fin order by 1,2,3;


select 
     report_month, 
     b.l1,
     b.l2,
     b.l3,
     count(distinct a.deal_id) deals_live, 
     count(distinct c.merchant_uuid) merchants_live,
     sum(total_imps) total_imps, 
     sum(deal_views) deal_views, 
     sum(all_transaction) total_transactions, 
     sum(units) units_sold, 
     sum(parent_orders_qty) parent_orders_qty, 
     sum(nob_usd) nob_usd, 
     sum(nor_usd) nor_usd
from sandbox.np_sl_deals_imps_trans as a 
left join 
     (select deal_id, 
             max(grt_l1_cat_name) l1, 
             max(grt_l2_cat_name) l2, 
             max(grt_l3_cat_name) l3  
       from user_edwprod.dim_gbl_deal_lob group by 1) as b on a.deal_id = b.deal_id
left join 
     sandbox.pai_deals as c on a.deal_id = c.deal_uuid
group by 1,2,3,4;





select 
   a.*, 
   case when b.supplier_name is not null then 1 else 0 end available_next_week,
   b.adspend
from 
(select  supplier_name, sum(adspend) adspend from nvp_temp_sl_adspend where adspend > 0 and report_week = '2022-01-16' and team_cohort = 'Enterprise' group by 1) as a 
left join 
(select supplier_name, sum(adspend) adspend from nvp_temp_sl_adspend where adspend > 0 and report_week = '2022-01-23' group by 1) as b on a.supplier_name = b.supplier_name 
order by 2 desc;

select * from np_groupon_wallet_sl where report_date = '2021-10-04' and supplier_name = 'Blue Apron - COUP';


select 
   a.*, 
   b.yzx, 
   a.xyz - b.yzx
from 
(select supplier_name, sum(adspend) xyz from nvp_temp_sl_adspend where adspend > 0 and report_week = '2022-01-02' and team_cohort = 'Goods' group by 1) as a 
left join 
(select supplier_name, sum(adspend) yzx from nvp_temp_sl_adspend where adspend > 0 and report_week = '2021-12-26' group by 1) as b on a.supplier_name = b.supplier_name 
order by 4 desc;



select 
        trunc(cast(report_date as date), 'iw') + 6 report_week,
        a.supplier_name,
        b.team_cohort,
        campaign_sub_type,
        sum(adspend) adspend, 
        sum(impressions) impressions,
        sum(clicks) clicks,
        sum(conversions) conversions,
        sum(unitsales) unitsales,
        sum(sales_revenue) sales_revenue
  from sandbox.np_sl_ad_snapshot AS a
  left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name
  group by 1,2,3,4
  order by 1 desc;

 
select 
      a.*, 
      b.impressions_tot_2, 
      b.clicks_tot_2, 
      b.ctr_2,
      a.ctr - b.ctr_2
from 
(select a.supplier_id, 
        a.supplier_name, 
        b.team_cohort, 
        a.l2,
        sum(impressions) impressions_tot, 
        sum(clicks) clicks_tot, 
        cast(clicks_tot as float)/nullifzero(impressions_tot) ctr
 from sandbox.np_supplier_l3_agg as a 
 left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name
 where report_month = '2021-09-01'
 group by 1,2,3, 4) as a 
left join 
(select a.supplier_id, 
        a.supplier_name, 
        b.team_cohort, 
        a.l2,
        sum(impressions) impressions_tot_2, 
        sum(clicks) clicks_tot_2, 
        cast(clicks_tot_2 as float)/nullifzero(impressions_tot_2) ctr_2
 from sandbox.np_supplier_l3_agg as a 
 left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name
 where report_month = '2021-08-01'
 group by 1,2,3, 4) as b on a.supplier_id = b.supplier_id
 where a.l2 = 'L2 - Retail'
 order by a.ctr - b.ctr_2 desc
 */
 
 
----without campaign subtype
select 
        trunc(cast(report_date as date), 'iw') + 6 report_week,
        a.supplier_name,
        b.team_cohort,
        max(c.l1) l1,
        max(c.l2) l2,
        max(c.l3) l3,
        sum(adspend) adspend, 
        sum(impressions) impressions,
        sum(clicks) clicks,
        sum(conversions) conversions,
        sum(unitsales) unitsales,
        sum(sales_revenue) sales_revenue, 
        cast(sum(sales_revenue) as float)/NULLIFZERO(sum(adspend)) roas, 
        case when roas > 1 then 1 else 0 end roas_more_than_1, 
        case when roas > 1.5 then 1 else 0 end roas_more_than_15, 
        case when sum(adspend) > 0 then 1 else 0 end merchants_with_adspend, 
        case when sum(adspend) > 100 then 1 else 0 end merchants_adspend_more_than_100
  from sandbox.np_sl_ad_snapshot AS a
  left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name
  left join (select 
                 deal_id, 
                 max(grt_l1_cat_name) l1, 
                 max(grt_l2_cat_name) l2, 
                 max(grt_l3_cat_name) l3  
             from user_edwprod.dim_gbl_deal_lob 
             group by 1) as c on a.sku = c.deal_id
  group by 1,2,3



        
        
select 
    report_week, 
    cast(sum(ads_more_than_1) as float)/sum(merchants_with_adspend) per_merch
    from
(select 
        trunc(cast(report_date as date), 'iw') + 6 report_week,
        a.supplier_name,
        b.team_cohort,
        sum(adspend) adspend, 
        sum(impressions) impressions,
        sum(clicks) clicks,
        sum(conversions) conversions,
        sum(unitsales) unitsales,
        sum(sales_revenue) sales_revenue, 
        cast(sum(adspend) as float)/NULLIFZERO(sum(clicks)) cpc,
        case when cpc >= 0.5 then 1 else 0 end ads_more_than_1, 
        case when sum(adspend) > 0 then 1 else 0 end merchants_with_adspend
  from sandbox.np_sl_ad_snapshot AS a
  left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name
  group by 1,2,3) as fin
  group by 1 order by 1 desc;

 

select 
        cast(report_date as date) - EXTRACT(DAY FROM cast(report_date as date)) + 1 report_month,
        a.supplier_name,
        a.campaign_name,
        a.campaign_id,
        a.campaign_status,
        b.team_cohort,
        max(c.l1) l1,
        max(c.l2) l2,
        max(c.l3) l3,
        sum(adspend) adspend, 
        sum(impressions) impressions,
        sum(clicks) clicks,
        sum(conversions) conversions,
        sum(unitsales) unitsales,
        sum(sales_revenue) sales_revenue, 
        cast(sum(sales_revenue) as float)/NULLIFZERO(sum(adspend)) roas,
        case when roas > 1 then 1 else 0 end roas_more_than_1, 
        case when roas > 1.5 then 1 else 0 end roas_more_than_15, 
        case when sum(adspend) > 0 then 1 else 0 end merchants_with_adspend, 
        case when sum(adspend) > 100 then 1 else 0 end merchants_adspend_more_than_100,
        case when sum(adspend) > 1000 then 1 else 0 end merchants_adspend_more_than_1000
  from sandbox.np_sl_ad_snapshot AS a
  left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name
  left join (select 
                 deal_id, 
                 max(grt_l1_cat_name) l1, 
                 max(grt_l2_cat_name) l2, 
                 max(grt_l3_cat_name) l3  
             from user_edwprod.dim_gbl_deal_lob 
             group by 1) as c on a.sku = c.deal_id
  where cast(report_date as date) - EXTRACT(DAY FROM cast(report_date as date)) + 1 = '2021-11-01'
  group by 1,2,3,4,5,6;
 


/*
select 
    distinct
    a.supplier_name, 
    wallet_name, 
    wallet_id, 
    team_cohort
from sandbox.np_groupon_wallet_sl as a
left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name
order by team_cohort;*/
 
------------------------RETENTION DEEPDIVE


select 
    x.next_report_week,
	x.report_week,
	x.team_cohort,
	count(x.supplier_name) total_supp_adspend,
	count(distinct y.supplier_name) retained_supp_adspend, 
	cast(retained_supp_adspend as float)/nullifzero(total_supp_adspend) retention_rate
from 
    (select
        trunc(cast(report_date as date), 'iw') + 6 report_week,
        trunc(cast(report_date as date), 'iw') + 34 next_report_week,
        trunc(cast(report_date as date), 'iw') - 29 last_report_week, 
        a.supplier_name, 
        b.team_cohort,
        sum(adspend) all_adspend 
     from sandbox.np_sl_ad_snapshot a
         left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name 
         group by 1,2,3,4,5
         having all_adspend > 0) as x
left join 
     (select
     	trunc(cast(report_date as date), 'iw') + 6 report_week,
        trunc(cast(report_date as date), 'iw') + 34 next_report_week,
        trunc(cast(report_date as date), 'iw') - 29 last_report_week,
        a.supplier_name, 
        sum(adspend) all_adspend 
     from sandbox.np_sl_ad_snapshot a
         left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name 
         group by 1,2,3,4
         having all_adspend > 0) as y on x.supplier_name = y.supplier_name and x.next_report_week = y.report_week
group by 1,2,3
order by 1 desc, 3;
 
select * from sandbox.np_sl_ad_snapshot;

-------------------------------------------------------------------------------------------------------------------------------------OKR NUMBERS
-------MONTHLY
select 
      'roas_more_than_one_deals' okr_category,
      team_cohort,
      cast(sum(case when roas > 1 then 1 else 0 end) as float)/sum(case when total_adspend2 > 0 then 1 else 0 end) per_campaign_roas_one
      from 
        (select sku, 
                team_cohort,
                sum(adspend) total_adspend2, 
                sum(sales_revenue) sales_revenue2, 
                cast(sales_revenue2 as float)/NULLIFZERO(total_adspend2) roas 
          from sandbox.np_sl_ad_snapshot as a
               left join sandbox.np_sl_supp_mapping as d on a.supplier_name = d.supplier_name 
          where d.team_cohort <> 'Test' and cast(report_date as date) >= cast('2022-04-01' as date) and cast(report_date as date) < cast('2022-07-01' as date)
          group by 1,2) as a
      where total_adspend2 > 0 
      group by 1,2

select 
      'roas_more_than_one_campaigns' okr_category,
      'QTD' quaterly, 
      cast(sum(case when roas > 1 then 1 else 0 end) as float)/sum(case when total_adspend2 > 0 then 1 else 0 end) per_campaign_roas_one
      from 
        (select campaign_id, 
                sum(adspend) total_adspend2, 
                sum(sales_revenue) sales_revenue2, 
                cast(sales_revenue2 as float)/NULLIFZERO(total_adspend2) roas 
          from sandbox.np_sl_ad_snapshot as a
               left join sandbox.np_sl_supp_mapping as d on a.supplier_name = d.supplier_name 
          where d.team_cohort <> 'Test' and cast(report_date as date) >= cast('2022-04-01' as date) and cast(report_date as date) < cast('2022-07-01' as date)
          group by 1) as a
      where total_adspend2 > 0 
      group by 1,2;
     

     

select 
    cast('merchants_retention' as varchar(120)) okr_category,
    'QTD' quaterly, 
    trunc(cast('2022-04-27' as date), 'iw') - 29,
    trunc(cast('2022-04-27' as date), 'iw')-1,
    count(distinct y.supplier_name),
    count(distinct x.supplier_name),
    cast(count(distinct y.supplier_name) as float)/cast(count(distinct x.supplier_name) as float) "values"
from 
    (select
        a.supplier_name, 
        sum(adspend) all_adspend 
     from sandbox.np_sl_ad_snapshot a
         left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name 
         where trunc(cast(report_date as date), 'iw') + 6 = trunc(cast('2022-04-27' as date), 'iw') - 29
         having all_adspend > 0
         group by 1) as x
left join 
     (select
        a.supplier_name, 
        sum(adspend) all_adspend 
     from sandbox.np_sl_ad_snapshot a
         left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name 
         where trunc(cast(report_date as date), 'iw') + 6 = trunc(cast('2022-04-27' as date), 'iw')-1
         having all_adspend > 0
         group by 1) as y on x.supplier_name = y.supplier_name;
        
select 
      'roas_more_than_one_campaigns' okr_category,
      'QTD' quaterly, 
      cast(sum(case when roas > 1 then 1 else 0 end) as float)/sum(case when total_adspend2 > 0 then 1 else 0 end) per_campaign_roas_one
      from 
        (select campaign_id, 
                sum(adspend) total_adspend2, 
                sum(sales_revenue) sales_revenue2, 
                cast(sales_revenue2 as float)/NULLIFZERO(total_adspend2) roas 
          from sandbox.np_sl_ad_snapshot as a
               left join sandbox.np_sl_supp_mapping as d on a.supplier_name = d.supplier_name 
          where d.team_cohort <> 'Test' and cast(report_date as date) >= cast('2022-01-01' as date) and cast(report_date as date) < cast('2022-04-01' as date)
          group by 1) as a
      where total_adspend2 > 0 
      group by 1,2;
      

select
       cast('merchants_with_adspend' as varchar(120)) okr_category,
       'QTD' quaterly, 
       cast(count(distinct supplier_name) as float) "values"
       from 
(select cast(report_date as date) - EXTRACT(DAY FROM cast(report_date as date)) + 1 report_month, 
        a.supplier_name, 
        b.team_cohort ,
        sum(adspend) all_adspend 
     from sandbox.np_sl_ad_snapshot a
         left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name 
          group by 1,2,3) as a
where all_adspend >0 and team_cohort <> 'Test' and cast(report_month as date) >= cast('2022-01-01' as date) and cast(report_month as date) < cast('2022-02-01' as date)
group by 1,2;


select 
       'SS_merchants_with_adspend' okr_category,
       'QTD' quaterly, 
       count(distinct supplier_name)
       from 
(select cast(report_date as date) - EXTRACT(DAY FROM cast(report_date as date)) + 1 report_month, 
        a.supplier_name, 
        b.team_cohort ,
        sum(adspend) all_adspend 
     from sandbox.np_sl_ad_snapshot a
         left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name 
          group by 1,2,3) as a
where all_adspend >0 and team_cohort = 'SelfServe' and cast(report_month as date) >= cast('2022-01-01' as date) and cast(report_month as date) < cast('2022-02-01' as date)
group by 1,2

------------------------------------------------------------------ROAS QUARTER
         select          
                team_cohort, 
                sum(adspend) total_adspend2, 
                sum(sales_revenue) sales_revenue2, 
                cast(sales_revenue2 as float)/NULLIFZERO(total_adspend2) roas 
          from sandbox.np_sl_ad_snapshot as a
               left join sandbox.np_sl_supp_mapping as d on a.supplier_name = d.supplier_name 
          where d.team_cohort <> 'Test' and cast(report_date as date) >= cast('2022-01-01' as date) and cast(report_date as date) < cast('2022-04-01' as date)
          group by 1

------------------------------------------------------------------AGG
select * from sandbox.np_sl_ad_snapshot;

   
select
       cast('merchants_with_adspend' as varchar(120)) okr_category,
       'QTD' quaterly, 
       cast(count(distinct supplier_name) as float) "values"
       from 
(select cast(report_date as date) - EXTRACT(DAY FROM cast(report_date as date)) + 1 report_month, 
        a.supplier_name, 
        b.team_cohort ,
        sum(adspend) all_adspend 
     from sandbox.np_sl_ad_snapshot a
         left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name 
          group by 1,2,3) as a
where all_adspend >0 and team_cohort <> 'Test' and cast(report_month as date) >= cast('2022-04-01' as date) and cast(report_month as date) < cast('2022-07-01' as date)
group by 1,2
union 
select 
    cast('merchants_retention' as varchar(120)) okr_category,
    'QTD' quaterly, 
    cast(count(distinct y.supplier_name) as float)/cast(count(distinct x.supplier_name) as float) "values"
from 
    (select
        a.supplier_name, 
        sum(adspend) all_adspend 
     from sandbox.np_sl_ad_snapshot a
         left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name 
         where trunc(cast(report_date as date), 'iw') + 6 = trunc(cast(current_date as date), 'iw') - 29
         having all_adspend > 0
         group by 1) as x
left join 
     (select
        a.supplier_name, 
        sum(adspend) all_adspend 
     from sandbox.np_sl_ad_snapshot a
         left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name 
         where trunc(cast(report_date as date), 'iw') + 6 = trunc(cast(current_date as date), 'iw')-1
         having all_adspend > 0
         group by 1) as y on x.supplier_name = y.supplier_name
union 
select 
       'SS_merchants_with_adspend' okr_category,
       'QTD' quaterly, 
       count(distinct supplier_name)
       from 
(select cast(report_date as date) - EXTRACT(DAY FROM cast(report_date as date)) + 1 report_month, 
        a.supplier_name, 
        b.team_cohort ,
        sum(adspend) all_adspend 
     from sandbox.np_sl_ad_snapshot a
         left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name 
          group by 1,2,3) as a
where all_adspend >0 and team_cohort = 'SelfServe' and cast(report_month as date) >= cast('2022-04-01' as date) and cast(report_month as date) < cast('2022-07-01' as date)
group by 1,2
union
select 
      'roas_more_than_one_supplier' okr_category,
      'QTD' quaterly, 
      cast(sum(case when roas > 1 then 1 else 0 end) as float)/sum(case when total_adspend2 > 0 then 1 else 0 end) per_campaign_roas_one
      from 
        (select supplier_id, 
                sum(adspend) total_adspend2, 
                sum(sales_revenue) sales_revenue2, 
                cast(sales_revenue2 as float)/NULLIFZERO(total_adspend2) roas 
          from sandbox.np_sl_ad_snapshot as a
               left join sandbox.np_sl_supp_mapping as d on a.supplier_name = d.supplier_name 
          where d.team_cohort <> 'Test' and cast(report_date as date) >= cast('2022-04-01' as date) and cast(report_date as date) < cast('2022-07-01' as date)
          group by 1) as a
      where total_adspend2 > 0 
      group by 1,2
union 
select 
      'roas_more_than_one_campaigns' okr_category,
      'QTD' quaterly, 
      cast(sum(case when roas > 1 then 1 else 0 end) as float)/sum(case when total_adspend2 > 0 then 1 else 0 end) per_campaign_roas_one
      from 
        (select campaign_id, 
                sum(adspend) total_adspend2, 
                sum(sales_revenue) sales_revenue2, 
                cast(sales_revenue2 as float)/NULLIFZERO(total_adspend2) roas 
          from sandbox.np_sl_ad_snapshot as a
               left join sandbox.np_sl_supp_mapping as d on a.supplier_name = d.supplier_name 
          where d.team_cohort <> 'Test' and cast(report_date as date) >= cast('2022-04-01' as date) and cast(report_date as date) < cast('2022-07-01' as date)
          group by 1) as a
      where total_adspend2 > 0 
      group by 1,2
union 
select 
       'merchants_adspend_on_groupon' okr_category,
       'QTD' quaterly, 
       sum(all_adspend) adspend
       from 
(select cast(report_date as date) - EXTRACT(DAY FROM cast(report_date as date)) + 1 report_month, 
        a.supplier_name, 
        b.team_cohort ,
        sum(adspend) all_adspend 
     from sandbox.np_sl_ad_snapshot a
         left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name 
          group by 1,2,3) as a
          where team_cohort <> 'Test' and cast(report_month as date) >= cast('2022-04-01' as date) and cast(report_month as date) < cast('2022-07-01' as date)
group by 1,2      
      

sel * from sandbox.np_sl_ad_snapshot order by report_date desc;

delete from sandbox.np_sl_ad_snapshot
where 
  report_date = '2022-05-16 TOTAL';


drop table np_sl_supp_name;
create volatile table np_sl_supp_name as 
(select 
 cast(report_date as date) report_week, 
 a.supplier_name,
 b.team_cohort,
 sum(a.available_balance) available_balance_sum
from sandbox.np_groupon_wallet_sl as a
 left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name
where archived = 'false'
 group by 1,2,3
) with data on commit preserve rows




select 
  a.*, 
  case when b.supplier_name is not null then 1 else 0 end, 
  b.available_balance_sum, 
  a.available_balance_sum-b.available_balance_sum
from 
(select 
   * 
from 
   np_sl_supp_name 
   where team_cohort = 'Enterprise' and report_week = '2021-09-12'
) as a 
left join 
(select 
   * 
from 
   np_sl_supp_name 
   where team_cohort = 'Enterprise' and report_week = '2021-09-19'
) as b on a.supplier_name = b.supplier_name
order by 
  7 desc;

 
 
 select 
    a.*, 
    case when b.supplier_id is not null then 1 else 0 end merchant_retained
    from 
(select 
     a.supplier_id,
     b.team_cohort
     from
     sandbox.np_supplier_l3_agg as a
     left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name
     where report_month = cast('2021-10-01' as date)
          AND total_adspend > 0
     group by 1,2) as a 
 left join 
(select 
     supplier_id
     from
     sandbox.np_supplier_l3_agg
     where report_month = cast('2021-11-01' as date)
          AND total_adspend > 0
     group by 1) as b on a.supplier_id = b.supplier_id;

/*
select
       cast('merchants_with_adspend' as varchar(120)) okr_category,
       'QTD' quaterly, 
       cast(count(distinct supplier_name) as float) "values"
       from 
(select cast(report_date as date) - EXTRACT(DAY FROM cast(report_date as date)) + 1 report_month, 
        a.supplier_name, 
        b.team_cohort ,
        sum(adspend) all_adspend 
     from sandbox.np_sl_ad_snapshot a
         left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name 
          group by 1,2,3) as a
where all_adspend >0 and team_cohort <> 'Test' and report_month = '2021-09-01' 



select 
       'SS_merchants_with_adspend' okr_category,
       'QTD' quaterly, 
       count(distinct supplier_name)
       from 
(select cast(report_date as date) - EXTRACT(DAY FROM cast(report_date as date)) + 1 report_month, 
        a.supplier_name, 
        b.team_cohort ,
        sum(adspend) all_adspend 
     from sandbox.np_sl_ad_snapshot a
         left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name 
          group by 1,2,3) as a
where all_adspend >0 and team_cohort = 'SelfServe' and report_month = '2021-09-01' 


select 
       'merchants_adspend_on_groupon' okr_category,
       'QTD' quaterly, 
       sum(all_adspend) adspend
       from 
(select cast(report_date as date) - EXTRACT(DAY FROM cast(report_date as date)) + 1 report_month, 
        a.supplier_name, 
        b.team_cohort ,
        sum(adspend) all_adspend 
     from sandbox.np_sl_ad_snapshot a
         left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name 
          group by 1,2,3) as a
          where team_cohort <> 'Test' and report_month = '2021-09-01' 
group by 1,2


select 
       'monthly_ctr' okr_category,
       'QTD' quaterly, 
       cast(sum(clicks) as float)/sum(impressions) clicks_per_impressions
       from 
(select cast(report_date as date) - EXTRACT(DAY FROM cast(report_date as date)) + 1 report_month, 
        a.supplier_name, 
        b.team_cohort ,
        sum(impressions) impressions,
         sum(clicks) clicks
     from sandbox.np_sl_ad_snapshot a
         left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name 
          group by 1,2,3
) as a
          where team_cohort <> 'Test' and report_month = cast('2021-09-01' as date)
group by 1,2



select 
       'monthly_cpc' okr_category,
       'QTD' quaterly, 
       cast(sum(total_adspend) as float)/sum(clicks) cost_per_clicks
       from 
(
select cast(report_date as date) - EXTRACT(DAY FROM cast(report_date as date)) + 1 report_month, 
        a.supplier_name, 
        b.team_cohort ,
        sum(adspend) total_adspend, 
        sum(clicks) clicks
     from sandbox.np_sl_ad_snapshot a
         left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name 
          group by 1,2,3
) as a
          where team_cohort <> 'Test' and report_month = '2021-09-01' 
group by 1,2


select 
      'roas_more_than_one_campaigns' okr_category,
      'QTD' quaterly, 
      cast(sum(case when roas > 1 then 1 else 0 end) as float)/sum(case when total_adspend2 > 0 then 1 else 0 end) per_campaign_roas_one
      from 
        (select campaign_id, 
                sum(adspend) total_adspend2, 
                sum(sales_revenue) sales_revenue2, 
                cast(sales_revenue2 as float)/NULLIFZERO(total_adspend2) roas 
          from sandbox.np_sl_ad_snapshot as a
               left join sandbox.np_sl_supp_mapping as d on a.supplier_name = d.supplier_name 
          where d.team_cohort <> 'Test' and cast(report_date as date) >= cast('2021-09-01' as date) and cast(report_date as date) < cast('2021-10-01' as date)
          group by 1) as a
      where total_adspend2 > 0 
      group by 1,2

-------Q3 OKRs
      
      
select 
      team_cohort,
      report_month,
      cast(sum(case when roas > 1 then 1 else 0 end) as float)/sum(case when total_adspend2 > 0 then 1 else 0 end) per_campaign_roas_one
      from 
        (select supplier_id, 
                team_cohort,
                cast(report_date as date) - EXTRACT(DAY FROM cast(report_date as date)) + 1 report_month, 
                sum(adspend) total_adspend2, 
                sum(sales_revenue) sales_revenue2, 
                cast(sales_revenue2 as float)/NULLIFZERO(total_adspend2) roas 
          from sandbox.np_sl_ad_snapshot as a
               left join sandbox.np_sl_supp_mapping as d on a.supplier_name = d.supplier_name 
          where d.team_cohort <> 'Test' and cast(report_date as date) >= cast('2021-07-01' as date)
          group by 1,2,3) as a
      where total_adspend2 > 0 
      group by 1,2
      order by 1,2;
     
select
       cast('merchants_with_adspend' as varchar(120)) okr_category,
       'QTD' quaterly, 
       cast(count(distinct supplier_name) as float) "values"
       from 
(select cast(report_date as date) - EXTRACT(DAY FROM cast(report_date as date)) + 1 report_month, 
        a.supplier_name, 
        b.team_cohort ,
        sum(adspend) all_adspend 
     from sandbox.np_sl_ad_snapshot a
         left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name 
          group by 1,2,3) as a
where all_adspend >0 and team_cohort <> 'Test' and report_month >= '2021-07-01' and cast(report_month as date) < cast('2021-10-01' as date)
group by 1,2
union 
select 
       'SS_merchants_with_adspend' okr_category,
       'QTD' quaterly, 
       count(distinct supplier_name)
       from 
(select cast(report_date as date) - EXTRACT(DAY FROM cast(report_date as date)) + 1 report_month, 
        a.supplier_name, 
        b.team_cohort ,
        sum(adspend) all_adspend 
     from sandbox.np_sl_ad_snapshot a
         left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name 
          group by 1,2,3) as a
where all_adspend >0 and team_cohort = 'SelfServe' and report_month >= '2021-07-01' and cast(report_month as date) < cast('2021-10-01' as date)
group by 1,2
union 
select 
       'merchants_adspend_on_groupon' okr_category,
       'QTD' quaterly, 
       sum(all_adspend) adspend
       from 
(select cast(report_date as date) - EXTRACT(DAY FROM cast(report_date as date)) + 1 report_month, 
        a.supplier_name, 
        b.team_cohort ,
        sum(adspend) all_adspend 
     from sandbox.np_sl_ad_snapshot a
         left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name 
          group by 1,2,3) as a
          where team_cohort <> 'Test' and report_month >= '2021-07-01' and cast(report_month as date) < cast('2021-10-01' as date)
group by 1,2
union 
select 
       'merchants_SL_sales_revenue' okr_category,
       'QTD' quaterly, 
       sum(sales_revenue) sales_revenue
       from 
(select cast(report_date as date) - EXTRACT(DAY FROM cast(report_date as date)) + 1 report_month, 
        a.supplier_name, 
        b.team_cohort ,
        sum(sales_revenue) sales_revenue
     from sandbox.np_sl_ad_snapshot a
         left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name 
          group by 1,2,3) as a
          where team_cohort <> 'Test' and report_month >= '2021-07-01' and cast(report_month as date) < cast('2021-10-01' as date)
group by 1,2
union 
select 
       'monthly_cpc' okr_category,
       'QTD' quaterly, 
       cast(sum(total_adspend) as float)/sum(clicks) cost_per_clicks
       from 
(
select cast(report_date as date) - EXTRACT(DAY FROM cast(report_date as date)) + 1 report_month, 
        a.supplier_name, 
        b.team_cohort ,
        sum(adspend) total_adspend, 
        sum(clicks) clicks
     from sandbox.np_sl_ad_snapshot a
         left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name 
          group by 1,2,3
) as a
          where team_cohort <> 'Test' and report_month >= '2021-07-01' and cast(report_month as date) < cast('2021-10-01' as date)
group by 1,2
union 
select 
       'monthly_ctr' okr_category,
       'QTD' quaterly, 
       cast(sum(clicks) as float)/sum(impressions) clicks_per_impressions
       from 
(select cast(report_date as date) - EXTRACT(DAY FROM cast(report_date as date)) + 1 report_month, 
        a.supplier_name, 
        b.team_cohort ,
        sum(impressions) impressions,
         sum(clicks) clicks
     from sandbox.np_sl_ad_snapshot a
         left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name 
          group by 1,2,3
) as a
          where team_cohort <> 'Test' and report_month >= cast('2021-07-01' as date) and cast(report_month as date) < cast('2021-10-01' as date)
group by 1,2
union
select 
      'roas_more_than_one_supplier' okr_category,
      'QTD' quaterly, 
      cast(sum(case when roas > 1 then 1 else 0 end) as float)/sum(case when total_adspend2 > 0 then 1 else 0 end) per_campaign_roas_one
      from 
        (select supplier_id, 
                sum(adspend) total_adspend2, 
                sum(sales_revenue) sales_revenue2, 
                cast(sales_revenue2 as float)/NULLIFZERO(total_adspend2) roas 
          from sandbox.np_sl_ad_snapshot as a
               left join sandbox.np_sl_supp_mapping as d on a.supplier_name = d.supplier_name 
          where d.team_cohort <> 'Test' and cast(report_date as date) >= cast('2021-07-01' as date) and cast(report_date as date) < cast('2021-10-01' as date)
          group by 1) as a
      where total_adspend2 > 0 
      group by 1,2
union 
select 
      'roas_more_than_one_campaigns' okr_category,
      'QTD' quaterly, 
      cast(sum(case when roas > 1 then 1 else 0 end) as float)/sum(case when total_adspend2 > 0 then 1 else 0 end) per_campaign_roas_one
      from 
        (select campaign_id, 
                sum(adspend) total_adspend2, 
                sum(sales_revenue) sales_revenue2, 
                cast(sales_revenue2 as float)/NULLIFZERO(total_adspend2) roas 
          from sandbox.np_sl_ad_snapshot as a
               left join sandbox.np_sl_supp_mapping as d on a.supplier_name = d.supplier_name 
          where d.team_cohort <> 'Test' and cast(report_date as date) >= cast('2021-07-01' as date) and cast(report_date as date) < cast('2021-10-01' as date)
          group by 1) as a
      where total_adspend2 > 0 
      group by 1,2
union 
select 
    'MoM Retention' okr_category, 
    '2021-07-01'  report_month, 
    cast(count(b.supplier_id) as float)/count(a.supplier_id) retention_rate
    from 
(select 
     supplier_id
     from
     sandbox.np_supplier_l3_agg
     where report_month = cast('2021-08-01' as date)
          AND total_adspend > 0
     group by 1) as a 
 left join 
(select 
     supplier_id
     from
     sandbox.np_supplier_l3_agg
     where report_month = cast('2021-09-01' as date)
          AND total_adspend > 0
     group by 1) as b on a.supplier_id = b.supplier_id
group by 1,2
;



select 
       'Q 2' quaterly, 
       campaign_sub_type,
       cast(cast(sum(clicks) as float)/sum(impressions) as decimal(5, 5)) clicks_per_impressions, 
       sum(clicks) clicks, 
       sum(impressions) impressions
       from 
(select cast(report_date as date) - EXTRACT(DAY FROM cast(report_date as date)) + 1 report_month, 
        campaign_sub_type, 
        b.team_cohort ,
        sum(impressions) impressions,
         sum(clicks) clicks
     from sandbox.np_sl_ad_snapshot a
         left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name 
          group by 1,2,3
) as a
          where team_cohort <> 'Test' and report_month >= cast('2021-04-01' as date) and report_month < cast('2021-07-01' as date)
          group by 2
union 
select 
       'Q 3' quaterly, 
       campaign_sub_type,
       cast(cast(sum(clicks) as float)/sum(impressions) as decimal(5, 5)) clicks_per_impressions, 
       sum(clicks) clicks, 
       sum(impressions) impressions
       from 
(select cast(report_date as date) - EXTRACT(DAY FROM cast(report_date as date)) + 1 report_month, 
        campaign_sub_type, 
        b.team_cohort ,
        sum(impressions) impressions,
         sum(clicks) clicks
     from sandbox.np_sl_ad_snapshot a
         left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name 
          group by 1,2,3
) as a
          where team_cohort <> 'Test' and report_month >= cast('2021-07-01' as date)
          group by 2;


         
         
         
         
         */



         
----------------------------------------------------------------------------------------------------------------

         

         
--------OLD MONTHLY
/*

select
       cast('merchants_with_adspend' as varchar(120)) okr_category,
       'QTD' quaterly, 
       cast(count(distinct supplier_name) as float) "values"
       from 
(select cast(report_date as date) - EXTRACT(DAY FROM cast(report_date as date)) + 1 report_month, 
        a.supplier_name, 
        b.team_cohort ,
        sum(adspend) all_adspend 
     from sandbox.np_sl_ad_snapshot a
         left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name 
          group by 1,2,3) as a
where all_adspend >0 and team_cohort <> 'Test' and report_month >= '2021-07-01'
group by 1,2
union 
select 
       'SS_merchants_with_adspend' okr_category,
       'QTD' quaterly, 
       count(distinct supplier_name)
       from 
(select cast(report_date as date) - EXTRACT(DAY FROM cast(report_date as date)) + 1 report_month, 
        a.supplier_name, 
        b.team_cohort ,
        sum(adspend) all_adspend 
     from sandbox.np_sl_ad_snapshot a
         left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name 
          group by 1,2,3) as a
where all_adspend >0 and team_cohort = 'SelfServe' and report_month >= '2021-07-01'
group by 1,2
union 
select 
       'merchants_adspend_on_groupon' okr_category,
       'QTD' quaterly, 
       sum(all_adspend) adspend
       from 
(select cast(report_date as date) - EXTRACT(DAY FROM cast(report_date as date)) + 1 report_month, 
        a.supplier_name, 
        b.team_cohort ,
        sum(adspend) all_adspend 
     from sandbox.np_sl_ad_snapshot a
         left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name 
          group by 1,2,3) as a
          where team_cohort <> 'Test' and report_month >= '2021-07-01'
group by 1,2
union 
select 
       'merchants_SL_sales_revenue' okr_category,
       'QTD' quaterly, 
       sum(sales_revenue) sales_revenue
       from 
(select cast(report_date as date) - EXTRACT(DAY FROM cast(report_date as date)) + 1 report_month, 
        a.supplier_name, 
        b.team_cohort ,
        sum(sales_revenue) sales_revenue
     from sandbox.np_sl_ad_snapshot a
         left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name 
          group by 1,2,3) as a
          where team_cohort <> 'Test' and report_month >= '2021-07-01'
group by 1,2
union 
select 
       'monthly_cpc' okr_category,
       'QTD' quaterly, 
       cast(sum(total_adspend) as float)/sum(clicks) cost_per_clicks
       from 
(
select cast(report_date as date) - EXTRACT(DAY FROM cast(report_date as date)) + 1 report_month, 
        a.supplier_name, 
        b.team_cohort ,
        sum(adspend) total_adspend, 
        sum(clicks) clicks
     from sandbox.np_sl_ad_snapshot a
         left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name 
          group by 1,2,3
) as a
          where team_cohort <> 'Test' and report_month >= '2021-07-01'
group by 1,2
union 
select 
       'monthly_ctr' okr_category,
       'QTD' quaterly, 
       cast(sum(clicks) as float)/sum(impressions) cost_per_clicks
       from 
(select cast(report_date as date) - EXTRACT(DAY FROM cast(report_date as date)) + 1 report_month, 
        a.supplier_name, 
        b.team_cohort ,
        sum(impressions) impressions,
         sum(clicks) clicks
     from sandbox.np_sl_ad_snapshot a
         left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name 
          group by 1,2,3
) as a
          where team_cohort <> 'Test' and report_month >= '2021-07-01'
group by 1,2
union 
select 
      'roas_more_than_one' okr_category,
      'QTD' quaterly, 
      cast(sum(case when roas > 1 then 1 else 0 end) as float)/sum(case when total_adspend > 0 then 1 else 0 end) per_supp_roas_one
      from 
      sandbox.np_supplier_l3_agg 
      where total_adspend > 0 and team_cohort <> 'Test'
      group by 1,2
union 
select 
      'roas_more_than_one_campaigns' okr_category,
      'QTD' quaterly, 
      cast(sum(case when roas > 1 then 1 else 0 end) as float)/sum(case when total_adspend2 > 0 then 1 else 0 end) per_campaign_roas_one
      from 
        (select cast(report_date as date) - EXTRACT(DAY FROM cast(report_date as date)) + 1 report_month, 
                campaign_id, 
                sum(adspend) total_adspend2, 
                sum(sales_revenue) sales_revenue2, 
                cast(sales_revenue2 as float)/NULLIFZERO(total_adspend2) roas 
          from sandbox.np_sl_ad_snapshot as a
               left join sandbox.np_sl_supp_mapping as d on a.supplier_name = d.supplier_name 
          where d.team_cohort <> 'Test'
          group by 1,2) as a
      where total_adspend2 > 0 and report_month >= '2021-07-01'
      group by 1,2
union 
select 
    'MoM Retention' okr_category, 
    '2021-07-01'  report_month, 
    cast(count(b.supplier_id) as float)/count(a.supplier_id) retention_rate
    from 
(select 
     supplier_id
     from
     sandbox.np_supplier_l3_agg
     where report_month = cast('2021-06-01' as date)
          AND total_adspend > 0
     group by 1) as a 
 left join 
(select 
     supplier_id
     from
     sandbox.np_supplier_l3_agg
     where report_month = cast('2021-07-01' as date)
          AND total_adspend > 0
     group by 1) as b on a.supplier_id = b.supplier_id
group by 1,2
;

*/
-------------------------------------------------------------------------------------------------------------------------ADHOCS for specific reports

(select a.*, 
       b.contract_sell_price , 
       b.contract_buy_price , 
       (b.contract_sell_price - b.contract_buy_price)/NULLIFZERO(b.contract_sell_price) groupon_margin, 
       case when sales_revenue * groupon_margin is null then 0 else sales_revenue * groupon_margin end groupon_deal_revenue
from 
     (select 
         report_month, 
         supplier_id,
         supplier_name,
         product_code, 
         sum(impressions) impressions,
         sum(clicks) clicks,
         sum(conversions) conversions,
         sum(unit_sales) unit_sales,
         sum(sales_revenue) sales_revenue,
         sum(impression_spend) impression_spend,
         sum(click_spend) click_spend,
         sum(total_adspend) total_adspend
     from sandbox.np_sl_summary_mtd as a 
     group by 1,2,3,4) as a
left join 
   (select 
        product_uuid, 
        avg(contract_sell_price) contract_sell_price,  
        avg(contract_buy_price) contract_buy_price 
     from user_edwprod.dim_offer_ext
     group by 1) as b on a.product_code = b.product_uuid);


select supplier_id, count(distinct supplier_name) cnz from sandbox.np_sl_summary_mtd group by 1 having cnz > 1;

select * from sandbox.np_sl_summary_mtd where product_code = 'de9b28e5-6393-497e-9e53-afb8a56dc2fd'

------------------------------------------------------------------------------------------------------------------------- OPS MAIN TABLES

/*




drop table sandbox.np_sl_wallet_spend_agg;
create table sandbox.np_sl_wallet_spend_agg as (
select 
    fin.*, 
    sn.adspend
from
    (select 
         a.report_date,
         a.supplier_name,
         b.team_cohort,
         count(distinct a.wallet_name) number_of_wallets, 
         sum(a.available_balance) available_balance_sum, 
         case when available_balance_sum <= 0 then 'a.<= 0 USD in the wallet'
          when available_balance_sum <= 250 then 'b.<= 250 USD in the wallet'
          when available_balance_sum <= 500 then 'c.<= 500 USD in the wallet'
          when available_balance_sum <= 1000 then 'd.<= 1000 USD in the wallet'
          when available_balance_sum <= 2000 then 'e.<= 2000 USD in the wallet'
          when available_balance_sum >2000 then 'f.more than 2000 usd in wallet'
          end available_balance_cohort, 
     case when lower(a.supplier_name) like '%test%' then 1 else 0 end test_account
from sandbox.np_groupon_wallet_sl as a
left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name
where cast(report_date as date) = trunc(cast(report_date as date), 'iw') + 6
group by 1,2,3,7) as fin 
left join 
     (select 
            trunc(cast(report_date as date), 'iw') + 6 report_week,
            supplier_name,
            sum(adspend) adspend
         from sandbox.np_sl_ad_snapshot 
         group by 1,2
     ) as sn on fin.report_date = sn.report_week and fin.supplier_name = sn.supplier_name
) with data;


drop table sandbox.np_sl_ad_snapshot_agg_supplier;
create table sandbox.np_sl_ad_snapshot_agg_supplier as (
select 
      trunc(cast(report_date as date), 'iw') + 6 report_week,
      a.supplier_name,
      a.region_namespace,
      b.team_cohort,
      a.campaign_sub_type,
      a.campaign_status,
      case when lower(a.supplier_name) like '%test%' then 1 else 0 end test_account,
      sum(a.impressions) impressions,
      sum(a.clicks) clicks,
      sum(a.adspend) adspend,
      sum(a.conversions) conversions,
      sum(a.unitsales) unitsales,
      sum(a.sales_revenue) sales_revenue
from sandbox.np_sl_ad_snapshot as a 
left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name
   group by 
      1,2,3,4,5,6) with data
;



drop table sandbox.np_sl_sup_bal;
create table sandbox.np_sl_sup_bal as (
select 
       fin.supplier_week_cat, 
       fin.supplier_name, 
       fin.team_cohort,
       fin.report_week, 
       case when fin.wallet_balance_this_week is null then 0 else fin.wallet_balance_this_week end wallet_balance_this_week2,
       case when fin.wallet_balance_prev_week is null then 0 else fin.wallet_balance_prev_week end wallet_balance_prev_week2,
       case when fin.adspend is null then 0 else fin.adspend end adspend2, 
       snp.adspend all_week_spend, 
       case when fin.supplier_week_cat in ('supplier_constant', 'supplier_added') 
            then wallet_balance_this_week2 + adspend2 - wallet_balance_prev_week2 
            else 0 end balance_added
 from
(select * from 
   (select 
         'supplier_constant' as supplier_week_cat,
         a.supplier_name,
         a.team_cohort,
         a.report_week, 
         a.available_balance_sum wallet_balance_this_week,
         b.available_balance_sum wallet_balance_prev_week, 
         c.adspend
     from 
     (select 
          cast(report_date as date) - 7 prev_week,
          cast(report_date as date) report_week, 
          cast(report_date as date) + 7 next_week,
          supplier_name,
          team_cohort,
          sum(available_balance_sum) available_balance_sum
      from 
          sandbox.np_sl_wallet_spend_agg 
          where test_account = 0
          group by 1,2,3,4,5) as a 
      join 
      (select 
          cast(report_date as date) - 7 prev_week,
          cast(report_date as date) report_week, 
          cast(report_date as date) + 7 next_week,
          supplier_name,
          sum(available_balance_sum) available_balance_sum
      from 
          sandbox.np_sl_wallet_spend_agg
          where test_account = 0
          group by 1,2,3,4) as b on a.report_week = b.next_week and a.supplier_name = b.supplier_name
       left join 
       (select 
           report_week,
           supplier_name,
           sum(adspend) adspend
       from 
           sandbox.np_sl_ad_snapshot_agg_supplier
       group by 1,2
       ) as c on a.report_week = c.report_week and a.supplier_name = c.supplier_name
  UNION ALL
   select 
         'supplier_added' as supplier_week_cat,
         a.supplier_name,
         a.team_cohort,
         a.report_week, 
         a.available_balance_sum wallet_balance_this_week,
         b.available_balance_sum wallet_balance_prev_week,
         c.adspend
       from 
		     (select 
		          cast(report_date as date) - 7 prev_week,
		          cast(report_date as date) report_week, 
		          cast(report_date as date) + 7 next_week,
		          supplier_name,
		          team_cohort,
		          sum(available_balance_sum) available_balance_sum
		      from 
		          sandbox.np_sl_wallet_spend_agg 
		          where test_account = 0
		          group by 1,2,3,4,5) as a 
        left join 
		      (select 
		          cast(report_date as date) - 7 prev_week,
		          cast(report_date as date) report_week, 
		          cast(report_date as date) + 7 next_week,
		          supplier_name,
		          sum(available_balance_sum) available_balance_sum
		      from 
		          sandbox.np_sl_wallet_spend_agg
		          where test_account = 0
		          group by 1,2,3,4) as b on a.report_week = b.next_week and a.supplier_name = b.supplier_name
        left join 
		       (select 
		           report_week,
		           supplier_name,
		           sum(adspend) adspend
		       from 
		           sandbox.np_sl_ad_snapshot_agg_supplier
		       group by 1,2
		       ) as c on a.report_week = c.report_week and a.supplier_name = c.supplier_name
         where b.next_week is null
  UNION ALL
    select 
         'supplier_lost' as supplier_week_cat,
         a.supplier_name,
         a.team_cohort,
         a.next_week week_lost,
         b.available_balance_sum wallet_balance_this_week,
         a.available_balance_sum wallet_balance_prev_week,
         c.adspend
       from 
		     (select 
		          cast(report_date as date) - 7 prev_week,
		          cast(report_date as date) report_week, 
		          cast(report_date as date) + 7 next_week,
		          supplier_name,
		          team_cohort,
		          sum(available_balance_sum) available_balance_sum
		      from 
		          sandbox.np_sl_wallet_spend_agg
		          where test_account = 0
		          group by 1,2,3,4,5) as a 
        left join 
		      (select 
		          cast(report_date as date) - 7 prev_week,
		          cast(report_date as date) report_week, 
		          cast(report_date as date) + 7 next_week,
		          supplier_name,
		          sum(available_balance_sum) available_balance_sum
		      from 
		          sandbox.np_sl_wallet_spend_agg
		          where test_account = 0
		          group by 1,2,3,4) as b on a.report_week = b.prev_week and a.supplier_name = b.supplier_name
        left join 
		       (select 
		           report_week,
		           supplier_name,
		           sum(adspend) adspend
		       from 
		           sandbox.np_sl_ad_snapshot_agg_supplier
		       group by 1,2
		       ) as c on c.report_week = a.next_week and a.supplier_name = c.supplier_name
         where b.prev_week is null
         ) as fin0
   ) as fin
left join 
      (select 
           report_week,
           supplier_name,
           sum(adspend) adspend
       from 
           sandbox.np_sl_ad_snapshot_agg_supplier
       group by 1,2
       ) as snp on fin.report_week = snp.report_week and fin.supplier_name = snp.supplier_name
) with data
;


-----------------------------------------------------------------------------------------------------------ANALYTICS


---LAST 30 DAYS ANALYTICS

create volatile table np_merchant_info as (
sel b.merchant_uuid, 
    b.product_uuid,
    max(c.country_code) country,
    max(sfa.id) account_id,
    max(sfa.name) account_name,
    max(grt_l2_cat_name) vertical,
    max(sfa.category_v3) sfa_vertical
from user_edwprod.dim_offer_ext as b 
     left join user_edwprod.dim_gbl_deal_lob as c on b.product_uuid = c.deal_id
     left join dwh_base_sec_view.opportunity_2 o2 on o2.deal_uuid = b.product_uuid
     left join dwh_base_sec_view.opportunity_1 o1 on o2.id = o1.id
     left join dwh_base_sec_view.sf_account sfa on o1.accountid = sfa.id
group by 1,2
) with data unique primary index(merchant_uuid, product_uuid) on commit preserve rows

drop table np_merch_redemption;
create volatile table np_merch_redemption as (
select 
    merchant_uuid, 
    merchant_name, 
    sum(deal_rev) merch_rev, 
    sum(deal_rev_30_dayfloat) merch_30_day_rev, 
    case when merch_rev is not null then merch_30_day_rev/merch_rev end red_rate_30
    from 
    sandbox.np_sl_deal_performance_base2
    where cast(eventdate as date) >= current_date - 30 
    group by 1,2
) with data on commit preserve rows;

select * from np_merch_redemption where lower(merchant_name) like '%laseraway%';


       
select 
    sup.*,
    red_rate_30, 
    mr.merchant_name, 
    lower(split_part(sup.supplier_name, '- ', 1)), 
    sup.supplier_name
from 
(select 
      a.supplier_name,
      a.region_namespace,
      b.team_cohort,
      case when lower(a.supplier_name) like '%test%' then 1 else 0 end test_account,
      sum(a.impressions) impressions,
      sum(a.clicks) clicks2,
      sum(a.adspend) adspend2,
      sum(a.conversions) conversions,
      sum(a.unitsales) unitsales,
      sum(a.sales_revenue) sales_revenue2,
      adspend2/NULLIFZERO(clicks2) cost_per_click,
      sales_revenue2/NULLIFZERO(clicks2) revenue_per_click
from sandbox.np_sl_ad_snapshot as a 
left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name
where report_date >= current_date - 30
   group by 
      1,2,3,4) as sup
left join 
   np_merch_redemption as mr on length( lower(split_part(sup.supplier_name, '- ', 1))) >5 and lower(mr.merchant_name) like CONCAT('%', lower(split_part(sup.supplier_name, '- ', 1)),'%');

  



  


drop table sandbox.np_sl_deal_performance_base2;
CREATE MULTISET TABLE sandbox.np_sl_deal_performance_base2 ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      eventdate varchar(64) CHARACTER SET UNICODE, 
   dealuuid varchar(100) CHARACTER SET UNICODE,
   deal_permalink varchar(100) CHARACTER SET UNICODE,
   merchant_uuid varchar(100) CHARACTER SET UNICODE,
   merch_segmentation varchar(100) CHARACTER SET UNICODE,
   merchant_name varchar(100) CHARACTER SET UNICODE,
   l1 varchar(100) CHARACTER SET UNICODE,
   l2 varchar(100) CHARACTER SET UNICODE,
   coupon varchar(100) CHARACTER SET UNICODE,
   deal_rev float, 
   deal_rev_same_day float, 
   deal_rev_30_day float, 
   deal_rev_120_day float, 
   deal_rev_all_red float,
   total_groupon_rev float,
   citrus_impressions float,
   citrus_clicks float,
   sli_impressions float, 
   sli_clicks float, 
   total_impression_aog float,
   orders_sold float)
NO PRIMARY INDEX ;






-----figure out suppliers which were lost last week

select 
    a.supplier_name,
    a.report_week this_week, 
    b.report_week previous_week,
    a.available_balance_sum wallet_balance_this_week,
    b.available_balance_sum balance_prev_week,
    c.adspend
from 
(select 
    cast(report_date as date) report_week, 
    supplier_name,
    sum(available_balance_sum) available_balance_sum
    from sandbox.np_sl_wallet_spend_agg
    group by 1,2) as a 
left join 
(select 
    cast(report_date as date) report_week, 
    supplier_name,
    sum(available_balance_sum) available_balance_sum
    from sandbox.np_sl_wallet_spend_agg
    group by 1,2) as b on a.report_week = b.report_week + 7 and a.supplier_name = b.supplier_name
left join 
(select 
    report_week,
    supplier_name,
    sum(adspend) adspend
from 
    sandbox.np_sl_ad_snapshot_agg_supplier
    group by 1,2
) as c on a.report_week = c.report_week and a.supplier_name = c.supplier_name
order by 2;




drop table sandbox.np_sl_lowbudget_actcamp_temp2;

create table sandbox.np_sl_lowbudget_actcamp_temp2 as (
select 
region_namespace,
      supplier_id,
      supplier_name,
      campaign_name ,
      campaign_id,
      wallet_name ,
      wallet_id ,
      max_total_spend,
      total_usd_spend,
      wallet_available_balance,
      active_state,
      valid_state
      from 
      sandbox.np_sl_lowbudget_actcamp) with data;
      
delete from sandbox.np_groupon_wallet_sl 
where 
  report_date >= (select min(report_date) from sandbox.np_groupon_wallet_sl_temp) ;

INSERT INTO sandbox.np_groupon_wallet_sl
select * from sandbox.np_groupon_wallet_sl_temp;

select distinct report_date from sandbox.np_sl_lowbudget_actcamp order by 1;







select 
   st2.*, 
   ads.adspend, 
   wallet_balance_this_week - wallet_balance_prev_week + adspend_from_supplier_this_week balance_added
from 
(select 
    st1.this_week, 
    count(supplier_name) total_suppliers_live,
    sum(case when st1.previous_week is null then 1 else 0 end) new_supplier_added,
    sum(case when st1.previous_week is null then st1.wallet_balance_this_week end) new_supplier_wallet,
    sum(st1.wallet_balance_this_week) wallet_balance_this_week, 
    sum(st1.wallet_balance_prev_week) wallet_balance_prev_week,
    sum(st1.adspend) adspend_from_supplier_this_week
    from
    (select 
         a.supplier_name,
         a.report_week this_week, 
         b.report_week previous_week,
         a.available_balance_sum wallet_balance_this_week,
         b.available_balance_sum wallet_balance_prev_week,
         c.adspend
     from 
     (select 
          cast(report_date as date) - 7 prev_week,
          cast(report_date as date) report_week, 
          cast(report_date as date) + 7 next_week,
          supplier_name,
          sum(available_balance_sum) available_balance_sum
      from 
          sandbox.np_sl_wallet_spend_agg
          group by 1,2) as a 
      left join 
      (select 
           cast(report_date as date) report_week, 
           supplier_name,
           sum(available_balance_sum) available_balance_sum
        from 
           sandbox.np_sl_wallet_spend_agg
           group by 1,2) as b on a.report_week = b.report_week + 7 and a.supplier_name = b.supplier_name
       left join 
      (select 
           report_week,
           supplier_name,
           sum(adspend) adspend
       from 
           sandbox.np_sl_ad_snapshot_agg_supplier
       group by 1,2
       ) as c on a.report_week = c.report_week and a.supplier_name = c.supplier_name
     ) st1
     group by 1
  ) st2
left join 
(select 
    report_week,
    sum(adspend) adspend
from 
    sandbox.np_sl_ad_snapshot_agg_supplier
    group by 1
) as ads on st2.this_week = ads.report_week
order by 1;



select 
     month(report_week), 
     count(distinct supplier_name)
from sandbox.np_sl_wallet_level_spend_agg 
where adspend > 0 and team_cohort <> 'Test'
group by 1
order by 1;

select 
    month_, 
    count(distinct supplier_name)
from 
(select 
        month(report_date) month_,
        a.supplier_name,
        b.team_cohort,
        sum(adspend) adspend, 
        sum(impressions) impressions,
        sum(clicks) clicks,
        sum(conversions) conversions,
        sum(unitsales) unitsales,
        sum(sales_revenue) sales_revenue
  from sandbox.np_sl_ad_snapshot AS a
  left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name
  group by 1,2,3) as a 
  where a.team_cohort <> 'Test' and adspend >0
  group by 1
  order by 1;
*/