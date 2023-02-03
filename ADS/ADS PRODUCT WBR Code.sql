
create volatile table np_sl_mrch_120 as
(select 
		merchant_id,
		min(case when a.total_spend_amount > 0  then cast(generated_datetime as date) end) min_date_spend_two,
		trunc(min(case when a.total_spend_amount > 0 then cast(generated_datetime as date) end), 'iw')+6 min_date_spend_week
	from user_gp.ads_rcncld_intrmdt_rpt as a 
		left join sandbox.pai_deals as b on a.deal_id = b.deal_uuid
		left join sandbox.pai_merchants as c on b.merchant_uuid = c.merchant_uuid 
	where a.is_self_serve = 1
	group by 1
	having min_date_spend_two is not null
) with data on commit preserve rows;
create volatile table np_sl_mrch_spend as
(select 
		a.merchant_id,
		c.merchant_name,
		c.merch_permalink,
		c.l2, 
		c.first_launch_date,
		c.last_live_date,
		xyz.lst_avail_offer_sent_on,
		min(case when a.total_spend_amount > 0  then cast(generated_datetime as date) end) min_date_spend,
		trunc(min(case when a.total_spend_amount > 0 then cast(generated_datetime as date) end), 'iw')+6 min_date_spend_week,
		max(case when a.total_spend_amount > 0  then cast(generated_datetime as date) end) max_date_spend,
		trunc(max(case when a.total_spend_amount > 0  then cast(generated_datetime as date) end), 'iw')+6 max_date_spend_week,
		count(distinct case when a.total_spend_amount > 0  then cast(generated_datetime as date) end) days_with_adspend,
		max(case when cast(generated_datetime as date) - min_one.min_date_spend_two <= 120 and a.total_spend_amount > 0 then cast(generated_datetime as date) end) max_date_spend_120,
		max_date_spend - min_date_spend days_bet_spend, 
		current_date - min_date_spend days_bet_cur_min_spend, 
		max_date_spend_120 - min_date_spend days_bet_spend_120
	from user_gp.ads_rcncld_intrmdt_rpt as a
		left join sandbox.pai_deals as b on a.deal_id = b.deal_uuid
		left join (select 
		             mer.merchant_uuid, 
		             mer.l2,
		             mer.first_launch_date,
		             mer.last_live_date,
		             mer.merchant_name,
		             max(per.Merchant_Permalink) merch_permalink
		           from 
		           sandbox.pai_merchants as mer 
		           left join (select Account_ID_18 as acc_id,MAX(Merchant_Permalink) Merchant_Permalink  from dwh_base_sec_view.sf_account group by 1) as per on mer.account_id = per.acc_id
		            group by 1,2,3,4,5
		            ) as c on b.merchant_uuid = c.merchant_uuid 
		left join np_sl_mrch_120 as min_one on a.merchant_id = min_one.merchant_id
		left join (select merchant_uuid, 
                     max(offer_sent_on) lst_avail_offer_sent_on, 
                     max(offer_availed_on) offer_availed_on
                    from sandbox.np_merch_free_credits 
                    where offer_availed_on is not null
                    group by 1) as xyz on a.merchant_id = xyz.merchant_uuid
	where a.is_self_serve = 1
	group by 1,2,3,4,5,6,7
	having min_date_spend is not null
) with data on commit preserve rows
;



--merchant id in spend: '481a588b-8ae0-4f95-995e-6b093251de89'
--deal id in spend: 7a9445a1-3387-46c9-bdff-01b7dfa30269 I can see this in citrusad_camp
--same deal id in pai_deals has: ce1ea913-c6fd-4a22-a178-d86f0d94ac47 merchant_uuid
--deal for '481a588b-8ae0-4f95-995e-6b093251de89' merchantuuid is 8cc791cd-11bf-4d3e-917f-5a90525a8502





--------------------------------------------------------------------------------------------------------CUSTOMER METRICS

create MULTISET volatile table np_temp_imps as (
select 
   trunc(cast(report_date as date), 'iw')+6 report_week,
   sum(total_impressions) total_impresssions
from user_edwprod.agg_gbl_impressions_deal
where 
   grt_l1_cat_name = 'L1 - Local'
   group by 1
) with data on commit preserve rows
;

drop table sandbox.np_sl_cust_met;
create multiset table sandbox.np_sl_cust_met as (
select 
    trunc(cast(generated_datetime as date), 'iw')+6 report_week,
    cast(generated_datetime as date) - EXTRACT(DAY FROM cast(generated_datetime as date)) + 1 report_month, 
    b.l2, b.country_code, 
    c.campaign_subtype,
    d.lst_avail_offer_sent_on,
    sum(impressioned) total_imps, 
    sum(clicked) total_clicks, 
    sum(converted) total_ords,
    sum(order_quantity) total_units, 
    sum(price_with_discount) orders_rev, 
    sum(total_spend_amount) adspend, 
    sum(click_spend_amount) clc_spend, 
    count(distinct case when a.total_spend_amount > 0 then a.campaign_id end) campaigns_with_spend, 
    count(distinct case when a.total_spend_amount > 0 then b.merchant_uuid end) merchant_with_spend
from user_gp.ads_rcncld_intrmdt_rpt as a 
left join sandbox.pai_deals as b on a.deal_id = b.deal_uuid
left join sandbox.np_citrusad_campaigns as c on a.campaign_id = c.citrusad_campaign_id
left join (select merchant_uuid, 
                  max(offer_sent_on) lst_avail_offer_sent_on, 
                  max(offer_availed_on) offer_availed_on
                 from sandbox.np_merch_free_credits 
                 where offer_availed_on is not null
                 group by 1) as d on a.merchant_id = d.merchant_uuid
where a.is_self_serve = 1 and trunc(cast(generated_datetime as date), 'iw')+6 <= current_date
group by 1,2,3,4,5,6) with data;

create multiset volatile table np_sl_cust_met_temp as (
select 
    trunc(cast(generated_datetime as date), 'iw')+6 report_week,
    a.merchant_id,
    b.merchant_name,
    sum(impressioned) total_imps, 
    sum(clicked) total_clicks, 
    sum(converted) total_ords,
    sum(order_quantity) total_units, 
    sum(price_with_discount) orders_rev, 
    sum(total_spend_amount) adspend, 
    sum(click_spend_amount) clc_spend, 
    count(distinct case when a.total_spend_amount > 0 then a.campaign_id end) campaigns_with_spend, 
    count(distinct case when a.total_spend_amount > 0 then b.merchant_uuid end) merchant_with_spend, 
    cast(orders_rev as float)/nullifzero(adspend) as roas
from user_gp.ads_rcncld_intrmdt_rpt as a 
left join sandbox.pai_merchants as b on a.merchant_id = b.merchant_uuid
left join sandbox.np_citrusad_campaigns as c on a.campaign_id = c.citrusad_campaign_id
where a.is_self_serve = 1 and trunc(cast(generated_datetime as date), 'iw')+6 <= current_date
group by 1,2,3) with data on commit preserve rows;
;

drop table  sandbox.np_sl_drop_wow_merch;
create multiset table sandbox.np_sl_drop_wow_merch as (
select 
    x.merchant_id,
    x.merchant_name, 
    x.next_report_week,
    x.report_week, 
    x.total_clicks2, 
    x.total_imps2, 
    x.adspend2,
    x.ctr2,
    y.total_clicks2 clicks2y, 
    y.total_imps2 imps2y, 
    y.adspend2 adspend2y,
    y.ctr2 ctr2y,
    case when y.total_clicks2 is null then 0 else y.total_clicks2 end - x.total_clicks2 change_in_click, 
    case when y.total_imps2 is null then 0 else y.total_imps2 end - x.total_imps2 change_in_imps, 
    case when y.adspend2 is null then 0 else y.adspend2 end - x.adspend2 change_in_adspend, 
    case when y.ctr2 is null then 0 else y.ctr2 end - x.ctr2 change_in_ctr
from 
   (select 
		report_week,
        report_week + 7 next_report_week,
		a.merchant_id, 
		a.merchant_name,
		sum(total_clicks) total_clicks2, 
		sum(total_imps) total_imps2, 
		sum(total_ords) total_ords2,
		sum(adspend) adspend2, 
		cast(total_clicks2 as float)/nullifzero(total_imps2) as ctr2
	from np_sl_cust_met_temp as a 
	where report_week >= current_date - 90
	group by 1,2,3,4) as x
left join 
	(select 
		report_week,
        report_week + 7 next_report_week,
		a.merchant_id, 
		a.merchant_name,
		sum(total_clicks) total_clicks2, 
		sum(total_imps) total_imps2, 
		sum(total_ords) total_ords2,
		sum(adspend) adspend2, 
		cast(total_clicks2 as float)/nullifzero(total_imps2) as ctr2
	from np_sl_cust_met_temp as a 
	where report_week >= current_date - 90
    group by 1,2,3,4) as y on x.merchant_id = y.merchant_id and x.next_report_week = y.report_week) with data;

---------------------------------------------------------TRAFFIC PERFORMANCE COMPARISON



drop table sandbox.np_sl_all_traffic;
create multiset table sandbox.np_sl_all_traffic as (
select 
   cast(report_date as date) - EXTRACT(DAY FROM cast(report_date as date)) + 1 report_month, 
   wk_end, 
   platform, 
   traffic_source,
   sum(uniq_visitors) unique_visitors
from user_edwprod.agg_gbl_traffic
where report_year >= 2021
and country_code = 'US'
and trunc(cast(report_date as date), 'iw')+6 <= current_date
group by 1,2,3,4
) with data;


select platform, sum(unique_visitors) 
from sandbox.np_sl_all_traffic where report_month = '2022-07-01'
group by 1;

select 
   sub_platform, 
   sum(uniq_visitors) unique_visitors
from user_edwprod.agg_gbl_traffic
where report_year = 2022
and report_month = 07
and country_code = 'US'
group by 1

create volatile table np_sl_min_imp as (
select 
   a.*, 
   b.min_live_on_groupon, 
   case when b.min_live_on_groupon - min_impression_date < 35 then b.min_live_on_groupon - min_impression_date
        when live_days_on_sl < 35 then live_days_on_sl 
        else 35 end days_to_consider_forty
from 
(select 
         deal_id,
         max(merchant_id) merchant_id,
         min(case when impressioned > 0 then cast(generated_datetime as date format 'yyyy-mm-dd') end ) min_impression_date, 
         max(case when impressioned > 0 then cast(generated_datetime as date format 'yyyy-mm-dd') end ) max_impression_date,
         min_impression_date + 35 thirty_aft_imp,
         min_impression_date - 35 thirty_bef_imp,
         max_impression_date - min_impression_date live_days_on_sl,
         current_date - min_impression_date days_on_sl_frm_min
     from user_gp.ads_rcncld_intrmdt_rpt
     where year(generated_datetime) > 2000 and is_self_serve = 1
       group by 1
) AS a 
left join 
(select deal_uuid, 
        min(load_date) min_live_on_groupon
      from user_groupondw.active_deals 
      where SOLD_OUT = 'false' group by deal_uuid) as b on a.deal_id = b.deal_uuid
) with data on commit preserve rows;

create multiset volatile table np_days_considered as (
SELECT 
    a.load_date, 
    b.*, 
    CASE WHEN a.load_date < b.min_impression_date THEN 'pre_sl' ELSE 'post_sl' END pre_post_sl_case
FROM 
     user_groupondw.active_deals as a
     JOIN np_sl_min_imp as b on a.deal_uuid = b.deal_id and a.load_date >= b.thirty_bef_imp and a.load_date < b.thirty_aft_imp
     WHERE a.SOLD_OUT = 'false') with data on commit preserve rows


create multiset volatile table np_imps_agg as 
(select 
      a.report_date,
      a.deal_id, 
      sum(total_impressions) total_imps
     from user_edwprod.agg_gbl_impressions_deal as a
       join np_days_considered as b on a.deal_id = b.deal_id and a.report_date = b.load_date
     where 
        cast(report_date as date) >= cast('2021-01-01' as date) 
     group by 
     1,2) with data on commit preserve rows
;

create multiset volatile table np_fin_agg as
( select 
       a.report_date,
       a.deal_id,
       sum(transactions) all_transaction,
       sum(transactions_qty) units, 
       sum(parent_orders_qty) parent_orders_qty,
       sum(deal_views) deal_views,
       sum(nob_usd) nob_usd, 
       sum(nor_usd) nor_usd
       from user_edwprod.agg_gbl_traffic_fin_deal as a
       join np_days_considered as b on a.deal_id = b.deal_id and a.report_date = b.load_date
       where 
          cast(a.report_date as date) >= cast('2021-01-01' as date) 
       group by 
       1,2
) with data on commit preserve rows;

create multiset volatile table np_sl_agg as
( select 
    cast(generated_datetime as date) report_date,
    a.deal_id,
    sum(impressioned) imps_sl, 
    sum(clicked) clk_sl, 
    sum(converted) ord_sl,
    sum(order_quantity) units_sl, 
    sum(price_with_discount) rev_sl, 
    sum(total_spend_amount) adspend_sl, 
    sum(click_spend_amount) clc_spend_sl
from user_gp.ads_rcncld_intrmdt_rpt as a 
group by 1,2) with data on commit preserve rows;

drop table sandbox.np_sl_agg;
create multiset table sandbox.np_sl_agg as(
select 
   fin.deal_id,
   fin.pre_post_sl_case,
   d.permalink deal_permalink, 
   e.merch_permalink,
   sum(fin.total_imps) total_imps, 
   sum(fin.deal_views) total_clks,
   sum(all_transaction) total_transactions,
   sum(units) total_units, 
   sum(parent_orders_qty) total_orders,
   sum(imps_sl) imps_sl, 
   sum(clk_sl) clks_sl, 
   sum(ord_sl) ord_sl,
   sum(units_sl) units_sl, 
   sum(rev_sl) rev_sl, 
   sum(adspend_sl) adspend_sl,
   count(distinct load_date) days_live
from 
(select 
    main.*, 
    a.total_imps,
    b.deal_views,
    b.all_transaction,
    b.units, 
    b.parent_orders_qty,
    b.nob_usd, 
    b.nor_usd, 
    c.imps_sl, 
    c.clk_sl, 
    c.ord_sl,
    c.units_sl, 
    c.rev_sl, 
    c.adspend_sl, 
    c.clc_spend_sl
from np_days_considered as main
left join np_imps_agg as a on main.load_date = a.report_date and main.deal_id = a.deal_id
left join np_fin_agg as b on main.load_date = b.report_date and main.deal_id = b.deal_id
left join np_sl_agg as c on main.load_date = c.report_date and main.deal_id = c.deal_id) as fin
left join sandbox.pai_deals as d on fin.deal_id = d.deal_uuid
left join (select 
		             mer.merchant_uuid, 
		             mer.l2,
		             max(per.Merchant_Permalink) merch_permalink
		           from 
		           sandbox.pai_merchants as mer 
		           left join (select Account_ID_18 as acc_id,MAX(Merchant_Permalink) Merchant_Permalink  from dwh_base_sec_view.sf_account group by 1) as per on mer.account_id = per.acc_id
		            group by 1,2
		            ) as e on fin.merchant_id = e.merchant_uuid 
group by 1,2,3,4
) with data;


------------HIVE

/*CREATE MULTISET TABLE sandbox.np_sl_deals_imps_trans ,NO FALLBACK ,
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
NO PRIMARY INDEX;*/

create table grp_gdoop_bizops_db.np_deals_imps_tran_date stored as orc as

create table grp_gdoop_bizops_db.np_sl_deals_imps_tran_date (
    deal_id string,
    total_imps int, 
    all_transaction int, 
    units int, 
    parent_orders_qty int, 
    deal_views int, 
    nob_usd float, 
    nor_usd float
) partitioned by (report_date string)
stored as orc
tblproperties ("orc.compress"="SNAPPY");

insert overwrite table grp_gdoop_bizops_db.np_sl_deals_imps_tran_date partition (report_date)
select 
     imp.deal_id, 
     imp.total_imps, 
     fin.all_transaction,
     fin.units, 
     fin.parent_orders_qty,
     fin.deal_views, 
     fin.nob_usd, 
     fin.nor_usd, 
     imp.report_date
from
(select 
      report_date,
      deal_id, 
      sum(total_impressions) total_imps
     from edwprod.agg_gbl_impressions_deal as a
       join user_groupondw.active_deals as b on a.deal_id = b.deal_uuid and a.report_date = b.load_date and b.SOLD_OUT = 'false'
     where cast(report_date as date) >= cast('2021-01-01' as date)
     group by 
     report_date,
     deal_id) as imp
left join
(select 
       report_date,
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
       group by 
       report_date, 
       deal_id) fin on imp.report_date = fin.report_date and imp.deal_id = fin.deal_id
;


select * from sandbox.pai_deals where deal_uuid = '012d5fd0-eb57-4d2f-8aa2-177a1decb28e';


-----investigate wbr 


create multiset volatile table np_sl_cust_met_temp as (
select 
    trunc(cast(generated_datetime as date), 'iw')+6 report_week,
    a.merchant_id,
    c.campaign_subtype,
    sum(impressioned) total_imps, 
    sum(clicked) total_clicks, 
    sum(converted) total_ords,
    sum(order_quantity) total_units, 
    sum(price_with_discount) orders_rev, 
    sum(total_spend_amount) adspend, 
    sum(click_spend_amount) clc_spend, 
    count(distinct case when a.total_spend_amount > 0 then a.campaign_id end) campaigns_with_spend, 
    count(distinct case when a.total_spend_amount > 0 then b.merchant_uuid end) merchant_with_spend, 
    cast(orders_rev as float)/nullifzero(adspend) as roas
from user_gp.ads_rcncld_intrmdt_rpt as a 
left join sandbox.pai_deals as b on a.deal_id = b.deal_uuid
left join sandbox.np_citrusad_campaigns as c on a.campaign_id = c.citrusad_campaign_id
where a.is_self_serve = 1 and trunc(cast(generated_datetime as date), 'iw')+6 <= current_date
group by 1,2,3) with data on commit preserve rows;
;



select 
   citrusad_campaign_id, 
   campaign_subtype, 
   product, 
   l2, 
   spend_type, 
   budget,
   status,
   merchant_uuid,
   create_week, 
   create_date,
   tot_search_words
from sandbox.np_sl_cmp_onb 
where create_date >= cast('2022-01-01' as date);

SELECT 
   a.*, 
   total_imps, 
   total_clicks, 
   total_ords,
   total_units, 
   orders_rev, 
   adspend, 
   clc_spend, 
   campaigns_with_spend, 
   merchant_with_spend
FROM 
sandbox.np_sl_cmp_onb as a 
left join 
(select 
    campaign_id,
    sum(impressioned) total_imps, 
    sum(clicked) total_clicks, 
    sum(converted) total_ords,
    sum(order_quantity) total_units, 
    sum(price_with_discount) orders_rev, 
    sum(total_spend_amount) adspend, 
    sum(click_spend_amount) clc_spend, 
    count(distinct case when a.total_spend_amount > 0 then a.campaign_id end) campaigns_with_spend, 
    count(distinct case when a.total_spend_amount > 0 then b.merchant_uuid end) merchant_with_spend
from user_gp.ads_rcncld_intrmdt_rpt as a 
left join sandbox.pai_deals as b on a.deal_id = b.deal_uuid
where a.is_self_serve = 1
group by 1) as b on a.citrusad_campaign_id = b.campaign_id


------------------------------------------------------------------------------------------------------MERCHANT METRICS

drop table np_sl_cmp_date;
drop table np_sl_mrch_min;
drop table np_int2;

create volatile table np_sl_cmp_date as     
(select 
         campaign_id,
         max(case when impressioned > 0 then cast(generated_datetime as date) end ) max_impression_date, 
         current_date - max_impression_date number_of_days_past_imps, 
         max(case when clicked > 0 then cast(generated_datetime as date) end ) max_click_date, 
         current_date - max_click_date number_of_days_past_cls,
         max(case when total_spend_amount > 0 then cast(generated_datetime as date) end ) max_ads_date, 
         current_date - max_ads_date number_of_days_past_spend
     from user_gp.ads_rcncld_intrmdt_rpt as a 
     left join sandbox.pai_deals as b on a.deal_id = b.deal_uuid
     group by 1
) with data on commit preserve rows;

create volatile table np_sl_mrch_min as     
(select 
         b.merchant_uuid, 
         min(cast(substr(create_datetime,1,10) as date)) first_campaign_created
     from sandbox.np_citrusad_campaigns as a 
     left join sandbox.pai_deals as b on a.product = b.deal_uuid
     group by 1
) with data on commit preserve rows;


create volatile multiset table np_int2 as (
WITH cte (citrusad_campaign_id, search_terms) AS
 (
   SELECT citrusad_campaign_id, search_terms FROM sandbox.np_citrusad_campaigns where campaign_subtype = 'SEARCH_ONLY'
 )
SELECT * 
FROM TABLE
 ( STRTOK_SPLIT_TO_TABLE( cte.citrusad_campaign_id, search_terms, ',') 
   RETURNS ( citrusad_campaign_id varchar(64)  , TokenNum INT , Token VARCHAR (256) CHARACTER SET UNICODE ) 
 ) dt
 ) with data on commit preserve rows
 ;



drop table sandbox.np_sl_cmp_onb;
create multiset table sandbox.np_sl_cmp_onb as (
select 
    'campaign' as category,
    a.citrusad_campaign_id,
    campaign_subtype, 
    product, 
    k.camp_start_date, 
    k.camp_end_date,
    b.l2, 
    b.merchant_uuid,
    target_locations, 
    spend_type, 
    a.budget, 
    a.status, 
    i.budget_type budget_type_mc,
    i.budget_value budget_value_mc,
    i.status status_mc,
    max_cpc, 
    trunc(cast(substr(create_datetime,1,10) as date), 'iw')+6 create_week,
    cast(substr(create_datetime,1,10) as date) - EXTRACT(DAY FROM cast(substr(create_datetime,1,10) as date)) + 1 create_month, 
    trunc(cast(substr(citrusad_update_datetime,1,10) as date), 'iw')+6 update_week,
    cast(substr(citrusad_update_datetime,1,10) as date) - EXTRACT(DAY FROM cast(substr(citrusad_update_datetime,1,10) as date)) + 1 update_month,
    cast(substr(create_datetime,1,10) as date) create_date, 
    case when create_date > first_campaign_created then 0 else 1 end mrch_crt_first_camp,
    case when d.dealuuid is not null then 1 else 0 end dealrecommended,
    cast(substr(citrusad_update_datetime,1,10) as date) - cast(substr(create_datetime,1,10) as date) diff_crt_upd_days, 
    max_ads_date last_spend_date,
    a.search_terms,
    tot_search_words,
    case when campaign_subtype = 'SEARCH_ONLY' and tot_search_words = 1 then 'a.1 search term'
         when campaign_subtype = 'SEARCH_ONLY' and tot_search_words <= 5 then 'b.less than 5 search term'
         when campaign_subtype = 'SEARCH_ONLY' and tot_search_words <= 10 then 'c.less than 10 search term'
         when campaign_subtype = 'SEARCH_ONLY' and tot_search_words <= 15 then 'd.less than 15 search term'
         when campaign_subtype = 'SEARCH_ONLY' and tot_search_words > 15 then 'e.greater than 15 search term'
         when campaign_subtype = 'SEARCH_ONLY' then 'f.unknown search terms'
         end search_term_case,
    f.lst_avail_offer_sent_on,
    case when cast(substr(create_datetime,1,10) as date) >= g.email_open_date then 'After email opened' 
         when cast(substr(create_datetime,1,10) as date) >= g.sentdate then 'After email sent'
         else 'no email sent' end targeted_category, 
    case when h.deal_id is not null then  1 else 0 end merchant_advisor
from sandbox.np_citrusad_campaigns as a 
left join sandbox.pai_deals as b on a.product = b.deal_uuid
left join np_sl_mrch_min as c on b.merchant_uuid = c.merchant_uuid
left join sandbox.np_sl_drecommender as d on a.product = d.dealuuid and cast(substr(a.create_datetime,1,10) as date) = cast(d.report_date as date)
left join (select citrusad_campaign_id, count(distinct token) tot_search_words from np_int2 group by 1) as e on a.citrusad_campaign_id = e.citrusad_campaign_id
left join (select merchant_uuid, 
                  max(offer_sent_on) lst_avail_offer_sent_on, 
                  max(offer_availed_on) offer_availed_on
                 from sandbox.np_merch_free_credits 
                 where offer_availed_on is not null
                 group by 1) as f on b.merchant_uuid = f.merchant_uuid
left join 
        (SELECT
	      merchant_uuid,
	      max(cast(substr(sentdate, 1,10) as date)) sentdate,
	      max(case when length(substr(firstopendate,1,10)) >= 10 then cast(substr(firstopendate,1,10) as date) end)  email_open_date
         FROM
	        sandbox.SFMC_EmailEngagement
         where 
	      journeyname 
	       in 
	       ('MM_Sponsored_DormantMerchants',
	         'MM_Sponsored_DormantMerchants',
	         'MM_Sponsored_DormantMerchants',
	         'MM_Sponsored_ReengagementSeries',
	         'MM_Retention_Milestones',
	         'MM_Retention_NotSellingUnits',
	         'MM_Sponsored_ClickedNavinMC',
	         'MM_Sponsored_DropOffSeries',
	         'MM_Onboarding_OnboardingSeries')
	     group by merchant_uuid  
	         ) as g on b.merchant_uuid = g.merchant_uuid
left join 
        (select 
            deal_id, 
            trunc(cast(substr(create_datetime,1,10) as date), 'iw')+6 create_week
         from sandbox.np_sponsored_campaign 
         where creation_source = 'MERCHANT_ADVISOR'
         group by 1,2) as h on a.product = h.deal_id and trunc(cast(substr(a.create_datetime,1,10) as date), 'iw')+6 = h.create_week
left join 
(select 
    merchant_id,
    deal_id, 
    budget_type,
    budget_value,
    status, 
    cast(substr(create_datetime, 1, 10) as date) create_date,
    row_number() over( partition by deal_id order by cast(substr(create_datetime, 1, 10) as date) desc ) row_num_lvl
from sandbox.np_sponsored_campaign) as i on a.product = i.deal_id and i.row_num_lvl = 1
left join np_sl_cmp_date as j on a.citrusad_campaign_id = j.campaign_id
left join (select citrusad_campaign_id, min(cast(substr(start_date,1, 10) as date)) camp_start_date, max(cast(substr(end_date,1, 10) as date)) camp_end_date from sandbox.np_citrusad_campaign_logs group by 1) as k on a.citrusad_campaign_id = k.citrusad_campaign_id
) with data;

 SELECT * FROM sandbox.pai_deals;

---------------------------------------------------------------wow active campaigns
   
drop table sandbox.np_sl_act_camp;
create table sandbox.np_sl_act_camp as (
select   
   week_dates, 
   count(distinct merchant_uuid) merchants_live_wow,
   count(distinct citrusad_campaign_id) campaigns_live_wow, 
   count(distinct case when campaign_subtype = 'CATEGORY_ONLY' then citrusad_campaign_id end) cat_camp_live_wow, 
   count(distinct case when campaign_subtype = 'SEARCH_ONLY' then citrusad_campaign_id end) sea_camp_live_wow
from 
(select 
*
from 
(select cast(week_end as date) week_dates
      from user_groupondw.dim_week
where week_dates <= current_date and week_dates >= cast('2020-06-01' as date)
) as a 
left join 
  (select * from sandbox.np_sl_cmp_onb where status <> 'ACTIVE' and citrusad_campaign_id is not null) as b on a.week_dates >= b.create_week and a.week_dates < b.update_week
UNION ALL
select 
*
from 
(select cast(week_end as date) week_dates
      from user_groupondw.dim_week
where week_dates <= current_date and week_dates >= cast('2020-06-01' as date)
) as a 
left join 
  (select * from sandbox.np_sl_cmp_onb where status = 'ACTIVE' and citrusad_campaign_id is not null) as b on a.week_dates >= b.create_week) as fin 
group by 1) with data
;



drop table sandbox.np_sl_agg_one_wbr;
create table sandbox.np_sl_agg_one_wbr as (
select 
*
from 
(select 
    cast(a.offer_sent_on as varchar(60)) dashboard_category,
    a.week_dates, 
    b.campaign_created, 
    b.campaign_created_aft_em,
    b.cat_campaign_id, 
    b.search_campaign_id, 
    b.merch_created_camp, 
    b.merch_created_camp_aft_em,
    b.activation_merch,
    b.dealrecommended_campaign,
    b.deal_created,
    b.dealrecommended_deal,
    b.merchant_advisor_deal,
    c.pause_camp_id, 
    c.archive_camp_id, 
    d.top_up_amount, 
    d.reup_count, 
    d.merchant_with_topup, 
    e.campaigns_with_adspend, 
    e.cat_camp_with_adspend,
    e.sea_camp_with_adspend,
    e.campaigns_with_conversion,
    e.campw_roas_gone, 
    e.total_imps, 
    e.total_clicks, 
    e.total_ords,
    e.total_units, 
    e.orders_rev, 
    e.adspend, 
    e.clc_spend,
    f.merch_with_adspend, 
    f.merch_with_conversion,
    f.merch_roas_gone,
    g.product_with_adspend, 
    g.product_with_conversion,
    g.product_roas_gone,
    h.merchants_live_wow,
    h.campaigns_live_wow, 
    h.cat_camp_live_wow,
    h.sea_camp_live_wow
from
	(select 
         x.*, 
         y.*
         from 
         (select cast(week_end as date) week_dates
		    from user_groupondw.dim_week
		    where week_dates <= current_date and week_dates >= cast('2020-06-01' as date)
		  ) as x
          left join (
          select cast('original' as varchar(40)) offer_sent_on from user_groupondw.dim_week where year_key = 2020 and week_key = 2020000034
          union all 
          select cast(offer_sent_on as varchar(30)) offer_sent_on from sandbox.np_merch_free_credits where offer_sent_on is not null group by 1 
          )  as y on 'apple' = 'apple'
         ) as a 
left join 
    (select create_week,
            case when lst_avail_offer_sent_on is null then 'original' else cast(lst_avail_offer_sent_on as varchar(40)) end lst_avail_offer_sent_on,
    		count(citrusad_campaign_id) campaign_created, 
    		count(case when create_date >= x.sentdate then citrusad_campaign_id end) campaign_created_aft_em,
    		count(distinct product) deal_created,
    		count(case when dealrecommended = 1 then citrusad_campaign_id end) dealrecommended_campaign,
    		count(case when dealrecommended = 1 then product end) dealrecommended_deal,
    		count(case when merchant_advisor = 1 then product end) merchant_advisor_deal,
    		---count(distinct citrusad_campaign_id) dist_campaign_created,
    		count(case when campaign_subtype = 'CATEGORY_ONLY' then citrusad_campaign_id end) cat_campaign_id, 
    		count(case when campaign_subtype = 'SEARCH_ONLY' then citrusad_campaign_id end) search_campaign_id, 
    		count(distinct a.merchant_uuid) merch_created_camp, 
    		count(distinct case when create_date >= x.sentdate then a.merchant_uuid end) merch_created_camp_aft_em,
    		count(distinct case when mrch_crt_first_camp = 1 then a.merchant_uuid end ) activation_merch
    	from
    		sandbox.np_sl_cmp_onb as a
    		left join 
    		(SELECT
		      merchant_uuid,
		      max(cast(substr(sentdate, 1,10) as date)) sentdate,
		      max(case when length(substr(firstopendate,1,10)) >= 10 then cast(substr(firstopendate,1,10) as date) end)  email_open_date
		     FROM  sandbox.SFMC_EmailEngagement
	         where 
		       journeyname 
		        in 
		       ('MM_Sponsored_DormantMerchants',
		         'MM_Sponsored_DormantMerchants',
		         'MM_Sponsored_DormantMerchants',
		         'MM_Sponsored_ReengagementSeries',
		         'MM_Retention_Milestones',
		         'MM_Retention_NotSellingUnits',
		         'MM_Sponsored_ClickedNavinMC',
		         'MM_Sponsored_DropOffSeries',
		         'MM_Onboarding_OnboardingSeries')
		        group by merchant_uuid  
		         ) as x on a.merchant_uuid = x.merchant_uuid
    		group by 1,2)  as b on a.week_dates = b.create_week and a.offer_sent_on = b.lst_avail_offer_sent_on
left join 
    (select update_week, 
            case when lst_avail_offer_sent_on is null then 'original' else cast(lst_avail_offer_sent_on as varchar(40)) end  lst_avail_offer_sent_on,
    		count(case when status = 'PAUSED' then citrusad_campaign_id end) pause_camp_id, 
    		count(case when status = 'ARCHIVED' then citrusad_campaign_id end) archive_camp_id
    	from
    		sandbox.np_sl_cmp_onb 
    		where lst_avail_offer_sent_on is not null
    		group by 1,2)  as c on a.week_dates = c.update_week and a.offer_sent_on = c.lst_avail_offer_sent_on
left join 
	(select 
	       trunc(cast(substr(create_datetime, 1,10) as date), 'iw') + 6 create_week,
	       case when lst_avail_offer_sent_on is null then 'original' else cast(lst_avail_offer_sent_on as varchar(40)) end lst_avail_offer_sent_on,
	       sum(amount) top_up_amount, 
	       count(amount) as reup_count, 
	       count(distinct merchant_id) merchant_with_topup
	from sandbox.np_merchant_topup_orders as a
	left join (select merchant_uuid, 
                     max(offer_sent_on) lst_avail_offer_sent_on, 
                     max(offer_availed_on) offer_availed_on
                    from sandbox.np_merch_free_credits 
                    where offer_availed_on is not null
                    group by 1) as xyz on a.merchant_id = xyz.merchant_uuid
	where event_type='CAPTURE' and event_status='SUCCESS'
	group by 1,2) as d on a.week_dates = d.create_week and a.offer_sent_on = d.lst_avail_offer_sent_on
left join
 (select 
      report_week,
      lst_avail_offer_sent_on,
      sum(campw_roas_gone) campw_roas_gone, 
      count(DISTINCT case when adspend > 0 then campaign_id end) campaigns_with_adspend, 
      count(DISTINCT case when adspend > 0 and campaign_subtype = 'CATEGORY_ONLY' then campaign_id end) cat_camp_with_adspend, 
      count(DISTINCT case when adspend > 0 and campaign_subtype = 'SEARCH_ONLY'then campaign_id end) sea_camp_with_adspend, 
      count(distinct case when total_ords > 0 then campaign_id end) campaigns_with_conversion, 
      sum(total_imps) total_imps, 
      sum(total_clicks) total_clicks, 
      sum(total_ords) total_ords,
      sum(total_units) total_units, 
      sum(orders_rev) orders_rev, 
      sum(adspend) adspend, 
      sum(clc_spend) clc_spend
   from 
	(select 
		trunc(cast(generated_datetime as date), 'iw')+6 report_week,
		case when lst_avail_offer_sent_on is null then 'original' else cast(lst_avail_offer_sent_on as varchar(40)) end lst_avail_offer_sent_on,
		campaign_id,
		campaign_subtype,
		sum(impressioned) total_imps, 
    	sum(clicked) total_clicks, 
    	sum(converted) total_ords,
    	sum(order_quantity) total_units, 
    	sum(price_with_discount) orders_rev, 
    	sum(total_spend_amount) adspend, 
    	sum(click_spend_amount) clc_spend, 
    	orders_rev/nullifzero(adspend) roas, 
    	case when roas > 1 then 1 else 0 end campw_roas_gone
	from user_gp.ads_rcncld_intrmdt_rpt as a 
	     left join (select merchant_uuid, 
                     max(offer_sent_on) lst_avail_offer_sent_on, 
                     max(offer_availed_on) offer_availed_on
                    from sandbox.np_merch_free_credits 
                    where offer_availed_on is not null
                    group by 1) as xyz on a.merchant_id = xyz.merchant_uuid
		left join sandbox.pai_deals as b on a.deal_id = b.deal_uuid
		left join sandbox.np_citrusad_campaigns as c on a.campaign_id = c.citrusad_campaign_id
		where a.is_self_serve = 1
		group by 1,2,3,4) as fin
	group by 1,2
	) as e on a.week_dates = e.report_week and a.offer_sent_on = e.lst_avail_offer_sent_on
left join
 ( select 
      report_week,
      lst_avail_offer_sent_on,
      sum(merch_roas_gone) merch_roas_gone, 
      count(DISTINCT case when adspend > 0 then merchant_id end) merch_with_adspend,
      count(DISTINCT case when total_ords > 0 then merchant_id end) merch_with_conversion
   from 
	(select 
		trunc(cast(generated_datetime as date), 'iw')+6 report_week,
		case when lst_avail_offer_sent_on is null then 'original' else cast(lst_avail_offer_sent_on as varchar(40)) end lst_avail_offer_sent_on,
		merchant_id,
		sum(impressioned) total_imps, 
    	sum(clicked) total_clicks, 
    	sum(converted) total_ords,
    	sum(order_quantity) total_units, 
    	sum(price_with_discount) orders_rev, 
    	sum(total_spend_amount) adspend, 
    	sum(click_spend_amount) clc_spend, 
    	orders_rev/nullifzero(adspend) roas, 
    	case when roas > 1 then 1 else 0 end merch_roas_gone
	from user_gp.ads_rcncld_intrmdt_rpt as a 
	     left join (select merchant_uuid, 
                     max(offer_sent_on) lst_avail_offer_sent_on, 
                     max(offer_availed_on) offer_availed_on
                    from sandbox.np_merch_free_credits 
                    where offer_availed_on is not null
                    group by 1) as xyz on a.merchant_id = xyz.merchant_uuid
		left join sandbox.pai_deals as b on a.deal_id = b.deal_uuid
		left join sandbox.np_citrusad_campaigns as c on a.campaign_id = c.citrusad_campaign_id
		where a.is_self_serve = 1
		group by 1,2,3) as fin
	group by 1,2
	) as f on a.week_dates = f.report_week and a.offer_sent_on = f.lst_avail_offer_sent_on
left join
 (select 
      report_week,
      lst_avail_offer_sent_on,
      sum(product_roas_gone) product_roas_gone, 
      count(DISTINCT case when adspend > 0 then product end) product_with_adspend, 
      count(distinct case when total_ords > 0 then product end) product_with_conversion, 
      sum(total_imps) total_imps, 
      sum(total_clicks) total_clicks, 
      sum(total_ords) total_ords,
      sum(total_units) total_units, 
      sum(orders_rev) orders_rev, 
      sum(adspend) adspend, 
      sum(clc_spend) clc_spend
   from 
	(select 
		trunc(cast(generated_datetime as date), 'iw')+6 report_week,
		case when lst_avail_offer_sent_on is null then 'original' else cast(lst_avail_offer_sent_on as varchar(40)) end lst_avail_offer_sent_on,
		product,
		sum(impressioned) total_imps, 
    	sum(clicked) total_clicks, 
    	sum(converted) total_ords,
    	sum(order_quantity) total_units, 
    	sum(price_with_discount) orders_rev, 
    	sum(total_spend_amount) adspend, 
    	sum(click_spend_amount) clc_spend, 
    	orders_rev/nullifzero(adspend) roas, 
    	case when roas > 1 then 1 else 0 end product_roas_gone
	from user_gp.ads_rcncld_intrmdt_rpt as a 
	     left join (select merchant_uuid, 
                     max(offer_sent_on) lst_avail_offer_sent_on, 
                     max(offer_availed_on) offer_availed_on
                    from sandbox.np_merch_free_credits 
                    where offer_availed_on is not null
                    group by 1) as xyz on a.merchant_id = xyz.merchant_uuid
		left join sandbox.pai_deals as b on a.deal_id = b.deal_uuid
		left join sandbox.np_citrusad_campaigns as c on a.campaign_id = c.citrusad_campaign_id
		where a.is_self_serve = 1
		group by 1,2,3) as fin
	group by 1,2
	) as g on a.week_dates = g.report_week and a.offer_sent_on = g.lst_avail_offer_sent_on
	left join sandbox.np_sl_act_camp as h on a.week_dates = h.week_dates and a.offer_sent_on = 'original'
) as fin2  
where 
  (campaign_created is not null 
  or cat_campaign_id is not null 
  or search_campaign_id is not null 
  or merch_created_camp is not null 
  or activation_merch is not null
  or dealrecommended_campaign is not null 
  or pause_camp_id is not null 
  or archive_camp_id is not null 
  or top_up_amount is not null 
  or reup_count is not null 
  or merchant_with_topup is not null 
  or campaigns_with_adspend is not null 
  or campaigns_with_conversion is not null 
  or campw_roas_gone is not null 
  or total_imps is not null 
  or total_clicks is not null 
  or total_ords is not null 
  or total_units is not null 
  or orders_rev is not null 
  or adspend is not null 
  or clc_spend is not null 
  or merch_with_adspend is not null 
  or merch_with_conversion is not null 
  or merch_roas_gone is not null
  or merchants_live_wow is not null
  or deal_created is not null
  or dealrecommended_deal is not null
  or product_with_adspend is not null 
  or product_with_conversion is not null
  or product_roas_gone is not null
  )
  ) with data;

 


----------------------------------------------------------------------------------------------RETENTION AND RE TARGETING SHEET
/*
select 
		merchant_id,
		sum(total_spend_amount) adspend, 
		sum(price_with_discount) orders_rev, 
		orders_rev/nullifzero(adspend) roas
	from user_gp.ads_rcncld_intrmdt_rpt as a 
		left join sandbox.pai_deals as b on a.deal_id = b.deal_uuid
		left join sandbox.pai_merchants as c on b.merchant_uuid = c.merchant_uuid 
	where a.total_spend_amount > 0 
	   and a.is_self_serve = 1
	group by 1
;

select 
		merchant_id,
    	sum(price_with_discount) orders_rev, 
    	sum(total_spend_amount) adspend, 
    	sum(click_spend_amount) clc_spend, 
    	orders_rev/nullifzero(adspend) roas
	from user_gp.ads_rcncld_intrmdt_rpt as a 
		where a.is_self_serve = 1 and a.total_spend_amount > 0
	group by 1;


select 
		cast(generated_datetime as date) dates,
        merchant_id,
    	price_with_discount,
    	total_spend_amount,
    	click_spend_amount,
    	case when total_spend_amount > 0 then 1 else 0 end, 
    	case when price_with_discount > 0 then 1 else 0 end
	from user_gp.ads_rcncld_intrmdt_rpt as a 
	where a.is_self_serve = 1 
	  and (total_spend_amount > 0 or price_with_discount > 0)
	 order by 1 desc, 2
	 ;-- and a.total_spend_amount > 0;
	 
select * from user_gp.ads_rcncld_intrmdt_rpt where merchant_id = '0fa505c1-e61a-4868-93ec-1bc3d41cf108' and cast(generated_datetime as date)  = '2022-04-17'

*/


SELECT * FROM sandbox.np_sl_cmp_onb;

create volatile table np_sl_mrch_120 as
(select 
		merchant_id,
		min(case when a.total_spend_amount > 0  then cast(generated_datetime as date) end) min_date_spend_two,
		trunc(min(case when a.total_spend_amount > 0 then cast(generated_datetime as date) end), 'iw')+6 min_date_spend_week
	from user_gp.ads_rcncld_intrmdt_rpt as a 
		left join sandbox.pai_deals as b on a.deal_id = b.deal_uuid
		left join sandbox.pai_merchants as c on b.merchant_uuid = c.merchant_uuid 
	where a.is_self_serve = 1
	group by 1
	having min_date_spend_two is not null
) with data on commit preserve rows;




create volatile table np_sl_mrch_spend as
(select 
		a.merchant_id,
		c.merchant_name,
		c.merch_permalink,
		c.l2, 
		c.first_launch_date,
		c.last_live_date,
		xyz.lst_avail_offer_sent_on,
		min(case when a.total_spend_amount > 0  then cast(generated_datetime as date) end) min_date_spend,
		trunc(min(case when a.total_spend_amount > 0 then cast(generated_datetime as date) end), 'iw')+6 min_date_spend_week,
		max(case when a.total_spend_amount > 0  then cast(generated_datetime as date) end) max_date_spend,
		trunc(max(case when a.total_spend_amount > 0  then cast(generated_datetime as date) end), 'iw')+6 max_date_spend_week,
		count(distinct case when a.total_spend_amount > 0  then cast(generated_datetime as date) end) days_with_adspend,
		max(case when cast(generated_datetime as date) - min_one.min_date_spend_two <= 120 and a.total_spend_amount > 0 then cast(generated_datetime as date) end) max_date_spend_120,
		max_date_spend - min_date_spend days_bet_spend, 
		current_date - min_date_spend days_bet_cur_min_spend, 
		max_date_spend_120 - min_date_spend days_bet_spend_120
	from user_gp.ads_rcncld_intrmdt_rpt as a
		left join sandbox.pai_deals as b on a.deal_id = b.deal_uuid
		left join (select 
		             mer.merchant_uuid, 
		             mer.l2,
		             mer.first_launch_date,
		             mer.last_live_date,
		             mer.merchant_name,
		             max(per.Merchant_Permalink) merch_permalink
		           from 
		           sandbox.pai_merchants as mer 
		           left join (select Account_ID_18 as acc_id,MAX(Merchant_Permalink) Merchant_Permalink  from dwh_base_sec_view.sf_account group by 1) as per on mer.account_id = per.acc_id
		            group by 1,2,3,4,5
		            ) as c on b.merchant_uuid = c.merchant_uuid 
		left join np_sl_mrch_120 as min_one on a.merchant_id = min_one.merchant_id
		left join (select merchant_uuid, 
                     max(offer_sent_on) lst_avail_offer_sent_on, 
                     max(offer_availed_on) offer_availed_on
                    from sandbox.np_merch_free_credits 
                    where offer_availed_on is not null
                    group by 1) as xyz on a.merchant_id = xyz.merchant_uuid
	where a.is_self_serve = 1
	group by 1,2,3,4,5,6,7
	having min_date_spend is not null
) with data on commit preserve rows
;


create volatile table np_sl_mrch_perf as
(select
    a.merchant_id,
    sum(impressioned) total_imps_sl,
    sum(clicked) total_clicks_sl,
    sum(converted) total_ords_sl,
    sum(price_with_discount) orders_rev_sl, 
    sum(total_spend_amount) adspend_sl, 
    orders_rev_sl/nullifzero(adspend_sl) roas_sl, 
    sum(case when cast(generated_datetime as date) <= min_date_spend + 30 then impressioned end) total_imps_30_sl,
    sum(case when cast(generated_datetime as date) <= min_date_spend + 30 then clicked end) total_clicks_30_sl,
    sum(case when cast(generated_datetime as date) <= min_date_spend + 30 then converted end) total_ords_30_sl,
    sum(case when cast(generated_datetime as date) <= min_date_spend + 30 then price_with_discount end) orders_rev_30_sl,
    sum(case when cast(generated_datetime as date) <= min_date_spend + 30 then total_spend_amount end) adspend_30_sl,
    orders_rev_30_sl/nullifzero(adspend_30_sl) roas_30_sl
from user_gp.ads_rcncld_intrmdt_rpt as a
   left join (select merchant_id, min_date_spend from np_sl_mrch_spend) as main on a.merchant_id = main.merchant_id
   where a.is_self_serve = 1
  group by 1) with data on commit preserve rows;


create volatile table np_sl_mrch_perf_group as
(select 
   c.merchant_uuid, 
   sum(case when cast(report_date as date) < min_date_spend and cast(report_date as date) >= min_date_spend - 30 then transactions end) pre_sl_30_orders_groupon,
   sum(case when cast(report_date as date) >= current_date - 30 then transactions end) last_30_day_orders_groupon
from user_edwprod.agg_gbl_financials_deal as a 
left join sandbox.pai_deals as b on a.deal_id = b.deal_uuid
left join sandbox.pai_merchants as c on b.merchant_uuid = c.merchant_uuid
join (select merchant_id, min_date_spend from np_sl_mrch_spend) as main on c.merchant_uuid = main.merchant_id
group by 1) with data on commit preserve rows
;


create volatile table np_sl_mrch_camp as     
(select 
		 b.merchant_uuid, 
         max(case when status = 'ACTIVE' then 1 else 0 end) has_active_campaign, 
         max(case when status = 'PAUSED' then 1 else 0 end) has_pause_campaign, 
         max(case when status = 'ARCHIVED' then 1 else 0 end) has_archived_campaign,
         max(case when status = 'PENDING' then 1 else 0 end) has_pending_campaign,
         max(case when status <> 'ACTIVE' then cast(substr(update_datetime, 1,10) as date) else cast(substr(create_datetime, 1,10) as date) end ) last_change_date
     from sandbox.np_citrusad_campaigns as a 
     left join sandbox.pai_deals as b on a.product = b.deal_uuid
     group by 1
) with data on commit preserve rows;



create multiset volatile table np_sl_change_status as 
(       select 
           a.merchant_id, 
           cast(max(case when status = 'ACTIVE' then a.status end) as varchar(64)) status,
           cast(max(case when status = 'ACTIVE' then a.status end) as varchar(64)) status_last_spend
        from
        sandbox.np_sponsored_campaign as a
        join 
        (select merchant_id, 
                max(case when status = 'ACTIVE' then 1 else 0 end) has_active_campaign
              from
                sandbox.np_sponsored_campaign
                group by 1) as b on a.merchant_id = b.merchant_id and b.has_active_campaign = 1
          group by 1
          UNION ALL      
          select 
             a.merchant_id, 
             max(case when cast(substr(a.update_datetime,1, 10) as date) - c.min_date_spend > 30 then 'ACTIVE'
                  else status end) status, 
             max(case when cast(substr(a.update_datetime,1, 10) as date) >= c.max_date_spend - 10 then status else 'ACTIVE' end) status_last_spend ---it was active if nothing was changed
          from
             sandbox.np_sponsored_campaign as a
          join 
             (select merchant_id, 
                max(case when status = 'ACTIVE' then 1 else 0 end) has_active_campaign, 
                max(update_datetime) update_datetime
              from
                sandbox.np_sponsored_campaign
                group by 1) as b on a.merchant_id = b.merchant_id and b.has_active_campaign = 0 and a.update_datetime = b.update_datetime
           left join np_sl_mrch_spend as c on a.merchant_id = c.merchant_id
          where a.status in ( 'ACTIVE','ARCHIVED', 'PAUSED', 'INACTIVE')
          group by 1
) with data on commit preserve rows;


create multiset volatile table np_sl_merch_topups as     
(select 
   merchant_id,
   max(case when last_top_up = 1 then cast(substr(event_time, 1,10) as date) end) last_top_up_date,
   sum(case when last_top_up = 1 then amount  end) last_top_up_amount,
   sum(amount) total_top_ups_amount
from 
(select merchant_id, 
       event_time,
       ROW_NUMBER() over (partition by merchant_id order by cast(substr(event_time, 1,10) as date) desc) last_top_up, 
       amount
from sandbox.np_merchant_topup_orders) as fin
group by 1
) with data on commit preserve rows;


create volatile table np_sl_balance as     
(select 
   merchant_id, 
   date_to_consider,
   day_to_consider,
   citrusad_import_date,
   ROW_NUMBER () over(partition by merchant_id order by day_to_consider asc) last_30day_balance, 
   total_balance
from 
(SELECT a.citrusad_import_date, 
       b.merchant_id,
       case when c.days_bet_spend <= 30 then c.max_date_spend else min_date_spend + 30 end date_to_consider,
       case when c.days_bet_spend <= 30 then abs(cast(c.max_date_spend as date) - cast(a.citrusad_import_date as date)) 
            else abs(cast(min_date_spend as date) + 30  - cast(a.citrusad_import_date as date)) end day_to_consider,
       sum(a.available_balance) total_balance
     FROM sandbox.np_slad_wlt_blc_htry as a 
     left join sandbox.citrusad_team_wallet as b on a.wallet_id = b.wallet_id 
     join np_sl_mrch_spend as c on b.merchant_id = c.merchant_id
     group by 1,2,3,4) fin where day_to_consider <= 10
) with data on commit preserve rows;

create volatile table np_sl_balance2 as     
(select 
   merchant_id, 
   date_to_consider,
   day_to_consider,
   citrusad_import_date,
   ROW_NUMBER () over(partition by merchant_id order by cast(citrusad_import_date as date) desc) last_balance, 
   total_balance
from 
(SELECT a.citrusad_import_date, 
       b.merchant_id,
       case when c.days_bet_spend <= 30 then c.max_date_spend else min_date_spend + 30 end date_to_consider,
       case when c.days_bet_spend <= 30 then abs(cast(c.max_date_spend as date) - cast(a.citrusad_import_date as date)) 
            else abs(cast(min_date_spend as date) + 30  - cast(a.citrusad_import_date as date)) end day_to_consider,
       sum(a.available_balance) total_balance
     FROM sandbox.np_slad_wlt_blc_htry as a 
     left join sandbox.citrusad_team_wallet as b on a.wallet_id = b.wallet_id 
     join np_sl_mrch_spend as c on b.merchant_id = c.merchant_id
     group by 1,2,3,4) fin 
) with data on commit preserve rows;


create volatile table np_sl_balance3 as     
(select 
   merchant_id, 
   date_to_consider,
   day_to_consider,
   citrusad_import_date,
   ROW_NUMBER () over(partition by merchant_id order by cast(citrusad_import_date as date) asc) last_close30_balance, 
   total_balance
from 
(SELECT a.citrusad_import_date, 
       b.merchant_id,
       case when c.days_bet_spend <= 30 then c.max_date_spend else min_date_spend + 30 end date_to_consider,
       case when c.days_bet_spend <= 30 then abs(cast(c.max_date_spend as date) - cast(a.citrusad_import_date as date)) 
            else abs(cast(min_date_spend as date) + 30  - cast(a.citrusad_import_date as date)) end day_to_consider,
       sum(a.available_balance) total_balance
     FROM sandbox.np_slad_wlt_blc_htry as a 
     left join sandbox.citrusad_team_wallet as b on a.wallet_id = b.wallet_id 
     join np_sl_mrch_spend as c on b.merchant_id = c.merchant_id
     group by 1,2,3,4) fin where cast(citrusad_import_date as date) >= date_to_consider
) with data on commit preserve rows;

      

drop table sandbox.np_sl_ss_retention;
create multiset table sandbox.np_sl_ss_retention as (
select 
	a.*, 
	b.last_change_date,
	c.last_login_mc,
    case when days_bet_spend > 30 then 1 else 0 end has_spend_after_30,
    case when days_bet_cur_min_spend > 30 then 1 else 0 end live_30_days, 
	---denominator is tied to atleast 30 days spend for lower one
    case when days_bet_cur_min_spend <= 30 then '30 days have not passed'
         when days_bet_spend > 30 then 'Retained after 30 days'
         else 'Attrited after 30 days'
         end case_retained_merchant,
    case when days_bet_cur_min_spend > 30 and has_active_campaign = 1 then 1 
         when days_bet_cur_min_spend > 30 and b.last_change_date - a.min_date_spend > 30 then 1 else 0 end active_campaign_after_30,
    case when days_bet_cur_min_spend > 30 and has_active_campaign = 1 then 'active at 30 days'
         when days_bet_cur_min_spend > 30 and b.last_change_date - a.min_date_spend > 30 then 'active at 30 days' 
         when days_bet_cur_min_spend > 30 and b.last_change_date - a.min_date_spend <= 30 and has_pause_campaign = 1 then 'paused at 30 days'
         when days_bet_cur_min_spend > 30 and b.last_change_date - a.min_date_spend <= 30 and has_archived_campaign = 1 then 'archived at 30 days'
         when days_bet_cur_min_spend > 30 then 'enough time passed (30 days) - no info about campaign'
         else 'not enough time passed (30 days)' end case_active_campaign,
    case when days_bet_cur_min_spend > 30 then i.status else 'not enough time passed (30 days)' end case_ui_status,
    i.status_last_spend last_spend_case_ui_status,
    f.roas_sl, 
    f.total_ords_sl,
    g.last_30_day_orders_groupon,
    g.pre_sl_30_orders_groupon,
    adspend_sl,
    case when g.last_30_day_orders_groupon is null then 'a. 0 orders seen'
         when g.last_30_day_orders_groupon = 0 then 'a. 0 orders seen'
         when g.last_30_day_orders_groupon <= 10 then 'b. <= 10 orders seen'
         when g.last_30_day_orders_groupon <= 20 then 'c. 11 - 20 orders seen'
         when g.last_30_day_orders_groupon <= 30 then 'd. 21 - 30 orders seen'
         when g.last_30_day_orders_groupon > 30 then 'e. > 30 orders seen'
         end case_last_30_day_grpn_ord,
    case when g.pre_sl_30_orders_groupon is null then 'a. 0 orders seen'
         when g.pre_sl_30_orders_groupon = 0 then 'a. 0 orders seen'
         when g.pre_sl_30_orders_groupon <= 10 then 'b. <= 10 orders seen'
         when g.pre_sl_30_orders_groupon <= 20 then 'c. 11 - 20 orders seen'
         when g.pre_sl_30_orders_groupon <= 30 then 'd. 21 - 30 orders seen'
         when g.pre_sl_30_orders_groupon > 30 then 'e. > 30 orders seen'
         end case_last_30_day_grpn_pre,
    case when days_bet_cur_min_spend > 30 and days_bet_spend <= 30 and max_date_spend < last_live_date then '30 day - merchant attrited before stopping on groupon'
         when days_bet_cur_min_spend > 30 and days_bet_spend <= 30 then '30 day - merchant no longer on groupon'
         when days_bet_cur_min_spend > 30 and max_date_spend < last_live_date then '30 day - merchant still active'
         else 'not enough time passed (30 days)' end case_live_groupon,
    case when adspend_sl <= 10 then 'a.adspend between $0 - $10'
	     when adspend_sl <= 50 then 'b.adspend between $10 - $50'
	     when adspend_sl <= 100 then 'c.adspend between $50 - $100'
	     when adspend_sl <= 200 then 'd.adspend between $100 - $200'
	     when adspend_sl > 200 then 'e.adspend  > $200' end adspend_category,
	case when roas_sl <= 0 then 'a.No orders seen'
	     when roas_sl <= 0.25 then 'b.roas between 0% - 25%'
	     when roas_sl <= 0.5 then 'c.roas between 25% - 50%'
	     when roas_sl <= 0.75 then 'd.roas between 50% - 75%'
	     when roas_sl <= 1 then 'e.roas between 75% - 100%'
	     when roas_sl <= 1.25 then 'f.roas between 100% - 125%'
	     when roas_sl > 1.25 then 'g.roas > 125%' end roas_category,
	CASE WHEN days_with_adspend <= 5 THEN 'a.Has adspend for <= 5 days'
	     WHEN days_with_adspend <= 10 THEN 'b.Has adspend for 6 - 10 days'
	     WHEN days_with_adspend <= 15 THEN 'c.Has adpsend for 11 - 15 days'
	     WHEN days_with_adspend <= 20 THEN 'd.Has adpsend for 16 - 20 days'
	     WHEN days_with_adspend <= 25 THEN 'e.Has adpsend for 21 - 25 days'
	     WHEN days_with_adspend > 25 THEN 'f.Has adpsend for > 25 days' 
	     END days_with_adspend_category,
	case when max_date_spend >= current_date - 7 then 'adspend in last 7 days'
	          when max_date_spend >= current_date - 15 then 'adspend in last 15 days'
              when max_date_spend >= current_date - 30 then 'adspend in last 30 days'
              when max_date_spend >= current_date - 45 then 'adspend in last 45 days'
              when max_date_spend >= current_date - 60 then 'adspend in last 60 days'
              else 'has no adspend in last 60 days' end
              last_adspend_day_category,
     case when d.total_balance_30 <= 0  then 'a. No balance available'
          when d.total_balance_30 <= 10 then 'b.<= 10 USD in the wallet'
          when d.total_balance_30 <= 25 then 'c.<= 25 USD in the wallet'
          when d.total_balance_30 <= 50 then 'd.<= 50 USD in the wallet'
          when d.total_balance_30 <= 100 then 'e.<= 100 USD in the wallet'
          when d.total_balance_30 <= 250 then 'f.<= 250 USD in the wallet'
          when d.total_balance_30 <= 500 then 'g.<= 500 USD in the wallet'
          when d.total_balance_30 <= 1000 then 'h.<= 1000 USD in the wallet'
          when d.total_balance_30 <= 2000 then 'i.<= 2000 USD in the wallet'
          when d.total_balance_30 >2000 then 'j.more than 2000 usd in wallet'
          when d.total_balance_30 is null then 'k.no data available'
         end available_balance_cohort, 
     case when e.last_balance_available <= 0  then 'a. No balance available'
          when e.last_balance_available <= 10 then 'b.<= 10 USD in the wallet'
          when e.last_balance_available <= 25 then 'c.<= 25 USD in the wallet'
          when e.last_balance_available<= 50 then 'd.<= 50 USD in the wallet'
          when e.last_balance_available <= 100 then 'e.<= 100 USD in the wallet'
          when e.last_balance_available <= 250 then 'f.<= 250 USD in the wallet'
          when e.last_balance_available <= 500 then 'g.<= 500 USD in the wallet'
          when e.last_balance_available <= 1000 then 'h.<= 1000 USD in the wallet'
          when e.last_balance_available <= 2000 then 'i.<= 2000 USD in the wallet'
          when e.last_balance_available >2000 then 'j.more than 2000 usd in wallet'
          when e.last_balance_available is null then 'k.no data available'
         end current_balance_cohort, 
     case when e2.balance_close_30 <= 0  then 'a. No balance available'
          when e2.balance_close_30 <= 10 then 'b.<= 10 USD in the wallet'
          when e2.balance_close_30 <= 25 then 'c.<= 25 USD in the wallet'
          when e2.balance_close_30<= 50 then 'd.<= 50 USD in the wallet'
          when e2.balance_close_30 <= 100 then 'e.<= 100 USD in the wallet'
          when e2.balance_close_30 <= 250 then 'f.<= 250 USD in the wallet'
          when e2.balance_close_30 <= 500 then 'g.<= 500 USD in the wallet'
          when e2.balance_close_30 <= 1000 then 'h.<= 1000 USD in the wallet'
          when e2.balance_close_30 <= 2000 then 'i.<= 2000 USD in the wallet'
          when e2.balance_close_30 >2000 then 'j.more than 2000 usd in wallet'
          when e2.balance_close_30 is null then 'k.no data available'
         end balance_close_30_cohort, 
     d.total_balance_30, 
     e.last_balance_available,
     e2.balance_close_30,
     h.last_top_up_date,
     h.last_top_up_amount,
     h.total_top_ups_amount
from 
    np_sl_mrch_spend as a
  left join 
    np_sl_mrch_camp as b on a.merchant_id = b.merchant_uuid
  left join 
     (select merchantid, max(eventdate) last_login_mc from sandbox.np_ss_sl_interaction_agg group by 1) as c on a.merchant_id = c.merchantid
  left join 
    (select merchant_id, total_balance total_balance_30, citrusad_import_date from np_sl_balance where last_30day_balance = 1) as d on a.merchant_id = d.merchant_id
  left join 
    (select merchant_id, total_balance last_balance_available, citrusad_import_date from np_sl_balance2 where last_balance = 1) as e on a.merchant_id = e.merchant_id
  left join 
    (select merchant_id, total_balance balance_close_30, citrusad_import_date from np_sl_balance3 where last_close30_balance = 1) as e2 on a.merchant_id = e2.merchant_id
  left join np_sl_mrch_perf as f on a.merchant_id = f.merchant_id
  left join np_sl_mrch_perf_group as g on a.merchant_id = g.merchant_uuid
  left join np_sl_merch_topups as h on a.merchant_id = h.merchant_id
  left join 
    np_sl_change_status as i on a.merchant_id = i.merchant_id
) with data
;



------------------RETARGETTING SHEET



SELECT 
   a.merchant_id, 
   merchant_name,
   merch_permalink,
   first_launch_date groupon_launch_date,
   last_live_date groupon_last_live_date,
   min_date_spend  first_spend_date_sl, 
   min_date_spend_week first_spend_week_sl,
   max_date_spend last_spend_date_sl,
   max_date_spend_week last_spend_week_sl,
   case_retained_merchant t_30_day_retention,
   last_adspend_day_category,
   lst_avail_offer_sent_on availed_free_credit_offer_sent_on,
   balance_close_30_cohort acct_bal_earliest_after_30_days,
   balance_close_30,
   current_balance_cohort last_available_balance_cohort,
   last_balance_available,
   case_ui_status t_30_day_ui_status,
   last_spend_case_ui_status last_ui_status,
   case_live_groupon,
   last_top_up_date, 
   last_top_up_amount,
   total_top_ups_amount,
   last_login_mc, 
   roas_category, 
   roas_sl, 
   total_ords_sl,
   adspend_sl,
   adspend_category,
   last_30_day_orders_groupon,
   pre_sl_30_orders_groupon, 
   all_time_deals_on_sl, 
   reasons
FROM sandbox.np_sl_ss_retention as a 
left join 
    (select merchant_uuid, 
            count(distinct citrusad_campaign_id) all_time_campaigns, 
            count(distinct product) all_time_deals_on_sl
     from sandbox.np_sl_cmp_onb group by 1) as b on a.merchant_id = b.merchant_uuid
left join 
  (select 
   merchant_id, 
   reasons,
   ROW_NUMBER () over(partition by merchant_id order by cast(substr(create_datetime, 1, 10) as date) desc) row_num 
from sandbox.np_ss_feedback where feedback_context  = 'CAMPAIGN_PAUSE') as c on a.merchant_id  = c.merchant_id and c.row_num = 1


-----------------------------------CAMPAIGN PERFORMANCE


select 
   citrusad_campaign_id , 
   campaign_subtype,
   camp_start_date,
   camp_end_date,
   product deal_uuid, 
   merchant_uuid,
   l2,
   target_locations, 
   spend_type,
   budget campaign_budget,
   max_cpc,
   budget_type_mc,
   budget_value_mc,
   status_mc,
   dealrecommended,
   merchant_advisor,
   create_week,
   update_week,
   last_spend_date,
   tot_search_words,
   search_term_case,
   search_terms,
   lst_avail_offer_sent_on, 
   targeted_category,
   total_ords total_sl_ords, 
   orders_rev orders_sl_revenue, 
   adspend, 
   roas,
   b.reasons
from sandbox.np_sl_cmp_onb as a 
left join 
(select 
   merchant_id, 
   reasons,
   ROW_NUMBER () over(partition by merchant_id order by cast(substr(create_datetime, 1, 10) as date) desc) row_num 
from sandbox.np_ss_feedback where feedback_context  = 'CAMPAIGN_PAUSE') as b on a.merchant_uuid = b.merchant_id and b.row_num = 1 and a.status <> 'ACTIVE'
left join 
   (select 
        campaign_id,
		sum(impressioned) total_imps, 
    	sum(clicked) total_clicks, 
    	sum(converted) total_ords,
    	sum(order_quantity) total_units, 
    	sum(price_with_discount) orders_rev, 
    	sum(total_spend_amount) adspend, 
    	sum(click_spend_amount) clc_spend, 
    	orders_rev/nullifzero(adspend) roas, 
    	case when roas > 1 then 1 else 0 end campw_roas_gone
	from user_gp.ads_rcncld_intrmdt_rpt
    group by 1) perf on a.citrusad_campaign_id = perf.campaign_id


-----------------------------------MERCHANT - CAMPAIGN AGG DETAIL BASED ON ONBOARDING last 30 days
    

SELECT 
   a.merchant_id, 
   a.merchant_name,
   a.merch_permalink,
   d.deal_uuid,
   d.deal_permalink,
   a.first_launch_date groupon_launch_date,
   a.last_live_date groupon_last_live_date,
   a.min_date_spend  first_spend_date_sl, 
   a.min_date_spend_week first_spend_week_sl,
   a.max_date_spend last_spend_date_sl,
   a.max_date_spend_week last_spend_week_sl,
   a.case_retained_merchant t_30_day_retention,
   a.last_adspend_day_category,
   a.lst_avail_offer_sent_on availed_free_credit_offer_sent_on,
   a.balance_close_30_cohort acct_bal_earliest_after_30_days,
   a.balance_close_30,
   a.current_balance_cohort last_available_balance_cohort,
   a.last_balance_available,
   a.case_ui_status t_30_day_ui_status,
   a.last_spend_case_ui_status last_ui_status,
   a.case_live_groupon,
   a.last_top_up_date, 
   a.last_top_up_amount,
   a.total_top_ups_amount,
   a.last_login_mc, 
   a.roas_category, 
   a.roas_sl, 
   a.total_ords_sl,
   a.adspend_sl,
   a.adspend_category,
   a.last_30_day_orders_groupon,
   a.pre_sl_30_orders_groupon, 
   b.all_time_deals_on_sl, 
   e.sentdate email_sent_date,
   reasons,
   d.l2, 
   d.dealrecommended,
   d.merchant_advisor, 
   d.target_locations, 
   d.status_mc,
   d.budget_type_mc,
   d.search_terms,
   d.search_max_cpc,
   d.category_max_cpc
FROM sandbox.np_sl_ss_retention as a 
left join 
    (select merchant_uuid, 
            count(distinct citrusad_campaign_id) all_time_campaigns, 
            count(distinct product) all_time_deals_on_sl
     from sandbox.np_sl_cmp_onb group by 1) as b on a.merchant_id = b.merchant_uuid
left join 
  (select 
   merchant_id, 
   reasons,
   ROW_NUMBER () over(partition by merchant_id order by cast(substr(create_datetime, 1, 10) as date) desc) row_num 
from sandbox.np_ss_feedback where feedback_context  = 'CAMPAIGN_PAUSE') as c on a.merchant_id  = c.merchant_id and c.row_num = 1
left join 
  (select
     merchant_uuid, 
     product deal_uuid,
     deal_permalink,
     l2,
     max(dealrecommended) dealrecommended,
     max(merchant_advisor) merchant_advisor, 
     max(target_locations) target_locations,
     max(status_mc) status_mc,
     max(budget_type_mc) budget_type_mc,
     max(search_terms) search_terms, 
     max(tot_search_words) tot_search_words, 
     max(case when campaign_subtype = 'SEARCH_ONLY' then max_cpc end) search_max_cpc,
     max(case when campaign_subtype = 'CATEGORY_ONLY' then max_cpc end) category_max_cpc
  from 
  (select ax.*, 
          ay.permalink deal_permalink,
          DENSE_RANK () over (partition by product order by create_date desc) dense_row 
    from sandbox.np_sl_cmp_onb as ax
    left join sandbox.pai_deals as ay on ax.product = ay.deal_uuid
   ) as fin
  where dense_row = 1
  group by 1,2,3,4) as d on a.merchant_id = d.merchant_uuid
 left join 
   (SELECT
	      merchant_uuid,
	      max(cast(substr(sentdate, 1,10) as date)) sentdate,
	      max(case when length(substr(firstopendate,1,10)) >= 10 then cast(substr(firstopendate,1,10) as date) end)  email_open_date
     FROM
	      sandbox.SFMC_EmailEngagement
     where 
	      journeyname 
	       in 
	  ('MM_Sponsored_DormantMerchants',
	         'MM_Sponsored_DormantMerchants',
	         'MM_Sponsored_DormantMerchants',
	         'MM_Sponsored_ReengagementSeries',
	         'MM_Retention_Milestones',
	         'MM_Retention_NotSellingUnits',
	         'MM_Sponsored_ClickedNavinMC',
	         'MM_Sponsored_DropOffSeries',
	         'MM_Onboarding_OnboardingSeries')
	     group by merchant_uuid  
	         ) as e on a.merchant_id = e.merchant_uuid
;



create volatile table np_sl_mrch_120 as
(select 
		merchant_id,
		min(case when a.total_spend_amount > 0  then cast(generated_datetime as date) end) min_date_spend_two,
		trunc(min(case when a.total_spend_amount > 0 then cast(generated_datetime as date) end), 'iw')+6 min_date_spend_week
	from user_gp.ads_rcncld_intrmdt_rpt as a 
		left join sandbox.pai_deals as b on a.deal_id = b.deal_uuid
		left join sandbox.pai_merchants as c on b.merchant_uuid = c.merchant_uuid 
	where a.is_self_serve = 1
	group by 1
	having min_date_spend_two is not null
) with data on commit preserve rows;




create volatile table np_sl_mrch_spend as
(select 
		a.merchant_id,
		c.merchant_name,
		c.merch_permalink,
		c.l2, 
		c.first_launch_date,
		c.last_live_date,
		xyz.lst_avail_offer_sent_on,
		min(case when a.total_spend_amount > 0  then cast(generated_datetime as date) end) min_date_spend,
		trunc(min(case when a.total_spend_amount > 0 then cast(generated_datetime as date) end), 'iw')+6 min_date_spend_week,
		max(case when a.total_spend_amount > 0  then cast(generated_datetime as date) end) max_date_spend,
		trunc(max(case when a.total_spend_amount > 0  then cast(generated_datetime as date) end), 'iw')+6 max_date_spend_week,
		count(distinct case when a.total_spend_amount > 0  then cast(generated_datetime as date) end) days_with_adspend,
		max(case when cast(generated_datetime as date) - min_one.min_date_spend_two <= 120 and a.total_spend_amount > 0 then cast(generated_datetime as date) end) max_date_spend_120,
		max_date_spend - min_date_spend days_bet_spend, 
		current_date - min_date_spend days_bet_cur_min_spend, 
		max_date_spend_120 - min_date_spend days_bet_spend_120
	from user_gp.ads_rcncld_intrmdt_rpt as a
		left join sandbox.pai_deals as b on a.deal_id = b.deal_uuid
		left join (select 
		             mer.merchant_uuid, 
		             mer.l2,
		             mer.first_launch_date,
		             mer.last_live_date,
		             mer.merchant_name,
		             max(per.Merchant_Permalink) merch_permalink
		           from 
		           sandbox.pai_merchants as mer 
		           left join (select Account_ID_18 as acc_id,MAX(Merchant_Permalink) Merchant_Permalink  from dwh_base_sec_view.sf_account group by 1) as per on mer.account_id = per.acc_id
		            group by 1,2,3,4,5
		            ) as c on b.merchant_uuid = c.merchant_uuid 
		left join np_sl_mrch_120 as min_one on a.merchant_id = min_one.merchant_id
		left join (select merchant_uuid, 
                     max(offer_sent_on) lst_avail_offer_sent_on, 
                     max(offer_availed_on) offer_availed_on
                    from sandbox.np_merch_free_credits 
                    where offer_availed_on is not null
                    group by 1) as xyz on a.merchant_id = xyz.merchant_uuid
	where a.is_self_serve = 1
	group by 1,2,3,4,5,6,7
	having min_date_spend is not null
) with data on commit preserve rows
;

----------------------------------------RETENTION DAYS
select * from user_edwprod.fact_gbl_transactions;

create multiset volatile table np_ret_days as (
SELECT ROW_NUMBER() OVER (ORDER BY day_rw) - 1 AS days
FROM user_groupondw.dim_day 
WHERE day_rw BETWEEN CURRENT_DATE - 125 AND CURRENT_DATE
) with data on commit preserve rows;



create volatile multiset table np_wow_retention as (
select 
    '4 week retention' category,
    x.next_report_week,
    x.l2,
    x.lst_avail_offer_sent_on,
    count(distinct y.merchant_id) retained_supp_adspend, 
	count(x.merchant_id) total_supp_adspend,
	cast(retained_supp_adspend as float)/nullifzero(total_supp_adspend) rtn_rate
from 
   (select 
		trunc(cast(generated_datetime as date), 'iw')+6 report_week,
        trunc(cast(generated_datetime as date), 'iw') + 34 next_report_week,
        trunc(cast(generated_datetime as date), 'iw') - 29 last_report_week, 
		merchant_id,
		lst_avail_offer_sent_on,
		l2,
    	sum(total_spend_amount) adspend
	from user_gp.ads_rcncld_intrmdt_rpt as a 
	   left join sandbox.pai_merchants as b on a.merchant_id = b.merchant_uuid
	   left join (select merchant_uuid, 
                     max(offer_sent_on) lst_avail_offer_sent_on, 
                     max(offer_availed_on) offer_availed_on
                    from sandbox.np_merch_free_credits 
                    where offer_availed_on is not null
                    group by 1) as xyz on a.merchant_id = xyz.merchant_uuid
		where a.is_self_serve = 1
		group by 1,2,3,4,5,6
		having adspend > 0) as x
left join 
	(select 
		trunc(cast(generated_datetime as date), 'iw')+6 report_week,
        trunc(cast(generated_datetime as date), 'iw') + 34 next_report_week,
        trunc(cast(generated_datetime as date), 'iw') - 29 last_report_week, 
		merchant_id,
    	sum(total_spend_amount) adspend
	from user_gp.ads_rcncld_intrmdt_rpt as a 
		where a.is_self_serve = 1
		group by 1,2,3,4
		having adspend > 0) as y on x.merchant_id = y.merchant_id and x.next_report_week = y.report_week
    group by 1,2,3,4
union all 
select 
    '1 week retention' category,
    x.next_report_week,
    x.l2,
    x.lst_avail_offer_sent_on,
    count(distinct y.merchant_id) retained_supp_adspend,
	count(x.merchant_id) total_supp_adspend,
	cast(retained_supp_adspend as float)/nullifzero(total_supp_adspend) rtn_rate
from 
   (select 
		trunc(cast(generated_datetime as date), 'iw')+6 report_week,
        trunc(cast(generated_datetime as date), 'iw') + 13 next_report_week,
        trunc(cast(generated_datetime as date), 'iw') - 8 last_report_week, 
		merchant_id,
		lst_avail_offer_sent_on,
		l2,
    	sum(total_spend_amount) adspend
	from user_gp.ads_rcncld_intrmdt_rpt as a 
	    left join sandbox.pai_merchants as b on a.merchant_id = b.merchant_uuid
	    left join (select merchant_uuid, 
                     max(offer_sent_on) lst_avail_offer_sent_on, 
                     max(offer_availed_on) offer_availed_on
                    from sandbox.np_merch_free_credits 
                    where offer_availed_on is not null
                    group by 1) as xyz on a.merchant_id = xyz.merchant_uuid
		where a.is_self_serve = 1
		group by 1,2,3,4,5,6
		having adspend > 0) as x
left join 
	(select 
		trunc(cast(generated_datetime as date), 'iw')+6 report_week,
        trunc(cast(generated_datetime as date), 'iw') + 13 next_report_week,
        trunc(cast(generated_datetime as date), 'iw') - 8 last_report_week, 
		merchant_id,
    	sum(total_spend_amount) adspend
	from user_gp.ads_rcncld_intrmdt_rpt as a 
		where a.is_self_serve = 1
		group by 1,2,3,4
		having adspend > 0) as y on x.merchant_id = y.merchant_id and x.next_report_week = y.report_week
   group by 1,2,3,4
) with data on commit preserve rows;


drop table sandbox.np_sl_ss_retention2;
create multiset table sandbox.np_sl_ss_retention2 as (
select 
   cast('Activation Ret' as varchar(120)) category,
   min_date_spend_week,
   lst_avail_offer_sent_on,
   days, 
   l2,
   sum(days_retained_spend) days_retained_spend,
   sum(days_eligible) days_eligible, 
   sum(days_active_camp) days_active_camp
from 
(select 
    b.days, 
    a.*,
    min_date_spend + days date_considered,
    case when days <= days_bet_spend_120 then 1 else 0 end days_retained_spend, 
    case when days <= days_bet_cur_min_spend  then 1 else 0 end days_eligible,
    case when active_campaign_after_30 = 1 then 1
         when date_considered <= last_change_date then 1 else 0 end days_active_camp
from 
(select 
  merchant_id, 
  l2, 
  min_date_spend, 
  lst_avail_offer_sent_on,
  min_date_spend_week,
  days_bet_spend_120,
  days_bet_cur_min_spend,
  active_campaign_after_30,
  last_change_date
from sandbox.np_sl_ss_retention) as a 
left join np_ret_days as b on 'apple' = 'apple') as fin
group by 1,2,3,4,5
union all 
  select 
     category, 
     next_report_week, 
     lst_avail_offer_sent_on,
     null other_one, 
     l2,  
     retained_supp_adspend, 
     total_supp_adspend,
     null other_two
  from 
    np_wow_retention
) with data;
 
select * from sandbox.np_sl_ss_retention;

------------------------------------------------------------------------------------------------------------------------------OKRS

select trunc(cast(generated_datetime as date), 'iw')+6 from user_gp.ads_rcncld_intrmdt_rpt 
where trunc(cast(generated_datetime as date), 'iw')+6 = trunc(current_date, 'iw') - 1;

create volatile multiset table np_qtr_perf_lvl as 
(select 
		---trunc(cast(generated_datetime as date), 'iw')+6 report_week,
		case when lst_avail_offer_sent_on is null then 'original' else cast(lst_avail_offer_sent_on as varchar(40)) end lst_avail_offer_sent_on,
		merchant_id,
		sum(impressioned) total_imps, 
    	sum(clicked) total_clicks, 
    	sum(converted) total_ords,
    	sum(order_quantity) total_units, 
    	sum(price_with_discount) orders_rev, 
    	sum(total_spend_amount) adspend, 
    	sum(click_spend_amount) clc_spend, 
    	orders_rev/nullifzero(adspend) roas, 
    	case when roas > 1 then 1 else 0 end merch_roas_gone
	from user_gp.ads_rcncld_intrmdt_rpt as a 
	     left join (select merchant_uuid, 
                     max(offer_sent_on) lst_avail_offer_sent_on, 
                     max(offer_availed_on) offer_availed_on
                    from sandbox.np_merch_free_credits 
                    where offer_availed_on is not null
                    group by 1) as xyz on a.merchant_id = xyz.merchant_uuid
		left join sandbox.pai_deals as b on a.deal_id = b.deal_uuid
		left join sandbox.np_citrusad_campaigns as c on a.campaign_id = c.citrusad_campaign_id
		where a.is_self_serve = 1 and cast(generated_datetime as date) >= cast('2022-07-01' as date )
		group by 1,2) with data on commit preserve rows;
	
create volatile multiset table np_daily_perf_lvl as 
(select 
		trunc(cast(generated_datetime as date), 'iw')+6 report_week,
		case when lst_avail_offer_sent_on is null then 'original' else cast(lst_avail_offer_sent_on as varchar(40)) end lst_avail_offer_sent_on,
		merchant_id,
		sum(impressioned) total_imps, 
    	sum(clicked) total_clicks, 
    	sum(converted) total_ords,
    	sum(order_quantity) total_units, 
    	sum(price_with_discount) orders_rev, 
    	sum(total_spend_amount) adspend, 
    	sum(click_spend_amount) clc_spend, 
    	orders_rev/nullifzero(adspend) roas, 
    	case when roas > 1 then 1 else 0 end merch_roas_gone
	from user_gp.ads_rcncld_intrmdt_rpt as a 
	     left join (select merchant_uuid, 
                     max(offer_sent_on) lst_avail_offer_sent_on, 
                     max(offer_availed_on) offer_availed_on
                    from sandbox.np_merch_free_credits 
                    where offer_availed_on is not null
                    group by 1) as xyz on a.merchant_id = xyz.merchant_uuid
		left join sandbox.pai_deals as b on a.deal_id = b.deal_uuid
		left join sandbox.np_citrusad_campaigns as c on a.campaign_id = c.citrusad_campaign_id
		where a.is_self_serve = 1 and cast(generated_datetime as date) >= cast('2022-07-01' as date )
		group by 1,2,3) with data on commit preserve rows;



select 
   'quarter' category,
   cast(null as date) date_category,
   count(distinct case when adspend > 0 then merchant_id end) qtr_merchant_with_adspend, 
   count(distinct case when merch_roas_gone = 1 then merchant_id end) qtr_merchant_with_roas_gone, 
   sum(adspend)/count(distinct case when adspend > 0 then merchant_id end) qtr_adspend_per_merchant
from 
   np_qtr_perf_lvl
union all
select 
   cast('weekly' as varchar(64)) as category,
   report_week date_category,
   count(distinct case when adspend > 0 then merchant_id end) week_merchant_with_adspend,
   count(distinct case when merch_roas_gone = 1 then merchant_id end) week_merchant_with_roas_gone,
   sum(adspend)/count(distinct case when adspend > 0 then merchant_id end) week_adspend_per_merchant
from 
   np_daily_perf_lvl
   where report_week >= trunc(current_date, 'iw') - 22
   group by 1,2
union all
select 
    '4 week retention' category,
    x.next_report_week,
    count(distinct y.merchant_id) retained_supp_adspend, 
	count(x.merchant_id) total_supp_adspend,
	cast(retained_supp_adspend as float)/nullifzero(total_supp_adspend) rtn_rate
from 
   (select 
		trunc(cast(generated_datetime as date), 'iw')+6 report_week,
        trunc(cast(generated_datetime as date), 'iw') + 34 next_report_week,
        trunc(cast(generated_datetime as date), 'iw') - 29 last_report_week, 
		merchant_id,
		lst_avail_offer_sent_on,
		l2,
    	sum(total_spend_amount) adspend
	from user_gp.ads_rcncld_intrmdt_rpt as a 
	   left join sandbox.pai_merchants as b on a.merchant_id = b.merchant_uuid
	   left join (select merchant_uuid, 
                     max(offer_sent_on) lst_avail_offer_sent_on, 
                     max(offer_availed_on) offer_availed_on
                    from sandbox.np_merch_free_credits 
                    where offer_availed_on is not null
                    group by 1) as xyz on a.merchant_id = xyz.merchant_uuid
		where a.is_self_serve = 1
		group by 1,2,3,4,5,6
		having adspend > 0) as x
left join 
	(select 
		trunc(cast(generated_datetime as date), 'iw')+6 report_week,
        trunc(cast(generated_datetime as date), 'iw') + 34 next_report_week,
        trunc(cast(generated_datetime as date), 'iw') - 29 last_report_week, 
		merchant_id,
    	sum(total_spend_amount) adspend
	from user_gp.ads_rcncld_intrmdt_rpt as a 
		where a.is_self_serve = 1
		group by 1,2,3,4
		having adspend > 0) as y on x.merchant_id = y.merchant_id and x.next_report_week = y.report_week
join 
         (select 
		---trunc(cast(generated_datetime as date), 'iw')+6 report_week,
		  merchant_id,
    	  sum(price_with_discount) orders_rev, 
    	  sum(total_spend_amount) adspend, 
    	  orders_rev/nullifzero(adspend) roas, 
    	  case when roas > 1 then 1 else 0 end merch_roas_gone
	    from user_gp.ads_rcncld_intrmdt_rpt as a 
	  	 where a.is_self_serve = 1 and cast(generated_datetime as date) >= cast('2022-07-01' as date )
		  group by 1
         ) as rgone on x.merchant_id = rgone.merchant_id and rgone.merch_roas_gone = 1
    where x.next_report_week >= trunc(current_date, 'iw') - 36 and x.next_report_week <= trunc(current_date, 'iw') - 1
    group by 1,2
    order by 2;



create table sandbox.np_sl_agg_one_wbr as (
select 
*
from 
(select 
    cast(a.offer_sent_on as varchar(60)) dashboard_category,
    a.week_dates, 
    b.campaign_created, 
    b.cat_campaign_id, 
    b.search_campaign_id, 
    b.merch_created_camp, 
    b.activation_merch,
    b.dealrecommended_campaign,
    b.deal_created,
    b.dealrecommended_deal,
    b.merchant_advisor_deal,
    c.pause_camp_id, 
    c.archive_camp_id, 
    d.top_up_amount, 
    d.reup_count, 
    d.merchant_with_topup, 
    e.campaigns_with_adspend, 
    e.campaigns_with_conversion,
    e.campw_roas_gone, 
    e.total_imps, 
    e.total_clicks, 
    e.total_ords,
    e.total_units, 
    e.orders_rev, 
    e.adspend, 
    e.clc_spend,
    f.merch_with_adspend, 
    f.merch_with_conversion,
    f.merch_roas_gone,
    g.product_with_adspend, 
    g.product_with_conversion,
    g.product_roas_gone,
    h.merchants_live_wow,
    h.campaigns_live_wow
from
	(select 
         x.*, 
         y.*
         from 
         (select cast(week_end as date) week_dates
		    from user_groupondw.dim_week
		    where week_dates <= current_date and week_dates >= cast('2020-06-01' as date)
		  ) as x
          left join (
          select cast('original' as varchar(40)) offer_sent_on from user_groupondw.dim_week where year_key = 2020 and week_key = 2020000034
          union all 
          select cast(offer_sent_on as varchar(30)) offer_sent_on from sandbox.np_merch_free_credits where offer_sent_on is not null group by 1 
          )  as y on 'apple' = 'apple'
         ) as a 
left join 
    (select create_week,
            case when lst_avail_offer_sent_on is null then 'original' else cast(lst_avail_offer_sent_on as varchar(40)) end lst_avail_offer_sent_on,
    		count(citrusad_campaign_id) campaign_created, 
    		count(distinct product) deal_created,
    		count(case when dealrecommended = 1 then citrusad_campaign_id end) dealrecommended_campaign,
    		count(case when dealrecommended = 1 then product end) dealrecommended_deal,
    		count(case when merchant_advisor = 1 then product end) merchant_advisor_deal,
    		---count(distinct citrusad_campaign_id) dist_campaign_created,
    		count(case when campaign_subtype = 'CATEGORY_ONLY' then citrusad_campaign_id end) cat_campaign_id, 
    		count(case when campaign_subtype = 'SEARCH_ONLY' then citrusad_campaign_id end) search_campaign_id, 
    		count(distinct merchant_uuid) merch_created_camp, 
    		count(distinct case when mrch_crt_first_camp = 1 then merchant_uuid end ) activation_merch
    	from
    		sandbox.np_sl_cmp_onb
    		group by 1,2)  as b on a.week_dates = b.create_week and a.offer_sent_on = b.lst_avail_offer_sent_on
left join 
    (select update_week, 
            case when lst_avail_offer_sent_on is null then 'original' else cast(lst_avail_offer_sent_on as varchar(40)) end  lst_avail_offer_sent_on,
    		count(case when status = 'PAUSED' then citrusad_campaign_id end) pause_camp_id, 
    		count(case when status = 'ARCHIVED' then citrusad_campaign_id end) archive_camp_id
    	from
    		sandbox.np_sl_cmp_onb 
    		where lst_avail_offer_sent_on is not null
    		group by 1,2)  as c on a.week_dates = c.update_week and a.offer_sent_on = c.lst_avail_offer_sent_on
left join 
	(select 
	       trunc(cast(substr(create_datetime, 1,10) as date), 'iw') + 6 create_week,
	       case when lst_avail_offer_sent_on is null then 'original' else cast(lst_avail_offer_sent_on as varchar(40)) end lst_avail_offer_sent_on,
	       sum(amount) top_up_amount, 
	       count(amount) as reup_count, 
	       count(distinct merchant_id) merchant_with_topup
	from sandbox.np_merchant_topup_orders as a
	left join (select merchant_uuid, 
                     max(offer_sent_on) lst_avail_offer_sent_on, 
                     max(offer_availed_on) offer_availed_on
                    from sandbox.np_merch_free_credits 
                    where offer_availed_on is not null
                    group by 1) as xyz on a.merchant_id = xyz.merchant_uuid
	where event_type='CAPTURE' and event_status='SUCCESS'
	group by 1,2) as d on a.week_dates = d.create_week and a.offer_sent_on = d.lst_avail_offer_sent_on
left join
 (select 
      report_week,
      lst_avail_offer_sent_on,
      sum(campw_roas_gone) campw_roas_gone, 
      count(DISTINCT case when adspend > 0 then campaign_id end) campaigns_with_adspend, 
      count(distinct case when total_ords > 0 then campaign_id end) campaigns_with_conversion, 
      sum(total_imps) total_imps, 
      sum(total_clicks) total_clicks, 
      sum(total_ords) total_ords,
      sum(total_units) total_units, 
      sum(orders_rev) orders_rev, 
      sum(adspend) adspend, 
      sum(clc_spend) clc_spend
   from 
	(select 
		trunc(cast(generated_datetime as date), 'iw')+6 report_week,
		case when lst_avail_offer_sent_on is null then 'original' else cast(lst_avail_offer_sent_on as varchar(40)) end lst_avail_offer_sent_on,
		campaign_id,
		sum(impressioned) total_imps, 
    	sum(clicked) total_clicks, 
    	sum(converted) total_ords,
    	sum(order_quantity) total_units, 
    	sum(price_with_discount) orders_rev, 
    	sum(total_spend_amount) adspend, 
    	sum(click_spend_amount) clc_spend, 
    	orders_rev/nullifzero(adspend) roas, 
    	case when roas > 1 then 1 else 0 end campw_roas_gone
	from user_gp.ads_rcncld_intrmdt_rpt as a 
	     left join (select merchant_uuid, 
                     max(offer_sent_on) lst_avail_offer_sent_on, 
                     max(offer_availed_on) offer_availed_on
                    from sandbox.np_merch_free_credits 
                    where offer_availed_on is not null
                    group by 1) as xyz on a.merchant_id = xyz.merchant_uuid
		left join sandbox.pai_deals as b on a.deal_id = b.deal_uuid
		left join sandbox.np_citrusad_campaigns as c on a.campaign_id = c.citrusad_campaign_id
		where a.is_self_serve = 1
		group by 1,2,3) as fin
	group by 1,2
	) as e on a.week_dates = e.report_week and a.offer_sent_on = e.lst_avail_offer_sent_on
left join
 ( select 
      report_week,
      lst_avail_offer_sent_on,
      sum(merch_roas_gone) merch_roas_gone, 
      count(DISTINCT case when adspend > 0 then merchant_id end) merch_with_adspend,
      count(DISTINCT case when total_ords > 0 then merchant_id end) merch_with_conversion
   from 
	(select 
		trunc(cast(generated_datetime as date), 'iw')+6 report_week,
		case when lst_avail_offer_sent_on is null then 'original' else cast(lst_avail_offer_sent_on as varchar(40)) end lst_avail_offer_sent_on,
		merchant_id,
		sum(impressioned) total_imps, 
    	sum(clicked) total_clicks, 
    	sum(converted) total_ords,
    	sum(order_quantity) total_units, 
    	sum(price_with_discount) orders_rev, 
    	sum(total_spend_amount) adspend, 
    	sum(click_spend_amount) clc_spend, 
    	orders_rev/nullifzero(adspend) roas, 
    	case when roas > 1 then 1 else 0 end merch_roas_gone
	from user_gp.ads_rcncld_intrmdt_rpt as a 
	     left join (select merchant_uuid, 
                     max(offer_sent_on) lst_avail_offer_sent_on, 
                     max(offer_availed_on) offer_availed_on
                    from sandbox.np_merch_free_credits 
                    where offer_availed_on is not null
                    group by 1) as xyz on a.merchant_id = xyz.merchant_uuid
		left join sandbox.pai_deals as b on a.deal_id = b.deal_uuid
		left join sandbox.np_citrusad_campaigns as c on a.campaign_id = c.citrusad_campaign_id
		where a.is_self_serve = 1
		group by 1,2,3) as fin
	group by 1,2
	) as f on a.week_dates = f.report_week and a.offer_sent_on = f.lst_avail_offer_sent_on
left join
 (select 
      report_week,
      lst_avail_offer_sent_on,
      sum(product_roas_gone) product_roas_gone, 
      count(DISTINCT case when adspend > 0 then product end) product_with_adspend, 
      count(distinct case when total_ords > 0 then product end) product_with_conversion, 
      sum(total_imps) total_imps, 
      sum(total_clicks) total_clicks, 
      sum(total_ords) total_ords,
      sum(total_units) total_units, 
      sum(orders_rev) orders_rev, 
      sum(adspend) adspend, 
      sum(clc_spend) clc_spend
   from 
	(select 
		trunc(cast(generated_datetime as date), 'iw')+6 report_week,
		case when lst_avail_offer_sent_on is null then 'original' else cast(lst_avail_offer_sent_on as varchar(40)) end lst_avail_offer_sent_on,
		product,
		sum(impressioned) total_imps, 
    	sum(clicked) total_clicks, 
    	sum(converted) total_ords,
    	sum(order_quantity) total_units, 
    	sum(price_with_discount) orders_rev, 
    	sum(total_spend_amount) adspend, 
    	sum(click_spend_amount) clc_spend, 
    	orders_rev/nullifzero(adspend) roas, 
    	case when roas > 1 then 1 else 0 end product_roas_gone
	from user_gp.ads_rcncld_intrmdt_rpt as a 
	     left join (select merchant_uuid, 
                     max(offer_sent_on) lst_avail_offer_sent_on, 
                     max(offer_availed_on) offer_availed_on
                    from sandbox.np_merch_free_credits 
                    where offer_availed_on is not null
                    group by 1) as xyz on a.merchant_id = xyz.merchant_uuid
		left join sandbox.pai_deals as b on a.deal_id = b.deal_uuid
		left join sandbox.np_citrusad_campaigns as c on a.campaign_id = c.citrusad_campaign_id
		where a.is_self_serve = 1
		group by 1,2,3) as fin
	group by 1,2
	) as g on a.week_dates = g.report_week and a.offer_sent_on = g.lst_avail_offer_sent_on
	left join sandbox.np_sl_act_camp as h on a.week_dates = h.week_dates and a.offer_sent_on = 'original'
) as fin2  
where 
  (campaign_created is not null 
  or cat_campaign_id is not null 
  or search_campaign_id is not null 
  or merch_created_camp is not null 
  or activation_merch is not null
  or dealrecommended_campaign is not null 
  or pause_camp_id is not null 
  or archive_camp_id is not null 
  or top_up_amount is not null 
  or reup_count is not null 
  or merchant_with_topup is not null 
  or campaigns_with_adspend is not null 
  or campaigns_with_conversion is not null 
  or campw_roas_gone is not null 
  or total_imps is not null 
  or total_clicks is not null 
  or total_ords is not null 
  or total_units is not null 
  or orders_rev is not null 
  or adspend is not null 
  or clc_spend is not null 
  or merch_with_adspend is not null 
  or merch_with_conversion is not null 
  or merch_roas_gone is not null
  or merchants_live_wow is not null
  or deal_created is not null
  or dealrecommended_deal is not null
  or product_with_adspend is not null 
  or product_with_conversion is not null
  or product_roas_gone is not null
  )
  ) with data;
 


select * from sandbox.np_sl_agg_one_wbr;

------------------------------------------------------------------------FEEDBACK

drop table sandbox.np_ss_feedback2;
create multiset table sandbox.np_ss_feedback2 as (
SELECT 
   a.*, 
   trunc(cast(substr(create_datetime,1,10) as date), 'iw')+6 week_date
FROM sandbox.np_ss_feedback as a
) with data;

select a.*, b.account_id, c.Merchant_Permalink
from sandbox.np_ss_feedback2 as a 
left join sandbox.pai_merchants as b on a.merchant_id = b.merchant_uuid
left join dwh_base_sec_view.sf_account as c on b.account_id = c.Id

select * from user_edwprod.dim_offer_ext;
select * from user_edwprod.dim_gbl_deal_lob;

-------------------------------------------------------------------------------------------SS SL INTERACTION
insert overwrite table grp_gdoop_bizops_db.np_ss_sl_user_granular
select 
    consumerid, 
    merchantid, 
    consumeridsource,
    rawpagetype,
    ROW_NUMBER () over (partition by merchantid, consumerid, consumeridsource, eventdate order by eventtime asc) row_num_rank,
    eventdate
from 
(select
    consumerid, 
    merchantid, 
    consumeridsource,
    rawpagetype,
    min(eventtime) eventtime,
    eventdate
from
    grp_gdoop_pde.junoHourly
where
    eventdate >= '2021-07-15'
    and clientplatform in ('web','Touch')
    and eventDestination = 'other'
    and event = 'merchantPageView'
    and country = 'US'
    and pageapp = 'sponsored-campaign-itier'
    and merchantid is not null
group by consumerid, merchantid, consumeridsource, rawpagetype, eventdate) as fin
;


insert overwrite table grp_gdoop_bizops_db.np_sssl_tab_login2
select 
    date_sub(next_day(a.eventdate, 'MON'), 1) eventweek, 
    case when a.row_num_rank = b.max_interaction then 1 else 0 end max_interaction,
    case when d.lst_avail_offer_sent_on is null then 'non free credit' else cast(d.lst_avail_offer_sent_on as string) end lst_avail_offer_sent_on,
    c.l2,
    a.rawpagetype, 
    a.row_num_rank,
    count(1) distinct_logins
from grp_gdoop_bizops_db.np_ss_sl_user_granular as a 
left join 
    (select 
         eventdate,
         merchantid, 
         consumerid,
         max(row_num_rank) max_interaction
    from grp_gdoop_bizops_db.np_ss_sl_user_granular
    group by 
         eventdate, 
         merchantid,
         consumerid
     ) as b on a.eventdate = b.eventdate and a.merchantid = b.merchantid and coalesce(a.consumerid,'apple') = COALESCE(b.consumerid, 'apple')
left join grp_gdoop_bizops_db.pai_merchants as c on a.merchantid = c.merchant_uuid
left join 
     (select merchant_uuid, 
             max(cast(offer_sent_on as date)) lst_avail_offer_sent_on, 
             max(offer_availed_on) offer_availed_on
      from grp_gdoop_bizops_db.np_merch_free_credits 
      where offer_availed_on is not null
      group by merchant_uuid) as d on a.merchantid = d.merchant_uuid 
group by 
     date_sub(next_day(a.eventdate, 'MON'), 1), 
     case when a.row_num_rank = b.max_interaction then 1 else 0 end,
     d.lst_avail_offer_sent_on,
     c.l2,
     a.rawpagetype, 
     a.row_num_rank;
    



    
------------------------------------------------------------------------------------------------------------------------------


select 
   a.*, 
   b.first_date_landing_sl, 
   b.drop_off_page, 
   case when c.min_start_date is not null then 1 else 0 end merchant_has_a_campaign,
   c.min_start_date,
   case when d.create_week is not null then 1 else 0 end merchant_has_a_campaign2,
   d.create_date, 
   offer_availed_on -offer_sent_on  days_bet_offer_availed
from 
(select * from sandbox.np_merch_free_credits where offer_sent_on >= cast('2022-03-23' as date)) as a 
left join 
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
from sandbox.np_ss_sl_interaction_agg 
group by 1) as b on a.merchant_uuid = b.merchantid
left join 
   (select merchant_id, 
           min(cast(substr(create_datetime, 1,10) as date)) min_start_date,
           count(distinct campaign_name) total_campaigns_created
           from sandbox.np_sponsored_campaign 
           where status not in ('DRAFT')
           group by 1) as c on a.merchant_uuid = c.merchant_id
left join 
    (select pd.merchant_uuid, 
             trunc(min(cast(substr(create_datetime,1,10) as date)), 'iw')+6 create_week,
             min(cast(substr(create_datetime,1,10) as date)) create_date
               from sandbox.np_citrusad_campaigns as cit
               left join sandbox.pai_deals as pd on cit.product = pd.deal_uuid
               group by 1) as d on a.merchant_uuid = d.merchant_uuid
               
;

-----------------------------------------------------------------------PAULS REVIEW

select
	td_week_begin(cast(report_date as date),'iso')  as week_Start,
	snap.supplier_name,
	supplier_first_spend,
	First_Week_cohort,
	supplier_last_spend,
	last_Week_cohort,
	all_time_ad_spend,	
	all_time_sales_rev,
	case
	when wallet_balance_rem is null then 0
	else wallet_balance_rem
	end as wallet_balance_rem0,
	sum(impressions) as wk_imps,	
	sum(clicks) as wk_clicks,	
	sum(conversions) as wk_conversions,	
	sum(unitsales) as wk_unit_sales,	
	sum(sales_revenue) as wk_sales_rev,	
	sum(adspend ) as wk_ad_spend,	
	sum(adspend ) /  nullifzero(sum(clicks)) as wk_CPC,
	count(distinct (report_date)) as wk_active_days
	from sandbox.te_sl_ad_snapshot as snap
	left join (select
	supplier_name,
	sum(adspend ) as all_time_ad_spend,	
	sum(sales_revenue) as all_time_sales_rev,
	min(report_date) as supplier_first_spend,
	td_week_begin(cast(min(report_date) as date),'iso')  as First_Week_cohort,
	max(report_date) as supplier_last_spend,
	td_week_begin(cast(max(report_date) as date),'iso')  as last_Week_cohort
	from sandbox.te_sl_ad_snapshot 
	where SUBSTRING(supplier_name from length(supplier_name) -2 for 3) = ' ss' 
	group by 1) cohort
	on snap.supplier_name = cohort.supplier_name
	left join (select
	supplier_name,
	sum(available_balance) as wallet_balance_rem
	from
	sandbox.te_sl_wallet_balances
	where archived = 'FALSE'
	and SUBSTRING(supplier_name from length(supplier_name) -2 for 3) = ' ss' 
	group by 1) wallet
	on snap.supplier_name = wallet.supplier_name
	where SUBSTRING(snap.supplier_name from length(snap.supplier_name) -2 for 3) = ' ss' 
	having wk_ad_spend > 0
	group by 1,2,3,4,5,6,7,8,9
	order by 1 desc;




select
    snap.supplier_name,
    supplier_first_spend,
    First_Week_cohort,
    supplier_last_spend,
    last_Week_cohort,
    active_days,
    case
       when wallet_balance_rem is null then 0
       else wallet_balance_rem
       end as wallet_balance_rem,
    case when supplier_last_spend - supplier_first_spend > 30 then 1 else 0 end has_spend_after_30,
    case when supplier_last_spend - supplier_first_spend > 60 then 1 else 0 end has_spend_after_60,
    case when supplier_last_spend - supplier_first_spend > 90 then 1 else 0 end has_spend_after_90,
    case when active_days > 30 then 1 else 0 end active_days_exceeds_30,
    case when active_days > 60 then 1 else 0 end  active_days_exceeds_60,
    case when active_days > 90 then 1 else 0 end  active_days_exceeds_90,
    sum(impressions) as all_imps,   
    sum(clicks) as all_clicks,  
    sum(conversions) as all_conversions,    
    sum(unitsales) as all_unit_sales,   
    sum(sales_revenue) as all_sales_rev,    
    sum(adspend ) as all_ad_spend,  
    sum(adspend ) /  nullifzero(sum(clicks)) as all_CPC,
    merchant_uuid,
    supplier_last_spend - supplier_first_spend spendays
    from sandbox.te_sl_ad_snapshot as snap
    left join
       (select
           supplier_name,
           min(cast(report_date as date)) as supplier_first_spend,
           td_week_end(cast(min(report_date) as date),'iso')  as First_Week_cohort,
           max(cast(report_date as date)) as supplier_last_spend,
           td_week_end(cast(max(report_date) as date),'iso') as last_Week_cohort,
           count(distinct (report_date)) as active_days
        from sandbox.te_sl_ad_snapshot 
        where SUBSTRING(supplier_name from length(supplier_name) -2 for 3) = ' ss' and adspend > 0
        group by 1) cohort on snap.supplier_name = cohort.supplier_name
    left join (select
           supplier_name,
           sum(available_balance) as wallet_balance_rem
         from
           sandbox.te_sl_wallet_balances
         where archived = 'FALSE'
         and SUBSTRING(supplier_name from length(supplier_name) -2 for 3) = ' ss' 
         group by 1) wallet
         on snap.supplier_name = wallet.supplier_name
    left join 
         (select supplier_name, max(sku) as select_deal_id, max(merchant_uuid) merchant_uuid
           from sandbox.te_sl_ad_snapshot as a 
           left join sandbox.pai_deals as b on a.sku = b.deal_uuid
           where SUBSTRING(supplier_name from length(supplier_name) -2 for 3) = ' ss'
           group by 1) as merch on snap.supplier_name = merch.supplier_name
    where SUBSTRING(snap.supplier_name from length(snap.supplier_name) -2 for 3) = ' ss' 
    having all_ad_spend > 0
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,21
    order by 1 asc


-----merchant had a spend and still has campaign after 30 days and spend after 30 days

create volatile table sh_bt_launch_dates as (
sel deal_uuid,
        max(has_gcal) has_gcal,
        min(load_date) launch_date
    from sandbox.sh_bt_active_deals_log_v4 ad
    where product_is_active_flag = 1
    and partner_inactive_flag = 0
    and load_date >= '2019-04-01'
    group by 1
) with data unique primary index (deal_uuid) on commit preserve rows;

create volatile table sh_bt_retention_30 as (
    sel ad.deal_uuid,
        max(case when ad.load_date >= cast(ld.launch_date as date) + interval '31' day then 1 else 0 end) live_after_30_days
    from sandbox.sh_bt_active_deals_log ad
    join sh_bt_launch_dates ld on ad.deal_uuid = ld.deal_uuid
    where product_is_active_flag = 1
    and partner_inactive_flag = 0
    group by 1
) with data unique primary index (deal_uuid) on commit preserve rows;



create volatile table sh_bt_still_live_30 as (
    sel ad.deal_uuid
    from user_groupondw.active_deals ad
    join sh_bt_launch_dates ld on ad.deal_uuid = ld.deal_uuid
    where ad.load_date >= cast(ld.launch_date as date) + interval '31' day
    and ad.sold_out = 'false'
    group by 1
) with data unique primary index (deal_uuid) on commit preserve rows;

create volatile table sh_booking_solution as (
    select o2.deal_uuid,
        max(sfa.id) sf_account_id,
        max(case when lower(sfa.scheduler_setup_type) in ('pen & paper','none') then 'pen & paper'
            when sfa.scheduler_setup_type is null then 'no data'
            else 'some booking tool'
            end) current_booking_solution,
        max(sfa.scheduler_setup_type) detailed_booking_solution,
        max(sfa.name) account_name,
        max(company_type) company_type
    from dwh_base_sec_view.sf_opportunity_1 o1
    join dwh_base_sec_view.sf_opportunity_2 o2 on o1.id = o2.id
    join dwh_base_sec_view.sf_account sfa on o1.accountid = sfa.id
    group by o2.deal_uuid
) with data unique primary index (deal_uuid) on commit preserve rows;

create volatile multiset table np_five_plus_ret as (
select 
    a.deal_uuid, 
    sum(case when b.order_date >= a.launch_date and b.order_date <= a.launch_date + 30 then transaction_qty end) units_sold, 
    count(distinct case when b.order_date >= a.launch_date and b.order_date <= a.launch_date + 30 then order_id end ) orders_places
from sh_bt_launch_dates as a
    left join user_edwprod.fact_gbl_transactions as b on a.deal_uuid = b.deal_uuid and b.action = 'authorize'
    group by 1
) with data on commit preserve rows;

drop table sandbox.sh_bt_retention_view;
create table sandbox.sh_bt_retention_view as (
    sel cast(dm.month_start as date) report_mth,
        case when gdl.country_code in ('US','CA') then 'NAM' else 'INTL' end region,
        gdl.grt_l2_cat_description,
        case when lower(sbs.current_booking_solution) = 'pen & paper' then 'p&p' else 'other' end booking_type,
        ld.has_gcal,
        count(distinct ld.deal_uuid) n_deals_launched_live30,
        count(distinct case when live_after_30_days = 1 then ld.deal_uuid end) n_deals_retained,
        cast(n_deals_retained as dec(18,3)) / cast(n_deals_launched_live30 as dec(18,3)) pct_retained, 
        count(distinct case when units.units_sold >= 5 then ld.deal_uuid end) five_units_deals_live_30,
        count(distinct case when units.units_sold >= 5  and live_after_30_days = 1 then ld.deal_uuid end)five_units_deals_retained,
        cast(five_units_deals_retained as dec(18,3)) / NULLIFZERO(cast(five_units_deals_live_30 as dec(18,3))) five_pct_retained
    from sh_bt_launch_dates ld
    join user_groupondw.dim_day dd on ld.launch_date = dd.day_rw
    join user_groupondw.dim_month dm on dd.month_key = dm.month_key
    join user_edwprod.dim_gbl_deal_lob gdl on ld.deal_uuid = gdl.deal_id
    join sh_bt_still_live_30 l30 on ld.deal_uuid = l30.deal_uuid
    join sh_booking_solution sbs on ld.deal_uuid = sbs.deal_uuid
    left join sh_bt_retention_30 r30 on ld.deal_uuid = r30.deal_uuid
    left join np_five_plus_ret units on ld.deal_uuid = units.deal_uuid
    where gdl.grt_l2_cat_description in ('F&D','HBW','TTD - Leisure')
    and not (gdl.grt_l2_cat_description = 'F&D' and gdl.country_code in ('US','CA'))
    and launch_date >= '2019-04-01'
    group by 1,2,3,4,5
) with data no primary index;

grant sel on sandbox.sh_bt_retention_view to public;


select * from user_gp.ads_rcncld_intrmdt_rpt;
select * from sb_merchant_experience.recommendations;
sel * from sb_merchant_experience.recommendations_tracker;

sel report_date, td_monday(cast(report_date as date))+ interval '6' DAY as week_end from sandbox.te_sl_ad_snapshot;

select
		cast('we_ly' as varchar(8)) as date_cut,
		to_date(dw.week_end) as report_date,
		row_number() over(order by week_end desc) as row_
	from user_groupondw.dim_week dw
	where
		to_date(dw.week_end) between '2019-01-01' and date_sub(current_date,365)


create volatile table np_ss_merch_perf as 
(SELECT 
      COALESCE(a.merchant_id, b.merchant_uuid) merchant_uuid,
      sum(impressions) total_imps, 
      sum(clicks) total_clicks, 
      sum(conversions) total_ords, 
      sum(total_spend_amount) adspend,
      sum(price_with_discount) orders_rev, 
      cast(total_clicks as float)/NULLIFZERO(total_imps) ss_ctr, 
      cast(total_ords as float)/NULLIFZERO(total_clicks) ss_cnv, 
      cast(orders_rev as float)/NULLIFZERO(adspend) roas
FROM sandbox.np_ss_performance_met as a 
     left join sandbox.pai_deals as b on a.deal_id = b.deal_uuid
group by 1
) with data on commit preserve rows;



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






select 
   a.supplier_name, 
   a.campaign_id, 
   b.team_cohort
from sandbox.np_sl_ad_snapshot as a 
join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name
where b.team_cohort <> 'SelfServe' and campaign_id = '859a0136-404a-444c-8aa3-b7ff2f7f8a34'
