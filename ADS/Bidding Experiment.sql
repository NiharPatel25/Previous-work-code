
drop table sandbox.est_maxcpc_by_temp;
CREATE MULTISET TABLE sandbox.est_maxcpc_by_temp ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      citrusad_campaign VARCHAR(50) CHARACTER SET UNICODE,
      status VARCHAR(50) CHARACTER SET UNICODE,
      dealuuid VARCHAR(50) CHARACTER SET UNICODE,
      in_treatment integer,
      maxcpc_roas1 decimal(15,10),
      maxcpc_roas1_5 decimal(15,10), 
      maxcpc_roas2 decimal(15,10),
      maxcpc_roas2_5 decimal(15,10),
      maxcpc_roas3 decimal(15,10),
      maxcpc_2excld decimal(15,10),
      assigned_maxcpc decimal(15,10)
      )
NO PRIMARY INDEX;

CREATE MULTISET TABLE sandbox.np_sc_deal_search_terms ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      deal_id VARCHAR(225) CHARACTER SET UNICODE,
      search_terms VARCHAR(200) CHARACTER SET UNICODE, 
      created_datetime VARCHAR(20) CHARACTER SET UNICODE,
      last_updated_datetime VARCHAR(20) CHARACTER SET UNICODE,
      max_cpc decimal(7,2)
      )
NO PRIMARY INDEX;



CREATE MULTISET TABLE sandbox.np_citrus_camp_log ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      id integer,
      campaign_id	varchar(50) CHARACTER SET UNICODE,
      team_id VARCHAR(100) CHARACTER SET UNICODE, 
      campaign_type varchar(50) CHARACTER SET UNICODE,
      campaign_subtype varchar(50) CHARACTER SET UNICODE,
      campaign_name varchar(250) CHARACTER SET UNICODE,	
      wallet	varchar(50) CHARACTER SET UNICODE,
      "catalog" varchar(50) CHARACTER SET UNICODE,
      product	varchar(500) CHARACTER SET UNICODE,	
      target_locations varchar(100) CHARACTER SET UNICODE,
      search_terms varchar(5000) CHARACTER SET UNICODE,
      placement varchar(500) CHARACTER SET UNICODE,
      spend_type varchar(50)	CHARACTER SET UNICODE,
      budget	decimal(18,2),	
      max_cpc	decimal(18,2),
      start_date varchar(50) CHARACTER SET UNICODE,
      end_date varchar(50) CHARACTER SET UNICODE,
      status	varchar(50) CHARACTER SET UNICODE,
      budget_spent decimal(18,2),
      create_datetime	VARCHAR(20) CHARACTER SET UNICODE,
      update_datetime	VARCHAR(20) CHARACTER SET UNICODE,
      log_datetime VARCHAR(20) CHARACTER SET UNICODE,	
      cpc_diff	decimal(7,2)
      )
NO PRIMARY INDEX;



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


SELECT * FROM sandbox.np_sc_deal_search_terms;
SELECT * FROM sandbox.np_citrus_camp_log;



