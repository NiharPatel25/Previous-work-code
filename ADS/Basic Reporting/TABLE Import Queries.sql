------------Self Serve

sandbox.np_sponsored_campaign
sandbox.np_sponsored_merchants
sandbox.np_merchant_topup_orders
sandbox.np_refund_request_orders
sandbox.np_ads_reconciled_intermediate_report
sandbox.np_citrusad_campaigns
sandbox.np_ss_sl_interaction
sandbox.np_ss_sl_interaction_agg
sandbox.citrusad_team_wallet;

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
      type_based_category VARCHAR(200) CHARACTER SET UNICODE,
      l1 VARCHAR(20) CHARACTER SET UNICODE,
      l2 VARCHAR(20) CHARACTER SET UNICODE,
      l3 VARCHAR(20) CHARACTER SET UNICODE
      )
NO PRIMARY INDEX;


drop table sandbox.np_sc_deal_search_terms;
CREATE MULTISET TABLE sandbox.np_sc_deal_search_terms ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      deal_id VARCHAR(225) CHARACTER SET UNICODE,
      search_terms VARCHAR(5000) CHARACTER SET UNICODE, 
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






drop table sandbox.np_ss_feedback;
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
      reasons VARCHAR(5000) CHARACTER SET UNICODE,
      create_datetime	VARCHAR(50) CHARACTER SET UNICODE
      )
NO PRIMARY INDEX;
------


drop table sandbox.np_sponsored_campaign;
CREATE MULTISET TABLE sandbox.np_sponsored_campaign ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      id INTEGER,
      merchant_id VARCHAR(128) CHARACTER SET UNICODE,
      merchant_name VARCHAR(300) CHARACTER SET UNICODE,
      deal_id VARCHAR(128) CHARACTER SET UNICODE,
      campaign_name VARCHAR(500) CHARACTER SET UNICODE,
      start_datetime VARCHAR(40) CHARACTER SET UNICODE,
      end_datetime VARCHAR(40) CHARACTER SET UNICODE,
      budget_type VARCHAR(40) CHARACTER SET UNICODE, 
      budget_value float,
      budget_spent float, 
      contract_signed_by VARCHAR(200) CHARACTER SET UNICODE,
      contract_signed_datetime VARCHAR(40) CHARACTER SET UNICODE,
      status  VARCHAR(50) CHARACTER SET UNICODE,
      last_updated_by VARCHAR(200) CHARACTER SET UNICODE,
      create_datetime VARCHAR(40) CHARACTER SET UNICODE,
      update_datetime VARCHAR(40) CHARACTER SET UNICODE,
      status_change_reason VARCHAR(600) CHARACTER SET UNICODE
      )
NO PRIMARY INDEX;


drop table sandbox.np_sponsored_campaign;
CREATE MULTISET TABLE sandbox.np_sponsored_campaign ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      id integer,
      merchant_id VARCHAR(50) CHARACTER SET UNICODE,
      merchant_name VARCHAR(150) CHARACTER SET UNICODE,
      deal_id VARCHAR(50) CHARACTER SET UNICODE,
      campaign_name VARCHAR(250) CHARACTER SET UNICODE,
      start_datetime VARCHAR(20) CHARACTER SET UNICODE,
      end_datetime VARCHAR(20) CHARACTER SET UNICODE,
      budget_type VARCHAR(20) CHARACTER SET UNICODE, 
      budget_value float,
      budget_spent float, 
      contract_signed_by VARCHAR(100) CHARACTER SET UNICODE,
      contract_signed_datetime VARCHAR(20) CHARACTER SET UNICODE,
      status  VARCHAR(50) CHARACTER SET UNICODE,
      last_updated_by VARCHAR(100) CHARACTER SET UNICODE,
      create_datetime VARCHAR(20) CHARACTER SET UNICODE,
      update_datetime VARCHAR(20) CHARACTER SET UNICODE,
      status_change_reason VARCHAR(300) CHARACTER SET UNICODE, 
      creation_source varchar(50) CHARACTER SET UNICODE
      )
NO PRIMARY INDEX;


/*
 * SELECT 
CAST(id as integer) as id,
CAST(merchant_id as char(50)) merchant_id, 
CAST(merchant_name as char(150)) merchant_name, 
CAST(deal_id as char(50)) deal_id, 
CAST(campaign_name as char(250)) campaign_name, 
CAST(start_datetime as char(20)) start_datetime, 
CAST(end_datetime as char(20)) end_datetime, 
CAST(budget_type as char(20)) budget_type, 
CAST(budget_value as decimal(18,2)) budget_value, 
CAST(budget_spent as decimal(18,2)) budget_spent, 
CAST(contract_signed_by as char(100)) contract_signed_by, 
CAST(contract_signed_datetime as char(20)) contract_signed_datetime, 
CAST("status" as char(25)) status, 
CAST(last_updated_by as char(100)) last_updated_by, 
CAST(create_datetime as char(20)) create_datetime, 
CAST(update_datetime as char(20)) update_datetime, 
CAST(status_change_reason as char(300)) status_change_reason
from 
ad_inv_serv_prod.sponsored_campaigns;

SELECT 
id,
merchant_id, 
merchant_name, 
deal_id, 
campaign_name, 
start_datetime, 
end_datetime, 
budget_type, 
budget_value, 
budget_spent, 
contract_signed_by, 
contract_signed_datetime, 
status, 
last_updated_by, 
create_datetime, 
update_datetime, 
status_change_reason
from 
ad_inv_serv_prod.sponsored_campaigns;
 */


drop table sandbox.np_sponsored_merchants;
CREATE MULTISET TABLE sandbox.np_sponsored_merchants ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      merchant_uuid VARCHAR(64) CHARACTER SET UNICODE,
      merchant_id VARCHAR(200) CHARACTER SET UNICODE,
      country VARCHAR(5) CHARACTER SET UNICODE, 
      is_active integer, 
      update_datetime VARCHAR(20) CHARACTER SET UNICODE, 
      test_account varchar(10) character set unicode
      )
NO PRIMARY INDEX;

CREATE MULTISET TABLE sandbox.np_merchant_topup_orders ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
			id	varchar(50) CHARACTER SET UNICODE,
			order_id	varchar(50) CHARACTER SET UNICODE,
			merchant_id	varchar(50) CHARACTER SET UNICODE,
			consumer_id	varchar(50) CHARACTER SET UNICODE,
			event_type	varchar(30) CHARACTER SET UNICODE,
			event_status	varchar(30) CHARACTER SET UNICODE,
			event_time	VARCHAR(20) CHARACTER SET UNICODE,
			inventory_unit_id	varchar(50) CHARACTER SET UNICODE,
			currency	varchar(50) CHARACTER SET UNICODE, 
			amount	decimal(7,2),	
			create_datetime	VARCHAR(20) CHARACTER SET UNICODE,
			update_datetime	VARCHAR(20) CHARACTER SET UNICODE,
			partial_card_number	varchar(50) CHARACTER SET UNICODE,
			card_type	varchar(50) CHARACTER SET UNICODE,	
			wallet_txn_id	varchar(50) CHARACTER SET UNICODE
) NO PRIMARY INDEX;



CREATE MULTISET TABLE sandbox.np_refund_request_orders ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO, 
     MAP = TD_MAP1
     (
		refund_id varchar(50) CHARACTER SET UNICODE,
		order_id  varchar(50) CHARACTER SET UNICODE,
		amount decimal(7,2),
		status varchar(30) CHARACTER SET UNICODE,
		create_datetime	VARCHAR(20) CHARACTER SET UNICODE,
		update_datetime	VARCHAR(20) CHARACTER SET UNICODE
) NO PRIMARY INDEX


CREATE MULTISET TABLE sandbox.np_ads_reconciled_intermediate_report ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO, 
     MAP = TD_MAP1
     (
id	integer, 
external_ad_id	varchar(190) CHARACTER SET UNICODE,
generated_datetime	VARCHAR(20) CHARACTER SET UNICODE,
merchant_id	varchar(50) CHARACTER SET UNICODE,
campaign_id	varchar(50) CHARACTER SET UNICODE,
wallet_id	varchar(50) CHARACTER SET UNICODE,
search_term	varchar(128) CHARACTER SET UNICODE,
deal_id	varchar(50) CHARACTER SET UNICODE,
impressioned integer,  
clicked	integer, 
converted integer, 
order_id	varchar(50) CHARACTER SET UNICODE,
order_quantity integer, 
price_with_discount	decimal(7,2), 
currency	varchar(20) CHARACTER SET UNICODE,
impression_spend_amount	decimal(7,4), 
click_spend_amount	decimal(7,4), 
total_spend_amount	decimal(7,4), 
topup_order_id	varchar(50) CHARACTER SET UNICODE,
topup_order_unit_id	varchar(50)	CHARACTER SET UNICODE,
excess_spend decimal(7,4),
is_self_serve integer, 
is_coupon_deal integer, 
created_at VARCHAR(20) CHARACTER SET UNICODE,
updated_at VARCHAR(20) CHARACTER SET UNICODE,
inactive_campaign_spend	decimal(7,4),
status integer, 
"version"	 integer, 
is_valid integer
) NO PRIMARY INDEX
;

drop table sandbox.np_citrusad_campaigns;
CREATE MULTISET TABLE sandbox.np_citrusad_campaigns ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO, 
     MAP = TD_MAP1
     (
correlation_id	integer,
citrusad_campaign_id varchar(50) CHARACTER SET UNICODE,
team_id	varchar(100) CHARACTER SET UNICODE,
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
citrusad_update_datetime VARCHAR(20) CHARACTER SET UNICODE,	
export_datetime	VARCHAR(20) CHARACTER SET UNICODE
) NO PRIMARY INDEX
;


drop table sandbox.np_ss_sl_interaction;
CREATE MULTISET TABLE sandbox.np_ss_sl_interaction ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO, 
     MAP = TD_MAP1
     (
      merchantid VARCHAR(100) CHARACTER SET UNICODE,
      consumeridsource VARCHAR(100) CHARACTER SET UNICODE,
      rawpagetype VARCHAR(100) CHARACTER SET UNICODE, 
      row_num_rank integer,
      eventdate VARCHAR(12) CHARACTER SET UNICODE
      )
NO PRIMARY INDEX;

drop table sandbox.np_selfserve_performance_metrics;
CREATE MULTISET TABLE sandbox.np_ss_performance_met ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO, 
     MAP = TD_MAP1
     (
      report_date VARCHAR(20) CHARACTER SET UNICODE,
      deal_id VARCHAR(50) CHARACTER SET UNICODE,
      merchant_id VARCHAR(50) CHARACTER SET UNICODE, 
      impressions integer,
      clicks integer,
      conversions integer,
      order_quantity integer, 
      price_with_discount decimal(11,4), 
      total_spend_amount decimal(9,4), 
      excess_spend decimal(9,4)
      )
NO PRIMARY INDEX;


drop table sandbox.citrusad_team_wallet;
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



drop table sandbox.citrusad_wallet_balance_history;
CREATE MULTISET TABLE sandbox.np_citrusad_wallet_balance_history ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO, 
     MAP = TD_MAP1
     (
      citrusad_import_date VARCHAR(50) CHARACTER SET UNICODE,
      wallet_id VARCHAR(50) CHARACTER SET UNICODE,
      wallet_name VARCHAR(150) CHARACTER SET UNICODE,
      team_id VARCHAR(50) CHARACTER SET UNICODE,
      team_name VARCHAR(150) CHARACTER SET UNICODE,
      available_balance decimal(11,4),
      is_archived integer, 
      create_at VARCHAR(50) CHARACTER SET UNICODE,
      update_at VARCHAR(50) CHARACTER SET UNICODE
      )
NO PRIMARY INDEX;


CREATE MULTISET TABLE sandbox.np_slad_wlt_blc_htry ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO, 
     MAP = TD_MAP1
     (
      citrusad_import_date VARCHAR(50) CHARACTER SET UNICODE,
      wallet_id VARCHAR(50) CHARACTER SET UNICODE,
      wallet_name VARCHAR(150) CHARACTER SET UNICODE,
      team_id VARCHAR(50) CHARACTER SET UNICODE,
      team_name VARCHAR(150) CHARACTER SET UNICODE,
      available_balance VARCHAR(50) CHARACTER SET UNICODE,
      is_archived integer, 
      create_at VARCHAR(50) CHARACTER SET UNICODE,
      update_at VARCHAR(50) CHARACTER SET UNICODE
      )
NO PRIMARY INDEX;



CREATE MULTISET TABLE sandbox.np_citrusad_campaign_logs ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO, 
     MAP = TD_MAP1
     (
      id integer,
      citrusad_campaign_id VARCHAR(50) CHARACTER SET UNICODE,
      team_id VARCHAR(100) CHARACTER SET UNICODE,
      campaign_type VARCHAR(50) CHARACTER SET UNICODE,
      campaign_subtype VARCHAR(50) CHARACTER SET UNICODE,
      campaign_name VARCHAR(250) CHARACTER SET UNICODE,
      wallet VARCHAR(50) CHARACTER SET UNICODE,
      catalog_ VARCHAR(50) CHARACTER SET UNICODE,
      product VARCHAR(500) CHARACTER SET UNICODE,
      target_location VARCHAR(200) CHARACTER SET UNICODE,
      search_terms VARCHAR(5000) CHARACTER SET UNICODE,
      placement VARCHAR(500) CHARACTER SET UNICODE,
      spend_type VARCHAR(50) CHARACTER SET UNICODE,
      budget decimal(11,4),
      max_cpc decimal(11,4),
      start_date VARCHAR(50) CHARACTER SET UNICODE,
      end_date VARCHAR(50) CHARACTER SET UNICODE,
      status VARCHAR(50) CHARACTER SET UNICODE,
      budget_spent decimal(11,4),
      create_datetime VARCHAR(50) CHARACTER SET UNICODE,
      update_datetime VARCHAR(50) CHARACTER SET UNICODE,
      log_datetime VARCHAR(50) CHARACTER SET UNICODE,
      cpc_diff decimal(11,4)
      )
NO PRIMARY INDEX;



drop table sandbox.sc_ads_cost_metrics;
CREATE MULTISET TABLE sandbox.sc_ads_cost_metrics ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (type_of VARCHAR(100) CHARACTER SET UNICODE,
      values_of VARCHAR(100) CHARACTER SET UNICODE,
      cpv decimal(11,4), 
      cpc decimal(11,4), 
      cpa decimal(11,4),
      update_at VARCHAR(50) CHARACTER SET UNICODE, 
      retain_term integer
      ) NO PRIMARY INDEX;

CREATE TABLE `sponsored_campaign_ads_cost_metrics` (
  `type` varchar(100) NOT NULL,
  `value` varchar(100) NOT NULL,
  `cpv` decimal(6,3) NOT NULL,
  `cpc` decimal(6,3) NOT NULL,
  `cpa` decimal(6,3) NOT NULL,
  `update_datetime` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `retain_term` tinyint(1) NOT NULL DEFAULT '1',
  PRIMARY KEY (`type`,`value`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
     
------------------------------------------------------------------------------------FREE CREDIT IMPORT

drop table sandbox.np_merch_free_credits;
CREATE MULTISET TABLE sandbox.np_merch_free_credits ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO, 
     MAP = TD_MAP1
     (
     merchant_uuid VARCHAR(100) CHARACTER SET UNICODE,
     merchant_name VARCHAR(100) CHARACTER SET UNICODE,
     freecredit decimal(10,2),
     sf_account VARCHAR(100) CHARACTER SET UNICODE,
     "rank" integer, 
     credit_status VARCHAR(100) CHARACTER SET UNICODE,
     offer_availed_on date, 
     offer_expires_on date, 
     offer_claimed_on date, 
     offer_sent_on date, 
     offer_valid_upto date,phase varchar(25) CHARACTER SET UNICODE
      )
NO PRIMARY INDEX;



---IN Hive
DROP TABLE grp_gdoop_bizops_db.np_merch_free_credits;

create table grp_gdoop_bizops_db.np_merch_free_credits  (
      merchant_uuid string,
      merchant_name string,
      freecredit float,
      sf_account string,
      rank int,
      credit_status string, offer_availed_on string, 
      offer_expires_on string, 
      offer_claimed_on string, 
      offer_sent_on string, 
      offer_valid_upto string,
      phase string
) stored as orc
tblproperties ("orc.compress"="SNAPPY");

----------------------------------------------------------------------------------------------------------------------DEAL RECOMMENDER


drop table sandbox.np_sl_drecommender;
CREATE MULTISET TABLE sandbox.np_sl_drecommender ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO, 
     MAP = TD_MAP1
     (
      account_id VARCHAR(50) CHARACTER SET UNICODE,
      merchant_uuid VARCHAR(50) CHARACTER SET UNICODE,
      dealuuid VARCHAR(50) CHARACTER SET UNICODE,
      cvr_rankng VARCHAR(25) CHARACTER SET UNICODE,
      report_date VARCHAR(25) CHARACTER SET UNICODE
      )
NO PRIMARY INDEX;
----------------------------------------------------------------------------------------------------------HIVE IMPORT SL AGG

drop table sandbox.np_ss_sl_interaction_agg;
CREATE MULTISET TABLE sandbox.np_ss_sl_interaction_agg ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      consumerid VARCHAR(100) CHARACTER SET UNICODE,
      merchantid VARCHAR(100) CHARACTER SET UNICODE,
      consumeridsource VARCHAR(100) CHARACTER SET UNICODE,
      rawpagetype VARCHAR(100) CHARACTER SET UNICODE,
      total_entries integer,
      row_num_rank integer,
      eventdate VARCHAR(12) CHARACTER SET UNICODE
      )
NO PRIMARY INDEX;


------HIVE QUERY

DROP TABLE grp_gdoop_bizops_db.np_ss_sl_interaction;
create table grp_gdoop_bizops_db.np_ss_sl_interaction  (
      consumerid string,
      merchantid string,
      consumeridsource string, 
      rawpagetype string, 
      total_entries int
)partitioned by (eventdate string) 
stored as orc
tblproperties ("orc.compress"="SNAPPY");



insert overwrite table grp_gdoop_bizops_db.np_ss_sl_interaction partition (eventdate)
select
    consumerid, 
    merchantid, 
    consumeridsource,
    rawpagetype,
    count(1) total_entries,
    eventdate
from
    grp_gdoop_pde.junoHourly
where
    eventdate >= date_sub(current_date, 5) 
    and clientplatform in ('web','Touch')
    and eventDestination = 'other'
    and event = 'merchantPageView'
    and country = 'US'
    and pageapp = 'sponsored-campaign-itier'
group by 
    consumerid, 
    merchantid, 
    consumeridsource,
    rawpagetype,
    eventdate;


drop table grp_gdoop_bizops_db.np_ss_sl_interaction_agg;
create table grp_gdoop_bizops_db.np_ss_sl_interaction_agg stored as orc as 
select 
     consumerid, 
     merchantid, 
     consumeridsource, 
     rawpagetype, 
     total_entries,
     ROW_NUMBER () over ( partition by merchantid order by eventdate) row_num_rank,
     eventdate
from 
grp_gdoop_bizops_db.np_ss_sl_interaction;
    



select 
      trunc(cast(eventdate as date), 'iw') + 6 report_week, 
       consumeridsource, 
       rawpagetype, 
       case when b.merchant_id is not null and created_date <= cast(eventdate as date) then 1 else 0 end merchant_has_a_campaign,
       sum(total_entries) page_visits, 
       count(distinct merchantid) distinct_merchant_visits, 
       count(distinct case when merchantid is not null then concat(eventdate, merchantid) end) logins
from grp_gdoop_bizops_db.np_ss_sl_interaction_agg as a 
left join 
    (select merchant_id, min(cast(substr(create_datetime, 1,10) as date)) created_date
            from grp_gdoop_bizops_db.np_sponsored_campaign 
            where status not in ('DRAFT')
            group by 1) as b on a.merchantid = b.merchant_id
     group by 1,2,3,4
;



------------------------------------------------------------------------------------------------------SL GRANULAR

CREATE MULTISET TABLE sandbox.np_ss_sl_user_granular_tmp ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO, 
     MAP = TD_MAP1
     (
      consumerid VARCHAR(50) CHARACTER SET UNICODE,
      merchantid VARCHAR(50) CHARACTER SET UNICODE,
      consumeridsource VARCHAR(50) CHARACTER SET UNICODE, 
      rawpagetype VARCHAR(50) CHARACTER SET UNICODE, 
      row_num_rank integer, 
      eventdate VARCHAR(50) CHARACTER SET UNICODE
      )
NO PRIMARY INDEX;


     
    
-------------------------------------------------------------------------------------------------------DEAL RECOMMENDER

    
create table grp_gdoop_bizops_db.np_sl_drecommender  (
      account_id string,
      merchant_uuid string,
      deal_uuid string,
      cvr_rank string
)partitioned by (report_date string) 
stored as orc
tblproperties ("orc.compress"="SNAPPY");
     
insert overwrite table grp_gdoop_bizops_db.np_sl_drecommender partition (report_date)
select 
     account_id, 
     merchant_uuid, 
     dealuuid, 
     cvr_rank, 
     current_date report_date
from grp_gdoop_bizops_db.avb_aog_mxa_recommended_deal;


----sandbox import
CREATE MULTISET TABLE sandbox.np_sl_drecommender ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO, 
     MAP = TD_MAP1
     (
      account_id VARCHAR(25) CHARACTER SET UNICODE,
      merchant_uuid VARCHAR(25) CHARACTER SET UNICODE,
      dealuuid VARCHAR(25) CHARACTER SET UNICODE,
      cvr_rankng VARCHAR(25) CHARACTER SET UNICODE,
      report_date VARCHAR(25) CHARACTER SET UNICODE
      )
NO PRIMARY INDEX;


---------------------------------------------------------------adsnapshot import

drop table grp_gdoop_bizops_db.np_sl_ad_snapshot_temp;
create table grp_gdoop_bizops_db.np_sl_ad_snapshot (
    report_date string,
      region_namespace string,
      supplier_name string,
      supplier_id string,
      campaign_name string,
      campaign_id string,
      campaign_start_date string,
      campaign_end_date string,
      budget float,
      campaign_sub_type string,
      campaign_location_cat string,
      sku string,
      wallet_name string,
      impressions float,
      clicks float,
      adspend float,
      conversions float,
      unitsales float,
      sales_revenue float,
      campaign_status string
) 
stored as orc
tblproperties ("orc.compress"="SNAPPY");



create table grp_gdoop_bizops_db.np_sl_supp_mapping (
      supplier_name string,
      team_cohort string
) 
stored as orc
tblproperties ("orc.compress"="SNAPPY");




---------------------------------------------------------------SUpplier name import
     /*
      * 
      * 
      * 
      * 
      * 

drop table grp_gdoop_bizops_db.np_sssl_tab_page;

insert overwrite table grp_gdoop_bizops_db.np_sssl_tab_page
select 
     date_sub(next_day(eventdate, 'MON'), 1) eventweek,
     b.l2, 
     rawpagetype, 
     row_num_rank,
     count(1) merchants_logins
from grp_gdoop_bizops_db.np_ss_sl_user_granular as a 
left join grp_gdoop_bizops_db.pai_merchants as b on a.merchantid = b.merchant_uuid
group by 
     date_sub(next_day(eventdate, 'MON'), 1), 
     rawpagetype, 
     row_num_rank, 
     b.l2;


    
    
create table grp_gdoop_bizops_db.np_sssl_tab_page2 stored as orc as
select 
     date_sub(next_day(eventdate, 'MON'), 1) eventweek,
     b.l2, 
     rawpagetype, 
     row_num_rank,
     count(1) merchants_logins
from grp_gdoop_bizops_db.np_ss_sl_user_granular as a 
left join grp_gdoop_bizops_db.pai_merchants as b on a.merchantid = b.merchant_uuid
where consumerid is null
group by 
     date_sub(next_day(eventdate, 'MON'), 1), 
     rawpagetype, 
     row_num_rank, 
     b.l2;
     
     
 
 
 
 
 
DROP TABLE   grp_gdoop_bizops_db.np_sssl_tab_login;  
create table grp_gdoop_bizops_db.np_sssl_tab_login stored as orc as
select 
    date_sub(next_day(a.eventdate, 'MON'), 1) eventweek, 
    case when b.merchantid is not null then 1 else 0 end max_interaction,
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
where consumerid is not null
     group by 
     eventdate, 
     merchantid,
     consumerid) as b on a.eventdate = b.eventdate and a.merchantid = b.merchantid and a.consumerid = b.consumerid and a.row_num_rank = b.max_interaction
left join grp_gdoop_bizops_db.pai_merchants as c on a.merchantid = c.merchant_uuid
where a.consumerid is not null
group by 
    date_sub(next_day(a.eventdate, 'MON'), 1), 
    case when b.merchantid is not null then 1 else 0 end,
    c.l2,
    a.rawpagetype, 
    a.row_num_rank
UNION ALL
select 
    date_sub(next_day(a.eventdate, 'MON'), 1) eventweek, 
    case when b.merchantid is not null then 1 else 0 end max_interaction,
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
where consumerid is null
     group by 
     eventdate, 
     merchantid,
     consumerid) as b on a.eventdate = b.eventdate and a.merchantid = b.merchantid and a.row_num_rank = b.max_interaction
left join grp_gdoop_bizops_db.pai_merchants as c on a.merchantid = c.merchant_uuid
where a.consumerid is null
group by 
    date_sub(next_day(a.eventdate, 'MON'), 1), 
    case when b.merchantid is not null then 1 else 0 end,
    c.l2,
    a.rawpagetype, 
    a.row_num_rank;
     
*/
     

    
    
    


select 
     date_sub(next_day(eventdate, 'MON'), 1) eventweek,
     case when row_num_rank > 50 then 'more than 50'
          when row_num_rank > 40 then 'more than 40'
          when row_num_rank > 30 then 'more than 30'
          when row_num_rank > 20 then 'more than 20'
          when row_num_rank > 10 then 'more than 10'
          else cast(row_num_rank as varchar(64)) end interactions2,
          count(distinct merchantid) merchant_interaction 
     from grp_gdoop_bizops_db.np_ss_sl_user_granular2 
     group by 
          date_sub(next_day(eventdate, 'MON'), 1),
          case when row_num_rank > 50 then 'more than 50'
          when row_num_rank > 40 then 'more than 40'
          when row_num_rank > 30 then 'more than 30'
          when row_num_rank > 20 then 'more than 20'
          when row_num_rank > 10 then 'more than 10'
          else cast(row_num_rank as varchar(64)) end
     order by 
          date_sub(next_day(eventdate, 'MON'), 1),
          case when row_num_rank > 50 then 'more than 50'
          when row_num_rank > 40 then 'more than 40'
          when row_num_rank > 30 then 'more than 30'
          when row_num_rank > 20 then 'more than 20'
          when row_num_rank > 10 then 'more than 10'
          else cast(row_num_rank as varchar(64)) end
;


create table grp_gdoop_bizops_db.np_temp_merch_ss_sl stored as orc as 
select 
     date_sub(next_day(eventdate, 'MON'), 1) eventweek,
     eventdate,
     merchantid, 
     case when row_num_rank > 50 then 'more than 50'
          when row_num_rank > 40 then 'more than 40'
          when row_num_rank > 30 then 'more than 30'
          when row_num_rank > 20 then 'more than 20'
          when row_num_rank > 10 then 'more than 10'
          else cast(row_num_rank as varchar(64)) end row_num_max_cat
from
(select eventdate, merchantid, max(row_num_rank) row_num_rank 
from grp_gdoop_bizops_db.np_ss_sl_user_granular2 
group by eventdate, merchantid)
as fin 
;






/*
   
   
   DROP TABLE grp_gdoop_bizops_db.np_ss_sl_interaction;
create table grp_gdoop_bizops_db.np_ss_sl_interaction  (
      merchantid string,
      consumeridsource string, 
      rawpagetype string, 
      row_num_rank int
)partitioned by (eventdate string) 
stored as orc
tblproperties ("orc.compress"="SNAPPY");


insert overwrite table grp_gdoop_bizops_db.np_ss_sl_interaction partition (eventdate)
select 
     consumerid, 
     fin.merchantid, 
     consumeridsource, 
     rawpagetype, 
     ROW_NUMBER () over ( partition by merchantid order by eventdate) row_num_rank,
     eventdate
from 
(select
distinct 
    merchantid, 
    consumeridsource,
    rawpagetype,
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
    and consumeridsource is not null
    and merchantid is not null) as fin */