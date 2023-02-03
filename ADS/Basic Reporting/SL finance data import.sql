
grant select on sandbox.np_groupon_wallet_sl to "joan.castells";
grant select on sandbox.np_sl_ad_snapshot to "joan.castells";
grant select on sandbox.np_sl_lowbudget_actcamp to "joan.castells";
grant select on sandbox.np_sl_ledger to "joan.castells";
grant select on sandbox.np_sl_summary_all_time to "joan.castells";
grant select on sandbox.np_sl_summary_mtd to "joan.castells";


select * from sandbox.np_sponsored_campaign;
select * from sandbox.np_citrusad_campaigns;

-------------------------------------------------------------------------------------------------DOMO TABLES

select * from sandbox.np_groupon_wallet_sl_temp;

drop table sandbox.np_groupon_wallet_sl_temp;
CREATE MULTISET TABLE sandbox.np_groupon_wallet_sl_temp ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      report_date VARCHAR(64) CHARACTER SET UNICODE,
      region_namespace VARCHAR(300) CHARACTER SET UNICODE,
      supplier_name VARCHAR(300) CHARACTER SET UNICODE,
      wallet_name VARCHAR(300) CHARACTER SET UNICODE,
      wallet_id VARCHAR(300) CHARACTER SET UNICODE,
      archived VARCHAR(300) CHARACTER SET UNICODE,
      available_balance FLOAT,
      wallet_count INTEGER,
      supplier_count VARCHAR(300) CHARACTER SET UNICODE
      )
NO PRIMARY INDEX;

drop table sandbox.np_sl_ad_snapshot_temp;
CREATE MULTISET TABLE sandbox.np_sl_ad_snapshot_temp ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      report_date VARCHAR(64) CHARACTER SET UNICODE,
      region_namespace VARCHAR(300) CHARACTER SET UNICODE,
      supplier_name VARCHAR(300) CHARACTER SET UNICODE,
      supplier_id VARCHAR(300) CHARACTER SET UNICODE,
      campaign_name VARCHAR(300) CHARACTER SET UNICODE,
      campaign_id VARCHAR(300) CHARACTER SET UNICODE,
      campaign_start_date VARCHAR(300) CHARACTER SET UNICODE,
      campaign_end_date VARCHAR(300) CHARACTER SET UNICODE,
      budget float,
      campaign_sub_type VARCHAR(300) CHARACTER SET UNICODE,
      campaign_location_cat VARCHAR(300) CHARACTER SET UNICODE,
      sku VARCHAR(300) CHARACTER SET UNICODE,
      wallet_name VARCHAR(300) CHARACTER SET UNICODE,
      impressions float,
      clicks float,
      adspend float,
      conversions float,
      unitsales float,
      sales_revenue float,
      campaign_status VARCHAR(300) CHARACTER SET UNICODE
)
NO PRIMARY INDEX;

drop table sandbox.np_sl_lowbudget_actcamp_temp;
CREATE MULTISET TABLE sandbox.np_sl_lowbudget_actcamp_temp ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      region_namespace VARCHAR(300) CHARACTER SET UNICODE,
      supplier_id VARCHAR(300) CHARACTER SET UNICODE,
      supplier_name VARCHAR(300) CHARACTER SET UNICODE,
      campaign_name VARCHAR(300) CHARACTER SET UNICODE,
      campaign_id VARCHAR(300) CHARACTER SET UNICODE,
      wallet_name VARCHAR(300) CHARACTER SET UNICODE,
      wallet_id VARCHAR(300) CHARACTER SET UNICODE,
      max_total_spend float,
      total_usd_spend float,
      wallet_available_balance float,
      active_state VARCHAR(300) CHARACTER SET UNICODE,
      valid_state VARCHAR(300) CHARACTER SET UNICODE
      )
NO PRIMARY INDEX;

drop table sandbox.np_sl_ledger_temp;
CREATE MULTISET TABLE sandbox.np_sl_ledger_temp ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      report_date date,
      region_namespace VARCHAR(300) CHARACTER SET UNICODE,
      supplier_name VARCHAR(300) CHARACTER SET UNICODE,
      wallet_id VARCHAR(300) CHARACTER SET UNICODE,
      wallet_name VARCHAR(300) CHARACTER SET UNICODE,
      retailername VARCHAR(300) CHARACTER SET UNICODE,
      ledger_style VARCHAR(300) CHARACTER SET UNICODE,
      product_ledger VARCHAR(300) CHARACTER SET UNICODE,
      reason VARCHAR(300) CHARACTER SET UNICODE,
      "transaction" float,
      total_amount float
      )
NO PRIMARY INDEX;

drop table sandbox.np_sl_summary_all_time_temp;
CREATE MULTISET TABLE sandbox.np_sl_summary_all_time_temp ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      region_namespace VARCHAR(300) CHARACTER SET UNICODE,
      supplier_id VARCHAR(300) CHARACTER SET UNICODE,
      supplier_name VARCHAR(300) CHARACTER SET UNICODE,
      campaign_type VARCHAR(300) CHARACTER SET UNICODE,
      campaign_subtype VARCHAR(300) CHARACTER SET UNICODE,
      campaign_name VARCHAR(300) CHARACTER SET UNICODE,
      campaign_id VARCHAR(300) CHARACTER SET UNICODE, 
      wallet_name VARCHAR(300) CHARACTER SET UNICODE,
      wallet_id VARCHAR(300) CHARACTER SET UNICODE, 
      catalog_products VARCHAR(300) CHARACTER SET UNICODE, 
      placements VARCHAR(300) CHARACTER SET UNICODE, 
      max_total_spend float, 
      max_cost_per_click float, 
      max_daily_spend float, 
      active_spend VARCHAR(300) CHARACTER SET UNICODE, 
      valid_state VARCHAR(300) CHARACTER SET UNICODE, 
      total_spend_usd float, 
      start_date VARCHAR(300) CHARACTER SET UNICODE, 
      end_date VARCHAR(300) CHARACTER SET UNICODE, 
      created_at VARCHAR(300) CHARACTER SET UNICODE
      )
NO PRIMARY INDEX;

drop table sandbox.np_sl_summary_mtd_temp;
CREATE MULTISET TABLE sandbox.np_sl_summary_mtd_temp ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      region_namespace VARCHAR(300) CHARACTER SET UNICODE,
      supplier_id VARCHAR(300) CHARACTER SET UNICODE,
      supplier_name VARCHAR(300) CHARACTER SET UNICODE,
      campaign_id VARCHAR(300) CHARACTER SET UNICODE,
      campaign_name VARCHAR(300) CHARACTER SET UNICODE,
      product_code VARCHAR(300) CHARACTER SET UNICODE,
      product_name VARCHAR(300) CHARACTER SET UNICODE, 
      impressions VARCHAR(300) CHARACTER SET UNICODE, 
      clicks float, 
      conversions float, 
      unit_sales float, 
      sales_revenue float, 
      impression_spend float, 
      click_spend float, 
      total_adspend float
      )
NO PRIMARY INDEX;


drop table sandbox.np_sl_supp_mapping_temp;
CREATE MULTISET TABLE sandbox.np_sl_supp_mapping_temp ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      supplier_name VARCHAR(300) CHARACTER SET UNICODE,
      team_cohort VARCHAR(300) CHARACTER SET UNICODE
      )
primary index (supplier_name);



CREATE MULTISET TABLE sandbox.np_sl_requests_temp ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      report_date VARCHAR(64) CHARACTER SET UNICODE,
      region_namespace VARCHAR(300) CHARACTER SET UNICODE,
      adtypename VARCHAR(300) CHARACTER SET UNICODE,
      adrequests float,
      total_ads_requested float,
      ads_served float,
      impressions float, 
      placement VARCHAR(64) CHARACTER SET UNICODE
      )
no primary index;


-------------------------------------------------------------------------------------------------------------------CREATING ANALYTICS TABLES



drop table sandbox.np_groupon_wallet_sl;
CREATE MULTISET TABLE sandbox.np_groupon_wallet_sl ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      report_date VARCHAR(64) CHARACTER SET UNICODE,
      region_namespace VARCHAR(300) CHARACTER SET UNICODE,
      supplier_name VARCHAR(300) CHARACTER SET UNICODE,
      wallet_name VARCHAR(300) CHARACTER SET UNICODE,
      wallet_id VARCHAR(300) CHARACTER SET UNICODE,
      archived VARCHAR(300) CHARACTER SET UNICODE,
      available_balance FLOAT,
      wallet_count INTEGER,
      supplier_count VARCHAR(300) CHARACTER SET UNICODE
      )
NO PRIMARY INDEX;

drop table sandbox.np_sl_ad_snapshot;
CREATE MULTISET TABLE sandbox.np_sl_ad_snapshot ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      report_date VARCHAR(64) CHARACTER SET UNICODE,
      region_namespace VARCHAR(300) CHARACTER SET UNICODE,
      supplier_name VARCHAR(300) CHARACTER SET UNICODE,
      supplier_id VARCHAR(300) CHARACTER SET UNICODE,
      campaign_name VARCHAR(300) CHARACTER SET UNICODE,
      campaign_id VARCHAR(300) CHARACTER SET UNICODE,
      campaign_start_date VARCHAR(300) CHARACTER SET UNICODE,
      campaign_end_date VARCHAR(300) CHARACTER SET UNICODE,
      budget float,
      campaign_sub_type VARCHAR(300) CHARACTER SET UNICODE,
      campaign_location_cat VARCHAR(300) CHARACTER SET UNICODE,
      sku VARCHAR(300) CHARACTER SET UNICODE,
      wallet_name VARCHAR(300) CHARACTER SET UNICODE,
      impressions float,
      clicks float,
      adspend float,
      conversions float,
      unitsales float,
      sales_revenue float,
      campaign_status VARCHAR(300) CHARACTER SET UNICODE
)
NO PRIMARY INDEX;

drop table sandbox.np_sl_lowbudget_actcamp;
CREATE MULTISET TABLE sandbox.np_sl_lowbudget_actcamp ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      report_date date,
      region_namespace VARCHAR(300) CHARACTER SET UNICODE,
      supplier_id VARCHAR(300) CHARACTER SET UNICODE,
      supplier_name VARCHAR(300) CHARACTER SET UNICODE,
      campaign_name VARCHAR(300) CHARACTER SET UNICODE,
      campaign_id VARCHAR(300) CHARACTER SET UNICODE,
      wallet_name VARCHAR(300) CHARACTER SET UNICODE,
      wallet_id VARCHAR(300) CHARACTER SET UNICODE,
      max_total_spend float,
      total_usd_spend float,
      wallet_available_balance float,
      active_state VARCHAR(300) CHARACTER SET UNICODE,
      valid_state VARCHAR(300) CHARACTER SET UNICODE
      )
NO PRIMARY INDEX;



drop table sandbox.np_sl_ledger;
CREATE MULTISET TABLE sandbox.np_sl_ledger ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      report_date date,
      region_namespace VARCHAR(300) CHARACTER SET UNICODE,
      supplier_name VARCHAR(300) CHARACTER SET UNICODE,
      wallet_id VARCHAR(300) CHARACTER SET UNICODE,
      wallet_name VARCHAR(300) CHARACTER SET UNICODE,
      retailername VARCHAR(300) CHARACTER SET UNICODE,
      ledger_style VARCHAR(300) CHARACTER SET UNICODE,
      product_ledger VARCHAR(300) CHARACTER SET UNICODE,
      reason VARCHAR(300) CHARACTER SET UNICODE,
      "transaction" float,
      total_amount float
      )
NO PRIMARY INDEX;

drop table sandbox.np_sl_summary_all_time;
CREATE MULTISET TABLE sandbox.np_sl_summary_all_time ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      region_namespace VARCHAR(300) CHARACTER SET UNICODE,
      supplier_id VARCHAR(300) CHARACTER SET UNICODE,
      supplier_name VARCHAR(300) CHARACTER SET UNICODE,
      campaign_type VARCHAR(300) CHARACTER SET UNICODE,
      campaign_subtype VARCHAR(300) CHARACTER SET UNICODE,
      campaign_name VARCHAR(300) CHARACTER SET UNICODE,
      campaign_id VARCHAR(300) CHARACTER SET UNICODE, 
      wallet_name VARCHAR(300) CHARACTER SET UNICODE,
      wallet_id VARCHAR(300) CHARACTER SET UNICODE, 
      catalog_products VARCHAR(300) CHARACTER SET UNICODE, 
      placements VARCHAR(300) CHARACTER SET UNICODE, 
      max_total_spend float, 
      max_cost_per_click float, 
      max_daily_spend float, 
      active_spend VARCHAR(300) CHARACTER SET UNICODE, 
      valid_state VARCHAR(300) CHARACTER SET UNICODE, 
      total_spend_usd float, 
      start_date VARCHAR(300) CHARACTER SET UNICODE, 
      end_date VARCHAR(300) CHARACTER SET UNICODE, 
      created_at VARCHAR(300) CHARACTER SET UNICODE
      )
NO PRIMARY INDEX;

drop table sandbox.np_sl_summary_mtd;
CREATE MULTISET TABLE sandbox.np_sl_summary_mtd ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      report_month date,
      region_namespace VARCHAR(300) CHARACTER SET UNICODE,
      supplier_id VARCHAR(300) CHARACTER SET UNICODE,
      supplier_name VARCHAR(300) CHARACTER SET UNICODE,
      campaign_id VARCHAR(300) CHARACTER SET UNICODE,
      campaign_name VARCHAR(300) CHARACTER SET UNICODE,
      product_code VARCHAR(300) CHARACTER SET UNICODE,
      product_name VARCHAR(300) CHARACTER SET UNICODE, 
      impressions VARCHAR(300) CHARACTER SET UNICODE, 
      clicks float, 
      conversions float, 
      unit_sales float, 
      sales_revenue float, 
      impression_spend float, 
      click_spend float, 
      total_adspend float
      )
NO PRIMARY INDEX;

drop table sandbox.np_sl_supp_mapping;
CREATE MULTISET TABLE sandbox.np_sl_supp_mapping ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      supplier_name VARCHAR(300) CHARACTER SET UNICODE,
      team_cohort VARCHAR(300) CHARACTER SET UNICODE
      )
primary index (supplier_name);



CREATE MULTISET TABLE sandbox.np_sl_requests ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      report_date VARCHAR(64) CHARACTER SET UNICODE,
      region_namespace VARCHAR(300) CHARACTER SET UNICODE,
      adtypename VARCHAR(300) CHARACTER SET UNICODE,
      adrequests float,
      total_ads_requested float,
      ads_served float,
      impressions float, 
      placement VARCHAR(64) CHARACTER SET UNICODE
      )
no primary index;


------------------------------------------------------------------------------------------------------------------Updating data
grant all access on sandbox.np_groupon_wallet_sl_temp to public
sandbox.np_groupon_wallet_sl
sandbox.np_sl_lowbudget_actcamp
sandbox.np_sl_lowbudget_actcamp_temp
sandbox.np_sl_ad_snapshot
sandbox.np_sl_ad_snapshot_temp
sandbox.np_sl_ledger_temp
sandbox.np_sl_ledger

------------WALLET
select  from sandbox.np_groupon_wallet_sl_temp;
select * from sandbox.np_groupon_wallet_sl where archived is null order by report_date;
/*
create volatile table np_additional_days as (
select report_date, 
       region_namespace, 
       supplier_name, 
       wallet_name, 
       wallet_id, 
       case when archived is null then 'FALSE' else archived end archived, 
       available_balance,  
       wallet_count, 
       supplier_count
       from sandbox.np_groupon_wallet_sl_temp 
where report_date >= '2021-08-31'
) with data on commit preserve rows;


select * from sandbox.np_groupon_wallet_sl where archived is null;


delete from sandbox.np_groupon_wallet_sl where report_date >= '2021-08-31';

create volatile table np_additional_days as (
select * from sandbox.np_groupon_wallet_sl_temp 
where report_date > trunc(current_date, 'iw')-8
) with data on commit preserve rows;*/ 

select * from user_edwprod.dim_offer_ext;
select deal_id, count(distinct deal_permalink) cnz_d , count(1) cnz from user_edwprod.dim_gbl_deal_lob where length(deal_id) > 6 group by 1 having cnz >1;

select distinct report_date from np_additional_days order by 1;

create volatile table np_additional_days as (
select * from sandbox.np_groupon_wallet_sl_temp 
where report_date > (select max(report_date) from sandbox.np_groupon_wallet_sl)
) with data on commit preserve rows;

create volatile table np_additional_days as (
select a.* from sandbox.np_groupon_wallet_sl_temp as a  
left join (select report_date from sandbox.np_groupon_wallet_sl group by 1) as b on a.report_date = b.report_date
where b.report_date is null
) with data on commit preserve rows;


 
INSERT INTO sandbox.np_groupon_wallet_sl
select * from np_additional_days;


------------LOWBUDGET
delete from sandbox.np_sl_lowbudget_actcamp
where 
  report_date = trunc(current_date, 'iw') + 6;
 
insert into sandbox.np_sl_lowbudget_actcamp
select trunc(current_date, 'iw') + 6  ,a.* from sandbox.np_sl_lowbudget_actcamp_temp as a;


------------SNAPSHOT

create volatile table np_temp_ad_snap as (
select a.* from sandbox.np_sl_ad_snapshot_temp as a
left join (select report_date from sandbox.np_sl_ad_snapshot group by 1) as b on a.report_date = b.report_date
where b.report_date is null
) with data on commit preserve rows;


delete from sandbox.np_sl_ad_snapshot 
where 
  report_date >= (select min(report_date) from sandbox.np_sl_ad_snapshot_temp);
 
 
INSERT INTO sandbox.np_sl_ad_snapshot
select * from sandbox.np_sl_ad_snapshot_temp;

select a.*, length(report_date) from sandbox.np_sl_ad_snapshot as a;


------------LEDGER

delete from sandbox.np_sl_ledger 
where 
  report_date >= (select min(report_date) from sandbox.np_sl_ledger_temp) ;

INSERT INTO sandbox.np_sl_ledger
select * from sandbox.np_sl_ledger_temp;


SELECT report_date FROM sandbox.np_sl_ledger group by 1 order by 1;

------------SUMMARY ALL TIME

delete from sandbox.np_sl_summary_all_time;

INSERT INTO sandbox.np_sl_summary_all_time
select * from sandbox.np_sl_summary_all_time_temp;



-------------MAPPING

delete from sandbox.np_sl_supp_mapping;

insert into sandbox.np_sl_supp_mapping
select a.* 
from sandbox.np_sl_supp_mapping_temp  as a 
left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name
where b.supplier_name is null;

select * from sandbox.np_sl_supp_mapping;


-------------ADS REQUEST



/*delete from sandbox.np_sl_requests 
where 
  cast(report_date as date) <= (select max(CAST(report_date AS DATE)) from sandbox.np_sl_requests_temp);*/


delete from sandbox.np_sl_requests 
where 
  cast(report_date as date) >= (select min(CAST(report_date AS DATE)) from sandbox.np_sl_requests_temp);
 
INSERT INTO sandbox.np_sl_requests
select * from sandbox.np_sl_requests_temp;


------------SUMMARY MTD

drop table sandbox.np_sl_summary_mtd ;
delete from sandbox.np_sl_summary_mtd 
where report_month = current_date - EXTRACT(DAY FROM current_date) + 1;

insert into sandbox.np_sl_summary_mtd
select current_date - EXTRACT(DAY FROM current_date) + 1, a.* from sandbox.np_sl_summary_mtd_temp as a;


/*INSERT INTO sandbox.np_sl_ad_snapshot
select a.* from sandbox.np_sl_ad_snapshot_temp as a
left join (select report_date from sandbox.np_sl_ad_snapshot group by 1) as b on a.report_date = b.report_date
where b.report_date is null;*/


/*insert into sandbox.np_sl_supp_mapping
select 
  a.supplier_name, 
  case    when lower(a.supplier_name) like '%test%' then 'Test'
          when lower(split_part(a.supplier_name, '- ', 2)) = 'coup' then 'Coupons'
          when lower(split_part(a.supplier_name, '- ', 2)) = 'ent' then 'Enterprise'
          when lower(split_part(a.supplier_name, '- ', 2)) = 'goods' then 'Goods'
          when lower(split_part(a.supplier_name, '- ', 2)) = 'ss' then 'SelfServe'
          when lower(split_part(a.supplier_name, '- ', 2)) = 'stiab' then 'STIAB'
          else 'Others'
          end team_cohort
from 
(select
     distinct
     supplier_name
from sandbox.np_groupon_wallet_sl) as a
left join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name
where b.supplier_name is null;*/






