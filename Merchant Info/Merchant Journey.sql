---min load date    ///      

DROP TABLE grp_gdoop_bizops_db.nvp_deal_min_date;
create table grp_gdoop_bizops_db.nvp_deal_min_date stored as orc as
select 
    e.merchant_uuid, 
    min(mn_load_date) mn_load_date
from (select deal_uuid, min(cast(load_date as date)) mn_load_date 
      from grp_gdoop_bizops_db.sh_bt_active_deals_log
      where product_is_active_flag = 1 and is_bookable = 1 and partner_inactive_flag = 0
      group by deal_uuid) a
join (select 
       deal_id 
       from user_edwprod.dim_gbl_deal_lob 
       where grt_l2_cat_name = 'L2 - Health / Beauty / Wellness') b on a.deal_uuid = b.deal_id
left join 
     (SELECT DISTINCT product_uuid, merchant_uuid FROM user_edwprod.dim_offer_ext) as e on a.deal_uuid = e.product_uuid
group by e.merchant_uuid
having mn_load_date >= '2020-08-12';



-----merchant onboarding
/* dwh_sc_opportunity user_edwprod.dim_dealunity

select * from grp_gdoop_bizops_db.nvp_deal_min_lddate limit 4;

drop table grp_gdoop_bizops_db.nvp_merch_min_lddate;
create table grp_gdoop_bizops_db.nvp_merch_min_lddate stored as orc as
select 
   b.merchant_uuid,
   min(a.mn_load_date) frst_load_date, 
   min(a.mn_order_date) frst_order_date,
   min(a.mn_book_date) frst_book_date, 
   min(a.mn_redeem_date) frst_redeem_date
from 
(select * from grp_gdoop_bizops_db.nvp_deal_min_date) as a 
join
(SELECT DISTINCT product_uuid, merchant_uuid FROM user_edwprod.dim_offer_ext) as b on a.deal_uuid = b.product_uuid
GROUP BY b.merchant_uuid
having frst_load_date >= cast('2020-08-12' as date); */



-----Merchant first_apt date

drop table grp_gdoop_bizops_db.nvp_merch_min_apt;
create table grp_gdoop_bizops_db.nvp_merch_min_apt stored as orc as
select 
     merchant_uuid, 
     min(cast(SUBSTRING(start_time,1,10) as date)) mn_apt_date,
     min(cast(SUBSTRING(created_at,1,10) as date)) mn_book_date,
     min(case when checked_in = 'checked-in' then cast(SUBSTRING(start_time,1,10) as date) end) mn_checkin_date
from grp_gdoop_bizops_db.sh_bt_bookings_rebuild 
where state = 'confirmed'
group by merchant_uuid;



------GCAL Sync

drop table grp_gdoop_bizops_db.nvp_merch_min_gcal;
create table grp_gdoop_bizops_db.nvp_merch_min_gcal stored as orc as
select 
   merchant_uuid, 
   country, 
   min(cast(SUBSTRING(created,1,10) as date)) mn_gcalcreated 
from 
 grp_gdoop_bizops_db.sh_bt_google_accounts
group by merchant_uuid, country;

------

drop table grp_gdoop_bizops_db.nvp_active_merchant_journey;
create table grp_gdoop_bizops_db.nvp_active_merchant_journey stored as orc as
select 
   a.merchant_uuid,
   d.salesforce_account_id, 
   case when a.mn_load_date > b.mn_book_date then mn_book_date else a.mn_load_date end onboarding_date,
   b.mn_book_date,
   b.mn_apt_date, 
   b.mn_checkin_date,
   c.mn_gcalcreated, 
   own.ownerid
from grp_gdoop_bizops_db.nvp_deal_min_date as a
left join grp_gdoop_bizops_db.nvp_merch_min_apt as b on a.merchant_uuid = b.merchant_uuid
left join grp_gdoop_bizops_db.nvp_merch_min_gcal as c on a.merchant_uuid = c.merchant_uuid
left join (select distinct merchant_uuid, salesforce_account_id from user_edwprod.dim_merchant) as d on a.merchant_uuid = d.merchant_uuid
left join (select distinct accountid, ownerid from grp_gdoop_sup_analytics_db.jc_merchant_mtd_attrib where close_recency = 1) as own on d.salesforce_account_id = own.accountid;


---Sales force account id

------ GCAL Info

/*
select * from grp_gdoop_bizops_db.sh_bt_google_calendar_integrations limit 5;
select * from grp_gdoop_bizops_db.sh_bt_google_accounts limit 5;
select * from grp_gdoop_bizops_db.sh_bt_google_calendar_entries limit 5;

select ga.merchant_uuid
, sum(case when integration_type ='export' then 1 else 0 end ) as cnt_exports 
, sum(case when integration_type ='import' then 1 else 0 end ) as cnt_imports 
, count(distinct groupon_calendar_uuid) cnt_groupon_calendar
, count(distinct google_calendar_id) cnt_google_calendar
-- select ga.*, gci.* , gce.integration_uuid, gce.groupon_booking_uuid,gce.blocked_time_rule_uuid,gce.google_event_id
from grp_gdoop_bizops_db.sh_bt_google_accounts ga
join grp_gdoop_bizops_db.sh_bt_google_calendar_integrations gci on ga.uuid = gci.google_account_uuid 
join grp_gdoop_bizops_db.sh_bt_google_calendar_entries gce on gce.integration_uuid = gci.uuid
left join (select merchant_uuid, max(country)country
	from grp_gdoop_bizops_db.sh_bt_partners
	group by merchant_uuid)mer on mer.merchant_uuid = ga.merchant_uuid
 where ga.merchant_uuid not in ( '9b6423cf-85de-4959-9cc5-2213df36c512','23a31be4-fde4-41ed-ac66-2d325961ecf0','75276b6a-3ee6-91a2-5974-61912c2b154c') --known booking test merch
group by ga.merchant_uuid;

select * from grp_gdoop_bizops_db.sh_bt_google_accounts;
select * from grp_gdoop_bizops_db.sh_bt_google_calendar_integrations; 
select * from grp_gdoop_bizops_db.sh_bt_google_calendar_entries*/


