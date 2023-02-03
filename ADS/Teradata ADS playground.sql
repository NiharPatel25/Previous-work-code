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


a.merchant_uuid, 
     merch.merchant_name, 
     merch.l2, 
     merch.contact_full_name, 
     merch.contact_email, 
     merch.BillingCity, 
     case when c.min_start_date is not null then 1 else 0 end merchant_has_a_campaign,
     d.offer_claimed_on,
     min_start_date merchant_active_date,
     total_campaigns_created, 
     a.list_category,
     d.freecredit,
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
           else 'landed on merchant center interacted ' 


from sandbox.np_ss_sl_merch_list as a
join (select * from sandbox.pai_merchants where contact_email is not null) as merch on a.merchant_uuid = merch.merchant_uuid
left join sandbox.np_ss_sl_interaction_agg as b on a.merchant_uuid = b.merchantid
left join 
   (select merchant_id, 
           min(cast(substr(create_datetime, 1,10) as date)) min_start_date,
           count(distinct campaign_name) total_campaigns_created
           from sandbox.np_sponsored_campaign 
           where status not in ('DRAFT')
           group by 1) as c on a.merchant_uuid = c.merchant_id
left join sandbox.np_merch_free_credits as d on a.merchant_uuid = d.merchant_uuid
group by 1,2,3,4,5,6,7,8,9,10,11,12

------------------------------------TEMPORARY GRANULAR TABLE FOR FREE CREDIT MERCHANTS LOGINS

select * from sandbox.np_ss_sl_user_granular_tmp;


drop table sandbox.np_ss_sl_user_granular_tmp;
CREATE MULTISET TABLE sandbox.np_ss_sl_user_granular_tmp ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO, 
     MAP = TD_MAP1
     (
      bcookie VARCHAR(50) CHARACTER SET UNICODE,
      consumerid VARCHAR(50) CHARACTER SET UNICODE,
      merchantid VARCHAR(50) CHARACTER SET UNICODE,
      consumeridsource VARCHAR(50) CHARACTER SET UNICODE, 
      rawpagetype VARCHAR(50) CHARACTER SET UNICODE, 
      row_num_rank integer, 
      eventdate VARCHAR(50) CHARACTER SET UNICODE
      )
NO PRIMARY INDEX;

SELECT bcookie,consumerid, count(1) FROM sandbox.np_ss_sl_user_granular_tmp where eventdate = '2022-03-28' group by 1,2 order by 3 desc;

drop table grp_gdoop_bizops_db.np_ss_sl_user_granular_tst;
create table grp_gdoop_bizops_db.np_ss_sl_user_granular_tst stored as orc as 
select 
    bcookie,
    consumerid, 
    merchantid, 
    consumeridsource,
    rawpagetype,
    ROW_NUMBER () over (partition by merchantid, consumerid, consumeridsource, eventdate order by eventtime asc) row_num_rank,
    eventdate
from 
(select
    bcookie,
    consumerid, 
    merchantid, 
    consumeridsource,
    rawpagetype,
    min(eventtime) eventtime,
    eventdate
from
    grp_gdoop_pde.junoHourly
where
    eventdate >= '2022-03-22'
    and clientplatform in ('web','Touch')
    and eventDestination = 'other'
    and event = 'merchantPageView'
    and country = 'US'
    and pageapp = 'sponsored-campaign-itier'
    and merchantid is not null
group by consumerid, merchantid, consumeridsource, rawpagetype, eventdate, bcookie) as fin;







select 
   a.*, 
   b.first_date_landing_sl, 
   b.drop_off_page, 
   b2.first_date_landing_sl, 
   b2.drop_off_page, 
   case when c.min_start_date is not null then 1 else 0 end merchant_has_a_campaign,
   c.min_start_date,
   case when d.create_week is not null then 1 else 0 end merchant_has_a_campaign2,
   d.create_date, 
   offer_availed_on -offer_sent_on  days_bet_offer_availed, 
   case when b2.first_date_landing_sl <> b.first_date_landing_sl then 1 else 0 end previously_logged_in
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
where eventdate >= cast('2022-03-23' as date)
group by 1) as b on a.merchant_uuid = b.merchantid
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
where eventdate < cast('2022-03-23' as date)
group by 1) as b2 on a.merchant_uuid = b2.merchantid
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
          
select * from sandbox.np_citrusad_campaigns;

               
               
select distinct status from sandbox.np_sponsored_campaign;
          

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
               max(contact_full_name) contact_full_name
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








select d.deal_uuid, 
               max(m.account_id) account_id, 
               max(m.acct_owner) account_owner, 
               max(acct_owner_name) acct_owner_name,
               max(contact_full_name) contact_full_name,
               max(sfa.name) account_name, 
               max(merchant_segmentation__c) merch_segmentation
        from sandbox.pai_deals d 
        join sandbox.pai_merchants m on d.merchant_uuid = m.merchant_uuid
        join dwh_base_sec_view.sf_account sfa on m.account_id = sfa.id
        group by d.deal_uuid;
 
select guid, 
       max(a.id) account_id, 
       max(sfa.name) account_name, 
       max(merchant_segmentation__c) merch_segmentation,
       max(Account_Manager)
     from dwh_base_sec_view.sf_account_2 a
     join dwh_base_sec_view.sf_account sfa on a.id = sfa.id
     group by 1;
    
select * from dwh_base_sec_view.sf_account;

select 
  a.merchant_uuid, 
  a.merchant_name, 
  a.freecredit, 
  merch.campaigns_created,
  case when b.total_adspend >= a.freecredit then 1 else 0 end spend_more_than_free_credit, 
  case when c.number_of_top_ups > 0 then 1 else 0 end merchant_topped_up, 
  c.number_of_top_ups, 
  c.amount_of_top_ups
from sandbox.np_merch_free_credits as a 
left join 
(select 
    a.merchant_id,
    a.merchant_name,
    m.account_id,
    min(cast(substr(create_datetime, 1,10) as date)) created_date, 
    count(distinct campaign_name) campaigns_created
from sandbox.np_sponsored_campaign as a
     left join sandbox.pai_merchants as m on a.merchant_id = m.merchant_uuid
     where a.status not in ('DRAFT')
group by 1,2,3) as merch on a.merchant_uuid = merch.merchant_id
left join 
	   (SELECT 
		      b.merchant_uuid,
		      sum(impressions) total_impressions,
		      sum(clicks) total_clicks,
		      sum(conversions) total_orders,
		      sum(total_spend_amount) total_adspend
		FROM sandbox.np_ss_performance_met as a
		left join sandbox.pai_deals as b on a.deal_id = b.deal_uuid
		group by 1) as b on a.merchant_uuid = b.merchant_uuid
left join 
		(select 
           merchant_id, 
           count(1) number_of_top_ups, 
           sum(amount) amount_of_top_ups
          from sandbox.np_merchant_topup_orders as a
           where event_type='CAPTURE' and event_status='SUCCESS'
           group by 1) as c on a.merchant_uuid = c.merchant_id
where a.offer_claimed_on is not null;



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
group by 1,2,3,4) fin;


select status, status_change_reason from sandbox.np_sponsored_campaign group by 1,2 order by 1,2;
--------------------------------------------------

Blue Apron - COUP
Fabletics - COUP

select distinct supplier_name, sku from sandbox.np_sl_ad_snapshot where supplier_name like '%LightRx%';

drop table sandbox.np_slad_wlt_blc_htry;
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



-----------------------------------SELF SERVICE PULL
create volatile table np_merch_catlist as
(select merchant_uuid, 
       max(account_name) account_name,
       max(case when list_category = '300' then 1 else 0 end) free_cred_300, 
       max(case when list_category = '100' then 1 else 0 end) free_cred_100, 
       max(case when list_category = 'handraiser' then 1 else 0 end) handraiser
   from  
       sandbox.np_ss_sl_merch_list
   group by 1) with data on commit preserve rows;

select 
        a.supplier_name,
        a.campaign_name,
        a.campaign_id,
        a.campaign_status,
        b.team_cohort,
        d.merchant_uuid,
        e.freecredit, 
        f.free_cred_300 one_to_eighty_list,
        f.free_cred_100 eighty_to_300_list, 
        f.handraiser,
        g.target_month MM_list,
        case when g.target_month is not null and g.rep_status is null then 'Email Only' 
             when g.target_month is not null and g.rep_status in ('Not Contacted', '#N/A') then 'Email Only' 
             when g.target_month is not null then 'Mets contacted' end rep_status_logic,
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
        case when roas > 1.5 then 1 else 0 end roas_more_than_1_5, 
        case when sum(adspend) > 0 then 1 else 0 end merchants_with_adspend, 
        case when sum(adspend) > 100 then 1 else 0 end merchants_adspend_more_than_100
  from sandbox.np_sl_ad_snapshot AS a
  join sandbox.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name and b.team_cohort = 'SelfServe'
  left join (select 
                 deal_id, 
                 max(grt_l1_cat_name) l1, 
                 max(grt_l2_cat_name) l2, 
                 max(grt_l3_cat_name) l3  
             from user_edwprod.dim_gbl_deal_lob 
             group by 1) as c on a.sku = c.deal_id
  left join sandbox.pai_deals as d on a.sku = d.deal_uuid
  left join sandbox.np_merch_free_credits as e on d.merchant_uuid = e.merchant_uuid
  left join np_merch_catlist as f on d.merchant_uuid = f.merchant_uuid
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
      group by 1,2,3,4) as g on d.merchant_uuid = g.merchant_uuid
  group by 1,2,3,4,5,6,7,8,9,10,11,12
  
  order by 8 desc;

SELECT * FROM sandbox.np_sl_ad_snapshot;

select 
     a.merchant_uuid, 
     merch.merchant_name, 
     merch.l2, 
     merch.contact_full_name, 
     merch.contact_email, 
     merch.BillingCity, 
     case when c.min_start_date is not null then 1 else 0 end merchant_has_a_campaign,
     d.offer_claimed_on,
     min_start_date merchant_active_date,
     total_campaigns_created, 
     a.list_category,
     d.freecredit,
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
           end drop_off_page
from sandbox.np_ss_sl_merch_list as a
join (select * from sandbox.pai_merchants where contact_email is not null) as merch on a.merchant_uuid = merch.merchant_uuid
left join sandbox.np_ss_sl_interaction_agg as b on a.merchant_uuid = b.merchantid
left join 
   (select merchant_id, 
           min(cast(substr(create_datetime, 1,10) as date)) min_start_date,
           count(distinct campaign_name) total_campaigns_created
           from sandbox.np_sponsored_campaign 
           where status not in ('DRAFT')
           group by 1) as c on a.merchant_uuid = c.merchant_id
left join sandbox.np_merch_free_credits as d on a.merchant_uuid = d.merchant_uuid
group by 1,2,3,4,5,6,7,8,9,10,11,12
;


select 
      a.*, 
      f.account_id, 
      free_cred.freecredit internal_free_credit, 
      free_cred.offer_sent_on, 
      free_cred.offer_availed_on,
      case when free_cred.offer_availed_on is not null then 1 else 0 end merchant_availed_offer,
      case when b1.merchant_id is not null then 1
           when b2.merchant_aware_of_ss = 1 then 1 
           else 0 end merchant_active,
      b1.min_start_date merchant_active_date, 
      b1.total_campaigns_created,
      c.total_impressions, 
      c.total_clicks,
      c.total_orders,
      c.total_adspend,
      c.orders_rev,
      d.number_of_top_ups,
      d.amount_of_top_ups, 
      e.drop_off_page activity_log_in_mc
from np_merch_catlist as a
left join sandbox.np_merch_free_credits as free_cred on a.merchant_uuid = free_cred.merchant_uuid
left join 
   (select merchant_id, 
           min(cast(substr(create_datetime, 1,10) as date)) min_start_date,
           count(distinct campaign_name) total_campaigns_created
           from sandbox.np_sponsored_campaign 
           where status not in ('DRAFT')
           group by 1) as b1 on a.merchant_uuid = b1.merchant_id
left join np_wallet_data as b2 on a.merchant_uuid = b2.merchant_id
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
		group by 1) as c on a.merchant_uuid = c.merchant_uuid
left join 
		(select 
           merchant_id, 
           count(1) number_of_top_ups, 
           sum(amount) amount_of_top_ups
          from sandbox.np_merchant_topup_orders as a
           where event_type='CAPTURE' and event_status='SUCCESS'
           group by 1) as d on a.merchant_uuid = d.merchant_id
left join 
        np_temp_merch_drop_off as e on a.merchant_uuid = e.merchant_uuid
left join 
        sandbox.pai_merchants as f on a.merchant_uuid = f.merchant_uuid
order by 6
;

select * from sandbox.pai_merchants;




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


SELECT 
      cast(report_date as date) report_date,
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
group by 1,2;


select 
   distinct deal_id
   from sandbox.np_ss_performance_met
   where 
     deal_id
      in 
     ('2888a2c3-5602-468b-b48b-486461661ccf',
      'c429c8aa-b166-4798-bf59-20a2e670b0f2',
      'd9c59ead-6ca3-48b0-b20b-889276246b80',
      '27931cc6-8c96-4a0a-8962-13e1b1eadb63',
      '3c727a84-4d32-4c1a-9ae2-fb3584d5cb9f')
;


drop table np_redeemed_orders;
create volatile table np_redeemed_orders  as 
(select 
   order_id, 
   purchaser_consumer_id,
   max(case when (customer_redeemed = 1 or merchant_redeemed = 1) then 1 else 0 end) redeemed
from user_gp.camp_membership_coupons
group by 1,2) with data on commit preserve rows;


drop table np_fgt_rose;
create volatile table np_fgt_rose  as (
select 
   deal_uuid, 
   order_date,
   order_id,
   user_uuid
 from user_edwprod.fact_gbl_transactions 
   where 
   action = 'authorize'
   and deal_uuid = 'f68ed31e-ec26-4140-92c4-215d3cf4bdb8'
   and order_date >= '2019-01-01' 
   and order_date <= '2021-04-30'
   and is_order_canceled = 0
   and is_zero_amount = 0
   ) with data on commit preserve rows
   ;

drop table np_fgt_rose_ref;
create volatile table np_fgt_rose_ref  as (
select 
   deal_uuid, 
   order_date,
   order_id,
   user_uuid
 from user_edwprod.fact_gbl_transactions 
   where 
   action = 'refund'
   and deal_uuid = 'f68ed31e-ec26-4140-92c4-215d3cf4bdb8'
   and order_date >= '2019-01-01' 
   and order_date <= '2021-04-30'
   ) with data on commit preserve rows
   ;

select * from np_fgt_rose_ref;
  
  
select 
   year(a.order_date) year_ord, 
   month(a.order_date) month_ord, 
   count(distinct a.order_id) total_orders, 
   count(distinct  case when redeemed = 1 then a.order_id end) redeemed_orders,
   cast(redeemed_orders as float)/total_orders,
   count(distinct case when c.order_id is null then a.order_id end) total_not_ref_orders, 
   count(distinct case when redeemed = 1 and c.order_id is null then a.order_id end) redeemed_not_ref_orders,
   cast(redeemed_not_ref_orders as float)/total_not_ref_orders
   from 
    np_fgt_rose as a 
   left join 
    np_redeemed_orders as b on a.user_uuid = b.purchaser_consumer_id and a.order_id = b.order_id
   left join 
    np_fgt_rose_ref as c on a.user_uuid = c.user_uuid and a.order_id = c.order_id
    group by 1,2
    order by 1,2;


sel deal_uuid,
            min(load_date) bt_launch_date
        from sandbox.sh_bt_active_deals_log
        where product_is_active_flag = 1
        and partner_inactive_flag = 0
        and deal_uuid in ('dc419485-ae91-448e-b967-3e89bf13eff8','b442807b-eb1e-423b-8ac4-c1c075bd88f5')
        group by 1;

select * from user_edwprod.dim_gbl_deal_lob;






select * from sandbox.np_sl_ad_snapshot;


create volatile table sh_bt_deals as (
    sel a.deal_uuid,
        sf.opportunity_id,
        account_name,
        account_owner,
        account_id,
        cast(bt_launch_date as date) bt_launch_date,
        division,
        has_gcal
    from sandbox.sh_bt_active_deals_log_v4 a
    join user_edwprod.dim_gbl_deal_lob gdl on a.deal_uuid = gdl.deal_id
    left join (
        sel deal_uuid,
            min(load_date) bt_launch_date
        from sandbox.sh_bt_active_deals_log
        where product_is_active_flag = 1
        and partner_inactive_flag = 0
        group by 1
    ) l on a.deal_uuid = l.deal_uuid
    left join (
        sel deal_uuid, max(o1.id) opportunity_id, max(o1.division) division, max(sfa.name) account_name, max(full_name) account_owner, max(o1.accountid) account_id
        from user_edwprod.sf_opportunity_1 o1
        join user_edwprod.sf_opportunity_2 o2 on o1.id = o2.id
        join user_edwprod.sf_account sfa on o1.accountid = sfa.id
        left join user_groupondw.dim_sf_person sfp on sfa.ownerid = sfp.person_id
        group by 1
    ) sf on a.deal_uuid = sf.deal_uuid
    where product_is_active_flag = 1
    and partner_inactive_flag = 0
    and load_date = current_date-2
    and gdl.country_code = 'US'
    and gdl.grt_l1_cat_name = 'L1 - Local'
    and gdl.grt_l2_cat_description = 'HBW'
) with data unique primary index (deal_uuid) on commit preserve rows;


drop table sh_fgt_ord;
create volatile table sh_fgt_ord as (
    sel fgt.*
    from user_edwprod.fact_gbl_transactions fgt
    join (select deal_uuid, load_date from sandbox.sh_bt_active_deals_log group by 1,2) act on fgt.deal_uuid = act.deal_uuid and fgt.order_date = act.load_date
    where action = 'authorize'
    ) with data primary index (order_id, action) on commit preserve rows;


select 
    a.*, 
    sum(ord.transaction_qty) units_sold_bt
    from
    sh_bt_deals as a 
    left join sh_fgt_ord as ord on a.deal_uuid = ord.deal_uuid
    group by 1,2,3,4,5,6,7,8
    ;