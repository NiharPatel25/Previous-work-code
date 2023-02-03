------------------------------------------------------------------------------------1 - 300 FREE CREDIT LIST
grant select on sandbox.np_ss_sl_merch_list to abautista;

drop table sandbox.np_ss_sl_merch_list;
CREATE MULTISET TABLE sandbox.np_ss_sl_merch_list ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO, 
     MAP = TD_MAP1
     (
      account_name VARCHAR(100) CHARACTER SET UNICODE,
      merchant_uuid VARCHAR(100) CHARACTER SET UNICODE,
      list_category varchar(10) character set unicode
      )
NO PRIMARY INDEX;

select * from sandbox.np_ss_sl_merch_list;
select * from user_gp.ads_rcncld_intrmdt_rpt;


------------------------------------------------------------------------------------MONTHLY MERCHANT TARGET LIST

drop table sandbox.np_ss_sl_mnthly_trgtlist;
CREATE MULTISET TABLE sandbox.np_ss_sl_mnthly_trgtlist ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO, 
     MAP = TD_MAP1
     (
      account_name VARCHAR(100) CHARACTER SET UNICODE,
      account_id VARCHAR(100) CHARACTER SET UNICODE,
      target_month varchar(10) character set unicode,
      rep_status varchar(25) character set unicode
      )
NO PRIMARY INDEX;


------------------------------------------------------------------------------------

drop table sandbox.np_ss_sl_list_rep;
CREATE MULTISET TABLE sandbox.np_ss_sl_list_rep ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO, 
     MAP = TD_MAP1
     (
     account_id VARCHAR(100) CHARACTER SET UNICODE, 
     account_name VARCHAR(100) CHARACTER SET UNICODE,
      md_ms varchar(10) character set unicode,
      rep_status varchar(25) character set unicode
      )
NO PRIMARY INDEX;
------------------------------------------------------------------------------------

drop table sandbox.np_ss_sl_list_opp;
CREATE MULTISET TABLE sandbox.np_ss_sl_list_opp ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO, 
     MAP = TD_MAP1
     (
     account_id VARCHAR(100) CHARACTER SET UNICODE, 
     account_name VARCHAR(100) CHARACTER SET UNICODE,
      opp_id varchar(100) character set unicode
      )
NO PRIMARY INDEX;


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
	  offer_valid_upto date,
	  phase varchar(25) CHARACTER SET UNICODE
      )
NO PRIMARY INDEX;






---------------------------------------------------------------------------------------------------------FREE CREDIT


create volatile table np_temp_freecred as 
(select 
      a.*, 
      case when a.offer_availed_on is not null then 1 else 0 end merchant_availed_offer,
      case when b.merchant_id is not null then 1 else 0 end merchant_active, 
      b.min_start_date merchant_active_date, 
      b.total_campaigns_created,
      c.adspend total_spend,
      c.seven_day_adspend adspend_last_week, 
      c.seven_day_roas,
      d.number_of_top_ups,
      d.amount_of_top_ups
from sandbox.np_merch_free_credits as a 
left join 
   (select merchant_id, 
           min(cast(substr(create_datetime, 1,10) as date)) min_start_date,
           count(distinct campaign_name) total_campaigns_created
           from sandbox.np_sponsored_campaign 
           where status not in ('DRAFT')
           group by 1) as b on a.merchant_uuid = b.merchant_id
left join 
	   (SELECT 
		      b.merchant_uuid,
		      sum(total_spend_amount) adspend,
		      sum(price_with_discount) orders_rev,  
		      cast(orders_rev as float)/NULLIFZERO(adspend) roas,
		      sum(case when cast(report_date as date) >= current_date - 7 then total_spend_amount end) seven_day_adspend, 
		      sum(case when cast(report_date as date) >= current_date - 7 then price_with_discount end) seven_day_orders_rev,
		      cast(seven_day_orders_rev as float)/NULLIFZERO(seven_day_adspend) seven_day_roas 
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
           group by 1) as d on a.merchant_uuid = d.merchant_id) with data on commit preserve rows;


          


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
) as a;




select * from sandbox.citrusad_team_wallet;

----------------------------------------------------------------------------------------------------------------------MONTHLY 3000 LIST 

drop table np_temp_merch_drop_off;
create volatile table np_wallet_data as(
select 
     merchant_id, 
     count(distinct wallet_id) total_wallets, 
     sum(balance) total_wallet_balance, 
     min(cast(substr(create_datetime, 1, 10) as date)) min_create_date, 
     min(cast(substr(citrusad_import_date, 1, 10) as date)) min_citrusas_import_date,
     max(is_self_serve) is_self_serve, 
     max(valid) merchant_aware_of_ss
from
(select 
     a.*,
     case when a.is_self_serve <> 1 then 1 
          when b.merchant_id is not null then 1 
          else 0 end valid
from sandbox.citrusad_team_wallet as a 
left join 
(select distinct merchant_id from sandbox.np_sponsored_campaign where status not in ('DRAFT')) as b on a.is_self_serve = 1 and a.merchant_id = b.merchant_id) as fin
group by 1
) with data on commit preserve rows;



create volatile table np_merch_catlist as
(select merchant_uuid, 
       max(account_name) account_name,
       max(case when list_category = '300' then 1 else 0 end) free_cred_300, 
       max(case when list_category = '100' then 1 else 0 end) free_cred_100, 
       max(case when list_category = 'handraiser' then 1 else 0 end) handraiser
   from  
       sandbox.np_ss_sl_merch_list
   group by 1) with data on commit preserve rows;


create volatile table np_temp_merch_drop_off as 
(select 
     a.merchant_uuid, 
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
from (select a.*, 
           max(COALESCE(live_merchant, was_live_merchant)) merchant_uuid
     from sandbox.np_ss_sl_mnthly_trgtlist as a 
     left join 
          (select 
             account_id, 
             max(case when is_live = 1 then merchant_uuid end) live_merchant, 
             max(case when was_live = 1 then merchant_uuid end) was_live_merchant 
             from sandbox.pai_merchants
             group by 1) as b on a.account_id = b.account_id
      group by 1,2,3,4
      ) as a
left join sandbox.np_ss_sl_interaction_agg as b on a.merchant_uuid = b.merchantid
left join 
   (select merchant_id, 
           min(cast(substr(create_datetime, 1,10) as date)) min_start_date,
           count(distinct campaign_name) total_campaigns_created
           from sandbox.np_sponsored_campaign 
           where status not in ('DRAFT')
           group by 1) as c on a.merchant_uuid = c.merchant_id
group by 1) with data on commit preserve rows;



select 
      base.account_name,
      base.account_id, 
      base.target_month,
      a.free_cred_300, 
      a.free_cred_100, 
      a.handraiser,
      case when b1.merchant_id is not null then 1
           when b2.merchant_id is not null then 1 
           else 0 end merchant_active,
      free_cred.freecredit internal_free_credit, 
      free_cred.offer_sent_on, 
      free_cred.offer_availed_on,
      case when free_cred.offer_availed_on is not null then 1 else 0 end merchant_availed_offer,
      coalesce(b1.min_start_date, b2.min_create_date) merchant_active_date, 
      b1.total_campaigns_created total_ss_campaigns_created,
      c.total_impressions, 
      c.total_clicks,
      c.total_orders,
      c.total_adspend,
      c.orders_rev,
      d.number_of_top_ups,
      d.amount_of_top_ups, 
      e.drop_off_page activity_log_in_mc, 
      case when visited_home + visited_hub 
                + visited_set_campaign 
                + visited_set_location + visited_set_date_page 
                + visited_set_budget + visited_add_payment 
                + visited_review + visited_final >= 1 then 1 else 0 end visited_mc,
      b2.total_wallet_balance,
      cast(c.orders_rev as float)/NULLIFZERO(c.total_adspend) roas,
      case when roas >= 1 then 1 else 0 end roas_more_than_1,
      case when roas < 1 then 1 else 0 end roas_less_than_1,
      case when roas >= 2 then 1 else 0 end roas_more_than_2,
      f.acct_owner, 
      f.l2,
      f.current_metal_segment,
      case when base.rep_status is null then '-' when base.rep_status in ('Not Contacted', '#N/A') then '-' else base.rep_status end rep_status_logic,
      case when cast(merchant_active_date as date) < cast('2021-10-06' as date) then 0 else 1 end valid_merchant,
      base.rep_status,
      trunc(cast(coalesce(b1.min_start_date, b2.min_create_date) as date), 'iw')+6 week_start_date
from 
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
      group by 1,2,3,4
      ) as base
left join np_merch_catlist as a on base.merchant_uuid = a.merchant_uuid
left join sandbox.np_merch_free_credits as free_cred on base.merchant_uuid = free_cred.merchant_uuid
left join 
   (select merchant_id, 
           min(cast(substr(create_datetime, 1,10) as date)) min_start_date,
           count(distinct campaign_name) total_campaigns_created
           from sandbox.np_sponsored_campaign 
           where status not in ('DRAFT')
           group by 1) as b1 on base.merchant_uuid = b1.merchant_id
left join np_wallet_data as b2 on base.merchant_uuid = b2.merchant_id and b2.merchant_aware_of_ss = 1
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
		group by 1) as c on base.merchant_uuid = c.merchant_uuid
left join 
		(select 
           merchant_id, 
           count(1) number_of_top_ups, 
           sum(amount) amount_of_top_ups
          from sandbox.np_merchant_topup_orders as a
           where event_type='CAPTURE' and event_status='SUCCESS'
           group by 1) as d on base.merchant_uuid = d.merchant_id
left join 
        np_temp_merch_drop_off as e on base.merchant_uuid = e.merchant_uuid
left join sandbox.pai_merchants as f on base.merchant_uuid = f.merchant_uuid
;
	


select 
    base.*, 
    b.min_start_date merchant_active_date,
    c.merchant_uuid merchat_uuid2, 
    c.report_week, 
    c.total_impressions,
	c.total_clicks,
	c.total_orders,
	c.adspend,
	c.orders_rev,
	c.roas,
    case when base.rep_status is null then '-' when base.rep_status in ('Not Contacted', '#N/A') then '-' else base.rep_status end rep_status_logic,
    case when cast(merchant_active_date as date) < cast('2021-10-06' as date) then 0 else 1 end valid_merchant,
    base.rep_status,
    cast(c.orders_rev as float)/NULLIFZERO(c.adspend) roas,
    case when roas >= 1 then 1 else 0 end roas_more_than_1,
    case when roas < 1 then 1 else 0 end roas_less_than_1,
    case when roas >= 2 then 1 else 0 end roas_more_than_2,
    f.acct_owner, 
    f.l2,
    f.current_metal_segment
    from
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
      group by 1,2,3,4
      ) as base
left join 
   (select merchant_id, 
           min(cast(substr(create_datetime, 1,10) as date)) min_start_date,
           count(distinct campaign_name) total_campaigns_created
           from sandbox.np_sponsored_campaign 
           where status not in ('DRAFT')
           group by 1) as b on base.merchant_uuid = b.merchant_id
left join 
	   (SELECT 
	          trunc(cast(a.report_date as date), 'iw')+6 report_week, 
		      b.merchant_uuid,
		      sum(impressions) total_impressions,
		      sum(clicks) total_clicks,
		      sum(conversions) total_orders,
		      sum(total_spend_amount) adspend,
		      sum(price_with_discount) orders_rev,
		      cast(orders_rev as float)/NULLIFZERO(adspend) roas
		FROM sandbox.np_ss_performance_met as a
		left join sandbox.pai_deals as b on a.deal_id = b.deal_uuid
		group by 1,2) as c on base.merchant_uuid = c.merchant_uuid
left join sandbox.pai_merchants as f on base.merchant_uuid = f.merchant_uuid
;



------top ups and other wow


select 
    base.merchant_uuid, 
    base.target_month,
    b.min_start_date merchant_active_date,
    case when base.rep_status is null then '-' when base.rep_status in ('Not Contacted', '#N/A') then '-' else base.rep_status end rep_status_logic,
    case when cast(merchant_active_date as date) < cast('2021-10-06' as date) then 0 else 1 end valid_merchant,
    base.rep_status,
    c.create_date,
    c.number_of_top_ups, 
    c.amount_of_top_ups,
    base.account_id
    from
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
      group by 1,2,3,4
      ) as base
left join 
   (select merchant_id, 
           min(cast(substr(create_datetime, 1,10) as date)) min_start_date,
           count(distinct campaign_name) total_campaigns_created
           from sandbox.np_sponsored_campaign 
           where status not in ('DRAFT')
           group by 1) as b on base.merchant_uuid = b.merchant_id
join
   (select 
       trunc(cast(substr(create_datetime, 1,10) as date), 'iw') + 6 create_date,
       merchant_id, 
       count(1) number_of_top_ups, 
       sum(amount) amount_of_top_ups
       from sandbox.np_merchant_topup_orders as a
       where event_type='CAPTURE' and event_status='SUCCESS'
       group by 1,2) as c on base.merchant_uuid = c.merchant_id

  



select 
   fin.*, 
   c.opportunity_id, 
   c.deal_uuid
   from 
  (select a.*, 
       COALESCE(live_merchant, was_live_merchant) merchant_uuid
     from sandbox.np_ss_sl_mnthly_trgtlist as a 
     left join 
          (select 
             account_id, 
             max(case when is_live = 1 then merchant_uuid end) live_merchant, 
             max(case when was_live = 1 then merchant_uuid end) was_live_merchant 
             from sandbox.pai_merchants
             group by 1) as b on a.account_id = b.account_id) fin 
     left join sandbox.pai_deals as c on fin.merchant_uuid = c.merchant_uuid

     
     
     
---------------------------------------------------------NET WALLET BALANCE WOW Monthly List 3000

select 
    base.merchant_uuid, 
    base.target_month,
    b.min_start_date merchant_active_date,
    case when base.rep_status is null then '-' when base.rep_status in ('Not Contacted', '#N/A') then '-' else base.rep_status end rep_status_logic,
    case when cast(merchant_active_date as date) < cast('2021-10-06' as date) then 0 else 1 end valid_merchant,
    base.rep_status,
    base.account_id, 
    week_start, 
    available_balance
    from
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
      group by 1,2,3,4
      ) as base
left join 
   (select merchant_id, 
           min(cast(substr(create_datetime, 1,10) as date)) min_start_date,
           count(distinct campaign_name) total_campaigns_created
           from sandbox.np_sponsored_campaign 
           where status not in ('DRAFT')
           group by 1) as b on base.merchant_uuid = b.merchant_id
left join
	(select 
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
	group by 1,2) as c on base.merchant_uuid = c.merchant_id
           



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
;

select * from sandbox.np_slad_wlt_blc_htry;
---------------------------------------------------------METRICS

sel deal_uuid, max(o1.id) opportunity_id, max(o1.division) division, max(sfa.name) account_name, max(full_name) account_owner, max(o1.accountid) account_id
        from user_edwprod.sf_opportunity_1 o1
        join user_edwprod.sf_opportunity_2 o2 on o1.id = o2.id
        join user_edwprod.sf_account sfa on o1.accountid = sfa.id
        left join user_groupondw.dim_sf_person sfp on sfa.ownerid = sfp.person_id;
       
select merchant_permalink from user_edwprod.sf_account where Account_ID_18 = '0013c00001qzhLHAAY';

select * from sandbox.pai_merchants as a 
join (select merchant_id, 
           min(cast(substr(create_datetime, 1,10) as date)) min_start_date,
           count(distinct campaign_name) total_campaigns_created
           from sandbox.np_sponsored_campaign 
           where status not in ('DRAFT')
           group by 1) as b on a.merchant_uuid = b.merchant_id;

select 
     a.*,
     max(case when rawpagetype = 'hub' then 1 else 0 end) visited_hub,
     min(case when rawpagetype = 'hub' then eventdate else 0 end) visited_hub_date
from sandbox.np_ss_sl_merch_list as a
left join sandbox.np_ss_sl_interaction_agg as b on a.merchant_uuid = b.merchantid
left join 
   (select merchant_id, 
           min(cast(substr(create_datetime, 1,10) as date)) min_start_date,
           count(distinct campaign_name) total_campaigns_created
           from sandbox.np_sponsored_campaign 
           where status not in ('DRAFT')
           group by 1) as b on a.merchant_uuid = c.merchant_id
group by 1,2,3
;



select * from sandbox.np_ss_sl_merch_list;
select * from sandbox.np_ss_sl_interaction_agg;
select distinct rawpagetype from sandbox.np_ss_sl_interaction_agg;




--------------------------------------------------------------------------------------------------------------------------------TABLEAU IMPORT FOR ALL LISTS



grant select on sandbox.np_mets_target_merch to 




create volatile table np_merch_alllist as(
select 
   merchant_uuid
from 
(select merchant_uuid from sandbox.np_ss_sl_merch_list
union 
select merchant_uuid from sandbox.np_merch_free_credits
union 
select merchant_uuid from 
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
      group by 1,2,3,4) as xyz
) as fin 
group by merchant_uuid
) with data on commit preserve rows
;


create volatile table np_merch_catlist as
(select merchant_uuid, 
       max(account_name) account_name,
       max(case when list_category = '300' then 1 else 0 end) free_cred_300, 
       max(case when list_category = '100' then 1 else 0 end) free_cred_100, 
       max(case when list_category = 'handraiser' then 1 else 0 end) handraiser
   from  
       sandbox.np_ss_sl_merch_list
   group by 1) with data on commit preserve rows;




create volatile table np_temp_merch_drop_off as 
(select 
     a.merchant_uuid, 
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
from np_merch_alllist as a
left join sandbox.np_ss_sl_interaction_agg as b on a.merchant_uuid = b.merchantid
left join 
   (select merchant_id, 
           min(cast(substr(create_datetime, 1,10) as date)) min_start_date,
           count(distinct campaign_name) total_campaigns_created
           from sandbox.np_sponsored_campaign 
           where status not in ('DRAFT')
           group by 1) as c on a.merchant_uuid = c.merchant_id
group by 1) with data on commit preserve rows
;


      
      

drop table sandbox.np_sl_ss_list_tableau;
create table sandbox.np_sl_ss_list_tableau as
(select 
      base.*,
      b_one.free_cred_300, 
      b_one.free_cred_100, 
      b_one.handraiser,
      b_two.offer_availed_on,
      b_two.freecredit, 
      b_three.target_month monthly_list,
      case when b_two.offer_availed_on is not null then 1 else 0 end merchant_availed_offer,
      case when b.merchant_id is not null then 1 else 0 end merchant_active, 
      b.min_start_date merchant_active_date, 
      b.total_campaigns_created,
      c.total_impressions,
      c.total_clicks, 
      c.total_orders, 
      c.total_adspend,
      c.orders_rev,
      c.roas,
      d.number_of_top_ups,
      d.amount_of_top_ups, 
      e.visited_home,
      e.visited_home_date,
      e.visited_hub,
      e.visited_hub_date,
      e.visited_set_campaign,
      e.visited_set_campaign_date,
      e.visited_set_location,
      e.visited_set_location_date,
      e.visited_set_date_page,
      e.visited_set_date_date,
      e.visited_set_budget,
      e.visited_set_budget_date,
      e.visited_add_payment,
      e.visited_add_payment_date,
      e.visited_review,
      e.visited_review_date,
      e.visited_final,
      e.visited_final_date, 
      e.drop_off_page, 
      f.country_code,
      case when visited_home + visited_hub 
                + visited_set_campaign 
                + visited_set_location + visited_set_date_page 
                + visited_set_budget + visited_add_payment 
                + visited_review + visited_final >= 1 then 1 else 0 end visited_mc,
      f.acct_owner, 
      f.l2,
      f.current_metal_segment,
      case when b_three.merchant_uuid is not null and cast(merchant_active_date as date) < cast('2021-10-06' as date) then 0 else 1 end valid_merchant,
      case when b_three.merchant_uuid is not null 
                and b_three.rep_status is null 
           then '-' 
           when b_three.merchant_uuid is not null 
                and b_three.rep_status in ('Not Contacted', '#N/A') 
           then '-' 
           else b_three.rep_status end rep_status_logic,
      b_three.rep_status, 
      case when (b_one.free_cred_300 = 1 or b_one.free_cred_100 = 1 or b_one.handraiser = 1 or b_three.merchant_uuid is not null) then 1 else 0 end is_in_list
from np_merch_alllist as base
left join np_merch_catlist as b_one on base.merchant_uuid = b_one.merchant_uuid
left join sandbox.np_merch_free_credits as b_two on base.merchant_uuid = b_two.merchant_uuid
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
      group by 1,2,3,4) as b_three on base.merchant_uuid = b_three.merchant_uuid
left join 
      (select merchant_id, 
           min(cast(substr(create_datetime, 1,10) as date)) min_start_date,
           count(distinct campaign_name) total_campaigns_created
           from sandbox.np_sponsored_campaign 
           where status not in ('DRAFT')
           group by 1) as b on base.merchant_uuid = b.merchant_id
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
		group by 1) as c on base.merchant_uuid = c.merchant_uuid
left join 
		(select 
           merchant_id, 
           count(1) number_of_top_ups, 
           sum(amount) amount_of_top_ups
          from sandbox.np_merchant_topup_orders as a
           where event_type='CAPTURE' and event_status='SUCCESS'
           group by 1) as d on base.merchant_uuid = d.merchant_id
left join 
        np_temp_merch_drop_off as e on base.merchant_uuid = e.merchant_uuid
left join 
        sandbox.pai_merchants as f on base.merchant_uuid = f.merchant_uuid) with data;


select a.*, 
       case when free_cred_300 = 1 then '1-80_list'
            when free_cred_100 = 1 then '81-300 list'
            else monthly_list end
from sandbox.np_sl_ss_list_tableau as a ;

grant select on sandbox.np_sl_supplier_l3_daily to public;
grant select on sandbox.np_sl_requests to public;
grant select on sandbox.np_sl_ss_list_tableau to public;


select * from sandbox.np_sl_ss_list_tableau;

-----------WOW All List

select * from sandbox.np_sl_ss_list_tableau;     

select 
count(1)
from
(select 
    base.merchant_uuid,
    base.free_cred_300,
    base.free_cred_100, 
    base.handraiser,
    base.monthly_list,
    base.offer_availed_on,
    base.freecredit, 
    base.acct_owner,
    base.rep_status_logic,
    c.merchant_uuid merchat_uuid2, 
    c.report_week, 
    c.total_impressions,
	c.total_clicks,
	c.total_orders,
	c.adspend,
	c.orders_rev,
	c.roas,
	base.valid_merchant,
	base.is_in_list
from sandbox.np_sl_ss_list_tableau as base
left join 
	   (SELECT 
	          trunc(cast(a.report_date as date), 'iw')+6 report_week, 
		      b.merchant_uuid,
		      sum(impressions) total_impressions,
		      sum(clicks) total_clicks,
		      sum(conversions) total_orders,
		      sum(total_spend_amount) adspend,
		      sum(price_with_discount) orders_rev,
		      cast(orders_rev as float)/NULLIFZERO(adspend) roas
		FROM sandbox.np_ss_performance_met as a
		left join sandbox.pai_deals as b on a.deal_id = b.deal_uuid
		group by 1,2) as c on base.merchant_uuid = c.merchant_uuid
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
      group by 1,2,3,4) as b_three on base.merchant_uuid = b_three.merchant_uuid) as fin;

       



---------------------------OLD Stuff

drop table sandbox.np_sl_merch_agg_date;
create table sandbox.np_sl_merch_agg_date as (
select 
    trunc(a.day_rw, 'iw') + 6 report_week,
    free_cred_300,
    free_cred_100,
    handraiser,
    freecredit,
    monthly_list,
    count(distinct merchant_uuid) distinct_merchants
from 
 (select 
     day_rw 
  from user_groupondw.dim_day 
  where cast(day_rw as date) >= cast('2021-08-01' as date) and cast(day_rw as date) <= current_date
  group by 1
  ) as a 
left join 
  sandbox.np_sl_ss_list_tableau as b on a.day_rw  = b.merchant_active_date
group by 1,2,3,4,5,6) with data
;



select 
    base.*, 
    c.merchant_uuid merchat_uuid2, 
    c.report_week, 
    c.total_impressions,
	c.total_clicks,
	c.total_orders,
	c.adspend,
	c.orders_rev,
	c.roas
    from
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
      group by 1,2,3
      ) as base
left join 
	   (SELECT 
	          trunc(cast(a.report_date as date), 'iw')+6 report_week, 
		      b.merchant_uuid,
		      sum(impressions) total_impressions,
		      sum(clicks) total_clicks,
		      sum(conversions) total_orders,
		      sum(total_spend_amount) adspend,
		      sum(price_with_discount) orders_rev,
		      cast(orders_rev as float)/NULLIFZERO(adspend) roas
		FROM sandbox.np_ss_performance_met as a
		left join sandbox.pai_deals as b on a.deal_id = b.deal_uuid
		group by 1,2) as c on base.merchant_uuid = c.merchant_uuid;
	


	
	(SELECT 
	          trunc(cast(a.report_date as date), 'iw')+6 report_week, 
		      b.merchant_uuid,
		      sum(impressions) total_impressions,
		      sum(clicks) total_clicks,
		      sum(conversions) total_orders,
		      sum(total_spend_amount) adspend,
		      sum(price_with_discount) orders_rev,
		      cast(orders_rev as float)/NULLIFZERO(adspend) roas
		FROM sandbox.np_ss_performance_met as a
		join sandbox.pai_deals as b on a.deal_id = b.deal_uuid
		join np_merch_alllist as c on b.merchant_uuid = c.merchant_uuid
		group by 1,2) as fina
   left join sandbox.np_sl_ss_list_tableau as finb on fina.merchant_uuid = finb.merchant_uuid 
        
	
	

	

select 
    
from np_merch_alllist as base
left join np_merch_catlist as b_one on base.merchant_uuid = b_one.merchant_uuid
left join sandbox.np_merch_free_credits as b_two on base.merchant_uuid = b_two.merchant_uuid
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
      group by 1,2,3) as b_three on base.merchant_uuid = b_three.merchant_uuid
      
      
      
	
	

select * from sandbox.np_sl_merch_agg_date order by report_week asc;


select * from sandbox.np_sl_ss_list_tableau where monthly_list is not null and merchant_active <> 0;

select merchant_active_date, count(1) from sandbox.np_sl_ss_list_tableau 
where free_cred_300 = 1 and merchant_active <> 0
group by 1
order by 1;

select merchant_active_date, count(1) from sandbox.np_sl_ss_list_tableau 
where free_cred_100 = 1 and merchant_active <> 0
group by 1
order by 1;

select min(merchant_active_date) 
from sandbox.np_sl_ss_list_tableau 
where free_cred_100 = 1 
      and merchant_active <> 0;
     
     
select * from sandbox.np_sl_ss_list_tableau where merchant_active_date = '2021-07-20';



select 
*
from 
 (select 
     day_rw 
  from user_groupondw.dim_day 
  where cast(day_rw as date) >= cast('2021-08-01' as date)
  group by 1
  ) as a 
left join 
  sandbox.np_sl_ss_list_tableau as b on a.day_rw  = b.merchant_active_date;
--------------------------------------------------------------------------------------------------------------------------------IMPORTING DATA WITH SPECIFIC LISTS










-------------------------------------OLD



---------Deprecate this
drop table sandbox.np_ss_free_credit;
CREATE MULTISET TABLE sandbox.np_ss_free_credit ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO, 
     MAP = TD_MAP1
     (
      account_name VARCHAR(50) CHARACTER SET UNICODE,
      merchant_uuid VARCHAR(50) CHARACTER SET UNICODE,
      email_id VARCHAR(30) CHARACTER SET UNICODE, 
      sdfc_account_name VARCHAR(30) CHARACTER SET UNICODE, 
      sdfc_account VARCHAR(30) CHARACTER SET UNICODE, 
      merchant_id VARCHAR(50) CHARACTER SET UNICODE, 
      credit_camp VARCHAR(30) CHARACTER SET UNICODE,
      account_owner VARCHAR(30) CHARACTER SET UNICODE, 
      rep_manager VARCHAR(30) CHARACTER SET UNICODE, 
      total_adspend  decimal(11,4),
      broad_imps integer, 
      category_imps integer, 
      search_imps integer, 
      rank_by_ad_rev integer, 
      v_lookups VARCHAR(30) CHARACTER SET UNICODE
      )
NO PRIMARY INDEX;