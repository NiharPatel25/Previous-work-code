select * from sandbox.citrusad_team_wallet;


select * from sandbox.np_citrusad_wallet_balance_history;



drop table sandbox.np_citrusad_wallet_balance_history;
CREATE MULTISET TABLE sandbox.np_cits_walbal_his ,NO FALLBACK ,
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

select count(distinct deal_id) from sandbox.np_ss_performance_met;
select count(distinct product) from sandbox.np_citrusad_campaigns;



---------------------------------------------------------------------------MAIN QUERY


create volatile table np_sl_merch_check as     
(select 
         deal_id, 
         a.merchant_id, 
         b.merchant_uuid,
         COALESCE(a.merchant_id, b.merchant_uuid) merchant_uuid_coal
     from sandbox.np_ss_performance_met as a 
     left join sandbox.pai_deals as b on a.deal_id = b.deal_uuid
     where impressions > 0
) with data on commit preserve rows;


create volatile table np_sl_max_impression_date as     
(select 
         COALESCE(a.merchant_id, b.merchant_uuid) merchant_uuid,
         max(cast(report_date as date)) max_impression_date, 
         current_date - max_impression_date number_of_days_past_imps
     from sandbox.np_ss_performance_met as a 
     left join sandbox.pai_deals as b on a.deal_id = b.deal_uuid
     where impressions > 0
     group by 1
) with data on commit preserve rows;

create volatile table np_sl_max_spend_date as     
(select 
         COALESCE(a.merchant_id, b.merchant_uuid) merchant_uuid,
         max(cast(report_date as date)) max_spend_date, 
         current_date - max_spend_date number_of_days_past_spend
     from sandbox.np_ss_performance_met as a 
     left join sandbox.pai_deals as b on a.deal_id = b.deal_uuid
     where total_spend_amount > 0
     group by 1
) with data on commit preserve rows;

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

select * from sandbox.td_est_maxcpc_by_dealid;
select * from np_temp_mc_int_merch;
create multiset volatile table np_temp_mc_int_merch as (
select 
   a.merchantid, 
   a.first_date_landing_sl, 
   a.drop_off_page,
   case when b.merchant_id is not null then 1 else 0 end merchant_created_a_campaign, 
   case when c.merchant_id is not null then 1 else 0 end merchant_has_draft_campaign,
   case when d.merchant_uuid is not null then 1 else 0 end merchant_has_campaign,
   b.created_date min_active_date, 
   c.min_draft_created_date, 
   c.max_draft_created_date
   --e.merchant_name, 
   --e.l2, e.l3
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
from  sandbox.np_ss_sl_interaction_agg group by 1) as a 
left join 
    (select merchant_id, min(cast(substr(create_datetime, 1,10) as date)) created_date
            from sandbox.np_sponsored_campaign 
            where status not in ('DRAFT')
            group by 1) as b on a.merchantid = b.merchant_id
left join 
    (select merchant_id, min(cast(substr(create_datetime, 1,10) as date)) min_draft_created_date, max(cast(substr(create_datetime, 1,10) as date)) max_draft_created_date
            from sandbox.np_sponsored_campaign 
            where status = 'DRAFT'
            group by 1) as c on a.merchantid = c.merchant_id
left join 
    ( select pd.merchant_uuid 
               from sandbox.np_citrusad_campaigns as cit
               left join sandbox.pai_deals as pd on cit.product = pd.deal_uuid
               group by 1) as d on a.merchantid = d.merchant_uuid
left join sandbox.pai_merchants as e on a.merchantid = e.merchant_uuid
) with data on commit preserve rows;

drop table sandbox.ss_merch_marketing_email;
create multiset table sandbox.ss_merch_marketing_email as (
select 
     pmerch.merchant_uuid, 
     pmerch.is_live, 
     pmerch.was_live,
     pmerch.l1 l1_category, pmerch.l2 l2_category, pmerch.country_code,
     case when fc.merchant_uuid is not null then 1 else 0 end free_credit_assigned, 
     fc.free_credit_offer_valid_upto, fc.free_credit_offer_sent_on,
     case when sl_data.merchantid is not null then 1 else 0 end landed_in_mc, 
     sl_data.first_date_landing_sl min_date_landed_in_mc, 
     sl_data.drop_off_page last_drop_off_page,
     sl_data.merchant_created_a_campaign created_a_campaign, 
     sl_data.merchant_has_draft_campaign has_draft_campaign,
     sl_data.merchant_has_campaign has_nondraft_campaign,
     sl_data.min_active_date min_nondraft_campaign_created_date, 
     sl_data.min_draft_created_date min_draft_campaign_created_date, 
     sl_data.max_draft_created_date max_draft_campaign_created_date,
     sl_data.max_impression_date, 
     sl_data.number_of_days_past_imps days_since_last_impression,  
	 sl_data.max_spend_date max_adspend_date,
	 sl_data.number_of_days_past_spend days_since_last_adspend, 
	 sl_data.active_wallet_count, 
	 sl_data.archived_wallet_count,
	 cast(sl_data.active_wallet_balance as decimal(10,2)) active_wallet_balance,
	 cast(sl_data.archived_balance as decimal(10,2)) archived_wallet_balance,
	 sl_data.wallet_data_available,
	 sl_data.total_imps total_impressions, 
	 sl_data.total_clicks, 
	 sl_data.total_ords total_orders, 
	 cast(sl_data.adspend as decimal(10,2)) total_adspend, 
	 cast(sl_data.orders_rev as decimal(10,2)) total_order_revenue, 
	 cast(sl_data.ss_ctr*100 as decimal(10,2)) ss_ctr, 
	 cast(sl_data.ss_cnv*100 as decimal(10,2)) ss_cnv, 
	 cast(sl_data.roas as decimal(12,4)) roas
from 
		sandbox.pai_merchants as pmerch 
		left join 
		(select 
		   merch.*, 
		   a.max_impression_date, 
		   a.number_of_days_past_imps,  
		   case when a.number_of_days_past_imps <= 30 then 1 else 0 end impression_in_last_30_days,
		   d.max_spend_date,
		   d.number_of_days_past_spend, 
		   case when d.number_of_days_past_spend <= 30 then 1 else 0 end adspend_in_last_30_days,
		   b.active_wallet_count, 
		   b.archived_wallet_count,
		   b.active_wallet_balance,
		   b.archived_balance,
		   case when b.merchant_id is not null then 1 else 0 end wallet_data_available,
		   c.total_imps, 
		   c.total_clicks, 
		   c.total_ords, 
		   c.adspend, 
		   c.orders_rev, 
		   c.ss_ctr, 
		   c.ss_cnv, 
		   c.roas,
		   case when c.roas > 1 then 1 else 0 end merchant_with_roas_greater_1
		from 
		   np_temp_mc_int_merch as merch
		left join np_sl_max_impression_date as a on merch.merchantid = a.merchant_uuid
		left join 
		(select merchant_id, 
		       sum(case when is_archived = 0 then 1 else 0 end) active_wallet_count,
		       sum(is_archived) archived_wallet_count,  
		       sum(case when is_archived = 0 then balance end) active_wallet_balance,
		       sum(case when is_archived = 1 then balance end) archived_balance
		  from sandbox.citrusad_team_wallet
		  group by 1
		  ) as b on a.merchant_uuid = b.merchant_id
		left join 
		   np_ss_merch_perf as c on a.merchant_uuid = c.merchant_uuid
		left join np_sl_max_spend_date as d on a.merchant_uuid = d.merchant_uuid) as sl_data on pmerch.merchant_uuid = sl_data.merchantid
		left join (select merchant_uuid, 
       					  max(freecredit) freecredit, 
       					  max(offer_availed_on) offer_availed_on, 
       					  max(offer_expires_on) offer_expires_on,
       					  max(offer_claimed_on) offer_claimed_on,
     					  max(offer_sent_on) free_credit_offer_sent_on, 
     					  max(offer_valid_upto) free_credit_offer_valid_upto
					from sandbox.np_merch_free_credits 
					group by 1) as fc on pmerch.merchant_uuid = fc.merchant_uuid
		where pmerch.country_code in ( 'US', 'CA')
) with data;

grant select on sandbox.ss_merch_marketing_email to public;

select * from sandbox.ss_merch_marketing_email;


select * from sandbox.pai_merchants where account_id = '0013c00001uuWiNAAU';

select * from sandbox.citrusad_team_wallet where merchant_id  = '615493f7-8ea7-471c-a37c-4864a503c290';

select a.merchantid, b.l1, b.country_code from sandbox.ss_merch_marketing_email as a 
left join sandbox.pai_merchants as b on a.merchantid = b.merchant_uuid;

select 
  fin.merchant_uuid, 
  fin.merchant_name, 
  fin.l1, fin.l2, fin.l3, 
  fin2.l1
from 
(select * from sandbox.pai_merchants where country_code = 'US' and l1 is null) as fin 
left join 
(select b.merchant_uuid, max(grt_l1_cat_description) l1 
from user_edwprod.dim_gbl_deal_lob as a 
left join sandbox.pai_deals as b on a.deal_id = b.deal_uuid
group by 1) as fin2 on fin.merchant_uuid = fin2.merchant_uuid



select 
   max(length(merchant_uuid)), 
   max(length(l1)), max(length(l2)), 
   max(length(country_code)), 
   max(length(merchantid)), 
   max(length(first_date_landing_sl)), 
   max(length(drop_off_page)),
   max(length(max_spend_date))
from 
sandbox.ss_merch_marketing_email;


-----where is archived = 0 - merhant performance 


select * from sandbox.citrusad_team_wallet;
---------------MERCHANT LANDED IN MC 

select * from np_temp_mc_int_merch;

create multiset volatile table np_temp_mc_int_merch as (
select 
   a.merchantid, 
   a.first_date_landing_sl, 
   a.drop_off_page,
   case when b.merchant_id is not null then 1 else 0 end merchant_created_a_campaign, 
   case when c.merchant_id is not null then 1 else 0 end merchant_has_draft_campaign,
   case when d.merchant_uuid is not null then 1 else 0 end merchant_has_campaign,
   b.created_date min_active_date, 
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
left join 
    ( select pd.merchant_uuid 
               from sandbox.np_citrusad_campaigns as cit
               left join sandbox.pai_deals as pd on cit.product = pd.deal_uuid
               group by 1) as d on a.merchantid = d.merchant_uuid
) with data on commit preserve rows;




--------------LAST DRAFT CAMPAIGN CREATED


select
    a.merchant_id, 
    a.max_created_date, 
    current_date - cast(max_created_date as date) number_of_days_since_last_draft, 
    case when b.merchant_id is not null then 1 else 0 end merchant_already_has_a_campaign
from 
(select merchant_id, 
        max(cast(substr(create_datetime, 1,10) as date)) max_created_date
       from sandbox.np_sponsored_campaign 
       where status = 'DRAFT'
       group by 1) as a 
left join 
    (select merchant_id
            from sandbox.np_sponsored_campaign 
            where status not in ('DRAFT')
            group by 1) as b on a.merchant_id = b.merchant_id;


---------------GOOD PERFORMING MERCHANTS

select max(report_date) from sandbox.np_ss_performance_met;

drop table np_sl_max_impression_date;
create volatile table np_sl_max_impression_date as     
(select 
         COALESCE(a.merchant_id, b.merchant_uuid) merchant_uuid,
         max(cast(report_date as date)) max_impression_date, 
         current_date - max_impression_date number_of_days_past_imps
     from sandbox.np_ss_performance_met as a 
     left join sandbox.pai_deals as b on a.deal_id = b.deal_uuid
     where impressions > 0
     group by 1
) with data on commit preserve rows;

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


drop table np_ss_merch_xyz;
create volatile table np_ss_merch_xyz as 
(select 
   a.*, 
   b.total_imps, 
   b.total_clicks, 
   b.total_ords, 
   b.adspend, 
   b.orders_rev, 
   b.ss_ctr, 
   b.ss_cnv, 
   b.roas, 
   case when b.roas > 1 then 1 else 0 end merchant_with_roas_greater_1
from 
   np_sl_max_impression_date as a 
left join 
   np_ss_merch_perf as b on a.merchant_uuid = b.merchant_uuid
) with data on commit preserve rows;



select * from np_ss_merch_xyz where number_of_days_past_imps < 30;



--------------------TRACKING PERFORMANCE AFTER LAUNCHING 
create multiset volatile table np_temp_mc_int_merch as (
select 
   a.merchantid, 
   a.first_date_landing_sl, 
   a.last_date_landing_sl,
   case when b.merchant_id is not null then 1 else 0 end merchant_created_a_campaign, 
   case when c.merchant_id is not null then 1 else 0 end merchant_has_draft_campaign,
   case when d.merchant_uuid is not null then 1 else 0 end merchant_has_campaign,
   b.min_created_date min_active_date, 
   c.min_draft_created_date, 
   c.max_draft_created_date
   --e.merchant_name, 
   --e.l2, e.l3
from 
(select 
     merchantid, 
     cast(min(eventdate) as date) first_date_landing_sl, 
     cast(max(eventdate) as date) last_date_landing_sl
from  sandbox.np_ss_sl_interaction_agg group by 1) as a 
left join 
    (select merchant_id, min(cast(substr(create_datetime, 1,10) as date)) min_created_date
            from sandbox.np_sponsored_campaign 
            where status not in ('DRAFT')
            group by 1) as b on a.merchantid = b.merchant_id
left join 
    (select merchant_id, 
    		min(cast(substr(create_datetime, 1,10) as date)) min_draft_created_date, 
    		max(cast(substr(create_datetime, 1,10) as date)) max_draft_created_date
            from sandbox.np_sponsored_campaign 
            where status = 'DRAFT'
            group by 1) as c on a.merchantid = c.merchant_id
left join 
    ( select pd.merchant_uuid 
               from sandbox.np_citrusad_campaigns as cit
               left join sandbox.pai_deals as pd on cit.product = pd.deal_uuid
               group by 1) as d on a.merchantid = d.merchant_uuid
left join sandbox.pai_merchants as e on a.merchantid = e.merchant_uuid
) with data on commit preserve rows;


SELECT
	merchant_uuid,
	sf_account_id,
	emailname,
	journeyname,
	cast(substr(sentdate, 1,10) as date) sentdate,
	trunc(cast(substr(sentdate, 1,10) as date) , 'iw') + 6 sentdate_week,
	delivered,
	case when length(substr(firstopendate,1,10)) >= 10 then cast(substr(firstopendate,1,10) as date) end  email_open_date,
	case when email_open_date is not null then 1 else 0 end email_opened,
	last_date_landing_sl,
	case when last_date_landing_sl >= cast(substr(sentdate, 1,10) as date) then 1 else 0 end landed_in_ss_after_email, 
	case when max_draft_created_date >= cast(substr(sentdate, 1,10) as date) then 1 else 0 end created_campaign_after_email
FROM
	sandbox.SFMC_EmailEngagement as a 
	left join np_temp_mc_int_merch as b on a.merchant_uuid = b.merchantid
	where 
	journeyname 
	in 
	('MM_NA_SponsoredCampaigns_Dormant1',
	 'MM_NA_SponsoredCampaigns_Dormant2',
	 'MM_Sponsored_DormantMerchants',
	 'MM_NA_SponsoredCampaigns_ReEngagement',
	 'MM_Sponsored_ReengagementSeries',
	 'MM_Retention_Milestones',
	 'MM_Retention_NotSellingUnits',
	 'MM_Retention_NoUnitsSoldIn7After30Days',
	 'MM_Sponsored_ClickedNavinMC',
	 'MM_Sponsored_DropOffSeries',
	 'MM_NA_SponsoredCampaigns_DropOff',
	 'MM_Onboarding_OnboardingSeries',
	 'MM_NA_SponsoredCampaigns_ClickedNavInMC',
	 'MM_ONBOARDING_EducationalSeries')
;



MM_NA_SponsoredCampaigns_Dormant1
MM_NA_SponsoredCampaigns_Dormant1
MM_NA_SponsoredCampaigns_Dormant2
MM_NA_SponsoredCampaigns_ReEngagement
MM_NA_SponsoredCampaigns_ReEngagement
MM_Retention_Milestones
MM_Retention_NotSellingUnits

MM_Retention_Milestones
MM_Retention_NotSellingUnits
 

MM_Retention_Milestones
MM_Retention_NotSellingUnits
 
MM_Retention_NotSellingUnits
MM_Retention_NotSellingUnits
MM_Retention_NoUnitsSoldIn7After30Days
MM_NA_SponsoredCampaigns_ClickedNavInMC
MM_NA_SponsoredCampaigns_DropOff
MM_NA_SponsoredCampaigns_DropOff
MM_NA_SponsoredCampaigns_DropOff
MM_ONBOARDING_EducationalSeries






MM_Retention_NotSellingUnits
MM_Retention_NotSellingUnits
MM_Retention_NoUnitsSoldIn7After30Days

MM_NA_SponsoredCampaigns_ClickedNavInMC
MM_NA_SponsoredCampaigns_DropOff
MM_NA_SponsoredCampaigns_DropOff
MM_NA_SponsoredCampaigns_DropOff





select * from sandbox.citrusad_team_wallet where merchant_id = '4e5342d8-dcd6-46da-80e5-ef9150452f5b';

select  from sandbox.np_slad_wlt_blc_htry 
where cast(citrusad_import_date as date) = current_date - 3;

select 
    a.*, 
    ROW_NUMBER() over(partition by merchant_id order by cast( substr(create_datetime, 1,10) as date) desc) rank_of_wallets
from sandbox.citrusad_team_wallet as a 
;

select * from sandbox.citrusad_team_wallet;
select * from sandbox.np_slad_wlt_blc_htry where wallet_name <> 'Default Wallet' order by wallet_id, citrusad_import_date ;



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
