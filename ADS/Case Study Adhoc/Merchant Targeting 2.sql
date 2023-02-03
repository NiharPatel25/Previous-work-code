------------------------------------------------------------------------------------------------------------------


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


select * from sandbox.np_sl_ss_list_tableau;
      
show table sandbox.np_sl_ss_list_tableau;

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


merchant_id string,
created_campaign int,
created_wallet int,
had_top_ups int

create table grp_gdoop_bizops_db.np_sl_ss_list_tableau (
      merchant_uuid string,
      free_cred_300 int,
      free_cred_100 int,
      handraiser int,
      offer_availed_on string,
      freecredit DECIMAL(10,2),
      monthly_list string,
      merchant_availed_offer int,
      merchant_active int,
      merchant_active_date string,
      total_campaigns_created int,
      total_impressions int,
      total_clicks int,
      total_orders int,
      total_adspend DECIMAL(38,4),
      orders_rev DECIMAL(38,4),
      roas FLOAT,
      number_of_top_ups int,
      amount_of_top_ups DECIMAL(38,2),
      visited_home int,
      visited_home_date string,
      visited_hub int,
      visited_hub_date string,
      visited_set_campaign int,
      visited_set_campaign_date string,
      visited_set_location int,
      visited_set_location_date string,
      visited_set_date_page int,
      visited_set_date_date string,
      visited_set_budget int,
      visited_set_budget_date string,
      visited_add_payment int,
      visited_add_payment_date string,
      visited_review int,
      visited_review_date string,
      visited_final int,
      visited_final_date string,
      drop_off_page string,
      country_code string,
      visited_mc int,
      acct_owner string,
      l2 string,
      current_metal_segment string,
      valid_merchant int,
      rep_status_logic string,
      rep_status string,
      is_in_list int
) stored as orc
tblproperties ("orc.compress"="SNAPPY");





------------------------------------------------------------------------------------------------------------------

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


select account_id, account_name, md_ms, rep_status 
from sandbox.np_ss_sl_list_rep
where account_id is not null
group by 1,2,3,4;



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
left join sandbox.pai_merchants as f on base.merchant_uuid = f.merchant_uuid;



select * from sandbox.np_citrusad_campaigns;
select * from sandbox.np_ads_vs_roas;

------------------------------------------------------------------------------------------------------------------------------------MERCHANT PERFORMING WELL

/*(select account_id, account_name, md_ms, rep_status 
from sandbox.np_ss_sl_list_rep
where account_id is not null
group by 1,2,3,4)*/

select  * from np_mets_target_merch;

drop table np_merch_catlist;
create volatile table np_merch_catlist as
(select merchant_uuid, 
       max(account_name) account_name,
       max(case when list_category = '300' then 1 else 0 end) free_cred_300, 
       max(case when list_category = '100' then 1 else 0 end) free_cred_100, 
       max(case when list_category = 'handraiser' then 1 else 0 end) handraiser
   from  
       sandbox.np_ss_sl_merch_list
   group by 1) with data on commit preserve rows;
  
drop table np_mets_target_merch;
create volatile table np_mets_target_merch as 
(    select a.*, 
           max(COALESCE(live_merchant, was_live_merchant)) merchant_uuid, 
           c.md_ms, 
           c.rep_status rep_status2
     from sandbox.np_ss_sl_mnthly_trgtlist as a 
     left join 
          (select 
             account_id, 
             max(case when is_live = 1 then merchant_uuid end) live_merchant, 
             max(case when was_live = 1 then merchant_uuid end) was_live_merchant 
             from sandbox.pai_merchants
             group by 1) as b on a.account_id = b.account_id
      left join 
           (select account_id, account_name, md_ms, rep_status 
             from sandbox.np_ss_sl_list_rep
             where account_id is not null
             group by 1,2,3,4) as c on a.account_id = c.account_id
      group by 1,2,3,4,6,7) with data on commit preserve rows;



drop table np_final_mets;	
create volatile table np_final_mets as 
(select 
    merch.*, 
    trunc(merch.created_date, 'iw') + 6 week_date,
    a.freecredit, 
    b.free_cred_300,
    b.free_cred_100, 
    b.handraiser,
    case when merch.created_date >= cast('2021-10-06' as date) and merch.created_date <= cast('2021-12-10' as date) and c.merchant_uuid is not null then 1 else 0 end is_mets,
    case when merch.created_date >= cast('2021-10-06' as date) and merch.created_date <= cast('2021-12-10' as date) then c.rep_status2 end rep_status2, 
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
    f.supplier_name,
    pm.l2, 
    g.max_spend_date, 
    case when max_spend_date > current_date - 15 then 'adspend in last 15 days'
              when max_spend_date > current_date - 30 then 'adspend in last 30 days'
              when max_spend_date > current_date - 45 then 'adspend in last 45 days'
              when max_spend_date > current_date - 60 then 'adspend in last 60 days'
              when max_spend_date is not null then 'has no adspend in last 60 days' 
              else 'Never had an adspend' end
              last_adspend_day_category,
    case when (free_cred_300 = 1 or free_cred_100 = 1 or is_mets = 1 or handraiser = 1) then 0 else 1 end didnt_belong_to_any_list,
    case when max_spend_date > current_date - 15 then 'adspend in last 15 days' 
         when num_days_from_first_day <= 10 then 'merchant spent for 10 days'
         when num_days_from_first_day <= 20 then 'merchant spent for 20 days'
         when num_days_from_first_day <= 30 then 'merchant spent for 30 days'
         when num_days_from_first_day > 30 then 'merchant spent for more than 30 days'
         when num_days_from_first_day is null then 'Never had an adspend' end 
         number_of_days_before_leaving,
    pause.status_change_reason,
    case when num_days_from_first_day <= 10 then 'merchant live for 10 days'
         when num_days_from_first_day <= 20 then 'merchant live for 20 days'
         when num_days_from_first_day <= 30 then 'merchant live for 30 days'
         when num_days_from_first_day > 30 then 'merchant live for more than 30 days'
         when num_days_from_first_day is null then 'Never had an adspend' end 
         number_of_days_merchant_is_live,
    cast(merch.created_date as date) - EXTRACT(DAY FROM cast(merch.created_date as date)) + 1 month_created,
    num_days_from_first_day
from
(select 
    a.merchant_id,
    min(a.merchant_name) merchant_name,
    m.account_id,
    min(cast(substr(create_datetime, 1,10) as date)) created_date
from sandbox.np_sponsored_campaign as a
     left join sandbox.pai_merchants as m on a.merchant_id = m.merchant_uuid
     where a.status not in ('DRAFT')
group by 1,3) as merch
left join sandbox.np_merch_free_credits as a on a.merchant_uuid = merch.merchant_id
left join np_merch_catlist as b on merch.merchant_id = b.merchant_uuid
left join 
     np_mets_target_merch as c on merch.merchant_id = c.merchant_uuid
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
    sf.account_id,
    sf.l2,
    sf.l3
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
    ) as f on merch.merchant_id = f.merchant_uuid
left join sandbox.pai_merchants as pm on merch.merchant_id = pm.merchant_uuid
left join 
    (SELECT 
		      b.merchant_uuid,
		      max(cast(report_date as date)) max_spend_date, 
		      min(cast(report_date as date)) min_spend_date, 
		      max_spend_date - min_spend_date num_days_from_first_day
		FROM sandbox.np_ss_performance_met as a
		left join sandbox.pai_deals as b on a.deal_id = b.deal_uuid
		where a.total_spend_amount > 0
		group by 1) as g on merch.merchant_id = g.merchant_uuid
left join 
     (select 
    a.merchant_id, 
    min(cast(substr(create_datetime, 1,10) as date)) created_date, 
    max(cast(substr(update_datetime, 1,10) as date)) update_date,
    max(status) campaign_status, 
    max(status_change_reason) status_change_reason, 
    max(last_live_date) last_live_date, 
    max(is_live) is_live_date
from sandbox.np_sponsored_campaign as a
     left join sandbox.pai_merchants as m on a.merchant_id = m.merchant_uuid
    where a.status not in ('DRAFT')
group by 1) as pause on merch.merchant_id = pause.merchant_id
) with data on commit preserve rows
;








--------------------------------------MERCHANTS DEALS ONBOARDED


select 
     count(distinct opp_id) dis_opp_id, 
     count(distinct case when fin = 1 then opp_id end) dis_opp_id_online, 
     count(opp_id) all_opp_id,
     count( case when fin = 1 then opp_id end) all_opp_id_online,
     count(distinct account_id)
from 
(select 
    op.*, 
    case when snp.opportunity_id is not null then 1 else 0 end fin
from 
sandbox.np_ss_sl_list_opp as op
join np_final_mets as acts on op.account_id = acts.account_id
left join 
(SELECT 
     a.*, 
     b.opportunity_id
FROM sandbox.np_sl_ad_snapshot as a 
left join sandbox.pai_deals as b on a.sku = b.deal_uuid
where a.adspend > 0) as snp on op.opp_id = snp.opportunity_id
) as finale
;


create volatile table np_mets_target_merch as 
(    select a.*, 
           max(COALESCE(live_merchant, was_live_merchant)) merchant_uuid, 
           c.md_ms, 
           c.rep_status rep_status2
     from sandbox.np_ss_sl_mnthly_trgtlist as a 
     left join 
          (select 
             account_id, 
             max(case when is_live = 1 then merchant_uuid end) live_merchant, 
             max(case when was_live = 1 then merchant_uuid end) was_live_merchant 
             from sandbox.pai_merchants
             group by 1) as b on a.account_id = b.account_id
      left join 
           (select account_id, account_name, md_ms, rep_status 
             from sandbox.np_ss_sl_list_rep
             where account_id is not null
             group by 1,2,3,4) as c on a.account_id = c.account_id
      group by 1,2,3,4,6,7) with data on commit preserve rows;


create table sandbox.np_mets_opp_pr as 
(select 
    fin.account_id, 
    fin.merchant_uuid,
    fin.opportunity_id, 
    fin.deal_id, 
    min(fin.created_date) created_date,
    max(on_initial_list) on_initial_list
from 
(select 
    a.account_id, 
    a.merchant_uuid,
    merch.opportunity_id, 
    merch.deal_id, 
    merch.created_date,
    '0' as on_initial_list
from np_mets_target_merch a 
join 
(select 
    a.merchant_id,
    a.deal_id,
    b.opportunity_id,
    min(cast(substr(create_datetime, 1,10) as date)) created_date
from sandbox.np_sponsored_campaign as a
     left join sandbox.pai_deals as b on a.deal_id = b.deal_uuid
     left join sandbox.pai_merchants as m on a.merchant_id = m.merchant_uuid
     where a.status not in ('DRAFT')
group by 1,2,3) as merch on a.merchant_uuid = merch.merchant_id
UNION 
select 
    a.account_id, 
    b.merchant_uuid,
    c.opportunity_id, 
    c.deal_uuid, 
    null as created_date,
    '1' as on_initial_list
from sandbox.np_ss_sl_list_opp as a 
left join np_mets_target_merch as b on a.account_id = b.account_id
left join sandbox.pai_deals as c on substr(a.opp_id, 1,15) = substr(c.opportunity_id,1,15)
) as fin 
group by 1,2,3,4
) with data
;


,
    cast(overall_order_qty as float)/f.overall_deal_views conv_rate

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




SELECT 
    opp.*, 
    case when created_date is not null then 1 else 0 end deal_live, 
    case when d.total_impressions is null then 0 else d.total_impressions end old_imps,
    case when d.total_clicks is null then 0 else d.total_clicks end old_clicks, 
    case when d.total_orders is null then 0 else d.total_orders end old_orders, 
    case when d.total_adspend is null then 0 else d.total_adspend end old_adspend, 
    case when d.orders_rev is null then 0 else d.orders_rev end old_rev, 
    roas,
    case when overall_imps is null then 0 else overall_imps end overall_imps,
    case when overall_deal_views is null then 0 else overall_deal_views end overall_deal_views,
    case when overall_transaction is null then 0 else overall_transaction end overall_transaction,
    case when overall_order_qty is null then 0 else overall_order_qty end overall_order_qty,
    case when overall_sales_revenue is null then 0 else overall_sales_revenue end overall_sales_revenue,
    case when f.total_impressions is null then 0 else f.total_impressions end new_imps,
    case when f.total_clicks is null then 0 else f.total_clicks end  new_clicks,
    case when f.total_orders is null then 0 else f.total_orders end new_orders,
    case when f.total_adspend is null then 0 else f.total_adspend end new_adspend,
    case when f.orders_rev is null then 0 else f.orders_rev end new_rev,
    cast(b.contract_buy_price as float)/NULLIFZERO(b.contract_sell_price) merchant_margin,
    (b.contract_sell_price - b.contract_buy_price)/NULLIFZERO(b.contract_sell_price) groupon_margin, 
    case when overall_sales_revenue * merchant_margin is null then 0 else overall_sales_revenue * merchant_margin end merch_overall_deal_revenue,
    case when old_rev * merchant_margin is null then 0 else old_rev * merchant_margin end SL_margin,
    case when overall_sales_revenue * groupon_margin is null then 0 else overall_sales_revenue * groupon_margin end groupon_overall_deal_revenue,
    case when old_rev * groupon_margin is null then 0 else old_rev * groupon_margin end SL_groupon_margin,
    b.contract_sell_price
from sandbox.np_mets_opp_pr as opp
left join 
	   (SELECT 
		      b.deal_uuid,
	          b.merchant_uuid,
		      sum(impressions) total_impressions,
		      sum(clicks) total_clicks,
		      sum(conversions) total_orders,
		      sum(total_spend_amount) total_adspend,
		      sum(price_with_discount) orders_rev,
		      cast(orders_rev as float)/NULLIFZERO(total_adspend) roas
		FROM sandbox.np_ss_performance_met as a
		left join sandbox.pai_deals as b on a.deal_id = b.deal_uuid
		group by 1,2) as d on opp.deal_id = d.deal_uuid
left join 
        (select deal_id, 
                sum(total_imps) overall_imps,
                sum(deal_views) overall_deal_views, 
                sum(all_transaction) overall_transaction, 
                sum(parent_orders_qty) overall_order_qty,
                sum(nor_usd) groupon_sales_profit,
                sum(nob_usd) overall_sales_revenue
         from sandbox.np_sixty_day_pre_sl
         group by 1) as e on opp.deal_id = e.deal_id
left join 
         (select 
             sku deal_uuid,
             sum(impressions) total_impressions,
             sum(clicks) total_clicks,
             sum(conversions) total_orders,
             sum(unitsales) unit_sales,
             sum(sales_revenue) orders_rev,
             sum(adspend) total_adspend
         from sandbox.np_sl_ad_snapshot
         group by 1
         ) as f on opp.deal_id = f.deal_uuid
left join 
         np_temp_avg_price as b on b.product_uuid = opp.deal_id
;



select deal_id, 
                sum(total_imps) overall_imps,
                sum(deal_views) overall_deal_views, 
                sum(all_transaction) overall_transaction, 
                sum(parent_orders_qty) overall_order_qty
         from sandbox.np_sl_deals_imps_trans
         where cast(report_month as date) = cast('2021-10-01' as date)
         group by 1

create table sandbox.np_sixty_day_pre_sl as (
select 
    imp.*, 
    all_transaction,
    units, 
    parent_orders_qty,
    deal_views,
    nob_usd, 
    nor_usd
from 
(select 
      a.deal_id, 
      sum(total_impressions) total_imps,
      min(a.report_date) min_report_date
     from user_edwprod.agg_gbl_impressions_deal as a
       join (select deal_id, created_date from sandbox.np_mets_opp_pr where created_date is not null) as c on a.deal_id = c.deal_id
     where
          a.report_date >= created_date - 60  and a.report_date <= created_date
     group by 1
) as imp 
left join
(select 
       a.deal_id,
       sum(transactions) all_transaction,
       sum(transactions_qty) units, 
       sum(parent_orders_qty) parent_orders_qty,
       sum(deal_views) deal_views,
       sum(nob_usd) nob_usd, 
       sum(nor_usd) nor_usd
       from user_edwprod.agg_gbl_traffic_fin_deal as a
       join (select deal_id, created_date from sandbox.np_mets_opp_pr where created_date is not null) as c on a.deal_id = c.deal_id
       where
          a.report_date >= created_date - 60  and a.report_date <= created_date
       group by
          1
) fin on imp.deal_id = fin.deal_id
) with data
;

select * from user_edwprod.active;
