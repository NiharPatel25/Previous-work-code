-------------------------------------------------------------------------------MOBILE APP QUERY


select 
    page_app,
    page_type, 
    mc_page, 
    widget_name
from sandbox.pai_merchant_center_visits
where event in ('genericClick','genericClickAll')
    and eventdate >= current_date - 7
    and lower(platform) = 'mobile'
    group by 1,2,3,4;



select 
    page_app,
    page_type, 
    mc_page, 
    event, 
    event_destination,
    widget_name
from sandbox.pai_merchant_center_visits
where event in ('genericClick','genericClickAll','merchantPageView','dealImpression', 'nonDealImpression')
    and eventdate >= current_date - 7
    and lower(platform) = 'mobile'
    group by 1,2,3,4,5,6
;

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
group by consumerid, merchantid, consumeridsource, rawpagetype, eventdate) as fin;


select 
  page_app, 
  page_path,
  page_type,
  platform,
  event,
  event_destination
from sandbox.pai_merchant_center_visits
where eventdate >= cast('2021-01-01' as date)
group by 1,2,3,4,5,6
;

--------------------------------------------------------------------PAGEVIEW EVENTS
select distinct page_app, page_type from sandbox.np_mc_mobile_pg_visits where page_type  in ('payments', 'payment_history');

when page_app ='sponsored-campaign-itier' then 'Documented - Sponsored Listing'
when page_app ='merchant-advisor-itier' then 'Documented - Virtual Advisor'
when page_type in ('embedded-webview', 'payments','customer_feedback') then 'Documented - Dashboard'
when page_type in ('settings', 'tour_app') then 'Documented - Navigation'
when page_type in ('topics', 'payment_details' , 'resources', 'email', 'g_dashboard', 'flutter_g_dashboard') then 'Documented'
when page_type in ('g_dashboard', 'homepage') then 'Other - Home'
when page_type in ('deal_list') then 'Other - Campaigns'
when page_type in ('advisor_main','Virtual Advisor') then 'Other - Virtual Advisor'
when page_type in ('vouchers', 'voucher_list') then 'Other - Voucher List'
when page_type in ('feedback') then 'Other - Feedback' --- removed 'customer_feedback' and added to dashboard instead
when page_type in ('impact_report', 'demographics') then 'Other - Demographics'
when page_type in ('payment_history') then 'Payments' --removed 'payments' and added to dashboard instead
when page_type in ('topics', 'Landing', 'landing') then 'Other - Support'
when page_type in ('accountHome') then 'Admin' ---removed 'settings' and added to navigation instead
when page_type in ('mdm_product-edit','mdm_main','mdm_end-date','mdm_description','mdm_photos'
			         ,'mdm_fineprint','mdm_product-new','mdm_locations','mdm_start-date','mdm_highlights'
			         , 'deal_edit','end_date_edit','start_date_edit','description_edit', 'fineprint'
			         , 'highlight_edit','location_edit','add-option', 'option', 'options_edit', 'details'
			         , 'photos', 'gallery', 'main', 'stock', 'preview', 'price_verification','text_edit') then 'Other - Editing Campaign'
when page_type in ('redemption-instruction','redemption-locations','bank-tax-info', 'additional-info', 'nutshell-about-business'
			         , 'choose-dates','lead_success_user_signup','options','images', 'contract','congratulations', 'dashboard'
			         , 'campaigns', 'MarketingForm', 'event-general-info', 'nutshell-deal-options', 'event-options'
			         , 'merchandise-campaign', 'file-upload', 'fineprint', 'nutshell-deal-highlight', 'launch-date', 'predraft'
			         , 'draft-bank-proxy', 'draft-bank-info', 'draft-ein', 'draft-ssn', 'draft-description', 'draft-highlight', 'draft-contract'
			         , 'draft-finish', 'draft-fine-print', 'intro', 'draft-launch-date', 'draft-option-list', 'draft-preview'
			         , 'draft-ranking', 'draft-progress', 'metro-intro-started') 
     then 'Other - Building Campaign'
when page_type in ('redemption_choice','voucher', 'success','search_by_scan', 'search_by_code','search_by_name','results', 'sales_cap', 'result') 
     then 'Other - Redeeming Voucher'
     else 'Other' end page_type
 
select 
   *
from 
   sandbox.pai_merchant_center_visits as a
  where page_type like '%live%';

drop table sandbox.np_mc_mobile_pg_visits;
create multiset table sandbox.np_mc_mobile_pg_visits as (
select 
    cast(dw.week_end as date) report_wk,
	pm.country_code,
	page_app,
	case 
		----when a.page_url like '%https://www.groupon.com/merchant/center/insights%' then 'insights_roi'
		when a.page_type is null then 'Null' else a.page_type end page_type,
	case
    when page_app ='sponsored-campaign-itier' then 'Documented - Sponsored Listing'
    when page_app ='merchant-advisor-itier' then 'Documented - Virtual Advisor'
    when page_type in ('embedded-webview', 'payments','customer_feedback') then 'Documented - Dashboard'
    when page_type in ('settings', 'tour_app') then 'Documented - Navigation'
    when page_type in ('topics', 'payment_details' , 'resources', 'email', 'g_dashboard', 'flutter_g_dashboard') then 'Documented'
    when page_type in ('g_dashboard', 'homepage') then 'Other - Home'
    when page_type in ('deal_list') then 'Other - Campaigns'
    when page_type in ('advisor_main','Virtual Advisor') then 'Other - Virtual Advisor'
    when page_type in ('vouchers', 'voucher_list') then 'Other - Voucher List'
    when page_type in ('feedback') then 'Other - Feedback' --- removed 'customer_feedback' and added to dashboard instead
    when page_type in ('impact_report', 'demographics') then 'Other - Demographics'
    when page_type in ('payment_history') then 'Payments' --removed 'payments' and added to dashboard instead
    when page_type in ('topics', 'Landing', 'landing') then 'Other - Support'
    when page_type in ('accountHome') then 'Admin' ---removed 'settings' and added to navigation instead
    when page_type in ('mdm_product-edit','mdm_main','mdm_end-date','mdm_description','mdm_photos'
			         ,'mdm_fineprint','mdm_product-new','mdm_locations','mdm_start-date','mdm_highlights'
			         , 'deal_edit','end_date_edit','start_date_edit','description_edit', 'fineprint'
			         , 'highlight_edit','location_edit','add-option', 'option', 'options_edit', 'details'
			         , 'photos', 'gallery', 'main', 'stock', 'preview', 'price_verification','text_edit') then 'Other - Editing Campaign'
    when page_type in ('redemption-instruction','redemption-locations','bank-tax-info', 'additional-info', 'nutshell-about-business'
			         , 'choose-dates','lead_success_user_signup','options','images', 'contract','congratulations', 'dashboard'
			         , 'campaigns', 'MarketingForm', 'event-general-info', 'nutshell-deal-options', 'event-options'
			         , 'merchandise-campaign', 'file-upload', 'fineprint', 'nutshell-deal-highlight', 'launch-date', 'predraft'
			         , 'draft-bank-proxy', 'draft-bank-info', 'draft-ein', 'draft-ssn', 'draft-description', 'draft-highlight', 'draft-contract'
			         , 'draft-finish', 'draft-fine-print', 'intro', 'draft-launch-date', 'draft-option-list', 'draft-preview'
			         , 'draft-ranking', 'draft-progress', 'metro-intro-started') 
         then 'Other - Building Campaign'
     when page_type in ('redemption_choice','voucher', 'success','search_by_scan', 'search_by_code','search_by_name','results', 'sales_cap', 'result') 
         then 'Other - Redeeming Voucher'
     else 'Other' end document_track_filter,
	a.mc_page,
    count(distinct concat(a.merchant_uuid,a.eventdate)) uniq_merch_login_daily
from sandbox.pai_merchant_center_visits a
join sandbox.pai_merchants pm
	on a.merchant_uuid = pm.merchant_uuid
join user_groupondw.dim_day dd
	on a.eventdate = dd.day_rw
join user_groupondw.dim_week dw
	on dd.week_key = dw.week_key
where eventdate >= cast('2021-01-01' as date)
and event = 'merchantPageView'
and coalesce(a.merchant_uuid,'')<>''
and lower(a.platform) = 'mobile'
and pm.l1='Local'
group by 1,2,3,4,5,6
) with data;

('genericClick','genericClickAll','merchantPageView','dealImpression', 'nonDealImpression')

select 
   page_type, 
   page_url, 
   page_app, 
   event
from sandbox.pai_merchant_center_visits 
where event in ('genericClick','genericClickAll','merchantPageView','dealImpression', 'nonDealImpression')
and page_type like '%g_dashboard_page%';



drop table sandbox.np_mc_mobile_events_visits;
create multiset table sandbox.np_mc_mobile_events_visits as (
select 
    cast(dw.week_end as date) report_wk,
	pm.country_code,
	page_app,
	case 
		----when a.page_url like '%https://www.groupon.com/merchant/center/insights%' then 'insights_roi'
		when a.page_type is null then 'Null' else a.page_type end page_type,
    case
    when page_app ='sponsored-campaign-itier' then 'Documented - Sponsored Listing'
    when page_app ='merchant-advisor-itier' then 'Documented - Virtual Advisor'
    when page_type in ('embedded-webview', 'payments','customer_feedback') then 'Documented - Dashboard'
    when page_type in ('settings', 'tour_app') then 'Documented - Navigation'
    when page_type in ('topics', 'payment_details' , 'resources', 'email', 'g_dashboard', 'flutter_g_dashboard') then 'Documented'
    when page_type in ('g_dashboard', 'homepage') then 'Other - Home'
    when page_type in ('deal_list') then 'Other - Campaigns'
    when page_type in ('advisor_main','Virtual Advisor') then 'Other - Virtual Advisor'
    when page_type in ('vouchers', 'voucher_list') then 'Other - Voucher List'
    when page_type in ('feedback') then 'Other - Feedback' --- removed 'customer_feedback' and added to dashboard instead
    when page_type in ('impact_report', 'demographics') then 'Other - Demographics'
    when page_type in ('payment_history') then 'Payments' --removed 'payments' and added to dashboard instead
    when page_type in ('topics', 'Landing', 'landing') then 'Other - Support'
    when page_type in ('accountHome') then 'Admin' ---removed 'settings' and added to navigation instead
    when page_type in ('mdm_product-edit','mdm_main','mdm_end-date','mdm_description','mdm_photos'
			         ,'mdm_fineprint','mdm_product-new','mdm_locations','mdm_start-date','mdm_highlights'
			         , 'deal_edit','end_date_edit','start_date_edit','description_edit', 'fineprint'
			         , 'highlight_edit','location_edit','add-option', 'option', 'options_edit', 'details'
			         , 'photos', 'gallery', 'main', 'stock', 'preview', 'price_verification','text_edit') then 'Other - Editing Campaign'
    when page_type in ('redemption-instruction','redemption-locations','bank-tax-info', 'additional-info', 'nutshell-about-business'
			         , 'choose-dates','lead_success_user_signup','options','images', 'contract','congratulations', 'dashboard'
			         , 'campaigns', 'MarketingForm', 'event-general-info', 'nutshell-deal-options', 'event-options'
			         , 'merchandise-campaign', 'file-upload', 'fineprint', 'nutshell-deal-highlight', 'launch-date', 'predraft'
			         , 'draft-bank-proxy', 'draft-bank-info', 'draft-ein', 'draft-ssn', 'draft-description', 'draft-highlight', 'draft-contract'
			         , 'draft-finish', 'draft-fine-print', 'intro', 'draft-launch-date', 'draft-option-list', 'draft-preview'
			         , 'draft-ranking', 'draft-progress', 'metro-intro-started') 
         then 'Other - Building Campaign'
     when page_type in ('redemption_choice','voucher', 'success','search_by_scan', 'search_by_code','search_by_name','results', 'sales_cap', 'result') 
         then 'Other - Redeeming Voucher'
     else 'Other' end document_track_filter,
	widget_name,
    count(distinct concat(a.merchant_uuid,a.eventdate)) uniq_merch_login_daily
from sandbox.pai_merchant_center_visits a
join sandbox.pai_merchants pm
	on a.merchant_uuid = pm.merchant_uuid
join user_groupondw.dim_day dd
	on a.eventdate = dd.day_rw
join user_groupondw.dim_week dw
	on dd.week_key = dw.week_key
where trunc(cast(eventdate as date), 'iw')+6 <= current_date and trunc(cast(eventdate as date), 'iw')+6 >= current_date - 120
and event_destination = 'genericClick'
and coalesce(a.merchant_uuid,'')<>''
and lower(a.platform) = 'mobile'
and pm.l1='Local'
group by 1,2,3,4,5,6
) with data;


grant select on sandbox.np_mc_mobile_events_visits to public;
grant select on sandbox.np_mc_mobile_pg_visits to public;



--and coalesce(page_type,'')<>''

    --and lower(widget_name) like '%promo%'

select count(distinct merchant_uuid) from sb_merchant_experience.merchant_insights_answers;
-----------------------------------------------------MC JAYS PAGE VISITS DASHBOARD
drop table jrg_mc_visitss;

drop table sandbox.jrg_mc_visitss;
create multiset table sandbox.jrg_mc_visitss as (
select 
	a.merchant_uuid
 	, pm.country_code
 	, pm.acct_owner
 	, case when lower(pm.current_metal_segment) in ('silver', 'gold', 'platinum') then 's+' else 'b-' end metal
	, case when a.page_url like '%https://www.groupon.com/merchant/center/insights%' then 'insights_roi' else a.page_app end page_app 
	, a.page_type
	, a.mc_page
	, a.platform
	, a.eventdate
	, cast(dw.week_end as date) report_wk
	, trunc(a.eventdate,'RM') report_mth
	, trunc(a.eventdate, 'Q') report_qtr
from sandbox.pai_merchant_center_visits a
join sandbox.pai_merchants pm
	on a.merchant_uuid = pm.merchant_uuid
join user_groupondw.dim_day dd
	on a.eventdate = dd.day_rw
join user_groupondw.dim_week dw
	on dd.week_key = dw.week_key
where event in ('genericClick','genericClickAll','merchantPageView','dealImpression', 'nonDealImpression')
and cast(eventdate as date) >= current_date - 120
and coalesce(a.merchant_uuid,'')<>''
---and coalesce(page_type,'')<>''
and pm.l1='Local'
) with data primary index(merchant_uuid, eventdate);

collect stats column(merchant_uuid, eventdate) on sandbox.jrg_mc_visitss;


create multiset volatile table jrg_mercs as (
 select
 	cast(dw.week_end as date) report_wk
 	, pm.merchant_uuid
 	, count(distinct ad.DEAL_UUID) deals_live
from user_groupondw.active_deals  ad
join sandbox.pai_deals pd
	on pd.deal_uuid = ad.deal_uuid
join sandbox.pai_merchants pm
	on pm.merchant_uuid = pd.merchant_uuid
join user_groupondw.dim_day dd
	on ad.load_date = dd.day_rw
join user_groupondw.dim_week dw
	on dd.week_key = dw.week_key
where ad.load_date >= '2021-11-01'
and pm.l1='Local'
group by 1,2
)  with data primary index(merchant_uuid, report_wk) on commit preserve rows;

collect stats column(merchant_uuid, report_wk) on jrg_mercs;

create multiset volatile table jrg_units as (
select
	 fgt.merchant_uuid
	, trunc(v.redeemed_at) redemption_date
	, fgt.order_date
	, sum(fgt.transaction_qty) units_sold
	, count(distinct case when v.redeemed_at is not null then voucher_barcode_id|| code end) units_redeemed
from user_edwprod.fact_gbl_transactions fgt
left join user_groupondw.acctg_red_voucher_base v
	on cast(v.order_id as varchar(64)) = fgt.order_id
	and fgt.unified_user_id = v.user_uuid
where fgt.source_key<>'CLO'
and "action"='capture'
and fgt.is_order_canceled = 0
and fgt.is_zero_amount = 0
and fgt.order_date>= '2021-11-01'
group by 1,2,3
)with data primary index(merchant_uuid, order_date, redemption_date ) on commit preserve rows;

collect stats column(merchant_uuid, order_date, redemption_date ) on jrg_units;

create multiset volatile table jrg_redeem as  (
select
 cast(dw.week_end as date) report_wk
	, merchant_uuid
	, sum(units_redeemed) units_redeemed
from jrg_units a
join user_groupondw.dim_day dd
	on a.redemption_date = dd.day_rw
join user_groupondw.dim_week dw
	on dd.week_key = dw.week_key
group by 1,2
where coalesce(redemption_date, date'1900-01-01')<>date'1900-01-01'
)with data primary index(report_wk, merchant_uuid ) on commit preserve rows;

collect stats column(report_wk, merchant_uuid ) on jrg_redeem;

create multiset volatile table jrg_sold as  (
select
	cast(dw.week_end as date) report_wk
	, merchant_uuid
	, sum(units_sold) units_sold
from jrg_units a
join user_groupondw.dim_day dd
	on a.order_date = dd.day_rw
join user_groupondw.dim_week dw
	on dd.week_key = dw.week_key
group by 1,2
where coalesce(order_date, date'1900-01-01')<>date'1900-01-01'
)with data primary index(report_wk, merchant_uuid ) on commit preserve rows;

collect stats column(report_wk, merchant_uuid ) on jrg_sold;


drop table sandbox.jrg_mc_weekly;
create multiset table sandbox.jrg_mc_weekly as (
select
	mc.report_wk
	, mc.country_code
	, mc.acct_owner
	, mc.metal
	,case when mc.page_app='sponsored-campaign-itier' then 'Sponsored Listing'
			when mc.page_app ='merchant-advisor-itier' then 'Virtual Advisor'
      when mc.page_app = 'insights_roi' then 'Insights'
			when mc.page_type in ('g_dashboard', 'homepage') then 'Home'
			when mc.page_type in ('deal_list') then 'Campaigns'
			when mc.page_type in ('advisor_main','Virtual Advisor') then 'Virtual Advisor'
			when mc.page_type in ('vouchers', 'voucher_list') then 'Voucher List'
			when mc.page_type in ('customer_feedback','feedback') then 'Feedback'
			when mc.page_type in ('impact_report', 'demographics') then 'Demographics'
			when mc.page_type  in ('payments', 'payment_history') then 'Payments'
			when mc.page_type in ('topics', 'Landing', 'landing') then 'Support'
			when mc.page_type in ('settings', 'accountHome') then 'Admin'
			when mc.page_type in ('mdm_product-edit','mdm_main','mdm_end-date','mdm_description','mdm_photos'
			,'mdm_fineprint','mdm_product-new','mdm_locations','mdm_start-date','mdm_highlights'
			, 'deal_edit','end_date_edit','start_date_edit','description_edit', 'fineprint'
			, 'highlight_edit','location_edit','add-option', 'option', 'options_edit', 'details'
			, 'photos', 'gallery', 'main', 'stock', 'preview', 'price_verification','text_edit') then 'Editing Campaign'
			when mc.page_type in ('redemption-instruction','redemption-locations','bank-tax-info', 'additional-info', 'nutshell-about-business'
			, 'choose-dates','lead_success_user_signup','options','images', 'contract','congratulations', 'dashboard'
			, 'campaigns', 'MarketingForm', 'event-general-info', 'nutshell-deal-options', 'event-options'
			, 'merchandise-campaign', 'file-upload', 'fineprint', 'nutshell-deal-highlight', 'launch-date', 'predraft'
			, 'draft-bank-proxy', 'draft-bank-info', 'draft-ein', 'draft-ssn', 'draft-description', 'draft-highlight', 'draft-contract'
			, 'draft-finish', 'draft-fine-print', 'intro', 'draft-launch-date', 'draft-option-list', 'draft-preview'
			, 'draft-ranking', 'draft-progress', 'metro-intro-started') then 'Building Campaign'
			when mc.page_type in ('redemption_choice','voucher', 'success','search_by_scan', 'search_by_code','search_by_name',
			'results', 'sales_cap', 'result') then 'Redeeming Voucher'
			else 'Other' end page_type
	, mc.platform
	, case when coalesce(ad.merchant_uuid,'')<>'' then 1 else 0 end is_live
	, case when ad.deals_live>5 then '5+' else coalesce(ad.deals_live,0) end as deals_live
	, case when coalesce(us.units_sold,0) = 0 then '0 units sold'
			when coalesce(us.units_sold,0)>0 and coalesce(us.units_sold,0)<=5 then '1 - 5 units sold'
			when coalesce(us.units_sold,0)>5 and coalesce(us.units_sold,0)<=10 then '6 - 10 units sold'
			when coalesce(us.units_sold,0)>10 and coalesce(us.units_sold,0)<=25 then '11 - 25 units sold'
			when coalesce(us.units_sold,0)>25 and coalesce(us.units_sold,0)<=50 then '26 - 50 units sold'
			when coalesce(us.units_sold,0)>50 and coalesce(us.units_sold,0)<=100 then '51 - 100 units sold'
			else '100+ units sold'
			end units_sold
	, case when coalesce(ur.units_redeemed,0) = 0 then '0 units redeemed'
			when coalesce(ur.units_redeemed,0)>0 and coalesce(ur.units_redeemed,0)<=5 then '1 - 5 units redeemed'
			when coalesce(ur.units_redeemed,0)>5 and coalesce(ur.units_redeemed,0)<=10 then '6 - 10 units redeemed'
			when coalesce(ur.units_redeemed,0)>10 and coalesce(ur.units_redeemed,0)<=25 then '11 - 25 units redeemed'
			when coalesce(ur.units_redeemed,0)>25 and coalesce(ur.units_redeemed,0)<=50 then '26 - 50 units redeemed'
			when coalesce(ur.units_redeemed,0)>50 and coalesce(ur.units_redeemed,0)<=100 then '51 - 100 units redeemed'
			else '100+ units redeemed'
			end units_redeemed
	, mc.merchant_uuid
	, count(distinct mc.merchant_uuid||mc.eventdate) log_ins
from sandbox.jrg_mc_visitss mc
left join jrg_mercs ad
	on ad.merchant_uuid = mc.merchant_uuid
	and ad.report_wk = mc.report_wk
left join jrg_sold us
	on us.merchant_uuid = mc.merchant_uuid
	and us.report_wk = mc.report_wk
left join jrg_redeem ur
	on ur.merchant_uuid = mc.merchant_uuid
	and ur.report_wk = mc.report_wk
group by 1,2,3,4,5,6,7,8,9,10,11
) with data primary index(report_wk, country_code, acct_owner, metal, page_type, platform, is_live, deals_live, units_sold, units_redeemed);



drop table jrg_redeem;
create multiset volatile table jrg_redeem as  (
select
 trunc(redemption_date, 'RM') mth
	, merchant_uuid
	, sum(units_redeemed) units_redeemed
from jrg_units a
join user_groupondw.dim_day dd
	on a.redemption_date = dd.day_rw
join user_groupondw.dim_week dw
	on dd.week_key = dw.week_key
group by 1,2
where coalesce(redemption_date, date'1900-01-01')<>date'1900-01-01'
)with data primary index(mth, merchant_uuid ) on commit preserve rows;

drop table jrg_sold;
create multiset volatile table jrg_sold as  (
select
	trunc(order_date, 'RM') mth
	, merchant_uuid
	, sum(units_sold) units_sold
from jrg_units a
join user_groupondw.dim_day dd
	on a.order_date = dd.day_rw
join user_groupondw.dim_week dw
	on dd.week_key = dw.week_key
group by 1,2
where coalesce(order_date, date'1900-01-01')<>date'1900-01-01'
)with data primary index(mth, merchant_uuid ) on commit preserve rows;

drop table jrg_mercs;
create multiset volatile table jrg_mercs as (
 select
 	 trunc(load_date,'RM') report_mth
 	, pm.merchant_uuid
 	, count(distinct ad.DEAL_UUID) deals_live
from user_groupondw.active_deals  ad
join sandbox.pai_deals pd
	on pd.deal_uuid = ad.deal_uuid
join sandbox.pai_merchants pm
	on pm.merchant_uuid = pd.merchant_uuid
join user_groupondw.dim_day dd
	on ad.load_date = dd.day_rw
join user_groupondw.dim_week dw
	on dd.week_key = dw.week_key
where ad.load_date >= '2021-11-01'
and pm.l1='Local'
--and pm.merchant_uuid ='5db1386c-0afd-481b-bcd3-8981e3fe8c31'
group by 1,2
)  with data primary index(merchant_uuid, report_mth) on commit preserve rows;

drop table sandbox.jrg_mc_monthly;
create multiset table sandbox.jrg_mc_monthly as (
select
	mc.report_mth
	, mc.country_code
	, mc.acct_owner
	, mc.metal
	,case when mc.page_app='sponsored-campaign-itier' then 'Sponsored Listing'
			when mc.page_app ='merchant-advisor-itier' then 'Virtual Advisor'
      when mc.page_app = 'insights_roi' then 'Insights'
			when mc.page_type in ('g_dashboard', 'homepage') then 'Home'
			when mc.page_type in ('deal_list') then 'Campaigns'
			when mc.page_type in ('advisor_main','Virtual Advisor') then 'Virtual Advisor'
			when mc.page_type in ('vouchers', 'voucher_list') then 'Voucher List'
			when mc.page_type in ('customer_feedback','feedback') then 'Feedback'
			when mc.page_type in ('impact_report', 'demographics') then 'Demographics'
			when mc.page_type  in ('payments', 'payment_history') then 'Payments'
			when mc.page_type in ('topics', 'Landing', 'landing') then 'Support'
			when mc.page_type in ('settings', 'accountHome') then 'Admin'
			when mc.page_type in ('mdm_product-edit','mdm_main','mdm_end-date','mdm_description','mdm_photos'
			,'mdm_fineprint','mdm_product-new','mdm_locations','mdm_start-date','mdm_highlights'
			, 'deal_edit','end_date_edit','start_date_edit','description_edit', 'fineprint'
			, 'highlight_edit','location_edit','add-option', 'option', 'options_edit', 'details'
			, 'photos', 'gallery', 'main', 'stock', 'preview', 'price_verification','text_edit') then 'Editing Campaign'
			when mc.page_type in ('redemption-instruction','redemption-locations','bank-tax-info', 'additional-info', 'nutshell-about-business'
			, 'choose-dates','lead_success_user_signup','options','images', 'contract','congratulations', 'dashboard'
			, 'campaigns', 'MarketingForm', 'event-general-info', 'nutshell-deal-options', 'event-options'
			, 'merchandise-campaign', 'file-upload', 'fineprint', 'nutshell-deal-highlight', 'launch-date', 'predraft'
			, 'draft-bank-proxy', 'draft-bank-info', 'draft-ein', 'draft-ssn', 'draft-description', 'draft-highlight', 'draft-contract'
			, 'draft-finish', 'draft-fine-print', 'intro', 'draft-launch-date', 'draft-option-list', 'draft-preview'
			, 'draft-ranking', 'draft-progress', 'metro-intro-started') then 'Building Campaign'
			when mc.page_type in ('redemption_choice','voucher', 'success','search_by_scan', 'search_by_code','search_by_name',
			'results', 'sales_cap', 'result') then 'Redeeming Voucher'
			else 'Other' end page_type
	, mc.platform
	, case when coalesce(ad.merchant_uuid,'')<>'' then 1 else 0 end is_live
	, case when ad.deals_live>5 then '5+' else coalesce(ad.deals_live,0) end as deals_live
	, case when coalesce(us.units_sold,0) = 0 then '0 units sold'
			when coalesce(us.units_sold,0)>0 and coalesce(us.units_sold,0)<=5 then '1 - 5 units sold'
			when coalesce(us.units_sold,0)>5 and coalesce(us.units_sold,0)<=10 then '6 - 10 units sold'
			when coalesce(us.units_sold,0)>10 and coalesce(us.units_sold,0)<=25 then '11 - 25 units sold'
			when coalesce(us.units_sold,0)>25 and coalesce(us.units_sold,0)<=50 then '26 - 50 units sold'
			when coalesce(us.units_sold,0)>50 and coalesce(us.units_sold,0)<=100 then '51 - 100 units sold'
			else '100+ units sold'
			end units_sold
	, case when coalesce(ur.units_redeemed,0) = 0 then '0 units redeemed'
			when coalesce(ur.units_redeemed,0)>0 and coalesce(ur.units_redeemed,0)<=5 then '1 - 5 units redeemed'
			when coalesce(ur.units_redeemed,0)>5 and coalesce(ur.units_redeemed,0)<=10 then '6 - 10 units redeemed'
			when coalesce(ur.units_redeemed,0)>10 and coalesce(ur.units_redeemed,0)<=25 then '11 - 25 units redeemed'
			when coalesce(ur.units_redeemed,0)>25 and coalesce(ur.units_redeemed,0)<=50 then '26 - 50 units redeemed'
			when coalesce(ur.units_redeemed,0)>50 and coalesce(ur.units_redeemed,0)<=100 then '51 - 100 units redeemed'
			else '100+ units redeemed'
			end units_redeemed
	, mc.merchant_uuid
	, count(distinct mc.merchant_uuid||mc.eventdate) log_ins
from jrg_mc_visitss mc
left join jrg_mercs ad on ad.merchant_uuid = mc.merchant_uuid and ad.report_mth = mc.report_mth
left join jrg_sold us on us.merchant_uuid = mc.merchant_uuid and us.mth = mc.report_mth
left join jrg_redeem ur on ur.merchant_uuid = mc.merchant_uuid and ur.mth = mc.report_mth
group by 1,2,3,4,5,6,7,8,9,10,11
) with data primary index(report_mth, country_code, acct_owner, metal, page_type, platform, is_live, deals_live, units_sold, units_redeemed);


drop table jrg_redeem;
create multiset volatile table jrg_redeem as  (
select
 trunc(redemption_date, 'Q') qtr
	, merchant_uuid
	, sum(units_redeemed) units_redeemed
from jrg_units a
join user_groupondw.dim_day dd
	on a.redemption_date = dd.day_rw
join user_groupondw.dim_week dw
	on dd.week_key = dw.week_key
group by 1,2
where coalesce(redemption_date, date'1900-01-01')<>date'1900-01-01'
)with data primary index(qtr, merchant_uuid ) on commit preserve rows;

drop table jrg_sold;
create multiset volatile table jrg_sold as  (
select
	trunc(order_date, 'Q') qtr
	, merchant_uuid
	, sum(units_sold) units_sold
from jrg_units a
join user_groupondw.dim_day dd
	on a.order_date = dd.day_rw
join user_groupondw.dim_week dw
	on dd.week_key = dw.week_key
group by 1,2
where coalesce(order_date, date'1900-01-01')<>date'1900-01-01'
)with data primary index(qtr, merchant_uuid ) on commit preserve rows;

drop table jrg_mercs;
create multiset volatile table jrg_mercs as (
 select
 	 trunc(load_date, 'Q') report_qtr
 	, pm.merchant_uuid
 	, count(distinct ad.DEAL_UUID) deals_live
from user_groupondw.active_deals  ad
join sandbox.pai_deals pd
	on pd.deal_uuid = ad.deal_uuid
join sandbox.pai_merchants pm
	on pm.merchant_uuid = pd.merchant_uuid
join user_groupondw.dim_day dd
	on ad.load_date = dd.day_rw
join user_groupondw.dim_week dw
	on dd.week_key = dw.week_key
where ad.load_date >= '2021-11-01'
and pm.l1='Local'
group by 1,2
)  with data primary index(merchant_uuid, report_qtr) on commit preserve rows;


drop table sandbox.jrg_mc_quarterly;
create multiset  table sandbox.jrg_mc_quarterly as (
select
	mc.report_qtr
	, mc.country_code
	, mc.acct_owner
	, mc.metal
	,case when mc.page_app='sponsored-campaign-itier' then 'Sponsored Listing'
			when mc.page_app ='merchant-advisor-itier' then 'Virtual Advisor'
      when mc.page_app = 'insights_roi' then 'Insights'
			when mc.page_type in ('g_dashboard', 'homepage') then 'Home'
			when mc.page_type in ('deal_list') then 'Campaigns'
			when mc.page_type in ('advisor_main','Virtual Advisor') then 'Virtual Advisor'
			when mc.page_type in ('vouchers', 'voucher_list') then 'Voucher List'
			when mc.page_type in ('customer_feedback','feedback') then 'Feedback'
			when mc.page_type in ('impact_report', 'demographics') then 'Demographics'
			when mc.page_type  in ('payments', 'payment_history') then 'Payments'
			when mc.page_type in ('topics', 'Landing', 'landing') then 'Support'
			when mc.page_type in ('settings', 'accountHome') then 'Admin'
			when mc.page_type in ('mdm_product-edit','mdm_main','mdm_end-date','mdm_description','mdm_photos'
			,'mdm_fineprint','mdm_product-new','mdm_locations','mdm_start-date','mdm_highlights'
			, 'deal_edit','end_date_edit','start_date_edit','description_edit', 'fineprint'
			, 'highlight_edit','location_edit','add-option', 'option', 'options_edit', 'details'
			, 'photos', 'gallery', 'main', 'stock', 'preview', 'price_verification','text_edit') then 'Editing Campaign'
			when mc.page_type in ('redemption-instruction','redemption-locations','bank-tax-info', 'additional-info', 'nutshell-about-business'
			, 'choose-dates','lead_success_user_signup','options','images', 'contract','congratulations', 'dashboard'
			, 'campaigns', 'MarketingForm', 'event-general-info', 'nutshell-deal-options', 'event-options'
			, 'merchandise-campaign', 'file-upload', 'fineprint', 'nutshell-deal-highlight', 'launch-date', 'predraft'
			, 'draft-bank-proxy', 'draft-bank-info', 'draft-ein', 'draft-ssn', 'draft-description', 'draft-highlight', 'draft-contract'
			, 'draft-finish', 'draft-fine-print', 'intro', 'draft-launch-date', 'draft-option-list', 'draft-preview'
			, 'draft-ranking', 'draft-progress', 'metro-intro-started') then 'Building Campaign'
			when mc.page_type in ('redemption_choice','voucher', 'success','search_by_scan', 'search_by_code','search_by_name',
			'results', 'sales_cap', 'result') then 'Redeeming Voucher'
			else 'Other' end page_type
	, mc.platform
	, case when coalesce(ad.merchant_uuid,'')<>'' then 1 else 0 end is_live
	, case when ad.deals_live>5 then '5+' else coalesce(ad.deals_live,0) end as deals_live
	, case when coalesce(us.units_sold,0) = 0 then '0 units sold'
			when coalesce(us.units_sold,0)>0 and coalesce(us.units_sold,0)<=5 then '1 - 5 units sold'
			when coalesce(us.units_sold,0)>5 and coalesce(us.units_sold,0)<=10 then '6 - 10 units sold'
			when coalesce(us.units_sold,0)>10 and coalesce(us.units_sold,0)<=25 then '11 - 25 units sold'
			when coalesce(us.units_sold,0)>25 and coalesce(us.units_sold,0)<=50 then '26 - 50 units sold'
			when coalesce(us.units_sold,0)>50 and coalesce(us.units_sold,0)<=100 then '51 - 100 units sold'
			else '100+ units sold'
			end units_sold
	, case when coalesce(ur.units_redeemed,0) = 0 then '0 units redeemed'
			when coalesce(ur.units_redeemed,0)>0 and coalesce(ur.units_redeemed,0)<=5 then '1 - 5 units redeemed'
			when coalesce(ur.units_redeemed,0)>5 and coalesce(ur.units_redeemed,0)<=10 then '6 - 10 units redeemed'
			when coalesce(ur.units_redeemed,0)>10 and coalesce(ur.units_redeemed,0)<=25 then '11 - 25 units redeemed'
			when coalesce(ur.units_redeemed,0)>25 and coalesce(ur.units_redeemed,0)<=50 then '26 - 50 units redeemed'
			when coalesce(ur.units_redeemed,0)>50 and coalesce(ur.units_redeemed,0)<=100 then '51 - 100 units redeemed'
			else '100+ units redeemed'
			end units_redeemed
	, mc.merchant_uuid
	, count(distinct mc.merchant_uuid||mc.eventdate) log_ins
from jrg_mc_visitss mc
left join jrg_mercs ad
	on ad.merchant_uuid = mc.merchant_uuid
	and ad.report_qtr = mc.report_qtr
left join jrg_sold us
	on us.merchant_uuid = mc.merchant_uuid
	and us.qtr = mc.report_qtr
left join jrg_redeem ur
	on ur.merchant_uuid = mc.merchant_uuid
	and ur.qtr = mc.report_qtr
group by 1,2,3,4,5,6,7,8,9,10,11
) with data primary index(report_qtr, country_code, acct_owner, metal, page_type, platform, is_live, deals_live, units_sold, units_redeemed);


drop table sandbox.jrg_mc_pages;
create multiset table sandbox.jrg_mc_pages as (
select
	report_wk dt
	, country_code
	, metal
	, is_live
	, deals_live
	, units_sold
	, units_redeemed
	, page_type
	, platform
	, 'week' time_period
	, count(distinct merchant_uuid) merchants
	, sum(log_ins) logins
from sandbox.jrg_mc_weekly
where page_type not in('Other', 'Redeeming Voucher','Editing Campaign', 'Building Campaign')
group by 1,2,3,4,5,6,7,8,9,10
	union all
select
	report_mth dt
	, country_code
	, metal
	, is_live
	, deals_live
	, units_sold
	, units_redeemed
	, page_type
	, platform
	, 'month' time_period
	, count(distinct merchant_uuid) merchants
	, sum(log_ins) logins
from sandbox.jrg_mc_monthly
where page_type not in('Other', 'Redeeming Voucher','Editing Campaign', 'Building Campaign')
group by 1,2,3,4,5,6,7,8,9,10
	union all
select
	report_qtr dt
	, country_code
	, metal
	, is_live
	, deals_live
	, units_sold
	, units_redeemed
	, page_type
	, platform
	, 'quarter' time_period
	, count(distinct merchant_uuid) merchants
	, sum(log_ins) logins
from sandbox.jrg_mc_quarterly
where page_type not in('Other', 'Redeeming Voucher','Editing Campaign', 'Building Campaign')
group by 1,2,3,4,5,6,7,8,9,10
)with data unique primary index(dt, country_code, metal, is_live, deals_live, units_sold, units_redeemed, page_type, platform, time_period);


drop table sandbox.jrg_mc_visits;
create multiset table sandbox.jrg_mc_visits as (
select
	report_wk dt
	, country_code
	, metal
	, is_live
	, deals_live
	, units_sold
	, units_redeemed
	, 'week' time_period
	, count(distinct merchant_uuid) merchants
	, avg(log_ins) logins
from sandbox.jrg_mc_weekly
group by 1,2,3,4,5,6,7,8
	union all
select
	report_mth dt
	, country_code
	, metal
	, is_live
	, deals_live
	, units_sold
	, units_redeemed
	, 'month' time_period
	, count(distinct merchant_uuid) merchants
	, avg(log_ins) logins
from sandbox.jrg_mc_monthly
group by 1,2,3,4,5,6,7,8
	union all
select
	report_qtr dt
	, country_code
	, metal
	, is_live
	, deals_live
	, units_sold
	, units_redeemed
	, 'quarter' time_period
	, count(distinct merchant_uuid) merchants
	, avg(log_ins) logins
from sandbox.jrg_mc_quarterly
group by 1,2,3,4,5,6,7,8
)with data unique primary index(dt, country_code, metal, is_live, deals_live, units_sold, units_redeemed, time_period);


drop table sandbox.jrg_mc_visits_platforms;
create multiset table sandbox.jrg_mc_visits_platforms as (
select
	report_wk dt
	, country_code
	, metal
	, is_live
	, deals_live
	, units_sold
	, units_redeemed
	, platform
	, 'week' time_period
	, count(distinct merchant_uuid) merchants
	, sum(log_ins) logins
from sandbox.jrg_mc_weekly
group by 1,2,3,4,5,6,7,8,9
	union all
select
	report_mth dt
	, country_code
	, metal
	, is_live
	, deals_live
	, units_sold
	, units_redeemed
	, platform
	, 'month' time_period
	, count(distinct merchant_uuid) merchants
	, sum(log_ins) logins
from sandbox.jrg_mc_monthly
group by 1,2,3,4,5,6,7,8,9
	union all
select
	report_qtr dt
	, country_code
	, metal
	, is_live
	, deals_live
	, units_sold
	, units_redeemed
	, platform
	, 'quarter' time_period
	, count(distinct merchant_uuid) merchants
	, sum(log_ins) logins
from sandbox.jrg_mc_quarterly
group by 1,2,3,4,5,6,7,8,9
)with data unique primary index(dt, country_code, metal, is_live, deals_live, units_sold, units_redeemed, platform, time_period);


drop table sandbox.jrg_mc_features;
create multiset table sandbox.jrg_mc_features as (
select
	report_wk dt
	, country_code
	, metal
	, is_live
	, deals_live
	, units_sold
	, units_redeemed
	, page_type
	, platform
	, 'week' time_period
	, count(distinct merchant_uuid) merchants
	, sum(log_ins) logins
from sandbox.jrg_mc_weekly
where page_type in('Redeeming Voucher','Editing Campaign', 'Building Campaign')
group by 1,2,3,4,5,6,7,8,9,10
	union all
select
	report_mth dt
	, country_code
	, metal
	, is_live
	, deals_live
	, units_sold
	, units_redeemed
	, page_type
	, platform
	, 'month' time_period
	, count(distinct merchant_uuid) merchants
	, sum(log_ins) logins
from sandbox.jrg_mc_monthly
where page_type in('Redeeming Voucher','Editing Campaign', 'Building Campaign')
group by 1,2,3,4,5,6,7,8,9,10
	union all
select
	report_qtr dt
	, country_code
	, metal
	, is_live
	, deals_live
	, units_sold
	, units_redeemed
	, page_type
	, platform
	, 'quarter' time_period
	, count(distinct merchant_uuid) merchants
	, sum(log_ins) logins
from sandbox.jrg_mc_quarterly
where page_type in('Redeeming Voucher','Editing Campaign', 'Building Campaign')
group by 1,2,3,4,5,6,7,8,9,10
)with data unique primary index(dt, country_code, metal, is_live, deals_live, units_sold, units_redeemed, page_type, platform, time_period);

grant select on sandbox.jrg_mc_features to public;
grant select on sandbox.jrg_mc_pages to public;
grant select on sandbox.jrg_mc_visits_platforms to public;
grant select on sandbox.jrg_mc_visits to public;
grant select on sandbox.jrg_mc_weekly to public;
