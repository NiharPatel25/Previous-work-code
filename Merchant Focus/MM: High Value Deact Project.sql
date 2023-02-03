---------DATA IMPORT
create multiset table sandbox.np_highvalue_deact 
(account_id VARCHAR(64) CHARACTER SET UNICODE,
 category VARCHAR(64) CHARACTER SET UNICODE
) no primary index;

----------REFERENCING 

---Merchant live on 2022-09-10 will not be considered

create multiset volatile table np_disregard as 
(select 
      b.merchant_uuid
 from user_groupondw.active_deals as a 
 left join sandbox.pai_deals as b on a.deal_uuid = b.deal_uuid
 where load_date = cast('2022-11-09' as date) and SOLD_OUT = 'false' 
 group by 1) with data on commit preserve rows;


create multiset volatile table np_accounts as 
(select a.account_id, 
       case when a.category = 'N/A' then 'No-incentive' else a.category end category,
       b.merchant_uuid, 
       b.last_live_date,
       b.is_live 
from sandbox.np_highvalue_deact as a
left join sandbox.pai_merchants as b on b.account_id = a.account_id and (is_live = 1 or was_live = 1)
left join np_disregard as c on b.merchant_uuid = c.merchant_uuid
where c.merchant_uuid is null
) with data on commit preserve rows;

create multiset volatile table np_cls_lnch as 
(select 
      account_id, 
      merchant_uuid, 
      min(CloseDate) as close_min_date, 
      min(launch_date) as launch_min_date, 
      trunc(close_min_date, 'iw') + 6 close_week_end, 
      trunc(launch_min_date, 'iw') + 6 launch_week_end, 
      count(distinct deal_uuid) deals_closed, 
      count(distinct case when launch_date is not null then deal_uuid end) deals_launched
 from sandbox.pai_opp_mtd_attrib 
 where CloseDate > cast('2022-11-09' as date)
 group by 1,2
) with data on commit preserve rows
;


create multiset volatile table np_lnch_wo_cls as 
(select 
      b.merchant_uuid, 
      min(load_date) launch_min_date_wo_cls,
      trunc(launch_min_date, 'iw') + 6 launch_week_end, 
      count(distinct DEAL_UUID) deals_launched
 from user_groupondw.active_deals as a 
 left join sandbox.pai_deals as b on a.deal_uuid = b.deal_uuid
 where load_date > cast('2022-11-09' as date) and SOLD_OUT = 'false' 
 group by 1) with data on commit preserve rows;



------Customer metrics

create multiset volatile table np_temp_user_merchn_pur as (
select 
  user_uuid, 
  fgt.merchant_uuid, 
  max(order_date) last_ord_bfr_attrit
from user_edwprod.fact_gbl_transactions fgt
join np_accounts ac on fgt.merchant_uuid = ac.merchant_uuid
where fgt.source_key<>'CLO'
and "action"='capture'
and fgt.is_order_canceled = 0
and fgt.is_zero_amount = 0
and fgt.country_id = '235'
and cast(fgt.order_date as date) <= cast('2022-11-09' as date)
group by 1,2
) with data on commit preserve rows;----checks if these are users who previously purchased here


create multiset volatile table nvp_ogp_temp as 
(select 
   a.parent_order_uuid, 
   a.order_id,
   sum(total_estimated_ogp_loc) ogp
from 
   user_edwprod.fact_gbl_ogp_transactions as a 
   join user_edwprod.fact_gbl_transactions as b on a.parent_order_uuid = b.parent_order_uuid and a.order_id = b.order_id and b."action" = 'authorize'
   where
      a.order_date >= cast('2022-11-09' as date)
      and a."action" = 'authorize' 
      and a.country_id = '235'
      and b.order_date = cast('2022-11-09' as date)
   group by 1,2) with data on commit preserve rows;

create volatile table jrg_orderss as (
select
    fgt.user_uuid, 
    is_activation,
    is_reactivation,
    case when re.user_uuid is not null then 1 else 0 end is_revisit,
    fgt.merchant_uuid, 
    deal_uuid, 
	fgt.order_id, 
	capture_nob_loc, 
	order_date,
	ogp
from user_edwprod.fact_gbl_transactions fgt
join np_accounts ac on fgt.merchant_uuid = ac.merchant_uuid
left join np_temp_user_merchn_pur as re on fgt.merchant_uuid = re.merchant_uuid and fgt.user_uuid = re.user_uuid
left join nvp_ogp_temp as ord on fgt.order_id = ord.order_id
where fgt.source_key<>'CLO'
and "action"='capture'
and fgt.is_order_canceled = 0
and fgt.is_zero_amount = 0
and cast(fgt.order_date as date) > cast('2022-11-09' as date)
and fgt.country_id = '235'
) with data on commit preserve rows;



-------import 

drop table sandbox.np_highvalue_deact_ex;
create multiset table sandbox.np_highvalue_deact_ex as (
select 
      a.*, 
      b.close_min_date close_min_date,
      b.close_week_end close_week_end,
      b.launch_min_date new_launch_date, 
      b.launch_week_end launch_date_week, 
      b.deals_closed, 
      b.deals_launched,
      
      d.min_deal_start_mc,
      d.min_deal_submit_mc,
      d.deals_started,
      e.total_users,
      e.user_orders,
      e.total_nob, 
      e.total_ogp,
      e.total_react_users, 
      e.total_react_user_orders, 
      e.react_user_nob, 
      e.react_user_ogp,
      e.total_revis_users, 
      e.total_revis_user_orders, 
      e.revis_user_nob,
      e.revis_user_ogp,
      c.email_sent_date
  from 
   np_accounts as a 
   left join np_cls_lnch as b on a.merchant_uuid = b.merchant_uuid
   left join 
       (select 
          merchant_uuid,
          min(cast(substr(sentdate, 1,10) as date)) email_sent_date
        from sandbox.SFMC_EmailEngagement 
         where cast(substr(sentdate, 1,10) as date) > cast('2022-11-09' as date)
         group by 1
       ) as c on a.merchant_uuid = c.merchant_uuid
   left join 
      (select 
          merchantuuid, 
          min(deal_start_date)  min_deal_start_mc,
          min(deal_submitdate_first) min_deal_submit_mc, 
          count(distinct dealid_start_first) deals_started
       from sandbox.pai_cb_midfunnel_agg 
       where deal_start_date > cast('2022-11-09' as date)
       group by 1
      ) as d on a.merchant_uuid = d.merchantuuid
   left join 
       (select merchant_uuid, 
               count(distinct user_uuid) total_users,
               count(distinct concat(user_uuid, order_id)) user_orders,
               sum(capture_nob_loc) total_nob, 
               sum(ogp) total_ogp,  
               count(distinct case when is_reactivation = 1 then user_uuid end) total_react_users, 
               count(distinct case when is_reactivation = 1 then concat(user_uuid, order_id) end) total_react_user_orders, 
               sum(case when is_reactivation = 1 then capture_nob_loc end) react_user_nob, 
               sum(case when is_reactivation = 1 then ogp end) react_user_ogp, 
               count(distinct case when is_revisit = 1 then user_uuid end) total_revis_users, 
               count(distinct case when is_revisit = 1 then concat(user_uuid, order_id) end) total_revis_user_orders, 
               sum(case when is_revisit = 1 then capture_nob_loc end) revis_user_nob,
               sum(case when is_revisit = 1 then ogp end) revis_user_ogp
        from jrg_orderss
        group by 1
       )as e on a.merchant_uuid = e.merchant_uuid
       ) with data;


-------importing deal level


create multiset volatile table np_dl_cls_lnch as 
(select 
      account_id, 
      merchant_uuid, 
      deal_uuid,
      CloseDate as close_date, 
      launch_date as launch_date, 
      trunc(close_date, 'iw') + 6 close_week_end, 
      trunc(launch_date, 'iw') + 6 launch_week_end
 from sandbox.pai_opp_mtd_attrib 
 where CloseDate > cast('2022-11-09' as date)
) with data on commit preserve rows
;

drop table sandbox.np_highvalue_deact_ex_dl;
create multiset table sandbox.np_highvalue_deact_ex_dl as (
select 
    a.*, 
    b.deal_uuid closed_deal_uuid, 
    b.close_date, 
    b.launch_date,
    b.close_week_end, 
    b.launch_week_end
from 
   np_accounts as a 
   left join np_dl_cls_lnch as b on a.merchant_uuid = b.merchant_uuid 
   left join 
       (select merchant_uuid, 
               deal_uuid,
               count(distinct user_uuid) total_users,
               count(distinct concat(user_uuid, order_id)) user_orders,
               sum(capture_nob_loc) total_nob, 
               sum(ogp) total_ogp,  
               count(distinct case when is_reactivation = 1 then user_uuid end) total_react_users, 
               count(distinct case when is_reactivation = 1 then concat(user_uuid, order_id) end) total_react_user_orders, 
               sum(case when is_reactivation = 1 then capture_nob_loc end) react_user_nob, 
               sum(case when is_reactivation = 1 then ogp end) react_user_ogp, 
               count(distinct case when is_revisit = 1 then user_uuid end) total_revis_users, 
               count(distinct case when is_revisit = 1 then concat(user_uuid, order_id) end) total_revis_user_orders, 
               sum(case when is_revisit = 1 then capture_nob_loc end) revis_user_nob,
               sum(case when is_revisit = 1 then ogp end) revis_user_ogp
        from jrg_orderss
        group by 1,2
       )as e on a.merchant_uuid = e.merchant_uuid and b.deal_uuid = e.deal_uuid) with data;

-------
      
      
/*
 * min(case when CloseDate > cast('2022-11-09' as date) then CloseDate end) as close_min_date, 
   min(case when launch_date  > cast('2022-11-09' as date) then launch_date end) as launch_min_date, 
 * 
 * create multiset volatile table np_cls_lnch as 
(select 
      b.merchant_uuid, 
      min(load_date) as min_date, 
      trunc(min_date, 'iw') + 6 min_week_end
 from user_groupondw.active_deals as a 
 left join sandbox.pai_deals as b on a.deal_uuid = b.deal_uuid
 where load_date > cast('2022-11-09' as date) and SOLD_OUT = 'false' 
 group by 1) with data on commit preserve rows
;*/
      
       
select 
bcookie, 
user_uuid, 
is_activation, 
is_reactivation 
from user_edwprod.fact_gbl_transactions fgt
where fgt.source_key<>'CLO'
and "action"='capture'
and fgt.is_order_canceled = 0
and fgt.is_zero_amount = 0
and fgt.country_id = '235'
and cast(fgt.order_date as date) > cast('2022-01-09' as date)
and lower(user_uuid) = lower(bcookie)
and length(user_uuid) > 5
sample 5;

select * from user_edwprod.fact_gbl_transactions fgt
where fgt.source_key<>'CLO'
and "action"='capture'
and fgt.is_order_canceled = 0
and fgt.is_zero_amount = 0
and fgt.country_id = '235'
sample 5;

-----------------------------------------------------------------------INVESTIGATION
select * 
from dwh_base_sec_view.sf_account as sfa
  left join user_groupondw.sf_lead as a
  on sfa.account_id_18 = a.convertedaccountid where a.campaign_name__c like '%TXN1_US_ALL_SEMB_GEN_EX_GOOG_TXT3%';
 

select 
   
from user_groupondw.sf_lead as a 
left join 
dwh_base_sec_view.sf_account as b on sfa.account_id_18 = a.convertedaccountid
where 
; 

select * from user_groupondw.sf_account;

select * from user_groupondw.active_deals;



show stat user_groupondw.sf_lead;
describe dwh_base_sec_view.sf_account;




select *
from grp_gdoop_pde.junohourly
 where eventdate ='2022-09-21'
 and event= 'genericPageView' 
 and eventdestination ='searchBrowseView'
 and bcookie ='ad6dfa97-612d-8603-e178-121ded57f0cb'
 and campaign like '%DIRM%'
 limit 10;

select *
from grp_gdoop_pde.junohourly
 where bcookie ='ad6dfa97-612d-8603-e178-121ded57f0cb'
 and event= 'genericPageView' 
 and campaign='TXN1_US_All_DIRM_URL_IMG1';



select 
     Company, 
     LeadSource, 
     Campaign_Name__c,
     CreatedDate
from user_groupondw.sf_lead 
where Campaign_Name__c like '%TXN1_US_TO_DIS_GEN__FB_IMG5%' and CreatedDate >= cast('2022-10-10' as date) and lower(Company) like '%nice%';

select 
     *
from user_groupondw.sf_lead 
where Campaign_Name__c like '%TXN1_US_ALL_SEMB_GEN_EX_GOOG_TXT3%' 
      and CreatedDate >= cast('2022-10-10' as date) and Company = 'Titan Student Union';

select * from user_groupondw.sf_lead

select Campaign_Name__c ,min(CreatedDate) min_create_date, max(CreatedDate) max_create_date
from user_groupondw.sf_lead where Campaign_Name__c like '%TXN1_US_ALL_SEMB_GEN_EX_GOOG_TXT3%'
group by 1;

select Campaign_Name__c ,min(CreatedDate) min_create_date, max(CreatedDate) max_create_date
from user_groupondw.sf_lead where Campaign_Name__c like '%TXN1_US_TO_DIS_GEN__FB_IMG5%'
group by 1;

select Campaign_Name__c , min(CreatedDate) min_create_date, max(CreatedDate) max_create_date
from user_groupondw.sf_lead where Campaign_Name__c like '%TXN1_US_TO_GPMC_GEN__GOOG_TXT6%'
group by 1;



create volatile table jrg_mc_visitss as (
select distinct
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
and coalesce(a.merchant_uuid,'')<>''
---and coalesce(page_type,'')<>''
and pm.l1='Local'
) with data primary index(merchant_uuid, eventdate) on commit preserve rows;

select * 
from sandbox.pai_merchant_center_visits 
where 
eventdate > CURRENT_DATE - 15 
and bcookie  = 'ad6dfa97-612d-8603-e178-121ded57f0cb'
sample 5;


