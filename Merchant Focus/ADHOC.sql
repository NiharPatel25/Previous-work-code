select * from sandbox.pai_merchants;

create MULTISET volatile table CBdeals
as
(
	select 
		distinct a.deal_id as deal_uuid
	from sb_merchant_experience.history_event as a
	where a.event_type in ('METRO_CONTRACT_SIGNIN','DRAFT_DEAL_CREATION','G1_METRO_CONTRACT_SIGNED')
)
	with data unique primary index (deal_uuid) on Commit preserve rows;
	collect stats on CBdeals column (deal_uuid); 


create multiset volatile table launched_deals
as
( select 
		distinct ac1.deal_uuid,
		opp.opportunity_id,
		opp.feature_country,
		lower(opp.opportunity_name) as opp_name,
		case when opp_name like any ('dmapi%','%*g1m*%') then 'Metro' else 'Non-metro' end as metro_opp_check,
		coalesce(opp.accountid,dm.salesforce_account_id,null) as account_id,
		coalesce(dm.merchant_uuid,pd.merchant_uuid,null) as merchantuuid,
		case when lower(AC.MERCHANT_segmentation__c) like any ('%gold%','%platinum%','%silver%') then 'S+' else 'B-' end as Metal_group,
		opp.createddate as deal_created_ts,
		ac1.min_date as deal_launch_ts,
		opp.permalink as deal_permalink,
		opp.Deal_Strengh,
		op3.hold_at__c    as hold_at,
		case when cbd.deal_uuid = ac1.deal_uuid then 1 else 0 end as deal_from_CB_flag
	from (select deal_uuid, min(dwh_create_ts) as min_date from user_groupondw.active_deals  group by 1 ) as ac1  --give me the actual launch date
	left join sandbox.pai_deals as pd on pd.deal_uuid = ac1.deal_uuid -- get merchant uuid
	left join user_edwprod.sf_opportunity_2 op2 on op2.deal_uuid = ac1.deal_uuid -- get opp ID from deal uuid
	left join dwh_base_sec_view.Opportunity_1 as opp on opp.id = op2.id --1st attempt get account id from opp ID
	left join dwh_base_sec_view.sf_opportunity_3 as op3 on opp.id = op3.id--to get orevetting and post vetting
	left join ( select 
	                merchant_uuid,
	                salesforce_account_id  
	                from user_edwprod.dim_merchant 
	                qualify (ROW_NUMBER() OVER (PARTITION BY salesforce_account_id ORDER BY updated_at )) = 1
	           ) as dm on dm.salesforce_account_id = opp.accountid--get merchant_uuid
	left join user_groupondw.sf_account as ac on ac.id = account_id
	left join CBdeals As cbd on cbd.deal_uuid = ac1.deal_uuid--check if the deal in active deals is from CB
	)with data unique primary index (deal_uuid,account_id) on commit preserve rows; 
collect stats on launched_deals column (merchantuuid, deal_uuid, deal_launch_ts,account_id,metro_opp_check);


drop table sandbox.np_temp_launched_deals;
create multiset table sandbox.np_temp_launched_deals
as
( select 
		distinct ac1.deal_uuid,
		opp.opportunity_id,
		opp.feature_country,
		lower(opp.opportunity_name) as opp_name,
		case when opp_name like any ('dmapi%','%*g1m*%') then 'Metro' else 'Non-metro' end as metro_opp_check,
		coalesce(opp.accountid,dm.salesforce_account_id,null) as account_id,
		coalesce(dm.merchant_uuid,pd.merchant_uuid,null) as merchantuuid,
		case when lower(AC.MERCHANT_segmentation__c) like any ('%gold%','%platinum%','%silver%') then 'S+' else 'B-' end as Metal_group,
		opp.createddate as deal_created_ts,
		ac1.min_date as deal_launch_ts,
		opp.permalink as deal_permalink,
		opp.Deal_Strengh,
		op3.hold_at__c    as hold_at,
		case when cbd.deal_uuid = ac1.deal_uuid then 1 else 0 end as deal_from_CB_flag,
		ROW_NUMBER() OVER (PARTITION BY pd.merchant_uuid ORDER BY ac1.min_date asc ) row_nr
	from (select deal_uuid, min(dwh_create_ts) as min_date from user_groupondw.active_deals  group by 1 ) as ac1  --give me the actual launch date
	left join sandbox.pai_deals as pd on pd.deal_uuid = ac1.deal_uuid -- get merchant uuid
	left join user_edwprod.sf_opportunity_2 op2 on op2.deal_uuid = ac1.deal_uuid -- get opp ID from deal uuid
	left join dwh_base_sec_view.Opportunity_1 as opp on opp.id = op2.id --1st attempt get account id from opp ID
	left join dwh_base_sec_view.sf_opportunity_3 as op3 on opp.id = op3.id--to get orevetting and post vetting
	left join ( select 
	                merchant_uuid,
	                salesforce_account_id  
	                from user_edwprod.dim_merchant 
	                qualify (ROW_NUMBER() OVER (PARTITION BY salesforce_account_id ORDER BY updated_at )) = 1
	           ) as dm on dm.salesforce_account_id = opp.accountid--get merchant_uuid
	left join user_groupondw.sf_account as ac on ac.id = account_id
	left join CBdeals As cbd on cbd.deal_uuid = ac1.deal_uuid--check if the deal in active deals is from CB
	)with data unique primary index (deal_uuid,account_id);
collect stats on launched_deals column (merchantuuid, deal_uuid, deal_launch_ts,account_id,metro_opp_check);


deal_uuid,
opportunity_id,
feature_country,
opp_name,
metro_opp_check,
account_id,
merchantuuid,
Metal_group,
deal_created_ts,
deal_launch_ts,
deal_permalink,
Deal_Strengh,
hold_at,
deal_from_CB_flag



drop table leads;
create MULTISET volatile table leads
as
(
select 
	a.convertedaccountid account_id, 
	max(case when upper(a.campaign_name__c) like 'TXN1%' then 1 else 0 end) as lead_category --is an identifier for all paid campaigns
from user_groupondw.sf_lead as a --all leads created and our main source
     where lower(leadsource)  like any ('%mia%','%metro%')--only looking for leads via these two sources
		   AND convertedaccountid IS NOT NULL--a lead without accountid isn't useful for any mapping downstream
group by 1
)
with data unique primary index (account_id) on commit preserve rows; 
collect stats on leads column(account_id,lead_created_ts,lead_category); 


drop table sandbox.np_temp_fgt;
create multiset table sandbox.np_temp_fgt as (
select 
    cast(order_date as date) - EXTRACT(DAY FROM cast(order_date as date)) + 1 revenue_month,
    fgt.merchant_uuid, 
    c.account_id,
    case when fgt.country_id in ('235', '40') then 'NAM' else 'INTL' end continent,
    fgt.deal_uuid, 
    b.first_launch_date merchant_first_launch,
    case when b.first_launch_date >= order_date - 30 then 'launched_in_30_days_before_order'
        when b.first_launch_date >= order_date - 60 then 'launched_in_30-60_days_before_order'
        when b.first_launch_date >= order_date - 90 then 'launched_in_60-90_days_before_order'
        when b.first_launch_date >= order_date - 120 then 'launched_in_90-120_days_before_order'
        when b.first_launch_date < order_date - 120 then 'launched_120_days_before_order'
        else 'no_launch_data'
        end merchant_launch_category,
    sum(fgt.auth_nob_loc) nob
 from  user_edwprod.fact_gbl_transactions as fgt
 left join (select 
  				merchant_uuid, 
  				first_launch_date 
			from sandbox.sh_new_existing 
			group by 1,2
			) as b on fgt.merchant_uuid = b.merchant_uuid
left join sandbox.pai_merchants as c on fgt.merchant_uuid = c.merchant_uuid
 where fgt.action = 'authorize'
	   and order_date >= '2022-01-01'
       and is_zero_amount = 0
       and length(fgt.merchant_uuid) >= 10
group by 1,2,3,4,5,6,7
) with data;




select 
    revenue_month, 
    merchant_launch_category, 
    deal_from_CB_flag, 
    lead_category,
    continent,
    sum(nob) nob, 
    count(distinct fgt.merchant_uuid) merchant_with_revenue
from sandbox.np_temp_fgt as fgt
join sandbox.pai_deals as pai on  fgt.deal_uuid = pai.deal_uuid and pai.l1 = 'Local'
join launched_deals as b on fgt.deal_uuid = b.deal_uuid
left join leads as lds on fgt.account_id = lds.account_id
group by 1,2,3,4,5
order by 3;

select 
    revenue_month, 
    cast(merchant_first_launch as date) - EXTRACT(DAY FROM cast(merchant_first_launch as date)) + 1 launched_month, 
    deal_from_CB_flag, 
    lead_category,
    continent,
    sum(nob) nob, 
    count(distinct fgt.merchant_uuid) merchant_with_revenue
from sandbox.np_temp_fgt as fgt
join sandbox.pai_deals as pai on  fgt.deal_uuid = pai.deal_uuid and pai.l1 = 'Local'
join launched_deals as b on fgt.deal_uuid = b.deal_uuid
left join leads as lds on fgt.account_id = lds.account_id
group by 1,2,3,4,5
order by 3;


select 
    revenue_month, 
    case when merchant_launch_category in ('launched_in_30_days_before_order', 'launched_in_30-60_days_before_order',  'launched_in_60-90_days_before_order') then 'New'
         WHEN merchant_launch_category in ('launched_in_90-120_days_before_order', 'launched_120_days_before_order') then 'Old'
         else merchant_launch_category
         end case_state,
    deal_from_CB_flag,
    lead_category,
    continent,
    count(distinct fgt.merchant_uuid) merchant_with_revenue
from sandbox.np_temp_fgt as fgt
join sandbox.pai_deals as pai on  fgt.deal_uuid = pai.deal_uuid and pai.l1 = 'Local'
join launched_deals as b on fgt.deal_uuid = b.deal_uuid
left join leads as lds on fgt.account_id = lds.account_id
group by 1,2,3,4,5;

select 
    revenue_month, 
    continent,
    count(distinct fgt.merchant_uuid) merchant_with_revenue
from sandbox.np_temp_fgt as fgt
join sandbox.pai_deals as pai on  fgt.deal_uuid = pai.deal_uuid and pai.l1 = 'Local'
join launched_deals as b on fgt.deal_uuid = b.deal_uuid
left join leads as lds on fgt.account_id = lds.account_id
group by 1,2;


select 
   live_month, 
   continent,
   count(distinct merchant_uuid) merch_count
from sandbox.np_temp_live1
group by 1,2
;





-----------------------------------LIVE MERCHANTS 



create multiset table sandbox.np_temp_live1 as (
select distinct 
         cast(a.dwh_create_ts as date) - EXTRACT(DAY FROM cast(a.dwh_create_ts as date)) + 1 live_month,
         cast(a.dwh_create_ts as date) live_date,
         a.deal_uuid,
         b.merchant_uuid, 
         case when b.country_code in ('US', 'CA') then 'NAM' else 'INTL' END continent
       from user_groupondw.active_deals as a 
       left join sandbox.pai_deals as b on a.deal_uuid = b.deal_uuid
       where sold_out = 'false' and dwh_create_ts >= cast('2022-01-01' as date)
) with data;


create multiset table sandbox.np_temp_live2 as (
select 
  				merchant_uuid, 
  				first_launch_date 
			from sandbox.sh_new_existing 
			where is_new = 'new'
			group by 1,2
	) with data;

create multiset table sandbox.np_temp_live as (
select 
   live.merchant_uuid, 
   live.deal_uuid, 
   live_date,
   b.first_launch_date
from 
    sandbox.np_temp_live1 as live
 left join 
      sandbox.np_temp_live2 as b on live.merchant_uuid = b.merchant_uuid
      ) with data
;

drop table sandbox.np_temp_live;
CREATE MULTISET TABLE sandbox.np_temp_live 
     (
      month_live VARCHAR(50) CHARACTER SET UNICODE,
      live_date VARCHAR(50) CHARACTER SET UNICODE,
      first_launch_date VARCHAR(50) CHARACTER SET UNICODE,
      merchant_uuid VARCHAR(50) CHARACTER SET UNICODE,
      deal_uuid VARCHAR(100) CHARACTER SET UNICODE, 
      merchant_launch_category VARCHAR(100) CHARACTER SET UNICODE
      )
NO PRIMARY INDEX;

select * from sandbox.np_temp_live;


      
DROP TABLE   grp_gdoop_bizops_db.np_temp_live; 
create table grp_gdoop_bizops_db.np_temp_live stored as orc as 
select 
month_live,
live_date, 
first_launch_date,
live.merchant_uuid, 
live.deal_uuid, 
case when b.first_launch_date >= date_sub(live_date,30) then 'launched_in_30_days_before_order'
        when b.first_launch_date >= date_sub(live_date,60) then 'launched_in_30-60_days_before_order'
        when b.first_launch_date >= date_sub(live_date,90) then 'launched_in_60-90_days_before_order'
        when b.first_launch_date >= date_sub(live_date,120) then 'launched_in_90-120_days_before_order'
        when b.first_launch_date < date_sub(live_date,120) then 'launched_120_days_before_order'
        else 'no_launch_data'
        end merchant_launch_category
from 
(select distinct 
         concat(substr(load_date, 1,4), '-', substr(load_date, 6,2), '-', '01') month_live,
         cast(a.load_date as date) live_date,
         a.deal_uuid,
         b.merchant_uuid, 
         case when b.country_code in ('US', 'CA') then 'NAM' else 'INTL' END continent
       from prod_groupondw.active_deals as a 
       left join grp_gdoop_bizops_db.pai_deals as b on a.deal_uuid = b.deal_uuid
       where sold_out = 'false' and cast(load_date as date) >= cast('2022-01-01' as date)) as live
 left join 
     (select merchant_uuid, 
             first_launch_date 
             from grp_gdoop_bizops_db.sh_new_existing
             where is_new = 'new'
             group by merchant_uuid, first_launch_date) as b on live.merchant_uuid = b.merchant_uuid
;













		
select country_code from sandbox.pai_deals group by 1
		
  left join 
  	(select 
   		day_rw
	from user_dw.v_dim_day 
	where day_rw >= '2022-01-01' and day_rw < '2023-01-01') 


select * from user_groupondw.active_deals sold_out;

select * from sandbox.sh_new_existing sample 10;



select 
    deal_from_CB_flag, 
    sum(nob) nob
from sandbox.np_temp_fgt as fgt
left join launched_deals as b on fgt.deal_uuid = b.deal_uuid
group by 1;


select * 
from sandbox.np_temp_fgt where merchant_launch_category= 'no_launch_data' order by nob desc;


select sum(nob)
from sandbox.np_temp_fgt;

select merchant_launch_category, count(1), cast(sum(nob) as integer)
from sandbox.np_temp_fgt
group by 1;

select * from sandbox.pai_merchants where merchant_uuid  = '58eb81d4-b4ec-4c21-b018-86c0c74e1258'


/*
 * 
 * 
	case when sf_lead_campaign_name like all ('TXN1%','%DIS%') then 'Display'
		when sf_lead_campaign_name like all ('TXN1%','%SEMN%') then 'SEM Non Brand'
		when sf_lead_campaign_name like all ('TXN1%','%SEMB%') then 'SEM Brand'
		when sf_lead_campaign_name like all ('TXN1%','%EVNT%') then 'Event Testing'
		when sf_lead_campaign_name like all ('TXN1%','%GPMC%') then 'Google Performance Max'
		when sf_lead_campaign_name like all ('TXN1%','%VID%') then 'Connected TV'
		when sf_lead_campaign_name like all ('TXN1%','%_CANDACE%') then 'Candace Holyfield Referral'
	end as campaign_category
	
*/
 */

