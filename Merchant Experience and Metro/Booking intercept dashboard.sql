create volatile multiset table nvp_temp_task as (
select 
      a.SF_Account_18,
      min(touch.ActivityDate) first_touch,
      count(distinct case when touch.ActivityDate >= a.event_date then concat(touch.ActivityDate, touch.event_type) end) touches_made
      from 
      (select SF_Account_18, event_date from sandbox.nvp_booking_intercept_dash group by 1,2) as a 
      join 
      (select 
          AccountId, 
          ActivityDate,
          tk."Type" event_type
        from 
          dwh_base_sec_view.sf_task as tk
        where tk."Type" in ('Email - Outbound', 'Call - Outbound')
        ) as touch on a.SF_Account_18 = touch.AccountID
        group by 1
) with data on commit preserve rows;




delete from sandbox.nvp_booking_intercept_dash;
insert into sandbox.nvp_booking_intercept_dash
select
   a.*,
   b.name as merchant_name,
   b.website,
   SUBSTR(b.salesforce_account_id,1,15) SF_Account_15,
   b.salesforce_account_id SF_Account_18,
   c.merchant_segmentation,
   c.account_owner,
   c.BillingCity as City,
   c.BillingState as State,
   c.BillingCountry as Country,
   op.Opportunity_ID,
   op.Primary_Deal_Services,
   op.Category,
   op.Category_v3,
   ad.mn_load_date first_deal_live_date,
   bt_.mn_bt_load_date first_bt_live_date,
   op.deal_uuid
from
    (select merchant_id,
       history_data.JSONExtractValue('$.restricted.bCookie') as bCookie,
       history_data.JSONExtractValue('$.additionalInfo.tool') as tool,
       history_data.JSONExtractValue('$.additionalInfo.reachout') as reachout,
       history_data.JSONExtractValue('$.additionalInfo.take_booking') as take_booking,
       event_date
    from sb_merchant_experience.history_event
    where event_type = 'BOOKING_SURVEY') as a
left join
     user_edwprod.dim_merchant as b on a.merchant_id = b.merchant_uuid
left join
     (select Account_ID_18,
            BillingCity,
            BillingCountry,
            BillingState,
            sfa.Merchant_Segmentation__c merchant_segmentation,
            max(full_name) account_owner
        from dwh_base_sec_view.sf_account sfa
        left join
             user_groupondw.dim_sf_person sfp on sfa.ownerid = sfp.person_id
        group by 1,2,3,4,5
        ) as c on b.salesforce_account_id = c.Account_ID_18
left join
    (select
         sf1.Opportunity_ID,
         sf1.AccountId,
         sf1.Primary_Deal_Services,
         sf2.deal_uuid deal_uuid,
         sf1.CreatedDate,----later if needed
         sf1.Category, 
         sf1.Category_v3
         from dwh_base_sec_view.sf_opportunity_1 sf1
         join user_edwprod.sf_opportunity_2 sf2 on sf1.id = sf2.id
         group by 1,2,3,4,5,6,7
     ) as op on c.Account_ID_18 = op.AccountID
left join
     (select
          deal_uuid,
          min(cast(load_date as date)) mn_load_date
      from user_groupondw.active_deals
      where
         sold_out = 'false'
         and available_qty > 0
      group by 1
     ) as ad on op.deal_uuid = ad.deal_uuid
left join 
     (select 
         deal_uuid, 
          min(load_date) mn_bt_load_date
       from sandbox.sh_bt_active_deals_log_v4
       where 
         is_bookable = 1 
         and partner_inactive_flag = 0 
         and product_is_active_flag = 1
       group by 1) as bt_ on op.deal_uuid = bt_.deal_uuid;

grant select on sandbox.nvp_booking_intercept_dash to public;



create volatile multiset table nvp_temp_task as (
select 
      a.SF_Account_18,
      min(touch.ActivityDate) first_touch,
      count(distinct case when touch.ActivityDate >= a.event_date then concat(touch.ActivityDate, touch.event_type) end) touches_made
      from 
      (select SF_Account_18, event_date from sandbox.nvp_booking_intercept_dash group by 1,2) as a 
      join 
      (select 
          AccountId, 
          ActivityDate,
          tk."Type" event_type
        from 
          dwh_base_sec_view.sf_task as tk
        where tk."Type" in ('Email - Outbound', 'Call - Outbound')
        ) as touch on a.SF_Account_18 = touch.AccountID
        group by 1
) with data on commit preserve rows;

delete from sandbox.nvp_booking_intercept_dash_funnel;
insert into sandbox.nvp_booking_intercept_dash_funnel
select 
  a.*
from
(select 
   cast(event_date as date) event_date,
   count(distinct merchant_id) total_merchants,
   count(distinct case when first_deal_live_date is not null then merchant_id end ) live_merchants,
   count(distinct case when first_bt_live_date is not null then merchant_id end) live_bt_merchants,
   count(distinct case when take_booking = 'Yes' then merchant_id end) yes_take_booking,
   count(distinct 
            case when take_booking = 'Yes'
            and reachout = 'Yes put me in touch with my Account Manager.'
            then merchant_id end) a_yes_reachout,
   count(distinct 
            case when take_booking = 'Yes'
            and reachout = 'Yes put me in touch with my Account Manager.'
            and task.touches_made >= 1
            then merchant_id end) a_1account_touch,
   count(distinct 
            case when take_booking = 'Yes'
            and reachout = 'Yes put me in touch with my Account Manager.'
            and task.touches_made >= 4
            then merchant_id end) a_4account_touch,
   count(distinct case when take_booking = 'No, but maybe in future' then merchant_id end) nobut_mayb_in_future,
   count(distinct 
            case when take_booking = 'No, but maybe in future' 
            and reachout = 'Yes put me in touch with my Account Manager.' 
            then merchant_id end) b_yes_reachout,
   count(distinct 
            case when take_booking = 'No, but maybe in future' 
            and reachout = 'Yes put me in touch with my Account Manager.'
            and task.touches_made >= 1  then merchant_id end) b_1account_touch,
   count(distinct 
            case when take_booking = 'No, but maybe in future' 
            and reachout = 'Yes put me in touch with my Account Manager.'
            and task.touches_made >= 4  then merchant_id end) b_4account_touch,
   count(distinct case when take_booking = 'No, and i don''t plan to' then merchant_id end) noand_idont_plan_to,
   count(distinct 
            case when take_booking = 'No, and i don''t plan to' 
            and reachout = 'Yes put me in touch with my Account Manager.' 
            then merchant_id end) c_yes_reachout, 
   count(distinct 
            case when take_booking = 'No, and i don''t plan to'
            and reachout = 'Yes put me in touch with my Account Manager.'
            and task.touches_made >= 1 then merchant_id end) c_1account_touch,
   count(distinct 
            case when take_booking = 'No, and i don''t plan to'
            and reachout = 'Yes put me in touch with my Account Manager.'
            and task.touches_made >= 4 then merchant_id end) c_4account_touch
   from 
       sandbox.nvp_booking_intercept_dash as dash
       left join nvp_temp_task as task on dash.SF_Account_18 = task.SF_Account_18
   group by 1) as a
;
grant select on sandbox.nvp_booking_intercept_dash_funnel to public;



delete from sandbox.nvp_booking_intercept_category_funnel;
insert into sandbox.nvp_booking_intercept_category_funnel
select 
   Category_v3,
   cast(event_date as date) event_date,
   count(distinct merchant_id) total_merchants,
   count(distinct case when first_deal_live_date is not null then merchant_id end ) live_merchants,
   count(distinct case when first_bt_live_date is not null then merchant_id end) live_bt_merchants,
   count(distinct case when take_booking = 'Yes' then merchant_id end) yes_take_booking,
   count(distinct 
            case when take_booking = 'Yes'
            and reachout = 'Yes put me in touch with my Account Manager.'
            then merchant_id end) a_yes_reachout,
   count(distinct 
            case when take_booking = 'Yes'
            and reachout = 'Yes put me in touch with my Account Manager.'
            and task.touches_made >= 1
            then merchant_id end) a_1account_touch,
   count(distinct 
            case when take_booking = 'Yes'
            and reachout = 'Yes put me in touch with my Account Manager.'
            and task.touches_made >= 4
            then merchant_id end) a_4account_touch,
   count(distinct case when take_booking = 'No, but maybe in future' then merchant_id end) nobut_mayb_in_future,
   count(distinct 
            case when take_booking = 'No, but maybe in future' 
            and reachout = 'Yes put me in touch with my Account Manager.' 
            then merchant_id end) b_yes_reachout,
   count(distinct 
            case when take_booking = 'No, but maybe in future' 
            and reachout = 'Yes put me in touch with my Account Manager.'
            and task.touches_made >= 1  then merchant_id end) b_1account_touch,
   count(distinct 
            case when take_booking = 'No, but maybe in future' 
            and reachout = 'Yes put me in touch with my Account Manager.'
            and task.touches_made >= 4  then merchant_id end) b_4account_touch,
   count(distinct case when take_booking = 'No, and i don''t plan to' then merchant_id end) noand_idont_plan_to,
   count(distinct 
            case when take_booking = 'No, and i don''t plan to' 
            and reachout = 'Yes put me in touch with my Account Manager.' 
            then merchant_id end) c_yes_reachout, 
   count(distinct 
            case when take_booking = 'No, and i don''t plan to'
            and reachout = 'Yes put me in touch with my Account Manager.'
            and task.touches_made >= 1 then merchant_id end) c_1account_touch,
   count(distinct 
            case when take_booking = 'No, and i don''t plan to'
            and reachout = 'Yes put me in touch with my Account Manager.'
            and task.touches_made >= 4 then merchant_id end) c_4account_touch
   from 
       sandbox.nvp_booking_intercept_dash as dash
       left join nvp_temp_task as task on dash.SF_Account_18 = task.SF_Account_18
   group by 1,2;


grant select on sandbox.nvp_booking_intercept_category_funnel to public; 

----------------------------------------------
drop table nvp_book;
create volatile multiset table nvp_book as (
select 
   fin.*, 
   case when touched = 0 then 1 end not_touched_but_bookable
   from(
select 
     merchant_id,
     dash.SF_Account_18,
     touches_made,
     case when touches_made is not null then 1 else 0 end touched,
     case when first_bt_live_date is not null then 1 else 0 end bookable
     from 
       sandbox.nvp_booking_intercept_dash as dash
     left join nvp_temp_task as task on dash.SF_Account_18 = task.SF_Account_18
       ) fin where bookable = 1) with data on commit preserve rows;
      
drop table nvp_book;

select not_touched_but_bookable, count(1) from nvp_book group by 1;


---------TEST AREA
select 
   AccountId,
   count(distinct a.Opportunity_ID) distinct_opps, 
   count(distinct Primary_Deal_Services) pds, 
   count(distinct deal_uuid) deal_uuid2
   from 
(select
         sf1.Opportunity_ID,
         sf1.AccountId,
         sf1.Primary_Deal_Services,
         sf2.deal_uuid deal_uuid,
         sf1.CreatedDate----later if needed
         from dwh_base_sec_view.sf_opportunity_1 sf1
         join user_edwprod.sf_opportunity_2 sf2 on sf1.id = sf2.id
         group by 1,2,3,4,5) as a
group by AccountID
;

select Opportunity_ID, 
   count(distinct Primary_Deal_Services) pds from
(select
         sf1.Opportunity_ID,
         sf1.AccountId,
         sf1.Primary_Deal_Services,
         sf2.deal_uuid deal_uuid,
         sf1.CreatedDate----later if needed
         from dwh_base_sec_view.sf_opportunity_1 sf1
         join user_edwprod.sf_opportunity_2 sf2 on sf1.id = sf2.id
         group by 1,2,3,4,5) as a
         group by 1
having pds > 1;



 
 select 
 AccountID,
 count(distinct Opportunity_ID) opp
 from dwh_base_sec_view.sf_opportunity_1
 group by 1
 having opp > 1
 


select close_date, count(distinct accountid) 
from sandbox.jc_merchant_mtd_attrib 
where dmapi_flag = 1 and close_date >= '2020-11-12' and close_order = 1
group by close_date
order by close_date;




{"restricted": 
       {"bCookie": "89b6c1a8-2dd2-9a06-741f-87bdd48343ce", 
       "ipAddress": "108.16.236.246", 
       "requestId": "63d9d4b1-fbf5-4061-9ff8-036e30f9ef16"}, 
 "additionalInfo": 
       {"reachout": "No thanks.", 
       "timestamp": "2020-11-24T16:31:08.354Z", 
       "take_booking": "No, but maybe in future", 
       "merchant_uuid": "7aaec662-0a1f-4f50-b3d8-0327136694e2"}}
       
       
       {"restricted": 
       				{"bCookie": "6eadcfdf-e046-5b8e-2319-ab57c1f96c11", 
       				"ipAddress": "47.154.203.29", 
       				"requestId": "ccd1b064-46a4-4e4e-ba15-c508e3401dbe"}, 
       	"additionalInfo": 
       				{"tool": "Digital calendar (Google Calendar, iCal, others)", 
       				"reachout": "No thanks.", 
       				"timestamp": "2020-11-24T05:47:40.362Z", 
       				"take_booking": "Yes", 
       				"merchant_uuid": "1acc24b7-61e1-42a3-88ad-6471fec35066"}}