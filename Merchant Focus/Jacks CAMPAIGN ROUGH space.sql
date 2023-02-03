select * from grp_gdoop_bizops_db.sg_lifecycle_merchant_journeys_v;
select * from grp_gdoop_bizops_db.sfmc_emailengagement;
select * from grp_gdoop_bizops_db.sg_merchant_event_agg_v;

select * from grp_gdoop_bizops_db.sg_lifecycle_monthly_deals_v;

select count(1)
from grp_gdoop_bizops_db.np_mm_fin_tabl;


select *
from sandbox.pai_merchant_center_visits 
where
eventdate > CURRENT_DATE - 15
and bcookie  = 'ad6dfa97-612d-8603-e178-121ded57f0cb'
sample 5;

select * from grp_gdoop_bizops_db.np_highvalue_mc 
where fullurl like '%DIRM%' 
order by eventtime asc;

drop table grp_gdoop_bizops_db.np_highvalue_mc;
create table grp_gdoop_bizops_db.np_highvalue_mc stored as orc as 
select
    bcookie,
    merchantid,
    event, 
    eventdestination,
    rawpagetype,
    widgetname,
    campaign, 
    extrainfo,
    fullurl,
    url,
    consumeridsource,
    eventdate, 
    eventtime
from
    grp_gdoop_pde.junoHourly
where
    eventdate in ('2022-09-21', '2022-09-22', '2022-09-23')  
    and clientplatform = 'web'
    and country = 'US'
    and bcookie = 'ad6dfa97-612d-8603-e178-121ded57f0cb';



select *
from grp_gdoop_pde.junohourly
 where eventdate ='2022-09-21'
 and event= 'genericPageView' 
 and eventdestination ='searchBrowseView'
 and bcookie ='ad6dfa97-612d-8603-e178-121ded57f0cb'
 and campaign like '%DIRM%'
;
 
select *
from grp_gdoop_pde.junohourly
 where bcookie ='ad6dfa97-612d-8603-e178-121ded57f0cb'
 and event= 'genericPageView' 
 and campaign='TXN1_US_All_DIRM_URL_IMG1'
 and eventdate = ;


select *
from grp_gdoop_pde.junohourly
 where bcookie ='ad6dfa97-612d-8603-e178-121ded57f0cb'
 and event= 'genericPageView' 
 and campaign= 'TXN1_US_ALL_SEMB_GEN_EX_GOOG_TXT3'
 and eventdate = '2022-10-04';



select * from grp_gdoop_bizops_db.np_highvalue_mc2 where url is not null and url like '%DIRM%';
   
drop table grp_gdoop_bizops_db.np_highvalue_mc2;
create table grp_gdoop_bizops_db.np_highvalue_mc2 stored as orc as 
select
    bcookie,
    consumerid, 
    merchantid,
    event, 
    eventdestination,
    rawpagetype,
    widgetname,
    extrainfo,
    url,
    eventdate, 
    eventtime
from
    grp_gdoop_pde.junoHourly
where
    eventdate in ('2022-09-21' and 
    and clientplatform = 'web'
    and country = 'US'
    and bcookie = 'ad6dfa97-612d-8603-e178-121ded57f0cb';
   
select *
from grp_gdoop_pde.junohourly
 where bcookie ='ad6dfa97-612d-8603-e178-121ded57f0cb'
 and event= 'genericPageView' 
 and campaign='TXN1_US_All_DIRM_URL_IMG1'

 
select * from grp_gdoop_bizops_db.np_highvalue_mc;
select * from grp_gdoop_bizops_db.np_highvalue_mc2 order by eventtime;

select 
*
from 
(select 
 merchant_uuid, 
 journeyname,
 count(distinct concat(journeyname, emailname)) xyz
from grp_gdoop_bizops_db.np_mm_fin_tabl_grn
where week_end_send = '2022-07-31'
group by 1,2)
where xyz > 1


select 
 *
from grp_gdoop_bizops_db.np_mm_fin_tabl_grn
where week_end_send = '2022-07-31'
and merchant_uuid = 'bc5308ab-ffef-46ea-bd72-a186c963fdd5'
and journeyname = 'MM_LeadNurture_DealDropOff_NA'
order by emailname;


select * from grp_gdoop_bizops_db.np_mm_fin_tabl where merchant_uuid = '839dd5dd-31bb-4cba-b7e3-a0f5ce54fccd' order by merchant_uuid,sent_date, order_date;

select 
    merchant_uuid, 
    count(distinct sent_date) sds, 
    count(distinct order_date), 
    count(distinct case when order_date is null then sent_date end)
from grp_gdoop_bizops_db.np_mm_fin_tabl group by merchant_uuid   order by 4 desc;

select from grp_gdoop_bizops_db.np_mm_fin_tabl;

select * from grp_gdoop_bizops_db.np_mm_fin_tabl;


select 
*
from 
(select accountid, count(1) xyz 
from grp_gdoop_sup_analytics_db.jc_w2l_mktg_acct_attrib
group by 1)
where xyz > 1; --- mkt on mkt.accountid = pm.account_id

select * from grp_gdoop_sup_analytics_db.jc_w2l_mktg_acct_attrib where accountid = '0013c000021GWG4AAO';

select * from edwprod.fact_gbl_transactions limit 4;

select * from grp_gdoop_bizops_db.sg_merchant_event_agg_v where event <> 'mc_visit';-- pma on pma.merchant_id = pm.merchant_uuid

select * from grp_gdoop_bizops_db.pai_merchant_center_visits where utm_campaign is not null;

create table grp_gdoop_bizops_db.np_mm_financial_base stored as orc as
select 
    a.*
from 
(select 
   date_format(order_date,'yyyy-MM-dd') order_date, 
   merchant_uuid, 
   sum(auth_nob_loc) nob, 
   sum(auth_nor_loc) nor
from edwprod.fact_gbl_transactions 
where 
   cast(order_date as date) >= cast('2021-01-01' as date)
   action = 'authorize'
group by 1,2) as a 
join 
(select 
merchant_uuid, 
date_format(min(sent_date),'yyyy-MM-dd') min_sent_date 
from grp_gdoop_bizops_db.np_mm_email_base
group by merchant_uuid) as b on a.merchant_uuid = b.merchant_uuid and a.order_date >= date_add(min_sent_date, -30)
;



drop table grp_gdoop_bizops_db.np_hp_imp_temp;
create table grp_gdoop_bizops_db.np_hp_imp_temp stored as orc as 
select 
  eventdate,
  eventtime,
  bcookie,
  dealuuid, 
  dealpermalink,
  extrainfo,
  get_json_object(extrainfo, '$.collectionCardTitleText') cct,
  get_json_object(extrainfo, '$.collectionCardName') ccn
from grp_gdoop_pde.junoHourly 
where eventdate >= '2022-07-01' and eventdate <= '2022-07-31'
and eventdestination = 'dealImpression'
and event = 'dealImpression'
and rawevent = 'GRP2'
and country in ('US')
and platform = 'mobile'
and lower(trim(regexp_replace(clientplatform,'\\t|\\n|\\r|\\u0001', ''))) = 'iphone'
and (lower(trim(extrainfo)) like '%crosschannel_homepage_sponsored_carousel%'
     or lower(trim(extrainfo)) like '%homepage_relevance-feed_deal_carousel%'
     or lower(trim(extrainfo)) like '%homepage_richrelevance_rr_recs%'
     )
;

select * from grp_gdoop_bizops_db.np_hp_imp_temp;

    
{"attributionId":"00000000-0000-0000-0000-000005528154","card_search_uuid":"00000000-0000-0000-0000-000005528154","deal_status":"available","dealServices":[],"hasTopRatedBadge":false,"hasTopRatedMerchantMedal":true,"idfv":"90B55DA1-5259-4D1F-9CC5-41F54B2B6871","IsCLO":"off","isD3CExperimentOn":true,"isD3CExperimentOn_3PIP":true,"isD3CExperimentOn_Booking":true,"isD3CExperimentOn_Goods":false,"isFullMenu":"false","merchantId":"peachy-airport-parking","oneClickClaimEnabled":"false",
"rating":"4.790000","screen_instance_id":"DEF5BCB9-7F04-4834-AB77-6A13F0CD4B73_1658450006522","tabName":"home_tab","UMSText":"Selling fast!"}


{"cardPermalink":"cardatron:mobile:home-cx90:US",
"collectionCardName":"homepage_Relevance-Feed_Deal_Carousel","collectionCardPosition":"2",
"collectionCardTitleText":"Trending Deals for You","collectionCardUUID":"2a787d77-48b5-4071-886f-4ba2f2802ef8",
"collectionTemplateId":"d2cd6438-58b3-4e55-b3fc-6942f0e8da64","collectionTemplateView":"HorizontalTwoUpCompoundCardView",
"idfv":"8F50DE54-A4E3-4187-B9EF-B07AF84361C6","isD3CExperimentOn":true,"isD3CExperimentOn_3PIP":true,"isD3CExperimentOn_Booking":true,
"isD3CExperimentOn_Goods":false,"screen_instance_id":"DEF5BCB9-7F04-4834-AB77-6A13F0CD4B73_1658976749801","wolfhoundPageId":"","wolfhoundPageUrl":""}

---------------ORDERS
drop table grp_gdoop_bizops_db.np_hp_ord_temp;
create table grp_gdoop_bizops_db.np_hp_ord_temp stored as orc as 
select 
    a.eventdate,
    a.bcookie, 
    a.dealuuid, 
    a.cct, 
    a.ccn,
    b.parent_order_uuid, 
    b.transaction_qty, 
    b.auth_nob_loc
from grp_gdoop_bizops_db.np_hp_imp_temp as a 
join edwprod.fact_gbl_transactions as b on lower(a.bcookie) = lower(b.bcookie) and a.eventdate = b.transaction_date and a.dealuuid = b.deal_uuid
where b.transaction_date >= '2022-07-01' and b.transaction_date <= '2022-07-31'
and b.country_id  = 235 and b.action = 'authorize';




----ROUGH DEAL WISE CLICKS

create table grp_gdoop_bizops_db.np_hp_dlv_temp stored as orc as 
select 
    a.eventdate,
    a.bcookie, 
    a.dealuuid, 
    a.cct, 
    a.ccn
from grp_gdoop_bizops_db.np_hp_imp_temp as a 
join 
   (select 
     eventdate,
     eventtime,
     bcookie,
     dealuuid
    from grp_gdoop_pde.junoHourly 
     where eventdate >= '2022-07-01' and eventdate <= '2022-07-31'
        and eventdestination = 'searchBrowseView'
        and event = 'dealView'
        and  rawevent = 'GRP3'
        and country in ('US')
        and platform = 'mobile'
        and lower(trim(regexp_replace(clientplatform,'\\t|\\n|\\r|\\u0001', ''))) = 'iphone'
        and get_json_object(extrainfo, '$.tabName') = 'home_tab'
        ) 
as b on lower(a.bcookie) = lower(b.bcookie) and a.eventdate = b.eventdate and a.dealuuid = b.dealuuid

{"attributionId":"","badgeDisplayText":"Takeout","badgeType":"TAKEOUT","bookable_deal":"off","card_search_uuid":"","deal_status":"available","default_deal_option_UUID":"b868c167-24fc-473e-bd1c-7e44be4d8c8a","gia":"off","idfv":"E1D760FC-3C00-4EB4-B5A9-82990D921DB2","IsCLO":"off","isD3CExperimentOn":true,"isD3CExperimentOn_3PIP":true,"isD3CExperimentOn_Booking":true,
"isD3CExperimentOn_Goods":false,"offerType":"SALE","opened_via_iMessage":"false","rating":"4.810000","tabName":"home_tab"}


{"cardPermalink":"cardatron:mobile:home-cx90:US",
"collectionCardName":"homepage_Relevance-Feed_Deal_Carousel","collectionCardPosition":"2",
"collectionCardTitleText":"Trending Deals for You",
"collectionCardUUID":"2a787d77-48b5-4071-886f-4ba2f2802ef8",
"collectionTemplateId":"d2cd6438-58b3-4e55-b3fc-6942f0e8da64",
"collectionTemplateView":"HorizontalTwoUpCompoundCardView",
"idfv":"70890216-FA39-4BD7-8A3A-98CDD4FA25B9","isD3CExperimentOn":true,
"isD3CExperimentOn_3PIP":true,"isD3CExperimentOn_Booking":true,
"isD3CExperimentOn_Goods":false,"wolfhoundPageId":"","wolfhoundPageUrl":""}

{"cardPermalink":"cardatron:mobile:home-cx90:US","collectionCardName":"homepage_Relevance-Feed_Deal_Carousel",
"collectionCardPosition":"2","collectionCardTitleText":"Trending Deals for You",
"collectionCardUUID":"2a787d77-48b5-4071-886f-4ba2f2802ef8",
"collectionTemplateId":"d2cd6438-58b3-4e55-b3fc-6942f0e8da64",
"collectionTemplateView":"HorizontalTwoUpCompoundCardView","idfv":"998CDAB6-36BF-45BA-BAC0-CF6A6849F0F0",
"isD3CExperimentOn":true,"isD3CExperimentOn_3PIP":true,"isD3CExperimentOn_Booking":true,"isD3CExperimentOn_Goods":false,
"screen_instance_id":"DEF5BCB9-7F04-4834-AB77-6A13F0CD4B73_1656634382333","wolfhoundPageId":"","wolfhoundPageUrl":""}

crossChannel_homepage_sponsored_carousel
homepage_richrelevance_rr_recs1 -  "Recently Viewed Deals"
homepage_richrelevance_rr_recs2 - "People With Similar Interests Viewed"



------------CLICKS

drop table grp_gdoop_bizops_db.np_hp_clk_temp;
create table grp_gdoop_bizops_db.np_hp_clk_temp stored as orc as 
select 
  eventdate,
  eventtime,
  bcookie,
  dealuuid, 
  dealpermalink,
  extrainfo,
  get_json_object(extrainfo, '$.collectionCardTitleText') cct,
  get_json_object(extrainfo, '$.collectionCardName') ccn
from grp_gdoop_pde.junoHourly 
where eventdate >= '2022-07-01' and eventdate <= '2022-07-31'
and  rawevent = 'GRP17'
and lower(eventdestination) = 'genericclick'
and lower(event) = 'genericclick'
and country in ('US')
and platform = 'mobile'
and lower(trim(regexp_replace(clientplatform,'\\t|\\n|\\r|\\u0001', ''))) = 'iphone'
and (lower(trim(extrainfo)) like '%crosschannel_homepage_sponsored_carousel%'
     or lower(trim(extrainfo)) like '%homepage_relevance-feed_deal_carousel%'
     or lower(trim(extrainfo)) like '%homepage_richrelevance_rr_recs%'
     );


select * from grp_gdoop_bizops_db.np_hp_clk_temp;  
{"cardPermalink":"cardatron:mobile:home:US",
"collectionCardName":"homepage_richrelevance_rr_recs1","collectionCardPosition":"3",
"collectionCardTitleText":"Recently Viewed Deals","collectionCardUUID":"941d2a63-437c-4c27-aeb6-c33e60d3ab30","collectionTemplateId":"61ffaa1a-d3da-4232-95bc-81d9d18643fe","collectionTemplateView":"HorizontalCompoundCardEndTileView","idfv":"D09D4516-5010-44E9-AE2E-CE7DA0F4ACAE","isD3CExperimentOn":true,"isD3CExperimentOn_3PIP":true,"isD3CExperimentOn_Booking":true,"isD3CExperimentOn_Goods":false,
"positionDeal":"1","screen_instance_id":"DEF5BCB9-7F04-4834-AB77-6A13F0CD4B73_1658282710394","wolfhoundPageId":"","wolfhoundPageUrl":""}
    
{"cardPermalink":"cardatron:mobile:home-cx90:US","collectionCardName":"crossChannel_homepage_sponsored_carousel","collectionCardPosition":"3","collectionCardTitleText":"Featured","collectionCardUUID":"12ebbd28-3a95-4823-9258-65eb7e57d0e3","collectionTemplateId":"d2cd6438-58b3-4e55-b3fc-6942f0e8da64","collectionTemplateView":"HorizontalTwoUpCompoundCardView","idfv":"48F437E0-9CB6-4C49-B3B8-23034A4BBD34","isD3CExperimentOn":true,"isD3CExperimentOn_3PIP":true,"isD3CExperimentOn_Booking":true,"isD3CExperimentOn_Goods":false,
"positionDeal":"3","screen_instance_id":"DEF5BCB9-7F04-4834-AB77-6A13F0CD4B73_1658284848624","wolfhoundPageId":"","wolfhoundPageUrl":""}

select 
   a.*, 
   b.ogp
from 
grp_gdoop_bizops_db.np_hp_ord_temp as a 
  left join ( 
      select 
      parent_order_uuid,
      sum(total_estimated_ogp_loc_usd) ogp
      from edwprod.fact_gbl_ogp_transactions 
      where 
         action = 'authorize' 
         and substr(transaction_date_ts, 1,10) >= '2022-07-01' 
         and substr(transaction_date_ts, 1,10) <= '2022-07-31'
      group by 1)  as b on a.parent_order_uuid = b.parent_order_uuid

select 
   a.*, 
   b.clks,
   c.total_orders,
   c.total_units,
   c.nob,
   c.ogp,
   d.imps imps2,
   d.deal_imos deal_imps2
from 
(select
    ccn,
    cct,
    count(distinct concat(eventdate,bcookie)) imps,
    count(distinct concat(eventdate, bcookie, dealuuid)) deal_imps
from grp_gdoop_bizops_db.np_hp_imp_temp
group by 1,2) as a 
left join 
(select
    ccn,
    cct,
    count(distinct concat(eventdate,bcookie)) clks
from grp_gdoop_bizops_db.np_hp_clk_temp
group by 1,2) as b on a.cct = b.cct and a.ccn = b.ccn
left join 
(select 
    ccn, 
    cct, 
    count(distinct a.parent_order_uuid) total_orders, 
    sum(transaction_qty) total_units, 
    sum(auth_nob_loc) nob,
    sum(ogp) ogp
  from 
  grp_gdoop_bizops_db.np_hp_ord_temp as a 
  left join ( 
      select 
      parent_order_uuid,
      sum(total_estimated_ogp_loc_usd) ogp
      from edwprod.fact_gbl_ogp_transactions 
      where 
         action = 'authorize' 
         and substr(transaction_date_ts, 1,10) >= '2022-07-01' 
         and substr(transaction_date_ts, 1,10) <= '2022-07-31'
      group by 1)  as b on a.parent_order_uuid = b.parent_order_uuid
  group by 1,2
) as c on a.cct = c.cct and a.ccn = c.ccn
left join 
(select
    ccn,
    cct,
    count(distinct concat(eventdate,bcookie)) imps,
    count(distinct concat(eventdate, bcookie, dealuuid)) deal_imos
from grp_gdoop_bizops_db.np_hp_dlv_temp
group by 1,2) as d on a.cct = d.cct and a.ccn = d.ccn

select
    *
from grp_gdoop_bizops_db.np_hp_ord_temp;

select
    ccn,
    count(distinct concat(eventdate,bcookie)) clks
from grp_gdoop_bizops_db.np_hp_clk_temp
group by 1
order by 1;







select * 
from sandbox.pai_merchant_center_visits 
where 
eventdate > CURRENT_DATE - 15 
and bcookie  = 'ad6dfa97-612d-8603-e178-121ded57f0cb'
sample 5;




{"cardPermalink":"cardatron:mobile:home-cx90:US",
"collectionCardName":"homepage_Relevance-Feed_Deal_Carousel","collectionCardPosition":"2",
"collectionCardTitleText":"Trending Deals for You","collectionCardUUID":"2a787d77-48b5-4071-886f-4ba2f2802ef8",
"collectionTemplateId":"d2cd6438-58b3-4e55-b3fc-6942f0e8da64","collectionTemplateView":"HorizontalTwoUpCompoundCardView",
"idfv":"C479D68E-1B37-4B22-8F44-A42674378933","isD3CExperimentOn":true,"isD3CExperimentOn_3PIP":true,"isD3CExperimentOn_Booking":true,
"isD3CExperimentOn_Goods":false,
"positionDeal":"0","screen_instance_id":"DEF5BCB9-7F04-4834-AB77-6A13F0CD4B73_1656698579101","wolfhoundPageId":"","wolfhoundPageUrl":""}

{"cardPermalink":"cardatron:mobile:home-cx90:US",
"collectionCardName":"homepage_Relevance-Feed_Deal_Carousel",
"collectionCardPosition":"1","collectionCardTitleText":"Trending Deals for You",
"collectionCardUuid":"2a787d77-48b5-4071-886f-4ba2f2802ef8","collectionTemplateId":"d2cd6438-58b3-4e55-b3fc-6942f0e8da64",
"collectionTemplateView":"HorizontalTwoUpCompoundCardView","idfv":"BA505E96-9C9F-4714-9630-F6253A9BB7CA","isD3CExperimentOn":true,"isD3CExperimentOn_3PIP":true,
"isD3CExperimentOn_Booking":true,"isD3CExperimentOn_Goods":false,"positionDeal":"2","wolfhoundPageId":"","wolfhoundPageUrl":""}


and lower(impressiontype) = 'collection_card_impression'
and platform = 'mobile'
and lower(trim(extrainfo)) like '%dealpage%'
and lower(trim(regexp_replace(clientplatform,'\\t|\\n|\\r|\\u0001', ''))) in ('iphone', 'android')
and (lower(trim(extrainfo)) like '%mobile_rr_-item_page_recs1%' 
     or lower(trim(extrainfo)) like '%mobile_rr_-item_page_recs2%');

{"apple_instant_pay":"visible","bookable_deal":"off","deal_status":"available",
"dealId":"gl-chicago-architecture-boat-tours-2022","dealUUID":"7d3db6e5-bf45-4237-89bc-40f4a8756488",
"gesture_to_buy":"instant_buy_not_visible","gia":"off","IsCLO":"off","rating":"4.730000","tabName":"home_tab"}


--------------OVERALL HOME PAGE IMPRESSION AND CLICKS 
drop table grp_gdoop_bizops_db.np_hp_imp_all;
create table grp_gdoop_bizops_db.np_hp_imp_all stored as orc as
select 
  eventdate,
  eventtime,
  bcookie,
  dealuuid
from grp_gdoop_pde.junoHourly 
where eventdate >= '2022-07-01' and eventdate <= '2022-07-31'
and eventdestination = 'dealImpression'
and event = 'dealImpression'
and rawevent = 'GRP2'
and country in ('US')
and platform = 'mobile'
and lower(trim(regexp_replace(clientplatform,'\\t|\\n|\\r|\\u0001', ''))) = 'iphone'
and get_json_object(extrainfo, '$.tabName') = 'home_tab';

drop table grp_gdoop_bizops_db.np_hp_clk_all;
create table grp_gdoop_bizops_db.np_hp_clk_all stored as orc as 
select 
  eventdate,
  eventtime,
  bcookie,
  dealuuid
from grp_gdoop_pde.junoHourly 
where eventdate >= '2022-07-01' and eventdate <= '2022-07-31'
and  rawevent = 'GRP17'
and lower(eventdestination) = 'genericclick'
and lower(event) = 'genericclick'
and country in ('US')
and platform = 'mobile'
and lower(trim(regexp_replace(clientplatform,'\\t|\\n|\\r|\\u0001', ''))) = 'iphone'
and get_json_object(extrainfo, '$.tabName') = 'home_tab';

---rough deal view: 

create table grp_gdoop_bizops_db.np_hp_dlv_all stored as orc as 
select 
    a.eventdate,
    a.bcookie, 
    a.dealuuid
from grp_gdoop_bizops_db.np_hp_imp_all as a 
join 
   (select 
     eventdate,
     eventtime,
     bcookie,
     dealuuid
    from grp_gdoop_pde.junoHourly 
     where eventdate >= '2022-07-01' and eventdate <= '2022-07-31'
        and eventdestination = 'searchBrowseView'
        and event = 'dealView'
        and  rawevent = 'GRP3'
        and country in ('US')
        and platform = 'mobile'
        and lower(trim(regexp_replace(clientplatform,'\\t|\\n|\\r|\\u0001', ''))) = 'iphone'
        and get_json_object(extrainfo, '$.tabName') = 'home_tab'
        ) 
as b on lower(a.bcookie) = lower(b.bcookie) and a.eventdate = b.eventdate and a.dealuuid = b.dealuuid

---rough deal orders: 
drop table grp_gdoop_bizops_db.np_hp_ord_all;
create table grp_gdoop_bizops_db.np_hp_ord_all stored as orc as 
select 
    a.eventdate,
    a.bcookie, 
    a.dealuuid, 
    b.parent_order_uuid, 
    b.transaction_qty, 
    b.auth_nob_loc
from grp_gdoop_bizops_db.np_hp_imp_all as a 
join edwprod.fact_gbl_transactions as b on lower(a.bcookie) = lower(b.bcookie) and a.eventdate = b.transaction_date and a.dealuuid = b.deal_uuid
where b.transaction_date >= '2022-07-01' and b.transaction_date <= '2022-07-31'
and b.country_id  = 235 and b.action = 'authorize'





select
    count(distinct concat(eventdate,bcookie)) imps
from grp_gdoop_bizops_db.np_hp_imp_all;

select
    count(distinct concat(eventdate,bcookie)) clks
from grp_gdoop_bizops_db.np_hp_clk_all 

select 
   count(distinct concat(eventdate,bcookie)) imps,
   count(distinct concat(eventdate, bcookie, dealuuid)) deal_imos
from grp_gdoop_bizops_db.np_hp_dlv_all;

select 
  count(distinct a.parent_order_uuid) total_orders, 
  sum(transaction_qty) total_units, 
  sum(auth_nob_loc) nob, 
  sum(ogp) ogp
from 
grp_gdoop_bizops_db.np_hp_ord_all as a 
  left join (
      select 
      parent_order_uuid,
      sum(total_estimated_ogp_loc_usd) ogp
      from edwprod.fact_gbl_ogp_transactions 
      where 
         action = 'authorize' 
         and substr(transaction_date_ts, 1,10) >= '2022-07-01' 
         and substr(transaction_date_ts, 1,10) <= '2022-07-31'
      group by 1)  as b on a.parent_order_uuid = b.parent_order_uuid;

select * from edwprod.agg_gbl_traffic;
select * from edwprod.agg_gbl_impressions_deal where lower(platform) = 'app' and sub_platform  = 'iPhone'

--------------DEAL PAGE VIEW


select 
  eventdate,
  eventtime,
  bcookie,
  rawevent,
  rawpagetype,
  eventdestination, 
  event,
  clicktype,
  widgetcontentname,
  clientplatform, 
  dealuuid, 
  dealpermalink,
  extrainfo, 
  clickmetadata 
from grp_gdoop_pde.junoHourly 
where eventdate = '2022-07-01'
and eventdestination = 'searchBrowseView'
and event = 'dealView'
and  rawevent = 'GRP3'
and country in ('US')
and platform = 'mobile'
and lower(trim(regexp_replace(clientplatform,'\\t|\\n|\\r|\\u0001', ''))) = 'iphone'

{"attributionId":"","badgeDisplayText":"","badgeType":"","bookable_deal":"off","card_search_uuid":"",
"deal_status":"available","default_deal_option_UUID":"2ef35b18-c404-4f31-bbf1-152180fd5a51","gia":"off",
"idfv":"5FBA14B4-FED2-4694-8D5A-6B68172EA500","IsCLO":"off","isD3CExperimentOn":true,"isD3CExperimentOn_3PIP":true,
"isD3CExperimentOn_Booking":true,"isD3CExperimentOn_Goods":false,"opened_via_iMessage":"false","rating":"4.730000","tabName":"home_tab"}

{"attributionId":"00000000-0000-0000-0000-000005522342","badgeDisplayText":"","badgeType":"","bookable_deal":"off",
"card_search_uuid":"00000000-0000-0000-0000-000005522342","deal_status":"available","default_deal_option_UUID":
"2ef35b18-c404-4f31-bbf1-152180fd5a51","gia":"off","idfv":"DBFE20B4-ECF4-48A4-8EC5-F6E601CE2C36","IsCLO":"off","isD3CExperimentOn":true,
"isD3CExperimentOn_3PIP":true,"isD3CExperimentOn_Booking":true,
"isD3CExperimentOn_Goods":false,"opened_via_iMessage":"false","presentation":"list_view","rating":"4.730000","tabName":"home_tab"}

create table grp_gdoop_bizops_db.np_dlpg_temp stored as orc as 
select 
  eventdate,
  eventtime,
  bcookie,
  rawevent,
  rawpagetype,
  eventdestination, 
  event,
  widgetcontentname,
  clientplatform
from grp_gdoop_pde.junoHourly 
where eventdate >= '2022-07-01' and eventdate <= '2022-07-15'
and  rawevent = 'GRP3'
and country in ('US')
and lower(eventdestination) = 'searchbrowseview'
and lower(event) = 'dealview'
and platform = 'mobile'
and lower(trim(regexp_replace(clientplatform,'\\t|\\n|\\r|\\u0001', ''))) = 'iphone';