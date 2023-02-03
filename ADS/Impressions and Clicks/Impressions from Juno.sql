select 
   eventdate,
   eventtime
   bcookie, 
   extrainfo, 
   position,
   case when json_extract_scalar(extrainfo, '$.sponsoredAdId') is not null then 1 else 0 end sponsored
from grp_gdoop_pde.junoHourly 
where 
eventdestination = 'dealImpression'
and event = 'dealImpression'
and eventdate = '2022-03-20'
and rawevent = 'GRP2'
and lower(trim(regexp_replace(clientplatform,'\\t|\\n|\\r|\\u0001', ''))) = 'iphone'
and country in ('US','CA')
and platform = 'mobile'
and bcookie is not null 
and bcookie <> ''
and lower(trim(bcookie)) <> 'null'
and lower(brand) = 'groupon' 
and bcookie not in ('android_automation_test' ,'ios_automation_test','android_perf_test_nightly','ios_perf_test_nightly')
and lower(trim(extrainfo)) like '%categories_tab%' 
and json_extract_scalar(extrainfo, '$.sponsoredAdId') is not null
;


select *
from grp_gdoop_pde.junoHourly 
where 
eventdestination = 'dealImpression'
and eventdate = '2022-03-15'
and lower(trim(regexp_replace(clientplatform,'\\t|\\n|\\r|\\u0001', ''))) = 'desktop'
and rawevent = 'GRP2'
and bcookie is not null 
and bcookie <> ''
and lower(trim(bcookie)) <> 'null'
and country in ('US','CA')
and lower(trim(extrainfo)) like '%sponsored%'
;



case when rawpagetype = 'browse/deals/index' and fullurl like '%context=local%' or fullurl like '%category=%' then 'browse'
           when rawpagetype in ('browse/deals/index') and search_query is not null  and search_query!='' then 'search'
           when rawpagetype in ('homepage' ,'homepage/index', 'featured/deals/index') then 'homepage'
           when rawpagetype in ( 'browse/deals/index') then 'browse' --'featured'
           when rawpagetype in ('nearby/deals/index', --featured? or local only 
                                 'goods/browse/index',
                                 'goods/index',
                                 'giftshop/deals/show',
                                 'giftshop/deals/index',
                                  'channels/show',
                                  'beautynow_promoted',
                                  'beautynow_salon',
                                  'beautynow_appointment_receipt',
                                  'beautynow_SELECT_appointment_time',
                                  'beautynow_SELECT_service') then 'browse'
           when rawpagetype like '%-%-%-%' then 'occasions'
           else rawpagetype;
          


select
   bcookie, 
   eventtime, 
   extrainfo,
   json_extract_scalar(lower(extrainfo), '$.screen_name') category,
   lead(eventtime) over (partition by bcookie order by eventtime) event_end_time
from grp_gdoop_pde.junoHourly 
where 
eventdestination = 'searchBrowseView'
and event = 'genericPageView'
and eventdate = '2022-03-20'
and lower(trim(regexp_replace(clientplatform,'\\t|\\n|\\r|\\u0001', ''))) = 'iphone'
and country in ('US','CA')
and platform = 'mobile'
and bcookie is not null 
and bcookie <> ''
and lower(trim(bcookie)) <> 'null'
and lower(brand) = 'groupon' 
and bcookie not in ('android_automation_test' ,'ios_automation_test','android_perf_test_nightly','ios_perf_test_nightly')
;






{"screen_uuid":"8F13DE8C-2716-4AC3-8A58-3946BA1DE693","screen_name":"Browse | Categories | Goods","division_id":"portland","category_uuid":"db2cb956-fc1a-4d8c-88f2-66657ac41c24"}


{"attributionId":"29fe38ec-1fef-f2c9-78c2-f9b93706fb26","card_search_uuid":"29fe38ec-1fef-f2c9-78c2-f9b93706fb26",
"clicktype":"category_click","deal_status":"available","hasTopRatedBadge":false,
"idfv":"D134FCD7-BCAF-4AD1-82D6-67665C52B492","IsCLO":"off","isD3CExperimentOn":true,
"isD3CExperimentOn_3PIP":true,"isD3CExperimentOn_Booking":true,"isD3CExperimentOn_Goods":false,"oneClickClaimEnabled":"false","rating":"4.000000","screen_instance_id":"8F13DE8C-2716-4AC3-8A58-3946BA1DE693_1639610554771","sponsoredAdId":"display_UcfN1Z3lQ_sZvW3RhEJ3dPax4AIxNGIyNWI3OS0xYzJjLTRhMTgtODc0NS01OGY3NTc0YzBmODU=",
"tabName":"categories_tab","Type":"Baby & Kids","UMSText":"20+ viewed today"}


{"cardPermalink":"cardatron:mobile:home-cx90:us","collectionCardName":"homepage_richrelevance_rr_recs1",
"collectionCardPosition":"3","collectionCardTitleText":"7/10 Gallon Heavy Duty Garden Potato Grow Bags Pots with Handles",
"collectionCardUUID":"941d2a63-437c-4c27-aeb6-c33e60d3ab30","collectionTemplateId":"d2cd6438-58b3-4e55-b3fc-6942f0e8da64",
"collectionTemplateView":"HorizontalTwoUpCompoundCardView"}


{"attributionId":"00000000-0000-0000-0000-000005490647","hasTopRatedBadge":true,
"hasTopRatedMerchantMedal":false,"isCoreCoupon":false,"numberOfOptions":2,
"offerType":"SALE","oneClickClaimEnabled":false,"pricePoint":0,"rating":4.53,
"sponsoredAdId":"display_Mn-T8Exl8OeiX4G6wZmQORe-wiUKJgokNjNiMmY4ZWQtNGZmZi00NzM4LWE0NDEtOTZiZmNhNWM3YTBiEgAaDAiPhL6RBhD_06P8AiICCAE=",
"udcPriceTag":"off",
"umsText":"Selling fast!",
"card_search_UUID":"00000000-0000-0000-0000-000005490647",
"deal_status":"available","isCLO":"off",
"home_tab_cold_start":false,
"placement":4,"time_in_database":0}


{"attributionId":"29fe38e1-6176-b569-3817-cedbafbcf566","card_search_uuid":"29fe38e1-6176-b569-3817-cedbafbcf566","clicktype":"category_click","deal_status":"available","hasTopRatedBadge":false,"idfv":"12EC1FC1-6409-49D5-ABCD-22C1AAEDEA1F","IsCLO":"off","isD3CExperimentOn":true,"isD3CExperimentOn_3PIP":true,"isD3CExperimentOn_Booking":true,"isD3CExperimentOn_Goods":false,"oneClickClaimEnabled":"false","rating":"4.000000","screen_instance_id":"8F13DE8C-2716-4AC3-8A58-3946BA1DE693_1639609709167",
"sponsoredAdId":"display_kj9BtDfWKv4vPka7LohwOnTanfkxNGIyNWI3OS0xYzJjLTRhMTgtODc0NS01OGY3NTc0YzBmODU=","tabName":"categories_tab","Type":"Baby & Kids","UMSText":"20+ viewed today"}


{"cardPermalink":"cardatron:mobile:home-cx90:US","collectionCardName":"crossChannel_homepage_sponsored_carousel","collectionCardPosition":"3",
"collectionCardTitleText":"Featured","collectionCardUUID":"12ebbd28-3a95-4823-9258-65eb7e57d0e3","collectionTemplateId":"d2cd6438-58b3-4e55-b3fc-6942f0e8da64",
"collectionTemplateView":"HorizontalTwoUpCompoundCardView","idfv":"A43A732A-848F-4C16-9609-1CEBECFC7E96","isD3CExperimentOn":true,"isD3CExperimentOn_3PIP":true,
"isD3CExperimentOn_Booking":true,"isD3CExperimentOn_Goods":false,"screen_instance_id":"DEF5BCB9-7F04-4834-AB77-6A13F0CD4B73_1646517912349",
"sponsoredAdId":"display_y9DkFGk7FtRgaIcyL9Ro9siGHHcKJgokNDRjYjY0MGItOTE1MC00OThkLWI5YmQtYThkNWM1YzY3MmExEgAaDAiZ4JSRBhCO0uSMASICCAE=","wolfhoundPageId":"","wolfhoundPageUrl":""}



insert overwrite table grp_gdoop_bizops_db.np_impressions_sl_app partition (eventdate)
   select 
          case when cl.aogcampaignid is not null then 'Sponsored' else 'Organic' end deal_landing,
          trim(lower(regexp_replace(search_query,'\\+',' '))) search_query,  
          cast(cast(position as int )-1 as string) position ,
          ogp,
          nor,
          nob,
          dv_time,
          bcookie,
          order_uuid,
          cl.dealuuid,
          clientplatform,
          aogcampaignid,
          extrainfo,
          case when search_query is not null and search_query!='' and search_query!='All+Deals' then 'search' 
               when (get_json_object(lower(extrainfo), '$.type' ) is null and get_json_object(extrainfo, '$.tabName' ) ='home_tab') then 'homepage' ---or channel='all'
               when rawpagetype in ('wolfhound_mobile_page', 'GlobalSearchResult', 'MapLedSearch') then 'browse'
               else 'other' end page,
          cl.eventdate
     from ai_reporting.sl_imp_clicks_app  cl 
     join (select dealuuid, min(eventdate) min_eventdate, max(eventdate) max_eventdate 
           from grp_gdoop_bizops_db.np_sl_all_deals
           group by dealuuid
           ) dl on cl.dealuuid = dl.dealuuid
     where 
        cl.eventdate >= date_add(current_date, -30)
        and cast(cl.eventdate as date) >= cast(dl.min_eventdate as date)
        and cast(cl.eventdate as date) <= cast(dl.max_eventdate as date)
;






{
  "eventID": 0,
  "distance": 0,
  "channel": "browse",
  "extras": {},
  "clientVersion": "22.6 (436002)",
  "presentation": "list_view",
  "appInstanceId": "89DCFB32D1394ED4A9F948CEF1093F40",
  "_eventfield12": {
    "deal_status": "available",
    "hasTopRatedBadge": false,
    "idfv": "9130B36F-D7B8-4356-B9F2-61F9E9CD7C19",
    "IsCLO": "off",
    "isCoreCoupon": true,
    "isD3CExperimentOn": true,
    "isD3CExperimentOn_3PIP": true,
    "isD3CExperimentOn_Booking": true,
    "isD3CExperimentOn_Goods": false,
    "oneClickClaimEnabled": "false",
    "screen_instance_id": "C7216696-0EBA-460A-B7DA-A09373847060_1648140599657",
    "sponsoredAdId": "display_I0_WEM5rxWB4jaKWcOlr0OWb1agKJgokMzVjM2QyNWYtYzI2Yi00NzM3LTg5YWYtNGUyNmNhYTg5NTFmEgAaDAi45feRBhCppIj5ASICCAE=",
    "tabName": "categories_tab",
    "UMSText": "80+ viewed today"
  },
  
  
  "clientBcookie": "5A2A5B24-5694-3E06-44F7-DFAA76FEB933",
  "clientPlatform": "IPHONECON (iPhone12,1)",
  "clientLocale": "en_US",
  "brand": "groupon",
  "_clientfield7": "US",
  "clientClientID": "e679498bd360fdefd5367e13784d6b94",
  "clientUserAgent": "Groupon/dogfood/22.6 (436002) a266ca7f10 (iOS 15.3.1; iPhone12,1; Verizon)",
  "_clientfield10": "",
  "dealID": "cpn-homer-feb22-3",
  "eventType": "GRP2",
  "_eventfield11": "35c3d25f-c26b-4737-89af-4e26caa8951f",
  "sessionId": "51304113-7634-4A78-8A49-5A1FA1288531",
  "_clientfield8": "18010e36-415c-11ea-8ecc-0242ac120002",
  "_clientfield9": "",
  "funnelID": "",
  "clientDeviceID": "5A2A5B24-5694-3E06-44F7-DFAA76FEB933",
  "time": 1648140600922,
  "parentEventID": 0,
  "placement": 1,
  "_clientfield11": "18010e36-415c-11ea-8ecc-0242ac120002"
}




{
  "eventID": 0,
  "distance": 0,
  "channel": "browse",
  "extras": {},
  "clientVersion": "22.6 (436002)",
  "presentation": "list_view",
  "appInstanceId": "89DCFB32D1394ED4A9F948CEF1093F40",
  "_eventfield12": {
    "deal_status": "available",
    "hasTopRatedBadge": false,
    "idfv": "9130B36F-D7B8-4356-B9F2-61F9E9CD7C19",
    "IsCLO": "off",
    "isCoreCoupon": true,
    "isD3CExperimentOn": true,
    "isD3CExperimentOn_3PIP": true,
    "isD3CExperimentOn_Booking": true,
    "isD3CExperimentOn_Goods": false,
    "oneClickClaimEnabled": "false",
    "screen_instance_id": "C7216696-0EBA-460A-B7DA-A09373847060_1648140599657",
    "sponsoredAdId": "display_logsI_ewKu7T9jaRAO102Z9AecEKJgokNmVmOTVkYTEtMDc1Mi00YzBkLTljODktMWMwNDAwYTcxMzE4EgAaDAi45feRBhDd94r5ASICCAE=",
    "tabName": "categories_tab",
    "UMSText": "10+ viewed today"
  },
  "clientBcookie": "5A2A5B24-5694-3E06-44F7-DFAA76FEB933",
  "clientPlatform": "IPHONECON (iPhone12,1)",
  "clientLocale": "en_US",
  "brand": "groupon",
  "_clientfield7": "US",
  "clientClientID": "e679498bd360fdefd5367e13784d6b94",
  "clientUserAgent": "Groupon/dogfood/22.6 (436002) a266ca7f10 (iOS 15.3.1; iPhone12,1; Verizon)",
  "_clientfield10": "",
  "dealID": "cpn-codespark-feb22-3",
  "eventType": "GRP2",
  "_eventfield11": "6ef95da1-0752-4c0d-9c89-1c0400a71318",
  "sessionId": "51304113-7634-4A78-8A49-5A1FA1288531",
  "_clientfield8": "18010e36-415c-11ea-8ecc-0242ac120002",
  "_clientfield9": "",
  "funnelID": "",
  "clientDeviceID": "5A2A5B24-5694-3E06-44F7-DFAA76FEB933",
  "time": 1648140618481,
  "parentEventID": 0,
  "placement": 4,
  "_clientfield11": "18010e36-415c-11ea-8ecc-0242ac120002"

select platform from grp_gdoop_bizops_db.np_temp_sl_cat_imps group by platform;

---------------------COMPLETE QUERY ANALYTICS
----need to add platform

{"attributionId":"00000000-0000-0000-0000-000005490858",
"card_search_uuid":"00000000-0000-0000-0000-000005490858","clicktype":
"category_click","deal_status":"available","hasTopRatedBadge":true,"idfv":"F49592F1-D020-41D3-9AFE-0371610E4619","IsCLO":"off","isD3CExperimentOn":true,
"isD3CExperimentOn_3PIP":true,"isD3CExperimentOn_Booking":true,"isD3CExperimentOn_Goods":false,"oneClickClaimEnabled":"false","rating":"4.460000",
"screen_instance_id":"8F13DE8C-2716-4AC3-8A58-3946BA1DE693_1647257620060","tabName":"categories_tab","Type":"Baby & Kids"}


select 
   eventtime,
   bcookie, 
   extrainfo, 
   dealuuid,
   position,
   case when json_extract_scalar(extrainfo, '$.sponsoredAdId') is not null then 1 else 0 end sponsored, 
   lower(trim(regexp_replace(clientplatform,'\\t|\\n|\\r|\\u0001', ''))) platform, 
   eventdate
from grp_gdoop_pde.junoHourly 
where 
eventdestination = 'dealImpression'
and event = 'dealImpression'
and eventdate = '2022-03-14'
and rawevent = 'GRP2'
and lower(trim(regexp_replace(clientplatform,'\\t|\\n|\\r|\\u0001', ''))) in ('iphone')
and country in ('US','CA')
and platform = 'mobile'
and bcookie is not null 
and bcookie <> ''
and lower(trim(bcookie)) <> 'null'
and lower(brand) = 'groupon' 
and bcookie not in ('android_automation_test' ,'ios_automation_test','android_perf_test_nightly','ios_perf_test_nightly')
and lower(trim(extrainfo)) like '%categories_tab%';
 eventtime,
   bcookie, 
   extrainfo, 
   dealuuid,
   position,
   case when json_extract_scalar(extrainfo, '$.sponsoredAdId') is not null then 1 else 0 end sponsored, 
   lower(trim(regexp_replace(clientplatform,'\\t|\\n|\\r|\\u0001', ''))) platform, 
   eventdate

select 
  json_extract_scalar(extrainfo, '$.tabName')
from grp_gdoop_pde.junoHourly 
where 
eventdestination = 'dealImpression'
and event = 'dealImpression'
and eventdate = '2022-03-14'
and rawevent = 'GRP2'
and lower(trim(regexp_replace(clientplatform,'\\t|\\n|\\r|\\u0001', ''))) in ('android')
and country in ('US','CA')
and platform = 'mobile'
and bcookie is not null 
and bcookie <> ''
and lower(trim(bcookie)) <> 'null'
and lower(brand) = 'groupon' 
and bcookie not in ('android_automation_test' ,'ios_automation_test','android_perf_test_nightly','ios_perf_test_nightly')
and json_extract_scalar(extrainfo, '$.tabName') is not null
and json_extract_scalar(extrainfo, '$.tabName') <> 'home_tab';

/*
select
   eventtime, 
   bcookie,
   extrainfo,
   get_json_object(lower(extrainfo), '$.screen_name') category,
   lead(eventtime) over (partition by bcookie order by eventtime) event_end_time,
   lower(trim(regexp_replace(clientplatform,'\\t|\\n|\\r|\\u0001', ''))) platform,
   eventdate
from grp_gdoop_pde.junoHourly 
where 
eventdestination = 'searchBrowseView'
and event = 'genericPageView'
and eventdate >= '2022-03-01' AND eventdate <= '2022-03-21'
and lower(trim(regexp_replace(clientplatform,'\\t|\\n|\\r|\\u0001', ''))) in ('iphone', 'android')
and country in ('US','CA')
and platform = 'mobile'
and bcookie is not null 
and bcookie <> ''
and lower(trim(bcookie)) <> 'null'
and lower(brand) = 'groupon' 
and bcookie not in ('android_automation_test' ,'ios_automation_test','android_perf_test_nightly','ios_perf_test_nightly')
and get_json_object(lower(extrainfo), '$.screen_name') is not null;*/

select 
  *
from grp_gdoop_pde.junoHourly 
where 
eventdestination = 'dealImpression'
and event = 'dealImpression'
and eventdate = '2022-03-01'
and rawevent = 'GRP2'
and lower(trim(regexp_replace(clientplatform,'\\t|\\n|\\r|\\u0001', ''))) in ('android')
and country in ('US','CA')
and platform = 'mobile'
and bcookie is not null 
and bcookie <> ''
and lower(trim(bcookie)) <> 'null'
and lower(brand) = 'groupon' 
and bcookie not in ('android_automation_test' ,'ios_automation_test','android_perf_test_nightly','ios_perf_test_nightly')
and bcookie = 'a14cf09b-3b42-3ff2-afcb-a86d9b3308b3'
and eventtime >= '1646099644638'
--and extrainfo like '%sponsor%'
order by eventtime asc;

{"attributionId":"00000000-0000-0000-0000-000005487128","hasTopRatedBadge":false,
"hasTopRatedMerchantMedal":false,"isCoreCoupon":false,"numberOfOptions":2,"oneClickClaimEnabled":false,"pricePoint":0,"rating":0.0,"udcPriceTag":"on",
"card_search_UUID":"00000000-0000-0000-0000-000005487128","deal_status":"available","isCLO":"off","home_tab_cold_start":false,"placement":131,"time_in_database":0}

{"attributionId":"00000000-0000-0000-0000-000005487128","hasTopRatedBadge":false,"hasTopRatedMerchantMedal":false,"isCoreCoupon":false,"numberOfOptions":2,
"oneClickClaimEnabled":false,"pricePoint":0,"rating":0.0,"udcPriceTag":"on",
"card_search_UUID":"00000000-0000-0000-0000-000005487128","deal_status":"available","isCLO":"off",
"home_tab_cold_start":false,"placement":128,"time_in_database":0}

{"attributionId":"00000000-0000-0000-0000-000005487128","hasTopRatedBadge":false,"hasTopRatedMerchantMedal":false,
"isCoreCoupon":false,"numberOfOptions":1,"oneClickClaimEnabled":false,"pricePoint":0,"rating":0.0,"udcPriceTag":"on",
"card_search_UUID":"00000000-0000-0000-0000-000005487128","deal_status":"available","isCLO":"off",
"home_tab_cold_start":false,"placement":109,"time_in_database":0}

{"cardPermalink":"cardatron:mobile:home-cx90:us",
"collectionCardName":"crossChannel_homepage_sponsored_carousel","collectionCardPosition":"1",
"collectionCardTitleText":"Meal Kit Deliveries for Two or Four People from HelloFresh (Up to 64% Off). Six Options Available. ",
"collectionCardUUID":"12ebbd28-3a95-4823-9258-65eb7e57d0e3",
"collectionTemplateId":"d2cd6438-58b3-4e55-b3fc-6942f0e8da64","collectionTemplateView":"HorizontalTwoUpCompoundCardView"}

{"hasTopRatedBadge":false,"hasTopRatedMerchantMedal":false,"isCoreCoupon":true,"numberOfOptions":1,"oneClickClaimEnabled":false,"pricePoint":0,"rating":0.0,
"sponsoredAdId":"display_hFHakgzCdynvPF6dmm3vgRY4GAIKJgokNmVmOTVkYTEtMDc1Mi00YzBkLTljODktMWMwNDAwYTcxMzE4EgAaDAi_nPuQBhCi3-PGAiICCAE=",
"udcPriceTag":"off","deal_status":"available","isCLO":"off","home_tab_cold_start":false,"placement":0,"time_in_database":0}

{"hasTopRatedBadge":false,"hasTopRatedMerchantMedal":false,"isCoreCoupon":true,"numberOfOptions":1,"oneClickClaimEnabled":false,
"pricePoint":0,"rating":0.0,"sponsoredAdId":"display_hFHakgzCdynvPF6dmm3vgRY4GAIKJgokNmVmOTVkYTEtMDc1Mi00YzBkLTljODktMWMwNDAwYTcxMzE4EgAaDAi_nPuQBhCi3-PGAiICCAE=",
"udcPriceTag":"off","deal_status":"available","isCLO":"off","home_tab_cold_start":false,"placement":0,"time_in_database":0}

{"hasTopRatedBadge":false,"hasTopRatedMerchantMedal":false,"isCoreCoupon":false,"numberOfOptions":1,"oneClickClaimEnabled":false,"pricePoint":0,"rating":0.0,
"udcPriceTag":"on","deal_status":"available","isCLO":"off","home_tab_cold_start":false,"placement":1,"time_in_database":0}

{"cardPermalink":"cardatron:mobile:home-cx90:us","collectionCardName":"homepage_Relevance-Feed_Deal_Carousel",
"collectionCardPosition":"1","collectionCardTitleText":"Conventional, Synthetic Blend, or Full Synthetic Signature Service Oil Change at Jiffy Lube (Up to 41% Off)   ",
"collectionCardUUID":"2a787d77-48b5-4071-886f-4ba2f2802ef8",
"collectionTemplateId":"d2cd6438-58b3-4e55-b3fc-6942f0e8da64","collectionTemplateView":"HorizontalTwoUpCompoundCardView"}

select
   eventtime, 
   bcookie,
   extrainfo,
   lead(eventtime) over (partition by bcookie order by eventtime) event_end_time,
   lower(trim(regexp_replace(clientplatform,'\\t|\\n|\\r|\\u0001', ''))) platform,
   eventdate
from grp_gdoop_pde.junoHourly 
where 
eventdestination = 'searchBrowseView'
and event = 'genericPageView'
and eventdate >= '2022-03-01' 
and lower(trim(regexp_replace(clientplatform,'\\t|\\n|\\r|\\u0001', ''))) = 'android'
and country in ('US','CA')
and platform = 'mobile'
and bcookie is not null 
and bcookie <> ''
and lower(trim(bcookie)) <> 'null'
and lower(brand) = 'groupon' 
and bcookie not in ('android_automation_test' ,'ios_automation_test','android_perf_test_nightly','ios_perf_test_nightly')
;


select *
from ai_reporting.sl_imp_clicks_app
where  eventdate = '2021-03-15' and clientplatform = 'Android' 
;


{col1=category_icon_thingstodo_click, col2={"isD3CExperimentOn":true,"isD3CExperimentOn_3PIP":true,"isD3CExperimentOn_Goods":false}, col3=null}
{col1=category_icon_thingstodo_click, col2={"isD3CExperimentOn":true,"isD3CExperimentOn_3PIP":true,"isD3CExperimentOn_Goods":false}, col3=null}
{col1=category_icon_fooddrink_click, col2={"isD3CExperimentOn":true,"isD3CExperimentOn_3PIP":true,"isD3CExperimentOn_Goods":false}, col3=null}
{col1=category_icon_thingstodo_click, col2={"isD3CExperimentOn":true,"isD3CExperimentOn_3PIP":true,"isD3CExperimentOn_Goods":false}, col3=null}
{col1=category_click, 
col2={"isD3CExperimentOn":true,"isD3CExperimentOn_3PIP":true,"isD3CExperimentOn_Goods":false,"search_result":{
"query":"Automotive,b92833c4-49cb-4f5f-83b2-660f6ab111b2,default,1"}}, col3=null}

{col1=filter_select_click, col2={"action":"select","filter_name":"Rating","filter_position":4,
"is_exposed":"true","isD3CExperimentOn":true,"isD3CExperimentOn_3PIP":true,"isD3CExperimentOn_Goods":false,"option_name":"4 Star & Up","option_selected":2}, col3=null}



{"attributionId":"29fe38e1-5d04-9d02-0e8b-07d76390f3be","card_search_uuid":"29fe38e1-5d04-9d02-0e8b-07d76390f3be",
"clicktype":"category_click","deal_status":"available","hasTopRatedBadge":false,"hasTopRatedMerchantMedal":true,"IsCLO":"off",
"isD3CExperimentOn":true,"isD3CExperimentOn_3PIP":true,"isD3CExperimentOn_Goods":false,"oneClickClaimEnabled":"false","rating":"4.620000",
"sponsoredAdId":"display_YvGzinJ0EA8H0J94yiJCEbiSxlAxMjY1NDA4Ny1kNzc3LTRhMjktOGJhNi00ZTQ5ZjNkNDE5ZWQ=","tabName":"categories_tab",
"Type":"Local","UMSText":"70+ bought today"}






create table grp_gdoop_bizops_db.np_sl_temp_imps_all stored as orc as 
select
	cast(position as integer )-1 position,
	sponsored, 
	case when sponsored = 1 then two.team_cohort else 'none' end team_cohort,
	case when category like '%things to do%' then 'TTD'
	     when category like '%food & drink%'  then 'FND'
	     when category like '%beauty & spas%' then 'HBW'
	     when category like '%local%' then 'other - local' 
	     else 'others - browse'
	     end category_page,
	platform,
	count(distinct concat(bcookie, dealuuid, eventdate)) impressions
from grp_gdoop_bizops_db.np_temp_sl_cat_aggthree as one
left join (select a.sku, max(b.team_cohort) team_cohort
           from grp_gdoop_bizops_db.np_sl_ad_snapshot as a
           left join grp_gdoop_bizops_db.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name
           group by a.sku) as two on one.dealuuid = two.sku
where position <= 21
group by 
   position, 
   sponsored, 
   case when category like '%things to do%' then 'TTD'
	     when category like '%food & drink%'  then 'FND'
	     when category like '%beauty & spas%' then 'HBW'
	     when category like '%local%' then 'other - local' 
	     else 'others - browse'
	     end,
   platform,
   case when sponsored = 1 then two.team_cohort else 'none' end
union all
select 
   cast(position as integer) position, 
   sponsored, 
   case when sponsored = 1 then two.team_cohort else 'none' end team_cohort,
   case when fullurl like '%things-to-do%' then 'TTD'
	     when fullurl like '%food-and-drink%'  then 'FND'
	     when fullurl like '%beauty-and-spas%' then 'HBW'
	     when fullurl like '%local%' then 'other - local' 
	     else 'others - browse'
	     end category_page,
   clientplatform,
   count(distinct concat(bcookie, eventdate, dealuuid)) impressions
from 
(select 
      case when cl.aogcampaignid is not null then 1 else 0 end sponsored,
      trim(lower(regexp_replace(search_query,'\\+',' '))) search_query,
      position,
      ogp,
      nor,
      nob,
      dv_time,
      bcookie,
      order_uuid,
      cl.dealuuid,
      clientplatform,
      aogcampaignid,
      '' extrainfo,
      case when rawpagetype = 'browse/deals/index' and fullurl like '%context=local%' or fullurl like '%category=%' then 'browse'
           when rawpagetype in ('browse/deals/index') and search_query is not null  and search_query!='' then 'search'
           when rawpagetype in ('homepage' ,'homepage/index', 'featured/deals/index') then 'homepage'
           when rawpagetype in ( 'browse/deals/index') then 'browse' --'featured'
           when rawpagetype in ('nearby/deals/index', --featured? or local only 
                                 'goods/browse/index',
                                 'goods/index',
                                 'giftshop/deals/show',
                                 'giftshop/deals/index',
                                  'channels/show',
                                  'beautynow_promoted',
                                  'beautynow_salon',
                                  'beautynow_appointment_receipt',
                                  'beautynow_SELECT_appointment_time',
                                  'beautynow_SELECT_service') then 'browse'
           when rawpagetype like '%-%-%-%' then 'occasions'
           else rawpagetype
           end page,
        rawpagetype,
        fullurl,
        cl.eventdate
     from ai_reporting.sl_imp_clicks cl
     where eventdate >= '2022-03-01' and eventdate <= '2022-03-21'
) as fin 
left join (select a.sku, max(b.team_cohort) team_cohort
           from grp_gdoop_bizops_db.np_sl_ad_snapshot as a
           left join grp_gdoop_bizops_db.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name
           group by a.sku) as two on fin.dealuuid = two.sku
where fin.page = 'browse' and cast(position as integer) <= 20
group by 
   position, 
   sponsored, 
   case when sponsored = 1 then two.team_cohort else 'none' end,
   case when fullurl like '%things-to-do%' then 'TTD'
	     when fullurl like '%food-and-drink%'  then 'FND'
	     when fullurl like '%beauty-and-spas%' then 'HBW'
	     when fullurl like '%local%' then 'other - local' 
	     else 'others - browse'
	     end,
   clientplatform
;
  

select 
*
from 
(select 
      case when cl.aogcampaignid is not null then 1 else 0 end sponsored,
      trim(lower(regexp_replace(search_query,'\\+',' '))) search_query,
      position,
      ogp,
      nor,
      nob,
      dv_time,
      bcookie,
      order_uuid,
      cl.dealuuid,
      clientplatform,
      aogcampaignid,
      '' extrainfo,
      case when rawpagetype = 'browse/deals/index' and fullurl like '%context=local%' or fullurl like '%category=%' then 'browse'
           when rawpagetype in ('browse/deals/index') and search_query is not null  and search_query!='' then 'search'
           when rawpagetype in ('homepage' ,'homepage/index', 'featured/deals/index') then 'homepage'
           when rawpagetype in ( 'browse/deals/index') then 'browse' --'featured'
           when rawpagetype in ('nearby/deals/index', --featured? or local only 
                                 'goods/browse/index',
                                 'goods/index',
                                 'giftshop/deals/show',
                                 'giftshop/deals/index',
                                  'channels/show',
                                  'beautynow_promoted',
                                  'beautynow_salon',
                                  'beautynow_appointment_receipt',
                                  'beautynow_SELECT_appointment_time',
                                  'beautynow_SELECT_service') then 'browse'
           when rawpagetype like '%-%-%-%' then 'occasions'
           else rawpagetype
           end page,
        rawpagetype,
        fullurl,
        cl.eventdate
     from ai_reporting.sl_imp_clicks cl
     where eventdate >= '2022-03-01' and eventdate <= '2022-03-21'
) as fin where page = 'browse'


select 
    sponsored, 
    ttd_cat,
    sum(impressions)
from 
(select
	position, 
	sponsored, 
	---case when sponsored = 1 then two.team_cohort else 'none' end team_cohort,
	category, 
	case when category like '%things to do%' then 1 else 0 end ttd_cat,
	count(distinct concat(bcookie, dealuuid, eventdate)) impressions, 
	count(distinct dealuuid) total_deals
from grp_gdoop_bizops_db.np_temp_sl_cat_aggthree as one
left join (select a.sku, b.team_cohort
           from grp_gdoop_bizops_db.np_sl_ad_snapshot as a
           left join grp_gdoop_bizops_db.np_sl_supp_mapping as b on a.supplier_name = b.supplier_name
           group by a.sku, b.team_cohort) as two on one.dealuuid = two.sku
group by 
   position, 
   sponsored, 
   ---case when category like '%things to do%' then 1 else 0 end,
   category,
   case when sponsored = 1 then two.team_cohort else 'none' end) as fin
where cast(position as integer) <= 3  
group by sponsored,ttd_cat
   ;
  

select 
   
from 
(select 
      case when cl.aogcampaignid is not null then 'Sponsored' else 'Organic' end deal_landing,
      trim(lower(regexp_replace(search_query,'\\+',' '))) search_query,
      position,
      ogp,
      nor,
      nob,
      dv_time,
      bcookie,
      order_uuid,
      cl.dealuuid,
      clientplatform,
      aogcampaignid,
      '' extrainfo,
      case when rawpagetype = 'browse/deals/index' and fullurl like '%context=local%' or fullurl like '%category=%' then 'browse'
           when rawpagetype in ('browse/deals/index') and search_query is not null  and search_query!='' then 'search'
           when rawpagetype in ('homepage' ,'homepage/index', 'featured/deals/index') then 'homepage'
           when rawpagetype in ( 'browse/deals/index') then 'browse' --'featured'
           when rawpagetype in ('nearby/deals/index', --featured? or local only 
                                 'goods/browse/index',
                                 'goods/index',
                                 'giftshop/deals/show',
                                 'giftshop/deals/index',
                                  'channels/show',
                                  'beautynow_promoted',
                                  'beautynow_salon',
                                  'beautynow_appointment_receipt',
                                  'beautynow_SELECT_appointment_time',
                                  'beautynow_SELECT_service') then 'browse'
           when rawpagetype like '%-%-%-%' then 'occasions'
           else rawpagetype
           end page,
        rawpagetype,
        fullurl,
        cl.eventdate
     from ai_reporting.sl_imp_clicks cl
) as fin where page = 'browse'
;


  
   event_time,
   user_browser_id, 
   wc_data_json, 
   deal_uuid,
   widget_content_position,
   case when lower(wc_data_json) like '%sponsored%' then 1 else 0 end sponsored,
   lower(platform) platform,
   dt,
   page_type,
   page_url

select 
   page_type
from prod_groupondw.bld_widget_contents 
where lower(platform) in ('desktop', 'touch')
and dt >= '2022-03-01' AND dt <= '2022-03-07'
and user_browser_id <> '' 
and user_browser_id is not null
and lower(page_country) IN ('us', 'ca')
and lower(page_hostname) like '%groupon%'
--and lower(page_type) like '%browse%'
--and lower(page_type) not like '%query=%'
group by page_type
;

select max(eventdate) from ai_reporting.sl_imp_clicks;

select 
	sponsored,
	case when category like '%things to do%' then 1 else 0 end ttd_cat,
	count(distinct concat(bcookie, dealuuid, eventdate)) impressions,
	count(distinct dealuuid) total_deals
from grp_gdoop_bizops_db.np_temp_sl_cat_aggthree
where cast(position as integer) <= 3
group by
   sponsored, 
   case when category like '%things to do%' then 1 else 0 end;

select * from grp_gdoop_bizops_db.np_temp_sl_cat_imps_web2;
select * from grp_gdoop_bizops_db.np_temp_sl_cat_imps_web;

{u'body': {u'section4': [{u'content': u
'New HelloFresh Customers: 1 Week of 3 Meals for 2 People - 6 Servings (Shipping Included)', u'type': u'title'}], 
u'section3': [{u'content': u'$69.93 $25', u'type': u'price'}, {u'content': 64, u'type': u'discount_percentage'}], 
u'section2': [{u'content': {u'count': u'19,923', u'numeric_value': u'4.08', u'showingNumericRating': True}, u'type': u'rating'}],
u'section1': [{u'content': u'HelloFresh', u'type': u'title'}]},
u'cardUUID': u'1e827ff1-5c1b-40d9-9f83-6c351d1d0874', u'attributionId': u'00000000-0000-0000-0000-000005487871',
u'cardSubtype': u'LOCAL', u'cardView': u'left-aligned', u'header': {u'content': 
{u'text': u'Sponsored', u'adId': 
u'display_kMypaYilewhPviw4dXpcmYC6IocKJgokMWU4MjdmZjEtNWMxYi00MGQ5LTlmODMtNmMzNTFkMWQwODc0EgAaDAjpmYuRBhCU5-nOAyICCAE='}, u'type': u'sponsoredQualifier'}}

35c3d25f-c26b-4737-89af-4e26caa8951f
-----------------------------------WEB

select 
*
from prod_groupondw.bld_widget_contents 
where lower(platform) in ('desktop', 'touch')
and dt = '2022-03-15'
and user_browser_id <> '' 
and user_browser_id is not null
and lower(page_country) IN ('us', 'ca')
and lower(page_hostname) like '%groupon%'
and lower(page_type) like '%browse%'
limit 10;

select 
   eventtime,
   bcookie, 
   extrainfo, 
   dealuuid,
   position,
   ---case when get_json_object(extrainfo, '$.sponsoredAdId') is not null then 1 else 0 end sponsored, 
   lower(trim(regexp_replace(clientplatform,'\\t|\\n|\\r|\\u0001', ''))) platform, 
   rawpagetype,
   eventdate,
   clientplatform,
   fullurl
from grp_gdoop_pde.junoHourly 
where 
eventdestination = 'searchBrowseView'
and event = 'genericPageView'
and lower(brand) = 'groupon' 
and eventdate = '2022-03-15'
and clientplatform in ('Touch','web')
and bcookie is not null 
and bcookie <> ''
and lower(trim(bcookie)) <> 'null'
and country in ('US','CA')
and rawpagetype like '%browse%'
limit 100
;




case when rawpagetype = 'browse/deals/index' and fullurl like '%context=local%' or fullurl like '%category=%' then 'browse'
           when rawpagetype in ('browse/deals/index') and search_query is not null  and search_query!='' then 'search'
           when rawpagetype in ('homepage' ,'homepage/index', 'featured/deals/index') then 'homepage'
           when rawpagetype in ( 'browse/deals/index') then 'browse' --'featured'
           when rawpagetype in ('nearby/deals/index', --featured? or local only 
                                 'goods/browse/index',
                                 'goods/index',
                                 'giftshop/deals/show',
                                 'giftshop/deals/index',
                                  'channels/show',
                                  'beautynow_promoted',
                                  'beautynow_salon',
                                  'beautynow_appointment_receipt',
                                  'beautynow_SELECT_appointment_time',
                                  'beautynow_SELECT_service') then 'browse'
           when rawpagetype like '%-%-%-%' then 'occasions'
           else rawpagetype,


select 
   eventtime,
   bcookie, 
   extrainfo, 
   dealuuid,
   position,
   lower(trim(regexp_replace(clientplatform,'\\t|\\n|\\r|\\u0001', ''))) platform, 
   rawpagetype,
   eventdate,
   clientplatform,
   sponsoredAdId,
   rawpagetype,
   eventdate,
   fullurl,
   case when fullurl like '%query=%' then str_to_map(translate(fullurl,'?','&'),'&','=')['query'] end  search_query
from grp_gdoop_pde.junoHourly 
where 
eventdestination = 'dealImpression'
and lower(brand) = 'groupon' 
and eventdate >= '2022-03-01' and eventdate <= '2022-03-21'
and clientplatform in ('Touch','web')
and bcookie is not null 
and bcookie <> ''
and lower(trim(bcookie)) <> 'null'
and country in ('US','CA')
and sponsoredAdId is not null;



-------------------------------------------------HOMEPAGE CAROUSEL IMPRESSIONS



select dt, platform, count(distinct concat(user_browser_id, dt)) as unique_impressions
from prod_groupondw.bld_widget_contents 
where lower(platform) in ('desktop', 'touch')
and dt >= date_sub(CURRENT_DATE, 30)
and user_browser_id <> '' 
and user_browser_id is not null
and lower(page_country) IN ('us', 'ca')
and lower(page_hostname) like '%groupon%'
and lower(page_type) like '%homepage%'
and lower(widget_content_name) like '%crosschannel_homepage_sponsored_carousel%'
and lower(widget_content_type) = 'compound'
group by dt, platform

UNION ALL 

select eventdate, clientplatform, count(distinct concat(bcookie, eventdate)) as unique_impressions
from grp_gdoop_pde.junoHourly 
where eventdate >= date_sub(CURRENT_DATE, 30)
and lower(trim(regexp_replace(clientplatform,'\\t|\\n|\\r|\\u0001', ''))) in ('iphone', 'android')
and country in ('US','CA')
and platform = 'mobile'
and bcookie is not null 
and bcookie <> ''
and lower(trim(bcookie)) <> 'null'
and lower(brand) = 'groupon' 
and bcookie not in ('android_automation_test' ,'ios_automation_test','android_perf_test_nightly','ios_perf_test_nightly')
and case when 'userAgent' like '%bot%' or useragent like '%crawler%' or useragent like '%search%' or useragent like '%spider%' or useragent like '%spyder%' then 1 else 0 end = 0
and lower(trim(extrainfo)) like '%crosschannel_homepage_sponsored_carousel%' 
and lower(impressiontype) = 'collection_card_impression'
and lower(eventdestination) = 'genericimpression'
and lower(event) = 'genericimpression'
group by 
eventdate, clientplatform





-------------------------------------------------OVERALL HOMEPAGE


select dt, platform, count(concat(bcookie, dt)) as page_views, count(distinct concat(bcookie, dt)) as unique_page_views
from prod_groupondw.bld_events
where dt >= date_sub(CURRENT_DATE, 30)
and lower(platform) in ('desktop', 'touch')
and lower(page_country) in ('us')
and lower(event) in ('pageview')
and lower(trim(page_type)) like '%homepage/index%'
and lower(page_hostname) like '%groupon%'
and bcookie is not null
and bcookie <> ''
and lower(trim(bcookie)) <> 'null'
and bot_flag = '0'
and internal_ip_ind = '0'
group by dt, platform

UNION all

select eventdate, lower(trim(regexp_replace(clientplatform,'\\t|\\n|\\r|\\u0001', ''))) as clientplatform, count(concat(bcookie, eventdate)) as page_views, count(distinct concat(bcookie, eventdate)) as unique_page_views
from grp_gdoop_pde.junohourly
where eventdate >= date_sub(CURRENT_DATE, 30) 
and lower(platform) = 'mobile'
and lower(trim(regexp_replace(clientplatform,'\\t|\\n|\\r|\\u0001', ''))) in ('android', 'iphone')
and lower(country) in ('us')
and rawevent = 'GRP14'
and lower(trim(REGEXP_REPLACE(pageid,'\\t|\\n|\\r|\\u0001', ' '))) in ('home_tab','home_page','featured', 'featuredrapifragment')
and lower(event) in ('genericpageview')
and lower(eventdestination) in ('searchbrowseview')
and bcookie is not null
and bcookie <> ''
and lower(brand) = 'groupon'
and lower(trim(bcookie)) <> 'null'
and bcookie not in ('android_automation_test' ,'ios_automation_test','android_perf_test_nightly','ios_perf_test_nightly')
and lower(trim(useragent)) not like '%bot%'
and lower(trim(useragent)) not like '%crawler%'
and lower(trim(useragent)) not like '%search%'
and lower(trim(useragent)) not like '%spider%'
and lower(trim(useragent)) not like '%spyder%'
and lower(trim(coalesce(pageid,'x'))) <> 'splash'
group by eventdate, lower(trim(regexp_replace(clientplatform,'\\t|\\n|\\r|\\u0001', '')))