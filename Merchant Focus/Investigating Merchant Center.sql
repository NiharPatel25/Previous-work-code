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
   
from
    grp_gdoop_pde.junoHourly
    where 
    eventdate = '2021-07-15'
-----------------------------------
/*CASE 
           WHEN COALESCE(LOWER(TRIM(REGEXP_REPLACE(clientplatform,'\\t|\\n|\\r|\\u0001', ''))), LOWER(TRIM(REGEXP_REPLACE(platform,'\\t|\\n|\\r|\\u0001', '')))) IN ('web', 'desktop') THEN 'Web'
           WHEN COALESCE(LOWER(TRIM(REGEXP_REPLACE(clientplatform,'\\t|\\n|\\r|\\u0001', ''))), LOWER(TRIM(REGEXP_REPLACE(platform,'\\t|\\n|\\r|\\u0001', '')))) IN ('touch') THEN 'Touch'
           WHEN LOWER(TRIM(REGEXP_REPLACE(clientplatform,'\\t|\\n|\\r|\\u0001', ''))) = 'mobile-merchant' THEN 'Mobile'
           ELSE 'Web'
      END AS */

select * from grp_gdoop_bizops_db.pai_merchant_center_visits where utm_campaign is not null;

select 
      page_app, 
      ---using fullurl, url 
      REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(fullurl, url), '%(25)+', '%'), '%26', '&'), '%2F', '/'), '%3D', '='), '%3F', '?'), '%3A', ':'), '%23', '#') AS page_url, 
      pagepath,
      COALESCE(rawpagetype, pageviewtype) AS page_type,
      country
      --using clientplatform, platform
      CASE 
           WHEN COALESCE(LOWER(TRIM(REGEXP_REPLACE(clientplatform,'\\t|\\n|\\r|\\u0001', ''))), LOWER(TRIM(REGEXP_REPLACE(platform,'\\t|\\n|\\r|\\u0001', '')))) IN ('web', 'desktop') THEN 'Web'
           WHEN COALESCE(LOWER(TRIM(REGEXP_REPLACE(clientplatform,'\\t|\\n|\\r|\\u0001', ''))), LOWER(TRIM(REGEXP_REPLACE(platform,'\\t|\\n|\\r|\\u0001', '')))) IN ('touch') THEN 'Touch'
           WHEN LOWER(TRIM(REGEXP_REPLACE(clientplatform,'\\t|\\n|\\r|\\u0001', ''))) = 'mobile-merchant' THEN 'Mobile'
           ELSE 'Web'
      END AS platform,
      campaign AS utm_campaign,
      medium AS utm_medium,
      source AS utm_source,
      event,
      eventdestination,
      widgetname
FROM grp_gdoop_pde.junohourly
WHERE eventdate BETWEEN DATE_SUB(CURRENT_DATE, 14) AND DATE_SUB(CURRENT_DATE, 1)
  AND platform = 'web'
  AND eventdestination IN ('dealImpression', 'genericBucket', 'genericClick', 'other', 'purchaseFunnel', 'searchBrowseView')
  AND LOWER(pageapp) 
         IN ('merchant-center-minsky', 'android-mobile-merchant', 'ios-mobile-merchant', 'merchant-support-echo', 
                 'metro-ui', 'merchant-center-auth','merchant-advisor-itier')
  AND event <> 'merchantPageView'
  AND country IN ('AE', 'AU', 'BE', 'CA', 'DE', 'ES', 'FR', 'GB', 'IE', 'IT', 'NL', 'PL', 'QC', 'UK', 'US')
  
select
      pageapp, 
       ---using fullurl, url 
      REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(fullurl, url), '%(25)+', '%'), '%26', '&'), '%2F', '/'), '%3D', '='), '%3F', '?'), '%3A', ':'), '%23', '#') AS page_url,
      pagepath,
      COALESCE(rawpagetype, pageviewtype) AS page_type,
      country,
      --using clientplatform, platform
      CASE 
           WHEN COALESCE(LOWER(TRIM(REGEXP_REPLACE(clientplatform,'\\t|\\n|\\r|\\u0001', ''))), LOWER(TRIM(REGEXP_REPLACE(platform,'\\t|\\n|\\r|\\u0001', '')))) IN ('web', 'desktop') THEN 'Web'
           WHEN COALESCE(LOWER(TRIM(REGEXP_REPLACE(clientplatform,'\\t|\\n|\\r|\\u0001', ''))), LOWER(TRIM(REGEXP_REPLACE(platform,'\\t|\\n|\\r|\\u0001', '')))) IN ('touch') THEN 'Touch'
           WHEN LOWER(TRIM(REGEXP_REPLACE(clientplatform,'\\t|\\n|\\r|\\u0001', ''))) = 'mobile-merchant' THEN 'Mobile'
           ELSE 'Web'
      END AS platform,
      campaign AS utm_campaign,
      medium AS utm_medium,
      source AS utm_source, 
      event, 
      eventdestination, 
      widgetname
FROM grp_gdoop_pde.junohourly
WHERE eventdate BETWEEN DATE_SUB(CURRENT_DATE, 14) AND DATE_SUB(CURRENT_DATE, 1)
  AND platform = 'other'
  AND eventdestination = 'other'
  AND event = 'merchantPageView'
  AND country IN ('AE', 'AU', 'BE', 'CA', 'DE', 'ES', 'FR', 'GB', 'IE', 'IT', 'NL', 'PL', 'QC', 'UK', 'US')

  