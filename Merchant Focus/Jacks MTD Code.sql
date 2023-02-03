use grp_gdoop_sup_analytics_db;drop table if exists sm_ss_unique_bcookie_utm purge;create table sm_ss_unique_bcookie_utm stored as orc as
  select
    a.*
    , case
        when lower(utm_campaign) = 'other' then 'No Campaign - Referred' else 
        case
            when lower(utm_campaign) in ('groupon-header-app1', 'groupon-header1', 'groupon-header-mweb1', 'groupon-footer') then lower(utm_campaign)
            when (lower(utm_campaign) like '%us_un_som_fac_tim_lb_rr_cbp%' and lower(utm_campaign) like '%fdp%') then 'Facebook F&D'
            when (lower(utm_campaign) like '%us_un_som_fac_tim_lb_rr_cbp%' and lower(utm_campaign) like '%hbp%') then 'Facebook HBW'
            when lower(utm_campaign) like '%us_un_som_fac_tim_lb_rr_cbp_chw_sy_v30%' then 'Social Retargeting'
            when lower(utm_campaign) like '%d*merchant-fb%' then 'Facebook_old'
            when lower(utm_campaign) like '%g*gmail-ads%' then  'GMail Ads'
            when lower(utm_campaign) like '%d*gw-dx-dis%' then  'DataXu'
            when lower(utm_campaign) like '%groupon%' then 'Referral'
            when lower(utm_campaign) like '%livingsocial_ib%' then 'Living Social'
            when lower(utm_campaign) like '%delivery-takeout-lp%' then 'Delivery Takeout'
            when utm_campaign = '50_DLS' then 'Referral'
            when utm_campaign = '50' then 'Referral'
            when (lower(utm_campaign) like '%grouponworks%' and lower(utm_campaign) like '%social%') then 'Social'
            when lower(utm_campaign) like '%merchant-retargeting%' then 'Merchant Retargeting'
            when lower(utm_campaign) like '%merchant-stream%' then 'Yahoo Stream Ads'
            when lower(utm_campaign) like '%d*merchant-gsp%' then 'Gmail Sponsored Promotions'
            --when lower(utm_campaign) like '%biz_page%' then 'Biz Pages'
            when lower(utm_campaign) like '%blog%' or lower(utm_campaign) like '%merchant_blog%' or lower(utm_campaign) like '%merchant_article%' or lower(utm_campaign) like '%merchant-blog-how-to-sell-post%' or lower(utm_campaign) like '%merchant-blog-sidebar%' then 'Merchant Blog/SBRC'
            when lower(utm_campaign) like '%merchantnl%' or lower(utm_campaign) like '%june2014%' or lower(utm_campaign) like '%july2014%' or lower(utm_campaign) like '%august2014%' or lower(utm_campaign) like '%september2014%' or lower(utm_campaign) like '%october2014%' or lower(utm_campaign) like '%november2014%' or lower(utm_campaign) like '%december2014%' or lower(utm_campaign) like '%january2015%' or lower(utm_campaign) like '%feb2015%' or lower(utm_campaign) like '%mar2015%' or lower(utm_campaign) like '%apr2015%' or lower(utm_campaign) like '%may2015%' or lower(utm_campaign) like '%june2015%' or lower(utm_campaign) like '%july2015%' or lower(utm_campaign) like '%august2015%' or lower(utm_campaign) like '%sept2015%' or lower(utm_campaign) like '%oct2015%' or lower(utm_campaign) like '%nov2015%' or lower(utm_campaign) like '%dec2015%' or lower(utm_campaign) like '%jan2016%' then 'Merchant Newsletter'
            when lower(utm_campaign) like '%print%' or lower(utm_campaign) like '%nra2016%' or lower(utm_campaign) like '%osr-cards%' or lower(utm_campaign) like '%hbw-2016%' or lower(utm_campaign) like '%austin-promo-16%' or lower(utm_campaign) like '%cultural-institutions-2015%' or lower(utm_campaign) like '%ttd-cultural-institutions%' or lower(utm_campaign) like '%ttd-culture-2016%' or lower(utm_campaign) like '%ttd-culture-2016%' or lower(utm_campaign) like '%ttd-activities-2016%' or lower(utm_campaign) like '%activities-2015%' or lower(utm_campaign) like '%events-2015%' or lower(utm_campaign) like '%astc-promo-16%' then 'Print'
            when lower(utm_campaign) like '%grouponworks_social%' then 'Social'
            --when lower(utm_campaign) like '%goods%' then 'Goods'
            --when lower(utm_campaign) like '%g1%' then 'G1'
            when lower(utm_campaign) like '%occasions_sponsor%' then 'Occasions_sponsor'
            when lower(utm_campaign) like '%occasions%' then 'Occasions'
            --when lower(utm_campaign) like '%collections%' then 'Collections'
            when lower(utm_campaign) like '%reserve%' then 'Reserve'
            when lower(utm_campaign) like '%getaways%' then 'Getaways'
            when lower(utm_campaign) like '%occasions_sponsor%' then 'Sponsored Occasions'
            when lower(utm_campaign) like '%st_text%' then 'GCN'
            when lower(utm_campaign) like '%toolkit%' then 'Score'
            when lower(utm_campaign) like '%mc_ppl%' then 'Merchant Circle'
            when lower(utm_campaign) like '%NRA_%' then 'NRA'
            when lower(utm_campaign) like '%linkedin_%' then 'LinkedIn'
            when lower(utm_campaign) like '%payments%' then 'Payments'
            when lower(utm_campaign) like '%goods%' then 'Goods'
            --when lower(utm_campaign) like '%112%' then 'Goods'
            --when no_campaign_chars = 36 then 'G1'
            when (lower(utm_campaign) like '%merchant%' and lower(utm_campaign) like '%food%' and lower(utm_campaign) like '%drink%' and lower(utm_campaign) like '%srm%') then 'Merchant-Food-Drink-SRM'
            when (lower(utm_campaign) like '%merchant%' and lower(utm_campaign) like '%ybr%' and lower(utm_campaign) like '%srm%') then 'Merchant-YBR-SRM'
            when (lower(utm_campaign) like '%merchant%' and lower(utm_campaign) like '%ybr%' and lower(utm_campaign) like '%scm%') then 'Merchant-YBR-SCM'
            when (lower(utm_campaign) like '%ppl%' and lower(utm_campaign) like '%gen%' and lower(utm_campaign) like '%sug2013%') then 'AdKnowledge_aug2013'
            when (lower(utm_campaign) like '%ppl%' and lower(utm_campaign) like '%info%') then 'InfoGroup'
            when (lower(utm_campaign) like '%merchant%' and lower(utm_campaign) like '%food%' and lower(utm_campaign) like '%drink%' and lower(utm_campaign) like '%scm%') then 'Merchant-Food-Drink-SCM'
            when (lower(utm_campaign) like '%leisure%' and lower(utm_campaign) like '%activities%') then 'Leisure-Activities'
            when (lower(utm_campaign) like '%beauty%' and lower(utm_campaign) like '%wellness%') then 'Beauty-Wellness'
            when (lower(utm_campaign) like '%food%' and lower(utm_campaign) like '%drink%') then 'Food & Drink'
            when lower(utm_campaign) like '%direct%' then 'Direct'
            when lower(utm_campaign) like '%organic%' then 'Organic'
            when lower(utm_campaign) like '%referral%' then 'Referral'
            when (lower(utm_campaign) like '%free_advertising%') then 'Google NB - Free Advertising'
            when (lower(utm_campaign) like '%sb_adv%') then 'Google NB - SB-Adv'
            when (lower(utm_campaign) like '%promote%') then 'Google NB - Promote'
            when (lower(utm_campaign) like '%advertise%') then 'Google NB - Advertise'
            when (lower(utm_campaign) like '%number%') then 'Google Brand - Number'
            when (lower(utm_campaign) like '%contact_merchant%') then 'Google Brand - Contact Merchant'
            when (lower(utm_campaign) like '%advertising%') then 'Google Brand - Advertising'
            when (lower(utm_campaign) like '%how_to_business%') then 'Google Brand - How To Business'
            when (lower(utm_campaign) like '%business%') then 'Google Brand - Business'
            when (lower(utm_campaign) like '%join%') then 'Google Brand - Join'
            when (lower(utm_campaign) like '%merchant_misc%') then 'Google Brand - Merchant Misc'
            when (sem_partner like '%ggl%' and sem_brand like '%merchant-competitors%') then 'Google - Competitor'
            when (sem_partner like '%bng%' and sem_brand like '%nbr%') then 'Bing - Non-Brand'
            when (sem_partner like '%bng%' and sem_brand like '%ybr%') then 'Bing - Brand'
            when (sem_partner like '%bng%' and sem_brand like '%cbr%') then 'Bing - Competitor'
            when lower(split(utm_campaign,'_')[1]) like 'g1%' then 'G1'
            when lower(split(utm_campaign,'_')[1]) like 'goods%' then 'Goods'
            when lower(split(utm_campaign,'_')[1]) like 'occasion%' then 'Occasions'
            when lower(split(utm_campaign,'_')[1]) like 'getaways%' then 'Getaways'
            when lower(split(utm_campaign,'_')[1]) like 'reserve%' then 'Reserve'
            when lower(split(utm_campaign,'_')[1]) like 'collection%' then 'Collections'
            when lower(split(utm_campaign,'_')[1]) like 'payments%' then 'Payments'
            when lower(utm_campaign) like 'k*%' then 'Google - Non-Brand'
            when lower(utm_campaign) like '%merchant_blog%' then 'Merchant Blog'
            when lower(utm_campaign) like '%grouponworks_social%' then 'Social'
            when (lower(utm_campaign) like '%ggl%' and lower(utm_campaign) like '%nbr%') then 'Google - Non-Brand'
            when (lower(utm_campaign) like '%ggl%' and lower(utm_campaign) like '%ybr%') then 'Google - Brand'
            when (lower(utm_campaign) like '%ggl%' and lower(utm_campaign) like '%merchant-competitors%') then 'Google - Competitor'
            when (lower(utm_campaign) like '%ggl%' and lower(utm_campaign) like '%rmk%') then 'Google - Remarketing'
            else 'Other'
        end
      end as campaign_type
  from (
    select
      distinct a.bcookie
      , utm_campaign
      , a.event_date as createddate
      , a.accountid
      , case when length(utm_campaign)-length(translate(utm_campaign,'_','')) > 11 then 1 else 0 end campaign_new_format
      , case when length(utm_campaign)-length(translate(utm_campaign,'_','')) > 11 then lower(split(utm_campaign,'_')[4]) else null end sem_partner
      , case when length(utm_campaign)-length(translate(utm_campaign,'_','')) > 11 then lower(split(utm_campaign,'_')[10]) else null end sem_brand
    from sm_ss_bcookies a
    join user_groupondw.bld_events b
      on a.bcookie = b.bcookie
    where b.dt > '2020-01-01'
      and b.page_type in ('User', 'business_info')
      and b.dt between date_sub(a.event_date, 30) and date_add(a.event_date, 1)
  ) a;

 show table user_groupondw.sf_lead;
 
select 
   distinct event, 
   platform
FROM prod_groupondw.bld_events a
    WHERE 
     dt = '2022-12-02'



SELECT
eventtime,
eventdate,
sessionid,
bcookie,
pagepath,
pagehostname,
rawpagetype,
rawparentpagetype,
pageapp,
useruuid,
userpermalink,
referralurl,
referrerdomain,
campaign,
source,
medium,
referralemailposition,
browser,
browserversion,
useros,
widgetname,
widgetcontentname,
widgetcontenttype,
widgetcontentposition,
widgetcontenttypepos,
platform
FROM grp_gdoop_pde.junohourly
WHERE 
  eventdate = '2022-12-02'
  AND (
      pageapp IN ( 'metro-ui', 'itier-merchant-inbound-acquisition', 'grouponworks-webtolead', 'groupon-webtolead')
      OR widgetname = 'w2lchannelselected'
      OR (pagepath = '/merchant' or pagepath = '/merchant/signup')
    )
limit 100;

select * 
from grp_gdoop_sup_analytics_db.jc_w2l_funnel_events 
where dt = '2022-12-02' 
and (page_path is not null or referrer_url  is not null)
limit 200;



select a.*
from grp_gdoop_sup_analytics_db.jc_w2l_funnel_events  as a 
where dt = '2022-12-02' 
and (
     length(utm_campaign_channel) > 4 or 
     length(utm_campaign_strategy) > 3 or 
     length(utm_campaign_inventory) > 3 or 
     length(utm_campaign_brand) > 3 or 
     length(mktg_adgroup) > 3 or 
     length(mktg_ad_matchtype) > 3 or 
     length(email_position) > 3 )
limit 200;

select 
   eventtime,
eventdate,
sessionid,
bcookie,
pagepath,
rawpagetype,
rawparentpagetype,
pageapp,
useruuid,
userpermalink,
referralurl,
referrerdomain,
campaign,
source,
medium,
referralemailposition,
browser,
browserversion,
useros,
widgetname,
widgetcontentname,
widgetcontenttype,
widgetcontentposition,
widgetcontenttypepos,
platform,
eventdestination,
event 
FROM grp_gdoop_pde.junohourly
WHERE 
  pageapp = 'itier-merchant-inbound-acquisition'
  and eventdate = '2022-12-02'
  and sessionid = 'fa5eb220-370a-4374-9eb2-20370af374d9'
 limit 5
;


select 
  *
FROM grp_gdoop_pde.junohourly
WHERE 
   eventdestination = 'searchBrowseView'
  and event = 'genericPageView'
  and pageapp = 'metro-ui'
  and eventdate = '2022-12-02'
  and sessionid = 'd1d1a337-eab7-4f20-91a3-37eab74f20c9'
;


select 
  *
FROM grp_gdoop_pde.junohourly
WHERE 
  pageapp = 'itier-merchant-inbound-acquisition'
  and eventdate = '2022-12-02'
  and sessionid = 'de8f1a5d-7f04-4b6c-8f1a-5d7f04fb6c5c';

select 
  eventdate,
  unix_timestamp(eventdate)
FROM grp_gdoop_pde.junohourly
WHERE 
   eventdestination = 'searchBrowseView'
  and event = 'genericPageView'
  and pageapp = 'itier-merchant-inbound-acquisition'
  and eventdate = '2022-12-02'
  and sessionid = 'fa5eb220-370a-4374-9eb2-20370af374d9'
 limit 3;
 
 select 
  fullurl,
  referralurl, 
  campaign
FROM grp_gdoop_pde.junohourly
WHERE 
   eventdestination = 'searchBrowseView'
  and event = 'genericPageView'
  and pageapp = 'itier-merchant-inbound-acquisition'
  and eventdate = '2022-12-02'
  and sessionid = 'de8f1a5d-7f04-4b6c-8f1a-5d7f04fb6c5c';



select 
  *
FROM grp_gdoop_pde.junohourly
WHERE 
  eventdate = '2022-12-02'
  and pagepath like '%/merchant/center/draft/campaigns%'
  and  pageapp = 'metro-ui'
  and sessionid = 'd1d1a337-eab7-4f20-91a3-37eab74f20c9'
;



---'fa5eb220-370a-4374-9eb2-20370af374d9'


page_country
page_type
event
widget_name
page_app
event
dt
bcookie
user_uuid
updated_user_device_type
updated_traffic_source
utm_campaign
utm_medium
utm_source
campaign_type
traffic_source
traffic_sub_source
referrer_url
referrer_domain
utm_campaign 
campaign_type
page_path
page_country
page_type
ext_deal_uuid,
event_time


select 
    concat(eventdate, ' 00:00:00') event_time, 
    sessionid
    bcookie
    fullurl
    pagepath
    channel
    division
    coalesce(rawpagetype, pageviewtype)
    pageapp
    consumerid
    userpermalink
    deviceid
    devicetype
    referralurl



INSERT OVERWRITE TABLE grp_gdoop_sup_analytics_db.jc_w2l_funnel_events
  PARTITION(dt)
  SELECT
         sub.event_time,
         -- sub.client_event_time,
         -- sub.event_id,
         -- sub.parent_event_id,
         sub.session_id,
         sub.bcookie,
         -- sub.client_ip_address,
         -- sub.page_id,
         -- sub.parent_page_id,
         sub.page_url,
         sub.page_path,
         -- sub.page_hostname,
         sub.page_channel,
         sub.page_division,
         sub.page_type,
         sub.page_app,
         -- sub.page_domain,
         sub.user_uuid,
         -- sub.user_agent,
         sub.user_permalink,
         -- sub.user_logged_in,
         sub.user_device,
         sub.user_device_type,
         -- sub.deal_permalink,
         -- sub.deal_option_id,
         -- sub.deal_uuid,
         -- sub.deal_channel,
         -- sub.order_id,
         -- sub.order_uuid,
         -- sub.parent_order_uuid,
         -- sub.parent_order_id,
         sub.referrer_url,
         sub.referrer_search_term,
         sub.referrer_domain,
         sub.utm_campaign,
         sub.utm_source,
         sub.utm_medium,
         sub.mktg_campaign,
         sub.utm_campaign_channel,
         sub.utm_campaign_strategy,
         sub.utm_campaign_inventory,
         sub.utm_campaign_brand,
         sub.mktg_adgroup,
         sub.mktg_ad_matchtype,
         sub.email_position,
         sub.browser,
         sub.browser_version,
         sub.os,
         sub.widget_name,
         sub.widget_content_name,
         sub.widget_content_type,
         sub.widget_content_position,
         sub.widget_content_typepos,
         -- sub.secondary_widgets,
         -- sub.shopping_cart_uuid,
         -- sub.cart_contents,
         -- sub.bot_flag,
         -- sub.mobile_flag,
         -- sub.internal_ip_ind,
         -- sub.page_campaign,
         -- sub.widget_campaign,
         -- sub.widget_content_campaign,
         sub.platform,
         -- sub.click_widget_data,
         -- sub.click_widget_content_data,
         sub.event,
         sub.page_country,
         sub.ref_attr_class_key,
         sub.campaign_new_format,
         sub.sem_partner,
         sub.sem_brand,
         CASE
                WHEN utm_source IN ('groupon', 'grouponapp', 'livingsocial') THEN 'Referral'
                WHEN traffic_source = 'Free Referral' THEN 'Referral'
                WHEN utm_medium = 'organic' THEN 'Organic'
                WHEN utm_medium = 'blog-post' THEN 'SEO'
                WHEN utm_source = 'merchant_blog' THEN 'SEO'
                WHEN utm_source = 'home' THEN 'SEO'
                WHEN utm_medium = 'afl' THEN 'Affiliate'
                WHEN utm_source LIKE 'gm_%' THEN 'Email'
                WHEN utm_medium = 'email' THEN 'Consumer Email'
                WHEN utm_source = 'fbk' THEN 'Display'
                WHEN utm_source = 'google display' THEN 'Display'
                WHEN utm_medium = 'social' THEN 'Social'
                WHEN traffic_sub_source = 'Direct' THEN 'Direct'
                WHEN utm_source = 'crm_im' THEN 'Consumer Email'
                WHEN utm_campaign LIKE '%YBR%' THEN 'SEM - Brand'
                WHEN utm_campaign LIKE '%NBR%' THEN 'SEM - NB'
         ELSE 'Other'
         END updated_traffic_source,
         REGEXP_EXTRACT(page_url, '.*\/deal\/(.*)\/', 1) AS ext_deal_uuid,
         traffic_source,
         traffic_sub_source,
         CASE
                WHEN page_app = 'grouponworks-webtolead' AND LOWER(os) = 'ios' THEN 'iPhone'
                WHEN page_app = 'grouponworks-webtolead' AND LOWER(os) = 'android' THEN 'Android'
                WHEN LOWER(platform) = 'desktop' THEN 'Desktop'
                WHEN LOWER(platform) = 'touch' AND LOWER(os) = 'ios' THEN 'iPhone'
                WHEN LOWER(platform) = 'touch' AND LOWER(os) = 'android' THEN 'Android'
         ELSE 'Other'
         END AS updated_user_device_type,
         CASE
                WHEN LOWER(utm_campaign) = 'other' THEN "'Other' in SF"
                WHEN LOWER(utm_campaign) = 'squareappmarketplace' then 'Square Marketplace'
                WHEN LOWER(utm_campaign) IN ('groupon-header-app1', 'groupon-header1', 'groupon-header-mweb1', 'groupon-footer') THEN LOWER(utm_campaign)
                WHEN (LOWER(utm_campaign) LIKE '%us_un_som_fac_tim_lb_rr_cbp%' AND LOWER(utm_campaign) LIKE '%fdp%') THEN 'Facebook F&D'
                WHEN (LOWER(utm_campaign) LIKE '%us_un_som_fac_tim_lb_rr_cbp%' AND LOWER(utm_campaign) LIKE '%hbp%') THEN 'Facebook HBW'
                WHEN LOWER(utm_campaign) LIKE '%us_un_som_fac_tim_lb_rr_cbp_chw_sy_v30%' THEN 'Social Retargeting'
                WHEN LOWER(utm_campaign) LIKE '%d*merchant-fb%' THEN 'Facebook_old'
                WHEN LOWER(utm_campaign) LIKE '%g*gmail-ads%' THEN  'GMail Ads'
                WHEN LOWER(utm_campaign) LIKE '%d*gw-dx-dis%' THEN  'DataXu'
                WHEN LOWER(utm_campaign) LIKE '%groupon%' THEN 'Referral'
                WHEN LOWER(utm_campaign) LIKE '%livingsocial_ib%' THEN 'Living Social'
                WHEN LOWER(utm_campaign) LIKE '%delivery-takeout-lp%' THEN 'Delivery Takeout'
                WHEN utm_campaign = '50_DLS' THEN 'Referral'
                WHEN utm_campaign = '50' THEN 'Referral'
                WHEN (LOWER(utm_campaign) LIKE '%grouponworks%' AND LOWER(utm_campaign) LIKE '%social%') THEN 'Social'
                WHEN LOWER(utm_campaign) LIKE '%merchant-retargeting%' THEN 'Merchant Retargeting'
                WHEN LOWER(utm_campaign) LIKE '%merchant-stream%' THEN 'Yahoo Stream Ads'
                WHEN LOWER(utm_campaign) LIKE '%d*merchant-gsp%' THEN 'Gmail Sponsored Promotions'
                WHEN LOWER(utm_campaign) LIKE '%blog%' OR LOWER(utm_campaign) LIKE '%merchant_blog%' OR LOWER(utm_campaign) LIKE '%merchant_article%' OR LOWER(utm_campaign) LIKE '%merchant-blog-how-to-sell-post%' OR LOWER(utm_campaign) LIKE '%merchant-blog-sidebar%' THEN 'Merchant Blog/SBRC'
                WHEN LOWER(utm_campaign) LIKE '%merchantnl%' OR LOWER(utm_campaign) LIKE '%june2014%' OR LOWER(utm_campaign) LIKE '%july2014%' OR LOWER(utm_campaign) LIKE '%august2014%' OR LOWER(utm_campaign) LIKE '%september2014%' OR LOWER(utm_campaign) LIKE '%october2014%' OR LOWER(utm_campaign) LIKE '%november2014%' OR LOWER(utm_campaign) LIKE '%december2014%' OR LOWER(utm_campaign) LIKE '%january2015%' OR LOWER(utm_campaign) LIKE '%feb2015%' OR LOWER(utm_campaign) LIKE '%mar2015%' OR LOWER(utm_campaign) LIKE '%apr2015%' OR LOWER(utm_campaign) LIKE '%may2015%' OR LOWER(utm_campaign) LIKE '%june2015%' OR LOWER(utm_campaign) LIKE '%july2015%' OR LOWER(utm_campaign) LIKE '%august2015%' OR LOWER(utm_campaign) LIKE '%sept2015%' OR LOWER(utm_campaign) LIKE '%oct2015%' OR LOWER(utm_campaign) LIKE '%nov2015%' OR LOWER(utm_campaign) LIKE '%dec2015%' OR LOWER(utm_campaign) LIKE '%jan2016%' THEN 'Merchant Newsletter'
                WHEN LOWER(utm_campaign) LIKE '%print%' OR LOWER(utm_campaign) LIKE '%nra2016%' OR LOWER(utm_campaign) LIKE '%osr-cards%' OR LOWER(utm_campaign) LIKE '%hbw-2016%' OR LOWER(utm_campaign) LIKE '%austin-promo-16%' OR LOWER(utm_campaign) LIKE '%cultural-institutions-2015%' OR LOWER(utm_campaign) LIKE '%ttd-cultural-institutions%' OR LOWER(utm_campaign) LIKE '%ttd-culture-2016%' OR LOWER(utm_campaign) LIKE '%ttd-culture-2016%' OR LOWER(utm_campaign) LIKE '%ttd-activities-2016%' OR LOWER(utm_campaign) LIKE '%activities-2015%' OR LOWER(utm_campaign) LIKE '%events-2015%' OR LOWER(utm_campaign) LIKE '%astc-promo-16%' THEN 'Print'
                WHEN LOWER(utm_campaign) LIKE '%grouponworks_social%' THEN 'Social'
                WHEN LOWER(utm_campaign) LIKE '%occasions_sponsor%' THEN 'Occasions_sponsor'
                WHEN LOWER(utm_campaign) LIKE '%occasions%' THEN 'Occasions'
                WHEN LOWER(utm_campaign) LIKE '%reserve%' THEN 'Reserve'
                WHEN LOWER(utm_campaign) LIKE '%getaways%' THEN 'Getaways'
                WHEN LOWER(utm_campaign) LIKE '%occasions_sponsor%' THEN 'Sponsored Occasions'
                WHEN LOWER(utm_campaign) LIKE '%st_text%' THEN 'GCN'
                WHEN LOWER(utm_campaign) LIKE '%toolkit%' THEN 'Score'
                WHEN LOWER(utm_campaign) LIKE '%mc_ppl%' THEN 'Merchant Circle'
                WHEN LOWER(utm_campaign) LIKE '%NRA_%' THEN 'NRA'
                WHEN LOWER(utm_campaign) LIKE '%linkedin_%' THEN 'LinkedIn'
                WHEN LOWER(utm_campaign) LIKE '%payments%' THEN 'Payments'
                WHEN LOWER(utm_campaign) LIKE '%goods%' THEN 'Goods'
                WHEN (LOWER(utm_campaign) LIKE '%merchant%' AND LOWER(utm_campaign) LIKE '%food%' AND LOWER(utm_campaign) LIKE '%drink%' AND LOWER(utm_campaign) LIKE '%srm%') THEN 'Merchant-Food-Drink-SRM'
                WHEN (LOWER(utm_campaign) LIKE '%merchant%' AND LOWER(utm_campaign) LIKE '%ybr%' AND LOWER(utm_campaign) LIKE '%srm%') THEN 'Merchant-YBR-SRM'
                WHEN (LOWER(utm_campaign) LIKE '%merchant%' AND LOWER(utm_campaign) LIKE '%ybr%' AND LOWER(utm_campaign) LIKE '%scm%') THEN 'Merchant-YBR-SCM'
                WHEN (LOWER(utm_campaign) LIKE '%ppl%' AND LOWER(utm_campaign) LIKE '%gen%' AND LOWER(utm_campaign) LIKE '%sug2013%') THEN 'AdKnowledge_aug2013'
                WHEN (LOWER(utm_campaign) LIKE '%ppl%' AND LOWER(utm_campaign) LIKE '%info%') THEN 'InfoGroup'
                WHEN (LOWER(utm_campaign) LIKE '%merchant%' AND LOWER(utm_campaign) LIKE '%food%' AND LOWER(utm_campaign) LIKE '%drink%' AND LOWER(utm_campaign) LIKE '%scm%') THEN 'Merchant-Food-Drink-SCM'
                WHEN (LOWER(utm_campaign) LIKE '%leisure%' AND LOWER(utm_campaign) LIKE '%activities%') THEN 'Leisure-Activities'
                WHEN (LOWER(utm_campaign) LIKE '%beauty%' AND LOWER(utm_campaign) LIKE '%wellness%') THEN 'Beauty-Wellness'
                WHEN (LOWER(utm_campaign) LIKE '%food%' AND LOWER(utm_campaign) LIKE '%drink%') THEN 'Food & Drink'
                WHEN LOWER(utm_campaign) LIKE '%direct%' THEN 'Direct'
                WHEN LOWER(utm_campaign) LIKE '%organic%' THEN 'Organic'
                WHEN LOWER(utm_campaign) LIKE '%referral%' THEN 'Referral'
                WHEN (sem_partner LIKE '%ggl%' AND sem_brand LIKE '%nbr%') THEN 'Google - Non-Brand'
                WHEN (sem_partner LIKE '%ggl%' AND sem_brand LIKE '%ybr%') THEN 'Google - Brand'
                WHEN (sem_partner LIKE '%ggl%' AND sem_brand LIKE '%merchant-competitors%') THEN 'Google - Competitor'
                WHEN (sem_partner LIKE '%bng%' AND sem_brand LIKE '%nbr%') THEN 'Bing - Non-Brand'
                WHEN (sem_partner LIKE '%bng%' AND sem_brand LIKE '%ybr%') THEN 'Bing - Brand'
                WHEN (sem_partner LIKE '%bng%' AND sem_brand LIKE '%cbr%') THEN 'Bing - Competitor'
                WHEN LOWER(split(utm_campaign,'_')[1]) LIKE 'g1%' THEN 'G1'
                WHEN LOWER(split(utm_campaign,'_')[1]) LIKE 'goods%' THEN 'Goods'
                WHEN LOWER(split(utm_campaign,'_')[1]) LIKE 'occasion%' THEN 'Occasions'
                WHEN LOWER(split(utm_campaign,'_')[1]) LIKE 'getaways%' THEN 'Getaways'
                WHEN LOWER(split(utm_campaign,'_')[1]) LIKE 'reserve%' THEN 'Reserve'
                WHEN LOWER(split(utm_campaign,'_')[1]) LIKE 'collection%' THEN 'Collections'
                WHEN LOWER(split(utm_campaign,'_')[1]) LIKE 'payments%' THEN 'Payments'
                WHEN LOWER(utm_campaign) LIKE 'k*%' THEN 'Google - Non-Brand'
                WHEN LOWER(utm_campaign) LIKE '%merchant_blog%' THEN 'Merchant Blog'
                WHEN LOWER(utm_campaign) LIKE '%grouponworks_social%' THEN 'Social'
                WHEN (LOWER(utm_campaign) LIKE '%ggl%' AND LOWER(utm_campaign) LIKE '%nbr%') THEN 'Google - Non-Brand'
                WHEN (LOWER(utm_campaign) LIKE '%ggl%' AND LOWER(utm_campaign) LIKE '%ybr%') THEN 'Google - Brand'
                WHEN (LOWER(utm_campaign) LIKE '%ggl%' AND LOWER(utm_campaign) LIKE '%merchant-competitors%') THEN 'Google - Competitor'
                WHEN (LOWER(utm_campaign) LIKE '%ggl%' AND LOWER(utm_campaign) LIKE '%rmk%') THEN 'Google - Remarketing'
                WHEN (LOWER(utm_campaign) LIKE '%free_advertising%') THEN 'Google NB - Free Advertising'
                WHEN (LOWER(utm_campaign) LIKE '%sb_adv%') THEN 'Google NB - SB-Adv'
                WHEN (LOWER(utm_campaign) LIKE '%promote%') THEN 'Google NB - Promote'
                WHEN (LOWER(utm_campaign) LIKE '%advertise%') THEN 'Google NB - Advertise'
                WHEN (LOWER(utm_campaign) LIKE '%number%') THEN 'Google Brand - Number'
                WHEN (LOWER(utm_campaign) LIKE '%contact_merchant%') THEN 'Google Brand - Contact Merchant'
                WHEN (LOWER(utm_campaign) LIKE '%advertising%') THEN 'Google Brand - Advertising'
                WHEN (LOWER(utm_campaign) LIKE '%how_to_business%') THEN 'Google Brand - How To Business'
                WHEN (LOWER(utm_campaign) LIKE '%business%') THEN 'Google Brand - Business'
                WHEN (LOWER(utm_campaign) LIKE '%join%') THEN 'Google Brand - Join'
                WHEN (LOWER(utm_campaign) LIKE '%merchant_misc%') THEN 'Google Brand - Merchant Misc'
                WHEN (sem_partner LIKE '%ggl%' AND sem_brand LIKE '%merchant-competitors%') THEN 'Google - Competitor'
                WHEN (sem_partner LIKE '%bng%' AND sem_brand LIKE '%nbr%') THEN 'Bing - Non-Brand'
                WHEN (sem_partner LIKE '%bng%' AND sem_brand LIKE '%ybr%') THEN 'Bing - Brand'
                WHEN (sem_partner LIKE '%bng%' AND sem_brand LIKE '%cbr%') THEN 'Bing - Competitor'
                WHEN LOWER(split(utm_campaign,'_')[1]) LIKE 'g1%' THEN 'G1'
                WHEN LOWER(split(utm_campaign,'_')[1]) LIKE 'goods%' THEN 'Goods'
                WHEN LOWER(split(utm_campaign,'_')[1]) LIKE 'occasion%' THEN 'Occasions'
                WHEN LOWER(split(utm_campaign,'_')[1]) LIKE 'getaways%' THEN 'Getaways'
                WHEN LOWER(split(utm_campaign,'_')[1]) LIKE 'reserve%' THEN 'Reserve'
                WHEN LOWER(split(utm_campaign,'_')[1]) LIKE 'collection%' THEN 'Collections'
                WHEN LOWER(split(utm_campaign,'_')[1]) LIKE 'payments%' THEN 'Payments'
                WHEN LOWER(utm_campaign) LIKE 'k*%' THEN 'Google - Non-Brand'
                WHEN LOWER(utm_campaign) LIKE '%merchant_blog%' THEN 'Merchant Blog'
                WHEN LOWER(utm_campaign) LIKE '%grouponworks_social%' THEN 'Social'
                WHEN (LOWER(utm_campaign) LIKE '%ggl%' AND LOWER(utm_campaign) LIKE '%nbr%') THEN 'Google - Non-Brand'
                WHEN (LOWER(utm_campaign) LIKE '%ggl%' AND LOWER(utm_campaign) LIKE '%ybr%') THEN 'Google - Brand'
                WHEN (LOWER(utm_campaign) LIKE '%ggl%' AND LOWER(utm_campaign) LIKE '%merchant-competitors%') THEN 'Google - Competitor'
                WHEN (LOWER(utm_campaign) LIKE '%ggl%' AND LOWER(utm_campaign) LIKE '%rmk%') THEN 'Google - Remarketing'
         ELSE 'Other'
         END AS campaign_type,
         sub.dt
  FROM
  (
    SELECT
          a.*,
          TrafficSource(utm_medium, utm_source, utm_campaign, referrer_domain) AS ref_attr_class_key,
          CASE WHEN LENGTH(utm_campaign)-LENGTH(TRANSLATE(utm_campaign,'_','')) > 11 THEN 1 ELSE 0 END campaign_new_format,
          CASE WHEN LENGTH(utm_campaign)-LENGTH(TRANSLATE(utm_campaign,'_','')) > 11 THEN LOWER(SPLIT(utm_campaign,'_')[4]) ELSE NULL END AS sem_partner,
          CASE WHEN LENGTH(utm_campaign)-LENGTH(TRANSLATE(utm_campaign,'_','')) > 11 tHEN LOWER(SPLIT(utm_campaign,'_')[10]) ELSE NULL END AS sem_brand
    FROM prod_groupondw.bld_events a
    WHERE event IN ('click', 'pageview')
    AND page_country NOT IN ('JP', 'NZ')
    AND dt BETWEEN date_sub(current_date, 8) AND current_date
    --AND dt >= '2019-01-01'
    AND (
      page_app IN ( 'metro-ui', 'itier-merchant-inbound-acquisition', 'grouponworks-webtolead', 'groupon-webtolead')
      OR widget_name = 'w2lchannelselected'
      OR (page_path = '/merchant' or page_path = '/merchant/signup')
    )
    AND COALESCE(TRIM(bcookie), '') != ''
    AND LOWER(TRIM(bcookie)) != 'null'
    AND COALESCE(TRIM(session_id), '') != ''
    AND LOWER(TRIM(session_id)) != 'null'
  ) sub
  LEFT JOIN prod_groupondw.ref_attr_class rac ON sub.ref_attr_class_key = rac.ref_attr_class_key
;





-- ANALYZE TABLE grp_gdoop_sup_analytics_db.jc_w2l_funnel_events PARTITION(dt) COMPUTE STATISTICS;
-- ANALYZE TABLE grp_gdoop_sup_analytics_db.jc_w2l_funnel_events PARTITION(dt) COMPUTE STATISTICS FOR COLUMNS;

select * from grp_gdoop_sup_analytics_db.jc_merchant_deal_order where account_id = '0013c00002312JsAAI';

INSERT OVERWRITE TABLE grp_gdoop_sup_analytics_db.jc_merchant_deal_order
  SELECT
         country_code,
         account_id,
         merchant_uuid,
         merchant_name,
         deal_uuid,
         opportunity_id,
         closedate,
         launch_date,
         deal_paused_date,
         grt_l2_cat_name AS vertical,
         pds_cat_id,
         pds,
         dmapi_flag,
         grt_l1_cat_name,
         metro_submit_time,
         metro_bld_events_flag,
         opportunity_name,
         division,
         go_live_date,
         Straight_to_Private_Sale,
         ownerid,
         metal_at_close,
         stagename,
         ROW_NUMBER() OVER (PARTITION BY country_code, account_id ORDER BY closedate, metro_submit_time) close_order,
         ROW_NUMBER() OVER (PARTITION BY country_code, account_id ORDER BY closedate DESC, metro_submit_time DESC) close_recency
  FROM
  (
   SELECT
         o1.country_code,
         COALESCE(o1.account_id, dm1.account_id, dm.account_id) AS account_id,
         COALESCE(dm1.merchant_uuid, dm.merchant_uuid) AS merchant_uuid,
         COALESCE(dm1.merchant_name, dm.merchant_name) AS merchant_name,
         o2.deal_uuid,
         o1.opportunity_id,
         o1.closedate,
         COALESCE(dmp.primary_dealservice_cat_id, grt.pds_cat_id, grt3.pds_cat_id, '') AS pds_cat_id,---missing
         COALESCE(grt.pds_cat_name, o1.primary_deal_services) AS pds,
         o1.dmapi_flag,
         coalesce(grt.grt_l2_cat_name, grt3.grt_l2_cat_name) AS grt_l2_cat_name,
         coalesce(grt.grt_l1_cat_name, grt3.grt_l1_cat_name) AS grt_l1_cat_name,
         CAST(CASE WHEN COALESCE(bh.ext_deal_uuid, bh1.ext_deal_uuid) IS NOT NULL THEN 1 ELSE 0 END AS TINYINT) AS metro_bld_events_flag,
         COALESCE(bh.metro_submit_time, bh1.metro_submit_time) AS metro_submit_time,
         ad.launch_date,
         ad.deal_paused_date,
         o1.opportunity_name,
         o1.division,
         o1.go_live_date,
         o1.Straight_to_Private_Sale,
         o1.ownerid,
         sda.merchant_seg_at_closed_won AS metal_at_close,
         o1.stagename
  FROM
    (
      SELECT
             CASE WHEN COALESCE(feature_country, 'US') = 'GB' THEN 'UK'
                  WHEN COALESCE(feature_country, 'US') IN ('VI', 'FM', 'PR', 'MH') THEN 'US'
                  ELSE COALESCE(feature_country, 'US')
             END  AS country_code,
             opportunity_id,
             closedate,
             accountid AS account_id,
             id,
             division,
             opportunity_name,
             primary_deal_services,
             go_live_date,
             Straight_to_Private_Sale,
             ownerid,
             deal_attribute,
             stagename AS stagename,
             CAST(CASE WHEN LOWER(opportunity_name) LIKE '%dmapi%' or LOWER(opportunity_name) LIKE '%*G1M*%' THEN 1 ELSE 0 END AS TINYINT) AS dmapi_flag
      FROM edwprod.sf_opportunity_1
      WHERE LENGTH(opportunity_id) = 15
      AND opportunity_id IS NOT NULL
      AND LOWER(stagename) IN ('closed lost', 'closed won', 'merchant not interested')
      -- accounts identified as engg test accounts
      AND accountid NOT IN ('0013c00001tcgNOAAY', '0013c00001tcjejAAA', '0013c00001sFbx6AAC', '0013c00001tbrwoAAA', '0013c00001sFbwrAAC', '001C0000017G6oTIAS',
                            '001C0000019gQijIAE', '0013c00001tcsUjAAI', '0013c00001tckUuAAI', '0013c00001tckRCAAY', '0013c00001tckUuAAI', '001C0000017EdaQIAS',
                            '0013c00001tckSPAAY', '0013c00001tckTDAAY')
      AND COALESCE(feature_country, 'US') IN ('AE', 'AU', 'BE', 'CA', 'DE', 'ES', 'FR', 'GB', 'IE', 'IT', 'NL', 'PL', 'QC', 'UK', 'US')
    ) o1
    LEFT OUTER JOIN (SELECT DISTINCT deal_uuid, Id FROM edwprod.sf_opportunity_2 WHERE LENGTH(opportunity_id) = 15 AND opportunity_id IS NOT NULL) o2 ON o1.ID = o2.ID
    LEFT OUTER JOIN grp_gdoop_sup_analytics_db.sf_deal_attribute sda ON sda.id = o1.deal_attribute
    LEFT OUTER JOIN
    (
      SELECT CASE WHEN COALESCE(country_code, 'US') = 'GB' THEN 'UK'
                  WHEN COALESCE(country_code, 'US') IN ('VI', 'FM', 'PR', 'MH') THEN 'US'
                  ELSE COALESCE(country_code, 'US')
             END AS country_code,
             deal_uuid,
             MIN(load_date) AS launch_date,
             MAX(load_date) AS deal_paused_date
      FROM prod_groupondw.active_deals
      WHERE country_code IN ('AE', 'AU', 'BE', 'CA', 'DE', 'ES', 'FR', 'GB', 'IE', 'IT', 'NL', 'PL', 'QC', 'UK', 'US')
      GROUP BY
             CASE WHEN COALESCE(country_code, 'US') = 'GB' THEN 'UK'
                  WHEN COALESCE(country_code, 'US') IN ('VI', 'FM', 'PR', 'MH') THEN 'US'
                  ELSE COALESCE(country_code, 'US')
             END,
             deal_uuid
    ) ad
    ON o2.deal_uuid = ad.deal_uuid
    AND o1.country_code = ad.country_code
    LEFT OUTER JOIN (SELECT DISTINCT deal_uuid, primary_dealservice_cat_id FROM edwprod.deal_merch_product) dmp ON dmp.deal_uuid = ad.deal_uuid
    LEFT OUTER JOIN dw.mv_dim_pds_grt_map grt ON dmp.primary_dealservice_cat_id = grt.pds_cat_id
    LEFT OUTER JOIN dw.mv_dim_pds_grt_map grt3 ON o1.primary_deal_services = grt3.pds_cat_name
    LEFT OUTER JOIN
    (
      SELECT CASE WHEN COALESCE(page_country, 'US') = 'GB' THEN 'UK'
                  WHEN COALESCE(page_country, 'US') IN ('VI', 'FM', 'PR', 'MH') THEN 'US'
                  ELSE COALESCE(page_country, 'US')
             END AS page_country,
             ext_deal_uuid,
             MIN(event_time) AS metro_submit_time
      FROM grp_gdoop_sup_analytics_db.jc_w2l_funnel_events--
      WHERE page_type = 'congratulations'
      OR (page_type = 'contract' AND event = 'click' AND widget_name = 'submitcontractfooter')
      GROUP BY
              CASE WHEN COALESCE(page_country, 'US') = 'GB' THEN 'UK'
                   WHEN COALESCE(page_country, 'US') IN ('VI', 'FM', 'PR', 'MH') THEN 'US'
                   ELSE COALESCE(page_country, 'US')
              END,
              ext_deal_uuid
    ) bh
    ON bh.ext_deal_uuid = o2.deal_uuid
    AND bh.page_country = o1.country_code
    LEFT OUTER JOIN
    (
      SELECT ext_deal_uuid,
             MIN(event_time) AS metro_submit_time
      FROM grp_gdoop_sup_analytics_db.jc_w2l_funnel_events--
      WHERE page_type = 'congratulations'
      OR (page_type = 'contract' AND event = 'click' AND widget_name = 'submitcontractfooter')
      GROUP BY 1
    ) bh1
    ON bh1.ext_deal_uuid = o2.deal_uuid
    LEFT OUTER JOIN edwprod.dim_offer_ext doe ON doe.product_uuid = o2.deal_uuid
    LEFT OUTER JOIN
    (
       SELECT merchant_uuid,
              SUBSTR(MAX(CONCAT(TO_DATE(updated_at), salesforce_account_id)), 11) AS account_id,
              SUBSTR(MAX(CONCAT(TO_DATE(updated_at), name)), 11) AS merchant_name
       FROM edwprod.dim_merchants_unity
       WHERE dwh_active = 1
       GROUP BY 1
    ) dm
    ON doe.merchant_uuid = dm.merchant_uuid
    LEFT OUTER JOIN
    (
      SELECT salesforce_account_id AS account_id,
             SUBSTR(MAX(CONCAT(TO_DATE(updated_at), merchant_uuid)), 11) AS merchant_uuid,
             SUBSTR(MAX(CONCAT(TO_DATE(updated_at), name)), 11) AS merchant_name
      FROM edwprod.dim_merchants_unity
      WHERE dwh_active = 1
      GROUP BY 1
    ) dm1
    ON dm1.account_id = COALESCE(o1.account_id, dm.account_id)
    GROUP BY
          o1.country_code,
          COALESCE(o1.account_id, dm1.account_id, dm.account_id),
          COALESCE(dm1.merchant_uuid, dm.merchant_uuid),
          COALESCE(dm1.merchant_name, dm.merchant_name),
          o2.deal_uuid,
          o1.opportunity_id,
          o1.closedate,
          COALESCE(dmp.primary_dealservice_cat_id, grt.pds_cat_id, grt3.pds_cat_id, ''),
          COALESCE(grt.pds_cat_name, o1.primary_deal_services),
          o1.dmapi_flag,
          coalesce(grt.grt_l2_cat_name, grt3.grt_l2_cat_name),
          coalesce(grt.grt_l1_cat_name, grt3.grt_l1_cat_name),
          CAST(CASE WHEN COALESCE(bh.ext_deal_uuid, bh1.ext_deal_uuid) IS NOT NULL THEN 1 ELSE 0 END AS TINYINT),
          COALESCE(bh.metro_submit_time, bh1.metro_submit_time),
          ad.launch_date,
          ad.deal_paused_date,
          o1.opportunity_name,
          o1.division,
          o1.go_live_date,
          o1.Straight_to_Private_Sale,
          o1.ownerid,
          sda.merchant_seg_at_closed_won,
          o1.stagename
  ) ld
;

-----------------------------------------------------
CREATE TABLE grp_gdoop_sup_analytics_db.jc_merchant_mtd_attrib
  (
    accountid STRING,
    feature_country STRING,
    deal_uuid STRING,
    opportunity_id STRING,
    close_date STRING,
    opportunity_name STRING,
    launch_date STRING,
    division STRING,
    metal_at_close STRING,
    billingcity STRING,
    grt_l1_cat_name STRING,
    pds STRING,
    vertical STRING,
    close_order SMALLINT,
    close_recency SMALLINT,
    next_close_on STRING,
    launch_order SMALLINT,
    deal_paused_date STRING,
    stagename STRING,
    go_live_date STRING,
    straight_to_private_sale STRING,
    dmapi_flag TINYINT,
    title_rw STRING,
    team STRING,
    mtd_attribution STRING,
    mtd_attribution_intl STRING,
    ownerid STRING,
    por_relaunch TINYINT,
    metro_submit_time STRING,
    metro_bld_events_flag TINYINT
  )
  CLUSTERED BY (opportunity_id) SORTED BY (opportunity_id) INTO 15 BUCKETS
  STORED AS ORC
  TBLPROPERTIES ('orc.compress'='SNAPPY')
;

select * from grp_gdoop_sup_analytics_db.jc_merchant_mtd_attrib where accountid =  '0013c00002312JsAAI';

SELECT * FROM user_groupondw.sf_lead WHERE convertedaccountid = '0013c00002312JsAAI';

INSERT OVERWRITE TABLE grp_gdoop_sup_analytics_db.jc_merchant_mtd_attrib
  SELECT
         ma.account_id AS accountid,
         ma.country_code,
         ma.deal_uuid,
         ma.opportunity_id,
         ma.closedate AS close_date,
         ma.opportunity_name,
         ma.launch_date,
         COALESCE(ma.division, dag.opp_division) AS division,
         COALESCE(ma.metal_at_close, 'Nickel') AS metal_at_close,
         COALESCE(day_prev_owner.billingcity, close_owner.billingcity) AS billingcity,
         ma.grt_l1_cat_name,
         ma.pds,
         ma.vertical,
         ma.close_order,
         ma.close_recency,
         CASE WHEN n_c.account_id IS NOT NULL THEN n_c.closedate ELSE NULL END AS next_close_on,
         nl.launch_order,
         CASE WHEN datediff(ma.deal_paused_date, CURRENT_DATE) < 2 THEN NULL ELSE ma.deal_paused_date END deal_paused_date,
         ma.stagename,
         ma.go_live_date,
         ma.Straight_to_Private_Sale,
         ma.dmapi_flag,
         COALESCE(day_prev_owner.title_rw, close_owner.title_rw) AS title_rw,
         COALESCE(day_prev_owner.team, close_owner.team) AS team,
         CASE
             WHEN (dl.convertedaccountid IS NOT NULL OR (ma.dmapi_flag = 1 AND ma.close_order = 1)) AND LOWER(ma.opportunity_name) NOT LIKE '%dmapi%' THEN 'Metro Lead - Sales Team Close'
             WHEN (dl.convertedaccountid IS NOT NULL OR (ma.dmapi_flag = 1 AND ma.close_order = 1)) AND ma.close_order > 1 THEN 'Existing Metro'
             WHEN (dl.convertedaccountid IS NOT NULL OR (ma.dmapi_flag = 1 AND ma.close_order = 1)) AND ma.close_order = 1 THEN 'New Metro'
             WHEN day_prev_owner.mtd_attribution IS NOT NULL THEN day_prev_owner.mtd_attribution
             ELSE close_owner.mtd_attribution
         END mtd_attribution,
         CASE
             WHEN (dl.convertedaccountid IS NOT NULL OR (ma.dmapi_flag = 1 AND ma.close_order = 1)) AND LOWER(ma.opportunity_name) NOT LIKE '%dmapi%' THEN 'Metro Lead - Sales Team Close'
             WHEN (dl.convertedaccountid IS NOT NULL OR (ma.dmapi_flag = 1 AND ma.close_order = 1)) AND ma.close_order > 1 THEN 'Existing Metro'
             WHEN (dl.convertedaccountid IS NOT NULL OR (ma.dmapi_flag = 1 AND ma.close_order = 1)) AND ma.close_order = 1 THEN 'New Metro'
             WHEN mdi.employee_sf_id IS NOT NULL THEN 'BD/MD'
             when msb.owner_group is not null then msb.owner_group
             ELSE 'Existing MS'
         END mtd_attribution_intl,
         ma.ownerid,
         CAST(CASE
                WHEN (ma.opportunity_name LIKE '%*POR RL W1*%' OR ma.opportunity_name LIKE '%POR_%') THEN 1
                WHEN ma.opportunity_name LIKE ('%*POR Wave 2a RL*%') THEN 1
                WHEN ma.opportunity_name LIKE ('%*POR Wave 2b RL*%') THEN 1
                WHEN ma.opportunity_name LIKE ('%*POR ULA RL*%') THEN 1
                WHEN ma.opportunity_name LIKE ('%*POR Wave 3a RL*%') THEN 1
                WHEN ma.opportunity_name LIKE ('%*POR Wave 3b RL*%') THEN 1
                WHEN ma.opportunity_name LIKE ('%*POR Wave 3c RL*%') THEN 1
                WHEN ma.opportunity_name LIKE ('%*POR Wave 3d RL*%') THEN 1
                WHEN ma.opportunity_name LIKE ('%*POR Wave 3e RL*%') THEN 1
                WHEN ma.opportunity_name LIKE ('%*POR Wave 4 RL*%') THEN 1
                WHEN ma.opportunity_name LIKE ('%POR WAVE%') THEN 1
                ELSE 0
         END AS TINYINT) AS por_relaunch,
         ma.metro_submit_time,
         ma.metro_bld_events_flag
  FROM grp_gdoop_sup_analytics_db.jc_merchant_deal_order ma
  LEFT JOIN dwh_base_sec_view.sf_account sfa ON sfa.account_id_18 = ma.account_id
  LEFT JOIN dwh_load_sf_view.sf_user_v2 sfu ON sfa.ownerid = sfu.id
  LEFT JOIN grp_gdoop_sup_analytics_db.jc_intl_md_opps_v mdi ON mdi.employee_sf_id = sfu.id
  left join (
      select
        distinct id
        , case when full_name__c like ('%Merchant M%') then 'MM'
            else case when specialization__c like ('%Merchant Services%') or id = '0053c00000BALn8AAH' then 'MC'
            end
          end as owner_group
      from dwh_load_sf_view.sf_user_v2
      where dwh_active = 1
        and (specialization__c like ('%Merchant Services%')
          or id = '0053c00000BALn8AAH' or full_name__c like ('%Merchant M%')
        )
    ) msb
    on msb.id = sfa.ownerid
  LEFT JOIN (SELECT DISTINCT convertedaccountid, leadsource 
             FROM user_groupondw.sf_lead 
             WHERE convertedaccountid IS NOT NULL AND LOWER(leadsource) = 'metro - self service') dl ON dl.convertedaccountid = ma.account_id
  LEFT JOIN
   (
    SELECT
          account_id,
          launch_date,
          opportunity_id,
          ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY launch_date ASC) launch_order
    FROM grp_gdoop_sup_analytics_db.jc_merchant_deal_order
    WHERE launch_date IS NOT NULL
   ) nl
  ON nl.opportunity_id = ma.opportunity_id
  LEFT JOIN
  (
   SELECT DISTINCT
          c.country_code,
          dag.deal_id,
          dag.opp_division
   FROM grp_gdoop_revenue_management_db.rev_mgmt_gbl_deal_attributes dag
   INNER JOIN
   (
    SELECT DISTINCT
           country_id,
           CASE WHEN COALESCE(country_code, 'US') = 'GB' THEN 'UK'
                WHEN COALESCE(country_code, 'US') IN ('VI', 'FM', 'PR', 'MH', 'US') THEN 'US'
                ELSE COALESCE(country_code, 'US')
           END  AS country_code
    FROM prod_groupondw.dim_country
    WHERE country_id IS NOT NULL
      AND country_code IN ('AE', 'AU', 'BE', 'CA', 'DE', 'ES', 'FR', 'GB', 'IE', 'IT', 'NL', 'PL', 'QC', 'UK', 'US', 'VI', 'FM', 'PR', 'MH')
   ) c
   ON dag.country_id = c.country_id
  )  dag
  ON dag.deal_id = ma.deal_uuid
  AND CASE WHEN dag.country_code IN ('CA', 'QC') THEN 'US' ELSE dag.country_code END = CASE WHEN ma.country_code IN ('CA', 'QC') THEN 'US' ELSE ma.country_code END
  LEFT JOIN
   (
    SELECT
          account_id,
          closedate,
          close_order
    FROM grp_gdoop_sup_analytics_db.jc_merchant_deal_order
   ) n_c
  ON n_c.account_id = ma.account_id
  AND ma.close_order + 1 = n_c.close_order
  LEFT JOIN (SELECT DISTINCT deal_id FROM groupon_production.clo_offers) clo ON clo.deal_id = ma.deal_uuid AND ma.country_code IN ('US', 'CA', 'QC')
  LEFT JOIN
   (
    SELECT
          employee_sf_id,
          CASE
              WHEN specialization IN ('Hunter','Inbound') THEN 'BD'
              WHEN specialization = 'Farmer' THEN 'MD'
              WHEN specialization = 'Hybrid' AND country_code = 'AE' THEN 'MD'
              ELSE specialization
          END AS rep_type,
          TO_DATE(snap_date) AS snap_date
    FROM grp_gdoop_sup_analytics_db.intl_active_sales
   ) is_curr
  ON is_curr.employee_sf_id = ma.ownerid
  AND ma.closedate = is_curr.snap_date
  LEFT JOIN
   (
    SELECT
          employee_sf_id,
          CASE
              WHEN specialization IN ('Hunter','Inbound') THEN 'BD'
              WHEN specialization = 'Farmer' THEN 'MD'
              WHEN specialization = 'Hybrid' AND country_code = 'AE' THEN 'MD'
              ELSE specialization
          END AS rep_type,
          TO_DATE(snap_date) AS snap_date
    FROM grp_gdoop_sup_analytics_db.intl_active_sales
   ) is_prev
  ON is_prev.employee_sf_id = ma.ownerid
  AND DATE_SUB(ma.closedate, 1) = is_prev.snap_date
  LEFT JOIN
   (
     SELECT
           ma.opportunity_id,
           MIN(sf2.billingcity) billingcity,
           MIN(COALESCE(ros1.title_rw, ros2.title_rw)) title_rw,
           MIN(COALESCE(ros1.team, ros2.team)) team,
           MIN(
                CASE
                    WHEN ros1.team = 'Getaway' THEN 'Getaway'
                    WHEN ros1.title_rw IN ('Business Development Director - Outside', 'Business Development Manager - Outside',  'Business Development Manager' , 'Business Development Director', 'Business Development Director - Outside','Business Development Representative - Outside', 'Business Development Representative')THEN 'BD'
                    WHEN ros1.title_rw IN ( 'Merchant Development Director','Merchant Development Manager', 'Merchant Development Representative' , 'Strategic Merchant Development Director') THEN 'MD'
                    WHEN sf2.ownerid IN ('005C000000455ZvIAI', '00580000001YaJIAA0') THEN 'Existing MS'
                    WHEN ros1.title_rw IN( 'Merchant Support Place Holder', 'MS Account Manager' ) THEN  'Existing MS'
                    WHEN ros1.team = 'Team - Inbound' AND ros1.title_rw LIKE '%Inbound%' THEN  'Inbound'
                    WHEN ros1.title_rw IN ( 'Live Account Manager', 'Live Sales Representative') THEN  'Live'
                    WHEN ros1.team = 'Team - FIN' AND ros1.title_rw = 'Business Development Specialist - FIN' THEN  'CLO'
                    WHEN ros2.team = 'Getaway' THEN 'Getaway'
                    WHEN ros2.title_rw IN ('Business Development Director - Outside', 'Business Development Manager - Outside',  'Business Development Manager' , 'Business Development Director', 'Business Development Director - Outside','Business Development Representative - Outside', 'Business Development Representative')THEN 'BD'
                    WHEN ros2.title_rw IN ( 'Merchant Development Director','Merchant Development Manager', 'Merchant Development Representative' , 'Strategic Merchant Development Director') THEN 'MD'
                    WHEN ros2.title_rw IN( 'Merchant Support Place Holder', 'MS Account Manager' ) THEN  'Existing MS'
                    WHEN ros2.team = 'Team - Inbound' AND ros2.title_rw LIKE '%Inbound%' THEN  'Inbound'
                    WHEN ros2.title_rw IN ( 'Live Account Manager', 'Live Sales Representative') THEN  'Live'
                    WHEN ros2.team = 'Team - FIN' AND ros2.title_rw = 'Business Development Specialist - FIN' THEN  'CLO'
                    ELSE 'N/A'
           END) AS mtd_attribution
     FROM grp_gdoop_sup_analytics_db.jc_merchant_deal_order ma
     LEFT JOIN
     (
      SELECT
            DISTINCT
            account_id,
            account_owner_id,
            TO_DATE(valid_from) AS valid_from,
            TO_DATE(valid_to) AS valid_to
         FROM grp_gdoop_sup_analytics_db.global_account_owner_hist
      WHERE account_id IS NOT NULL
        AND account_owner_id IS NOT NULL
     ) own
     ON own.account_id = ma.account_id
     LEFT JOIN (SELECT DISTINCT opportunity_id, person_key FROM prod_groupondw.dim_opportunity WHERE LENGTH(opportunity_id) = 15 AND opportunity_id IS NOT NULL) op ON op.opportunity_id = ma.opportunity_id
     LEFT JOIN (SELECT DISTINCT ultipro_id, person_id FROM prod_groupondw.dim_sf_person WHERE person_id IS NOT NULL) sfp ON sfp.person_id = own.account_owner_id
     LEFT JOIN (SELECT DISTINCT ultipro_id, person_key FROM prod_groupondw.dim_sf_person WHERE person_key IS NOT NULL) sfp_opp ON sfp_opp.person_key = op.person_key
     LEFT JOIN (SELECT DISTINCT emplid, roster_date, team, title_rw FROM grp_gdoop_sup_analytics_db.ops_roster_all WHERE emplid IS NOT NULL AND roster_date IS NOT NULL) ros1 ON sfp.ultipro_id = ros1.emplid AND ros1.roster_date = ma.closedate
     LEFT JOIN (SELECT DISTINCT emplid, roster_date, team, title_rw FROM grp_gdoop_sup_analytics_db.ops_roster_all WHERE emplid IS NOT NULL AND roster_date IS NOT NULL) ros2 ON sfp_opp.ultipro_id = ros2.emplid AND ros2.roster_date = ma.closedate
     LEFT JOIN (SELECT DISTINCT ownerid, account_id_18, billingcity FROM dwh_base_sec_view.sf_account WHERE account_id_18 IS NOT NULL) sf2 ON sf2.account_id_18 = ma.account_id
     WHERE ma.closedate BETWEEN own.valid_from AND own.valid_to
     GROUP BY 1
  ) close_owner
  ON close_owner.opportunity_id = ma.opportunity_id
  LEFT JOIN
   (
     SELECT
           ma.opportunity_id,
           MIN(sf2.billingcity) billingcity,
           MIN(COALESCE(ros1.title_rw, ros2.title_rw)) title_rw,
           MIN(COALESCE(ros1.team, ros2.team)) team,
           MIN(
                CASE
                    WHEN ros1.team = 'Getaway' THEN 'Getaway'
                    WHEN ros1.title_rw IN ('Business Development Director - Outside', 'Business Development Manager - Outside',  'Business Development Manager' , 'Business Development Director', 'Business Development Director - Outside','Business Development Representative - Outside', 'Business Development Representative')THEN 'BD'
                    WHEN ros1.title_rw IN ( 'Merchant Development Director','Merchant Development Manager', 'Merchant Development Representative' , 'Strategic Merchant Development Director') THEN 'MD'
                    WHEN sf2.ownerid IN ('005C000000455ZvIAI', '00580000001YaJIAA0') THEN 'Existing MS'
                    WHEN ros1.title_rw IN( 'Merchant Support Place Holder', 'MS Account Manager' ) THEN  'Existing MS'
                    WHEN ros1.team = 'Team - Inbound' AND ros1.title_rw LIKE '%Inbound%' THEN  'Inbound'
                    WHEN ros1.title_rw IN ( 'Live Account Manager', 'Live Sales Representative') THEN  'Live'
                    WHEN ros1.team = 'Team - FIN' AND ros1.title_rw = 'Business Development Specialist - FIN' THEN  'CLO'
                    WHEN ros2.team = 'Getaway' THEN 'Getaway'
                    WHEN ros2.title_rw IN ('Business Development Director - Outside', 'Business Development Manager - Outside',  'Business Development Manager' , 'Business Development Director', 'Business Development Director - Outside','Business Development Representative - Outside', 'Business Development Representative')THEN 'BD'
                    WHEN ros2.title_rw IN ( 'Merchant Development Director','Merchant Development Manager', 'Merchant Development Representative' , 'Strategic Merchant Development Director') THEN 'MD'
                    WHEN ros2.title_rw IN( 'Merchant Support Place Holder', 'MS Account Manager' ) THEN  'Existing MS'
                    WHEN ros2.team = 'Team - Inbound' AND ros2.title_rw LIKE '%Inbound%' THEN  'Inbound'
                    WHEN ros2.title_rw IN ( 'Live Account Manager', 'Live Sales Representative') THEN  'Live'
                    WHEN ros2.team = 'Team - FIN' AND ros2.title_rw = 'Business Development Specialist - FIN' THEN  'CLO'
                ELSE 'N/A'
                END) AS mtd_attribution
     FROM grp_gdoop_sup_analytics_db.jc_merchant_deal_order ma
     LEFT JOIN
     (
      SELECT
            DISTINCT
            account_id,
            account_owner_id,
            TO_DATE(valid_from) AS valid_from,
            TO_DATE(valid_to) AS valid_to
         FROM grp_gdoop_sup_analytics_db.global_account_owner_hist
      WHERE account_id IS NOT NULL
        AND account_owner_id IS NOT NULL
     ) own
     ON own.account_id = ma.account_id
     LEFT JOIN (SELECT DISTINCT opportunity_id, person_key FROM prod_groupondw.dim_opportunity WHERE LENGTH(opportunity_id) = 15 AND opportunity_id IS NOT NULL) op ON op.opportunity_id = ma.opportunity_id
     LEFT JOIN (SELECT DISTINCT ultipro_id, person_id FROM prod_groupondw.dim_sf_person WHERE person_id IS NOT NULL) sfp ON sfp.person_id = own.account_owner_id
     LEFT JOIN (SELECT DISTINCT ultipro_id, person_key FROM prod_groupondw.dim_sf_person WHERE person_key IS NOT NULL) sfp_opp ON sfp_opp.person_key = op.person_key
     LEFT JOIN (SELECT DISTINCT emplid, roster_date, team, title_rw FROM grp_gdoop_sup_analytics_db.ops_roster_all WHERE emplid IS NOT NULL AND roster_date IS NOT NULL) ros1 ON sfp.ultipro_id = ros1.emplid AND ros1.roster_date = ma.closedate
     LEFT JOIN (SELECT DISTINCT emplid, roster_date, team, title_rw FROM grp_gdoop_sup_analytics_db.ops_roster_all WHERE emplid IS NOT NULL AND roster_date IS NOT NULL) ros2 ON sfp_opp.ultipro_id = ros2.emplid AND ros2.roster_date = ma.closedate
     LEFT JOIN (SELECT DISTINCT ownerid, account_id_18, billingcity FROM dwh_base_sec_view.sf_account WHERE account_id_18 IS NOT NULL) sf2 ON sf2.account_id_18 = ma.account_id
     WHERE DATE_SUB(ma.closedate, 1) BETWEEN TO_DATE(own.valid_from) AND TO_DATE(own.valid_to)
     GROUP BY 1
  ) day_prev_owner
  ON day_prev_owner.opportunity_id = ma.opportunity_id
  WHERE sfa.type__c <> 'Testing' or sfa.website <> 'www.groupon.com'

 
  
  
  
  --------------------------------------------------------------------------------------------------------------------------OLD CODE



DROP TABLE grp_gdoop_sup_analytics_db.jc_merchant_mtd_attrib PURGE;
CREATE TABLE grp_gdoop_sup_analytics_db.jc_merchant_mtd_attrib
  (
    accountid STRING,
    feature_country STRING,
    deal_uuid STRING,
    opportunity_id STRING,
    close_date STRING,
    opportunity_name STRING,
    launch_date STRING,
    division STRING,
    metal_at_close STRING,
    billingcity STRING,
    grt_l1_cat_name STRING,
    pds STRING,
    vertical STRING,
    close_order SMALLINT,
    close_recency SMALLINT,
    next_close_on STRING,
    launch_order SMALLINT,
    deal_paused_date STRING,
    stagename STRING,
    go_live_date STRING,
    straight_to_private_sale STRING,
    dmapi_flag TINYINT,
    title_rw STRING,
    team STRING,
    mtd_attribution STRING,
    mtd_attribution_intl STRING,
    ownerid STRING,
    por_relaunch TINYINT,
    metro_submit_time STRING,
    metro_bld_events_flag TINYINT
  )
  CLUSTERED BY (opportunity_id) SORTED BY (opportunity_id) INTO 15 BUCKETS
  STORED AS ORC
  TBLPROPERTIES ('orc.compress'='SNAPPY');
  
 INSERT OVERWRITE TABLE grp_gdoop_sup_analytics_db.jc_merchant_mtd_attrib
  SELECT
         ma.account_id AS accountid,
         ma.country_code,
         ma.deal_uuid,
         ma.opportunity_id,
         ma.closedate AS close_date,
         ma.opportunity_name,
         ma.launch_date,
         COALESCE(ma.division, dag.opp_division) AS division,
         COALESCE(ma.metal_at_close, 'Nickel') AS metal_at_close,
         COALESCE(day_prev_owner.billingcity, close_owner.billingcity) AS billingcity,
         ma.grt_l1_cat_name,
         ma.pds,
         ma.vertical,
         ma.close_order,
         ma.close_recency,
         CASE WHEN n_c.account_id IS NOT NULL THEN n_c.closedate ELSE NULL END AS next_close_on,
         nl.launch_order,
         CASE WHEN datediff(ma.deal_paused_date, CURRENT_DATE) < 2 THEN NULL ELSE ma.deal_paused_date END deal_paused_date,
         ma.stagename,
         ma.go_live_date,
         ma.Straight_to_Private_Sale,
         ma.dmapi_flag,
         COALESCE(day_prev_owner.title_rw, close_owner.title_rw) AS title_rw,
         COALESCE(day_prev_owner.team, close_owner.team) AS team,
         CASE
             WHEN (dl.convertedaccountid IS NOT NULL OR (ma.dmapi_flag = 1 AND ma.close_order = 1)) AND LOWER(ma.opportunity_name) NOT LIKE '%dmapi%' THEN 'Metro Lead - Sales Team Close'
             WHEN (dl.convertedaccountid IS NOT NULL OR (ma.dmapi_flag = 1 AND ma.close_order = 1)) AND ma.close_order > 1 THEN 'Existing Metro'
             WHEN (dl.convertedaccountid IS NOT NULL OR (ma.dmapi_flag = 1 AND ma.close_order = 1)) AND ma.close_order = 1 THEN 'New Metro'
             WHEN day_prev_owner.mtd_attribution IS NOT NULL THEN day_prev_owner.mtd_attribution
             ELSE close_owner.mtd_attribution
         END mtd_attribution,
         CASE
             WHEN (dl.convertedaccountid IS NOT NULL OR (ma.dmapi_flag = 1 AND ma.close_order = 1)) AND LOWER(ma.opportunity_name) NOT LIKE '%dmapi%' THEN 'Metro Lead - Sales Team Close'
             WHEN (dl.convertedaccountid IS NOT NULL OR (ma.dmapi_flag = 1 AND ma.close_order = 1)) AND ma.close_order > 1 THEN 'Existing Metro'
             WHEN (dl.convertedaccountid IS NOT NULL OR (ma.dmapi_flag = 1 AND ma.close_order = 1)) AND ma.close_order = 1 THEN 'New Metro'
             WHEN mdi.employee_sf_id IS NOT NULL THEN 'BD/MD'
             ELSE 'Existing MS'
         END mtd_attribution_intl,
         ma.ownerid,
         CAST(CASE
                WHEN (ma.opportunity_name LIKE '%*POR RL W1*%' OR ma.opportunity_name LIKE '%POR_%') THEN 1
                WHEN ma.opportunity_name LIKE ('%*POR Wave 2a RL*%') THEN 1
                WHEN ma.opportunity_name LIKE ('%*POR Wave 2b RL*%') THEN 1
                WHEN ma.opportunity_name LIKE ('%*POR ULA RL*%') THEN 1
                WHEN ma.opportunity_name LIKE ('%*POR Wave 3a RL*%') THEN 1
                WHEN ma.opportunity_name LIKE ('%*POR Wave 3b RL*%') THEN 1
                WHEN ma.opportunity_name LIKE ('%*POR Wave 3c RL*%') THEN 1
                WHEN ma.opportunity_name LIKE ('%*POR Wave 3d RL*%') THEN 1
                WHEN ma.opportunity_name LIKE ('%*POR Wave 3e RL*%') THEN 1
                WHEN ma.opportunity_name LIKE ('%*POR Wave 4 RL*%') THEN 1
                WHEN ma.opportunity_name LIKE ('%POR WAVE%') THEN 1
                ELSE 0
         END AS TINYINT) AS por_relaunch,
         ma.metro_submit_time,
         ma.metro_bld_events_flag
  FROM grp_gdoop_sup_analytics_db.jc_merchant_deal_order ma
  LEFT JOIN dwh_base_sec_view.sf_account sfa ON sfa.account_id_18 = ma.account_id
  LEFT JOIN dwh_load_sf_view.sf_user_v2 sfu ON sfa.ownerid = sfu.id
  LEFT JOIN grp_gdoop_sup_analytics_db.jc_intl_md_opps_v mdi ON mdi.employee_sf_id = sfu.id
  LEFT JOIN 
     (SELECT DISTINCT 
             convertedaccountid, leadsource 
             FROM user_groupondw.sf_lead 
             WHERE convertedaccountid IS NOT NULL AND LOWER(leadsource) = 'metro - self service'
             ) dl ON dl.convertedaccountid = ma.account_id
  LEFT JOIN
   (
    SELECT
          account_id,
          launch_date,
          opportunity_id,
          ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY launch_date ASC) launch_order
    FROM grp_gdoop_sup_analytics_db.jc_merchant_deal_order
    WHERE launch_date IS NOT NULL
   ) nl
  ON nl.opportunity_id = ma.opportunity_id
  LEFT JOIN
  (
   SELECT DISTINCT
          c.country_code,
          dag.deal_id,
          dag.opp_division
   FROM grp_gdoop_revenue_management_db.rev_mgmt_gbl_deal_attributes dag
   INNER JOIN
   (
    SELECT DISTINCT
           country_id,
           CASE WHEN COALESCE(country_code, 'US') = 'GB' THEN 'UK'
                WHEN COALESCE(country_code, 'US') IN ('VI', 'FM', 'PR', 'MH', 'US') THEN 'US'
                ELSE COALESCE(country_code, 'US')
           END  AS country_code
    FROM prod_groupondw.dim_country
    WHERE country_id IS NOT NULL
      AND country_code IN ('AE', 'AU', 'BE', 'CA', 'DE', 'ES', 'FR', 'GB', 'IE', 'IT', 'NL', 'PL', 'QC', 'UK', 'US', 'VI', 'FM', 'PR', 'MH')
   ) c
   ON dag.country_id = c.country_id
  )  dag
  ON dag.deal_id = ma.deal_uuid
  AND CASE WHEN dag.country_code IN ('CA', 'QC') THEN 'US' ELSE dag.country_code END = CASE WHEN ma.country_code IN ('CA', 'QC') THEN 'US' ELSE ma.country_code END
  LEFT JOIN
   (
    SELECT
          account_id,
          closedate,
          close_order
    FROM grp_gdoop_sup_analytics_db.jc_merchant_deal_order
   ) n_c
  ON n_c.account_id = ma.account_id
  AND ma.close_order + 1 = n_c.close_order
  LEFT JOIN (SELECT DISTINCT deal_id FROM groupon_production.clo_offers) clo ON clo.deal_id = ma.deal_uuid AND ma.country_code IN ('US', 'CA', 'QC')
  LEFT JOIN
   (
    SELECT
          employee_sf_id,
          CASE
              WHEN specialization IN ('Hunter','Inbound') THEN 'BD'
              WHEN specialization = 'Farmer' THEN 'MD'
              WHEN specialization = 'Hybrid' AND country_code = 'AE' THEN 'MD'
              ELSE specialization
          END AS rep_type,
          TO_DATE(snap_date) AS snap_date
    FROM grp_gdoop_sup_analytics_db.intl_active_sales
   ) is_curr
  ON is_curr.employee_sf_id = ma.ownerid
  AND ma.closedate = is_curr.snap_date
  LEFT JOIN
   (
    SELECT
          employee_sf_id,
          CASE
              WHEN specialization IN ('Hunter','Inbound') THEN 'BD'
              WHEN specialization = 'Farmer' THEN 'MD'
              WHEN specialization = 'Hybrid' AND country_code = 'AE' THEN 'MD'
              ELSE specialization
          END AS rep_type,
          TO_DATE(snap_date) AS snap_date
    FROM grp_gdoop_sup_analytics_db.intl_active_sales
   ) is_prev
  ON is_prev.employee_sf_id = ma.ownerid
  AND DATE_SUB(ma.closedate, 1) = is_prev.snap_date
  LEFT JOIN
   (
     SELECT
           ma.opportunity_id,
           MIN(sf2.billingcity) billingcity,
           MIN(COALESCE(ros1.title_rw, ros2.title_rw)) title_rw,
           MIN(COALESCE(ros1.team, ros2.team)) team,
           MIN(
                CASE
                    WHEN ros1.team = 'Getaway' THEN 'Getaway'
                    WHEN ros1.title_rw IN ('Business Development Director - Outside', 'Business Development Manager - Outside',  'Business Development Manager' , 'Business Development Director', 'Business Development Director - Outside','Business Development Representative - Outside', 'Business Development Representative')THEN 'BD'
                    WHEN ros1.title_rw IN ( 'Merchant Development Director','Merchant Development Manager', 'Merchant Development Representative' , 'Strategic Merchant Development Director') THEN 'MD'
                    WHEN sf2.ownerid IN ('005C000000455ZvIAI', '00580000001YaJIAA0') THEN 'Existing MS'
                    WHEN ros1.title_rw IN( 'Merchant Support Place Holder', 'MS Account Manager' ) THEN  'Existing MS'
                    WHEN ros1.team = 'Team - Inbound' AND ros1.title_rw LIKE '%Inbound%' THEN  'Inbound'
                    WHEN ros1.title_rw IN ( 'Live Account Manager', 'Live Sales Representative') THEN  'Live'
                    WHEN ros1.team = 'Team - FIN' AND ros1.title_rw = 'Business Development Specialist - FIN' THEN  'CLO'
                    WHEN ros2.team = 'Getaway' THEN 'Getaway'
                    WHEN ros2.title_rw IN ('Business Development Director - Outside', 'Business Development Manager - Outside',  'Business Development Manager' , 'Business Development Director', 'Business Development Director - Outside','Business Development Representative - Outside', 'Business Development Representative')THEN 'BD'
                    WHEN ros2.title_rw IN ( 'Merchant Development Director','Merchant Development Manager', 'Merchant Development Representative' , 'Strategic Merchant Development Director') THEN 'MD'
                    WHEN ros2.title_rw IN( 'Merchant Support Place Holder', 'MS Account Manager' ) THEN  'Existing MS'
                    WHEN ros2.team = 'Team - Inbound' AND ros2.title_rw LIKE '%Inbound%' THEN  'Inbound'
                    WHEN ros2.title_rw IN ( 'Live Account Manager', 'Live Sales Representative') THEN  'Live'
                    WHEN ros2.team = 'Team - FIN' AND ros2.title_rw = 'Business Development Specialist - FIN' THEN  'CLO'
                    ELSE 'N/A'
           END) AS mtd_attribution
     FROM grp_gdoop_sup_analytics_db.jc_merchant_deal_order ma
     LEFT JOIN
     (
      SELECT
            DISTINCT
            account_id,
            account_owner_id,
            TO_DATE(valid_from) AS valid_from,
            TO_DATE(valid_to) AS valid_to
         FROM grp_gdoop_sup_analytics_db.global_account_owner_hist
      WHERE account_id IS NOT NULL
        AND account_owner_id IS NOT NULL
     ) own
     ON own.account_id = ma.account_id
     LEFT JOIN (SELECT DISTINCT opportunity_id, person_key FROM prod_groupondw.dim_opportunity WHERE LENGTH(opportunity_id) = 15 AND opportunity_id IS NOT NULL) op ON op.opportunity_id = ma.opportunity_id
     LEFT JOIN (SELECT DISTINCT ultipro_id, person_id FROM prod_groupondw.dim_sf_person WHERE person_id IS NOT NULL) sfp ON sfp.person_id = own.account_owner_id
     LEFT JOIN (SELECT DISTINCT ultipro_id, person_key FROM prod_groupondw.dim_sf_person WHERE person_key IS NOT NULL) sfp_opp ON sfp_opp.person_key = op.person_key
     LEFT JOIN (SELECT DISTINCT emplid, roster_date, team, title_rw FROM grp_gdoop_sup_analytics_db.ops_roster_all WHERE emplid IS NOT NULL AND roster_date IS NOT NULL) ros1 ON sfp.ultipro_id = ros1.emplid AND ros1.roster_date = ma.closedate
     LEFT JOIN (SELECT DISTINCT emplid, roster_date, team, title_rw FROM grp_gdoop_sup_analytics_db.ops_roster_all WHERE emplid IS NOT NULL AND roster_date IS NOT NULL) ros2 ON sfp_opp.ultipro_id = ros2.emplid AND ros2.roster_date = ma.closedate
     LEFT JOIN (SELECT DISTINCT ownerid, account_id_18, billingcity FROM dwh_base_sec_view.sf_account WHERE account_id_18 IS NOT NULL) sf2 ON sf2.account_id_18 = ma.account_id
     WHERE ma.closedate BETWEEN own.valid_from AND own.valid_to
     GROUP BY 1
  ) close_owner
  ON close_owner.opportunity_id = ma.opportunity_id
  LEFT JOIN
   (
     SELECT
           ma.opportunity_id,
           MIN(sf2.billingcity) billingcity,
           MIN(COALESCE(ros1.title_rw, ros2.title_rw)) title_rw,
           MIN(COALESCE(ros1.team, ros2.team)) team,
           MIN(
                CASE
                    WHEN ros1.team = 'Getaway' THEN 'Getaway'
                    WHEN ros1.title_rw IN ('Business Development Director - Outside', 'Business Development Manager - Outside',  'Business Development Manager' , 'Business Development Director', 'Business Development Director - Outside','Business Development Representative - Outside', 'Business Development Representative')THEN 'BD'
                    WHEN ros1.title_rw IN ( 'Merchant Development Director','Merchant Development Manager', 'Merchant Development Representative' , 'Strategic Merchant Development Director') THEN 'MD'
                    WHEN sf2.ownerid IN ('005C000000455ZvIAI', '00580000001YaJIAA0') THEN 'Existing MS'
                    WHEN ros1.title_rw IN( 'Merchant Support Place Holder', 'MS Account Manager' ) THEN  'Existing MS'
                    WHEN ros1.team = 'Team - Inbound' AND ros1.title_rw LIKE '%Inbound%' THEN  'Inbound'
                    WHEN ros1.title_rw IN ( 'Live Account Manager', 'Live Sales Representative') THEN  'Live'
                    WHEN ros1.team = 'Team - FIN' AND ros1.title_rw = 'Business Development Specialist - FIN' THEN  'CLO'
                    WHEN ros2.team = 'Getaway' THEN 'Getaway'
                    WHEN ros2.title_rw IN ('Business Development Director - Outside', 'Business Development Manager - Outside',  'Business Development Manager' , 'Business Development Director', 'Business Development Director - Outside','Business Development Representative - Outside', 'Business Development Representative')THEN 'BD'
                    WHEN ros2.title_rw IN ( 'Merchant Development Director','Merchant Development Manager', 'Merchant Development Representative' , 'Strategic Merchant Development Director') THEN 'MD'
                    WHEN ros2.title_rw IN( 'Merchant Support Place Holder', 'MS Account Manager' ) THEN  'Existing MS'
                    WHEN ros2.team = 'Team - Inbound' AND ros2.title_rw LIKE '%Inbound%' THEN  'Inbound'
                    WHEN ros2.title_rw IN ( 'Live Account Manager', 'Live Sales Representative') THEN  'Live'
                    WHEN ros2.team = 'Team - FIN' AND ros2.title_rw = 'Business Development Specialist - FIN' THEN  'CLO'
                ELSE 'N/A'
                END) AS mtd_attribution
     FROM grp_gdoop_sup_analytics_db.jc_merchant_deal_order ma
     LEFT JOIN
     (
      SELECT
            DISTINCT
            account_id,
            account_owner_id,
            TO_DATE(valid_from) AS valid_from,
            TO_DATE(valid_to) AS valid_to
         FROM grp_gdoop_sup_analytics_db.global_account_owner_hist
      WHERE account_id IS NOT NULL
        AND account_owner_id IS NOT NULL
     ) own
     ON own.account_id = ma.account_id
     LEFT JOIN (SELECT DISTINCT opportunity_id, person_key FROM prod_groupondw.dim_opportunity WHERE LENGTH(opportunity_id) = 15 AND opportunity_id IS NOT NULL) op ON op.opportunity_id = ma.opportunity_id
     LEFT JOIN (SELECT DISTINCT ultipro_id, person_id FROM prod_groupondw.dim_sf_person WHERE person_id IS NOT NULL) sfp ON sfp.person_id = own.account_owner_id
     LEFT JOIN (SELECT DISTINCT ultipro_id, person_key FROM prod_groupondw.dim_sf_person WHERE person_key IS NOT NULL) sfp_opp ON sfp_opp.person_key = op.person_key
     LEFT JOIN (SELECT DISTINCT emplid, roster_date, team, title_rw FROM grp_gdoop_sup_analytics_db.ops_roster_all WHERE emplid IS NOT NULL AND roster_date IS NOT NULL) ros1 ON sfp.ultipro_id = ros1.emplid AND ros1.roster_date = ma.closedate
     LEFT JOIN (SELECT DISTINCT emplid, roster_date, team, title_rw FROM grp_gdoop_sup_analytics_db.ops_roster_all WHERE emplid IS NOT NULL AND roster_date IS NOT NULL) ros2 ON sfp_opp.ultipro_id = ros2.emplid AND ros2.roster_date = ma.closedate
     LEFT JOIN (SELECT DISTINCT ownerid, account_id_18, billingcity FROM dwh_base_sec_view.sf_account WHERE account_id_18 IS NOT NULL) sf2 ON sf2.account_id_18 = ma.account_id
     WHERE DATE_SUB(ma.closedate, 1) BETWEEN TO_DATE(own.valid_from) AND TO_DATE(own.valid_to)
     GROUP BY 1
  ) day_prev_owner
  ON day_prev_owner.opportunity_id = ma.opportunity_id