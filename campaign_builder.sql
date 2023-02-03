
delete from sandbox.pai_leads;
insert into sandbox.pai_leads
select 
    convertedaccountid,
    case when country = 'GB' then 'UK' else country end as country_code, 
    createddate lead_create_date,
    campaign_name__c campaign_name,
    leadsource,
    campaign_new_format,
    sem_partner,
    sem_brand,
    STRTOK(REGEXP_REPLACE(LOWER(TRIM(campaign_name)),'__', '_x_'), '_', 1) as mktg_txny_version,
    STRTOK(REGEXP_REPLACE(LOWER(TRIM(campaign_name)),'__', '_x_'), '_', 2) as mktg_country,
    STRTOK(REGEXP_REPLACE(LOWER(TRIM(campaign_name)),'__', '_x_'), '_', 3) as mktg_test_division,
    STRTOK(REGEXP_REPLACE(LOWER(TRIM(campaign_name)),'__', '_x_'), '_', 4) as mktg_traffic_source,
    STRTOK(REGEXP_REPLACE(LOWER(TRIM(campaign_name)),'__', '_x_'), '_', 5) as mktg_audience,
    STRTOK(REGEXP_REPLACE(LOWER(TRIM(campaign_name)),'__', '_x_'), '_', 6) as mktg_sem_type,
    STRTOK(REGEXP_REPLACE(LOWER(TRIM(campaign_name)),'__', '_x_'), '_', 7) as mktg_platform,
    STRTOK(REGEXP_REPLACE(LOWER(TRIM(campaign_name)),'__', '_x_'), '_', 8) as mktg_creative,
    campaign_paid_category,
    campaign_group_lvl1_2,
    campaign_group_lvl1_1,
    campaign_group_lvl2,
    last_touch,
    --max_value,
    --paid_direct_first,
    case when max_value = 1 and campaign_paid_category = 1 then paid_direct_first
         when max_value = 0 and campaign_paid_category = 0 then paid_direct_first
         else 0 end paid_first_last_touch
from
(select 
   c.*,
   case when campaign_name__c like 'TXN1%' then 1 else 0 end as campaign_paid_category,
   CASE 
   when lower(campaign_name__c) in  ('groupon-header-app1', 'groupon-header1', 'groupon-header-mweb1', 'groupon-footer') then 'Groupon Header & Footer'
   when (lower(campaign_name__c) like all ('%beauty%', '%wellness%')) then 'Direct / Referral / Other'---added additional 
   when lower(campaign_name__c) like '%groupon%' then 'Direct / Referral / Other' ---added additional 
   when lower(campaign_name__c) like '%blog%' 
		or lower(campaign_name__c) like '%merchant_blog%' 
		or lower(campaign_name__c)  like '%merchant_article%' 
		or lower(campaign_name__c) like '%merchant-blog-how-to-sell-post%' 
		or lower(campaign_name__c) like '%merchant-blog-sidebar%' then 'SEO' ----removed from SEO or case statement and brought to the top
   when campaign_name__c like all ('TXN1%','%SEMB%')  then 'Google-SEM'
   when campaign_name__c like all ('TXN1%','%SEMN%') and  campaign_name__c not like '%SEMNB%' then  'Google-SEM-NB'
   when campaign_name__c like all ('TXN1%','%DIS_GEN__FB%') then 'FB-Display'
   when campaign_name__c like all ('TXN1%','%DIS_GEN_AMZN%') then 'AMZON-Display'
   when campaign_name__c like 'TXN1%' and STRTOK(REGEXP_REPLACE(TRIM(campaign_name__c),'__', '_x_'), '_', 4) = 'VID' then 'MNTN'
   when campaign_name__c like all ('TXN1%','%GPMC%') THEN 'GPMC'
   when campaign_name__c like all ('TXN1%','%SEMNB%') then 'SEM-NB'
   when campaign_name__c like '%always_on%' then 'SEM-Brand'
   when campaign_name__c like all ('TXN1%','%_candace%', '%FB%') then 'Influencer_fb'
   when campaign_name__c like all ('TXN1%','%_candace%', '%INS%') then 'Influencer_insta'
   when (lower(campaign_name__c) like '%free_advertising%') then 'SEM-NB'---removed from the or group
   when (lower(campaign_name__c) like '%ggl%' and lower(campaign_name__c) like '%ybr%')
        or (lower(campaign_name__c) like '%ggl%' and lower(campaign_name__c) like '%rmk%')
        or (lower(campaign_name__c) like '%advertising%') 
        or (lower(campaign_name__c) like '%business%')
        or (lower(campaign_name__c) like '%contact_merchant%')
        or (lower(campaign_name__c) like '%how_to_business%')
        or (lower(campaign_name__c) like '%join%')
        or (lower(campaign_name__c) like '%merchant_misc%')
        or (lower(campaign_name__c) like '%number%') then 'SEM-Brand'
   when (sem_partner like '%bng%' and sem_brand like '%cbr%')
        or (sem_partner like '%ggl%' and sem_brand like '%merchant-competitors%') 
        or (lower(campaign_name__c) like '%ggl%' and lower(campaign_name__c) like '%nbr%')
        or (lower(campaign_name__c) like '%advertise%')
        or (lower(campaign_name__c) like '%promote%') 
        or (lower(campaign_name__c) like '%sb_adv%') then 'SEM-NB'
   when lower(campaign_name__c) like '%livingsocial_ib%'
        or lower(campaign_name__c) like '%merchantnl%'
        or lower(campaign_name__c) like '%june2014%' 
        or lower(campaign_name__c) like '%july2014%' 
        or lower(campaign_name__c) like '%august2014%' 
        or lower(campaign_name__c) like '%september2014%' 
        or lower(campaign_name__c) like '%october2014%' 
        or lower(campaign_name__c) like '%november2014%' 
        or lower(campaign_name__c) like '%december2014%' 
        or lower(campaign_name__c) like '%january2015%' 
        or lower(campaign_name__c) like '%feb2015%' 
        or lower(campaign_name__c) like '%mar2015%' 
        or lower(campaign_name__c) like '%apr2015%' 
        or lower(campaign_name__c) like '%may2015%' 
        or lower(campaign_name__c) like '%june2015%' 
        or lower(campaign_name__c) like '%july2015%' 
        or lower(campaign_name__c) like '%august2015%' 
        or lower(campaign_name__c) like '%sept2015%' 
        or lower(campaign_name__c) like '%oct2015%' 
        or lower(campaign_name__c) like '%nov2015%' 
        or lower(campaign_name__c) like '%dec2015%' 
        or lower(campaign_name__c) like '%jan2016%'
        or lower(campaign_name__c) like '%print%' 
        or lower(campaign_name__c) like '%nra2016%' 
        or lower(campaign_name__c) like '%osr-cards%' 
        or lower(campaign_name__c) like '%hbw-2016%' 
        or lower(campaign_name__c) like '%austin-promo-16%' 
        or lower(campaign_name__c) like '%cultural-institutions-2015%' 
        or lower(campaign_name__c) like '%ttd-cultural-institutions%' 
        or lower(campaign_name__c) like '%ttd-culture-2016%' 
        or lower(campaign_name__c) like '%ttd-culture-2016%' 
        or lower(campaign_name__c) like '%ttd-activities-2016%' 
        or lower(campaign_name__c) like '%activities-2015%' 
        or lower(campaign_name__c) like '%events-2015%'
        or lower(campaign_name__c) like '%astc-promo-16%' 
        or lower(campaign_name__c) like '%organic%' then 'SEO'
   else 'Direct / Referral / Other' end campaign_group_lvl1_2 
   ,CASE 
	    when lower(campaign_name__c) in  ('groupon-header-app1', 'groupon-header1', 'groupon-header-mweb1', 'groupon-footer') then 'Groupon Header & Footer'
	    when (lower(campaign_name__c) like all ('%beauty%', '%wellness%')) then 'Direct / Referral / Other' ---added additional 
        when lower(campaign_name__c) like '%groupon%' then 'Direct / Referral / Other' ---added additional 
        when lower(campaign_name__c) like '%blog%' 
		or lower(campaign_name__c) like '%merchant_blog%' 
		or lower(campaign_name__c)  like '%merchant_article%' 
		or lower(campaign_name__c) like '%merchant-blog-how-to-sell-post%' 
		or lower(campaign_name__c) like '%merchant-blog-sidebar%' then 'SEO' ----removed from SEO or case statement and brought to the top
	    when campaign_name__c like all ('TXN1%','%SEMB%')  then 'SEM-Brand'
        when campaign_name__c like all ('TXN1%','%SEMN%') and  campaign_name__c not like '%SEMNB%' then  'SEM-NB'
        when campaign_name__c like 'TXN1%' and STRTOK(REGEXP_REPLACE(TRIM(campaign_name__c),'__', '_x_'), '_', 4) = 'DIS' then'Display'
        when campaign_name__c like 'TXN1%' and STRTOK(REGEXP_REPLACE(TRIM(campaign_name__c),'__', '_x_'), '_', 4) = 'VID' then 'SEM-Brand'
        when campaign_name__c like all ('TXN1%','%GPMC%') THEN 'SEM-Brand'
        when campaign_name__c like all ('TXN1%','%SEMNB%') then 'SEM-NB'
        when campaign_name__c like '%always_on%' then 'SEM-Brand'
        when campaign_name__c like all ('TXN1%','%_candace%', '%FB%') then 'Influencer_fb'
        when campaign_name__c like all ('TXN1%','%_candace%', '%INS%') then 'Influencer_insta'
        when (lower(campaign_name__c) like '%free_advertising%')  then 'SEM-NB' ---removed from the or group
        when (lower(campaign_name__c) like '%ggl%' and lower(campaign_name__c) like '%ybr%')
	        or (lower(campaign_name__c) like '%ggl%' and lower(campaign_name__c) like '%rmk%')
	        or (lower(campaign_name__c) like '%advertising%')
	        or (lower(campaign_name__c) like '%business%')
	        or (lower(campaign_name__c) like '%contact_merchant%')
	        or (lower(campaign_name__c) like '%how_to_business%')
	        or (lower(campaign_name__c) like '%join%')
	        or (lower(campaign_name__c) like '%merchant_misc%')
	        or (lower(campaign_name__c) like '%number%') then 'SEM-Brand'
       when (sem_partner like '%bng%' and sem_brand like '%cbr%')
	        or (sem_partner like '%ggl%' and sem_brand like '%merchant-competitors%') 
	        or (lower(campaign_name__c) like '%ggl%' and lower(campaign_name__c) like '%nbr%')
	        or (lower(campaign_name__c) like '%advertise%')
	        or (lower(campaign_name__c) like '%promote%') 
	        or (lower(campaign_name__c) like '%sb_adv%') then 'SEM-NB'
       when lower(campaign_name__c) like '%livingsocial_ib%'
	        or lower(campaign_name__c) like '%merchantnl%'
	        or lower(campaign_name__c) like '%june2014%' 
	        or lower(campaign_name__c) like '%july2014%' 
	        or lower(campaign_name__c) like '%august2014%' 
	        or lower(campaign_name__c) like '%september2014%' 
	        or lower(campaign_name__c) like '%october2014%' 
	        or lower(campaign_name__c) like '%november2014%' 
	        or lower(campaign_name__c) like '%december2014%' 
	        or lower(campaign_name__c) like '%january2015%' 
	        or lower(campaign_name__c) like '%feb2015%' 
	        or lower(campaign_name__c) like '%mar2015%' 
	        or lower(campaign_name__c) like '%apr2015%' 
	        or lower(campaign_name__c) like '%may2015%' 
	        or lower(campaign_name__c) like '%june2015%' 
	        or lower(campaign_name__c) like '%july2015%' 
	        or lower(campaign_name__c) like '%august2015%' 
	        or lower(campaign_name__c) like '%sept2015%' 
	        or lower(campaign_name__c) like '%oct2015%' 
	        or lower(campaign_name__c) like '%nov2015%' 
	        or lower(campaign_name__c) like '%dec2015%' 
	        or lower(campaign_name__c) like '%jan2016%'
	        or lower(campaign_name__c) like '%print%' 
	        or lower(campaign_name__c) like '%nra2016%' 
	        or lower(campaign_name__c) like '%osr-cards%' 
	        or lower(campaign_name__c) like '%hbw-2016%' 
	        or lower(campaign_name__c) like '%austin-promo-16%' 
	        or lower(campaign_name__c) like '%cultural-institutions-2015%' 
	        or lower(campaign_name__c) like '%ttd-cultural-institutions%' 
	        or lower(campaign_name__c) like '%ttd-culture-2016%' 
	        or lower(campaign_name__c) like '%ttd-culture-2016%' 
	        or lower(campaign_name__c) like '%ttd-activities-2016%' 
	        or lower(campaign_name__c) like '%activities-2015%' 
	        or lower(campaign_name__c) like '%events-2015%'
	        or lower(campaign_name__c) like '%astc-promo-16%' ----can remove everything with dates in here. as its old 
	        or lower(campaign_name__c) like '%organic%'then 'SEO'
       else 'Direct / Referral / Other' end campaign_group_lvl1_1
       ,case when leadsource like '%Phone Bank%' then 'Phone Bank- No Campaign'
          when lower(campaign_name__c) = 'other' then 'No Campaign - Referred' 
          when lower(campaign_name__c) in ('groupon-header-app1', 'groupon-header1', 'groupon-header-mweb1', 'groupon-footer') then lower(campaign_name__c)
          when (lower(campaign_name__c) like '%us_un_som_fac_tim_lb_rr_cbp%' and lower(campaign_name__c) like '%fdp%') then 'Facebook F&D'
          when (lower(campaign_name__c) like '%us_un_som_fac_tim_lb_rr_cbp%' and lower(campaign_name__c) like '%hbp%') then 'Facebook HBW'
          when lower(campaign_name__c) like '%us_un_som_fac_tim_lb_rr_cbp_chw_sy_v30%' then 'Social Retargeting'
          when lower(campaign_name__c) like '%d*merchant-fb%' then 'Facebook_old'
          when lower(campaign_name__c) like '%g*gmail-ads%' then  'GMail Ads'
          when lower(campaign_name__c) like '%d*gw-dx-dis%' then  'DataXu'
          when lower(campaign_name__c) like '%groupon%' then 'Referral'
          when lower(campaign_name__c) like '%delivery-takeout-lp%' then 'Delivery Takeout'
          when campaign_name__c = '50_DLS' then 'Referral'
          when campaign_name__c = '50' then 'Referral'
          when (lower(campaign_name__c) like '%grouponworks%' and lower(campaign_name__c) like '%social%') then 'Social'
          when lower(campaign_name__c) like '%merchant-retargeting%' then 'Merchant Retargeting'
          when lower(campaign_name__c) like '%merchant-stream%' then 'Yahoo Stream Ads'
          when lower(campaign_name__c) like '%d*merchant-gsp%' then 'Gmail Sponsored Promotions'
              --when lower(campaign_name__c) like '%biz_page%' then 'Biz Pages'
          when lower(campaign_name__c) like '%livingsocial_ib%' then 'Living Social'
          when lower(campaign_name__c) like '%blog%' 
               or lower(campaign_name__c) like '%merchant_blog%' 
               or lower(campaign_name__c) like '%merchant_article%' 
               or lower(campaign_name__c) like '%merchant-blog-how-to-sell-post%' 
               or lower(campaign_name__c) like '%merchant-blog-sidebar%' then 'Merchant Blog/SBRC'---
          when lower(campaign_name__c) like '%merchantnl%' 
               or lower(campaign_name__c) like '%june2014%' 
               or lower(campaign_name__c) like '%july2014%' 
               or lower(campaign_name__c) like '%august2014%' 
               or lower(campaign_name__c) like '%september2014%' 
               or lower(campaign_name__c) like '%october2014%' 
               or lower(campaign_name__c) like '%november2014%' 
               or lower(campaign_name__c) like '%december2014%' 
               or lower(campaign_name__c) like '%january2015%' 
               or lower(campaign_name__c) like '%feb2015%' 
               or lower(campaign_name__c) like '%mar2015%' 
               or lower(campaign_name__c) like '%apr2015%' 
               or lower(campaign_name__c) like '%may2015%' 
               or lower(campaign_name__c) like '%june2015%' 
               or lower(campaign_name__c) like '%july2015%' 
               or lower(campaign_name__c) like '%august2015%' 
               or lower(campaign_name__c) like '%sept2015%' 
               or lower(campaign_name__c) like '%oct2015%' 
               or lower(campaign_name__c) like '%nov2015%' 
               or lower(campaign_name__c) like '%dec2015%' 
               or lower(campaign_name__c) like '%jan2016%' then 'Merchant Newsletter'---
          when lower(campaign_name__c) like '%print%' 
               or lower(campaign_name__c) like '%nra2016%' 
               or lower(campaign_name__c) like '%osr-cards%' 
               or lower(campaign_name__c) like '%hbw-2016%' 
               or lower(campaign_name__c) like '%austin-promo-16%' 
               or lower(campaign_name__c) like '%cultural-institutions-2015%' 
               or lower(campaign_name__c) like '%ttd-cultural-institutions%' 
               or lower(campaign_name__c) like '%ttd-culture-2016%' 
               or lower(campaign_name__c) like '%ttd-culture-2016%' 
               or lower(campaign_name__c) like '%ttd-activities-2016%' 
               or lower(campaign_name__c) like '%activities-2015%' 
               or lower(campaign_name__c) like '%events-2015%' 
               or lower(campaign_name__c) like '%astc-promo-16%' then 'Print'---
          when lower(campaign_name__c) like '%grouponworks_social%' then 'Social'
          when lower(campaign_name__c) like '%occasions_sponsor%' then 'Occasions_sponsor'
          when lower(campaign_name__c) like '%occasions%' then 'Occasions'
          when lower(campaign_name__c) like '%reserve%' then 'Reserve'
          when lower(campaign_name__c) like '%getaways%' then 'Getaways'
          when lower(campaign_name__c) like '%occasions_sponsor%' then 'Sponsored Occasions'
          when lower(campaign_name__c) like '%st_text%' then 'GCN'
          when lower(campaign_name__c) like '%toolkit%' then 'Score'
          when lower(campaign_name__c) like '%mc_ppl%' then 'Merchant Circle'
          when lower(campaign_name__c) like '%NRA_%' then 'NRA'
          when lower(campaign_name__c) like '%linkedin_%' then 'LinkedIn'
          when lower(campaign_name__c) like '%payments%' then 'Payments'
          when lower(campaign_name__c) like '%goods%' then 'Goods'
              --when lower(campaign_name__c) like '%112%' then 'Goods'
              --when no_campaign_chars = 36 then 'G1'
          when (lower(campaign_name__c) like '%merchant%' and lower(campaign_name__c) like '%food%' and lower(campaign_name__c) like '%drink%' and lower(campaign_name__c) like '%srm%') then 'Merchant-Food-Drink-SRM'
          when (lower(campaign_name__c) like '%merchant%' and lower(campaign_name__c) like '%ybr%' and lower(campaign_name__c) like '%srm%') then 'Merchant-YBR-SRM'
          when (lower(campaign_name__c) like '%merchant%' and lower(campaign_name__c) like '%ybr%' and lower(campaign_name__c) like '%scm%') then 'Merchant-YBR-SCM'
          when (lower(campaign_name__c) like '%ppl%' and lower(campaign_name__c) like '%gen%' and lower(campaign_name__c) like '%sug2013%') then 'AdKnowledge_aug2013'
          when (lower(campaign_name__c) like '%ppl%' and lower(campaign_name__c) like '%info%') then 'InfoGroup'
          when (lower(campaign_name__c) like '%merchant%' and lower(campaign_name__c) like '%food%' and lower(campaign_name__c) like '%drink%' and lower(campaign_name__c) like '%scm%') then 'Merchant-Food-Drink-SCM'
          when (lower(campaign_name__c) like '%leisure%' and lower(campaign_name__c) like '%activities%') then 'Leisure-Activities'
          when (lower(campaign_name__c) like '%beauty%' and lower(campaign_name__c) like '%wellness%') then 'Beauty-Wellness'
          when (lower(campaign_name__c) like '%food%' and lower(campaign_name__c) like '%drink%') then 'Food & Drink'
          when lower(campaign_name__c) like '%direct%' then 'Direct'
          when lower(campaign_name__c) like '%organic%' then 'Organic'
          when lower(campaign_name__c) like '%referral%' then 'Referral'
          when (lower(campaign_name__c) like '%free_advertising%') then 'Google NB - Free Advertising'--
          when (lower(campaign_name__c) like '%sb_adv%') then 'Google NB - SB-Adv'--
          when (lower(campaign_name__c) like '%promote%') then 'Google NB - Promote'--
          when (lower(campaign_name__c) like '%advertise%') then 'Google NB - Advertise'--
          when (lower(campaign_name__c) like '%number%') then 'Google Brand - Number'--
          when (lower(campaign_name__c) like '%contact_merchant%') then 'Google Brand - Contact Merchant'--
          when (lower(campaign_name__c) like '%advertising%') then 'Google Brand - Advertising'--
          when (lower(campaign_name__c) like '%how_to_business%') then 'Google Brand - How To Business'--
          when (lower(campaign_name__c) like '%business%') then 'Google Brand - Business'--
          when (lower(campaign_name__c) like '%join%') then 'Google Brand - Join'--
          when (lower(campaign_name__c) like '%merchant_misc%') then 'Google Brand - Merchant Misc'--
          when (sem_partner like '%ggl%' and sem_brand like '%merchant-competitors%') then 'Google - Competitor'--
          when (sem_partner like '%bng%' and sem_brand like '%nbr%') then 'Bing - Non-Brand'
          when (sem_partner like '%bng%' and sem_brand like '%ybr%') then 'Bing - Brand'
          when (sem_partner like '%bng%' and sem_brand like '%cbr%') then 'Bing - Competitor'--
          when STRTOK(REGEXP_REPLACE(LOWER(TRIM(campaign_name__c)),'__', '_x_'), '_', 2) like 'g1%' then 'G1'
          when STRTOK(REGEXP_REPLACE(LOWER(TRIM(campaign_name__c)),'__', '_x_'), '_', 2) like 'goods%' then 'Goods'
          when STRTOK(REGEXP_REPLACE(LOWER(TRIM(campaign_name__c)),'__', '_x_'), '_', 2) like 'occasion%' then 'Occasions'
          when STRTOK(REGEXP_REPLACE(LOWER(TRIM(campaign_name__c)),'__', '_x_'), '_', 2) like 'getaways%' then 'Getaways'
          when STRTOK(REGEXP_REPLACE(LOWER(TRIM(campaign_name__c)),'__', '_x_'), '_', 2) like 'reserve%' then 'Reserve'
          when STRTOK(REGEXP_REPLACE(LOWER(TRIM(campaign_name__c)),'__', '_x_'), '_', 2) like 'collection%' then 'Collections'
          when STRTOK(REGEXP_REPLACE(LOWER(TRIM(campaign_name__c)),'__', '_x_'), '_', 2) like 'payments%' then 'Payments'
          when lower(campaign_name__c) like 'k*%' then 'Google - Non-Brand'
          when lower(campaign_name__c) like '%merchant_blog%' then 'Merchant Blog'
          when lower(campaign_name__c) like '%grouponworks_social%' then 'Social'
          when (lower(campaign_name__c) like '%ggl%' and lower(campaign_name__c) like '%nbr%') then 'Google - Non-Brand'--
          when (lower(campaign_name__c) like '%ggl%' and lower(campaign_name__c) like '%ybr%') then 'Google - Brand'--
          when (lower(campaign_name__c) like '%ggl%' and lower(campaign_name__c) like '%merchant-competitors%') then 'Google - Competitor'--
          when (lower(campaign_name__c) like '%ggl%' and lower(campaign_name__c) like '%rmk%') then 'Google - Remarketing'--
        else 'Other' end  campaign_group_lvl2, 
        case when dense_rank () over (partition by convertedaccountid order by createddate desc) = 1 then 1 else 0 end last_touch, 
        case when dense_rank () over (partition by convertedaccountid, campaign_paid_category order by createddate desc) = 1 then 1 else 0 end paid_direct_first, 
        max(case when campaign_name__c like 'TXN1%' then 1 else 0 end) over (partition by convertedaccountid) max_value
        ---case when dense_rank () over (partition by convertedaccountid, campaign_paid_category order by createddate asc) = 1 then 1 else 0 end paid_direct_first
from 
(SELECT 
       convertedaccountid,
       country, 
       createddate,
       campaign_name__c,
       leadsource,
       CASE WHEN LENGTH(campaign_name__c) - LENGTH(REGEXP_REPLACE(campaign_name__c, '_', '')) > 11 THEN 1 ELSE 0 END AS campaign_new_format,
       CASE WHEN LENGTH(campaign_name__c) - LENGTH(REGEXP_REPLACE(campaign_name__c, '_', '')) > 11 THEN STRTOK(REGEXP_REPLACE(LOWER(TRIM(campaign_name__c)),'__', '_x_'), '_', 5) ELSE NULL END AS sem_partner,
       CASE WHEN LENGTH(campaign_name__c) - LENGTH(REGEXP_REPLACE(campaign_name__c, '_', '')) > 11 THEN STRTOK(REGEXP_REPLACE(LOWER(TRIM(campaign_name__c)),'__', '_x_'), '_', 11) ELSE NULL END AS sem_brand
FROM user_groupondw.sf_lead
WHERE lower(leadsource) like any ('%mia%','%metro%')
      and CAST(createddate AS DATE) > '2019-01-01'
      AND convertedaccountid IS NOT NULL
) as c) as x


-------------------------------------opportunity_base_mtd

select * from user_edwprod.sf_opportunity_1 where Opportunity_ID = '0063c00001MzwtZ';
select * from sandbox.pai_opp_mtd_attrib where Opportunity_ID = '0063c00001MzwtZ';

 grant sel on sandbox.pai_opp_mtd_attrib  to public;
 grant all on sandbox.pai_opp_mtd_attrib to abautista with grant option

show table sandbox.pai_opp_mtd_attrib;
drop table sandbox.pai_opp_mtd_attrib;
CREATE MULTISET TABLE sandbox.pai_opp_mtd_attrib ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      country_code VARCHAR(4) CHARACTER SET LATIN CASESPECIFIC,
      account_id VARCHAR(32) CHARACTER SET LATIN CASESPECIFIC,
      merchant_uuid VARCHAR(256) CHARACTER SET LATIN CASESPECIFIC,
      merchant_name VARCHAR(512) CHARACTER SET UNICODE NOT CASESPECIFIC,
      ID VARCHAR(32) CHARACTER SET LATIN CASESPECIFIC,
      deal_uuid VARCHAR(64) CHARACTER SET LATIN CASESPECIFIC,
      Opportunity_ID VARCHAR(32) CHARACTER SET LATIN CASESPECIFIC,
      CloseDate DATE FORMAT 'yyyy-mm-dd',
      Division VARCHAR(128) CHARACTER SET UNICODE CASESPECIFIC,
      opportunity_name VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC,
      Go_Live_Date DATE FORMAT 'yyyy-mm-dd',
      Straight_to_Private_Sale VARCHAR(128) CHARACTER SET LATIN NOT CASESPECIFIC,
      OwnerId CHAR(18) CHARACTER SET LATIN CASESPECIFIC,
      Deal_Attribute CHAR(18) CHARACTER SET LATIN NOT CASESPECIFIC,
      stagename VARCHAR(40) CHARACTER SET LATIN CASESPECIFIC,
      dmapi_flag BYTEINT,
      por_relaunch BYTEINT,
      Cloned_From CHAR(18) CHARACTER SET LATIN CASESPECIFIC,
      launch_date DATE FORMAT 'yyyy-mm-dd',
      pds_cat_id VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
      primary_deal_services VARCHAR(255) CHARACTER SET UNICODE CASESPECIFIC,
      grt_l2_cat_name VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
      grt_l1_cat_name VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
      metal VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
      launch_order INTEGER,
      close_order INTEGER,
      close_recency INTEGER)
PRIMARY INDEX ( country_code ,account_id ,merchant_uuid ,deal_uuid ,
Opportunity_ID ,dmapi_flag ,launch_date );

delete from sandbox.pai_opp_mtd_attrib;
insert into sandbox.pai_opp_mtd_attrib
with o1 as (
	SELECT   
		CASE 
			WHEN COALESCE(feature_country, 'US') = 'GB' THEN 'UK'
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
        CASE WHEN LOWER(opportunity_name) LIKE '%dmapi%' or LOWER(opportunity_name) LIKE '%*G1M*%' THEN 1 ELSE 0 END AS dmapi_flag,
        CASE
            WHEN (opportunity_name LIKE '%*POR RL W1*%' OR 
			opportunity_name LIKE '%POR''\_%') THEN 1
            WHEN opportunity_name LIKE ('%*POR Wave 2a RL*%') THEN 1
            WHEN opportunity_name LIKE ('%*POR Wave 2b RL*%') THEN 1
            WHEN opportunity_name LIKE ('%*POR ULA RL*%') THEN 1
            WHEN opportunity_name LIKE ('%*POR Wave 3a RL*%') THEN 1
            WHEN opportunity_name LIKE ('%*POR Wave 3b RL*%') THEN 1
            WHEN opportunity_name LIKE ('%*POR Wave 3c RL*%') THEN 1
            WHEN opportunity_name LIKE ('%*POR Wave 3d RL*%') THEN 1
            WHEN opportunity_name LIKE ('%*POR Wave 3e RL*%') THEN 1
            WHEN opportunity_name LIKE ('%*POR Wave 4 RL*%') THEN 1
            WHEN opportunity_name LIKE ('%POR WAVE%') THEN 1
            ELSE 0 END AS por_relaunch,
        cloned_from
    FROM user_edwprod.sf_opportunity_1
    	WHERE 
			LENGTH(opportunity_id) = 15
      		AND opportunity_id IS NOT NULL
            AND LOWER(stagename) IN ('closed lost', 'closed won', 'merchant not interested')
            AND COALESCE(feature_country, 'US') IN ('AE', 'AU', 'BE', 'CA', 'DE', 'ES', 'FR', 'GB', 'IE', 'IT', 'NL', 'PL', 'QC', 'UK', 'US')
			/*and cast(dwh_created_at as date) >= '2022-11-01'*/),
o2 as (
	SELECT 
		DISTINCT deal_uuid, Id 
		FROM user_edwprod.sf_opportunity_2 
		WHERE LENGTH(opportunity_id) = 15 AND opportunity_id IS NOT NULL),
dmp as (
	sel deal_uuid, pds_cat_name, dmp.pds_cat_id,grt_l1_cat_name,grt_l2_cat_name from
	(select
		deal_uuid, max(primary_dealservice_cat_id) pds_cat_id 
	from user_edwprod.deal_merch_product
	where primary_dealservice_cat_id is not null
	group by 1) dmp
	LEFT JOIN user_dw.v_dim_pds_grt_map grt ON dmp.pds_cat_id = grt.pds_cat_id),
dm as (
	SELECT merchant_uuid,
	    SUBSTR(MAX(CONCAT(cast(updated_at as date), salesforce_account_id)), 11) AS account_id,
	    SUBSTR(MAX(CONCAT(cast(updated_at as date), name)), 11) AS merchant_name
	FROM user_edwprod.dim_merchants
	WHERE dwh_active = 1
	GROUP BY 1),
ad as (
	SELECT 
		CASE 
			WHEN COALESCE(country_code, 'US') = 'GB' THEN 'UK'
    		WHEN COALESCE(country_code, 'US') IN ('VI', 'FM', 'PR', 'MH') THEN 'US'
            ELSE COALESCE(country_code, 'US')
        END AS country_code,
        deal_uuid,
        MIN(load_date) AS launch_date,
        MAX(load_date) AS deal_paused_date
    FROM user_groupondw.active_deals
    WHERE country_code IN ('AE', 'AU', 'BE', 'CA', 'DE', 'ES', 'FR', 'GB', 'IE', 'IT', 'NL', 'PL', 'QC', 'UK', 'US')
    GROUP BY 1,2),
dm1 as (
	SELECT 
		salesforce_account_id AS account_id,
        SUBSTR(MAX(CONCAT(cast(updated_at as date), merchant_uuid)), 11) AS merchant_uuid,
        SUBSTR(MAX(CONCAT(cast(updated_at as date), name)), 11) AS merchant_name
    FROM user_edwprod.dim_merchants
    WHERE dwh_active = 1
    GROUP BY 1),
sfa as (select account_id_18 account_id , merchant_segmentation__c metal from dwh_base_sec_view.sf_account sfa),
pds as (sel pds_cat_name, max(pds_cat_id) as pds_cat_id, grt_l1_cat_name, grt_l2_cat_name
	from user_dw.v_dim_pds_grt_map	where  pds_cat_name <> 'Martial Arts / Karate / MMA' group by 1,3,4)
select 
    fin.*,
    case when launch_date is not null then DENSE_RANK () OVER (PARTITION BY account_id, case when launch_date is null then 0 else 1 end ORDER BY launch_date ASC) end launch_order,
    case when closedate is not null then DENSE_RANK() OVER (PARTITION BY country_code, account_id, case when closedate is null then 0 else 1 end ORDER BY closedate asc) end close_order,
    case when closedate is not null then DENSE_RANK() OVER (PARTITION BY country_code, account_id, case when closedate is null then 0 else 1 end ORDER BY closedate desc) end close_recency 
from (
   SELECT
         o1.country_code,
         COALESCE(o1.account_id, dm1.account_id, dm.account_id) AS account_id,
         COALESCE(dm1.merchant_uuid, dm.merchant_uuid) AS merchant_uuid,
         COALESCE(dm1.merchant_name, dm.merchant_name) AS merchant_name,
         o1.id,
         o2.deal_uuid,
         o1.opportunity_id,
         o1.closedate,
         o1.division,
         o1.opportunity_name,
         o1.go_live_date,
         o1.Straight_to_Private_Sale,
         o1.ownerid,
         o1.deal_attribute,
         o1.stagename,
         o1.dmapi_flag,
         o1.por_relaunch,
         o1.cloned_from,
         ad.launch_date,
         dmp.pds_cat_id AS pds_cat_id,---missing
         COALESCE(dmp.pds_cat_name, coalesce(o1.primary_deal_services,pds.pds_cat_name)) AS primary_deal_services,
         coalesce(dmp.grt_l2_cat_name,pds.grt_l2_cat_name) grt_l2_cat_name,
         coalesce(dmp.grt_l1_cat_name,pds.grt_l1_cat_name) grt_l1_cat_name,
         sfa.metal
  from o1
  left join o2 ON o1.ID = o2.ID
  left join dmp ON dmp.deal_uuid = o2.deal_uuid ----in mtd this is joined to active_deals
  left join user_edwprod.dim_offer_ext doe ON doe.product_uuid = o2.deal_uuid
  left join dm ON doe.merchant_uuid = dm.merchant_uuid
  left join ad ON o2.deal_uuid = ad.deal_uuid AND o1.country_code = ad.country_code
  left join dm1 ON dm1.account_id = COALESCE(o1.account_id, dm.account_id)
  left join sfa on sfa.account_id = COALESCE(o1.account_id, dm.account_id)
  left join pds on pds.pds_cat_name = o1.primary_deal_services
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24) as fin;
    
delete from sandbox.np_opportunity_details_2;
insert into sandbox.np_opportunity_details_2
select 
  distinct 
  o1.*, 
  o2.deal_uuid
from 
(SELECT   
			 CASE WHEN COALESCE(feature_country, 'US') = 'GB' THEN 'UK'
                  WHEN COALESCE(feature_country, 'US') IN ('VI', 'FM', 'PR', 'MH') THEN 'US'
                  ELSE COALESCE(feature_country, 'US')
             END  AS country_code,
             opportunity_id,
             accountid AS account_id,
             id,
             cloned_from
      FROM user_edwprod.sf_opportunity_1
      WHERE opportunity_id IS NOT NULL
) o1
    LEFT JOIN (SELECT DISTINCT deal_uuid, Id FROM user_edwprod.sf_opportunity_2 WHERE LENGTH(opportunity_id) = 15 AND opportunity_id IS NOT NULL) o2 ON o1.ID = o2.ID

   select * from sandbox.pai_opp_mtd_attrib where deal_uuid = '2bc35647-aa04-43df-a443-b8f0c0f2c247';


delete from sandbox.np_opportunity_details_2;
insert into sandbox.np_opportunity_details_2
select 
  distinct 
  o1.*, 
  o2.deal_uuid
from 
(SELECT   
			 CASE WHEN COALESCE(feature_country, 'US') = 'GB' THEN 'UK'
                  WHEN COALESCE(feature_country, 'US') IN ('VI', 'FM', 'PR', 'MH') THEN 'US'
                  ELSE COALESCE(feature_country, 'US')
             END  AS country_code,
             opportunity_id,
             accountid AS account_id,
             id,
             cloned_from
      FROM user_edwprod.sf_opportunity_1
      WHERE opportunity_id IS NOT NULL
) o1
    LEFT JOIN (SELECT DISTINCT deal_uuid, Id FROM user_edwprod.sf_opportunity_2 WHERE LENGTH(opportunity_id) = 15 AND opportunity_id IS NOT NULL) o2 ON o1.ID = o2.ID
;




---------------------------------------RECURSIVE LOGIC FOR CLONES DEALS


create multiset volatile table np_postvet_temp1 as 
(select 
       history_data.jsonextractvalue('$.additionalInfo.deal.clonedFrom') as parent_deal_id, 
       deal_id as child_deal_id
          from sb_merchant_experience.history_event
          where event_type = 'DRAFT_DEAL_CREATION'
          and history_data.jsonextractvalue('$.additionalInfo.deal.clonedFrom') is not null
          and event_date < current_date
          group by 1,2
) with data on commit preserve rows;

create multiset volatile table np_postvet_temp2 as (
select 
    a.deal_id as parent_deal_id, 
    c.deal_uuid as child_deal_id 
from sb_merchant_experience.history_event a
join sandbox.np_opportunity_details_2 as b on a.deal_id = b.deal_uuid
join sandbox.np_opportunity_details_2 as c on c.cloned_from = b.id
where event_type = 'POST_VETTING_STARTED' and a.event_date < current_date
) with data on commit preserve rows;


create multiset volatile table np_postvet_temp3 as (
select 
   coalesce(b.parent_deal_id, a.parent_deal_id) parent_deal_id, 
   coalesce(b.child_deal_id, a.child_deal_id) child_deal_id,
   case when b.parent_deal_id is not null then 1 else 0 end submitted
from 
  np_postvet_temp1 as a 
  full outer join np_postvet_temp2 as b on a.parent_deal_id = b.parent_deal_id and a.child_deal_id = b.child_deal_id
) with data on commit preserve rows;


create multiset volatile table np_non_dupe as (
     select deal_id, 
       case when b.deal_uuid is not null then 1 else 0 end submitted
     from sb_merchant_experience.history_event as a
     left join sandbox.np_opportunity_details_2 as b on a.deal_id = b.deal_uuid
     where event_type = 'DRAFT_DEAL_CREATION'
          and history_data.jsonextractvalue('$.additionalInfo.deal.clonedFrom') is null
          and event_date < current_date
     group by 1,2
) with data on commit preserve rows;

create multiset volatile table np_non_dupe2 as (
	select 
		deal_id prime_deal_id, 
		deal_id parent_deal_id, 
		cast(null as varchar(36)) child_deal_id, 
		submitted
	from np_non_dupe ----only ones in draft which have no cloned from
union all 
     select  
          coalesce(dupe.deal_id, a.parent_deal_id) prime_deal_id,
          coalesce(dupe.deal_id, a.parent_deal_id) parent_deal_id, 
  	      a.child_deal_id, 
  	      a.submitted
  	from 
  	   np_non_dupe dupe
  	 join      
  	   np_postvet_temp3 as a on dupe.deal_id = a.parent_deal_id
) with data on commit preserve rows;


delete from sandbox.np_deal_clone_rel;
insert into sandbox.np_deal_clone_rel
with recursive cte (prime_deal_id, parent_deal_id, child_deal_id, depth, submitted)
as 
( select  
          prime_deal_id,
          parent_deal_id, 
  	      child_deal_id, 
  	      case when child_deal_id is null then cast(1 as int) else cast(2 as int) end as depth,
  	      submitted
  	from 
  	   np_non_dupe2
union all 
   select
      cte.prime_deal_id
      ,b.parent_deal_id
      , b.child_deal_id
      , cast(cte.depth + 1 as int) as depth
      , b.submitted
  	from cte
  	join np_postvet_temp3 b
  		on cte.child_deal_id = b.parent_deal_id
)
select * from cte;grant all on sandbox.np_deal_clone_rel to abautista, jkerin with grant option;



/*
drop table sandbox.np_all_opp_clone_rel;
create multiset table sandbox.np_all_opp_clone_rel (
    prime_deal_id varchar(36)
	,parent_deal_id varchar(36)
	, child_deal_id varchar(36)
	, depth smallint
) primary index (prime_deal_id, parent_deal_id, child_deal_id, depth)



create multiset volatile table np_all_opp_child as (
select 
    b.deal_uuid as parent_deal_id, 
    c.deal_uuid as child_deal_id 
from sandbox.np_opportunity_details_2 as b 
left join sandbox.np_opportunity_details_2 as c on c.cloned_from = b.id
group by 1,2
) with data on commit preserve rows;


insert into sandbox.np_all_opp_clone_rel
with recursive cte (prime_deal_id, parent_deal_id, child_deal_id, depth)
as 
( select  
          deal_uuid prime_deal_id,
          deal_uuid parent_deal_id, 
  	      cast(null as varchar(36)) child_deal_id, 
  	      cast(1 as int) depth
  	from 
  	   sandbox.np_opportunity_details_2 as a 
  	   where cloned_from is null 
union all 
   select
      cte.prime_deal_id
      ,b.parent_deal_id
      , b.child_deal_id
      , cast(cte.depth + 1 as int) as depth
  	from cte as cte
  	join np_all_opp_child b on b.parent_deal_id = cte.parent_deal_id 
)
select * from cte 

grant all on sandbox.np_deal_clone_rel to abautista, jkerin with grant option;*/
------------------------------------------------------------------------------------------------cb_midfunnel_base



create multiset volatile table  hist_event as (
select 	a.merchant_id as merchantuuid, 
	a.deal_id as dealuuid, 
        a.event_type,
	case when a.event_type = 'DRAFT_DEAL_CREATION' then 1 else 0 end as deal_start_flag, 
	case when a.event_type = 'DRAFT_DEAL_CREATION' and history_data.jsonextractvalue('$.additionalInfo.deal.clonedFrom') is not null
	     then 1 else 0 end as dupe_flag,
	case when a.event_type in ('METRO_CONTRACT_SIGNIN','G1_METRO_CONTRACT_SIGNED') then 1 else 0 end as deal_close_flag, 
	cast(a.event_date as timestamp) as event_ts, 
	cast(a.event_date as date) as eventdate, 
	a.user_type, 
	history_data.JSONExtractValue('$.additionalInfo.places.0.country')  as deal_country, 
	history_data.jsonextractvalue('$.additionalInfo.deal.dealIdentifierAttributes.locale') as deal_locale, 
	history_data.jsonextractvalue('$.additionalInfo.deal.dealIdentifierAttributes.dealType') as deal_type,
	history_data.jsonextractvalue('$.additionalInfo.deal.dealIdentifierAttributes.merchandisingEventName') as campaign,
	history_data.jsonextractvalue('$.additionalInfo.holdAt') hold_at,
	history_data.jsonextractvalue('$.additionalInfo.dealStatus') deal_status,
	regexp_replace(cast(history_data.jsonExtract('$.additionalInfo.invalidData.*') as varchar(100)), '\[|\]|"') as invalid_data_he,
	character_length(invalid_data_he) - character_length(oreplace(invalid_data_he,',','')) + 1 as invalid_reasons_count  
from sb_merchant_experience.history_event as a
qualify (ROW_NUMBER() OVER (PARTITION BY merchantuuid,dealuuid,a.event_type ORDER BY event_ts )) = 1
where eventdate >= '2022-01-01'
and a.event_type in ('METRO_CONTRACT_SIGNIN','DRAFT_DEAL_CREATION','G1_METRO_CONTRACT_SIGNED')
)with data  primary index (dealuuid,merchantuuid) on commit preserve rows;


delete from sandbox.jk_CB_base_v1;
insert into sandbox.jk_CB_base_v1
sel a.*,
	opp.permalink as deal_permalink, 
	op2.id as Opportunity_ID, 
	opp.opportunity_name, 
	opp.category_v3, opp.subcategory_v3, opp.primary_deal_services, opp.feature_country, 
	coalesce(opp.accountid,dm.salesforce_account_id,null) as account_id,
	case when lower(AC.MERCHANT_segmentation__c) like any ('%gold%','%platinum%','%silver%') then 'S+' else 'B-' end as Metal_group, 
	AC.MERCHANT_segmentation__c
from hist_event a
left join user_edwprod.sf_opportunity_2 op2 on op2.deal_uuid = a.dealuuid -- get opp ID from deal uuid
left join dwh_base_sec_view.Opportunity_1 as opp on opp.id = op2.id and lower(opp.opportunity_name) like any ('dmapi%','%*g1m*%') --identifiers for a metro deal
left join dwh_base_sec_view.sf_opportunity_3 as op3 on opp.id = op3.id--to get orevetting and post vetting
left join ( select 
             merchant_uuid, 
             salesforce_account_id  
             from user_edwprod.dim_merchant 
             qualify (ROW_NUMBER() OVER (PARTITION BY salesforce_account_id ORDER BY updated_at desc )) = 1
           ) as dm on dm.merchant_uuid = a.merchantuuid --get merchant_uuid
left join user_groupondw.sf_account as ac on ac.id = account_id;


create multiset volatile table jk_cb_deals as (
select 
cbb1.merchantuuid, 
cbb1.dealuuid, 
cbb1.deal_start_flag, 
cbb1.event_ts create_ts, 
cbb1.eventdate create_eventdate, 
cbb1.user_type,  
cbb2.deal_close_flag, 
cbb2.event_ts submit_ts ,
cbb2.eventdate submit_eventdate, 
cbb2.hold_at, cbb2.deal_status, 
cbb2.invalid_data_he, 
cbb2.invalid_reasons_count,
upper(coalesce (cbb1.deal_country, cbb2.deal_country, cbb1.deal_locale, cbb2.deal_locale)) country, 
coalesce (cbb1.deal_type, cbb2.deal_type) deal_type, coalesce (cbb1.campaign, cbb2.campaign) campaign
,coalesce(cbb1.deal_permalink, cbb2.deal_permalink) deal_permalink ,
coalesce(cbb1.opportunity_id ,cbb2.opportunity_id) opportunity_id ,
coalesce(cbb1.opportunity_name ,cbb2.opportunity_name) opportunity_name
,coalesce(cbb1.category_v3 ,cbb2.category_v3) category_v3,  
coalesce(cbb1.subcategory_v3 ,cbb2.subcategory_v3)subcategory_v3, 
coalesce(cbb1.primary_deal_services ,cbb2.primary_deal_services) PDS,
coalesce(cbb1.account_id ,cbb2.account_id)account_id, coalesce(cbb1.metal_group ,cbb2.metal_group) metal_group, 
coalesce(cbb1.merchant_segmentation__c ,cbb2.merchant_segmentation__c)  metal
from 
    (sel * from sandbox.jk_CB_base_v1 where event_type ='DRAFT_DEAL_CREATION') cbb1 
left join 
    (select * from sandbox.jk_CB_base_v1 where event_type in ('METRO_CONTRACT_SIGNIN','G1_METRO_CONTRACT_SIGNED'))cbb2 
	on cbb1.merchantuuid  = cbb2.merchantuuid and  cbb1.dealuuid = cbb2.dealuuid
where cbb1.user_type = 	'merchant_account' 
)with data primary index (merchantuuid, dealuuid)on commit preserve rows;



delete from sandbox.jk_CB_midfunnel;
insert into sandbox.jk_CB_midfunnel
select  
cb.*, 
dc.prime_deal_id, 
dc.deal_to_launch , 
depth, 
submitted
, case when depth=1 then 1 else 0 end as prime_flag
, case when deal_to_launch = cb.dealuuid then 1 else 0 end as  final_child_flag 
, was_live as Launched, Launch_date
from jk_cb_deals cb
join (select prime_deal_id ,
          case when depth= 1 then parent_deal_id else child_deal_id end as deal_id , 
          depth, 
          submitted 
		, last_value(child_deal_id) OVER(PARTITION BY prime_deal_id ORDER BY depth asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as final_child  -- finds the deal that will be launched 
		, coalesce(final_child , deal_id) deal_to_launch -- removes nulls from deals without a child
	from sandbox.np_deal_clone_rel) dc on dc.deal_id = cb.dealuuid
left join(select distinct deal_uuid , was_live  , Launch_date from sandbox.pai_deals) pd on pd.deal_uuid= cb.dealuuid





------------------------------------------------------------------------------------------------cb_midfunnel_agg



create volatile multiset table first_deal as (
sel
merchantuuid,
m.first_close_date,
m.first_launch_date,
p.account_id,
metal_group,
metal,
dealuuid as start_dealid,
opportunity_id,
deal_permalink,
opportunity_name,
category_v3,
subcategory_v3,
pds,
case 
    when m.merchant_uuid is null then 1
    when p.create_eventdate <= m.first_launch_date then 1
    when (first_close_date is null and first_launch_date is null) then 1
    else 0 end as new_merchant_flag,
prime_deal_id,
create_eventdate as start_date_first,
create_ts as start_timestamp_first,
deal_close_flag as deal_submitted_first,
submit_eventdate,
deal_status as first_deal_status,
invalid_data_he,
invalid_reasons_count,
country,
deal_type,
campaign as campaign_first,
submitted as cb_submitted,
launch_date
from sandbox.jk_cb_midfunnel p
left join sandbox.pai_merchants m on p.merchantuuid = m.merchant_uuid
where
	prime_flag = 1
) with data primary index (start_dealid, merchantuuid) on commit preserve rows;

create volatile multiset table last_deal as (
sel
merchantuuid,
dealuuid,
opportunity_id,
deal_permalink,
opportunity_name,
category_v3,
subcategory_v3,
pds,
prime_deal_id,
prime_flag,
create_eventdate,
deal_close_flag,
submit_eventdate,
deal_status,
invalid_data_he,
invalid_reasons_count,
country,
deal_type,
campaign as campaign_last,
submitted as cb_submitted,
launch_date,
depth
from sandbox.jk_cb_midfunnel p
qualify row_number() over(partition by prime_deal_id order by create_eventdate desc) = 1
) with data primary index (merchantuuid, prime_deal_id) on commit preserve rows;


----tested this before doing the next query: select deal_uuid, count(distinct close_order) xyz from sandbox.pai_opp_mtd_attrib group by 1 having xyz > 1;
delete from sandbox.pai_cb_midfunnel_agg;
insert into sandbox.pai_cb_midfunnel_agg
select 
   f.*, 
   case when mt1.deal_uuid is not null then 1 else 0 end opp_has_close,
   case when deal_submitdate_first is not null and mt1.close_order = 1 then 1 
     when deal_submitdate_first is not null then 0 
     end first_close,
   case when deal_launch_date is not null and launch_order = 1 then 1 
     when deal_launch_date is not null then 0 
     end first_launch,
   por_relaunch 
from 
(sel
p.merchantuuid,
p.new_merchant_flag,
p.account_id,
p.metal_group,
p.metal,
case when p.country = 'GB' then 'UK' else p.country end country,
p.deal_type,
p.campaign_first,
p.start_dealid as dealid_start_first,
p.start_date_first deal_start_date,
p.start_timestamp_first as deal_start_timestamp,
p.deal_submitted_first submitted_flag,
p.submit_eventdate as deal_submitdate_first,
p.first_deal_status as deal_submit_status,
p.invalid_data_he as start_invalid_reasons,
p.invalid_reasons_count as start_reasons_count,
case when f.launch_date is not null then f.dealuuid else p.prime_deal_id end as dealid_final,
case when coalesce(f.launch_date,p.launch_date) is not null then 1 else 0 end launched,
coalesce(f.launch_date,p.launch_date) as deal_launch_date,
case when f.launch_date is not null then f.dealuuid when p.launch_date is not null then p.start_dealid end as deal_launched,
case when coalesce(f.launch_date,p.launch_date) is not null then coalesce(f.category_v3, p.category_v3) end launch_category_v3,
case when coalesce(f.launch_date,p.launch_date) is not null then coalesce(f.subcategory_v3, p.subcategory_v3) end launch_subcategory_v3,
case when coalesce(f.launch_date,p.launch_date) is not null then coalesce(f.pds, p.pds) end launch_pds,
case when coalesce(f.launch_date,p.launch_date) is not null then coalesce(f.opportunity_name, p.opportunity_name) end launch_opportunity_name,
f.deal_permalink,
case when depth > 1 then depth - 1 else 0 end as cloned_number
from first_deal p
left join last_deal f on p.start_dealid = f.prime_deal_id) as f 
left join (select 
               merchant_uuid, 
               deal_uuid,
               max(por_relaunch) por_relaunch,
               min(closedate) closedate, 
               min(close_order) close_order
            from sandbox.pai_opp_mtd_attrib 
            group by 1,2) mt1 on mt1.merchant_uuid = f.merchantuuid and mt1.deal_uuid = f.dealid_start_first
left join 
           (select 
               merchant_uuid, 
               deal_uuid,
               min(launch_date) launch_date, 
               min(launch_order) launch_order
            from sandbox.pai_opp_mtd_attrib 
            group by 1,2) mt2 on mt2.merchant_uuid = f.merchantuuid and mt2.deal_uuid = f.deal_launched
;
grant select on sandbox.pai_cb_midfunnel_agg to public;
GRANT ALL ON sandbox.pai_cb_midfunnel_agg TO abautista, nihpatel,akuthiala, jkerin, ub_bizops WITH GRANT OPTION;


/* Another test
select 
opp_has_close, count(1)
from 
sandbox.pai_cb_midfunnel_agg
where deal_submitdate_first is not null
group by 1;*/

----a bunch of deals with launches but wont have any lead assigned to the lead start
case when launch_date is not null then DENSE_RANK () OVER (PARTITION BY account_id, case when launch_date is null then 0 else 1 end ORDER BY launch_date ASC) end launch_order,
case when closedate is not null then DENSE_RANK() OVER (PARTITION BY country_code, account_id, case when closedate is null then 0 else 1 end ORDER BY closedate asc) end close_order,
case when closedate is not null then DENSE_RANK() OVER (PARTITION BY country_code, account_id, case when closedate is null then 0 else 1 end ORDER BY closedate desc) end close_recency 

---getting paid first last touch ---doing distinct to get ---the country is added because even if in close order it is considered. we need to consider her while ranking
/*

insert into sandbox.np_deal_clone_rel
with recursive cte (prime_deal_id, parent_deal_id, child_deal_id, depth, submitted)
as 
( select  
          prime_deal_id,
          parent_deal_id, 
  	      child_deal_id, 
  	      case when child_deal_id is null then cast(1 as int) else cast(2 as int) end as depth,
  	      submitted
  	from 
  	   np_non_dupe2
union all 
   select
      cte.prime_deal_id
      ,b.parent_deal_id
      , b.child_deal_id
      , cast(cte.depth + 1 as int) as depth
      , b.submitted
  	from cte
  	join np_postvet_temp3 b
  		on cte.child_deal_id = b.parent_deal_id
)
select * from cte;grant all on sandbox.np_deal_clone_rel to abautista, jkerin with grant option;


select 
   account_id, 
   count(1) xyz
from 
(select 
   account_id, 
   merchantuuid merchant_uuid, 
   count(distinct country) distinct_country,
   min(deal_start_date) first_activity_date
from sandbox.pai_cb_midfunnel_agg
group by 1,2,3) as fin 
group by 1 
having xyz >1;

select 
   country country_code, 
   account_id, 
   merchantuuid merchant_uuid, 
   deal_start_date
from sandbox.pai_cb_midfunnel_agg
where account_id = '0013c00001zFbQgAAK';


select 
   account_id, 
   count(distinct merchantuuid) distinct_country,
   min(deal_start_date) first_activity_date
from sandbox.pai_cb_midfunnel_agg
group by 1
where length(country) > 1
having distinct_country > 1;*/

/*
drop table sandbox.np_temp_lead_assoc;
create multiset table sandbox.np_temp_lead_assoc as (
select 
   distinct 
   COALESCE (a.country_code, b.country_code) country_code,
   COALESCE (a.account_id, b.account_id) account_id, 
   coalesce(a.merchant_uuid, b.merchant_uuid) merchant_uuid, 
   COALESCE (a.first_activity_date, b.first_activity_date) first_activity_date
from 
(select 
   country country_code, 
   account_id, 
   max(merchantuuid) merchant_uuid, 
   min(deal_start_date) first_activity_date
from sandbox.pai_cb_midfunnel_agg ---deal start but no closes
where length(country) > 1
group by 1,2) as a 
full outer join 
(select 
   country_code,
   account_id, 
   merchant_uuid,
   CloseDate first_activity_date ----closed merchants
from 
sandbox.pai_opp_mtd_attrib
where CloseDate <= current_date
and close_order = 1) as b on a.account_id = b.account_id and a.country_code = b.country_code  
)  with data primary index (country_code,account_id);


collect stats on sandbox.np_temp_lead_assoc columns (country_code,account_id);



create multiset volatile table np_temp_lead_assoc as ( 
select 
   a.country country_code, 
   a.account_id, 
   max(merchantuuid) merchant_uuid, 
   min(deal_start_date) first_activity_date
from sandbox.pai_cb_midfunnel_agg as a
left join sandbox.np_merch_lead_asgnmt as b on a.account_id = b.account_id and a.country = b.country_code
where length(country) > 1 and b.account_id is null
group by 1,2) with data on commit preserve rows;

create multiset table sandbox.np_merch_lead_asgnmt as (

delete from sandbox.np_merch_lead_asgnmt;
insert into sandbox.np_merch_lead_asgnmt
select 
   distinct 
   f.*, 
   case when max_value = 1 and campaign_paid_category = 1 then paid_direct_first
         when max_value = 0 and campaign_paid_category = 0 then paid_direct_first
         else 0 end paid_first_last_touch
from 
(select 
     a.country_code,
     a.account_id, 
     a.merchant_uuid,
     a.first_activity_date, 
     b.lead_create_date, 
     b.campaign_name, 
     b.LeadSource,
     b.campaign_new_format,
     b.sem_partner,
     b.sem_brand,
     b.mktg_txny_version,
     b.mktg_country,
     b.mktg_test_division,
     b.mktg_traffic_source,
     b.mktg_audience,
     b.mktg_sem_type,
     b.mktg_platform,
     b.mktg_creative,
     b.campaign_paid_category,
     b.campaign_group_lvl1_2,
     b.campaign_group_lvl1_1,
     b.campaign_group_lvl2,
     case when dense_rank () over (partition by convertedaccountid, a.country_code, first_activity_date  order by lead_create_date desc) = 1 then 1 else 0 end last_touch,
     case when dense_rank () over (partition by convertedaccountid, a.country_code, first_activity_date, campaign_paid_category order by lead_create_date desc) = 1 then 1 else 0 end paid_direct_first, 
     max(campaign_paid_category) over (partition by convertedaccountid, a.country_code, first_activity_date) max_value 
from sandbox.np_temp_lead_assoc as a 
left join sandbox.pai_leads as b on a.account_id = b.convertedaccountid and b.lead_create_date <= a.first_activity_date and a.country_code = b.country_code
where first_activity_date < '2020-01-01'
) as f 
where 
     case when max_value = 1 and campaign_paid_category = 1 then paid_direct_first
         when max_value = 0 and campaign_paid_category = 0 then paid_direct_first
         else 0 end = 1
;*/

------
select * from np_temp_lead_assoc where account_id = '0013c00001xrQGzAAM';
drop table np_temp_lead_assoc;


create volatile table np_temp_lead_assoc as ( 
select 
   COALESCE (a.country_code, b.country_code) country_code,
   COALESCE (a.account_id, b.account_id) account_id, 
   COALESCE (a.launch_date, b.CloseDate) first_activity_date
from 
(select 
   country_code,
   account_id, 
   launch_date 
from 
sandbox.pai_opp_mtd_attrib 
where launch_order = 1) as a
full outer join 
(select 
   country_code,
   account_id, 
   CloseDate 
from 
sandbox.pai_opp_mtd_attrib 
where close_order  = 1) as b on a.country_code = b.country_code and a.account_id = b.account_id 
) with data unique primary index (account_id, country_code) on commit preserve rows
;


delete from sandbox.np_merch_lead_asgnmt;
insert into sandbox.np_merch_lead_asgnmt
select 
   distinct 
   f.*, 
   case when max_value = 1 and campaign_paid_category = 1 then paid_direct_first
         when max_value = 0 and campaign_paid_category = 0 then paid_direct_first
         else 0 end paid_first_last_touch
from 
(select 
     a.country_code,
     a.account_id, 
     a.first_activity_date first_activity_date, 
     b.lead_create_date, 
     b.campaign_name, 
     b.LeadSource,
     b.campaign_new_format,
     b.sem_partner,
     b.sem_brand,
     b.mktg_txny_version,
     b.mktg_country,
     b.mktg_test_division,
     b.mktg_traffic_source,
     b.mktg_audience,
     b.mktg_sem_type,
     b.mktg_platform,
     b.mktg_creative,
     b.campaign_paid_category,
     b.campaign_group_lvl1_2,
     b.campaign_group_lvl1_1,
     b.campaign_group_lvl2,
     case when dense_rank () over (partition by convertedaccountid, first_activity_date  order by lead_create_date desc) = 1 then 1 else 0 end last_touch,
     case when dense_rank () over (partition by convertedaccountid, first_activity_date, campaign_paid_category order by lead_create_date desc) = 1 then 1 else 0 end paid_direct_first, 
     max(campaign_paid_category) over (partition by convertedaccountid, a.country_code, first_activity_date) max_value 
from np_temp_lead_assoc as a 
left join sandbox.pai_leads as b on a.account_id = b.convertedaccountid and b.lead_create_date <= a.first_activity_date and a.country_code = b.country_code) as f 
where 
     case when max_value = 1 and campaign_paid_category = 1 then paid_direct_first
         when max_value = 0 and campaign_paid_category = 0 then paid_direct_first
         else 0 end = 1
;


create volatile table np_temp_lead_assoc2 as 
(select 
   a.country country_code, 
   a.account_id, 
   min(deal_start_date) first_activity_date
from sandbox.pai_cb_midfunnel_agg as a ---deal start but no closes
left join np_temp_lead_assoc as b on a.country = b.country_code and a.account_id = b.account_id 
where length(a.country) > 1 and b.account_id is null
group by 1,2
)  with data unique primary index (country_code,account_id) on commit preserve rows;


insert into sandbox.np_merch_lead_asgnmt
select 
   distinct 
   f.*, 
   case when max_value = 1 and campaign_paid_category = 1 then paid_direct_first
         when max_value = 0 and campaign_paid_category = 0 then paid_direct_first
         else 0 end paid_first_last_touch
from 
(select 
     a.country_code,
     a.account_id, 
     a.first_activity_date first_activity_date, 
     b.lead_create_date, 
     b.campaign_name, 
     b.LeadSource,
     b.campaign_new_format,
     b.sem_partner,
     b.sem_brand,
     b.mktg_txny_version,
     b.mktg_country,
     b.mktg_test_division,
     b.mktg_traffic_source,
     b.mktg_audience,
     b.mktg_sem_type,
     b.mktg_platform,
     b.mktg_creative,
     b.campaign_paid_category,
     b.campaign_group_lvl1_2,
     b.campaign_group_lvl1_1,
     b.campaign_group_lvl2,
     case when dense_rank () over (partition by convertedaccountid, first_activity_date  order by lead_create_date desc) = 1 then 1 else 0 end last_touch,
     case when dense_rank () over (partition by convertedaccountid, first_activity_date, campaign_paid_category order by lead_create_date desc) = 1 then 1 else 0 end paid_direct_first, 
     max(campaign_paid_category) over (partition by convertedaccountid, a.country_code, first_activity_date) max_value 
from np_temp_lead_assoc2 as a 
left join sandbox.pai_leads as b on a.account_id = b.convertedaccountid and b.lead_create_date <= a.first_activity_date and a.country_code = b.country_code) as f 
where 
     case when max_value = 1 and campaign_paid_category = 1 then paid_direct_first
         when max_value = 0 and campaign_paid_category = 0 then paid_direct_first
         else 0 end = 1
;

collect stats column (account_id, country_code) on sandbox.np_merch_lead_asgnmt;

delete from sandbox.pai_ss_leads_midfunnel;
insert into sandbox.pai_ss_leads_midfunnel
select 
     a.*, 
     b.lead_create_date, 
     b.campaign_name, 
     b.LeadSource,
     b.campaign_new_format,
     b.sem_partner,
     b.sem_brand,
     case when (a.new_merchant_flag = 1 or a.first_close = 1 or a.first_launch = 1) then coalesce(b.campaign_paid_category, 0) end campaign_paid_category,
     case when (a.new_merchant_flag = 1 or a.first_close = 1 or a.first_launch = 1) then coalesce(b.campaign_group_lvl1_2, 'Direct / Referral / Other') end campaign_group_lvl1_2,
     case when (a.new_merchant_flag = 1 or a.first_close = 1 or a.first_launch = 1) then coalesce(b.campaign_group_lvl1_1, 'Direct / Referral / Other') end campaign_group_lvl1_1
from 
   sandbox.pai_cb_midfunnel_agg as a 
   left join sandbox.np_merch_lead_asgnmt as b 
      on a.account_id  = b.account_id and a.country = b.country_code
      and (a.new_merchant_flag = 1 or a.first_close = 1 or a.first_launch = 1)
;


delete from sandbox.pai_lead_opp_mtd_attrib;
insert into sandbox.pai_lead_opp_mtd_attrib
select 
   fin.*, 
    case when (LOWER(LeadSource) = 'metro - self service' or (dmapi_flag = 1 and close_order = 1)) and LOWER(opportunity_name) not like '%dmapi%' then 'Metro Lead - Sales Team Close'
         when LOWER(LeadSource) = 'metro - self service' and close_order > 1 then 'Existing Metro'
         when (LOWER(LeadSource) = 'metro - self service' or (dmapi_flag = 1 and close_order = 1)) and close_order  = 1 then 'New Metro'
         else 'Other' end mtd_attribution
from 
(select 
    a.*, 
    case when (a.close_order = 1 or a.launch_order = 1) then b.lead_create_date end lead_create_date,
    case when (a.close_order = 1 or a.launch_order = 1) then b.campaign_name end campaign_name, 
    case when (a.close_order = 1 or a.launch_order = 1) then b.LeadSource end LeadSource,
    case when (a.close_order = 1 or a.launch_order = 1) then b.campaign_new_format end campaign_new_format,
    case when (a.close_order = 1 or a.launch_order = 1) then b.sem_partner end sem_partner,
    case when (a.close_order = 1 or a.launch_order = 1) then b.sem_brand end sem_brand,
    case when (a.close_order = 1 or a.launch_order = 1) then b.mktg_txny_version end mktg_txny_version,
    case when (a.close_order = 1 or a.launch_order = 1) then b.mktg_country end mktg_country,
    case when (a.close_order = 1 or a.launch_order = 1) then b.mktg_test_division end mktg_test_division,
    case when (a.close_order = 1 or a.launch_order = 1) then b.mktg_traffic_source end mktg_traffic_source,
    case when (a.close_order = 1 or a.launch_order = 1) then b.mktg_audience end mktg_audience,
    case when (a.close_order = 1 or a.launch_order = 1) then b.mktg_sem_type end mktg_sem_type,
    case when (a.close_order = 1 or a.launch_order = 1) then b.mktg_platform end mktg_platform,
    case when (a.close_order = 1 or a.launch_order = 1) then b.mktg_creative end mktg_creative,
    case when  (a.close_order = 1 or launch_order = 1) then coalesce(b.campaign_paid_category,0) end campaign_paid_category, 
    case when  (a.close_order = 1 or launch_order = 1) then coalesce(b.campaign_group_lvl1_2, 'Direct / Referral / Other') end campaign_group_lvl1_2,
    case when  (a.close_order = 1 or launch_order = 1) then coalesce(b.campaign_group_lvl1_1, 'Direct / Referral / Other') end campaign_group_lvl1_1,
    case when (a.close_order = 1 or a.launch_order = 1) then b.campaign_group_lvl2 end campaign_group_lvl2
from sandbox.pai_opp_mtd_attrib as a 
left join sandbox.np_merch_lead_asgnmt as b on a.account_id  = b.account_id and a.country_code = b.country_code
) as fin
;

grant select on sandbox.np_merch_lead_asgnmt to public;
grant select on sandbox.pai_ss_leads_midfunnel to public;
grant select on sandbox.pai_lead_opp_mtd_attrib to public;

show table sandbox.pai_lead_opp_mtd_attrib;



----the null could be because they dont have a lead or because the country is null in the midfunnel. you will need to first take care of the length issue for some countries. 

drop table sandbox.pai_lead_opp_mtd_attrib;

select case when lead_create_date is not null then 1 else 0 end, 
count(1)
from sandbox.pai_ss_leads_midfunnel as a 
where (a.new_merchant_flag = 1 or a.first_close = 1 or a.first_launch = 1)
group by 1;

--------------------

select 
    case when campaign_name is not null then 1 else 0 end, 
    count(1) 
from sandbox.pai_ss_leads_midfunnel 
where new_merchant_flag = 1
group by 1;



case when (LOWER(leadsource) = 'metro - self service' or (dmapi_flag = 1 and close_order = 1)) and lower(opportunity)

CASE
             WHEN (dl.convertedaccountid IS NOT NULL OR (ma.dmapi_flag = 1 AND ma.close_order = 1)) AND LOWER(ma.opportunity_name) NOT LIKE '%dmapi%' THEN 'Metro Lead - Sales Team Close'
             WHEN (dl.convertedaccountid IS NOT NULL OR (ma.dmapi_flag = 1 AND ma.close_order = 1)) AND ma.close_order > 1 THEN 'Existing Metro'
             WHEN (dl.convertedaccountid IS NOT NULL OR (ma.dmapi_flag = 1 AND ma.close_order = 1)) AND ma.close_order = 1 THEN 'New Metro'
             WHEN day_prev_owner.mtd_attribution IS NOT NULL THEN day_prev_owner.mtd_attribution
             ELSE close_owner.mtd_attribution
         END mtd_attribution,



select * from sandbox.pai_ss_leads_midfunnel where new_merchant_flag = 0 and LeadSource  is null;

select * from user_groupondw.sf_lead where ConvertedAccountId = '0013c000021HDp9AAG';
select * from sandbox.pai_leads where ConvertedAccountId = '0013c000021HDp9AAG';




    
---------------------------------------------APPENDIX
select distinct campaign_name, campaign_group_lvl1_2, campaign_group_lvl1_1 , campaign_group_lvl2  
from sandbox.pai_leads order by 1,2,3;



select 
    trunc(deal_launch_date, 'iw') + 6 launch_week, 
    deal_launch_date,
    merchantuuid, 
    account_id,
    deal_launched 
from sandbox.pai_leads_midfunnel 
where first_launch = 1 and account_id  = '0013c000021QUlRAAW'
order by 1;







select
   b.merchant_uuid,
   c.account_id,
   a.*
from sandbox.np_deal_clone_rel as a 
left join sandbox.pai_deals as b on a.parent_deal_id = b.deal_uuid
left join sandbox.pai_merchants as c on b.merchant_uuid = c.merchant_uuid
where b.merchant_uuid = '3d2e1692-025c-4594-9e1c-efb4944895d2';

select * from sb_merchant_experience.history_event where merchant_id = '42b3a2cc-b320-4527-ac1b-94029c10fd32';
select * from sandbox.jk_CB_midfunnel where account_id = '0013c000021QUlRAAW';
select * from sandbox.pai_leads_midfunnel plm  where account_id  = '0013c000021QUlRAAW';
select * from sandbox.pai_opp_mtd_attrib where account_id = '0013c0000222n4zAAA';



CAST(CASE WHEN LOWER(opportunity_name) LIKE '%dmapi%' or LOWER(opportunity_name) LIKE '%*G1M*%' THEN 1 ELSE 0 END AS TINYINT) AS dmapi_flag

  select
    c.launch_date as dt
    , case when c.feature_country = 'GB' then 'UK' else c.feature_country end feature_country
    , c.accountid
    , c.deal_uuid
from 
grp_gdoop_sup_analytics_db.pai_opp_mtd_attrib c
    where c.feature_country in ( 'US', 'CA')
      and c.grt_l1_cat_name = 'L1 - Local'
      and (lower(c.mtd_attribution) like '%metro%' or lower(c.mtd_attribution_intl) like '%metro%')
      and c.launch_order = 1
      and c.launch_date is not null
      and c.por_relaunch = 0;
     
     LOWER(leadsource) = 'metro - self service'

WHEN (b.account_id IS NOT NULL OR (ma.dmapi_flag = 1 AND ma.close_order = 1)) AND LOWER(ma.opportunity_name) NOT LIKE '%dmapi%' THEN 'Metro Lead - Sales Team Close'
        WHEN (dl.convertedaccountid IS NOT NULL OR (ma.dmapi_flag = 1 AND ma.close_order = 1)) AND ma.close_order > 1 THEN 'Existing Metro'
        WHEN (dl.convertedaccountid IS NOT NULL OR (ma.dmapi_flag = 1 AND ma.close_order = 1)) AND ma.close_order = 1 THEN 'New Metro'
-----launch and close order has to be 1
        
select 
   trunc(launch_date, 'iw') + 6 launch_week, 
   count(distinct account_id) total_accs
from sandbox.pai_lead_opp_mtd_attrib as c
where country_code in ( 'US', 'CA')
      and c.grt_l1_cat_name = 'L1 - Local'
      and c.launch_order = 1
      and c.launch_date is not null
      and c.por_relaunch = 0
      and (LOWER(leadsource) = 'metro - self service' or (dmapi_flag = 1 and close_order = 1))
group by 1
order by 1 desc;

select * from sandbox.pai_lead_opp_mtd_attrib sample 5;


CASE
        WHEN (b.account_id IS NOT NULL OR (ma.dmapi_flag = 1 AND ma.close_order = 1)) AND LOWER(ma.opportunity_name) NOT LIKE '%dmapi%' THEN 'Metro Lead - Sales Team Close'
        WHEN (dl.convertedaccountid IS NOT NULL OR (ma.dmapi_flag = 1 AND ma.close_order = 1)) AND ma.close_order > 1 THEN 'Existing Metro'
        WHEN (dl.convertedaccountid IS NOT NULL OR (ma.dmapi_flag = 1 AND ma.close_order = 1)) AND ma.close_order = 1 THEN 'New Metro'
        WHEN day_prev_owner.mtd_attribution IS NOT NULL THEN day_prev_owner.mtd_attribution
        ---ELSE close_owner.mtd_attribution figure out how to add close owner
    END mtd_attribution

case when launch_date is not null then DENSE_RANK () OVER (PARTITION BY account_id, case when launch_date is null then 0 else 1 end ORDER BY launch_date ASC) end launch_order,
case when closedate is not null then DENSE_RANK() OVER (PARTITION BY country_code, account_id, case when closedate is null then 0 else 1 end ORDER BY closedate asc) end close_order,
case when closedate is not null then DENSE_RANK() OVER (PARTITION BY country_code, account_id, case when closedate is null then 0 else 1 end ORDER BY closedate desc) end close_recency 
    

SELECT
     a.merchantuuid, 
     a.dealuuid, 
     a.country, 
     a.account_id, 
     mt2.launch_date,
     mt2.launch_order
from sandbox.jk_CB_midfunnel as a
left join
(select 
               merchant_uuid, 
               deal_uuid,
               min(launch_date) launch_date, 
               min(launch_order) launch_order
            from sandbox.pai_opp_mtd_attrib 
            group by 1,2) mt2 on mt2.merchant_uuid = a.merchantuuid and mt2.deal_uuid = a.dealuuid

delete from sandbox.avb_core_mtd_attrib;
/*with 
grt as (
	sel deal_id, grt_l1_description, grt_l2_cat_description, grt_l3_cat_description, max(pds_cat_id) pds 
	from user_edwprod.dim_gbl_deal_lob group by 1,2,3,4),
deal_live as (
	sel deal_uuid, min(load_date) as live_date, max(load_date) as pause_date
	from user_groupondw.active_deals group by 1)*/

insert into sandbox.avb_core_mtd_attrib
sel
	 case 
	 	when coalesce(feature_country, 'US') = 'GB' then 'UK'
	    when coalesce(feature_country, 'US') in ('VI', 'FM', 'PR', 'MH') then 'US'
	    else coalesce(feature_country, 'US')
	 end  as country_code,
	 o1.opportunity_id,
	 o2.deal_uuid,
	 closedate,
	 accountid AS account_id,
	 sda.merchant_seg_at_closed_won AS metal_at_close,
	 o1.id,
	 division,
	 opportunity_name,
	 primary_deal_services,
	 go_live_date,
	 Straight_to_Private_Sale,
	 o1.ownerid,
	 deal_attribute,
	 stagename AS stagename,
	 (case when lower(opportunity_name) like '%dmapi%' or lower(opportunity_name) like '%*g1m*%' then 1 else 0 end) as dmapi_flag
from user_edwprod.sf_opportunity_1 o1
left join dwh_base_sec_view.sf_deal_attribute sda on sda.id = o1.deal_attribute
left join user_edwprod.sf_opportunity_2 o2 on o1.id = o2.id
--left join grt on grt.deal_id = o2.deal_uuid
--left join deal_live ad on ad.deal_uuid = o2.deal_uuid
where 
	o1.opportunity_id is not null
	and lower(stagename) in ('closed lost', 'closed won', 'merchant not interested')
	-- accounts identified as engg test accounts
	and accountid not in ('0013c00001tcgNOAAY', '0013c00001tcjejAAA', '0013c00001sFbx6AAC', '0013c00001tbrwoAAA', '0013c00001sFbwrAAC', '001C0000017G6oTIAS',
	                '001C0000019gQijIAE', '0013c00001tcsUjAAI', '0013c00001tckUuAAI', '0013c00001tckRCAAY', '0013c00001tckUuAAI', '001C0000017EdaQIAS',
	                '0013c00001tckSPAAY', '0013c00001tckTDAAY')
	and coalesce(feature_country, 'US') in ('AE', 'AU', 'BE', 'CA', 'DE', 'ES', 'FR', 'GB', 'IE', 'IT', 'NL', 'PL', 'QC', 'UK', 'US');
select dealid_start_first, count(1) xyz from sandbox.pai_leads_midfunnel group by 1 having xyz > 1;

select 
    convertedaccountid,
    country, 
    max(lead_create_date) lead_create,
    max(campaign_name) campaign_name,
    max(leadsource) leadsource,
    max(campaign_paid_category) campaign_paid_category,
    max(campaign_group_lvl1_2) campaign_group_lvl1_2,
    max(campaign_group_lvl1_1) campaign_group_lvl1_1
from sandbox.pai_leads
where paid_first_last_touch = 1
group by 1,2;

select * from sandbox.pai_leads;

select 
   trunc(deal_launch_date, 'iw') + 6 launch_week, 
   count(distinct account_id) accounts_launched
from 
  sandbox.pai_leads_midfunnel
  where first_launch = 1
  and por_relaunch = 0
  and country in('US', 'CA')
  group by 1
  order by 1 desc;
 
 select 
   trunc(deal_submitdate_first, 'iw') + 6 close_week, 
   count(distinct account_id) accounts_closed
from 
  sandbox.pai_leads_midfunnel
  where first_close = 1
  and por_relaunch = 0
  and country in('US', 'CA')
  group by 1
  order by 1 desc;

select 
    trunc(launch_date, 'iw') + 6 launch_week, 
    count(distinct concat(country, account_id)) account_launched
from 
(SELECT
     a.merchantuuid, 
     a.dealuuid, 
     a.country, 
     a.account_id, 
     mt2.launch_date,
     mt2.launch_order
from sandbox.jk_CB_midfunnel as a
left join
(select 
               merchant_uuid, 
               deal_uuid,
               min(launch_date) launch_date, 
               min(launch_order) launch_order
            from sandbox.pai_opp_mtd_attrib 
            group by 1,2) mt2 on mt2.merchant_uuid = a.merchantuuid and mt2.deal_uuid = a.dealuuid
) as fin
where launch_order = 1
and country in('US', 'CA')
group by 1
order by 1 desc
;
 
select 

delete from sandbox.pai_cb_midfunnel_agg;
insert into sandbox.pai_cb_midfunnel_agg
sel
p.merchantuuid,
p.new_merchant_flag,
p.account_id,
p.metal_group,
p.metal,
p.start_dealid as dealid_start_first,
p.start_date_first deal_start_date,
p.start_timestamp_first as deal_start_timestamp,
p.deal_submitted_first submitted_flag,
p.submit_eventdate as deal_submitdate_first,
p.first_deal_status as deal_submit_status,
p.invalid_data_he as start_invalid_reasons,
p.invalid_reasons_count as start_reasons_count,
p.country,
p.deal_type,
p.campaign_first,
case when f.launched is not null then f.dealuuid else p.prime_deal_id end as dealid_final,
f.launched,
coalesce(f.launch_date,p.launch_date) as deal_launch_date,
f.category_v3 launch_category_v3,
f.subcategory_v3 launch_subcategory_v3,
f.pds as launch_pds,
f.opportunity_name as launch_opportunity_name,
f.deal_permalink,
case when depth > 1 then depth - 1 else 0 end as cloned_number,
mt.por_relaunch,

case when p.submit_eventdate is not null and close_order = 1 then 1 
     when p.submit_eventdate is not null then 0 
     end first_close

case when deal_launch_date is not null and launch_order = 1 then 1 
     when p.submit_eventdate is not null then 0 
     end first_close
     
from abautista.first_deal p
left join abautista.last_deal f on p.prime_deal_id = f.prime_deal_id
left join (select 
               merchant_uuid, 
               deal_uuid,
               max(por_relaunch) por_relaunch,
               min(closedate) closedate, 
               min(close_order) close_order,
            from sandbox.pai_opp_mtd_attrib 
            group by 1,2) mt on p.merchant_uuid = mt.merchant_uuid and p.prime_deal_id = mt.deal_uuid
            
left join (select 
               merchant_uuid, 
               deal_uuid,
               min(date) closedate, 
               min(close_order) close_order,
            from sandbox.pai_opp_mtd_attrib 
            group by 1,2) mt on p.merchant_uuid = mt.merchant_uuid and f.launch_date is not null then  = mt.deal_uuid
;
grant select on sandbox.pai_cb_midfunnel_agg to public;



select * from sandbox.pai_cb_midfunnel_agg sample 5;
---------------------------------------------------------------


select 
    mt.account_id, 
    mt.merchant_uuid, 
    mt.deal_uuid, 
    coalesce(closedate, min_sub_eventdate) close_date,
    ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY launch_date ASC) launch_order
    ROW_NUMBER() OVER (PARTITION BY country_code, account_id ORDER BY closedate, metro_submit_time) close_order
from 
  (select 
    merchant_uuid,
    deal_uuid, 
    min(closedate) closedate,
    min(launch_date) launch_date
  from 
  sandbox.pai_opp_mtd_attrib
  group by 1,2) mt
left join 
(select 
    dealuuid, 
    min(submit_eventdate) min_sub_eventdate
from sandbox.jk_CB_midfunnel
where submit_eventdate is not null
group by 1) cb on mt.deal_uuid  = cb.dealuuid

select deal_uuid, count(1) xyz from sandbox.pai_opp_mtd_attrib group by 1 having xyz >1;

select * from sandbox.pai_opp_mtd_attrib sample 5;

select * from sandbox.jk_cb_midfunnel where prime_flag = 1 sample 5;


delete from sandbox.avb_core_mtd_attrib;
/*with 
grt as (
	sel deal_id, grt_l1_description, grt_l2_cat_description, grt_l3_cat_description, max(pds_cat_id) pds 
	from user_edwprod.dim_gbl_deal_lob group by 1,2,3,4),
deal_live as (
	sel deal_uuid, min(load_date) as live_date, max(load_date) as pause_date
	from user_groupondw.active_deals group by 1)*/
insert into sandbox.avb_core_mtd_attrib
sel
	 case 
	 	when coalesce(feature_country, 'US') = 'GB' then 'UK'
	    when coalesce(feature_country, 'US') in ('VI', 'FM', 'PR', 'MH') then 'US'
	    else coalesce(feature_country, 'US')
	 end  as country_code,
	 o1.opportunity_id,
	 o2.deal_uuid,
	 closedate,
	 accountid AS account_id,
	 sda.merchant_seg_at_closed_won AS metal_at_close,
	 o1.id,
	 division,
	 opportunity_name,
	 primary_deal_services,
	 go_live_date,
	 Straight_to_Private_Sale,
	 o1.ownerid,
	 deal_attribute,
	 stagename AS stagename,
	 (case when lower(opportunity_name) like '%dmapi%' or lower(opportunity_name) like '%*g1m*%' then 1 else 0 end) as dmapi_flag
from user_edwprod.sf_opportunity_1 o1
left join dwh_base_sec_view.sf_deal_attribute sda on sda.id = o1.deal_attribute
left join user_edwprod.sf_opportunity_2 o2 on o1.id = o2.id
--left join grt on grt.deal_id = o2.deal_uuid
--left join deal_live ad on ad.deal_uuid = o2.deal_uuid
where 
	o1.opportunity_id is not null
	and lower(stagename) in ('closed lost', 'closed won', 'merchant not interested')
	-- accounts identified as engg test accounts
	and accountid not in ('0013c00001tcgNOAAY', '0013c00001tcjejAAA', '0013c00001sFbx6AAC', '0013c00001tbrwoAAA', '0013c00001sFbwrAAC', '001C0000017G6oTIAS',
	                '001C0000019gQijIAE', '0013c00001tcsUjAAI', '0013c00001tckUuAAI', '0013c00001tckRCAAY', '0013c00001tckUuAAI', '001C0000017EdaQIAS',
	                '0013c00001tckSPAAY', '0013c00001tckTDAAY')
	and coalesce(feature_country, 'US') in ('AE', 'AU', 'BE', 'CA', 'DE', 'ES', 'FR', 'GB', 'IE', 'IT', 'NL', 'PL', 'QC', 'UK', 'US');



/*
 * 
 * 
 * 
 * 
create multiset volatile table np_temp_lead_mid as (
select 
    f.*, 
    case when max_value = 1 and campaign_paid_category = 1 then paid_direct_first
         when max_value = 0 and campaign_paid_category = 0 then paid_direct_first
         else 0 end paid_first_last_touch
from 
(select 
   a.merchantuuid, 
   a.account_id, 
   a.country,
   a.dealid_start_first,
   a.deal_start_date,
   b.lead_create_date, 
   b.campaign_name, 
   b.LeadSource,
   b.campaign_paid_category,
   b.campaign_group_lvl1_2,
   b.campaign_group_lvl1_1,
   case when dense_rank () over (partition by convertedaccountid, dealid_start_first order by lead_create_date desc) = 1 then 1 else 0 end last_touch,
   case when dense_rank () over (partition by convertedaccountid, dealid_start_first, campaign_paid_category order by lead_create_date desc) = 1 then 1 else 0 end paid_direct_first, 
   max(campaign_paid_category) over (partition by convertedaccountid, dealid_start_first) max_value 
from sandbox.pai_cb_midfunnel_agg as a
left join 
     sandbox.pai_leads as b on a.account_id = b.convertedaccountid and b.lead_create_date <= a.deal_start_date
) as f
) with data on commit preserve rows;



---trying regexreplace

select 
   distinct campaign_name__c, 
   campaign_name__cx
from 
(SELECT 
       convertedaccountid,
       country, 
       createddate,
       campaign_name__c,
       REGEXP_REPLACE(campaign_name__c, '__', '_x_')campaign_name__cx,
       leadsource,
       CASE WHEN LENGTH(campaign_name__c) - LENGTH(REGEXP_REPLACE(campaign_name__c, '_', '')) > 11 THEN 1 ELSE 0 END AS campaign_new_format,
       CASE WHEN LENGTH(campaign_name__c) - LENGTH(REGEXP_REPLACE(campaign_name__c, '_', '')) > 11 THEN STRTOK(LOWER(TRIM(campaign_name__c)), '_', 5) ELSE NULL END AS sem_partner,
       CASE WHEN LENGTH(campaign_name__c) - LENGTH(REGEXP_REPLACE(campaign_name__c, '_', '')) > 11 THEN STRTOK(LOWER(TRIM(campaign_name__c)), '_', 11) ELSE NULL END AS sem_brand
FROM user_groupondw.sf_lead
) as x where campaign_name__cx <> campaign_name__c
;

SELECT 
       convertedaccountid,
       country, 
       createddate,
       campaign_name__c,
       leadsource,
       LENGTH(campaign_name__c),
       LENGTH(REGEXP_REPLACE(campaign_name__c, '_', '')),
       CASE WHEN LENGTH(campaign_name__c) - LENGTH(REGEXP_REPLACE(campaign_name__c, '_', '')) > 11 THEN 1 ELSE 0 END AS campaign_new_format,
       CASE WHEN LENGTH(campaign_name__c) - LENGTH(REGEXP_REPLACE(campaign_name__c, '_', '')) > 11 THEN STRTOK(LOWER(TRIM(campaign_name__c)), '_', 5) ELSE NULL END AS sem_partner,
       CASE WHEN LENGTH(campaign_name__c) - LENGTH(REGEXP_REPLACE(campaign_name__c, '_', '')) > 11 THEN STRTOK(LOWER(TRIM(campaign_name__c)), '_', 11) ELSE NULL END AS sem_brand
FROM user_groupondw.sf_lead
WHERE lower(leadsource) like any ('%mia%','%metro%')
      and CAST(createddate AS DATE) > '2019-01-01'
      and 
      CASE WHEN LENGTH(campaign_name__c) - LENGTH(REGEXP_REPLACE(campaign_name__c, '_', '')) > 11 
      THEN STRTOK(LOWER(TRIM(campaign_name__c)), '_', 11) ELSE NULL END is not null
      sample 5;
     
SELECT 
       convertedaccountid,
       country, 
       createddate,
       campaign_name__c,
       leadsource,
       LENGTH(campaign_name__c),
       TRIM(campaign_name__c)
FROM user_groupondw.sf_lead
WHERE lower(leadsource) like any ('%mia%','%metro%')
      and CAST(createddate AS DATE) > '2019-01-01'
      and TRIM(campaign_name__c) <> campaign_name__c
      sample 5;
---campaign name
---campaign type
---campaign group
select 
   distinct campaign_name__c, 
   campaign_name__cx
from 
(SELECT 
       convertedaccountid,
       country, 
       createddate,
       campaign_name__c,
       REGEXP_REPLACE(campaign_name__c, '__', '_x_')campaign_name__cx,
       leadsource,
       CASE WHEN LENGTH(campaign_name__c) - LENGTH(REGEXP_REPLACE(campaign_name__c, '_', '')) > 11 THEN 1 ELSE 0 END AS campaign_new_format,
       CASE WHEN LENGTH(campaign_name__c) - LENGTH(REGEXP_REPLACE(campaign_name__c, '_', '')) > 11 THEN STRTOK(LOWER(TRIM(campaign_name__c)), '_', 5) ELSE NULL END AS sem_partner,
       CASE WHEN LENGTH(campaign_name__c) - LENGTH(REGEXP_REPLACE(campaign_name__c, '_', '')) > 11 THEN STRTOK(LOWER(TRIM(campaign_name__c)), '_', 11) ELSE NULL END AS sem_brand
FROM user_groupondw.sf_lead
) as x 
where campaign_name__c like all ('TXN1%','%SEMN%') and  campaign_name__c not like '%SEMNB%' and campaign_name__cx <> campaign_name__c
;

select 
   distinct
   a.*
from 
(select 
campaign_name__c,
case when campaign_name__c like 'TXN1%' and campaign_name__c  like '%candaceholyfield_Gen__INS%' then 1 else 0 end old_log, 
case when campaign_name__c like all ('TXN1%','%DIS%', '%FB%')  then 1 else 0 end new_log
FROM user_groupondw.sf_lead
WHERE lower(leadsource) like any ('%mia%','%metro%')
      and CAST(createddate AS DATE) > '2019-01-01') as a 
where old_log = new_log and old_log = 1;

select 
   *
from 
(select 
campaign_name__c,
case when campaign_name__c like 'TXN1%' and campaign_name__c  like '%DIS_GEN__FB%' then 1 else 0 end old_log, 
case when campaign_name__c like all ('TXN1%','%DIS%', '%FB%')  then 1 else 0 end new_log
FROM user_groupondw.sf_lead
WHERE lower(leadsource) like any ('%mia%','%metro%')
      and CAST(createddate AS DATE) > '2019-01-01') as a 
where old_log <> new_log
*/



/*
select
    convertedaccountid as accountid
    , date_format(c2.createddate,'yyyy-MM-dd') as createddate
    , case when country = 'GB' then 'UK' else country end as country_code
    , campaign_name__c
    , campaign_type
    , case
        when campaign_name__c like 'TXN1%' and split(campaign_name__c, '_')[3] = 'SEMB' then 'SEM-Brand'
        when campaign_name__c like 'TXN1%' and split(campaign_name__c, '_')[3] = 'SEMN' then 'SEM-NB'
        when campaign_name__c like 'TXN1%' and split(campaign_name__c, '_')[3] = 'DIS' then 'Display'
        when campaign_name__c like 'TXN1%' and split(campaign_name__c, '_')[3] = 'GPMC' then 'SEM-Brand'
        when campaign_name__c like 'TXN1%' and split(campaign_name__c, '_')[3] = 'SEMNB' then 'SEM-NB'
        when campaign_name__c like 'TXN1%' and split(campaign_name__c, '_')[3] = 'VID' then 'SEM-Brand'
        when campaign_name__c like '%SEMC %' then 'SEM-Brand'
  		when campaign_name__c like 'TXN1%' and campaign_name__c  like '%candaceholyfield_Gen__FB%' then 'Influencer_fb'
  		when campaign_name__c like 'TXN1%' and campaign_name__c  like '%candaceholyfield_Gen__INS%' then 'Influencer_insta'
        when campaign_name__c  like '%always_on%' then 'SEM-Brand'
        -- when campaign_type = 'No Campaign - Referred' then 'Referral'
        when campaign_type in ('groupon-header-app1', 'groupon-header1', 'groupon-header-mweb1', 'groupon-footer') then 'Groupon Header & Footer'
        when campaign_type in ('Google - Brand','Google - Remarketing','Google Brand - Advertising','Google Brand - Business','Google Brand - Contact Merchant','Google Brand - How To Business','Google Brand - Join','Google Brand - Merchant Misc','Google Brand - Number') then 'SEM-Brand'
        -- when campaign_type = 'Direct' then 'Direct'
        -- when campaign_type in ('Facebook F&D','Facebook HBW','Social Retargeting', 'Facebook Silver Lookalike') then 'Display'
        when campaign_type in ('Bing - Competitor','Google - Competitor','Google - Non-Brand','Google NB - Advertise','Google NB - Free Advertising','Google NB - Promote','Google NB - SB-Adv') then 'SEM-NB'
        -- when campaign_type = 'Phone Bank - No Campaign' then 'Phone Bank'
        -- when campaign_type = 'Referral' then 'Referral'
        when campaign_type in ('Living Social','Merchant Blog/SBRC','Merchant Newsletter','Organic','Print') then 'SEO'
        else 'Direct / Referral / Other'
     end campaign_group
    , case when campaign_name__c like 'TXN1%' then 1 else 0 end pd_mm_relaunch_flag
    , split(campaign_name__c, '_')[0] as mktg_txny_version
    , split(campaign_name__c, '_')[1] as mktg_country
    , split(campaign_name__c, '_')[2] as mktg_test_division
    , split(campaign_name__c, '_')[3] as mktg_traffic_source
    , split(campaign_name__c, '_')[4] as mktg_audience
    , split(campaign_name__c, '_')[5] as mktg_sem_type
    , split(campaign_name__c, '_')[6] as mktg_platform
    , split(campaign_name__c, '_')[7] as mktg_creative
    , sfa.merchant_segmentation__c acct_metal
  from (*/
    

--to check if a deal originated from CB. Helps check the latest deal ts when there are clones from GSO
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

--GET ALL DEALS LAUNCHED MAPPED TO SF INFO
--all deals that ever went live
--use it later to check which CLOSED deal LAUNCHED 
--drop TABLE all_deals; 

select deal_uuid, count(1) as xyz from user_edwprod.sf_opportunity_2 group by 1 having xyz > 1;
select id, count(1) xyz from dwh_base_sec_view.Opportunity_1 group by 1 having xyz > 1;
select * from dwh_base_sec_view.sf_opportunity_3 sample 3;

select * from dwh_base_sec_view.sf_opportunity_3;

create multiset volatile table all_deals
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
		opp.createddate as deal_created_ts, ac1.min_date as deal_launch_ts, 
		opp.permalink as deal_permalink, 
		opp.Deal_Strengh,
		op3.hold_at__c    as hold_at, 
		case when cbd.deal_uuid = ac1.deal_uuid then 1 else 0 end as deal_from_CB_flag, 
		ROW_NUMBER() OVER (PARTITION BY pd.merchant_uuid , ac1.min_date, deal_from_CB_flag ORDER BY opp.createddate desc ) as row_nr--trying something, 
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
	)with data unique primary index (deal_uuid,account_id,merchantuuid,row_nr) on commit preserve rows; 
	collect stats on all_deals column (merchantuuid, deal_uuid, deal_launch_ts,account_id,metro_opp_check);

--get all starts and closes from engg table
--grain: 1 row per merchantuuid,dealuuid, and event_type (start and close)
drop TABLE sandbox.PAI_CB_base; 
create multiset table sandbox.PAI_CB_base
as
(select 
    a.event_type,
	case when a.event_type = 'DRAFT_DEAL_CREATION' then 1 else 0 end as deal_start_flag, 
	case when a.event_type in ('METRO_CONTRACT_SIGNIN','G1_METRO_CONTRACT_SIGNED') then 1 else 0 end as deal_close_flag, 
	cast(a.event_date as timestamp) as event_ts, 
	cast(a.event_date as date) as eventdate, 
	a.user_type, 
--get bcookie
	a.merchant_id as merchantuuid, 
	a.deal_id as dealuuid, 
	history_data.JSONExtractValue('$.additionalInfo.places.0.country')  as deal_country, 
	history_data.jsonextractvalue('$.additionalInfo.deal.dealIdentifierAttributes.locale') as deal_locale, 
	history_data.jsonextractvalue('$.additionalInfo.deal.dealIdentifierAttributes.dealType') as deal_type, 
	history_data.jsonextractvalue('$.additionalInfo.holdAt') hold_at,
	history_data.jsonextractvalue('$.additionalInfo.dealStatus') deal_status, 
	opp.permalink as deal_permalink, 
	op2.id as Opportunity_ID, 
	opp.opportunity_name, 
	opp.category_v3, opp.subcategory_v3, opp.primary_deal_services, opp.feature_country, 
	coalesce(opp.accountid,dm.salesforce_account_id,null) as account_id,
	case when lower(AC.MERCHANT_segmentation__c) like any ('%gold%','%platinum%','%silver%') then 'S+' else 'B-' end as Metal_group, 
	AC.MERCHANT_segmentation__c,
	ROW_NUMBER() OVER (PARTITION BY merchantuuid,dealuuid ORDER BY event_ts ) as row_nr, 
	case where history_data.jsonextractvalue('$.additionalInfo.deal.clonedFrom') is null then 1 else 0 end not_cloned_deal
from sb_merchant_experience.history_event as a
left join user_edwprod.sf_opportunity_2 op2 on op2.deal_uuid = a.deal_id -- get opp ID from deal uuid
left join dwh_base_sec_view.Opportunity_1 as opp on opp.id = op2.id and lower(opp.opportunity_name) like any ('dmapi%','%*g1m*%') --identifiers for a metro deal
left join dwh_base_sec_view.sf_opportunity_3 as op3 on opp.id = op3.id--to get orevetting and post vetting
left join ( select 
             merchant_uuid, 
             salesforce_account_id  
             from user_edwprod.dim_merchant 
             qualify (ROW_NUMBER() OVER (PARTITION BY salesforce_account_id ORDER BY updated_at )) = 1
           ) as dm on dm.merchant_uuid = a.merchant_id--get merchant_uuid
left join user_groupondw.sf_account as ac on ac.id = account_id
where eventdate >= '2022-01-01'
and a.event_type in ('METRO_CONTRACT_SIGNIN','DRAFT_DEAL_CREATION','G1_METRO_CONTRACT_SIGNED')
--and a.deal_id = '5760f310-2713-4f0d-a649-9d67a10473ae'
qualify (ROW_NUMBER() OVER (PARTITION BY merchantuuid,dealuuid,a.event_type ORDER BY event_ts )) = 1
)
with data unique primary index (merchantuuid, dealuuid,event_type);
collect stats on sandbox.PAI_CB_base column(merchantuuid,dealuuid,row_nr);

history_data.jsonextractvalue('$.additionalInfo.deal.clonedFrom') is null 




--drop TABLE table2; 
create MULTISET volatile table table2 as sandbox.PAI_CB_base with data on commit preserve rows;  
collect stats on table2 column(merchantuuid,dealuuid,row_nr);

--select * from sandbox.PAI_CB_base where dealuuid = '9e26fbbe-5c16-4402-b9e4-48ecb50d8e7d'
----MM information for leads that started a campiagn, submitted a campaign, if there were clone and launched a campaign

drop TABLE sandbox.pai_CB_midfunnel;
create MULTISET TABLE sandbox.pai_CB_midfunnel
as
(
select 
	a.event_ts,
	a.event_type,
	a.eventdate,
	a.deal_start_flag,
	a.dealuuid,
	a.Opportunity_ID,  
	a.deal_permalink, 
	a.opportunity_name,  
	a.deal_status,                   
	a.deal_type, 
	a.deal_country,                    
	a.hold_at,                       
	a.Merchant_Segmentation__c,
	a.merchantuuid,   
	a.account_id,
	--ADD MERCHANT NAME
	a.Metal_group,                   
	a.Category_v3,                    
	a.Subcategory_v3,   
	a.Primary_Deal_Services,   
	a.user_type,                     
	a.row_nr, 
	---min(case when event_type in ('METRO_CONTRACT_SIGNIN','G1_METRO_CONTRACT_SIGNED') then event_ts end) deal_close_ts --- this wont work coz no flag for cloned
	/*(	select 
				b.event_ts
		from table2 As b
		where a.merchantuuid = b.merchantuuid 
		and a.dealuuid = b.dealuuid
		and b.row_nr = a.row_nr + 1
	) as deal_close_ts, ----NOT needed IF we remove the draft clause AT the end
	cast(deal_close_ts as date) as deal_closed_date, 
	(	select 
			b.event_type
		from table2 As b
		where a.merchantuuid = b.merchantuuid 
		and a.dealuuid = b.dealuuid
		and b.row_nr = a.row_nr + 1
	) as next_event, 
	case when deal_close_ts is not null then 1 else 0 end as close_flag, */
--identify launches AFTER lead created timestamp
	(
	select 
		count(distinct(ad1.opportunity_id)) 
	from all_deals as ad1
	where a.merchantuuid = ad1.merchantuuid 
	and ad1.metro_opp_check = 'Metro' 
	and ad1.deal_launch_ts < a.event_ts
	) as Metro_Deals, ------Gets ALL metro opportunity launched BEFORE this one AT account LEVEL? 
	(select 
		count(distinct(ad2.opportunity_id)) 
	from all_deals as ad2
	where a.merchantuuid = ad2.merchantuuid 
	and ad2.metro_opp_check = 'Non-metro'
	and ad2.deal_launch_ts < a.event_ts
	) as Non_Metro_Deals, ------Gets ALL non - metro opportunity launched BEFORE this one AT account LEVEL?
	case when Metro_Deals = 0  AND Non_Metro_Deals = 0  then 1 else 0 end as new_merchant_flag,
	--financials
	GB_usd, 
	refunds_usd,
	nob_usd,
	units_sold,  
	refunds_qty,
	(select 
			min(ad.deal_launch_ts)
		from all_deals as ad
		where a.merchantuuid = ad.merchantuuid 
		and ad.metro_opp_check = 'Metro' 
		and ad.deal_from_CB_flag = 0--ensures to look for any deal that launched as a metro deal BUT isn't from CB i.e. created by cloning 
		and ad.deal_launch_ts > a.event_ts--launch date after CB activity
	) as next_deal_launch_ts, 
	(select 
			ad3.opportunity_id
		from all_deals as ad3
		where a.merchantuuid = ad3.merchantuuid
		and ad3.metro_opp_check = 'Metro' 
		and ad3.deal_from_CB_flag = 0--ensures to look for any deal that launched as a metro deal BUT isn't from CB i.e. created by cloning 
		and next_deal_launch_ts = ad3.deal_launch_ts
		and ad3.deal_launch_ts > a.event_ts
		and ad3.row_nr = 1--if there are 2 deals with exact sam launch ts, first one is picked
	) as next_launched_opp_id,
	ad2.deal_launch_ts as deal_launch_ts,--this is the match for the deal created via CB and actual deal launched
--if there's no direct match then look for next deal launched which IS NOT PRESENT in CB
	case when a.opportunity_id is NOT null then  coalesce(ad2.deal_launch_ts, next_deal_launch_ts,null) else null end as launch_date, 
	case when launch_date is not null then 1 else 0 end as launch_flag
from sandbox.PAI_CB_base As a ----cloned deals + actual non cloned deals - no flag to identify cloned deals
left join all_deals as ad2 on ad2.deal_uuid = a.dealuuid and ad2.metro_opp_check = 'Metro' --checks launched deals
left join 
	(
	select 
		deal_id, 
		sum(gross_bookings_usd) as GB_usd, 
		sum(refunds_amt_usd) as refunds_usd, 
		sum(nob_usd) as nob_usd, 
		sum(transactions) as units_sold, 
		sum(refunds_qty) as refunds_qty
	from user_edwprod.agg_gbl_traffic_fin_deal as fin 
	join sandbox.PAI_CB_base as a2 on a2.dealuuid = fin.deal_id and a2.event_type in ('METRO_CONTRACT_SIGNIN','G1_METRO_CONTRACT_SIGNED')--identifier for closes and JOIN tested for dupes
	group by 1
	) as f on f.deal_id = a.dealuuid
---where a.event_type = 'DRAFT_DEAL_CREATION'
--and a.dealuuid = '9e26fbbe-5c16-4402-b9e4-48ecb50d8e7d'
)
with data unique primary index (merchantuuid,dealuuid); 




/*
------testing
select 
	a.event_type,
	a.dealuuid,
	a.merchantuuid,   
	a.account_id,
	--ADD MERCHANT NAME               
	a.row_nr, 
	(	select 
				min(b.event_ts)
		from table2 As b
		where a.merchantuuid = b.merchantuuid 
		and b.row_nr = a.row_nr + 1
	) as deal_close_ts
from sandbox.PAI_CB_base As a
order by 3, 2

------additional case

case when new_merchant_flag = 1 
then 
(
select max(l.lead_created_ts)--finds the latest lead timestamp created by the merchant
from leads as l
where l.convertedaccountid = a.account_id
and a.event_ts > l.lead_created_ts
and l.lead_category = 'Paid'--preference given to Paid
) else null end  as lead_created_timestamp,

--get campaign name using previous field i.e lead_created_timestamp
(select l2.leadsource
from leads as l2
where l2.convertedaccountid = a.account_id
and a.event_ts > l2.lead_created_ts
and l2.lead_category = 'Paid'--preference given to Paid
and lead_created_timestamp = l2.lead_created_ts
) as SF_leadsource,

--get campaign name using previoius field
(select l3.sf_lead_campaign_name
from sandbox.pai_sf_mm_leads as l3
where l3.convertedaccountid = a.account_id
and a.event_ts > l3.lead_created_ts
and l3.lead_category = 'Paid'--preference given to Paid
and lead_created_timestamp = l3.lead_created_ts
) as SF_campaign_name,

case when SF_campaign_name like 'TXN1%' then 'Paid' else 'Direct' end as lead_category, --is an identifier for all paid campaigns
case when SF_campaign_name like all ('TXN1%','%DIS%') then 'Display'
when SF_campaign_name like all ('TXN1%','%SEMN%') then 'SEM Non Brand'
when SF_campaign_name like all ('TXN1%','%SEMB%') then 'SEM Brand'
when SF_campaign_name like all ('TXN1%','%EVNT%') then 'Event Testing'
when SF_campaign_name like all ('TXN1%','%GPMC%') then 'Google Performance Max'
when SF_campaign_name like all ('TXN1%','%VID%') then 'Connected TV'
when SF_campaign_name like all ('TXN1%','%_CANDACE%') then 'Candace Holyfield Referral'
end as Paid_lead_source*/

select * From sb_merchant_experience.history_event sample 100; 





------------------------------------------------------------------------------------------------------------------------JACKS CODE


select
            substr(invalid_data_front_trim, 1, len_invalid_data_front_trim - 3) invalid_data__c
            , holdAt
            , deal_id
          from (
            select
              substr(cast(history_data.jsonextract('$.additionalInfo.invalidData') as varchar(75)),4) invalid_data_front_trim
              , length(invalid_data_front_trim) len_invalid_data_front_trim
              , history_data.jsonextractvalue('$.additionalInfo.holdAt') holdAt
              , deal_id
            from sb_merchant_experience.history_event a
            where event_type in ('METRO_CONTRACT_SIGNIN')
            and holdAt in ('Prevetting')
            ) a;
           
select distinct stagename from sandbox.jc_merchant_mtd_attrib;

            
------------------------all deals closed with other information about why it went into pre vetting
create volatile table jc_deal_invalid_data_temp as (
    select
      c.deal_uuid
      --, FROM_BYTES(cast(from_bytes(TO_BYTES('3B', 'base16'), 'ascii') as varchar(1)), 'ascii') as cast(from_bytes(TO_BYTES('3B', 'base16'), 'ascii') as varchar(1))
      , case 
	      when pv.deal_id is not null and b.deal_id is null and coalesce(o3.invalid_data__c, idt.invalid_data__c) <> pv.invalid_data__c then concat(concat(coalesce(o3.invalid_data__c, idt.invalid_data__c), cast(from_bytes(TO_BYTES('3B', 'base16'), 'ascii') as varchar(1))),pv.invalid_data__c)
          when pv.deal_id is not null and b.deal_id is null then pv.invalid_data__c
          when pv.deal_id is not null and b.deal_id is not null and coalesce(o3.invalid_data__c, idt.invalid_data__c) is null then pv.invalid_data__c
          when pv.deal_id is not null and b.deal_id is not null then concat(concat(coalesce(o3.invalid_data__c, idt.invalid_data__c), cast(from_bytes(TO_BYTES('3B', 'base16'), 'ascii') as varchar(1))),pv.invalid_data__c)
          when pv.deal_id is null then coalesce(o3.invalid_data__c, idt.invalid_data__c)
        else null end invalid_data__c
    from sandbox.jc_merchant_mtd_attrib c
    join dwh_base_sec_view.opportunity_1 d on c.opportunity_id = d.opportunity_id
    join dwh_base_sec_view.opportunity_2 o2 on d.id = o2.id
    join dwh_base_sec_view.sf_opportunity_3 o3 on o3.id = d.id
    left join sandbox.jc_invalid_data_test idt on idt.opportunity_id = c.opportunity_id
    left join (
        select
          oreplace(oreplace(invalid_data__c, '"',''),',',FROM_BYTES(TO_BYTES('3B', 'base16'), 'ascii')) invalid_data__c
          , holdAt
          , deal_id
        from (
          select
            substr(invalid_data_front_trim, 1, len_invalid_data_front_trim - 3) invalid_data__c
            , holdAt
            , deal_id
          from (
            select
              substr(cast(history_data.jsonextract('$.additionalInfo.invalidData') as varchar(75)),4) invalid_data_front_trim
              , length(invalid_data_front_trim) len_invalid_data_front_trim
              , history_data.jsonextractvalue('$.additionalInfo.holdAt') holdAt
              , deal_id
            from sb_merchant_experience.history_event a
            where event_type in ('METRO_CONTRACT_SIGNIN')
            and holdAt in ('Prevetting')
            ) a
        ) a
      ) pv on pv.deal_id = c.deal_uuid
    left join (
        select deal_id
        from sb_merchant_experience.history_event
        where event_type in ('PRE_VETTING_UPDATE')
        ) b on b.deal_id = c.deal_uuid
    where c.feature_country in ('US', 'UK', 'GB', 'AU', 'PL', 'FR', 'IT', 'AE', 'DE', 'CA', 'ES')
      and c.grt_l1_cat_name = 'L1 - Local'
      and c.stagename in ('Closed Won', 'Closed Lost', 'Merchant Not Interested')
      and c.por_relaunch = 0
      and c.dmapi_flag = 1
  ) with data primary index(deal_uuid) on commit preserve rows
;

SELECT 
       a.deal_id as parent_deal_id,
       a.*
FROM sb_merchant_experience.history_event as a
where event_type = 'POST_VETTING_STARTED'
sample 5;



----all cloned deals also seem to have the same closed status
----pulls all the deal_ids for those cloned only so a parent wont be seen as a child anywhere. 

create volatile table jc_postvetting_relationships_v as (
    select
      a.deal_id as parent_deal_id
      , o1.opportunity_id as parent_opp_id
      , ivd.invalid_data__c as parent_invalid_data
      , mtd.deal_uuid as child_deal_id
      , mtd.opportunity_id as child_opp_id
      , ivd2.invalid_data__c as child_invalid_data
    from sb_merchant_experience.history_event a
    join user_edwprod.sf_opportunity_2 o2 on a.deal_id = o2.deal_uuid
    join user_edwprod.sf_opportunity_1 o1 on o2.id = o1.id
    join jc_deal_invalid_data_temp ivd on ivd.deal_uuid = a.deal_id
    -- join to child
    join user_edwprod.sf_opportunity_1 o12 on o12.cloned_from = o1.id
    join sandbox.jc_merchant_mtd_attrib mtd on o12.opportunity_id = mtd.opportunity_id
    join jc_deal_invalid_data_temp ivd2 on ivd2.deal_uuid = mtd.deal_uuid
    where event_type = 'POST_VETTING_STARTED' and a.event_date < current_date
  ) with data no primary index on commit preserve rows
;

drop table sandbox.jc_postvetting_agg_v;
create table sandbox.jc_postvetting_agg_v (
	parent_deal_id varchar(36)
	, child_deal_id varchar(36)
	, depth smallint
	, invalid_data__c varchar(150)
);

insert into sandbox.jc_postvetting_agg_v
with recursive cte (parent_deal_id, child_deal_id, depth, invalid_data__c)
  as
  (select 
  	     parent_deal_id, 
  	      child_deal_id, 
  	      cast(1 as int) as depth, 
  	      concat(concat(parent_invalid_data, cast(from_bytes(TO_BYTES('3B', 'base16'), 'ascii') as varchar(1))),child_invalid_data) as invalid_data__c
  	from jc_postvetting_relationships_v a
  	 join (
        select deal_id
        from sb_merchant_experience.history_event
        where event_type = 'DRAFT_DEAL_CREATION'
          and history_data.jsonextractvalue('$.additionalInfo.deal.clonedFrom') is null
          and event_date < current_date
      ) dupe
      on dupe.deal_id = a.parent_deal_id
  	union all
  	select
      cte.parent_deal_id
      , b.child_deal_id
      , cast(cte.depth + 1 as int) as depth
      , concat(concat(cte.invalid_data__c, cast(from_bytes(TO_BYTES('3B', 'base16'), 'ascii') as varchar(1))),b.child_invalid_data) invalid_data__c
  	from cte
  	join jc_postvetting_relationships_v b
  		on cte.child_deal_id = b.parent_deal_id and b.child_deal_id is not null
  )
  select * from cte
;



create volatile table jc_postvetting_dim_v as 
(  select
    	a.*
      , b.invalid_data__c
    	, b.child_deal_id curr_child_deal_id
    from 
    (select
      	coalesce(b.parent_deal_id, a.deal_id) deal_uuid
      	, max(case when b.parent_deal_id is not null then 1 else 0 end) postv_submit_flag
      	, max(depth) num_postv_submits
      	, max(case when mtd.deal_uuid is not null then 1 else 0 end) postv_launch_flag
      	, max(mtd.deal_uuid) postv_launched_deal
      from sb_merchant_experience.history_event a
      left join jc_postvetting_agg_v b
        on a.deal_id = b.parent_deal_id
          or a.deal_id = b.child_deal_id
      left join sandbox.jc_merchant_mtd_attrib mtd
      	on (mtd.deal_uuid = b.parent_deal_id or mtd.deal_uuid = b.child_deal_id)
      		and mtd.launch_date is not null
      where event_type = 'POST_VETTING_STARTED' and event_date < current_date
    	group by 1
    ) a
    left join jc_postvetting_agg_v b on a.deal_uuid = b.parent_deal_id and a.num_postv_submits = b.depth
    group by 1,2,3,4,5,6,7
  ) with data primary index(deal_uuid) on commit preserve rows
;



---- CHECKSUM 1 --
---- SHOULD RETURN ALL NULL IN COLUMN B
---- select
--   c.deal_uuid, dupe.deal_id
--   from jc_postvetting_dim_v c
--   left join (
--       select *
--       from sb_merchant_experience.history_event
--       where event_type = 'DRAFT_DEAL_CREATION'
--         and history_data.jsonextractvalue('$.additionalInfo.deal.clonedFrom') is not null
--         --and deal_id = '31078fb5-15a0-45e8-8892-61d6ceb8324f'
--     ) dupe
--     on dupe.deal_id = c.deal_uuid
--     order by 2 desc;



create volatile table jc_merchant_mtd_attrib_dedupe as (
    select a.*
    from sandbox.jc_merchant_mtd_attrib a
    left join (
        select deal_id
        from sb_merchant_experience.history_event
        where event_type = 'DRAFT_DEAL_CREATION'
          and history_data.jsonextractvalue('$.additionalInfo.deal.clonedFrom') is not null
      ) dupe
      on dupe.deal_id = a.deal_uuid
    where a.feature_country in ('US', 'UK', 'GB', 'AU', 'PL', 'FR', 'IT', 'AE', 'DE', 'CA', 'ES')
      and a.grt_l1_cat_name = 'L1 - Local'
      and a.stagename in ('Closed Won', 'Closed Lost', 'Merchant Not Interested')
      and a.por_relaunch = 0
      and a.dmapi_flag = 1
      and dupe.deal_id is null
  ) with data primary index(deal_uuid) on commit preserve rows
;

create volatile table jc_deal_invalid_data as (
    select
      a.deal_uuid
      , coalesce(b.invalid_data__c, c.invalid_data__c) invalid_data__c
    from jc_merchant_mtd_attrib_dedupe a
    left join jc_postvetting_dim_v b on a.deal_uuid = b.deal_uuid
    left join jc_deal_invalid_data_temp c on a.deal_uuid = c.deal_uuid
  ) with data primary index (deal_uuid) on commit preserve rows
;

create volatile table jc_deal_fix_flagging as (
    select
      a.deal_uuid
      , case when
            ( -- all the FIX reasons in invalid_data__c
              lower(id.invalid_data__c) like any ('%url%', '%insurance%', '%license%'
                , '%tin%', '%vat%', '%vat/registration number%', '%deal details%', '%fine print%', '%no price verification%', '%incomplete merchant address%'
                , '%missing redemption method%', '%incorrect pricing%', '%incorrect expiration%', '%legal%'
                , '%incorrect Discount%', '%provide required info%')
            )
          or
            ( -- PDS FIX
              a.close_date > '2021-04-02'
                and lower(id.invalid_data__c) like '%pds%'
                and lower(id.invalid_data__c) not like '%self service pds%'
            )
          or
            ( -- legacy FIX logic
              lower(d.City_Planning_Private_Notes_c) like '%fix%'
              and (lower(sfo.accountcoordinator_notes__c) not like '%goods%' and lower(sfo.accountcoordinator_notes__c) not like '%dnc%')
            )
        then 1 else 0 end fix_required
      , case when pv.deal_id is not null then 1 else 0 end pre_vetting_flag
      , case when pvs.deal_id is not null then 1 else 0 end pre_vetting_submit_flag
      , case when pvd.deal_uuid is not null then 1 else 0 end post_vetting_flag
      , case when pvd.curr_child_deal_id is not null then 1 else 0 end post_vetting_submit_flag
    from jc_merchant_mtd_attrib_dedupe a
    left join dwh_base_sec_view.opportunity_1 d
      on a.opportunity_id = d.opportunity_id
    left join user_edwprod.sf_opportunity_2 sfo
      on sfo.id = d.id
    left join jc_deal_invalid_data id on a.deal_uuid = id.deal_uuid
    left join jc_postvetting_dim_v pvd on pvd.deal_uuid = a.deal_uuid
    left join (
        select
          substr(cast(history_data.jsonextract('$.additionalInfo.invalidData') as varchar(75)),4) invalid_data_front_trim
          , length(invalid_data_front_trim) len_invalid_data_front_trim
          , history_data.jsonextractvalue('$.additionalInfo.holdAt') holdAt
          , deal_id
        from sb_merchant_experience.history_event a
        where event_type in ('METRO_CONTRACT_SIGNIN')
        and holdAt in ('Prevetting')
      ) pv on a.deal_uuid = pv.deal_id
    left join (
        select deal_id
        from sb_merchant_experience.history_event
        where event_type in ('PRE_VETTING_UPDATE')
      ) pvs on a.deal_uuid = pvs.deal_id
  ) with data primary index(deal_uuid) on commit preserve rows
;

-- create table first:
--   https://github.groupondev.com/hanson/Revenue-Management-and-Analytics/blob/master/supply/deal_structure_table_updates/03.%20recreate_final_deal_restriction_table.sql
COLLECT STATISTICS COLUMN (deal_uuid), COLUMN (opportunity_id), COLUMN (accountid) ON jc_merchant_mtd_attrib_dedupe;
-- last try: less volatile tables, more permanent + delete
-- query less of active_deals

select * from sandbox.jc_ss_deal_status_v;

drop table sandbox.jc_ss_deal_status_v;
create table sandbox.jc_ss_deal_status_v as (
    select
      -- DEAL STATUS LOGIC
      case
          -- LAUNCH
          when ad.launch_date is not null or pvd.postv_launch_flag = 1 then 'Launched'
          -- MERCHANT DELETED
          when d.deal_strengh = 'Rejected' and d.stagename = 'Merchant Not Interested' then 'Merchant Deleted'
          when d.deal_strengh = 'Rejected' and d.stagename = 'Closed Lost' and hed.deal_id is not null then 'Merchant Deleted'
          -- FAILED FIX
          when d.deal_strengh = 'Rejected'
            and d.stagename in ('Closed Lost', 'Closed Won')
            and fix_required = 1
            and post_vetting_flag = 0
          then 'Merchant Failed to FIX'
          -- DNC
          when d.deal_strengh = 'Rejected' and d.stagename = 'Closed Lost' and fix_required = 0 then 'Deal Rejected: DNC'
          -- AWAITING LAUNCH
          when d.deal_strengh = 'A Sure Thing' and a.go_live_date >= current_date then 'Awaiting Launch'
          -- MISSED LAUNCH
          when d.deal_strengh = 'A Sure Thing' then 'Missed Launch'
          -- PRE-VETTING FIX
          when d.deal_strengh in ('HOLD', 'Rejected') and pre_vetting_flag = 1 and pre_vetting_submit_flag = 0 then 'Awaiting FIX: Pre-Vetting'
          -- POST-VETTING FIX
          when d.deal_strengh in ('HOLD', 'Rejected') and post_vetting_flag = 1 then 'Awaiting FIX: Post-Vetting'
          -- AWAITING FIX
          when d.deal_strengh in ('HOLD', 'Rejected') and fix_required = 1 then 'Awaiting FIX: GSO/Other'
          -- DUPE ACCT
          when sfa.unmergeable_accts > 0 then 'Dupe Account'
          -- VETTING
          when d.deal_strengh = 'Rep Getting Info' then 'Vetting'
          -- FALLBACK TO ERROR
          else 'Other'
        end deal_flag
      ----
      , td_week_end(a.close_date) week_end
      , a.close_date
      , a.go_live_date
      , a.opportunity_id
      , sfo.accountcoordinator_notes__c
      , a.rev_management_metal_at_close
      , d.stagename
      , d.deal_strengh
      , ivd.invalid_data__c
      -- dnc reason
      , case
        -- In Home
          when deal_flag = 'Deal Rejected: DNC' and ivd.invalid_data__c like '%Ineligible New Merchant - In home service%' then 'In-Home'
          when deal_flag = 'Deal Rejected: DNC' and sfo.accountcoordinator_notes__c like '%In-home%' then 'In-Home'
          when deal_flag = 'Deal Rejected: DNC' and sfo.accountcoordinator_notes__c like '%In Home%' then 'In-Home'
          when deal_flag = 'Deal Rejected: DNC' and sfo.accountcoordinator_notes__c like '%In-Home%' then 'In-Home'
        -- Goods
          when deal_flag = 'Deal Rejected: DNC' and ivd.invalid_data__c like '%Ineligible New Merchant - Goods%' then 'Goods'
          when deal_flag = 'Deal Rejected: DNC' and sfo.accountcoordinator_notes__c like '%Goods%' then 'Goods'
        -- Legal
          when deal_flag = 'Deal Rejected: DNC' and ivd.invalid_data__c like '%Legal%' then 'Legal'
          when deal_flag = 'Deal Rejected: DNC' and sfo.accountcoordinator_notes__c like '%Legal%' then 'Legal'
        -- Non-SS Eligible PDS: implemented as of Apr-2
          when deal_flag = 'Deal Rejected: DNC' and a.close_date > '2021-04-02' and ivd.invalid_data__c like 'Self Service PDS' then 'Non-SS PDS'
        -- DNC (Do Not Close for Groupon - including reps): implemented as of Apr-2
          when deal_flag = 'Deal Rejected: DNC' and a.close_date > '2021-04-02' and ivd.invalid_data__c like '%DNC%' then 'DNC PDS'
        -- PDS
        -- note that this DNC reason captures ALL dnc pds issues prior to Apr-2
          when deal_flag = 'Deal Rejected: DNC' and ivd.invalid_data__c like '%PDS%' then 'PDS'
          when deal_flag = 'Deal Rejected: DNC' and sfo.accountcoordinator_notes__c like '%Rejected - DNC%' then 'PDS'
          when deal_flag = 'Deal Rejected: DNC' and sfo.accountcoordinator_notes__c like '%Incorrect PDS%' then 'PDS'
          when deal_flag = 'Deal Rejected: DNC' and sfo.accountcoordinator_notes__c like '%PDS%' then 'PDS'
        -- Kam deal?
          when deal_flag = 'Deal Rejected: DNC' and sfo.accountcoordinator_notes__c like '%Kam%' then 'KAM Deal'
        -- No go
          when deal_flag = 'Deal Rejected: DNC' and sfo.accountcoordinator_notes__c like '%NO GO%' then 'No-Go'
          when deal_flag = 'Deal Rejected: DNC' and lower(sfo.accountcoordinator_notes__c) like '%no go%' then 'No-Go'
          when deal_flag = 'Deal Rejected: DNC' and lower(sfo.accountcoordinator_notes__c) like '%no-go%' then 'No-Go'
        -- G1
          when deal_flag = 'Deal Rejected: DNC' and ivd.invalid_data__c like '%Ineligible New Merchant - G1%' then 'G1'
          when deal_flag = 'Deal Rejected: DNC' and lower(sfo.accountcoordinator_notes__c) like '%g1%' then 'G1'
          when deal_flag = 'Deal Rejected: DNC' and lower(sfo.accountcoordinator_notes__c) like '%incorrect expiration%' then 'G1'
        -- National
          when deal_flag = 'Deal Rejected: DNC' and ivd.invalid_data__c like '%National%' then 'National'
          when deal_flag = 'Deal Rejected: DNC' and sfo.accountcoordinator_notes__c like '%National%' then 'National'
        -- Travel
          when deal_flag = 'Deal Rejected: DNC' and ivd.invalid_data__c like '%Travel%' then 'Travel'
          when deal_flag = 'Deal Rejected: DNC' and sfo.accountcoordinator_notes__c like '%Travel%' then 'Travel'
        -- Live / Event
          when deal_flag = 'Deal Rejected: DNC' and ivd.invalid_data__c like '%Live/Event%' then 'Live/Event'
          when deal_flag = 'Deal Rejected: DNC' and sfo.accountcoordinator_notes__c like '%Live%' then 'Live/Event'
        -- Ineligible new merchant
        -- note that this a catch all for any other ineligible new merchant flags before taxonomy change on Apr-2
          when deal_flag = 'Deal Rejected: DNC' and ivd.invalid_data__c like '%Ineligible New Merchant%' then 'Ineligible New Merchant'
          when deal_flag = 'Deal Rejected: DNC' and sfo.accountcoordinator_notes__c like '%New Merchant Check Failed%' then 'Ineligible New Merchant'
        -- Pricing / Discount / Deal Set-Up
          when deal_flag = 'Deal Rejected: DNC' and lower(sfo.accountcoordinator_notes__c) like '%incorrect discount%' then 'Incorrect Discount/Pricing'
          when deal_flag = 'Deal Rejected: DNC' and lower(sfo.accountcoordinator_notes__c) like '%incorrect pricing%' then 'Incorrect Discount/Pricing'
        -- COVID
          when deal_flag = 'Deal Rejected: DNC' and lower(ivd.invalid_data__c) like '%covid%' then 'COVID'
          when deal_flag = 'Deal Rejected: DNC' and sfo.accountcoordinator_notes__c like '%COVID%' then 'COVID'
        else 'Other' end dnc_reason
      ----
      , a.pds
      , grt.grt_l3_cat_name
      , a.feature_country
      , a.mtd_attribution
      , ad.launch_date
      , dm.permalink
      , case when adc.launch_date is null or adc.launch_date >= a.close_date then 'Pre-Feature' else 'Post-Feature' end pre_v_post_feature
      , a.deal_uuid
      , sfo3.hold_at__c
      , chi.opportunity_id curr_child_opp_id
      , count(distinct a.opportunity_id) closes
    from jc_merchant_mtd_attrib_dedupe a
    left join jc_deal_fix_flagging f on a.deal_uuid = f.deal_uuid
    left join jc_deal_invalid_data ivd on a.deal_uuid = ivd.deal_uuid
    left join jc_postvetting_dim_v pvd on pvd.deal_uuid = a.deal_uuid
    left join dwh_base_sec_view.opportunity_1 d on a.opportunity_id = d.opportunity_id
    left join user_edwprod.sf_opportunity_2 sfo on sfo.id = d.id
    left join dwh_base_sec_view.sf_opportunity_3 sfo3 on sfo3.id = sfo.id
    left join sandbox.jc_merchant_mtd_attrib chi on chi.deal_uuid = pvd.curr_child_deal_id
    left join user_dw.v_dim_pds_grt_map grt on grt.pds_cat_name = a.pds
    left join user_groupondw.dim_merchant dm on dm.salesforce_account_id = a.accountid
    left join (select distinct deal_id from sb_merchant_experience.history_event where event_type = 'DELETED_BY_MERCHANT') hed
      on hed.deal_id = a.deal_uuid
    left join (select deal_uuid, min(load_date) launch_date from user_groupondw.active_deals group by 1) ad
      on ad.deal_uuid = a.deal_uuid
    left join (
        select account_id_18, sum(case when Parent_Relationship = 'Duplicate: Unmergeable Child' then 1 else 0 end) unmergeable_accts
        from user_groupondw.sf_account
        group by 1
      ) sfa
      on sfa.account_id_18 = a.accountid
    left join (
        select accountid, min(load_date) launch_date
        from user_groupondw.active_deals ad
        join sandbox.jc_merchant_mtd_attrib mtd
          on ad.deal_uuid = mtd.deal_uuid
        group by 1
      ) adc
      on adc.accountid = a.accountid
    where a.feature_country in ('US', 'UK', 'GB', 'AU', 'PL', 'FR', 'IT', 'AE', 'DE', 'CA', 'ES')
      and a.grt_l1_cat_name = 'L1 - Local'
      and a.stagename in ('Closed Won', 'Closed Lost', 'Merchant Not Interested')
      and a.por_relaunch = 0
      and a.dmapi_flag = 1
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
  ) with data primary index(opportunity_id)
;

drop table sandbox.jc_ss_deal_status;
create table sandbox.jc_ss_deal_status as (
    select *
    from sandbox.jc_ss_deal_status_v
    where (feature_country in ('US', 'CA') and close_date >= current_date - interval '30' day)
      or (feature_country <> 'US' and close_date >= current_date - interval '90' day)
  ) with data primary index(opportunity_id)
;

grant select on sandbox.jc_ss_deal_status to public;

drop table sandbox.jc_ss_deal_status_reporting;
create table sandbox.jc_ss_deal_status_reporting as (
    select
      a.deal_flag
      , a.week_end
      , case when a.feature_country in ('US', 'CA') then 'NAM' else 'INTL' end region
      , a.feature_country
      , case when lower(c.rev_management_metal_at_close) in ('silver', 'sliver', 'gold', 'platinum') then 'S+' else 'B-' end metal_group
      , pre_v_post_feature
      , a.dnc_reason
      , c.vertical
    	, count(distinct a.opportunity_id) closes
    from sandbox.jc_ss_deal_status_v a
    join sandbox.jc_merchant_mtd_attrib c
      on a.opportunity_id = c.opportunity_id
    where a.week_end < current_date
    group by 1,2,3,4,5,6,7,8
  ) with data primary index(week_end)
;

grant select on sandbox.jc_ss_deal_status_reporting to public;

create volatile table jc_ss_fix_opp_v as (
    select distinct b.opportunity_id, trim(b.token) as fix_reason
    from (
        select *
        from table (
          strtok_split_to_table(sandbox.jc_ss_deal_status_v.opportunity_id, sandbox.jc_ss_deal_status_v.invalid_data__c, FROM_BYTES(TO_BYTES('3B', 'base16'), 'ascii'))
          returns (
            opportunity_id varchar(32) character set unicode
            , tokennum integer
            , token varchar(255) character set unicode
          )
        ) d
      ) b
      where token like any ('%URL%', '%Insurance%', '%Licenses%', '%TIN%', '%VAT%', '%No Price verification%', '%Incomplete Merchant Address%'
          , '%Missing Redemption method%', '%Incorrect Pricing%', '%Incorrect Expiration%', '%Incorrect Discount%', '%provide required info%', '%PDS%', '%Details%'
          , '%Fine Print%', '%Legal%'
      )
      and lower(token) not like '%self service pds%'
  ) with data primary index(opportunity_id) on commit preserve rows
;

drop table sandbox.jc_ss_deal_fix_reasons;
create table sandbox.jc_ss_deal_fix_reasons as (
    select
      a.deal_flag
      , a.week_end
      , case when a.feature_country in ('US', 'CA') then 'NAM' else 'INTL' end region
      , a.feature_country
      , case when lower(c.rev_management_metal_at_close) in ('silver', 'sliver', 'gold', 'platinum') then 'S+' else 'B-' end metal_group
      , pre_v_post_feature
      , case
          when f.fix_reason like 'Incomplete Merchant Address%' or lower(a.accountcoordinator_notes__c) like '%address%' then 'Incomplete Merchant Address'
          when f.fix_reason like 'Incorrect Discount%' or lower(a.accountcoordinator_notes__c) like '%discount%' then 'Incorrect Discount'
          when f.fix_reason like 'Incorrect Expiration%' or lower(a.accountcoordinator_notes__c) like '%expiration%' then 'Incorrect Expiration'
          when f.fix_reason like 'Incorrect Pricing%' or lower(a.accountcoordinator_notes__c) like '%pricing%' then 'Incorrect Pricing'
          when f.fix_reason like 'Insurance%' or lower(a.accountcoordinator_notes__c) like '%insurnace%' then 'Insurance'
          when f.fix_reason like 'Licenses%' or lower(a.accountcoordinator_notes__c) like '%license%' then 'Licenses'
          when f.fix_reason like '%provide required info%' or lower(a.accountcoordinator_notes__c) like '%merchant failed%' then ''
          when f.fix_reason like 'Missing Redemption method%' or lower(a.accountcoordinator_notes__c) like '%redemption%' then 'Missing Redemption method'
          when f.fix_reason like 'No Price verification%' or lower(a.accountcoordinator_notes__c) like '%price verification%' then 'No Price verification'
          when f.fix_reason like 'TIN%' or lower(a.accountcoordinator_notes__c) like '%tin%' then 'TIN'
          when f.fix_reason like 'URL%' or lower(a.accountcoordinator_notes__c) like '%url%' then 'URL'
          when lower(f.fix_reason) like '%vat%' or lower(a.accountcoordinator_notes__c) like '%vat%' then 'VAT'
          when f.fix_reason like 'Legal%' or lower(a.accountcoordinator_notes__c) like '%legal%' then 'Legal'
          when f.fix_reason like 'Deal Details%' then 'Deal Details'
          when f.fix_reason like 'Fine Print%' then 'Fine Print'
          when f.fix_reason like 'PDS%' then 'PDS'
        else 'Other' end fix_reason_v
      , c.vertical
      , count(distinct a.opportunity_id) closes
    from sandbox.jc_ss_deal_status_v a
    join sandbox.jc_merchant_mtd_attrib c
      on a.opportunity_id = c.opportunity_id
    left join jc_ss_fix_opp_v f
      on f.opportunity_id = a.opportunity_id
    where fix_reason_v <> ''
      and a.week_end < current_date
    group by 1,2,3,4,5,6,7,8
  ) with data primary index(week_end)
;

grant select on sandbox.jc_ss_deal_fix_reasons to public;

drop table sandbox.jc_ss_ops_reporting_c;
create table sandbox.jc_ss_ops_reporting_c as (
  select
      dw.week_end
      , pre_v_post_feature
      , a.feature_country
      , count(distinct a.opportunity_id) closes
      , count(distinct case when a.invalid_data__c is null then a.opportunity_id end) gtg_closes
      , count(distinct case when fix_required = 1 then a.opportunity_id end) fix_closes
      , count(distinct case when deal_flag = 'Deal Rejected: DNC' then a.opportunity_id end) dnc_closes

      , count(distinct case when a.launch_date between a.close_date - interval '1' day and a.close_date + interval '14' day or pvd.postv_launch_flag = 1 then a.opportunity_id end) launches

      , count(distinct case when a.invalid_data__c is null
          and a.launch_date between a.close_date - interval '1' day and a.close_date + interval '14' day
        then a.opportunity_id end) gtg_launches
      , count(distinct case when a.invalid_data__c is null
          and a.launch_date between a.close_date - interval '1' day and a.close_date + interval '14' day
          and a.launch_date <= a.go_live_date + interval '1' day
        then a.opportunity_id end) gtg_launches_on_time

      , count(distinct case
            when fix_required = 1 and a.launch_date between a.close_date - interval '1' day and a.close_date + interval '14' day then a.opportunity_id
            when fix_required = 1 and pvd.postv_launch_flag = 1 then a.opportunity_id
          end
        ) fix_launches

      , count(distinct case when a.launch_date <= a.go_live_date + interval '1' day
            and a.launch_date between a.close_date - interval '1' day and a.close_date + interval '14' day
          then a.opportunity_id end
        ) launches_on_time
    from sandbox.jc_ss_deal_status_v a
    join sandbox.jc_merchant_mtd_attrib c on a.opportunity_id = c.opportunity_id
    join user_dw.v_dim_day dd on dd.day_rw = a.close_date
    join user_dw.v_dim_week dw on dw.week_key = dd.week_key
    left join jc_deal_fix_flagging f on f.deal_uuid = c.deal_uuid
    left join jc_postvetting_dim_v pvd on pvd.deal_uuid = a.deal_uuid
    left join (
        select deal_id, history_data.jsonextractvalue('$.additionalInfo.holdAt') holdAt
        from sb_merchant_experience.history_event a
        where event_type in ('METRO_CONTRACT_SIGNIN')
          and holdAt in ('Prevetting')
      ) pv on pv.deal_id = c.deal_uuid
    where a.close_date >= '2020-01-01'
      and deal_flag <> 'Vetting'
    group by 1,2,3
  ) with data no primary index
;

grant select on sandbox.jc_ss_ops_reporting_c to public;
grant select on sandbox.jc_ss_deal_status_reporting to public;
grant select on sandbox.jc_ss_deal_status_v to public;

------------------------------------------------------------
select
  --a.week_end
  case when a.close_date between date '2021-11-15' and current_date - interval '14' day then 'Post-Test'
      when a.close_date between date '2021-11-15' - interval '28' day and date '2021-11-15' - interval '14' day then 'Pre-Test'
    else 'Other' end test_grp
  , case when a.deal_flag = 'Launched' then 1 else 0 end launch_flag
  , case when post_vetting_flag = 1 and pre_vetting_flag = 1 then 'Pre + Post Vetting'
      when pre_vetting_flag = 1 and deal_flag = 'Awaiting FIX: GSO/Other' then 'Pre + GSO Vetting'
      when post_vetting_flag = 1 then 'Post-Vetting'
      when pre_vetting_flag = 1 then 'Pre-Vetting'
    else 'Other' end
  , count(distinct case when fix_required = 1 then a.opportunity_id end) fix_closes
  , count(distinct case
        when fix_required = 1 and a.launch_date between a.close_date - interval '1' day and a.close_date + interval '14' day then a.opportunity_id
        when fix_required = 1 and pvd.postv_launch_flag = 1 then a.opportunity_id
      end
    ) fix_launches
from sandbox.jc_ss_deal_status_v a
left join jc_deal_fix_flagging f on a.deal_uuid = f.deal_uuid
left join jc_postvetting_dim_v pvd on pvd.deal_uuid = a.deal_uuid
where a.feature_country in ('US', 'CA')
  and a.close_date >= current_date - interval '67' day
  and a.deal_flag <> 'Vetting'
group by 1,2,3
--
--
-- select
--   case when invalid_data__c in ('Licenses', 'TIN'
--         ,'Merchant doesn''t provide required info;TIN'
--         ,'Licenses;Merchant doesn''t provide required info'
--         ,'Licenses;Merchant doesn''t provide required info;TIN'
--     ) then 1 else 0 end test_flag
--   , case when b.close_date <= '2021-09-15' then 'Pre' else 'Post' end period
--   , count(distinct a.opportunity_id) fixes
--   , count(distinct case when b.launch_date is not null then a.opportunity_id end) deals_launched
-- from sandbox.jc_ss_deal_status_v a
-- join sandbox.jc_merchant_mtd_attrib b on a.opportunity_id = b.opportunity_id
-- where b.close_date between date '2021-09-15' - interval '70' day and date '2021-09-15' + interval '70' day
--   and b.dmapi_flag = 1
--   and b.feature_country = 'US'
--   and a.deal_flag <> 'Deal is DNC'
--   and lower(a.invalid_data__c) like any ('%license%', '%tin%')
--   and test_flag = 1
-- group by 1,2
--
--
drop table sandbox.jc_ss_deal_status_adhoc;
create table sandbox.jc_ss_deal_status_adhoc as (
    select
      a.deal_flag
      , td_month_end(a.close_date) month_end_dt
      , case when a.feature_country in ('US', 'CA') then 'NAM' else 'INTL' end region
      , a.feature_country
      , case when lower(c.rev_management_metal_at_close) in ('silver', 'sliver', 'gold', 'platinum') then 'S+' else 'B-' end metal_group
      , pre_v_post_feature
      , fr.fix_required
      , case
          when f.fix_reason like 'Incomplete Merchant Address%' or lower(a.accountcoordinator_notes__c) like '%address%' then 'Incomplete Merchant Address'
          when f.fix_reason like 'Incorrect Discount%' or lower(a.accountcoordinator_notes__c) like '%discount%' then 'Incorrect Discount'
          when f.fix_reason like 'Incorrect Expiration%' or lower(a.accountcoordinator_notes__c) like '%expiration%' then 'Incorrect Expiration'
          when f.fix_reason like 'Incorrect Pricing%' or lower(a.accountcoordinator_notes__c) like '%pricing%' then 'Incorrect Pricing'
          when f.fix_reason like 'Insurance%' or lower(a.accountcoordinator_notes__c) like '%insurnace%' then 'Insurance'
          when f.fix_reason like 'Licenses%' or lower(a.accountcoordinator_notes__c) like '%license%' then 'Licenses'
          when f.fix_reason like '%provide required info%' or lower(a.accountcoordinator_notes__c) like '%merchant failed%' then ''
          when f.fix_reason like 'Missing Redemption method%' or lower(a.accountcoordinator_notes__c) like '%redemption%' then 'Missing Redemption method'
          when f.fix_reason like 'No Price verification%' or lower(a.accountcoordinator_notes__c) like '%price verification%' then 'No Price verification'
          when f.fix_reason like 'TIN%' or lower(a.accountcoordinator_notes__c) like '%tin%' then 'TIN'
          when f.fix_reason like 'URL%' or lower(a.accountcoordinator_notes__c) like '%url%' then 'URL'
          when lower(f.fix_reason) like '%vat%' or lower(a.accountcoordinator_notes__c) like '%vat%' then 'VAT'
          when f.fix_reason like 'Legal%' or lower(a.accountcoordinator_notes__c) like '%legal%' then 'Legal'
          when f.fix_reason like 'Deal Details%' then 'Deal Details'
          when f.fix_reason like 'Fine Print%' then 'Fine Print'
          when f.fix_reason like 'PDS%' then 'PDS'
        else 'Other' end fix_reason_v
      , c.vertical
      , count(distinct a.opportunity_id) closes
    from sandbox.jc_ss_deal_status_v a
    join sandbox.jc_merchant_mtd_attrib c
      on a.opportunity_id = c.opportunity_id
    join (
      select distinct deal_uuid
      from sandbox.jc_template_deals_vw
      where test_pds <> 'other'
        and merchant_bucket_dt is not null
        and close_date <= current_date - interval '14' day
        and template_chosen = 'Template'
        and pds <> '82d970c1-1cce-49b8-b84f-0071eed2ff55'
    ) tmp on tmp.deal_uuid = a.deal_uuid
    left join jc_ss_fix_opp_v f
      on f.opportunity_id = a.opportunity_id
      left join jc_deal_fix_flagging fr on a.deal_uuid = fr.deal_uuid
left join jc_postvetting_dim_v pvd on pvd.deal_uuid = a.deal_uuid
    where fix_reason_v <> ''
      and a.week_end < current_date
    group by 1,2,3,4,5,6,7,8,9
    ) with data no primary index
