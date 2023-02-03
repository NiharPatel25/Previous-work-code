--------------------------------------------------------------------------------------------------
----last touch and paid first 

create table grp_gdoop_bizops_db.np_w2l_mkt_tmp stored as orc as 
select
      c1.*
      , case
          when leadsource like '%Phone Bank%' then 'Phone Bank- No Campaign'
          when lower(campaign_name__c) = 'other' then 'No Campaign - Referred' else case
              when lower(campaign_name__c) in ('groupon-header-app1', 'groupon-header1', 'groupon-header-mweb1', 'groupon-footer') then lower(campaign_name__c)
              when (lower(campaign_name__c) like '%us_un_som_fac_tim_lb_rr_cbp%' and lower(campaign_name__c) like '%fdp%') then 'Facebook F&D'
              when (lower(campaign_name__c) like '%us_un_som_fac_tim_lb_rr_cbp%' and lower(campaign_name__c) like '%hbp%') then 'Facebook HBW'
              when lower(campaign_name__c) like '%us_un_som_fac_tim_lb_rr_cbp_chw_sy_v30%' then 'Social Retargeting'
              when lower(campaign_name__c) like '%d*merchant-fb%' then 'Facebook_old'
              when lower(campaign_name__c) like '%g*gmail-ads%' then  'GMail Ads'
              when lower(campaign_name__c) like '%d*gw-dx-dis%' then  'DataXu'
              when lower(campaign_name__c) like '%groupon%' then 'Referral'
              when lower(campaign_name__c) like '%livingsocial_ib%' then 'Living Social'
              when lower(campaign_name__c) like '%delivery-takeout-lp%' then 'Delivery Takeout'
              when campaign_name__c = '50_DLS' then 'Referral'
              when campaign_name__c = '50' then 'Referral'
              when (lower(campaign_name__c) like '%grouponworks%' and lower(campaign_name__c) like '%social%') then 'Social'
              when lower(campaign_name__c) like '%merchant-retargeting%' then 'Merchant Retargeting'
              when lower(campaign_name__c) like '%merchant-stream%' then 'Yahoo Stream Ads'
              when lower(campaign_name__c) like '%d*merchant-gsp%' then 'Gmail Sponsored Promotions'
              --when lower(campaign_name__c) like '%biz_page%' then 'Biz Pages'
              when lower(campaign_name__c) like '%blog%' or lower(campaign_name__c) like '%merchant_blog%' or lower(campaign_name__c) like '%merchant_article%' or lower(campaign_name__c) like '%merchant-blog-how-to-sell-post%' or lower(campaign_name__c) like '%merchant-blog-sidebar%' then 'Merchant Blog/SBRC'
              when lower(campaign_name__c) like '%merchantnl%' or lower(campaign_name__c) like '%june2014%' or lower(campaign_name__c) like '%july2014%' or lower(campaign_name__c) like '%august2014%' or lower(campaign_name__c) like '%september2014%' or lower(campaign_name__c) like '%october2014%' or lower(campaign_name__c) like '%november2014%' or lower(campaign_name__c) like '%december2014%' or lower(campaign_name__c) like '%january2015%' or lower(campaign_name__c) like '%feb2015%' or lower(campaign_name__c) like '%mar2015%' or lower(campaign_name__c) like '%apr2015%' or lower(campaign_name__c) like '%may2015%' or lower(campaign_name__c) like '%june2015%' or lower(campaign_name__c) like '%july2015%' or lower(campaign_name__c) like '%august2015%' or lower(campaign_name__c) like '%sept2015%' or lower(campaign_name__c) like '%oct2015%' or lower(campaign_name__c) like '%nov2015%' or lower(campaign_name__c) like '%dec2015%' or lower(campaign_name__c) like '%jan2016%' then 'Merchant Newsletter'
              when lower(campaign_name__c) like '%print%' or lower(campaign_name__c) like '%nra2016%' or lower(campaign_name__c) like '%osr-cards%' or lower(campaign_name__c) like '%hbw-2016%' or lower(campaign_name__c) like '%austin-promo-16%' or lower(campaign_name__c) like '%cultural-institutions-2015%' or lower(campaign_name__c) like '%ttd-cultural-institutions%' or lower(campaign_name__c) like '%ttd-culture-2016%' or lower(campaign_name__c) like '%ttd-culture-2016%' or lower(campaign_name__c) like '%ttd-activities-2016%' or lower(campaign_name__c) like '%activities-2015%' or lower(campaign_name__c) like '%events-2015%' or lower(campaign_name__c) like '%astc-promo-16%' then 'Print'
              when lower(campaign_name__c) like '%grouponworks_social%' then 'Social'
              --when lower(campaign_name__c) like '%goods%' then 'Goods'
              --when lower(campaign_name__c) like '%g1%' then 'G1'
              when lower(campaign_name__c) like '%occasions_sponsor%' then 'Occasions_sponsor'
              when lower(campaign_name__c) like '%occasions%' then 'Occasions'
              --when lower(campaign_name__c) like '%collections%' then 'Collections'
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
              else case
                  when cast(createddate as date) <= '2017-04-01' then case
                      when (sem_partner like '%ggl%' and sem_brand like '%nbr%') then 'Google - Non-Brand'
                      when (sem_partner like '%ggl%' and sem_brand like '%ybr%') then 'Google - Brand'
                      when (sem_partner like '%ggl%' and sem_brand like '%merchant-competitors%') then 'Google - Competitor'
                        when (sem_partner like '%bng%' and sem_brand like '%nbr%') then 'Bing - Non-Brand'
                      when (sem_partner like '%bng%' and sem_brand like '%ybr%') then 'Bing - Brand'
                      when (sem_partner like '%bng%' and sem_brand like '%cbr%') then 'Bing - Competitor'
                      when lower(split(campaign_name__c,'_')[1]) like 'g1%' then 'G1'
                      when lower(split(campaign_name__c,'_')[1]) like 'goods%' then 'Goods'
                      when lower(split(campaign_name__c,'_')[1]) like 'occasion%' then 'Occasions'
                      when lower(split(campaign_name__c,'_')[1]) like 'getaways%' then 'Getaways'
                      when lower(split(campaign_name__c,'_')[1]) like 'reserve%' then 'Reserve'
                      when lower(split(campaign_name__c,'_')[1]) like 'collection%' then 'Collections'
                      when lower(split(campaign_name__c,'_')[1]) like 'payments%' then 'Payments'
                      when lower(campaign_name__c) like 'k*%' then 'Google - Non-Brand'
                      when lower(campaign_name__c) like '%merchant_blog%' then 'Merchant Blog'
                      when lower(campaign_name__c) like '%grouponworks_social%' then 'Social'
                      when (lower(campaign_name__c) like '%ggl%' and lower(campaign_name__c) like '%nbr%') then 'Google - Non-Brand'
                      when (lower(campaign_name__c) like '%ggl%' and lower(campaign_name__c) like '%ybr%') then 'Google - Brand'
                      when (lower(campaign_name__c) like '%ggl%' and lower(campaign_name__c) like '%merchant-competitors%') then 'Google - Competitor'
                      when (lower(campaign_name__c) like '%ggl%' and lower(campaign_name__c) like '%rmk%') then 'Google - Remarketing'
                      else 'Other' end
                  else case
                      when (lower(campaign_name__c) like '%free_advertising%') then 'Google NB - Free Advertising'
                      when (lower(campaign_name__c) like '%sb_adv%') then 'Google NB - SB-Adv'
                      when (lower(campaign_name__c) like '%promote%') then 'Google NB - Promote'
                      when (lower(campaign_name__c) like '%advertise%') then 'Google NB - Advertise'
                      when (lower(campaign_name__c) like '%number%') then 'Google Brand - Number'
                      when (lower(campaign_name__c) like '%contact_merchant%') then 'Google Brand - Contact Merchant'
                      when (lower(campaign_name__c) like '%advertising%') then 'Google Brand - Advertising'
                      when (lower(campaign_name__c) like '%how_to_business%') then 'Google Brand - How To Business'
                      when (lower(campaign_name__c) like '%business%') then 'Google Brand - Business'
                      when (lower(campaign_name__c) like '%join%') then 'Google Brand - Join'
                      when (lower(campaign_name__c) like '%merchant_misc%') then 'Google Brand - Merchant Misc'
                      when (sem_partner like '%ggl%' and sem_brand like '%merchant-competitors%') then 'Google - Competitor'
                      when (sem_partner like '%bng%' and sem_brand like '%nbr%') then 'Bing - Non-Brand'
                      when (sem_partner like '%bng%' and sem_brand like '%ybr%') then 'Bing - Brand'
                      when (sem_partner like '%bng%' and sem_brand like '%cbr%') then 'Bing - Competitor'
                      when lower(split(campaign_name__c,'_')[1]) like 'g1%' then 'G1'
                      when lower(split(campaign_name__c,'_')[1]) like 'goods%' then 'Goods'
                      when lower(split(campaign_name__c,'_')[1]) like 'occasion%' then 'Occasions'
                      when lower(split(campaign_name__c,'_')[1]) like 'getaways%' then 'Getaways'
                      when lower(split(campaign_name__c,'_')[1]) like 'reserve%' then 'Reserve'
                      when lower(split(campaign_name__c,'_')[1]) like 'collection%' then 'Collections'
                      when lower(split(campaign_name__c,'_')[1]) like 'payments%' then 'Payments'
                      when lower(campaign_name__c) like 'k*%' then 'Google - Non-Brand'
                      when lower(campaign_name__c) like '%merchant_blog%' then 'Merchant Blog'
                      when lower(campaign_name__c) like '%grouponworks_social%' then 'Social'
                      when (lower(campaign_name__c) like '%ggl%' and lower(campaign_name__c) like '%nbr%') then 'Google - Non-Brand'
                      when (lower(campaign_name__c) like '%ggl%' and lower(campaign_name__c) like '%ybr%') then 'Google - Brand'
                      when (lower(campaign_name__c) like '%ggl%' and lower(campaign_name__c) like '%merchant-competitors%') then 'Google - Competitor'
                      when (lower(campaign_name__c) like '%ggl%' and lower(campaign_name__c) like '%rmk%') then 'Google - Remarketing'
                      else 'Other' end
              end
          end
        end as campaign_type
    from (
      select
        *
        , case when length(campaign_name__c)-length(translate(campaign_name__c,'_','')) > 11 then 1 else 0 end campaign_new_format
        , case when length(campaign_name__c)-length(translate(campaign_name__c,'_','')) > 11 then lower(split(campaign_name__c,'_')[4]) else null end sem_partner
        , case when length(campaign_name__c)-length(translate(campaign_name__c,'_','')) > 11 then lower(split(campaign_name__c,'_')[10]) else null end sem_brand
      from user_groupondw.sf_lead
       where leadsource in ('Metro - Self Service', 'MIA - Inbound')
        and createddate > '2019-01-01'
      ) c1


drop table if exists grp_gdoop_bizops_db.np_w2l_mktg_acct_attrib_temp;
create table grp_gdoop_bizops_db.np_w2l_mktg_acct_attrib_temp stored as orc as
  select
    convertedaccountid as accountid
    , date_format(c2.createddate,'yyyy-MM-dd') as createddate
    , case when country = 'GB' then 'UK' else country end as country_code
    , campaign_name__c
    , campaign_type
    ,  case
        when campaign_name__c like 'TXN1%' and split(campaign_name__c, '_')[3] = 'SEMB' then 'Google-SEM'-- made a change should be 4
      when campaign_name__c like 'TXN1%' and split(campaign_name__c, '_')[3] = 'SEMN' then  'Google-SEM-NB'
        when campaign_name__c like 'TXN1%' and  campaign_name__c  like '%DIS_GEN__FB%' then 'FB-Display'
          when campaign_name__c like 'TXN1%'  and campaign_name__c  like '% DIS_GEN_AMZN%' then 'AMZON-Display' 
         when campaign_name__c like 'TXN1%' and split(campaign_name__c, '_')[3] = 'VID' then 'MNTN'
        when campaign_name__c like 'TXN1%' and split(campaign_name__c, '_')[3] = 'GPMC' then 'GPMC'
        when campaign_name__c like 'TXN1%' and split(campaign_name__c, '_')[3] = 'SEMNB' then 'SEM-NB'
        when campaign_name__c like '%always_on%' then 'SEM-Brand'
         when campaign_name__c like 'TXN1%' and campaign_name__c  like '%candaceholyfield_Gen__FB%' then 'Influencer_fb'
  when campaign_name__c like 'TXN1%' and campaign_name__c  like '%candaceholyfield_Gen__INS%' then 'Influencer_insta'
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
  from grp_gdoop_bizops_db.np_w2l_mkt_tmp c2
  left join dwh_base_sec_view.sf_account sfa
    on sfa.account_id_18 = c2.convertedaccountid
  join user_dw.v_dim_day dd
    on date_format(c2.createddate, 'yyyy-MM-dd') = dd.day_rw
  join user_dw.v_dim_week wk
    on dd.week_key = wk.week_key;
    
 
   create table sm_w2l_mktg_acct_attrib as
  select
    convertedaccountid as accountid
    , date_format(c2.createddate,'yyyy-MM-dd') as createddate
    , case when country = 'GB' then 'UK' else country end as country_code
    , campaign_name__c
    , campaign_type
    , case
      when campaign_name__c like 'TXN1%' and split(campaign_name__c, '_')[3] = 'SEMB' then 'SEM-Brand'-- made a change should be 4
      when campaign_name__c like 'TXN1%' and split(campaign_name__c, '_')[3] = 'SEMN' then 'SEM-NB'
      when campaign_name__c like 'TXN1%' and split(campaign_name__c, '_')[3] = 'DIS' then 'Display'
      when campaign_name__c like 'TXN1%' and split(campaign_name__c, '_')[3] = 'VID' then 'SEM-Brand'
      when campaign_name__c like 'TXN1%' and split(campaign_name__c, '_')[3] = 'GPMC' then 'SEM-Brand'
      when campaign_name__c like 'TXN1%' and split(campaign_name__c, '_')[3] = 'SEMNB' then 'SEM-NB'
      when campaign_name__c like '%always_on%' then 'SEM-Brand'
      when campaign_name__c like '%SEMC %' then 'SEM-Brand'
      when campaign_name__c like 'TXN1%' and campaign_name__c  like '%candaceholyfield_Gen__FB%' then 'Influencer_fb'
      when campaign_name__c like 'TXN1%' and campaign_name__c  like '%candaceholyfield_Gen__INS%' then 'Influencer_insta'
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
  from (
    select
      c1.*
      , case
          when leadsource like '%Phone Bank%' then 'Phone Bank- No Campaign'
          when lower(campaign_name__c) = 'other' then 'No Campaign - Referred' else case
              when lower(campaign_name__c) in ('groupon-header-app1', 'groupon-header1', 'groupon-header-mweb1', 'groupon-footer') then lower(campaign_name__c)
              when (lower(campaign_name__c) like '%us_un_som_fac_tim_lb_rr_cbp%' and lower(campaign_name__c) like '%fdp%') then 'Facebook F&D'
              when (lower(campaign_name__c) like '%us_un_som_fac_tim_lb_rr_cbp%' and lower(campaign_name__c) like '%hbp%') then 'Facebook HBW'
              when lower(campaign_name__c) like '%us_un_som_fac_tim_lb_rr_cbp_chw_sy_v30%' then 'Social Retargeting'
              when lower(campaign_name__c) like '%d*merchant-fb%' then 'Facebook_old'
              when lower(campaign_name__c) like '%g*gmail-ads%' then  'GMail Ads'
              when lower(campaign_name__c) like '%d*gw-dx-dis%' then  'DataXu'
              when lower(campaign_name__c) like '%groupon%' then 'Referral'
              when lower(campaign_name__c) like '%livingsocial_ib%' then 'Living Social'
              when lower(campaign_name__c) like '%delivery-takeout-lp%' then 'Delivery Takeout'
              when campaign_name__c = '50_DLS' then 'Referral'
              when campaign_name__c = '50' then 'Referral'
              when (lower(campaign_name__c) like '%grouponworks%' and lower(campaign_name__c) like '%social%') then 'Social'
              when lower(campaign_name__c) like '%merchant-retargeting%' then 'Merchant Retargeting'
              when lower(campaign_name__c) like '%merchant-stream%' then 'Yahoo Stream Ads'
              when lower(campaign_name__c) like '%d*merchant-gsp%' then 'Gmail Sponsored Promotions'
              --when lower(campaign_name__c) like '%biz_page%' then 'Biz Pages'
              when lower(campaign_name__c) like '%blog%' or lower(campaign_name__c) like '%merchant_blog%' or lower(campaign_name__c) like '%merchant_article%' or lower(campaign_name__c) like '%merchant-blog-how-to-sell-post%' or lower(campaign_name__c) like '%merchant-blog-sidebar%' then 'Merchant Blog/SBRC'
              when lower(campaign_name__c) like '%merchantnl%' or lower(campaign_name__c) like '%june2014%' or lower(campaign_name__c) like '%july2014%' or lower(campaign_name__c) like '%august2014%' or lower(campaign_name__c) like '%september2014%' or lower(campaign_name__c) like '%october2014%' or lower(campaign_name__c) like '%november2014%' or lower(campaign_name__c) like '%december2014%' or lower(campaign_name__c) like '%january2015%' or lower(campaign_name__c) like '%feb2015%' or lower(campaign_name__c) like '%mar2015%' or lower(campaign_name__c) like '%apr2015%' or lower(campaign_name__c) like '%may2015%' or lower(campaign_name__c) like '%june2015%' or lower(campaign_name__c) like '%july2015%' or lower(campaign_name__c) like '%august2015%' or lower(campaign_name__c) like '%sept2015%' or lower(campaign_name__c) like '%oct2015%' or lower(campaign_name__c) like '%nov2015%' or lower(campaign_name__c) like '%dec2015%' or lower(campaign_name__c) like '%jan2016%' then 'Merchant Newsletter'
              when lower(campaign_name__c) like '%print%' or lower(campaign_name__c) like '%nra2016%' or lower(campaign_name__c) like '%osr-cards%' or lower(campaign_name__c) like '%hbw-2016%' or lower(campaign_name__c) like '%austin-promo-16%' or lower(campaign_name__c) like '%cultural-institutions-2015%' or lower(campaign_name__c) like '%ttd-cultural-institutions%' or lower(campaign_name__c) like '%ttd-culture-2016%' or lower(campaign_name__c) like '%ttd-culture-2016%' or lower(campaign_name__c) like '%ttd-activities-2016%' or lower(campaign_name__c) like '%activities-2015%' or lower(campaign_name__c) like '%events-2015%' or lower(campaign_name__c) like '%astc-promo-16%' then 'Print'
              when lower(campaign_name__c) like '%grouponworks_social%' then 'Social'
              --when lower(campaign_name__c) like '%goods%' then 'Goods'
              --when lower(campaign_name__c) like '%g1%' then 'G1'
              when lower(campaign_name__c) like '%occasions_sponsor%' then 'Occasions_sponsor'
              when lower(campaign_name__c) like '%occasions%' then 'Occasions'
              --when lower(campaign_name__c) like '%collections%' then 'Collections'
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
              else case
                  when cast(createddate as date) <= '2017-04-01' then case
                      when (sem_partner like '%ggl%' and sem_brand like '%nbr%') then 'Google - Non-Brand'
                      when (sem_partner like '%ggl%' and sem_brand like '%ybr%') then 'Google - Brand'
                      when (sem_partner like '%ggl%' and sem_brand like '%merchant-competitors%') then 'Google - Competitor'
                        when (sem_partner like '%bng%' and sem_brand like '%nbr%') then 'Bing - Non-Brand'
                      when (sem_partner like '%bng%' and sem_brand like '%ybr%') then 'Bing - Brand'
                      when (sem_partner like '%bng%' and sem_brand like '%cbr%') then 'Bing - Competitor'
                      when lower(split(campaign_name__c,'_')[1]) like 'g1%' then 'G1'
                      when lower(split(campaign_name__c,'_')[1]) like 'goods%' then 'Goods'
                      when lower(split(campaign_name__c,'_')[1]) like 'occasion%' then 'Occasions'
                      when lower(split(campaign_name__c,'_')[1]) like 'getaways%' then 'Getaways'
                      when lower(split(campaign_name__c,'_')[1]) like 'reserve%' then 'Reserve'
                      when lower(split(campaign_name__c,'_')[1]) like 'collection%' then 'Collections'
                      when lower(split(campaign_name__c,'_')[1]) like 'payments%' then 'Payments'
                      when lower(campaign_name__c) like 'k*%' then 'Google - Non-Brand'
                      when lower(campaign_name__c) like '%merchant_blog%' then 'Merchant Blog'
                      when lower(campaign_name__c) like '%grouponworks_social%' then 'Social'
                      when (lower(campaign_name__c) like '%ggl%' and lower(campaign_name__c) like '%nbr%') then 'Google - Non-Brand'
                      when (lower(campaign_name__c) like '%ggl%' and lower(campaign_name__c) like '%ybr%') then 'Google - Brand'
                      when (lower(campaign_name__c) like '%ggl%' and lower(campaign_name__c) like '%merchant-competitors%') then 'Google - Competitor'
                      when (lower(campaign_name__c) like '%ggl%' and lower(campaign_name__c) like '%rmk%') then 'Google - Remarketing'
                      else 'Other' end
                  else case
                      when (lower(campaign_name__c) like '%free_advertising%') then 'Google NB - Free Advertising'
                      when (lower(campaign_name__c) like '%sb_adv%') then 'Google NB - SB-Adv'
                      when (lower(campaign_name__c) like '%promote%') then 'Google NB - Promote'
                      when (lower(campaign_name__c) like '%advertise%') then 'Google NB - Advertise'
                      when (lower(campaign_name__c) like '%number%') then 'Google Brand - Number'
                      when (lower(campaign_name__c) like '%contact_merchant%') then 'Google Brand - Contact Merchant'
                      when (lower(campaign_name__c) like '%advertising%') then 'Google Brand - Advertising'
                      when (lower(campaign_name__c) like '%how_to_business%') then 'Google Brand - How To Business'
                      when (lower(campaign_name__c) like '%business%') then 'Google Brand - Business'
                      when (lower(campaign_name__c) like '%join%') then 'Google Brand - Join'
                      when (lower(campaign_name__c) like '%merchant_misc%') then 'Google Brand - Merchant Misc'
                      when (sem_partner like '%ggl%' and sem_brand like '%merchant-competitors%') then 'Google - Competitor'
                      when (sem_partner like '%bng%' and sem_brand like '%nbr%') then 'Bing - Non-Brand'
                      when (sem_partner like '%bng%' and sem_brand like '%ybr%') then 'Bing - Brand'
                      when (sem_partner like '%bng%' and sem_brand like '%cbr%') then 'Bing - Competitor'
                      when lower(split(campaign_name__c,'_')[1]) like 'g1%' then 'G1'
                      when lower(split(campaign_name__c,'_')[1]) like 'goods%' then 'Goods'
                      when lower(split(campaign_name__c,'_')[1]) like 'occasion%' then 'Occasions'
                      when lower(split(campaign_name__c,'_')[1]) like 'getaways%' then 'Getaways'
                      when lower(split(campaign_name__c,'_')[1]) like 'reserve%' then 'Reserve'
                      when lower(split(campaign_name__c,'_')[1]) like 'collection%' then 'Collections'
                      when lower(split(campaign_name__c,'_')[1]) like 'payments%' then 'Payments'
                      when lower(campaign_name__c) like 'k*%' then 'Google - Non-Brand'
                      when lower(campaign_name__c) like '%merchant_blog%' then 'Merchant Blog'
                      when lower(campaign_name__c) like '%grouponworks_social%' then 'Social'
                      when (lower(campaign_name__c) like '%ggl%' and lower(campaign_name__c) like '%nbr%') then 'Google - Non-Brand'
                      when (lower(campaign_name__c) like '%ggl%' and lower(campaign_name__c) like '%ybr%') then 'Google - Brand'
                      when (lower(campaign_name__c) like '%ggl%' and lower(campaign_name__c) like '%merchant-competitors%') then 'Google - Competitor'
                      when (lower(campaign_name__c) like '%ggl%' and lower(campaign_name__c) like '%rmk%') then 'Google - Remarketing'
                      else 'Other' end
              end
          end
        end as campaign_type
    from (
      select
        *
        , case when length(campaign_name__c)-length(translate(campaign_name__c,'_','')) > 11 then 1 else 0 end campaign_new_format
        , case when length(campaign_name__c)-length(translate(campaign_name__c,'_','')) > 11 then lower(split(campaign_name__c,'_')[4]) else null end sem_partner
        , case when length(campaign_name__c)-length(translate(campaign_name__c,'_','')) > 11 then lower(split(campaign_name__c,'_')[10]) else null end sem_brand
      from user_groupondw.sf_lead
       where leadsource in ('Metro - Self Service', 'MIA - Inbound')
        and createddate > '2019-01-01'
      ) c1
    ) c2
  left join dwh_base_sec_view.sf_account sfa
    on sfa.account_id_18 = c2.convertedaccountid
  join user_dw.v_dim_day dd
    on date_format(c2.createddate, 'yyyy-MM-dd') = dd.day_rw
  join user_dw.v_dim_week wk
    on dd.week_key = wk.week_key;
    
   
 drop table if exists sm_ss_bcookies purge;
 create table sm_ss_bcookies stored as orc as
  select
    distinct get_json_object(history_data,'$.restricted.bCookie') bcookie
    , mkt.accountid
    , mkt.createddate as event_date
  from grp_gdoop_sup_analytics_db.sm_w2l_mktg_acct_attrib mkt
  left join user_edwprod.dim_merchant dm
    on mkt.accountid = dm.salesforce_account_id
  left join (select * from grp_gdoop_sup_analytics_db.metro_history_event where event_type = 'MERCHANT_CREATION') he
    on get_json_object(history_data, '$.additionalInfo.response.contact.merchants[0].id') = dm.merchant_uuid
      and date_format(he.event_date, 'yyyy-MM-dd') = date_format(mkt.createddate, 'yyyy-MM-dd')
  where mkt.createddate > '2020-01-01';
  
 
 drop table if exists sm_ss_unique_bcookie_utm purge;
 create table sm_ss_unique_bcookie_utm stored as orc as
  select
    a.*
    , case
        when lower(utm_campaign) = 'other' then 'No Campaign - Referred' else case
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
            else case
                when cast(createddate as date) <= '2017-04-01' then case
                    when (sem_partner like '%ggl%' and sem_brand like '%nbr%') then 'Google - Non-Brand'
                    when (sem_partner like '%ggl%' and sem_brand like '%ybr%') then 'Google - Brand'
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
                    else 'Other' end
                else case
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
                    else 'Other' end
            end
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
  
 
 drop table if exists sm_ss_assisted_tmp purge;create table sm_ss_assisted_tmp stored as orc as
  select
    bcookie
    , accountid
    , max(campaign_group) as asst_attrib_cmpn
  from (
    select
      bcookie
      , utm_campaign
      , accountid
      , campaign_type
      -- campaign groups are lebele with the folowing:
      -- 6: Display, 5: SEMNB, 4: SEMB, 3: SEO, 2: GRPN H&F, 1: Direct / Referral / Other
      , case
          when utm_campaign like 'TXN1%' and split(utm_campaign, '_')[3] = 'SEMB' then 4 --'SEM-Brand'
          when utm_campaign like 'TXN1%' and split(utm_campaign, '_')[3] = 'GPMC' then 4 --'SEM-Brand'
          when utm_campaign like 'TXN1%' and split(utm_campaign, '_')[3] = 'SEMN' then 5 --'SEM-NB'
          when utm_campaign like 'TXN1%' and split(utm_campaign, '_')[3] = 'DIS' then 6 --'Display'
          when  utm_campaign like 'TXN1%' and split(utm_campaign, '_')[3] = 'SEMNB' then 5--'SEM-NB'
          when utm_campaign like 'TXN1%' and split(utm_campaign, '_')[3] = 'VID' then 4
           when utm_campaign like '%always_on%' then 4
          when utm_campaign like '%SEMC %' then 4
          -- when campaign_type = 'No Campaign - Referred' then 'Referral'
          when campaign_type in ('groupon-header-app1', 'groupon-header1', 'groupon-header-mweb1', 'groupon-footer') then 2 --'Groupon Header & Footer'
          when campaign_type in ('Google - Brand','Google - Remarketing','Google Brand - Advertising','Google Brand - Business','Google Brand - Contact Merchant','Google Brand - How To Business','Google Brand - Join','Google Brand - Merchant Misc','Google Brand - Number') then 4 --'SEM-Brand'
          -- when campaign_type = 'Direct' then 'Direct'
          -- when campaign_type in ('Facebook F&D','Facebook HBW','Social Retargeting', 'Facebook Silver Lookalike') then 'Display'
          when campaign_type in ('Bing - Competitor','Google - Competitor','Google - Non-Brand','Google NB - Advertise','Google NB - Free Advertising','Google NB - Promote','Google NB - SB-Adv') then 5 -- 'SEM-NB'
          -- when campaign_type = 'Phone Bank - No Campaign' then 'Phone Bank'
          -- when campaign_type = 'Referral' then 'Referral'
          when campaign_type in ('Living Social','Merchant Blog/SBRC','Merchant Newsletter','Organic','Print') then 3 --'SEO'
          else 1 -- 'Direct / Referral / Other'
       end campaign_group
      , case when utm_campaign like 'TXN1%' then 1 else 0 end pd_mm_relaunch_flag
      , split(utm_campaign, '_')[0] as mktg_txny_version
      , split(utm_campaign, '_')[1] as mktg_country
      , split(utm_campaign, '_')[2] as mktg_test_division
      , split(utm_campaign, '_')[3] as mktg_traffic_source
      , split(utm_campaign, '_')[4] as mktg_audience
      , split(utm_campaign, '_')[5] as mktg_sem_type
      , split(utm_campaign, '_')[6] as mktg_platform
      , split(utm_campaign, '_')[7] as mktg_creative
    from sm_ss_unique_bcookie_utm
  ) a
  group by 1,2;
  
 
 drop table if exists sm_w2l_mktg_acct_attrib_tmp purge;create table sm_w2l_mktg_acct_attrib_tmp as
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
  from (
    select
      c1.*
      , case
          when leadsource like '%Phone Bank%' then 'Phone Bank- No Campaign'
          when lower(campaign_name__c) = 'other' then 'No Campaign - Referred' else case
              when lower(campaign_name__c) in ('groupon-header-app1', 'groupon-header1', 'groupon-header-mweb1', 'groupon-footer') then lower(campaign_name__c)
              when (lower(campaign_name__c) like '%us_un_som_fac_tim_lb_rr_cbp%' and lower(campaign_name__c) like '%fdp%') then 'Facebook F&D'
              when (lower(campaign_name__c) like '%us_un_som_fac_tim_lb_rr_cbp%' and lower(campaign_name__c) like '%hbp%') then 'Facebook HBW'
              when lower(campaign_name__c) like '%us_un_som_fac_tim_lb_rr_cbp_chw_sy_v30%' then 'Social Retargeting'
              when lower(campaign_name__c) like '%d*merchant-fb%' then 'Facebook_old'
              when lower(campaign_name__c) like '%g*gmail-ads%' then  'GMail Ads'
              when lower(campaign_name__c) like '%d*gw-dx-dis%' then  'DataXu'
              when lower(campaign_name__c) like '%groupon%' then 'Referral'
              when lower(campaign_name__c) like '%livingsocial_ib%' then 'Living Social'
              when lower(campaign_name__c) like '%delivery-takeout-lp%' then 'Delivery Takeout'
              when campaign_name__c = '50_DLS' then 'Referral'
              when campaign_name__c = '50' then 'Referral'
              when (lower(campaign_name__c) like '%grouponworks%' and lower(campaign_name__c) like '%social%') then 'Social'
              when lower(campaign_name__c) like '%merchant-retargeting%' then 'Merchant Retargeting'
              when lower(campaign_name__c) like '%merchant-stream%' then 'Yahoo Stream Ads'
              when lower(campaign_name__c) like '%d*merchant-gsp%' then 'Gmail Sponsored Promotions'
              --when lower(campaign_name__c) like '%biz_page%' then 'Biz Pages'
              when lower(campaign_name__c) like '%blog%' or lower(campaign_name__c) like '%merchant_blog%' or lower(campaign_name__c) like '%merchant_article%' or lower(campaign_name__c) like '%merchant-blog-how-to-sell-post%' or lower(campaign_name__c) like '%merchant-blog-sidebar%' then 'Merchant Blog/SBRC'
              when lower(campaign_name__c) like '%merchantnl%' or lower(campaign_name__c) like '%june2014%' or lower(campaign_name__c) like '%july2014%' or lower(campaign_name__c) like '%august2014%' or lower(campaign_name__c) like '%september2014%' or lower(campaign_name__c) like '%october2014%' or lower(campaign_name__c) like '%november2014%' or lower(campaign_name__c) like '%december2014%' or lower(campaign_name__c) like '%january2015%' or lower(campaign_name__c) like '%feb2015%' or lower(campaign_name__c) like '%mar2015%' or lower(campaign_name__c) like '%apr2015%' or lower(campaign_name__c) like '%may2015%' or lower(campaign_name__c) like '%june2015%' or lower(campaign_name__c) like '%july2015%' or lower(campaign_name__c) like '%august2015%' or lower(campaign_name__c) like '%sept2015%' or lower(campaign_name__c) like '%oct2015%' or lower(campaign_name__c) like '%nov2015%' or lower(campaign_name__c) like '%dec2015%' or lower(campaign_name__c) like '%jan2016%' then 'Merchant Newsletter'
              when lower(campaign_name__c) like '%print%' or lower(campaign_name__c) like '%nra2016%' or lower(campaign_name__c) like '%osr-cards%' or lower(campaign_name__c) like '%hbw-2016%' or lower(campaign_name__c) like '%austin-promo-16%' or lower(campaign_name__c) like '%cultural-institutions-2015%' or lower(campaign_name__c) like '%ttd-cultural-institutions%' or lower(campaign_name__c) like '%ttd-culture-2016%' or lower(campaign_name__c) like '%ttd-culture-2016%' or lower(campaign_name__c) like '%ttd-activities-2016%' or lower(campaign_name__c) like '%activities-2015%' or lower(campaign_name__c) like '%events-2015%' or lower(campaign_name__c) like '%astc-promo-16%' then 'Print'
              when lower(campaign_name__c) like '%grouponworks_social%' then 'Social'
              --when lower(campaign_name__c) like '%goods%' then 'Goods'
              --when lower(campaign_name__c) like '%g1%' then 'G1'
              when lower(campaign_name__c) like '%occasions_sponsor%' then 'Occasions_sponsor'
              when lower(campaign_name__c) like '%occasions%' then 'Occasions'
              --when lower(campaign_name__c) like '%collections%' then 'Collections'
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
              else case
                  when cast(createddate as date) <= '2017-04-01' then case
                      when (sem_partner like '%ggl%' and sem_brand like '%nbr%') then 'Google - Non-Brand'
                      when (sem_partner like '%ggl%' and sem_brand like '%ybr%') then 'Google - Brand'
                      when (sem_partner like '%ggl%' and sem_brand like '%merchant-competitors%') then 'Google - Competitor'
                        when (sem_partner like '%bng%' and sem_brand like '%nbr%') then 'Bing - Non-Brand'
                      when (sem_partner like '%bng%' and sem_brand like '%ybr%') then 'Bing - Brand'
                      when (sem_partner like '%bng%' and sem_brand like '%cbr%') then 'Bing - Competitor'
                      when lower(split(campaign_name__c,'_')[1]) like 'g1%' then 'G1'
                      when lower(split(campaign_name__c,'_')[1]) like 'goods%' then 'Goods'
                      when lower(split(campaign_name__c,'_')[1]) like 'occasion%' then 'Occasions'
                      when lower(split(campaign_name__c,'_')[1]) like 'getaways%' then 'Getaways'
                      when lower(split(campaign_name__c,'_')[1]) like 'reserve%' then 'Reserve'
                      when lower(split(campaign_name__c,'_')[1]) like 'collection%' then 'Collections'
                      when lower(split(campaign_name__c,'_')[1]) like 'payments%' then 'Payments'
                      when lower(campaign_name__c) like 'k*%' then 'Google - Non-Brand'
                      when lower(campaign_name__c) like '%merchant_blog%' then 'Merchant Blog'
                      when lower(campaign_name__c) like '%grouponworks_social%' then 'Social'
                      when (lower(campaign_name__c) like '%ggl%' and lower(campaign_name__c) like '%nbr%') then 'Google - Non-Brand'
                      when (lower(campaign_name__c) like '%ggl%' and lower(campaign_name__c) like '%ybr%') then 'Google - Brand'
                      when (lower(campaign_name__c) like '%ggl%' and lower(campaign_name__c) like '%merchant-competitors%') then 'Google - Competitor'
                      when (lower(campaign_name__c) like '%ggl%' and lower(campaign_name__c) like '%rmk%') then 'Google - Remarketing'
                      else 'Other' end
                  else case
                      when (lower(campaign_name__c) like '%free_advertising%') then 'Google NB - Free Advertising'
                      when (lower(campaign_name__c) like '%sb_adv%') then 'Google NB - SB-Adv'
                      when (lower(campaign_name__c) like '%promote%') then 'Google NB - Promote'
                      when (lower(campaign_name__c) like '%advertise%') then 'Google NB - Advertise'
                      when (lower(campaign_name__c) like '%number%') then 'Google Brand - Number'
                      when (lower(campaign_name__c) like '%contact_merchant%') then 'Google Brand - Contact Merchant'
                      when (lower(campaign_name__c) like '%advertising%') then 'Google Brand - Advertising'
                      when (lower(campaign_name__c) like '%how_to_business%') then 'Google Brand - How To Business'
                      when (lower(campaign_name__c) like '%business%') then 'Google Brand - Business'
                      when (lower(campaign_name__c) like '%join%') then 'Google Brand - Join'
                      when (lower(campaign_name__c) like '%merchant_misc%') then 'Google Brand - Merchant Misc'
                      when (sem_partner like '%ggl%' and sem_brand like '%merchant-competitors%') then 'Google - Competitor'
                      when (sem_partner like '%bng%' and sem_brand like '%nbr%') then 'Bing - Non-Brand'
                      when (sem_partner like '%bng%' and sem_brand like '%ybr%') then 'Bing - Brand'
                      when (sem_partner like '%bng%' and sem_brand like '%cbr%') then 'Bing - Competitor'
                      when lower(split(campaign_name__c,'_')[1]) like 'g1%' then 'G1'
                      when lower(split(campaign_name__c,'_')[1]) like 'goods%' then 'Goods'
                      when lower(split(campaign_name__c,'_')[1]) like 'occasion%' then 'Occasions'
                      when lower(split(campaign_name__c,'_')[1]) like 'getaways%' then 'Getaways'
                      when lower(split(campaign_name__c,'_')[1]) like 'reserve%' then 'Reserve'
                      when lower(split(campaign_name__c,'_')[1]) like 'collection%' then 'Collections'
                      when lower(split(campaign_name__c,'_')[1]) like 'payments%' then 'Payments'
                      when lower(campaign_name__c) like 'k*%' then 'Google - Non-Brand'
                      when lower(campaign_name__c) like '%merchant_blog%' then 'Merchant Blog'
                      when lower(campaign_name__c) like '%grouponworks_social%' then 'Social'
                      when (lower(campaign_name__c) like '%ggl%' and lower(campaign_name__c) like '%nbr%') then 'Google - Non-Brand'
                      when (lower(campaign_name__c) like '%ggl%' and lower(campaign_name__c) like '%ybr%') then 'Google - Brand'
                      when (lower(campaign_name__c) like '%ggl%' and lower(campaign_name__c) like '%merchant-competitors%') then 'Google - Competitor'
                      when (lower(campaign_name__c) like '%ggl%' and lower(campaign_name__c) like '%rmk%') then 'Google - Remarketing'
                      else 'Other' end
              end
          end
        end as campaign_type
    from (
      select
        *
        , case when length(campaign_name__c)-length(translate(campaign_name__c,'_','')) > 11 then 1 else 0 end campaign_new_format
        , case when length(campaign_name__c)-length(translate(campaign_name__c,'_','')) > 11 then lower(split(campaign_name__c,'_')[4]) else null end sem_partner
        , case when length(campaign_name__c)-length(translate(campaign_name__c,'_','')) > 11 then lower(split(campaign_name__c,'_')[10]) else null end sem_brand
      from user_groupondw.sf_lead
       where leadsource in ('Metro - Self Service', 'MIA - Inbound')
        and createddate > '2019-01-01'
      ) c1
    ) c2
  left join dwh_base_sec_view.sf_account sfa
    on sfa.account_id_18 = c2.convertedaccountid
  join user_dw.v_dim_day dd
    on date_format(c2.createddate, 'yyyy-MM-dd') = dd.day_rw
  join user_dw.v_dim_week wk
    on dd.week_key = wk.week_key;drop table if exists sm_w2l_mktg_acct_attrib purge;create table sm_w2l_mktg_acct_attrib stored as orc as
  select
    a.*
    , case when asst_attrib_cmpn = 6 or a.campaign_group = 'Display' then 'Display'
        when asst_attrib_cmpn = 5 or a.campaign_group = 'SEM-NB' then 'SEM-NB'
        when asst_attrib_cmpn = 4 or a.campaign_group = 'SEM-Brand' then 'SEM-Brand'
        when asst_attrib_cmpn = 3 or a.campaign_group = 'SEO' then 'SEO'
        when asst_attrib_cmpn = 2 or a.campaign_group = 'Groupon Header & Footer' then 'Groupon Header & Footer'
        when asst_attrib_cmpn = 1 or a.campaign_group = 'Direct / Referral / Other' then 'Direct / Referral / Other'
      else -1 end highest_touch
    , ast.bcookie
  from sm_w2l_mktg_acct_attrib_tmp a
  left join sm_ss_assisted_tmp ast
    on ast.accountid = a.accountid;drop table if exists sm_ss_bcookies purge;drop table if exists sm_ss_unique_bcookie_utm purge;drop table if exists sm_ss_assisted_tmp purge;drop table if exists sm_w2l_mktg_acct_attrib_tmp purge
    
    


   -------------------------------OPTIMUS JOB START  STEP 1: marketing_attrib
    

create table grp_gdoop_bizops_db.np_ss_assisted_tmp0 stored as orc as
select
      bcookie
      , utm_campaign
      , accountid
      , campaign_type
      -- campaign groups are lebele with the folowing:
      -- 6: Display, 5: SEMNB, 4: SEMB, 3: SEO, 2: GRPN H&F, 1: Direct / Referral / Other
      , case
          when utm_campaign like 'TXN1%' and split(utm_campaign, '_')[3] = 'SEMB' then 4 --'SEM-Brand'
          when utm_campaign like 'TXN1%' and split(utm_campaign, '_')[3] = 'GPMC' then 4 --'SEM-Brand'
          when utm_campaign like 'TXN1%' and split(utm_campaign, '_')[3] = 'SEMN' then 5 --'SEM-NB'
          when utm_campaign like 'TXN1%' and split(utm_campaign, '_')[3] = 'DIS' then 6 --'Display'
          when  utm_campaign like 'TXN1%' and split(utm_campaign, '_')[3] = 'SEMNB' then 5--'SEM-NB'
          when utm_campaign like 'TXN1%' and split(utm_campaign, '_')[3] = 'VID' then 4
          when utm_campaign like '%always_on%' then 4
          when utm_campaign like '%SEMC %' then 4
          -- when campaign_type = 'No Campaign - Referred' then 'Referral'
          when campaign_type in ('groupon-header-app1', 'groupon-header1', 'groupon-header-mweb1', 'groupon-footer') then 2 --'Groupon Header & Footer'
          when campaign_type in ('Google - Brand','Google - Remarketing','Google Brand - Advertising','Google Brand - Business','Google Brand - Contact Merchant','Google Brand - How To Business','Google Brand - Join','Google Brand - Merchant Misc','Google Brand - Number') then 4 --'SEM-Brand'
          -- when campaign_type = 'Direct' then 'Direct'
          -- when campaign_type in ('Facebook F&D','Facebook HBW','Social Retargeting', 'Facebook Silver Lookalike') then 'Display'
          when campaign_type in ('Bing - Competitor','Google - Competitor','Google - Non-Brand','Google NB - Advertise','Google NB - Free Advertising','Google NB - Promote','Google NB - SB-Adv') then 5 -- 'SEM-NB'
          -- when campaign_type = 'Phone Bank - No Campaign' then 'Phone Bank'
          -- when campaign_type = 'Referral' then 'Referral'
          when campaign_type in ('Living Social','Merchant Blog/SBRC','Merchant Newsletter','Organic','Print') then 3 --'SEO'
          else 1 -- 'Direct / Referral / Other'
       end campaign_group
      , case when utm_campaign like 'TXN1%' then 1 else 0 end pd_mm_relaunch_flag
      , split(utm_campaign, '_')[0] as mktg_txny_version
      , split(utm_campaign, '_')[1] as mktg_country
      , split(utm_campaign, '_')[2] as mktg_test_division
      , split(utm_campaign, '_')[3] as mktg_traffic_source
      , split(utm_campaign, '_')[4] as mktg_audience
      , split(utm_campaign, '_')[5] as mktg_sem_type
      , split(utm_campaign, '_')[6] as mktg_platform
      , split(utm_campaign, '_')[7] as mktg_creative
    from grp_gdoop_bizops_db.sm_ss_unique_bcookie_utm
;


 create table grp_gdoop_bizops_db.np_ss_bcookies stored as orc as
  select
    distinct get_json_object(history_data,'$.restricted.bCookie') bcookie
    , mkt.accountid
    , mkt.createddate as event_date
  from grp_gdoop_sup_analytics_db.sm_w2l_mktg_acct_attrib mkt
  left join user_edwprod.dim_merchant dm
    on mkt.accountid = dm.salesforce_account_id
  left join (select * from grp_gdoop_sup_analytics_db.metro_history_event where event_type = 'MERCHANT_CREATION') he
    on get_json_object(history_data, '$.additionalInfo.response.contact.merchants[0].id') = dm.merchant_uuid
      and date_format(he.event_date, 'yyyy-MM-dd') = date_format(mkt.createddate, 'yyyy-MM-dd')
  where mkt.createddate > '2020-01-01';

 
create table grp_gdoop_bizops_db.np_ss_unique_bcookie_utm stored as orc as
  select
    a.*
    , case
        when lower(utm_campaign) = 'other' then 'No Campaign - Referred' else case
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
            else case
                when cast(createddate as date) <= '2017-04-01' then case
                    when (sem_partner like '%ggl%' and sem_brand like '%nbr%') then 'Google - Non-Brand'
                    when (sem_partner like '%ggl%' and sem_brand like '%ybr%') then 'Google - Brand'
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
                    else 'Other' end
                else case
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
                    else 'Other' end
            end
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
    from grp_gdoop_bizops_db.np_ss_bcookies a 
    join user_groupondw.bld_events b
      on a.bcookie = b.bcookie
    where b.dt > '2020-01-01'
      and b.page_type in ('User', 'business_info')
      and b.dt between date_sub(a.event_date, 30) and date_add(a.event_date, 1)
  ) a;
 
 
--lead attritbution
---gets the bcookie from merchant_creation event with leads attribution above happened
---bcookie level utms 
   
set hive.exec.dynamic.partition.mode=nonstrict;set hive.exec.max.dynamic.partitions=2048;set hive.exec.max.dynamic.partitions.pernode=256;
set hive.auto.convert.join=true;set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=100000000;set hive.cbo.enable=true;
set hive.stats.fetch.column.stats=true;set hive.stats.fetch.partition.stats=true;
set hive.merge.tezfiles=true;set hive.merge.smallfiles.avgsize=128000000;set hive.merge.size.per.task=128000000;
set hive.tez.container.size=8192;set hive.tez.java.opts=-Xmx6000M;set hive.groupby.orderby.position.alias=true;
set hive.exec.parallel=true;




use grp_gdoop_bizops_db;
drop table if exists sm_w2l_mktg_acct_attrib purge;
create table sm_w2l_mktg_acct_attrib as
  select
    convertedaccountid as accountid
    , date_format(c2.createddate,'yyyy-MM-dd') as createddate
    , case when country = 'GB' then 'UK' else country end as country_code
    , campaign_name__c
    , campaign_type
    ,  case
        when campaign_name__c like 'TXN1%' and split(campaign_name__c, '_')[3] = 'SEMB' then 'Google-SEM'-- made a change should be 4
      when campaign_name__c like 'TXN1%' and split(campaign_name__c, '_')[3] = 'SEMN' then  'Google-SEM-NB'
        when campaign_name__c like 'TXN1%' and  campaign_name__c  like '%DIS_GEN__FB%' then 'FB-Display'
          when campaign_name__c like 'TXN1%'  and campaign_name__c  like '% DIS_GEN_AMZN%' then 'AMZON-Display' 
         when campaign_name__c like 'TXN1%' and split(campaign_name__c, '_')[3] = 'VID' then 'MNTN'
        when campaign_name__c like 'TXN1%' and split(campaign_name__c, '_')[3] = 'GPMC' then 'GPMC'
        when campaign_name__c like 'TXN1%' and split(campaign_name__c, '_')[3] = 'SEMNB' then 'SEM-NB'
        when campaign_name__c like '%always_on%' then 'SEM-Brand'
         when campaign_name__c like 'TXN1%' and campaign_name__c  like '%candaceholyfield_Gen__FB%' then 'Influencer_fb'
  when campaign_name__c like 'TXN1%' and campaign_name__c  like '%candaceholyfield_Gen__INS%' then 'Influencer_insta'
        -- when campaign_type = 'No Campaign - Referred' then 'Referral'
        when campaign_type in ('groupon-header-app1', 'groupon-header1', 'groupon-header-mweb1', 'groupon-footer') then 'Groupon Header & Footer'
        when campaign_type in 
           ('Google - Brand','Google - Remarketing','Google Brand - Advertising','Google Brand - Business','Google Brand - Contact Merchant','Google Brand - How To Business','Google Brand - Join',
                'Google Brand - Merchant Misc','Google Brand - Number') then 'SEM-Brand'
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
  from (
    select
      c1.*
      , case
          when leadsource like '%Phone Bank%' then 'Phone Bank- No Campaign'
          when lower(campaign_name__c) = 'other' then 'No Campaign - Referred' else case
              when lower(campaign_name__c) in ('groupon-header-app1', 'groupon-header1', 'groupon-header-mweb1', 'groupon-footer') then lower(campaign_name__c)
              when (lower(campaign_name__c) like '%us_un_som_fac_tim_lb_rr_cbp%' and lower(campaign_name__c) like '%fdp%') then 'Facebook F&D'
              when (lower(campaign_name__c) like '%us_un_som_fac_tim_lb_rr_cbp%' and lower(campaign_name__c) like '%hbp%') then 'Facebook HBW'
              when lower(campaign_name__c) like '%us_un_som_fac_tim_lb_rr_cbp_chw_sy_v30%' then 'Social Retargeting'
              when lower(campaign_name__c) like '%d*merchant-fb%' then 'Facebook_old'
              when lower(campaign_name__c) like '%g*gmail-ads%' then  'GMail Ads'
              when lower(campaign_name__c) like '%d*gw-dx-dis%' then  'DataXu'
              when lower(campaign_name__c) like '%groupon%' then 'Referral'
              when lower(campaign_name__c) like '%livingsocial_ib%' then 'Living Social'
              when lower(campaign_name__c) like '%delivery-takeout-lp%' then 'Delivery Takeout'
              when campaign_name__c = '50_DLS' then 'Referral'
              when campaign_name__c = '50' then 'Referral'
              when (lower(campaign_name__c) like '%grouponworks%' and lower(campaign_name__c) like '%social%') then 'Social'
              when lower(campaign_name__c) like '%merchant-retargeting%' then 'Merchant Retargeting'
              when lower(campaign_name__c) like '%merchant-stream%' then 'Yahoo Stream Ads'
              when lower(campaign_name__c) like '%d*merchant-gsp%' then 'Gmail Sponsored Promotions'
              --when lower(campaign_name__c) like '%biz_page%' then 'Biz Pages'
              when lower(campaign_name__c) like '%blog%' or lower(campaign_name__c) like '%merchant_blog%' or lower(campaign_name__c) like '%merchant_article%' or lower(campaign_name__c) like '%merchant-blog-how-to-sell-post%' or lower(campaign_name__c) like '%merchant-blog-sidebar%' then 'Merchant Blog/SBRC'
              when lower(campaign_name__c) like '%merchantnl%' or lower(campaign_name__c) like '%june2014%' or lower(campaign_name__c) like '%july2014%' or lower(campaign_name__c) like '%august2014%' or lower(campaign_name__c) like '%september2014%' or lower(campaign_name__c) like '%october2014%' or lower(campaign_name__c) like '%november2014%' or lower(campaign_name__c) like '%december2014%' or lower(campaign_name__c) like '%january2015%' or lower(campaign_name__c) like '%feb2015%' or lower(campaign_name__c) like '%mar2015%' or lower(campaign_name__c) like '%apr2015%' or lower(campaign_name__c) like '%may2015%' or lower(campaign_name__c) like '%june2015%' or lower(campaign_name__c) like '%july2015%' or lower(campaign_name__c) like '%august2015%' or lower(campaign_name__c) like '%sept2015%' or lower(campaign_name__c) like '%oct2015%' or lower(campaign_name__c) like '%nov2015%' or lower(campaign_name__c) like '%dec2015%' or lower(campaign_name__c) like '%jan2016%' then 'Merchant Newsletter'
              when lower(campaign_name__c) like '%print%' or lower(campaign_name__c) like '%nra2016%' or lower(campaign_name__c) like '%osr-cards%' or lower(campaign_name__c) like '%hbw-2016%' or lower(campaign_name__c) like '%austin-promo-16%' or lower(campaign_name__c) like '%cultural-institutions-2015%' or lower(campaign_name__c) like '%ttd-cultural-institutions%' or lower(campaign_name__c) like '%ttd-culture-2016%' or lower(campaign_name__c) like '%ttd-culture-2016%' or lower(campaign_name__c) like '%ttd-activities-2016%' or lower(campaign_name__c) like '%activities-2015%' or lower(campaign_name__c) like '%events-2015%' or lower(campaign_name__c) like '%astc-promo-16%' then 'Print'
              when lower(campaign_name__c) like '%grouponworks_social%' then 'Social'
              --when lower(campaign_name__c) like '%goods%' then 'Goods'
              --when lower(campaign_name__c) like '%g1%' then 'G1'
              when lower(campaign_name__c) like '%occasions_sponsor%' then 'Occasions_sponsor'
              when lower(campaign_name__c) like '%occasions%' then 'Occasions'
              --when lower(campaign_name__c) like '%collections%' then 'Collections'
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
              else case
                  when cast(createddate as date) <= '2017-04-01' then case
                      when (sem_partner like '%ggl%' and sem_brand like '%nbr%') then 'Google - Non-Brand'
                      when (sem_partner like '%ggl%' and sem_brand like '%ybr%') then 'Google - Brand'
                      when (sem_partner like '%ggl%' and sem_brand like '%merchant-competitors%') then 'Google - Competitor'
                        when (sem_partner like '%bng%' and sem_brand like '%nbr%') then 'Bing - Non-Brand'
                      when (sem_partner like '%bng%' and sem_brand like '%ybr%') then 'Bing - Brand'
                      when (sem_partner like '%bng%' and sem_brand like '%cbr%') then 'Bing - Competitor'
                      when lower(split(campaign_name__c,'_')[1]) like 'g1%' then 'G1'
                      when lower(split(campaign_name__c,'_')[1]) like 'goods%' then 'Goods'
                      when lower(split(campaign_name__c,'_')[1]) like 'occasion%' then 'Occasions'
                      when lower(split(campaign_name__c,'_')[1]) like 'getaways%' then 'Getaways'
                      when lower(split(campaign_name__c,'_')[1]) like 'reserve%' then 'Reserve'
                      when lower(split(campaign_name__c,'_')[1]) like 'collection%' then 'Collections'
                      when lower(split(campaign_name__c,'_')[1]) like 'payments%' then 'Payments'
                      when lower(campaign_name__c) like 'k*%' then 'Google - Non-Brand'
                      when lower(campaign_name__c) like '%merchant_blog%' then 'Merchant Blog'
                      when lower(campaign_name__c) like '%grouponworks_social%' then 'Social'
                      when (lower(campaign_name__c) like '%ggl%' and lower(campaign_name__c) like '%nbr%') then 'Google - Non-Brand'
                      when (lower(campaign_name__c) like '%ggl%' and lower(campaign_name__c) like '%ybr%') then 'Google - Brand'
                      when (lower(campaign_name__c) like '%ggl%' and lower(campaign_name__c) like '%merchant-competitors%') then 'Google - Competitor'
                      when (lower(campaign_name__c) like '%ggl%' and lower(campaign_name__c) like '%rmk%') then 'Google - Remarketing'
                      else 'Other' end
                  else case
                      when (lower(campaign_name__c) like '%free_advertising%') then 'Google NB - Free Advertising'
                      when (lower(campaign_name__c) like '%sb_adv%') then 'Google NB - SB-Adv'
                      when (lower(campaign_name__c) like '%promote%') then 'Google NB - Promote'
                      when (lower(campaign_name__c) like '%advertise%') then 'Google NB - Advertise'
                      when (lower(campaign_name__c) like '%number%') then 'Google Brand - Number'
                      when (lower(campaign_name__c) like '%contact_merchant%') then 'Google Brand - Contact Merchant'
                      when (lower(campaign_name__c) like '%advertising%') then 'Google Brand - Advertising'
                      when (lower(campaign_name__c) like '%how_to_business%') then 'Google Brand - How To Business'
                      when (lower(campaign_name__c) like '%business%') then 'Google Brand - Business'
                      when (lower(campaign_name__c) like '%join%') then 'Google Brand - Join'
                      when (lower(campaign_name__c) like '%merchant_misc%') then 'Google Brand - Merchant Misc'
                      when (sem_partner like '%ggl%' and sem_brand like '%merchant-competitors%') then 'Google - Competitor'
                      when (sem_partner like '%bng%' and sem_brand like '%nbr%') then 'Bing - Non-Brand'
                      when (sem_partner like '%bng%' and sem_brand like '%ybr%') then 'Bing - Brand'
                      when (sem_partner like '%bng%' and sem_brand like '%cbr%') then 'Bing - Competitor'
                      when lower(split(campaign_name__c,'_')[1]) like 'g1%' then 'G1'
                      when lower(split(campaign_name__c,'_')[1]) like 'goods%' then 'Goods'
                      when lower(split(campaign_name__c,'_')[1]) like 'occasion%' then 'Occasions'
                      when lower(split(campaign_name__c,'_')[1]) like 'getaways%' then 'Getaways'
                      when lower(split(campaign_name__c,'_')[1]) like 'reserve%' then 'Reserve'
                      when lower(split(campaign_name__c,'_')[1]) like 'collection%' then 'Collections'
                      when lower(split(campaign_name__c,'_')[1]) like 'payments%' then 'Payments'
                      when lower(campaign_name__c) like 'k*%' then 'Google - Non-Brand'
                      when lower(campaign_name__c) like '%merchant_blog%' then 'Merchant Blog'
                      when lower(campaign_name__c) like '%grouponworks_social%' then 'Social'
                      when (lower(campaign_name__c) like '%ggl%' and lower(campaign_name__c) like '%nbr%') then 'Google - Non-Brand'
                      when (lower(campaign_name__c) like '%ggl%' and lower(campaign_name__c) like '%ybr%') then 'Google - Brand'
                      when (lower(campaign_name__c) like '%ggl%' and lower(campaign_name__c) like '%merchant-competitors%') then 'Google - Competitor'
                      when (lower(campaign_name__c) like '%ggl%' and lower(campaign_name__c) like '%rmk%') then 'Google - Remarketing'
                      else 'Other' end
              end
          end
        end as campaign_type
    from (
      select
        *
        , case when length(campaign_name__c)-length(translate(campaign_name__c,'_','')) > 11 then 1 else 0 end campaign_new_format
        , case when length(campaign_name__c)-length(translate(campaign_name__c,'_','')) > 11 then lower(split(campaign_name__c,'_')[4]) else null end sem_partner
        , case when length(campaign_name__c)-length(translate(campaign_name__c,'_','')) > 11 then lower(split(campaign_name__c,'_')[10]) else null end sem_brand
      from user_groupondw.sf_lead
       where leadsource in ('Metro - Self Service', 'MIA - Inbound')
        and createddate > '2019-01-01'
      ) c1
    ) c2
  left join dwh_base_sec_view.sf_account sfa
    on sfa.account_id_18 = c2.convertedaccountid
  join user_dw.v_dim_day dd
    on date_format(c2.createddate, 'yyyy-MM-dd') = dd.day_rw
  join user_dw.v_dim_week wk
    on dd.week_key = wk.week_key;
    
   use grp_gdoop_bizops_db;
   drop table if exists sm_ss_bcookies purge;
   create table sm_ss_bcookies stored as orc as
  select
    distinct get_json_object(history_data,'$.restricted.bCookie') bcookie
    , mkt.accountid
    , mkt.createddate as event_date
  from sm_w2l_mktg_acct_attrib mkt
  left join user_edwprod.dim_merchant dm
    on mkt.accountid = dm.salesforce_account_id
  left join (select * from grp_gdoop_sup_analytics_db.metro_history_event where event_type = 'MERCHANT_CREATION') he
    on get_json_object(history_data, '$.additionalInfo.response.contact.merchants[0].id') = dm.merchant_uuid
      and date_format(he.event_date, 'yyyy-MM-dd') = date_format(mkt.createddate, 'yyyy-MM-dd')
  where mkt.createddate > '2020-01-01';
  
 
use grp_gdoop_bizops_db;
drop table if exists np_bld_event_tmp purge;
create table np_bld_event_tmp stored as orc as
select
      distinct a.bcookie
      , utm_campaign
      , a.event_date as createddate
      , a.accountid
      , case when length(utm_campaign)-length(translate(utm_campaign,'_','')) > 11 then 1 else 0 end campaign_new_format
      , case when length(utm_campaign)-length(translate(utm_campaign,'_','')) > 11 then lower(split(utm_campaign,'_')[4]) else null end sem_partner
      , case when length(utm_campaign)-length(translate(utm_campaign,'_','')) > 11 then lower(split(utm_campaign,'_')[10]) else null end sem_brand
    from user_groupondw.bld_events as b
    join sm_ss_bcookies a
      on a.bcookie = b.bcookie
    where b.dt > '2020-01-01'
      and b.page_type in ('User', 'business_info')
      and b.dt between date_sub(a.event_date, 30) and date_add(a.event_date, 1);

use grp_gdoop_bizops_db;
 drop table if exists sm_ss_unique_bcookie_utm purge;
 create table sm_ss_unique_bcookie_utm stored as orc as
  select
    a.*
    , case
        when lower(utm_campaign) = 'other' then 'No Campaign - Referred' else case
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
            else case
                when cast(createddate as date) <= '2017-04-01' then case
                    when (sem_partner like '%ggl%' and sem_brand like '%nbr%') then 'Google - Non-Brand'
                    when (sem_partner like '%ggl%' and sem_brand like '%ybr%') then 'Google - Brand'
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
                    else 'Other' end
                else case
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
                    else 'Other' end
            end
        end
      end as campaign_type
  from np_bld_event_tmp a;
  
 
 use grp_gdoop_bizops_db;
 drop table if exists sm_ss_assisted_tmp purge;
 create table sm_ss_assisted_tmp stored as orc as
  select
    bcookie
    , accountid
    , max(campaign_group) as asst_attrib_cmpn
  from (
    select
      bcookie
      , utm_campaign
      , accountid
      , campaign_type
      -- campaign groups are lebele with the folowing:
      -- 6: Display, 5: SEMNB, 4: SEMB, 3: SEO, 2: GRPN H&F, 1: Direct / Referral / Other
      , case
          when utm_campaign like 'TXN1%' and split(utm_campaign, '_')[3] = 'SEMB' then 4 --'SEM-Brand'
          when utm_campaign like 'TXN1%' and split(utm_campaign, '_')[3] = 'GPMC' then 4 --'SEM-Brand'
          when utm_campaign like 'TXN1%' and split(utm_campaign, '_')[3] = 'SEMN' then 5 --'SEM-NB'
          when utm_campaign like 'TXN1%' and split(utm_campaign, '_')[3] = 'DIS' then 6 --'Display'
          when  utm_campaign like 'TXN1%' and split(utm_campaign, '_')[3] = 'SEMNB' then 5--'SEM-NB'
          when utm_campaign like 'TXN1%' and split(utm_campaign, '_')[3] = 'VID' then 4
           when utm_campaign like '%always_on%' then 4
          -- when campaign_type = 'No Campaign - Referred' then 'Referral'
          when campaign_type in ('groupon-header-app1', 'groupon-header1', 'groupon-header-mweb1', 'groupon-footer') then 2 --'Groupon Header & Footer'
          when campaign_type in ('Google - Brand','Google - Remarketing','Google Brand - Advertising','Google Brand - Business','Google Brand - Contact Merchant','Google Brand - How To Business','Google Brand - Join','Google Brand - Merchant Misc','Google Brand - Number') then 4 --'SEM-Brand'
          -- when campaign_type = 'Direct' then 'Direct'
          -- when campaign_type in ('Facebook F&D','Facebook HBW','Social Retargeting', 'Facebook Silver Lookalike') then 'Display'
          when campaign_type in ('Bing - Competitor','Google - Competitor','Google - Non-Brand','Google NB - Advertise','Google NB - Free Advertising','Google NB - Promote','Google NB - SB-Adv') then 5 -- 'SEM-NB'
          -- when campaign_type = 'Phone Bank - No Campaign' then 'Phone Bank'
          -- when campaign_type = 'Referral' then 'Referral'
          when campaign_type in ('Living Social','Merchant Blog/SBRC','Merchant Newsletter','Organic','Print') then 3 --'SEO'
          else 1 -- 'Direct / Referral / Other'
       end campaign_group
      , case when utm_campaign like 'TXN1%' then 1 else 0 end pd_mm_relaunch_flag
      , split(utm_campaign, '_')[0] as mktg_txny_version
      , split(utm_campaign, '_')[1] as mktg_country
      , split(utm_campaign, '_')[2] as mktg_test_division
      , split(utm_campaign, '_')[3] as mktg_traffic_source
      , split(utm_campaign, '_')[4] as mktg_audience
      , split(utm_campaign, '_')[5] as mktg_sem_type
      , split(utm_campaign, '_')[6] as mktg_platform
      , split(utm_campaign, '_')[7] as mktg_creative
    from sm_ss_unique_bcookie_utm
  ) a
  group by 1,2;
  
 
 use grp_gdoop_bizops_db;
 drop table if exists sm_w2l_mktg_acct_attrib_tmp purge;
 create table sm_w2l_mktg_acct_attrib_tmp as
  select
    convertedaccountid as accountid
    , date_format(c2.createddate,'yyyy-MM-dd') as createddate
    , case when country = 'GB' then 'UK' else country end as country_code
    , campaign_name__c
    , campaign_type
    , case
        when campaign_name__c like 'TXN1%' and split(campaign_name__c, '_')[3] = 'SEMB' then 'Google-SEM'-- made a change should be 4
      when campaign_name__c like 'TXN1%' and split(campaign_name__c, '_')[3] = 'SEMN' then  'Google-SEM-NB'
        when campaign_name__c like 'TXN1%' and campaign_name__c  like '%DIS_GEN__FB%' then 'FB-Display'
         when campaign_name__c like 'TXN1%' and campaign_name__c  like '% DIS_GEN_AMZN%'  then 'AMZON-Display'
         when campaign_name__c like 'TXN1%' and split(campaign_name__c, '_')[3] = 'VID' then 'MNTN'
        when campaign_name__c like 'TXN1%' and split(campaign_name__c, '_')[3] = 'GPMC' then 'GPMC'
        when campaign_name__c like 'TXN1%' and split(campaign_name__c, '_')[3] = 'SEMNB' then 'SEM-NB'
        when campaign_name__c like '%always_on%' then 'SEM-Brand'
         when campaign_name__c like 'TXN1%' and campaign_name__c  like '%candaceholyfield_Gen__FB%' then 'Influencer_fb'
  when campaign_name__c like 'TXN1%' and campaign_name__c  like '%candaceholyfield_Gen__INS%' then 'Influencer_insta'
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
  from (
    select
      c1.*
      , case
          when leadsource like '%Phone Bank%' then 'Phone Bank- No Campaign'
          when lower(campaign_name__c) = 'other' then 'No Campaign - Referred' else case
              when lower(campaign_name__c) in ('groupon-header-app1', 'groupon-header1', 'groupon-header-mweb1', 'groupon-footer') then lower(campaign_name__c)
              when (lower(campaign_name__c) like '%us_un_som_fac_tim_lb_rr_cbp%' and lower(campaign_name__c) like '%fdp%') then 'Facebook F&D'
              when (lower(campaign_name__c) like '%us_un_som_fac_tim_lb_rr_cbp%' and lower(campaign_name__c) like '%hbp%') then 'Facebook HBW'
              when lower(campaign_name__c) like '%us_un_som_fac_tim_lb_rr_cbp_chw_sy_v30%' then 'Social Retargeting'
              when lower(campaign_name__c) like '%d*merchant-fb%' then 'Facebook_old'
              when lower(campaign_name__c) like '%g*gmail-ads%' then  'GMail Ads'
              when lower(campaign_name__c) like '%d*gw-dx-dis%' then  'DataXu'
              when lower(campaign_name__c) like '%groupon%' then 'Referral'
              when lower(campaign_name__c) like '%livingsocial_ib%' then 'Living Social'
              when lower(campaign_name__c) like '%delivery-takeout-lp%' then 'Delivery Takeout'
              when campaign_name__c = '50_DLS' then 'Referral'
              when campaign_name__c = '50' then 'Referral'
              when (lower(campaign_name__c) like '%grouponworks%' and lower(campaign_name__c) like '%social%') then 'Social'
              when lower(campaign_name__c) like '%merchant-retargeting%' then 'Merchant Retargeting'
              when lower(campaign_name__c) like '%merchant-stream%' then 'Yahoo Stream Ads'
              when lower(campaign_name__c) like '%d*merchant-gsp%' then 'Gmail Sponsored Promotions'
              --when lower(campaign_name__c) like '%biz_page%' then 'Biz Pages'
              when lower(campaign_name__c) like '%blog%' or lower(campaign_name__c) like '%merchant_blog%' or lower(campaign_name__c) like '%merchant_article%' or lower(campaign_name__c) like '%merchant-blog-how-to-sell-post%' or lower(campaign_name__c) like '%merchant-blog-sidebar%' then 'Merchant Blog/SBRC'
              when lower(campaign_name__c) like '%merchantnl%' or lower(campaign_name__c) like '%june2014%' or lower(campaign_name__c) like '%july2014%' or lower(campaign_name__c) like '%august2014%' or lower(campaign_name__c) like '%september2014%' or lower(campaign_name__c) like '%october2014%' or lower(campaign_name__c) like '%november2014%' or lower(campaign_name__c) like '%december2014%' or lower(campaign_name__c) like '%january2015%' or lower(campaign_name__c) like '%feb2015%' or lower(campaign_name__c) like '%mar2015%' or lower(campaign_name__c) like '%apr2015%' or lower(campaign_name__c) like '%may2015%' or lower(campaign_name__c) like '%june2015%' or lower(campaign_name__c) like '%july2015%' or lower(campaign_name__c) like '%august2015%' or lower(campaign_name__c) like '%sept2015%' or lower(campaign_name__c) like '%oct2015%' or lower(campaign_name__c) like '%nov2015%' or lower(campaign_name__c) like '%dec2015%' or lower(campaign_name__c) like '%jan2016%' then 'Merchant Newsletter'
              when lower(campaign_name__c) like '%print%' or lower(campaign_name__c) like '%nra2016%' or lower(campaign_name__c) like '%osr-cards%' or lower(campaign_name__c) like '%hbw-2016%' or lower(campaign_name__c) like '%austin-promo-16%' or lower(campaign_name__c) like '%cultural-institutions-2015%' or lower(campaign_name__c) like '%ttd-cultural-institutions%' or lower(campaign_name__c) like '%ttd-culture-2016%' or lower(campaign_name__c) like '%ttd-culture-2016%' or lower(campaign_name__c) like '%ttd-activities-2016%' or lower(campaign_name__c) like '%activities-2015%' or lower(campaign_name__c) like '%events-2015%' or lower(campaign_name__c) like '%astc-promo-16%' then 'Print'
              when lower(campaign_name__c) like '%grouponworks_social%' then 'Social'
              --when lower(campaign_name__c) like '%goods%' then 'Goods'
              --when lower(campaign_name__c) like '%g1%' then 'G1'
              when lower(campaign_name__c) like '%occasions_sponsor%' then 'Occasions_sponsor'
              when lower(campaign_name__c) like '%occasions%' then 'Occasions'
              --when lower(campaign_name__c) like '%collections%' then 'Collections'
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
              else case
                  when cast(createddate as date) <= '2017-04-01' then case
                      when (sem_partner like '%ggl%' and sem_brand like '%nbr%') then 'Google - Non-Brand'
                      when (sem_partner like '%ggl%' and sem_brand like '%ybr%') then 'Google - Brand'
                      when (sem_partner like '%ggl%' and sem_brand like '%merchant-competitors%') then 'Google - Competitor'
                        when (sem_partner like '%bng%' and sem_brand like '%nbr%') then 'Bing - Non-Brand'
                      when (sem_partner like '%bng%' and sem_brand like '%ybr%') then 'Bing - Brand'
                      when (sem_partner like '%bng%' and sem_brand like '%cbr%') then 'Bing - Competitor'
                      when lower(split(campaign_name__c,'_')[1]) like 'g1%' then 'G1'
                      when lower(split(campaign_name__c,'_')[1]) like 'goods%' then 'Goods'
                      when lower(split(campaign_name__c,'_')[1]) like 'occasion%' then 'Occasions'
                      when lower(split(campaign_name__c,'_')[1]) like 'getaways%' then 'Getaways'
                      when lower(split(campaign_name__c,'_')[1]) like 'reserve%' then 'Reserve'
                      when lower(split(campaign_name__c,'_')[1]) like 'collection%' then 'Collections'
                      when lower(split(campaign_name__c,'_')[1]) like 'payments%' then 'Payments'
                      when lower(campaign_name__c) like 'k*%' then 'Google - Non-Brand'
                      when lower(campaign_name__c) like '%merchant_blog%' then 'Merchant Blog'
                      when lower(campaign_name__c) like '%grouponworks_social%' then 'Social'
                      when (lower(campaign_name__c) like '%ggl%' and lower(campaign_name__c) like '%nbr%') then 'Google - Non-Brand'
                      when (lower(campaign_name__c) like '%ggl%' and lower(campaign_name__c) like '%ybr%') then 'Google - Brand'
                      when (lower(campaign_name__c) like '%ggl%' and lower(campaign_name__c) like '%merchant-competitors%') then 'Google - Competitor'
                      when (lower(campaign_name__c) like '%ggl%' and lower(campaign_name__c) like '%rmk%') then 'Google - Remarketing'
                      else 'Other' end
                  else case
                      when (lower(campaign_name__c) like '%free_advertising%') then 'Google NB - Free Advertising'
                      when (lower(campaign_name__c) like '%sb_adv%') then 'Google NB - SB-Adv'
                      when (lower(campaign_name__c) like '%promote%') then 'Google NB - Promote'
                      when (lower(campaign_name__c) like '%advertise%') then 'Google NB - Advertise'
                      when (lower(campaign_name__c) like '%number%') then 'Google Brand - Number'
                      when (lower(campaign_name__c) like '%contact_merchant%') then 'Google Brand - Contact Merchant'
                      when (lower(campaign_name__c) like '%advertising%') then 'Google Brand - Advertising'
                      when (lower(campaign_name__c) like '%how_to_business%') then 'Google Brand - How To Business'
                      when (lower(campaign_name__c) like '%business%') then 'Google Brand - Business'
                      when (lower(campaign_name__c) like '%join%') then 'Google Brand - Join'
                      when (lower(campaign_name__c) like '%merchant_misc%') then 'Google Brand - Merchant Misc'
                      when (sem_partner like '%ggl%' and sem_brand like '%merchant-competitors%') then 'Google - Competitor'
                      when (sem_partner like '%bng%' and sem_brand like '%nbr%') then 'Bing - Non-Brand'
                      when (sem_partner like '%bng%' and sem_brand like '%ybr%') then 'Bing - Brand'
                      when (sem_partner like '%bng%' and sem_brand like '%cbr%') then 'Bing - Competitor'
                      when lower(split(campaign_name__c,'_')[1]) like 'g1%' then 'G1'
                      when lower(split(campaign_name__c,'_')[1]) like 'goods%' then 'Goods'
                      when lower(split(campaign_name__c,'_')[1]) like 'occasion%' then 'Occasions'
                      when lower(split(campaign_name__c,'_')[1]) like 'getaways%' then 'Getaways'
                      when lower(split(campaign_name__c,'_')[1]) like 'reserve%' then 'Reserve'
                      when lower(split(campaign_name__c,'_')[1]) like 'collection%' then 'Collections'
                      when lower(split(campaign_name__c,'_')[1]) like 'payments%' then 'Payments'
                      when lower(campaign_name__c) like 'k*%' then 'Google - Non-Brand'
                      when lower(campaign_name__c) like '%merchant_blog%' then 'Merchant Blog'
                      when lower(campaign_name__c) like '%grouponworks_social%' then 'Social'
                      when (lower(campaign_name__c) like '%ggl%' and lower(campaign_name__c) like '%nbr%') then 'Google - Non-Brand'
                      when (lower(campaign_name__c) like '%ggl%' and lower(campaign_name__c) like '%ybr%') then 'Google - Brand'
                      when (lower(campaign_name__c) like '%ggl%' and lower(campaign_name__c) like '%merchant-competitors%') then 'Google - Competitor'
                      when (lower(campaign_name__c) like '%ggl%' and lower(campaign_name__c) like '%rmk%') then 'Google - Remarketing'
                      else 'Other' end
              end
          end
        end as campaign_type
    from (
      select
        *
        , case when length(campaign_name__c)-length(translate(campaign_name__c,'_','')) > 11 then 1 else 0 end campaign_new_format
        , case when length(campaign_name__c)-length(translate(campaign_name__c,'_','')) > 11 then lower(split(campaign_name__c,'_')[4]) else null end sem_partner
        , case when length(campaign_name__c)-length(translate(campaign_name__c,'_','')) > 11 then lower(split(campaign_name__c,'_')[10]) else null end sem_brand
      from user_groupondw.sf_lead
    where leadsource in ('Metro - Self Service', 'MIA - Inbound')
        and createddate > '2019-01-01'
      ) c1
    ) c2
  left join dwh_base_sec_view.sf_account sfa
    on sfa.account_id_18 = c2.convertedaccountid
  join user_dw.v_dim_day dd
    on date_format(c2.createddate, 'yyyy-MM-dd') = dd.day_rw
  join user_dw.v_dim_week wk
    on dd.week_key = wk.week_key;
    
   
   drop table if exists sm_w2l_mktg_acct_attrib purge;
   create table sm_w2l_mktg_acct_attrib stored as orc as
  select
    a.*
    ,case when asst_attrib_cmpn = 6 or a.campaign_group = 'FB-Display' then 'Display'
       when asst_attrib_cmpn = 6 or a.campaign_group = 'AMZON-Display' then 'Display'
        when asst_attrib_cmpn = 5 or a.campaign_group = 'Google-SEM-NB' then 'SEM-NB'
        when asst_attrib_cmpn = 4 or a.campaign_group = 'Google-SEM' then 'SEM-Brand'
         when asst_attrib_cmpn = 4 or a.campaign_group = 'GPMC' then 'SEM-Brand'
           when asst_attrib_cmpn = 4 or a.campaign_group = 'MNTN' then 'SEM-Brand'
        when asst_attrib_cmpn = 3 or a.campaign_group = 'SEO' then 'SEO'
        when asst_attrib_cmpn = 2 or a.campaign_group = 'Groupon Header & Footer' then 'Groupon Header & Footer'
        when asst_attrib_cmpn = 1 or a.campaign_group = 'Direct / Referral / Other' then 'Direct / Referral / Other'
      else -1 end highest_touch
    , ast.bcookie
  from sm_w2l_mktg_acct_attrib_tmp a
  left join sm_ss_assisted_tmp ast
    on ast.accountid = a.accountid;
    
   
drop table if exists sm_ss_bcookies purge;
drop table if exists sm_ss_unique_bcookie_utm purge;
drop table if exists sm_ss_assisted_tmp purge;
drop table if exists sm_w2l_mktg_acct_attrib_tmp purge
    
    
    ---comparing case of sm_ss_assisted_tmp to get context
case
          when utm_campaign like 'TXN1%' and split(utm_campaign, '_')[3] = 'SEMB' then 4 --'SEM-Brand'
          when utm_campaign like 'TXN1%' and split(utm_campaign, '_')[3] = 'GPMC' then 4 --'SEM-Brand'
          when utm_campaign like 'TXN1%' and split(utm_campaign, '_')[3] = 'SEMN' then 5 --'SEM-NB'
          when utm_campaign like 'TXN1%' and split(utm_campaign, '_')[3] = 'DIS' then 6 --'Display'
          when  utm_campaign like 'TXN1%' and split(utm_campaign, '_')[3] = 'SEMNB' then 5--'SEM-NB'
          when utm_campaign like 'TXN1%' and split(utm_campaign, '_')[3] = 'VID' then 4
          when utm_campaign like '%always_on%' then 4
          when utm_campaign like '%SEMC %' then 4
          -- when campaign_type = 'No Campaign - Referred' then 'Referral'
          when campaign_type in ('groupon-header-app1', 'groupon-header1', 'groupon-header-mweb1', 'groupon-footer') then 2 --'Groupon Header & Footer'
          when campaign_type in ('Google - Brand','Google - Remarketing','Google Brand - Advertising','Google Brand - Business','Google Brand - Contact Merchant','Google Brand - How To Business','Google Brand - Join','Google Brand - Merchant Misc','Google Brand - Number') then 4 --'SEM-Brand'
          -- when campaign_type = 'Direct' then 'Direct'
          -- when campaign_type in ('Facebook F&D','Facebook HBW','Social Retargeting', 'Facebook Silver Lookalike') then 'Display'
          when campaign_type in ('Bing - Competitor','Google - Competitor','Google - Non-Brand','Google NB - Advertise','Google NB - Free Advertising','Google NB - Promote','Google NB - SB-Adv') then 5 -- 'SEM-NB'
          -- when campaign_type = 'Phone Bank - No Campaign' then 'Phone Bank'
          -- when campaign_type = 'Referral' then 'Referral'
          when campaign_type in ('Living Social','Merchant Blog/SBRC','Merchant Newsletter','Organic','Print') then 3 --'SEO'
          else 1 -- 'Direct / Referral / Other'
       end campaign_group
       
       
   drop table if exists sm_ss_bcookies purge;
   drop table if exists sm_ss_unique_bcookie_utm purge;
   drop table if exists sm_ss_assisted_tmp purge;
   drop table if exists sm_w2l_mktg_acct_attrib_tmp purge
--------------------------------------------------------------------------------------------------STEP 2: w2l_kpis
---use this to see the sem related things
/*comparing case statements
 * case
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
case
          when utm_campaign like 'TXN1%' and split(utm_campaign, '_')[3] = 'SEMB'  then 'Google-SEM'--- made a change should be 4
          when utm_campaign like 'TXN1%' and split(utm_campaign, '_')[3] = 'SEMN' then 'Google-SEM-NB'
          when utm_campaign like 'TXN1%' and  utm_campaign like '%DIS_GEN__FB%' then 'FB-Display'
          when utm_campaign like 'TXN1%'  and utm_campaign  like '% DIS_GEN_AMZN%' then 'AMZON-Display'
          when utm_campaign like 'TXN1%' and split(utm_campaign, '_')[3] = 'GPMC' then '  GPMC'
          when utm_campaign like 'TXN1%' and split(utm_campaign, '_')[3] = 'SEMNB' then 'SEM-NB'
          when utm_campaign like 'TXN1%' and split(utm_campaign, '_')[3] = 'VID' then 'MNTN'
          when utm_campaign like 'TXN1%' and split(utm_campaign, '_')[3] = 'candanceholyfield' then 'Influencer'
          when utm_campaign like '%always_on%' then 'SEM-Brand'
          when campaign_type = 'Square Marketplace' then campaign_type
          when page_path like '%/merchant/use-groupon%'then 'SEM-Brand'
          -- when campaign_type = "'Other' in SF" then 'Referral'
          when campaign_type in ('groupon-header-app1', 'groupon-header1', 'groupon-header-mweb1', 'groupon-footer', 'groupon-footer-banner', 'groupon-footer-signup') then 'Groupon Header & Footer'
          when utm_campaign in ('groupon-header-app1', 'groupon-header1', 'groupon-header-mweb1', 'groupon-footer', 'groupon-footer-banner', 'groupon-footer-signup', 'groupon-footer-hp') then 'Groupon Header & Footer'
          when campaign_type in ('Google - Brand','Google - Remarketing','Google Brand - Advertising','Google Brand - Business','Google Brand - Contact Merchant','Google Brand - How To Business','Google Brand - Join','Google Brand - Merchant Misc','Google Brand - Number') then 'SEM-Brand'
          -- when campaign_type = 'Direct' then 'Direct'
          -- when campaign_type in ('Facebook F&D','Facebook HBW','Social Retargeting', 'Facebook Silver Lookalike') then 'Display'
          when campaign_type in ('Bing - Competitor','Google - Competitor','Google - Non-Brand','Google NB - Advertise','Google NB - Free Advertising','Google NB - Promote','Google NB - SB-Adv') then 'SEM-NB'
          -- when campaign_type = 'Phone Bank - No Campaign' then 'Phone Bank'
          -- when campaign_type = 'Referral' then 'Referral'
          when campaign_type in ('Living Social','Merchant Blog/SBRC','Merchant Newsletter','Organic','Print') then 'SEO'
          -- when lower(utm_medium) like '%referral%' then 'Referral'
          -- when referrer_domain is null or referrer_domain = '' or referrer_domain = ' ' then 'Direct'
          -- else 'Referral'
          else 'Direct / Referral / Other'
       end campaign_group*/
       
       
set hive.exec.dynamic.partition.mode=nonstrict;set hive.exec.max.dynamic.partitions=2048;set hive.exec.max.dynamic.partitions.pernode=256;
set hive.auto.convert.join=true;set hive.auto.convert.join.noconditionaltask=true;set hive.auto.convert.join.noconditionaltask.size=100000000;
set hive.cbo.enable=true;set hive.stats.fetch.column.stats=true;set hive.stats.fetch.partition.stats=true;set hive.merge.tezfiles=true;
set hive.merge.smallfiles.avgsize=128000000;set hive.merge.size.per.task=128000000;set hive.tez.container.size=8192;set hive.tez.java.opts=-Xmx6000M;
set hive.groupby.orderby.position.alias=true;set hive.exec.parallel=true;add jar hdfs:///user/grp_gdoop_marketing_analytics/mktg-hive-udf.jar;
add jar hdfs:///user/grp_gdoop_marketing_analytics/scala-library-2.11.6.jar;
add jar hdfs:///user/grp_gdoop_marketing_analytics/traffic-source-lib_2.11-1.0.3.jar;
create temporary function TrafficSource as 'com.groupon.marketing.analytics.hive.udf.TrafficSourceUDF';


use grp_gdoop_bizops_db;
drop table if exists sm_w2l_mia_funnel;
create table sm_w2l_mia_funnel stored as orc as
  select
    a.dt
    , a.bcookie
    , a.user_uuid
    , a.updated_user_device_type
    , a.page_country
    , a.campaign_group
    , a.updated_traffic_source
    , a.traffic_source
    , a.traffic_sub_source
    , a.referrer_url
    , a.referrer_domain
    , a.utm_campaign
    , a.campaign_type
    , a.pd_mm_relaunch_flag
    , a.mktg_txny_version
    , a.mktg_country
    , a.mktg_test_division
    , a.mktg_traffic_source
    , a.mktg_audience
    , a.mktg_sem_type
    , a.mktg_platform
    , a.utm_medium
    , a.utm_source
    , a.mktg_creative
    , 'Overall' as highest_touch
    , 'Overall' as metal
    , case when sum(mia_form_view_v2) > 0 then 1 else 0 end as mia_form_view_v2
    , case when sum(mia_form_view) > 0 then 1 else 0 end as mia_form_view
    , case when sum(mia_success_ib_view) > 0 then 1 else 0 end as mia_success_ib_view
    , case when sum(mia_success_metro_view) > 0 then 1 else 0 end as mia_success_metro_view
    , case when sum(merchant_pg_view) > 0 then 1 else 0 end as merchant_pg_view
  from (
    select
      dt
      , bcookie
      , user_uuid
      , updated_user_device_type
      , updated_traffic_source
      , utm_campaign
      , utm_medium
      , utm_source
      , campaign_type
      , traffic_source
      , traffic_sub_source
      , referrer_url
      , referrer_domain
      , case
          when utm_campaign like 'TXN1%' and split(utm_campaign, '_')[3] = 'SEMB'  then 'Google-SEM'--- made a change should be 4
          when utm_campaign like 'TXN1%' and split(utm_campaign, '_')[3] = 'SEMN' then 'Google-SEM-NB'
           when utm_campaign like 'TXN1%' and  utm_campaign like '%DIS_GEN__FB%' then 'FB-Display'
            when utm_campaign like 'TXN1%'  and utm_campaign  like '% DIS_GEN_AMZN%' then 'AMZON-Display'
          when utm_campaign like 'TXN1%' and split(utm_campaign, '_')[3] = 'GPMC' then '  GPMC'
          when utm_campaign like 'TXN1%' and split(utm_campaign, '_')[3] = 'SEMNB' then 'SEM-NB'
          when utm_campaign like 'TXN1%' and split(utm_campaign, '_')[3] = 'VID' then 'MNTN'
          when utm_campaign like 'TXN1%' and split(utm_campaign, '_')[3] = 'candanceholyfield' then 'Influencer'
          when utm_campaign like '%always_on%' then 'SEM-Brand'
          when campaign_type = 'Square Marketplace' then campaign_type
          when page_path like '%/merchant/use-groupon%'then 'SEM-Brand'
          -- when campaign_type = "'Other' in SF" then 'Referral'
          when campaign_type in ('groupon-header-app1', 'groupon-header1', 'groupon-header-mweb1', 'groupon-footer', 'groupon-footer-banner', 'groupon-footer-signup') then 'Groupon Header & Footer'
          when utm_campaign in ('groupon-header-app1', 'groupon-header1', 'groupon-header-mweb1', 'groupon-footer', 'groupon-footer-banner', 'groupon-footer-signup', 'groupon-footer-hp') then 'Groupon Header & Footer'
          when campaign_type in ('Google - Brand','Google - Remarketing','Google Brand - Advertising','Google Brand - Business','Google Brand - Contact Merchant','Google Brand - How To Business','Google Brand - Join','Google Brand - Merchant Misc','Google Brand - Number') then 'SEM-Brand'
          -- when campaign_type = 'Direct' then 'Direct'
          -- when campaign_type in ('Facebook F&D','Facebook HBW','Social Retargeting', 'Facebook Silver Lookalike') then 'Display'
          when campaign_type in ('Bing - Competitor','Google - Competitor','Google - Non-Brand','Google NB - Advertise','Google NB - Free Advertising','Google NB - Promote','Google NB - SB-Adv') then 'SEM-NB'
          -- when campaign_type = 'Phone Bank - No Campaign' then 'Phone Bank'
          -- when campaign_type = 'Referral' then 'Referral'
          when campaign_type in ('Living Social','Merchant Blog/SBRC','Merchant Newsletter','Organic','Print') then 'SEO'
          -- when lower(utm_medium) like '%referral%' then 'Referral'
          -- when referrer_domain is null or referrer_domain = '' or referrer_domain = ' ' then 'Direct'
          -- else 'Referral'
          else 'Direct / Referral / Other'
       end campaign_group
      , case when utm_campaign like 'TXN1%' then 1 else 0 end pd_mm_relaunch_flag
      , split(utm_campaign, '_')[0] as mktg_txny_version
      , split(utm_campaign, '_')[1] as mktg_country
      , split(utm_campaign, '_')[2] as mktg_test_division
      , split(utm_campaign, '_')[3] as mktg_traffic_source
      , split(utm_campaign, '_')[4] as mktg_audience
      , split(utm_campaign, '_')[5] as mktg_sem_type
      , split(utm_campaign, '_')[6] as mktg_platform
      , split(utm_campaign, '_')[7] as mktg_creative
      , page_country
      , case when page_path like '/merchant%' and page_path not like '/merchant/center%' then 1 else 0 end merchant_pg_view
      , case when page_path = '/merchant' or page_path = '/merchant/square' or page_path='/merchant/use-groupon' or (page_app = 'itier-merchant-inbound-acquisition' and page_type in ('User', 'business_info')) then 1 else 0 end mia_form_view_v2
      , case when page_app = 'itier-merchant-inbound-acquisition' and page_type in ('User', 'business_info') then 1 else 0 end as mia_form_view
      , case when page_app = 'itier-merchant-inbound-acquisition' and page_type in ('InBound', 'lead_success_inbound') then 1 else 0 end as mia_success_ib_view
      , case when page_app = 'itier-merchant-inbound-acquisition' and page_type in ('SuccessReturn', 'lead_success_user_signup') then 1 else 0 end as mia_success_metro_view
    from grp_gdoop_sup_analytics_db.jc_w2l_funnel_events a
    where page_app in ('itier-merchant-inbound-acquisition', 'grouponworks-webtolead', 'groupon-webtolead')
      and event = 'pageview'
      and dt > '2019-01-01'
  ) a
  left join sm_w2l_mktg_acct_attrib mkt
    on mkt.bcookie = a.bcookie
  group by 
    a.dt
    , a.bcookie
    , a.user_uuid
    , a.updated_user_device_type
    , a.page_country
    , a.campaign_group
    , a.updated_traffic_source
    , a.traffic_source
    , a.traffic_sub_source
    , a.referrer_url
    , a.referrer_domain
    , a.utm_campaign
    , a.campaign_type
    , a.pd_mm_relaunch_flag
    , a.mktg_txny_version
    , a.mktg_country
    , a.mktg_test_division
    , a.mktg_traffic_source
    , a.mktg_audience
    , a.mktg_sem_type
    , a.mktg_platform
    , a.utm_medium
    , a.utm_source
    , a.mktg_creative;


 drop table if exists sm_w2l_mia_traffic;
 create table sm_w2l_mia_traffic stored as orc as
  select
    dt
    , utm_campaign
    , campaign_type
    , campaign_group
    , page_country
    , pd_mm_relaunch_flag
    , mktg_txny_version
    , mktg_country
    , case when pd_mm_relaunch_flag = 1 then mktg_test_division
        else 'Non Paid Re-Launch Campaign' end mktg_test_division
    , mktg_traffic_source
    , mktg_audience
    , mktg_sem_type
    , mktg_platform
    , mktg_creative
    , bld.highest_touch
    , metal
    , count(distinct case when mia_form_view_v2 = 1 then bcookie end) merchant_pg_views
    , count(distinct case when mia_form_view = 1 then bcookie end) mia_form_views
    , count(distinct case when mia_success_ib_view = 1 then bcookie end) mia_success_ib_views
    , count(distinct case when mia_success_metro_view = 1 then bcookie end) mia_success_metro_views
  from sm_w2l_mia_funnel bld
  where dt > '2019-01-01'
  group by 
    dt
    , utm_campaign
    , campaign_type
    , campaign_group
    , page_country
    , pd_mm_relaunch_flag
    , mktg_txny_version
    , mktg_country
    , case when pd_mm_relaunch_flag = 1 then mktg_test_division
        else 'Non Paid Re-Launch Campaign' end 
    , mktg_traffic_source
    , mktg_audience
    , mktg_sem_type
    , mktg_platform
    , mktg_creative
    , bld.highest_touch
    , metal;
  
 
 drop table if exists sm_w2l_leads;
 create table sm_w2l_leads stored as orc as
  select
    sf.createddate as dt
    , campaign_name__c
    , campaign_type
    , coalesce(campaign_group, 'Direct / Referral / Other') as campaign_group
    , pd_mm_relaunch_flag
    , mktg_txny_version
    , mktg_country
    , case when pd_mm_relaunch_flag = 1 then mktg_test_division
        else 'Non Paid Re-Launch Campaign' end mktg_test_division
    , mktg_traffic_source
    , mktg_audience
    , mktg_sem_type
    , mktg_platform
    , mktg_creative
    , sf.highest_touch
    , case when sf.country_code = 'GB' then 'UK' else sf.country_code end feature_country
    , case when lower(sf.acct_metal) in ('silver', 'gold', 'sliver', 'platinum') then 'S+' else 'B-' end metal
    , count(distinct sf.accountid) leads
    , count(distinct case when lower(sf.acct_metal) in ('silver', 'gold', 'platinum') then sf.accountid end) s_plus_leads
  from sm_w2l_mktg_acct_attrib sf
  group by 
    sf.createddate
    , campaign_name__c
    , campaign_type
    , coalesce(campaign_group, 'Direct / Referral / Other') 
    , pd_mm_relaunch_flag
    , mktg_txny_version
    , mktg_country
    , case when pd_mm_relaunch_flag = 1 then mktg_test_division
        else 'Non Paid Re-Launch Campaign' end 
    , mktg_traffic_source
    , mktg_audience
    , mktg_sem_type
    , mktg_platform
    , mktg_creative
    , sf.highest_touch
    , case when sf.country_code = 'GB' then 'UK' else sf.country_code end 
    , case when lower(sf.acct_metal) in ('silver', 'gold', 'sliver', 'platinum') then 'S+' else 'B-' end;
  
 
 drop table if exists sm_w2l_closes;
 create table sm_w2l_closes stored as orc as
  select
    c.close_date as dt
    , campaign_name__c
    , campaign_type
    , coalesce(campaign_group, 'Direct / Referral / Other') as campaign_group
    , pd_mm_relaunch_flag
    , mktg_txny_version
    , mktg_country
    , case when mkt.pd_mm_relaunch_flag = 1 then mkt.mktg_test_division
        else 'Non Paid Re-Launch Campaign' end mktg_test_division
    , mktg_traffic_source
    , mktg_audience
    , mktg_sem_type
    , mktg_platform
    , mktg_creative
    , case when c.feature_country = 'GB' then 'UK' else c.feature_country end feature_country
    , mkt.highest_touch
    , case when lower(mkt.acct_metal) in ('silver', 'gold', 'sliver', 'platinum') then 'S+' else 'B-' end metal
    , count(distinct c.accountid) closes
  from grp_gdoop_sup_analytics_db.jc_merchant_mtd_attrib c
  left join sm_w2l_mktg_acct_attrib mkt
    on c.accountid = mkt.accountid
    where c.feature_country in ('GB', 'UK', 'US', 'AU', 'PL', 'FR', 'IT', 'AE', 'DE', 'ES', 'CA')
      and c.grt_l1_cat_name = 'L1 - Local'
      and (lower(c.mtd_attribution) like '%metro%' or lower(c.mtd_attribution_intl) like '%metro%')
      and c.close_order = 1
      and c.por_relaunch = 0
  group by 
    c.close_date as dt
    , campaign_name__c
    , campaign_type
    , coalesce(campaign_group, 'Direct / Referral / Other')
    , pd_mm_relaunch_flag
    , mktg_txny_version
    , mktg_country
    , case when mkt.pd_mm_relaunch_flag = 1 then mkt.mktg_test_division
        else 'Non Paid Re-Launch Campaign' end 
    , mktg_traffic_source
    , mktg_audience
    , mktg_sem_type
    , mktg_platform
    , mktg_creative
    , case when c.feature_country = 'GB' then 'UK' else c.feature_country end 
    , mkt.highest_touch
    , case when lower(mkt.acct_metal) in ('silver', 'gold', 'sliver', 'platinum') then 'S+' else 'B-' end;
  
 
 drop table if exists sm_w2l_launches;
 create table sm_w2l_launches stored as orc as
  select
    c.launch_date as dt
    , campaign_name__c
    , campaign_type
    , coalesce(campaign_group, 'Direct / Referral / Other') as campaign_group
    , pd_mm_relaunch_flag
    , mktg_txny_version
    , mktg_country
    , case when mkt.pd_mm_relaunch_flag = 1 then mkt.mktg_test_division
        else 'Non Paid Re-Launch Campaign' end mktg_test_division
    , mktg_traffic_source
    , mktg_audience
    , mktg_sem_type
    , mktg_platform
    , mktg_creative
    , case when c.feature_country = 'GB' then 'UK' else c.feature_country end feature_country
    , mkt.highest_touch
    , case when lower(mkt.acct_metal) in ('silver', 'gold', 'sliver', 'platinum') then 'S+' else 'B-' end metal
    , count(distinct c.accountid) launches
  from jc_merchant_mtd_attrib c
  left join sm_w2l_mktg_acct_attrib mkt
    on c.accountid = mkt.accountid
    where c.feature_country in ('GB', 'UK', 'US', 'AU', 'PL', 'FR', 'IT', 'AE', 'DE', 'ES', 'CA')
      and c.grt_l1_cat_name = 'L1 - Local'
      and (lower(c.mtd_attribution) like '%metro%' or lower(c.mtd_attribution_intl) like '%metro%')
      and c.launch_order = 1
      and c.launch_date is not null
      and c.por_relaunch = 0
  group by 
   c.launch_date
    , campaign_name__c
    , campaign_type
    , coalesce(campaign_group, 'Direct / Referral / Other') 
    , pd_mm_relaunch_flag
    , mktg_txny_version
    , mktg_country
    , case when mkt.pd_mm_relaunch_flag = 1 then mkt.mktg_test_division
        else 'Non Paid Re-Launch Campaign' end 
    , mktg_traffic_source
    , mktg_audience
    , mktg_sem_type
    , mktg_platform
    , mktg_creative
    , case when c.feature_country = 'GB' then 'UK' else c.feature_country end
    , mkt.highest_touch
    , case when lower(mkt.acct_metal) in ('silver', 'gold', 'sliver', 'platinum') then 'S+' else 'B-' end;


 drop table if exists smm_w2l_kpis;
 create table smm_w2l_kpis stored as orc as
  select
    coalesce(a.dt, b.dt, c.dt, d.dt) week
    , coalesce(a.utm_campaign, b.campaign_name__c, c.campaign_name__c, d.campaign_name__c) campaign_name
    , coalesce(a.campaign_type, b.campaign_type, c.campaign_type, d.campaign_type) campaign_type
    , coalesce(a.campaign_group, b.campaign_group, c.campaign_group, d.campaign_group) campaign_group
    , coalesce(a.page_country, b.feature_country, c.feature_country, d.feature_country) feature_country
    , coalesce(a.pd_mm_relaunch_flag, b.pd_mm_relaunch_flag, c.pd_mm_relaunch_flag, d.pd_mm_relaunch_flag) pd_mm_relaunch_flag
    , coalesce(a.mktg_txny_version, b.mktg_txny_version, c.mktg_txny_version, d.mktg_txny_version) mktg_txny_version
    , coalesce(a.mktg_country, b.mktg_country, c.mktg_country, d.mktg_country) mktg_country
    , coalesce(a.mktg_test_division, b.mktg_test_division, c.mktg_test_division, d.mktg_test_division) mktg_test_division
    , coalesce(a.mktg_traffic_source, b.mktg_traffic_source, c.mktg_traffic_source, d.mktg_traffic_source) mktg_traffic_source
    , coalesce(a.mktg_audience, b.mktg_audience, c.mktg_audience, d.mktg_audience) mktg_audience
    , coalesce(a.mktg_sem_type, b.mktg_sem_type, c.mktg_sem_type, d.mktg_sem_type) mktg_sem_type
    , coalesce(a.mktg_platform, b.mktg_platform, c.mktg_platform, d.mktg_platform) mktg_platform
    , coalesce(a.mktg_creative, b.mktg_creative, c.mktg_creative, d.mktg_creative) mktg_creative
    , coalesce(a.highest_touch, b.highest_touch, c.highest_touch, d.highest_touch) highest_touch
    , coalesce(a.metal, b.metal, c.metal, d.metal) metal
    , sum(a.merchant_pg_views) merchant_pg_views
    , sum(a.mia_form_views) mia_form_views
    , sum(a.mia_success_ib_views) mia_success_ib_views
    , sum(a.mia_success_metro_views) mia_success_metro_views
    , sum(d.leads) leads
    , sum(d.s_plus_leads) s_plus_leads
    , sum(b.closes) closes
    , sum(c.launches) launches
  from sm_w2l_mia_traffic a
  full outer join sm_w2l_leads d
    on a.dt = d.dt
      and a.campaign_group = d.campaign_group
      and a.page_country = d.feature_country
      and a.utm_campaign = d.campaign_name__c
      and a.campaign_type = d.campaign_type
      and a.pd_mm_relaunch_flag = d.pd_mm_relaunch_flag
      and a.mktg_txny_version = d.mktg_txny_version
      and a.mktg_country = d.mktg_country
      and a.mktg_test_division = d.mktg_test_division
      and a.mktg_traffic_source = d.mktg_traffic_source
      and a.mktg_audience = d.mktg_audience
      and a.mktg_sem_type = d.mktg_sem_type
      and a.mktg_platform = d.mktg_platform
      and a.mktg_creative = d.mktg_creative
      and a.highest_touch = d.highest_touch
      and a.metal = d.metal
  full outer join sm_w2l_closes b
    on d.dt = b.dt
      and d.campaign_group = b.campaign_group
      and d.feature_country = b.feature_country
      and d.campaign_name__c = b.campaign_name__c
      and d.campaign_type = b.campaign_type
      and b.pd_mm_relaunch_flag = d.pd_mm_relaunch_flag
      and b.mktg_txny_version = d.mktg_txny_version
      and b.mktg_country = d.mktg_country
      and b.mktg_test_division = d.mktg_test_division
      and b.mktg_traffic_source = d.mktg_traffic_source
      and b.mktg_audience = d.mktg_audience
      and b.mktg_sem_type = d.mktg_sem_type
      and b.mktg_platform = d.mktg_platform
      and b.mktg_creative = d.mktg_creative
      and b.highest_touch = d.highest_touch
      and b.metal = d.metal
  full outer join sm_w2l_launches c
    on d.dt = c.dt
      and d.campaign_group = c.campaign_group
      and d.feature_country = c.feature_country
      and d.campaign_name__c = c.campaign_name__c
      and d.campaign_type = c.campaign_type
      and c.pd_mm_relaunch_flag = d.pd_mm_relaunch_flag
      and c.mktg_txny_version = d.mktg_txny_version
      and c.mktg_country = d.mktg_country
      and c.mktg_test_division = d.mktg_test_division
      and c.mktg_traffic_source = d.mktg_traffic_source
      and c.mktg_audience = d.mktg_audience
      and c.mktg_sem_type = d.mktg_sem_type
      and c.mktg_platform = d.mktg_platform
      and c.mktg_creative = d.mktg_creative
      and c.highest_touch = d.highest_touch
      and c.metal = d.metal
  where coalesce(a.dt, b.dt, c.dt, d.dt) < current_date
  group by 
    coalesce(a.dt, b.dt, c.dt, d.dt) 
    , coalesce(a.utm_campaign, b.campaign_name__c, c.campaign_name__c, d.campaign_name__c) 
    , coalesce(a.campaign_type, b.campaign_type, c.campaign_type, d.campaign_type) 
    , coalesce(a.campaign_group, b.campaign_group, c.campaign_group, d.campaign_group) 
    , coalesce(a.page_country, b.feature_country, c.feature_country, d.feature_country) 
    , coalesce(a.pd_mm_relaunch_flag, b.pd_mm_relaunch_flag, c.pd_mm_relaunch_flag, d.pd_mm_relaunch_flag) 
    , coalesce(a.mktg_txny_version, b.mktg_txny_version, c.mktg_txny_version, d.mktg_txny_version) 
    , coalesce(a.mktg_country, b.mktg_country, c.mktg_country, d.mktg_country) 
    , coalesce(a.mktg_test_division, b.mktg_test_division, c.mktg_test_division, d.mktg_test_division) 
    , coalesce(a.mktg_traffic_source, b.mktg_traffic_source, c.mktg_traffic_source, d.mktg_traffic_source) 
    , coalesce(a.mktg_audience, b.mktg_audience, c.mktg_audience, d.mktg_audience) 
    , coalesce(a.mktg_sem_type, b.mktg_sem_type, c.mktg_sem_type, d.mktg_sem_type) 
    , coalesce(a.mktg_platform, b.mktg_platform, c.mktg_platform, d.mktg_platform) 
    , coalesce(a.mktg_creative, b.mktg_creative, c.mktg_creative, d.mktg_creative) 
    , coalesce(a.highest_touch, b.highest_touch, c.highest_touch, d.highest_touch) 
    , coalesce(a.metal, b.metal, c.metal, d.metal);
 
select * 
from grp_gdoop_bizops_db.np_temp_close
where launch_week >= '2022-10-01';

select * 
from grp_gdoop_bizops_db.np_temp_launch 
where launch_week >= '2022-10-01' and account_id;


select 
    count(1)
from 
(select 
   accountid, 
   count(distinct campaign_name__c) diff_camps
from grp_gdoop_bizops_db.np_temp_close
where launch_week >= '2022-01-01'
group by accountid) as fin 
where diff_camps > 1;

select * from grp_gdoop_bizops_db.np_temp_close where accountid = '0013c000021PDfWAAW';

 
----------------------------------------------------------EXPORTING TO SHEET RAW
 
 use grp_gdoop_bizops_db;
 select
wk.week_end
, case when campaign_group is null or campaign_group in ('SEO', 'Direct / Referral / Other') then 'Direct / Referral / Other'
else campaign_group
end campaign_group
, case when c.campaign_group not in ('SEO', 'Direct / Referral / Other', 'Groupon Header & Footer', 'Square Marketplace') then 1 else 0 end pd_mktg_campaign
, c.highest_touch
, case when c.highest_touch not in ('SEO', 'Direct / Referral / Other', 'Groupon Header & Footer','Square Marketplace') then 1 else 0 end pd_mktg_highest_touch
, c.metal
, case when c.feature_country = 'US' then 'NAM' else 'INTL' end region
, sum(mia_form_views) mia_form_views
, sum(leads) leads
, sum(s_plus_leads) s_plus_leads
, sum(closes) closes
, sum(launches) launches
, sum(merchant_pg_views) merchant_pg_views
from smm_w2l_kpis c
join user_dw.v_dim_day dd
on date_format(c.week,'yyyy-MM-dd') = dd.day_rw
join user_dw.v_dim_week wk
on dd.week_key = wk.week_key
where  c.week > '2020-01-01'
and c.feature_country in ('US', 'DE', 'UK')
and date_format(wk.week_end, 'yyyy-MM-dd') < date_format(current_date, 'yyyy-MM-dd')
and c.campaign_name <> 'TXN1_HOU_UPF_DIS_GEN__GOOG_DUG1'
group by
wk.week_end
, case when campaign_group is null or campaign_group in ('SEO', 'Direct / Referral / Other') then 'Direct / Referral / Other'
else campaign_group
end
, case when c.campaign_group not in ('SEO', 'Direct / Referral / Other', 'Groupon Header & Footer', 'Square Marketplace') then 1 else 0 end
, c.highest_touch
, case when c.highest_touch not in ('SEO', 'Direct / Referral / Other', 'Groupon Header & Footer','Square Marketplace') then 1 else 0 end
, c.metal
, case when c.feature_country = 'US' then 'NAM' else 'INTL' end
order by wk.week_end desc



----------------------------------------------------------EXPORTING TO UV SHEET


select
    wk_end
, case when country_code = 'US' then 'NAM' else 'INTL' end region
    , case when traffic_source in ('SEM', 'Display') then 1 else 0 end as pd_mktg_campaign
    , 'Overall' as highest_touch
    , 'Overall' as pd_mktg_highest_touch
    , 'Overall' as metal
    , sum(uniq_visitors) uvs
  from user_edwprod.agg_gbl_traffic
  where wk_end >= '2019-01-01'
    and country_code in ('US', 'DE', 'UK')
    and wk_end < current_date
  group by 1,2,3,4,5,6

----------------------------------------------------------MONTHLY VIEW
    
