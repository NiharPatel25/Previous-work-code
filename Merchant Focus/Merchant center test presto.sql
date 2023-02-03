create table grp_gdoop_bizops_db.np_home_deal_clk_mob stored as orc as 
select 
  eventdate,
  bcookie,
  rawevent, 
  rawpagetype,
  eventdestination, 
  event, 
  widgetcontentname,
  clientplatform
from grp_gdoop_pde.junoHourly 
where eventdate >= '2022-07-01' and eventdate <= '2022-07-15'
and  rawevent = 'GRP17'
and lower(eventdestination) = 'genericclick'
and lower(event) = 'genericclick'
and country in ('US')
and platform = 'mobile'
and dealuuid is not null
and lower(trim(regexp_replace(clientplatform,'\\t|\\n|\\r|\\u0001', ''))) in ('iphone', 'android')
and clicktype in ('embedded_collection_card_click')
and extrainfo not like '%homepage%';



select * from grp_gdoop_bizops_db.pai_merchant_center_visits where merchant_uuid is not null;
select count(1) from grp_gdoop_bizops_db.np_sssl_tab_login2;

select count(distinct rawpagetype) from grp_gdoop_bizops_db.np_ss_sl_user_granular;



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
from grp_gdoop_sup_analytics_db.smm_w2l_kpis c
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