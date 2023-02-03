-------TERADATA

select 
      trunc(cast(report_date as date) , 'iw') + 6 report_week, 
      sku deal_uuid, 
      case when campaign_name like '%mail%' then 1 else 0 end has_email, 
      sum(impressions) impressions, 
      sum(clicks) total_clicks, 
      sum(conversions) orders, 
      cast(orders as float)/NULLIFZERO(total_clicks) conv 
from sandbox.np_sl_ad_snapshot
group by 
     trunc(cast(report_date as date) , 'iw') + 6, 
    case when campaign_name like '%mail%' then 1 else 0 end, 
    sku
order by 1 desc,2;






create table grp_gdoop_bizops_db.np_sl_roas_final_deal stored as orc as 
select
           date_sub(next_day(eventdate, 'MON'), 1) date_start_end,
           dealuuid, 
           coupon,
           max(merch_segmentation) merch_segmentation,
           max(l1) l1,
           max(l2) l2,
           sum(deal_rev) merch_rev, 
           sum(deal_rev_same_day) merch_rev_same_day, 
           sum(deal_rev_30_day) merch_rev_30_day, 
           sum(deal_rev_120_day) merch_rev_120_day, 
           sum(deal_rev_all_red) merch_rev_all_red,
           sum(sli_impressions) sli_impressions, 
           sum(total_impression_aog) total_impression_aog,
           sum(sli_clicks) sli_clicks, 
           sum(orders_sold) orders_sold,
           sum(deal_rev_org) merch_rev_org, 
           sum(deal_rev_same_day_org) merch_rev_same_day_org,
           sum(deal_rev_30_day_org) merch_rev_30_day_org,
           sum(deal_rev_120_day_org) merch_rev_120_day_org,
           sum(deal_rev_all_red_org) merch_rev_all_red_org,
           sum(sli_impressions_org) sli_impressions_org,
           sum(total_impression_aog_org) total_impression_aog_org,
           sum(sli_clicks_org) sli_clicks_org,
           sum(orders_sold_org) orders_sold_org,
           count(distinct eventdate) number_of_days,
           sum(total_groupon_rev) total_groupon_rev,
           sum(citrus_impressions) citrus_impressions,
           sum(citrus_clicks) citrus_clicks
           from
       grp_gdoop_bizops_db.np_sl_performance_base
       group by 
           date_sub(next_day(eventdate, 'MON'), 1),
           dealuuid, 
           coupon;