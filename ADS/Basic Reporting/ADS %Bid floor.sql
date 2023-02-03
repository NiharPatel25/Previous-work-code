SELECT distinct quarter FROM ai_reporting.search_bid_floors;

drop table grp_gdoop_bizops_db.np_all_category_pds;
create table grp_gdoop_bizops_db.np_all_category_pds stored as orc as 
select 
   distinct
   'L1' category_type,
   pds_cat_id, 
   pds_cat_name, 
   cft_l1_cat_id cft_cat_id, 
   cft_l1_cat_name cft_cat_name
   from 
   dw.v_dim_pds_cft_map
union all
select 
   distinct
   'L2' category_type,
   pds_cat_id, 
   pds_cat_name, 
   cft_l2_cat_id, 
   cft_l2_cat_name
   from 
   dw.v_dim_pds_cft_map
union all
select 
   distinct
   'L3' category_type,
   pds_cat_id, 
   pds_cat_name, 
   cft_l3_cat_id cft_cat_id, 
   cft_l3_cat_name
   from 
   dw.v_dim_pds_cft_map
union all 
select 
   distinct
   'L4' category_type,
   pds_cat_id, 
   pds_cat_name, 
   cft_l4_cat_id, 
   cft_l4_cat_name
   from 
   dw.v_dim_pds_cft_map
union all 
select 
   distinct
   'L5' category_type,
   pds_cat_id, 
   pds_cat_name, 
   cft_l5_cat_id, 
   cft_l5_cat_name
   from 
   dw.v_dim_pds_cft_map;



drop table grp_gdoop_bizops_db.np_citrus_sl_bid;
create table grp_gdoop_bizops_db.np_citrus_sl_bid2 stored as orc as
select 
    report_date,
    case when month(cast(report_date as date)) <= 3 then 'Q1' 
         when month(cast(report_date as date)) <= 6 then 'Q2'
         when month(cast(report_date as date)) <= 9 then 'Q3'
         when month(cast(report_date as date)) <= 12 then 'Q4'
         end quarter, 
    month(cast(report_date as date)) report_month, 
    year(cast(report_date as date)) report_year,
    a.deal_id,
    page_type,
    bid_join,
    platform,
    country_code, 
    total_cpc, 
    citrus_impressions, 
    citrus_clicks,
    case when cft1.cft_category_type1 = 'L5' then categoryid1
         when cft2.cft_category_type2 = 'L5' then categoryid2
         when cft3.cft_category_type3 = 'L5' then categoryid3
         when cft4.cft_category_type4 = 'L5' then categoryid4
         when cft5.cft_category_type5 = 'L5' then categoryid5
         when cft1.cft_category_type1 = 'L4' then categoryid1
         when cft2.cft_category_type2 = 'L4' then categoryid2
         when cft3.cft_category_type3 = 'L4' then categoryid3
         when cft4.cft_category_type4 = 'L4' then categoryid4
         when cft5.cft_category_type5 = 'L4' then categoryid5
         when cft1.cft_category_type1 = 'L3' then categoryid1
         when cft2.cft_category_type2 = 'L3' then categoryid2
         when cft3.cft_category_type3 = 'L3' then categoryid3
         when cft4.cft_category_type4 = 'L3' then categoryid4
         when cft5.cft_category_type5 = 'L3' then categoryid5
         when cft1.cft_category_type1 = 'L2' then categoryid1
         when cft2.cft_category_type2 = 'L2' then categoryid2
         when cft3.cft_category_type3 = 'L2' then categoryid3
         when cft4.cft_category_type4 = 'L2' then categoryid4
         when cft5.cft_category_type5 = 'L2' then categoryid5
         when cft1.cft_category_type1 = 'L1' then categoryid1 
         when cft2.cft_category_type2 = 'L1' then categoryid2
         when cft3.cft_category_type3 = 'L1' then categoryid3
         when cft4.cft_category_type4 = 'L1' then categoryid4
         when cft5.cft_category_type5 = 'L1' then categoryid5
         else bid_join
         end bid_join_final, 
    case when cft1.cft_category_type1 = 'L5' then concat(cft1.cft_category_type1, '-', cft1.pds_cat_name)
         when cft2.cft_category_type2 = 'L5' then concat(cft2.cft_category_type2, '-', cft2.pds_cat_name)
         when cft3.cft_category_type3 = 'L5' then concat(cft3.cft_category_type3, '-', cft3.pds_cat_name)
         when cft4.cft_category_type4 = 'L5' then concat(cft4.cft_category_type4, '-', cft4.pds_cat_name)
         when cft5.cft_category_type5 = 'L5' then concat(cft5.cft_category_type5, '-', cft5.pds_cat_name)
         when cft1.cft_category_type1 = 'L4' then concat(cft1.cft_category_type1, '-', cft1.pds_cat_name)
         when cft2.cft_category_type2 = 'L4' then concat(cft2.cft_category_type2, '-', cft2.pds_cat_name)
         when cft3.cft_category_type3 = 'L4' then concat(cft3.cft_category_type3, '-', cft3.pds_cat_name)
         when cft4.cft_category_type4 = 'L4' then concat(cft4.cft_category_type4, '-', cft4.pds_cat_name)
         when cft5.cft_category_type5 = 'L4' then concat(cft5.cft_category_type5, '-', cft5.pds_cat_name)
         when cft1.cft_category_type1 = 'L3' then concat(cft1.cft_category_type1, '-', cft1.pds_cat_name)
         when cft2.cft_category_type2 = 'L3' then concat(cft2.cft_category_type2, '-', cft2.pds_cat_name)
         when cft3.cft_category_type3 = 'L3' then concat(cft3.cft_category_type3, '-', cft3.pds_cat_name)
         when cft4.cft_category_type4 = 'L3' then concat(cft4.cft_category_type4, '-', cft4.pds_cat_name)
         when cft5.cft_category_type5 = 'L3' then concat(cft5.cft_category_type5, '-', cft5.pds_cat_name)
         when cft1.cft_category_type1 = 'L2' then concat(cft1.cft_category_type1, '-', cft1.pds_cat_name)
         when cft2.cft_category_type2 = 'L2' then concat(cft2.cft_category_type2, '-', cft2.pds_cat_name)
         when cft3.cft_category_type3 = 'L2' then concat(cft3.cft_category_type3, '-', cft3.pds_cat_name)
         when cft4.cft_category_type4 = 'L2' then concat(cft4.cft_category_type4, '-', cft4.pds_cat_name)
         when cft5.cft_category_type5 = 'L2' then concat(cft5.cft_category_type5, '-', cft5.pds_cat_name)
         when cft1.cft_category_type1 = 'L1' then concat(cft1.cft_category_type1, '-', cft1.pds_cat_name)
         when cft2.cft_category_type2 = 'L1' then concat(cft2.cft_category_type2, '-', cft2.pds_cat_name)
         when cft3.cft_category_type3 = 'L1' then concat(cft3.cft_category_type3, '-', cft3.pds_cat_name)
         when cft4.cft_category_type4 = 'L1' then concat(cft4.cft_category_type4, '-', cft4.pds_cat_name)
         when cft5.cft_category_type5 = 'L1' then concat(cft5.cft_category_type5, '-', cft5.pds_cat_name)
         when page_type = 'Browse' then 'other browse'
         when page_type = 'Search' then bid_join
         when page_type = 'Broad' then gdl.l3
         else bid_join
         end type_based_category, 
    gdl.l1, gdl.l2, gdl.l3
    from 
(select 
    report_date,
    deal_id,
    page_type,
    bid_join,
    platform,
    country_code, 
    total_cpc, 
    citrus_impressions, 
    citrus_clicks, 
    categoryid1,
    categoryid2,
    categoryid3,
    categoryid4,
    get_json_object(bid_join5,'$.category_id') categoryid5
from 
(select 
        fin4.*, 
        get_json_object(bid_join4,'$.category_id') categoryid4,
        case when length(regexp_extract(bid_join4, concat('"category_id":"',get_json_object(bid_join4,'$.category_id'), '"', ','), 0)) > 5 
               then regexp_replace(bid_join4, regexp_extract(bid_join4, concat('"category_id":"',get_json_object(bid_join4,'$.category_id'), '"', ','), 0) , '')
               when length(regexp_extract(bid_join4, concat(',"category_id":"',get_json_object(bid_join4,'$.category_id'), '"'), 0)) >5
               then regexp_replace(bid_join4, regexp_extract(bid_join4, concat(',"category_id":"',get_json_object(bid_join4,'$.category_id'), '"'), 0) , '')
               end bid_join5
   from
   (select 
        fin3.*, 
        get_json_object(bid_join3,'$.category_id') categoryid3,
        case when length(regexp_extract(bid_join3, concat('"category_id":"',get_json_object(bid_join3,'$.category_id'), '"', ','), 0)) > 5 
               then regexp_replace(bid_join3, regexp_extract(bid_join3, concat('"category_id":"',get_json_object(bid_join3,'$.category_id'), '"', ','), 0) , '')
               when length(regexp_extract(bid_join3, concat(',"category_id":"',get_json_object(bid_join3,'$.category_id'), '"'), 0)) >5
               then regexp_replace(bid_join3, regexp_extract(bid_join3, concat(',"category_id":"',get_json_object(bid_join3,'$.category_id'), '"'), 0) , '')
               end bid_join4
        from
        (select 
          fin2.*,
          get_json_object(bid_join2,'$.category_id') categoryid2,
          case when length(regexp_extract(bid_join2, concat('"category_id":"',get_json_object(bid_join2,'$.category_id'), '"', ','), 0)) > 5 
               then regexp_replace(bid_join2, regexp_extract(bid_join2, concat('"category_id":"',get_json_object(bid_join2,'$.category_id'), '"', ','), 0) , '')
               when length(regexp_extract(bid_join2, concat(',"category_id":"',get_json_object(bid_join2,'$.category_id'), '"'), 0)) >5
               then regexp_replace(bid_join2, regexp_extract(bid_join2, concat(',"category_id":"',get_json_object(bid_join2,'$.category_id'), '"'), 0) , '')
               end bid_join3
      from
      (select 
          fin.*, 
          get_json_object(bid_join,'$.category_id') as categoryid1,
          case when length(regexp_extract(bid_join, concat('"category_id":"',get_json_object(bid_join,'$.category_id'), '"', ','), 0)) > 5 
               then regexp_replace(bid_join, regexp_extract(bid_join, concat('"category_id":"',get_json_object(bid_join,'$.category_id'), '"', ','), 0) , '')
               when length(regexp_extract(bid_join, concat(',"category_id":"',get_json_object(bid_join,'$.category_id'), '"'), 0)) >5
               then regexp_replace(bid_join, regexp_extract(bid_join, concat(',"category_id":"',get_json_object(bid_join,'$.category_id'), '"'), 0) , '')
               end bid_join2
      from 
      (select 
             report_date,
             deal_id,
             CASE
                WHEN page = 'BROAD_DISPLAY' then 'Broad'
                WHEN page = 'CATEGORY_ONLY' then 'Browse'
                WHEN page = 'SEARCH_ONLY' then 'Search'
                when page = 'CATEGORY_AND_SEARCH'   then 'Search'
                END  page_type,
             CASE 
                WHEN page = 'BROAD_DISPLAY' then 'Broad'
                WHEN page = 'CATEGORY_ONLY' then translate(regexp_replace(translate(sponsored_product_filters,'[','{'),':','":"'),']','}')
                WHEN page = 'SEARCH_ONLY' and search_keyword != '' then search_keyword
                when page = 'CATEGORY_AND_SEARCH' and search_keyword != '' then search_keyword
                END     bid_join,
              get_json_object(translate(regexp_replace(translate(sponsored_product_filters,'[','{'),':','":"'),']','}'), '$.platform') platform,
              get_json_object(translate(regexp_replace(translate(sponsored_product_filters,'[','{'),':','":"'),']','}'), '$.country') country_code, 
              sum(revenue) total_cpc, 
              sum(impressions) citrus_impressions, 
              sum(clicks) citrus_clicks
      from ad_reporting_na_prod.citrus_master_report_citrus_51_v1
          group by
             report_date,
             deal_id,
             CASE
                WHEN page = 'BROAD_DISPLAY' then 'Broad'
                WHEN page = 'CATEGORY_ONLY' then 'Browse'
                WHEN page = 'SEARCH_ONLY' then 'Search'
                when page = 'CATEGORY_AND_SEARCH'   then 'Search'
                END,
             CASE 
                WHEN page = 'BROAD_DISPLAY' then 'Broad'
                WHEN page = 'CATEGORY_ONLY' then translate(regexp_replace(translate(sponsored_product_filters,'[','{'),':','":"'),']','}')  
                WHEN page = 'SEARCH_ONLY' and search_keyword != '' then search_keyword
                when page = 'CATEGORY_AND_SEARCH' and search_keyword != '' then search_keyword
                END,
              get_json_object(translate(regexp_replace(translate(sponsored_product_filters,'[','{'),':','":"'),']','}'), '$.platform'),
              get_json_object(translate(regexp_replace(translate(sponsored_product_filters,'[','{'),':','":"'),']','}'), '$.country')
           ) as fin) fin2) fin3) fin4)fin5) as a
left join 
     (select cft_cat_id, max(category_type) cft_category_type1, max(pds_cat_name) pds_cat_name
             from grp_gdoop_bizops_db.np_all_category_pds group by cft_cat_id) as cft1 on cft1.cft_cat_id = a.categoryid1
left join 
     (select cft_cat_id, max(category_type) cft_category_type2, max(pds_cat_name) pds_cat_name
             from grp_gdoop_bizops_db.np_all_category_pds group by cft_cat_id) as cft2 on cft2.cft_cat_id = a.categoryid2       
left join 
     (select cft_cat_id, max(category_type) cft_category_type3, max(pds_cat_name) pds_cat_name
             from grp_gdoop_bizops_db.np_all_category_pds group by cft_cat_id) as cft3 on cft3.cft_cat_id = a.categoryid3
left join 
     (select cft_cat_id, max(category_type) cft_category_type4, max(pds_cat_name) pds_cat_name
             from grp_gdoop_bizops_db.np_all_category_pds group by cft_cat_id) as cft4 on cft4.cft_cat_id = a.categoryid4
left join 
     (select cft_cat_id, max(category_type) cft_category_type5, max(pds_cat_name) pds_cat_name
             from grp_gdoop_bizops_db.np_all_category_pds group by cft_cat_id) as cft5 on cft5.cft_cat_id = a.categoryid5
left join 
     (select deal_id, max(grt_l1_cat_name) l1, max(grt_l2_cat_name) l2, max(grt_l3_cat_name) l3 
             from user_edwprod.dim_gbl_deal_lob 
             group by deal_id) as gdl on a.deal_id = gdl.deal_id
;

 and a.quarter = b.quarter



DROP TABLE grp_gdoop_bizops_db.np_sl_bidfloor_tableau;
create table grp_gdoop_bizops_db.np_sl_bidfloor_tableau stored as orc as 
select 
   'monthly_cut' Dashboard_cut, 
   date_add(last_day(add_months(report_date, -1)),1) date_start_end, 
   page_type, 
   platform, 
   country_code, 
   case when cast((page_bid_cpc - page_bid_floor) as float)/page_bid_floor > 0 then 1 else 0 end cleared_bid_floor,
   case when cast((page_bid_cpc - page_bid_floor) as float)/page_bid_floor >= 0.15 then 1 else 0 end cleared_bid_floor_15_per,
   case when cast((page_bid_cpc - page_bid_floor) as float)/page_bid_floor >= 0.25 then 1 else 0 end cleared_bid_floor_25_per,
   case when cast((page_bid_cpc - page_bid_floor) as float)/page_bid_floor >= 0.35 then 1 else 0 end cleared_bid_floor_35_per,
   case when cast((page_bid_cpc - page_bid_floor) as float)/page_bid_floor >= 0.50 then 1 else 0 end cleared_bid_floor_50_per,
   bid_join_final, 
   type_based_category,
   l1,
   l2,
   l3,
   sum(citrus_clicks) citrus_clicks, 
   sum(citrus_impressions) citrus_impressions
   from
   (select 
       fin.*, 
       case when page_bid is null then 0.5 else page_bid end page_bid_floor
   from 
       (select 
            a.*, 
            coalesce(cast(b.page_bid as double), cast(c.page_bid as double)) page_bid,
            total_cpc/citrus_clicks page_bid_cpc
        from grp_gdoop_bizops_db.np_citrus_sl_bid as a 
        left join 
             ai_reporting.search_bid_floors as b on a.bid_join_final = b.search_query
        left join 
             ai_reporting.category_bid_floors as c on a.bid_join_final = c.cat_uuid) as fin) as monthly
    group by 
     date_add(last_day(add_months(report_date, -1)),1), 
     page_type, 
     platform, 
     country_code, 
     case when cast((page_bid_cpc - page_bid_floor) as float)/page_bid_floor > 0 then 1 else 0 end,
     case when cast((page_bid_cpc - page_bid_floor) as float)/page_bid_floor >= 0.15 then 1 else 0 end,
     case when cast((page_bid_cpc - page_bid_floor) as float)/page_bid_floor >= 0.25 then 1 else 0 end,
     case when cast((page_bid_cpc - page_bid_floor) as float)/page_bid_floor >= 0.35 then 1 else 0 end,
     case when cast((page_bid_cpc - page_bid_floor) as float)/page_bid_floor >= 0.50 then 1 else 0 end,
     bid_join_final, 
     type_based_category,
     l1,
   l2,
   l3
union 
select 
   'weekly_cut' Dashboard_cut, 
   date_sub(next_day(report_date, 'MON'), 1) date_start_end, 
   page_type, 
   platform, 
   country_code, 
   case when cast((page_bid_cpc - page_bid_floor) as float)/page_bid_floor > 0 then 1 else 0 end cleared_bid_floor,
   case when cast((page_bid_cpc - page_bid_floor) as float)/page_bid_floor >= 0.15 then 1 else 0 end cleared_bid_floor_15_per,
   case when cast((page_bid_cpc - page_bid_floor) as float)/page_bid_floor >= 0.25 then 1 else 0 end cleared_bid_floor_25_per,
   case when cast((page_bid_cpc - page_bid_floor) as float)/page_bid_floor >= 0.35 then 1 else 0 end cleared_bid_floor_35_per,
   case when cast((page_bid_cpc - page_bid_floor) as float)/page_bid_floor >= 0.50 then 1 else 0 end cleared_bid_floor_50_per,
   bid_join_final, 
   type_based_category,
   l1,
   l2,
   l3,
   sum(citrus_clicks) citrus_clicks, 
   sum(citrus_impressions) citrus_impressions
   from
   (select 
       fin.*, 
       case when page_bid is null then 0.5 else page_bid end page_bid_floor
   from 
       (select 
            a.*, 
            coalesce(cast(b.page_bid as double), cast(c.page_bid as double)) page_bid,
            total_cpc/citrus_clicks page_bid_cpc
        from grp_gdoop_bizops_db.np_citrus_sl_bid as a 
        left join 
             ai_reporting.search_bid_floors as b on a.bid_join_final = b.search_query
        left join 
             ai_reporting.category_bid_floors as c on a.bid_join_final = c.cat_uuid) as fin) as weekly
     group by 
     date_sub(next_day(report_date, 'MON'), 1), 
     page_type, 
     platform, 
     country_code, 
     case when cast((page_bid_cpc - page_bid_floor) as float)/page_bid_floor > 0 then 1 else 0 end,
     case when cast((page_bid_cpc - page_bid_floor) as float)/page_bid_floor >= 0.15 then 1 else 0 end,
     case when cast((page_bid_cpc - page_bid_floor) as float)/page_bid_floor >= 0.25 then 1 else 0 end,
     case when cast((page_bid_cpc - page_bid_floor) as float)/page_bid_floor >= 0.35 then 1 else 0 end,
     case when cast((page_bid_cpc - page_bid_floor) as float)/page_bid_floor >= 0.50 then 1 else 0 end,
     bid_join_final, 
     type_based_category, 
    l1,
   l2,
   l3;



  
  
create table grp_gdoop_bizops_db.np_sl_bidfloor_tableau2 stored as orc as 
select 
   'monthly_cut' Dashboard_cut, 
   date_add(last_day(add_months(report_date, -1)),1) date_start_end,
   deal_id,
   page_type, 
   platform, 
   country_code, 
   case when cast((page_bid_cpc - page_bid_floor) as float)/page_bid_floor > 0 then 1 else 0 end cleared_bid_floor,
   case when cast((page_bid_cpc - page_bid_floor) as float)/page_bid_floor >= 0.15 then 1 else 0 end cleared_bid_floor_15_per,
   case when cast((page_bid_cpc - page_bid_floor) as float)/page_bid_floor >= 0.25 then 1 else 0 end cleared_bid_floor_25_per,
   case when cast((page_bid_cpc - page_bid_floor) as float)/page_bid_floor >= 0.35 then 1 else 0 end cleared_bid_floor_35_per,
   case when cast((page_bid_cpc - page_bid_floor) as float)/page_bid_floor >= 0.50 then 1 else 0 end cleared_bid_floor_50_per,
   bid_join_final, 
   type_based_category,
   l1,
   l2,
   l3,
   sum(citrus_clicks) citrus_clicks, 
   sum(citrus_impressions) citrus_impressions
   from
   (select 
       fin.*, 
       case when page_bid is null then 0.5 else page_bid end page_bid_floor
   from 
       (select 
            a.*, 
            coalesce(cast(b.page_bid as double), cast(c.page_bid as double)) page_bid,
            total_cpc/citrus_clicks page_bid_cpc
        from grp_gdoop_bizops_db.np_citrus_sl_bid as a 
        left join 
             ai_reporting.search_bid_floors as b on a.bid_join_final = b.search_query
        left join 
             ai_reporting.category_bid_floors as c on a.bid_join_final = c.cat_uuid) as fin) as monthly
    group by 
     date_add(last_day(add_months(report_date, -1)),1), 
     deal_id,
     page_type, 
     platform, 
     country_code, 
     case when cast((page_bid_cpc - page_bid_floor) as float)/page_bid_floor > 0 then 1 else 0 end,
     case when cast((page_bid_cpc - page_bid_floor) as float)/page_bid_floor >= 0.15 then 1 else 0 end,
     case when cast((page_bid_cpc - page_bid_floor) as float)/page_bid_floor >= 0.25 then 1 else 0 end,
     case when cast((page_bid_cpc - page_bid_floor) as float)/page_bid_floor >= 0.35 then 1 else 0 end,
     case when cast((page_bid_cpc - page_bid_floor) as float)/page_bid_floor >= 0.50 then 1 else 0 end,
     bid_join_final, 
     type_based_category,
     l1,
   l2,
   l3
union 
select 
   'weekly_cut' Dashboard_cut, 
   date_sub(next_day(report_date, 'MON'), 1) date_start_end, 
   deal_id, 
   page_type, 
   platform, 
   country_code, 
   case when cast((page_bid_cpc - page_bid_floor) as float)/page_bid_floor > 0 then 1 else 0 end cleared_bid_floor,
   case when cast((page_bid_cpc - page_bid_floor) as float)/page_bid_floor >= 0.15 then 1 else 0 end cleared_bid_floor_15_per,
   case when cast((page_bid_cpc - page_bid_floor) as float)/page_bid_floor >= 0.25 then 1 else 0 end cleared_bid_floor_25_per,
   case when cast((page_bid_cpc - page_bid_floor) as float)/page_bid_floor >= 0.35 then 1 else 0 end cleared_bid_floor_35_per,
   case when cast((page_bid_cpc - page_bid_floor) as float)/page_bid_floor >= 0.50 then 1 else 0 end cleared_bid_floor_50_per,
   bid_join_final, 
   type_based_category,
   l1,
   l2,
   l3,
   sum(citrus_clicks) citrus_clicks, 
   sum(citrus_impressions) citrus_impressions
   from
   (select 
       fin.*, 
       case when page_bid is null then 0.5 else page_bid end page_bid_floor
   from 
       (select 
            a.*, 
            coalesce(cast(b.page_bid as double), cast(c.page_bid as double)) page_bid,
            total_cpc/citrus_clicks page_bid_cpc
        from grp_gdoop_bizops_db.np_citrus_sl_bid as a 
        left join 
             ai_reporting.search_bid_floors as b on a.bid_join_final = b.search_query
        left join 
             ai_reporting.category_bid_floors as c on a.bid_join_final = c.cat_uuid) as fin) as weekly
     group by 
     date_sub(next_day(report_date, 'MON'), 1), 
     deal_id,
     page_type, 
     platform, 
     country_code, 
     case when cast((page_bid_cpc - page_bid_floor) as float)/page_bid_floor > 0 then 1 else 0 end,
     case when cast((page_bid_cpc - page_bid_floor) as float)/page_bid_floor >= 0.15 then 1 else 0 end,
     case when cast((page_bid_cpc - page_bid_floor) as float)/page_bid_floor >= 0.25 then 1 else 0 end,
     case when cast((page_bid_cpc - page_bid_floor) as float)/page_bid_floor >= 0.35 then 1 else 0 end,
     case when cast((page_bid_cpc - page_bid_floor) as float)/page_bid_floor >= 0.50 then 1 else 0 end,
     bid_join_final, 
     type_based_category, 
    l1,
   l2,
   l3;


-------------------------------------------------------------------------------OLD QUERY

create table grp_gdoop_bizops_db.np_sl_bid_floor stored as orc as 
select 
    fin.*, 
    case when page_bid is not null and page_bid_cpc > page_bid then 1 
         when page_bid_cpc > 0.5 then 1 
         else 0 end cleared_bid_floor
         from 
(select 
    a.*, 
    coalesce(b.page_bid, br.page_bid) page_bid,
    total_cpc/citrus_clicks page_bid_cpc
from grp_gdoop_bizops_db.np_citrus_sl_bid as a 
left join 
(select 
      search_query,
      sum(cookies1) imp1,
      round(case when sum(ogp10)/sum(dv10)*0.35/(1-0.4) < 0.5 then 0.5 
                 when sum(ogp10) is null or sum(dv10)=0 then 0.5
                 else sum(ogp10)/sum(dv10)*0.35/(1-0.4)
                 end,2) page_bid
from ai_reporting.search_new_bid
where eventdate between ('2021-01-05' ) and ('2021-05-20')
group by search_query, eventdate) as b on a.bid_join_final = b.search_query and a.report_date = b.eventdate
left join 
    prod_groupondw.dim_category as dcat on a.bid_join_final = dcat.category_id
left join 
    (
     select 
      eventdate,
      cft_perma,
      case when 0.00+sum(ogp10)/sum(dv10)*0.5/(1-0.42) < 0.5 then 0.5 
           when sum(ogp10) is null or sum(dv10)=0 then 0.5
           else 0.00+sum(ogp10)/sum(dv10)*0.5/(1-0.42)  end page_bid,
      case when sum(dv10)>0 then sum(ogp10)/sum(dv10)*0.5/(1-0.42) else 0 end realbid
      from  ai_reporting.sl_bid_pagecategory
      where 
      page_type_sl='browse'
      group by cft_perma, eventdate 
    ) as br on dcat.permalink = br.cft_perma and a.report_date = br.eventdate) as fin
          



    
    
select 
   *
from 
   grp_gdoop_bizops_db.np_sl_bid_floor;
    
    
    
    
    
    
    
    
    
    
    
    


    
select * from ai_reporting.sl_bid_pagecategory;
select * from dw.v_dim_pds_cft_map;
select category_id, count(1) cnz from prod_groupondw.dim_category group by category_id having cnz >1;
select * from ai_reporting.search_new_bid2;



select 
left join (select distinct cft_l1_cat_name,cft_l1_cat_id from dw.v_dim_pds_cft_map ) dcr1
   on json_extract_scalar(replace(replace(sponsored_product_filters,'[','{'),':','":"'), '$.category_id')=dcr1.cft_l1_cat_id
left join (select distinct cft_l2_cat_name,cft_l2_cat_id from dw.v_dim_pds_cft_map ) dcr2
   on json_extract_scalar(replace(replace(sponsored_product_filters,'[','{'),':','":"'), '$.category_id')=dcr2.cft_l2_cat_id
where report_Date='2020-11-08'


select 
     cft_perma,
     division,
     level,
     max(grt_id),
     sum(cookies1) imp1,
     sum(cookies10) imp10,
     sum(dv1) click1,
     sum(dv10) click10,
     case when 0.00+sum(ogp10)/sum(dv10)*0.5/(1-0.42)<.5 then 0.5 
          when sum(ogp10) is null or sum(dv10)=0 then 0.5
          else 0.00+sum(ogp10)/sum(dv10)*0.5/(1-0.42)  end page_bid,
     case when sum(dv10)>0 then sum(ogp10)/sum(dv10)*0.5/(1-0.42) else 0 end realbid
from  ai_reporting.sl_bid_pagecategory



select * from ai_reporting.sl_bid_pagecategory;

select distinct clientplatform from ai_reporting.search_new_bid2;

select * from dw.v_dim_pds_cft_map;

-------------------------------------------------Bid Floor by search

select 
search_query,
sum(cookies1) imp1,
round(case when sum(ogp10)/sum(dv10)*0.35/(1-0.4) < 0.5 then 0.5 
     when sum(ogp10) is null or sum(dv10)=0 then 0.5
     else sum(ogp10)/sum(dv10)*0.35/(1-0.4)
     end,2) page_bid
from ai_reporting.search_new_bid2
where eventdate between '2021-03-01' and '2021-04-01'
group by search_query



-------------------------------------------------Bid Floor by page categories


select division,
      avg(page_bid) avg_bid,stddev(page_bid), 
      avg(case when realbid>0 then realbid else 0 end) avg_realbid,
      stddev(realbid) stdreal,
      100.00*count(case when realbid<.5 then 1 end)/count(*) under05, 
      min(click10) minclick,
      avg(click10),
      sum(click1) clicks1, 
      count(division) divisions
from (
select 
     cft_perma,
     division,
     level,
     max(grt_id),
     sum(cookies1) imp1,
     sum(cookies10) imp10,
     sum(dv1) click1,
     sum(dv10) click10,
     case when 0.00+sum(ogp10)/sum(dv10)*0.5/(1-0.42)<.5 then 0.5 
          when sum(ogp10) is null or sum(dv10)=0 then 0.5
          else 0.00+sum(ogp10)/sum(dv10)*0.5/(1-0.42)  end page_bid,
     case when sum(dv10)>0 then sum(ogp10)/sum(dv10)*0.5/(1-0.42) else 0 end realbid
from  ai_reporting.sl_bid_pagecategory
--left join prod_groupondw.dim_category dcat 
--   on friendly_name=
where  --cft_l1_cat_name='goods' and cft_l2_cat_name = 'Baby & Kids'
     page_type_sl='browse'
     and eventdate between '2021-03-01' and '2021-04-01'
group by 
     cft_perma,
     division,
     level --,2
) bid_division
group by division


left join (select distinct cft_l1_cat_name,cft_l1_cat_id from dw.v_dim_pds_cft_map ) dcr1
   on json_extract_scalar(replace(replace(sponsored_product_filters,'[','{'),':','":"'), '$.category_id')=dcr1.cft_l1_cat_id
left join (select distinct cft_l2_cat_name,cft_l2_cat_id from dw.v_dim_pds_cft_map ) dcr2
   on json_extract_scalar(replace(replace(sponsored_product_filters,'[','{'),':','":"'), '$.category_id')=dcr2.cft_l2_cat_id

-------------------------------------------------Base Categories
select distinct page from ai_reporting.CITRUS_MASTER_REPORT_CITRUS_1_v1;


select
     'Sponsor Listings' source, 
     '' ad_unit , 
     '' advertiser_name, 
     '' advertiser_domain, 
     '--' country, 
     '' buyer_network, 
     0 total_ad_requests, 
     sum(impressions) total_impressions, 
     sum(clicks)   total_clicks, 
     sum(revenue) total_revenue, 
     0 page_views, 
     0 actual_page_views, 
     0 bounces, 
     0 uv, 
     0 merch_impressions, 
     0 impressions, 
     0 all_impressions, 
     position_in_pod placement,
     CASE WHEN page = 'CATEGORY_ONLY' then 'Browse'
          WHEN page = 'SEARCH_ONLY' then 'Search'
          when page = 'CATEGORY_AND_SEARCH'   then 'Search'
          END  page_type,
     CASE WHEN page = 'CATEGORY_ONLY' then coalesce(get_json_object(translate(regexp_replace(translate(sponsored_product_filters,'[','{'),':','":"'),']','}'), '$.category'),'Other')  
          WHEN page = 'SEARCH_ONLY' then 'Search'
          when page = 'CATEGORY_AND_SEARCH'   then 'Search'
          else 'Home'
          END     ad_unit_type,
     get_json_object(translate(regexp_replace(translate(sponsored_product_filters,'[','{'),':','":"'),']','}'), '$.country') country_code, 
     'N/A' platform, 
     NULL as pricing_rule, 
     'Groupon' Brand, 
     cast(report_Date as date) report_date,
     lob.grt_l1_cat_name
FROM  ai_reporting.CITRUS_MASTER_REPORT_CITRUS_1_v1 cit
      LEFT JOIN edwprod.dim_gbl_deal_lob lob on lob.deal_id=cit.deal_id
WHERE cast(report_Date as date)<current_Date
      group by
      position_in_pod,
      CASE WHEN page = 'CATEGORY_ONLY' then 'Browse'
           WHEN page = 'SEARCH_ONLY' then 'Search'
           when page = 'CATEGORY_AND_SEARCH'   then 'Search' end, 
      CASE WHEN page = 'CATEGORY_ONLY' then coalesce(get_json_object(translate(regexp_replace(translate(sponsored_product_filters,'[','{'),':','":"'),']','}'), '$.category'),'Other') 
           WHEN page = 'SEARCH_ONLY' then 'Search'
           when page = 'CATEGORY_AND_SEARCH'   then 'Search'
           else 'Home'
           end,
      get_json_object(translate(regexp_replace(translate(sponsored_product_filters,'[','{'),':','":"'),']','}'), '$.country'), 
      cast(report_Date as date),
      lob.grt_l1_cat_name;


Select * from ai_reporting.CITRUS_MASTER_REPORT_CITRUS_1_v1;



--------------------------------------------------------------------------------------------------------------------------------------------TRIAL


drop table grp_gdoop_bizops_db.np_citrus_sl_bid_temp;
create table grp_gdoop_bizops_db.np_citrus_sl_bid_temp stored as orc as
select bid_join,
       regexp_replace(bid_join,
                 coalesce(regexp_extract(bid_join, concat('"category_id":"',get_json_object(bid_join,'$.category_id'), '"', ','), 0) >, 
                          regexp_extract(bid_join, concat(',"category_id":"',get_json_object(bid_join,'$.category_id'), '"'), 0))
                 , '') complete_replace, 
       coalesce(regexp_extract(bid_join, concat('"category_id":"',get_json_object(bid_join,'$.category_id'), '"', ','), 0), 
                          regexp_extract(bid_join, concat(',"category_id":"',get_json_object(bid_join,'$.category_id'), '"'), 0)) with_coalesce_all,
       regexp_extract(bid_join, concat(',"category_id":"',get_json_object(bid_join,'$.category_id'), '"'), 0) extract_for_prev_semi,
       concat(',"category_id":"',get_json_object(bid_join,'$.category_id'), '"') concat_value
       from 
      grp_gdoop_bizops_db.np_citrus_sl_bid as a
      where deal_id = 'ee852112-e0a7-4fbf-b868-b24e9c68f1ac' and report_date = '2020-10-30';
     
     
     
drop table grp_gdoop_bizops_db.np_citrus_sl_bid_temp2;
create table grp_gdoop_bizops_db.np_citrus_sl_bid_temp2 stored as orc as
select bid_join,
       regexp_extract(bid_join, concat('"category_id":"',get_json_object(bid_join,'$.category_id'), '"', ','), 0) extract_A,
       regexp_extract(bid_join, concat(',"category_id":"',get_json_object(bid_join,'$.category_id'), '"'), 0) extract_B,
       coalesce(regexp_extract(bid_join, concat('"category_id":"',get_json_object(bid_join,'$.category_id'), '"', ','), 0), 
                          regexp_extract(bid_join, concat(',"category_id":"',get_json_object(bid_join,'$.category_id'), '"'), 0)) coalesce_a_b,
                          
       case when length(regexp_extract(bid_join, concat('"category_id":"',get_json_object(bid_join,'$.category_id'), '"', ','), 0)) > 5 
                 then regexp_replace(bid_join, regexp_extract(bid_join, concat('"category_id":"',get_json_object(bid_join,'$.category_id'), '"', ','), 0) , '')
            when length(regexp_extract(bid_join, concat(',"category_id":"',get_json_object(bid_join,'$.category_id'), '"'), 0)) >5
                 then regexp_replace(bid_join, regexp_extract(bid_join, concat(',"category_id":"',get_json_object(bid_join,'$.category_id'), '"'), 0) , '')
            end bid_join2
       from 
      grp_gdoop_bizops_db.np_citrus_sl_bid as a
      where deal_id = 'ee852112-e0a7-4fbf-b868-b24e9c68f1ac' and report_date = '2020-10-30';     

     
     
 
     
create table grp_gdoop_bizops_db.np_citrus_sl_bid_temp stored as orc as
select bid_join bid_joinx,
       regexp_replace(bid_join,
                 coalesce(regexp_extract(bid_join, concat('"category_id":"',get_json_object(bid_join,'$.category_id'), '"', ','), 0), 
                          regexp_extract(bid_join, concat(',"category_id":"',get_json_object(bid_join,'$.category_id'), '"'), 0))
                 , '') complete_replace, 
       coalesce(regexp_extract(bid_join, concat('"category_id":"',get_json_object(bid_join,'$.category_id'), '"', ','), 0), 
                          regexp_extract(bid_join, concat(',"category_id":"',get_json_object(bid_join,'$.category_id'), '"'), 0)) with_coalesce_all,
       regexp_extract(bid_join, concat(',"category_id":"',get_json_object(bid_join,'$.category_id'), '"'), 0) extract_for_prev_semi,
       concat(',"category_id":"',get_json_object(bid_join,'$.category_id'), '"') concat_value
       from 
      grp_gdoop_bizops_db.np_citrus_sl_bid as a
      where deal_id = 'ee852112-e0a7-4fbf-b868-b24e9c68f1ac' and report_date = '2020-10-30';    
 

