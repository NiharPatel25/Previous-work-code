------Local HBW deals with options > 3 as well as share of total (NAM)
(select deal_uuid, min(cast(load_date as date)) mn_load_date 
      from grp_gdoop_bizops_db.sh_bt_active_deals_log
      where product_is_active_flag = 1 and is_bookable = 1 and partner_inactive_flag = 0
      group by deal_uuid) a
join (select 
       deal_id 
       from user_edwprod.dim_gbl_deal_lob 
       where grt_l2_cat_name = 'L2 - Health / Beauty / Wellness') b on a.deal_uuid = b.deal_id
left join 
     (SELECT DISTINCT product_uuid, merchant_uuid FROM user_edwprod.dim_offer_ext) as e on a.deal_uuid = e.product_uuid;
     



select 
    year_of_date, 
    month_of_date, 
    sum(case when deal_options > 3 then count_of_deals end) greater_than_3_deals,
    sum(count_of_deals) total_hbw_deals
from
(select 
    ad.year_of_date, 
    ad.month_of_date, 
    do.deal_options, 
    count(distinct deal_uuid) count_of_deals
from 
     (select
         deal_uuid,
         year(load_date) year_of_date, 
         month(load_date) month_of_date
      from prod_groupondw.active_deals
      where
          sold_out = 'false'
          and available_qty > 0
          and load_date >= '2020-06-01'
      group by deal_uuid, year(load_date), month(load_date)) as ad
     join user_edwprod.dim_gbl_deal_lob gdl on gdl.deal_id = ad.deal_uuid and grt_l2_cat_name= 'L2 - Health / Beauty / Wellness' and gdl.country_code = 'US'
     left join
     (select 
            product_uuid product_uuid,  
            count(distinct inv_product_uuid) deal_options
       from user_edwprod.dim_offer_ext  
       where inv_product_uuid <> '-1' and inventory_service_name in ('vis', 'voucher')
       group by product_uuid) do on ad.deal_uuid = do.product_uuid
     group by 
       ad.year_of_date, 
       ad.month_of_date, 
       do.deal_options) as a
group by 
    year_of_date, 
    month_of_date
    ;

select distinct inventory_service_name from user_edwprod.dim_offer_ext;
 
drop table grp_gdoop_bizops_db.nvp_na_hbw_pull_dlvl;
create table grp_gdoop_bizops_db.nvp_na_hbw_pull_dlvl stored as orc as
select 
    ad.year_of_date, 
    ad.month_of_date, 
    do.deal_options, 
    deal_uuid
from 
(select
      deal_uuid,
      year(load_date) year_of_date, 
      month(load_date) month_of_date
   from prod_groupondw.active_deals
  where
       sold_out = 'false'
       and available_qty > 0
       and load_date >= '2020-06-01'
    group by deal_uuid, year(load_date), month(load_date)) as ad
join user_edwprod.dim_gbl_deal_lob gdl on gdl.deal_id = ad.deal_uuid and grt_l2_cat_name= 'L2 - Health / Beauty / Wellness' and gdl.country_code = 'US'
left join
   (select 
         product_uuid product_uuid,  
         count(distinct inv_product_uuid) deal_options
    from user_edwprod.dim_offer_ext  
    where inv_product_uuid <> '-1' and inventory_service_name in ('vis', 'voucher')
    group by product_uuid) do on ad.deal_uuid = do.product_uuid
;
drop table grp_gdoop_bizops_db.nvp_na_hbw_pull;
create table grp_gdoop_bizops_db.nvp_na_hbw_pull stored as orc as
select 
    ad.year_of_date, 
    ad.month_of_date, 
    do.deal_options, 
    count(distinct deal_uuid) count_of_deals
from 
     (select
         deal_uuid,
         year(load_date) year_of_date, 
         month(load_date) month_of_date
      from prod_groupondw.active_deals
      where
          sold_out = 'false'
          and available_qty > 0
          and load_date >= '2020-06-01'
      group by deal_uuid, year(load_date), month(load_date)) as ad
     join user_edwprod.dim_gbl_deal_lob gdl on gdl.deal_id = ad.deal_uuid and grt_l2_cat_name= 'L2 - Health / Beauty / Wellness' and gdl.country_code = 'US'
     join
     (select 
            product_uuid product_uuid,  
            count(distinct inv_product_uuid) deal_options
       from user_edwprod.dim_offer_ext  
       where inv_product_uuid <> '-1' and inventory_service_name in ('vis', 'voucher')
       group by product_uuid) do on ad.deal_uuid = do.product_uuid
     group by 
       ad.year_of_date, 
       ad.month_of_date, 
       do.deal_options
;
