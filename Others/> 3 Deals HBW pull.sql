select 
    year_of_date, 
    month_of_date, 
    sum(case when deal_options > 3 then count_of_deals end) greater_than_3_deals,
    sum(count_of_deals) total_hbw_deals
from
(select 
    ad.year_of_date, 
    ad.month_of_date, 
    doa.deal_options, 
    count(distinct deal_uuid) count_of_deals
from 
     (select
         deal_uuid,
         year(load_date) year_of_date, 
         month(load_date) month_of_date
      from user_groupondw.active_deals
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
       group by product_uuid) as doa on ad.deal_uuid = doa.product_uuid
     group by 
       ad.year_of_date, 
       ad.month_of_date, 
       doa.deal_options) as a
group by 
    year_of_date, 
    month_of_date
    ;