-------------------------------------------------------

drop table grp_gdoop_bizops_db.nvp_temp_attrited_deals;
create table grp_gdoop_bizops_db.nvp_temp_attrited_deals stored as orc as
    select 
        date_sub(next_day(opt_out_date, 'MON'), 1) opt_out_week,
        country_code,
        l2,
        l3,
        t.deal_uuid, 
        merchant_uuid
    from (
        select 
            ad.deal_uuid,
            merch.merchant_uuid,
            min(load_date) opt_out_date,
            grt_l2_cat_description l2,
            grt_l3_cat_description l3,
            gdl.country_code
        from grp_gdoop_bizops_db.sh_bt_active_deals_log ad
        join user_edwprod.dim_gbl_deal_lob gdl on ad.deal_uuid = gdl.deal_id and gdl.country_code = 'US'
        join 
            (select product_uuid,
               max(merchant_uuid) merchant_uuid
               from user_edwprod.dim_offer_ext
               group by  product_uuid) merch on ad.deal_uuid  = merch.product_uuid
        where (ad.partner_inactive_flag = 1 
             or ad.product_is_active_flag = 0) 
        group by 
           ad.deal_uuid,
           grt_l2_cat_description,
           grt_l3_cat_description,
           gdl.country_code, 
           merch.merchant_uuid
    ) t
    join (select deal_uuid 
          from grp_gdoop_bizops_db.sh_bt_active_deals_log 
          where product_is_active_flag = 1
          and partner_inactive_flag = 0 
          group by deal_uuid) adl on t.deal_uuid = adl.deal_uuid
    group by 
        date_sub(next_day(opt_out_date, 'MON'), 1),
        country_code,
        l2,
        l3,
        t.deal_uuid, merchant_uuid;

       
select deal_uuid,
        min(load_date) bt_launch_date
    from sandbox.sh_bt_active_deals_log
    where product_is_active_flag = 1
    and partner_inactive_flag = 0;
   
   
       
       
       
create table grp_gdoop_bizops_db.nvp_temp_attrited_deals_units stored as orc as
select 
    fgt_2.deal_uuid,
    sum(fgt_2.transaction_qty) units, 
    sum(case when b.deal_uuid is not null then fgt_2.transaction_qty end) units_bt
from 
(select 
    fgt.*,
    gdl.grt_l2_cat_description
    from 
    user_edwprod.fact_gbl_transactions fgt
    join user_edwprod.dim_gbl_deal_lob gdl on fgt.deal_uuid = gdl.deal_id
    where
    gdl.country_code = 'US'
    and gdl.grt_l1_cat_name = 'L1 - Local'
    and fgt.order_date >= cast('2020-01-01' as date) 
    and action = 'authorize'
) as fgt_2
left join
(select  
       deal_uuid,
       cast(load_date as date) load_date 
    from grp_gdoop_bizops_db.sh_bt_active_deals_log
    where product_is_active_flag = 1
          and partner_inactive_flag = 0
    group by deal_uuid, cast(load_date as date)
) as b on fgt_2.deal_uuid = b.deal_uuid and cast(fgt_2.order_date as date) = cast(b.load_date as date)
group by fgt_2.deal_uuid;




------------------------------------------------------------

select * from grp_gdoop_bizops_db.sh_bt_active_deals_log;

select 
   grt_l2_cat_description,
   sum(fgt_2.transaction_qty) units, 
   count(distinct order_uuid) orders, 
   count(distinct parent_order_uuid) parent_order_uuid
from
(select 
    fgt.*,
    gdl.grt_l2_cat_description
    from 
    user_edwprod.fact_gbl_transactions fgt
    join user_edwprod.dim_gbl_deal_lob gdl on fgt.deal_uuid = gdl.deal_id
    where
    gdl.country_code = 'US'
    and gdl.grt_l1_cat_name = 'L1 - Local'
    and fgt.order_date >= cast('2020-01-01' as date) 
    and action = 'authorize'
) as fgt_2
join
(select  
       deal_uuid,
       cast(load_date as date) load_date 
    from grp_gdoop_bizops_db.sh_bt_active_deals_log
    where product_is_active_flag = 1
          and partner_inactive_flag = 0
    group by 1,2
) as b on fgt_2.deal_uuid = b.deal_uuid and cast(fgt_2.order_date as date) = cast(b.load_date as date)
group by 1;

drop table grp_gdoop_bizops_db.nvp_temp_2020_bt_units;
create table grp_gdoop_bizops_db.nvp_temp_2020_bt_units stored as orc as 
select 
   case when b.deal_uuid is not null then 1 else 0 end bookable,
   case when live.deal_uuid is not null then 1 else 0 end live_currently,
   grt_l2_cat_description,
   sum(fgt_2.transaction_qty) units, 
   count(distinct order_uuid) orders, 
   count(distinct parent_order_uuid) parent_order_uuid
from
(select 
    fgt.*,
    gdl.grt_l2_cat_description
    from 
    user_edwprod.fact_gbl_transactions fgt
    join user_edwprod.dim_gbl_deal_lob gdl on fgt.deal_uuid = gdl.deal_id
    where
    gdl.country_code = 'US'
    and gdl.grt_l1_cat_name = 'L1 - Local'
    and fgt.order_date >= cast('2020-01-01' as date) 
    and action = 'authorize'
) as fgt_2
left join
(select  
       deal_uuid,
       cast(load_date as date) load_date 
    from grp_gdoop_bizops_db.sh_bt_active_deals_log
    where product_is_active_flag = 1
          and partner_inactive_flag = 0
    group by deal_uuid, cast(load_date as date)
) as b on fgt_2.deal_uuid = b.deal_uuid and cast(fgt_2.order_date as date) = cast(b.load_date as date)
left join 
(select  
       deal_uuid
    from grp_gdoop_bizops_db.sh_bt_active_deals_log
    where product_is_active_flag = 1
          and partner_inactive_flag = 0
          and cast(load_date as date) >= cast('2021-03-01' as date)
    group by deal_uuid
) as live on fgt_2.deal_uuid = live.deal_uuid
group by 
   case when b.deal_uuid is not null then 1 else 0 end,
   case when live.deal_uuid is not null then 1 else 0 end,
   grt_l2_cat_description
order by 
   bookable,
   live_currently,
   grt_l2_cat_description;

drop table grp_gdoop_bizops_db.nvp_temp_2020_bt_units_2;
create table grp_gdoop_bizops_db.nvp_temp_2020_bt_units_2 stored as orc as 
select 
   case when b.deal_uuid is not null then 1 else 0 end bookable,
   case when live.deal_uuid is not null then 1 else 0 end live_currently,
   grt_l2_cat_description,
   sum(case when order_date >= bt_date then fgt_2.transaction_qty end) units, 
   count(distinct order_uuid) orders, 
   count(distinct parent_order_uuid) parent_order_uuid
from
(select 
    fgt.*,
    gdl.grt_l2_cat_description
    from 
    user_edwprod.fact_gbl_transactions fgt
    join user_edwprod.dim_gbl_deal_lob gdl on fgt.deal_uuid = gdl.deal_id
    where
    gdl.country_code = 'US'
    and gdl.grt_l1_cat_name = 'L1 - Local'
    and fgt.order_date >= cast('2020-01-01' as date) 
    and action = 'authorize'
) as fgt_2
left join
(select  
       deal_uuid, 
       min(load_date) bt_date
    from grp_gdoop_bizops_db.sh_bt_active_deals_log
    where product_is_active_flag = 1
          and partner_inactive_flag = 0
          and cast(load_date as date) >= cast('2020-01-01' as date)
          and is_bookable = 1
    group by deal_uuid
) as b on fgt_2.deal_uuid = b.deal_uuid
left join 
(select  
       deal_uuid
    from grp_gdoop_bizops_db.sh_bt_active_deals_log
    where product_is_active_flag = 1
          and partner_inactive_flag = 0
          and is_bookable = 1
          and cast(load_date as date) >= cast('2021-03-01' as date)
    group by deal_uuid
) as live on fgt_2.deal_uuid = live.deal_uuid
group by 
   case when b.deal_uuid is not null then 1 else 0 end,
   case when live.deal_uuid is not null then 1 else 0 end,
   grt_l2_cat_description
order by 
   bookable,
   live_currently,
   grt_l2_cat_description;
   
  
  
