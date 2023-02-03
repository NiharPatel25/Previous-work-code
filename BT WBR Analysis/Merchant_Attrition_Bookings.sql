drop table grp_gdoop_bizops_db.nvp_bt_merchant_attrition;
create table grp_gdoop_bizops_db.nvp_bt_merchant_attrition stored as orc as
select 
  fin.*, 
  datediff(fin.mx_merch_load_date, fin.mn_merch_load_date) attrition_days
from
(select
  mn.merchant_uuid, 
  mn.l1,
  mn.l2,
  mn.country_code,
  mn.mn_merch_load_date, 
  mn.mx_merch_load_date,
  count(distinct case when mb.book_date between mn.mn_merch_load_date and mn.mx_merch_load_date then mb.parent_order_uuid end)  total_orders
from 
   (select 
       e.merchant_uuid,
       gdl.l1,
       gdl.l2,
       gdl.country_code,
       min(mn_load_date) mn_merch_load_date,
       max(mx_load_date) mx_merch_load_date
     from 
        (select 
              deal_uuid,
              min(cast(load_date as date)) mn_load_date,
              max(cast(load_date as date)) mx_load_date
          from grp_gdoop_bizops_db.sh_bt_active_deals_log
          where product_is_active_flag = 1 and is_bookable = 1 and partner_inactive_flag = 0
          group by deal_uuid) bd
     left join 
          (select 
               distinct 
               product_uuid, 
               merchant_uuid 
           from user_edwprod.dim_offer_ext) as e on bd.deal_uuid = e.product_uuid
     join (select 
               deal_id, 
               grt_l1_cat_name l1, 
               grt_l2_cat_name l2,
               grt_l2_cat_name l3, 
               country_code
             from user_edwprod.dim_gbl_deal_lob
            ) gdl on bd.deal_uuid = gdl.deal_id
        group by e.merchant_uuid, bd.deal_uuid, gdl.l1, gdl.l2, gdl.country_code
        having cast(mx_merch_load_date as date) <= date_sub(CURRENT_DATE, 7)) mn
    left join
         grp_gdoop_bizops_db.nvp_wbr_merch_booked mb on mn.merchant_uuid = mb.merchant_uuid
    group by 
       mn.merchant_uuid,
       mn.l1,
       mn.l2,
       mn.country_code,
       mn.mn_merch_load_date, 
       mn.mx_merch_load_date
) as fin
;


------   having cast(mx_merch_load_date as date) <= date_sub(CURRENT_DATE, 7) and cast(mx_bt_merch_load_date as date) <= date_sub(CURRENT_DATE, 7)
select 
    count(distinct merchant_uuid)
from grp_gdoop_bizops_db.nvp_bt_merchant_attrition
where 
country_code = 'US'
and cast(mn_bt_merch_load_date as date) >= cast('2020-06-01' as date) 
and cast(mn_bt_merch_load_date as date) <= cast('2020-06-30' as date)
;

drop table if exists grp_gdoop_bizops_db.nvp_wbr_deals_booked;
create table grp_gdoop_bizops_db.nvp_wbr_deals_booked stored as orc as
select 
    cast(b.order_date as date) order_date,
    cast(b.book_date as date) book_date,
    b.parent_order_uuid,
    deal_uuid
from grp_gdoop_bizops_db.rt_bt_txns as b 
group by 
   book_date,
   order_date,
   deal_uuid, 
   parent_order_uuid
;


drop table if exists grp_gdoop_bizops_db.nvp_wbr_merch_booked;
create table grp_gdoop_bizops_db.nvp_wbr_merch_booked stored as orc as
select 
    cast(b.order_date as date) order_date,
    cast(b.book_date as date) book_date,
    b.parent_order_uuid,
    merchant_uuid
from grp_gdoop_bizops_db.rt_bt_txns as b 
left join (
    select 
         product_uuid product_uuid, 
         max(merchant_uuid) merchant_uuid
    from user_edwprod.dim_offer_ext  
    where inv_product_uuid <> '-1'
    group by product_uuid) merch on b.deal_uuid = merch.product_uuid
group by 
   book_date,
   order_date,
   merchant_uuid, 
   parent_order_uuid
;



describe formatted grp_gdoop_bizops_db.nvp_bt_merchant_attrition;

drop table grp_gdoop_bizops_db.nvp_bt_merchant_attrition;
create table grp_gdoop_bizops_db.nvp_bt_merchant_attrition stored as orc as
select 
    fin.*,
    datediff(mx_merch_load_date, mx_bt_merch_load_date) normal_vs_bt_attrition_fl,
    datediff(mx_merch_load_date, mn_merch_load_date) normal_merch_attrition_days,
    datediff(mx_bt_merch_load_date, mn_bt_merch_load_date) bt_attrition_days
from
(select
  mn.merchant_uuid,
  mn.l1,
  mn.l2,
  mn.country_code,
  mn.mn_merch_load_date,
  mn.mx_merch_load_date,
  mn.mn_bt_merch_load_date, 
  mn.mx_bt_merch_load_date, 
  count(distinct case when cast(mb.book_date as date) between mn.mn_bt_merch_load_date and mn.mx_bt_merch_load_date then mb.parent_order_uuid end)  total_bt_orders,
  count(distinct case when cast(mb.order_date as date) between mn.mn_merch_load_date and mn.mx_merch_load_date then mb.parent_order_uuid end)  total_orders
  from
(select
  mn1.merchant_uuid, 
  mn1.l1,
  mn1.l2,
  mn1.country_code,
  case when mn1.mn_merch_load_date > mn1.mn_bt_merch_load_date then mn1.mn_bt_merch_load_date else mn1.mn_merch_load_date end mn_merch_load_date,
  case when mn1.mx_merch_load_date < mn1.mx_bt_merch_load_date then mn1.mx_bt_merch_load_date else mn1.mx_merch_load_date end mx_merch_load_date,
  mn1.mn_bt_merch_load_date, 
  mn1.mx_bt_merch_load_date
from 
(select
     e.merchant_uuid,
     gdl.country_code country_code,
     max(gdl.l1) l1,
     max(gdl.l2) l2,
     min(cast(ad.mn_load_date as date)) mn_merch_load_date,
     max(cast(ad.mx_load_date as date)) mx_merch_load_date, 
     min(cast(bt.mn_bt_load_date as date)) mn_bt_merch_load_date,
     max(cast(bt.mx_bt_load_date as date)) mx_bt_merch_load_date
from
      (select
          deal_uuid,
          min(cast(load_date as date)) mn_load_date,
          max(cast(load_date as date)) mx_load_date
       from prod_groupondw.active_deals
      where
         sold_out = 'false'
         and available_qty > 0
         and cast(load_date as date) >= cast('2018-01-01' as date)
        group by deal_uuid) ad
   left join 
       (select
             deal_uuid,
             min(cast(load_date as date)) mn_bt_load_date,
             max(cast(load_date as date)) mx_bt_load_date
         from grp_gdoop_bizops_db.sh_bt_active_deals_log
         where 
            partner_inactive_flag = 0
            and product_is_active_flag = 1
            and cast(load_date as date) >= cast('2018-01-01' as date)
        group by deal_uuid) bt on bt.deal_uuid = ad.deal_uuid
   left join 
       (select 
             product_uuid, 
             max(merchant_uuid) merchant_uuid 
           from user_edwprod.dim_offer_ext 
           group by product_uuid) as e on ad.deal_uuid = e.product_uuid
   join      
        (select 
               deal_id, 
               grt_l1_cat_name l1,
               grt_l2_cat_name l2,
               grt_l2_cat_name l3,
               country_code
             from user_edwprod.dim_gbl_deal_lob
            ) gdl on ad.deal_uuid = gdl.deal_id
   group by e.merchant_uuid, gdl.country_code
        ) mn1 ) mn
left join
         grp_gdoop_bizops_db.nvp_wbr_merch_booked mb on mn.merchant_uuid = mb.merchant_uuid
group by
      mn.merchant_uuid, 
      mn.l1,
      mn.l2,
      mn.country_code,
      mn.mn_merch_load_date,
      mn.mx_merch_load_date,
      mn.mn_bt_merch_load_date, 
      mn.mx_bt_merch_load_date) fin;

----------------------------------------------------------------------------------------------------DEAL Attrition

     
create TEMPORARY table grp_gdoop_bizops_db.sh_bt_paused_reason stored as orc as
    select 
    deal_uuid, max(o2.booking_pause_reason__c) booking_pause_reason, max(sfa.id) account_id
    from edwprod.sf_opportunity_2 o2
    join edwprod.sf_opportunity_1 o1 on o2.id = o1.id
    join dwh_base_sec_view.sf_account sfa on o1.accountid = sfa.id
    group by deal_uuid
    having booking_pause_reason is not null
;

select count(*) from grp_gdoop_bizops_db.sh_bt_paused_reason;

create TEMPORARY table grp_gdoop_bizops_db.nvp_bt_groupon_pause_reason stored as orc as
select 
   fin.deal_uuid, 
   max(fin.pause_reason) pause_reason, 
   max(pause_date) pause_date
from 
(select 
   p.*, 
   row_number() over(partition by deal_uuid order by cast(pause_date as date) desc) row_num
from grp_gdoop_bizops_db.jj_paused_deals_all_v p) fin 
where fin.row_num = 1 
group by fin.deal_uuid;

create TEMPORARY table grp_gdoop_bizops_db.nvp_bt_deal_dates stored as orc as
select
             deal_uuid,
             min(cast(load_date as date)) mn_bt_load_date,
             max(cast(load_date as date)) mx_bt_load_date
         from grp_gdoop_bizops_db.sh_bt_active_deals_log
         where 
            partner_inactive_flag = 0
            and product_is_active_flag = 1
            and cast(load_date as date) >= cast('2018-01-01' as date)
        group by deal_uuid;

create TEMPORARY table grp_gdoop_bizops_db.nvp_bt_groupon_dates stored as orc as      
select
          deal_uuid,
          min(cast(load_date as date)) mn_load_date,
          max(cast(load_date as date)) mx_load_date
       from prod_groupondw.active_deals
      where
         sold_out = 'false'
         and available_qty > 0
         and cast(load_date as date) >= cast('2018-01-01' as date)
        group by deal_uuid;
       
       
       
  
drop table if exists grp_gdoop_bizops_db.nvp_bt_deal_attrition;
create table grp_gdoop_bizops_db.nvp_bt_deal_attrition stored as orc as
select 
  fin_2.*, 
  booking_pause_reason, 
  pause_reason
from 
(select 
    fin.*,
    datediff(mx_deal_load_date, mx_bt_deal_load_date) normal_vs_bt_attrition_fl,
    datediff(mx_deal_load_date, mn_deal_load_date) normal_merch_attrition_days,
    datediff(mx_bt_deal_load_date, mn_bt_deal_load_date) bt_attrition_days
from
(select
  mn.deal_uuid,
  mn.l1,
  mn.l2,
  mn.country_code,
  mn.mn_deal_load_date,
  mn.mx_deal_load_date,
  mn.mn_bt_deal_load_date, 
  mn.mx_bt_deal_load_date, 
  count(distinct case when cast(d.book_date as date) between mn.mn_bt_deal_load_date and mn.mx_bt_deal_load_date then d.parent_order_uuid end)  total_bt_orders,
  count(distinct case when cast(d.order_date as date) between mn.mn_deal_load_date and mn.mx_deal_load_date then d.parent_order_uuid end)  total_orders
  from
(select
  mn1.deal_uuid, 
  mn1.l1,
  mn1.l2,
  mn1.country_code,
  case when mn1.mn_deal_load_date > mn1.mn_bt_deal_load_date then mn1.mn_bt_deal_load_date else mn1.mn_deal_load_date end mn_deal_load_date,
  case when mn1.mx_deal_load_date < mn1.mx_bt_deal_load_date then mn1.mx_bt_deal_load_date else mn1.mx_deal_load_date end mx_deal_load_date,
  mn1.mn_bt_deal_load_date, 
  mn1.mx_bt_deal_load_date
from 
(select
     ad.deal_uuid,
     gdl.country_code,
     gdl.l1,
     gdl.l2,
     min(cast(ad.mn_load_date as date)) mn_deal_load_date,
     max(cast(ad.mx_load_date as date)) mx_deal_load_date, 
     min(cast(bt.mn_bt_load_date as date)) mn_bt_deal_load_date,
     max(cast(bt.mx_bt_load_date as date)) mx_bt_deal_load_date
from
      grp_gdoop_bizops_db.nvp_bt_groupon_dates ad
   left join 
       grp_gdoop_bizops_db.nvp_bt_deal_dates bt on bt.deal_uuid = ad.deal_uuid
   join      
        (select 
               deal_id, 
               max(grt_l1_cat_name) l1,
               max(grt_l2_cat_name) l2,
               max(grt_l2_cat_name) l3,
               max(country_code) country_code
             from user_edwprod.dim_gbl_deal_lob group by deal_id
            ) gdl on ad.deal_uuid = gdl.deal_id
   group by ad.deal_uuid, gdl.country_code, gdl.l2, gdl.l1
        ) mn1 ) mn
left join
         grp_gdoop_bizops_db.nvp_wbr_deals_booked d on mn.deal_uuid = d.deal_uuid
group by
      mn.deal_uuid, 
      mn.l1,
      mn.l2,
      mn.country_code,
      mn.mn_deal_load_date,
      mn.mx_deal_load_date,
      mn.mn_bt_deal_load_date, 
      mn.mx_bt_deal_load_date) fin) fin_2
left join 
     grp_gdoop_bizops_db.sh_bt_paused_reason as b on fin_2.deal_uuid = b.deal_uuid
left join 
     grp_gdoop_bizops_db.nvp_bt_groupon_pause_reason as c on fin_2.deal_uuid = c.deal_uuid  
;
     
     
     
drop table grp_gdoop_bizops_db.nvp_temp_attrition;
 create table grp_gdoop_bizops_db.nvp_temp_attrition stored as orc as
 select 
     gdl.country_code, 
     count(distinct ad.deal_uuid),
     count(distinct e.merchant_uuid)
 from
    (select
          deal_uuid,
          min(cast(load_date as date)) mn_load_date,
          max(cast(load_date as date)) mx_load_date
       from prod_groupondw.active_deals
      where
         sold_out = 'false'
         and available_qty > 0
         and cast(load_date as date) >= cast('2018-01-01' as date)
        group by deal_uuid) ad
  left join
       (select
             deal_uuid,
             min(cast(load_date as date)) mn_bt_load_date,
             max(cast(load_date as date)) mx_bt_load_date
         from grp_gdoop_bizops_db.sh_bt_active_deals_log
         where 
            partner_inactive_flag = 0
            and product_is_active_flag = 1
        group by deal_uuid
        ) as bt on bt.deal_uuid = ad.deal_uuid
  left join
       (select 
             product_uuid, 
             max(merchant_uuid) merchant_uuid 
           from user_edwprod.dim_offer_ext 
           group by product_uuid) as e on ad.deal_uuid = e.product_uuid
  join
       (select 
              deal_id, 
              grt_l1_cat_name l1,
              grt_l2_cat_name l2,
              grt_l2_cat_name l3,
              country_code
            from user_edwprod.dim_gbl_deal_lob
           ) gdl on ad.deal_uuid = gdl.deal_id
   where bt.mn_bt_load_date between cast('2020-06-01' as date) and cast('2020-06-30' as date)
   group by gdl.country_code
   ;
     
     
     
     