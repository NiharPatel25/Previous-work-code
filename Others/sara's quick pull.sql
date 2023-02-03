/*

Merchants who use the Booking Tool experience xx% more volume (conversions) done

Merchants who use the Booking Tool generate xx% more repeated customers? done
Merchants who use the Booking Tool reduce the returns by xx%? done
How many merchants use the Booking Tool? (Either % or number) done
How many merchants use the integration with other Booking Tools? (either % or number). GCAL sync NAM Only
And for the HBW page, we need % - how many HBW merchants use the Booking Tool?  done

--------------------------------*/



--------------------------------Conversion
drop table nvp_udv;

create volatile multiset table nvp_udv as (
select 
  report_date, 
  deal_id, 
  country_code,
  sum(uniq_deal_views) udv, 
  sum(uniq_buy_btn_clicks) ubbc, 
  sum(uniq_cart_chkout_views) ucheckout, 
  sum(transactions) transactions, 
  sum(transactions_qty) transaction_qty
from
 user_edwprod.agg_gbl_traffic_fin_deal as a
 where cast(a.report_date as date) >=cast('2019-07-01' as date) 
     and cast(a.report_date as date) <= cast('2019-09-30' as date)
     and grt_l1_cat_name = 'L1 - Local'
     and country_code <> 'US'
 group by 1,2,3
) with data on commit preserve rows;


drop table nvp_conv;
create volatile multiset table nvp_conv as(
select 
    ad.deal_uuid, 
    ad.load_date, 
    case when bo.deal_uuid is not null then 1 else 0 end bookable
    from
(select
          deal_uuid,
          load_date
       from user_groupondw.active_deals
      where
         sold_out = 'false'
         and available_qty > 0
         and cast(load_date as date) >= cast('2019-07-01' as date) and cast(load_date as date) <= cast('2019-09-30' as date)
        group by load_date, deal_uuid) ad
join user_edwprod.dim_gbl_deal_lob gdl on gdl.deal_id = ad.deal_uuid and gdl.grt_l1_cat_name = 'L1 - Local' and country_code <> 'US'
left join 
(select
          deal_uuid,
          load_date
      from sandbox.sh_bt_active_deals_log
      where product_is_active_flag = 1 and is_bookable = 1 and partner_inactive_flag = 0 
            and cast(load_date as date) >= cast('2019-07-01' as date) 
            and cast(load_date as date) <= cast('2019-09-30' as date)
      group by deal_uuid, load_date) bo on ad.deal_uuid = bo.deal_uuid and ad.load_date = bo.load_date
)with data on commit preserve rows;
     


select 
    bookable,
    count(distinct a.deal_uuid) total_deals,
    sum(udv) udv, 
    sum(ubbc) ubbc, 
    sum(ucheckout) checkout, 
    sum(transactions) transactions, 
    sum(transaction_qty) transaction_qty
from
   nvp_conv as a 
   left join 
   nvp_udv as b on a.deal_uuid = b.deal_id and a.load_date = b.report_date
   group by 1;


--------------------------------No Impact on refund rate

drop table nvp_book_orders;
select * from user_edwprod.dim_gbl_deal_lob;

drop table nvp_book_orders;
create volatile multiset table nvp_book_orders as(
select 
    parent_order_uuid, 
    order_uuid,
    gdl.country_code,
    gdl.grt_l2_cat_name,
    gdl.grt_l3_cat_name,
    case when bo.deal_uuid is not null then 1 else 0 end as bookable_order
from user_edwprod.fact_gbl_transactions as a
join user_edwprod.dim_gbl_deal_lob gdl on gdl.deal_id = a.deal_uuid and gdl.grt_l1_cat_name = 'L1 - Local' and country_code = 'US'
left join 
     (select
          deal_uuid,
          load_date
      from sandbox.sh_bt_active_deals_log
      where product_is_active_flag = 1 and is_bookable = 1 and partner_inactive_flag = 0 
            and cast(load_date as date) >= cast('2020-07-01' as date) 
            and cast(load_date as date) <= cast('2020-09-30' as date)
      group by deal_uuid, load_date) as bo on a.deal_uuid = bo.deal_uuid and a.order_date = bo.load_date
where a.action = 'authorize' 
      and cast(a.order_date as date) >= cast('2020-07-01' as date) 
      and cast(a.order_date as date) <= cast('2020-09-30' as date)
      and a.country_id = 235
      and a.order_uuid <> '-1'
     ) with data on commit preserve rows;


drop table nvp_book_refunds;
create volatile multiset table nvp_book_refunds as(
select 
    parent_order_uuid, 
    order_uuid
from user_edwprod.fact_gbl_transactions as a
where a.action = 'refund'
      and a.country_id = 235
      and a.order_uuid <> '-1'
      group by 1,2
) with data on commit preserve rows;

select * from user_edwprod.dim_gbl_deal_lob;

select 
   grt_l2_cat_name,
   grt_l3_cat_name,
   bookable_order, 
   count(distinct order_uuid) total_orders, 
   count(distinct case when refunded = 1 then order_uuid end) refunded_orders, 
   cast(refunded_orders as float)/total_orders
   from
(select 
   a.*, 
   case when b.order_uuid is not null then 1 else 0 end refunded
   from 
   nvp_book_orders as a 
   left join nvp_book_refunds as b on a.parent_order_uuid = b.parent_order_uuid and a.order_uuid = b.order_uuid) as fin
   group by 1,2,3
   order by 1,2,3
  ;




-------% merchants who use booking tool
drop table nvp_deal_book_info;
create volatile multiset table nvp_deal_book_info as(
select 
     ad.deal_uuid, 
     doe.merchant_uuid, 
     gdl.grt_l2_cat_name,
     max(case when bt_.deal_uuid is not null then 1 else 0 end) deal_bookable
     from
(select
          deal_uuid,
          load_date
       from user_groupondw.active_deals
      where
         sold_out = 'false'
         and available_qty > 0
         and cast(load_date as date) >= cast('2019-07-01' as date) and cast(load_date as date) <= cast('2019-07-07' as date)
        group by load_date, deal_uuid) ad
left join
(select
          deal_uuid,
          load_date
      from sandbox.sh_bt_active_deals_log
      where product_is_active_flag = 1 and is_bookable = 1 and partner_inactive_flag = 0 
            and cast(load_date as date) >= cast('2019-07-01' as date) 
            and cast(load_date as date) <= cast('2019-07-07' as date)
      group by deal_uuid, load_date) bt_ on bt_.deal_uuid = ad.deal_uuid and ad.load_date = bt_.load_date
join user_edwprod.dim_gbl_deal_lob gdl on gdl.deal_id = ad.deal_uuid and gdl.grt_l1_cat_name = 'L1 - Local' and gdl.country_code <> 'US'
left join (select product_uuid, max(merchant_uuid) merchant_uuid from user_edwprod.dim_offer_ext group by 1) as doe on ad.deal_uuid = doe.product_uuid
group by 1,2,3
) with data on commit preserve rows;



select 
   grt_l2_cat_name,
   count(distinct merchant_uuid) total_merchants, 
   count(distinct case when deal_bookable = 1 then merchant_uuid end) bookable_merchant
   from 
   nvp_deal_book_info group by 1;



select 
   count(distinct merchant_uuid) total_merchants, 
   count(distinct case when bookability = 1 then merchant_uuid end) bookable_merchants
   from
(select 
   a.merchant_uuid, 
   max(b.deal_bookable) bookability
from user_edwprod.dim_offer_ext as a
       join nvp_deal_book_info b on a.product_uuid = b.deal_uuid
    where inv_product_uuid <> '-1'
    group by 1) as tp;


drop table nvp_deal_book_info2;
create volatile multiset table nvp_deal_book_info2 as(
select 
     ad.deal_uuid, 
     doe.merchant_uuid, 
     gdl.grt_l2_cat_name,
     max(case when gcal.deal_uuid is not null then 1 else 0 end) gcal_synced,
     max(case when bt_.deal_uuid is not null then 1 else 0 end) deal_bookable
     from
(select
          deal_uuid,
          load_date
       from user_groupondw.active_deals
      where
         sold_out = 'false'
         and available_qty > 0
         and cast(load_date as date) >= cast('2020-12-01' as date) and cast(load_date as date) <= cast('2020-12-07' as date)
        group by load_date, deal_uuid) ad
left join
(select
          deal_uuid,
          load_date
      from sandbox.sh_bt_active_deals_log
      where product_is_active_flag = 1 and is_bookable = 1 and partner_inactive_flag = 0 
            and cast(load_date as date) >= cast('2020-12-01' as date) 
            and cast(load_date as date) <= cast('2020-12-07' as date)
      group by deal_uuid, load_date) bt_ on bt_.deal_uuid = ad.deal_uuid and ad.load_date = bt_.load_date
left join 
(select 
          deal_uuid, 
          load_date
      from 
          sandbox.sh_bt_active_deals_log_v4
      where cast(load_date as date) >= cast('2020-12-01' as date) and cast(load_date as date) <= cast('2020-12-07' as date) and has_gcal = 1
      group by deal_uuid, load_date
) gcal on gcal.deal_uuid = ad.deal_uuid and gcal.load_date = ad.load_date
join user_edwprod.dim_gbl_deal_lob gdl on gdl.deal_id = ad.deal_uuid and gdl.grt_l1_cat_name = 'L1 - Local' and gdl.country_code = 'US'
left join (select product_uuid, max(merchant_uuid) merchant_uuid from user_edwprod.dim_offer_ext group by 1) as doe on ad.deal_uuid = doe.product_uuid
group by 1,2,3
) with data on commit preserve rows;


select 
   count(distinct merchant_uuid) total_merchants, 
   count(distinct case when deal_bookable = 1 then merchant_uuid end) bookable_merchant,
   count(distinct case when gcal_synced = 1 then merchant_uuid end) gcal_merchant
from
   nvp_deal_book_info2;

  
  
select * from sandbox.sh_bt_active_deals_log_v4;

-------Bookable Merchants





--------------------------------Booking gerenates xx% more repeated customers ?

/* USING THIS FOR DIFF AND DIFF BELOW
drop table nvp_deal_info;
create volatile multiset table nvp_deal_info as(
select 
     ad.deal_uuid, 
     ad.mn_load_date,
     ad.mx_load_date,
     bt_.mn_bt_load_date, 
     bt_.mx_bt_load_date
     from
(select
          deal_uuid,
          min(cast(load_date as date)) mn_load_date,
          max(cast(load_date as date)) mx_load_date
       from user_groupondw.active_deals
      where
         sold_out = 'false'
         and available_qty > 0
         and cast(load_date as date) >= cast('2019-07-01' as date) and cast(load_date as date) <= cast('2019-07-31' as date)
        group by deal_uuid) ad
left join
(select
          deal_uuid,
          min(cast(load_date as date)) mn_bt_load_date,
          max(cast(load_date as date)) mx_bt_load_date
      from sandbox.sh_bt_active_deals_log
      where product_is_active_flag = 1 and is_bookable = 1 and partner_inactive_flag = 0 
            and cast(load_date as date) >= cast('2019-03-01' as date) 
            and cast(load_date as date) <= cast('2019-03-31' as date)
      group by deal_uuid) bt_ on bt_.deal_uuid = ad.deal_uuid
group by 1,2,3,4,5
) with data on commit preserve rows;*/

drop table nvp_deal_book_info;
create volatile multiset table nvp_deal_book_info as(
select 
     ad.deal_uuid, 
     doe.merchant_uuid, 
     gdl.grt_l2_cat_name,
     max(case when bt_.deal_uuid is not null then 1 else 0 end) deal_bookable
     from
(select
          deal_uuid,
          load_date
       from user_groupondw.active_deals
      where
         sold_out = 'false'
         and available_qty > 0
         and cast(load_date as date) >= cast('2020-06-01' as date) and cast(load_date as date) <= cast('2020-06-30' as date)
        group by load_date, deal_uuid) ad
left join
(select
          deal_uuid,
          load_date
      from sandbox.sh_bt_active_deals_log
      where product_is_active_flag = 1 and is_bookable = 1 and partner_inactive_flag = 0 
            and cast(load_date as date) >= cast('2020-06-01' as date) 
            and cast(load_date as date) <= cast('2020-06-30' as date)
      group by deal_uuid, load_date) bt_ on bt_.deal_uuid = ad.deal_uuid and ad.load_date = bt_.load_date
join user_edwprod.dim_gbl_deal_lob gdl on gdl.deal_id = ad.deal_uuid and gdl.grt_l1_cat_name = 'L1 - Local' and gdl.country_code <> 'US'
left join (select product_uuid, max(merchant_uuid) merchant_uuid from user_edwprod.dim_offer_ext group by 1) as doe on ad.deal_uuid = doe.product_uuid
group by 1,2,3
) with data on commit preserve rows;


/*
drop table nvp_merch_info;
create volatile multiset table nvp_merch_info as 
 (select
         product_uuid product_uuid,
         mn_load_date,
         mx_load_date,
         mn_bt_load_date,
         mx_bt_load_date,
         merchant_uuid
    from user_edwprod.dim_offer_ext as a
       left join nvp_deal_info b on a.product_uuid = b.deal_uuid
    where inv_product_uuid <> '-1'
    group by 1,2,3,4,5,6
    ) with data on commit preserve rows;

select * from nvp_merch_info_book;*/
/*
drop table nvp_merch_info_book;
create volatile multiset table nvp_merch_info_book as (
select 
merchant_uuid,
max(case when mn_bt_load_date <= cast('2019-03-02' as date) and mx_bt_load_date >= cast('2019-03-29' as date) then 1 else 0 end) merchant_bookable
from
(select 
    merchant_uuid, 
    min(mn_bt_load_date) mn_bt_load_date, 
    max(mx_bt_load_date) mx_bt_load_date
from nvp_merch_info
group by 1
) as a group by 1 )
with data on commit preserve rows;*/





drop table nvp_merch_info_book;
create volatile multiset table nvp_merch_info_book as (
select 
merchant_uuid, 
max(deal_bookable) merchant_bookable
from nvp_deal_book_info group by 1) with data on commit preserve rows;


drop table nvp_first_transaction;
create volatile multiset table nvp_first_transaction as (
select 
    user_uuid user_uuid_in, 
    fin.merchant_uuid merchant_uuid_in, 
    country_code country_code_in,
    fin2.merchant_bookable, 
    min(order_date) order_date_in,
    max(grt_l2_cat_name) l2_in,
    max(grt_l3_cat_name) l3_in
    from
    (select 
		      a.*,
		      ROW_NUMBER() over (partition by user_uuid, merchant_uuid order by order_date) rank_heirarchy
		   from 
		   (select
		      fgt.*, 
		      gdl.country_code,
		      gdl.grt_l2_cat_name,
		      gdl.grt_l3_cat_name, 
		      pds.pds_cat_name
		    from
		      user_edwprod.fact_gbl_transactions as fgt
		      join user_edwprod.dim_gbl_deal_lob gdl on gdl.deal_id = fgt.deal_uuid and gdl.country_code <> 'US' and gdl.grt_l1_cat_name = 'L1 - Local'
		      left join user_dw.v_dim_pds_grt_map pds on pds.pds_cat_id = gdl.pds_cat_id
		    where 
		      order_date >= cast('2020-06-01' AS date)
		      AND 
		      order_date <= cast('2020-06-30' AS date)
		      and
		      fgt.action = 'authorize'
		      and fgt.order_uuid <> '-1') as a
		    ) as fin 
    left join nvp_merch_info_book as fin2 on fin.merchant_uuid = fin2.merchant_uuid
    group by 1,2,3,4
) with data no primary index on commit preserve rows;

select * from nvp_first_transaction order by user_uuid_in;

drop table nvp_first_transaction_stage2;
create volatile multiset table nvp_first_transaction_stage2 as 
(select 
         b.*,
         gdl.country_code country_code,
         gdl.grt_l2_cat_name l2,
         gdl.grt_l3_cat_name l3, 
         pds.pds_cat_name
      from
         user_edwprod.fact_gbl_transactions as b
         join user_edwprod.dim_gbl_deal_lob gdl on gdl.deal_id = b.deal_uuid and gdl.country_code <> 'US' and gdl.grt_l1_cat_name = 'L1 - Local'
         left join user_dw.v_dim_pds_grt_map pds on pds.pds_cat_id = gdl.pds_cat_id
      where b."action" = 'authorize' and b.order_date >= cast('2020-06-01' AS date) and b.order_date <= cast('2021-01-15' AS date) and b.order_uuid <> '-1') 
with data on commit preserve rows;



drop table sandbox.nvp_repeat_merchant_website;
create multiset table sandbox.nvp_repeat_merchant_website as (
select 
    a.*,
    fgt.deal_uuid, 
    fgt.order_date, 
    fgt.order_uuid, 
    fgt.merchant_uuid,
    fgt.l2, 
    fgt.l3, 
    fgt.pds_cat_name
from nihpatel.nvp_first_transaction as a 
left join
     nihpatel.nvp_first_transaction_stage2 fgt 
        on fgt.order_date <= a.order_date_in + 60
        and fgt.order_date > a.order_date_in 
        and a.user_uuid_in = fgt.user_uuid 
        and a.merchant_uuid_in = fgt.merchant_uuid
group by 
     1,2,3,4,5,6,7,8,9,10,11,12,13
) with data;



select 
   merchant_bookable, 
   count(distinct concat(user_uuid_in, merchant_uuid_in)) initial_purchases, 
   count(distinct case when order_purchased > 0 then concat(user_uuid_in, merchant_uuid_in) end) repeat_purchases,
   sum(order_purchased) repeats, 
   cast(repeats as float)/initial_purchases
from 
(select 
    user_uuid_in, 
    merchant_uuid_in,
    merchant_bookable,
    count(distinct order_uuid) order_purchased
from sandbox.nvp_repeat_merchant_website
group by 1,2,3) as a
group by 1
order by merchant_bookable;



------TONJA REPEATING AT SAME MERCHANT > 4 TIMES
drop table nvp_deal_book_info;
create volatile multiset table nvp_deal_book_info as(
select 
     ad.deal_uuid, 
     doe.merchant_uuid, 
     gdl.grt_l2_cat_name,
     max(case when bt_.deal_uuid is not null then 1 else 0 end) deal_bookable
     from
(select
          deal_uuid,
          load_date
       from user_groupondw.active_deals
      where
         cast(load_date as date) >= cast('2020-01-01' as date) and cast(load_date as date) <= cast('2020-09-30' as date)
        group by load_date, deal_uuid) ad
left join
(select
          deal_uuid,
          load_date
      from sandbox.sh_bt_active_deals_log
      where product_is_active_flag = 1 and is_bookable = 1 and partner_inactive_flag = 0 
            and cast(load_date as date) >= cast('2020-06-01' as date) 
            and cast(load_date as date) <= cast('2020-06-30' as date)
      group by deal_uuid, load_date) bt_ on bt_.deal_uuid = ad.deal_uuid and ad.load_date = bt_.load_date
join user_edwprod.dim_gbl_deal_lob gdl on gdl.deal_id = ad.deal_uuid and gdl.grt_l1_cat_name = 'L1 - Local' and gdl.country_code = 'US'
left join (select product_uuid, merchant_uuid from user_edwprod.dim_offer_ext group by 1,2) as doe on ad.deal_uuid = doe.product_uuid
group by 1,2,3
) with data on commit preserve rows;

select * from user_edwprod.dim_offer_ext where merchant_uuid = '60b95f8b-13ee-499b-bfbc-52ed6516ac55';
select * from user_edwprod.dim_gbl_deal_lob where deal_id = '4d3110ec-d6b3-4ac8-85f1-0ca6c4189207';
select * from user_groupondw.active_deals 
where deal_uuid = '4d3110ec-d6b3-4ac8-85f1-0ca6c4189207';

drop table nvp_merch_info_book;
create volatile multiset table nvp_merch_info_book as (
select 
merchant_uuid, 
max(deal_bookable) merchant_bookable
from nvp_deal_book_info group by 1) with data on commit preserve rows;

select count(distinct merchant_uuid) from nvp_merch_info_book;

drop table nvp_first_transaction;
create volatile multiset table nvp_first_transaction as (
select 
    user_uuid user_uuid_in, 
    fin.merchant_uuid merchant_uuid_in, 
    country_code country_code_in,
    case when fin2.merchant_bookable = 1 then 1 else 0 end merchant_bookable, 
    min(order_date) order_date_in,
    max(grt_l2_cat_name) l2_in,
    max(grt_l3_cat_name) l3_in, 
    max(deal_uuid) deal_sample_in
    from
    (select 
		      a.*,
		      ROW_NUMBER() over (partition by user_uuid, merchant_uuid order by order_date) rank_heirarchy
		   from 
		   (select
		      fgt.*, 
		      gdl.country_code,
		      gdl.grt_l2_cat_name,
		      gdl.grt_l3_cat_name, 
		      pds.pds_cat_name
		    from
		      user_edwprod.fact_gbl_transactions as fgt
		      join user_edwprod.dim_gbl_deal_lob gdl on gdl.deal_id = fgt.deal_uuid and gdl.country_code = 'US' and gdl.grt_l1_cat_name = 'L1 - Local'
		      left join user_dw.v_dim_pds_grt_map pds on pds.pds_cat_id = gdl.pds_cat_id
		    where 
		      fgt.order_date >= cast('2020-06-01' AS date)
		      AND 
		      fgt.order_date <= cast('2020-06-30' AS date)
		      and
		      fgt.action = 'authorize'
		      and fgt.order_uuid <> '-1') as a
		    ) as fin 
    left join nvp_merch_info_book as fin2 on fin.merchant_uuid = fin2.merchant_uuid
    where rank_heirarchy = 1
    group by 1,2,3,4
) with data no primary index on commit preserve rows;



drop table nvp_first_transaction_stage2;
create volatile multiset table nvp_first_transaction_stage2 as 
(select 
         b.*,
         gdl.country_code country_code,
         gdl.grt_l2_cat_name l2,
         gdl.grt_l3_cat_name l3, 
         pds.pds_cat_name
      from
         user_edwprod.fact_gbl_transactions as b
         join user_edwprod.dim_gbl_deal_lob gdl on gdl.deal_id = b.deal_uuid and gdl.country_code = 'US' and gdl.grt_l1_cat_name = 'L1 - Local'
         left join user_dw.v_dim_pds_grt_map pds on pds.pds_cat_id = gdl.pds_cat_id
      where b."action" = 'authorize' and b.order_date >= cast('2020-06-01' AS date) and b.order_date <= cast('2021-01-15' AS date) and b.order_uuid <> '-1') 
with data on commit preserve rows;



drop table sandbox.nvp_repeat_merchant_4;
create multiset table sandbox.nvp_repeat_merchant_4 as (
select 
    a.*,
    fgt.deal_uuid, 
    fgt.order_date, 
    fgt.order_uuid, 
    fgt.merchant_uuid,
    fgt.l2, 
    fgt.l3, 
    fgt.pds_cat_name
from nihpatel.nvp_first_transaction as a 
left join
     nihpatel.nvp_first_transaction_stage2 fgt 
        on fgt.order_date <= a.order_date_in + 180
        and fgt.order_date > a.order_date_in 
        and a.user_uuid_in = fgt.user_uuid 
        and a.merchant_uuid_in = fgt.merchant_uuid
group by 
     1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
) with data;


select 
   merchant_bookable, 
   count(distinct user_uuid_in) user_merchant_concat, 
   count(distinct case when order_purchased >= 4 then user_uuid_in end) user_merchant_repeats_concat,
   sum(order_purchased) repeat_orders, 
   cast(user_merchant_repeats_concat as float)/user_merchant_concat
from 
(select 
    user_uuid_in, 
    merchant_uuid_in,
    merchant_bookable,
    count(distinct order_uuid) order_purchased
from sandbox.nvp_repeat_merchant_4
group by 1,2,3) as a
group by 1
order by merchant_bookable;


select 
   merchant_uuid_in,
   merchant_bookable,
   deal_sample,
   max(case when order_purchased >= 4 then 1 else 0 end) merchant_had_user_come_4times_or_more,
   max(l2_in) l2_in,
   max(l3_in) l3_in,
   count(distinct user_uuid_in) total_purchasers_june,
   count(distinct case when order_purchased <> 0 then user_uuid_in end) repeat_purchasing_users, 
   count(distinct case when order_purchased >= 4 then user_uuid_in end) users_purchased_more_than_4times,
   sum(order_purchased) total_repeat_orders,
   sum(case when order_purchased >= 4 then order_purchased end) orders_by_users_more_than4
from 
(select 
    user_uuid_in, 
    merchant_uuid_in,
    merchant_bookable,
    l2_in,
    l3_in,
    count(distinct order_uuid) order_purchased,
    max(deal_sample_in) deal_sample
from sandbox.nvp_repeat_merchant_4
group by 1,2,3,4,5) as a
group by 1,2,3
order by merchant_bookable desc;


select 
   merchant_bookable, 
   count(distinct merchant_uuid_in) distinct_merchants, 
   count(distinct case when order_purchased >= 4 then merchant_uuid_in end) merch_repeat
from (select 
    user_uuid_in, 
    merchant_uuid_in,
    merchant_bookable,
    l2_in,
    l3_in,
    count(distinct order_uuid) order_purchased
from sandbox.nvp_repeat_merchant_4
group by 1,2,3,4,5) as a
group by 1;

---------------DIFF AND DIFF


drop table nvp_deal_info2;
create volatile multiset table nvp_deal_info2 as(
select 
     doe.merchant_uuid,
     ad.deal_uuid, 
     ad.mn_load_date,
     ad.mx_load_date,
     bt_.mn_bt_load_date, 
     bt_.mx_bt_load_date
from
(select
          deal_uuid,
          min(cast(load_date as date)) mn_load_date,
          max(cast(load_date as date)) mx_load_date
       from user_groupondw.active_deals
      where
         sold_out = 'false'
         and available_qty > 0
        group by deal_uuid) ad
left join
(select
          deal_uuid,
          min(cast(load_date as date)) mn_bt_load_date,
          max(cast(load_date as date)) mx_bt_load_date
      from sandbox.sh_bt_active_deals_log
      where product_is_active_flag = 1 and is_bookable = 1 and partner_inactive_flag = 0 
      group by deal_uuid) bt_ on bt_.deal_uuid = ad.deal_uuid
left join (select product_uuid, merchant_uuid from user_edwprod.dim_offer_ext group by 1,2) as doe on ad.deal_uuid = doe.product_uuid
join 
group by 1,2,3,4,5,6
) with data on commit preserve rows;

drop table nvp_merch_bt;
create multiset volatile table nvp_merch_bt as (
select
    a.*,
    mn_bt_load_date - mn_load_date on_non_bt_time,
    mx_bt_load_date - mn_bt_load_date on_bt_time
    from
(select 
     merchant_uuid, 
     min(mn_load_date) mn_load_date, 
     max(mx_load_date) mx_load_date, 
     min(mn_bt_load_date) mn_bt_load_date, 
     max(mx_bt_load_date) mx_bt_load_date
  from 
    nvp_deal_info2 group by 1
) as a 
where mn_bt_load_date - mn_load_date >= 60
and mx_bt_load_date - mn_bt_load_date >= 60
and mn_bt_load_date >= cast('2019-07-01' AS date) and mn_bt_load_date <= cast('2019-09-30' as date)
) with data on commit preserve rows
;

drop table nvp_merch_non_bt;
create multiset volatile table nvp_merch_non_bt as (
select
   a.merchant_uuid, 
   min(a.mn_load_date) mn_load_date, 
   max(a.mx_load_date) mx_load_date
from
   nvp_deal_info2 a 
   where a.mn_bt_load_date is null
   group by 1
) with data on commit preserve rows


select * from nvp_merch_non_bt;

drop table nvp_fgt_transactions;
create volatile multiset table nvp_fgt_transactions as 
(select 
         b.*,
         gdl.country_code country_code,
         gdl.grt_l2_cat_name l2,
         gdl.grt_l3_cat_name l3, 
         pds.pds_cat_name
      from
         user_edwprod.fact_gbl_transactions as b
         join user_edwprod.dim_gbl_deal_lob gdl on gdl.deal_id = b.deal_uuid and gdl.country_code <> 'US' and gdl.grt_l1_cat_name = 'L1 - Local'
         left join user_dw.v_dim_pds_grt_map pds on pds.pds_cat_id = gdl.pds_cat_id
      where b."action" = 'authorize' and b.order_date >= cast('2019-04-01' AS date) and b.order_date < cast('2020-01-30' AS date) and b.order_uuid <> '-1') 
with data on commit preserve rows;



select 
   * 
   from
   (select 
      country_code,
      count(distinct merchant_uuid) total_merchants_bt, 
      case when sum(users_before_bt) <> 0 then cast(sum(order_before_bt) as float)/sum(users_before_bt) end frequency_before, 
      case when sum(users_after_bt) <> 0 then cast(sum(order_after_bt) as float)/sum(users_after_bt) end frequency_after
   from
   (select 
       a.*, 
       b.country_code,
       count(distinct case when b.order_date >= a.mn_bt_load_date - 60 and b.order_date < a.mn_bt_load_date then order_uuid end) order_before_bt, 
       count(distinct case when b.order_date >= a.mn_bt_load_date - 60 and b.order_date < a.mn_bt_load_date then user_uuid end) users_before_bt, 
       count(distinct case when b.order_date <= a.mn_bt_load_date + 60 and b.order_date > a.mn_bt_load_date then order_uuid end) order_after_bt, 
       count(distinct case when b.order_date <= a.mn_bt_load_date + 60 and b.order_date > a.mn_bt_load_date then user_uuid end) users_after_bt  
      from 
        nvp_merch_bt as a
      left join 
        nvp_fgt_transactions as b on a.merchant_uuid = b.merchant_uuid
       group by 1,2,3,4,5,6,7,8) as fin
    group by 1 ) as fin_a 
left join 
   (select 
     country_code, 
     count(distinct merchant_uuid) total_non_bt_merch, 
     case when sum(user_before_mid) <> 0 then cast(sum(order_before_mid) as float)/sum(user_before_mid) end mid_frequency_before, 
     case when sum(user_after_mid) <> 0 then cast(sum(order_after_mid) as float)/sum(user_after_mid) end mid_frequency_after
     from 
     (select 
         a.*, 
         b.country_code, 
         count(distinct case when b.order_date >= cast('2019-08-15' as date) - 60 and b.order_date < cast('2019-08-15' as date) then order_uuid end) order_before_mid,
         count(distinct case when b.order_date >= cast('2019-08-15' as date) - 60 and b.order_date < cast('2019-08-15' as date) then user_uuid end) user_before_mid,
         count(distinct case when b.order_date <= cast('2019-08-15' as date) + 60 and b.order_date > cast('2019-08-15' as date) then order_uuid end) order_after_mid,
         count(distinct case when b.order_date <= cast('2019-08-15' as date) + 60 and b.order_date > cast('2019-08-15' as date) then user_uuid end) user_after_mid
         from 
         nvp_merch_non_bt as a 
         left join 
         nvp_fgt_transactions as b on a.merchant_uuid = b.merchant_uuid
         where a.mn_load_date <= cast('2019-08-15' as date) - 60 and a.mx_load_date >= cast('2019-08-15' as date) + 60
         group by 1,2,3,4
     ) as fin
     group by 1
) as fin_b on fin_a.country_code = fin_b.country_code
;


select 
   * 
   from
(select 
    joining,
    case when sum(users_before_bt) <> 0 then cast(sum(order_before_bt) as float)/sum(users_before_bt) end frequency_before, 
    case when sum(users_after_bt) <> 0 then cast(sum(order_after_bt) as float)/sum(users_after_bt) end frequency_after
from
   (select 
     'x' joining,
     count(distinct a.merchant_uuid) merchants,
     count(distinct case when b.order_date >= a.mn_bt_load_date - 60 and b.order_date < a.mn_bt_load_date then order_uuid end) order_before_bt, 
     count(distinct case when b.order_date >= a.mn_bt_load_date - 60 and b.order_date < a.mn_bt_load_date then user_uuid end) users_before_bt, 
     count(distinct case when b.order_date <= a.mn_bt_load_date + 60 and b.order_date > a.mn_bt_load_date then order_uuid end) order_after_bt, 
     count(distinct case when b.order_date <= a.mn_bt_load_date + 60 and b.order_date > a.mn_bt_load_date then user_uuid end) users_after_bt  
   from 
     nvp_merch_bt as a
   left join 
     nvp_fgt_transactions as b on a.merchant_uuid = b.merchant_uuid
    group by 1) as fin
group by 1 ) as fin_a 
left join 
(select 
     joining,
     case when sum(user_before_mid) <> 0 then cast(sum(order_before_mid) as float)/sum(user_before_mid) end mid_frequency_before, 
     case when sum(user_after_mid) <> 0 then cast(sum(order_after_mid) as float)/sum(user_after_mid) end mid_frequency_after
     from 
     (select 
         'x' joining,
         count(distinct a.merchant_uuid) merchants,
         count(distinct case when b.order_date >= cast('2019-08-15' as date) - 60 and b.order_date < cast('2019-08-15' as date) then order_uuid end) order_before_mid,
         count(distinct case when b.order_date >= cast('2019-08-15' as date) - 60 and b.order_date < cast('2019-08-15' as date) then user_uuid end) user_before_mid,
         count(distinct case when b.order_date <= cast('2019-08-15' as date) + 60 and b.order_date > cast('2019-08-15' as date) then order_uuid end) order_after_mid,
         count(distinct case when b.order_date <= cast('2019-08-15' as date) + 60 and b.order_date > cast('2019-08-15' as date) then user_uuid end) user_after_mid
         from 
         nvp_merch_non_bt as a 
         left join 
         nvp_fgt_transactions as b on a.merchant_uuid = b.merchant_uuid
         where a.mn_load_date <= cast('2019-08-15' as date) - 60 and a.mx_load_date >= cast('2019-08-15' as date) + 60
         group by 1
     ) as fin
     group by 1
) as fin_b on fin_a.joining = fin_b.joining
;



----------------------CONVERSION 


drop table nvp_udv;
create volatile multiset table nvp_udv as (
select 
  report_date, 
  deal_id, 
  country_code,
  sum(uniq_deal_views) udv, 
  sum(uniq_buy_btn_clicks) ubbc, 
  sum(uniq_cart_chkout_views) ucheckout, 
  sum(transactions) transactions, 
  sum(transactions_qty) transaction_qty
from
 user_edwprod.agg_gbl_traffic_fin_deal as a
 where cast(a.report_date as date) >=cast('2019-04-01' AS date) 
     and cast(a.report_date as date) <= cast('2020-01-30' AS date)
     and grt_l1_cat_name = 'L1 - Local'
     and country_code <> 'US'
 group by 1,2,3
) with data on commit preserve rows;



create volatile multiset table nvp_merch_udv as (
select 
   a.report_date, 
   doe.merchant_uuid, 
   a.country_code, 
   sum(udv) udv, 
   sum(ubbc) ubcc, 
   sum(ucheckout) ucheckout, 
   sum(transactions) transactions, 
   sum(transaction_qty) transaction_qty
   from 
   nvp_udv as a 
   left join (select product_uuid, merchant_uuid from user_edwprod.dim_offer_ext group by 1,2) as doe on a.deal_id = doe.product_uuid
   group by 1,2,3
) with data on commit preserve rows;





select 
   * 
   from
(select 
    joining,
    case when sum(udv_before_bt) <> 0 then cast(sum(transaction_qty_before_bt) as float)/sum(udv_before_bt) end conv_before, 
    case when sum(udv_after_bt) <> 0 then cast(sum(transaction_qty_after_bt) as float)/sum(udv_after_bt) end conv_after, 
    case when sum(merchants) <> 0 then cast(sum(udv_before_bt) as float)/sum(merchants) end udv_per_merch_before, 
    case when sum(merchants) <> 0 then cast(sum(udv_after_bt) as float)/sum(merchants) end udv_per_merch_after
from
   (select 
     'x' joining,
     count(distinct a.merchant_uuid) merchants,
     sum(distinct case when b.report_date >= a.mn_bt_load_date - 60 and b.report_date < a.mn_bt_load_date then transaction_qty end) transaction_qty_before_bt, 
     sum(distinct case when b.report_date >= a.mn_bt_load_date - 60 and b.report_date < a.mn_bt_load_date then udv end) udv_before_bt, 
     sum(distinct case when b.report_date <= a.mn_bt_load_date + 60 and b.report_date > a.mn_bt_load_date then transaction_qty end) transaction_qty_after_bt, 
     sum(distinct case when b.report_date <= a.mn_bt_load_date + 60 and b.report_date > a.mn_bt_load_date then udv end) udv_after_bt  
   from 
     nvp_merch_bt as a
    left join 
    nvp_merch_udv as b on a.merchant_uuid = b.merchant_uuid
    group by 1) as fin
group by 1 ) as fin_a 
left join 
(select 
     joining,
     case when sum(udv_before_mid) <> 0 then cast(sum(transaction_before_mid) as float)/sum(udv_before_mid) end mid_conv_before, 
     case when sum(udv_after_mid) <> 0 then cast(sum(transaction_after_mid) as float)/sum(udv_after_mid) end mid_conv_after, 
     case when sum(merchants) <> 0 then cast(sum(udv_after_mid) as float)/sum(merchants) end udv_per_merch_after_mid, 
     case when sum(merchants) <> 0 then cast(sum(udv_before_mid) as float)/sum(merchants) end udv_per_merch_before_mid 
     from 
    (select 
         'x' joining,
         count(distinct a.merchant_uuid) merchants,
         sum(distinct case when b.report_date >= cast('2019-08-15' as date) - 60 and b.report_date < cast('2019-08-15' as date) then transaction_qty end) transaction_before_mid,
         sum(distinct case when b.report_date >= cast('2019-08-15' as date) - 60 and b.report_date < cast('2019-08-15' as date) then udv end) udv_before_mid,
         sum(distinct case when b.report_date <= cast('2019-08-15' as date) + 60 and b.report_date > cast('2019-08-15' as date) then transaction_qty end) transaction_after_mid,
         sum(distinct case when b.report_date <= cast('2019-08-15' as date) + 60 and b.report_date > cast('2019-08-15' as date) then udv end) udv_after_mid
         from 
         nvp_merch_non_bt as a 
         left join 
         nvp_merch_udv as b on a.merchant_uuid = b.merchant_uuid
         where a.mn_load_date <= cast('2019-08-15' as date) - 60 and a.mx_load_date >= cast('2019-08-15' as date) + 60
         group by 1
     ) as fin
group by 1) fin_b on fin_a.joining = fin_b.joining
;



-----------Purchase frequency of users

drop table nvp_fgt_transactions2;
create volatile multiset table nvp_fgt_transactions2 as 
(select 
         b.*,
         gdl.country_code country_code,
         gdl.grt_l2_cat_name l2,
         gdl.grt_l3_cat_name l3, 
         pds.pds_cat_name
      from
         user_edwprod.fact_gbl_transactions as b
         join user_edwprod.dim_gbl_deal_lob gdl on gdl.deal_id = b.deal_uuid and gdl.country_code <> 'US' and gdl.grt_l1_cat_name = 'L1 - Local'
         left join user_dw.v_dim_pds_grt_map pds on pds.pds_cat_id = gdl.pds_cat_id
      where b."action" = 'authorize' and b.order_date >= cast('2019-07-01' AS date) and b.order_date <= cast('2020-09-30' AS date) and b.order_uuid <> '-1') 
with data on commit preserve rows;

create volatile multiset table nvp_purchase_freq as (
select 
   a.*, 
   case when b.deal_uuid is not null then 1 else 0 end bookable
from 
   nvp_fgt_transactions2 as a
   left join 
   (select 
      deal_uuid, load_date
      from sandbox.sh_bt_active_deals_log
      where product_is_active_flag = 1 and is_bookable = 1 and partner_inactive_flag = 0 and load_date >= cast('2019-07-01' as date) and load_date <= cast('2020-09-30' as date)
      group by 1,2) as b on a.deal_uuid = b.deal_uuid and a.order_date = b.load_date
) with data on commit preserve rows;


select 
    bookable, 
    count(distinct user_uuid) total_users, 
    count(distinct order_uuid) total_orders, 
    cast(count(distinct user_uuid) as float)/count(distinct order_uuid) freq
    from 
    nvp_purchase_freq
    group by 1
    
    
-------need not be first repeat purchase

(select 
   user_uuid,
   merchant_bookable,
   count(distinct case when merchant_uuid_in = merchant_uuid then order_uuid end) num
from sandbox.nvp_first_transaction_stage3
   group by 1,2)