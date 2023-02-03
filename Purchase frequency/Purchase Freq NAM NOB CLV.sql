drop table grp_gdoop_bizops_db.nvp_purch_clv_nam;
create table grp_gdoop_bizops_db.nvp_purch_clv_nam stored as orc as 
select 
   a.parent_order_uuid,
   a.deal_uuid, 
   a.user_uuid,
   case when bt.deal_uuid2 is not null then 1 else 0 end bookable,
   a.booked,
   a.redeemed,
   gbl.l2,
   a.units, 
   a.nob, 
   a.nor
   from 
   grp_gdoop_bizops_db.rt_bt_txns as a
   join
      (select 
          deal_id, 
          grt_l2_cat_name l2
       from 
          user_edwprod.dim_gbl_deal_lob 
         where grt_l1_cat_name = 'L1 - Local' and grt_l2_cat_name <> 'L2 - Retail' 
         group by deal_id, grt_l2_cat_name) gbl on gbl.deal_id = a.deal_uuid
    join 
       (select 
          product_uuid
        from 
          user_edwprod.dim_offer_ext 
        where inventory_service_name <> 'tpis' 
        group by product_uuid) c on a.deal_uuid = c.product_uuid
   left join 
   (select 
          load_date, 
          deal_uuid deal_uuid2
        from 
        grp_gdoop_bizops_db.sh_bt_active_deals_log 
        where product_is_active_flag = 1 and partner_inactive_flag = 0 and is_bookable = 1 and load_date >= '2020-01-01'
        ) bt on a.deal_uuid = bt.deal_uuid2 and a.order_date = bt.load_date
   where 
        cast(a.order_date as date) >= cast('2020-01-01' as date)
        and cast(a.order_date as date) <= cast('2020-12-31' as date)
        and a.country_code = 'US'
;

       
       
select 
    bookable, 
    count(distinct parent_order_uuid) total_orders, 
    sum(units) total_units, 
    sum(nob) total_nob, 
    sum(nor) total_nor,
    count(distinct user_uuid) total_users
    from 
    grp_gdoop_bizops_db.nvp_purch_clv_nam
    group by bookable
    ;
   
   

