------------------------------------------------------------------------Base table



create table grp_gdoop_bizops_db.nvp_okr_merchonboarding stored as orc as
select 
    e.merchant_uuid, 
    min(mn_load_date) mn_load_date
from (select 
           deal_uuid, 
           min(cast(load_date as date)) mn_load_date 
      from prod_groupondw.active_deals
      where sold_out = 'false' and available_qty > 0
      group by deal_uuid) a
left join 
     (SELECT DISTINCT 
            product_uuid, 
            merchant_uuid 
         FROM user_edwprod.dim_offer_ext) as e on a.deal_uuid = e.product_uuid
group by e.merchant_uuid;

------------------------------------------------------------------------30 day metro merchant count/Total New Merchants

drop table grp_gdoop_bizops_db.nvp_okr_clsd_acct;
create table grp_gdoop_bizops_db.nvp_okr_clsd_acct stored as orc as
select 
   distinct c.accountid 
from grp_gdoop_sup_analytics_db.jc_merchant_mtd_attrib c
   where cast(c.close_date as date) >= date_sub(CURRENT_DATE, 30)
   and c.grt_l1_cat_name = 'L1 - Local'
   and close_order = 1;

 

------------------------------------------------------------------------BT Eligible merchant logins MC


drop table grp_gdoop_bizops_db.nvp_mc_raw_events;
create table grp_gdoop_bizops_db.nvp_mc_raw_events stored as orc as
  select
    bcookie, 
    user_uuid, 
    event_time, 
    page_app, 
    page_url, 
    page_path, 
    split(split(page_path, '/merchant/center/')[1],'/')[0] page_path_type, 
    case when page_country = 'GB' then 'UK' else page_country end page_country, 
    platform, 
    bot_flag, 
    internal_ip_ind, 
    event, 
    widget_name, 
    widget_content_name, 
    page_type, 
    user_device, 
    user_device_type, 
    browser, 
    dt
  from prod_groupondw.bld_events
  where dt >= date_sub(current_date, 30)
   and page_app in ('merchant-center-minsky', 'android-mobile-merchant', 'ios-mobile-merchant', 'merchant-center-echo');




drop table grp_gdoop_bizops_db.nvp_okr_merch_addressable;
create table grp_gdoop_bizops_db.nvp_okr_merch_logic stored as orc as
select 
  distinct 
     mc.user_uuid,
     con.merchant_uuid, 
     con.account_uuid,
     dm.salesforce_account_id
  from grp_gdoop_bizops_db.nvp_mc_raw_events mc
  left join grp_gdoop_bizops_db.de_merchant_contact con on mc.user_uuid = con.account_uuid
  left join edwprod.dim_merchants_unity dm on con.merchant_uuid = dm.merchant_uuid;



select 
     c.vertical, 
     c.feature_country,
     count(distinct c.accountid) campaigns_submitted_accounts, 
     count(distinct case when c.dmapi_flag = 1 and close_order = 1 then c.accountid end) campaigns_metro_accounts, 
     count(distinct case when c.launch_date is not null then c.accountid end) launched_accounts,
     count(distinct case when c.launch_date is not null and c.dmapi_flag = 1 then c.accountid end) launched_metro_accounts,
     count(distinct case when c.launch_date is not null then c.deal_uuid end) deal_launches, 
     count(distinct case when c.launch_date is not null and c.dmapi_flag = 1 then c.deal_uuid end) metro_deal_launches, 
     count(distinct case when lower(ad.is_addressable_booking) = 'yes' then c.accountid end) overall_bt_addressable, 
     count(distinct case when lower(ad.is_addressable_booking) = 'yes' and c.dmapi_flag = 1 then c.accountid end) metro_bt_addressable,
     count(distinct case when lower(ad.is_addressable_booking) = 'yes' and mc.accountid is not null then c.accountid end) Merchant_center_used
from 
  grp_gdoop_sup_analytics_db.jc_merchant_mtd_attrib c
   join grp_gdoop_bizops_db.nvp_okr_clsd_acct fil on c.accountid = fil.accountid
   left join user_dw.v_dim_pds_grt_map mp on c.pds = mp.pds_cat_name
   left join grp_gdoop_bizops_db.sh_pds_addressability ad on mp.pds_cat_id = ad.pds_cat_id
   left join (select salesforce_account_id accountid from grp_gdoop_bizops_db.nvp_okr_merch_logic group by salesforce_account_id) mc on c.accountid = mc.accountid
where cast(c.close_date as date) >= date_sub(CURRENT_DATE, 30)
and c.grt_l1_cat_name = 'L1 - Local'
group by 
c.vertical,
c.feature_country
order by 
c.vertical desc, 
c.feature_country;



/*drop table grp_gdoop_bizops_db.nvp_okr_bt_addressable_merch;
create table grp_gdoop_bizops_db.nvp_okr_bt_addressable_merch stored as orc as
select
   c.vertical, 
   c.feature_country,
   count(distinct c.accountid) booking_addressable
   case when mc.accountid is not null then 1 end mc_used
from 
  grp_gdoop_sup_analytics_db.jc_merchant_mtd_attrib c
   join grp_gdoop_bizops_db.nvp_okr_clsd_acct fil on c.accountid = fil.accountid
   left join user_dw.v_dim_pds_grt_map mp on c.pds = mp.pds_cat_name
   left join grp_gdoop_bizops_db.sh_pds_addressability ad on mp.pds_cat_id = ad.pds_cat_id
   left join (select salesforce_account_id from grp_gdoop_bizops_db.nvp_okr_merch_addressable group by accountid) mc on c.accountid = mc.accountid
where 
cast(c.close_date as date) >= date_sub(CURRENT_DATE, 30)
and c.grt_l1_cat_name = 'L1 - Local'
and lower(ad.is_addressable_booking) = 'yes'
group by 
c.accountid,
case when mc.accountid is not null then 1 end;*/

------------------------------------------------------------------------------------Other Try



drop table grp_gdoop_bizops_db.nvp_okr_bt_addressable;

create table grp_gdoop_bizops_db.nvp_okr_bt_addressable stored as orc as
select 
     c.vertical, 
     c.feature_country, 
     c.accountid, 
     max(case when lower(ad.is_addressable_booking) = 'yes' then 1 else 0 end) addressable_booking
from 
(select * from grp_gdoop_sup_analytics_db.jc_merchant_mtd_attrib 
    where
    cast(close_date as date) >= date_sub(CURRENT_DATE, 30)
    and grt_l1_cat_name = 'L1 - Local'
    and close_order = 1) as c
left join user_dw.v_dim_pds_grt_map mp on c.pds = mp.pds_cat_name
left join grp_gdoop_bizops_db.sh_pds_addressability ad on mp.pds_cat_id = ad.pds_cat_id
group by 
c.vertical,
c.feature_country,
c.accountid;



/*
 * 
select 
stg1.*, 
stg2.*
from
(;

) stg1
left join 
(select 
    vertical,
    feature_country, 
    sum(addressable_booking) total_btaddressable,
    sum(case when dmapi_flag = 1 then addressable_booking end) metro_onboarded_btaddressable
  from
   (select 
       c.vertical, 
       c.feature_country, 
       c.accountid, 
       c.dmapi_flag,
       max(case when lower(ad.is_addressable_booking) = 'yes' then 1 else 0 end) addressable_booking
    from 
       (select a.vertical, a.feature_country, a.accountid, a.dmapi_flag, a.pds
          from grp_gdoop_sup_analytics_db.jc_merchant_mtd_attrib a
             join grp_gdoop_bizops_db.nvp_okr_clsd_acct fil on a.accountid = fil.accountid
          where
          cast(close_date as date) >= date_sub(CURRENT_DATE, 30)
          and grt_l1_cat_name = 'L1 - Local'
          ) as c
        left join user_dw.v_dim_pds_grt_map mp on c.pds = mp.pds_cat_name
        left join grp_gdoop_bizops_db.sh_pds_addressability ad on mp.pds_cat_id = ad.pds_cat_id
        group by 
          c.vertical,
          c.feature_country,
          c.accountid,
          c.dmapi_flag) xyz
  group by 
) stg2 on stg1.vertical = stg2.vertical and stg1.feature_country = stg2.feature_country
;

*/

select * from user_dw.v_dim_pds_grt_map;

------------------------------------------------------------------------AVG BT Deals PER MERCHANT (Has to be launched)
drop table grp_gdoop_bizops_db.nvp_okr_merch_deal;

create table grp_gdoop_bizops_db.nvp_okr_bt_merch stored as orc as
select 
   e.country_code, 
   b.l2, 
   count(distinct c.merchant_uuid) total_bt_merchants, 
   count(distinct a.deal_uuid) total_bt_deals
from
   (select deal_uuid
      from grp_gdoop_bizops_db.sh_bt_active_deals_log
      where 
        cast(load_date as date) >= date_sub(CURRENT_DATE, 30)
        and partner_inactive_flag = 0
        and product_is_active_flag = 1
        group by deal_uuid) a
  left join 
    (select 
         deal_id,
         grt_l2_cat_name l2
     from 
        user_edwprod.dim_gbl_deal_lob 
     ) b on a.deal_uuid = b.deal_id
  left join 
     (select 
         product_uuid product_uuid,
         max(merchant_uuid) merchant_uuid
      from user_edwprod.dim_offer_ext  
      where inv_product_uuid <> '-1'
      group by product_uuid) c on a.deal_uuid = c.product_uuid
  left join 
     (select 
          merchant_uuid, 
          feature_country 
       from user_edwprod.dim_merchant 
       group by merchant_uuid, feature_country
      ) d on c.merchant_uuid = d.merchant_uuid
  left join 
      (select 
          country_code, 
          country_id 
        from user_groupondw.dim_country
        group by country_code, country_id
      )  e on d.feature_country = e.country_id
     group by e.country_code, b.l2;


select l2, country_code country, total_bt_merchants, total_bt_deals from grp_gdoop_bizops_db.nvp_okr_bt_merch order by l2 desc, country;


------------------------------------------------------------------------avg orders/bookable deals avg bookings/bookable deals
drop table grp_gdoop_bizops_db.nvp_okr_30days_deal;
create table grp_gdoop_bizops_db.nvp_okr_30days_deal stored as orc as
select 
   e.country_code, 
   b.l2,
   b.l1,
   c.merchant_uuid,
   a.deal_uuid
from
   (select deal_uuid
      from grp_gdoop_bizops_db.sh_bt_active_deals_log
      where 
        cast(load_date as date) = date_sub(CURRENT_DATE, 31)
        and partner_inactive_flag = 0
        and product_is_active_flag = 1
        group by deal_uuid) a
  join 
    (select deal_uuid
      from grp_gdoop_bizops_db.sh_bt_active_deals_log
      where 
        cast(load_date as date) = date_sub(CURRENT_DATE, 1)
        and partner_inactive_flag = 0
        and product_is_active_flag = 1
        group by deal_uuid) a1 on a.deal_uuid = a1.deal_uuid
  left join 
    (select 
         deal_id,
         grt_l2_cat_name l2, 
         grt_l1_cat_name l1
     from 
        user_edwprod.dim_gbl_deal_lob 
     ) b on a.deal_uuid = b.deal_id
  left join 
     (select 
         product_uuid product_uuid,
         max(merchant_uuid) merchant_uuid
      from user_edwprod.dim_offer_ext  
      where inv_product_uuid <> '-1'
      group by product_uuid) c on a.deal_uuid = c.product_uuid
  left join 
     (select 
          merchant_uuid, 
          feature_country 
       from user_edwprod.dim_merchant 
       group by merchant_uuid, feature_country
      ) d on c.merchant_uuid = d.merchant_uuid
  left join 
      (select 
          country_code, 
          country_id 
        from user_groupondw.dim_country
        group by country_code, country_id
      )  e on d.feature_country = e.country_id
     group by e.country_code, b.l2,b.l1, c.merchant_uuid, a.deal_uuid;


drop table grp_gdoop_bizops_db.nvp_okr_ords_units;
create table grp_gdoop_bizops_db.nvp_okr_orders stored as orc as 
select
    sum(units) units,
    count(distinct parent_order_uuid) orders,
    count(distinct b1.deal_uuid) total_bookable_deals,
    b1.l2, 
    b1.country_code
from grp_gdoop_bizops_db.rt_bt_txns b
right join grp_gdoop_bizops_db.nvp_okr_30days_deal b1 on b.deal_uuid = b1.deal_uuid
where
    b1.l1 = 'L1 - Local'
    and cast(order_date as date) >= date_sub(CURRENT_DATE, 30)
group by
    b1.l2, 
    b1.country_code;

create TEMPORARY table grp_gdoop_bizops_db.nvp_okr_bookings stored as orc as 
select
    sum(case when b.booked = 1 then units end) bookings_units,
    count(distinct b1.deal_uuid) total_bookable_deals,
    b1.l2, 
    b1.country_code
from grp_gdoop_bizops_db.rt_bt_txns b
right join grp_gdoop_bizops_db.nvp_okr_30days_deal b1 on b.deal_uuid = b1.deal_uuid
where
    b1.l1 = 'L1 - Local'
    and cast(b.book_date as date) >= date_sub(CURRENT_DATE, 30)
group by
    b1.l2, 
    b1.country_code;

   
select 
   a.l2, 
   a.country_code, 
   a.total_bookable_deals, 
   a.orders, 
   a.units, 
   b.bookings_units
from grp_gdoop_bizops_db.nvp_okr_orders a
left join grp_gdoop_bizops_db.nvp_okr_bookings b on a.l2 = b.l2 and a.country_code = b.country_code
order by a.l2, a.country_code;


---bookable units per orders

create table grp_gdoop_bizops_db.nvp_okr_units_per_order stored as orc as 
select
    sum(case when a.deal_uuid is not null then units end) bt_units,
    count(distinct case when a.deal_uuid is not null then parent_order_uuid end) total_bt_orders,
    sum(units) total_local_units, 
    count(distinct parent_order_uuid) total_local_orders,
    gdl.country_code country_code,
    gdl.grt_l2_cat_name l2
from grp_gdoop_bizops_db.rt_bt_txns b
left join (
    select
        load_date,
        deal_uuid
    from grp_gdoop_bizops_db.sh_bt_active_deals_log
    where
        partner_inactive_flag = 0
        and product_is_active_flag = 1
    group by load_date, deal_uuid
    ) a on a.deal_uuid = b.deal_uuid and a.load_date = b.order_date
left join user_edwprod.dim_gbl_deal_lob gdl on gdl.deal_id = b.deal_uuid
left join user_groupondw.gbl_dim_country c on gdl.country_code = c.country_iso_code_2
where
    grt_l1_cat_name = 'L1 - Local'
    and cast(order_date as date) >= date_sub(CURRENT_DATE, 30)
group by
    gdl.country_code, 
    gdl.grt_l2_cat_name
    ;

select l2, country_code, total_bt_orders, bt_units, total_local_orders, total_local_units from grp_gdoop_bizops_db.nvp_okr_units_per_order order by l2 desc, country_code;







------------------------------------------------------------------------Using active deals table. Based on Launch date
------------------------------------------------------------------------30 Day metro merchant count/Total New Merchants/ Total BT Eligible



------doesnt mean the merchants havent submitted a campaign before. 30 Day Metro Merchant Count/Total New Merchants/ Total BT Eligible

select 
s1.vertical, 
s1.feature_country, 
count(distinct s1.merchant_uuid) all_frstlaunch_merch, 
count(distinct case when s1.metro_launch = 1 then s1.merchant_uuid end) metro_launched_merch
from 
(select 
     x.merchant_uuid, 
     case when y.merchant_uuid is not null then 1 end metro_launch, 
     y.vertical, 
     y.feature_country
from grp_gdoop_bizops_db.nvp_okr_merchonboarding x 
    left join
      (select distinct 
         a.accountid, 
         a.vertical, 
         a.feature_country,
         a.a.dmapi_flag,
         b.merchant_uuid
       from grp_gdoop_sup_analytics_db.jc_merchant_mtd_attrib a
       join user_edwprod.dim_merchant b on a.accountid = b.salesforce_account_id
       where 
       cast(a.launch_date as date) >= date_sub(CURRENT_DATE, 30)
       and a.dmapi_flag = 1
       and a.launch_order = 1) y on x.merchant_uuid = y.merchant_uuid
where cast(x.mn_load_date as date) >= date_sub(CURRENT_DATE, 30)
) as s1 group by s1.vertical, s1.feature_country;



select 
x.*, 
y.merchant_uuid, 
y.mn_load_date,
case when cast(y.mn_load_date as date) >= date_sub(CURRENT_DATE, 30) then 1 end  allowed
from 
(select
         a.accountid, 
         a.vertical, 
         a.feature_country,
         a.dmapi_flag,
         b.merchant_uuid
       from grp_gdoop_sup_analytics_db.jc_merchant_mtd_attrib a
       join user_edwprod.dim_merchant b on a.accountid = b.salesforce_account_id
       where 
       cast(a.launch_date as date) >= date_sub(CURRENT_DATE, 30)
       and a.launch_order = 1) x 
left join grp_gdoop_bizops_db.nvp_okr_merchonboarding y on x.merchant_uuid = y.merchant_uuid;

join user_dw.v_dim_pds_grt_map grt
        on jc_merchant_mtd_attrib.pds = grt.pds_cat_name;
        
       
select * from grp_gdoop_sup_analytics_db.jc_merchant_mtd_attrib where dmapi_flag = 1;
select * from user_dw.v_dim_pds_grt_map;

----------