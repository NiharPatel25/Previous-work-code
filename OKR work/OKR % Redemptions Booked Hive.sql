select * from grp_gdoop_bizops_db.rt_bt_inventory limit 5;


drop table grp_gdoop_bizops_db.nvp_bt_orders;
create table grp_gdoop_bizops_db.nvp_bt_orders stored as orc as
select * from(
select 
     x.parent_order_uuid parent_order_uuid, 
     x.order_uuid order_uuid,
     x.order_id order_id, 
     x.order_date order_date, 
     x.unified_deal_option_id deal_id,
     x.deal_uuid deal_uuid, 
     x.user_uuid fgt_user_uuid, 
     x.country_id country_id,
     y.country_code,
     case when z.deal_uuid is not null then 1 else 0 end bookable_at_purchase
     from 
        (select * from user_edwprod.fact_gbl_transactions where action = 'authorize' and cast(order_date as date) >= cast('2020-01-01' as date) ) x
     left join (select distinct deal_id, country_id, country_code, grt_l2_cat_name from user_edwprod.dim_gbl_deal_lob) y on x.deal_uuid = y.deal_id and x.country_id = y.country_id
     left join 
         (select 
                load_date,
                deal_uuid
              from grp_gdoop_bizops_db.sh_bt_active_deals_log
          where partner_inactive_flag = 0 and product_is_active_flag = 1 and is_bookable = 1
          group by load_date, deal_uuid
         ) z on x.deal_uuid = z.deal_uuid and cast(x.order_date as date) = cast(z.load_date as date)
) as uni;



---------BT VOUCHERS AND REDEMPTIONS

drop table grp_gdoop_bizops_db.nvp_kr_vouchers;
create table grp_gdoop_bizops_db.nvp_kr_vouchers stored as orc as
select
    v.billing_id parent_order_uuid,
    v.security_code,
    v.voucher_code, 
    v.usage_date,
    v.redeem_date,
    x.deal_uuid,
    y.country_code,
    case when coalesce(b.voucher_code,b.security_code) is not null then 1 else 0 end booked, 
    x.order_date, 
    case when z.deal_uuid is not null then 1 else 0 end bt_eligible
from 
    (select billing_id, 
            security_code,
            voucher_code, 
            usage_date,
            case when usage_state_id = 2 then substr(last_modified,1,10) end redeem_date
            from dwh_base.vouchers
            where
            cast(substr(last_modified,1,10) as date) >= cast('2020-01-01' as date) ) v
left join 
    (select 
          voucher_code, 
          security_code
        from 
           grp_gdoop_bizops_db.sh_bt_bookings_rebuild
         where
           is_a_groupon_booking = 1 and lower(booked_by) in ('customer', 'api') or booked_by is null
         group by voucher_code, security_code
        ) b on b.voucher_code = v.voucher_code and b.security_code = v.security_code
left join 
     (select
               parent_order_uuid,
               order_date,
               max(deal_uuid) as deal_uuid
        from user_edwprod.fact_gbl_transactions 
        where action = 'authorize' and cast(order_date as date) >= cast('2019-01-01' as date)
        group by parent_order_uuid, order_date
      ) x on v.billing_id = x.parent_order_uuid
left join 
      (select 
          distinct 
             deal_id, 
             country_code 
         from user_edwprod.dim_gbl_deal_lob
       ) y on x.deal_uuid = y.deal_id
left join 
      (select 
             load_date,
             deal_uuid
        from grp_gdoop_bizops_db.sh_bt_active_deals_log
        where partner_inactive_flag = 0 and product_is_active_flag = 1 and is_bookable = 1
        and cast(load_date as date) >= cast('2019-01-01' as date)
        group by load_date, deal_uuid
      ) z on x.deal_uuid = z.deal_uuid and cast(x.order_date as date) = cast(z.load_date as date);

-----error correcting. ... 
create table grp_gdoop_bizops_db.nvp_kr_vouchers2 stored as orc as
select 
    v.parent_order_uuid,
    v.security_code,
    v.voucher_code, 
    v.usage_date,
    v.redeem_date,
    v.booked,
    x.deal_uuid,
    y.country_code,
    x.order_date, 
    case when z.deal_uuid is not null then 1 else 0 end bt_eligible
from grp_gdoop_bizops_db.nvp_kr_vouchers v
left join 
     (select
               parent_order_uuid,
               order_date,
               max(deal_uuid) as deal_uuid
        from user_edwprod.fact_gbl_transactions 
        where action = 'authorize' and cast(order_date as date) >= cast('2019-01-01' as date)
        group by parent_order_uuid, order_date) x on v.parent_order_uuid = x.parent_order_uuid
left join 
      (select 
          distinct 
             deal_id, 
             country_code 
         from user_edwprod.dim_gbl_deal_lob) y on x.deal_uuid = y.deal_id
left join 
      (select 
             load_date,
             deal_uuid
        from grp_gdoop_bizops_db.sh_bt_active_deals_log
        where partner_inactive_flag = 0 and product_is_active_flag = 1 and is_bookable = 1
        and cast(load_date as date) >= cast('2019-01-01' as date)
        group by load_date, deal_uuid
      ) z on x.deal_uuid = z.deal_uuid and cast(x.order_date as date) = cast(z.load_date as date)
;
     
----------------------------------------------------------------------------------------------------------------Coupons - NA

drop table grp_gdoop_bizops_db.nvp_kr_coupons;
create table grp_gdoop_bizops_db.nvp_kr_coupons stored as orc as
select 
        t.parent_order_uuid,
        v.security_code,
        v.voucher_code,
        v.usage_date,
        v.redeem_date,
        t.deal_uuid deal_id, 
        t.country_code,
        case when coalesce(b.voucher_code,b.security_code) is not null then 1 else 0 end booked,
        t.order_date,
        t.bt_eligible
    from 
       (select 
            code voucher_code,
            cast(merchant_redemption_code as varchar(64)) security_code,
            coalesce(customer_redeemed_at,merchant_redeemed_at) usage_date,
            case when customer_redeemed = 1 then substr(updated_at,1,10) end redeem_date,
            purchaser_consumer_id,
            order_id
            from user_gp.camp_membership_coupons 
            where
            cast(substr(updated_at,1,10) as date) >= cast('2019-07-01' as date)
         ) v
    left join 
        (select 
          voucher_code, 
          security_code 
        from 
           grp_gdoop_bizops_db.sh_bt_bookings_rebuild
         where
           is_a_groupon_booking = 1 and lower(booked_by) in ('customer', 'api') or booked_by is null
         ) b on b.voucher_code = v.voucher_code and  v.security_code = b.security_code
    left join 
         (select 
            distinct 
               x.parent_order_uuid, 
               x.user_uuid, 
               x.order_id, 
               x.deal_uuid, 
               x.order_date,
               y.country_code, 
               case when z.deal_uuid is not null then 1 else 0 end bt_eligible
        from user_edwprod.fact_gbl_transactions x
        left join (select distinct deal_id, country_code from user_edwprod.dim_gbl_deal_lob) y on x.deal_uuid = y.deal_id
        left join 
           (select 
             load_date,
             deal_uuid
            from grp_gdoop_bizops_db.sh_bt_active_deals_log
            where partner_inactive_flag = 0 and product_is_active_flag = 1 and is_bookable = 1
            and cast(load_date as date) >= cast('2019-01-01' as date)
            group by load_date, deal_uuid
            ) z on x.deal_uuid = z.deal_uuid and cast(x.order_date as date) = cast(z.load_date as date)
        where x.action = 'authorize' and cast(order_date as date) >= cast('2019-01-01' as date)) t 
       on t.user_uuid = v.purchaser_consumer_id  and t.order_id = v.order_id;


    
drop table grp_gdoop_bizops_db.nvp_okr_vcagg;
create table grp_gdoop_bizops_db.nvp_okr_vcagg stored as orc as
select 
    x.parent_order_uuid,
    x.security_code,
    x.voucher_code,
    x.usage_date,
    x.redeem_date,
    x.deal_id, 
    x.country_code,
    x.booked,
    x.order_date, 
    x.bt_eligible,
    y.l2
from grp_gdoop_bizops_db.nvp_kr_coupons x
left join (select distinct deal_id,grt_l2_cat_name l2 from user_edwprod.dim_gbl_deal_lob) y on x.deal_id = y.deal_id
where x.parent_order_uuid is not null and x.redeem_date is not null
union
select 
    x.parent_order_uuid,
    x.security_code,
    x.voucher_code, 
    x.usage_date,
    x.redeem_date,
    x.deal_uuid,
    x.country_code,
    x.booked, 
    x.order_date, 
    x.bt_eligible,
    y.l2
from 
grp_gdoop_bizops_db.nvp_kr_vouchers2 x
left join (select distinct deal_id, grt_l2_cat_name l2 from user_edwprod.dim_gbl_deal_lob) y on x.deal_uuid = y.deal_id
where x.parent_order_uuid is not null and x.redeem_date is not null;



/*
 * 
 * 
 *     (select 
	        parent_order_uuid, 
			order_uuid,
			order_date, 
			deal_id,
			deal_uuid,
			fgt_user_uuid user_uuid, 
			country_id, 
			country_code,
			bookable, 
			is_bookable, 
			partner_inactive_flag, 
			product_is_active_flag
     from grp_gdoop_bizops_db.np_bt_orders where country_id<>235 and country_id <> 40
			) as fgt
	left join 
	(select 
		voucher_code, 
		security_code,
		billing_id parent_order_uuid,
		deal_id deal_id, 
		country_id
		from dwh_base.vouchers where date(created_at)>='2020-01-01' and country_id <> 235 and country_id <> 40
	) as v on fgt.parent_order_uuid = v.parent_order_uuid and fgt.deal_id = v.deal_id and fgt.country_id = v.country_id
 */
---------


use grp_gdoop_bizops_db;
drop table rt_vouchers;
 create table rt_vouchers stored as orc as
 select
 count(distinct concat(voucher_code, user_uuid)) vouchers,
 order_date,
 redeem_date,
 is_redeemed,
 is_expired,
 is_POR,
 gdl.grt_l2_cat_name,
 e.economic_area,
 gdl.country_code
 from (
select voucher_code, user_uuid, deal_uuid, order_date, is_redeemed, redeem_date, is_expired
from (
  select v.voucher_code, v.user_id user_uuid, t.deal_uuid deal_uuid,t.order_date,case when usage_state_id = 2 then 1 else 0 end is_redeemed, min(case when usage_state_id = 2 then substr(last_modified,1,10) end) redeem_date, case when current_date >= valid_before then 1 else 0 end is_expired
from dwh_base.vouchers v
join user_edwprod.fact_gbl_transactions t on t.parent_order_uuid = v.billing_id
 group by v.voucher_code,v.user_id,t.deal_uuid,t.order_date,case when usage_state_id = 2 then 1 else 0 end,case when current_date >= valid_before then 1 else 0 end
union
select v.code voucher_code, v.purchaser_consumer_id user_uuid, t.deal_uuid deal_uuid,t.order_date order_date, customer_redeemed is_redeemed,min(case when customer_redeemed = 1 then substr(v.updated_at,1,10) end) redeem_date,case when current_date > v.expires_at then 1 else 0 end is_expired
from user_gp.camp_membership_coupons v
join user_edwprod.fact_gbl_transactions t on t.user_uuid = v.purchaser_consumer_id  and t.order_id = v.order_id
join user_groupondw.acctg_red_voucher_base vb on v.code = vb.code and v.purchaser_consumer_id = vb.user_uuid
group by v.code,v.purchaser_consumer_id,t.deal_uuid,t.order_date,customer_redeemed,case when current_date > v.expires_at then 1 else 0 end
) a
group by
voucher_code,
user_uuid,
deal_uuid,
order_date,
is_redeemed,
redeem_date,
is_expired
 ) a
left join (
   select deal_uuid, case when lower(payment_terms) in ('redemption system','payment on redemption (0%)','on redemption')  then 1 else 0 end is_PoR
   from edwprod.sf_opportunity_2 op2
   left join edwprod.sf_opportunity_1 op on op.opportunity_id = op2.opportunity_id
   group by deal_uuid, case when lower(payment_terms) in ('redemption system','payment on redemption (0%)','on redemption')  then 1 else 0 end
 ) POR on POR.deal_uuid = a.deal_uuid
 left join user_edwprod.dim_gbl_deal_lob gdl on gdl.deal_id = a.deal_uuid
 join user_groupondw.gbl_dim_country c on gdl.country_code = c.country_iso_code_2
 join user_groupondw.gbl_dim_economic_area e on c.economic_area_id = e.economic_area_id
 where grt_l1_cat_name = 'L1 - Local'
 and order_date >= '2020-01-01'
 group by
 order_date,
redeem_date,
 is_redeemed,
 is_expired,
 is_POR,
 gdl.grt_l2_cat_name,
 e.economic_area,
 gdl.country_code
 ;
 

select 
   COALESCE(bookings_redeemed_units, 0) bookings_redeemed_units, 
   COALESCE(bt_eligible_txns_redeemed_units, 0) bt_eligible_txns_redeemed_units, 
   economic_area, 
   country_code,
   l2, 
   cast(redeem_date as date) redeemed_date, 
   year(cast(redeem_date as date)) year_red_date, 
   month(cast(redeem_date as date)) month_red_date
from grp_gdoop_bizops_db.rt_bt_reds_booked
where country_code = 'US';


