select * from user_edwprod.fact_gbl_transactions;

----Rachels rt_bt_txns

use grp_gdoop_bizops_db;
drop table rt_bt_txns;
create table rt_bt_txns stored as orc as
select
  t.deal_uuid deal_uuid,
  t.parent_order_uuid parent_order_uuid,
  t.user_uuid user_uuid,
  max(c.country_iso_code_2) country_code,
  max(case when lower(platform) in ('iphone','ipad') then 'iOS'
                  when lower(platform) in ('android','touch') then platform
                   when lower(platform) in ('web','desktop') then 'desktop'
                   else 'other' end) platform,
  max(a.traffic_source) traffic_source,
  max(case when b.parent_order_uuid is not null then 1 else 0 end) booked,
  max(case when r.usage_state_id = 2 then 1 else 0 end) redeemed,
  max(case when (refund_amount_loc * coalesce(approved_avg_exchange_rate,1) <> 0) then 1 else 0 end) is_refunded,
  max(is_expired) is_expired,
  max(is_PoR) is_PoR,
  min(transaction_date) order_date,
  min(case when length(min_booked_at) > 10 then substr(min_booked_at,1,10) end) book_date,
  min(case when length(usage_date) > 10 then substr(usage_date,1,10) end) usage_date,
  min(case when usage_state_id = 2 then substr(redeem_date,1,10) end) redeem_date,
  sum(case when action = 'authorize' then transaction_qty end) units,
  sum(capture_nob_loc * coalesce(approved_avg_exchange_rate,1)) nob,
  sum(capture_nor_loc * coalesce(approved_avg_exchange_rate,1)) nor
from user_edwprod.fact_gbl_transactions t
      left join rt_bt_bookings b on t.parent_order_uuid = b.parent_order_uuid
      left join rt_bt_reds r on t.parent_order_uuid = r.parent_order_uuid
      left join (
        select billing_id, is_expired
        from (
        select billing_id, case when current_date >= date(valid_before) then 1 else 0 end as is_expired
        from (
        select billing_id, case when length(valid_before) > 10 then substr(valid_before,1,10) end valid_before
        from dwh_base.vouchers
        group by billing_id, case when length(valid_before) > 10 then substr(valid_before,1,10) end
        ) a
        group by billing_id, case when current_date >= date(valid_before) then 1 else 0 end
        union
          select parent_order_uuid billing_id, expired_yn is_expired
          from (	
          select  t.parent_order_uuid, expired_yn
          from user_groupondw.acctg_red_voucher_base vb
          join user_gp.camp_membership_coupons cp on cp.code = vb.code and cp.purchaser_consumer_id = vb.user_uuid
          join user_edwprod.fact_gbl_transactions t on t.user_uuid = cp.purchaser_consumer_id  and t.order_id = cp.order_id
          group by t.parent_order_uuid, expired_yn
        ) v group by  parent_order_uuid, expired_yn ) a
 ) ex on ex.billing_id = t.parent_order_uuid
      left join (
        select deal_uuid, event_date, traffic_source, platform, cookie_b unique_deal_views
        from (
          select
          deal_uuid,
          event_date,
          cookie_b,
          max(cookie_first_traf_source) traffic_source,
          max(cookie_first_sub_platform) platform
          from user_groupondw.gbl_traffic_superfunnel_deal
          where event_date >= '2018-01-01'
          group by deal_uuid, event_date, cookie_b
        ) t
        group by deal_uuid, event_date, traffic_source, platform, cookie_b
      ) a on a.event_date = t.order_date and a.deal_uuid = t.deal_uuid and a.unique_deal_views = t.bcookie
      join user_groupondw.dim_day dd on dd.day_rw = t.order_date
      join (
              select
                  currency_from,
                  currency_to,
                  fx_neutral_exchange_rate,
                  approved_avg_exchange_rate,
                  period_key
                  from user_groupondw.gbl_fact_exchange_rate
                  where currency_to = 'USD'
                  group by
                  currency_from,
                  currency_to,
                  fx_neutral_exchange_rate,
                  approved_avg_exchange_rate,
                  period_key
                  ) er on t.currency_code = er.currency_from and dd.month_key  = er.period_key
join user_groupondw.gbl_dim_country c on t.country_id = c.country_key
left join (
  select deal_uuid, case when lower(payment_terms) in ('redemption system','payment on redemption (0%)','on redemption')  then 1 else 0 end is_PoR
  from edwprod.sf_opportunity_2 op2
  left join edwprod.sf_opportunity_1 op on op.opportunity_id = op2.opportunity_id
  group by deal_uuid, case when lower(payment_terms) in ('redemption system','payment on redemption (0%)','on redemption')  then 1 else 0 end
) POR on POR.deal_uuid = t.deal_uuid
where t.order_date >= '2018-01-01' and is_zero_amount = 0
group by
t.deal_uuid,
t.parent_order_uuid,
t.user_uuid;



