------import initial tables from Tiered NAM Comparison
drop table grp_gdoop_bizops_db.nvp_bt_user_star;
create table grp_gdoop_bizops_db.nvp_bt_user_star stored as orc as
select 
   b.user_uuid, 
   b.order_date,
  case when st.recency_segment in ( 'acquisition' ,'sub_activation') then 'activation'
      when st.recency_segment in ('reactivation') then 'reactivation'
      when st.recency_segment in ('current_purchasers','Recent First Purchasers') and st.frequency_segment in ('order_cnt_1') then 'LH'
      when st.recency_segment in ('current_purchasers') and st.frequency_segment in ('order_cnt_2-4') then 'MH'
      when st.recency_segment in ('current_purchasers') and st.frequency_segment in ('order_cnt_5-10','order_cnt_11-20','order_cnt_20+') then 'HH'
      when st.recency_segment in ('lapsed_first_purchasers','lapsed_current_purchasers') and  st.frequency_segment in ('order_cnt_1') then 'LM'
      when st.recency_segment in ('lapsed_current_purchasers') and  st.frequency_segment in ('order_cnt_2-4') then 'MM'
      when st.recency_segment in ('lapsed_current_purchasers') and  st.frequency_segment in ('order_cnt_5-10','order_cnt_11-20','order_cnt_20+') then 'HM'
      when st.recency_segment in ('pre-attrition','retention') and st.frequency_segment in ('order_cnt_1') then 'LL'
      when st.recency_segment in ('pre-attrition','retention') and st.frequency_segment  in ('order_cnt_2-4') then 'ML'
      when st.recency_segment in ('pre-attrition','retention') and  st.frequency_segment in ('order_cnt_5-10','order_cnt_11-20','order_cnt_20+') then 'HL'
      else 'WTF' end as user_rating
from grp_gdoop_bizops_db.rt_bt_txns b
left join 
   (select 
      * 
    from user_groupondw.agg_user_ord_seg_day_na
    union
    select 
      * 
    from user_groupondw.agg_user_ord_seg_day_intl) as st on b.user_uuid = st.user_uuid
where 
    cast(b.order_date as date) >= cast(st.valid_date_start as date)
    and cast(b.order_date as date) <= cast(st.valid_date_end as date)
group by
      b.user_uuid, 
      b.order_date,
      case when st.recency_segment in ( 'acquisition' ,'sub_activation') then 'activation'
      when st.recency_segment in ('reactivation') then 'reactivation'
      when st.recency_segment in ('current_purchasers','Recent First Purchasers') and st.frequency_segment in ('order_cnt_1') then 'LH'
      when st.recency_segment in ('current_purchasers') and st.frequency_segment in ('order_cnt_2-4') then 'MH'
      when st.recency_segment in ('current_purchasers') and st.frequency_segment in ('order_cnt_5-10','order_cnt_11-20','order_cnt_20+') then 'HH'
      when st.recency_segment in ('lapsed_first_purchasers','lapsed_current_purchasers') and  st.frequency_segment in ('order_cnt_1') then 'LM'
      when st.recency_segment in ('lapsed_current_purchasers') and  st.frequency_segment in ('order_cnt_2-4') then 'MM'
      when st.recency_segment in ('lapsed_current_purchasers') and  st.frequency_segment in ('order_cnt_5-10','order_cnt_11-20','order_cnt_20+') then 'HM'
      when st.recency_segment in ('pre-attrition','retention') and st.frequency_segment in ('order_cnt_1') then 'LL'
      when st.recency_segment in ('pre-attrition','retention') and st.frequency_segment  in ('order_cnt_2-4') then 'ML'
      when st.recency_segment in ('pre-attrition','retention') and  st.frequency_segment in ('order_cnt_5-10','order_cnt_11-20','order_cnt_20+') then 'HL'
      else 'WTF' end
;

select * from grp_gdoop_bizops_db.nvp_bt_tiered_star;

create table grp_gdoop_bizops_db.nvp_bt_tiered_star stored as orc as
select
  sum(units) units,
  booked,
  redeemed,
  case when a.deal_uuid is not null then 1 else 0 end bt_eligible,
  case when cast(b.order_date as date) > cast(bt_f.min_redeem_date as date) and a.deal_uuid is not null then 1 else 0 end bt_prev_user,
  gdl.country_code country_code,
  e.economic_area,
  gdl.grt_l2_cat_name l2,
  gdl.grt_l3_cat_name l3,
  b.order_date,
  book_date,
  redeem_date,
  case when tie.deal_uuid is not null then 1 else 0 end tiered_market,
  case when st.user_rating in ('HH','HM') then 'three_star' 
       when st.user_rating in ('MH','MM','HL') then 'two_star'
       when st.user_rating in ('LH','LM','LL','ML') then 'one_star' end customer_rating
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
left join user_groupondw.gbl_dim_economic_area e on c.economic_area_id = e.economic_area_id
left join grp_gdoop_bizops_db.nvp_deals_tiered_market tie on b.deal_uuid = tie.deal_uuid
left join grp_gdoop_bizops_db.nvp_bt_first_txns bt_f on b.user_uuid = bt_f.user_uuid
left join grp_gdoop_bizops_db.nvp_bt_user_star st on b.order_date = st.order_date and st.user_uuid = b.user_uuid
where
  grt_l1_cat_name = 'L1 - Local'
group by
  booked,
  redeemed,
  case when a.deal_uuid is not null then 1 else 0 end,
  gdl.country_code,
  e.economic_area,
  gdl.grt_l2_cat_name,
  gdl.grt_l3_cat_name,
  b.order_date,
  book_date,
  redeem_date,
  case when tie.deal_uuid is not null then 1 else 0 end,
  case when cast(b.order_date as date) > cast(bt_f.min_redeem_date as date) and a.deal_uuid is not null then 1 else 0 end,
    case when st.user_rating in ('HH','HM') then 'three_star' 
       when st.user_rating in ('MH','MM','HL') then 'two_star'
       when st.user_rating in ('LH','LM','LL','ML') then 'one_star' end
;


drop table grp_gdoop_bizops_db.nvp_bt_metrics_star;
create table grp_gdoop_bizops_db.nvp_bt_metrics_star stored as orc as
select
  sum(a.units) units,
  sum(a.bt_eligible_units)bt_eligible_units,
  sum(b.bookings_units) bookings,
  sum(c.bookings_redeemed_units) bookings_redeemed,
  sum(c.bt_eligible_txns_redeemed_units) bt_eligible_txns_redeemed,
  a.country_code,
  a.l2,
  a.l3,
  a.tiered_market,
  a.bt_prev_user, 
  a.order_date, 
  a.customer_rating
from (
  select
    sum(units) units,
    sum(case when bt_eligible = 1 then units end) bt_eligible_units,
    country_code,
    l2,
    l3,
    order_date,
    a.tiered_market,
    bt_prev_user,
    case when customer_rating is not null then customer_rating else 'null' end customer_rating
  from grp_gdoop_bizops_db.nvp_bt_tiered_star a
  group by country_code, l2, l3, order_date,a.tiered_market,a.bt_prev_user,
  case when customer_rating is not null then customer_rating else 'null' end) a
left join (
  select
    sum(case when booked = 1 then units end) bookings_units,
    country_code,
    l2,
    l3,
    book_date,
    a.tiered_market,
    bt_prev_user,
    case when customer_rating is not null then customer_rating else 'null' end customer_rating
  from grp_gdoop_bizops_db.nvp_bt_tiered_star a
  where book_date is not null
  group by country_code,l2, l3, book_date, a.tiered_market, a.bt_prev_user, 
  case when customer_rating is not null then customer_rating else 'null' end) b
  on a.l2 = b.l2 and a.l3 = b.l3 and a.country_code = b.country_code 
  and a.tiered_market = b.tiered_market and a.order_date = b.book_date and a.bt_prev_user = b.bt_prev_user 
  and a.customer_rating = b.customer_rating
left join (
  select
    sum(case when bt_eligible = 1 and booked = 1 then units end) bookings_redeemed_units,
    sum(case when bt_eligible = 1 then units end) bt_eligible_txns_redeemed_units,
    country_code,
    l2,
    l3,
    redeem_date,
    a.tiered_market, 
    bt_prev_user,
    case when customer_rating is not null then customer_rating else 'null' end customer_rating
  from grp_gdoop_bizops_db.nvp_bt_tiered_star a
  where redeem_date is not null
  group by country_code,l2, l3, redeem_date, tiered_market, bt_prev_user, 
  case when customer_rating is not null then customer_rating else 'null' end) c
  on a.l2 = c.l2 
     and a.l3 = c.l3 
     and a.country_code = c.country_code 
     and a.tiered_market = c.tiered_market 
     and a.order_date = c.redeem_date 
     and a.bt_prev_user = c.bt_prev_user
     and a.customer_rating = c.customer_rating
group by a.country_code, a.tiered_market, a.l2, a.l3, a.bt_prev_user, a.order_date, a.customer_rating;