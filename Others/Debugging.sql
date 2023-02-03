insert overwrite table grp_gdoop_bizops_db.avb_bookable_deals
select
report_date,
deal_uuid,
'3pip mbo' as booking_type
from grp_gdoop_sup_analytics_db.hbw_deals_bookings_flags m
where
report_date >= '2020-01-01' and mbo_flag = 1 
and deal_uuid <> '88610046-a287-48e7-8fd3-e5092b5926f1' -- permalink booking-test-co-5'
UNION ALL
select
load_date,
deal_uuid,
'3pip' as booking_type
from grp_gdoop_bizops_db.bzops_booking_deals tp
join prod_groupondw.active_deals ad on tp.product_uuid = ad.deal_uuid
where inv_service_id = 'tpis' and merchant_name not in (
'Local Flavor',
'Groupon To Go & BeautyNow & CLO (testing account)',
'Groupon Select',
'Grubhub',
'giftango',
'Gratafy',
'Vagaro',
'Vacation Express USA Corp (Grandchild)',
'Epitourean',
'Buffalo Wild Wings',
'Uber',
'TTD OPS Test Account - TEST ACCOUNT')
and ad.load_date >= '2020-01-01' and sold_out = 'false' and available_qty > 0 and country_code in ('US','CA')
UNION ALL
select
load_date,
deal_uuid,
'booking tool' as booking_type
from grp_gdoop_bizops_db.sh_bt_active_deals_log_v3
where
load_date >= '2020-01-01' and is_bookable = 1 and partner_inactive_flag = 0 
and product_is_active_flag =1



select * from grp_gdoop_bizops_db.avb_bookable_deals where report_date = '2020-09-25' and booking_type <> '3pip' and booking_type <> '3pip mbo';



s

----
select 
booked, 
bookable_during_redemption,
country_code,
count(1)
from grp_gdoop_bizops_db.nvp_okr_vcagg 
where deal_id is not null and redeem_date > '2019-07-01'
group by 1,2,3
;


select * 
from grp_gdoop_bizops_db.nvp_kr_coupons where booked = 1;

select * from grp_gdoop_bizops_db.nvp_kr_vouchers where booked = 1;

select * from grp_gdoop_bizops_db.nvp_okr_vcagg;


select 
case when country_code = 'US' then 'NAM' when country_code is not null then 'INTL' end region,
l2, 
count(distinct case when booked = 1 then concat(security_code, voucher_code) end) booked_units,
count(distinct case when bt_eligible = 1 then concat(security_code, voucher_code) end) bt_eligible_units
from grp_gdoop_bizops_db.nvp_okr_vcagg 
where cast(redeem_date as date) >= '2019-07-01'
and deal_id is not null
;


select 
case when cast(redeem_date as date) between cast('2019-07-01' as date) and cast('2019-09-30' as date) then 'Q32019'
     when cast(redeem_date as date) between cast('2019-10-01' as date) and cast('2019-12-31' as date) then 'Q42019'
     when cast(redeem_date as date) >= cast('2020-07-01' as date) then 'Q32020' end quarter,
case when country_code = 'US' then 'NAM' when country_code is not null then 'INTL' end region,
l2,
count(distinct case when booked = 1 then concat(security_code, voucher_code) end) booked_units,
count(distinct case when bt_eligible = 1 then concat(security_code, voucher_code) end) bt_eligible_units
from grp_gdoop_bizops_db.nvp_okr_vcagg 
where redeem_date is not null
group by 
   case when cast(redeem_date as date) between cast('2019-07-01' as date) and cast('2019-09-30' as date) then 'Q32019'
     when cast(redeem_date as date) between cast('2019-10-01' as date) and cast('2019-12-31' as date) then 'Q42019'
     when cast(redeem_date as date) >= cast('2020-07-01' as date) then 'Q32020' end,
     l2, 
     case when country_code = 'US' then 'NAM' when country_code is not null then 'INTL' end
;


select 
year(cast(redeem_date as date)) year_of_redemp,
month(cast(redeem_date as date)) month_of_redemp,
case when country_code = 'US' then 'NAM' when country_code is not null then 'INTL' end region,
l2,
count(distinct case when booked = 1 then concat(security_code, voucher_code) end) booked_units,
count(distinct case when bt_eligible = 1 then concat(security_code, voucher_code) end) bt_eligible_units
from grp_gdoop_bizops_db.nvp_okr_vcagg 
where redeem_date is not null
group by 
   year(cast(redeem_date as date)),
   month(cast(redeem_date as date)),
   l2, 
   case when country_code = 'US' then 'NAM' when country_code is not null then 'INTL' end


select * from grp_gdoop_bizops_db.nvp_kr_vouchers2;