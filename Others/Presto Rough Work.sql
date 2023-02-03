select * from grp_gdoop_bizops_db.nvp_jouno_all_bt_logs order by dash_category;
select * from grp_gdoop_bizops_db.nvp_bss_funnel;
select * from grp_gdoop_bizops_db.nvp_jouno_all_bt_logs;

select * from grp_gdoop_bizops_db.nvp_jouno_events_temp_exp;
select devicetype, widgetname,  sum(distinct_logins) dis_logs 
from grp_gdoop_bizops_db.nvp_jouno_events_temp_exp 
group by devicetype, widgetname
order by devicetype, dis_logs desc ;

select * from grp_gdoop_bizops_db.nvp_jouno_events_temp_exp;

select * from ad_reporting_na_prod.CITRUS_MASTER_REPORT_CITRUS_51_v1;


{"questionnaire":


[{"stepId":"businessHours","isPassed":true,"isCurrent":false,"isChanged":false,"questions":
[{"locationsHours":[{"id":"80d737f9-6050-b4f3-b550-01776d898a25","name":"Trusted Hearing Center","address":"4859 Williams Drive Ste 109, Georgetown, US 78633","location":
{"street_address":"4859 Williams Drive Ste 109","locality":"Georgetown","region":"TX","postcode":"78633","country":"US","lon_lat":
{"lon":-97.73115949999999,"lat":30.694566},"time_zone":"America/Chicago","neighborhood":"Olde Oaks"},"weekDays":
[{"dayOfWeek":7,"isClosed":true,"hours":[{}]},{"dayOfWeek":1,"isClosed":false,"hours":[{"from":"09:00","until":"17:00"}]},{"dayOfWeek":2,"isClosed":false,"hours":[
{"from":"09:00","until":"17:00"}]},{"dayOfWeek":3,"isClosed":false,"hours":[{"from":"09:00","until":"17:00"}]},{"dayOfWeek":4,"isClosed":false,"hours":
[{"from":"09:00","until":"17:00"}]},{"dayOfWeek":5,"isClosed":false,"hours":[{"from":"08:00","until":"12:00"}]},{"dayOfWeek":6,"isClosed":false,"hours":
[{"from":"09:00","until":"12:00"}]}]}]}]},{"stepId":"staff","isPassed":true,"isCurrent":false,"isChanged":false,"questions":
[{"staffValues":[{"staffLocation":"Trusted Hearing Center, 4859 Williams Drive Ste 109, Georgetown, US 78633","id":"80d737f9-6050-b4f3-b550-01776d898a25","staffAmount":1}]}]},
{"stepId":"bookingCampaignsSelection","isPassed":true,"isCurrent":false,"isChanged":false,"questions":[{"deals":[]}]},
{"stepId":"bookingsCampaignsContextPage","isPassed":true,"isCurrent":false,"isChanged":false,"questions":[]},
{"stepId":"ac99e497-a171-4183-8e0b-0acba53e5e1e_6e76c88a-4645-417d-83f4-58a2261ddbd2","isPassed":true,"isCurrent":false,"isChanged":false,"campaignId":"ac99e497-a171-4183-8e0b-0acba53e5e1e","dealProductId":"6e76c88a-4645-417d-83f4-58a2261ddbd2","dealName":"Hearing aid cleaning & Repair","questions":
[{"dealDuration":{"hours":"0","minutes":"45"}},{"dealBlockedTime":{"isChanged":false,"hours":"0","minutes":"15","isChecked":true}},{"dealCustomersAmount":1},
{"dealVisits":{"id":"singleVisit","isChanged":false,"amount":1}},{"dealLocations":
[{"title":"Trusted Hearing Center, 4859 Williams Drive Ste 109, Georgetown, US 78633","id":"80d737f9-6050-b4f3-b550-01776d898a25"}]},
{"dealOpeningHours":{"optionId":"anyTime","openingHours":[]}}]},{"stepId":"ac99e497-a171-4183-8e0b-0acba53e5e1e_1e647388-b85f-4a91-b540-a1ee98140dcc",
"isPassed":true,"isCurrent":false,"isChanged":false,"campaignId":"ac99e497-a171-4183-8e0b-0acba53e5e1e","dealProductId":"1e647388-b85f-4a91-b540-a1ee98140dcc",
"dealName":"Hearing Aid Reprogramming","questions":[{"dealDuration":{"hours":"0","minutes":"45"}},{"dealBlockedTime":{"isChanged":
false,"hours":"0","minutes":"15","isChecked":true}},{"dealCustomersAmount":1},{"dealVisits":{"id":"singleVisit","isChanged":false,"amount":1}},
{"dealLocations":[{"title":"Trusted Hearing Center, 4859 Williams Drive Ste 109, Georgetown, US 78633","id":"80d737f9-6050-b4f3-b550-01776d898a25"}]},
{"dealOpeningHours":{"optionId":"anyTime","openingHours":[]}}]},
{"stepId":"bookingCampaignsDealSetupReview","isPassed":true,"isCurrent":false,"isChanged":false,"questions":[{"completedCampaign":{}}]},
{"stepId":"grouponAvailability","isPassed":true,"isCurrent":false,"isChanged":false,"questions":[{"grouponAvailability":
[{"id":"80d737f9-6050-b4f3-b550-01776d898a25","isAcceptedUntilFullyBooked":true,"isAdjustedForEachWeekDay":false}]}]},
{"stepId":"bookingPolicies","isPassed":true,"isCurrent":false,"isChanged":false,"questions":[{"arrivalFrequency":"according_to_service_duration"},
{"bookingPolicy":1},{"cancellationPolicy":4}]},{"stepId":"bookingNotifications","isPassed":true,"isCurrent":false,"isChanged":false,"questions":
[{"emails":["phillip@trustedhearingcenter.org"]},{"notificationFrequency":"instant"}]},{"stepId":"review","isPassed":false,"isCurrent":true,"isChanged":false,"questions":[]}]}

select * from grp_gdoop_bizops_db.jk_bt_attrition_agg;
drop table grp_gdoop_bizops_db.jk_bt_attrition_agg;

select deal_uuid, max(has_gcal) has_gcal_deal from grp_gdoop_bizops_db.sh_bt_active_deals_log_v4 where country <> = 'US' group by deal_uuid;


select 
  deal_uuid,
  reference_date, 
  gss_total_minutes, 
  report_date, 
  a.*
from 
grp_gdoop_bizops_db.jk_bt_availability_gbl_2 a
where deal_uuid = 'feb48d63-3f9f-4ec3-8fe4-6d5e9caable  t0dbea'
and days_delta <= 10
order by report_date desc,reference_date desc limit 10;

select * from grp_gdoop_bizops_db.np_booking_scope_deals where deal_uuid = 'c9f50afa-effc-4ad6-b0fe-809c76e3bdb5' order by week_end_date;
select * from grp_gdoop_bizops_db.sh_bt_active_deals_log_v4 where deal_uuid = 'c9f50afa-effc-4ad6-b0fe-809c76e3bdb5' order by load_date desc;


select * from grp_gdoop_bizops_db.np_lost_deals where region = 'NAM' and cast(week_end_date as date) = cast('2021-04-18' as date);

select 
   distinct 
   deals_in, 
   deals_both, 
   deals_days_delta
   from
(select 
   a.deal_uuid, 
   case when b.deal_uuid is not null then 1 else 0 end deals_in, 
   case when b.deals_both is not null then 1 else 0 end deals_both,
   case when b.deals_days_delta is not null then 1 else 0 end deals_days_delta
   from 
(select deal_uuid from grp_gdoop_bizops_db.sh_bt_active_deals_log_v4 where cast(load_date as date) >= cast('2021-04-01' as date) and country = 'US' group by 1) as a 
left join 
(select deal_uuid, max(deals_both) deals_both, max(deals_days_delta) deals_days_delta from grp_gdoop_bizops_db.nvp_availability_deals where cast(reference_date as date) >= cast('2021-04-01' as date) group by 1)as b on a.deal_uuid = b.deal_uuid
) as fin;



select * from grp_gdoop_bizops_db.np_booking_scope_deals where cast(week_end_date as date) >= cast('2021-03-28' as date);



select count(1) from grp_gdoop_bizops_db.jw_asterix;



select 
    week_end_date, 
    sum(live_deals) live_deals,
    sum(added_deals) added_deals, 
    sum(lost_deals) lost_deals, 
    sum(left_bt_deals) left_bt_deals
from grp_gdoop_bizops_db.np_bt_attrition_agg
    where l2 = 'HBW' 
          and region = 'NAM'
    group by 1 
    order by 1 desc
    ;

select 
   *
from 
grp_gdoop_bizops_db.jk_bt_availability_gbl;

select deal_uuid from grp_gdoop_bizops_db.np_booking_scope_deals;

select * from grp_gdoop_bizops_db.nvp_tiered_nam_sup_com;

select 
    a.*, 
    b.all_deals prev_all_deals, 
    b.bt_eligible prev_bt_eligible, 
    c.all_deals new_all_deals, 
    c.bt_eligible new_bt_eligible
    from 
    grp_gdoop_bizops_db.nvp_tiered_nam_deepdive as a 
    left join (select * from grp_gdoop_bizops_db.nvp_tiered_nam_sup_com2 where cast(week_end as date) = cast('2020-10-04' as date) ) as b on a.l2 = b.l2 and a.l3 = b.l3 and a.country_code = b.country_code and a.tiered_market = b.tiered_market 
    left join (select * from grp_gdoop_bizops_db.nvp_tiered_nam_sup_com2 where cast(week_end as date) = cast('2021-01-03' as date) ) as c on a.l2 = c.l2 and a.l3 = c.l3 and a.country_code = c.country_code and a.tiered_market = c.tiered_market;


   
select * from grp_gdoop_bizops_db.nvp_tiered_nam_deepdive ;
select min(week_end), max(week_end) from grp_gdoop_bizops_db.nvp_tiered_nam_sup_com;

select country, 
CAST(date_parse( '2020:'||  CAST(week AS varchar), '%x:%v') AS date) week_start
, gdl.grt_l2_cat_description, gdl.grt_l3_cat_description
	,count(distinct deal_uuid) cnt
	,  count(distinct case when  max_avail = 0 then deal_uuid end) no_avail  
	--, count(distinct case when  max_avail = 0 then deal_uuid end)/   cast(count(distinct deal_uuid) as double) perc_no_avail
	, count(distinct case when num_dow <=4 then deal_uuid end) three_dow_blackedout
	--, count(distinct case when num_dow <=4 then deal_uuid end)/   cast(count(distinct deal_uuid) as double) perc_three_dow_blackedout
	, count(distinct case when bookings > 0  then deal_uuid end) bookings_gt_zero
	--, count(distinct case when bookings > 0  then deal_uuid end) /   cast(count(distinct deal_uuid) as double) perc_bookings_gt_zero
	, count(distinct case when bookings >= 5  then deal_uuid end) bookings_gte_five
	--, count(distinct case when bookings >= 5  then deal_uuid end) /   cast(count(distinct deal_uuid) as double) perc_bookings_gte_five
	, count(distinct case when num_dow_avail_booked > 0  then deal_uuid end) dow_full_booked_gt_zero
	--, count(distinct case when num_dow_avail_booked > 0  then deal_uuid end) /   cast(count(distinct deal_uuid) as double) perc_dow_full_booked_gt_zero
from (select avail.deal_uuid, country ,week(reference_date) week --,gdl.grt_l2_cat_description, gdl.grt_l3_cat_description
, max(gss_total_availability )  max_avail 
, count(distinct case when gss_total_availability > 0 then format_datetime(reference_date,'EEEE') end ) num_dow
, sum(gbk_morning +gbk_noon + gbk_afternoon + gbk_evening) bookings	
, count(distinct  case when 
	gss_total_availability > 0  and (gbk_morning+ gbk_noon +gbk_afternoon + gbk_evening) * avail_taken_per_booking   >= gss_total_availability
	then format_datetime(reference_date,'EEEE') end
)as num_dow_avail_booked
from (select * 
	 	, row_number() over(partition by merchant_uuid, deal_uuid,deal_option_uuid,calendar_uuid , reference_date order by days_delta ) update_order
		from grp_gdoop_bizops_db.jk_bt_availability_gbl
		 where reference_date < current_date
		) avail
	join (select groupon_real_deal_uuid as deal_uuid ,groupon_deal_uuid as deal_option_uuid ,  
			(case when min(participants_per_coupon)  OVER (PARTITION BY groupon_real_deal_uuid)= 0 then 1 else participants_per_coupon/ min(participants_per_coupon)  OVER (PARTITION BY groupon_real_deal_uuid) end ) as avail_taken_per_booking 
		from  grp_gdoop_bizops_db.sh_bt_deals
	)deals on deals.deal_uuid = avail.deal_uuid and deals.deal_option_uuid = avail.deal_option_uuid 
where update_order  = 1
group by avail.deal_uuid, country , week(reference_date) --,gdl.grt_l2_cat_description, gdl.grt_l3_cat_description
)a 
join  edwprod.dim_gbl_deal_lob gdl on gdl.deal_id = a.deal_uuid -- and gdl.country_code = a.country
group by country , CAST(date_parse( '2020:'||  CAST(week AS varchar), '%x:%v') AS date),gdl.grt_l2_cat_description, gdl.grt_l3_cat_description
order by country , CAST(date_parse( '2020:'|| CAST(week AS varchar), '%x:%v') AS date),gdl.grt_l2_cat_description, gdl.grt_l3_cat_description;

select 
     wbr_week, 
     cy_week,
     country_code,
     l2,
     tiered_market,
     sum(bt_eligible) bt_eligible, 
     sum(all_deals) all_deals, 
     sum(units) units, 
     sum(bt_eligible_units) bt_eligible_units, 
     sum(bookings) bookings, 
     sum(bookings_redeemed) bookings_redeemed, 
     sum(bt_eligible_txns_redeemed) bt_eligible_txns_redeemed
from grp_gdoop_bizops_db.nvp_bt_marketplace_view_dash
group by 
wbr_week, 
cy_week,
l2,
country_code, 
tiered_market;


select 
     wbr_week, 
     cy_week,
     country_code,
     l2,
     tiered_market,
geo_locale
     sum(bt_eligible) bt_eligible, 
     sum(all_deals) all_deals, 
     sum(units) units, 
     sum(bt_eligible_units) bt_eligible_units, 
     sum(bookings) bookings, 
     sum(bookings_redeemed) bookings_redeemed, 
     sum(bt_eligible_txns_redeemed) bt_eligible_txns_redeemed
from grp_gdoop_bizops_db.nvp_bt_marketplace_view_dash
where country_code = 'US'
group by 
wbr_week, 
cy_week,
l2,
country_code, 
tiered_market, geo_locale;


select distinct geo_locale from grp_gdoop_bizops_db.nvp_deals_geo_locale;

select 
wbr_week,
cy_week, 
geo_locale,
country_code, 
tiered_market, 
sum(bt_eligible) bt_eligible, 
sum(all_deals) all_deals, 
sum(units) units, 
sum(bt_eligible_units) bt_eligible_units, 
sum(bookings) bookings,
sum(bookings_redeemed) bookings_redeemed, 
sum(bt_eligible_txns_redeemed) bt_eligible_txns_redeemed
from grp_gdoop_bizops_db.nvp_bt_marketplace_view_dash
where country_code = 'US'
group by 
wbr_week,
geo_locale,
cy_week, 
country_code, 
tiered_market;

select 
   deal_uuid, 
   this_week_units - last_week_units diff
from
(select 
deal_uuid, 
sum(case when wbr_week = cast('2021-01-03' as date) then units end) last_week_units, 
sum(case when wbr_week = cast('2021-01-10' as date) then units end) this_week_units
from grp_gdoop_bizops_db.rt_bt_txns as a
join grp_gdoop_bizops_db.nvp_day_week_end2 as b on cast(a.book_date as date) = cast(b.day_rw as date)
where country_code = 'US' and booked = 1
and b.wbr_week in (cast('2021-01-03' as date), cast('2021-01-10' as date)) 
group by deal_uuid)
order by diff;

select 
sum(case when wbr_week = cast('2021-01-03' as date) then units end) last_week_units, 
sum(case when wbr_week = cast('2021-01-10' as date) then units end) this_week_units
from grp_gdoop_bizops_db.rt_bt_txns as a
join grp_gdoop_bizops_db.nvp_day_week_end2 as b on cast(a.book_date as date) = cast(b.day_rw as date)
where country_code = 'FR' and booked = 1
and b.wbr_week in (cast('2021-01-03' as date), cast('2021-01-10' as date));

select * from grp_gdoop_bizops_db.nvp_bt_marketplace_view_dash;
select * from grp_gdoop_bizops_db.nvp_bt_marketplace_ytd;
select * from grp_gdoop_bizops_db.nvp_bt_marketplace_view_fin order by dash_selector;
select 
   a.*, 
   b.mn_load_date
from 
   grp_gdoop_bizops_db.nvp_sr_merchant_onboarding as a
   left join 
   grp_gdoop_bizops_db.nvp_sr_min_bt_merchant as b on a.merchant_uuid = b.merchant_uuid
;

select sum(units) from grp_gdoop_bizops_db.nvp_bt_funnel_dash as a
join grp_gdoop_bizops_db.nvp_day_week_end2 as b on cast(a.book_date as date) = cast(b.day_rw as date) and b.wbr_week = cast('2021-01-03' as date)
where a.country_code = 'US';

select sum(bookings) from grp_gdoop_bizops_db.nvp_bt_units_dash where country_code = 'US' and wbr_week = cast('2021-01-03' as date);

select
    sum(case when booked = 1 then units end) bookings_units,
    country_code
  from grp_gdoop_bizops_db.nvp_bt_funnel_dash a
  join grp_gdoop_bizops_db.nvp_day_week_end2 we on a.book_date = we.day_rw and we.wbr_week = cast('2021-01-03' as date)
  group by country_code;

select dt, platform,sum(udv), sum(repeat_views) ,sum(user_max_errors) from grp_gdoop_bizops_db.nvp_usermaxerror_tab group by dt, platform order by dt;

select lower(platform) from grp_gdoop_bizops_db.nvp_deal_udv group by lower(platform);
select lower(platform) from grp_gdoop_bizops_db.nvp_um_repeat_udv group by lower(platform);
select lower(platform), count(1) from grp_gdoop_bizops_db.nvp_user_max_widgets_agg2 group by lower(platform);
select lower(platform), count(1) from grp_gdoop_bizops_db.nvp_bh_user_max_widgets2 group by lower(platform);

select * from prod_groupondw.gbl_traffic_superfunnel_deal 
where lower(cookie_first_sub_platform) = 'touch'
and cast(event_date as date) >= cast('2020-09-15' as date);





select
    lower(bld_platform) platform,
    count(distinct concat(bcookie, case when length(deal_uuid) = 36 then deal_uuid end)) user_max_error, 
    dt
  from grp_gdoop_bizops_db.nvp_bh_user_max_widgets4
  where 
   cast(dt as date) between cast('2020-09-01' as date) and cast('2020-09-05' as date)
group by 
   lower(bld_platform),
   dt;



select dt, platform, sum(user_max_errors) from grp_gdoop_bizops_db.nvp_user_max_tab2 where country_code = 'US' group by dt, platform order by dt;

select platform from grp_gdoop_bizops_db.nvp_user_max_widgets_agg2 group by platform;

select
    lower(bld_platform) platform,
    l2, 
    deal_uuid,
    count(distinct concat(bcookie, case when length(deal_uuid) = 36 then deal_uuid end)) user_max_error, 
    dt
  from grp_gdoop_bizops_db.nvp_bh_user_max_widgets4
  where 
   cast(dt as date) between cast('2020-09-01' as date) and cast('2020-09-05' as date)
group by 
   lower(bld_platform),
   dt, deal_uuid,
   l2;
  
select * from grp_gdoop_bizops_db.nvp_bh_um_udv4 where dt = '2020-09-01' and deal_uuid = 'ca223ee8-4663-45cf-9b8c-07fc9832ff7c';

select lower(platform), count(*) from grp_gdoop_bizops_db.nvp_user_max_widgets_agg2 group by lower(platform);

select * from grp_gdoop_bizops_db.nvp_na_hbw_pull;

select * from grp_gdoop_bizops_db.nvp_na_hbw_pull_dlvl where deal_options > 20;

select * from grp_gdoop_bizops_db.nvp_bh_um_udv4;

select 
    year_of_date, 
    month_of_date, 
    sum(case when deal_options > 3 then count_of_deals end) three_deals,
    sum(count_of_deals) total_hbw_deals
from grp_gdoop_bizops_db.nvp_na_hbw_pull 
group by 
    year_of_date, 
    month_of_date;
    
   
select * from grp_gdoop_bizops_db.nvp_na_hbw_pull;

select * from grp_gdoop_bizops_db.nvp_um_repeat_udv;


select 
    dt, 
    country_code,
    platform,
    sum(udv), 
    sum(repeat_views), 
    sum(user_max_errors)
  from grp_gdoop_bizops_db.nvp_user_max_tab2
  where country_code = 'US'
  group by dt, country_code, platform
  order by dt desc, country_code, platform;