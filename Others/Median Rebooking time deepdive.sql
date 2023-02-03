


create table grp_gdoop_bizops_db.nvp_bt_useravg_time stored as orc as
select * from 
(
select 
     country_code,
     wbr_week,
     latest_cat_l2,
     count(distinct case when book_diff <= 30 and latest_cat_l2 = previous_cat_l2 then user_uuid end) less_than_30,
     count(distinct case when book_diff <= 60 and latest_cat_l2 = previous_cat_l2 then user_uuid end) less_than60,
     count(distinct case when book_diff <= 90 and latest_cat_l2 = previous_cat_l2 then user_uuid end) less_than90
from grp_gdoop_bizops_db.nvp_bt_usermd_time_st2
group by latest_cat_l2, wbr_week, country_code
UNION
select 
    country_code,
    wbr_week,
    'all' as l2,
    count(distinct case when book_diff <= 30 then user_uuid end) less_than_30,
    count(distinct case when book_diff <= 60 then user_uuid end) less_than_60,
    count(distinct case when book_diff <= 90 then user_uuid end) less_than_90
from 
   grp_gdoop_bizops_db.nvp_bt_usermd_time_st2
where country_code <> 'US' and country_code <> 'CA'
group by wbr_week
) as a;

select * from grp_gdoop_bizops_db.nvp_bt_useravg_time;

select * from user_gp.camp_membership_coupons where
          cast(SUBSTRING(created_at,1,10) as date) >= cast('2020-10-01' as date) order by code limit 100;
          
         
select * from grp_gdoop_bizops_db.nvp_bt_usermd_time_st2 where wbr_week = '2020-11-15';

select 
    count(distinct merchant_uuid)
from grp_gdoop_bizops_db.nvp_bt_merchant_attrition
where 
country_code = 'US'
and cast(mn_bt_merch_load_date as date) >= cast('2020-06-01' as date) 
and cast(mn_bt_merch_load_date as date) <= cast('2020-06-30' as date)
;
