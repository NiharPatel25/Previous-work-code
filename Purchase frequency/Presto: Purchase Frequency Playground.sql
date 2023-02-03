


select 
    booked_frst_ord,
    year_of_purch, 
    month_of_purch,
    sum(distinct_user_1) total_redeemers,
    sum(three_star_user) three_star_users, 
    sum(two_star_user) two_star_users,
    sum(one_star_user) one_star_users,
    sum(repeat_visitors) repeat_visitors, 
    sum(uv) Total_uv, 
    sum(udv) Total_udv, 
    sum(udv_notbt_eligible) udv_notbt,
    sum(udv_bt_eligible) udv_bt,
    sum(ninety_day_order) nine_day_orders,
    sum(ninety_day_order_from_b) ninety_day_order_from_b,
    sum(three_star_day_order) three_star_day_order, 
    sum(two_star_day_order) two_star_day_order, 
    sum(one_star_day_order) one_star_day_order, 
    sum(units_total) total_units, 
    sum(units_notbt_eligible) non_bt_units, 
    sum(three_star_units_notbt_eligible) three_star_units_notbt_eligible, 
    sum(two_star_units_notbt_eligible) two_star_units_notbt_eligible, 
    sum(one_star_units_notbt_eligible) one_star_units_notbt_eligible,
    sum(units_bt_eligible) bt_eligible_units,
    sum(three_star_units_bt_eligible) three_star_units_bt_eligible, 
    sum(two_star_units_bt_eligible) two_star_units_bt_eligible, 
    sum(one_star_units_bt_eligible) one_star_units_bt_eligible
from grp_gdoop_bizops_db.nvp_engagement_purch_basic_tableau_nam90 
where country_code = 'US' and l2 = 'L2 - Health / Beauty / Wellness'
group by booked_frst_ord, year_of_purch, month_of_purch;



select * from grp_gdoop_bizops_db.nvp_engagement_purch_basic_tableau_nam90;



select * from grp_gdoop_bizops_db.nvp_purch_trial_one_nam90
where country_code = 'US' and l2 = 'L2 - Health / Beauty / Wellness';

select * from grp_gdoop_bizops_db.nvp_purch_freq_final_nam90
where country_code = 'US' and l2 = 'L2 - Health / Beauty / Wellness';





select
     ltwo,
     booked_frst_ord,
     sum(udv) udv, 
     sum(udv_notbt_eligible) udv_notbt, 
     sum(udv_bt_eligible) udv_bt, 
     sum(units_total) units, 
     sum(units_notbt_eligible) non_bt_units, 
     sum(units_bt_eligible) bt_eligible_units
from 
grp_gdoop_bizops_db.nvp_purch_freq_fin_bt_down_nam90
where country_code = 'US' and l2 = 'L2 - Health / Beauty / Wellness'
group by booked_frst_ord, ltwo
order by booked_frst_ord, ltwo;






