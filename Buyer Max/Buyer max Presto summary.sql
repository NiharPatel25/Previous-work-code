-----USER MAX Widet tracking

select
a.deal_uuid,
a.l2,
sum(uniq_attempts) user_max,
sum(uniq_rpt_views) rpt_view, 
sum(three_wk_uk) udv
from
(select deal_uuid, l2, sum(uniq_attempts) uniq_attempts
from grp_gdoop_bizops_db.nvp_user_max_widgets_agg 
group by deal_uuid, l2) a
left join 
(select deal_uuid, sum(uniq_rpt_views) uniq_rpt_views 
from grp_gdoop_bizops_db.nvp_bh_udv_useruuid2 
where dt between '2020-07-16' and '2020-08-04' group by deal_uuid) b on a.deal_uuid = b.deal_uuid
left join grp_gdoop_bizops_db.nvp_user_max_avg c on a.deal_uuid = c.deal_id
group by l2, a.deal_uuid order by user_max desc
limit 1000;



select
a.l2,
sum(uniq_attempts) user_max,
sum(uniq_rpt_views) rpt_view, 
count(distinct a.deal_uuid) 
from
(select deal_uuid, l2, sum(uniq_attempts) uniq_attempts
from grp_gdoop_bizops_db.nvp_user_max_widgets_agg 
group by deal_uuid, l2) a
left join 
(select deal_uuid, sum(uniq_rpt_views) uniq_rpt_views 
from grp_gdoop_bizops_db.nvp_bh_udv_useruuid2 
where dt between '2020-07-16' and '2020-08-04' group by deal_uuid) b on a.deal_uuid = b.deal_uuid
group by a.l2 order by l2;



select count(deal_uuid) from grp_gdoop_bizops_db.nvp_user_max_widgets_agg;

select sum(total_rpt_views), sum(user_max_error) from grp_gdoop_bizops_db.nvp_widget_tab;

select * from grp_gdoop_bizops_db.nvp_bh_udv_useruuid2;



-------


select year_of_dt, month_of_dt,l2, sum(total_uniq_attempts) user_max, sum(total_uniq_rpt_views) udv, count(distinct deals_usermax)
from grp_gdoop_bizops_db.nvp_user_max_tableau_conversion4
group by year_of_dt, month_of_dt, l2 order by year_of_dt asc, month_of_dt asc, l2 asc;

select min(dt), max(dt) from grp_gdoop_bizops_db.nvp_bh_udv_useruuid2;

select * from grp_gdoop_bizops_db.nvp_user_max_tableau_conversion4;


-----

select * from grp_gdoop_bizops_db.nvp_user_max_avg;


-----


select max(dt) from grp_gdoop_bizops_db.nvp_bh_user_max_widgets;