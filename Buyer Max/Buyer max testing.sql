-----TEST
select * from grp_gdoop_bizops_db.nvp_bh_user_max_all;
select * from grp_gdoop_bizops_db.nvp_bh_udv_withdeals2;
select * from grp_gdoop_bizops_db.sh_bh_purchasers;
grp_gdoop_bizops_db.nvp_bh_user_max_all
grp_gdoop_bizops_db.nvp_bh_udv_withdeals
grp_gdoop_bizops_db.nvp_bh_udv_withdeals2
grp_gdoop_bizops_db.nvp_bh_user_uuid_max_all
grp_gdoop_bizops_db.nvp_bh_user_uuid_max_all


--------

select year_of_dt, month_of_dt, sum(total_uniq_rpt_views) udv, sum(total_uniq_attempts) user_max, count(deals_usermax) total_deals
from grp_gdoop_bizops_db.nvp_user_max_tableau_conversion4
group by year_of_dt, month_of_dt
order by year_of_dt asc, month_of_dt asc;

-------

select min(dt), max(dt) from grp_gdoop_bizops_db.nvp_user_max_widgets_agg;

select deal_uuid, sum(user_max_error) user_max_error, sum(total_rpt_views) udv from grp_gdoop_bizops_db.nvp_widget_tab where deal_uuid = '2af0fe3e-7793-febd-1055-46c6ce97e629' group by deal_uuid; 

select sum(user_max_error) user_max_error, sum(total_rpt_views) udv from grp_gdoop_bizops_db.nvp_widget_tab;

select dt from grp_gdoop_bizops_db.nvp_widget_tab group by dt order by dt;

select * from grp_gdoop_bizops_db.nvp_bh_udv_useruuid2 where dt between '2020-07-16' and '2020-08-04';

SELECT SUM(uniq_rpt_views) from (select dt, deal_uuid, sum(uniq_rpt_views) uniq_rpt_views from grp_gdoop_bizops_db.nvp_bh_udv_useruuid2 where dt between group by dt, deal_uuid) fin;

select 
*
from
(select deal_uuid, sum(uniq_rpt_views) uniq_rpt_views 
from grp_gdoop_bizops_db.nvp_bh_udv_useruuid2 
where dt between '2020-07-16' and '2020-08-04'group by deal_uuid) b
right join 
(select deal_uuid, sum(uniq_attempts) uniq_attempts
from grp_gdoop_bizops_db.nvp_user_max_widgets_agg 
group by deal_uuid) a on a.deal_uuid = b.deal_uuid;

----
select 
	a.deals_usermax,
	a.deal_not_active july_end_deal_inactive,
	a.total_uniq_attempts user_max_july, 
	a.total_uniq_rpt_views udv_july, 
	b.deal_not_active sept_end_deal_inactive,
	b.total_uniq_attempts user_max_sept, 
	b.total_uniq_rpt_views udv_sept,
	b.total_uniq_attempts - a.total_uniq_attempts difference
from
(select deals_usermax, deal_not_active, total_uniq_attempts, total_uniq_rpt_views
from grp_gdoop_bizops_db.nvp_user_max_tableau_conversion4 
where month_of_dt = 7 and year_of_dt = 2019) as a 
left join
(select deals_usermax, deal_not_active, total_uniq_attempts, total_uniq_rpt_views
from grp_gdoop_bizops_db.nvp_user_max_tableau_conversion4 
where month_of_dt = 9 and year_of_dt = 2019) as b on a.deals_usermax = b.deals_usermax
having b.deal_not_active = 0
order by difference asc
;

-------


select 
	a.deals_usermax,
	a.deal_not_active july_end_deal_inactive,
	a.total_uniq_attempts user_max_july, 
	a.total_uniq_rpt_views udv_july, 
	b.deal_not_active sept_end_deal_inactive,
	b.total_uniq_attempts user_max_sept, 
	b.total_uniq_rpt_views udv_sept,
	b.total_uniq_attempts - a.total_uniq_attempts difference
from
(select deals_usermax, deal_not_active, total_uniq_attempts, total_uniq_rpt_views
from grp_gdoop_bizops_db.nvp_user_max_tableau_conversion2 
where month_of_dt = 7 and year_of_dt = 2019) as a 
left join
(select deals_usermax, deal_not_active, total_uniq_attempts, total_uniq_rpt_views
from grp_gdoop_bizops_db.nvp_user_max_tableau_conversion2
where month_of_dt = 9 and year_of_dt = 2019) as b on a.deals_usermax = b.deals_usermax
having b.deal_not_active = 0
order by difference asc
;


select 
	a.deals_usermax,
	a.deal_not_active july_end_deal_inactive,
	a.total_uniq_attempts user_max_july, 
	a.total_uniq_rpt_views udv_july, 
	b.deal_not_active sept_end_deal_inactive,
	b.total_uniq_attempts user_max_sept, 
	b.total_uniq_rpt_views udv_sept,
	b.total_uniq_attempts - a.total_uniq_attempts difference
from
(select deals_usermax, deal_not_active, total_uniq_attempts, total_uniq_rpt_views
from grp_gdoop_bizops_db.nvp_user_max_tableau_conversion3 
where month_of_dt = 7 and year_of_dt = 2019) as a 
left join
(select deals_usermax, deal_not_active, total_uniq_attempts, total_uniq_rpt_views
from grp_gdoop_bizops_db.nvp_user_max_tableau_conversion3
where month_of_dt = 9 and year_of_dt = 2019) as b on a.deals_usermax = b.deals_usermax
having b.deal_not_active = 0
order by difference asc
;


select * from grp_gdoop_bizops_db.nvp_user_max_tableau_conversion2;
-------
select 
 a.deal_uuid, a.rank_of_dealsqtr ,a.total_uniq_attempts, b.total_uniq_attempts, b.total_uniq_attempts - a.total_uniq_attempts difference
from
(select deal_uuid, total_uniq_attempts, rank_of_dealsqtr from grp_gdoop_bizops_db.nvp_user_max_top_deals where quarter = 'Q32019') as a
left join 
(select deal_uuid, total_uniq_attempts from grp_gdoop_bizops_db.nvp_user_max_top_deals where quarter = 'Q42019') as b on a.deal_uuid = b.deal_uuid
order by difference asc;



grp_gdoop_bizops_db.nvp_user_max_udv

select 
 a.deal_uuid, a.rank_of_dealsqtr ,a.total_uniq_rpt_views, b.total_uniq_rpt_views, b.total_uniq_rpt_views - a.total_uniq_rpt_views difference
from
(select deal_uuid, total_uniq_rpt_views, rank_of_dealsqtr from grp_gdoop_bizops_db.nvp_user_max_udv_top where quarter = 'Q32019') as a
left join 
(select deal_uuid, total_uniq_rpt_views from grp_gdoop_bizops_db.nvp_user_max_udv_top where quarter = 'Q42019') as b on a.deal_uuid = b.deal_uuid
order by difference asc;


select * from grp_gdoop_bizops_db.nvp_user_max_udv_top;