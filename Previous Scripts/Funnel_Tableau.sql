
use grp_gdoop_bizops_db;
create table np_funnel_tab as
select 
	agtfl2.report_date as date_, 
	agtfl2.country_code country_code, 
	agtfl2.economic_area econ_area,
	agtfl2.country_id country_id, 
	agtfl2.grt_l1_cat_name l1, 
	agtfl2.grt_l2_cat_name l2, 
	agtf.uniq_visitors UV, 
	agtfl2.uniq_deal_views udv, 
	agtfl2.uniq_deal_view_visitors udvv
from user_edwprod.agg_gbl_traffic_l2 agtfl2 
left join edwprod.agg_gbl_traffic_financials agtf on 
	agtfl2.country_code = agtf.country_code and agtfl2.report_date = agtf.report_date and agtfl2.platform = agtf.platform and agtfl2.sub_platform = agtf.sub_platform and agtfl2.traffic_sub_source = agtf.traffic_sub_source where year(agtfl2.report_date) >2018;

drop table grp_gdoop_bizops_db.np_funnel_tab;

select * from user_edwprod.agg_gbl_traffic limit 5;
select * from user_edwprod.agg_gbl_traffic_l2 limit 5;

create table np_funnel_tableau as
select 
	l2.report_date as date_,
	l2.country_code as country, 
	l2.economic_area as econ_area,
	l2.grt_l1_cat_name as l1_cat,
	l2.grt_l2_cat_name as l2_cat,
	traffic.uniq_visitors as UV, 
	l2.uniq_deal_views as UDV, 
	l2.uniq_deal_view_visitors as UDVV, 
	l2.uniq_conf_page_views + l2.uniq_cart_conf_page_views as uniq_conf_page_view, 
	l2.uniq_receipt_page_visitors as UCRV
from user_edwprod.agg_gbl_traffic_l2 l2
left join user_edwprod.agg_gbl_traffic traffic on 
l2.report_date = traffic.report_date and l2.country_code = traffic.country_code and l2.economic_area = traffic.economic_area and l2.sub_platform = traffic.sub_platform and l2.traffic_source = traffic.traffic_source and l2.traffic_sub_source = traffic.traffic_sub_source where l2.report_year > 2017;


alter table grp_gdoop_bizops_db.np_funnel_tableau change date_ date_ date;