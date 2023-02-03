select top 5 * from user_edwprod.agg_gbl_traffic_financials;

create table sandbox.np_funnel_tab as
SELECT 
	report_date as report_Date, 
	report_year as report_year, 
	report_month as report_month, 
	report_week as report_week, 
	country_id as country_id, 
	country_code as country_code, 
	platform as platform,
	economic_area as economic_area, 
	uniq_visitors as uniq_v,
	uniq_deal_views as uniq_deal_views,
	uniq_deal_view_visitors as uniq_deal_view_visitor,
	gross_revenue_USD as gross_revenue_USD,
	nob_USD as nob_USD,
	nor_USD as nor_USD,
	transactions as transactions, 
	net_transactions as net_transactions
FROM user_edwprod.agg_gbl_traffic_financials




/*uniq_buy_btn_clickers as ubbc, 
uniq_conf_page_visitors_gl as ucpg, 
uniq_cart_chkout_visitors as uniq_cart_chkout, 
uniq_conf_page_visitors as uniq_conf_page_visitors,
uniq_cart_summarypage_visitors as uniq_carsumpage_vis,
uniq_cart_conf_page_visitors as uniq_cart_conf_vis, 
uniq_comp_ordr_btn_clcks_gl as uniq_comp_ordr_btn_clcks_gl,
uniq_user_cart_comp_clickers as uniq_user_cart_comp_clickers,
uniq_usr_buy_butt_finl_clckrs as uniq_usr_buy_butt_finl_clckrs, 
uniq_receipt_page_visitors as uniq_receipt_page_visitors, 
uniq_deal_views as uniq_deal_views, 
uniq_buy_btn_clicks as uniq_buy_btn_clicks, 
uniq_cart_chkout_views as uniq_cart_chkout_views, 
uniq_conf_page_views as uniq_conf_page_views,
uniq_cart_summary_page_views as uniq_cart_summary_page_views, 
uniq_cart_conf_page_views, */

select country, l1_, l2_, week_start_a, week_start_b, deals_prev, deals_post, case when cg.deal_uuid is not null then 1 else 0 end is_active, cg.week_start
from 
(
select a.deal_uuid deals_prev, b.deal_uuid deals_post, a.week_start week_start_a, a.week_of_year_num, b.week_start week_start_b, a.country, a.l1 l1_, a.l2 l2_
from
	(
	select ad.deal_uuid, week_of_year_num, week_start, country, i.grt_l1_cat_name l1, i.grt_l2_cat_name l2
	from sandbox.sh_bt_active_deals_log ad
	left join user_edwprod.dim_gbl_deal_lob i on i.deal_id = ad.deal_uuid and i.country_code = ad.country
	join user_groupondw.dim_day bh on bh.day_rw = ad.load_date
	join user_groupondw.dim_week c on bh.week_key = c.week_key
	and ad.load_date >= '2019-07-01'
	and week_of_year_num >= 1
	and partner_inactive_flag = 0 and product_is_active_flag = 1
	and is_bookable = 1
	group by ad.deal_uuid, week_of_year_num, week_start, country, l1, l2
	) a
	left join
	(
	select ad.deal_uuid, week_of_year_num, week_start, country
	from sandbox.sh_bt_active_deals_log ad
	join user_groupondw.dim_day bh on bh.day_rw = ad.load_date
	join user_groupondw.dim_week c on bh.week_key = c.week_key
	and ad.load_date >= '2019-07-01'
	and partner_inactive_flag = 0 and product_is_active_flag = 1
	and is_bookable = 1
	and week_of_year_num >=1
	group by ad.deal_uuid, week_of_year_num, week_start, country
	) b on a.deal_uuid = b.deal_uuid and a.country = b.country and year(a.week_start) = year(b.week_start) and b.week_of_year_num = a.week_of_year_num+1
	group by a.deal_uuid, b.deal_uuid, a.week_start, a.week_of_year_num, b.week_start, a.country, a.l1, a.l2) fn
left join 
	(
	select deal_uuid, week_start, week_of_year_num
	from user_groupondw.active_deals ad
	join user_groupondw.dim_day bh on bh.day_rw = ad.load_date
	join user_groupondw.dim_week c on bh.week_key = c.week_key
	where load_date >= '2019-07-01'
	group by deal_uuid,  week_start, week_of_year_num) cg on cg.deal_uuid = fn.deals_prev and cg.week_of_year_num = fn.week_of_year_num+1 and year(fn.week_start_a) = year(cg.week_start)
group by 1,2,3,4,5,6,7,8,9;
	

select country, l1_, l2_, week_start_a, week_start_b, count(distinct deals_prev), count(distinct deals_post), case when cg.deal_uuid is not null then 1 else 0 end is_active
from 
(
select a.deal_uuid deals_prev, b.deal_uuid deals_post, a.week_start week_start_a, a.week_of_year_num, b.week_start week_start_b, a.country, a.l1 l1_, a.l2 l2_
from
	(
	select ad.deal_uuid, week_of_year_num, week_start, country, i.grt_l1_cat_name l1, i.grt_l2_cat_name l2
	from sandbox.sh_bt_active_deals_log ad
	left join user_edwprod.dim_gbl_deal_lob i on i.deal_id = ad.deal_uuid and i.country_code = ad.country
	join user_groupondw.dim_day bh on bh.day_rw = ad.load_date
	join user_groupondw.dim_week c on bh.week_key = c.week_key
	and ad.load_date >= '2019-07-01'
	and week_of_year_num >= 1
	and partner_inactive_flag = 0 and product_is_active_flag = 1
	and is_bookable = 1
	group by ad.deal_uuid, week_of_year_num, week_start, country, l1, l2
	) a
	left join
	(
	select ad.deal_uuid, week_of_year_num, week_start, country
	from sandbox.sh_bt_active_deals_log ad
	join user_groupondw.dim_day bh on bh.day_rw = ad.load_date
	join user_groupondw.dim_week c on bh.week_key = c.week_key
	and ad.load_date >= '2019-07-01'
	and partner_inactive_flag = 0 and product_is_active_flag = 1
	and is_bookable = 1
	and week_of_year_num >=1
	group by ad.deal_uuid, week_of_year_num, week_start, country
	) b on a.deal_uuid = b.deal_uuid and a.country = b.country and year(a.week_start) = year(b.week_start) and b.week_of_year_num = a.week_of_year_num+1
	group by a.deal_uuid, b.deal_uuid, a.week_start, a.week_of_year_num, b.week_start, a.country, a.l1, a.l2) fn
left join 
	(
	select deal_uuid, week_start, week_of_year_num
	from user_groupondw.active_deals ad
	join user_groupondw.dim_day bh on bh.day_rw = ad.load_date
	join user_groupondw.dim_week c on bh.week_key = c.week_key
	where load_date >= '2019-07-01'
	group by deal_uuid,  week_start, week_of_year_num) cg on cg.deal_uuid = fn.deals_prev and cg.week_of_year_num = fn.week_of_year_num+1 and year(fn.week_start_a) = year(cg.week_start)
group by 1,2,3,4,5,8;

/* 90 day attrition */

select country, l1, l2, sum(fin.active_fr_90), count(*) from 
        (                        select pri.*,
                        case when pri.bt_upload_date + 90 <= pri.bt_upto_date then 1 else 0 end active_fr_90, 
                        case when pri.bt_upload_date + 90 <= pri.grp_max_date then 1 when pri.bt_upload_date + 90 > pri.grp_max_date and pri.grp_max_date = pri.bt_upto_date then 0 else 1 end still_on_grp
                        from 
                        (select 
                         act.deal_uuid deal_uuid, act.upload_date bt_upload_date, act.on_upto bt_upto_date, up.max_date grp_max_date, act.country, i.grt_l1_cat_name l1, i.grt_l2_cat_name l2
                                from 
                                        (select deal_uuid, country country, cast(min(load_date) as date) upload_date, max(load_date) on_upto from sandbox.sh_bt_active_deals_log group by deal_uuid, country having upload_date >= '2019-07-01' and upload_date < current_date - 94 where load_date <= current_date-4 and  is_bookable = 1 and partner_inactive_flag = 0 and product_is_active_flag = 1) act
                                left join 
                                        (select deal_uuid,case when country_code = 'GB' then 'UK' else country_code end country_code, max(load_date) max_date from user_groupondw.active_deals where load_date >= '2019-07-01' and load_date <= current_date group by deal_uuid, country_code) up on act.deal_uuid = up.deal_uuid and act.country = up.country_code
                                left join user_edwprod.dim_gbl_deal_lob i on i.deal_id = act.deal_uuid and i.country_code = act.country ) pri) fin group by country, l1, l2 where fin.still_on_grp = 1;

                               
select country, l1, l2, sum(fin.active_fr_90), count(*) from 
        (                        select pri.*,
                        case when pri.bt_upload_date + 90 <= pri.bt_upto_date then 1 else 0 end active_fr_90, 
                        case when pri.bt_upload_date + 90 <= pri.grp_max_date then 1 when pri.bt_upload_date + 90 > pri.grp_max_date and pri.bt_upto_date = pri.grp_max_date then 0 when pri.bt_upload_date + 90 > pri.grp_max_date and pri.bt_upto_date < pri.grp_max_date then 0 else 1 end still_on_grp
                        from 
                        (select 
                         act.deal_uuid deal_uuid, act.upload_date bt_upload_date, act.on_upto bt_upto_date, up.max_date grp_max_date, act.country, i.grt_l1_cat_name l1, i.grt_l2_cat_name l2
                                from 
                                        (select deal_uuid, country country, cast(min(load_date) as date) upload_date, max(load_date) on_upto from sandbox.sh_bt_active_deals_log group by deal_uuid, country having upload_date >= '2019-07-01' and upload_date < current_date - 94 where load_date <= current_date-4 and  is_bookable = 1 and partner_inactive_flag = 0 and product_is_active_flag = 1) act
                                left join 
                                        (select deal_uuid,case when country_code = 'GB' then 'UK' else country_code end country_code, max(load_date) max_date from user_groupondw.active_deals where load_date >= '2019-07-01' and load_date <= current_date group by deal_uuid, country_code) up on act.deal_uuid = up.deal_uuid and act.country = up.country_code
                                left join user_edwprod.dim_gbl_deal_lob i on i.deal_id = act.deal_uuid and i.country_code = act.country ) pri) fin group by country, l1, l2 where fin.still_on_grp = 1;

                               
select country, l1, l2, sum(fin.inactive_fr_90), count(*) from 
        (          select pri.*,
                        case when pri.bt_upto_date < pri.bt_upload_date + 90 then 1 else 0 end inactive_fr_90,
                        case when pri.grp_max_date > pri.bt_upload_date+90 then 1 else 0 end still_on_grp
                        from 
                        (select 
                         act.deal_uuid deal_uuid, act.upload_date bt_upload_date, act.on_upto bt_upto_date, up.max_date grp_max_date, act.country, i.grt_l1_cat_name l1, i.grt_l2_cat_name l2
                                from 
                                        (select deal_uuid, country country, cast(min(load_date) as date) upload_date, max(load_date) on_upto from sandbox.sh_bt_active_deals_log group by deal_uuid, country having upload_date >= '2019-07-01' and upload_date < current_date - 94 where load_date <= current_date and  is_bookable = 1 and partner_inactive_flag = 0 and product_is_active_flag = 1) act
                                left join 
                                        (select deal_uuid,case when country_code = 'GB' then 'UK' else country_code end country_code, max(load_date) max_date from user_groupondw.active_deals where load_date >= '2019-07-01' and load_date <= current_date group by deal_uuid, country_code) up on act.deal_uuid = up.deal_uuid and act.country = up.country_code
                                left join user_edwprod.dim_gbl_deal_lob i on i.deal_id = act.deal_uuid and i.country_code = act.country ) pri) fin group by country, l1, l2 where fin.still_on_grp = 1;
                                              
                                                              