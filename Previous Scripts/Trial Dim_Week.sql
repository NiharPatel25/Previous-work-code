select ad.deal_uuid, week_of_year_num, week_start, country, i.grt_l1_cat_name l1, i.grt_l2_cat_name l2
	from sandbox.sh_bt_active_deals_log ad
	left join user_edwprod.dim_gbl_deal_lob i on i.deal_id = ad.deal_uuid and i.country_code = ad.country
	join user_groupondw.dim_day bh on bh.day_rw = ad.load_date
	join user_groupondw.dim_week c on bh.week_key = c.week_key
	and ad.load_date >= '2019-07-01'
	and week_of_year_num >= 1
	and partner_inactive_flag = 0 and product_is_active_flag = 1
	and is_bookable = 1
	group by ad.deal_uuid, week_of_year_num, week_start, country, l1, l2;
	


select * from grp_gdoop_bizops_db.sh_bt_active_deals_log limit 5;
select * from user_groupondw.dim_day limit 5;
select * from user_groupondw.dim_week limit 5;
select * from user_groupondw.dim_deal_location limit 5;
