/*create MULTISET table sandbox.np_fine_deals as
(select a.*, b.lone, b.ltwo, b.lthree,c.deal_uuid as active from
(select 
			x.parent_order_uuid parent_order_uuid, 
			x.order_uuid order_uuid,
			x.order_id order_id, 
			x.order_date order_date, 
			x.unified_deal_option_id deal_id,
			x.deal_uuid deal_uuid, 
			x.user_uuid fgt_user_uuid, 
			x.country_id country_id, 
			y.country_iso_code_2 country_code
	from
			user_edwprod.fact_gbl_transactions x
			left join user_groupondw.gbl_dim_country y on x.country_id = y.country_key
			where x.order_date >= '2020-01-01' and x.action = 'authorize'
		) as a
inner join 
(select 
	deal_id as deal_uuid, 
	country_code as country_code,  
	country_id as country_id,
	grt_l1_cat_name as lone, 
	grt_l2_cat_name as ltwo, 
	grt_l3_cat_name as lthree
from user_edwprod.dim_gbl_deal_lob where grt_l1_cat_name = 'L1 - Local') b on b.deal_uuid = a.deal_uuid and a.country_code = b.country_code
inner join 
(select 
	deal_uuid deal_uuid, 
	case when country_code = 'GB' then 'UK' else country_code end country_code
	from user_groupondw.active_deals where load_date = '2020-03-22' and available_qty > 0 and sold_out = 'false' group by deal_uuid, country_code) as c on c.deal_uuid = a.deal_uuid and a.country_code = c.country_code) with data;
---------

create multiset table sandbox.np_top_fine_deals3 as
(select deal_uuid, count_, country_code,  row_number() over(partition by country_code order by count_ desc) rank_ from 
(select deal_uuid, country_code, count(parent_order_uuid) count_  from sandbox.np_fine_deals where country_code <> 'US'group by deal_uuid, country_code) as a) with data;*/

---------


select * from day_of_week_res_table;

create table sandbox.np_regexp as(
select a.*, b.fine_print fine_print from sandbox.np_top_fine_deals3 as a 
left join user_gp.deal_localized_contents as b on a.deal_uuid = b.deal_uuid where a.rank_<=200) with data;

drop table sandbox.np_regexp;

---------- creating restriction sandbox
drop table sandbox.nvp_time_date_rstr;

create multiset table sandbox.nvp_time_date_rstr
(text_res varchar(100) character set unicode, 
text_res_agg varchar(100) character set unicode,
restrict_type varchar(100) character set unicode
);

select * from sandbox.nvp_time_date_rstr;




------------- selecting all active deals and their fine print

drop table sandbox.np_fine_deals;

create MULTISET table sandbox.np_fine_deals as
(select a.*, c.fine_print, lower(REGEXP_REPLACE(REGEXP_REPLACE(c.fine_print, '\<strong\>' , ''), '[^a-zA-Z0-9.:_-]',' ')) fine_print_c, 
	d.min_contract_sell, 
	d.max_contract_sell, 
	d.afternoon_tea,
	e.sum_nob, 
	e.count_orders, 
	f.udv udv
From 
		(select 
			deal_id as deal_uuid, 
			country_code as country_code,  
			country_id as country_id,
			grt_l1_cat_name as lone, 
			grt_l2_cat_name as ltwo, 
			grt_l3_cat_name as lthree
		from user_edwprod.dim_gbl_deal_lob where grt_l1_cat_name = 'L1 - Local' AND country_code = 'UK') as a
	inner join 
		(select 
			distinct deal_uuid deal_uuid1, 
			case when country_code = 'GB' then 'UK' else country_code end country_code
			from user_groupondw.active_deals where country_code = 'GB' and load_date >= '2019-09-01' and load_date < '2020-01-01' and available_qty > 0 and sold_out = 'false' 
			group by deal_uuid, country_code) as b 
			on b.deal_uuid1 = a.deal_uuid and b.country_code = a.country_code
	left join 
		(select deal_uuid deal_uuid2, 
				fine_print from 
				user_gp.deal_localized_contents 
				where locale = 'en_GB') as c on a.deal_uuid = c.deal_uuid2
	left join
		(select product_uuid,
				min(contract_sell_price) min_contract_sell,
				max(contract_sell_price) max_contract_sell,
				min(REGEXP_SUBSTR(lower(offer_description), 'afternoon tea', 1, 1, 'i')) afternoon_tea
		from user_edwprod.dim_offer_ext group by product_uuid) as d on a.deal_uuid = d.product_uuid
	left join
		(select 
					x.deal_uuid deal_uuid, 
					y.country_iso_code_2 country_code,
					count(parent_order_uuid) count_orders,
					sum(x.auth_nob_loc) sum_nob
			from
					user_edwprod.fact_gbl_transactions x
					left join user_groupondw.gbl_dim_country y on x.country_id = y.country_key
					where x.order_date >= '2019-09-01' and x.order_date < '2020-01-01' and x.action = 'authorize'
					group by deal_uuid, country_code
		) as e on e.deal_uuid = a.deal_uuid and e.country_code = a.country_code
	left join 
		(select 
			deal_id, 
			country_code,
			sum(uniq_deal_views) udv
		from user_edwprod.agg_gbl_traffic_deal 
		where report_date >= '2019-09-01' and report_date < '2020-01-01'
		group by deal_id, country_code
		) as f on f.deal_id = a.deal_uuid and f.country_code = a.country_code
) with data;

select deal_uuid, cast(count_orders as float)/cast(udv as float) from sandbox.np_fine_deals2;
select distinct locale from user_gp.deal_localized_contents;
select * from user_gp.deal_localized_contents ;
	
-------------count(deal_uuid), sum(union_case), sum(day_date_only) ,sum(time_only), sum(holiday_only), sum(all_intersection), sum(day_and_holiday), sum(day_and_time), sum(holiday_and_time)
-----avg_sell_price, cast(day_date_only as char(1)), cast(holiday_only as char(1)), cast(time_only as char(1)), cast(day_and_holiday as char(1)), cast(day_and_time as char(1)), cast(holiday_and_time as char(1)), cast(all_intersection as char(1)), count(deal_uuid), sum(count_orders), sum(sum_nob)


---and ltwo = 'L2 - Food & Drink'
---'(-(\d+)?[.:-]?\d+pm | -(\d+)?[.:-]?\d+am)'

---when REGEXP_SUBSTR(fine_print_c, '(-([0-6])[.:-]\d+pm | -([0-6])[.:-]\d+am)', 1, 1, 'i') is not null then 'am-pm rest'
---when REGEXP_SUBSTR(fine_print_c, '(-([0-6])pm | -([0-6])am)', 1, 1, 'i') is not null then 'am-pm rest'

					
					
/*when REGEXP_SUBSTR(fine_print_c, '(after.?[0-12].?pm|before.?[0-12].?pm|after.?[0-12].?am|before.?[0-12].?am) ' , 1, 1, 'i') is not null then 'before - am/pm rest'*/

/*select  count(deal_uuid), sum(union_case), sum(day_date_only) ,sum(time_only12), sum(time_only2), sum(time_only1), sum(holiday_only), sum(all_intersection) from (
	select deal_uuid, l1, l2, l3, 
			fine_print_c, 
			avg_sell_price, 
			sum_nob, 
			max(day_date_restriction) day_date_restriction_a, 
			max(holiday_restriction) holiday_restriction_a, 
			max(time_restriction) time_restriction1_a,
			max(time_restriction2) time_restriction2_a,
			case when day_date_restriction_a = 1 and holiday_restriction_a <> 1 and time_restriction1_a <> 1 and time_restriction2_a <> 1 then 1 else 0 end day_date_only, 
			case when day_date_restriction_a <> 1 and holiday_restriction_a = 1 and time_restriction1_a <> 1 and time_restriction2_a <> 1 then 1 else 0 end holiday_only ,
			case when day_date_restriction_a <> 1 and holiday_restriction_a <> 1 and time_restriction1_a = 1 and time_restriction2_a <> 1 then 1 else 0 end time_only1,
			case when day_date_restriction_a <> 1 and holiday_restriction_a <> 1 and time_restriction1_a <> 1 and time_restriction2_a = 1 then 1 else 0 end time_only2,
			case when day_date_restriction_a <> 1 and holiday_restriction_a <> 1 and time_restriction1_a = 1 or time_restriction2_a = 1 then 1 else 0 end time_only12,
			case when day_date_restriction_a = 1 and holiday_restriction_a = 1 and time_restriction1_a = 1 and time_restriction2_a = 1 then 1 else 0 end all_intersection,
			case when day_date_restriction_a = 1 or holiday_restriction_a = 1 or time_restriction1_a = 1 or  time_restriction2_a = 1 then 1 else 0 end union_case
	from 
			(select 
			dd.deal_uuid deal_uuid, 
			dd.country_code country_cd, 
			dd.country_id country_id, 
			dd.lone l1, 
			dd.ltwo l2, 
			dd.lthree l3, 
			dd.fine_print_c fine_print_c, 
			case when dd.avg_sell_price < 25 then '<25' when dd.avg_sell_price >=25 and dd.avg_sell_price < 50 then '25-50' when dd.avg_sell_price >= 50 then '>=50' else null end avg_sell_price,
			dd.sum_nob,
			case when typ_a.ind = 1 then 1 else 0 end day_date_restriction, 
			case when typ_b.ind = 1 then 1 else 0 end holiday_restriction,
			case when typ_c.ind = 1 then 1 else 0 end time_restriction, 
			case when ext_time.ind = 1 then 1 else 0 end time_restriction2
			from 
				(	select 
					deal_uuid, 
					country_code,
					country_id, 
					lone, 
					ltwo, 
					lthree, 
					fine_print_c,
					(min_contract_sell + max_contract_sell)/2 avg_sell_price,
					sum_nob
					from 
					sandbox.np_fine_deals where country_code = 'UK' and ltwo = 'L2 - Food & Drink') dd
				left join 
					(	select 
						text_res,
						text_res_agg,
						restrict_type,
						1 AS ind
						from 
						sandbox.nvp_time_date_rstr where restrict_type = 'Date/Day Restrictions'
						) typ_a on REGEXP_SUBSTR(dd.fine_print_c, lower(typ_a.text_res), 1, 1, 'i') = lower(typ_a.text_res)
				left join 
					( select 
						text_res,
						text_res_agg,
						restrict_type,
						1 AS ind
						from 
						sandbox.nvp_time_date_rstr where restrict_type = 'Holiday Restrictions'
					) typ_b on REGEXP_SUBSTR(dd.fine_print_c, lower(typ_b.text_res), 1, 1, 'i') = lower(typ_b.text_res)
				left join
					(
					select 
						text_res,
						text_res_agg,
						restrict_type,
						1 AS ind
						from 
						sandbox.nvp_time_date_rstr where restrict_type = 'Time Restriction'
					) typ_c on REGEXP_SUBSTR(dd.fine_print_c, lower(typ_c.text_res), 1, 1, 'i') = lower(typ_c.text_res)
				left join
					(select 
								deal_uuid deal_uuid2, 
								fine_print_c, 
								case when REGEXP_SUBSTR(fine_print_c, '([0-12]?.?[0-60]?-[0-12]?.?[0-60]pm | [0-12]?.?[0-60]?-[0-12]?.?[0-60]am) ' , 1, 1, 'i') is not null then 'am-pm rest' 
									 end as text_res_agg, 
								case when text_res_agg is not null then 'Time Restriction' end as restrict_type,
								1 as ind
						from sandbox.np_fine_deals where text_res_agg is not null
					) ext_time on dd.deal_uuid = ext_time.deal_uuid2
) fin 
group by 1,2,3,4,5,6,7) as final_
;*/

---------------------avg_sell_price, text_res_agg, restrict_type, count(distinct deal_uuid) count_of_deals, sum(sum_nob) sum_nob, ROW_NUMBER() over( partition by avg_sell_price order by count_of_deals desc) top_value




-----count(deal_uuid), sum(union_case), sum(day_date_only) ,sum(time_only), sum(holiday_only), sum(all_intersection), sum(day_and_holiday), sum(day_and_time), sum(holiday_and_time)
-----avg_sell_price, cast(day_date_only as char(1)), cast(holiday_only as char(1)), cast(time_only as char(1)), cast(day_and_holiday as char(1)), cast(day_and_time as char(1)), cast(holiday_and_time as char(1)), cast(all_intersection as char(1)), count(deal_uuid), sum(count_orders), sum(sum_nob)
----530
--(-(\d+)?[.:-]?\d+pm | -(\d+)?[.:-]?\d+am)'
select * from sandbox.np_fine_deals;
select * from sandbox.np_fine_deals;
---'L2 - Health / Beauty / Wellness', ltwo = 'L2 - Food & Drink', 

select count(deal_uuid), 
		sum(union_case), 
		sum(union_span),
		sum(new_and_span_intersec),
		sum(day_date_only) ,
		sum(time_only), 
		sum(holiday_only), 
		sum(all_intersection), 
		sum(day_and_holiday), 
		sum(day_and_time), 
		sum(holiday_and_time),
		sum(new_client_a)
from (
	select deal_uuid, l1, l2, l3, 
			fine_print_c, 
			avg_sell_price, 
			sum_nob,
			count_orders,
			udv,
			max(day_date_restriction) day_date_restriction_a, 
			max(holiday_restriction) holiday_restriction_a, 
			max(time_restriction) time_restriction_a,
			max(new_client_restriction) new_client_a,
			case when day_date_restriction_a = 1 and holiday_restriction_a <> 1 and time_restriction_a <> 1 then 1 else 0 end day_date_only, 
			case when day_date_restriction_a <> 1 and holiday_restriction_a = 1 and time_restriction_a <> 1 then 1 else 0 end holiday_only ,
			case when day_date_restriction_a <> 1 and holiday_restriction_a <> 1 and time_restriction_a = 1 then 1 else 0 end time_only,
			case when day_date_restriction_a = 1 and holiday_restriction_a = 1 and time_restriction_a <> 1 then 1 else 0 end day_and_holiday,
			case when day_date_restriction_a = 1 and holiday_restriction_a <> 1 and time_restriction_a = 1 then 1 else 0 end day_and_time,
			case when day_date_restriction_a <> 1 and holiday_restriction_a = 1 and time_restriction_a = 1 then 1 else 0 end holiday_and_time,
			case when day_date_restriction_a = 1 and holiday_restriction_a = 1 and time_restriction_a = 1  then 1 else 0 end all_intersection,
			case when day_date_restriction_a = 1 or holiday_restriction_a = 1 or time_restriction_a = 1 then 1 else 0 end union_span,
			case when union_span = 1 or new_client_a = 1 then 1 else 0 end union_case,
			case when new_client_a = 1 and union_span = 1 then 1 else 0 end new_and_span_intersec
	from 
			(select 
			dd.deal_uuid deal_uuid, 
			dd.country_code country_cd, 
			dd.country_id country_id, 
			dd.lone l1, 
			dd.ltwo l2, 
			dd.lthree l3, 
			dd.fine_print_c fine_print_c, 
			case when dd.avg_sell_price < 25 then '<25' when dd.avg_sell_price >=25 and dd.avg_sell_price < 50 then '25-50' when dd.avg_sell_price >= 50 then '>=50' else null end avg_sell_price,
			dd.sum_nob,
			dd.count_orders,
			dd.udv,
			case when typ_a.ind = 1 then 1 else 0 end day_date_restriction, 
			case when typ_b.ind = 1 then 1 else 0 end holiday_restriction,
			case when typ_c.ind = 1 or ext_time.ind = 1 then 1 else 0 end time_restriction,
			case when typ_d.ind = 1 then 1 else 0 end new_client_restriction
			from 
					(	select 
						deal_uuid, 
						country_code,
						country_id, 
						lone, 
						ltwo, 
						lthree, 
						fine_print_c,
						(min_contract_sell + max_contract_sell)/2 avg_sell_price,
						sum_nob,
						count_orders, 
						udv
						from 
						sandbox.np_fine_deals where country_code = 'UK' and afternoon_tea is null and ltwo = 'L2 - Things to Do - Leisure' or ltwo = 'L2 - Things to Do - Live') dd
				left join 
					(	select 
						text_res,
						text_res_agg,
						restrict_type,
						1 AS ind
						from 
						sandbox.nvp_time_date_rstr where restrict_type = 'Date/Day Restrictions'
						) typ_a on REGEXP_SUBSTR(dd.fine_print_c, lower(typ_a.text_res), 1, 1, 'i') = lower(typ_a.text_res)
				left join 
					( select 
						text_res,
						text_res_agg,
						restrict_type,
						1 AS ind
						from 
						sandbox.nvp_time_date_rstr where restrict_type = 'Holiday Restrictions'
					) typ_b on REGEXP_SUBSTR(dd.fine_print_c, lower(typ_b.text_res), 1, 1, 'i') = lower(typ_b.text_res)
				left join
					(
					select 
						text_res,
						text_res_agg,
						restrict_type,
						1 AS ind
						from 
						sandbox.nvp_time_date_rstr where restrict_type = 'Time Restriction'
					) typ_c on REGEXP_SUBSTR(dd.fine_print_c, lower(typ_c.text_res), 1, 1, 'i') = lower(typ_c.text_res)
				left join
					(
					select 
						text_res,
						text_res_agg,
						restrict_type,
						1 AS ind
						from 
						sandbox.nvp_time_date_rstr where restrict_type = 'New Clients Restriction'
					) typ_d on REGEXP_SUBSTR(dd.fine_print_c, lower(typ_d.text_res), 1, 1, 'i') = lower(typ_d.text_res)
				left join
					(select
								deal_uuid deal_uuid2, 
								fine_print_c, 
								case when REGEXP_SUBSTR(fine_print_c, '(-([0-5])[.:-]\d+pm|-([0-9]+)[.:-]\d+am)', 1, 1, 'i') is not null then 'am-pm rest'
								when REGEXP_SUBSTR(fine_print_c, '(-([0-5])pm|-([0-9]+)am)', 1, 1, 'i') is not null then 'am-pm rest'end as text_res_agg,
								case when text_res_agg is not null then 'Time Restriction' end as restrict_type,
								1 as ind
						from sandbox.np_fine_deals where text_res_agg is not null
					) ext_time on dd.deal_uuid = ext_time.deal_uuid2
	) fin 
group by 1,2,3,4,5,6,7,8,9) as final_;

SELECT top 5 * FROM user_edwprod.agg_gbl_traffic_deal;
select * from sandbox.nvp_time_date_rstr;

-----deal type < 5:30

select 
l2, 
text_res_agg,
restrict_type,
count(distinct deal_uuid) count_of_deals, 
ROW_NUMBER() over(partition by l2, restrict_type order by count_of_deals desc) top_value 
from 
	(select * from (select 
	dd.deal_uuid deal_uuid, 
	dd.country_code country_cd, 
	dd.country_id country_id, 
	dd.lone l1,
	dd.ltwo l2,
	dd.lthree l3, 
	dd.fine_print_c fine_print_c, 
	case when dd.avg_sell_price < 25 then '<25' when dd.avg_sell_price >=25 and dd.avg_sell_price < 50 then '25-50' when dd.avg_sell_price >= 50 then '>=50' else null end avg_sell_price,
	dd.sum_nob sum_nob,
	typ_a.text_res_agg text_res_agg,
	typ_a.restrict_type restrict_type,
	case when typ_a.ind = 1 then 1 else 0 end restriction_seen
	from 
		   (select 
			deal_uuid, 
			country_code,
			country_id, 
			lone, 
			ltwo, 
			lthree, 
			fine_print_c,
			(min_contract_sell + max_contract_sell)/2 avg_sell_price,
			sum_nob
			from 
			sandbox.np_fine_deals where country_code = 'UK') dd
		left join 
			(	select 
				text_res,
				text_res_agg,
				restrict_type,
				1 AS ind
				from 
				sandbox.nvp_time_date_rstr
				) typ_a on REGEXP_SUBSTR(dd.fine_print_c, lower(typ_a.text_res), 1, 1, 'i') = lower(typ_a.text_res)
		WHERE text_res_agg is not null
	) fin 
	UNION
	(select 
	dd.deal_uuid deal_uuid, 
	dd.country_code country_cd, 
	dd.country_id country_id, 
	dd.lone l1, 
	dd.ltwo l2, 
	dd.lthree l3, 
	dd.fine_print_c fine_print_c, 
	case when dd.avg_sell_price < 25 then '<25' when dd.avg_sell_price >=25 and dd.avg_sell_price < 50 then '25-50' when dd.avg_sell_price >= 50 then '>=50' else null end avg_sell_price,
	dd.sum_nob sum_nob,
	ext_time.text_res_agg text_res_agg,
	ext_time.restrict_type restrict_type,
	case when ext_time.ind = 1 then 1 else 0 end restriction_seen
	from 
		   (select 
			deal_uuid, 
			country_code,
			country_id, 
			lone, 
			ltwo, 
			lthree, 
			fine_print_c,
			(min_contract_sell + max_contract_sell)/2 avg_sell_price,
			sum_nob
			from 
			sandbox.np_fine_deals where country_code = 'UK') dd
		left join 
			(select 
				deal_uuid deal_uuid2, 
				fine_print_c, 
				case when REGEXP_SUBSTR(fine_print_c, '(-([0-5])[.:-]\d+pm|-([0-9]+)[.:-]\d+am)') is not null then 'am-pm rest'
								when REGEXP_SUBSTR(fine_print_c, '(-([0-5])pm|-([0-9]+)am)', 1, 1, 'i') is not null then 'am-pm rest'end as text_res_agg,
				case when text_res_agg is not null then 'Time Restriction' end as restrict_type,
				1 as ind
			from sandbox.np_fine_deals where text_res_agg is not null
			) ext_time on dd.deal_uuid = ext_time.deal_uuid2
			WHERE text_res_agg is not null
	)  ) final_ GROUP BY 1,2,3;


---when REGEXP_SUBSTR(fine_print_c, '(-([0-6])[.:-]\d+pm | -([0-6])[.:-]\d+am)', 1, 1, 'i') is not null then 'am-pm rest'
---when REGEXP_SUBSTR(fine_print_c, '(-([0-6])pm | -([0-6])am)', 1, 1, 'i') is not null then 'am-pm rest'



select 
 pm_time1, am_time1, pm_time2, am_time2,
 count(deal_uuid) count_
from 
	(select 
	dd.deal_uuid deal_uuid, 
	dd.country_code country_cd, 
	dd.country_id country_id, 
	dd.lone l1, 
	dd.ltwo l2, 
	dd.lthree l3, 
	dd.fine_print_c fine_print_c, 
	case when dd.avg_sell_price < 25 then '<25' when dd.avg_sell_price >=25 and dd.avg_sell_price < 50 then '25-50' when dd.avg_sell_price >= 50 then '>=50' else null end avg_sell_price,
	dd.sum_nob sum_nob,
	ext_time.pm_time1 pm_time1,
	ext_time.am_time1 am_time1,
	ext_time.pm_time2 pm_time2,
	ext_time.am_time1 am_time2,
	case when ext_time.ind = 1 then 1 else 0 end restriction_seen
	from 
		   (select 
			deal_uuid, 
			country_code,
			country_id, 
			lone, 
			ltwo, 
			lthree, 
			fine_print_c,
			(min_contract_sell + max_contract_sell)/2 avg_sell_price,
			sum_nob
			from 
			sandbox.np_fine_deals where country_code = 'UK' and ltwo = 'L2 - Food & Drink') dd
		inner join 
			(select 
				deal_uuid deal_uuid2, 
				fine_print_c, 
				REGEXP_SUBSTR(fine_print_c, '-([0-5])[.:-]\d+pm', 1, 1, 'i') pm_time1,
				REGEXP_SUBSTR(fine_print_c, '-([0-9]+)[.:-]\d+am', 1, 1, 'i') am_time1,
				REGEXP_SUBSTR(fine_print_c, '-([0-5])pm', 1, 1, 'i') pm_time2,
				REGEXP_SUBSTR(fine_print_c, '-([0-9]+)am', 1, 1, 'i') am_time2,
				1 as ind
			from sandbox.np_fine_deals
			) ext_time on dd.deal_uuid = ext_time.deal_uuid2
	) fin_ group by 1,2,3,4 order by count_ desc;
	
select 
				deal_uuid deal_uuid2, 
				fine_print_c, 
				REGEXP_SUBSTR(fine_print_c, '-([0-5])[.:-]\d+pm', 1, 1, 'i') pm_time1,
				REGEXP_SUBSTR(fine_print_c, '-([0-9]+)[.:-]\d+am', 1, 1, 'i') am_time1,
				REGEXP_SUBSTR(fine_print_c, '-([0-5])pm', 1, 1, 'i') pm_time2,
				REGEXP_SUBSTR(fine_print_c, '-([0-9]+)am', 1, 1, 'i') am_time2,
				case when pm_time1 is null and pm_time2 is null and am_time1 is null and am_time2 is null then 0 else 1 end text_res_agg,
				1 as ind
			from sandbox.np_fine_deals where text_res_agg = 1
			
			