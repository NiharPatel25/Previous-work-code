-- New client restriction intl
---- user_gp.deal_localized_contents  is a source of duplication, since it has same deals in multiple languages

drop table sandbox.nvp_fine_deals_intl;
create MULTISET table sandbox.nvp_fine_deals_intl as
(select a.*, 
	c.fine_print, 
	lower(c.fine_print) fine_print_c, 
	c.locale lang_loc,
	d.min_contract_sell, 
	d.max_contract_sell, 
	d.offer_description, --just min offer desc
	e.sum_nob, 
	e.count_orders, 
	e.udv udv
From 
		(select 
			deal_id as deal_uuid, 
			grt_l1_cat_name as lone, 
			grt_l2_cat_name as ltwo, 
			grt_l3_cat_name as lthree,
			min(country_code) as country_code,  
			min(country_id) as country_id,
			count(deal_id) as count_
		from user_edwprod.dim_gbl_deal_lob where grt_l1_cat_name = 'L1 - Local' group by 1,2,3,4 having count_ = 1) as a
	inner join 
		(select 
			distinct deal_uuid deal_uuid1, 
			case when country_code = 'GB' then 'UK' when country_code = 'QC' then 'CA' else country_code end country_code
		from 
			user_groupondw.active_deals 
			where load_date = CURRENT_DATE - 2 and available_qty > 0 and sold_out = 'false' 
			group by deal_uuid, country_code
		) as b on b.deal_uuid1 = a.deal_uuid and a.country_code = b.country_code
	left join 
		(select 
			deal_uuid deal_uuid2, 
			locale, 
			fine_print 
		from user_gp.deal_localized_contents 
		) as c on a.deal_uuid = c.deal_uuid2
	left join
		(select product_uuid,
				min(contract_sell_price) min_contract_sell,
				max(contract_sell_price) max_contract_sell,
				min(offer_description) offer_description
		from user_edwprod.dim_offer_ext 
		group by product_uuid) as d on a.deal_uuid = d.product_uuid
	left join
		(select 	country_code,
					deal_id deal_uuid, 
					sum(uniq_deal_views) udv,
					sum(nob_USD) sum_nob,
					sum(transactions) count_orders
			from
					user_edwprod.agg_gbl_traffic_fin_deal
					where report_date >= CURRENT_DATE - 90
					group by deal_uuid, country_code
		) as e on e.deal_uuid = a.deal_uuid and e.country_code = a.country_code
) with data;

/*	left join
		(select 
					x.deal_uuid deal_uuid, 
					y.country_iso_code_2 country_code,
					count(parent_order_uuid) count_orders,
					sum(x.auth_nob_loc) sum_nob
			from
					user_edwprod.fact_gbl_transactions x
					left join user_groupondw.gbl_dim_country y on x.country_id = y.country_key
					where x.order_date >= CURRENT_DATE - 90 and x.action = 'authorize'
					group by deal_uuid, country_code
		) as e on e.deal_uuid = a.deal_uuid and e.country_code = a.country_code
	left join 
		(select 
			deal_id, 
			country_code,
			sum(uniq_deal_views) udv
		from user_edwprod.agg_gbl_traffic_deal 
		where report_date >= CURRENT_DATE - 90
		group by deal_id, country_code
		) as f on f.deal_id = a.deal_uuid and f.country_code = a.country_code*/

select * from sandbox.nvp_fine_deals_intl2;----- make sure to look at this if you update it
drop table sandbox.nvp_fine_deals_intl2;
create MULTISET table sandbox.nvp_fine_deals_intl2 as
(select a.*, 
	c.fine_print, 
	lower(c.fine_print) fine_print_c, 
	c.locale lang_loc,
	d.min_contract_sell, 
	d.max_contract_sell, 
	d.offer_description, --just min offer desc
	e.sum_nob, 
	e.count_orders, 
	e.udv udv
From 
		(select 
			deal_id as deal_uuid, 
			grt_l1_cat_name as lone, 
			grt_l2_cat_name as ltwo, 
			grt_l3_cat_name as lthree,
			min(country_code) as country_code,  
			min(country_id) as country_id
		from user_edwprod.dim_gbl_deal_lob where grt_l1_cat_name = 'L1 - Local' group by 1,2,3,4) as a
	inner join 
		(select 
			distinct deal_uuid deal_uuid1, 
			case when country_code = 'GB' then 'UK' when country_code = 'QC' then 'CA' else country_code end country_code
		from 
			user_groupondw.active_deals 
			where load_date = '2020-04-01' and available_qty > 0 and sold_out = 'false' 
			group by deal_uuid, country_code
		) as b on b.deal_uuid1 = a.deal_uuid and a.country_code = b.country_code
	left join 
		(select 
			deal_uuid deal_uuid2, 
			locale, 
			fine_print 
		from user_gp.deal_localized_contents 
		) as c on a.deal_uuid = c.deal_uuid2
	left join
		(select product_uuid,
				min(contract_sell_price) min_contract_sell,
				max(contract_sell_price) max_contract_sell,
				min(offer_description) offer_description
		from user_edwprod.dim_offer_ext 
		group by product_uuid) as d on a.deal_uuid = d.product_uuid
	left join
		(select 	country_code,
					deal_id deal_uuid, 
					sum(uniq_deal_views) udv,
					sum(nob_USD) sum_nob,
					sum(transactions) count_orders
			from
					user_edwprod.agg_gbl_traffic_fin_deal
					where report_date >= cast('2020-04-01' as date) - 90 and report_date <= cast('2020-04-01' as date)
					group by deal_uuid, country_code
		) as e on e.deal_uuid = a.deal_uuid and e.country_code = a.country_code
) with data;




---Always make sure we have consistent languages and locations. and no errors are seen here.
select country_code, lang_loc, count (distinct deal_uuid), count(deal_uuid) from sandbox.nvp_fine_deals_intl group by country_code, lang_loc order by country_code; 
select count(deal_uuid) from sandbox.nvp_fine_deals_intl;


-----------------Restriction data

drop table sandbox.nvp_restriction_intl;

create multiset table sandbox.nvp_restriction_intl
(text_res varchar(100) character set unicode, 
text_res_eng_exact_translation varchar(100) character set unicode,
text_l3 varchar(100) character set unicode, 
text_l2 varchar(100) character set unicode,
text_l1 varchar(100) character set unicode,
restriction_type varchar(100) character set unicode,
country varchar(100) character set unicode
);


-----------------------------------------Inserting time restriction in the restrictions table



SELECT * FROM sandbox.nvp_restriction_intl;

insert into sandbox.nvp_restriction_intl
select * from 
(select
     distinct 
         case when country_code = 'BE' then REGEXP_SUBSTR(fine_print_c,'([0-1][0-7]h([0-9]+)?|([0-1][0-7])[.:-]\d+heures|tot.?([0-1][0-7])[.:-]\d+.?uur|tot.?[0-1][0-7].?u|tot.?([0-9])u)' , 1, 1, 'i') 
          when country_code = 'FR' then REGEXP_SUBSTR(fine_print_c,'([0-1][0-7]h([0-9]+)?|([0-1][0-7])[.:-]\d+heures)' , 1, 1, 'i')
          when country_code = 'NL' then REGEXP_SUBSTR(fine_print_c,'(tot.?([0-1][0-7])[.:-]\d+.?uur|tot.?[0-1][0-7].?u|tot.?([0-9])u)' , 1, 1, 'i')
          when country_code = 'IT' then REGEXP_SUBSTR(fine_print_c, '(alle [0-1][0-7][.:-]\d+|alle [0-9][:-]\d+|-[0-1][0-7][.:-]\d+)', 1, 1, 'i')
          when country_code = 'UK' then REGEXP_SUBSTR(fine_print_c, '(-([0-5])[.:-]\d+pm|-([0-9]+)[.:-]\d+am|-([0-5])pm|-([0-9]+)am)', 1, 1, 'i')
          when country_code = 'US' then REGEXP_SUBSTR(fine_print_c, '(-([0-5])[.:-]\d+pm|-([0-9]+)[.:-]\d+am|-([0-5])pm|-([0-9]+)am)', 1, 1, 'i')
          when country_code = 'CA' then REGEXP_SUBSTR(fine_print_c, '(-([0-5])[.:-]\d+pm|-([0-9]+)[.:-]\d+am|-([0-5])pm|-([0-9]+)am|[0-1][0-7]h([0-9]+)?|([0-1][0-7])[.:-]\d+heures)', 1, 1, 'i')
          when country_code = 'AE' then REGEXP_SUBSTR(fine_print_c, '(-([0-5])[.:-]\d+pm|-([0-9]+)[.:-]\d+am|-([0-5])pm|-([0-9]+)am)', 1, 1, 'i')
          when country_code = 'IE' then REGEXP_SUBSTR(fine_print_c, '(-([0-5])[.:-]\d+pm|-([0-9]+)[.:-]\d+am|-([0-5])pm|-([0-9]+)am)', 1, 1, 'i')
          when country_code = 'AU' then REGEXP_SUBSTR(fine_print_c, '(-([0-5])[.:-]\d+pm|-([0-9]+)[.:-]\d+am|-([0-5])pm|-([0-9]+)am)', 1, 1, 'i')
          when country_code = 'NZ' then REGEXP_SUBSTR(fine_print_c, '(-([0-5])[.:-]\d+pm|-([0-9]+)[.:-]\d+am|-([0-5])pm|-([0-9]+)am)', 1, 1, 'i')
          when country_code = 'DE' then REGEXP_SUBSTR(fine_print_c, '(-([0-1][0-7])[.:-]\d+.?uhr|-([7-9]).?uhr)', 1, 1, 'i')
          when country_code = 'PL' then REGEXP_SUBSTR(fine_print_c, '(godzinach.?[0-9]+[-][0-1][0-7]|godzinach.?[0-9]+[:][0-9]+[-][0-1][0-7]|do [0-1][0-7][:-]..|-[0-1][0-7][:][0-9]+)', 1, 1, 'i')
          when country_code = 'ES' then REGEXP_SUBSTR(fine_print_c, '(-[0-1][0-7]h|-[0-9]h)', 1, 1, 'i') end as text_res,
      case when text_res is not null then text_res end as text_res_eng_exact_translation,
      case when text_res is not null then 'AM-PM restriction' end as text_l3,
      'AM-PM restriction' as text_l2,
      'Time Restriction' as text_l1,
      'Time Restrictions' as restriction_type, 
      country_code as country
from sandbox.nvp_fine_deals_intl
where text_res is not null) fin;

select distinct * from sandbox.nvp_restriction_intl where country = 'BE' ORDER BY text_res;

select distinct REGEXP_SUBSTR(fine_print_c,'([0-1][0-7]h([0-9]+)?|([0-1][0-7])[.:-]\d+heures)' , 1, 1, 'i') 
from sandbox.nvp_fine_deals_intl 
where country_code = 'FR';

grant select on sandbox.nvp_fine_deals_intl to public;

insert into sandbox.nvp_restriction_intl
select * from 
(select
		distinct 
			case when country_code = 'BE' then REGEXP_SUBSTR(fine_print_c,'([0-1][0-7]h([0-9]+)?|([0-1][0-7])[.:-]\d+heures|tot.?([0-1][0-7])[.:-]\d+.?uur|tot.?[0-1][0-7].?u|tot.?([0-9])u)' , 1, 1, 'i') 
			when country_code = 'FR' then REGEXP_SUBSTR(fine_print_c,'([0-1][0-7]h([0-9]+)?|([0-1][0-7])[.:-]\d+heures)' , 1, 1, 'i')
			when country_code = 'NL' then REGEXP_SUBSTR(fine_print_c,'(tot.?([0-1][0-7])[.:-]\d+.?uur|tot.?[0-1][0-7].?u|tot.?([0-9])u)' , 1, 1, 'i')
			when country_code = 'IT' then REGEXP_SUBSTR(fine_print_c, '(alle [0-1][0-7][.:-]\d+|alle [0-9][:-]\d+|-[0-1][0-7][.:-]\d+)', 1, 1, 'i')
			when country_code = 'UK' then REGEXP_SUBSTR(fine_print_c, '(-([0-5])[.:-]\d+pm|-([0-9]+)[.:-]\d+am|-([0-5])pm|-([0-9]+)am)', 1, 1, 'i')
			when country_code = 'US' then REGEXP_SUBSTR(fine_print_c, '(-([0-5])[.:-]\d+pm|-([0-9]+)[.:-]\d+am|-([0-5])pm|-([0-9]+)am)', 1, 1, 'i')
			when country_code = 'CA' then REGEXP_SUBSTR(fine_print_c, '(-([0-5])[.:-]\d+pm|-([0-9]+)[.:-]\d+am|-([0-5])pm|-([0-9]+)am|[0-1][0-7]h([0-9]+)?|([0-1][0-7])[.:-]\d+heures)', 1, 1, 'i')
			when country_code = 'AE' then REGEXP_SUBSTR(fine_print_c, '(-([0-5])[.:-]\d+pm|-([0-9]+)[.:-]\d+am|-([0-5])pm|-([0-9]+)am)', 1, 1, 'i')
			when country_code = 'IE' then REGEXP_SUBSTR(fine_print_c, '(-([0-5])[.:-]\d+pm|-([0-9]+)[.:-]\d+am|-([0-5])pm|-([0-9]+)am)', 1, 1, 'i')
			when country_code = 'AU' then REGEXP_SUBSTR(fine_print_c, '(-([0-5])[.:-]\d+pm|-([0-9]+)[.:-]\d+am|-([0-5])pm|-([0-9]+)am)', 1, 1, 'i')
			when country_code = 'NZ' then REGEXP_SUBSTR(fine_print_c, '(-([0-5])[.:-]\d+pm|-([0-9]+)[.:-]\d+am|-([0-5])pm|-([0-9]+)am)', 1, 1, 'i')
			when country_code = 'DE' then REGEXP_SUBSTR(fine_print_c, '(-([0-1][0-7])[.:-]\d+.?uhr|-([7-9]).?uhr)', 1, 1, 'i')
			when country_code = 'PL' then REGEXP_SUBSTR(fine_print_c, '(godzinach.?[0-9]+[-][0-1][0-7]|godzinach.?[0-9]+[:][0-9]+[-][0-1][0-7]|do [0-1][0-7][:-]..|-[0-1][0-7][:][0-9]+)', 1, 1, 'i')
			when country_code = 'ES' then REGEXP_SUBSTR(fine_print_c, '(-[0-1][0-7]h|-[0-9]h)', 1, 1, 'i') end as text_res,
		case when text_res is not null then text_res end as text_res_eng_exact_translation,
		case when text_res is not null then 'AM-PM restriction' end as text_l3, 
		'AM-PM restriction' as text_l2, 
		'Time Restriction' as text_l1, 
		'Time Restrictions' as restriction_type, 
		country_code as country
from sandbox.nvp_fine_deals_intl
where text_res is not null) fin

SELECT * FROM sandbox.nvp_fine_deals_intl;


--- EN: (-([0-5])[.:-]\d+pm|-([0-9]+)[.:-]\d+am|-([0-5])pm|-([0-9]+)am)
--- FR:[0-1][0-7]h([0-9]+)?|([0-1][0-7])[.:-]\d+heures   more accuracy for french of a^ is figured out
---NL: tot.?([0-1][0-7])[.:-]\d+.?uur | tot.?[0-1][0-7].?u| tot.?([0-9])u suffice for now check for belguim
---DE: -([0-1][0-7])[.:-]\d+.?uhr|-([7-9]).?uhr
---BE Improvments : |[^0-9][0-9]h([0-9]+)?
---PL: (godzinach.?[0-9]+[-][0-1][0-7]|godzinach.?[0-9]+[:][0-9]+[-][0-1][0-7]|do [0-1][0-7][:-]..|-[0-1][0-7][:][0-9]+)
----IT: (alle [0-1][0-7][.:-]\d+|alle [0-9][:-]\d+|-[0-1][0-7][.:-]\d+)
----ES: (-[0-1][0-7]h|-[0-9]h)
---- Only japanese remaining


select distinct restriction_type from sandbox.nvp_restriction_intl

---------------------------
select count(deal_uuid), count(distinct deal_uuid) from sandbox.nvp_fineprint_dash_sum;



select * from sandbox.nvp_fineprint_dash_sum;
drop table sandbox.nvp_fineprint_dash_sum;
create MULTISET table sandbox.nvp_fineprint_dash_sum as(
select deal_uuid, 
	   country_cd, 
	   country_id, 
	   l1 Deal_L1,
	   l2 Deal_L2,
	   l3 Deal_L3,
	   avg_sell_price, 
	   sum_nob, 
	   count_orders, 
	   udv, 
	   max(day_date_restriction) day_date_rest, 
	   max(holiday_restriction) holiday_rest, 
	   max(time_restriction) time_rest, 
	   max(new_client_restriction) new_client_rest, 
	   max(appoint_restriction) res_apt_rest, 
	   max(menu_service_restriction) menu_serv_rest, 
	   max(additional_fees) add_fees_rest, 
	   case when day_date_rest = 1 or  holiday_rest = 1 or time_rest = 1 or new_client_rest = 1 or res_apt_rest = 1 or menu_serv_rest = 1 or add_fees_rest = 1 then 'with restriction' else 'w/o restriction' END union_case 
from 
	   (select 
			dd.deal_uuid deal_uuid, 
			dd.country_code country_cd, 
			dd.country_id country_id, 
			dd.lone l1, 
			dd.ltwo l2, 
			dd.lthree l3, 
			dd.fine_print_c fine_print_c, 
			dd.offer_description,
			case when dd.avg_sell_price < 25 then '<25' when dd.avg_sell_price >=25 and dd.avg_sell_price < 50 then '25-50' when dd.avg_sell_price >= 50 then '>=50' else null end avg_sell_price,
			dd.sum_nob sum_nob,
			dd.count_orders count_orders,
			dd.udv udv,
			case when typ_a.ind = 1 then 1 else 0 end day_date_restriction, 
			case when typ_b.ind = 1 then 1 else 0 end holiday_restriction,
			case when typ_c.ind = 1 then 1 else 0 end time_restriction,
			case when typ_d.ind = 1 then 1 else 0 end new_client_restriction,
			case when typ_e.ind = 1 then 1 else 0 end appoint_restriction,
			case when typ_f.ind = 1 then 1 else 0 end menu_service_restriction,
			case when typ_g.ind = 1 then 1 else 0 end additional_fees
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
						offer_description,
						sum_nob,
						count_orders, 
						udv
						from 
						sandbox.nvp_fine_deals_intl) dd
				left join 
					(	select 
						text_res,
						text_l3,
						text_l2,
						text_l1,
						restriction_type,
						country,
						1 AS ind
						from 
						sandbox.nvp_restriction_intl where restriction_type = 'Date/Day Restrictions'
						) typ_a on REGEXP_SUBSTR(dd.fine_print_c, lower(typ_a.text_res), 1, 1, 'i') = lower(typ_a.text_res) and dd.country_code = typ_a.country
				left join 
					( select 
						text_res,
						text_l3,
						text_l2,
						text_l1,
						restriction_type,
						country,
						1 AS ind
						from 
						sandbox.nvp_restriction_intl where restriction_type = 'Holiday Restrictions'
					) typ_b on REGEXP_SUBSTR(dd.fine_print_c, lower(typ_b.text_res), 1, 1, 'i') = lower(typ_b.text_res) and dd.country_code = typ_b.country
				left join
					(
					select 
						text_res,
						text_l3,
						text_l2,
						text_l1,
						restriction_type,
						country,
						1 AS ind
						from 
						sandbox.nvp_restriction_intl where restriction_type = 'Time Restrictions'
					) typ_c on REGEXP_SUBSTR(dd.fine_print_c, lower(typ_c.text_res), 1, 1, 'i') = lower(typ_c.text_res) and dd.country_code = typ_c.country
				left join
					(
					select 
						text_res,
						text_l3,
						text_l2,
						text_l1,
						restriction_type,
						country,
						1 AS ind
						from 
						sandbox.nvp_restriction_intl where restriction_type = 'New Clients Restrictions'
					) typ_d on REGEXP_SUBSTR(dd.fine_print_c, lower(typ_d.text_res), 1, 1, 'i') = lower(typ_d.text_res) and dd.country_code = typ_d.country
				left join 
					(
					select 
						text_res,
						text_l3,
						text_l2,
						text_l1,
						restriction_type,
						country,
						1 AS ind
						from 
						sandbox.nvp_restriction_intl where restriction_type = 'Reservations/Appointment Needed'
					) typ_e on REGEXP_SUBSTR(dd.fine_print_c, lower(typ_e.text_res), 1, 1, 'i') = lower(typ_e.text_res) and dd.country_code = typ_e.country
				left join 
					(
					select 
						text_res,
						text_l3,
						text_l2,
						text_l1,
						restriction_type,
						country,
						1 AS ind
						from 
						sandbox.nvp_restriction_intl where restriction_type = 'Menu/Service restrictions'
					) typ_f on REGEXP_SUBSTR(dd.fine_print_c, lower(typ_f.text_res), 1, 1, 'i') = lower(typ_f.text_res) and dd.country_code = typ_f.country
				left join 
					(
					select 
						text_res,
						text_l3,
						text_l2,
						text_l1,
						restriction_type,
						country,
						1 AS ind
						from 
						sandbox.nvp_restriction_intl where restriction_type = 'Additional Fees'
					) typ_g on REGEXP_SUBSTR(dd.fine_print_c, lower(typ_g.text_res), 1, 1, 'i') = lower(typ_g.text_res) and dd.country_code = typ_g.country) as fin_ group by 1,2,3,4,5,6,7,8,9,10
) with data;
grant select on sandbox.nvp_fineprint_dash_sum to public;



-----INPUT Table 2: 
grant select on sandbox.nvp_fineprint_dash_in4 to public;

drop table sandbox.nvp_fineprint_dash_in4;
create MULTISET table sandbox.nvp_fineprint_dash_in4 as(
select 
	distinct
	dd.deal_uuid deal_uuid, 
	dd.country_code country_cd, 
	dd.country_id country_id, 
	dd.lone Deal_L1,
	dd.ltwo Deal_L2,
	dd.lthree Deal_L3, 
	case when dd.avg_sell_price < 25 then '<25' when dd.avg_sell_price >=25 and dd.avg_sell_price < 50 then '25-50' when dd.avg_sell_price >= 50 then '>=50' else null end avg_sell_price,
	dd.sum_nob sum_nob,
	typ_a.restriction_type restriction_type,
	typ_a.text_l1 text_l1,
	typ_a.text_l2 text_l2,
	typ_a.text_l3 text_l3
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
					offer_description,
					sum_nob,
					count_orders, 
					udv
			from 
					sandbox.nvp_fine_deals_intl) dd
		left join 
			( select 
						text_res,
						text_l3,
						text_l2,
						text_l1,
						restriction_type,
						country,
						1 AS ind
						from 
						sandbox.nvp_restriction_intl
					) typ_a on REGEXP_SUBSTR(dd.fine_print_c, lower(typ_a.text_res), 1, 1, 'i') = lower(typ_a.text_res) and dd.country_code = typ_a.country
	) with data;

select * from sandbox.nvp_fineprint_dash_in4 where text_l1 = 'Menu restriction';
-------TRASH: 
select * from sandbox.nvp_fineprint_dash_in;
drop table sandbox.nvp_fineprint_dash_in;
create MULTISET table sandbox.nvp_fineprint_dash_in as(
select 
			dd.deal_uuid deal_uuid, 
			dd.country_code country_cd, 
			dd.country_id country_id, 
			dd.lone l1, 
			dd.ltwo l2, 
			dd.lthree l3, 
			dd.fine_print_c fine_print_c, 
			dd.offer_description,
			case when dd.avg_sell_price < 25 then '<25' when dd.avg_sell_price >=25 and dd.avg_sell_price < 50 then '25-50' when dd.avg_sell_price >= 50 then '>=50' else null end avg_sell_price,
			dd.sum_nob sum_nob,
			dd.count_orders count_orders,
			dd.udv udv,
			typ_a.text_res day_date_res,
			typ_a.text_l3 day_date_l3,
			typ_a.text_l2 day_date_l2,
			typ_a.text_l1 day_date_l1,
			typ_b.text_res holiday_res,
			typ_b.text_l3 holiday_l3,
			typ_b.text_l2 holiday_l2,
			typ_b.text_l1 holiday_l1,
			typ_c.text_res time_res,
			typ_c.text_l3 time_rest_l3,
			typ_c.text_l2 time_rest_l2,
			typ_c.text_l1 time_rest_l1,
			typ_d.text_res new_client_res,
			typ_d.text_l3 new_client_l3,
			typ_d.text_l2 new_client_l2,
			typ_d.text_l1 new_client_l1,
			typ_e.text_res reservation_apt_res,
			typ_e.text_l3 reservation_l3,
			typ_e.text_l2 reservation_l2,
			typ_e.text_l1 reservation_l1,
			typ_f.text_res menu_res,
			typ_f.text_l3 menu_l3,
			typ_f.text_l2 menu_l2,
			typ_f.text_l1 menu_l1,
			typ_g.text_res add_fees_res,
			typ_g.text_l3 add_fees_l3,
			typ_g.text_l2 add_fees_l2,
			typ_g.text_l1 add_fees_l1,
			case when typ_a.ind = 1 then 1 else 0 end day_date_restriction, 
			case when typ_b.ind = 1 then 1 else 0 end holiday_restriction,
			case when typ_c.ind = 1 then 1 else 0 end time_restriction,
			case when typ_d.ind = 1 then 1 else 0 end new_client_restriction,
			case when typ_e.ind = 1 then 1 else 0 end reservation_apt_restriction,
			case when typ_f.ind = 1 then 1 else 0 end menu_service_restriction,
			case when typ_g.ind = 1 then 1 else 0 end additional_fees
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
						offer_description,
						sum_nob,
						count_orders, 
						udv
						from 
						sandbox.nvp_fine_deals_intl) dd
				left join 
					(	select 
						text_res,
						text_l3,
						text_l2,
						text_l1,
						restriction_type,
						country,
						1 AS ind
						from 
						sandbox.nvp_restriction_intl where restriction_type = 'Date/Day Restrictions'
						) typ_a on REGEXP_SUBSTR(dd.fine_print_c, lower(typ_a.text_res), 1, 1, 'i') = lower(typ_a.text_res) and dd.country_code = typ_a.country
				left join 
					( select 
						text_res,
						text_l3,
						text_l2,
						text_l1,
						restriction_type,
						country,
						1 AS ind
						from 
						sandbox.nvp_restriction_intl where restriction_type = 'Holiday Restrictions'
					) typ_b on REGEXP_SUBSTR(dd.fine_print_c, lower(typ_b.text_res), 1, 1, 'i') = lower(typ_b.text_res) and dd.country_code = typ_b.country
				left join
					(
					select 
						text_res,
						text_l3,
						text_l2,
						text_l1,
						restriction_type,
						country,
						1 AS ind
						from 
						sandbox.nvp_restriction_intl where restriction_type = 'Time Restrictions'
					) typ_c on REGEXP_SUBSTR(dd.fine_print_c, lower(typ_c.text_res), 1, 1, 'i') = lower(typ_c.text_res) and dd.country_code = typ_c.country
				left join
					(
					select 
						text_res,
						text_l3,
						text_l2,
						text_l1,
						restriction_type,
						country,
						1 AS ind
						from 
						sandbox.nvp_restriction_intl where restriction_type = 'New Clients Restrictions'
					) typ_d on REGEXP_SUBSTR(dd.fine_print_c, lower(typ_d.text_res), 1, 1, 'i') = lower(typ_d.text_res) and dd.country_code = typ_d.country
				left join 
					(
					select 
						text_res,
						text_l3,
						text_l2,
						text_l1,
						restriction_type,
						country,
						1 AS ind
						from 
						sandbox.nvp_restriction_intl where restriction_type = 'Reservations/Appointment Needed'
					) typ_e on REGEXP_SUBSTR(dd.fine_print_c, lower(typ_e.text_res), 1, 1, 'i') = lower(typ_e.text_res) and dd.country_code = typ_e.country
				left join 
					(
					select 
						text_res,
						text_l3,
						text_l2,
						text_l1,
						restriction_type,
						country,
						1 AS ind
						from 
						sandbox.nvp_restriction_intl where restriction_type = 'Menu/Service restrictions'
					) typ_f on REGEXP_SUBSTR(dd.fine_print_c, lower(typ_f.text_res), 1, 1, 'i') = lower(typ_f.text_res) and dd.country_code = typ_f.country
				left join 
					(
					select 
						text_res,
						text_l3,
						text_l2,
						text_l1,
						restriction_type,
						country,
						1 AS ind
						from 
						sandbox.nvp_restriction_intl where restriction_type = 'Additional Fees'
					) typ_g on REGEXP_SUBSTR(dd.fine_print_c, lower(typ_g.text_res), 1, 1, 'i') = lower(typ_g.text_res) and dd.country_code = typ_g.country
) with data;
----VENN Diagram
--------------
----1: Day and date only; 2: Holiday Only, 3: Time Only 4: day_and_holiday 5: day_and_time 6: day_and_time 7: holiday_and_time

DROP TABLE sandbox.nvp_bt_circle;
CREATE MULTISET TABLE  sandbox.nvp_bt_circle
  (country_cd varchar(10),
   point int,
   circle int
   );


INSERT INTO sandbox.nvp_bt_circle VALUES ('AE',8, NULL);
INSERT INTO sandbox.nvp_bt_circle VALUES ('AE',9, NULL);
INSERT INTO sandbox.nvp_bt_circle VALUES ('AE',10, NULL);
INSERT INTO sandbox.nvp_bt_circle VALUES ('DE',8, NULL);
INSERT INTO sandbox.nvp_bt_circle VALUES ('DE',9, NULL);
INSERT INTO sandbox.nvp_bt_circle VALUES ('DE',10, NULL);
INSERT INTO sandbox.nvp_bt_circle VALUES ('PL',8, NULL);
INSERT INTO sandbox.nvp_bt_circle VALUES ('PL',9, NULL);
INSERT INTO sandbox.nvp_bt_circle VALUES ('PL',10, NULL);
INSERT INTO sandbox.nvp_bt_circle VALUES ('BE',8, NULL);
INSERT INTO sandbox.nvp_bt_circle VALUES ('BE',9, NULL);
INSERT INTO sandbox.nvp_bt_circle VALUES ('BE',10, NULL);
INSERT INTO sandbox.nvp_bt_circle VALUES ('FR',8, NULL);
INSERT INTO sandbox.nvp_bt_circle VALUES ('FR',9, NULL);
INSERT INTO sandbox.nvp_bt_circle VALUES ('FR',10, NULL);
INSERT INTO sandbox.nvp_bt_circle VALUES ('NZ',8, NULL);
INSERT INTO sandbox.nvp_bt_circle VALUES ('NZ',9, NULL);
INSERT INTO sandbox.nvp_bt_circle VALUES ('NZ',10, NULL);
INSERT INTO sandbox.nvp_bt_circle VALUES ('IE',8, NULL);
INSERT INTO sandbox.nvp_bt_circle VALUES ('IE',9, NULL);
INSERT INTO sandbox.nvp_bt_circle VALUES ('IE',10, NULL);
INSERT INTO sandbox.nvp_bt_circle VALUES ('UK',8, NULL);
INSERT INTO sandbox.nvp_bt_circle VALUES ('UK',9, NULL);
INSERT INTO sandbox.nvp_bt_circle VALUES ('UK',10, NULL);
INSERT INTO sandbox.nvp_bt_circle VALUES ('JP',8, NULL);
INSERT INTO sandbox.nvp_bt_circle VALUES ('JP',9, NULL);
INSERT INTO sandbox.nvp_bt_circle VALUES ('JP',10, NULL);
INSERT INTO sandbox.nvp_bt_circle VALUES ('IT',8, NULL);
INSERT INTO sandbox.nvp_bt_circle VALUES ('IT',9, NULL);
INSERT INTO sandbox.nvp_bt_circle VALUES ('IT',10, NULL);
INSERT INTO sandbox.nvp_bt_circle VALUES ('NL',8, NULL);
INSERT INTO sandbox.nvp_bt_circle VALUES ('NL',9, NULL);
INSERT INTO sandbox.nvp_bt_circle VALUES ('NL',10, NULL); 
INSERT INTO sandbox.nvp_bt_circle VALUES ('ES',8, NULL);
INSERT INTO sandbox.nvp_bt_circle VALUES ('ES',9, NULL);
INSERT INTO sandbox.nvp_bt_circle VALUES ('ES',10, NULL);
INSERT INTO sandbox.nvp_bt_circle VALUES ('AU',8, NULL);
INSERT INTO sandbox.nvp_bt_circle VALUES ('AU',9, NULL);
INSERT INTO sandbox.nvp_bt_circle VALUES ('AU',10, NULL);
INSERT INTO sandbox.nvp_bt_circle VALUES ('US',8, NULL);
INSERT INTO sandbox.nvp_bt_circle VALUES ('US',9, NULL);
INSERT INTO sandbox.nvp_bt_circle VALUES ('US',10, NULL);
INSERT INTO sandbox.nvp_bt_circle VALUES ('CA',8, NULL);
INSERT INTO sandbox.nvp_bt_circle VALUES ('CA',9, NULL);
INSERT INTO sandbox.nvp_bt_circle VALUES ('CA',10, NULL);

 
drop table sandbox.nvp_fineprint_dash_in3;
select * from sandbox.nvp_fineprint_dash_in3;
create MULTISET table sandbox.nvp_fineprint_dash_in3 as(
select * from 
	(select country_cd, 
		   point_restriction, 
		   count(distinct deal_uuid) count_distinct_deal from 
		(select 
			deal_uuid, 
			country_cd, 
			max(day_date_restriction) day_date_restriction_a,
			max(holiday_restriction) holiday_restriction_a,
			max(time_restriction) time_restriction_a,
			max(new_client_restriction) new_client_restriction_a, 
			case when day_date_restriction_a = 1 or holiday_restriction_a = 1 or time_restriction_a = 1 or new_client_restriction_a = 1 then 1 else 0 end union_case,
			case when day_date_restriction_a = 1 and holiday_restriction_a <> 1 and time_restriction_a <> 1 then 1 
				 when day_date_restriction_a = 1 and holiday_restriction_a <> 1 and time_restriction_a = 1 then 2
				 when day_date_restriction_a <> 1 and holiday_restriction_a <> 1 and time_restriction_a = 1 then 3
				 when day_date_restriction_a <> 1 and holiday_restriction_a = 1 and time_restriction_a = 1 then 4
				 when day_date_restriction_a <> 1 and holiday_restriction_a = 1 and time_restriction_a <> 1 then 5
				 when day_date_restriction_a = 1 and holiday_restriction_a = 1 and time_restriction_a <> 1 then 6
				 when day_date_restriction_a = 1 and holiday_restriction_a = 1 and time_restriction_a = 1  then 7
				 END point_restriction
			from 
		(select 
					dd.deal_uuid deal_uuid, 
					dd.country_code country_cd, 
					dd.country_id country_id, 
					dd.lone l1, 
					dd.ltwo l2, 
					dd.lthree l3, 
					dd.fine_print_c fine_print_c, 
					dd.offer_description,
					case when dd.avg_sell_price < 25 then '<25' when dd.avg_sell_price >=25 and dd.avg_sell_price < 50 then '25-50' when dd.avg_sell_price >= 50 then '>=50' else null end avg_sell_price,
					dd.sum_nob sum_nob,
					dd.count_orders count_orders,
					dd.udv udv,
					case when typ_a.ind = 1 then 1 else 0 end day_date_restriction, 
					case when typ_b.ind = 1 then 1 else 0 end holiday_restriction,
					case when typ_c.ind = 1 then 1 else 0 end time_restriction,
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
								offer_description,
								sum_nob,
								count_orders, 
								udv
								from 
								sandbox.nvp_fine_deals_intl) dd
						left join 
							(	select 
								text_res,
								text_l3,
								text_l2,
								text_l1,
								restriction_type,
								country,
								1 AS ind
								from 
								sandbox.nvp_restriction_intl where restriction_type = 'Date/Day Restrictions'
								) typ_a on REGEXP_SUBSTR(dd.fine_print_c, lower(typ_a.text_res), 1, 1, 'i') = lower(typ_a.text_res) and dd.country_code = typ_a.country
						left join 
							( select 
								text_res,
								text_l3,
								text_l2,
								text_l1,
								restriction_type,
								country,
								1 AS ind
								from 
								sandbox.nvp_restriction_intl where restriction_type = 'Holiday Restrictions'
							) typ_b on REGEXP_SUBSTR(dd.fine_print_c, lower(typ_b.text_res), 1, 1, 'i') = lower(typ_b.text_res) and dd.country_code = typ_b.country
						left join
							(
							select 
								text_res,
								text_l3,
								text_l2,
								text_l1,
								restriction_type,
								country,
								1 AS ind
								from 
								sandbox.nvp_restriction_intl where restriction_type = 'Time Restrictions'
							) typ_c on REGEXP_SUBSTR(dd.fine_print_c, lower(typ_c.text_res), 1, 1, 'i') = lower(typ_c.text_res) and dd.country_code = typ_c.country
						left join
							(
							select 
								text_res,
								text_l3,
								text_l2,
								text_l1,
								restriction_type,
								country,
								1 AS ind
								from 
								sandbox.nvp_restriction_intl where restriction_type = 'New Clients Restrictions'
							) typ_d on REGEXP_SUBSTR(dd.fine_print_c, lower(typ_d.text_res), 1, 1, 'i') = lower(typ_d.text_res) and dd.country_code = typ_d.country
			) fin group by 1,2) fin_ where point_restriction is not null group by 1,2) final_
UNION 
SELECT * FROM sandbox.nvp_bt_circle
)with data;


-----------



/*
select * from sandbox.nvp_fineprint_dash_in2;
create MULTISET table sandbox.nvp_fineprint_dash_in2 as;


select fin_.*,
	   ROW_NUMBER() over(partition by country_cd order by sum_nob) order_
	  from 
		(select 
			deal_uuid, 
			country_cd, 
			sum_nob,
			max(day_date_restriction) day_date_restriction_a,
			max(holiday_restriction) holiday_restriction_a,
			max(time_restriction) time_restriction_a,
			max(new_client_restriction) new_client_restriction_a, 
			case when day_date_restriction_a = 1 or holiday_restriction_a = 1 or time_restriction_a = 1 or new_client_restriction_a = 1 then 1 else 0 end union_case,
			case when day_date_restriction_a = 1 and holiday_restriction_a <> 1 and time_restriction_a <> 1 then 'Day/Date Restriction Only' 
				 when day_date_restriction_a = 1 and holiday_restriction_a <> 1 and time_restriction_a = 1 then 'Day/Date and Time Restriction'
				 when day_date_restriction_a <> 1 and holiday_restriction_a <> 1 and time_restriction_a = 1 then 'Time Restriction Only'
				 when day_date_restriction_a <> 1 and holiday_restriction_a = 1 and time_restriction_a = 1 then 'Time And Holiday Restriction'
				 when day_date_restriction_a <> 1 and holiday_restriction_a = 1 and time_restriction_a <> 1 then 'Holiday Restriction Only'
				 when day_date_restriction_a = 1 and holiday_restriction_a = 1 and time_restriction_a <> 1 then 'Day/Date and Holiday Restriction'
				 when day_date_restriction_a = 1 and holiday_restriction_a = 1 and time_restriction_a = 1  then 'All Restriction'
				 END point_restriction
			from 
		(select 
					dd.deal_uuid deal_uuid, 
					dd.country_code country_cd, 
					dd.country_id country_id, 
					dd.lone l1, 
					dd.ltwo l2, 
					dd.lthree l3, 
					dd.fine_print_c fine_print_c, 
					dd.offer_description,
					case when dd.avg_sell_price < 25 then '<25' when dd.avg_sell_price >=25 and dd.avg_sell_price < 50 then '25-50' when dd.avg_sell_price >= 50 then '>=50' else null end avg_sell_price,
					dd.sum_nob sum_nob,
					dd.count_orders count_orders,
					dd.udv udv,
					case when typ_a.ind = 1 then 1 else 0 end day_date_restriction, 
					case when typ_b.ind = 1 then 1 else 0 end holiday_restriction,
					case when typ_c.ind = 1 then 1 else 0 end time_restriction,
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
								offer_description,
								sum_nob,
								count_orders, 
								udv
								from 
								sandbox.nvp_fine_deals_intl) dd
						left join 
							(	select 
								text_res,
								text_l3,
								text_l2,
								text_l1,
								restriction_type,
								country,
								1 AS ind
								from 
								sandbox.nvp_restriction_intl where restriction_type = 'Date/Day Restrictions'
								) typ_a on REGEXP_SUBSTR(dd.fine_print_c, lower(typ_a.text_res), 1, 1, 'i') = lower(typ_a.text_res) and dd.country_code = typ_a.country
						left join 
							( select 
								text_res,
								text_l3,
								text_l2,
								text_l1,
								restriction_type,
								country,
								1 AS ind
								from 
								sandbox.nvp_restriction_intl where restriction_type = 'Holiday Restrictions'
							) typ_b on REGEXP_SUBSTR(dd.fine_print_c, lower(typ_b.text_res), 1, 1, 'i') = lower(typ_b.text_res) and dd.country_code = typ_b.country
						left join
							(
							select 
								text_res,
								text_l3,
								text_l2,
								text_l1,
								restriction_type,
								country,
								1 AS ind
								from 
								sandbox.nvp_restriction_intl where restriction_type = 'Time Restriction'
							) typ_c on REGEXP_SUBSTR(dd.fine_print_c, lower(typ_c.text_res), 1, 1, 'i') = lower(typ_c.text_res) and dd.country_code = typ_c.country
						left join
							(
							select 
								text_res,
								text_l3,
								text_l2,
								text_l1,
								restriction_type,
								country,
								1 AS ind
								from 
								sandbox.nvp_restriction_intl where restriction_type = 'New Clients Restriction'
							) typ_d on REGEXP_SUBSTR(dd.fine_print_c, lower(typ_d.text_res), 1, 1, 'i') = lower(typ_d.text_res) and dd.country_code = typ_d.country
			) fin group by 1,2,3) fin_;*/
--------
	
	