

drop table if exists grp_gdoop_bizops_db.nvp_fine_deals_intl;
create table grp_gdoop_bizops_db.nvp_fine_deals_intl as
select a.*, 
    c.fine_print, 
    lower(c.fine_print) fine_print_c, 
    c.locale lang_loc,
    d.min_contract_sell, 
    d.max_contract_sell, 
    d.offer_description, --just min offer desc
    e.sum_nob, 
    e.count_orders, 
    f.udv udv
From 
       (select 
           deal_id as deal_uuid, 
           grt_l1_cat_name as lone, 
           grt_l2_cat_name as ltwo, 
           grt_l3_cat_name as lthree,
           min(country_code) as country_code,  
           min(country_id) as country_id,
           count(deal_id) as count_
         from user_edwprod.dim_gbl_deal_lob where grt_l1_cat_name = 'L1 - Local' group by deal_id, grt_l1_cat_name, grt_l2_cat_name, grt_l3_cat_name  having count_ = 1) as a
     inner join 
       (select 
           deal_uuid deal_uuid1, 
           case when country_code = 'GB' then 'UK' when country_code = 'QC' then 'CA' else country_code end country_code
         from 
            user_groupondw.active_deals 
            where load_date = date_add(CURRENT_DATE(), -2) and available_qty > 0 and sold_out = 'false' 
            group by deal_uuid, country_code
          ) as b on b.deal_uuid1 = a.deal_uuid and a.country_code = b.country_code
     left join 
     (select 
         deal_uuid deal_uuid2, 
         locale, 
         fine_print 
       from user_gp.deal_localized_contents
       group by deal_uuid, locale, fine_print
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
           sum(transactions) count_orders, 
           sum(nob_loc) sum_nob
         from
           user_edwprod.agg_gbl_financials_deal
           where report_date >= date_add(CURRENT_DATE(), -90)
           group by deal_id, country_code
         ) as e on e.deal_uuid = a.deal_uuid and e.country_code = a.country_code
     left join
      (select 	country_code,
           deal_id deal_uuid, 
           sum(uniq_deal_views) udv
       from
       user_edwprod.agg_gbl_traffic_deal
       where report_date >= date_add(CURRENT_DATE(), -90)
       group by deal_id, country_code
      ) as f on f.deal_uuid = a.deal_uuid and f.country_code = a.country_code;
		
	



---Always make sure we have consistent languages and locations. and no errors are seen here. 


select
  country_code,
  lang_loc,
  count (distinct deal_uuid),
count(deal_uuid) 
from grp_gdoop_bizops_db.nvp_fine_deals_intl group by country_code, lang_loc order by country_code; 
select count(deal_uuid) from sandbox.nvp_fine_deals_intl;


------Restriction data

drop table grp_gdoop_bizops_db.nvp_restriction_intl;
create table grp_gdoop_bizops_db.nvp_restriction_intl  (
    text_res string,
    text_res_eng_exact_translation string,
    text_l3 string,
    text_l2 string,
    text_l1 string,
    restriction_type string, 
    country string
) stored as orc
tblproperties ("orc.compress"="SNAPPY");

-----INSERTING TIME RESTRICTIONS

insert into grp_gdoop_bizops_db.nvp_restriction_intl
select * from 
(select 
    distinct
    base.text_res, 
    case when base.text_res is not null then text_res end as text_res_eng_exact_translation,
    case when base.text_res is not null then 'AM-PM restriction' end as text_l3, 
    text_l2, 
    text_l1, 
    restriction_type, 
    country
from 
(select
       case when country_code = 'BE' then REGEXP_EXTRACT(fine_print_c,"([0-1][0-7]h([0-9]+)?|([0-1][0-7])[.:-]\d+heures|tot.?([0-1][0-7])\\p{Punct}\d+.?uur|tot.?[0-1][0-7].?u|tot.?([0-9])u)") 
        when country_code = 'FR' then REGEXP_EXTRACT(fine_print_c,"([0-1][0-7]h([0-9]+)?|([0-1][0-7])[.:-]\d+heures)")
        when country_code = 'NL' then REGEXP_EXTRACT(fine_print_c,"(tot.?([0-1][0-7])[.:-]\d+.?uur|tot.?[0-1][0-7].?u|tot.?([0-9])u)")
        when country_code = 'IT' then REGEXP_EXTRACT(fine_print_c, "(alle [0-1][0-7][.:-]\d+|alle [0-9][:-]\d+|-[0-1][0-7][.:-]\d+)")
        when country_code = 'UK' then REGEXP_EXTRACT(fine_print_c, "(-([0-5])[.:-]\d+pm|-([0-9]+)[.:-]\d+am|-([0-5])pm|-([0-9]+)am)")
        when country_code = 'US' then REGEXP_EXTRACT(fine_print_c, "(-([0-5])[.:-]\d+pm|-([0-9]+)[.:-]\d+am|-([0-5])pm|-([0-9]+)am)")
        when country_code = 'CA' then REGEXP_EXTRACT(fine_print_c, "(-([0-5])[.:-]\d+pm|-([0-9]+)[.:-]\d+am|-([0-5])pm|-([0-9]+)am|[0-1][0-7]h([0-9]+)?|([0-1][0-7])[.:-]\d+heures)")
        when country_code = 'AE' then REGEXP_EXTRACT(fine_print_c, "(-([0-5])[.:-]\d+pm|-([0-9]+)[.:-]\d+am|-([0-5])pm|-([0-9]+)am)")
        when country_code = 'IE' then REGEXP_EXTRACT(fine_print_c, "(-([0-5])[.:-]\d+pm|-([0-9]+)[.:-]\d+am|-([0-5])pm|-([0-9]+)am)")
        when country_code = 'AU' then REGEXP_EXTRACT(fine_print_c, "(-([0-5])[.:-]\d+pm|-([0-9]+)[.:-]\d+am|-([0-5])pm|-([0-9]+)am)")
        when country_code = 'NZ' then REGEXP_EXTRACT(fine_print_c, "(-([0-5])[.:-]\d+pm|-([0-9]+)[.:-]\d+am|-([0-5])pm|-([0-9]+)am)")
        when country_code = 'DE' then REGEXP_EXTRACT(fine_print_c, "(-([0-1][0-7])[.:-]\d+.?uhr|-([7-9]).?uhr)")
        when country_code = 'PL' then REGEXP_EXTRACT(fine_print_c, "(godzinach.?[0-9]+[-][0-1][0-7]|godzinach.?[0-9]+[:][0-9]+[-][0-1][0-7]|do [0-1][0-7][:-]..|-[0-1][0-7][:][0-9]+)")
        when country_code = 'ES' then REGEXP_EXTRACT(fine_print_c, "(-[0-1][0-7]h|-[0-9]h)") end as text_res,
    'AM-PM restriction' as text_l2, 
    'Time Restriction' as text_l1, 
    'Time Restrictions' as restriction_type, 
    country_code as country
from grp_gdoop_bizops_db.nvp_fine_deals_intl) base) base_2 where text_res is not null;



select
    distinct REGEXP_EXTRACT(fine_print_c,"(tot.?([0-1][0-7])[.:-]\d+.?uur|tot.?[0-1][0-7].?u|tot.?([0-9])u)")
from grp_gdoop_bizops_db.nvp_fine_deals_intl
where country_code = 'FR';


select * from grp_gdoop_bizops_db.nvp_restriction_intl limit 5;



--------Importing other data


use grp_gdoop_bizops_db;
drop table if exists grp_gdoop_bizops_db.nvp_fine_inter_import;


create external table grp_gdoop_bizops_db.nvp_fine_inter_import (
	text_res string,
	text_res_eng_exact_translation string,
    text_l3 string,
    text_l2 string,
    text_l1 string,
    restriction_type string,
    country string
)  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES
(
    "separatorChar" = ",",
    "quoteChar"     = "\""
)
location 'hdfs://cerebro-namenode/user/nihpatel/fine_print'
;



insert into grp_gdoop_bizops_db.nvp_restriction_intl
select 
    text_res, 
    text_res_eng_exact_translation, 
    text_l3, 
    text_l2, 
    text_l1, 
    restriction_type, 
    country
   	from 
    grp_gdoop_bizops_db.nvp_fine_inter_import;



select * from grp_gdoop_bizops_db.nvp_restriction_intl; 

------

drop table grp_gdoop_bizops_db.nvp_fineprint_dash_sum;
create table grp_gdoop_bizops_db.nvp_fineprint_dash_sum stored as orc as
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
						grp_gdoop_bizops_db.nvp_fine_deals_intl) dd
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
						grp_gdoop_bizops_db.nvp_restriction_intl where restriction_type = 'Date/Day Restrictions'
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
						grp_gdoop_bizops_db.nvp_restriction_intl where restriction_type = 'Holiday Restrictions'
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
						grp_gdoop_bizops_db.nvp_restriction_intl where restriction_type = 'Time Restrictions'
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
						grp_gdoop_bizops_db.nvp_restriction_intl where restriction_type = 'New Clients Restrictions'
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
						grp_gdoop_bizops_db.nvp_restriction_intl where restriction_type = 'Reservations/Appointment Needed'
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
						grp_gdoop_bizops_db.nvp_restriction_intl where restriction_type = 'Menu/Service restrictions'
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
						grp_gdoop_bizops_db.nvp_restriction_intl where restriction_type = 'Additional Fees'
					) typ_g on REGEXP_SUBSTR(dd.fine_print_c, lower(typ_g.text_res), 1, 1, 'i') = lower(typ_g.text_res) and dd.country_code = typ_g.country) as fin_ group by 1,2,3,4,5,6,7,8,9,10


			
select 
dd.*, 
typ_a.*,
case when REGEXP_EXTRACT(dd.fine_print_c, lower(typ_a.text_res), 1) = lower(typ_a.text_res) then 1 end kuch
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
        grp_gdoop_bizops_db.nvp_fine_deals_intl) dd
left join 
(  select 
        text_res,
        text_l3,
        text_l2,
        text_l1,
        restriction_type,
        country,
        1 AS ind
        from 
     grp_gdoop_bizops_db.nvp_restriction_intl where restriction_type = 'Menu/Service restrictions'
) typ_a  on dd.country_code = typ_a.country;




