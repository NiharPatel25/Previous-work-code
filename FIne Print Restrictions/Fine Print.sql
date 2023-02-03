SELECT * FROM grp_gdoop_bizops_db.np_fine_deals;
select * from grp_gdoop_bizops_db.np_top_fine_deals2;

drop table grp_gdoop_bizops_db.np_fine_deals;

create table grp_gdoop_bizops_db.np_fine_deals stored as orc as 
select a.*, b.lone, b.ltwo, b.lthree,c.deal_uuid as active from
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
	from user_groupondw.active_deals where load_date = '2020-03-22' and available_qty > 0 and sold_out = 'false' group by deal_uuid, country_code) as c on c.deal_uuid = a.deal_uuid and a.country_code = c.country_code;

-----------

create table grp_gdoop_bizops_db.np_top_fine_deals3 stored as orc as
select deal_uuid, count_, country_code, ltwo, row_number() over(partition by country_code order by count_ desc) rank_ from 
(select deal_uuid, country_code, ltwo, count(parent_order_uuid) count_  from grp_gdoop_bizops_db.np_fine_deals where country_code <> 'US'group by deal_uuid, country_code, ltwo) as a;

drop table grp_gdoop_bizops_db.np_top_fine_deals3;

create table grp_gdoop_bizops_db.np_regexp stored as orc as
select a.*, b.fine_print fine_print from grp_gdoop_bizops_db.np_top_fine_deals3 as a 
left join user_gp.deal_localized_contents as b on a.deal_uuid = b.deal_uuid where a.rank_<=200;



select count(*) from grp_gdoop_bizops_db.np_top_fine_deals3 where country_code; 


select deal_uuid, count_, country_code, ltwo, row_number() over(partition by country_code order by count_ desc) rank_ from 
(select deal_uuid, country_code, ltwo, count(parent_order_uuid) count_  from grp_gdoop_bizops_db.np_fine_deals where country_code <> 'US' group by deal_uuid, country_code, ltwo) as a;


select a.*, b.fine_print fine_print from grp_gdoop_bizops_db.np_top_fine_deals3 as a 
left join user_gp.deal_localized_contents as b on a.deal_uuid = b.deal_uuid where a.rank_<= and a.country_code = 'JP';


