----- Local Inventory
drop table grp_gdoop_bizops_db.nvp_local_inventory;
create table grp_gdoop_bizops_db.nvp_local_inventory stored as orc as
select distinct deal_id, grt_l2_cat_name ltwo, country_code, country_id 
from user_edwprod.dim_gbl_deal_lob 
where grt_l1_cat_name = 'L1 - Local';

select * from grp_gdoop_bizops_db.nvp_local_inventory;
select * from grp_gdoop_bizops_db.sa_tickets_glive;

----First transaction

select * from grp_gdoop_bizops_db.rt_bt_txns;
select * from grp_gdoop_bizops_db.jk_bt_txns;

drop table if exists grp_gdoop_bizops_db.nvp_bt_first_txns;
create table grp_gdoop_bizops_db.nvp_bt_first_txns stored as orc as
select user_uuid, country_code, min(cast(order_date as date)) first_order_date, min(cast(redeem_date as date)) first_redeem_date, min(cast(usage_date as date)) first_usage_date, min(cast(book_date as date)) first_book_date
from grp_gdoop_bizops_db.rt_bt_txns 
where booked = 1 and is_refunded = 0
group by user_uuid, country_code;

-----

select 
  country, 
  count(distinct bcookie) as clicks
from grp_gdoop_pde.junohourly
where 
eventdate between '2022-03-01' and '2022-03-07'
and lower(platform) ='web' and eventDestination='genericClick' and event='genericClick' 
and widgetname in ( 'e348bba9-6b97-4179-b0d5-16cb04c39019-header-row-filter-pill' , 'e348bba9-6b97-4179-b0d5-16cb04c39019-filter-box')
group by 1;

select
	bcookie,
	order_date,
	lower(transaction_platform) platform,
	sum(transaction_qty) units,
	count(distinct order_uuid) orders
	from grp_gdoop_bizops_db.sa_tickets_glive t
	left join edwprod.fact_gbl_transactions gbt
	on t.deal_id = gbt.deal_uuid 
	where action = 'capture'
	and lower(transaction_platform) ='web'
	and order_date between '2022-03-01' and '2022-03-07'
	group by 1,2,3
;

select * from edwprod.fact_gbl_transactions;

select coalesce (gbt.bcookie, j.bcookie),
gbt.orders,
gbt.units,
j.clicks
--case when gbt.orders is not null and j.clicks is not null then '1'
--else '0' end as found
from
(select 
bcookie,
platform,
eventdate,
count(distinct bcookie) as clicks
from grp_gdoop_pde.junohourly
where 
eventdate = '2022-06-23' --between '2022-03-01' and '2022-04-30'
and platform='web' and eventDestination='genericClick' and event='genericClick' 
and widgetname in ( 'e348bba9-6b97-4179-b0d5-16cb04c39019-header-row-filter-pill' , 'e348bba9-6b97-4179-b0d5-16cb04c39019-filter-box')
group by 1,2,3) j
join
(select
	bcookie,
	order_date,
	case when t.deal_id is enot null then 1 else 0 end glive_deal_purchased,
	sum(transaction_qty) units,
	count(distinct order_uuid) orders
	from edwprod.fact_gbl_transactions gbt
	left join grp_gdoop_bizops_db.sa_tickets_glive t
	on t.deal_id = gbt.deal_uuid 
	where action = 'capture'
	and lower(transaction_platform) ='web'
	and order_date = '2022-06-23'--between '2022-03-01' and '2022-04-30'
	group by 1,2,3
	) gbt
on gbt.bcookie = j.bcookie 
and gbt.platform = j.platform and 
gbt.order_date = j.eventdate;

drop table IF EXISTS grp_gdoop_bizops_db.nvp_2018_txns;
select * from grp_gdoop_bizops_db.rt_bt_txns_1;

select * from ad_reporting_na_prod.citrus_master_report_citrus_51_v1;

create table grp_gdoop_bizops_db.nvp_2018_txns stored as orc as
select 
	user_uuid, 
	country_code country_code,
	first_redeem_date, 
	max(booked) booked_frst_ord,
	max(l2) l2
from 
	(select 
			t.user_uuid, 
			t.parent_order_uuid,
			t.deal_uuid, 
			t.country_code, 
			t.booked, 
			t.order_date, 
			t.redeem_date,
			t.first_redeem_date, 
			case when t.redeem_date = t.first_redeem_date then 1 else 0 end accepted_redeemed_ord, 
			gbl.l2 l2
	from 
			(select user_uuid, 
					parent_order_uuid, 
					deal_uuid,
					country_code, 
					booked, 
					cast(order_date as date) order_date,
					cast(redeem_date as date) redeem_date,
					min(cast(redeem_date as date)) over(partition by country_code, user_uuid) first_redeem_date
				from grp_gdoop_bizops_db.rt_bt_txns 
				WHERE cast(redeem_date as date) >= cast('2018-12-01' as date) and cast(redeem_date as date) <= cast('2018-12-31' as date)
			) as t
		join 
		(select parent_order_uuid, is_zero_amount
  					from user_edwprod.fact_gbl_transactions
  					where is_zero_amount = 0
  					group by parent_order_uuid, is_zero_amount) tx on tx.parent_order_uuid = t.parent_order_uuid
		join
			(select 
				deal_id, 
				grt_l2_cat_name l2
				from 
					user_edwprod.dim_gbl_deal_lob 
				where grt_l1_cat_name = 'L1 - Local' and grt_l2_cat_name <> 'L2 - Retail' 
				group by deal_id, grt_l2_cat_name) gbl on gbl.deal_id = t.deal_uuid
		join 
			(select 
					product_uuid, 
					merchant_uuid 
				from 
					user_edwprod.dim_offer_ext 
				where inventory_service_name <> 'tpis' 
				group by product_uuid, merchant_uuid) c on t.deal_uuid = c.product_uuid
		group by t.user_uuid, t.deal_uuid, t.country_code, t.booked, t.order_date, t.redeem_date, t.first_redeem_date,t.parent_order_uuid, gbl.l2) fin_ 
where accepted_redeemed_ord = 1 group by user_uuid, country_code,first_redeem_date;
	

drop table IF EXISTS grp_gdoop_bizops_db.nvp_2019_txns;
create table grp_gdoop_bizops_db.nvp_2019_txns stored as orc as
select 
	user_uuid, 
	country_code country_code,
	first_redeem_date, 
	max(booked) booked_frst_ord,
	max(l2) l2
from 
	(select 
			t.user_uuid, 
			t.parent_order_uuid,
			t.deal_uuid, 
			t.country_code, 
			t.booked, 
			t.order_date, 
			t.redeem_date,
			t.first_redeem_date, 
			case when t.redeem_date = t.first_redeem_date then 1 else 0 end accepted_redeemed_ord, 
			gbl.l2 l2
	from 
			(select user_uuid, 
					parent_order_uuid, 
					deal_uuid,
					country_code, 
					booked, 
					cast(order_date as date) order_date,
					cast(redeem_date as date) redeem_date,
					min(cast(redeem_date as date)) over(partition by country_code, user_uuid) first_redeem_date
				from grp_gdoop_bizops_db.rt_bt_txns 
				WHERE cast(redeem_date as date) >= cast('2019-12-01' as date) and cast(redeem_date as date) <= cast('2019-12-31' as date)
			) as t
		join (select parent_order_uuid, is_zero_amount
  					from user_edwprod.fact_gbl_transactions
  					where is_zero_amount = 0
  					group by parent_order_uuid, is_zero_amount) tx on tx.parent_order_uuid = t.parent_order_uuid
		join
			(select 
				deal_id, 
				grt_l2_cat_name l2
				from 
					user_edwprod.dim_gbl_deal_lob 
				where grt_l1_cat_name = 'L1 - Local' and grt_l2_cat_name <> 'L2 - Retail' 
				group by deal_id, grt_l2_cat_name) gbl on gbl.deal_id = t.deal_uuid
		join 
			(select 
					product_uuid, 
					merchant_uuid 
				from 
					user_edwprod.dim_offer_ext 
				where inventory_service_name <> 'tpis' 
				group by product_uuid, merchant_uuid) c on t.deal_uuid = c.product_uuid
		group by t.user_uuid, t.deal_uuid, t.country_code, t.booked, t.order_date, t.redeem_date, t.first_redeem_date,t.parent_order_uuid, gbl.l2) fin_ 
where accepted_redeemed_ord = 1 group by user_uuid, country_code,first_redeem_date;

----To get the exact deal which was redeemed
/*drop table grp_gdoop_bizops_db.nvp_2018_txns_one;
create table grp_gdoop_bizops_db.nvp_2018_txns_one stored as orc as
select 
			t.user_uuid, 
			t.parent_order_uuid,
			t.deal_uuid, 
			t.country_code, 
			t.booked, 
			t.order_date, 
			t.redeem_date,
			t.first_redeem_date, 
			case when t.redeem_date = t.first_redeem_date then 1 else 0 end accepted_redeemed_ord, 
			ROW_NUMBER() over(partition by user_uuid, country_code, parent_order_uuid) rank_for_ordering,
			gbl.l2 l2
	from 
			(select user_uuid, 
					parent_order_uuid, 
					deal_uuid,
					country_code, 
					booked, 
					cast(order_date as date) order_date,
					cast(redeem_date as date) redeem_date,
					min(cast(redeem_date as date)) over(partition by country_code, user_uuid) first_redeem_date
				from grp_gdoop_bizops_db.rt_bt_txns 
				WHERE cast(redeem_date as date) >= cast('2018-12-01' as date) and cast(redeem_date as date) <= cast('2018-12-31' as date)
			) as t
		join (select parent_order_uuid, is_zero_amount
  					from user_edwprod.fact_gbl_transactions
  					where is_zero_amount = 0
  					group by parent_order_uuid, is_zero_amount) tx on tx.parent_order_uuid = t.parent_order_uuid
		join
			(select 
				deal_id, 
				grt_l2_cat_name l2
				from 
					user_edwprod.dim_gbl_deal_lob 
				where grt_l1_cat_name = 'L1 - Local' and grt_l2_cat_name <> 'L2 - Retail' 
				group by deal_id, grt_l2_cat_name) gbl on gbl.deal_id = t.deal_uuid
		join 
			(select 
					product_uuid, 
					merchant_uuid 
				from 
					user_edwprod.dim_offer_ext 
				where inventory_service_name <> 'tpis' 
				group by product_uuid, merchant_uuid) c on t.deal_uuid = c.product_uuid
		group by t.user_uuid, t.deal_uuid, t.country_code, t.booked, t.order_date, t.redeem_date, t.first_redeem_date,t.parent_order_uuid, gbl.l2;


drop table grp_gdoop_bizops_db.nvp_2019_txns_one;
create table grp_gdoop_bizops_db.nvp_2019_txns_one stored as orc as
select 
			t.user_uuid, 
			t.parent_order_uuid,
			t.deal_uuid, 
			t.country_code, 
			t.booked, 
			t.order_date, 
			t.redeem_date,
			t.first_redeem_date, 
			case when t.redeem_date = t.first_redeem_date then 1 else 0 end accepted_redeemed_ord, 
			ROW_NUMBER() over(partition by user_uuid, country_code, parent_order_uuid) rank_for_ordering,
			gbl.l2 l2
	from 
			(select user_uuid, 
					parent_order_uuid, 
					deal_uuid,
					country_code, 
					booked, 
					cast(order_date as date) order_date,
					cast(redeem_date as date) redeem_date,
					min(cast(redeem_date as date)) over(partition by country_code, user_uuid) first_redeem_date
				from grp_gdoop_bizops_db.rt_bt_txns 
				WHERE cast(redeem_date as date) >= cast('2019-12-01' as date) and cast(redeem_date as date) <= cast('2019-12-31' as date)
			) as t
		join (select parent_order_uuid, is_zero_amount
  					from user_edwprod.fact_gbl_transactions
  					where is_zero_amount = 0
  					group by parent_order_uuid, is_zero_amount) tx on tx.parent_order_uuid = t.parent_order_uuid
		join
			(select 
				deal_id, 
				grt_l2_cat_name l2
				from 
					user_edwprod.dim_gbl_deal_lob 
				where grt_l1_cat_name = 'L1 - Local' and grt_l2_cat_name <> 'L2 - Retail' 
				group by deal_id, grt_l2_cat_name) gbl on gbl.deal_id = t.deal_uuid
		join 
			(select 
					product_uuid, 
					merchant_uuid 
				from 
					user_edwprod.dim_offer_ext 
				where inventory_service_name <> 'tpis' 
				group by product_uuid, merchant_uuid) c on t.deal_uuid = c.product_uuid
		group by t.user_uuid, t.deal_uuid, t.country_code, t.booked, t.order_date, t.redeem_date, t.first_redeem_date,t.parent_order_uuid, gbl.l2;



*/

-----------60 DAYS FREQUENCY


drop table if exists grp_gdoop_bizops_db.nvp_2018three;

create table grp_gdoop_bizops_db.nvp_2018three stored as orc as
select 
	t.user_uuid, 
	t.country_code, 
	t.parent_order_uuid, 
	t.deal_uuid,
	t.order_date, 
	t.units, 
	t.nob
from 
			(select 
				user_uuid, 
				country_code, 
				parent_order_uuid, 
				deal_uuid, 
				cast(order_date as date) order_date, 
				units, 
				nob
			from grp_gdoop_bizops_db.rt_bt_txns 
				where cast(order_date as date) between cast('2018-12-01' as date) and cast('2019-03-05' as date)) t
			join 
				(select parent_order_uuid, is_zero_amount
	  				from user_edwprod.fact_gbl_transactions
	  				where is_zero_amount = 0
	  				group by parent_order_uuid, is_zero_amount) tx on tx.parent_order_uuid = t.parent_order_uuid
	  		join
				(select 
					deal_id
					from user_edwprod.dim_gbl_deal_lob 
					where grt_l1_cat_name = 'L1 - Local' and grt_l2_cat_name <> 'L2 - Retail' 
					group by deal_id) gbl on gbl.deal_id = t.deal_uuid
			join 
				(select 
					product_uuid
				from 
					user_edwprod.dim_offer_ext 
				where inventory_service_name <> 'tpis' 
					group by product_uuid) c on t.deal_uuid = c.product_uuid;




drop table if exists grp_gdoop_bizops_db.nvp_2019three;
create table grp_gdoop_bizops_db.nvp_2019three stored as orc as
select 
	t.user_uuid, 
	t.country_code, 
	t.parent_order_uuid, 
	t.deal_uuid,
	t.order_date, 
	t.units, 
	t.nob
from 
		(select 
			user_uuid, 
			country_code, 
			parent_order_uuid, 
			deal_uuid, 
			cast(order_date as date) order_date, 
			units, 
			nob
		from grp_gdoop_bizops_db.rt_bt_txns 
		where cast(order_date as date) between cast('2019-12-01' as date) and cast('2020-03-05' as date)) t
	join 
		(select 
			parent_order_uuid, is_zero_amount
		from 
			user_edwprod.fact_gbl_transactions
		where is_zero_amount = 0
		group by parent_order_uuid, is_zero_amount) tx on tx.parent_order_uuid = t.parent_order_uuid
	join
		(select 
			deal_id
		from 
			user_edwprod.dim_gbl_deal_lob 
		where grt_l1_cat_name = 'L1 - Local' and grt_l2_cat_name <> 'L2 - Retail' 
		group by deal_id) gbl on gbl.deal_id = t.deal_uuid
	join 
		(select 
			product_uuid
		from
			user_edwprod.dim_offer_ext 
		where inventory_service_name <> 'tpis' 
		group by product_uuid) c on t.deal_uuid = c.product_uuid;




-----Country wise frequency		 

----COUNTRY AND L2 WISE

---- The reason for left join is because we are looking for redeem date and not order date unlike Rachel's logic previously used. 
drop table if exists grp_gdoop_bizops_db.nvp_purch_freq_fin2;
create table grp_gdoop_bizops_db.nvp_purch_freq_fin2 stored as orc as
select * from 			
(select 
	b.country_code, 
	b.booked_frst_ord,
	b.l2,
	'2018' as year_of_purc,
	count(distinct b.user_uuid) distinct_users, 
	count(distinct case when cast(a.order_date as date) between date_add(cast(b.first_redeem_date as date), 1) and date_add(cast(b.first_redeem_date as date), 60) then a.parent_order_uuid end) sixty_day_order
from grp_gdoop_bizops_db.nvp_2018_txns b 
left join grp_gdoop_bizops_db.nvp_2018three a on a.user_uuid = b.user_uuid and a.country_code = b.country_code
group by b.country_code, b.booked_frst_ord, b.l2
UNION
select 
	b.country_code, 
	b.booked_frst_ord,
	b.l2,
	'2019' as year_of_purc,
	count(distinct b.user_uuid) distinct_users, 
	count(distinct case when cast(a.order_date as date) between date_add(cast(b.first_redeem_date as date), 1) and date_add(cast(b.first_redeem_date as date), 60) then a.parent_order_uuid end) sixty_day_order
from grp_gdoop_bizops_db.nvp_2019_txns b  
left join grp_gdoop_bizops_db.nvp_2019three a on a.user_uuid = b.user_uuid and a.country_code = b.country_code
group by b.country_code, b.booked_frst_ord, b.l2) as a order by country_code, year_of_purc, booked_frst_ord;				


------- COUNT WITH UDV --- Higher level is achieved from grp_gdoop_bizops_db.nvp_purch_freq_fin3_2019 
drop table grp_gdoop_bizops_db.nvp_purch_freq_fin3_tableau;

create table grp_gdoop_bizops_db.nvp_engagement_purch_basic_tableau stored as orc as
select 
a.country_code, 
a.booked_frst_ord, 
a.l2,
a.year_of_purch, 
a.distinct_user_1,
a.repeat_visitors,
a.uv,a.udvva,a.udva,a.udvv,a.udv,
a.sixty_day_order,
a.distinct_user_2,
a.repeat_purchaser,
b.udv udv_from_b,
b.udv_notbt_eligible,
b.udv_bt_eligible,
b.sixty_day_order sixty_day_order_from_b,
b.units_total,
b.units_notbt_eligible,
b.units_bt_eligible
from 
(select * from grp_gdoop_bizops_db.nvp_purch_freq_fin3_2018
union 
select * from grp_gdoop_bizops_db.nvp_purch_freq_fin3_2019) as a 
left join 
(select * from grp_gdoop_bizops_db.nvp_purch_freq_fin_bt_2018
union 
select * from grp_gdoop_bizops_db.nvp_purch_freq_fin_bt_2019) as b on a.country_code = b.country_code and a.booked_frst_ord = b.booked_frst_ord and a.l2 = b.l2 and a.year_of_purch = b.year_of_purch
order by country_code, booked_frst_ord;


create table grp_gdoop_bizops_db.nvp_purch_freq_fin3_2019 stored as orc as 
select
u.country_code, 
u.booked_frst_ord, 
u.l2,
u.year_of_purch, 
u.distinct_users distinct_user_1,
u.repeat_visitors,
u.uv,u.udvva,u.udva,v.udvv,v.udv,
t.sixty_day_order,
t.distinct_users as distinct_user_2,
t.repeat_purchaser
from
	(select 
	a.country_code, 
	a.booked_frst_ord, 
	a.l2, 
	'2019' as year_of_purch, 
	count(distinct a.user_uuid) distinct_users,
	count(distinct b.user_uuid) repeat_visitors, 
	count(distinct concat(b.eventdate, b.user_uuid)) uv, 
	count(distinct case when b.deal_uuid is not null then concat(b.eventdate, b.user_uuid) end) udvva,
	count(distinct case when b.deal_uuid is not null then concat(b.eventdate, b.user_uuid, b.deal_uuid) end) udva
	from grp_gdoop_bizops_db.nvp_2019_txns as a 
	left join grp_gdoop_bizops_db.nvp_2019_all as b on a.user_uuid = b.user_uuid
	where a.country_code <> 'US' and a.country_code <> 'CA'
	group by a.country_code, a.booked_frst_ord, a.l2
	) as u
left join
	(select 
	a.country_code, 
	a.booked_frst_ord, 
	a.l2, 
	'2019' as year_of_purch, 
	count(distinct case when r.deal_uuid is not null then concat(r.eventdate, r.user_uuid) end) udvv,
	count(distinct case when r.deal_uuid is not null then concat(r.eventdate, r.user_uuid, r.deal_uuid) end) udv
	from grp_gdoop_bizops_db.nvp_2019_txns as a
	left join 
	(select x.*
	from grp_gdoop_bizops_db.nvp_2019_all x
	join grp_gdoop_bizops_db.nvp_local_inventory y on x.deal_uuid = y.deal_id) as r on a.user_uuid = r.user_uuid
	where a.country_code <> 'US' and a.country_code <> 'CA'
	group by a.country_code, a.booked_frst_ord, a.l2
	) as v on u.country_code = v.country_code and u.booked_frst_ord = v.booked_frst_ord and u.l2 = v.l2
left join
	(select 
	a.country_code, 
	a.booked_frst_ord,
	a.l2,
	'2019' as year_of_purc,
	count(distinct a.user_uuid) distinct_users, 
	count(distinct b.user_uuid) repeat_purchaser,
	count(distinct case when cast(b.order_date as date) between date_add(cast(a.first_redeem_date as date), 1) and date_add(cast(a.first_redeem_date as date), 60) then b.parent_order_uuid end) sixty_day_order
	from grp_gdoop_bizops_db.nvp_2019_txns a
	left join grp_gdoop_bizops_db.nvp_2019three b on a.user_uuid = b.user_uuid
	where a.country_code <> 'US' and a.country_code <> 'CA'
	group by a.country_code, a.booked_frst_ord, a.l2
	) as t on u.country_code = t.country_code and u.booked_frst_ord = t.booked_frst_ord and u.l2 = t.l2;

drop table grp_gdoop_bizops_db.nvp_purch_freq_fin3_2018;
create table grp_gdoop_bizops_db.nvp_purch_freq_fin3_2018 stored as orc as 
select
u.country_code, 
u.booked_frst_ord, 
u.l2,
u.year_of_purch, 
u.distinct_users distinct_user_1,
u.repeat_visitors,
u.uv,u.udvva,u.udva,v.udvv,v.udv,
t.sixty_day_order,
t.distinct_users as distinct_user_2,
t.repeat_purchaser
from
(select 
a.country_code, 
a.booked_frst_ord, 
a.l2, 
'2018' as year_of_purch, 
count(distinct a.user_uuid) distinct_users,
count(distinct b.user_uuid) repeat_visitors, 
count(distinct concat(b.eventdate, b.user_uuid)) uv, 
count(distinct case when b.deal_uuid is not null then concat(b.eventdate, b.user_uuid) end) udvva,
count(distinct case when b.deal_uuid is not null then concat(b.eventdate, b.user_uuid, b.deal_uuid) end) udva
from grp_gdoop_bizops_db.nvp_2018_txns as a 
left join grp_gdoop_bizops_db.nvp_2018_all as b on a.user_uuid = b.user_uuid
where a.country_code <> 'US' and a.country_code <> 'CA'
group by a.country_code, a.booked_frst_ord, a.l2
) as u
join
(select 
a.country_code, 
a.booked_frst_ord, 
a.l2, 
'2018' as year_of_purch, 
count(distinct case when r.deal_uuid is not null then concat(r.eventdate, r.user_uuid) end) udvv,
count(distinct case when r.deal_uuid is not null then concat(r.eventdate, r.user_uuid, r.deal_uuid) end) udv
from grp_gdoop_bizops_db.nvp_2018_txns as a
left join 
(select x.*
from grp_gdoop_bizops_db.nvp_2018_all x
join grp_gdoop_bizops_db.nvp_local_inventory y on x.deal_uuid = y.deal_id) as r on a.user_uuid = r.user_uuid
where a.country_code <> 'US' and a.country_code <> 'CA'
group by a.country_code, a.booked_frst_ord, a.l2
) as v on u.country_code = v.country_code and u.booked_frst_ord = v.booked_frst_ord and u.l2 = v.l2
join
(select 
a.country_code, 
a.booked_frst_ord,
a.l2,
'2018' as year_of_purc,
count(distinct a.user_uuid) distinct_users, 
count(distinct b.user_uuid) repeat_purchaser,
count(distinct case when cast(b.order_date as date) between date_add(cast(a.first_redeem_date as date), 1) and date_add(cast(a.first_redeem_date as date), 60) then b.parent_order_uuid end) sixty_day_order
from grp_gdoop_bizops_db.nvp_2018_txns a
left join grp_gdoop_bizops_db.nvp_2018three b on a.user_uuid = b.user_uuid
where a.country_code <> 'US' and a.country_code <> 'CA'
group by a.country_code, a.booked_frst_ord, a.l2
) as t on u.country_code = t.country_code and u.booked_frst_ord = t.booked_frst_ord and u.l2 = t.l2;
-----

create table grp_gdoop_bizops_db.nvp_purch_freq_fin_bt2_tableau stored as orc as
select * from 
(select * from grp_gdoop_bizops_db.nvp_purch_freq_fin_bt2_2018
union 
select * from grp_gdoop_bizops_db.nvp_purch_freq_fin_bt2_2019) fin_ order by country_code, booked_frst_ord;

drop table grp_gdoop_bizops_db.nvp_purch_freq_fin_bt2_2019;
create table grp_gdoop_bizops_db.nvp_purch_freq_fin_bt2_2019 stored as orc as 
select
v.country_code, 
v.booked_frst_ord, 
v.l2,
v.ltwo, 
v.year_of_purch, 
v.udv,
v.udv_notbt_eligible,
v.udv_bt_eligible,
v.repeat_uniql2_visitor,
t.units_total,
t.units_notbt_eligible,
t.units_bt_eligible
from
(select 
	a.country_code, 
	a.booked_frst_ord, 
	a.l2, 
	r.ltwo, 
	'2019' as year_of_purch,
	count(distinct case when r.deal_uuid is not null then concat(r.eventdate, r.user_uuid, r.deal_uuid) end) udv,
	count(distinct case when r.deal_uuid is not null and r.bt_eligible = 0 then concat(r.eventdate, r.user_uuid, r.deal_uuid) end) udv_notbt_eligible,
	count(distinct case when r.deal_uuid is not null and r.bt_eligible = 1 then concat(r.eventdate, r.user_uuid, r.deal_uuid) end) udv_bt_eligible,
	count(distinct r.user_uuid) repeat_uniql2_visitor
	from grp_gdoop_bizops_db.nvp_2019_txns as a
	left join 
	(select x.*, y.ltwo, case when z.deal_uuid2 is not null then 1 else 0 end as bt_eligible
		from grp_gdoop_bizops_db.nvp_2019_all x
		join grp_gdoop_bizops_db.nvp_local_inventory y on x.deal_uuid = y.deal_id
		left join 
		(select 
				load_date, 
				deal_uuid deal_uuid2
			from 
			grp_gdoop_bizops_db.sh_bt_active_deals_log 
			where product_is_active_flag = 1 and partner_inactive_flag = 0 and is_bookable = 1 and load_date >= '2019-12-01' and load_date <='2020-03-05'
			) z on x.deal_uuid = z.deal_uuid2 and x.eventdate = z.load_date
	) as r on a.user_uuid = r.user_uuid
	where a.country_code <> 'US' and a.country_code <> 'CA'
	group by a.country_code, a.booked_frst_ord, a.l2, r.ltwo
) as v
left join
(select 
	a.country_code, 
	a.booked_frst_ord,
	a.l2,
	b.ltwo,
	'2019' as year_of_purc,
	sum(case when cast(b.order_date as date) between date_add(cast(a.first_redeem_date as date), 1) and date_add(cast(a.first_redeem_date as date), 60) then b.units end) as units_total,
	sum(case when cast(b.order_date as date) >= date_add(cast(a.first_redeem_date as date), 1) and cast(b.order_date as date) <= date_add(cast(a.first_redeem_date as date), 60) and bt_eligible = 1 then b.units end) as units_bt_eligible,
	sum(case when cast(b.order_date as date) >= date_add(cast(a.first_redeem_date as date), 1) and cast(b.order_date as date) <= date_add(cast(a.first_redeem_date as date), 60) and bt_eligible = 0 then b.units end) as units_notbt_eligible
	from grp_gdoop_bizops_db.nvp_2019_txns a
	left join 
	(select x.*, y.ltwo, case when z.deal_uuid2 is not null then 1 else 0 end bt_eligible 
		from grp_gdoop_bizops_db.nvp_2019three x
		left join grp_gdoop_bizops_db.nvp_local_inventory y on x.deal_uuid = y.deal_id
		left join 
			(select 
				load_date, 
				deal_uuid deal_uuid2
			from 
			grp_gdoop_bizops_db.sh_bt_active_deals_log 
			where product_is_active_flag = 1 and partner_inactive_flag = 0 and is_bookable = 1 and load_date >= '2019-12-01' and load_date <='2020-03-05'
			) z on x.deal_uuid = z.deal_uuid2 and x.order_date = z.load_date
	) b on a.user_uuid = b.user_uuid
	where a.country_code <> 'US' and a.country_code <> 'CA'
	group by a.country_code, a.booked_frst_ord, a.l2, b.ltwo
) as t on v.country_code = t.country_code and v.booked_frst_ord = t.booked_frst_ord and v.l2 = t.l2 and v.ltwo = t.ltwo;


create table grp_gdoop_bizops_db.nvp_purch_freq_fin_bt2_2018 stored as orc as 
select
v.country_code, 
v.booked_frst_ord, 
v.l2,
v.ltwo, 
v.year_of_purch, 
v.udv,
v.udv_notbt_eligible,
v.udv_bt_eligible,
v.repeat_uniql2_visitor,
t.units_total,
t.units_notbt_eligible,
t.units_bt_eligible
from
(select 
	a.country_code, 
	a.booked_frst_ord, 
	a.l2, 
	r.ltwo, 
	'2018' as year_of_purch,
	count(distinct case when r.deal_uuid is not null then concat(r.eventdate, r.user_uuid, r.deal_uuid) end) udv,
	count(distinct case when r.deal_uuid is not null and r.bt_eligible = 0 then concat(r.eventdate, r.user_uuid, r.deal_uuid) end) udv_notbt_eligible,
	count(distinct case when r.deal_uuid is not null and r.bt_eligible = 1 then concat(r.eventdate, r.user_uuid, r.deal_uuid) end) udv_bt_eligible,
	count(distinct r.user_uuid) repeat_uniql2_visitor
	from grp_gdoop_bizops_db.nvp_2018_txns as a
	left join 
	(select x.*, y.ltwo, case when z.deal_uuid2 is not null then 1 else 0 end as bt_eligible
		from grp_gdoop_bizops_db.nvp_2018_all x
		join grp_gdoop_bizops_db.nvp_local_inventory y on x.deal_uuid = y.deal_id
		left join 
		(select 
				load_date, 
				deal_uuid deal_uuid2
			from 
			grp_gdoop_bizops_db.sh_bt_active_deals_log 
			where product_is_active_flag = 1 and partner_inactive_flag = 0 and is_bookable = 1 and load_date >= '2018-12-01' and load_date <='2019-03-05'
			) z on x.deal_uuid = z.deal_uuid2 and x.eventdate = z.load_date
	) as r on a.user_uuid = r.user_uuid
	where a.country_code <> 'US' and a.country_code <> 'CA'
	group by a.country_code, a.booked_frst_ord, a.l2, r.ltwo
) as v
left join
(select 
	a.country_code, 
	a.booked_frst_ord,
	a.l2,
	b.ltwo,
	'2018' as year_of_purc,
	sum(case when cast(b.order_date as date) between date_add(cast(a.first_redeem_date as date), 1) and date_add(cast(a.first_redeem_date as date), 60) then b.units end) as units_total,
	sum(case when cast(b.order_date as date) >= date_add(cast(a.first_redeem_date as date), 1) and cast(b.order_date as date) <= date_add(cast(a.first_redeem_date as date), 60) and bt_eligible = 1 then b.units end) as units_bt_eligible,
	sum(case when cast(b.order_date as date) >= date_add(cast(a.first_redeem_date as date), 1) and cast(b.order_date as date) <= date_add(cast(a.first_redeem_date as date), 60) and bt_eligible = 0 then b.units end) as units_notbt_eligible
	from grp_gdoop_bizops_db.nvp_2018_txns a
	left join 
	(select x.*, y.ltwo, case when z.deal_uuid2 is not null then 1 else 0 end bt_eligible 
		from grp_gdoop_bizops_db.nvp_2018three x
		left join grp_gdoop_bizops_db.nvp_local_inventory y on x.deal_uuid = y.deal_id
		left join 
			(select 
				load_date, 
				deal_uuid deal_uuid2
			from 
			grp_gdoop_bizops_db.sh_bt_active_deals_log 
			where product_is_active_flag = 1 and partner_inactive_flag = 0 and is_bookable = 1 and load_date >= '2018-12-01' and load_date <='2019-03-05'
			) z on x.deal_uuid = z.deal_uuid2 and x.order_date = z.load_date
	) b on a.user_uuid = b.user_uuid
	where a.country_code <> 'US' and a.country_code <> 'CA'
	group by a.country_code, a.booked_frst_ord, a.l2, b.ltwo
) as t on v.country_code = t.country_code and v.booked_frst_ord = t.booked_frst_ord and v.l2 = t.l2 and v.ltwo = t.ltwo;

------
drop table grp_gdoop_bizops_db.nvp_purch_freq_fin_bt_tableau;

create table grp_gdoop_bizops_db.nvp_purch_freq_fin_bt_tableau stored as orc as
select * from 
(select * from grp_gdoop_bizops_db.nvp_purch_freq_fin_bt_2018
union 
select * from grp_gdoop_bizops_db.nvp_purch_freq_fin_bt_2019) fin_ order by country_code, booked_frst_ord;



------GET UDV FOR BT ELIGIBLE AND NON BT ELIGIBLE without downstream
drop table grp_gdoop_bizops_db.nvp_purch_freq_fin_bt_2019;
create table grp_gdoop_bizops_db.nvp_purch_freq_fin_bt_2019 stored as orc as 
select
v.country_code, 
v.booked_frst_ord, 
v.l2,
v.year_of_purch, 
v.udv,
v.udv_notbt_eligible,
v.udv_bt_eligible,
t.sixty_day_order,
t.units_total,
t.units_notbt_eligible,
t.units_bt_eligible
from
(select 
	a.country_code, 
	a.booked_frst_ord, 
	a.l2, 
	'2019' as year_of_purch,
	count(distinct case when r.deal_uuid is not null then concat(r.eventdate, r.user_uuid, r.deal_uuid) end) udv,
	count(distinct case when r.deal_uuid is not null and r.bt_eligible = 0 then concat(r.eventdate, r.user_uuid, r.deal_uuid) end) udv_notbt_eligible,
	count(distinct case when r.deal_uuid is not null and r.bt_eligible = 1 then concat(r.eventdate, r.user_uuid, r.deal_uuid) end) udv_bt_eligible
	from grp_gdoop_bizops_db.nvp_2019_txns as a
	left join 
	(select x.*, case when z.deal_uuid2 is not null then 1 else 0 end as bt_eligible
		from grp_gdoop_bizops_db.nvp_2019_all x
		join grp_gdoop_bizops_db.nvp_local_inventory y on x.deal_uuid = y.deal_id
		left join 
		(select 
				load_date, 
				deal_uuid deal_uuid2
			from 
			grp_gdoop_bizops_db.sh_bt_active_deals_log 
			where product_is_active_flag = 1 and partner_inactive_flag = 0 and is_bookable = 1 and load_date >= '2019-12-01' and load_date <='2020-03-05'
			) z on x.deal_uuid = z.deal_uuid2 and x.eventdate = z.load_date
	) as r on a.user_uuid = r.user_uuid
	where a.country_code <> 'US' and a.country_code <> 'CA'
	group by a.country_code, a.booked_frst_ord, a.l2
) as v
left join
(select 
	a.country_code, 
	a.booked_frst_ord,
	a.l2,
	'2019' as year_of_purc,
	count(distinct case when cast(b.order_date as date) between date_add(cast(a.first_redeem_date as date), 1) and date_add(cast(a.first_redeem_date as date), 60) then b.parent_order_uuid end) sixty_day_order,
	sum(case when cast(b.order_date as date) between date_add(cast(a.first_redeem_date as date), 1) and date_add(cast(a.first_redeem_date as date), 60) then b.units end) as units_total,
	sum(case when cast(b.order_date as date) >= date_add(cast(a.first_redeem_date as date), 1) and cast(b.order_date as date) <= date_add(cast(a.first_redeem_date as date), 60) and bt_eligible = 1 then b.units end) as units_bt_eligible,
	sum(case when cast(b.order_date as date) >= date_add(cast(a.first_redeem_date as date), 1) and cast(b.order_date as date) <= date_add(cast(a.first_redeem_date as date), 60) and bt_eligible = 0 then b.units end) as units_notbt_eligible
	from grp_gdoop_bizops_db.nvp_2019_txns a
	left join 
	(select x.*, case when z.deal_uuid2 is not null then 1 else 0 end bt_eligible 
		from grp_gdoop_bizops_db.nvp_2019three x 
		left join 
			(select 
				load_date, 
				deal_uuid deal_uuid2
			from 
			grp_gdoop_bizops_db.sh_bt_active_deals_log 
			where product_is_active_flag = 1 and partner_inactive_flag = 0 and is_bookable = 1 and load_date >= '2019-12-01' and load_date <='2020-03-05'
			) z on x.deal_uuid = z.deal_uuid2 and x.order_date = z.load_date
	) b on a.user_uuid = b.user_uuid
	where a.country_code <> 'US' and a.country_code <> 'CA'
	group by a.country_code, a.booked_frst_ord, a.l2
) as t on v.country_code = t.country_code and v.booked_frst_ord = t.booked_frst_ord and v.l2 = t.l2;

drop table grp_gdoop_bizops_db.nvp_purch_freq_fin_bt_2018;

create table grp_gdoop_bizops_db.nvp_purch_freq_fin_bt_2018 stored as orc as 
select
v.country_code, 
v.booked_frst_ord, 
v.l2,
v.year_of_purch, 
v.udv,
v.udv_notbt_eligible,
v.udv_bt_eligible,
t.sixty_day_order,
t.units_total,
t.units_notbt_eligible,
t.units_bt_eligible
from
(select 
	a.country_code, 
	a.booked_frst_ord, 
	a.l2, 
	'2018' as year_of_purch,
	count(distinct case when r.deal_uuid is not null then concat(r.eventdate, r.user_uuid, r.deal_uuid) end) udv,
	count(distinct case when r.deal_uuid is not null and r.bt_eligible = 0 then concat(r.eventdate, r.user_uuid, r.deal_uuid) end) udv_notbt_eligible,
	count(distinct case when r.deal_uuid is not null and r.bt_eligible = 1 then concat(r.eventdate, r.user_uuid, r.deal_uuid) end) udv_bt_eligible
	from grp_gdoop_bizops_db.nvp_2018_txns as a
	left join 
	(select x.*, case when z.deal_uuid2 is not null then 1 else 0 end as bt_eligible
		from grp_gdoop_bizops_db.nvp_2018_all x
		join grp_gdoop_bizops_db.nvp_local_inventory y on x.deal_uuid = y.deal_id
		left join 
		(select 
				load_date, 
				deal_uuid deal_uuid2
			from 
			grp_gdoop_bizops_db.sh_bt_active_deals_log 
			where product_is_active_flag = 1 and partner_inactive_flag = 0 and is_bookable = 1 and load_date >= '2018-12-01' and load_date <='2019-03-05'
			) z on x.deal_uuid = z.deal_uuid2 and x.eventdate = z.load_date
	) as r on a.user_uuid = r.user_uuid
	where a.country_code <> 'US' and a.country_code <> 'CA'
	group by a.country_code, a.booked_frst_ord, a.l2
) as v
left join
(select 
	a.country_code, 
	a.booked_frst_ord,
	a.l2,
	'2018' as year_of_purc,
	count(distinct case when cast(b.order_date as date) between date_add(cast(a.first_redeem_date as date), 1) and date_add(cast(a.first_redeem_date as date), 60) then b.parent_order_uuid end) sixty_day_order,
	sum(case when cast(b.order_date as date) between date_add(cast(a.first_redeem_date as date), 1) and date_add(cast(a.first_redeem_date as date), 60) then b.units end) as units_total,
	sum(case when cast(b.order_date as date) >= date_add(cast(a.first_redeem_date as date), 1) and cast(b.order_date as date) <= date_add(cast(a.first_redeem_date as date), 60) and bt_eligible = 1 then b.units end) as units_bt_eligible,
	sum(case when cast(b.order_date as date) >= date_add(cast(a.first_redeem_date as date), 1) and cast(b.order_date as date) <= date_add(cast(a.first_redeem_date as date), 60) and bt_eligible = 0 then b.units end) as units_notbt_eligible
	from grp_gdoop_bizops_db.nvp_2018_txns a
	left join 
	(select x.*, case when z.deal_uuid2 is not null then 1 else 0 end bt_eligible 
		from grp_gdoop_bizops_db.nvp_2018three x 
		left join 
			(select 
				load_date, 
				deal_uuid deal_uuid2
			from 
			grp_gdoop_bizops_db.sh_bt_active_deals_log 
			where product_is_active_flag = 1 and partner_inactive_flag = 0 and is_bookable = 1 and load_date >= '2018-12-01' and load_date <='2019-03-05'
			) z on x.deal_uuid = z.deal_uuid2 and x.order_date = z.load_date
	) b on a.user_uuid = b.user_uuid
	where a.country_code <> 'US' and a.country_code <> 'CA'
	group by a.country_code, a.booked_frst_ord, a.l2
) as t on v.country_code = t.country_code and v.booked_frst_ord = t.booked_frst_ord and v.l2 = t.l2;




----- Bought not bought in 2018, 2019




select * from grp_gdoop_bizops_db.rt_bt_txns;

drop table grp_gdoop_bizops_db.nvp_2018_local_yr;
create table grp_gdoop_bizops_db.nvp_2018_local_yr stored as orc as
select 
 t.user_uuid, 
 t.country_code, 
 t.parent_order_uuid, 
 t.deal_uuid,
 t.order_date, 
 t.units, 
 t.nob, 
 t.booked, 
 t.redeemed, 
 t.redeem_date, 
 gbl.grt_l2_cat_name
from 
   (select 
      user_uuid, 
      country_code, 
      parent_order_uuid, 
      deal_uuid, 
      cast(order_date as date) order_date, 
      booked,
      redeemed,
      redeem_date, 
      units, 
      nob
    from grp_gdoop_bizops_db.sh_bt_txns 
    where cast(order_date as date) between cast('2018-01-01' as date) and cast('2018-12-31' as date)) t
join 
    (select parent_order_uuid, is_zero_amount
     from user_edwprod.fact_gbl_transactions
     where is_zero_amount = 0
     group by parent_order_uuid, is_zero_amount) tx on tx.parent_order_uuid = t.parent_order_uuid
join
    (select 
       deal_id, 
       grt_l2_cat_name
       from user_edwprod.dim_gbl_deal_lob 
       where grt_l1_cat_name = 'L1 - Local' and grt_l2_cat_name <> 'L2 - Retail' 
       group by deal_id, grt_l2_cat_name) gbl on gbl.deal_id = t.deal_uuid
join 
    (select 
       product_uuid
    from 
       user_edwprod.dim_offer_ext 
     where inventory_service_name <> 'tpis' 
    group by product_uuid) c on t.deal_uuid = c.product_uuid;

				
drop table grp_gdoop_bizops_db.nvp_2019_local_yr;
create table grp_gdoop_bizops_db.nvp_2019_local_yr stored as orc as
select 
 t.user_uuid, 
 t.country_code, 
 t.parent_order_uuid, 
 t.deal_uuid,
 t.order_date, 
 t.units, 
 t.nob, 
 t.booked, 
 t.redeemed, 
 t.redeem_date, 
 gbl.grt_l2_cat_name
from 
   (select 
      user_uuid, 
      country_code, 
      parent_order_uuid, 
      deal_uuid, 
      cast(order_date as date) order_date, 
      booked,
      redeemed,
      redeem_date, 
      units, 
      nob
    from grp_gdoop_bizops_db.sh_bt_txns 
    where cast(order_date as date) between cast('2019-01-01' as date) and cast('2019-12-31' as date)) t
join 
    (select parent_order_uuid, is_zero_amount
     from user_edwprod.fact_gbl_transactions
     where is_zero_amount = 0
     group by parent_order_uuid, is_zero_amount) tx on tx.parent_order_uuid = t.parent_order_uuid
join
    (select 
       deal_id, 
       grt_l2_cat_name
       from user_edwprod.dim_gbl_deal_lob 
       where grt_l1_cat_name = 'L1 - Local' and grt_l2_cat_name <> 'L2 - Retail' 
       group by deal_id, grt_l2_cat_name) gbl on gbl.deal_id = t.deal_uuid
join 
    (select 
       product_uuid
    from 
       user_edwprod.dim_offer_ext 
     where inventory_service_name <> 'tpis' 
    group by product_uuid) c on t.deal_uuid = c.product_uuid;

create table grp_gdoop_bizops_db.nvp_2018_local_yr_red stored as orc as
select 
t.user_uuid, 
t.country_code, 
t.parent_order_uuid, 
t.deal_uuid,
t.order_date, 
t.units, 
t.nob, 
t.booked, 
t.redeemed, 
t.redeem_date, 
grt_l2_cat_name
from 
    (select 
      user_uuid, 
      country_code, 
      parent_order_uuid, 
      deal_uuid,
      cast(order_date as date) order_date, 
      booked,
      redeemed,
      redeem_date, 
      units, 
      nob
    from grp_gdoop_bizops_db.sh_bt_txns 
    where cast(redeem_date as date) between cast('2018-01-01' as date) and cast('2018-12-31' as date)) t
join 
    (select 
      parent_order_uuid, is_zero_amount
     from user_edwprod.fact_gbl_transactions
     where is_zero_amount = 0
     group by parent_order_uuid, is_zero_amount) tx on tx.parent_order_uuid = t.parent_order_uuid
join
    (select 
      deal_id, 
      grt_l2_cat_name
      from user_edwprod.dim_gbl_deal_lob 
      where grt_l1_cat_name = 'L1 - Local' and grt_l2_cat_name <> 'L2 - Retail' 
      group by deal_id, grt_l2_cat_name) gbl on gbl.deal_id = t.deal_uuid
join 
    (select 
      product_uuid
     from 
      user_edwprod.dim_offer_ext 
     where inventory_service_name <> 'tpis' 
     group by product_uuid) c on t.deal_uuid = c.product_uuid;


create table grp_gdoop_bizops_db.nvp_2019_local_yr_red stored as orc as
select 
t.user_uuid, 
t.country_code, 
t.parent_order_uuid, 
t.deal_uuid,
t.order_date, 
t.units, 
t.nob, 
t.booked, 
t.redeemed, 
t.redeem_date, 
grt_l2_cat_name
from 
    (select 
      user_uuid, 
      country_code, 
      parent_order_uuid, 
      deal_uuid,
      cast(order_date as date) order_date, 
      booked,
      redeemed,
      redeem_date, 
      units, 
      nob
    from grp_gdoop_bizops_db.sh_bt_txns 
    where cast(redeem_date as date) between cast('2019-01-01' as date) and cast('2019-12-31' as date)) t
join 
    (select 
      parent_order_uuid, is_zero_amount
     from user_edwprod.fact_gbl_transactions
     where is_zero_amount = 0
     group by parent_order_uuid, is_zero_amount) tx on tx.parent_order_uuid = t.parent_order_uuid
join
    (select 
      deal_id, 
      grt_l2_cat_name
      from user_edwprod.dim_gbl_deal_lob 
      where grt_l1_cat_name = 'L1 - Local' and grt_l2_cat_name <> 'L2 - Retail' 
      group by deal_id, grt_l2_cat_name) gbl on gbl.deal_id = t.deal_uuid
join 
    (select 
      product_uuid
     from 
      user_edwprod.dim_offer_ext 
     where inventory_service_name <> 'tpis' 
     group by product_uuid) c on t.deal_uuid = c.product_uuid;


-----Active Deals in 2018/2019

select * from grp_gdoop_bizops_db.sh_bt_active_deals_log limit 5;


drop table grp_gdoop_bizops_db.bt_deals_info_2019;

create table grp_gdoop_bizops_db.bt_deals_info_2019 stored as orc as
select 
a.deal_uuid, 
case when b.deal_uuid is not null then 1 else 0 end bt_active
from 
(select deal_uuid  from user_groupondw.active_deals x
join 
grp_gdoop_bizops_db.nvp_local_inventory y on x.deal_uuid = y.deal_id
where x.available_qty > 0 and x.sold_out = 'false' and x.country_code <> 'US' and x.country_code <> 'CA' 
and x.load_date between cast('2019-12-01' as date) and cast('2020-03-05' as date)
group by deal_uuid) a
left join 
(select deal_uuid from grp_gdoop_bizops_db.sh_bt_active_deals_log
where country <> 'US' and country <> 'CA' and sold_out = 'false' and is_bookable = 1 and partner_inactive_flag = 0
and load_date between cast('2019-12-01' as date) and cast('2020-03-05' as date)
group by deal_uuid) b on a.deal_uuid = b.deal_uuid; 


create table grp_gdoop_bizops_db.bt_deals_info_2018 stored as orc as
select 
a.deal_uuid, 
case when b.deal_uuid is not null then 1 else 0 end bt_active
from 
(select deal_uuid  from user_groupondw.active_deals x
join 
grp_gdoop_bizops_db.nvp_local_inventory y on x.deal_uuid = y.deal_id
where x.available_qty > 0 and x.sold_out = 'false' and x.country_code <> 'US' and x.country_code <> 'CA' 
and x.load_date between cast('2018-12-01' as date) and cast('2019-03-05' as date)
group by deal_uuid) a
left join 
(select deal_uuid from grp_gdoop_bizops_db.sh_bt_active_deals_log
where country <> 'US' and country <> 'CA' and sold_out = 'false' and is_bookable = 1 and partner_inactive_flag = 0
and load_date between cast('2018-12-01' as date) and cast('2019-03-05' as date)
group by deal_uuid) b on a.deal_uuid = b.deal_uuid;



select * from user_groupondw.active_deals limit 5;
select * from grp_gdoop_bizops_db.sh_bt_active_deals_log limit 5;
				
-----Purchaser of the same deal again
drop table grp_gdoop_bizops_db.nvp_user_min_red;

create table grp_gdoop_bizops_db.nvp_user_min_red stored as orc as
select user_uuid, first_booked_red
from 
(select user_uuid, min(cast(redeem_date as date)) over(partition by user_uuid) first_booked_red 
from
(select * from grp_gdoop_bizops_db.sh_bt_txns 
where country_code <> 'US' and country_code <> 'CA' and cast(order_date as date) >= cast('2018-10-01' as date) and cast(order_date as date) <= cast('2019-12-31' as date)) as inside 
where booked = 1) as xyz group by user_uuid, first_booked_red;


-----

select 
	a.country_code, 
	b.booked_frst_ord,
	count(distinct b.user_uuid) distinct_users, 
	count(distinct case when cast(a.order_date as date) between date_add(cast(b.first_redeem_date as date), 1) and date_add(cast(b.first_redeem_date as date), 60) then a.parent_order_uuid end) sixty_day_order,
	count(distinct case when cast(a.order_date as date) between date_add(cast(b.first_redeem_date as date), 1) and date_add(cast(b.first_redeem_date as date), 60) and deal_uuid_a = deal_uuid_b then a.parent_order_uuid end) repeat_deal_purch
from 
(select 
	t.user_uuid, 
	t.country_code, 
	t.booked
	t.deal_uuid deal_uuid_a, 
	t.first_redeem_date, 
	gbl.l2, 
	c.merchant_uuid
from 
	(select 
		user_uuid,
		country_code,
		first_redeem_date,
		max(deal_uuid) deal_uuid,
		max(booked) booked
		from grp_gdoop_bizops_db.nvp_2019_txns_one 
		where accept_redeemed_ord = 1 
		group by user_uuid, country_code, first_redeem_date, accepted_redeemed_ord) as t 
	left join 
	(select 
			deal_id, 
			grt_l2_cat_name l2
			from 
			user_edwprod.dim_gbl_deal_lob 
			group by deal_id, grt_l2_cat_name) gbl on gbl.deal_id = t.deal_uuid
	left join 
	(select 
			product_uuid, 
			min(merchant_uuid) merchant_uuid
			from 
			user_edwprod.dim_offer_ext 
			where inventory_service_name <> 'tpis' 
			group by product_uuid) c on t.deal_uuid = c.product_uuid) as a
left join 
(select 
	t.user_uuid, 
	t.country_code, 
	t.parent_order_uuid, 
	t.deal_uuid deal_uuid_b,
	t.order_date, 
	t.units, 
	t.nob
from 
grp_gdoop_bizops_db.nvp_2018three) b on a.user_uuid = b.user_uuid and a.country_code = b.country_code




------TRASH

drop table IF EXISTS grp_gdoop_bizops_db.nvp_2019_freq;
create table grp_gdoop_bizops_db.nvp_2019_freq stored as orc as
select 
	user_uuid, 
	booked_frst_ord,
	min(country_code),
	count(distinct parent_order_uuid) count_of_order,
	min(l2) l2
from 
	(select 
		a.user_uuid,
		a.parent_order_uuid,
		a.order_date, 
		b.day_60, 
		b.first_redeem_date,
		b.booked_frst_ord,
		b.country_code,
		b.l2,
		case when order_date <= b.day_60 and order_date > b.first_redeem_date then 1 else 0 end to_be_included
	from
	(select 
			t.user_uuid, 
			t.country_code,
			t.parent_order_uuid, 
			t.order_date
	from 
			(select 
				user_uuid, 
				country_code, 
				parent_order_uuid, 
				deal_uuid, 
				cast(order_date as date) order_date
			from grp_gdoop_bizops_db.rt_bt_txns 
				where cast(order_date as date) between cast('2019-12-01' as date) and cast('2020-03-05' as date)) t
		join
			(select 
				deal_id
			from user_edwprod.dim_gbl_deal_lob 
			where grt_l1_cat_name = 'L1 - Local' and grt_l2_cat_name <> 'L2 - Retail' 
			group by deal_id) gbl on gbl.deal_id = t.deal_uuid
		join 
			(select 
				product_uuid, 
				merchant_uuid 
			from 
				user_edwprod.dim_offer_ext 
			where inventory_service_name <> 'tpis' 
			group by product_uuid, merchant_uuid) c on t.deal_uuid = c.product_uuid 
		group by t.user_uuid, t.parent_order_uuid, t.order_date, t.country_code) a
	join 
	(select 
		user_uuid,
		country_code, 
		first_redeem_date,
		booked_frst_ord,
		l2, 
		date_add(first_redeem_date, 60) day_60
	from grp_gdoop_bizops_db.nvp_2019_txns) b on a.user_uuid = b.user_uuid and a.country_code = b.country_code) fin_ where fin_.to_be_included = 1 group by user_uuid, country_code,booked_frst_ord;




drop table IF EXISTS grp_gdoop_bizops_db.nvp_2018_freq;
create table grp_gdoop_bizops_db.nvp_2018_freq stored as orc as
select 
	user_uuid, 
	booked_frst_ord,
	min(country_code),
	count(distinct parent_order_uuid) count_of_order,
	min(l2) l2
from 
	(select 
		a.user_uuid,
		a.parent_order_uuid,
		a.order_date, 
		b.day_60, 
		b.first_redeem_date,
		b.booked_frst_ord,
		b.country_code,
		b.l2,
		case when order_date <= b.day_60 and order_date > b.first_redeem_date then 1 else 0 end to_be_included
	from
	(select 
			t.user_uuid, 
			t.country_code,
			t.parent_order_uuid, 
			t.order_date
	from 
			(select 
				user_uuid, 
				country_code, 
				parent_order_uuid, 
				deal_uuid, 
				cast(order_date as date) order_date
			from grp_gdoop_bizops_db.rt_bt_txns 
				where cast(order_date as date) between cast('2018-12-01' as date) and cast('2019-03-05' as date)) t
		join
			(select 
				deal_id
			from user_edwprod.dim_gbl_deal_lob 
			where grt_l1_cat_name = 'L1 - Local' and grt_l2_cat_name <> 'L2 - Retail' 
			group by deal_id) gbl on gbl.deal_id = t.deal_uuid
		join 
			(select 
				product_uuid, 
				merchant_uuid 
			from 
				user_edwprod.dim_offer_ext 
			where inventory_service_name <> 'tpis' 
			group by product_uuid, merchant_uuid) c on t.deal_uuid = c.product_uuid 
		group by t.user_uuid, t.parent_order_uuid, t.order_date, t.country_code) a
	join 
	(select 
		user_uuid,
		country_code, 
		first_redeem_date,
		booked_frst_ord,
		l2, 
		date_add(first_redeem_date, 60) day_60
	from grp_gdoop_bizops_db.nvp_2018_txns) b on a.user_uuid = b.user_uuid and a.country_code = b.country_code) fin_ where fin_.to_be_included = 1 group by user_uuid, country_code,booked_frst_ord;




drop table IF EXISTS grp_gdoop_bizops_db.nvp_2019_freq;
create table grp_gdoop_bizops_db.nvp_2019_freq stored as orc as
select 
	user_uuid, 
	booked_frst_ord,
	min(country_code),
	count(distinct parent_order_uuid) count_of_order,
	min(l2) l2
from 
	(select 
		a.user_uuid,
		a.parent_order_uuid,
		a.order_date, 
		b.day_60, 
		b.first_redeem_date,
		b.booked_frst_ord,
		b.country_code,
		b.l2,
		case when order_date <= b.day_60 and order_date > b.first_redeem_date then 1 else 0 end to_be_included
	from
	(select 
			t.user_uuid, 
			t.country_code,
			t.parent_order_uuid, 
			t.order_date
	from 
			(select 
				user_uuid, 
				country_code, 
				parent_order_uuid, 
				deal_uuid, 
				cast(order_date as date) order_date
			from grp_gdoop_bizops_db.rt_bt_txns 
				where cast(order_date as date) between cast('2019-12-01' as date) and cast('2020-03-05' as date)) t
		join
			(select 
				deal_id
			from user_edwprod.dim_gbl_deal_lob 
			where grt_l1_cat_name = 'L1 - Local' and grt_l2_cat_name <> 'L2 - Retail' 
			group by deal_id) gbl on gbl.deal_id = t.deal_uuid
		join 
			(select 
				product_uuid, 
				merchant_uuid 
			from 
				user_edwprod.dim_offer_ext 
			where inventory_service_name <> 'tpis' 
			group by product_uuid, merchant_uuid) c on t.deal_uuid = c.product_uuid 
		group by t.user_uuid, t.parent_order_uuid, t.order_date, t.country_code) a
	join 
	(select 
		user_uuid,
		country_code, 
		first_redeem_date,
		booked_frst_ord,
		l2, 
		date_add(first_redeem_date, 60) day_60
	from grp_gdoop_bizops_db.nvp_2019_txns) b on a.user_uuid = b.user_uuid and a.country_code = b.country_code) fin_ where fin_.to_be_included = 1 group by user_uuid, country_code,booked_frst_ord;


------
----Experimentation


drop table IF EXISTS grp_gdoop_bizops_db.nvp_2018_txns_one;

create table grp_gdoop_bizops_db.nvp_2018_txns_one stored as orc as
select 
	user_uuid, 
	country_code country_code,
	first_redeem_date, 
	max(booked) booked_frst_ord
from 
	(select 
			t.user_uuid, 
			t.parent_order_uuid,
			t.deal_uuid, 
			t.country_code, 
			t.booked, 
			t.order_date, 
			t.redeem_date,
			t.first_redeem_date, 
			case when t.redeem_date = t.first_redeem_date then 1 else 0 end accepted_redeemed_ord
	from 
			(select user_uuid, 
					parent_order_uuid, 
					deal_uuid,
					country_code, 
					booked, 
					cast(order_date as date) order_date,
					cast(redeem_date as date) redeem_date,
					min(cast(redeem_date as date)) over(partition by country_code, user_uuid) first_redeem_date
				from grp_gdoop_bizops_db.rt_bt_txns 
				WHERE cast(redeem_date as date) >= cast('2018-04-01' as date) and cast(redeem_date as date) <= cast('2018-04-30' as date)
			) as t
		inner join
			(select 
				deal_id
				from 
					user_edwprod.dim_gbl_deal_lob 
				where grt_l1_cat_name = 'L1 - Local' and grt_l2_cat_name <> 'L2 - Retail' 
				group by deal_id) gbl on gbl.deal_id = t.deal_uuid
		inner join 
			(select 
					product_uuid, 
					merchant_uuid 
				from 
					user_edwprod.dim_offer_ext 
				where inventory_service_name <> 'tpis' 
				group by product_uuid, merchant_uuid) c on t.deal_uuid = c.product_uuid
		group by t.user_uuid, t.deal_uuid, t.country_code, t.booked, t.order_date, t.redeem_date, t.first_redeem_date,t.parent_order_uuid) fin_ 
where accepted_redeemed_ord = 1 group by user_uuid, country_code,first_redeem_date;
	

drop table IF EXISTS grp_gdoop_bizops_db.nvp_2019_txns_one;
create table grp_gdoop_bizops_db.nvp_2019_txns_one stored as orc as
select 
	user_uuid, 
	country_code country_code,
	first_redeem_date, 
	max(booked) booked_frst_ord
from 
	(select 
			t.user_uuid, 
			t.parent_order_uuid,
			t.deal_uuid, 
			t.country_code, 
			t.booked, 
			t.order_date, 
			t.redeem_date,
			t.first_redeem_date, 
			case when t.redeem_date = t.first_redeem_date then 1 else 0 end accepted_redeemed_ord
	from 
			(select user_uuid, 
					parent_order_uuid, 
					deal_uuid,
					country_code, 
					booked, 
					cast(order_date as date) order_date,
					cast(redeem_date as date) redeem_date,
					min(cast(redeem_date as date)) over(partition by country_code, user_uuid) first_redeem_date
				from grp_gdoop_bizops_db.rt_bt_txns 
				where cast(redeem_date as date) >= cast('2019-04-01' as date) and cast(redeem_date as date) <= cast('2019-04-30' as date)
			) as t
		inner join
			(select 
				deal_id
				from 
					user_edwprod.dim_gbl_deal_lob 
				where grt_l1_cat_name = 'L1 - Local' and grt_l2_cat_name <> 'L2 - Retail' 
				group by deal_id) gbl on gbl.deal_id = t.deal_uuid
		inner join 
			(select 
					product_uuid, 
					merchant_uuid 
				from 
					user_edwprod.dim_offer_ext 
				where inventory_service_name <> 'tpis' 
				group by product_uuid, merchant_uuid) c on t.deal_uuid = c.product_uuid
		group by t.user_uuid, t.deal_uuid, t.country_code, t.booked, t.order_date, t.redeem_date, t.first_redeem_date,t.parent_order_uuid) fin_ 
where accepted_redeemed_ord = 1 group by user_uuid, country_code,first_redeem_date;


drop table IF EXISTS grp_gdoop_bizops_db.nvp_2018_freq_one;
create table grp_gdoop_bizops_db.nvp_2018_freq_one stored as orc as
select 
	user_uuid, 
	booked_frst_ord,
	country_code,
	count(parent_order_uuid) count_of_order
from 
	(select 
		a.user_uuid,
		a.parent_order_uuid,
		a.order_date, 
		b.day_60, 
		b.booked_frst_ord,
		b.country_code,
		case when b.day_60 >= order_date then 1 else 0 end to_be_included
	from
	(select 
			t.user_uuid, 
			t.country_code,
			t.parent_order_uuid, 
			t.order_date
	from 
			(select 
				user_uuid, 
				country_code, 
				parent_order_uuid, 
				deal_uuid, 
				cast(order_date as date) order_date
			from grp_gdoop_bizops_db.rt_bt_txns 
				where cast(order_date as date) between cast('2018-04-01' as date) and cast('2018-07-05' as date)) t
		join
			(select 
				deal_id
			from user_edwprod.dim_gbl_deal_lob 
			where grt_l1_cat_name = 'L1 - Local' and grt_l2_cat_name <> 'L2 - Retail' 
			group by deal_id) gbl on gbl.deal_id = t.deal_uuid
		join 
			(select 
				product_uuid, 
				merchant_uuid 
			from 
				user_edwprod.dim_offer_ext 
			where inventory_service_name <> 'tpis' 
			group by product_uuid, merchant_uuid) c on t.deal_uuid = c.product_uuid 
		group by t.user_uuid, t.parent_order_uuid, t.order_date, t.country_code) a
	inner join 
	(select 
		user_uuid,
		country_code, 
		first_redeem_date,
		booked_frst_ord,
		date_add(first_redeem_date, 60) day_60
	from grp_gdoop_bizops_db.nvp_2018_txns_one) b on a.user_uuid = b.user_uuid and a.country_code = b.country_code) fin_ where fin_.to_be_included = 1 group by user_uuid, country_code,booked_frst_ord;




drop table IF EXISTS grp_gdoop_bizops_db.nvp_2019_freq_one;
create table grp_gdoop_bizops_db.nvp_2019_freq_one stored as orc as
select 
	user_uuid, 
	booked_frst_ord,
	country_code,
	count(parent_order_uuid) count_of_order
from 
	(select 
		a.user_uuid,
		a.parent_order_uuid,
		a.order_date, 
		b.day_60, 
		b.booked_frst_ord,
		b.country_code,
		case when b.day_60 >= order_date then 1 else 0 end to_be_included
	from
	(select 
			t.user_uuid, 
			t.country_code,
			t.parent_order_uuid, 
			t.order_date
	from 
			(select 
				user_uuid, 
				country_code, 
				parent_order_uuid, 
				deal_uuid, 
				cast(order_date as date) order_date
			from grp_gdoop_bizops_db.rt_bt_txns 
				where cast(order_date as date) between cast('2019-04-01' as date) and cast('2019-07-05' as date)) t
		join
			(select 
				deal_id
			from user_edwprod.dim_gbl_deal_lob 
			where grt_l1_cat_name = 'L1 - Local' and grt_l2_cat_name <> 'L2 - Retail' 
			group by deal_id) gbl on gbl.deal_id = t.deal_uuid
		join 
			(select 
				product_uuid, 
				merchant_uuid 
			from 
				user_edwprod.dim_offer_ext 
			where inventory_service_name <> 'tpis' 
			group by product_uuid, merchant_uuid) c on t.deal_uuid = c.product_uuid 
		group by t.user_uuid, t.parent_order_uuid, t.order_date, t.country_code) a
	inner join 
	(select 
		user_uuid,
		country_code, 
		first_redeem_date,
		booked_frst_ord,
		date_add(first_redeem_date, 60) day_60
	from grp_gdoop_bizops_db.nvp_2019_txns_one) b on a.user_uuid = b.user_uuid and a.country_code = b.country_code) fin_ where fin_.to_be_included = 1 group by user_uuid, country_code,booked_frst_ord;





-------
use grp_gdoop_bizops_db;
select * from nvp_init_purc_18;
drop table nvp_init_purc_18;
create table nvp_init_purc_18 stored as orc as
select user_uuid, first_order_dt, booked, country_code from (
select t.user_uuid, booked, country_code, l2, PRE_star_segment, first_order_dt, case when length(d.month_start) > 10 then substr(d.month_start,1,10) end month_start
from (
    select
        t.user_uuid,
        t.order_date,
        min(booked) booked,
        min(t.country_code) country_code,
        min(gdl.grt_l2_cat_name) l2,
        min(t.order_date) over (partition by t.user_uuid) first_order_dt,
        count(distinct t.parent_order_uuid) orders,
        sum(t.units) units,
        sum(t.nob) nob,
        sum(t.nor) nor
    from grp_gdoop_bizops_db.rt_bt_txns t
    join user_edwprod.dim_gbl_deal_lob gdl on gdl.deal_id = t.deal_uuid
    where t.order_date between '2018-12-01' and '2018-12-31'
    and grt_l1_cat_name = 'L1 - Local'
    group by
    t.user_uuid,
    t.order_date
  ) t
  left join user_groupondw.dim_day bh on bh.day_rw = t.first_order_dt
  left join user_groupondw.dim_month d on bh.month_key = d.month_key
) t1 where l2 = 'L2 - Food & Drink' and month_start = '2018-07-01'
group by user_uuid, first_order_dt, booked, country_code, PRE_star_segment
;


use grp_gdoop_bizops_db;
drop table rt_july18_freq;
create table rt_july18_freq stored as orc as
select
count(distinct a.user_uuid),
count(distinct case when b.order_date between date_add(first_order_dt, 1) and date_add(first_order_dt, 90) then b.parent_order_uuid end) 90_day_orders,
sum(case when b.order_date between date_add(first_order_dt, 1) and date_add(first_order_dt, 90) then b.units end) 90_day_units,
sum(case when b.order_date between date_add(first_order_dt, 1) and date_add(first_order_dt, 90) then b.nor end) 90_day_nor,
sum(case when b.order_date between date_add(first_order_dt, 1) and date_add(first_order_dt, 90) then b.nob end) 90_day_nob,
a.booked,
a.country_code
from grp_gdoop_bizops_db.rt_bt_txns b
join grp_gdoop_bizops_db.rt_init_purc_july18 a on a.user_uuid = b.user_uuid
join user_edwprod.dim_gbl_deal_lob gdl on gdl.deal_id = b.deal_uuid
where grt_l1_cat_name = 'L1 - Local'
and b.country_code <> 'US' and b.country_code <> 'CA'
group by
a.booked,
a.country_code
;

create table grp_gdoop_bizops_db.nvp_bt_txns stored as orc as
select 
	user_uuid, 
	parent_order_uuid, 
	country_code, 
	platform, 
	booked, 
	redeemed, 
	is_refunded, 
	is_expired, 
	ROW_NUMBER() over(partition by user_uuid, year(order_date), month(order_date) order by order_date) month_rank,
	ROW_NUMBER() over(partition by user_uuid order by order_date) overall_rank
from 
	(select 
		user_uuid user_uuid, 
		parent_order_uuid parent_order_uuid, 
		country_code, 
		platform, 
		booked, 
		redeemed, 
		is_refunded, 
		is_expired, 
		cast(order_date as date) order_date
	from grp_gdoop_bizops_db.rt_bt_txns where order_date >= '2018-12-01') as a;