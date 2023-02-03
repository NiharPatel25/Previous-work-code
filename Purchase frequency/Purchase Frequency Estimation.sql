
select 
	booked_2018, 
	booked_2019,
	count(distinct user_uuid) total_users,
	sum(total_orders_2018) orders_2018, 
	sum(total_orders_2019) orders_2019, 
	sum(total_units_2018) units_2018, 
	sum(total_units_2019) units_2019
from 
(select 
	fina.user_uuid,
	fina.total_orders_2018, 
	fina.booked_2018, 
	fina.total_units_2018,
	finb.total_orders_2019, 
	finb.booked_2019,
	finb.total_units_2019
from 
(select 
	base.user_uuid,
	base.booked booked_2018,
	count(distinct(x.parent_order_uuid)) total_orders_2018,
	sum(x.units) total_units_2018
	from 
	(select 
		a.user_uuid, max(a.booked) booked
		from 
		grp_gdoop_bizops_db.nvp_2018qtr_red a 
		join grp_gdoop_bizops_db.nvp_2019qtr_red b on a.user_uuid = b.user_uuid and a.country_code <> 'US' and a.country_code <> 'CA'
		group by a.user_uuid) as base 
	left join 
	grp_gdoop_bizops_db.nvp_2018qtr x on x.user_uuid = base.user_uuid and x.country_code <> 'CA' and x.country_code <> 'US'
	group by base.user_uuid, base.booked) fina
left join
(select 
	base.user_uuid,
	base.booked booked_2019, 
	count(distinct(y.parent_order_uuid)) total_orders_2019,
	sum(y.units) total_units_2019
	from
	(select b.user_uuid, max(b.booked) booked 
		from 
		grp_gdoop_bizops_db.nvp_2018qtr_red a 
		join grp_gdoop_bizops_db.nvp_2019qtr_red b on a.user_uuid = b.user_uuid and a.country_code <> 'US' and a.country_code <> 'CA'
		group by b.user_uuid) as base
	left join grp_gdoop_bizops_db.nvp_2019qtr y on y.user_uuid = base.user_uuid and y.country_code <> 'CA' and y.country_code <> 'US'
	group by base.user_uuid, base.booked) finb on fina.user_uuid = finb.user_uuid) fin_ group by booked_2019, booked_2018 order by booked_2018 desc, booked_2019 desc;


select 
   booked_2018,
   count(user_uuid) total_users,
   count(distinct user_uuid) total_user2,
   sum(total_orders_2018) total_parent_orders,
   sum(total_units_2018) total_units,
   var_pop(total_units_2018) variance
from 
(select 
	base.user_uuid,
	base.booked booked_2018,
	count(distinct(x.parent_order_uuid)) total_orders_2018,
	sum(x.units) total_units_2018
	from 
	(select 
		a.user_uuid, max(a.booked) booked
		from 
		grp_gdoop_bizops_db.nvp_2018qtr_red a 
		left join grp_gdoop_bizops_db.nvp_2019qtr_red b on a.user_uuid = b.user_uuid and a.country_code <> 'US' and a.country_code <> 'CA'
		where b.user_uuid is null
		group by a.user_uuid) as base 
	left join 
	grp_gdoop_bizops_db.nvp_2018qtr x on x.user_uuid = base.user_uuid and x.country_code <> 'CA' and x.country_code <> 'US'
	group by base.user_uuid, base.booked) fin group by booked_2018;


	
select 
   booked_2019,
   count(user_uuid) total_users,
   count(distinct user_uuid) total_user2,
   sum(total_orders_2019) total_parent_orders,
   sum(total_units_2019) total_units,
   var_pop(total_units_2019) variance
from
(select 
	base.user_uuid,
	base.booked booked_2019, 
	count(distinct(y.parent_order_uuid)) total_orders_2019,
	sum(y.units) total_units_2019
	from
	(select b.user_uuid, max(b.booked) booked 
		from 
		grp_gdoop_bizops_db.nvp_2018qtr_red a 
		right join grp_gdoop_bizops_db.nvp_2019qtr_red b on a.user_uuid = b.user_uuid and a.country_code <> 'US' and a.country_code <> 'CA'
		where a.user_uuid is null
		group by b.user_uuid) as base
	left join grp_gdoop_bizops_db.nvp_2019qtr y on y.user_uuid = base.user_uuid and y.country_code <> 'CA' and y.country_code <> 'US'
	group by base.user_uuid, base.booked) fin group by booked_2019;
	
select count(distinct user_uuid) from grp_gdoop_bizops_db.nvp_2018qtr_red;

--------VARIANCE POP
select 
	count(user_uuid) total_users, 
	sum(total_units_2018) total_units_2018, 
	sum(total_units_2019) total_units_2019, 
	var_pop(total_units_2018) variance_2018, 
	var_pop(total_units_2019) variance_2019
from
(select 
	fina.user_uuid,
	fina.total_orders_2018, 
	fina.booked_2018, 
	fina.total_units_2018,
	finb.total_orders_2019, 
	finb.booked_2019,
	finb.total_units_2019
from 
(select 
	base.user_uuid,
	base.booked booked_2018,
	count(distinct(x.parent_order_uuid)) total_orders_2018,
	sum(x.units) total_units_2018
	from 
	(select 
		a.user_uuid, max(a.booked) booked
		from 
		grp_gdoop_bizops_db.nvp_2018qtr_red a 
		join grp_gdoop_bizops_db.nvp_2019qtr_red b on a.user_uuid = b.user_uuid and a.country_code <> 'US' and a.country_code <> 'CA'
		group by a.user_uuid) as base 
	left join 
	grp_gdoop_bizops_db.nvp_2018qtr x on x.user_uuid = base.user_uuid and x.country_code <> 'CA' and x.country_code <> 'US'
	group by base.user_uuid, base.booked) fina
full join
(select 
	base.user_uuid,
	base.booked booked_2019, 
	count(distinct(y.parent_order_uuid)) total_orders_2019,
	sum(y.units) total_units_2019
	from
	(select b.user_uuid, max(b.booked) booked 
		from 
		grp_gdoop_bizops_db.nvp_2018qtr_red a 
		join grp_gdoop_bizops_db.nvp_2019qtr_red b on a.user_uuid = b.user_uuid and a.country_code <> 'US' and a.country_code <> 'CA'
		group by b.user_uuid) as base
	left join grp_gdoop_bizops_db.nvp_2019qtr y on y.user_uuid = base.user_uuid and y.country_code <> 'CA' and y.country_code <> 'US'
	group by base.user_uuid, base.booked) finb on fina.user_uuid = finb.user_uuid) final_ where final_.booked_2018 = 0 and final_.booked_2019 = 1;



----- Avg Frequency for people who redeemed in the Q4. 2018/2019



select 
   a.booked,
   count(distinct a.user_uuid) total_users,
   sum(x.units) total_units
 from
(select 
    user_uuid, max(booked) booked
    from 
    grp_gdoop_bizops_db.nvp_2018qtr_red where country_code <> 'CA' and country_code <> 'US' group by user_uuid) as a
left join 
grp_gdoop_bizops_db.nvp_2018qtr x on x.user_uuid = a.user_uuid and x.country_code <> 'CA' and x.country_code <> 'US'
group by a.booked order by booked desc;

----VARIANCE POP

select 
   booked,
   var_pop(total_units)
from
(select 
      a.user_uuid, 
      a.booked, 
      sum(x.units) total_units
     from
       (select 
         user_uuid, max(booked) booked
         from 
         grp_gdoop_bizops_db.nvp_2018qtr_red where country_code <> 'CA' and country_code <> 'US' group by user_uuid) as a
      left join 
         grp_gdoop_bizops_db.nvp_2018qtr x on x.user_uuid = a.user_uuid and x.country_code <> 'CA' and x.country_code <> 'US'
    group by a.user_uuid, a.booked) fin group by booked;



----Number of people who did not make a repeat purchase

select 
  booked, 
  is_null, 
  count(distinct user_uuid) total_users,
  sum(total_units)
  from
(select 
   user_uuid, 
   booked, 
   case when total_units is null then 1 else 0 end is_null, 
   total_units
from
   (select 
      a.user_uuid, 
      a.booked, 
      sum(x.units) total_units
     from
       (select 
         user_uuid, max(booked) booked
         from 
         grp_gdoop_bizops_db.nvp_2018qtr_red where country_code <> 'CA' and country_code <> 'US' group by user_uuid) as a
      left join 
         grp_gdoop_bizops_db.nvp_2018qtr x on x.user_uuid = a.user_uuid and x.country_code <> 'CA' and x.country_code <> 'US'
    group by a.user_uuid, a.booked) fin
  ) fin2 group by booked, is_null order by booked desc, is_null desc
;


select 
month(cast(order_date as date)),year(cast(order_date as date)),
min(order_date), 
max(order_date)
from 
(select order_date from grp_gdoop_bizops_db.sh_bt_txns where order_date is not null) as f 
group by month(cast(order_date as date)),year(cast(order_date as date)) 
order by year(cast(order_date as date)) asc, month(cast(order_date as date)) asc; 


-----grp_gdoop_bizops_db.nvp_2018_local_yr_red
-----grp_gdoop_bizops_db.nvp_2018_local_yr



select 
   a.booked,
   count(distinct a.user_uuid) total_users,
   sum(x.units) total_units
 from
(select 
    user_uuid, max(booked) booked
    from 
    grp_gdoop_bizops_db.nvp_2019_local_yr_red where country_code <> 'CA' and country_code <> 'US' group by user_uuid) as a
left join 
grp_gdoop_bizops_db.nvp_2019_local_yr x on x.user_uuid = a.user_uuid and x.country_code <> 'CA' and x.country_code <> 'US'
group by a.booked order by a.booked desc;



select 
   booked,
   count(distinct user_uuid) user_uuids,
   sum(total_units) units,
   var_pop(total_units)
from
(select 
      a.user_uuid, 
      a.booked, 
      sum(x.units) total_units
     from
       (select 
         user_uuid, 
         max(booked) booked
         from 
         grp_gdoop_bizops_db.nvp_2019_local_yr_red where country_code <> 'CA' and country_code <> 'US' group by user_uuid) as a
       left join 
         grp_gdoop_bizops_db.nvp_2019_local_yr x on x.user_uuid = a.user_uuid and x.country_code <> 'CA' and x.country_code <> 'US'
    group by a.user_uuid, a.booked) fin group by booked;



select 
  booked, 
  is_null, 
  count(distinct user_uuid) total_users,
  sum(total_units)
  from
(select 
   user_uuid, 
   booked, 
   case when total_units is null then 1 else 0 end is_null, 
   total_units
from
   (select 
      a.user_uuid, 
      a.booked, 
      sum(x.units) total_units
     from
       (select 
         user_uuid, max(booked) booked
         from 
         grp_gdoop_bizops_db.nvp_2019_local_yr_red where country_code <> 'CA' and country_code <> 'US' group by user_uuid) as a
      left join 
         grp_gdoop_bizops_db.nvp_2019_local_yr x on x.user_uuid = a.user_uuid and x.country_code <> 'CA' and x.country_code <> 'US'
    group by a.user_uuid, a.booked) fin
  ) fin2 group by booked, is_null order by booked desc, is_null desc
;