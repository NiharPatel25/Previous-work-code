nvp_agg_dealview_nst

drop table grp_gdoop_bizops_db.nvp_agg_otherview_bh;


-----------60 day UDV
--Creating Empty tables
drop table if exists grp_gdoop_bizops_db.nvp_agg_dealview_bh2018;

create table grp_gdoop_bizops_db.nvp_agg_dealview_bh2018  (
    eventdate string,
	user_uuid string,
    platform string,
    deal_uuid string,
    l1 string,
    l2 string
) stored as orc
tblproperties ("orc.compress"="SNAPPY");

create table grp_gdoop_bizops_db.nvp_agg_otherview_bh2018  (
    eventdate string,
	user_uuid string,
    platform string,
    deal_uuid string,
    l1 string,
    l2 string
) stored as orc
tblproperties ("orc.compress"="SNAPPY");

create table grp_gdoop_bizops_db.nvp_agg_dealview_nst2018  (
    eventdate string,
	user_uuid string,
    platform string,
    deal_uuid string,
    l1 string,
    l2 string
) stored as orc
tblproperties ("orc.compress"="SNAPPY");

create table grp_gdoop_bizops_db.nvp_agg_otherview_nst2018  (
    eventdate string,
	user_uuid string,
    platform string,
    deal_uuid string,
    l1 string,
    l2 string
) stored as orc
tblproperties ("orc.compress"="SNAPPY");

create table grp_gdoop_bizops_db.nvp_agg_dealview_bh2019  (
    eventdate string,
	user_uuid string,
    platform string,
    deal_uuid string,
    l1 string,
    l2 string
) stored as orc
tblproperties ("orc.compress"="SNAPPY");

create table grp_gdoop_bizops_db.nvp_agg_otherview_bh2019  (
    eventdate string,
	user_uuid string,
    platform string,
    deal_uuid string,
    l1 string,
    l2 string
) stored as orc
tblproperties ("orc.compress"="SNAPPY");

drop table grp_gdoop_bizops_db.nvp_agg_dealview_nst2019;
create table grp_gdoop_bizops_db.nvp_agg_dealview_nst2019  (
    eventdate string,
	user_uuid string,
    platform string,
    deal_uuid string,
    l1 string,
    l2 string
) stored as orc
tblproperties ("orc.compress"="SNAPPY");

create table grp_gdoop_bizops_db.nvp_agg_otherview_nst2019  (
    eventdate string,
	user_uuid string,
    platform string,
    deal_uuid string,
    l1 string,
    l2 string
) stored as orc
tblproperties ("orc.compress"="SNAPPY");

-----
select count(*) from grp_gdoop_bizops_db.nvp_agg_dealview_nst2018;

drop table grp_gdoop_bizops_db.nvp_agg_dealview_bh2018
insert into grp_gdoop_bizops_db.nvp_agg_dealview_bh2018
select event_date, user_uuid, platform, deal_uuid, l1,l2 from 
(select
            b.dt event_date,
            a.user_uuid,
            case when lower(b.platform) = 'web' then 'desktop' else lower(b.platform) end as platform,
            b.deal_uuid,
            gbl.grt_l1_cat_name l1,
            gbl.grt_l2_cat_name l2
from grp_gdoop_bizops_db.nvp_2018_txns a
join user_groupondw.bld_events b on lower(a.user_uuid) = lower(b.user_uuid) and b.page_country = a.country_code
join user_edwprod.dim_gbl_deal_lob gbl on lower(b.deal_uuid) = lower(gbl.deal_id)
where cast(b.dt as date) between cast('2019-02-01' as date) and cast('2019-03-05' as date)
and cast(b.dt as date) between date_add(a.first_redeem_date,1) and date_add(a.first_redeem_date,60)
and b.bot_flag = 0
and lower(b.platform) in ('web','desktop','touch') ) fin_ group by event_date, user_uuid, platform, deal_uuid, l1, l2;


insert into grp_gdoop_bizops_db.nvp_agg_otherview_bh2018
select event_date, user_uuid, platform, deal_uuid, l1,l2 from 
(select
            b.dt event_date,
            a.user_uuid,
            case when lower(b.platform) = 'web' then 'desktop' else lower(b.platform) end as platform,
            null as deal_uuid,
            null as l1,
            null as l2
from grp_gdoop_bizops_db.nvp_2018_txns a
join user_groupondw.bld_events b on lower(a.user_uuid) = lower(b.user_uuid) and b.page_country = a.country_code
where cast(b.dt as date) between cast('2018-12-01' as date) and cast('2018-12-31' as date)
and cast(b.dt as date) between date_add(a.first_redeem_date,1) and date_add(a.first_redeem_date,60)
and b.bot_flag = 0
and lower(b.platform) in ('web','desktop','touch') ) fin_ group by event_date, user_uuid, platform, deal_uuid, l1, l2;


 
insert into grp_gdoop_bizops_db.nvp_agg_dealview_nst2018
select event_date, user_uuid, platform, deal_uuid, l1,l2 from
(select d.eventdate event_date, 
c.user_uuid,
case when lower(d.platform) in ('ipad','iphone') then 'ios' else lower(d.platform) end as platform,
d.deal_uuid,
gbl_2.grt_l1_cat_name l1,
gbl_2.grt_l2_cat_name l2
from grp_gdoop_bizops_db.nvp_2018_txns c
join user_groupondw.m_raw_dealview d on lower(c.user_uuid) = lower(d.consumer_id) and d.countrycode = c.country_code
join user_edwprod.dim_gbl_deal_lob gbl_2 on lower(d.deal_uuid) = lower(gbl_2.deal_id)
where cast(d.eventdate as date) between cast('2019-02-01' as date) and cast('2019-03-05' as date)
and cast(d.eventdate as date) between date_add(c.first_redeem_date,1) and date_add(c.first_redeem_date,60)) fin_ group by event_date, user_uuid, platform, deal_uuid, l1, l2;


insert into grp_gdoop_bizops_db.nvp_agg_otherview_nst2018
select event_date, user_uuid, platform, deal_uuid, l1,l2 from
(select d.eventdate event_date, 
c.user_uuid,
case when lower(d.platform) in ('ipad','iphone') then 'ios' else lower(d.platform) end as platform,
null as deal_uuid,
null as l1,
null as l2
from grp_gdoop_bizops_db.nvp_2018_txns c
join user_groupondw.m_raw_pageview d on lower(c.user_uuid) = lower(d.consumer_id) and d.countrycode = c.country_code
where cast(d.eventdate as date) between cast('2019-02-01' as date) and cast('2019-03-05' as date)
and cast(d.eventdate as date) between date_add(c.first_redeem_date,1) and date_add(c.first_redeem_date,60)) fin_ group by event_date, user_uuid, platform, deal_uuid, l1, l2;


insert into grp_gdoop_bizops_db.nvp_agg_dealview_bh2019
select event_date, user_uuid, platform, deal_uuid, l1,l2 from 
(select
            b.dt event_date,
            a.user_uuid,
            case when lower(b.platform) = 'web' then 'desktop' else lower(b.platform) end as platform,
            b.deal_uuid,
            gbl.grt_l1_cat_name l1,
            gbl.grt_l2_cat_name l2
from grp_gdoop_bizops_db.nvp_2019_txns a
join user_groupondw.bld_events b on lower(a.user_uuid) = lower(b.user_uuid) and b.page_country = a.country_code
join user_edwprod.dim_gbl_deal_lob gbl on lower(b.deal_uuid) = lower(gbl.deal_id)
where cast(b.dt as date) between cast('2020-02-01' as date) and cast('2020-03-05' as date)
and cast(b.dt as date) between date_add(a.first_redeem_date,1) and date_add(a.first_redeem_date,60)
and b.bot_flag = 0
and lower(b.platform) in ('web','desktop','touch') ) fin_ group by event_date, user_uuid, platform, deal_uuid, l1, l2;

insert into grp_gdoop_bizops_db.nvp_agg_otherview_bh2019
select event_date, user_uuid, platform, deal_uuid, l1,l2 from 
(select
            b.dt event_date,
            a.user_uuid,
            case when lower(b.platform) = 'web' then 'desktop' else lower(b.platform) end as platform,
            null as deal_uuid,
            null as l1,
            null as l2
from grp_gdoop_bizops_db.nvp_2019_txns a
join user_groupondw.bld_events b on lower(a.user_uuid) = lower(b.user_uuid) and b.page_country = a.country_code
where cast(b.dt as date) between cast('2019-02-01' as date) and cast('2019-03-05' as date)
and cast(b.dt as date) between date_add(a.first_redeem_date,1) and date_add(a.first_redeem_date,60)
and b.bot_flag = 0
and lower(b.platform) in ('web','desktop','touch') ) fin_ group by event_date, user_uuid, platform, deal_uuid, l1, l2;



insert into grp_gdoop_bizops_db.nvp_agg_dealview_nst2019
select event_date, user_uuid, platform, deal_uuid, l1,l2 from
(select d.eventdate event_date, 
c.user_uuid,
case when lower(d.platform) in ('ipad','iphone') then 'ios' else lower(d.platform) end as platform,
d.deal_uuid,
gbl_2.grt_l1_cat_name l1,
gbl_2.grt_l2_cat_name l2
from grp_gdoop_bizops_db.nvp_2019_txns c
join user_groupondw.m_raw_dealview d on lower(c.user_uuid) = lower(d.consumer_id) and d.countrycode = c.country_code
join user_edwprod.dim_gbl_deal_lob gbl_2 on lower(d.deal_uuid) = lower(gbl_2.deal_id)
where cast(d.eventdate as date) between cast('2020-02-01' as date) and cast('2020-03-05' as date)
and cast(d.eventdate as date) between date_add(c.first_redeem_date,1) and date_add(c.first_redeem_date,60)) fin_ group by event_date, user_uuid, platform, deal_uuid, l1, l2;


insert into grp_gdoop_bizops_db.nvp_agg_otherview_nst2019
select event_date, user_uuid, platform, deal_uuid, l1,l2 from
(select d.eventdate event_date, 
c.user_uuid,
case when lower(d.platform) in ('ipad','iphone') then 'ios' else lower(d.platform) end as platform,
null as deal_uuid,
null as l1,
null as l2
from grp_gdoop_bizops_db.nvp_2019_txns c
join user_groupondw.m_raw_pageview d on lower(c.user_uuid) = lower(d.consumer_id) and d.countrycode = c.country_code
where cast(d.eventdate as date) between cast('2020-02-01' as date) and cast('2020-03-05' as date)
and cast(d.eventdate as date) between date_add(c.first_redeem_date,1) and date_add(c.first_redeem_date,60)) fin_ group by event_date, user_uuid, platform, deal_uuid, l1, l2;



------UNION CASE

create table grp_gdoop_bizops_db.nvp_2018_all stored as orc as
select * from (
select * from grp_gdoop_bizops_db.nvp_agg_dealview_bh2018
UNION ALL
select * from grp_gdoop_bizops_db.nvp_agg_dealview_nst2018
UNION ALL
select * from grp_gdoop_bizops_db.nvp_agg_otherview_bh2018
UNION ALL 
select * from grp_gdoop_bizops_db.nvp_agg_otherview_nst2018
) fin_;

create table grp_gdoop_bizops_db.nvp_2019_all stored as orc as
select * from (
select * from grp_gdoop_bizops_db.nvp_agg_dealview_bh2019
UNION ALL
select * from grp_gdoop_bizops_db.nvp_agg_dealview_nst2019
UNION ALL 
select * from grp_gdoop_bizops_db.nvp_agg_otherview_bh2019
UNION ALL
select * from grp_gdoop_bizops_db.nvp_agg_otherview_nst2019
) fin_; 

------EXPERIMENTING WITH SUPER FUNNEL



create table grp_gdoop_bizops_db.nvp_model_deal_info stored as orc as
select 
	deal_uuid, 
	purchase_case, 
	sum(udv_int) sum_udv,
	count(distinct udvisitor_date_deal_id_udv) count_udv,
	count(distinct bcook_date_deal_id_udv) count_bcook_udv,
	count(distinct bcook_date_udvv) count_bcook_udvv
from 
(select 
	a.deal_uuid deal_uuid,
	case when e.user_uuid is not null and e.first_purchase < a.event_date then 'old_client' else 'first_time_client' end purchase_case, 
	a.unique_deal_views udv_int,
	a.udvisitor_date_deal_id_udv as udvisitor_date_deal_id_udv,
	a.bcook_date_deal_id_udv as bcook_date_deal_id_udv,
	a.bcook_date_udvv as bcook_date_udvv
from 
	(select 
		edw_modified_ts, 
		cookie_b,
		deal_uuid,
		cast(event_date as date) event_date,  
		unique_deal_views,
		CONCAT(cast(unique_dv_visitors as string),'+', cast(event_date as string),'+', cast(deal_uuid as string)) udvisitor_date_deal_id_udv,
		CONCAT(cast(cookie_b as string), '+', cast(event_date as string),'+', cast(deal_uuid as string)) bcook_date_deal_id_udv, 
		CONCAT(cast(cookie_b as string), '+',cast(event_date as string)) as bcook_date_udvv
		from 
		user_groupondw.gbl_traffic_superfunnel_deal where cast(event_date as date) >= cast('2019-09-01' as date) and cast(event_date as date) <= cast('2019-09-30'as date) and deal_uuid is not null
	) a 
	inner join 
	(select 
		deal_id as deal_uuid
		from
		user_edwprod.dim_gbl_deal_lob where grt_l1_cat_name = 'L1 - Local'
	) as b on a.deal_uuid = b.deal_uuid
	left join 
	(select 
		product_uuid, 
		max(merchant_uuid) as merchant_uuid
		from 
		user_edwprod.dim_offer_ext group by product_uuid
	) as c on a.deal_uuid = c.product_uuid
	left join 
	( select 
		bcookie,
		user_uuid,
		cast(event_date as date) event_date2
		from 
		user_groupondw.user_bcookie_mapping where cast(event_date as date) >= cast('2019-09-01' as date) and cast(event_date as date) <= cast('2019-09-30'as date)
	) as d on a.cookie_b = d.bcookie and a.event_date = d.event_date2
	left join 
	(select 
		user_uuid, 
		merchant_uuid,
		cast(first_purchase as date) first_purchase
		from
		grp_gdoop_bizops_db.nvp_model_user_info
	) as e on d.user_uuid = e.user_uuid and c.merchant_uuid = e.merchant_uuid) fin_ group by deal_uuid, purchase_case;