-- supply trends
SET hive.cli.print.header = true;
SET hive.default.fileformat = Orc;
SET hive.groupby.orderby.position.alias = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET mapred.task.timeout = 1200000;SET hive.tez.container.size = 12288;
SET hive.tez.java.opts =-Xmx9000M;
SET hive.exec.max.dynamic.partitions.pernode = 19000;
SET hive.exec.max.dynamic.partitions = 19000;
SET hive.auto.convert.join.noconditionaltask.size = 3862953984;
set hive.limit.query.max.table.partition = 5000;
use grp_gdoop_bizops_db;



insert overwrite table grp_gdoop_bizops_db.jw_bt_supply_trends partition (load_date)
	select
		case 
			when g.deal_uuid is not null then 1 
			else 0 
		end bt_eligible,
		gdl.grt_l2_cat_name L2,
		pds.pds_cat_name PDS,
		gdl.country_code,
		count(distinct ad.deal_uuid) deals,
		ad.load_date
	from (
		select 
			deal_uuid, 
			load_date
		from prod_groupondw.active_deals
		where 
			sold_out = 'false' 
			and available_qty > 0
			and load_date between date_sub(current_date,8) and date_sub(current_date,1)
		group by deal_uuid, load_date) ad
	left join (
		select 
			load_date, 
			deal_uuid
		from grp_gdoop_bizops_db.sh_bt_active_deals_log
		where 
			partner_inactive_flag = 0 
			and product_is_active_flag = 1
		group by load_date, deal_uuid) g on g.deal_uuid = ad.deal_uuid and ad.load_date = g.load_date
	join (
		select distinct 
			deal_uuid 
		from edwprod.deal_merch_product 
		where 
			inv_service_id = 'vis') v on v.deal_uuid = ad.deal_uuid
	left join user_edwprod.dim_gbl_deal_lob gdl on gdl.deal_id = ad.deal_uuid
	left join user_dw.v_dim_pds_grt_map pds on gdl.pds_cat_id = pds.pds_cat_id
	where 
		gdl.grt_l1_cat_name = 'L1 - Local'
	group by 
		case when g.deal_uuid is not null then 1 else 0 end, gdl.grt_l2_cat_name, pds_cat_name , gdl.country_code, ad.load_date


-----  ----- 
-- table 2
-- Hive_Asterix_Table


use grp_gdoop_bizops_db;

create temporary table jw_live stored as orc as
	select 
		a.country,
		a.load_date as report_date,
		coalesce(ex.exclusion_type,'None') as exclusion_type,
		coalesce(gdl.grt_l2_cat_name,'unknown') as L2,
		coalesce(gdl.grt_l3_cat_name,'unknown') as L3,
		coalesce(pds.pds_cat_name,'unknown') as PDS,
		do.merchant_uuid,
		COALESCE(p.new_merchant,'0') as new_merchant_flag,
		a.deal_uuid
	from grp_gdoop_bizops_db.sh_bt_active_deals_log_v3 a
	left join
		(select distinct
			deal_id,
			grt_l2_cat_name, 
			grt_l3_cat_name,
			pds_cat_id
		from user_edwprod.dim_gbl_deal_lob) gdl on a.deal_uuid = gdl.deal_id
	left join 
		(select distinct
			merchant_uuid,
			product_uuid as deal_uuid
		from user_edwprod.dim_offer) do on a.deal_uuid = do.deal_uuid 
	left join
		(select distinct
			pds_cat_id,
			exclusion_type
		from grp_gdoop_bizops_db.jj_deal_adoption_exclusions) ex on gdl.pds_cat_id = ex.pds_cat_id 
	left join
		(select distinct
			pds_cat_id,
			pds_cat_name
		from user_dw.v_dim_pds_grt_map) pds on gdl.pds_cat_id = pds.pds_cat_id
	left join 
		(select
			merchant_uuid,
			new_merchant
		from grp_gdoop_bizops_db.sh_bt_partners
		where 
			length(merchant_uuid) = '36'
			and new_merchant = '1') p on do.merchant_uuid = p.merchant_uuid
	where
		is_bookable = '1'
		and partner_inactive_flag = '0'
		and product_is_active_flag = '1'
		and sold_out = 'false'
		and a.deal_uuid not in ('7d230fa6-e7a9-42c8-b713-1d9a0b21792b','f702d482-3067-43f0-9fa4-086d060ddc7b','3f60ba0c-6bbc-499c-8e1e-32bb9d34dd7e','84ff87a8-31b9-40a3-986c-948a8e297b2a')
		and load_date between date_sub(current_date,3) and date_sub(current_date,1);

create temporary table jw_opt_in stored as orc as
	select
		a.deal_uuid,
		max(cast(to_date(a.new_bt_opt_in_date) as date)) as report_date
	from grp_gdoop_bizops_db.sh_bt_active_deals_log_v3 a 
	where
		cast(to_date(a.new_bt_opt_in_date) as date) is not null
	group by a.deal_uuid;



	insert overwrite table grp_gdoop_bizops_db.jw_asterix partition (report_date)
	select
		a.country,
		a.exclusion_type,
		a.L2,
		a.L3,
		a.PDS,
		a.new_merchant_flag,
		a.merchant_uuid,
		a.deal_uuid,
		case when a.report_date >= b.report_date then '1' else '0' end as total_opt_in_flag,
		case when a.report_date = b.report_date then '1' else '0' end as daily_opt_in_flag,
		a.report_date
	from jw_live a
	left join jw_opt_in b on
		a.deal_uuid = b.deal_uuid;
drop table jw_live purge;
drop table jw_opt_in purge

-----  ----- 
-- table  3
--------------------------------------------------------------- ATTRITION


USE grp_gdoop_bizops_db;
drop table grp_gdoop_bizops_db.np_booking_solution;
create table grp_gdoop_bizops_db.np_booking_solution stored as orc as 
    select o2.deal_uuid,
        max(sfa.id) sf_account_id,
        max(case when lower(sfa.scheduler_setup_type) = 'pen & paper' then 'pen & paper'
            when sfa.scheduler_setup_type is null then 'no data'
            else 'some booking tool'
            end) current_booking_solution,
        max(sfa.name) account_name,
        max(company_type) company_type
    from dwh_base_sec_view.opportunity_1 o1
    join dwh_base_sec_view.opportunity_2 o2 on o1.id = o2.id
    join dwh_base_sec_view.sf_account sfa on o1.accountid = sfa.id
    group by o2.deal_uuid;
    
   
----------
   
drop table grp_gdoop_bizops_db.np_booking_scope_deals;
create table grp_gdoop_bizops_db.np_booking_scope_deals stored as orc as 
select 
    t.deal_uuid,
    to_date(dw.week_end)  as week_end_date,
    date_sub(to_date(dw.week_end) , 7) previous_week,
    date_add(to_date(dw.week_end ), 7) next_week,
    max(is_bookable) as is_bookable,
    max(partner_inactive_flag) as partner_inactive_flag,
    max(product_is_active_flag) as product_is_active_flag,
    max(t.country) as country,
    max(case when gdl.country_code = 'US' then 'NAM' else 'INTL' end) region,
    max(grt_l1_cat_name) l1,
    max(grt_l2_cat_description) l2,
    max(grt_l3_cat_description) l3,
    max(bs.current_booking_solution) as current_booking_solution
from grp_gdoop_bizops_db.sh_bt_active_deals_log t
join(
    select deal_uuid, load_date
    from user_groupondw.active_deals
    where sold_out = 'false' 
    and available_qty > 0
    and load_date >= '2020-01-01'
    group by deal_uuid, load_date
    ) ad on t.deal_uuid = ad.deal_uuid and ad.load_date = t.load_date
join user_dw.v_dim_week dw on to_date(dw.week_end ) =t.load_date
join user_edwprod.dim_gbl_deal_lob gdl on gdl.deal_id = t.deal_uuid
join (select deal_uuid, current_booking_solution
from grp_gdoop_bizops_db.np_booking_solution
     )as bs on bs.deal_uuid = t.deal_uuid
where partner_inactive_flag = 0 
     and product_is_active_flag = 1
group by t.deal_uuid,to_date(dw.week_end),date_sub(to_date(dw.week_end) , 7) ,date_add(to_date(dw.week_end ), 7);


-------------

create table grp_gdoop_bizops_db.np_bt_attrition_agg stored as orc as
select
    live.week_end_date,
    live.region,
    live.l1,
    live.l2,
    live.l3,
    live.current_booking_solution,
    sum(live.accts) live_deals,
    sum(adds.accts) added_deals,
    sum(lost.accts) lost_deals,
    sum(lost.left_bt_accts) left_bt_deals,
    sum(lost.left_grpn_accts) left_grpn_deals
from (
select week_end_date,
      region,
      l1,
      l2,
      l3,
      current_booking_solution,
      count(distinct deal_uuid) accts
     from grp_gdoop_bizops_db.np_booking_scope_deals group by week_end_date, region, l1, l2, l3, current_booking_solution
) as live
left join (
    select bsd.week_end_date,
    bsd.region,
    bsd.l1,
    bsd.l2,
    bsd.l3,
    bsd.current_booking_solution,
    count(distinct bsd.deal_uuid) accts
    from grp_gdoop_bizops_db.np_booking_scope_deals bsd
    left join grp_gdoop_bizops_db.np_booking_scope_deals bsd2 on cast(bsd.week_end_date as date) = cast(bsd2.next_week as date) and bsd.deal_uuid = bsd2.deal_uuid
    where bsd2.deal_uuid is null group by bsd.week_end_date, bsd.region, bsd.l1, bsd.l2, bsd.l3, bsd.current_booking_solution) as adds on live.week_end_date = adds.week_end_date and live.region = adds.region and live.l1 = adds.l1 and live.l2 = adds.l2 and live.l3 = adds.l3 and live.current_booking_solution = adds.current_booking_solution 
left join (select
--bds.week_end_date + interval '7' day as week_end_date,
        date_add (bds.week_end_date ,7) as week_end_date,
        bds.region,
        bds.l1,
        bds.l2,
        bds.l3,
        bds.current_booking_solution,
        count(distinct bds.deal_uuid) accts,
        count(distinct case when btad.partner_inactive_flag = 1 or btad.product_is_active_flag = 0 then bds.deal_uuid end) left_bt_accts,
        count(distinct case when btad.partner_inactive_flag is null then bds.deal_uuid end) left_grpn_accts
    from grp_gdoop_bizops_db.np_booking_scope_deals bds
    left join grp_gdoop_bizops_db.np_booking_scope_deals bds3 on cast(bds.week_end_date as date)= cast(bds3.previous_week as date) and bds.deal_uuid = bds3.deal_uuid
    left join grp_gdoop_bizops_db.sh_bt_active_deals_log btad on date_add(bds.week_end_date ,7) = btad.load_date and bds.deal_uuid = btad.deal_uuid
    where bds3.deal_uuid is null 
    group by bds.week_end_date, bds.region, bds.l1, bds.l2, bds.l3, bds.current_booking_solution
    ) lost on live.week_end_date = lost.week_end_date and live.region = lost.region and live.l1 = lost.l1 and live.l2 = lost.l2 and live.l3 = lost.l3 and live.current_booking_solution = lost.current_booking_solution 	
group by live.week_end_date,
    live.region,
    live.l1,
    live.l2,
    live.l3,
    live.current_booking_solution;

   
   
drop table grp_gdoop_bizops_db.np_lost_deals;
create table grp_gdoop_bizops_db.np_lost_deals stored as orc as
select
        date_add (bds.week_end_date ,7) as week_end_date,
        bds.region,
        bds.l1,
        bds.l2,
        bds.l3,
        bds.current_booking_solution,
        bds.deal_uuid,
        case when btad.partner_inactive_flag = 1 or btad.product_is_active_flag = 0 then bds.deal_uuid end left_bt_accts,
        case when btad.partner_inactive_flag is null then bds.deal_uuid end left_grpn_accts
    from grp_gdoop_bizops_db.np_booking_scope_deals bds
    left join grp_gdoop_bizops_db.np_booking_scope_deals bds3 on cast(bds.week_end_date as date)= cast(bds3.previous_week as date) and bds.deal_uuid = bds3.deal_uuid
    left join grp_gdoop_bizops_db.sh_bt_active_deals_log btad on date_add(bds.week_end_date ,7) = btad.load_date and bds.deal_uuid = btad.deal_uuid
    where bds3.deal_uuid is null;
   
select * from grp_gdoop_bizops_db.np_lost_deals;
   
select * from grp_gdoop_bizops_db.np_lost_deals where region = 'NAM' and cast(week_end_date as date) = cast('2021-04-18' as date);

drop table grp_gdoop_bizops_db.np_lost_deals2;
create table grp_gdoop_bizops_db.np_lost_deals2 stored as orc as
select 
  fin.*, 
  date_sub(next_day(min_load_date, 'MON'), 1) min_load_week
  from
(select 
    deal_uuid, 
    country,
    min(cast(load_date as date)) min_load_date
    from 
    grp_gdoop_bizops_db.sh_bt_active_deals_log
    where 
         partner_inactive_flag = 1 
         or product_is_active_flag = 0
         group by deal_uuid, country) as fin where country = 'US';

select * from grp_gdoop_bizops_db.np_lost_deals2 where min_load_week >= cast('2021-01-01' as date);


select * from grp_gdoop_bizops_db.sh_bt_active_deals_log where deal_uuid = '7063fac4-aaa6-47c1-9cb6-8c86aefaa8e2' order by load_date desc;
select * from grp_gdoop_bizops_db.sh_bt_active_deals_log where deal_uuid = '2b9648cf-9d49-4a7e-9df8-93a70dfbd64e' order by load_date desc;
select * from grp_gdoop_bizops_db.sh_bt_active_deals_log where deal_uuid = 'c39b57c1-e61e-4357-8138-362659d68cbf' order by load_date desc;
select * from grp_gdoop_bizops_db.sh_bt_active_deals_log where deal_uuid = 'f662b401-64ba-493f-be7e-9bce36715756' order by load_date desc;


select * from user_groupondw.active_deals where deal_uuid = 'c9f50afa-effc-4ad6-b0fe-809c76e3bdb5' order by load_date desc;
select * from user_dw.v_dim_week;
-----  -----wangy_db-- section 4 
-- Pause reason 

create  TEMPORARY table jk_bt_paused_reason as 
    select deal_uuid, max(o2.booking_pause_reason__c) booking_pause_reason, max(sfa.id) account_id
    from edwprod.sf_opportunity_2 o2
    join edwprod.sf_opportunity_1 o1 on o2.id = o1.id
    join dwh_base_sec_view.sf_account sfa on o1.accountid = sfa.id
    group by o2.deal_uuid
	having booking_pause_reason is not null;create TEMPORARY table jk_booking_solution as 
    select o2.deal_uuid,
        max(sfa.id) sf_account_id,
        max(case when lower(sfa.scheduler_setup_type) = 'pen & paper' then 'pen & paper'
            when sfa.scheduler_setup_type is null then 'no data'
            else 'some booking tool'
            end) current_booking_solution,
        max(sfa.scheduler_setup_type) detailed_booking_solution,
        max(sfa.name) account_name,
        max(company_type) company_type 
    from dwh_base_sec_view.opportunity_1 o1
    join dwh_base_sec_view.opportunity_2 o2 on o1.id = o2.id
    join dwh_base_sec_view.sf_account sfa on o1.accountid = sfa.id
    group by o2.deal_uuid;
    
   
   
create TEMPORARY table jk_txns as 
    select deal_id,
        sum(net_transactions_qty - zdo_net_transactions_qty) units
    from user_edwprod.agg_gbl_financials_deal
    where report_date >= '2020-01-01'
    group by deal_id;create temporary table jk_opt_out_notes as 
select *     
from( select deal_uuid,
        Product_Opt_Out_Notes__c product_opt_out_notes,
        st.createddate,
        row_number() over (partition by deal_uuid order by st.createddate desc) rownumdesc
    from dwh_base_sec_view.sf_task st
    join dwh_base_sec_view.opportunity_1 o1 on st.accountid = o1.accountid
    join dwh_base_sec_view.opportunity_2 o2 on o1.id = o2.id
    where Product_Opt_Out_Notes__c is not null
)a
where rownumdesc = 1;


create temporary table jk_opt_out_deals as 
    select to_date(dw.week_end) opt_out_week,
        region,
        country_code,
        l2,
        l3,
        booking_pause_reason,
        deal_uuid,
        sum(units) units_sold_2020
    from (
        select ad.deal_uuid,
            min(load_date) opt_out_date,
            p.booking_pause_reason,
            grt_l2_cat_description l2,
            grt_l3_cat_description l3,
            case when gdl.country_code = 'US' then 'NAM' else 'INTL' end region,
            gdl.country_code,
            sum(units) units
        from grp_gdoop_bizops_db.sh_bt_active_deals_log ad
        left join jk_bt_paused_reason p on ad.deal_uuid = p.deal_uuid
        join user_edwprod.dim_gbl_deal_lob gdl on ad.deal_uuid = gdl.deal_id
        left join jk_txns t on ad.deal_uuid = t.deal_id
        where partner_inactive_flag = 1 or product_is_active_flag = 0
        group by ad.deal_uuid,p.booking_pause_reason,grt_l2_cat_description ,grt_l3_cat_description
        	,case when gdl.country_code = 'US' then 'NAM' else 'INTL' end,gdl.country_code      
    ) t
    join user_dw.v_dim_day dd on t.opt_out_date = dd.day_rw
    join user_dw.v_dim_week dw on dd.week_key = dw.week_key
    -- group by 1,2,3,4,5,6,7
    group by dw.week_end ,region,country_code, l2, l3, booking_pause_reason,deal_uuid;
    
   
drop table jk_bt_pause_nam_HBW purge;
create table jk_bt_pause_nam_HBW stored as orc  as
select  opt_out_week,
    region,
    case when l2 in ('F&D','HBW') then l2 else 'Other' end l2,
    l3,
    booking_pause_reason,
    bs.current_booking_solution,
    od.deal_uuid,
    units_sold_2020
from jk_opt_out_deals od
left join jk_booking_solution bs on od.deal_uuid = bs.deal_uuid
join grp_gdoop_bizops_db.sh_bt_active_deals_log adl on od.deal_uuid = adl.deal_uuid
where opt_out_week between date_sub(current_date,28) and current_date
and adl.product_is_active_flag = 1
and adl.partner_inactive_flag = 0
and od.region = 'NAM'
and od.l2 = 'HBW'
group by opt_out_week, region, case when l2 in ('F&D','HBW') then l2 else 'Other' end,l3
 ,booking_pause_reason,bs.current_booking_solution,od.deal_uuid,units_sold_2020;drop table jk_bt_pause_int purge;
 

create table  jk_bt_pause_int stored as orc  as
select opt_out_week,
    od.country_code,
    case when l2 in ('F&D','HBW') then l2 else 'Other' end l2,
    l3,
    booking_pause_reason,
    bs.current_booking_solution,
    od.deal_uuid,
    units_sold_2020
from jk_opt_out_deals od
left join jk_booking_solution bs on od.deal_uuid = bs.deal_uuid
join grp_gdoop_bizops_db.sh_bt_active_deals_log adl on od.deal_uuid = adl.deal_uuid
where opt_out_week  between date_sub(current_date,28) and current_date
and adl.product_is_active_flag = 1
and adl.partner_inactive_flag = 0
and od.region = 'INTL'

group by opt_out_week, od.country_code, case when l2 in ('F&D','HBW') then l2 else 'Other' end ,
    l3, booking_pause_reason, bs.current_booking_solution, od.deal_uuid, units_sold_2020;
   
   
   
drop table jk_bt_pause_over_all purge;
create table  jk_bt_pause_over_all stored as orc  as
select gdl.grt_l2_cat_description,
    gdl.country_code,
    pd.pause_reason,
    count(distinct ad.deal_uuid)cnt_deals
from (
    select deal_uuid, max(cast(dw.week_end as date)) last_wk
    from grp_gdoop_bizops_db.sh_bt_active_deals_log ad
    join user_groupondw.dim_day dd on ad.load_date = dd.day_rw
    join user_groupondw.dim_week dw on dd.week_key = dw.week_key
    group by deal_uuid
    having last_wk between date_sub(current_date,7) and current_date

) ad
join user_edwprod.dim_gbl_deal_lob gdl on ad.deal_uuid = gdl.deal_id
left join grp_gdoop_bizops_db.de_paused_deals pd on ad.deal_uuid = pd.deal_uuid
where gdl.grt_l2_cat_description in ('F&D','HBW','TTD - Live','TTD - Leisure')
group by gdl.grt_l2_cat_description, gdl.country_code, pd.pause_reason
order by gdl.grt_l2_cat_description, gdl.country_code, pd.pause_reason;drop table jk_bt_pause_live_nam_HBW purge;


create table  jk_bt_pause_live_nam_HBW stored as orc  as
select grt_l3_cat_description,
    current_booking_solution,
    count(distinct adl.deal_uuid) cnt_deals
from grp_gdoop_bizops_db.sh_bt_active_deals_log adl
join user_edwprod.dim_gbl_deal_lob gdl on adl.deal_uuid = gdl.deal_id
join (
    select cast(dw.week_end as date) wk
    from user_groupondw.dim_day dd
    join user_groupondw.dim_week dw on dd.week_key = dw.week_key
    where cast(dw.week_end as date) between date_sub(current_date,7) and current_date
) dw on adl.load_date = dw.wk
join jk_booking_solution sbs on adl.deal_uuid = sbs.deal_uuid
where product_is_active_flag = 1
and partner_inactive_flag = 0
and adl.country = 'US'
and gdl.grt_l2_cat_description = 'HBW'
group by grt_l3_cat_description, current_booking_solution   