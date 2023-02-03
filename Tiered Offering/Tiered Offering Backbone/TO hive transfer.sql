drop hbw_owners;


----INCOMPLETE HBW Owner


-----THIS CODE RUNS IN TERADATA AND THEN IS TRANSFERRED TO HIVE

sel
account_manager ownerid,
account_id_18 account_id,
ros.rep as account_owner,
ros.m1,
name
from dwh_base_sec_view.sf_account sfa
left join user_groupondw.dim_sf_person sfp on sfp.person_id = sfa.ownerid
left join (
	select
	roster_date,
	emplid,
	team,
	title_rw,
	rep,
	m1,
	m2,
	m3
	from sandbox.ops_roster_all
	where roster_date = current_date - 1
	qualify row_number() over(partition by roster_date,emplid order by start_date,month_end_date,length(title_rw) desc) = 1
	) ros on ros.emplid = sfp.ultipro_id


	
	
SET hive.cli.print.header = true;
SET hive.default.fileformat = Orc;
SET hive.groupby.orderby.position.alias = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET mapred.task.timeout = 1200000;
SET hive.tez.container.size = 12288;
SET hive.tez.java.opts =-Xmx9000M;
SET hive.exec.max.dynamic.partitions.pernode = 19000;
SET hive.exec.max.dynamic.partitions = 19000;
SET hive.auto.convert.join.noconditionaltask.size = 3862953984;
set hive.limit.query.max.table.partition = 5000;
use grp_gdoop_bizops_db;
drop table if exists grp_gdoop_bizops_db.to_hbw_owners_tmp purge;
create temporary table grp_gdoop_bizops_db.to_hbw_owners_tmp ( 
ownerid string,
account_id string,
account_owner string,
m1 string,
name string
)ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES
(
    "separatorChar" = ",",
    "quoteChar"     = "\""
);
LOAD DATA INPATH 'hdfs://cerebro-namenode/user/grp_gdoop_BizOps/pg_orders/to_hbw_owners.csv'
OVERWRITE INTO TABLE grp_gdoop_bizops_db.to_hbw_owners_tmp;
drop table if exists grp_gdoop_bizops_db.to_hbw_owners;
create table grp_gdoop_bizops_db.to_hbw_owners stored as orc as
    select * from grp_gdoop_bizops_db.to_hbw_owners_tmp;drop table if exists grp_gdoop_bizops_db.to_hbw_owners_tmp
  


------HBW PDS
  


drop table if exists grp_gdoop_bizops_db.to_hbw_pds;
create table grp_gdoop_bizops_db.to_hbw_pds stored as orc as
select
a.account_id,
a.deal_id,
p.grt_l3_cat_description as l3,
p.pds_cat_name as pds
from (
    select
    f.deal_id,
    f.account_id,
    sum(units) units,
    row_number() over(partition by f.account_id order by sum(units) desc) as units_rank
    from grp_gdoop_revenue_management_db.rev_mgmt_gbl_deal_funnel f
    join grp_gdoop_bizops_db.avb_to_t600_hbw h on f.account_id = h.account_id
    where
        report_date between '2019-10-01' and '2020-02-28'
    group by f.deal_id, f.account_id
) a 
join user_edwprod.dim_gbl_deal_lob l on a.deal_id = l.deal_id
join user_dw.v_dim_pds_grt_map p on p.pds_cat_id = l.pds_cat_id
where units_rank = 1
group by a.account_id, a.deal_id, p.grt_l3_cat_description, p.pds_cat_name;



------

drop table if exists grp_gdoop_bizops_db.to_hbw_pop_svcs;
create table grp_gdoop_bizops_db.to_hbw_pop_svcs stored as orc as
select
pds_cat_id,
grt_l5_cat_name
from user_dw.v_dim_pds_grt_map
where grt_l5_cat_name in (
'L5 - Injection - Wrinkle',
'L5 - Laser Hair Removal',
'L5 - Fat/ Cellulite Reduction Procedure',
'L5 - Spa Package',
'L5 - Saunas & Steam Rooms',
'L5 - Specialty Facial',
'L5 - Laser Eye Surgery',
'L5 - Spa - Day Pass',
'L5 - Chiropractic / Osteopathy',
'L5 - Floating - Isolation Tank / Sensory Deprivation',
'L5 - Eyebrow & Eyelash Care',
'L5 - Medical Care',
'L5 - Teeth Whitening',
'L5 - Cryotherapy',
'L5 - Dental Checkup/ Cleaning',
'L5 - Salt Cave',
'L5 - Weight Loss & Nutrition',
'L5 - Other Alternative Therapies',
'L5 - Acupuncture',
'L5 - Injection - B12',
'L5 - Massage',
'L5 - Haircut/Color',
'L5 - Facial',
'L5 - Eyelash Extensions',
'L5 - Nail Services',
'L5 - Waxing',
'L5 - Facial- Photo',
'L5 - Facial- Rejuvenation',
'L5 - Tanning',
'L5 - Straightening Treatment',
'L5 - Reflexology',
'L5 - Blow Dry',
'L5 - Beauty Package - Hair Salon',
'L5 - Threading',
'L5 - Sugaring',
'L5 - Eyelash Tinting',
'L5 - Hair Styling',
'L5 - Haircut - Men',
'L5 - Hair Conditioning Treatment',
'L5 - Cupping',
'L5 - Yoga',
'L5 - Boxing / Kickboxing',
'L5 - Boot Camp',
'L5 - Pilates',
'L5 - Fitness Studio',
'L5 - Gym',
'L5 - Spinning / Indoor Cycling',
'L5 - Barre',
'L5 - Cross Fit',
'L5 - Sexy Fitness',
'L5 - Gymnastics',
'L5 - Personal Trainer',
'L5 - Fitness Conditioning',
'L5 - Dance Class',
'L5 - Aerial Fitness',
'L5 - Zumba',
'L5 - Personalized Fitness Program',
'L5 - Aerobics',
'L5 - Circuit Training',
'L5 - Gym / Fitness Center - In Spa')
;


----



drop table if exists grp_gdoop_bizops_db.to_hbw_720;
create table grp_gdoop_bizops_db.to_hbw_720 stored as orc as 
select
    o.account_id,
    count(distinct m.grt_l5_cat_name) as services_live,
    count(distinct case when p.pds_cat_id is not null then m.grt_l5_cat_name end) pop_services,
    count(distinct case when deals_live = 1 then pds_name end) as pds_live,
    count(distinct ad.inventory_id) as options_live
from grp_gdoop_sup_analytics_db.sup_analytics_deal_counts_final o
    join user_dw.v_dim_pds_grt_map m on o.pds_cat_id = m.pds_cat_id
    join user_groupondw.fact_active_deals ad on o.deal_uuid = ad.deal_uuid and lower(sold_out) = 'false' and load_date = '2020-07-19'
    join grp_gdoop_bizops_db.avb_to_t600_hbw a on o.account_id = a.account_id
    left join grp_gdoop_bizops_db.to_hbw_pop_svcs p on m.pds_cat_id = p.pds_cat_id
    where report_date = '2020-07-19' and accts_live = 1 and deals_live = 1
    group by o.account_id;

   
   



-----001C000001BS3R6IAL

drop table if exists grp_gdoop_bizops_db.to_hbw_now;
create table grp_gdoop_bizops_db.to_hbw_now stored as orc as
select
    o.account_id, 
    count(distinct m.grt_l5_cat_name) as services_live,
    count(distinct case when p.pds_cat_id is not null then m.grt_l5_cat_name end) pop_services,
    count(distinct case when deals_live = 1 then pds_name end) as pds_live,
    count(distinct ad.inventory_id) as options_live
  from grp_gdoop_sup_analytics_db.sup_analytics_deal_counts_final o
    join user_dw.v_dim_pds_grt_map m on o.pds_cat_id = m.pds_cat_id
    join user_groupondw.fact_active_deals ad on o.deal_uuid = ad.deal_uuid and lower(sold_out) = 'false' and load_date = date_add(current_date, -2)
    join grp_gdoop_bizops_db.avb_to_t600_hbw a on o.account_id = a.account_id
    left join grp_gdoop_bizops_db.to_hbw_pop_svcs p on m.pds_cat_id = p.pds_cat_id
    where report_date = date_add(current_date, -2) and accts_live = 1 and deals_live = 1
   group by o.account_id;


  
----


drop table if exists grp_gdoop_bizops_db.to_hbw_new_svcs;
create table grp_gdoop_bizops_db.to_hbw_new_svcs stored as orc as
select
a.account_id,
count(distinct a.services_live) as new_svcs_live,
count(distinct case when c.grt_l5_cat_name is not null then c.grt_l5_cat_name end) new_pop_svcs_live
from 
    (select 
           distinct o.account_id,
           m.grt_l5_cat_name as services_live
       from grp_gdoop_sup_analytics_db.sup_analytics_deal_counts_final o
       join user_dw.v_dim_pds_grt_map m on o.pds_cat_id = m.pds_cat_id
       join grp_gdoop_bizops_db.avb_to_t600_hbw a on o.account_id = a.account_id
       where report_date = date_add(current_date, -2) and accts_live = 1 and deals_live = 1
     ) a
  left join 
    (select 
           distinct o.account_id,
           m.grt_l5_cat_name as services_live
     from grp_gdoop_sup_analytics_db.sup_analytics_deal_counts_final o
     join user_dw.v_dim_pds_grt_map m on o.pds_cat_id = m.pds_cat_id
     join grp_gdoop_bizops_db.avb_to_t600_hbw a on o.account_id = a.account_id
     where report_date = '2020-07-19' and accts_live = 1 and deals_live = 1
     ) b on a.account_id = b.account_id and a.services_live = b.services_live
  left join grp_gdoop_bizops_db.to_hbw_pop_svcs c on a.services_live = c.grt_l5_cat_name
  where
      b.account_id is null and b.services_live is null
  group by a.account_id;



-----



drop table if exists grp_gdoop_bizops_db.to_hbw_booking_720;
create table grp_gdoop_bizops_db.to_hbw_booking_720 stored as orc as
select
    distinct salesforce_account_id as account_id
from grp_gdoop_bizops_db.sh_bt_active_deals_log a
    join user_edwprod.dim_offer o on a.deal_uuid = o.product_uuid
    join user_edwprod.dim_merchant m on m.merchant_uuid = o.merchant_uuid
    where load_date = '2020-07-19' and is_bookable = 1 and partner_inactive_flag = 0 and product_is_active_flag = 1
UNION ALL
select
    distinct account_id
  from grp_gdoop_sup_analytics_db.hbw_deals_bookings_flags m
    where 
      mbo_flag = 1
      and m.deal_uuid <> '88610046-a287-48e7-8fd3-e5092b5926f1' -- permalink booking-test-co-5'
      and m.report_date = '2020-07-19';
        

     
----

     
drop table if exists grp_gdoop_bizops_db.to_hbw_booking_now;
create table grp_gdoop_bizops_db.to_hbw_booking_now stored as orc as
select
    distinct salesforce_account_id as account_id
 from grp_gdoop_bizops_db.sh_bt_active_deals_log a
 join user_edwprod.dim_offer o on a.deal_uuid = o.product_uuid
 join user_edwprod.dim_merchant m on m.merchant_uuid = o.merchant_uuid
 where load_date = date_add(current_date,-2) and is_bookable = 1 and partner_inactive_flag = 0 and product_is_active_flag = 1
UNION ALL
select
   distinct account_id
 from grp_gdoop_sup_analytics_db.hbw_deals_bookings_flags m
 join (select max(cast(report_date as date)) reported from grp_gdoop_sup_analytics_db.hbw_deals_bookings_flags) n on cast(m.report_date as date) = n.reported
 where m.mbo_flag = 1
 and m.deal_uuid <> '88610046-a287-48e7-8fd3-e5092b5926f1'; -- permalink booking-test-co-5'

 
-----to_hbw_unres_720

drop table if exists grp_gdoop_bizops_db.to_hbw_unres_720;
create table grp_gdoop_bizops_db.to_hbw_unres_720 stored as orc as
	select
		o.account_id,
		count(distinct m.grt_l5_cat_name) as unres_services
	from grp_gdoop_sup_analytics_db.sup_analytics_deal_counts_final o
	join grp_gdoop_sup_analytics_db.sup_analytics_dim_deal_tier a on o.deal_uuid = a.deal_uuid
	join user_dw.v_dim_pds_grt_map m on m.pds_cat_id = o.pds_cat_id
	where 
		tier in (1,3)
		and report_date = '2020-07-19' and o.accts_live = 1 and o.deals_live = 1
	group by o.account_id;


----to_hbw_unres_now

drop table if exists grp_gdoop_bizops_db.to_hbw_unres_now;
create table grp_gdoop_bizops_db.to_hbw_unres_now stored as orc as
	select
		o.account_id,
		count(distinct m.grt_l5_cat_name) as unres_services
	from grp_gdoop_sup_analytics_db.sup_analytics_deal_counts_final o
	join grp_gdoop_sup_analytics_db.sup_analytics_dim_deal_tier a on o.deal_uuid = a.deal_uuid
	join user_dw.v_dim_pds_grt_map m on m.pds_cat_id = o.pds_cat_id
	left join grp_gdoop_bizops_db.avb_to_t600_hbw c on o.account_id = c.account_id
	where 
		tier in (1,3)
		and report_date = date_sub(current_date,2) and o.accts_live = 1 and o.deals_live = 1
	group by o.account_id;


-----to_hbw_res_720

drop table if exists grp_gdoop_bizops_db.to_hbw_res_720;
create table grp_gdoop_bizops_db.to_hbw_res_720 stored as orc as
select
    account_id,
    count(distinct case when unrestricted_flag = 0 then grt_l5_cat_name end) as restricted_services
from (
     select
        o.account_id,
        m.grt_l5_cat_name,
        o.deal_uuid,
        u.salesforce_id,
        case
            when dt.tier in ('1','3') then 1
            when dores.deal_uuid is null then 0
            when COALESCE(dres.new_customer_res, 0) > 0 then 0
            when COALESCE(dres.active_within_res, 0) > 0 then 0
            when COALESCE(dres.num_guests_res, 0) > 0 then 0
            when COALESCE(dres.pref_guests_res, 0) > 0 then 0
            when COALESCE(dores.dt_time_res, 0) > 0 then 0
            else 1
        end as unrestricted_flag
    from grp_gdoop_sup_analytics_db.sup_analytics_deal_counts_final o
    join user_dw.v_dim_pds_grt_map m on o.pds_cat_id = m.pds_cat_id
    join user_edwprod.dim_deal_unity u on o.deal_uuid = u.deal_uuid
    left join
    (
       select distinct 
           opportunity_id,
           tier
       from grp_gdoop_sup_analytics_db.sup_analytics_dim_deal_tier
       where is_current = 1
    ) dt on dt.opportunity_id = u.salesforce_id
    left join 
    (select
           deal_uuid,
           max(dt_time_res) as dt_time_res,
           max(buyer_max_res) as buyer_max_res
     FROM grp_gdoop_sup_analytics_db.temp_to_do_restrictions
         where deal_option_uuid is not null
         and coalesce(buyer_max,0) > 0
         and coalesce(repurchase_control,0) > 0
         and coalesce(buyer_max,0) >= coalesce(repurchase_control,0)
         group by deal_uuid
     ) dores on dores.deal_uuid = o.deal_uuid
    left join
    (
       SELECT
            deal_uuid,
            gen_spend,
            new_customer_res,
            active_within_res,
            repurchase_control_res,
            num_guests_res,
            pref_guests_res
        FROM 
        (SELECT
            deal_uuid,
            gen_spend,
            new_customer_res,
            active_within_res,
            repurchase_control_res,
            num_guests_res,
            pref_guests_res,
            ROW_NUMBER() OVER (PARTITION BY deal_uuid ORDER BY deal_option_uuid, buyer_max DESC) AS pick_one
          FROM grp_gdoop_sup_analytics_db.temp_to_do_restrictions
       ) a
      WHERE pick_one = 1
     ) dres on o.deal_uuid = dres.deal_uuid
    where report_date = '2020-07-19' and accts_live = 1 and deals_live = 1
) x
group by account_id;


-----to_hbw_res_now

drop table if exists grp_gdoop_bizops_db.to_hbw_res_now;
create table grp_gdoop_bizops_db.to_hbw_res_now stored as orc as
select
    account_id,
    count(distinct case when unrestricted_flag = 0 then grt_l5_cat_name end) as restricted_services
from (select
        o.account_id,
        m.grt_l5_cat_name,
        o.deal_uuid,
        u.salesforce_id,
        case 
            when dt.tier in ('1','3') then 1
            when dores.deal_uuid is null then 0
            when COALESCE(dres.new_customer_res, 0) > 0 then 0
            when COALESCE(dres.active_within_res, 0) > 0 then 0
            when COALESCE(dres.num_guests_res, 0) > 0 then 0
            when COALESCE(dres.pref_guests_res, 0) > 0 then 0
            when COALESCE(dores.dt_time_res, 0) > 0 then 0
            else 1
            end as unrestricted_flag
    from grp_gdoop_sup_analytics_db.sup_analytics_deal_counts_final o
    join user_dw.v_dim_pds_grt_map m on o.pds_cat_id = m.pds_cat_id
    join user_edwprod.dim_deal_unity u on o.deal_uuid = u.deal_uuid
    left join
    (select distinct 
          opportunity_id,
          tier
        from grp_gdoop_sup_analytics_db.sup_analytics_dim_deal_tier
        where is_current = 1
     ) dt on dt.opportunity_id = u.salesforce_id
 left join 
    (select
        deal_uuid,
        max(dt_time_res) as dt_time_res,
        max(buyer_max_res) as buyer_max_res
    FROM grp_gdoop_sup_analytics_db.temp_to_do_restrictions
        where deal_option_uuid is not null
        and coalesce(buyer_max,0) > 0
        and coalesce(repurchase_control,0) > 0
        and coalesce(buyer_max,0) >= coalesce(repurchase_control,0)
        group by deal_uuid
    ) dores on dores.deal_uuid = o.deal_uuid
left join
   (SELECT 
        deal_uuid,
        gen_spend,
        new_customer_res,
        active_within_res,
        repurchase_control_res,
        num_guests_res,
        pref_guests_res
     FROM 
     ( 
     SELECT
         deal_uuid,
         gen_spend,
         new_customer_res,
         active_within_res,
         repurchase_control_res,
         num_guests_res,
         pref_guests_res,
         ROW_NUMBER() OVER (PARTITION BY deal_uuid ORDER BY deal_option_uuid, buyer_max DESC) AS pick_one
         FROM grp_gdoop_sup_analytics_db.temp_to_do_restrictions
         ) a
         WHERE pick_one = 1
         ) dres on o.deal_uuid = dres.deal_uuid
         where report_date = date_sub(current_date,2) and accts_live = 1 and deals_live = 1
) x
group by account_id;

-----


drop table if exists grp_gdoop_bizops_db.to_hbw_tiers_deals;
create table grp_gdoop_bizops_db.to_hbw_tiers_deals stored as orc as
select
    a.account_id,
    count(distinct case when a.option_tier = 1 then a.option_uuid end) as offers,
    count(distinct case when a.option_tier = 2 then a.option_uuid end) as deals
from grp_gdoop_sup_analytics_db.ex_deals_options_tier_audited_daily_snapshot a
    join (select max(cast(report_date as date)) report from grp_gdoop_sup_analytics_db.ex_deals_options_tier_audited_daily_snapshot) b on cast(a.report_date as date) = b.report
where
    cast(a.op_close_date as date) >= cast('2020-07-19' as date)
    and a.deal_tier in (1,3)
group by a.account_id;



------FINAL




drop table grp_gdoop_bizops_db.avb_to_top_accts_hbw;
create table grp_gdoop_bizops_db.avb_to_top_accts_hbw stored as orc as
 select
    row_rank,
    account_id_18 as account_id,
    case when
       lower(coalesce(a.Rej_Groupon_not_intersd,'na')) not in ('banned category','out of business') then 'workable'
       else 'oob' end as acct_status,
    a.name as account_name,
    coalesce(c.l3,d.l3) as l3,
    coalesce(c.pds,d.pds) as pds,
    coalesce(g.division,e.permalink__c) as account_division,
    merchant_segmentation__c as account_metal,
    coalesce(l.last_close_date,a.Most_Recent_Last_Close_Date) as close_date,
    coalesce(j.contracts_sent,0) as contracts_sent_720,
    coalesce(k.cc_,0)  as contracts_closed_720,
    case when g.test_group_hierarchy = '1' then 'V1' 
        when g.test_group_hierarchy = '2' then 'V2'
        when g.test_group_hierarchy = '3' then 'V3'
        when g.test_group_hierarchy = '5' then 'COVID'
      else 'BAU' end as variant,
    case when h.account_id is not null then 1 else 0 end as live_720,
    case when i.account_id is not null then 1 else 0 end live_today,
    case when h.account_id is not null then h.services_live else 0 end as svcs_720,
    case when i.account_id is not null then i.services_live else 0 end svcs_now,
    case when i.account_id is not null then i.pop_services else 0 end as pop_svcs_now,
    case when h.account_id is not null then h.options_live else 0 end as options_720,
    case when i.account_id is not null then i.options_live else 0 end as options_now,
    case when p.account_id is not null then p.unres_services else 0 end unres_720,
    case when q.account_id is not null then q.unres_services else 0 end unres_now,
    case when r.account_id is not null then r.restricted_services else 0 end res_720,
    case when s.account_id is not null then s.restricted_services else 0 end res_now,
    case when m.account_id is not null then 1 else 0 end as bookable_720,
    case when n.account_id is not null then 1 else 0 end as bookable_now,
    case when o.account_id is not null then bk_options else 0 end as book_options_live,
    coalesce(case when t.account_id is not null then deals end,0) as opt_deals,
    coalesce(case when t.account_id is not null then offers end,0) as opt_offers,
    case when u.account_id is not null then u.account_owner end as acct_owner,
    case when u.account_id is not null then u.m1 end as mgr,
    case when v.account_id is not null then v.new_svcs_live else 0 end as new_svcs_live,
    case when i.account_id is not null then h.pop_services else 0 end as pop_svcs_720,
    case when v.account_id is not null then v.new_pop_svcs_live else 0 end as new_pop_svcs_live
from dwh_base_sec_view.sf_account a
join grp_gdoop_bizops_db.avb_to_t600_hbw b on a.account_id_18 = b.account_id
left join grp_gdoop_bizops_db.to_hbw_pds c on c.account_id = b.account_id
left join ( -- pds for non-closed accounts )
   select
    h.account_id,
    h.primary_deal_services as pds,
    h.l3
    from (
    select
        h.account_id,
        opportunity_id,
        primary_deal_services,
        p.grt_l3_cat_description as l3,
        closedate,
        row_number() over(partition by account_id order by closedate asc) as close_row
        from dwh_base_sec_view.opportunity_1 o
        join grp_gdoop_bizops_db.avb_to_t600_hbw h on o.accountid = h.account_id
        join user_dw.v_dim_pds_grt_map p on p.pds_cat_name = o.primary_deal_services
        group by h.account_id, opportunity_id, primary_deal_services,p.grt_l3_cat_description, closedate
     ) h where h.close_row = 1
 ) d on b.account_id = d.account_id
left join user_edwprod.sf_division e on e.id = a.division 
left join grp_gdoop_bizops_db.to_hbw_owners f on f.account_id = b.account_id
left join grp_gdoop_sup_analytics_db.eh_greenlist_detail g on g.account_id = b.account_id
left join grp_gdoop_bizops_db.to_hbw_720 h on h.account_id = b.account_id 
left join grp_gdoop_bizops_db.to_hbw_now i on i.account_id = b.account_id 
left join grp_gdoop_bizops_db.to_hbw_sent j on b.account_id = j.account_id
left join grp_gdoop_bizops_db.to_hbw_close k on b.account_id = k.account_id
left join grp_gdoop_bizops_db.to_hbw_last_close l on l.account_id = b.account_id
left join grp_gdoop_bizops_db.to_hbw_booking_720 m on m.account_id = b.account_id
left join grp_gdoop_bizops_db.to_hbw_booking_now n on n.account_id = b.account_id
--left join sandbox.avb_to_bt_options_live o on o.account_id = b.account_id
left join (
    select
        account_id,
        sum(total_options_bookable) as bk_options
     from (
        select * from grp_gdoop_bizops_db.avb_to_t600_hbw_op
        union all 
        select
            m.account_id,
            count(distinct inventory_id) option_live
        from grp_gdoop_sup_analytics_db.hbw_deals_bookings_flags m
        join user_groupondw.fact_active_deals ad on m.deal_uuid = ad.deal_uuid
        join (select max(report_date) report from grp_gdoop_sup_analytics_db.hbw_deals_bookings_flags) x on m.report_date = x.report
        where 
           mbo_flag in (1)
           and m.deal_uuid <> '88610046-a287-48e7-8fd3-e5092b5926f1' -- permalink booking-test-co-5'
           and m.report_date = ad.load_date
           and sold_out = 'FALSE' and available_qty > 0
        group by m.account_id
        ) bk_options
        group by account_id
) o on o.account_id = b.account_id
left join grp_gdoop_bizops_db.to_hbw_unres_720 p on p.account_id = b.account_id
left join grp_gdoop_bizops_db.to_hbw_unres_now q on q.account_id = b.account_id
left join grp_gdoop_bizops_db.to_hbw_res_720 r on r.account_id = b.account_id
left join grp_gdoop_bizops_db.to_hbw_res_now s on s.account_id = b.account_id
left join grp_gdoop_bizops_db.to_hbw_tiers_deals t on t.account_id = b.account_id
left join grp_gdoop_bizops_db.to_hbw_owners u on u.account_id = b.account_id
left join grp_gdoop_bizops_db.to_hbw_new_svcs v on v.account_id = b.account_id;



select * from grp_gdoop_bizops_db.to_hbw_owners;
