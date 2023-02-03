

drop table grp_gdoop_bizops_db.nvp_deals_live_international;
create table grp_gdoop_bizops_db.nvp_deals_live_international stored as orc as 
    select a.deal_uuid,
        gdl.grt_l2_cat_name,
        gdl.country_code,
        cast(bt_launch_date as date) bt_launch_date,
        has_gcal, 
        units_sold
    from grp_gdoop_bizops_db.sh_bt_active_deals_log_v4 a
    join user_edwprod.dim_gbl_deal_lob gdl on a.deal_uuid = gdl.deal_id
    left join (
        select deal_uuid,
            min(load_date) bt_launch_date
        from grp_gdoop_bizops_db.sh_bt_active_deals_log_v4
        where product_is_active_flag = 1
        and partner_inactive_flag = 0
        group by deal_uuid
    ) l on a.deal_uuid = l.deal_uuid
    left join 
    (select 
        deal_uuid, 
        cntry.country_iso_code_2 country_code, 
        sum(transaction_qty) units_sold
    from user_edwprod.fact_gbl_transactions fgt
         join dwh_base_sec_view.country cntry on fgt.country_id = cntry.country_id
         where order_date >= date_sub(current_date,30)
               and action = 'authorize'
         group by 
                deal_uuid, 
                cntry.country_iso_code_2
      ) fgt1 on a.deal_uuid = fgt1.deal_uuid and gdl.country_code = fgt1.country_code
    where product_is_active_flag = 1
    and partner_inactive_flag = 0
    and a.load_date = date_sub(current_date,3)
    and gdl.country_code <> 'US'
    and gdl.grt_l1_cat_name = 'L1 - Local';


   
drop table grp_gdoop_bizops_db.nvp_intl_avail_problem;
create table grp_gdoop_bizops_db.nvp_intl_avail_problem stored as orc as
select
      avail.deal_uuid,
      avail.country,
      avail.reference_date,
      max(avail.report_date) report_date,
      case when max(gss_available_minutes) > 0 then date_format(reference_date,'E') end day_available, 
      case when max(gss_total_minutes) > 0 then date_format(reference_date,'E') end day_available_total
from
  (select *
       from grp_gdoop_bizops_db.jk_bt_availability_gbl_2
       where
       report_date >= cast('2020-09-01' as date)
       and days_delta <= 10
   ) avail
   WHERE country <> 'US' AND CAST(reference_date AS date) <= date_sub(next_day(CURRENT_DATE, 'MON'), 1)
   group by 
      avail.deal_uuid,
      avail.country ,
      avail.reference_date
 ;

drop table grp_gdoop_bizops_db.nvp_deals_intl_problem;
create table grp_gdoop_bizops_db.nvp_deals_intl_problem stored as orc as
select 
    a.*, 
    b.reference_date,
    c.report_date
from grp_gdoop_bizops_db.nvp_deals_live_international as a 
   left join 
      (select 
           deal_uuid, 
           country, 
           reference_date,
           row_number () over (partition by deal_uuid, country order by reference_date desc) ord
           from grp_gdoop_bizops_db.nvp_intl_avail_problem) as b on a.deal_uuid = b.deal_uuid and b.ord = 1 
    left join 
        (select 
           deal_uuid, 
           country, 
           max(report_date) report_date
           from grp_gdoop_bizops_db.nvp_intl_avail_problem
           group by deal_uuid, country) as c on a.deal_uuid = c.deal_uuid;
   
   
   select 
    a.*, 
    case when reference_date is null then 1 else 0 end no_availability_data, 
    case when reference_date <= cast('2021-03-06' as date) then 1 else 0 end latest_data_not_available
from grp_gdoop_bizops_db.nvp_deals_intl_problem as a;