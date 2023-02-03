select * from user_groupondw.active_deals;
-------Total BT eligible units purchased

select 
   case when b.deal_uuid is not null then 1 else 0 end bookable,
   case when live.deal_uuid is not null then 1 else 0 end live_currently,
   grt_l2_cat_description,
   sum(fgt.transaction_qty) units, 
   count(distinct order_uuid) orders, 
   count(distinct parent_order_uuid) parent_order_uuid
from
(select 
    fgt.*,
    gdl.grt_l2_cat_description
    from 
    user_edwprod.fact_gbl_transactions fgt
    join user_edwprod.dim_gbl_deal_lob gdl on fgt.deal_uuid = gdl.deal_id
    where
    gdl.country_code = 'US'
    and gdl.grt_l1_cat_name = 'L1 - Local'
    and fgt.order_date >= cast('2020-01-01' as date) 
    and action = 'authorize'
) as fgt_2
left join
(select  
       deal_uuid,
       cast(load_date as date) load_date 
    from sandbox.sh_bt_active_deals_log_v4
    where product_is_active_flag = 1
          and partner_inactive_flag = 0
    group by 1,2
) as b on fgt_2.deal_uuid = b.deal_uuid and cast(fgt_2.order_date as date) = cast(b.load_date as date)
left join 
(select  
       deal_uuid
    from sandbox.sh_bt_active_deals_log_v4
    where product_is_active_flag = 1
          and partner_inactive_flag = 0
          and cast(load_date as date) >= cast('2021-02-01' as date)
    group by 1
) as live on fgt.deal_uuid = live.deal_uuid
group by 1,2,3
order by ;

select * from sandbox.sh_bt_active_deals_log_v4;


select 
   grt_l2_cat_description,
   sum(fgt_2.transaction_qty) units, 
   count(distinct order_uuid) orders, 
   count(distinct parent_order_uuid) parent_order_uuid
from
(select 
    fgt.*,
    gdl.grt_l2_cat_description
    from 
    user_edwprod.fact_gbl_transactions fgt
    join user_edwprod.dim_gbl_deal_lob gdl on fgt.deal_uuid = gdl.deal_id
    where
    gdl.country_code = 'US'
    and gdl.grt_l1_cat_name = 'L1 - Local'
    and fgt.order_date >= cast('2020-01-01' as date) 
    and action = 'authorize'
) as fgt_2
join
(select  
       deal_uuid,
       cast(load_date as date) load_date 
    from sandbox.sh_bt_active_deals_log
    where product_is_active_flag = 1
          and partner_inactive_flag = 0
    group by 1,2
) as b on fgt_2.deal_uuid = b.deal_uuid and cast(fgt_2.order_date as date) = cast(b.load_date as date)
group by 1;



----------------------------------------------

select 
    sf_account_id, 
    max(Gcal_eligible) Gcal_eligible_fin
from 
(select
  sfa.id sf_account_id,
  case 
    when lower(sfa.scheduler_setup_type) = 'pen & paper' then 'pen & paper'
    when sfa.scheduler_setup_type is null then 'no data'
    else 'some booking tool' end current_booking_solution,
  coalesce(sfa.scheduler_setup_type,'NA') detailed_booking_solution,
  sfa.name account_name,
  case when sfa.scheduler_setup_type in 
         ('Acuity', 'AppointmentPlus', 'Appointy', 
         'BookFresh (Square)', 'Calendly', 'ClinicSense', 
         'Facebook', 'Full Slate (Intuit)', 'Genbook', 
         'GetTimely', 'GoDaddy', 'Google Calendar', 'MassageBook', 'Setmore', 'SimplyBook', 'Square (Bookfresh)', 'vcita') then 1 else 0 end
         Gcal_eligible,
   max(case when has_gcal = 1 then 1 else 0 end) gcal_enabled
from dwh_base_sec_view.sf_opportunity_1 o1
    join dwh_base_sec_view.sf_opportunity_2 o2 on o1.id = o2.id
    join dwh_base_sec_view.sf_account sfa on sfa.id = o1.accountid
    left join 
        (select deal_uuid, max(has_gcal) has_gcal from sandbox.sh_bt_active_deals_log_v4 group by 1) gcal on o2.deal_uuid = gcal.deal_uuid
where BillingCountry = 'US'
group by 1,2,3,4,5
) as fin 
WHERE gcal_enabled = 0
group by 1 having Gcal_eligible_fin >0;

select * from dwh_base_sec_view.sf_opportunity_2;
select * from sandbox.sh_bt_active_deals_log_v4;
