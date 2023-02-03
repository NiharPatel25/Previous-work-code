select max(report_date) from ai_reporting.CITRUS_MASTER_REPORT_CITRUS_363_v1;


select max(days_delta) from grp_gdoop_bizops_db.jk_bt_availability_gbl_2;
create table grp_gdoop_bizops_db.np_bt_availability_gbl_2 stored as orc as 
select * from grp_gdoop_bizops_db.jk_bt_availability_gbl_2;

select 
  deal_uuid,
  reference_date, 
  gss_total_minutes, 
  report_date, 
  a.*
from 
grp_gdoop_bizops_db.jk_bt_availability_gbl_2 a
where deal_uuid = '83208242-68a6-4092-9477-661ef6ad76d9'
and days_delta <= 10
order by report_date desc,reference_date desc;


'feb48d63-3f9f-4ec3-8fe4-6d5e9ca0dbea'
'aaf917fe-513c-462e-98f8-36bd5e621444'

'2612e628-dbd5-4609-9218-6b623bce6185'
'e54e65b9-cc91-4a0f-b87d-5c19db653090'
'83208242-68a6-4092-9477-661ef6ad76d9'

SELECT * FROM grp_gdoop_bizops_db.jk_bt_availability_gbl LIMIT 5;

select * from grp_gdoop_bizops_db.nvp_to_temp_availablity;

select deal_uuid,inventory_product_uuid, opportunity_id,--name,
    gapp_enabled,
    is_active,
    inactive,
    gapp_reason,
    is_active_groupon,
    is_purchasable_groupon,
    is_active_bt,
    country_code,
    l2,
    validity_start,
    validity_end,
    deal_duration,
    amount_session,
    redemption_locations,
    gapp_registered,
    gapp_updated,
    gapp_deleted,
    gapp_status,
    booking_delay,
    cancellation_period,
    duration, is_workshop
 from (
 SELECT
 d.groupon_real_deal_uuid deal_uuid,inventory_product_uuid,
 opportunity_id, pr.name,
 gapp_enabled,
 pr.is_active,pa.inactive ,
 case when ad.deal_uuid is not null then 1 else 0 end is_active_groupon,
 case when adp.deal_uuid is not null then 1 else 0 end is_purchasable_groupon,
 case when (pr.is_active = 1 and pa.inactive = 0) then 1 else 0 end is_active_bt,
 case when gapp_enabled = 1 then 'already_in_gapp'
 when gapp_enabled = 0 and inventory_product_uuid is null then 'old_deal'
 when gapp_enabled = 0 and (pr.is_active = 0 or pa.inactive = 1) then 'deal_not_active'
 when gapp_enabled = 0 and pr.comment_required = 1 then 'comment_required'
 when gapp_enabled = 0 and d.amount_session > 1 then 'multi_session_deal'
 when gapp_enabled = 0 and pa.multi_agenda_enabled = 1 then 'multi_agenda_deal' else 'NULL' end gapp_reason,
 d.country country_code,
 grt_l2_cat_name l2,
d.validity_start, d.validity_end, d.duration as deal_duration, d.amount_session, d.redemption_locations,d.gapp_registered, d.gapp_updated, d.gapp_deleted, d.gapp_status,
pr.booking_delay, pr.cancellation_period, pr.duration, pr.is_workshop
 FROM
 (
 select groupon_real_deal_uuid,
        inventory_product_uuid,
        partners_id,
        products_id,
        gapp_enabled,
        amount_session,
        country,
        validity_start,
        validity_end,
        duration,
        redemption_locations,
        gapp_registered,
        gapp_updated,
        gapp_deleted,
        gapp_status
 from (
        select
            groupon_real_deal_uuid,
            row_number() over (partition by groupon_real_deal_uuid,products_id order by ts_modfied desc) row_num,
            partners_id, inventory_product_uuid,products_id, gapp_enabled, amount_session, country,
            validity_start, validity_end, duration, redemption_locations, gapp_registered, gapp_updated,gapp_deleted, gapp_status
      from grp_gdoop_bizops_db.sh_bt_deals
     ) a
 where row_num = 1
 GROUP BY groupon_real_deal_uuid,inventory_product_uuid, partners_id, products_id, gapp_enabled, amount_session, country,
   validity_start, validity_end, duration, redemption_locations, gapp_registered,gapp_updated,gapp_deleted, gapp_status
  ) d
 left join (
  select id,
        inactive,
        multi_agenda_enabled,
        country
  from (
        select
            merchant_uuid,
            id,
            row_number() over (partition by id,country order by last_update desc) row_num,
            inactive,
            multi_agenda_enabled,
            country
        from grp_gdoop_bizops_db.sh_bt_partners
 ) pa
  where row_num = 1
  group by id, inactive, multi_agenda_enabled, country
 ) pa on d.partners_id = pa.id and d.country = pa.country
 left join (
  select id,
         name,is_active,
         comment_required,
         booking_delay,
         cancellation_period,
         duration,
         is_workshop,
         country from (
              select
                  id,
                  name,
                  row_number() over (partition by id order by last_update desc) row_num,
                  is_active,
                  comment_required,
                  booking_delay,
                  cancellation_period,
                  duration,
                  is_workshop,
                  country
              from grp_gdoop_bizops_db.sh_bt_products
            ) a
            where row_num = 1
            GROUP BY id, name,is_active, comment_required, booking_delay, cancellation_period, duration, is_workshop, country
 ) pr on d.products_id = pr.id and d.country=pr.country
 left join edwprod.dim_gbl_deal_lob gdl on gdl.deal_id = d.groupon_real_deal_uuid
 left join dwh_base_sec_view.opportunity_2 op2 on op2.deal_uuid = d.groupon_real_deal_uuid
 left join (
          select deal_uuid
                from user_groupondw.active_deals
                where load_date = date_sub(current_date, 1)
                group by deal_uuid
            ) ad on ad.deal_uuid = d.groupon_real_deal_uuid
 left join (
          select deal_uuid
          from user_groupondw.active_deals
          where sold_out = 'false' and available_qty > 0 and load_date = date_sub(current_date, 1)
          group by deal_uuid
        ) adp on adp.deal_uuid = d.groupon_real_deal_uuid
-- where d.country = ‘US’
-- left join groupon_production.clo_merchants clo on clo.m3_merchant_id=pa.id
 ) x
 WHERE
 --is_active_groupon = ‘1’
 --AND is_purchasable_groupon = ‘1’
 --AND is_active_bt = ‘1’
 --AND gapp_reason <> ‘deal_not_active’
 deal_uuid='bf785bd3-a04f-4bd8-890b-c5448ca05489'
 --and opportunity_id=‘0063c00001Erduv’
 group by deal_uuid, inventory_product_uuid,opportunity_id, gapp_enabled, is_active,inactive,gapp_reason, is_active_groupon, is_purchasable_groupon, is_active_bt, country_code, l2,
 validity_start, validity_end, deal_duration, amount_session, redemption_locations,gapp_registered, gapp_updated, gapp_deleted, gapp_status,
booking_delay, cancellation_period, duration, is_workshop
 ;