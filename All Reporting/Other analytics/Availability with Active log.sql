drop table grp_gdoop_bizops_db.nvp_to_temp_availablity;
create table grp_gdoop_bizops_db.nvp_to_temp_availablity stored as orc as
select * from 
(select 
    fin_0.*, 
    row_number() over(partition by deal_uuid,deal_option_uuid order by report_date desc) update_order
    from
 (select 
       avail.deal_uuid, 
       avail.deal_option_uuid, 
       avail.country ,
       avail.report_date,
       max(case when days_delta = 0 then gss_total_availability end)  max_avail,
       count(distinct case when gss_total_availability > 0 then date_format(reference_date,'E' ) end ) num_dow
 from
   (select *
        from grp_gdoop_bizops_db.jk_bt_availability_gbl
        where
        report_date >= cast('2020-09-01' as date)
        and days_delta < 7
    ) avail
 join
      (select
            groupon_real_deal_uuid as deal_uuid,
            groupon_deal_uuid as deal_option_uuid,
            (case when min(participants_per_coupon) OVER (PARTITION BY groupon_real_deal_uuid)= 0 then 1
            else participants_per_coupon/ min(participants_per_coupon)  OVER (PARTITION BY groupon_real_deal_uuid) end
            ) as avail_taken_per_booking
       from
          grp_gdoop_bizops_db.sh_bt_deals
   )  deals on deals.deal_uuid = avail.deal_uuid and deals.deal_option_uuid = avail.deal_option_uuid
 join
      (select
            ad.deal_uuid,
            ad.load_date
       from
          (select
                 deal_uuid,
                 load_date
            from prod_groupondw.active_deals
            where
                 sold_out = 'false'
                 and available_qty > 0
                 and cast(load_date as date) >= cast('2020-09-01' as date)
            group by deal_uuid, load_date) ad
        join (
             select
                  load_date,
                  deal_uuid
              from grp_gdoop_bizops_db.sh_bt_active_deals_log
              where
                   partner_inactive_flag = 0
                   and product_is_active_flag = 1
                   and cast(load_date as date) >= cast('2020-09-01' as date)
              group by load_date, deal_uuid) g
                    on g.deal_uuid = ad.deal_uuid and ad.load_date = g.load_date
        ) bt_deals on avail.deal_uuid = bt_deals.deal_uuid and avail.reference_date = cast(bt_deals.load_date as date)
   WHERE country= 'US'
   group by avail.deal_uuid, country ,report_date, avail.deal_option_uuid)
  fin_0) as 
fin_1 where update_order = 1;



select
                  load_date,
                  deal_uuid
              from grp_gdoop_bizops_db.sh_bt_active_deals_log
              where
                   partner_inactive_flag = 0
                   and product_is_active_flag = 1
                   and cast(load_date as date) >= cast('2020-09-01' as date)
                   and deal_uuid = '196e7136-c3c7-429f-b26a-1944fc58dc02'
              group by load_date, deal_uuid;
             
             
             
             
 
