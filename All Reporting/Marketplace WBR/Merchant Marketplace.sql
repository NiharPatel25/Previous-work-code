create volatile multiset table nvp_lost_deals as
(sel gdl.country_code,
    gdl.grt_l2_cat_description,
    count(distinct t.deal_uuid) n_deals_left
from (
    sel deal_uuid,
        max(load_date) last_dt
    from sandbox.sh_bt_active_deals_log adl
    where product_is_active_flag = 1
    and partner_inactive_flag = 0
    group by 1
) t
join user_groupondw.dim_day dd on t.last_dt = dd.day_rw
join user_groupondw.dim_week dw on dd.week_key = dw.week_key
join user_edwprod.dim_gbl_deal_lob gdl on t.deal_uuid = gdl.deal_id
where cast(dw.week_end as date) between current_date-7 and current_date
group by 1,2) with data on commit preserve rows;


select country_code, sum(n_deals_left) from nvp_lost_deals group by 1;


create volatile multiset table nvp_lost_deals_reasons as
(sel gdl.grt_l2_cat_description,
    gdl.country_code,
    pd.pause_reason,
    count(distinct ad.deal_uuid) deals
from (
    sel deal_uuid, max(cast(dw.week_end as date)) last_wk
    from sandbox.sh_bt_active_deals_log ad
    join user_groupondw.dim_day dd on ad.load_date = dd.day_rw
    join user_groupondw.dim_week dw on dd.week_key = dw.week_key
    group by 1
    having last_wk between current_date -7 and current_date
) ad
join user_edwprod.dim_gbl_deal_lob gdl on ad.deal_uuid = gdl.deal_id
left join user_groupondw.paused_deals pd on ad.deal_uuid = pd.deal_uuid
where gdl.grt_l2_cat_description in ('F&D','HBW','TTD - Live','TTD - Leisure')
group by 1,2,3) with data on commit preserve rows;


select 
   country_code, 
   pause_reason, 
   sum(deals) deals_lost
   from nvp_lost_deals_reasons
   where country_code = 'US'
   GROUP BY 1,2
   order by 3 desc;
   
  
  create volatile table sh_fgt_ord as (
    sel fgt.*
    from user_edwprod.fact_gbl_transactions fgt
    where order_date >= current_date-30
) with data primary index (order_id, action) on commit preserve rows;
