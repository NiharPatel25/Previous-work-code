


create volatile table jk_booking_solution as (
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
    group by o2.deal_uuid) with data on commit preserve rows;
    
----------


select * from nihpatel.booking_scope_deals;
create multiset volatile table booking_scope_deals as (
select 
    t.deal_uuid,
    cast(dw.week_end as date)  as week_end_date,
    cast(dw.week_end as date) - 7 as  previous_week,
    cast(dw.week_end as date) + 7 next_week,
    max(is_bookable) as is_bookable,
    max(partner_inactive_flag) as partner_inactive_flag,
    max(product_is_active_flag) as product_is_active_flag,
    max(t.country) as country,
    max(case when gdl.country_code = 'US' then 'NAM' else 'INTL' end) region,
    max(grt_l1_cat_name) l1,
    max(grt_l2_cat_description) l2,
    max(grt_l3_cat_description) l3,
    max(bs.current_booking_solution) as current_booking_solution
from sandbox.sh_bt_active_deals_log t
join(
    select deal_uuid, load_date
    from user_groupondw.active_deals
    where sold_out = 'false' 
    and available_qty > 0
    and load_date >= '2020-01-01'
    group by deal_uuid, load_date
    ) ad on t.deal_uuid = ad.deal_uuid and ad.load_date = t.load_date
join user_dw.v_dim_week dw on cast(dw.week_end as date) = cast(t.load_date as date)
join user_edwprod.dim_gbl_deal_lob gdl on gdl.deal_id = t.deal_uuid
join (select deal_uuid, current_booking_solution
	from jk_booking_solution
	)as bs on bs.deal_uuid = t.deal_uuid
where partner_inactive_flag = 0 
	and product_is_active_flag = 1
group by t.deal_uuid,
         cast(dw.week_end as date),
         cast(dw.week_end as date) - 7,
         cast(dw.week_end as date) + 7
) with data on commit preserve rows
;

select * from nihpatel.booking_scope_deals 
where cast(week_end_date as date) >= cast('2021-03-28' as date);


-------------this would cause a sold out deal to be lost. and a new deal that was previously sold not which is now replished to be a new deal. 



drop table sandbox.np_bt_attrition_agg;
create multiset table sandbox.np_bt_attrition_agg as (
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
	     from nihpatel.booking_scope_deals 
	     group by week_end_date, region, l1, l2, l3, current_booking_solution
        ) as live
left join (
	select bsd.week_end_date,
	       bsd.region,
	       bsd.l1,
	       bsd.l2,
	       bsd.l3,
	       bsd.current_booking_solution,
	       count(distinct bsd.deal_uuid) accts
	    from nihpatel.booking_scope_deals bsd
	    left join nihpatel.booking_scope_deals bsd2 on cast (bsd.week_end_date as date)= cast(bsd2.next_week as date) and bsd.deal_uuid = bsd2.deal_uuid
	    where bsd2.deal_uuid is null
	    group by bsd.week_end_date, bsd.region, bsd.l1, bsd.l2, bsd.l3, bsd.current_booking_solution
         ) as adds on live.week_end_date = adds.week_end_date 
                   and live.region = adds.region 
                   and live.l1 = adds.l1 
		           and live.l2 = adds.l2 
		           and live.l3 = adds.l3 
		           and live.current_booking_solution = adds.current_booking_solution 
left join 
        (select
              --bds.week_end_date + interval '7' day as week_end_date,
    	    cast(bds.week_end_date as date) + 7 as week_end_date,
		    bds.region,
    	    bds.l1,
    	    bds.l2,
    	    bds.l3,
    	    bds.current_booking_solution,
    	    count(distinct bds.deal_uuid) accts,
            count(distinct case when btad.partner_inactive_flag = 1 or btad.product_is_active_flag = 0 then bds.deal_uuid end) left_bt_accts,
            count(distinct case when btad.partner_inactive_flag is null then bds.deal_uuid end) left_grpn_accts
	    from nihpatel.booking_scope_deals bds
	    left join nihpatel.booking_scope_deals bds3 on cast(bds.week_end_date as date)= cast(bds3.previous_week as date) and bds.deal_uuid = bds3.deal_uuid
        left join sandbox.sh_bt_active_deals_log btad on cast(bds.week_end_date as date) +7 = btad.load_date and bds.deal_uuid = btad.deal_uuid
	    where bds3.deal_uuid is null
	    group by bds.week_end_date, bds.region, bds.l1, bds.l2, bds.l3, bds.current_booking_solution
       ) lost on live.week_end_date = lost.week_end_date 
              and live.region = lost.region 
              and live.l1 = lost.l1 
		      and live.l2 = lost.l2 
		      and live.l3 = lost.l3 
		      and live.current_booking_solution = lost.current_booking_solution 	
group by 
    live.week_end_date,
    live.region,
    live.l1,
    live.l2,
    live.l3,
    live.current_booking_solution) with data;

   
select 
    week_end_date, 
    sum(live_deals) live_deals, 
    sum(added_deals) deals_added,
    sum(lost_deals) deals_lost, 
    sum(left_bt_deals) bt_lost_deals
from sandbox.np_bt_attrition_agg
where region = 'NAM' and l2 = 'HBW'
group by 1
order by 1 desc
;



create volatile multiset table nvp_temp_lost as (
select
              --bds.week_end_date + interval '7' day as week_end_date,
    	    cast(bds.week_end_date as date) + 7 as week_end_date,
		    bds.region,
    	    bds.l1,
    	    bds.l2,
    	    bds.l3,
    	    bds.current_booking_solution,
    	    bds.deal_uuid,
            case when btad.partner_inactive_flag = 1 or btad.product_is_active_flag = 0 then bds.deal_uuid end left_bt_accts,
            case when btad.partner_inactive_flag is null then bds.deal_uuid end left_grpn_accts
	    from nihpatel.booking_scope_deals bds
	    left join nihpatel.booking_scope_deals bds3 on cast(bds.week_end_date as date)= cast(bds3.previous_week as date) and bds.deal_uuid = bds3.deal_uuid
        left join sandbox.sh_bt_active_deals_log btad on cast(bds.week_end_date as date) +7 = btad.load_date and bds.deal_uuid = btad.deal_uuid
	    where bds3.deal_uuid is null
	   ) with data on commit preserve rows;
	  
	  
select * from nvp_temp_lost where region = 'NAM' and week_end_date = '2021-04-18' order by 1 desc;


select * from nihpatel.booking_scope_deals where deal_uuid = 'c9f50afa-effc-4ad6-b0fe-809c76e3bdb5' order by 2;


'2b9648cf-9d49-4a7e-9df8-93a70dfbd64e'
'7dec48ae-5e72-40f0-a9b0-0377152f128f'
'c9f50afa-effc-4ad6-b0fe-809c76e3bdb5';

'2b9648cf-9d49-4a7e-9df8-93a70dfbd64e'

select * from sandbox.sh_bt_active_deals_log where deal_uuid = '7063fac4-aaa6-47c1-9cb6-8c86aefaa8e2' order by load_date desc;
select * from sandbox.sh_bt_active_deals_log where deal_uuid = '2b9648cf-9d49-4a7e-9df8-93a70dfbd64e' order by load_date desc;
select * from sandbox.sh_bt_active_deals_log where deal_uuid = 'c39b57c1-e61e-4357-8138-362659d68cbf' order by load_date desc;
select * from sandbox.sh_bt_active_deals_log where deal_uuid = 'f662b401-64ba-493f-be7e-9bce36715756' order by load_date desc;

-----------


drop table np_lost_deals;
create volatile multiset table np_lost_deals as (
select 
  fin.*, 
  trunc(fin.min_load_date, 'iw') + 6 min_load_week
  from
(select 
    deal_uuid, 
    country,
    min(cast(load_date as date)) min_load_date
    from 
    sandbox.sh_bt_active_deals_log
    where 
         partner_inactive_flag = 1 
         or product_is_active_flag = 0
         group by 1,2) as fin where country = 'US'
) with data on commit preserve rows;


select * from np_lost_deals where min_load_week >= cast('2021-01-01' as date);



