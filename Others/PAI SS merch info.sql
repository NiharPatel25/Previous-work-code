create volatile table np_self_serve_temp as 
(select 
   merchant_id
from 
(select 
     merchant_id
from sandbox.np_sponsored_campaign
where status not in ('DRAFT')
union all
select merchant_id 
from sandbox.citrusad_team_wallet 
where is_self_serve = 1 
      and is_archived = 0
union all
select 
    merchant_id
from
sandbox.np_merchant_topup_orders
where 
    event_type='CAPTURE' 
    and event_status='SUCCESS') as a 
group by 1) with data on commit preserve rows;


drop table sandbox.np_mc_sssl_tracking;
create table sandbox.np_mc_sssl_tracking as (
select 
     'created_campaigns' event_type,
     merchant_id, 
     update_datetime
   from sandbox.np_sponsored_campaign
   where status not in ('DRAFT')
   group by 1,2,3
union all
select 
       'created_wallet' event_type,
       merchant_id, 
       create_datetime
    from sandbox.citrusad_team_wallet 
    where is_self_serve = 1 
      and is_archived = 0
    group by 1,2,3
union all
select 
    'had_top_ups' event_type,
    merchant_id,
    create_datetime
    from
    sandbox.np_merchant_topup_orders
    where 
    event_type='CAPTURE' 
    and event_status='SUCCESS'
    group by 1,2,3) with data


select * from sandbox.np_mc_sssl_tracking;


drop table sandbox.np_mc_sssl_tracking;
create table sandbox.np_mc_sssl_tracking as (
select 
   a.*, 
   case when b.merchant_id is not null then 1 else 0 end created_campaign, 
   case when c.merchant_id is not null then 1 else 0 end created_wallet, 
   case when d.merchant_id is not null then 1 else 0 end had_top_ups
from np_self_serve_temp as a 
left join 
   ( 
   select 
     'created_campaigns' event_type
     merchant_id, 
     update_datetime
   from sandbox.np_sponsored_campaign
   where status not in ('DRAFT')
   group by 1
   ) as b on a.merchant_id = b.merchant_id
left join 
   ( select 
         'created_wallet' event_type,
         merchant_id, 
         create_datetime
    from sandbox.citrusad_team_wallet 
    where is_self_serve = 1 
      and is_archived = 0
    group by 1,2
   ) as c on a.merchant_id = c.merchant_id 
left join 
   (
    select 
    'had_top_ups' event_type,
    merchant_id,
    create_datetime
    from
    sandbox.np_merchant_topup_orders
    where 
    event_type='CAPTURE' 
    and event_status='SUCCESS'
    group by 1
   ) as d on a.merchant_id = d.merchant_id) with data;

select * from sandbox.np_merchant_topup_orders;
  
  
(select 
     day_rw 
  from user_groupondw.dim_day 
  where cast(day_rw as date) >= cast('2021-08-01' as date) and cast(day_rw as date) <= current_date
  group by 1
  ) as a 
left join 



