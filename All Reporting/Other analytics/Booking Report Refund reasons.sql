select * from user_groupondw.dim_zendesk_reason;

select * from user_gp.order_audit_records;

create volatile multiset table nvp_refund_reasons as (
select 
    a.*, 
    b.reason, 
    b.comments
from sh_refunds as a 
left join 
    user_gp.order_audit_records as b on a.order_id = b.order_id) with data on commit preserve rows
;

drop table nvp_refund_reasons_1;
create volatile multiset table nvp_refund_reasons_1 as (
sel 
    f.deal_uuid,
    cat.action_cohort,
    re.reason, 
    re.comments,
    count(distinct b.order_id) orders_booked, 
    count(distinct case when r.order_id is not null then r.order_id end) order_refunded, 
    sum(units) units
from 
    sh_booked b
join 
    (sel order_id, max(deal_uuid) deal_uuid, 
          max(unified_deal_option_id) unified_deal_option_id, 
          sum(transaction_qty) units
          from sh_fgt_book 
          action = 'authorize'
          group by 1) f on b.order_id = f.order_id
join sandbox.nvp_hbw_booking_status_deal cat on f.deal_uuid = cat.deal_uuid
left join sh_refunds r on b.order_id = r.order_id
left join nvp_refund_reasons as re on b.order_id = re.order_id
where booked_bt = 1 
group by 1,2,3,4) with data on commit preserve rows;


select 
   reason, 
   comments, 
   sum(order_refunded) booked_refunded
   from
(select 
     a.*, 
     ROW_NUMBER() over(partition by deal_uuid order by units desc) top_reasons
from nvp_refund_reasons_1 as a) as fin
where 
action_cohort = 'd. high refunds on booked vouchers'
group by reason, comments
order by 3 desc
;



----------Cancellation reasons
select distinct cancelled_by from sh_booked_cancelled_by;

drop table nvp_fin_cancel;
create volatile multiset table nvp_fin_cancel as 
(select 
    f.deal_uuid, 
    cat.action_cohort,
    sum(f.units) total_refunds,
    sum(case when can.cancelled_by = 'merchant' then f.units end) dis_merch_ord, 
    sum(case when can.cancelled_by in ('customer', 'api') then f.units end) dis_cust_ord,
    sum(case when can.cancelled_by = 'admin' then f.units end) dis_admin,
    sum(case when can.cancelled_by is null then f.units end) dis_null_ord
from 
     (sel order_id, max(deal_uuid) deal_uuid, 
          max(unified_deal_option_id) unified_deal_option_id, 
          sum(transaction_qty) units
          from sh_fgt_book 
          action = 'authorize'
          group by 1) as f 
     join sandbox.nvp_hbw_booking_status_deal cat on f.deal_uuid = cat.deal_uuid
     join sh_refunds r on f.order_id = r.order_id
     left join sh_booked_cancelled_by can on f.order_id = can.order_id
     group by 1,2
 ) with data on commit preserve rows;

select 
   action_cohort, 
   count(distinct deal_uuid) total_deals, 
   sum(total_refunds) total_refunds,
   sum(dis_merch_ord) merch_refunds, 
   sum(dis_cust_ord) cust_refunds, 
   sum(dis_admin) admin_refunds, 
   sum(dis_null_ord) null_refunds
from nvp_fin_cancel
group by 1
order by 1;
     
     
    select * from sh_booked_cancelled_by;