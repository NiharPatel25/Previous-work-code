-----------INSIGHT - VOICE OF CUSTOMERS

drop table sandbox.np_insight_voice;
create multiset table sandbox.np_insight_voice (
merchant_uuid varchar(64) character set unicode,
l2 varchar(64) character set unicode,
start_date date, 
end_date date, 
last_updated date, 
t30_units_sold integer, 
t30_units_refunded integer, 
t30_refund_rate float,
t30_refund_rate_l2 float,
t30_neg_feedback integer,
t30_neg_feedback_rate float,
t30_neg_feedback_l2_rate float,
t30_cust_contact integer,
t30_cust_contact_rate float,
t30_cust_contact_l2_rate float,
ncx_rate float
) no primary index;

grant select on sandbox.np_insight_voice to public;

merchant_uuid, 
    country_code, 
    l2, 
    min_order_date, 
    max_order_date, 
    start_date, 
    end_date, 
    t60_30_total_orders,
    t60_30_total_units,
    orders_redeemed_within_30_days,
    units_redeemed_within_30_days, 
    orders_refunded_within_30_days,
    units_refunded_within_30_days,
     t60_30_refund_rate,
    contacts_within_30_days,
    t30_average_ratings, 
    t30_less_than_three_rating,
    ncx_rate




select * from sandbox.np_insight_voice;


--NA deals which has either order or refund or voucher activity or contacts in last 180 days

drop table na_deals_base_l180d;

create multiset volatile table na_deals_base_l180d as (
select
distinct deal_uuid,
order_id
from
(
	select	--deals with either order or refund in last 180 days
	distinct unified_deal_id as deal_uuid,
	order_id
	from user_dw.v_fact_gbl_transactions
	where platform_key <> 2
	and country_id in (235, 40)
	and is_zero_amount = 0
	and user_brand_affiliation = 'groupon'
	and ((action = 'capture'
	and cast(order_date_ts as date) between (current_date - 180) and (current_date - 1))
	or (action = 'refund'
	and cast(transaction_date_ts as date) between (current_date - 180) and (current_date - 1)))
	union all
	select	--deals with either voucher activity (redemption, refund) or contacts in last 180 days
	distinct b.unified_deal_id as deal_uuid,
	a.order_id
	from
	(
		select
		cast(order_id as varchar(64)) as order_id
		from user_gp.camp_membership_coupons
		where cast(created_at as date) between (current_date - 180) and (current_date - 1)
		or cast(coalesce(customer_redeemed_at, merchant_redeemed_at) as date) between (current_date - 180) and (current_date - 1)
		or cast(refunded_at as date) between (current_date - 180) and (current_date - 1)
		union all
		select
		cast(order_id as varchar(64)) as order_id
		from user_dw.v_fact_zendesk_cases
		where cast(created_at_ts as date) between (current_date - 180) and (current_date - 1)
		and trycast(trim(order_id) as bigint) is not null
		and length(cast(order_id as varchar(64))) >= 8
	)a
	inner join user_dw.v_fact_gbl_transactions b on b.order_id = a.order_id
	where b.platform_key <> 2
	and b.country_id in (235, 40)
	and b.is_zero_amount = 0
	and b.user_brand_affiliation = 'groupon'
)a
where order_id <> '-1'
and length(deal_uuid) = 36
)with data primary index(deal_uuid, order_id) on commit preserve rows;

collect stats on na_deals_base_l180d column(deal_uuid, order_id);

--order and refund data


drop table na_deals_base_l180d_order_n_refund;

create multiset volatile table na_deals_base_l180d_order_n_refund as (
select
e.country_id,
e.deal_uuid,
e.order_id,
e.order_date,
e.units_sold,
e.nob_usd,
e.refund_date,
case when e.refund_date is not null then e.refund_reason_2 end as refund_reason,
case when e.refund_date is not null then coalesce(translate(f.international_code using latin_to_unicode), e.refund_reason) end as refund_reason_code,
g.cx_bucket as refund_reason_bucket,
h.category as refund_reason_category,
h.subcategory as refund_reason_subcategory,
case when lower(trim(h.experience)) = 'bad' then 1
	 when e.refund_date is not null then 0
end as bce_refund_flag,
case when refund_reason_code in ('2E8', '2E6', '2F9', '2E2', '2E1', '2E12', '2E18', '2F6', '2E9', '2D1', '2F8', '2E11', '2G4', '2E3', '2E10', '2F7') or e.refund_reason_2 like any('Merchant Doesn''t Have Availability%', 'Merchant doesn''t have ava%', 'Issue During/After Check-In%', 'Redemption Code Doesn''t Work on Merchant''s Website%', 'is-merchant-initiated%', 'Merchant Issue%') then 1 when e.refund_date is not null then 0 end as merchant_controllable_refund_reason_flag,
case when e.refund_date is not null then e.units_refunded end as units_refunded,
case when e.refund_date is not null then e.refund_amount_usd end as refund_amount_usd
from
(
	select
	country_id,
	unified_deal_id as deal_uuid,
	order_id,
	order_date,
	units_sold,
	nob_usd,
	min(refund_date) as refund_date,
	min(case when refund_date = refund_date_min then refund_reason end) as refund_reason,
	min(case when refund_date = refund_date_min then refund_reason_2 end) as refund_reason_2,
	sum(zeroifnull(transaction_qty)) as units_refunded,
	sum(zeroifnull(refund_amount_usd)) as refund_amount_usd
	from
	(
		select distinct
		a.country_id,
		a.unified_deal_id,
		a.order_id,
		a.order_date,
		a.units_sold,
		a.nob_usd,
		cast(b.transaction_date_ts as date) as refund_date,
		coalesce(trim(b.refund_reason), 'n/a') as refund_reason,
		cast(
		case when b.refund_reason is not null and lower(trim(b.refund_reason)) not in ('adjusting-order', 'cs_trade_in', 'other', 'resigned', 'self service exchange') and trim(b.refund_reason) not like '[REDACTED] ID%' then trim(b.refund_reason)
			 when lower(trim(b.refund_reason)) = 'adjusting-order' and c.cyclops_refund_comments like 'Voucher Exchange Refund - (Auto Extension):%' then 'Voucher Exchange Refund - (Auto Extension)'
			 when lower(trim(b.refund_reason)) = 'adjusting-order' and c.cyclops_refund_comments like 'Voucher Exchange Refund - (CS Agent):%' then 'Voucher Exchange Refund - (CS Agent)'
			 when lower(trim(b.refund_reason)) = 'adjusting-order' and c.cyclops_refund_comments like 'Voucher Exchange Refund - (Window Extended):%' then 'Voucher Exchange Refund - (Window Extended)'
			 when lower(trim(b.refund_reason)) = 'adjusting-order' and c.cyclops_refund_comments like 'Voucher Exchange Refund - (program_1):%' then 'Voucher Exchange Refund - (program_1)'
			 when lower(trim(b.refund_reason)) = 'adjusting-order' and c.cyclops_refund_comments like 'Voucher Exchange Refund - (program_2):%' then 'Voucher Exchange Refund - (program_2)'
			 when lower(trim(b.refund_reason)) = 'adjusting-order' and c.cyclops_refund_comments like 'Voucher Exchange Refund - %' then 'Voucher Exchange Refund'
			 when lower(coalesce(trim(b.refund_reason), 'n/a')) in ('cs_trade_in', 'other', 'resigned', 'self service exchange', 'n/a') and lower(coalesce(c.cyclops_refund_reason, 'n/a')) not in ('cs_trade_in', 'other', 'resigned', 'self service exchange', 'n/a') and c.cyclops_refund_reason not like '[REDACTED] ID%' then c.cyclops_refund_reason
			 else coalesce(c.cyclops_refund_comments, trim(b.refund_reason), 'n/a')
		end as varchar(2000) character set unicode not casespecific) as refund_reason_2,
		b.transaction_qty,
		b.refund_amount_usd,
		min(cast(b.transaction_date_ts as date)) over(partition by a.order_id, a.country_id) refund_date_min
		from
		(
			select
			country_id,
			unified_deal_id,
			order_id,
			cast(order_date_ts as date) as order_date,
			sum(zeroifnull(transaction_qty)) as units_sold,
			sum(zeroifnull(capture_nob_usd)) as nob_usd
			from user_dw.v_fact_gbl_transactions
			where (unified_deal_id, order_id) in (select distinct deal_uuid, order_id from na_deals_base_l180d)
			and action = 'capture'
			and platform_key <> 2
			and country_id in (235, 40)
			and gross_bookings_loc > 0
			and is_zero_amount = 0
			and user_brand_affiliation = 'groupon'
			group by 1, 2, 3, 4
		) a
		left join user_dw.v_fact_gbl_transactions b on b.order_id = a.order_id and b.country_id = a.country_id and b.action = 'refund'
		left join
		(
			select
			cast(order_id as varchar(64)) as order_id,
			trim(reason) as cyclops_refund_reason,
			trim(comments) as cyclops_refund_comments
			from user_gp.order_audit_records
			where amount < 0
			qualify row_number() over(partition by cast(order_id as varchar(64)) order by created_at desc) = 1
		)c on c.order_id = b.order_id
	)d
	group by 1, 2, 3, 4, 5, 6
)e
left join user_groupondw.refund_reasons_cs f on cast(f.id as varchar(3)) = (case when e.refund_reason_2 like 'customer-support-%' then substr(e.refund_reason_2, instr(e.refund_reason_2, '-', 1, 2) + 1, 3) end)
left join sandbox.reason_cx_bucketing_vw g on translate(g.reason using latin_to_unicode) = case when f.international_code is not null then translate(f.international_code using latin_to_unicode) else e.refund_reason_2 end
left join
(
	select
	international_code,
	category,
	subcategory,
	experience
	from user_groupondw.refund_reasons_cs
	group by 1, 2, 3, 4, status
	qualify row_number() over(partition by international_code order by status asc) = 1
)h on translate(h.international_code using latin_to_unicode) = case when f.international_code is not null then translate(f.international_code using latin_to_unicode) else e.refund_reason_2 end
)with data primary index(deal_uuid, order_id, country_id) on commit preserve rows;

collect stats on na_deals_base_l180d_order_n_refund column(order_id);

--vouchers data

create multiset volatile table na_deals_base_l180d_voucher as (
select distinct a.*,
b.int_value as voucher_redemption_rating
from
(
	select distinct cast(order_id as varchar(64)) as order_id,
	uuid as voucher_uuid,
	case when status = 'collected' then cast(coalesce(customer_redeemed_at, merchant_redeemed_at) as date) end as voucher_redeemed_date,
	cast(refunded_at as date) as voucher_refunded_date,
	cast(expires_at as date) as voucher_expiry_date
	from user_gp.camp_membership_coupons
	where cast(order_id as varchar(64)) in (
	select distinct 
	order_id
	from na_deals_base_l180d_order_n_refund
	)
)a
left join user_gp.ugc_answer b on cast(b.voucher_uuid as varchar(64)) = a.voucher_uuid and trim(b.source_key) not like all('%VOUCHER_REDEEMED_SURVEY_GOODS%', '%VOUCHER_REDEEMED_SURVEY_SHOPPING%', '%VOUCHER_REDEEMED_SURVEY_GETAWAYS%') and b.question_key = 'generalRating' and b.status = 'submitted'
qualify zeroifnull(row_number() over(partition by a.order_id, a.voucher_uuid order by case when b.int_value is not null then 1 else 0 end desc, case when b.created_at is not null then b.created_at end desc, case when b.updated_at is not null then b.updated_at end desc)) < 2
)with data primary index(order_id, voucher_uuid) on commit preserve rows;

collect stats on na_deals_base_l180d_voucher column(order_id);


/*
12668409 10390285 12668409
*/

--customer contacts data

select order_id, count(distinct zd_id) xyz from na_deals_base_l180d_cust_contacts group by 1 having xyz > 1;

drop table na_deals_base_l180d_cust_contacts;

create multiset volatile table na_deals_base_l180d_cust_contacts as (
select
distinct a.order_id,
a.zd_id,
cast(a.created_at_ts as date) as contact_created_date,
case when a.boldchat_record_id > 0 or a.tags like any('%contact_us%', '%contact-us%', '%c2c_callback%') then 'CS-Handled' end as service_type,
trim(b.reason_code) as contact_reason_code,
cast(coalesce(c.cx_bucket, 'n/a') as varchar(30)) as contact_reason_bucket,
case when d.category is not null and d.category like '%1. About a Groupon I haven"t bought%' then trim(substr('1. About a Groupon I haven''t bought', instr('1. About a Groupon I haven''t bought', '.', 1, 1) + 1, 80))
	 when d.category is not null and d.category like '%5. Not a customer query%' then trim(substr('5. Not a customer question', instr('5. Not a customer question', '.', 1, 1) + 1, 80))
	 when d.category is not null then trim(substr(d.category, instr(d.category, '.', 1, 1) + 1, 80))
	 else 'n/a'
end as contact_reason_category,
case when d.subcategory is not null then trim(substr(d.subcategory, instr(d.subcategory, '.', 1, 1) + 1, 50))
	 else 'n/a'
end as contact_reason_subcategory,
case when d.reason is not null then trim(substr(d.reason, instr(d.reason, '.', 1, 1) + 1, 130))
	 else 'n/a'
end as contact_reason_description
from user_dw.v_fact_zendesk_cases a
left join user_dw.v_dim_zendesk_reason b on b.reason_key = a.reason_key
left join sandbox.reason_cx_bucketing_vw c on c.reason = trim(b.reason_code)
left join dwh_base_sec_view.cs_contact_reasons d on upper(trim(d.reason_code)) = upper(trim(b.reason_code))
where a.order_id in (select distinct order_id from na_deals_base_l180d_order_n_refund)
)with data primary index(order_id, zd_id) on commit preserve rows;

collect stats on na_deals_base_l180d_cust_contacts column(order_id);



drop table sandbox.na_deals_l180d_all_data_raw_np;

create multiset table sandbox.na_deals_l180d_all_data_raw_np as (
select distinct 
trim(cast(oreplace(e.grt_l1_cat_name, 'L1 - ', '') as varchar(36))) as lob,
trim(cast(oreplace(e.grt_l2_cat_name, 'L2 - ', '') as varchar(36))) as l2_vertical,
trim(cast(oreplace(e.grt_l3_cat_name, 'L3 - ', '') as varchar(64))) as l3_vertical,
d.*
from
(
	select distinct a.*,
	b.voucher_uuid,
	b.voucher_redeemed_date,
	b.voucher_refunded_date,
	b.voucher_expiry_date,
	b.voucher_redemption_rating,
	c.zd_id,
	c.contact_created_date,
	c.service_type,
	c.contact_reason_code,
	c.contact_reason_bucket,
	c.contact_reason_category,
	c.contact_reason_subcategory,
	c.contact_reason_description
	from na_deals_base_l180d_order_n_refund a
	left join na_deals_base_l180d_voucher b on b.order_id = a.order_id
	left join na_deals_base_l180d_cust_contacts c on c.order_id = a.order_id
)d
left join user_edwprod.dim_gbl_deal_lob e on e.deal_id = d.deal_uuid and e.platform_key <> 2 and e.country_id = 235
)with data primary index(deal_uuid, order_id, country_id, voucher_uuid, zd_id);

grant select on sandbox.na_deals_l180d_all_data_raw_np to public;

/*
16700494 396767 13930522 12668409 1317550
*/

---refund is at both order and voucher level
---the contact is at order level
---customer rating seems to be at the voucher level
---multiplication could also be happening because of customer contacts can be more than one for the same order 

----ord based on order date

select
   *
from sandbox.np_insight_voice where ncx_ranking is null and merchant_uuid like '%1b83f%'

drop table sandbox.np_insight_voice;
create multiset table sandbox.np_insight_voice as (
select 
   fin.*, 
   case when t60_30_total_units <= 10 then 'Unavailable'
        when ncx_ranking <= 0.1 then 'Very poor'
        when ncx_ranking > 0.1 and ncx_ranking <= 0.35 then 'Poor'
        when ncx_ranking > 0.35 and ncx_ranking <= 0.65 then 'Fair'
        when ncx_ranking > 0.65 and ncx_ranking <= 0.9 then 'Good'
        when ncx_ranking > 0.9 then 'Excellent'
        end ncx_conclusion
from 
 (select 
    merchant_uuid, 
    country_code, 
    l2, 
    min(order_date) min_order_date, 
    max(order_date) max_order_date, 
    current_date - 61 start_date, 
    current_date - 31 end_date, 
    current_date last_update,
    count(distinct a.order_id) t60_30_total_orders,
    sum(a.units_sold) t60_30_total_units,
    count(distinct case when units_redeemed_within_30_days > 0 then a.order_id end) orders_redeemed_within_30_days,
    sum(a.units_redeemed_within_30_days) units_redeemed_within_30_days, 
    count(distinct case when units_refunded_within_30_days > 0 then a.order_id end) orders_refunded_within_30_days,
    coalesce(sum(a.units_refunded_within_30_days),0) units_refunded_within_30_days,
    cast(sum(a.units_refunded_within_30_days) as float)/nullifzero(t60_30_total_units) t60_30_refund_rate,
    coalesce(sum(a.contacts_within_30_days_of_order), 0) contacts_within_30_days,
    sum(t30_all_voucher_rating)/nullifzero(sum(t30_total_rating_count)) t30_average_ratings, 
    coalesce(sum(t30_less_than_three_rating), 0) t30_less_than_three_rating,
    cast((coalesce(sum(a.units_refunded_within_30_days),0) + coalesce(sum(t30_less_than_three_rating),0) + coalesce(sum(a.contacts_within_30_days_of_order),0)) as float)/nullifzero(sum(a.units_sold)) ncx_rate, 
    case when t60_30_total_units > 10 then 
        PERCENT_RANK() over(partition by l2, case when t60_30_total_units <= 10 then 1 else 0 end order by ncx_rate desc, merchant_uuid) 
        end ncx_ranking
from 
    (select 
        z.merchant_uuid, 
        z.country_code, 
        z.l2,
        x.country_id, 
        x.deal_uuid, 
        x.order_id, 
        x.order_date, 
        x.units_sold, 
        x.nob_usd, 
        count(distinct case when voucher_refunded_date <= order_date + 30 then voucher_uuid end) units_refunded_within_30_days,
        count(distinct case when voucher_redeemed_date <= order_date + 30 then voucher_uuid end) units_redeemed_within_30_days,
        count(distinct case when contact_created_date <= order_date + 30 then zd_id end) contacts_within_30_days_of_order
        from sandbox.na_deals_l180d_all_data_raw_np as x
        join sandbox.pai_deals as y on x.deal_uuid = y.deal_uuid 
        join sandbox.pai_merchants as z on y.merchant_uuid = z.merchant_uuid
        where order_date between (current_date - 61) and (current_date - 31) 
              and  z.country_code in ( 'US' ,'CA')
              and x.lob = 'Local'
        group by 1,2,3,4,5,6,7,8,9
     ) as a 
left join 
     (select 
        a.order_id,
        sum(case when voucher_redeemed_date <= order_date + 30 then voucher_redemption_rating end) t30_all_voucher_rating, 
        sum(case when voucher_redeemed_date <= order_date + 30 then 1 end) t30_total_rating_count, 
        sum(case when voucher_redemption_rating < 3 and voucher_redeemed_date <= order_date + 30 then 1 end) t30_less_than_three_rating
      from 
        (select 
           distinct 
            order_id, 
            order_date,
            voucher_redeemed_date,
            voucher_uuid, 
            voucher_redemption_rating
          from 
           sandbox.na_deals_l180d_all_data_raw_np 
          where order_date between (current_date - 61) and (current_date - 31)
         ) as a 
         group by 1
      ) as b on a.order_id = b.order_id
group by 1,2,3) as fin 
) with data;

grant select on sandbox.np_insight_voice to public;


--------------------------------------------------------------------Refund controllable
drop table np_temp; 
CREATE MULTISET volatile table np_temp as (
select 
   yu.*, 
   e.merchant_uuid
from 
(
		select 
			recommendation_id
			, entity_uuid
			, template_id
			, created_at
		from sb_merchant_experience.recommendations
				UNION ALL
		select 
			recommendation_id
			, entity_uuid
			, template_id
			, initial_creation as created_at
		from sb_merchant_experience.recommendations_tracker-- holds only completed recommendations. when a recommendation is completed it leaves sb_merchant_experience.recommendations and comes to this table
		where recommendation_id not in (select distinct recommendation_id from sb_merchant_experience.recommendations) -- sometimes the recommendation will still be in the other table dues to refresh schedule but we dont want any dupes so we take that out
		and initial_creation is not null
) yu
join sb_merchant_experience.merchant_advisor_entities e on e.entity_uuid = yu.entity_uuid
) with data on commit preserve rows;



select 
    COUNTRY_ID, 
    trunc(cast(fin.order_date as date), 'RM') order_month,
    lob, 
    l2_vertical,
    sum(units_sold) units_sold, 
    sum(case when refund_date <= order_date + 30 then units_refunded end) units_refunded_30d,
    sum(case when merchant_controllable_refund_reason_flag <> 1 and refund_date <= order_date + 30 then units_refunded end) noncontrollable_units_refunded_30d,
    sum(case when merchant_controllable_refund_reason_flag = 1 and refund_date <= order_date + 30 then units_refunded end) controllable_units_refunded_30d
from 
(select 
   distinct
    lob, 
    l2_vertical,
    COUNTRY_ID,
    order_date,
    a.deal_uuid, 
    order_id, 
    units_sold, 
    nob_usd, 
    refund_date, 
    refund_reason, 
    refund_reason_code, 
    refund_reason_bucket, 
    refund_reason_category, 
    refund_reason_subcategory, 
    merchant_controllable_refund_reason_flag, 
    units_refunded
from sandbox.na_deals_l180d_all_data_raw_np as a 
join sandbox.pai_deals pd on a.deal_uuid = pd.deal_uuid
join (select distinct merchant_uuid from np_temp) m on pd.merchant_uuid = m.merchant_uuid
where cast(order_date as date) >= cast('2022-07-01' as date)) as fin 
group by 1,2,3,4
order by 1 desc,2,3,4;

select * from sandbox.na_deals_l180d_all_data_raw_np sample 5;









--------------------------------------------------------------------OLD CODE



select * from sandbox.pai_merchants;

select distinct lob from sandbox.na_deals_l180d_all_data_raw;

create multiset volatile table np_vol_temp_table as 
(select 
        y.merchant_uuid, 
        y.country_code, 
        y.l2,
        x.country_id, 
        x.deal_uuid, 
        x.order_id, 
        x.order_date, 
        x.units_sold, 
        x.nob_usd, 
        count(distinct case when voucher_refunded_date <= order_date + 30 then voucher_uuid end) units_refunded_within_30_days,
        count(distinct case when voucher_redeemed_date <= order_date + 30 then voucher_uuid end) units_redeemed_within_30_days,
        count(distinct case when contact_created_date <= order_date + 30 then zd_id end) contacts_within_30_days_of_order
        from sandbox.na_deals_l180d_all_data_raw as x
        join sandbox.pai_deals as y on x.deal_uuid = y.deal_uuid 
        where order_date between (current_date - 61) and (current_date - 31) 
              and  y.country_code in ( 'US' ,'CA')
              and y.l1 = 'Local'
        group by 1,2,3,4,5,6,7,8,9
     ) with data on commit preserve rows;

    
select 
   merchant_uuid, 
   count(1) xyz 
from sandbox.np_insight_voice 
group by 1 having xyz > 1;


select * from np_vol_temp_table where merchant_uuid = 'e50a498f-5ebc-478e-b2b1-75bdacd3cf9f';

select * from sandbox.pai_deals where merchant_uuid  = '927fdb09-5816-449f-9c96-7b2ca75fb41a';
select * from sandbox.pai_merchants where merchant_uuid =  '927fdb09-5816-449f-9c96-7b2ca75fb41a'

/* testing
 * 

drop table np_temp;

create multiset volatile table np_temp as (
select distinct country_id, deal_uuid, order_id, order_date, units_sold, nob_usd from sandbox.na_deals_l180d_all_data_raw
) with data on commit preserve rows;
select order_id, count(1) xyz from np_temp group by 1 having xyz > 1

**/

drop table np_merch_ord;
create multiset volatile table np_merch_ord as 
(select 
    b.merchant_uuid, 
    min(order_date) min_date, 
    max(order_date) max_date, 
    count(distinct a.order_id) t60_30_total_orders, 
    sum(units_sold) t60_30_total_units
from (select distinct country_id, deal_uuid, order_id, order_date, units_sold, nob_usd from sandbox.na_deals_l180d_all_data_raw) as a 
left join sandbox.pai_deals as b on a.deal_uuid = b.deal_uuid 
where a.order_date between (current_date - 61) and (current_date - 31)
group by 1
) with data on commit preserve rows; 


----based on redemption ---not tied to order



drop table np_merch_rating_red;
create multiset volatile table np_merch_rating_red as 
(select 
    b.merchant_uuid, 
    count(distinct case when voucher_redeemed_date is not null then order_id end) t30_order_redeemed,
    count(distinct case when voucher_redeemed_date is not null then voucher_uuid end) t30_units_redeemed,
    avg(voucher_redemption_rating) t30_avg_voucher_rating, 
    sum(case when voucher_redemption_rating < 3 then 1 end) less_than_three_rating
from 
      (select 
        distinct 
        country_id, 
        deal_uuid, 
        order_id, 
        order_date,
        voucher_redeemed_date,
        voucher_uuid, 
        voucher_redemption_rating
      from 
      sandbox.na_deals_l180d_all_data_raw 
      where voucher_redeemed_date between (current_date - 31) and (current_date - 11)
      ) as a 
left join sandbox.pai_deals as b on a.deal_uuid = b.deal_uuid 
group by 1
) with data on commit preserve rows;


-----refund rate base on 30 day from order date

drop table np_merch_ref;
create multiset volatile table np_merch_ref as 
(select 
    b.merchant_uuid, 
    count(distinct a.order_id) t60_total_orders, 
    sum(a.units_sold) t60_total_units,
    sum(a.units_refunded_within_30_days) units_refunded_within_30_days
from 
    (select 
        country_id, 
        deal_uuid, 
        order_id, 
        order_date, 
        units_sold, 
        nob_usd, 
        count(distinct case when voucher_refunded_date <= order_date + 30 then voucher_uuid end) units_refunded_within_30_days
        from sandbox.na_deals_l180d_all_data_raw
        where order_date between (current_date - 61) and (current_date - 31)
        group by 1,2,3,4,5,6
     ) as a 
left join sandbox.pai_deals as b on a.deal_uuid = b.deal_uuid 
group by 1
) with data on commit preserve rows; 

create multiset volatile table np_merch_ref_2 as 
(select 
    b.merchant_uuid, 
    count(distinct a.order_id) t30_orders_ref, 
    count(distinct a.voucher_uuid) t30_units_ref 
from sandbox.na_deals_l180d_all_data_raw as a
left join sandbox.pai_deals as b on a.deal_uuid = b.deal_uuid 
where voucher_refunded_date between (current_date - 31) and (current_date - 1)
group by 1
) with data on commit preserve rows; 


----Contacts 

drop table np_merch_cont;
create multiset volatile table np_merch_cont as 
(select 
    b.merchant_uuid, 
    count(distinct zd_id) t30_contacts
from 
    (select 
        distinct 
        country_id, 
        deal_uuid, 
        order_id, 
        zd_id
        from sandbox.na_deals_l180d_all_data_raw
        where contact_created_date between (current_date - 31) and (current_date - 1)
     ) as a 
left join sandbox.pai_deals as b on a.deal_uuid = b.deal_uuid 
group by 1
) with data on commit preserve rows; 


-----

drop table sandbox.np_insight_voice;
create multiset table sandbox.np_insight_voice as (
select 
   main.merchant_uuid, 
   main.country_code, 
   main.l2, 
   a.min_date min_order_date, 
   a.max_date max_order_date,
   current_date - 31 start_date, 
   current_date - 1 end_date, 
   a.t30_total_orders t30_orders_sold,
   a.t30_total_units t30_units_sold,
   b.t30_order_redeemed,
   b.t30_units_redeemed,
   b.t30_avg_voucher_rating,
   b.less_than_three_rating,
   cast(c.units_refunded_within_30_days as float)/nullifzero(c.t60_total_units) t60_30_refund_rate, 
   d.t30_units_ref t30_units_refunded,
   e.t30_contacts t30_cust_contact, 
   cast((d.t30_units_ref + b.less_than_three_rating + e.t30_contacts) as float)/nullifzero(t30_total_units) ncx_rate
from sandbox.pai_merchants main 
left join np_merch_ord as a on main.merchant_uuid = a.merchant_uuid
left join np_merch_rating_red as b on main.merchant_uuid = b.merchant_uuid
left join np_merch_ref as c on main.merchant_uuid = c.merchant_uuid
left join np_merch_ref_2 as d on main.merchant_uuid = d.merchant_uuid 
left join np_merch_cont as e on main.merchant_uuid = e.merchant_uuid
where main.country_code in ( 'US' ,'CA')
and l1 = 'Local'
) with data
;


grant select on sandbox.np_insight_voice to public;

drop table sandbox.np_insight_voice;
create multiset table sandbox.np_insight_voice (
merchant_uuid varchar(64) character set unicode,
l2 varchar(64) character set unicode,
start_date date, 
end_date date, 
last_updated date, 
t30_units_sold integer, 
t30_units_refunded integer, 
t30_refund_rate float,
t30_refund_rate_l2 float,
t30_neg_feedback integer,
t30_neg_feedback_rate float,
t30_neg_feedback_l2_rate float,
t30_cust_contact integer,
t30_cust_contact_rate float,
t30_cust_contact_l2_rate float,
ncx_rate float
) no primary index;

----

select order_id, max(units_sold) max_units, count(1) xyz, count(distinct voucher_uuid) total_vouchers 
from sandbox.na_deals_l180d_all_data_raw group by 1

select order_id, max(units_sold) max_units, count(1) xyz, count(distinct voucher_uuid) total_vouchers 
from sandbox.na_deals_l180d_all_data_raw group by 1
having total_vouchers <> max_units;



select order_id, count(distinct zd_id) xyz from sandbox.na_deals_l180d_all_data_raw where units_sold = 1 group by 1 having xyz > 1;

select * from sandbox.na_deals_l180d_all_data_raw where order_id = '1544053871';

drop table np_merch_red;



or (refund_date between (current_date - 31) and (current_date - 1))
or (voucher_redeemed_date between (current_date - 31) and (current_date - 1))
or ()

---------ROI
/*******************************************************************************
Purpose: Logic for mechant roi tables
Optimus Job: https://optimus.groupondev.com/#/jobs/edit/96935

Edits:
2022-08-23 Updated comments
2022-08-29 Added in last comments

Callouts: WE NEED REMITTANCE. This isnt remittance but a proxy
********************************************************************************/

---- this is for CLO data, this query was found in CLO FAQs


sb_merchant_experience.merchant_insights_answers

select * 
from user_edwprod.fact_gbl_ogp_transactions 
where "action" = 'authorize' 
and user_brand_affiliation ='groupon' and platform_key =1
and order_id = '1530600445'
sample 5;

select * 
from user_edwprod.fact_gbl_transactions fgt
where fgt.source_key<>'CLO'
and "action"='capture'
and fgt.is_order_canceled = 0
and fgt.is_zero_amount = 0
and fgt.order_date> '2021-12-31'
and fgt.country_id in ('235', '40')
and fgt.platform_key = 1
and fgt.discount_amount_loc <> 0
sample 5;/Users/nihpatel/Documents/Atom Scripts/Merchant Focus/Merchant Insights.sql

select * 
from user_edwprod.fact_gbl_transactions fgt
where fgt.source_key<>'CLO'
and "action"='capture'
and fgt.is_order_canceled = 0
and fgt.is_zero_amount = 0
and fgt.order_date> '2021-12-31'
and fgt.country_id in ('235', '40')
and fgt.platform_key = 1
and fgt.deal_uuid = '1a04bd7f-1302-4d1f-a893-4a8dbde6f8fd'
sample 5;


select * from sandbox.merchant_adviser_cohort_30_view where Target_category = 'Merchant Adviser Notification';
select * from sandbox.jrg_refund_rec jrr; 




/*
 * 	, ((order_rev*units_redeemed) + clo_rev) order_sales
	, (order_rev*units_redeemed) groupon_sales
	, clo_rev outside_sales
	
	select 
   merchant_uuid
	, user_uuid
	, order_id
	, redemption_date
	, margin_amount
	, discount
	, vfm_crd
	, order_cost
from sandbox.jrg_user_trxns
 where merchant_uuid = '849c3c0e-f389-4235-9781-fd042847274d'
 and user_uuid = '4d41ea90-e43d-11e6-ae71-a45e60e86e11'
;

select 
   discount
   ,coalesce(discount, 0) discount_x
   , vfm+credit_card_fee_loc vfm_crd
   , vfm
   ,credit_card_fee_loc
	, coalesce(vfm, 0) +coalesce(credit_card_fee_loc, 0) vfm_crd_x
	,unit_sell_price * (margin_percent/100) margins
	, coalesce((unit_sell_price * (margin_percent/100)), 0) margin_amount_x
	, coalesce(((unit_sell_price * (margin_percent/100)) + credit_card_fee_loc + discount+ vfm),0) order_cost_x
from jrg_orderss
 where merchant_uuid = '849c3c0e-f389-4235-9781-fd042847274d'
 and user_id = '4d41ea90-e43d-11e6-ae71-a45e60e86e11';
 */



create volatile table jrg_clo as (
select
	vt.id
	, vt.merchant_id
	, pm.merchant_uuid
	, pm.merchant_name
	, vt.processed_at
	, u.consumer_id
	, vt.groupon_transaction_group_id
	, qt.merchant_charge
	, qt.incentive_amount
	, qt.total_reward_amount
	, qt.base_amount --customer_spend will sometime be 0 since the data disappears for legal reasons
from user_gp.clo_qualified_transactions qt
join user_gp.clo_visa_transactions vt on vt.id = qt.network_transaction_id and vt.raw_transaction_type = 'Clear'
join user_gp.clo_claims cl on cl.id = vt.claim_id
join user_gp.clo_users u on u.id = cl.user_id
left join user_gp.clo_merchants cms on cms.id = vt.merchant_id
join sandbox.pai_merchants pm on pm.merchant_uuid = cms.m3_merchant_id
where pm.l1='Local'
and pm.country_code in ('US', 'CA')
)with data unique primary index(id) on commit preserve rows;


create volatile table jrg_orderss as (
select
	fgt.order_id
	, fgt.order_date
	, fgt.unified_user_id user_id
	, fgt.merchant_uuid
	, fgt.transaction_qty
	, trunc(v.redeemed_at) redemption_date
	, fgt.unit_buy_price
	, fgt.unit_sell_price
	, fgt.margin_percent
	, -fgt.discount_amount_loc discount_amount_loc
	, (-fgt.discount_amount_loc)/NULLIFZERO (fgt.transaction_qty) discount -- need the discount per unit
	, fgt.credit_card_fee_loc/NULLIFZERO (fgt.transaction_qty) ---why is this not discounted per units 
	, -ogp.vfm vfm_amount_loc
	, (ogp.vfm )/NULLIFZERO (fgt.transaction_qty) vfm -- getting it at a unit level
	, count(distinct case when v.redeemed_at is not null then voucher_barcode_id|| code end) units_redeemed
from user_edwprod.fact_gbl_transactions fgt
left join user_groupondw.acctg_red_voucher_base v on cast(v.order_id as varchar(64)) = fgt.order_id and fgt.unified_user_id = v.user_uuid
left join (select
				order_id
				, user_uuid
				, merchant_uuid
				, sum(vfm_amount_loc) vfm
			from user_edwprod.fact_gbl_ogp_transactions
			where "action" = 'authorize' and user_brand_affiliation ='groupon' and platform_key =1
			group by 1,2,3
           ) ogp on ogp.order_id = fgt.order_id and ogp.merchant_uuid  = fgt.merchant_uuid and ogp.user_uuid = fgt.unified_user_id
where fgt.source_key<>'CLO'
and "action"='capture'
and fgt.is_order_canceled = 0
and fgt.is_zero_amount = 0
and fgt.order_date> '2016-12-31'
and fgt.country_id in ('235', '40')
and fgt.platform_key = 1
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
) with data primary index(order_id, redemption_date) on commit preserve rows;





---credit card fees is not per unit -- can change that
drop table sandbox.jrg_user_trxns;
create table sandbox.jrg_user_trxns as (
select
	a.order_id
	, a.redemption_date
	, a.user_uuid
	, a.merchant_uuid
	, coalesce(o.unit_sell_price,0) unit_sell_price
	, coalesce((o.margin_percent/100),0) margin
	, coalesce(o.credit_card_fee_loc,0) credit_card_fee
	, coalesce(o.discount_amount_loc,0) discount_amt
	, coalesce(o.discount, 0) discount
	, coalesce(o.vfm, 0) +coalesce(o.credit_card_fee_loc, 0) vfm_crd
	, coalesce((o.unit_sell_price * (o.margin_percent/100)), 0) margin_amount
	, coalesce((coalesce(o.unit_sell_price * (o.margin_percent/100),0) + coalesce(o.credit_card_fee_loc,0) + coalesce(o.discount, 0)+ coalesce(o.vfm, 0)),0) order_cost
	, coalesce((o.unit_sell_price - (coalesce(o.unit_sell_price * (o.margin_percent/100),0) + coalesce(o.credit_card_fee_loc,0) + coalesce(o.discount, 0)+ coalesce(o.vfm, 0))),0) order_rev
	, coalesce((cl.base_amount)/100,0) clo_rev
	, coalesce(o.units_redeemed,0) units_redeemed
from
(
		select distinct
			a1.*
		from (
				select
					groupon_transaction_group_id order_id
					, trunc(processed_at) redemption_date
					, consumer_id user_uuid
					, merchant_uuid
					, 'CLO' sources
				from jrg_clo
					UNION ALL
				select
					order_id
					, redemption_date
					, user_id user_uuid
					, merchant_uuid
					, 'TRNS' as sources
				from jrg_orderss
				where redemption_date is not null
		) a1
	) a
left join jrg_orderss o
	on o.order_id = a.order_id
	and o.redemption_date = a.redemption_date
	and o.redemption_date is not null
left join jrg_clo cl
	on cl.groupon_transaction_group_id = a.order_id
)with data primary index(order_id, redemption_date);

grant select on sandbox.jrg_user_trxns to public;

---- this section can probably be done a more effeicient way. but i do this to get users that are repeat after using groupon and getting that frequency
create volatile table jrg_order_timeline as (
select
	a.user_uuid
	, a.merchant_uuid
	, a.order_id
	, a.redemption_date
	, case when a.order_id like'%-%' then 'CLO' else 'NA' end sources
	, row_number() over (partition by a.user_uuid , a.merchant_uuid order by a.redemption_date desc) order_time
	, row_number() over (partition by a.user_uuid , a.merchant_uuid order by a.redemption_date asc) original_time
from (
		select
			user_uuid
			, merchant_uuid
			, order_id
			, max(redemption_date) redemption_date
		from sandbox.jrg_user_trxns
		group by 1,2,3
		) a
 join (
 		select
			user_uuid
			, merchant_uuid
			, count(distinct order_id) orders
		from sandbox.jrg_user_trxns
		having count(distinct order_id)>1 -- get customers that repeated
		group by 1,2
		) b
	on a.user_uuid = b.user_uuid
	and a.merchant_uuid  = b.merchant_uuid
) with data primary index(order_id)	on commit preserve rows;

create volatile table jrg_testing_meas as (
select
	a.user_uuid
	, a.merchant_uuid
	, a.order_id
	, a.redemption_date
	, a.sources
	, a.original_time
	, b.redemption_date next_redemption
	, b.sources next_source
	, b.original_time next_order
from jrg_order_timeline a
join jrg_order_timeline b
	on a.user_uuid = b.user_uuid
	and a.merchant_uuid = b.merchant_uuid
	and a.original_time+1 = b.original_time
)with data primary index(order_id)	on commit preserve rows;

create volatile table jrg_repeats as ( --- used to get repeat customers
select
 merchant_uuid
 , count(distinct case when sources='NA' and next_source='CLO' then user_uuid end) repeat_out_of_groupon
 , count(distinct case when sources='NA' and next_source='NA' then user_uuid end) repeat_in_groupon
 , count(distinct case when sources='NA' then user_uuid end) repeat_customers
from (
select
	a.user_uuid
	, a.merchant_uuid
	, a.order_id
	, a.redemption_date
	, a.sources
	, a.original_time
	, b.redemption_date next_redemption
	, b.sources next_source
	, b.original_time next_order
from jrg_order_timeline a
join jrg_order_timeline b
	on a.user_uuid = b.user_uuid
	and a.merchant_uuid = b.merchant_uuid
	and a.original_time+1 = b.original_time
) ad
group by 1
) with data primary index(merchant_uuid) on commit preserve rows;


--this should only be getting frequency outside of groupon


----FREQUENCY LOGIC BASED ON THE FINAL ROI_TABLE
-----OLD LOGIC BASED ON TIME BETWEEN PURCHASE
-----CHANGING THIS FROM only repeat using this jrg_order_timeline table to the main table
/*create volatile table jrg_frequency as (
select
	merchant_uuid
	, avg(time_apart) avg_frequency
	, avg(case when redemption_date between CURRENT_DATE -180 and CURRENT_DATE then time_apart end) avg_frequency_180
	, avg(case when order_id like'%-%' and redemption_date between CURRENT_DATE -180 and CURRENT_DATE then time_apart end) avg_frequency_180_out
	, avg(case when order_id like'%-%' then time_apart end) avg_frequency_out
from (
select
	a.user_uuid
	, a.merchant_uuid
	, a.order_id
	, a.redemption_date
	, b.redemption_date next_redemptions
	, a.redemption_date - b.redemption_date time_apart -- days between the redemptions/visits
from  jrg_order_timeline a
join jrg_order_timeline b
	on b.user_uuid = a.user_uuid
	and b.merchant_uuid = a.merchant_uuid
	and b.order_time = a.order_time+1
where 
a.user_uuid||a.merchant_uuid in (select user_uuid||merchant_uuid from jrg_testing_meas where sources='NA' and next_source='CLO' ) 
-- only get customers that first visited through groupon then on their own
) a
group by 1
)with data primary index(merchant_uuid)	on commit preserve rows;*/




------------------------------------------------------------------------------------

--- get the new customers in the last 180 days

create volatile table jrg_new_customers as (
select
merchant_uuid
, count(distinct case when original_time=1 then user_uuid end) new_customers
from (
select
	user_uuid
	, merchant_uuid
	, order_id
	, redemption_date
	, row_number() over (partition by user_uuid , merchant_uuid order by redemption_date asc) original_time
from sandbox.jrg_user_trxns
where order_id not like'%-%'
) a
where redemption_date between current_date - 180 and current_date
group by 1
)with data unique primary index (merchant_uuid) on commit preserve rows;


------ this is building the final table ---Only redeemed units are considered


create volatile table jrg_user_merc as (
select
merchant_uuid
, user_uuid
, sum(order_sales) lifetime_sales
, sum(case when redemption_date between CURRENT_DATE -30 and CURRENT_DATE then order_sales end) sales_30d
, sum(case when redemption_date between CURRENT_DATE -90 and CURRENT_DATE then order_sales end) sales_90d
, sum(case when redemption_date between CURRENT_DATE -180 and CURRENT_DATE then order_sales end) sales_180d
, sum(order_cost) lifetime_cost
, sum(case when redemption_date between CURRENT_DATE -30 and CURRENT_DATE then order_cost end) cost_30d
, sum(case when redemption_date between CURRENT_DATE -90 and CURRENT_DATE then order_cost end) cost_90d
, sum(case when redemption_date between CURRENT_DATE -180 and CURRENT_DATE then order_cost end) cost_180d
, sum(margin_cost) margin_cost_all
, sum(case when redemption_date between CURRENT_DATE -180 and CURRENT_DATE then margin_cost end) margin_cost_180d
, sum(discount_cost) discount_cost_all
, sum(case when redemption_date between CURRENT_DATE -180 and CURRENT_DATE then discount_cost end) discount_cost_180d
, sum(vfm_crd_cost) vfm_crd_cost_all
, sum(case when redemption_date between CURRENT_DATE -180 and CURRENT_DATE then vfm_crd_cost end) vfm_crd_cost_180d
, sum(groupon_sales) groupon_sales
, sum(case when redemption_date between CURRENT_DATE -30 and CURRENT_DATE then groupon_sales end) groupon_sales_30d
, sum(case when redemption_date between CURRENT_DATE -90 and CURRENT_DATE then groupon_sales end) groupon_sales_90d
, sum(case when redemption_date between CURRENT_DATE -180 and CURRENT_DATE then groupon_sales end) groupon_sales_180d
, sum(outside_sales) outside_sales
, sum(case when redemption_date between CURRENT_DATE -30 and CURRENT_DATE then outside_sales end) outside_sales_30d
, sum(case when redemption_date between CURRENT_DATE -90 and CURRENT_DATE then outside_sales end) outside_sales_90d
, sum(case when redemption_date between CURRENT_DATE -180 and CURRENT_DATE then outside_sales end) outside_sales_180d
from (
select
	merchant_uuid
	, user_uuid
	, order_id
	, redemption_date
	, (margin_amount*units_redeemed) margin_cost
	, (discount*units_redeemed) discount_cost
	, (vfm_crd*units_redeemed) vfm_crd_cost
	, (order_cost*units_redeemed) order_cost
	, ((order_rev*units_redeemed) + clo_rev) order_sales
	, (order_rev*units_redeemed) groupon_sales
	, clo_rev outside_sales
from sandbox.jrg_user_trxns
where coalesce(merchant_uuid,'')<>''
and coalesce(user_uuid,'')<>''
) a
group by 1,2
)with data unique primary index(merchant_uuid, user_uuid) on commit preserve rows;



---understand why this <> matters

----unnecessary join it seems to me

drop table jrg_roi;
create volatile table jrg_roi as (
select
	a.merchant_uuid
	, sum(a.lifetime_sales) total_sales ----clo revenue + groupon revenue
	, avg(a.lifetime_sales) avg_sales
	, sum(a.sales_180d) sales_180d
	, avg(a.sales_180d) avg_sales_180d
	, sum(a.lifetime_cost) total_cost
	, avg(a.lifetime_cost) avg_cost
	, sum(a.margin_cost_all) total_margin_cost
	, avg(a.margin_cost_all) avg_margin_cost
	, sum(a.discount_cost_all) total_discount_cost
	, avg(a.discount_cost_all) avg_discount_cost
	, sum(a.vfm_crd_cost_all) total_vfm_crd_cost
	, avg(a.vfm_crd_cost_all) avg_vfm_crd_cost
	, sum(a.cost_180d) cost_180
	, avg(a.cost_180d) avg_cost_180
	, sum(a.margin_cost_180d) margin_cost_180
	, avg(a.margin_cost_180d) avg_margin_cost_180
	, sum(a.discount_cost_180d) discount_cost_180
	, avg(a.discount_cost_180d) avg_discount_cost_180
	, sum(a.vfm_crd_cost_180d) vfm_crd_cost_180d
	, avg(a.vfm_crd_cost_180d) avg_vfm_crd_cost_180d
	, sum(a.groupon_sales) total_groupon_sales ---groupon revenue
	, avg(a.groupon_sales) avg_groupon_sales
	, sum(a.groupon_sales_180d) groupon_sales_180d
	, avg(a.groupon_sales_180d) avg_groupon_sales_180
	, sum(outside_sales) total_outside_sales
	, avg(outside_sales) avg_outside_sales
	, count(distinct case when b.groupon_orders>0 then a.user_uuid end) total_customers
	, count(distinct case when b.groupon_orders180>0 then a.user_uuid end) customers_last180
	, count(distinct case when b.outside_order>0 then a.user_uuid end) total_outside_customers
	, count(distinct case when b.outside_orders180>0 then a.user_uuid end) outside_customers_last180
	, sum(visits)/NULLIFZERO(count(distinct case when visits >0 then a.user_uuid end))  purc_freq_all
	, sum(visits_180)/NULLIFZERO(count(distinct case when visits_180 >0 then a.user_uuid end))  purc_freq_all_180
	, sum(groupon_orders)/NULLIFZERO(total_customers)  purc_freq_groupon
	, sum(groupon_orders180)/NULLIFZERO(customers_last180)  purc_freq_groupon_180
	, sum(outside_order)/NULLIFZERO(total_outside_customers) purc_freq_outside
	, sum(outside_orders180)/NULLIFZERO(outside_customers_last180) purc_freq_outside_180
from jrg_user_merc a
left join (
			select
				merchant_uuid
				, user_uuid
				, count(distinct order_id) visits
				, count(distinct case when redemption_date between CURRENT_DATE -180 and CURRENT_DATE then order_id end) visits_180
				, count(distinct case when order_id not like '%-%' then order_id end) groupon_orders
				, count(distinct case when order_id not like '%-%'  and redemption_date between CURRENT_DATE -180 and CURRENT_DATE then order_id end) groupon_orders180
				, count(distinct case when order_id like '%-%' then order_id end) outside_order
				, count(distinct case when order_id like '%-%'  and redemption_date between CURRENT_DATE -180 and CURRENT_DATE then order_id end) outside_orders180
			from sandbox.jrg_user_trxns
			group by 1,2
		) b on b.merchant_uuid = a.merchant_uuid and b.user_uuid = a.user_uuid
group by 1
)with data unique primary index(merchant_uuid) on commit preserve rows;

----groupon 
-------in and out (clo)

----clo 
-------in and out 




----- this is the final table - Everything is calculated at unit level and then aggregated. 
/*		, ro.outside_customers_last180
		, ro.purc_freq_all 
		, ro.purc_freq_groupon
		, ro.purc_freq_groupon_180
		, ro.purc_freq_outside
		*/
---- avg_frequency_of_repeat_visit_in_days - this is outside frequency in the next table
select * from sandbox.jrg_merchant_roi where avg_frequency_of_repeat_visit_in_days is not null;

drop table sandbox.jrg_merchant_roi;

create table sandbox.jrg_merchant_roi as (
	select
		ro.merchant_uuid
		, ro.total_customers --only groupon
		, ro.total_outside_customers
		, coalesce(jr.repeat_customers,0) repeat_customers
		, coalesce(jr.repeat_in_groupon,0) repeat_in_groupon
		, coalesce(jr.repeat_out_of_groupon,0) repeat_out_groupon
		, coalesce(ro.customers_last180,0) customers_last180--only groupon
		, coalesce(purc_freq_outside_180, 0) avg_frequency_of_repeat_visit_in_days
		--, coalesce(180/nullifzero(jf.avg_frequency_180_out),0) avg_frequency_of_repeat_visit_in_days
		, coalesce(ro.total_cost,0) total_cost
		, coalesce(ro.avg_cost,0) avg_cost
		, coalesce(ro.total_margin_cost, 0) total_margin_cost
		, coalesce(ro.avg_margin_cost, 0) avg_margin_cost
		, coalesce(ro.total_discount_cost, 0) total_discount_cost
		, coalesce(ro.avg_discount_cost, 0) avg_discount_cost
		, coalesce(ro.total_vfm_crd_cost, 0) total_vfm_crd_cost
		, coalesce(ro.avg_vfm_crd_cost, 0) avg_vfm_crd_cost
		, coalesce(ro.cost_180,0) cost_180
		, coalesce(ro.avg_cost_180,0) avg_cost_180
		, coalesce(ro.margin_cost_180, 0) margin_cost_180
		, coalesce(ro.avg_margin_cost_180, 0) avg_margin_cost_180
		, coalesce(ro.discount_cost_180, 0) discount_cost_180
		, coalesce(ro.avg_discount_cost_180, 0) avg_discount_cost_180
		, coalesce(ro.vfm_crd_cost_180d, 0) vfm_crd_cost_180d
		, coalesce(ro.avg_vfm_crd_cost_180d, 0) avg_vfm_crd_cost_180d
		, coalesce(ro.total_groupon_sales,0) total_groupon_sales
		, coalesce(ro.avg_groupon_sales,0) avg_groupon_sales
		, coalesce(ro.groupon_sales_180d,0) groupon_sales_180d
		, coalesce(ro.avg_groupon_sales_180,0) avg_groupon_sales_180
		, coalesce(ro.total_outside_sales,0) total_outside_sales
		, coalesce(ro.avg_outside_sales,0) avg_outside_sales
		, coalesce(nc.new_customers,0) new_customers
		, pm.l2
		, CURRENT_DATE() updated_at
    , ( CURRENT_DATE() - 180) start_date
	from jrg_roi ro
	join sandbox.pai_merchants pm on pm.merchant_uuid = ro.merchant_uuid
	left join jrg_repeats jr on jr.merchant_uuid = ro.merchant_uuid
	--left join jrg_frequency jf on jf.merchant_uuid = ro.merchant_uuid
	left join jrg_new_customers nc on nc.merchant_uuid = ro.merchant_uuid
	where pm.l1='Local'
	and pm.country_code in ('US','CA')
	and pm.is_live=1
  and pm.l2='HBW'
	and coalesce(ro.groupon_sales_180d,0) >0
)with data unique primary index(merchant_uuid);

grant select on sandbox.jrg_merchant_roi to public;


create multiset volatile table np_merchroi_all_l2temp as (
	select
		ro.merchant_uuid
		, ro.total_customers --only groupon
		, ro.total_outside_customers
		, coalesce(jr.repeat_customers,0) repeat_customers
		, coalesce(jr.repeat_in_groupon,0) repeat_in_groupon
		, coalesce(jr.repeat_out_of_groupon,0) repeat_out_groupon
		, ro.customers_last180 --only groupon
		, purc_freq_outside_180 avg_frequency_of_repeat_visit_in_days
		--, coalesce(180/nullifzero(jf.avg_frequency_180_out),0) avg_frequency_of_repeat_visit_in_days
		, coalesce(ro.total_cost,0) total_cost
		, coalesce(ro.avg_cost,0) avg_cost
		, coalesce(ro.total_margin_cost, 0) total_margin_cost
		, coalesce(ro.avg_margin_cost, 0) avg_margin_cost
		, coalesce(ro.total_discount_cost, 0) total_discount_cost
		, coalesce(ro.avg_discount_cost, 0) avg_discount_cost
		, coalesce(ro.total_vfm_crd_cost, 0) total_vfm_crd_cost
		, coalesce(ro.avg_vfm_crd_cost, 0) avg_vfm_crd_cost
		, coalesce(ro.cost_180,0) cost_180
		, coalesce(ro.avg_cost_180,0) avg_cost_180
		, coalesce(ro.margin_cost_180, 0) margin_cost_180
		, coalesce(ro.avg_margin_cost_180, 0) avg_margin_cost_180
		, coalesce(ro.discount_cost_180, 0) discount_cost_180
		, coalesce(ro.avg_discount_cost_180, 0) avg_discount_cost_180
		, coalesce(ro.vfm_crd_cost_180d, 0) vfm_crd_cost_180d
		, coalesce(ro.avg_vfm_crd_cost_180d, 0) avg_vfm_crd_cost_180d
		, coalesce(ro.total_groupon_sales,0) total_groupon_sales
		, coalesce(ro.avg_groupon_sales,0) avg_groupon_sales
		, coalesce(ro.groupon_sales_180d,0) groupon_sales_180d
		, coalesce(ro.avg_groupon_sales_180,0) avg_groupon_sales_180
		, coalesce(ro.total_outside_sales,0) total_outside_sales
		, coalesce(ro.avg_outside_sales,0) avg_outside_sales
		, coalesce(nc.new_customers,0) new_customers
		, pm.l2
		, CURRENT_DATE() updated_at
    , ( CURRENT_DATE() - 180) start_date
	from jrg_roi ro
	join sandbox.pai_merchants pm on pm.merchant_uuid = ro.merchant_uuid
	left join jrg_repeats jr on jr.merchant_uuid = ro.merchant_uuid
	left join jrg_new_customers nc on nc.merchant_uuid = ro.merchant_uuid
	where pm.l1='Local'
	and pm.country_code in ('US','CA')
	and pm.is_live=1
	and coalesce(ro.groupon_sales_180d,0) >0 
)with data unique primary index(merchant_uuid) on commit preserve rows;


drop table sandbox.np_merchroi_all_l2;
create multiset table sandbox.np_merchroi_all_l2 as 
(select 
  l2, 
  sum(total_customers) total_customer,
  sum(total_cost) total_cost,
  sum(total_groupon_sales) total_groupon_sales, 
  sum(total_groupon_sales)/sum(total_customers) avg_groupon_sales
from 
np_merchroi_all_l2temp 
group by 1) with data;
grant select on sandbox.np_merchroi_all_l2 to public;


select * from sandbox.np_merchroi_all_l2;

------------------ used to get roi merchants for pilot

select
	roi.merchant_uuid
	, pm.account_id
	, pm.merchant_name
	, pm.l2
	, ad.live_deals
	, ad.days_live
	, mc.visits as mc_visits_in_last30
	, cast(smm.total_emails_opened as dec(18,3))/ nullifzero(cast(smm.total_emails as dec(18,3))) pct_emails_opened
	, cast(smm.total_emails_clicked as dec(18,3))/ nullifzero(cast(smm.total_emails as dec(18,3))) pct_emails_clicked
from sandbox.jrg_merchant_roi roi
join sandbox.pai_merchants pm
	on pm.merchant_uuid = roi.merchant_uuid
join (
			select
				merchant_uuid
				, count(distinct deal_uuid) live_deals
				, sum(days_live) days_live
			from (
					select
						pd.merchant_uuid
						, ad.DEAL_UUID
        				, max(case when ad.load_date = current_date-1 then 1 else 0 end) is_live
        				, min(ad.load_date) first_live_date
        				, max(ad.load_date) last_live_date
        				, (max(ad.load_date) - min(ad.load_date)) days_live
        			from user_groupondw.fact_active_deals ad
					join sandbox.pai_deals pd
						on pd.deal_uuid = ad.deal_uuid
					group by 1,2
				) ad
			where is_live=1
			group by 1
		) ad
	on ad.merchant_uuid = roi.merchant_uuid
left join (
			select
				merchant_uuid
				, count(distinct merchant_uuid ||eventdate) visits
			from sandbox.pai_merchant_center_visits
			where eventdate between CURRENT_DATE-30 and CURRENT_DATE
			group by 1
		) mc
	on mc.merchant_uuid = roi.merchant_uuid
left join (
			select
				merchant_uuid
				, count(distinct emailname) total_emails
				, count(distinct case when coalesce(firstopendate,'')<>''  then emailname end) total_emails_opened
				, count(distinct case when coalesce(firstclickdate,'')<>'' then emailname end) total_emails_clicked
			from sfmc_emailengagement
			where delivered ='True' --and merchant_uuid='2fe07110-e9bf-43ba-8eb4-dcc5786cdf2a'
			group by 1
		) smm
	on smm.merchant_uuid = roi.merchant_uuid
where roi.customers_last180>5
and pm.l2='HBW'
and ad.live_deals=1
and mc.visits >= 2
and smm.total_emails_opened>0
and ad.days_live>=180


------------------GOALS TABLE 



create volatile table np_new_customers_twty as (
select
merchant_uuid
, count(distinct case when original_time=1 then user_uuid end) new_customers
from (
select
	user_uuid
	, merchant_uuid
	, order_id
	, redemption_date
	, row_number() over (partition by user_uuid , merchant_uuid order by redemption_date asc) original_time
from sandbox.jrg_user_trxns
where order_id not like'%-%'
) a
where redemption_date between current_date - 28 and current_date
group by 1
)with data unique primary index (merchant_uuid) on commit preserve rows;


create MULTISET volatile table np_temp_imps as (
select 
   pd.merchant_uuid, 
   sum(uniq_deal_views) uniq_deal_views,
   sum(deal_views) deal_views
from user_edwprod.agg_gbl_traffic_deal agtd 
left join sandbox.pai_deals as pd on agtd.deal_id = pd.deal_uuid
where agtd.country_id in (40, 235)
and
agtd.report_date between current_date - 28 and current_date
group by pd.merchant_uuid 
) with data on commit preserve rows
;

drop table sandbox.np_mc_insights_goal;
create multiset table sandbox.np_mc_insights_goal as (
select 
   a.merchant_uuid
   , b.new_customers new_customers_28
   , c.deal_views deal_views_28
   , b.new_customers*1.05 new_customers_28_strtr
   , b.new_customers*1.2 new_customers_28_inter
   , b.new_customers*1.5 new_customers_28_adv
   , c.deal_views*1.05 deal_views_28_strtr
   , c.deal_views*1.2 deal_views_28_inter
   , c.deal_views*1.5 deal_views_28_adv
   , null new_cust_goal_start_date
   , null deal_views_goal_start_date
   , null new_cust_frm_start_date
   , null deal_views_frm_start_date
from sandbox.pai_merchants as a 
left join np_new_customers_twty as b on a.merchant_uuid  = b.merchant_uuid
left join np_temp_imps as c on a.merchant_uuid = c.merchant_uuid
where a.country_code in ('US', 'CA')
) with data;

grant select on sandbox.np_mc_insights_goal to public;




select 
   *
from user_edwprod.agg_gbl_financials_deal sample 5;

