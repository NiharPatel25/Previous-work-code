--- gets the refund rate overall

/*  BT TEST
create multiset volatile table np_temp_bt_deal as 
(sel distinct
	ad.load_date
	, ad.deal_uuid as deal_uuid
	, pd.merchant_uuid
	, pd.country_code
	, case
		when bkt.deal_uuid is not null then 1
		when rb.deal_uuid is not null then 1
		when tpis.merch_uuid
			in ('72b54fd4-05dd-11e3-af2d-00259096a2fa', '9b9d5de3-2e20-4b59-9d61-e1ac85ba1948') then 0
		-- exclude Local Flavor and BJs Wholesale Club
		when tpis.product_uuid is not null then 1
		else 0 end as is_bookable
	, case
		when bkt.deal_uuid is not null then 'Booking Tool'
		when rb.deal_uuid is not null then 'Rails Bookable'
		when tpis.merch_uuid
			in ('72b54fd4-05dd-11e3-af2d-00259096a2fa', '9b9d5de3-2e20-4b59-9d61-e1ac85ba1948')
			then 'TPIS Non-Bookable'
		when tpis.product_uuid is not null then 'TPIS Bookable'
		-- exclude Local Flavor and BJs Wholesale Club
		else 'Non-Bookable' end as booking_platform
	, case
		when tpis.product_uuid is not null then 'TPIS'
		when clo.product_uuid is not null then 'CLO'
		else 'Voucher' end as inventory_type
from user_groupondw.active_deals ad
join sandbox.pai_deals pd on ad.deal_uuid = pd.deal_uuid
left join sandbox.sh_bt_active_deals_log bkt on bkt.load_date = ad.load_date
	and bkt.deal_uuid = ad.deal_uuid and bkt.is_bookable = 1 and bkt.partner_inactive_flag = 0
	and bkt.product_is_active_flag = 1
left join sandbox.avb_tpis_bookable_live rb on rb.deal_uuid = ad.deal_uuid
	and rb.report_date = ad.load_date and rb.is_active_deal = 1
left join sandbox.bzops_booking_deals tpis on tpis.product_uuid = ad.deal_uuid
	and tpis.inv_service_id = 'tpis' and tpis.exclude_flag = 0
left join sandbox.bzops_booking_deals clo on clo.product_uuid = ad.deal_uuid
	and clo.inv_service_id = 'clo' and clo.exclude_flag = 0
where
	ad.load_date = trunc(current_date-1,'iw')-1
	and ad.sold_out = 'FALSE'
) with data on commit preserve rows;


select * from jrg_booking_tool where is_bookable = 1;
select * from sandbox.pai_deals where merchant_uuid = 'ec06e166-aaf9-46c5-ae4d-889983cb71e7' and is_live = 1;
select * from np_temp_bt_deal where merchant_uuid = 'ec06e166-aaf9-46c5-ae4d-889983cb71e7';

SELECT * FROM user_edwprod.dim_offer_ext;
*/

select * from sandbox.pai_deals where merchant_uuid = '11774167-882e-4e52-915f-80dc4622565d';
select * from sandbox.jrg_refund_rec where merchant_uuid = '11774167-882e-4e52-915f-80dc4622565d';

select * from sandbox.sh_bt_active_deals_log sbadl where deal_uuid = '7d531ca2-e6fe-41c0-8b28-f4873ceef230' order by load_date desc;
select * from sandbox.merchant_adviser_cohort_30_view;
drop table jrg_refunders;


create volatile table jrg_cust as (
select
	a.merchant_uuid
	, sum(case when action = 'refund' then -1 * REFUND_AMOUNT_USD end) as refund_usd
	, sum(case when action = 'capture' then GROSS_BOOKINGS_USD end) as GB_usd
	, sum(case when action = 'capture' then -1 * DISCOUNT_AMOUNT_USD end) as DISCOUNT
	, sum(case when action = 'capture' then CAPTURE_NOB_USD end) as NOB
	, count(distinct case when action = 'refund' then a.order_id end) as refunded_orders
	, count(distinct(a.order_id)) as total_orders
	/*, sum(case when na.Refund_Reason in ('Customer was negatively affected by merchant behavior or service (Promise - Redeemed only)','merchant_temporarily_not_honoring','Merchant is not contactable'
		,'Self Service: Merchant Issue','Order/Booking/reservation placed but no confirmation from the merchant','Merchant refuses to honor Groupon','Merchant location closed/moved (not bankrupt)'
		,'Merchant is refusing to schedule past expiration date for promotional value','Decision to not redeem after attempt - merchant was rude or unprofessional','Merchant has closed (temporarily or permanently)') then 1 else 0 end) as merchant_issue*/
	, count(distinct case when action = 'refund' then a.unified_user_id end) as customer_refunded
	, sum(case when action = 'refund' then transaction_qty  end) as Unit_Refunded
	, sum(case when action = 'capture' then transaction_qty end) as Units_Sold
from  user_dw.v_fact_gbl_transactions as a
left join (
			select distinct
				order_id
				, MERCH_PRODUCT_UUID
				, Refund_Reason
				, Refund_Category
			from sandbox.na_cs_orders_consolidated as a
			where action  = 'refund'
			qualify (ROW_NUMBER() OVER (PARTITION BY a.order_id ORDER BY transaction_date_ts )) = 1
			) as na
	on na.order_id = a.order_id
where a.action in ('refund','capture')
and a.transaction_date between CURRENT_DATE-30 and CURRENT_DATE
group by 1
)with data primary index (merchant_uuid) on commit preserve rows;-- find top refund reason


create volatile table jrg_refund_reason as (
select
	merchant_uuid
	, refund_reason
	, sum(Units_Refunded) units
	, row_number() over (partition by merchant_uuid order by units desc) reason_rank
from sandbox.merchant_adviser_cohort_30_view
where coalesce("Refund $",0)<>0
and LOB='L1 - Local'
and controllable ='Y'
group by 1,2
qualify reason_rank=1
)with data primary index (merchant_uuid) on commit preserve rows;--- get the percentage of units are from the top refund reason


create volatile table jrg_refunders as (
select
	merchant_uuid
	, cast(top_reason_units as dec(18,3))/ nullifzero(cast(other_units  as dec(18,3))) controllable_units
from (
		select
			a.merchant_uuid
			, sum(case when a.refund_reason = b.refund_reason then a.units end) top_reason_units
			, sum(a.units) other_units
		from (
				select
					merchant_uuid
					, refund_reason
					, sum(Units_Refunded) units
				from sandbox.merchant_adviser_cohort_30_view
				where coalesce("Refund $",0)<>0
				and LOB='L1 - Local'
				and controllable ='Y'
				group by 1,2
			) a
		left join jrg_refund_reason b
			on b.merchant_uuid = a.merchant_uuid
		group by 1
	) ab
)with data primary index (merchant_uuid) on commit preserve rows;--- get merchant refund rate


create volatile table jrg_refunders2 as (
select
	merchant_uuid
	, cast(top_reason_units as dec(18,3))/ nullifzero(cast(other_units  as dec(18,3))) per_ref_top
from (
		select
			a.merchant_uuid
			, sum(case when a.refund_reason = b.refund_reason then a.units end) top_reason_units
			, sum(a.units) other_units
		from (
				select
					merchant_uuid
					, refund_reason
					, sum(Units_Refunded) units
				from sandbox.merchant_adviser_cohort_30_view
				where coalesce("Refund $",0)<>0
				and LOB='L1 - Local'
				group by 1,2
			) a
		left join jrg_refund_reason b
			on b.merchant_uuid = a.merchant_uuid
		group by 1
	) ab
)with data primary index (merchant_uuid) on commit preserve rows;


create volatile table jrg_refunds as (
select
	a.merchant_uuid
	, sum(Units_Refunded) unit_refund
	, sum(Units_Sold) units
	, coalesce(cast(unit_refund as dec(18,3))/cast(units as dec(18,3)),0) refund_rate
from sandbox.merchant_adviser_cohort_30_view a
group by 1
) with data unique primary index (merchant_uuid) on commit preserve rows;--- get cohort refund rate


create volatile table  jrg_cohort_refunds as (
select
	b.l2
	, b.l3
	, b.l4
	, lower(b.division) division
	, b.top_pds as pds
	, count(distinct b.merchant_uuid) n_merchants
	, sum(a.Unit_Refunded) unit_refund
	, sum(a.Units_Sold) units
	, coalesce(cast(unit_refund as dec(18,3))/nullifzero(cast(units as dec(18,3))),0) refund_rate
from jrg_cust a
join sandbox.pai_merchants b
	on a.merchant_uuid = b.merchant_uuid
where b.country_code='US'
and b.l1='Local'
group by 1,2,3,4,5
)with data unique primary index (l2,l3,l4,division,pds) on commit preserve rows;-- get login infor for MC

create volatile table jrg_log_ins as (
select
	merchant_uuid
	, count(distinct merchant_uuid || eventdate) log_ins
from sandbox.pai_merchant_center_visits
where platform<>'Mobile'
and eventdate BETWEEN CURRENT_DATE - 31 and CURRENT_DATE-1
group by 1
)with data unique primary index (merchant_uuid) on commit preserve rows;-- see if merchant is on booking tool

create volatile table jrg_booking_tool as (
select
	merchant_uuid
	, max(is_bookable) is_bookable
	, max(case when booking_platform='TPIS Bookable' then 1 else 0 end) is_3pip
	, case when max(booking_platform2) = 5 then 'Booking Tool' 
	       when max(booking_platform2) = 4 then 'Rails Bookable' 
	       when max(booking_platform2) = 3 then 'TPIS Bookable'
	       when max(booking_platform2) = 0 then 'Non-Bookable' end booking_platform
from
(
sel distinct
	ad.load_date
	, ad.deal_uuid as deal_uuid
	, pd.merchant_uuid
	, pd.country_code
	, case
		when bkt.deal_uuid is not null then 1
		when rb.deal_uuid is not null then 1
		when tpis.merch_uuid
			in ('72b54fd4-05dd-11e3-af2d-00259096a2fa', '9b9d5de3-2e20-4b59-9d61-e1ac85ba1948') then 0
		-- exclude Local Flavor and BJs Wholesale Club
		when tpis.product_uuid is not null then 1
		else 0 end as is_bookable
	, case
		when bkt.deal_uuid is not null then 'Booking Tool'
		when rb.deal_uuid is not null then 'Rails Bookable'
		when tpis.merch_uuid
			in ('72b54fd4-05dd-11e3-af2d-00259096a2fa', '9b9d5de3-2e20-4b59-9d61-e1ac85ba1948')
			then 'TPIS Non-Bookable'
		when tpis.product_uuid is not null then 'TPIS Bookable'
		-- exclude Local Flavor and BJs Wholesale Club
		else 'Non-Bookable' end as booking_platform
    ,case
		when bkt.deal_uuid is not null then 5
		when rb.deal_uuid is not null then 4
		when tpis.merch_uuid
			in ('72b54fd4-05dd-11e3-af2d-00259096a2fa', '9b9d5de3-2e20-4b59-9d61-e1ac85ba1948')
			then 0
		when tpis.product_uuid is not null then 3
		-- exclude Local Flavor and BJs Wholesale Club
		else 0 end as booking_platform2
	, case
		when tpis.product_uuid is not null then 'TPIS'
		when clo.product_uuid is not null then 'CLO'
      else 'Voucher' end as inventory_type
from user_groupondw.active_deals ad
join sandbox.pai_deals pd on ad.deal_uuid = pd.deal_uuid
left join sandbox.sh_bt_active_deals_log bkt on bkt.load_date = ad.load_date
	and bkt.deal_uuid = ad.deal_uuid and bkt.is_bookable = 1 and bkt.partner_inactive_flag = 0
	and bkt.product_is_active_flag = 1
left join sandbox.avb_tpis_bookable_live rb on rb.deal_uuid = ad.deal_uuid
	and rb.report_date = ad.load_date and rb.is_active_deal = 1
left join sandbox.bzops_booking_deals tpis on tpis.product_uuid = ad.deal_uuid
	and tpis.inv_service_id = 'tpis' and tpis.exclude_flag = 0
left join sandbox.bzops_booking_deals clo on clo.product_uuid = ad.deal_uuid
	and clo.inv_service_id = 'clo' and clo.exclude_flag = 0
where
	ad.load_date = trunc(current_date-1,'iw')-1
	and ad.sold_out = 'FALSE'
	) d
group by 1
having max(is_bookable) = 1
)with data unique primary index (merchant_uuid) on commit preserve rows;---get voucher cap




create volatile table jrg_cap_v as (
select
	merchant_uuid
	, sum(ttl_option_cap) cap
from (
select
	product_uuid as deal_uuid
	, pd.merchant_uuid
    , count(distinct inv_product_uuid) num_options
    , avg((doe.groupon_value - doe.contract_sell_price) / nullifzero(doe.groupon_value)) adp
    , sum(cast(offer_max as bigint)) ttl_option_cap
    , avg(doe.contract_sell_price) asp
    , min(doe.contract_sell_price) min_price
    , max(doe.contract_sell_price) max_price
from user_edwprod.dim_offer_ext doe
join sandbox.pai_deals pd
	on pd.deal_uuid = doe.product_uuid
where pd.l1='Local'
and pd.country_code='US'
and pd.is_live=1
group by 1,2
) a
group by 1
) with data primary index(merchant_uuid) on commit preserve rows;

create volatile table jrg_vouchers as (
select
	v.order_id
	, v.voucher_barcode_id
	, v.code
	, cast(redeemed_at as date) redemption_date
from (
          select a.*
          from user_groupondw.acctg_red_voucher_base a
          where cast(created_at as date) between '2020-12-31' and current_date
            and external_yn = 0
          qualify row_number() over (partition by id order by created_at desc) = 1
       ) v
join (
          select
          	deal_uuid
            , parent_order_uuid
            , order_id
            , min(case when action = 'refund' then transaction_date end) refund_date
         from user_edwprod.fact_gbl_transactions
         where deal_uuid is not null
         and order_date between '2020-12-31' and current_date
          group by 1,2,3
		)fgt
	on cast(v.order_id as varchar(64)) = fgt.order_id
join sandbox.pai_deals pd
	on pd.deal_uuid = fgt.deal_uuid
join sandbox.pai_merchants pm
	on pd.merchant_uuid = pm.merchant_uuid
where pd.l1 = 'Local'
and pd.country_code in ('US','CA')
and refund_date is null
and redeemed_at is not null
)with data unique primary index(order_id, voucher_barcode_id) on commit preserve rows;


create volatile table jrg_units as (
select
	pd.merchant_uuid
	, vr.redemption_date
	, count(distinct fgt.order_id) orders
	, sum(gross_bookings_loc) as gb
	, sum(gross_revenue_loc) as gr
	, sum(transaction_qty) as units
	, avg(unit_sell_price) sell_price
	, count(distinct vr.voucher_barcode_id || vr.code) as vouchers
from jrg_vouchers vr
join user_edwprod.fact_gbl_ogp_transactions fgt
	on cast(vr.order_id as varchar(20)) = fgt.order_id
join sandbox.pai_options po
	on fgt.inv_product_uuid = po.inv_product_uuid
join sandbox.pai_deals pd
	on po.deal_uuid = pd.deal_uuid
where fgt.action = 'authorize'
group by 1,2
)with data unique primary index(merchant_uuid, redemption_date) on commit preserve rows;

create volatile table jrg_redeem as (
select
	merchant_uuid
	, avg(vouchers) vouchers
	, avg(sell_price) price
from (
		select
			trunc(redemption_date, 'RM') mth
			, merchant_uuid
			, sum(units) units
			, sum(vouchers) vouchers
			, avg(sell_price) sell_price
		from jrg_units
		where redemption_date between  current_date -90 and  current_date
		group by 1,2
) a
group by 1
)with data unique primary index(merchant_uuid) on commit preserve rows;



drop table sandbox.jrg_refund_rec;
create table sandbox.jrg_refund_rec as (
select distinct
	a.merchant_uuid
	, a.Account_ID_18
	, a.account_manager
	, mm.acct_owner_name
	, a.Company_Legal_Name
	, a.min_order_date start_date
	, a.max_order_date end_date
	, c.refund_reason primary_controllable_reason
	, d.controllable_units controllable_pct_refunded
	, f.refund_rate merchant_refund_rate
	, g.refund_rate cohort_refund_rate
	, ((merchant_refund_rate-cohort_refund_rate)/merchant_refund_rate) delta_refund
	, CURRENT_DATE last_updated
	, lg.log_ins
	, v.cap
	, rd.vouchers
	, (case when bk.merchant_uuid is null then 0 else 1 end) booking_tool_flag
	, booking_platform
	, rd.price
	, c.units controllable_units_refunded
	, CAST(cst.customer_refunded AS float) * CAST(d2.per_ref_top AS float) customer_refunded----multiplying *controllable_pct_refunded TO GET custoemer share that refunded
from sandbox.merchant_adviser_cohort_30_view a
join sandbox.pai_merchants mm
	on mm.merchant_uuid = a.merchant_uuid
join jrg_refund_reason c
	on c.merchant_uuid = a.merchant_uuid and a.refund_reason = c.refund_reason
join jrg_refunders d --- percent controllable refund units
	on d.merchant_uuid = a.merchant_uuid
join jrg_refunders2 d2 on d2.merchant_uuid = a.merchant_uuid 
left join jrg_refunds f
	on f.merchant_uuid = a.merchant_uuid
left join jrg_log_ins lg
	on lg.merchant_uuid = a.merchant_uuid
left join jrg_redeem rd
	on rd.merchant_uuid = a.merchant_uuid
left join jrg_cap_v v
	on v.merchant_uuid = a.merchant_uuid
left join jrg_booking_tool bk
	on bk.merchant_uuid = a.merchant_uuid
left join jrg_cust cst
	on cst.merchant_uuid = a.merchant_uuid
left join jrg_cohort_refunds g
	on g.l2=mm.l2
	and g.l3=mm.l3
	and g.l4=mm.l4
	and g.pds=mm.top_pds
	and g.division = lower(mm.division)
where mm.l1='Local'
and mm.country_code='US'
and coalesce(Units_Refunded,0)>0
and Target_category='Merchant Adviser Notification'
and lg.log_ins>=4
)with data unique primary index(merchant_uuid);
GRANT ALL ON sandbox.jrg_refund_rec TO abautista, nihpatel, smalandkar, jkerin, ub_bizops WITH GRANT OPTION;
GRANT select ON sandbox.jrg_refund_rec TO public;


select * from sandbox.jrg_refund_rec;




select * from jrg_refunders where merchant_uuid  = '565a0c7a-aec8-11e1-8e54-00259060b612';



create volatile table jrg_booking_tool as (
select
	merchant_uuid
	, max(is_bookable) is_bookable
	, max(case when booking_platform='TPIS Bookable' then 1 else 0 end) is_3pip
	, max(booking_platform) booking_platform
from
(
sel distinct
	ad.load_date
	, ad.deal_uuid as deal_uuid
	, pd.merchant_uuid
	, pd.country_code
	, case
		when bkt.deal_uuid is not null then 1
		when rb.deal_uuid is not null then 1
		when tpis.merch_uuid
			in ('72b54fd4-05dd-11e3-af2d-00259096a2fa', '9b9d5de3-2e20-4b59-9d61-e1ac85ba1948') then 0
		-- exclude Local Flavor and BJs Wholesale Club
		when tpis.product_uuid is not null then 1
		else 0 end as is_bookable
	, case
		when bkt.deal_uuid is not null then 'Booking Tool'
		when rb.deal_uuid is not null then 'Rails Bookable'
		when tpis.merch_uuid
			in ('72b54fd4-05dd-11e3-af2d-00259096a2fa', '9b9d5de3-2e20-4b59-9d61-e1ac85ba1948')
			then 'TPIS Non-Bookable'
		when tpis.product_uuid is not null then 'TPIS Bookable'
		-- exclude Local Flavor and BJs Wholesale Club
		else 'Non-Bookable' end as booking_platform
	, case
		when tpis.product_uuid is not null then 'TPIS'
		when clo.product_uuid is not null then 'CLO'
		else 'Voucher' end as inventory_type
from user_groupondw.active_deals ad
join sandbox.pai_deals pd on ad.deal_uuid = pd.deal_uuid
left join sandbox.sh_bt_active_deals_log bkt on bkt.load_date = ad.load_date
	and bkt.deal_uuid = ad.deal_uuid and bkt.is_bookable = 1 and bkt.partner_inactive_flag = 0
	and bkt.product_is_active_flag = 1
left join sandbox.avb_tpis_bookable_live rb on rb.deal_uuid = ad.deal_uuid
	and rb.report_date = ad.load_date and rb.is_active_deal = 1
left join sandbox.bzops_booking_deals tpis on tpis.product_uuid = ad.deal_uuid
	and tpis.inv_service_id = 'tpis' and tpis.exclude_flag = 0
left join sandbox.bzops_booking_deals clo on clo.product_uuid = ad.deal_uuid
	and clo.inv_service_id = 'clo' and clo.exclude_flag = 0
where
	ad.load_date = trunc(current_date-1,'iw')-1
	and ad.sold_out = 'FALSE'
	) d
group by 1
having max(is_bookable) = 1
)with data unique primary index (merchant_uuid) on commit preserve rows;---get voucher cap



/*create table sandbox.jrg_refund_rec as (
select distinct
	a.merchant_uuid
	, a.Account_ID_18
	, a.account_manager
	, mm.acct_owner_name
	, a.Company_Legal_Name
	, a.min_order_date start_date
	, a.max_order_date end_date
	, c.refund_reason primary_controllable_reason
	, d.controllable_units controllable_pct_refunded
	, f.refund_rate merchant_refund_rate
	, g.refund_rate cohort_refund_rate
	, ((merchant_refund_rate-cohort_refund_rate)/merchant_refund_rate) delta_refund
	, CURRENT_DATE last_updated
	, lg.log_ins
	, v.cap
	, rd.vouchers
	, (case when bk.merchant_uuid is null then 0 else 1 end) booking_tool_flag
	, rd.price
	, c.units controllable_units_refunded
	, cst.customer_refunded
from sandbox.merchant_adviser_cohort_30_view a
join sandbox.pai_merchants mm
	on mm.merchant_uuid = a.merchant_uuid
join jrg_refund_reason c
	on c.merchant_uuid = a.merchant_uuid
join jrg_refunders d
	on d.merchant_uuid = a.merchant_uuid
left join jrg_refunds f
	on f.merchant_uuid = a.merchant_uuid
left join jrg_log_ins lg
	on lg.merchant_uuid = a.merchant_uuid
left join jrg_redeem rd
	on rd.merchant_uuid = a.merchant_uuid
left join jrg_cap_v v
	on v.merchant_uuid = a.merchant_uuid
left join jrg_booking_tool bk
	on bk.merchant_uuid = a.merchant_uuid
left join jrg_cust cst
	on cst.merchant_uuid = a.merchant_uuid
left join jrg_cohort_refunds g
	on g.l2=mm.l2
	and g.l3=mm.l3
	and g.l4=mm.l4
	and g.pds=mm.top_pds
	and g.division = lower(mm.division)
where mm.l1='Local'
and mm.country_code='US'
and mm.l2='HBW'
and coalesce(Units_Refunded,0)>0
and Target_category='Merchant Adviser Notification'
and lg.log_ins>=4
and c.refund_reason='Merchant fully booked/ No Availability'
and bk.is_bookable=0
and bk.is_3pip=0
)with data unique primary index(merchant_uuid);
grant select on sandbox.jrg_refund_rec to public;*/
