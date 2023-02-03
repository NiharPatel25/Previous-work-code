create volatile multiset table vouchers as (
    select 
        voucher_code, 
        security_code, 
        redeem_origin_id, 
        usage_state_id, 
        cast(usage_date as date) as usage_date, 
        billing_id as parent_order_uuid
    from dwh_base_sec_view.vouchers
    where dwh_active = 1
    and cast(created_at as date) >= '2018-01-01'
    and voucher_code is not null
) with data on commit preserve rows;

select * from dwh_base_sec_view.vouchers;

create volatile multiset table coupons as (
	select 
		code as voucher_code, 
		cast(merchant_redemption_code as varchar(64)) security_code,
		purchaser_consumer_id, 
		cast(order_id as varchar(64)) order_id,
		merchant_redeemed_at, 
		customer_redeemed_at, 
		customer_redeemed,
		merchant_redeemed, 
		id
	from user_gp.camp_membership_coupons
) with data on commit preserve rows;



create volatile multiset table us_reds2 as (
	select 
		membership_coupon_id, status, id
	from user_gp.redemptions
	qualify row_number() over(partition by membership_coupon_id order by id desc) = 1
) with data primary index (membership_coupon_id) on commit preserve rows;






