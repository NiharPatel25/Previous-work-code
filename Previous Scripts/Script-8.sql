/* SCRIPT FOR creating sh_bt_bookings_rebuild*/
/*
create table sh_bt_bookings_rebuild stored as orc as
    select
        pb.id as booking_id,
        pb.country as country_id,
        pb.checked_in,
        p.merchant_uuid,
        d.groupon_deal_id as cda_number,
        d.groupon_deal_uuid as deal_option_uuid,
        d.groupon_real_deal_uuid as deal_uuid,
        o.coupon_code as voucher_code,
        o.security_code,
        d.participants_per_coupon,
        case when o.coupon_code is not null then 1 else 0 end is_a_groupon_booking,
        case when pb.origin_screen like 'api%' then 'api'
             when pb.origin_screen like 'client%' then 'customer'
             when pb.origin_screen like 'partner%' then 'merchant'
             when pb.origin_screen like 'admin%' or origin_screen = 'cyclops' then 'admin'
             when trim(pb.origin_screen) in ('','NULL',null) then null
         end booked_by,
         case when (pb.state = 'cancelled' and pb.origin_screen like 'api%') then 'api'
              when (pb.state = 'cancelled' and pb.origin_screen like 'client%') then 'customer'
              when (pb.state = 'cancelled' and pb.origin_screen like 'partner%') then 'merchant'
              when (pb.state = 'cancelled' and (pb.origin_screen like 'admin%' or origin_screen = 'cyclops')) then 'admin'
              when (pb.state = 'cancelled' and trim(pb.origin_screen) in ('','NULL',null)) then null
          end cancelled_by,
        c.groupon_user_uuid as user_uuid,
        pb.start_time,
        pb.end_time,
        pb.created as created_at,
pb.state,
        case when pb.state = 'cancelled' then pb.last_update end deleted_at
    from sh_bt_products_bookings pb
    left join sh_bt_partners p on pb.partners_id = p.id and pb.country = p.country
    left join sh_bt_clients c on pb.clients_id = c.id and pb.country = c.country
    left join sh_bt_deals d on pb.deals_id = d.id and pb.country = d.country
    left join sh_bt_coupons o on pb.coupons_id = o.id and pb.country = o.country
    where pb.state in ('confirmed','cancelled')*/

/*
select
    id,
    language,
    lead_time_sms_reminder,
    lead_time_email_reminder,
    groupon_user_response,
    groupon_user_id,
    groupon_user_uuid,
    ical_key,
    is_deleted,
    date_of_deletion
from clients

select
    booking_delay,
    cancellation_period,
    checkin_hour,
    checkout_hour,
    comment_heading,
    comment_required,
    duration,
    frequency,
    id,
    is_active,
    is_bookable,
    is_bookable_non_groupon,
    is_free_offer,
    is_mpp_offer,
    is_workshop,
    last_update,
    name,
    participants,
    partners_id,
    post_reservation_blocked_minutes,
    product_offer_views,
    show_warning_message,
    warning_message
from products

select
    id,
    partners_id,
    products_id,
    groupon_deal_id,
    groupon_deal_uuid,
    groupon_permalink,
    groupon_real_deal_uuid,
    groupon_location_uuid,
    validity_start,
    validity_end,
    duration,
    amount_sold,
    amount_session,
    name,
    city,
    redemption_locations,
    capping_day,
    capping_week,
    capping_month,
    cancellation_period,
    calendar_view,
    capping_monday,
    capping_tuesday,
    capping_wednesday,
    capping_thursday,
    capping_friday,
    capping_saturday,
    capping_sunday,
    booking_delay,
    bookings_max_per_coupon,
    booking_till,
    capacity,
    comment_required,
    comment_heading,
    participants_per_coupon,
    applied_sessions,
    applied_days_per_session,
    is_g2_deal,
    is_btos_deal,
    show_warning_message,
    warning_message,
    accept_redeemed_vouchers,
    arrival_days,
    validity_dates,
    multicoupon_after_session_reminder_enabled,
    multicoupon_ending_validity_reminder_enabled,
    multicoupon_ending_validity_reminder_days,
    multicoupon_validity_enabled,
    multicoupon_validity_days,
    multicoupon_minimum_interval_enabled,
    multicoupon_minimum_interval_days,
    customers_agenda_choice_enabled,
    ts_btos_update,
    image_small_url,
    ts_created,
    ts_modfied,
    created_by_ssu,
    ssu_opportunity_id,
    inventory_product_uuid,
    inventory_service_id,
    gapp_enabled,
    gapp_registered,
    gapp_updated,
    gapp_deleted,
    gapp_status
from deals

select
    clients_id,
    count_reminder_is_sent,
    coupons_id,
    created,
    deals_id,
    duration,
    checked_in,
    email_reminder_hours,
    email_reminder_is_sent,
    end_time,
    id,
    is_async,
    is_btos_booking,
    last_update,
    origin_screen,
    parent_products_bookings_id,
    participants_non_groupon,
    partner_comment,
    partners_id,
    products_id,
    reminder_email_failure_count,
    reminder_email_is_sent,
    reminder_sms_failure_count,
    reminder_sms_is_sent,
    reminder_time_email,
    reminder_time_sms,
    reminders_id,
    reminders_time_calculated,
    start_day,
    start_month,
    start_time,
    start_week,
    state
from products_bookings

select
    id,
    deals_id,
    clients_id,
    is_used,
    amount_used,
    ref_code,
    security_code,
    coupon_code,
    validity_end,
    redemption_date,
    redemption_status,
    accept_redeemed,
    validity_reminder_is_sent,
    gift_date,
    uuid
from coupons

select
    id,
    merchant_number,
    merchant_uuid,
    created,
    name,
    homepage,
    description,
    logo,
    booking_solution,
    language,
    last_login,
    last_update,
    holidays,
    inactive,
    inactive_message,
new_bt_opt_in,
    new_merchant,
    new_bt_opt_in_date,
    created_by,
    lastedit_by,
    product_booking_url,
    summary_notification_mails,
    notification_frequency,
    notification_instant,
    default_session_duration,
    sms_summary_notification,
    sms_notification_frequency,
    booking_language,
    allow_availability_overlapping,
    allow_mass_modification,
    allow_non_groupon_reservations,
    voucher_redemption_enabled,
    multi_agenda_enabled,
    allow_manage_offers,
    allow_manage_product_offers,
    messaging_system_enabled,
    messaging_system_booking_only,
    groupon_merchant_id,
    allow_booking_deletion,
    allow_manage_availabilities,
    allow_manage_groupon_bookings,
    allow_booking_mass_deletion,
    allow_yield_management,
    allow_widget_management,
    allow_view_offers,
    hide_name_in_preview,
    agenda_min_time,
    agenda_max_time,
    redeem_before_checking_date,
    auto_clone_availibilities,
    last_availibility_cloning,
    last_availibility_update
from partners

*/


select user_uuid, deal_uuid,  count(*) from grp_gdoop_bizops_db.sh_bt_bookings_rebuild where year(created_at) = '2019' and user_uuid is not null  and user_uuid <> '' and length(user_uuid) > 0 group by user_uuid, deal_uuid;


select * from grp_gdoop_bizops_db.sh_bt_active_deals_log limit 5;


select * from grp_gdoop_bizops_db.sh_bt_bookings_rebuild where user_uuid = 'ec66f465-2b8c-4727-802f-bb16e871e90a' and deal_uuid = '00136f0f-884b-4f86-8ca3-df75ec8a5793';

select * from grp_gdoop_bizops_db.sh_bt_bookings_rebuild where user_uuid = '00003811-a002-494a-8c1e-f75f599fd3a3' and deal_uuid = 'd5eab2a1-8952-4c9d-8038-efbd336c93a3';

	
select * from dwh_base.vouchers limit 5;

select * from grp_gdoop_bizops_db.sh_bt_products_bookings limit 5;

select * from grp_gdoop_bizops_db.sh_bt_products limit 5;
select * from grp_gdoop_bizops_db.sh_bt_clients limit 5;
select * from grp_gdoop_bizops_db.sh_bt_deals limit 5;

/*dwh_base.vouchers v
   join grp_gdoop_bizops_db.sh_bt_bookings_rebuild b
     on b.voucher_code = v.voucher_code and b.security_code = v.security_code*/
     

select * from dwh_base.vouchers limit 5;

select v.voucher_id, v.billing_id, b.user_uuid, b.merchant_uuid
from dwh_base.vouchers v
join grp_gdoop_bizops_db.sh_bt_bookings_rebuild b
     on b.voucher_code = v.voucher_code and b.security_code = v.security_code
     where b.voucher_code = 'VS-39XJ-4WWG-GFL3-1GYG' and b.security_code = 'D759758957';
    
select * from user_edwprod.fact_gbl_transactions limit 5;
select * from dwh_base.vouchers limit 6;