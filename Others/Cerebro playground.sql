Describe formatted grp_gdoop_bizops_db.pai_merchant_center_visits;

select ad.deal_uuid, d.deal_name option_title, d.amount_session no_of_sessions, pa.multi_agenda_enabled is_multi_agenda, is_workshop is_class
from (select * from grp_gdoop_bizops_db.sh_bt_active_deals_log where load_date = '2021-07-06' and is_bookable = 1 and partner_inactive_flag = 0 and product_is_active_flag = 1 and sold_out= 'false') ad
join grp_gdoop_bizops_db.sh_bt_deals d on ad.deal_uuid = d.groupon_real_deal_uuid
join grp_gdoop_bizops_db.sh_bt_products p on d.partners_id = p.partners_id and d.products_id = p.id and d.country = p.country
join grp_gdoop_bizops_db.sh_bt_partners pa on d.partners_id = pa.id and d.country = pa.country
where d.country = 'US' and p.is_free_offer = 0;


select count(1) from grp_gdoop_bizops_db.nvp_jouno_events_main;

select * from grp_gdoop_bizops_db.nvp_jouno_events_imp_bt;


select eventdestination, widgetname, sum(distinct_users) total_users 
from grp_gdoop_bizops_db.np_mbt_events 
where widgetname = 'bhw-main_menu-calendar'
group by widgetname, eventdestination order by total_users desc;


select 
user_uuid
merchant_uuid
account_id
account_name
metal
metal_group
vertical
acct_owner
account_owner_group
page_url
page_path
page_app
page_type
mc_page
platform
sub_platform
user_device
device_category
user_device_type
app_version
utm_campaign
utm_medium
utm_source
referrer_domain
event
event_destination
widget_name
user_agent
uqeventkey
mc_cb_drft_ind
mc_cb_ind
live_flag
orig_merchant_uuid
orig_user_uuid
orig_merch_upd
orig_merch_ind
user_merch_ind
bcookie_merch_ind
no_merch_ind
event_time
from user_groupondw.dim_sf_person;


from dwh_base_sec_view.sf_opportunity_1 o1
    join dwh_base_sec_view.sf_opportunity_2 o2 on o1.id = o2.id
    join dwh_base_sec_view.sf_account sfa on o1.accountid = sfa.id
    left join user_groupondw.dim_sf_person sfp on sfa.ownerid = sfp.person_id
    left join sandbox.rev_mgmt_deal_attributes mat on o2.deal_uuid = mat.deal_id

'bhw-schedule-mbt_schedule_view_calendar_widget_launch'
'bhw-schedule-mbt_schedule_cta';


'bhw-main_menu_logo'
'bhw-main_menu-calendar'
'bhw-main_menu-redemption_history'
'bhw-main_menu-support'

select useros, devicetype,rawevent,parent, eventdestination, widgetname, sum(distinct_users) 
from grp_gdoop_bizops_db.np_mbt_events 
where widgetname = 'bhw-main_menu-logo'
group by eventdestination, widgetname, rawevent, devicetype, useros, parent
order by useros;


select useros, devicetype,rawpagetype, eventdestination, widgetname, sum(distinct_users) 
from grp_gdoop_bizops_db.np_mbt_events
where widgetname = 'bhw-main_menu-redemption_history'
group by useros, devicetype,rawpagetype, eventdestination, widgetname
;

select rawevent, eventdestination, rawpagetype, sum(distinct_users) distinct_users
from grp_gdoop_bizops_db.np_mbt_events 
group by eventdestination, rawpagetype, rawevent
order by eventdestination, rawpagetype, rawevent;




select 
   eventdestination,  
   count(1) cnz
from 
(select eventdestination, rawpagetype, sum(distinct_users) distinct_users
from grp_gdoop_bizops_db.np_mbt_events 
group by eventdestination, rawpagetype
order by eventdestination, rawpagetype)
as fi
group by eventdestination;

select distinct eventdestination from grp_gdoop_bizops_db.np_mbt_events;

bhw-calendar-mbt_calendar_view_calendar_widget_launch
bhw-calendar-mbt_calendar_view_calendar_widget_launch;



select column_name 
from cerebro_metadata.table_metadata
where database_name = 'grp_gdoop_pde' 
and table_name = 'junoHourly' order by column_name;