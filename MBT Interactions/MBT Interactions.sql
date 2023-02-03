--------------------------------------------------------------------TRIALS

drop table grp_gdoop_bizops_db.nvp_jouno_events_temp_exp;
create table grp_gdoop_bizops_db.nvp_jouno_events_temp_exp stored as orc as 
select 
    country,
    widgetname,
    devicetype,
    clientplatform,
    useros,
    mobiledevice,
    rawevent,
    eventdestination,
    useragent,
    count(distinct bcookie) distinct_users,
    count(distinct concat(bcookie, eventdate)) distinct_logins,
    substring(eventdate, 1,4) eventyear,
    substring(eventdate, 6,2) eventmonth
from
    grp_gdoop_pde.junoHourly
where eventdate >= '2021-03-01' and eventdate <= '2021-03-31'
    and eventdestination = 'dealImpression'
    and pageapp = 'mbt'
group by
    country,
    widgetname,
    devicetype,
    clientplatform,
    useros,
    mobiledevice,
    rawevent,
    eventdestination,
    useragent,
    substring(eventdate, 1,4), 
    substring(eventdate, 6,2)
;


drop table grp_gdoop_bizops_db.nvp_jouno_btimp_st1;
create table grp_gdoop_bizops_db.nvp_jouno_btimp_st1 (
    country string,
    devicetype string,
    rawevent string,
    eventdestination string,
    useragent string,
    useros string,
    bcookie string
) partitioned by (eventyear string, eventmonth string)
stored as orc
tblproperties ("orc.compress"="SNAPPY");


insert overwrite table grp_gdoop_bizops_db.nvp_jouno_btimp_st1 partition (eventyear, eventmonth)
select 
    country,
    devicetype,
    rawevent,
    eventdestination,
    useragent,
    useros,
    bcookie,
    substring(eventdate, 1,4) eventyear,
    substring(eventdate, 6,2) eventmonth
from
    grp_gdoop_pde.junoHourly
where eventdate >= '2021-03-01' and eventdate <= '2021-03-31'
    and eventdestination = 'dealImpression'
    and pageapp = 'mbt'
    and  widgetname in (
                   'bhw-main_menu-logo',
                   'bhw-schedule-mbt_schedule_view_calendar_widget_launch',
                   'bhw-main_menu-menu_toggle',
                   'bhw-main_menu-calendar' , 
                   'bhw-main_menu-redemption_history', 
                   'bhw-main_menu-support')
;


----------------------------------------------REAL CODE


drop table grp_gdoop_bizops_db.nvp_jouno_events_imp_bt;
create table grp_gdoop_bizops_db.nvp_jouno_events_imp_bt (
    country string,
    devicetype string,
    rawevent string,
    eventdestination string,
    useragent string, 
    useros string,
    distinct_users int,
    distinct_logins int
) partitioned by (eventyear string, eventmonth string) 
stored as orc
tblproperties ("orc.compress"="SNAPPY");



insert overwrite table grp_gdoop_bizops_db.nvp_jouno_events_imp_bt partition (eventyear, eventmonth)
select 
    country,
    devicetype,
    rawevent,
    eventdestination,
    useragent, 
    useros, 
    count(distinct bcookie) distinct_users,
    count(distinct concat(bcookie, eventdate)) distinct_logins,
    substring(eventdate, 1,4) eventyear,
    substring(eventdate, 6,2) eventmonth
from
    grp_gdoop_pde.junoHourly
where eventdate >= date_add(last_day(add_months(current_date, -2)),1) and eventdate <= last_day(add_months(current_date, -1))
    and eventdestination = 'dealImpression'
    and pageapp = 'mbt'
    and widgetname in (
                   'bhw-main_menu-logo',
                   'bhw-schedule-mbt_schedule_view_calendar_widget_launch',
                   'bhw-main_menu-menu_toggle',
                   'bhw-main_menu-calendar' , 
                   'bhw-main_menu-redemption_history', 
                   'bhw-main_menu-support')
group by
    country,
    devicetype,
    rawevent,
    eventdestination,
    useragent, 
    useros,
    substring(eventdate, 1,4), 
    substring(eventdate, 6,2)
;




drop table grp_gdoop_bizops_db.nvp_jouno_events_main_bt;
create table grp_gdoop_bizops_db.nvp_jouno_events_main_bt (
    country string,
    rawevent string,
    eventdestination string,
    widgetname string,
    widget_categories string, 
    distinct_users int, 
    total_login_clicks int
) partitioned by (eventyear string, eventmonth string) 
stored as orc
tblproperties ("orc.compress"="SNAPPY");



insert overwrite table grp_gdoop_bizops_db.nvp_jouno_events_main_bt partition (eventyear, eventmonth)
select 
    country,
    rawevent,
    eventdestination,
    case when widgetname in 
                   ('bhw-schedule-mbt_schedule_view_calendar_widget_launch', 
                    'bhw-schedule-mbt_schedule_view_list_check_in', 
                    'bhw-schedule-mbt_schedule_view_today',
                    'bhw-schedule-mbt_schedule_view_next_day',
                    'bhw-schedule-mbt_schedule_view_previous_day',
                    'bhw-main_menu-mbt_settings_cta')
         then concat(widgetname, ' (desktop) ') else widgetname end widgetname,
    case when widgetname in ('bhw-main_menu-calendar', 
                             'bhw-main_menu-redemption_history', 
                             'bhw-main_menu-support', 
                             'bhw-main_menu-Latest_activity', 
                             'bhw-main_menu-mbt_settings_cta', 
                             'bhw-main_menu-translate'
                             ) then 'Main Menu'
         when widgetname in ('bhw-calendar-mbt_calendar_view_button', 
                             'bhw-capacity-mbt_capacity_cta', 
                             'bhw-schedule-mbt_schedule_cta',
                             'bhw-calendar-mbt_add_calendar_item_cta'
                             ) then 'Main Menu - Bookings'
         when widgetname in ('bhw-schedule-mbt_schedule_view_calendar_widget_launch', 
                             'bhw-schedule-mbt_schedule_view_select_booking',
                             'bhw-schedule-mbt_schedule_view_list_check_in',
                             'bhw-schedule-mbt_booking_details_check_in',
                             'bhw-schedule-mbt_booking_details_cancel_booking', 
                             'bhw-schedule-mbt_booking_details_no_show',
                             'bhw-schedule-mbt_schedule_view_today',
                             'bhw-schedule-mbt_schedule_view_next_day',
                             'bhw-schedule-mbt_schedule_view_previous_day'
                             ) then 'Main Menu - Bookings - Schedule' end widget_categories,
    count(distinct bcookie) distinct_users,
    count(distinct concat(bcookie, eventdate)) total_login_clicks,
    substring(eventdate, 1,4) eventyear,
    substring(eventdate, 6,2) eventmonth
from
    grp_gdoop_pde.junoHourly
where eventdate >= date_add(last_day(add_months(current_date, -2)),1) and eventdate <= last_day(add_months(current_date, -1))
    and eventdestination = 'genericClick'
    and pageapp = 'mbt'
    and widgetname in (
                   'bhw-schedule-mbt_schedule_view_calendar_widget_launch', 
                             'bhw-schedule-mbt_schedule_view_select_booking',
                             'bhw-schedule-mbt_schedule_view_list_check_in',
                             'bhw-schedule-mbt_booking_details_check_in',
                             'bhw-schedule-mbt_booking_details_cancel_booking', 
                             'bhw-schedule-mbt_booking_details_no_show',
                             'bhw-schedule-mbt_schedule_view_today',
                             'bhw-schedule-mbt_schedule_view_next_day',
                             'bhw-schedule-mbt_schedule_view_previous_day', 
                             'bhw-calendar-mbt_calendar_view_button', 
                             'bhw-capacity-mbt_capacity_cta', 
                             'bhw-schedule-mbt_schedule_cta',
                             'bhw-calendar-mbt_add_calendar_item_cta', 
                             'bhw-main_menu-calendar', 
                             'bhw-main_menu-redemption_history', 
                             'bhw-main_menu-support', 
                             'bhw-main_menu-Latest_activity', 
                             'bhw-main_menu-mbt_settings_cta', 
                             'bhw-main_menu-translate'
                             )
group by
    country,
    rawevent,
    eventdestination,
    case when widgetname in 
                   ('bhw-schedule-mbt_schedule_view_calendar_widget_launch', 
                    'bhw-schedule-mbt_schedule_view_list_check_in', 
                    'bhw-schedule-mbt_schedule_view_today',
                    'bhw-schedule-mbt_schedule_view_next_day',
                    'bhw-schedule-mbt_schedule_view_previous_day',
                    'bhw-main_menu-mbt_settings_cta')
         then concat(widgetname, ' (desktop) ') else widgetname end,
    case when widgetname in ('bhw-main_menu-calendar', 
                             'bhw-main_menu-redemption_history', 
                             'bhw-main_menu-support', 
                             'bhw-main_menu-Latest_activity', 
                             'bhw-main_menu-mbt_settings_cta', 
                             'bhw-main_menu-translate'
                             ) then 'Main Menu'
         when widgetname in ('bhw-calendar-mbt_calendar_view_button', 
                             'bhw-capacity-mbt_capacity_cta', 
                             'bhw-schedule-mbt_schedule_cta',
                             'bhw-calendar-mbt_add_calendar_item_cta'
                             ) then 'Main Menu - Bookings'
         when widgetname in ('bhw-schedule-mbt_schedule_view_calendar_widget_launch', 
                             'bhw-schedule-mbt_schedule_view_select_booking',
                             'bhw-schedule-mbt_schedule_view_list_check_in',
                             'bhw-schedule-mbt_booking_details_check_in',
                             'bhw-schedule-mbt_booking_details_cancel_booking', 
                             'bhw-schedule-mbt_booking_details_no_show',
                             'bhw-schedule-mbt_schedule_view_today',
                             'bhw-schedule-mbt_schedule_view_next_day',
                             'bhw-schedule-mbt_schedule_view_previous_day'
                             ) then 'Main Menu - Bookings - Schedule' end,
    substring(eventdate, 1,4), 
    substring(eventdate, 6,2)
   ;

drop table grp_gdoop_bizops_db.nvp_jouno_all_bt_logs;
create table grp_gdoop_bizops_db.nvp_jouno_all_bt_logs stored as orc as 
select 
    'event_data' dash_category,
    country,
    null as devicetype_cat,
    rawevent,
    eventdestination,
    widgetname,
    widget_categories, 
    distinct_users unique_monthly_merchants, 
    total_login_clicks unique_daily_merchants, 
    eventyear, 
    eventmonth
   from grp_gdoop_bizops_db.nvp_jouno_events_main_bt
union 
select 
    'impression_data' dash_category,
    country,
    case when useragent like '%app-embedded-web-view%' then 'app embedded web view' 
         when devicetype = 'desktop' and (useros = 'iOS' or useros = 'Android') then 'Desktop - iOS/Android'
         when devicetype = 'desktop' then 'Desktop'
         when devicetype = 'mobile' and (useros = 'iOS' or useros = 'Android') then 'Mobile - iOS/Android'
         when devicetype = 'mobile' then 'Mobile - Unknown'
         when devicetype = 'tablet' then 'Tablet - iOS/Android'
         else 'Other' end devicetype_cat,
    rawevent,
    eventdestination,
    null as widgetname, 
    null as widget_categories,
    sum(distinct_users) unique_monthly_merchants,
    sum(distinct_logins) unique_daily_merchants,
    eventyear, 
    eventmonth
   from grp_gdoop_bizops_db.nvp_jouno_events_imp_bt
   group by 
    country,
    case when useragent like '%app-embedded-web-view%' then 'app embedded web view' 
         when devicetype = 'desktop' and (useros = 'iOS' or useros = 'Android') then 'Desktop - iOS/Android'
         when devicetype = 'desktop' then 'Desktop'
         when devicetype = 'mobile' and (useros = 'iOS' or useros = 'Android') then 'Mobile - iOS/Android'
         when devicetype = 'mobile' then 'Mobile - Unknown'
         when devicetype = 'tablet' then 'Tablet - iOS/Android'
         else 'Other' end, 
    rawevent,
    eventdestination,
    eventyear, 
    eventmonth
;


select * from grp_gdoop_bizops_db.nvp_jouno_all_bt_logs;


  
    
------------------------------------------

4e6a1655-4bad-4b29-aa16-554badab291d
9ff14507-69b4-49f9-b145-0769b4c9f9f5
042d1f9b-888a-44b3-ad1f-9b888ae4b3f9
4e6a1655-4bad-4b29-aa16-554badab291d

9ff14507-69b4-49f9-b145-0769b4c9f9f5
042d1f9b-888a-44b3-ad1f-9b888ae4b3f9
da6c84e6-c982-cc24-7c4c-aae05b1d80f7
6c5f3a3e-0f68-881f-bccb-ee93494c0773


------seems like my bcookie is 9b591b41-1111-4b62-991b-411111db626f


select 
    country,
    devicetype,
    useros,
    rawevent,
    eventdestination,
    parent,
    rawpagetype,
    widgetname,
    widgetcontenttype, 
    bcookie, 
    platform
from grp_gdoop_pde.junoHourly 
    where eventdate = '2021-05-04'
    and eventdestination = 'dealImpression'
    and pageapp = 'mbt' 
    and 
    limit 5;

   merchant-booking-tool
   
select 
    *
from grp_gdoop_pde.junoHourly 
    where eventdate = '2021-04-16'
    and eventdestination = 'dealImpression'
    and platform = 'web'
    and widgetname = 'bhw-main_menu-logo'
   ;
   

  
  
select 
    *
from grp_gdoop_pde.junoHourly 
    where eventdate = '2021-05-10'
    and eventdestination = 'genericClick'
    and platform = 'web'
    and sessionid = '9b591b41-1111-4b62-991b-411111db626f';


select 
    *
from grp_gdoop_pde.junoHourly 
    where eventdate = '2021-05-10'
    and eventdestination in ('genericClick', 'dealImpression')
    and bcookie = '236a0423-802f-c2de-7ed4-c4fb783d8c84';
   
select 
    rawpagetype, 
    count(1) 
from grp_gdoop_pde.junoHourly 
    where pageapp = 'mbt' 
    and eventdate = '2021-04-15' 
    group by 1 order by 1;


create table grp_gdoop_bizops_db.np_mbt_events (
    country string,
    devicetype string,
    useros string,
    rawevent string,
    eventdestination string,
    parent string,
    rawpagetype string,
    widgetname string,
    widgetcontenttype string,
    distinct_users int
) partitioned by (eventdate string) 
stored as orc
tblproperties ("orc.compress"="SNAPPY");


insert overwrite table grp_gdoop_bizops_db.np_mbt_events partition (eventdate)
select 
    country,
    devicetype,
    useros,
    rawevent,
    eventdestination,
    parent,
    rawpagetype,
    widgetname,
    widgetcontenttype,
    count(distinct bcookie) distinct_users,
    eventdate
from
    grp_gdoop_pde.junoHourly
where cast(eventdate as date) = cast('2021-04-15' as date)
    and pageapp = 'mbt'
group by
    country,
    devicetype,
    useros,
    eventdestination,
    parent,
    rawevent,
    rawpagetype,
    widgetname,
    widgetcontenttype,
    eventdate;


SELECT 
    *
    FROM 
    grp_gdoop_pde.junoHourly
    where pageapp = 'mbt'
    limit 5
 ;
    


select *
from cerebro_metadata.table_metadata
where database_name = 'grp_gdoop_pde' 
and table_name = 'junoHourly' order by 1;




   