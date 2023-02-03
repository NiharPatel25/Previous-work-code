select
        event_date,
        unique_visitors as bcookie,
        max(cookie_first_country_code) country_code
    from user_groupondw.gbl_traffic_superfunnel
    where cast(event_date as date) = cast('2020-09-14' as date)
   group by event_date, unique_visitors
   having country_code = 'US';
   
  
select * from user_groupondw.bld_widget_contents 
where 
cast(dt as date) = cast('2020-08-15' as date)
and page_country = 'US';

select * from user_groupondw.bld_widget_contents 
where 
lower(widget_name) in ('cartmessage','usermessage')
and lower(wc_data_json) = 'inventory_product_declined'
and lower(platform) in ('web','desktop','touch') 
and cast(dt as date) = cast('2020-09-14' as date);


select * from user_groupondw.bld_widget_contents 
where 
lower(widget_name) in ('cartmessage','usermessage')
and lower(widget_content_type) = 'inventory_product_declined'
and lower(platform) in ('web','desktop','touch') 
and cast(dt as date) = cast('2020-09-14' as date);


select * from user_groupondw.bld_widget_contents 
where 
lower(widget_name) in ('cartmessage','usermessage')
and lower(wc_data_json) = 'inventory_product_declined'
and lower(platform) in ('web','desktop','touch') 
and cast(dt as date) between cast('2020-09-13' as date) and cast('2020-09-16' as date)
and page_country = 'US';

select * 
from 
prod_groupondw.bld_widget_contents
where
cast(dt as date) = cast('2020-09-15' as date)
and page_country = 'US';


desc formatted user_groupondw.bld_widget_contents;

select `bld_widget_contents`.`event_type`, 
`bld_widget_contents`.`event_date`, 
`bld_widget_contents`.`event_hour`, `bld_widget_contents`.`event_name`, `bld_widget_contents`.`event_time`, 
`bld_widget_contents`.`session_id`, `bld_widget_contents`.`event_id`, `bld_widget_contents`.`page_id`, `bld_widget_contents`.`parent_page_id`,
`bld_widget_contents`.`parent_event_id`, `bld_widget_contents`.`user_browser_id`, `bld_widget_contents`.`client_ip_address`, `bld_widget_contents`.`page_hostname`,
`bld_widget_contents`.`page_channel`, `bld_widget_contents`.`page_division`, `bld_widget_contents`.`page_type`, `bld_widget_contents`.`page_app`, `bld_widget_contents`.`page_domain`, 
`bld_widget_contents`.`page_url`, `bld_widget_contents`.`page_path`, `bld_widget_contents`.`page_campaign`, `bld_widget_contents`.`user_uuid`, `bld_widget_contents`.`user_permalink`, `bld_widget_contents`.`user_agent`, `bld_widget_contents`.`user_logged_in`, 
`bld_widget_contents`.`user_device`, `bld_widget_contents`.`user_device_type`, `bld_widget_contents`.`widget_content_name`, `bld_widget_contents`.`widget_content_type`, `bld_widget_contents`.`widget_content_position`, `bld_widget_contents`.`widget_content_typepos`, 
`bld_widget_contents`.`wc_campaign`, `bld_widget_contents`.`wc_data_json`, `bld_widget_contents`.`wc_height`, `bld_widget_contents`.`wc_width`, 
`bld_widget_contents`.`deal_permalink`, `bld_widget_contents`.`deal_uuid`, `bld_widget_contents`.`widget_name`, `bld_widget_contents`.`widget_type`, 
`bld_widget_contents`.`platform`,
`bld_widget_contents`.`page_country`, `bld_widget_contents`.`dt`, `bld_widget_contents`.`hr` 
from `prod_groupondw`.`bld_widget_contents`;

desc formatted prod_groupondw.bld_widget_contents;

select
cookie,
consumerid,
dealuuid,
parentpageid,
clientplatform as platform,
eventtime,
widgetname,
widgetscontentdataslot_1content,
widgetscontentdataslot_1type,
widgetscontentdataslot_2content,
widgetscontentdataslot_2type, 
eventdestination
from grp_gdoop_pde.janus_all
where ds = '2020-09-14'
and
lower(widgetname) in ('cartmessage','usermessage')
;

select
bcookie,
consumerid,
dealuuid,
parentpageid,
clientplatform as platform,
eventtime,
widgetname,
eventdestination
from grp_gdoop_pde.janus_all
where ds = '2020-08-14'
and
lower(widgetname) in ('cartmessage','usermessage');


select
ds,
bcookie,
consumerid,
dealuuid,
parentpageid,
clientplatform as platform,
widgetname,
eventdestination
from
grp_gdoop_pde.junoHourly
where ds = '2020-08-14'
and
lower(widgetname) in ('cartmessage','usermessage')
;






select 
widgettype,
eventname,
widgetname,
widgettreatment,
widgetindex,
widgetscontentdataslot_1content,
widgetscontentdataslot_1type,
widgetscontentdataslot_2content,
widgetscontentdataslot_2type,
widgetcontenttype,
widgetcontentname,
componentname,
tagname,
errorcode,
errormessage,
errortag,
buyermax,
buyermaxwindow,
eventdestination
from grp_gdoop_pde.junoHourly
where eventdate = '2020-08-14'
and lower(widgetname) in ('cartmessage','usermessage')
limit 5;



select 
widgettype,
eventname,
widgetname,
widgettreatment,
widgetindex,
widgetscontentdataslot_1content,
widgetscontentdataslot_1type,
widgetscontentdataslot_2content,
widgetscontentdataslot_2type,
widgetcontenttype,
widgetcontentname,
componentname,
tagname,
errorcode,
errormessage,
errortag,
buyermax,
buyermaxwindow,
eventdestination
from grp_gdoop_pde.junoHourly
where eventdate = '2020-08-14'
and lower(widgetname) in ('cartmessage','usermessage')
and eventdestination = 'genericClick'
and e
limit 5;

select 
eventdate,
bcookie,
consumerid,
dealuuid,
parentpageid,
clientplatform as platform,
widgettype,
eventname,
widgetname,
widgettreatment,
widgetindex,
widgetscontentdataslot_1content,
widgetscontentdataslot_1type,
widgetscontentdataslot_2content,
widgetscontentdataslot_2type,
widgetcontenttype,
widgetcontentname,
widgetcontenttypepos,
componentname,
tagname,
errorcode,
errormessage,
errortag,
buyermax,
buyermaxwindow,
eventdestination,
fullurl,
url,
parentpageurl,
userpermalink,
orderdetails
from grp_gdoop_pde.junoHourly
where eventdate = '2020-09-14'
and lower(widgetname) in ('cartmessage','usermessage')
and bcookie = 'a0a3f628-a6df-4202-a3f6-28a6dfa20229';



select 
widgettype,
eventname,
widgetname,
widgettreatment,
widgetindex,
widgetscontentdataslot_1content,
widgetscontentdataslot_1type,
widgetscontentdataslot_2content,
widgetscontentdataslot_2type,
widgetcontenttype,
widgetcontentname,
componentname,
tagname,
errorcode,
errormessage,
errortag,
buyermax,
buyermaxwindow,
eventdestination
from grp_gdoop_pde.junoHourly
where eventdate = '2020-08-14'
and errormessage is not null
limit 5;


select
        eventdate dt,
        bcookie,
        consumerid,
        parentpageid, 
        platform
    from grp_gdoop_pde.junoHourly
    where lower(widgetname) in ('cartmessage','usermessage')
    and lower(widgetcontenttype) = 'inventory_product_declined'
    and cast(eventdate as date) between cast('2020-08-13' as date) and cast('2020-08-15' as date)
    and lower(platform) = 'touch';
    
   
select distinct 
        clientplatform
    from grp_gdoop_pde.junoHourly
    where lower(widgetname) in ('cartmessage','usermessage')
    and lower(widgetcontenttype) = 'inventory_product_declined'
    and cast(eventdate as date)  between cast('2020-08-13' as date) and cast('2020-08-15' as date);
    
  
select * from user_groupondw.bld_events limit 5;


