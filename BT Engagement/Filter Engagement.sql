select * from user_groupondw.bld_widget_contents limit 5;
select * from user_groupondw.bld_widgets limit 5;



select * from user_groupondw.bld_widget_contents limit 5;
select * from user_groupondw.bld_widgets limit 5;

select * from np_temp_widget;


select
event_type, 
event_name, 
session_id, 
event_id,
page_id, 
page_type, 
widget_content_name, 
widget_content_type, 
widget_name
from user_groupondw.bld_widget_contents 
where dt = '2020-02-14' and page_country = 'NZ' and platform = 'touch' and lower(widget_name) like '%date%';

select * from user_groupondw.bld_widget_contents limit 5;


select
event_type, 
event_name, 
session_id, 
event_id,
page_id, 
page_type, 
widget_content_name, 
widget_content_type, 
widget_name
from user_groupondw.bld_widget_contents 
where dt = '2020-02-14' and page_country = 'NZ' and platform = 'touch' and lower(widget_name) like '%date\_%';



----date_time null



select
distinct
platform,
widget_name
from user_groupondw.bld_widget_contents 
where dt = '2020-03-01' and page_country = 'UK' and lower(widget_name) like '%date%';


select
distinct
platform,
widget_name
from user_groupondw.bld_widget_contents 
where dt = '2020-03-01' and page_country = 'UK' and lower(widget_name) like '%filter%';

select
distinct
platform,
widget_name
from user_groupondw.bld_widget_contents 
where dt = '2020-03-01' and page_country = 'UK' and lower(widget_name) like '%filter%';



select 
widget_name
from user_groupondw.bld_widgets
where dt = '2019-03-01' and page_country = 'UK' and lower(widget_name) like '%date%';

select 
widget_name
from user_groupondw.bld_widgets
where dt = '2019-02-15' and page_country = 'NZ' and lower(widget_name) like '%booking%';

select 
widget_name
from user_groupondw.bld_widgets
where dt = '2019-02-15' and page_country = 'NZ' and lower(widget_name) like '%date%';

select 
distinct
widget_name
from user_groupondw.bld_widgets
where dt = '2019-02-15' and page_country = 'NZ' and lower(widget_name) like '%filter%';

select
widget_name
from user_groupondw.bld_widgets
where dt = '2019-02-15' and page_country = 'NZ' and lower(widget_name) like '%bookingdatetimefilter%';




select
widget_name
from user_groupondw.bld_widget_contents 
where dt = '2020-03-01' and page_country = 'UK' and platform = 'touch' and lower(widget_name) = 'booking_date_time_filter' limit 1;




select * FROM user_groupondw.gbl_bloodhound_widgets limit 5;



select widget_name from user_groupondw.gbl_bloodhound_widgets where widget_name like 'date\_%';


user_groupondw.gbl_bloodhound_widgets
-----------------


select * from user_groupondw.bloodhound_events_v3 where event_time > '2019=01-01' and widget_name is not null limit 5; -- widget_name is empty

select * from user_groupondw.bloodhound_widget_contents_cd limit 5;