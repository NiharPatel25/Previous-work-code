select 
    eventdestination, 
    pageapp
from
    grp_gdoop_pde.junoHourly
where eventdate >= '2021-03-15' and eventdate <= '2021-03-31'
    and  widgetname ='bss_hub_hours_and_staff_view'
    limit 5
;