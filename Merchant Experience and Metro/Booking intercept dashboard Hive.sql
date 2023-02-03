
select
    cast(bld.dt as date) as event_date
    , count(distinct bld.user_browser_id) views
  from prod_groupondw.bld_widgets bld
  where bld.page_country = 'US'
    and cast(bld.dt as date) > cast('2020-11-12' as date) and cast(bld.dt as date)< cast('2021-01-01' as date)
    and lower(bld.widget_name) = 'bookingsurvey'
  group by cast(bld.dt as date)
  order by cast(bld.dt as date);
  


drop table if exists grp_gdoop_bizops_db.nvp_intercept_tempviews;
create table grp_gdoop_bizops_db.nvp_intercept_tempviews stored as orc as 
select
     cast(bld.dt as date) event_date,
     count(distinct bld.user_browser_id) views
from prod_groupondw.bld_widgets bld
where cast(bld.dt as date) > cast('2020-11-12' as date)
    and lower(bld.widget_name) = 'bookingsurvey'
group by cast(bld.dt as date);

---------TERADATA


create table sandbox.nvp_booking_intercept_views (
     event_date date,
     view_counts integer
) primary index(event_date);
