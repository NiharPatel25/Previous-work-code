select 
    report_date,
    reference_date,
    days_delta, 
    gss_total_availability,
    gbk_morning+ gbk_noon +gbk_afternoon + gbk_evening sum_gbk
from grp_gdoop_bizops_db.jk_bt_availability_gbl
where deal_uuid = '196e7136-c3c7-429f-b26a-1944fc58dc02'
and cast(report_date as date) > cast('2020-09-01' as date)
and days_delta <=7 and reference_date > cast('2020-12-01' as date)
order by report_date,reference_date;

select 
    report_date,
    reference_date,
    days_delta, 
    gss_total_availability,
    gbk_morning+ gbk_noon +gbk_afternoon + gbk_evening
from grp_gdoop_bizops_db.jk_bt_availability_gbl
where deal_uuid = 'd84c19bc-57e6-4a01-8ece-788e11235077'
and cast(report_date as date) > cast('2020-09-01' as date)
and days_delta <=7 and reference_date > cast('2020-12-01' as date)
order by report_date,reference_date;



select * from grp_gdoop_bizops_db.nvp_to_temp_availablity 
where
deal_uuid = 'ce4175ed-f3a3-4ce6-ac39-b6d0d8a6ebe4'
order by report_date desc;



select 
    report_date,
    reference_date,
    days_delta, 
    gss_total_availability,
    gbk_morning+ gbk_noon +gbk_afternoon + gbk_evening
from grp_gdoop_bizops_db.jk_bt_availability_gbl
where deal_uuid = '2f801f4a-e637-4dd0-a355-58d5144a2e4f'
and cast(report_date as date) > cast('2020-09-01' as date)
and days_delta <=7 and reference_date > cast('2020-12-01' as date)
order by report_date,reference_date;

