select * from grp_gdoop_bizops_db.jk_availability_report_intake2;


select 
	cast(id as int), 
	created,--cannot be converted to datetime
	cast(report_date as date),
	merchant_uuid, 
	deal_uuid, 
	deal_option_uuid, 
	calendar_uuid,
	cast(string(reference_date) as date),
	cast(string(days_delta) as int),
	cast(string(oss_blocked_hours) as int),
	cast(string(oss_max_end_time) as date)
from grp_gdoop_bizops_db.jk_availability_report_intake;


/*select 
cast(created as datetime),
date_format(created,'yyyy-MM-dd HH:MM:SS'),
from_unixtime(unix_timestamp(created), 'yyyy-MM-dd HH:MM:SS'),
	 created
from grp_gdoop_bizops_db.jk_availability_report_intake


date_format(created,'yyyy-MM-dd HH:MM:SS')*/