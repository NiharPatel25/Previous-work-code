select * from user_dw.v_dim_pds_grt_map;

select * from grp_gdoop_sup_analytics_db.eh_to_merch_loc_agg;

select * from grp_gdoop_bizops_db.avb_to_t600_hbw;

select * from grp_gdoop_bizops_db.to_hbw_720;


select cast(a.row_rank as int) row_rank, 
a.*
from grp_gdoop_bizops_db.avb_to_top_accts_hbw a order by row_rank asc;

select max(report_wk), max(report_date)  from grp_gdoop_sup_analytics_db.eh_to_merch_loc_agg;

select * from grp_gdoop_bizops_db.to_hbw_now where account_id = '0013c00001oTTWkAAO';

select * from grp_gdoop_bizops_db.to_hbw_booking_now where account_id = '001C000001YIqPeIAL';

select count(*) from (select distinct * from grp_gdoop_bizops_db.to_hbw_booking_now);


select * from grp_gdoop_bizops_db.avb_to_top_accts_hbw order by cast(row_rank as int); 

select * from grp_gdoop_bizops_db.to_hbw_owners;

select * from grp_gdoop_sup_analytics_db.jc_merchant_mtd_attrib;

select * from grp_gdoop_bizops_db.avb_to_top_accts_hbw;
