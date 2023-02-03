CREATE table grp_gdoop_bizops_db.avb_to_t600_hbw(
      row_rank int,
      account_id string
) stored as orc
tblproperties ("orc.compress"="SNAPPY");
      
show table sandbox.avb_to_bt_options_live2;
     
----TERADATA TABLE 

sandbox.avb_to_bt_options_live;


CREATE MULTISET TABLE sandbox.avb_to_bt_options_live ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      account_id VARCHAR(32) CHARACTER SET LATIN NOT CASESPECIFIC,
      total_options_bookable INTEGER)
PRIMARY INDEX ( account_id );


-----HIVE EXECUTE
use grp_gdoop_bizops_db;drop table if exists nvp_to_transfer_tmp;create temporary table nvp_to_transfer_tmp ( 
row_rank int,
account_id string
)ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES
(
    "separatorChar" = ",",
    "quoteChar"     = "\""
);
LOAD DATA INPATH 'hdfs://cerebro-namenode/user/grp_gdoop_BizOps/pg_orders/avb_to_t600_hbw2.csv'
OVERWRITE INTO TABLE nvp_to_transfer_tmp;
drop table if exists avb_to_t600_hbw;
create table avb_to_t600_hbw stored as orc as
    select * from nvp_to_transfer_tmp;
drop table if exists nvp_to_transfer_tmp;


------

select * from grp_gdoop_bizops_db.avb_to_t600_hbw;


select * from grp_gdoop_bizops_db.sh_bt_products limit 100;
select * from grp_gdoop_bizops_db.sh_bt_products_bookings limit 100;
select * from grp_gdoop_bizops_db.sh_bt_deals where country = 'US' limit 5;


drop table if exists grp_gdoop_bizops_db.avb_to_t600_hbw_op;
create table grp_gdoop_bizops_db.avb_to_t600_hbw_op stored as orc as
select
        a.account_id,
        coalesce(sum(x.bookable_options),0) as total_options_bookable
  from grp_gdoop_bizops_db.avb_to_t600_hbw a
    left join edwprod.dim_merchants_unity mu on mu.salesforce_account_id = a.account_id 
    left join (
    select
       pa.merchant_uuid,
       count(distinct case when pr.is_bookable = 1 and pr.is_active = 1 then pa.id end) bookable_options
       from grp_gdoop_bizops_db.sh_bt_deals d
       join grp_gdoop_bizops_db.sh_bt_partners pa on d.partners_id = pa.id and d.country = pa.country
       join grp_gdoop_bizops_db.sh_bt_products pr on d.products_id = pr.id and d.country = pr.country
       join user_groupondw.active_deals ad on d.groupon_real_deal_uuid = ad.deal_uuid
       where pa.country = 'US' and ad.load_date = date_sub(current_date,1)
       group by pa.merchant_uuid) x on x.merchant_uuid = mu.merchant_uuid
    group by a.account_id;


delete from sandbox.avb_to_bt_options_live;insert into sandbox.avb_to_bt_options_live
sel
    account_id,
    sum(total_options_bookable) as bk_options
from (
    sel * from sandbox.avb_to_bt_options_live2
    union all 
    sel
        m.account_id,
        count(distinct inventory_id) option_live
    from sandbox.hbw_deals_bookings_flags m
    join user_groupondw.fact_active_deals ad on m.deal_uuid = ad.deal_uuid
    where 
        mbo_flag in (1)
        and m.deal_uuid <> '88610046-a287-48e7-8fd3-e5092b5926f1' -- permalink booking-test-co-5'
        and m.report_date = (sel max(report_date) from sandbox.hbw_deals_bookings_flags)
        and m.report_date = ad.load_date
        and sold_out = 'FALSE' and available_qty > 0
    group by 1
) bk_options
group by 1

select * from sandbox.avb_to_bt_options_live;

-----
