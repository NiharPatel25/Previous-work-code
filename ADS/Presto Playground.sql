select ds from grp_gdoop_bizops_db.np_opt_cons_copy group by ds order by ds;

select * from grp_gdoop_bizops_db.np_opt_cons_copy;
SELECT * FROM ai_reporting.wallet_info limit 5;

select * from edwprod.fact_gbl_transactions;
select * from grp_gdoop_bizops_db.pai_orders;

select * from grp_gdoop_bizops_db.np_bld_event_tmp
---------------------------------PERSONALIZED 



select 
a.*, 
b.personalized_clk, 
b.all_clks
from 
(select
    clientplatform, 
    variantname,
    count(distinct case when sponsoredpersonalised  = 'true' then concat(consumerid, eventdate) end) personalized_imps, 
    count(distinct concat(consumerid, eventdate)) all_imps
from grp_gdoop_bizops_db.np_pers_test_imp3
    group by 
    clientplatform, 
    variantname) as a 
left join  
(select
    clientplatform, 
    variantname,
    count(distinct case when sponsoredpersonalised  = 'true' then concat(consumerid, eventdate) end) personalized_clk, 
    count(distinct concat(consumerid, eventdate)) all_clks
from grp_gdoop_bizops_db.np_pers_test_clk3
    group by 
    clientplatform, 
    variantname) as b on a.clientplatform = b.clientplatform and a.variantname = b.variantname
    


select
    eventdate,
    clientplatform, 
    variantname,
    count(consumerid) total_impressions, 
    count(distinct consumerid) unique_consumer_imp
from grp_gdoop_bizops_db.np_pers_test_imp3
    group by 
    eventdate,
    clientplatform, 
    variantname;
   
select 
    eventdate,
    clientplatform, 
    variantname,
    count(consumerid) total_clks, 
    count(distinct consumerid) unique_consumer_clk
from 
grp_gdoop_bizops_db.np_pers_test_clk3
group by 
    eventdate,
    clientplatform, 
    variantname;

select 
    eventdate,
    clientplatform, 
    variantName,
    count(distinct consumerid) unique_cooks
from 
grp_gdoop_bizops_db.np_pers_test_imp
group by eventdate, clientplatform, variantName


select
    eventdate,
    clientplatform, 
    variantname,
    count(consumerid) total_impressions, 
    count(distinct consumerid) unique_consumer_imp
from grp_gdoop_bizops_db.np_pers_test_imp3
    group by 
    eventdate,
    clientplatform, 
    variantname
    
select 
    eventdate,
    clientplatform, 
    variantname,
    count(consumerid) total_clks, 
    count(distinct consumerid) unique_consumer_clk
from 
grp_gdoop_bizops_db.np_pers_test_clk3
group by 
    eventdate,
    clientplatform, 
    variantname
    
select 
a.*, 
b.personalized_clk, 
b.all_clks
from 
(select
    clientplatform, 
    variantname,
    count(distinct case when sponsoredpersonalised  = 'true' then concat(consumerid, eventdate) end) personalized_imps, 
    count(distinct concat(consumerid, eventdate)) all_imps
from grp_gdoop_bizops_db.np_pers_test_imp3
    group by 
    clientplatform, 
    variantname) as a 
left join  
(select
    clientplatform, 
    variantname,
    count(distinct case when sponsoredpersonalised  = 'true' then concat(consumerid, eventdate) end) personalized_clk, 
    count(distinct concat(consumerid, eventdate)) all_clks
from grp_gdoop_bizops_db.np_pers_test_clk3
    group by 
    clientplatform, 
    variantname) as b on a.clientplatform = b.clientplatform and a.variantname = b.variantname

--------
SET hive.auto.convert.join=false;SET mapred.reduce.tasks=503;SET mapreduce.job.reduces=503;SET mapreduce.input.fileinputformat.split.minsize=1;SET mapreduce.input.fileinputformat.split.maxsize=10000000;SET tez.grouping.min-size=1;SET tez.grouping.max-size=10000000;set hive.exec.dynamic.partition.mode=nonstrict;set hive.merge.size.per.task=32000000;set hive.exec.reducers.bytes.per.reducer=5000000;insert overwrite table grp_gdoop_bizops_db.np_opt_cons_copy partition (ds)
select consumerId, 
       experimentName, 
       variantName,
       clientplatform,
       count(1) impressions,
       ds
from grp_gdoop_pde.janus_all 
where experimentName='relevance-sl-personalisation-cc' and ds >= date_sub(current_date,3)
group by 
      consumerId, 
      experimentName, 
      variantName,
      clientplatform,
      ds
      
      SET hive.auto.convert.join=false;SET mapred.reduce.tasks=503;SET mapreduce.job.reduces=503;SET mapreduce.input.fileinputformat.split.minsize=1;SET mapreduce.input.fileinputformat.split.maxsize=10000000;SET tez.grouping.min-size=1;SET tez.grouping.max-size=10000000;set hive.exec.dynamic.partition.mode=nonstrict;set hive.merge.size.per.task=32000000;set hive.exec.reducers.bytes.per.reducer=5000000;DROP TABLE grp_gdoop_bizops_db.np_pers_test_imp;create table grp_gdoop_bizops_db.np_pers_test_imp stored as orc as 
  select 
           jh.bcookie, 
           jh.eventdate, 
           jh.clientplatform, 
           jh.extrainfo,
           exp_users.variantName,
           cast(jh.rawevent as varchar(64)) raw_event_name,
           jh.consumerid
           ----count(distinct concat(bcookie, eventdate)) as unique_impressions
    from
    (select bcookie, eventdate, clientplatform, extrainfo, rawevent, consumerid
    from grp_gdoop_pde.junoHourly 
    where eventdate >= '2022-05-19'
    and lower(trim(regexp_replace(clientplatform,'\\t|\\n|\\r|\\u0001', ''))) in ('iphone', 'android')
    and lower(eventdestination) = 'genericimpression'
    and lower(event) = 'genericimpression'
    and country in ('US','CA')
    and platform = 'mobile'
    and consumerid is not null
    and lower(brand) = 'groupon' 
    and bcookie not in ('android_automation_test' ,'ios_automation_test','android_perf_test_nightly','ios_perf_test_nightly')
    and case when 'userAgent' like '%bot%' or useragent like '%crawler%' or useragent like '%search%' or useragent like '%spider%' or useragent like '%spyder%' then 1 else 0 end = 0
    and lower(trim(extrainfo)) like '%crosschannel_homepage_sponsored_carousel%' 
    and lower(impressiontype) = 'collection_card_impression') as jh
    join 
       (select * from grp_gdoop_bizops_db.np_opt_cons_copy where ds >= '2022-05-19') as  exp_users
          on lower(trim(jh.consumerid)) = lower(trim(exp_users.consumerId)) and jh.eventdate = exp_users.ds
union all 
select 
   jh.user_browser_id, 
   jh.dt,
   jh.platform,
   null as extrainfo,
   exp_users.variantName,
   jh.event_name,
   jh.user_uuid
from 
(select 
     user_browser_id,
     dt, 
     case when platform = 'desktop' then 'web' else platform end platform,
     event_name, 
     user_uuid
from prod_groupondw.bld_widget_contents 
where lower(platform) in ('desktop', 'touch')
and dt >= '2022-05-19'
and user_uuid is not null
and lower(page_country) IN ('us', 'ca')
and lower(page_hostname) like '%groupon%'
and lower(page_type) like '%homepage%'
and lower(widget_content_name) like '%crosschannel_homepage_sponsored_carousel%'
and lower(widget_content_type) = 'compound') as jh
join 
       (select * from grp_gdoop_bizops_db.np_opt_cons_copy where ds >= '2022-05-19') as  exp_users
          on lower(trim(jh.user_uuid)) = lower(trim(exp_users.consumerId)) and jh.dt = exp_users.ds
    
    
    
    SET hive.auto.convert.join=false;SET mapred.reduce.tasks=503;SET mapreduce.job.reduces=503;SET mapreduce.input.fileinputformat.split.minsize=1;SET mapreduce.input.fileinputformat.split.maxsize=10000000;SET tez.grouping.min-size=1;SET tez.grouping.max-size=10000000;set hive.exec.dynamic.partition.mode=nonstrict;set hive.merge.size.per.task=32000000;set hive.exec.reducers.bytes.per.reducer=5000000;insert overwrite table grp_gdoop_bizops_db.np_opt_cons_copy partition (ds)
select consumerId, 
       experimentName, 
       variantName,
       clientplatform,
       count(1) impressions,
       ds
from grp_gdoop_pde.janus_all 
where experimentName='relevance-sl-personalisation-cc' and ds >= date_sub(current_date,3)
group by 
      consumerId, 
      experimentName, 
      variantName,
      clientplatform,
      ds
      
      
 SET hive.auto.convert.join=false;SET mapred.reduce.tasks=503;SET mapreduce.job.reduces=503;SET mapreduce.input.fileinputformat.split.minsize=1;SET mapreduce.input.fileinputformat.split.maxsize=10000000;SET tez.grouping.min-size=1;SET tez.grouping.max-size=10000000;set hive.exec.dynamic.partition.mode=nonstrict;set hive.merge.size.per.task=32000000;set hive.exec.reducers.bytes.per.reducer=5000000;DROP TABLE grp_gdoop_bizops_db.np_pers_test_imp;create table grp_gdoop_bizops_db.np_pers_test_imp stored as orc as 
  select 
           jh.bcookie, 
           jh.eventdate, 
           jh.clientplatform, 
           jh.extrainfo,
           exp_users.variantName,
           cast(jh.rawevent as varchar(64)) raw_event_name,
           jh.consumerid
           ----count(distinct concat(bcookie, eventdate)) as unique_impressions
    from
    (select bcookie, eventdate, clientplatform, extrainfo, rawevent, consumerid
    from grp_gdoop_pde.junoHourly 
    where eventdate >= '2022-05-19'
    and lower(trim(regexp_replace(clientplatform,'\\t|\\n|\\r|\\u0001', ''))) in ('iphone', 'android')
    and lower(eventdestination) = 'genericimpression'
    and lower(event) = 'genericimpression'
    and country in ('US','CA')
    and platform = 'mobile'
    and consumerid is not null
    and lower(brand) = 'groupon' 
    and bcookie not in ('android_automation_test' ,'ios_automation_test','android_perf_test_nightly','ios_perf_test_nightly')
    and case when 'userAgent' like '%bot%' or useragent like '%crawler%' or useragent like '%search%' or useragent like '%spider%' or useragent like '%spyder%' then 1 else 0 end = 0
    and lower(trim(extrainfo)) like '%crosschannel_homepage_sponsored_carousel%' 
    and lower(impressiontype) = 'collection_card_impression') as jh
    join 
       (select * from grp_gdoop_bizops_db.np_opt_cons_copy where ds >= '2022-05-19') as  exp_users
          on lower(trim(jh.consumerid)) = lower(trim(exp_users.consumerId)) and jh.eventdate = exp_users.ds
union all 
select 
   jh.user_browser_id, 
   jh.dt,
   jh.platform,
   null as extrainfo,
   exp_users.variantName,
   jh.event_name,
   jh.user_uuid
from 
(select 
     user_browser_id,
     dt, 
     case when platform = 'desktop' then 'web' else platform end platform,
     event_name, 
     user_uuid
from prod_groupondw.bld_widget_contents 
where lower(platform) in ('desktop', 'touch')
and dt >= '2022-05-19'
and user_uuid is not null
and lower(page_country) IN ('us', 'ca')
and lower(page_hostname) like '%groupon%'
and lower(page_type) like '%homepage%'
and lower(widget_content_name) like '%crosschannel_homepage_sponsored_carousel%'
and lower(widget_content_type) = 'compound') as jh
join 
       (select * from grp_gdoop_bizops_db.np_opt_cons_copy where ds >= '2022-05-19') as  exp_users
          on lower(trim(jh.user_uuid)) = lower(trim(exp_users.consumerId)) and jh.dt = exp_users.ds

select 
   min(ds)
from grp_gdoop_bizops_db.np_opt_cons_copy;

select 
* 
from 
(select 
    bcookie, 
    count(distinct event_date) xyz 
from prod_groupondw.user_bcookie_mapping
where event_date  >= '2022-06-09'
group by bcookie) where xyz > 1;

select * from prod_groupondw.user_bcookie_mapping where bcookie  = '2e636adf-fbc1-87bc-65b8-678b2d04eefd';
select * from prod_groupondw.user_bcookie_mapping where user_uuid = '00603d02-1e93-11e9-846f-0242ac110002' and event_date >= '2022-06-01';

describe table grp_gdoop_local_ds_db.est_maxcpc_by_dealid;

select * from grp_gdoop_bizops_db.SFMC_EmailEngagement;
select * from grp_gdoop_bizops_db.np_sl_pagev_temp 
union all 
select * from grp_gdoop_bizops_db.np_sl_pagev_temp2;
Select * from grp_gdoop_bizops_db.np_sl_pagev_temp;

select distinct ds from grp_gdoop_bizops_db.np_opt_cons_copy order by ds;

select * from  grp_gdoop_bizops_db.np_pers_test_imp

select distinct eventdate from grp_gdoop_bizops_db.np_impressions_sl where eventdate >= '2022-05-19' order by eventdate;
select distinct eventdate from grp_gdoop_bizops_db.np_impressions_sl_app where eventdate >= '2022-05-19' order by eventdate;

select distinct eventdate from ai_reporting.sl_imp_clicks_app where eventdate >= '2022-05-19' order by eventdate;

select distinct eventdate from grp_gdoop_bizops_db.np_pers_test_clk3  order by eventdate;


select * from ai_reporting.sl_imp_clicks_app;

select ds, 
       clientplatform,
       sum(total_impressions) total_imps, 
       sum(case when total_variants = 3 then total_impressions end) three_variants, 
       sum(case when total_variants = 2 then total_impressions end) two_variants
       from grp_gdoop_bizops_db.np_opt_cons_copy
       group by ds, clientplatform;

select * from grp_gdoop_bizops_db.np_pers_test_imp3;

 select
    eventdate,
    clientplatform, 
    variantname,
    count(consumerid) total_impressions, 
    count(distinct consumerid) unique_consumer_imp
from grp_gdoop_bizops_db.np_pers_test_imp3
    group by 
    eventdate,
    clientplatform, 
    variantname
      
select 
    eventdate,
    clientplatform, 
    variantname,
    count(consumerid) total_clks, 
    count(distinct consumerid) unique_consumer_clk
from 
grp_gdoop_bizops_db.np_pers_test_clk3
group by 
    eventdate,
    clientplatform, 
    variantname;

select 
   ds, 
   clientplatform,
   count(consumerid) total_uniq_imps
   count(case when vars = 1 then consumerid end) one_variants, 
   count(case when vars = 2 then consumerid end) two_variants, 
   count(case when vars = 3 then consumerid end) three_variants
from 
(select 
       ds, 
       consumerid, 
       clientplatform,
       count(distinct variantname) vars
from grp_gdoop_bizops_db.np_opt_cons_copy
group by ds, consumerid)
as fin 
group by ds,clientplatform
;

select *
    from grp_gdoop_pde.junoHourly 
    where eventdate >= '2022-05-19'
    and lower(eventdestination) = 'genericimpression'
    and lower(event) = 'genericimpression'
    and country in ('US','CA')
    and platform = 'desktop'
    and consumerid is not null
    and lower(brand) = 'groupon';

select * from grp_gdoop_bizops_db.np_sl_pagev_temp;

select eventdate from grp_gdoop_bizops_db.np_pers_test_imp3 group by 1 order by eventdate desc;

select 
   ds, 
   clientplatform,
   count(consumerid) total_uniq_imps,
   count(case when vars = 1 then consumerid end) one_variants, 
   count(case when vars = 2 then consumerid end) two_variants, 
   count(case when vars = 3 then consumerid end) three_variants
from 
(select 
       ds, 
       consumerid, 
       clientplatform,
       count(distinct variantname) vars
from grp_gdoop_bizops_db.np_opt_cons_copy
group by ds, consumerid, clientplatform) as fin 
group by ds,clientplatform;


select ds, 
       clientplatform, 
       variantName, 
       count(distinct consumerid) unique_logins,
       sum(impressions) total_impressions 
       from grp_gdoop_bizops_db.np_opt_cons_copy
where ds >= '2022-05-19'
group by ds, clientplatform, variantName;


select * from grp_gdoop_bizops_db.np_pers_test_clk3;
select distinct eventdate from grp_gdoop_bizops_db.np_impressions_sl where eventdate  >= '2022-05-19' order by eventdate;
select distinct eventdate from grp_gdoop_bizops_db.np_impressions_sl_app where eventdate  >= '2022-05-19' order by eventdate;

2022-05-25
2022-05-28
2022-05-31
2022-06-04
-------------------------------------------POSITION WISE AND DEAL CATEGORY


SET hive.execution.engine=tez;SET hive.exec.dynamic.partition.mode=nonstrict;SET hive.exec.dynamic.partition=true;
SET hive.exec.max.dynamic.partitions.pernode=3000;SET hive.tez.java.opts=-Xmx6000M;SET hive.tez.container.size=8142;
SET hive.exec.parallel=true;SET hive.vectorized.execution.enabled=true;SET hive.vectorized.execution.enabled=false;
SET hive.input.format=org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;SET hive.enforce.sorting=true;
SET hive.enforce.bucketing=true;SET hive.enforce.bucketmapjoin=true;SET hive.enforce.sortmergebucketmapjoin=true;
SET hive.optimize.bucketmapjoin=true;SET hive.optimize.bucketmapjoin.sortedmerge=true;
SET hive.auto.convert.sortmerge.join.bigtable.selection.policy=org.apache.hadoop.hive.ql.optimizer.TableSizeBasedBigTableSelectorForAutoSMJ;
SET hive.optimize.ppd=true;SET hive.optimize.ppd.storage=true;SET hive.cbo.enable=true;SET hive.compute.query.using.stats=true;
SET hive.stats.dbclass=fs;SET hive.stats.fetch.column.stats=true;SET hive.stats.fetch.partition.stats=true;
SET hive.groupby.orderby.position.alias=true;SET hive.optimize.groupby=true;SET hive.auto.convert.join=true;
SET hive.auto.convert.join.noconditionaltask.size=1000000000;SET hive.auto.convert.sortmerge.join=true;SET hive.auto.convert.join.noconditionaltask=true;
SET hive.optimize.skewjoin=true;SET hive.optimize.skewjoin.compiletime=true;SET hive.skewjoin.mapjoin.map.tasks=10000;SET hive.skewjoin.key=10000;
SET hive.exec.compress.output=true;SET mapred.input.compress=true;SET hive.exec.compress.intermediate=true;SET mapred.output.compress=true;
SET mapred.map.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;SET mapreduce.output.fileoutputformat.compress=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec;SET mapreduce.map.output.compress=true;SET mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET mapred.compress.map.output=true;SET mapred.map.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;SET mapred.input.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET tez.am.container.reuse.enabled=true;SET tez.session.am.dag.submit.timeout.secs=900;SET tez.am.container.session.delay-allocation-millis=900000;
SET hive.mapjoin.smalltable.filesize=1000000000;SET tez.runtime.shuffle.failed.check.since-last.completion=false;SET hive.resultset.use.unique.column.names=false;
SET hive.optimize.sort.dynamic.partition=true;SET mapred.reduce.tasks=353;SET mapreduce.job.reduces=353;SET mapreduce.input.fileinputformat.split.minsize=1;
SET mapreduce.input.fileinputformat.split.maxsize=1000000;SET tez.grouping.min-size=1;SET tez.grouping.max-size=1000000;
with category_ids as (
--grt    grt_id    cft_l1_cat_name    cft_l2_cat_name    cft_l3_cat_name    cft_l4_cat_name    cft_l5_cat_name    cft_l1_cat_id    cft_l2_cat_id    cft_l3_cat_id    cft_l4_cat_id    cft_l5_cat_id    lev    permalink    friendly_name_singular
--Baby & Kids    f09218a4-3c84-475e-842a-6260627398d8    Local    Retail    Personalized Items    Baby & Kids    [NULL]    c09790ba-a6b9-40fc-ad81-4cdf25260b5e    c60d65f4-6015-4ed8-b847-d02fd70b5f14    1f3a71fa-3e51-47f5-bc5b-6ee5b6132a31    f09218a4-3c84-475e-842a-6260627398d8    [NULL]    L4    baby-kids    Baby & Kid
--Baby & Kids    9070f5c4-bd1f-40e5-b680-60f15097aa9d    Goods    Baby & Kids    [NULL]    [NULL]    [NULL]    db2cb956-fc1a-4d8c-88f2-66657ac41c24    9070f5c4-bd1f-40e5-b680-60f15097aa9d    [NULL]    [NULL]    [NULL]    L2    baby-kids-and-toys    Baby & Kid
--there are duplicates names on "grt" column, but the grt_id is unique
--add row counter when GRT are repeated, so we can select only the first one
select gr.*,dcat.permalink,dcat.friendly_name_singular 
,row_number() over (partition by grt order by lev) rownum
from (
select dcr1.cft_l1_cat_name grt
      ,cft_l1_cat_id grt_id
      ,dcr1.cft_l1_cat_name 
      ,null cft_l2_cat_name
      ,null  cft_l3_cat_name
      ,null cft_l4_cat_name
       ,null cft_l5_cat_name
       ,cft_l1_cat_id
       ,null cft_l2_cat_id
       ,null cft_l3_cat_id
       ,null cft_l4_cat_id
       ,null cft_l5_cat_id
       , 'L1' lev
      from dw.v_dim_pds_cft_map dcr1 
      where cft_l2_cat_name  is not null
      group by dcr1.cft_l1_cat_name 
       ,cft_l1_cat_id 
union all
select dcr1.cft_l2_cat_name grt
      ,cft_l2_cat_id grt_id
      ,dcr1.cft_l1_cat_name 
      ,dcr1.cft_l2_cat_name
      ,null  cft_l3_cat_name
      ,null cft_l4_cat_name
       ,null cft_l5_cat_name
       ,cft_l1_cat_id
       ,cft_l2_cat_id
       ,null cft_l3_cat_id
       ,null cft_l4_cat_id
       ,null cft_l5_cat_id
             , 'L2' lev
      from dw.v_dim_pds_cft_map dcr1 
      where cft_l2_cat_name  is not null
      group by dcr1.cft_l1_cat_name  ,dcr1.cft_l2_cat_name
       ,cft_l1_cat_id ,cft_l2_cat_id
union all
select dcr1.cft_l3_cat_name grt
      ,cft_l3_cat_id grt_id
      ,dcr1.cft_l1_cat_name 
      ,dcr1.cft_l2_cat_name
      ,cft_l3_cat_name
      ,null cft_l4_cat_name
       ,null cft_l5_cat_name
       ,cft_l1_cat_id
       ,cft_l2_cat_id
       ,cft_l3_cat_id
       ,null cft_l4_cat_id
       ,null cft_l5_cat_id
             , 'L3' lev
      from dw.v_dim_pds_cft_map dcr1 
      where cft_l3_cat_name  is not null
      group by dcr1.cft_l1_cat_name ,dcr1.cft_l2_cat_name, cft_l3_cat_name
       ,cft_l1_cat_id ,cft_l2_cat_id ,cft_l3_cat_id 
union all
select dcr1.cft_l4_cat_name grt
      ,cft_l4_cat_id grt_id
      ,dcr1.cft_l1_cat_name 
      ,dcr1.cft_l2_cat_name
      ,cft_l3_cat_name
      ,cft_l4_cat_name
       ,null cft_l5_cat_name
       ,cft_l1_cat_id
       ,cft_l2_cat_id
       ,cft_l3_cat_id
       ,cft_l4_cat_id
       ,null cft_l5_cat_id
             , 'L4' lev
      from dw.v_dim_pds_cft_map dcr1 
      where cft_l4_cat_name  is not null
      group by dcr1.cft_l1_cat_name ,dcr1.cft_l2_cat_name, cft_l3_cat_name, cft_l4_cat_name
       ,cft_l1_cat_id ,cft_l2_cat_id ,cft_l3_cat_id ,cft_l4_cat_id 
union all
select dcr1.cft_l5_cat_name grt
      ,cft_l5_cat_id grt_id
      ,dcr1.cft_l1_cat_name 
      ,dcr1.cft_l2_cat_name
      ,cft_l3_cat_name
      ,cft_l4_cat_name
       , cft_l5_cat_name
       ,cft_l1_cat_id
       ,cft_l2_cat_id
       ,cft_l3_cat_id
       ,cft_l4_cat_id
       , cft_l5_cat_id
             , 'L5' lev
      from dw.v_dim_pds_cft_map dcr1 
      where cft_l5_cat_name  is not null
      group by dcr1.cft_l1_cat_name ,dcr1.cft_l2_cat_name, cft_l3_cat_name, cft_l4_cat_name, cft_l5_cat_name
       ,cft_l1_cat_id ,cft_l2_cat_id ,cft_l3_cat_id ,cft_l4_cat_id ,cft_l5_cat_id  
) gr
left join prod_groupondw.dim_category dcat 
  on dcat.category_id = gr.grt_id
), 

categories_app as (
select 
case
 when get_json_object(lower(extrainfo), '$.type') is not null then 'browse'  --get_json_object(lower(extrainfo), '$.type')
 when search_query is not null and   search_query!='' and    search_query!='All+Deals'  and search_query != ' '
      and search_query != '{"search":""}' then 'search' --and  search_query not in ('hotel')
 when trim(lower(translate(search_query,'+',' '))) in ('all deals') or rawpagetype='Featured' then 'featured'
 when search_query in ('hotel') then 'browse'
 when (rawpagetype = 'home_tab' or rawpagetype ='GlobalSearchResult') 
     --and get_json_object(lower(extrainfo), '$.tabName') = 'home_tab'
     and (search_query is null or  search_query='')
     and coalesce(cat2.lev, cat.lev) is null 
     and get_json_object(lower(extrainfo), '$.type') is null 
     and channel = 'all' then 'homepage' 
 when  get_json_object(lower(extrainfo), '$.tabName') = 'home_tab' then 'homepage'
 when get_json_object(lower(extrainfo), '$.wolfhoundpageid') is not null then 'occasions'
 when get_json_object(lower(extrainfo), '$.tabname') = 'notifications_tab' then 'notifications_tab'
 when rawpagetype in ( 'GlobalSearchResult', 'MapLedSearch')  or (rawpagetype = 'Homepage' and channel = 'local' ) then 'browse'
 when rawpagetype = 'wolfhound_mobile_page' then 'occasions'
 else 'other' --rawpagetype 
 end page_type_SL
 --Category_pagetype 
   ,case when case when lower(channel)='global_search' then coalesce(prev_clickinfo.col1,channel) else channel end in ('search_freetext','autocomplete_selection_click','recent_search_click') or (search_query is not null 
    and   search_query!='' and    search_query!='All+Deals'  and search_query != ' ' and search_query != '{"search":""}')  then 'search'
   when coalesce(prev_clickinfo.col1,channel)  in ('in_app_message') then 'Mobile Search - In App Message'
   when coalesce(prev_clickinfo.col1,channel)  in ('push_notification_alert','notification_action_click', 'notification') then 'Push'
   --push prev_group_grp in ('GRP9') and prev_channel in('notification','notifications_tab_click')    only the first page comes from push, if click happens ignore it
   --prev_clickinfo is null  or like notification_tab_click      and lower(channel) = 'global_search'           (prev_clickinfo is null or prev_clickinfo.col1 in ('notification','notifications_tab_click')  )  and
   when ref_Attr_class_key in ('41','29')  and lower(channel) = 'global_search' then 'Push'
   when prev_clickinfo.col1 in ('nav_card_click','sticky_nav_card_click') and (prev_clickinfo.col2 like '%Play%' or prev_clickinfo.col2 like '%ThingsToDo%') then 'Local Browse / Nearby'
when  prev_clickinfo.col1 in ('nav_card_click','sticky_nav_card_click') and  prev_clickinfo.col2 like '%Goods%' then 'Goods Browse / Tab'
when  prev_clickinfo.col1 in ('nav_card_click','sticky_nav_card_click') and  (prev_clickinfo.col2 like '%Pampered%' or prev_clickinfo.col2 like '%BeautyandSpas%') then 'Local Browse / Nearby'
when  prev_clickinfo.col1 in ('nav_card_click','sticky_nav_card_click') and  (prev_clickinfo.col2 like '%Eat%' or prev_clickinfo.col2 like '%Restaurants%') then 'Local Browse / Nearby'
when  prev_clickinfo.col1 in ('nav_card_click','sticky_nav_card_click') and  prev_clickinfo.col2 like '%Travel%' then 'Getaways Page / Tab'
when  prev_clickinfo.col1 in ('nav_card_click','sticky_nav_card_click') and prev_clickinfo.col2 like '%Occasion%' then 'Occasions Page / Tab'
when  prev_clickinfo.col1 in ('nav_card_click','sticky_nav_card_click') and prev_clickinfo.col2 like '%Health%' then 'Local Browse / Nearby'
when  prev_clickinfo.col1 in ('nav_card_click','sticky_nav_card_click')  then 'Local Browse / Nearby'
when case when lower(channel)='global_search' then coalesce(prev_clickinfo.col1,channel) else channel end in ('band_card_refinement_click','category_click','category_selection_click','sub-category_click','popular_category_click', 'category_card_click') then 'Mobile Search - Categories'
when lower(channel) in ('all','featured') then 'Homepage'
when lower(channel) = 'global_search' then 'Mobile Search - Other'
when channel in ('search_tab_click', 'notifications_tab_click', 'mystuff_tab_click', 'saved_deal_tab_click', 'saved_deals_tab_click','categories_tab_click') then channel
else 'Other'
END page_type
 ,case when aogcampaignid is not null then 1 else 0 end SL
 --,get_json_object(lower(extrainfo), '$.type')  category
  , case
        when lower(prev_clickinfo.col1) in ('search_freetext','autocomplete_selection_click','recent_search_click') then 'Search'
        when lower(prev_clickinfo.col1) in ('in_app_message', 'push_notification_alert') then 'Mobile Search - In App Message'
        WHEN lower(prev_clickinfo.col1) IN ('nav_card_click','sticky_nav_card_click') 
            AND (lower(prev_clickinfo.col2) LIKE '%beautynow%' 
            OR lower(prev_clickinfo.col2) = 'beauty now' 
            OR lower(prev_clickinfo.col2) = 'bn') 
            THEN 'BeautyNow Page / Tab'
        WHEN lower(prev_clickinfo.col1) IN ('nav_card_click','sticky_nav_card_click') 
            AND lower(prev_clickinfo.col2) LIKE '%grouponplus%' 
            THEN 'Groupon+'
        WHEN lower(prev_clickinfo.col1) IN ('nav_card_click','sticky_nav_card_click') 
            AND lower(prev_clickinfo.col2) LIKE '%more%' 
            THEN 'More'
        WHEN lower(prev_clickinfo.col1) IN ('nav_card_click','sticky_nav_card_click') 
            AND (lower(prev_clickinfo.col2) LIKE '%occasion%'
            OR lower(prev_clickinfo.col2) LIKE '%bestofgroupon%') 
            THEN 'Occasions Page / Tab'
        WHEN lower(prev_clickinfo.col1) IN ('nav_card_click','sticky_nav_card_click') 
            AND (lower(prev_clickinfo.col2) LIKE '%restaurants%' 
            OR lower(prev_clickinfo.col2) LIKE '%beautyandspas%' 
            OR lower(prev_clickinfo.col2) LIKE '%thingstodo%' 
            OR lower(prev_clickinfo.col2) LIKE '%automotive%' 
            OR lower(prev_clickinfo.col2) LIKE '%play%' 
            OR lower(prev_clickinfo.col2) LIKE '%pampered%' 
            OR lower(prev_clickinfo.col2) LIKE '%eat%'  
            OR lower(prev_clickinfo.col2) LIKE '%health%') 
            THEN 'Local Browse / Nearby'
        WHEN lower(prev_clickinfo.col1) IN ('nav_card_click','sticky_nav_card_click') 
        AND  (lower(prev_clickinfo.col2) LIKE '%good%' 
        OR lower(prev_clickinfo.col2) LIKE '%shop%') 
        THEN 'Goods Browse / Tab'
        WHEN lower(prev_clickinfo.col1) IN ('nav_card_click','sticky_nav_card_click') 
            AND  (lower(prev_clickinfo.col2) LIKE '%travel%' OR lower(prev_clickinfo.col2) LIKE '%escapes%') 
            THEN 'Getaways Page / Tab'
        when lower(prev_clickinfo.col1) in ('band_card_refinement_click','category_click','category_selection_click','filter_select_click','sub-category_click','popular_category_click') 
            then 'Mobile Search - Categories' 
   end click_page_type
 ,case when coalesce(cat2.lev, cat.lev) is not null then coalesce(cat2.lev, cat.lev)
   when trim(lower(translate(search_query,'+',' '))) in ('all deals') or rawpagetype='Featured' then 'featured' 
    when rawpagetype = 'home_tab' and search_query is null and coalesce(cat2.lev, cat.lev) is null and get_json_object(lower(extrainfo), '$.type') is null 
     and channel = 'all' then 'homepage' 
 end level
 --,count(*),count(distinct bcookie)
 --,min(extrainfo),max(extrainfo)
 ,coalesce(cat2.grt_id, cat.grt_id) grt_id
 ,coalesce(cat2.cft_l1_cat_name, cat.cft_l1_cat_name) cft_l1_cat_name
 ,coalesce(cat2.cft_l2_cat_name, cat.cft_l2_cat_name) cft_l2_cat_name
 ,coalesce(cat2.cft_l3_cat_name, cat.cft_l3_cat_name) cft_l3_cat_name
 ,coalesce(cat2.cft_l4_cat_name, cat.cft_l4_cat_name) cft_l4_cat_name
 ,coalesce(cat2.cft_l5_cat_name, cat.cft_l5_cat_name) cft_l5_cat_name
 ,coalesce(cat2.cft_l1_cat_id, cat.cft_l1_cat_id) cft_l1_cat_id
 ,coalesce(cat2.cft_l2_cat_id, cat.cft_l2_cat_id) cft_l2_cat_id
 ,coalesce(cat2.cft_l3_cat_id, cat.cft_l3_cat_id) cft_l3_cat_id
 ,coalesce(cat2.cft_l4_cat_id, cat.cft_l4_cat_id) cft_l4_cat_id
 ,coalesce(cat2.cft_l5_cat_id, cat.cft_l5_cat_id) cft_l5_cat_id
 ,coalesce(cat2.permalink, cat.permalink) permalink
 ,imp.bcookie
 ,imp.dv_time 
 ,imp.order_uuid
 ,imp.ogp 
 ,imp.nob
 ,imp.nor
 ,imp.dealuuid
 ,eventdate
 ,clientplatform
 ,grt1,grt2,grt3,grt4
 ,cast(position as int )-1 position
 ,division
 ,consumerid
 ,aogcampaignid
 ,search_query
 ,rawpagetype
 ,coalesce(get_json_object(extrainfo, '$.attributionId')
 ,concat(get_json_object(extrainfo, '$.collectionCardUUID'), bcookie)
 ,concat(get_json_object(extrainfo, '$.tabName'),bcookie) ) pageid 
from ai_reporting.sl_imp_clicks_app imp
     left join  category_ids cat
        --on cat.grt_id = split_part(get_json_object(lower(prev_clickinfo.col2), '$.search_result.query'),',',2) --on iPhone there is the category_id inside the previos click with the category name
       on ( lower(cat.grt) = lower(case 
               when regexp_replace(get_json_object(lower(extrainfo), '$.type'),'travel','getaways' )in ('all nearby deals','all local deals') then 'Local'
            when regexp_replace(get_json_object(lower(extrainfo), '$.type'),'travel','getaways' )in ('all travel deals', 'hotels') then 'Getaways'
            when regexp_replace(get_json_object(lower(extrainfo), '$.type'),'travel','getaways' ) in ('all goods deals') then 'Goods'
            else regexp_replace(get_json_object(lower(extrainfo), '$.type'),'travel','getaways' )
        end)  
           and cat.rownum=1) --this will generate duplicates, use a rejoin with the table aggregated at GRT level
     left join  category_ids cat2
         on cat2.grt_id = split(get_json_object(lower(prev_clickinfo.col2), '$.search_result.query'),',')[1] --on iPhone there is the category_id inside the previos click with the category name
--or get_json_object(lower(extrainfo), '$.type') = lower(cft.grt2) 
--or get_json_object(lower(extrainfo), '$.type') = lower(cft.grt4) 
     where 
     cast(position as int ) between 1 and 20 and
    --eventdate between '2021-11-10' and '2021-11-28'
    cast( eventdate as date) between date_sub(current_date,2) and date_sub(current_date,1) --'2020-08-01' and '2020-08-11' 
) , 

categories as (
select 
cc.*
,cat.*
,coalesce(cat.lev,cc.levb) level
,coalesce(cat.cft_l1_cat_name ,cc.grt_l1) cft_l1_cat_name2
from (
select  bcookie
 ,dv_time 
 ,order_uuid
 ,ogp 
 ,nob
 ,nor
 ,dealuuid
 ,eventdate
 ,clientplatform
 ,grt1,grt2,grt3,grt4
 ,cast(position as int )  position
 ,division
 ,consumerid
 ,aogcampaignid
 ,rawpagetype
 ,pageid
 ,search_query
 ,case when aogcampaignid is not null then 1 else 0 end SL
 ,case --apgetype = browse
       when search_query is not null and   search_query!='' and    search_query!='All+Deals'  and search_query != ' '
       and search_query != '{"search":""}' then 'search'
       when REGEXP_EXTRACt(fullurl,'\\.\\w{2,3}/([\\w-]*)',1) ='' then 'homepage'
       when coalesce(REGEXP_EXTRACt(fullurl,'\\.\\w{2,3}/browse/([\\w-]*)',1),'')!='' 
       and coalesce(REGEXP_EXTRACt(fullurl,'context=([\\w-]*)',1),'')=''
       and coalesce(REGEXP_EXTRACt(fullurl,'topcategory=([\\w-]*)',1),'')=''
       and coalesce(REGEXP_EXTRACt(fullurl,'category=([\\w-]*)',1),'')='' 
       and coalesce(REGEXP_EXTRACt(fullurl,'subcategory2=([\\w-]*)',1),'')=''
       and coalesce(REGEXP_EXTRACt(fullurl,'subcategory3=([\\w-]*)',1),'')=''
       and coalesce(REGEXP_EXTRACt(fullurl,'query=([\\w-]*)',1),'')=''   --avoid searches
   then 'featured'
            --'search'
            -- avoid https://www.groupon.com/occasion/gg-apparel-accessories
            -- avoid https://www.groupon.com/landing/deal-of-the-day
            -- add occasions pages classification
            when REGEXP_EXTRACt(fullurl,'\\.\\w{2,3}/([\\w-]*)',1)='occasion' then 'occasions'
            when (REGEXP_EXTRACt(fullurl,'context=([\\w-]*)',1) = 'local' --https://www.groupon.com/browse/chicago?context=local
            or REGEXP_EXTRACt(fullurl,'topcategory=([\\w-]*)',1) in ('goods','travel','local')   --https://www.groupon.com/browse/chicago?topcategory=goods&category=for-the-home
            or REGEXP_EXTRACt(fullurl,'\\.\\w{2,3}/([\\w-]*)',1) in ('goods','getaways') )  --https://www.groupon.com/goods/
            and  ( coalesce(REGEXP_EXTRACt(fullurl,'[\\?\\&]category=([\\w-]*)',1),'')=''  --https://www.groupon.com/browse/chicago?category=home-improvement
            and  coalesce(REGEXP_EXTRACt(fullurl,'[\\?\\&]subcategory=([\\w-]*)',1),'')=''  --https://www.livingsocial.com/browse/ann-arbor?topcategory=travel&subcategory=beach-vacations-travel
            and  coalesce(REGEXP_EXTRACt(fullurl,'[\\?\\&]subcategory2=([\\w-]*)',1),'')=''
             and coalesce(REGEXP_EXTRACt(fullurl,'subcategory3=([\\w-]*)',1),'')=''
            and    coalesce(REGEXP_EXTRACt(fullurl,'\\.\\w{2,3}/(travel|goods)/([\\w-]*)',2),'') in ('','all')  --https://www.groupon.ca/goods/all
              )  --https://www.groupon.com/goods/for-the-home
            then 'L1'
            when coalesce(
            REGEXP_EXTRACt(fullurl,'goods/([\\w-]*)',1),
            REGEXP_EXTRACt(fullurl,'[\\?\\&]category=([\\w-]*)',1)) in (
            'home-improvement',
            'health-and-fitness',
            'beauty-and-spas',
            'personal-services',
            'things-to-do',
            'automotive',
            'retail',
            'food-and-drink',
            'for-the-home',
            'mens-clothing-shoes-and-accessories',
            'grocery-and-household',
            'jewelry-and-watches',
            'pet-supplies',
            'baby-kids-and-toys',
            'health-and-beauty',
            'auto-and-home-improvement',
            'womens-clothing-shoes-and-accessories',
            'sports-and-outdoors',
            'electronics',
            'entertainment-and-media',
            'toys',
            'hotels-and-accommodations'
            )
            and (coalesce(REGEXP_EXTRACt(fullurl,'subcategory=([\\w-]*)',1),'')='' and coalesce(REGEXP_EXTRACt(fullurl,'subcategory2=([\\w-]*)',1),'')=''  and coalesce(REGEXP_EXTRACt(fullurl,'subcategory3=([\\w-]*)',1),'')='' )  --exclude L3 and L4 categories https://www.groupon.com/browse/lansing?category=beauty-and-spas&subcategory=salons&subcategory2=v-hair-salons
            --OR REGEXP_EXTRACt(fullurl,'travel/([\\w-]*)/hotels',1)  is not null  --seo/getaways/city = https://www.groupon.com/travel/branson-mo/hotels
            then 'L2'
            when coalesce(REGEXP_EXTRACt(fullurl,'subcategory=([\\w-]*)',1),'') != ''  
               --or coalesce(REGEXP_EXTRACt(fullurl,'\\.\\w{2,3}/(travel|goods)/([\\w-]*)',2),'') != '' )  --this can be L3 or L4 https://www.groupon.com/goods/ NO
               and coalesce(REGEXP_EXTRACt(fullurl,'subcategory3=([\\w-]*)',1),'')='' 
               and coalesce(REGEXP_EXTRACt(fullurl,'subcategory2=([\\w-]*)',1),'')=''
            then 'L3'
            when coalesce(REGEXP_EXTRACt(fullurl,'subcategory2=([\\w-]*)',1),'') != '' and coalesce(REGEXP_EXTRACt(fullurl,'subcategory3=([\\w-]*)',1),'')=''   
            then 'L4'
            --subcategory3  https://www.groupon.com/browse/seattle?topcategory=goods&subcategory3=anal-plugs
            when coalesce(REGEXP_EXTRACt(fullurl,'subcategory3=([\\w-]*)',1),'') != ''  
            then 'L4'
            --L5
            end Levb
            -- get the last level of GRT
    , case 
            --L5
            when coalesce(REGEXP_EXTRACt(fullurl,'subcategory3=([\\w-]*)',1),'') != ''   then REGEXP_EXTRACt(fullurl,'subcategory3=([\\w-]*)',1)
            --  L4
            when coalesce(REGEXP_EXTRACt(fullurl,'subcategory2=([\\w-]*)',1),'')!='' then REGEXP_EXTRACt(fullurl,'subcategory2=([\\w-]*)',1)
            -- L3
            when coalesce(REGEXP_EXTRACt(fullurl,'subcategory2=([\\w-]*)',1),'')='' and coalesce(REGEXP_EXTRACt(fullurl,'subcategory=([\\w-]*)',1),'') != '' 
              then REGEXP_EXTRACt(fullurl,'subcategory=([\\w-]*)',1)
            when coalesce(REGEXP_EXTRACt(fullurl,'\\.\\w{2,3}/(travel|goods)/([\\w-]*)',2),'') not in  ('','all') 
              then coalesce(regexp_replace(REGEXP_EXTRACt(fullurl,'\\.\\w{2,3}/(travel|goods)/([\\w-]*)',2),'travel','getaways' ),'')
            --https://www.livingsocial.com/goods/baby-kids-and-toys?page=9
            --L2
            when 
            coalesce(
            REGEXP_EXTRACt(fullurl,'goods/([\\w-]*)',1),
            REGEXP_EXTRACt(fullurl,'[\\?\\&]category=([\\w-]*)',1))  in (
            'home-improvement',
            'health-and-fitness',
            'beauty-and-spas',
            'personal-services',
            'things-to-do',
            'automotive',
            'retail',
            'food-and-drink',
            'for-the-home',
            'mens-clothing-shoes-and-accessories',
            'grocery-and-household',
            'jewelry-and-watches',
            'pet-supplies',
            'baby-kids-and-toys',
            'health-and-beauty',
            'auto-and-home-improvement',
            'womens-clothing-shoes-and-accessories',
            'sports-and-outdoors',
            'electronics',
            'entertainment-and-media',
            'toys',
            'hotels-and-accommodations'
            ) then  coalesce(
            REGEXP_EXTRACt(fullurl,'goods/([\\w-]*)',1),
            REGEXP_EXTRACt(fullurl,'[\\?\\&]category=([\\w-]*)',1))
            --L1   --rename getaways to travel!!!!
            else coalesce(REGEXP_EXTRACt(fullurl,'context=([\\w-]*)',1) ,   --https://www.groupon.com/browse/chicago?context=local
             REGEXP_EXTRACt(fullurl,'topcategory=([\\w-]*)',1),  --https://www.groupon.com/browse/chicago?topcategory=goods&category=for-the-home
             --regexp_replace(REGEXP_EXTRACt(fullurl,'\\.\\w{2,3}/([\\w-]*)',1),'getaways','travel' ))   --remove if browse    https://www.groupon.ca/browse/cantonsdelest        
             REGEXP_EXTRACt(fullurl,'\\.\\w{2,3}/([\\w-]*)',1))
             --https://www.livingsocial.com/getaways?destination_id=cc05e11e-2ae2-4ad5-afdb-7e75d4edaaf8&destination_name=Reno, NV, USA&check_in=2020-12-30&check_out=2021-01-02&city_slug=reno
             END grt_perm
             --GRT1
             --https://www.groupon.com/browse/chicago?context=local
             --https://www.groupon.com/browse/chicago?topcategory=goods&category=for-the-home
             --https://www.livingsocial.com/getaways?destination_id=cc05e11e-2ae2-4ad5-afdb-7e75d4edaaf8&destination_na
             --https://www.livingsocial.com/goods/baby-kids-and-toys?page=9
             ,case when coalesce(REGEXP_EXTRACt(fullurl,'context=([\\w-]*)',1),'')='local' then 'local'
             when coalesce(REGEXP_EXTRACt(fullurl,'topcategory=([\\w-]*)',1) ,'')!='' then REGEXP_EXTRACt(fullurl,'topcategory=([\\w-]*)',1)
             when REGEXP_EXTRACt(fullurl,'\\.\\w{2,3}/([\\w-]*)',1) in ('goods','getaways') then  REGEXP_EXTRACt(fullurl,'\\.\\w{2,3}/([\\w-]*)',1)
             end grt_l1
             --channel if GRT1 is not correct
             --extract parameters:
        --     ,coalesce(REGEXP_EXTRACt(fullurl,'context=([\\w-]*)',1),'') url_context
        --     ,coalesce(REGEXP_EXTRACt(fullurl,'topcategory=([\\w-]*)',1) ,'') url_topcategory
        --     ,REGEXP_EXTRACt(fullurl,'\\.\\w{2,3}/([\\w-]*)',1) url_path
        --     ,coalesce(REGEXP_EXTRACt(fullurl,'[\\?\\&]category=([\\w-]*)',1),'') url_category
        --     ,coalesce(REGEXP_EXTRACt(fullurl,'subcategory=([\\w-]*)',1),'') url_subcategory
        --     ,coalesce(REGEXP_EXTRACt(fullurl,'subcategory2=([\\w-]*)',1),'') url_subcategory2
        --     ,REGEXP_EXTRACt(fullurl,'goods/([\\w-]*)',1) url_goods_subpath
             --end
             ,'' page_type
             ,fullurl fullrul2
from ai_reporting.sl_imp_clicks
where
cast(position as int ) between 0 and 19  and
--eventdate between '2021-11-10' and '2021-11-28'
cast( eventdate as date) between date_sub(current_date,2) and date_sub(current_date,1) --'2020-08-01' and '2020-08-11'
--and rawpagetype != 'deals/show'
) cc
--V3
left join category_ids cat
   on cat.permalink=  case when cc.grt_perm in ('all nearby deals','all local deals') then 'Local'
        when cc.grt_perm in ('all travel deals', 'hotels') then 'Getaways'
        when cc.grt_perm in ('all goods deals') then 'Goods'
        else cc.grt_perm
        end
   and friendly_name_singular is not null
   --on regexp_replace(lower(cat.friendly_name),'travel','getaways' ) =lower(cft.grt)
)
insert overwrite table grp_gdoop_bizops_db.aog_sl_bid_pagecategory partition (eventdate)
select clientplatform,level
, page_type_sl
,division
,SL
--,case when aogcampaignid is not null then 1 else 0 end SL
, cft_l1_cat_name
--,cft_l2_cat_name
--,cft_l3_cat_name
--,cft_l4_cat_name
--,cft_l5_cat_name
       ,dcat2.permalink  cft_2_perm
       ,dcat3.permalink  cft_3_perm
       ,dcat4.permalink cft_4_perm
       ,dcat5.permalink cft_5_perm
,coalesce(impr.permalink,cft_l1_cat_name) permalink
,grt_id
 ,count(case when position=0 then bcookie end) cookies1
 , count(case when position=0 then dv_time end) dv1
 ,count(case when position=0 then order_uuid end) orders1
 ,sum(case when position=0 then ogp else 0 end) ogp1
 , sum(case when position=0 then nob else 0 end) - sum(case when position=0 then nor else 0 end) merch_rev1
   ,count(case when position=1 then bcookie end) cookies2
 , count(case when position=1 then dv_time end) dv2
 ,count(case when position=1 then order_uuid end) orders2
 ,sum(case when position=1 then ogp else 0 end) ogp2
 , sum(case when position=1 then nob else 0 end) - sum(case when position=1 then nor else 0 end) merch_rev2
    ,count(case when position=2 then bcookie end) cookies3
 , count(case when position=2 then dv_time end) dv3
 ,count(case when position=2 then order_uuid end) orders3
 ,sum(case when position=2 then ogp else 0 end) ogp3
 , sum(case when position=2 then nob else 0 end) - sum(case when position=2 then nor else 0 end) merch_rev3
  ,count(case when position=3 then bcookie end) cookies4
 , count(case when position=3 then dv_time end) dv4
 ,count(case when position=3 then order_uuid end) orders4
 ,sum(case when position=3 then ogp else 0 end) ogp4
 , sum(case when position=3 then nob else 0 end) - sum(case when position=3 then nor else 0 end) merch_rev4
   ,count(case when position between 0 and  5 then bcookie end) cookies6
 , count(case when position between 0 and  5 then dv_time end) dv6
 ,count(case when position between 0 and  5 then order_uuid end) orders6
 ,sum(case when position between 0 and  5 then ogp else 0 end) ogp6
 , sum(case when position between 0 and 5 then nob else 0 end) - sum(case when position between 0 and  5 then nor else 0 end) merch_rev6
  ,count(case when position between 0 and  6 then bcookie end) cookies7
 , count(case when position between 0 and  6 then dv_time end) dv7
 ,count(case when position between 0 and  6 then order_uuid end) orders7
 ,sum(case when position between 0 and  6 then ogp else 0 end) ogp7
 , sum(case when position between 0 and 6 then nob else 0 end) - sum(case when position between 0 and  6 then nor else 0 end) merch_rev7
 ,count(case when position between 0 and  7 then bcookie end) cookies8
 , count(case when position between 0 and  7 then dv_time end) dv8
 ,count(case when position between 0 and  7 then order_uuid end) orders8
 ,sum(case when position between 0 and  7 then ogp else 0 end) ogp8
 , sum(case when position between 0 and  7 then nob else 0 end) - sum(case when position between 0 and  7 then nor else 0 end) merch_rev8
    ,count(case when position between 0 and  8 then bcookie end) cookies9
 , count(case when position between 0 and  8 then dv_time end) dv9
 ,count(case when position between 0 and  8 then order_uuid end) orders9
 ,sum(case when position between 0 and  8 then ogp else 0 end) ogp9
 , sum(case when position between 0 and  8 then nob else 0 end) - sum(case when position between 0 and  8 then nor else 0 end) merch_rev9
 ,count(case when position between 0 and  9 then bcookie end) cookies10
  ,count(case when position between 0 and  9 then dv_time end) dv10
 ,count(case when position between 0 and  9 then order_uuid end) orders10
 ,sum(case when position between 0 and  9 then ogp else 0 end) ogp10
 , sum(case when position between 0 and  9 then nob else 0 end) - sum(case when position between 0 and  9 then nor else 0 end) merch_rev10
  ,count(case when position between 1 and  9 then bcookie end) cookies10b
  ,count(case when position between 1 and  9 then dv_time end) dv10b
 ,count(case when position between 1 and  9 then order_uuid end) orders10b
 ,sum(case when position between 1 and  9 then ogp else 0 end) ogp10b
 , sum(case when position between 1 and  9 then nob else 0 end) - sum(case when position between 1 and  9 then nor else 0 end) merch_rev10b
  ,count(distinct dealuuid) deals
  ,count(distinct eventdate) days
  --new columns
  , count(distinct bcookie) cookies, avg(position) avg_positions
, avg(case when dv_time is not null then position end) avg_dv_pos
, max(position ) max_pos
,eventdate
from (select eventdate,clientplatform,pageid,aogcampaignid,dealuuid,page_type_sl, nob,nor,ogp,dv_time,order_uuid,position,bcookie,consumerid,division
    ,case when page_type_sl='search' and level is null then 'search' else level end LEVEL,SL
     ,search_query
    --concat(Grt...)
    ,lower(cft_l1_cat_name ) cft_l1_cat_name
   --,cft_l2_cat_name
   --,cft_l3_cat_name
   --,cft_l4_cat_name
   ,cft_l2_cat_id
      ,cft_l3_cat_id
   ,cft_l4_cat_id
   ,cft_l5_cat_id
   ,permalink
   ,grt_id
    from categories_app 
    --where dealuuid='69a9dffa-6d95-4d8e-9faa-3d40770273ab'
    union all
    select eventdate,clientplatform,pageid,aogcampaignid,dealuuid,case when level like 'L%' then 'browse' else level end page_type_sl, nob,nor,ogp,dv_time,order_uuid,position,bcookie,consumerid,division
    ,LEVEL,SL
    ,search_query
    ,lower(coalesce(cft_l1_cat_name ,grt_l1) ) cft_l1_cat_name
    --,cft_l2_cat_name
    --,cft_l3_cat_name
    --,cft_l4_cat_name
    ,cft_l2_cat_id
       ,cft_l3_cat_id
   ,cft_l4_cat_id
   ,cft_l5_cat_id
    ,permalink
    ,grt_id
    from categories
    --where dealuuid='69a9dffa-6d95-4d8e-9faa-3d40770273ab'
    ) impr
      left join prod_groupondw.dim_category dcat2
        on impr.cft_l2_cat_id=dcat2.category_id
      left join prod_groupondw.dim_category dcat3
        on impr.cft_l3_cat_id=dcat3.category_id
      left join prod_groupondw.dim_category dcat4
        on impr.cft_l4_cat_id=dcat4.category_id        
      left join prod_groupondw.dim_category dcat5
        on impr.cft_l5_cat_id=dcat5.category_id
group by eventdate,clientplatform,level
,page_type_sl
,division
,SL
,cft_l1_cat_name
,dcat2.permalink
,dcat3.permalink
,dcat4.permalink
,dcat5.permalink
,coalesce(impr.permalink,cft_l1_cat_name)
,grt_id
;


SET hive.execution.engine=tez;SET hive.exec.dynamic.partition.mode=nonstrict;SET hive.exec.dynamic.partition=true;SET hive.exec.max.dynamic.partitions.pernode=3000;SET hive.tez.java.opts=-Xmx6000M;SET hive.tez.container.size=8142;SET hive.exec.parallel=true;SET hive.vectorized.execution.enabled=true;SET hive.vectorized.execution.enabled=false;SET hive.input.format=org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;SET hive.enforce.sorting=true;SET hive.enforce.bucketing=true;SET hive.enforce.bucketmapjoin=true;SET hive.enforce.sortmergebucketmapjoin=true;SET hive.optimize.bucketmapjoin=true;SET hive.optimize.bucketmapjoin.sortedmerge=true;SET hive.auto.convert.sortmerge.join.bigtable.selection.policy=org.apache.hadoop.hive.ql.optimizer.TableSizeBasedBigTableSelectorForAutoSMJ;SET hive.optimize.ppd=true;SET hive.optimize.ppd.storage=true;SET hive.cbo.enable=true;SET hive.compute.query.using.stats=true;SET hive.stats.dbclass=fs;SET hive.stats.fetch.column.stats=true;SET hive.stats.fetch.partition.stats=true;SET hive.groupby.orderby.position.alias=true;SET hive.optimize.groupby=true;SET hive.auto.convert.join=true;SET hive.auto.convert.join.noconditionaltask.size=1000000000;SET hive.auto.convert.sortmerge.join=true;SET hive.auto.convert.join.noconditionaltask=true;SET hive.optimize.skewjoin=true;SET hive.optimize.skewjoin.compiletime=true;SET hive.skewjoin.mapjoin.map.tasks=10000;SET hive.skewjoin.key=10000;SET hive.exec.compress.output=true;SET mapred.input.compress=true;SET hive.exec.compress.intermediate=true;SET mapred.output.compress=true;SET mapred.map.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;SET mapreduce.output.fileoutputformat.compress=true;SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec;SET mapreduce.map.output.compress=true;SET mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec;SET mapred.compress.map.output=true;SET mapred.map.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;SET mapred.input.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;SET tez.am.container.reuse.enabled=true;SET tez.session.am.dag.submit.timeout.secs=900;SET tez.am.container.session.delay-allocation-millis=900000;SET hive.mapjoin.smalltable.filesize=1000000000;SET tez.runtime.shuffle.failed.check.since-last.completion=false;SET hive.resultset.use.unique.column.names=false;SET hive.optimize.sort.dynamic.partition=true;SET mapred.reduce.tasks=353;SET mapreduce.job.reduces=353;SET mapreduce.input.fileinputformat.split.minsize=1;SET mapreduce.input.fileinputformat.split.maxsize=1000000;SET tez.grouping.min-size=1;SET tez.grouping.max-size=1000000;insert overwrite table grp_gdoop_bizops_db.aog_sl_bid_pagecategory_etl
select 
eventdate
,cft_perma as category_permalink
,cft_l1_cat_perm
,case when level in ('L2','L3','L4','L5') then  cft_l2_cat_perm end L2
,case when level in ('L3','L4','L5') then cft_l3_cat_perm end L3
,case when level in ('L4','L5') then cft_l4_cat_perm end L4
,max(grt_id) as category_id
,sum(case when sl=0 then cookies1 else 0 end) imp
,sum(case when sl=0 then dv1 else 0 end) dv 
,sum(case when sl=0 then orders1 end) orders
,sum(case when sl=0 then ogp1 else 0 end) ogp
, sum(case when sl=0 then merch_rev1 else 0 end) merch_rev
---SL columns
,sum(case when sl=1 then cookies1 else 0 end) imp_sl
,sum(case when sl=1 then dv1 else 0 end) dv_sl
,sum(case when sl=1 then orders1 end) orders_sl
,sum(case when sl=1 then ogp1 else 0 end) ogp_sl
, sum(case when sl=1 then merch_rev1 else 0 end) merch_rev_sl
--other metrics
,sum(deals) deals
,sum(case when sl=1 then deals end) u_deals_sl
FROM grp_gdoop_bizops_db.aog_sl_bid_pagecategory 
WHERE --cft_l1_cat_name='goods' and cft_l2_cat_name = 'Baby & Kids' 
page_type_sl='browse'
and eventdate between date_sub(current_date,31) and date_sub(current_date,1)
--filter out SL
GROUP BY eventdate,cft_perma  
,cft_l1_cat_perm
,case when level in ('L2','L3','L4','L5') then  cft_l2_cat_perm end 
,case when level in ('L3','L4','L5') then cft_l3_cat_perm end 
,case when level in ('L4','L5') then cft_l4_cat_perm end

select 	fgt.order_date report_date,
		fgt.deal_uuid,
		fgt.bcookie,
    	pageapp, 
    	pageid, 
    	pageviewtype, 
    	fullurl, 
   		pagename,
		case when inv_service_id = 'tpis' then 'tpis'
	    else 'local' end as inv_service_type,
		st.grt_l2_cat_name ,
		case when sub_platform in ('web', 'desktop') then 'desktop'
		when sub_platform in ('iphone','ipad','android','touch') then lower(sub_platform)
		else 'other' end platform ,
		deal_state,
		udv ,
		ubbc ,
	    ucv ,
	    urv
from
(select bcookie,
	dealuuid,
    pageapp, 
    pageid, 
    pageviewtype, 
    fullurl, 
    pagename,
    platform,
    eventdate
from grp_gdoop_pde.junohourly
where eventdate between '2022-03-01' and '2022-04-30'
and platform in ('web')
and lower(country) in ('us')
and lower(event) in ('genericpageview')
and pageapp like '%checkout%'
and bcookie is not null
and dealuuid is not null
and bcookie <> '' ) juno
join 
(select distinct deal_uuid,
		bcookie, 
		inv_service_id,
		order_date
from edwprod.fact_gbl_transactions fgt
left join grp_gdoop_bizops_db.bzops_booking_deals bbd 
on fgt.deal_uuid = bbd.product_uuid  and inv_service_id = 'tpis'
where order_date between '2022-03-01' and '2022-04-30') fgt
on juno.dealuuid = fgt.deal_uuid and juno.bcookie = fgt.bcookie
join
(select distinct deal_id, 
		report_date,
		grt_l2_cat_name,
		deal_state,
		sub_platform,
		sum(deal_views) udv, 
		sum(buy_btn_clicks) ubbc, 
		sum(conf_page_views) ucv, 
		sum(receipt_page_views) urv 
from edwprod.agg_gbl_traffic_fin_deal gbt
join grp_gdoop_sup_analytics_db.adhoc_deal_redemption_location_bucketed st
on gbt.deal_id = st.deal_uuid 
where grt_l2_cat_name in (
'L2 - Things to Do - Leisure',
'L2 - Things to Do - Live', 
'L2 - Health / Beauty / Wellness')
and gbt.country_code = 'US'
and deal_state in ('CA', 'TX')
and report_date between '2022-03-01' and '2022-04-30'
group by 1,2,3,4,5) st
on fgt.deal_uuid = st.deal_id;


select *
from grp_gdoop_pde.janus_all 
where experimentName='relevance-sl-personalisation-cc' and ds >= '2022-05-19';

select 
    eventdate,
    clientplatform, 
    variant_name,
    count(distinct bcookie) unique_imps,
    count(distinct consumerid) unique_imps2
from 
grp_gdoop_bizops_db.np_pers_test_imp
group by eventdate, clientplatform, variant_name



select bcookie, eventdate, clientplatform, extrainfo, rawevent, consumerid
    from grp_gdoop_pde.junoHourly 
    where eventdate >= '2022-04-28'
    and lower(eventdestination) = 'genericimpression'
    and lower(event) = 'genericimpression'
    and country in ('US','CA')
    and platform in ('web', 'desktop')
    and consumerid is not null
    and lower(brand) = 'groupon' 


 
select 
     user_browser_id,
     dt, 
     case when platform = 'desktop' then 'web' else platform end platform,
     event_name
from prod_groupondw.bld_widget_contents 
where lower(platform) in ('desktop', 'touch')
and dt >= '2022-05-19'
and user_browser_id <> '' 
and user_browser_id is not null
and lower(page_country) IN ('us', 'ca')
and lower(page_hostname) like '%groupon%'
and lower(page_type) like '%homepage%'
and lower(widget_content_name) like '%crosschannel_homepage_sponsored_carousel%'
and lower(widget_content_type) = 'compound';
    
select 
   substr(eventdate, 6,2) month_of,
   count(distinct eventdate)
from grp_gdoop_bizops_db.np_impressions_sl_app 
where eventdate >= '2021-01-01' and eventdate < '2022-01-01'
group by substr(eventdate, 6,2)
order by substr(eventdate, 6,2);

select 
   substr(eventdate, 6,2) month_of,
   count(distinct eventdate)
from grp_gdoop_bizops_db.np_impressions_sl
where eventdate >= '2021-01-01' and eventdate < '2022-01-01'
group by substr(eventdate, 6,2)
order by substr(eventdate, 6,2);
select 
   distinct 
   substr(eventdate, 6,2) month_of,
   eventdate
from grp_gdoop_bizops_db.np_impressions_sl_app
where eventdate >= '2021-01-01' and eventdate < '2022-01-01'
order by eventdate
;

select bcookie, eventdate, clientplatform, extrainfo, rawevent, consumerid
    from grp_gdoop_pde.junoHourly 
    where eventdate >= '2022-05-19'
    and lower(trim(regexp_replace(clientplatform,'\\t|\\n|\\r|\\u0001', ''))) in ('iphone', 'android')
    and lower(eventdestination) = 'genericimpression'
    and lower(event) = 'genericimpression'
    and country in ('US','CA')
    and platform = 'mobile'
    and bcookie is not null 
    and bcookie <> ''
    and lower(trim(bcookie)) <> 'null'
    and lower(brand) = 'groupon' 
    and bcookie not in ('android_automation_test' ,'ios_automation_test','android_perf_test_nightly','ios_perf_test_nightly')
    and case when 'userAgent' like '%bot%' or useragent like '%crawler%' or useragent like '%search%' or useragent like '%spider%' or useragent like '%spyder%' then 1 else 0 end = 0
    and lower(trim(extrainfo)) like '%crosschannel_homepage_sponsored_carousel%' 
    and lower(impressiontype) = 'collection_card_impression';

insert overwrite table grp_gdoop_bizops_db.np_impressions_sl partition (eventdate)
   select 
      case when cl.aogcampaignid is not null then 'Sponsored' else 'Organic' end deal_landing,
      trim(lower(regexp_replace(search_query,'\\+',' '))) search_query,
      position,
      ogp,
      nor,
      nob,
      dv_time,
      bcookie,
      order_uuid,
      cl.dealuuid,
      clientplatform,
      aogcampaignid,
      '' extrainfo,
      case when rawpagetype = 'browse/deals/index' and fullurl like '%context=local%' or fullurl like '%category=%' then 'browse'
           when rawpagetype in ('browse/deals/index') and search_query is not null  and search_query!='' then 'search'
           when rawpagetype in ('homepage' ,'homepage/index', 'featured/deals/index') then 'homepage'
           when rawpagetype in ( 'browse/deals/index') then 'browse' --'featured'
           when rawpagetype in ('nearby/deals/index', --featured? or local only 
                                 'goods/browse/index',
                                 'goods/index',
                                 'giftshop/deals/show',
                                 'giftshop/deals/index',
                                  'channels/show',
                                  'beautynow_promoted',
                                  'beautynow_salon',
                                  'beautynow_appointment_receipt',
                                  'beautynow_SELECT_appointment_time',
                                  'beautynow_SELECT_service') then 'browse'
           when rawpagetype like '%-%-%-%' then 'occasions'
           else rawpagetype
           end page,
        cl.eventdate
     from ai_reporting.sl_imp_clicks cl 
     join (select dealuuid, min(eventdate) min_eventdate, max(eventdate) max_eventdate 
           from grp_gdoop_bizops_db.np_sl_all_deals
           group by dealuuid
           ) dl on cl.dealuuid = dl.dealuuid 
     where 
        cl.eventdate in (
        '2021-06-26', '2021-08-15', '2021-08-23', '2021-09-08', '2021-09-27', '2021-09-29', '2021-10-02', '2021-11-24', '2021-12-24', '2021-12-28'
        )

select max(eventdate)
from grp_gdoop_bizops_db.np_impressions_sl_app
where eventdate >= '2022-06-12'


        
        
        
select 
   cast(case when clientplatform in ('iPad', 'iPhone') then 'iOS' else clientplatform end as varchar(80)) clientplatform,
   page,
   b.l2, 
   case when clientplatform in ('iPad', 'iPhone') and position in ('-1', '0') then '0' 
        when clientplatform = 'Android' then cast(cast(position as integer) + 1 as varchar)
        else position end positions,
   sum(ogp) total_ogp,
   sum(nor) total_nor,
   sum(nob) total_nob,
   count(1) total_impressions,
   sum(case when deal_landing = 'Sponsored' then ogp end) total_ogp_sl,
   sum(case when deal_landing = 'Sponsored' then nor end) total_nor_sl,
   sum(case when deal_landing = 'Sponsored' then nob end) total_nob_sl,
   sum(case when deal_landing = 'Organic' then ogp end) total_ogp_org,
   sum(case when deal_landing = 'Organic' then nor end) total_nor_org, 
   sum(case when deal_landing = 'Organic' then nob end) total_nob_org,
   count(case when deal_landing = 'Sponsored' then 1 end) total_impressions_sl,
   count(case when deal_landing = 'Organic' then 1 end) total_impressions_org
from grp_gdoop_bizops_db.np_impressions_sl_app as a 
left join grp_gdoop_bizops_db.pai_deals as b on a.dealuuid = b.deal_uuid
where eventdate >= '2022-06-12' and eventdate < '2022-08-14' and page in ('homepage', 'browse', 'search') and 
     cast(case when clientplatform in ('iPad', 'iPhone') and position in ('-1', '0') then '0' 
        when clientplatform = 'Android' then cast(cast(position as integer) + 1 as varchar)
        else position end as integer) < 11
group by 
   cast(case when clientplatform in ('iPad', 'iPhone') then 'iOS' else clientplatform end as varchar(80)),
   page,
   b.l2, 
   case when clientplatform in ('iPad', 'iPhone') and position in ('-1', '0') then '0' 
        when clientplatform = 'Android' then cast(cast(position as integer) + 1 as varchar)
        else position end
union all 
select 
   'web and touch' as clientplatform,
   page,
   b.l2, 
   position,
   sum(ogp) total_ogp, 
   sum(nor) total_nor, 
   sum(nob) total_nob, 
   count(1) total_impressions,
   sum(case when deal_landing = 'Sponsored' then ogp end) total_ogp_sl, 
   sum(case when deal_landing = 'Sponsored' then nor end) total_nor_sl, 
   sum(case when deal_landing = 'Sponsored' then nob end) total_nob_sl,
   sum(case when deal_landing = 'Organic' then ogp end) total_ogp_org, 
   sum(case when deal_landing = 'Organic' then nor end) total_nor_org, 
   sum(case when deal_landing = 'Organic' then nob end) total_nob_org,
   count(case when deal_landing = 'Sponsored' then 1 end) total_impressions_sl,
   count(case when deal_landing = 'Organic' then 1 end) total_impressions_org
from grp_gdoop_bizops_db.np_impressions_sl as a 
left join grp_gdoop_bizops_db.pai_deals as b on a.dealuuid = b.deal_uuid
where eventdate >= '2022-06-12' and eventdate < '2022-08-14' and page in ('homepage', 'browse', 'search') and cast(position as integer)< 11
group by 
   page,
   b.l2, 
   position
;
  

create table 
select
    bcookie,
    consumerid, 
    merchantid, 
    consumeridsource,
    rawpagetype,
    min(eventtime) eventtime,
    eventdate
from
    grp_gdoop_pde.junoHourly
where
    eventdate >= '2022-03-23'
    and clientplatform in ('web','Touch')
    and eventDestination = 'other'
    and event = 'merchantPageView'
    and country = 'US'
    and pageapp = 'sponsored-campaign-itier'
    and merchantid is not null
group by consumerid, merchantid, consumeridsource, rawpagetype, eventdate, bcookie;


select count(1) from grp_gdoop_bizops_db.np_ss_sl_user_granular where eventdate >= '2022-03-22';
  
  sum(case when deal_landing = 'Sponsored' then nob - nor end) deal_rev, 
        sum(case when deal_landing = 'Sponsored' and redeem_date <= cast(eventdate as date) then (nob-nor) when deal_landing = 'Sponsored' and (nob-nor) is not null then 0 end) red_same_day, 
        sum(case when deal_landing = 'Sponsored' and redeem_date <= date_add(cast(eventdate as date), 30) then (nob-nor) when deal_landing = 'Sponsored' and (nob-nor) is not null then 0 end) red_30,
        sum(case when deal_landing = 'Sponsored' and redeem_date <= date_add(cast(eventdate as date), 120) then (nob-nor) when deal_landing = 'Sponsored' and (nob-nor) is not null then 0 end) red_120, -----redemption 120 days doesn't work when updating oly last 30 days. 
        sum(case when deal_landing = 'Sponsored' and reds.order_uuid is not null then nob-nor when (nob-nor) is not null then 0 end) all_reds,
        count(case when deal_landing = 'Sponsored' then 1 end) total_impressions, 
        count(distinct case when deal_landing = 'Sponsored' then aogcampaignid end) total_impression_aog,
        count(distinct case when deal_landing = 'Sponsored' then dv_time end) total_clicks, 
        count(distinct case when deal_landing = 'Sponsored' then sli.order_uuid end) orders_sold, 
        sum(case when deal_landing = 'Organic' then nob - nor end) deal_rev_org, 
        sum(case when deal_landing = 'Organic' and redeem_date <= cast(eventdate as date) then (nob-nor) when deal_landing = 'Organic' and (nob-nor) is not null then 0 end) red_same_day_org, 
        sum(case when deal_landing = 'Organic' and redeem_date <= date_add(cast(eventdate as date), 30) then (nob-nor) when deal_landing = 'Organic' and (nob-nor) is not null then 0 end) red_30_org,
        sum(case when deal_landing = 'Organic' and redeem_date <= date_add(cast(eventdate as date), 120) then (nob-nor) when deal_landing = 'Organic' and (nob-nor) is not null then 0 end) red_120_org,
        sum(case when deal_landing = 'Organic' and reds.order_uuid is not null then nob-nor when deal_landing = 'Organic' and (nob-nor) is not null then 0 end) all_reds_org,
        count(case when deal_landing = 'Organic' then 1 end) total_impressions_org, 
        count(distinct case when deal_landing = 'Organic' then aogcampaignid end) total_impression_aog_org,
        count(distinct case when deal_landing = 'Organic' then dv_time end) total_clicks_org, 
        count(distinct case when deal_landing = 'Organic' then sli.order_uuid end) orders_sold_org
  
select * from grp_gdoop_bizops_db.pai_deals;

select * from grp_gdoop_bizops_db.np_impressions_sl_app;

select  eventdate, clientplatform, count(case when position = '-1' then 1 end) p1_count, count(case when position = '0' then 1 end) p2_count 
from grp_gdoop_bizops_db.np_impressions_sl_app 
where cast(position as integer) <= 0 and eventdate >= '2021-07-01'
group by eventdate,clientplatform
order by eventdate desc,clientplatform;

select eventdate, clientplatform, count(case when position = '-1' then 1 end) p1_count, count(case when position = '0' then 1 end) p2_count 
from ai_reporting.sl_imp_clicks_app
where cast(position as integer) <= 0 and eventdate >= '2021-07-01'
group by eventdate,clientplatform
order by eventdate desc,clientplatform;



select * from grp_gdoop_bizops_db.np_opt_cons_copy

select * from grp_gdoop_optimize_db.impressions where experiment_name = 'relevance-sl-personalisation-cc' and ds >= '2022-04-28';
select * from prod_groupondw.user_bcookie_mapping where country_code in ('US', 'CA')

select * from grp_gdoop_bizops_db.np_pers_test_imp3;

select * from grp_gdoop_pde.janus_all 
where experimentName='relevance-sl-personalisation-cc' 
and ds >= '2022-04-28' 
and  consumerid = 'd37c1b70-e3e9-11e6-b7c6-002590980910';

select 
    *
from 
(select consumerid, count(distinct variantname) xyz from grp_gdoop_bizops_db.np_opt_cons_copy group by consumerid) as fin 
where xyz > 1;

select dt, 
       platform, 
       count(concat(bcookie, dt)) as page_views, 
       count(distinct concat(bcookie, dt)) as unique_page_views
from prod_groupondw.bld_events
where ds >= '2022-04-28'
and lower(platform) in ('desktop', 'touch')
and lower(page_country) in ('us')
and lower(event) in ('pageview')
and lower(trim(page_type)) like '%homepage/index%'
and lower(page_hostname) like '%groupon%'
and bcookie is not null
and bcookie <> ''
and lower(trim(bcookie)) <> 'null'
and bot_flag = '0'
and internal_ip_ind = '0'
;



with category_ids as (
--grt    grt_id    cft_l1_cat_name    cft_l2_cat_name    cft_l3_cat_name    cft_l4_cat_name    cft_l5_cat_name    cft_l1_cat_id    cft_l2_cat_id    cft_l3_cat_id    cft_l4_cat_id    cft_l5_cat_id    lev    permalink    friendly_name_singular
--Baby & Kids    f09218a4-3c84-475e-842a-6260627398d8    Local    Retail    Personalized Items    Baby & Kids    [NULL]    c09790ba-a6b9-40fc-ad81-4cdf25260b5e    c60d65f4-6015-4ed8-b847-d02fd70b5f14    1f3a71fa-3e51-47f5-bc5b-6ee5b6132a31    f09218a4-3c84-475e-842a-6260627398d8    [NULL]    L4    baby-kids    Baby & Kid
--Baby & Kids    9070f5c4-bd1f-40e5-b680-60f15097aa9d    Goods    Baby & Kids    [NULL]    [NULL]    [NULL]    db2cb956-fc1a-4d8c-88f2-66657ac41c24    9070f5c4-bd1f-40e5-b680-60f15097aa9d    [NULL]    [NULL]    [NULL]    L2    baby-kids-and-toys    Baby & Kid
--there are duplicates names on "grt" column, but the grt_id is unique
--add row counter when GRT are repeated, so we can select only the first one
select gr.*,dcat.permalink,dcat.friendly_name_singular 
,row_number() over (partition by grt order by lev) rownum
from (
select dcr1.cft_l1_cat_name grt
      ,cft_l1_cat_id grt_id
      ,dcr1.cft_l1_cat_name 
      ,null cft_l2_cat_name
      ,null  cft_l3_cat_name
      ,null cft_l4_cat_name
       ,null cft_l5_cat_name
       ,cft_l1_cat_id
       ,null cft_l2_cat_id
       ,null cft_l3_cat_id
       ,null cft_l4_cat_id
       ,null cft_l5_cat_id
       , 'L1' lev
      from dw.v_dim_pds_cft_map dcr1 
      where cft_l2_cat_name  is not null
      group by dcr1.cft_l1_cat_name 
       ,cft_l1_cat_id 
union all
select dcr1.cft_l2_cat_name grt
      ,cft_l2_cat_id grt_id
      ,dcr1.cft_l1_cat_name 
      ,dcr1.cft_l2_cat_name
      ,null  cft_l3_cat_name
      ,null cft_l4_cat_name
       ,null cft_l5_cat_name
       ,cft_l1_cat_id
       ,cft_l2_cat_id
       ,null cft_l3_cat_id
       ,null cft_l4_cat_id
       ,null cft_l5_cat_id
             , 'L2' lev
      from dw.v_dim_pds_cft_map dcr1 
      where cft_l2_cat_name  is not null
      group by dcr1.cft_l1_cat_name  ,dcr1.cft_l2_cat_name
       ,cft_l1_cat_id ,cft_l2_cat_id
union all
select dcr1.cft_l3_cat_name grt
      ,cft_l3_cat_id grt_id
      ,dcr1.cft_l1_cat_name 
      ,dcr1.cft_l2_cat_name
      ,cft_l3_cat_name
      ,null cft_l4_cat_name
       ,null cft_l5_cat_name
       ,cft_l1_cat_id
       ,cft_l2_cat_id
       ,cft_l3_cat_id
       ,null cft_l4_cat_id
       ,null cft_l5_cat_id
             , 'L3' lev
      from dw.v_dim_pds_cft_map dcr1 
      where cft_l3_cat_name  is not null
      group by dcr1.cft_l1_cat_name ,dcr1.cft_l2_cat_name, cft_l3_cat_name
       ,cft_l1_cat_id ,cft_l2_cat_id ,cft_l3_cat_id 
union all
select dcr1.cft_l4_cat_name grt
      ,cft_l4_cat_id grt_id
      ,dcr1.cft_l1_cat_name 
      ,dcr1.cft_l2_cat_name
      ,cft_l3_cat_name
      ,cft_l4_cat_name
       ,null cft_l5_cat_name
       ,cft_l1_cat_id
       ,cft_l2_cat_id
       ,cft_l3_cat_id
       ,cft_l4_cat_id
       ,null cft_l5_cat_id
             , 'L4' lev
      from dw.v_dim_pds_cft_map dcr1 
      where cft_l4_cat_name  is not null
      group by dcr1.cft_l1_cat_name ,dcr1.cft_l2_cat_name, cft_l3_cat_name, cft_l4_cat_name
       ,cft_l1_cat_id ,cft_l2_cat_id ,cft_l3_cat_id ,cft_l4_cat_id 
union all
select dcr1.cft_l5_cat_name grt
      ,cft_l5_cat_id grt_id
      ,dcr1.cft_l1_cat_name 
      ,dcr1.cft_l2_cat_name
      ,cft_l3_cat_name
      ,cft_l4_cat_name
       , cft_l5_cat_name
       ,cft_l1_cat_id
       ,cft_l2_cat_id
       ,cft_l3_cat_id
       ,cft_l4_cat_id
       , cft_l5_cat_id
             , 'L5' lev
      from dw.v_dim_pds_cft_map dcr1 
      where cft_l5_cat_name  is not null
      group by dcr1.cft_l1_cat_name ,dcr1.cft_l2_cat_name, cft_l3_cat_name, cft_l4_cat_name, cft_l5_cat_name
       ,cft_l1_cat_id ,cft_l2_cat_id ,cft_l3_cat_id ,cft_l4_cat_id ,cft_l5_cat_id  
) gr
left join prod_groupondw.dim_category dcat 
  on dcat.category_id = gr.grt_id
)
, categories_app as (
 select 
case
 when get_json_object(lower(extrainfo), '$.type') is not null then 'browse'  --get_json_object(lower(extrainfo), '$.type')
 when search_query is not null and   search_query!='' and    search_query!='All+Deals'  and search_query != ' '
      and search_query != '{"search":""}' then 'search' --and  search_query not in ('hotel')
 when trim(lower(translate(search_query,'+',' '))) in ('all deals') or rawpagetype='Featured' then 'featured'
 when search_query in ('hotel') then 'browse'
 when (rawpagetype = 'home_tab' or rawpagetype ='GlobalSearchResult') 
     --and get_json_object(lower(extrainfo), '$.tabName') = 'home_tab'
     and (search_query is null or  search_query='')
     and coalesce(cat2.lev, cat.lev) is null 
     and get_json_object(lower(extrainfo), '$.type') is null 
     and channel = 'all' then 'homepage' 
 when  get_json_object(lower(extrainfo), '$.tabName') = 'home_tab' then 'homepage'
 when get_json_object(lower(extrainfo), '$.wolfhoundpageid') is not null then 'occasions'
when get_json_object(lower(extrainfo), '$.tabname') = 'notifications_tab' then 'notifications_tab'
when rawpagetype in ( 'GlobalSearchResult', 'MapLedSearch')  or (rawpagetype = 'Homepage' and channel = 'local' ) then 'browse'
when rawpagetype = 'wolfhound_mobile_page' then 'occasions'
else 'other' --rawpagetype 
 end page_type_SL
 --Category_pagetype 
   ,case when case when lower(channel)='global_search' then coalesce(prev_clickinfo.col1,channel) else channel end in ('search_freetext','autocomplete_selection_click','recent_search_click') or (search_query is not null 
    and   search_query!='' and    search_query!='All+Deals'  and search_query != ' ' and search_query != '{"search":""}')  then 'search'
   when coalesce(prev_clickinfo.col1,channel)  in ('in_app_message') then 'Mobile Search - In App Message'
   when coalesce(prev_clickinfo.col1,channel)  in ('push_notification_alert','notification_action_click', 'notification') then 'Push'
   --push prev_group_grp in ('GRP9') and prev_channel in('notification','notifications_tab_click')    only the first page comes from push, if click happens ignore it
   --prev_clickinfo is null  or like notification_tab_click      and lower(channel) = 'global_search'           (prev_clickinfo is null or prev_clickinfo.col1 in ('notification','notifications_tab_click')  )  and
   when ref_Attr_class_key in ('41','29')  and lower(channel) = 'global_search' then 'Push'
   when prev_clickinfo.col1 in ('nav_card_click','sticky_nav_card_click') and (prev_clickinfo.col2 like '%Play%' or prev_clickinfo.col2 like '%ThingsToDo%') then 'Local Browse / Nearby'
when  prev_clickinfo.col1 in ('nav_card_click','sticky_nav_card_click') and  prev_clickinfo.col2 like '%Goods%' then 'Goods Browse / Tab'
when  prev_clickinfo.col1 in ('nav_card_click','sticky_nav_card_click') and  (prev_clickinfo.col2 like '%Pampered%' or prev_clickinfo.col2 like '%BeautyandSpas%') then 'Local Browse / Nearby'
when  prev_clickinfo.col1 in ('nav_card_click','sticky_nav_card_click') and  (prev_clickinfo.col2 like '%Eat%' or prev_clickinfo.col2 like '%Restaurants%') then 'Local Browse / Nearby'
when  prev_clickinfo.col1 in ('nav_card_click','sticky_nav_card_click') and  prev_clickinfo.col2 like '%Travel%' then 'Getaways Page / Tab'
when  prev_clickinfo.col1 in ('nav_card_click','sticky_nav_card_click') and prev_clickinfo.col2 like '%Occasion%' then 'Occasions Page / Tab'
when  prev_clickinfo.col1 in ('nav_card_click','sticky_nav_card_click') and prev_clickinfo.col2 like '%Health%' then 'Local Browse / Nearby'
when  prev_clickinfo.col1 in ('nav_card_click','sticky_nav_card_click')  then 'Local Browse / Nearby'
when case when lower(channel)='global_search' then coalesce(prev_clickinfo.col1,channel) else channel end in ('band_card_refinement_click','category_click','category_selection_click','sub-category_click','popular_category_click', 'category_card_click') then 'Mobile Search - Categories'
when lower(channel) in ('all','featured') then 'Homepage'
when lower(channel) = 'global_search' then 'Mobile Search - Other'
when channel in ('search_tab_click', 'notifications_tab_click', 'mystuff_tab_click', 'saved_deal_tab_click', 'saved_deals_tab_click','categories_tab_click') then channel
else 'Other'
END page_type
 ,case when aogcampaignid is not null then 1 else 0 end SL
 --,get_json_object(lower(extrainfo), '$.type')  category
  , case
        when lower(prev_clickinfo.col1) in ('search_freetext','autocomplete_selection_click','recent_search_click') then 'Search'
        when lower(prev_clickinfo.col1) in ('in_app_message', 'push_notification_alert') then 'Mobile Search - In App Message'
        WHEN lower(prev_clickinfo.col1) IN ('nav_card_click','sticky_nav_card_click') 
            AND (lower(prev_clickinfo.col2) LIKE '%beautynow%' 
            OR lower(prev_clickinfo.col2) = 'beauty now' 
            OR lower(prev_clickinfo.col2) = 'bn') 
            THEN 'BeautyNow Page / Tab'
        WHEN lower(prev_clickinfo.col1) IN ('nav_card_click','sticky_nav_card_click') 
            AND lower(prev_clickinfo.col2) LIKE '%grouponplus%' 
            THEN 'Groupon+'
        WHEN lower(prev_clickinfo.col1) IN ('nav_card_click','sticky_nav_card_click') 
            AND lower(prev_clickinfo.col2) LIKE '%more%' 
            THEN 'More'
        WHEN lower(prev_clickinfo.col1) IN ('nav_card_click','sticky_nav_card_click') 
            AND (lower(prev_clickinfo.col2) LIKE '%occasion%'
            OR lower(prev_clickinfo.col2) LIKE '%bestofgroupon%') 
            THEN 'Occasions Page / Tab'
        WHEN lower(prev_clickinfo.col1) IN ('nav_card_click','sticky_nav_card_click') 
            AND (lower(prev_clickinfo.col2) LIKE '%restaurants%' 
            OR lower(prev_clickinfo.col2) LIKE '%beautyandspas%' 
            OR lower(prev_clickinfo.col2) LIKE '%thingstodo%' 
            OR lower(prev_clickinfo.col2) LIKE '%automotive%' 
            OR lower(prev_clickinfo.col2) LIKE '%play%' 
            OR lower(prev_clickinfo.col2) LIKE '%pampered%' 
            OR lower(prev_clickinfo.col2) LIKE '%eat%'  
            OR lower(prev_clickinfo.col2) LIKE '%health%') 
            THEN 'Local Browse / Nearby'
        WHEN lower(prev_clickinfo.col1) IN ('nav_card_click','sticky_nav_card_click') 
        AND  (lower(prev_clickinfo.col2) LIKE '%good%' 
        OR lower(prev_clickinfo.col2) LIKE '%shop%') 
        THEN 'Goods Browse / Tab'
        WHEN lower(prev_clickinfo.col1) IN ('nav_card_click','sticky_nav_card_click') 
            AND  (lower(prev_clickinfo.col2) LIKE '%travel%' OR lower(prev_clickinfo.col2) LIKE '%escapes%') 
            THEN 'Getaways Page / Tab'
        when lower(prev_clickinfo.col1) in ('band_card_refinement_click','category_click','category_selection_click','filter_select_click','sub-category_click','popular_category_click') 
            then 'Mobile Search - Categories' 
   end click_page_type
 ,case when coalesce(cat2.lev, cat.lev) is not null then coalesce(cat2.lev, cat.lev)
   when trim(lower(translate(search_query,'+',' '))) in ('all deals') or rawpagetype='Featured' then 'featured' 
    when rawpagetype = 'home_tab' and search_query is null and coalesce(cat2.lev, cat.lev) is null and get_json_object(lower(extrainfo), '$.type') is null 
     and channel = 'all' then 'homepage' 
 end level
 --,count(*),count(distinct bcookie)
 --,min(extrainfo),max(extrainfo)
 ,coalesce(cat2.grt_id, cat.grt_id) grt_id
 ,coalesce(cat2.cft_l1_cat_name, cat.cft_l1_cat_name) cft_l1_cat_name
 ,coalesce(cat2.cft_l2_cat_name, cat.cft_l2_cat_name) cft_l2_cat_name
 ,coalesce(cat2.cft_l3_cat_name, cat.cft_l3_cat_name) cft_l3_cat_name
 ,coalesce(cat2.cft_l4_cat_name, cat.cft_l4_cat_name) cft_l4_cat_name
  ,coalesce(cat2.cft_l5_cat_name, cat.cft_l5_cat_name) cft_l5_cat_name
  ,coalesce(cat2.cft_l1_cat_id, cat.cft_l1_cat_id) cft_l1_cat_id
 ,coalesce(cat2.cft_l2_cat_id, cat.cft_l2_cat_id) cft_l2_cat_id
 ,coalesce(cat2.cft_l3_cat_id, cat.cft_l3_cat_id) cft_l3_cat_id
 ,coalesce(cat2.cft_l4_cat_id, cat.cft_l4_cat_id) cft_l4_cat_id
  ,coalesce(cat2.cft_l5_cat_id, cat.cft_l5_cat_id) cft_l5_cat_id
 ,coalesce(cat2.permalink, cat.permalink) permalink
 ,imp.bcookie
  ,imp.dv_time 
 ,imp.order_uuid
 ,imp.ogp 
 ,imp.nob
,imp.nor
  ,imp.dealuuid
  ,eventdate
  ,clientplatform
  ,grt1,grt2,grt3,grt4
 ,cast(position as int )-1 position
 ,division
 ,consumerid
 ,aogcampaignid
  ,search_query
  ,rawpagetype
  ,coalesce(get_json_object(extrainfo, '$.attributionId')
  , concat(get_json_object(extrainfo, '$.collectionCardUUID'), bcookie)
  ,concat(get_json_object(extrainfo, '$.tabName'),bcookie) ) pageid 
    from ai_reporting.sl_imp_clicks_app imp
     left join  category_ids cat
        --on cat.grt_id = split_part(get_json_object(lower(prev_clickinfo.col2), '$.search_result.query'),',',2) --on iPhone there is the category_id inside the previos click with the category name
       on ( lower(cat.grt) = lower(case 
               when regexp_replace(get_json_object(lower(extrainfo), '$.type'),'travel','getaways' )in ('all nearby deals','all local deals') then 'Local'
            when regexp_replace(get_json_object(lower(extrainfo), '$.type'),'travel','getaways' )in ('all travel deals', 'hotels') then 'Getaways'
            when regexp_replace(get_json_object(lower(extrainfo), '$.type'),'travel','getaways' ) in ('all goods deals') then 'Goods'
            else regexp_replace(get_json_object(lower(extrainfo), '$.type'),'travel','getaways' )
        end)  
           and cat.rownum=1) --this will generate duplicates, use a rejoin with the table aggregated at GRT level
     left join  category_ids cat2
         on cat2.grt_id = split(get_json_object(lower(prev_clickinfo.col2), '$.search_result.query'),',')[1] --on iPhone there is the category_id inside the previos click with the category name
--or get_json_object(lower(extrainfo), '$.type') = lower(cft.grt2) 
--or get_json_object(lower(extrainfo), '$.type') = lower(cft.grt4) 
     where 
     cast(position as int ) between 1 and 20 and
    --eventdate between '2021-11-10' and '2021-11-28'
    cast( eventdate as date) between date_sub(current_date,2) and date_sub(current_date,1) --'2020-08-01' and '2020-08-11' 
) , categories as (
select 
cc.*
,cat.*
,coalesce(cat.lev,cc.levb) level
,coalesce(cat.cft_l1_cat_name ,cc.grt_l1) cft_l1_cat_name2
from (
    select  bcookie
  ,dv_time 
 ,order_uuid
 ,ogp 
 ,nob
,nor
  ,dealuuid
  ,eventdate
  ,clientplatform
  ,grt1,grt2,grt3,grt4
 ,cast(position as int )  position
 ,division
 ,consumerid
 ,aogcampaignid
 ,rawpagetype
 ,pageid
  ,search_query
   ,case when aogcampaignid is not null then 1 else 0 end SL
    , case --apgetype = browse
            when search_query is not null and   search_query!='' and    search_query!='All+Deals'  and search_query != ' '
                 and search_query != '{"search":""}' then 'search'
            when REGEXP_EXTRACt(fullurl,'\\.\\w{2,3}/([\\w-]*)',1) ='' then 'homepage'
            when coalesce(REGEXP_EXTRACt(fullurl,'\\.\\w{2,3}/browse/([\\w-]*)',1),'')!='' 
             and coalesce(REGEXP_EXTRACt(fullurl,'context=([\\w-]*)',1),'')=''
             and coalesce(REGEXP_EXTRACt(fullurl,'topcategory=([\\w-]*)',1),'')=''
             and coalesce(REGEXP_EXTRACt(fullurl,'category=([\\w-]*)',1),'')='' 
             and coalesce(REGEXP_EXTRACt(fullurl,'subcategory2=([\\w-]*)',1),'')=''
              and coalesce(REGEXP_EXTRACt(fullurl,'subcategory3=([\\w-]*)',1),'')=''
              and coalesce(REGEXP_EXTRACt(fullurl,'query=([\\w-]*)',1),'')=''   --avoid searches
            then 'featured'
            --'search'
            -- avoid https://www.groupon.com/occasion/gg-apparel-accessories
            -- avoid https://www.groupon.com/landing/deal-of-the-day
            -- add occasions pages classification
            when REGEXP_EXTRACt(fullurl,'\\.\\w{2,3}/([\\w-]*)',1)='occasion' then 'occasions'
            when (REGEXP_EXTRACt(fullurl,'context=([\\w-]*)',1) = 'local' --https://www.groupon.com/browse/chicago?context=local
            or REGEXP_EXTRACt(fullurl,'topcategory=([\\w-]*)',1) in ('goods','travel','local')   --https://www.groupon.com/browse/chicago?topcategory=goods&category=for-the-home
            or REGEXP_EXTRACt(fullurl,'\\.\\w{2,3}/([\\w-]*)',1) in ('goods','getaways') )  --https://www.groupon.com/goods/
            and  ( coalesce(REGEXP_EXTRACt(fullurl,'[\\?\\&]category=([\\w-]*)',1),'')=''  --https://www.groupon.com/browse/chicago?category=home-improvement
            and  coalesce(REGEXP_EXTRACt(fullurl,'[\\?\\&]subcategory=([\\w-]*)',1),'')=''  --https://www.livingsocial.com/browse/ann-arbor?topcategory=travel&subcategory=beach-vacations-travel
            and  coalesce(REGEXP_EXTRACt(fullurl,'[\\?\\&]subcategory2=([\\w-]*)',1),'')=''
             and coalesce(REGEXP_EXTRACt(fullurl,'subcategory3=([\\w-]*)',1),'')=''
            and    coalesce(REGEXP_EXTRACt(fullurl,'\\.\\w{2,3}/(travel|goods)/([\\w-]*)',2),'') in ('','all')  --https://www.groupon.ca/goods/all
              )  --https://www.groupon.com/goods/for-the-home
            then 'L1'
            when coalesce(
            REGEXP_EXTRACt(fullurl,'goods/([\\w-]*)',1),
            REGEXP_EXTRACt(fullurl,'[\\?\\&]category=([\\w-]*)',1)) in (
            'home-improvement',
            'health-and-fitness',
            'beauty-and-spas',
            'personal-services',
            'things-to-do',
            'automotive',
            'retail',
            'food-and-drink',
            'for-the-home',
            'mens-clothing-shoes-and-accessories',
            'grocery-and-household',
            'jewelry-and-watches',
            'pet-supplies',
            'baby-kids-and-toys',
            'health-and-beauty',
            'auto-and-home-improvement',
            'womens-clothing-shoes-and-accessories',
            'sports-and-outdoors',
            'electronics',
            'entertainment-and-media',
            'toys',
            'hotels-and-accommodations'
            )
            and (coalesce(REGEXP_EXTRACt(fullurl,'subcategory=([\\w-]*)',1),'')='' and coalesce(REGEXP_EXTRACt(fullurl,'subcategory2=([\\w-]*)',1),'')=''  and coalesce(REGEXP_EXTRACt(fullurl,'subcategory3=([\\w-]*)',1),'')='' )  --exclude L3 and L4 categories https://www.groupon.com/browse/lansing?category=beauty-and-spas&subcategory=salons&subcategory2=v-hair-salons
            --OR REGEXP_EXTRACt(fullurl,'travel/([\\w-]*)/hotels',1)  is not null  --seo/getaways/city = https://www.groupon.com/travel/branson-mo/hotels
            then 'L2'
            when coalesce(REGEXP_EXTRACt(fullurl,'subcategory=([\\w-]*)',1),'') != ''  
               --or coalesce(REGEXP_EXTRACt(fullurl,'\\.\\w{2,3}/(travel|goods)/([\\w-]*)',2),'') != '' )  --this can be L3 or L4 https://www.groupon.com/goods/ NO
               and coalesce(REGEXP_EXTRACt(fullurl,'subcategory3=([\\w-]*)',1),'')='' 
               and coalesce(REGEXP_EXTRACt(fullurl,'subcategory2=([\\w-]*)',1),'')=''
            then 'L3'
            when coalesce(REGEXP_EXTRACt(fullurl,'subcategory2=([\\w-]*)',1),'') != '' and coalesce(REGEXP_EXTRACt(fullurl,'subcategory3=([\\w-]*)',1),'')=''   
            then 'L4'
            --subcategory3  https://www.groupon.com/browse/seattle?topcategory=goods&subcategory3=anal-plugs
            when coalesce(REGEXP_EXTRACt(fullurl,'subcategory3=([\\w-]*)',1),'') != ''  
            then 'L4'
            --L5
            end Levb
            -- get the last level of GRT
    , case 
            --L5
            when coalesce(REGEXP_EXTRACt(fullurl,'subcategory3=([\\w-]*)',1),'') != ''   then REGEXP_EXTRACt(fullurl,'subcategory3=([\\w-]*)',1)
            --  L4
            when coalesce(REGEXP_EXTRACt(fullurl,'subcategory2=([\\w-]*)',1),'')!='' then REGEXP_EXTRACt(fullurl,'subcategory2=([\\w-]*)',1)
            -- L3
            when coalesce(REGEXP_EXTRACt(fullurl,'subcategory2=([\\w-]*)',1),'')='' and coalesce(REGEXP_EXTRACt(fullurl,'subcategory=([\\w-]*)',1),'') != '' 
              then REGEXP_EXTRACt(fullurl,'subcategory=([\\w-]*)',1)
            when coalesce(REGEXP_EXTRACt(fullurl,'\\.\\w{2,3}/(travel|goods)/([\\w-]*)',2),'') not in  ('','all') 
              then coalesce(regexp_replace(REGEXP_EXTRACt(fullurl,'\\.\\w{2,3}/(travel|goods)/([\\w-]*)',2),'travel','getaways' ),'')
            --https://www.livingsocial.com/goods/baby-kids-and-toys?page=9
            --L2
            when 
            coalesce(
            REGEXP_EXTRACt(fullurl,'goods/([\\w-]*)',1),
            REGEXP_EXTRACt(fullurl,'[\\?\\&]category=([\\w-]*)',1))  in (
            'home-improvement',
            'health-and-fitness',
            'beauty-and-spas',
            'personal-services',
            'things-to-do',
            'automotive',
            'retail',
            'food-and-drink',
            'for-the-home',
            'mens-clothing-shoes-and-accessories',
            'grocery-and-household',
            'jewelry-and-watches',
            'pet-supplies',
            'baby-kids-and-toys',
            'health-and-beauty',
            'auto-and-home-improvement',
            'womens-clothing-shoes-and-accessories',
            'sports-and-outdoors',
            'electronics',
            'entertainment-and-media',
            'toys',
            'hotels-and-accommodations'
            ) then  coalesce(
            REGEXP_EXTRACt(fullurl,'goods/([\\w-]*)',1),
            REGEXP_EXTRACt(fullurl,'[\\?\\&]category=([\\w-]*)',1))
            --L1   --rename getaways to travel!!!!
            else coalesce(REGEXP_EXTRACt(fullurl,'context=([\\w-]*)',1) ,   --https://www.groupon.com/browse/chicago?context=local
             REGEXP_EXTRACt(fullurl,'topcategory=([\\w-]*)',1),  --https://www.groupon.com/browse/chicago?topcategory=goods&category=for-the-home
             --regexp_replace(REGEXP_EXTRACt(fullurl,'\\.\\w{2,3}/([\\w-]*)',1),'getaways','travel' ))   --remove if browse    https://www.groupon.ca/browse/cantonsdelest        
             REGEXP_EXTRACt(fullurl,'\\.\\w{2,3}/([\\w-]*)',1))
             --https://www.livingsocial.com/getaways?destination_id=cc05e11e-2ae2-4ad5-afdb-7e75d4edaaf8&destination_name=Reno, NV, USA&check_in=2020-12-30&check_out=2021-01-02&city_slug=reno
             END grt_perm
             --GRT1
             --https://www.groupon.com/browse/chicago?context=local
             --https://www.groupon.com/browse/chicago?topcategory=goods&category=for-the-home
             --https://www.livingsocial.com/getaways?destination_id=cc05e11e-2ae2-4ad5-afdb-7e75d4edaaf8&destination_na
             --https://www.livingsocial.com/goods/baby-kids-and-toys?page=9
             ,case when coalesce(REGEXP_EXTRACt(fullurl,'context=([\\w-]*)',1),'')='local' then 'local'
             when coalesce(REGEXP_EXTRACt(fullurl,'topcategory=([\\w-]*)',1) ,'')!='' then REGEXP_EXTRACt(fullurl,'topcategory=([\\w-]*)',1)
             when REGEXP_EXTRACt(fullurl,'\\.\\w{2,3}/([\\w-]*)',1) in ('goods','getaways') then  REGEXP_EXTRACt(fullurl,'\\.\\w{2,3}/([\\w-]*)',1)
             end grt_l1
             --channel if GRT1 is not correct
             --extract parameters:
        --     ,coalesce(REGEXP_EXTRACt(fullurl,'context=([\\w-]*)',1),'') url_context
        --     ,coalesce(REGEXP_EXTRACt(fullurl,'topcategory=([\\w-]*)',1) ,'') url_topcategory
        --     ,REGEXP_EXTRACt(fullurl,'\\.\\w{2,3}/([\\w-]*)',1) url_path
        --     ,coalesce(REGEXP_EXTRACt(fullurl,'[\\?\\&]category=([\\w-]*)',1),'') url_category
        --     ,coalesce(REGEXP_EXTRACt(fullurl,'subcategory=([\\w-]*)',1),'') url_subcategory
        --     ,coalesce(REGEXP_EXTRACt(fullurl,'subcategory2=([\\w-]*)',1),'') url_subcategory2
        --     ,REGEXP_EXTRACt(fullurl,'goods/([\\w-]*)',1) url_goods_subpath
             --end
             ,'' page_type
             ,fullurl fullrul2
from ai_reporting.sl_imp_clicks
where
cast(position as int ) between 0 and 19  and
--eventdate between '2021-11-10' and '2021-11-28'
cast( eventdate as date) between date_sub(current_date,2) and date_sub(current_date,1) --'2020-08-01' and '2020-08-11'
--and rawpagetype != 'deals/show'
) cc
--V3
left join category_ids cat
   on cat.permalink=  case when cc.grt_perm in ('all nearby deals','all local deals') then 'Local'
        when cc.grt_perm in ('all travel deals', 'hotels') then 'Getaways'
        when cc.grt_perm in ('all goods deals') then 'Goods'
        else cc.grt_perm
        end
   and friendly_name_singular is not null
   --on regexp_replace(lower(cat.friendly_name),'travel','getaways' ) =lower(cft.grt)
)
insert overwrite table grp_gdoop_bizops_db.aog_sl_bid_pagecategory partition (eventdate)
select clientplatform,level
, page_type_sl
,division
,SL
--,case when aogcampaignid is not null then 1 else 0 end SL
, cft_l1_cat_name
--,cft_l2_cat_name
--,cft_l3_cat_name
--,cft_l4_cat_name
--,cft_l5_cat_name
       ,dcat2.permalink  cft_2_perm
       ,dcat3.permalink  cft_3_perm
       ,dcat4.permalink cft_4_perm
       ,dcat5.permalink cft_5_perm
,coalesce(impr.permalink,cft_l1_cat_name) permalink
,grt_id
 ,count(case when position=0 then bcookie end) cookies1
 , count(case when position=0 then dv_time end) dv1
 ,count(case when position=0 then order_uuid end) orders1
 ,sum(case when position=0 then ogp else 0 end) ogp1
 , sum(case when position=0 then nob else 0 end) - sum(case when position=0 then nor else 0 end) merch_rev1
   ,count(case when position=1 then bcookie end) cookies2
 , count(case when position=1 then dv_time end) dv2
 ,count(case when position=1 then order_uuid end) orders2
 ,sum(case when position=1 then ogp else 0 end) ogp2
 , sum(case when position=1 then nob else 0 end) - sum(case when position=1 then nor else 0 end) merch_rev2
    ,count(case when position=2 then bcookie end) cookies3
 , count(case when position=2 then dv_time end) dv3
 ,count(case when position=2 then order_uuid end) orders3
 ,sum(case when position=2 then ogp else 0 end) ogp3
 , sum(case when position=2 then nob else 0 end) - sum(case when position=2 then nor else 0 end) merch_rev3
  ,count(case when position=3 then bcookie end) cookies4
 , count(case when position=3 then dv_time end) dv4
 ,count(case when position=3 then order_uuid end) orders4
 ,sum(case when position=3 then ogp else 0 end) ogp4
 , sum(case when position=3 then nob else 0 end) - sum(case when position=3 then nor else 0 end) merch_rev4
   ,count(case when position between 0 and  5 then bcookie end) cookies6
 , count(case when position between 0 and  5 then dv_time end) dv6
 ,count(case when position between 0 and  5 then order_uuid end) orders6
 ,sum(case when position between 0 and  5 then ogp else 0 end) ogp6
 , sum(case when position between 0 and 5 then nob else 0 end) - sum(case when position between 0 and  5 then nor else 0 end) merch_rev6
  ,count(case when position between 0 and  6 then bcookie end) cookies7
 , count(case when position between 0 and  6 then dv_time end) dv7
 ,count(case when position between 0 and  6 then order_uuid end) orders7
 ,sum(case when position between 0 and  6 then ogp else 0 end) ogp7
 , sum(case when position between 0 and 6 then nob else 0 end) - sum(case when position between 0 and  6 then nor else 0 end) merch_rev7
 ,count(case when position between 0 and  7 then bcookie end) cookies8
 , count(case when position between 0 and  7 then dv_time end) dv8
 ,count(case when position between 0 and  7 then order_uuid end) orders8
 ,sum(case when position between 0 and  7 then ogp else 0 end) ogp8
 , sum(case when position between 0 and  7 then nob else 0 end) - sum(case when position between 0 and  7 then nor else 0 end) merch_rev8
    ,count(case when position between 0 and  8 then bcookie end) cookies9
 , count(case when position between 0 and  8 then dv_time end) dv9
 ,count(case when position between 0 and  8 then order_uuid end) orders9
 ,sum(case when position between 0 and  8 then ogp else 0 end) ogp9
 , sum(case when position between 0 and  8 then nob else 0 end) - sum(case when position between 0 and  8 then nor else 0 end) merch_rev9
 ,count(case when position between 0 and  9 then bcookie end) cookies10
  ,count(case when position between 0 and  9 then dv_time end) dv10
 ,count(case when position between 0 and  9 then order_uuid end) orders10
 ,sum(case when position between 0 and  9 then ogp else 0 end) ogp10
 , sum(case when position between 0 and  9 then nob else 0 end) - sum(case when position between 0 and  9 then nor else 0 end) merch_rev10
  ,count(case when position between 1 and  9 then bcookie end) cookies10b
  ,count(case when position between 1 and  9 then dv_time end) dv10b
 ,count(case when position between 1 and  9 then order_uuid end) orders10b
 ,sum(case when position between 1 and  9 then ogp else 0 end) ogp10b
 , sum(case when position between 1 and  9 then nob else 0 end) - sum(case when position between 1 and  9 then nor else 0 end) merch_rev10b
  ,count(distinct dealuuid) deals
  ,count(distinct eventdate) days
  --new columns
  , count(distinct bcookie) cookies, avg(position) avg_positions
, avg(case when dv_time is not null then position end) avg_dv_pos
, max(position ) max_pos
,eventdate
from (select eventdate,clientplatform,pageid,aogcampaignid,dealuuid,page_type_sl, nob,nor,ogp,dv_time,order_uuid,position,bcookie,consumerid,division
    ,case when page_type_sl='search' and level is null then 'search' else level end LEVEL,SL
     ,search_query
    --concat(Grt...)
    ,lower(cft_l1_cat_name ) cft_l1_cat_name
   --,cft_l2_cat_name
   --,cft_l3_cat_name
   --,cft_l4_cat_name
   ,cft_l2_cat_id
      ,cft_l3_cat_id
   ,cft_l4_cat_id
   ,cft_l5_cat_id
   ,permalink
   ,grt_id
    from categories_app 
    --where dealuuid='69a9dffa-6d95-4d8e-9faa-3d40770273ab'
    union all
    select eventdate,clientplatform,pageid,aogcampaignid,dealuuid,case when level like 'L%' then 'browse' else level end page_type_sl, nob,nor,ogp,dv_time,order_uuid,position,bcookie,consumerid,division
    ,LEVEL,SL
    ,search_query
    ,lower(coalesce(cft_l1_cat_name ,grt_l1) ) cft_l1_cat_name
    --,cft_l2_cat_name
    --,cft_l3_cat_name
    --,cft_l4_cat_name
    ,cft_l2_cat_id
       ,cft_l3_cat_id
   ,cft_l4_cat_id
   ,cft_l5_cat_id
    ,permalink
    ,grt_id
    from categories
    --where dealuuid='69a9dffa-6d95-4d8e-9faa-3d40770273ab'
    ) impr
      left join prod_groupondw.dim_category dcat2
        on impr.cft_l2_cat_id=dcat2.category_id
      left join prod_groupondw.dim_category dcat3
        on impr.cft_l3_cat_id=dcat3.category_id
      left join prod_groupondw.dim_category dcat4
        on impr.cft_l4_cat_id=dcat4.category_id        
      left join prod_groupondw.dim_category dcat5
        on impr.cft_l5_cat_id=dcat5.category_id
group by eventdate,clientplatform,level
,page_type_sl
,division
,SL
,cft_l1_cat_name
,dcat2.permalink
,dcat3.permalink
,dcat4.permalink
,dcat5.permalink
,coalesce(impr.permalink,cft_l1_cat_name)
,grt_id

select *
from grp_gdoop_pde.junohourly
where eventdate >= '2022-04-28'
and lower(event) in ('genericpageview')
and lower(country) in ('us')
and rawevent = 'GRP14'
and lower(trim(bcookie)) <> 'null'
and bcookie = '02838cb4-8b9b-61b7-0415-1d8e2165d04a'
and lower(trim(useragent)) not like '%bot%'
and lower(trim(useragent)) not like '%crawler%'
and lower(trim(useragent)) not like '%search%'
and lower(trim(useragent)) not like '%spider%'
and lower(trim(useragent)) not like '%spyder%'
and lower(trim(coalesce(pageid,'x'))) <> 'splash'
;



--------------------

insert overwrite table grp_gdoop_bizops_db.avb_aog_fillrate_etl
select 
*
from (
  select search_query
  ,eventdate
  ,sum(case when sl=0 then 1 else 0 end) imp
  ,count(case when sl=0 then dv_time end) dv 
  ,count(case when sl=0 then order_uuid end) orders
  ,sum(case when sl=0 then ogp else 0 end) ogp
  , sum(case when sl=0 then nob else 0 end) - sum(case when sl=0 then nor else 0 end) merch_rev
  ---SL columns
  ,sum(case when sl=1 then 1 else 0 end) imp_sl
  ,count(case when sl=1 then dv_time end) dv_sl
  ,count(case when sl=1 then order_uuid end) orders_sl
  ,sum(case when sl=1 then ogp else 0 end) ogp_sl
  , sum(case when sl=1 then nob else 0 end) - sum(case when sl=1 then nor else 0 end) merch_rev_sl
  from (
  select 
    eventdate,search_query,position,ogp,nor,nob,dv_time,bcookie,order_uuid,dealuuid,clientplatform
    ,grt1,grt2,grt3,grt4,grt5,grt6
    --add window counting per search, platform, the count of each grtX
    ,division
    ,case when aogcampaignid is not null then 1 else 0 end SL
  from ai_reporting.sl_imp_clicks
  where position = '0' 
       and eventdate between date_sub(current_date,31) and current_date
       and search_query is not null
  union all
  select eventdate,search_query,  position ,ogp,nor,nob,dv_time,bcookie,order_uuid,dealuuid,clientplatform
    ,grt1,grt2,grt3,grt4,grt5,grt6
    ,division
    ,case when aogcampaignid is not null then 1 else 0 end SL
  from ai_reporting.sl_imp_clicks_app
   where 
     (  
       (position ='1' and clientplatform='Android') 
       or 
       ( position ='2' and clientplatform in ('iPhone','iPad')) --and extrainfo like '%card_search_uuid%'
     ) and
       eventdate between date_sub(current_date,31) and current_date
       and extrainfo not like '%collectionCardName":"crossChannel_mobile_forYouTab%'
       and extrainfo not like '%collectionTemplateId":"61ffaa1a-d3da-4232-95bc-81d9d18643fe%'
       and search_query is not null
  ) imp
  group by search_query ,eventdate
 ) bid
left join (
  select --deal_id
  search_keyword 
  ,report_Date
  ,max(sponsored_product_filters) filters
  ,sum(impressions) impressions
  ,sum(clicks) clicks
  from ad_reporting_na_prod.citrus_master_report_citrus_51_v1 cr
  where report_date between date_sub(current_date,31) and current_date
    and position_in_pod = '0'
  group by 1,2
) citr on citr.search_keyword = bid.search_query
  and citr.report_date = bid.eventdate
where bid.imp > 10

----------------QUERIES

select ds, 
       clientplatform, 
       variant_name, 
       sum(impression_count) total_impressions 
       from grp_gdoop_optimize_db.impressions 
where experiment_name = 'relevance-sl-personalisation-cc' and ds >= '2022-04-28'
group by ds, clientplatform, variant_name;

select ds, 
       clientplatform,
       sum(total_impressions) total_imps, 
       sum(case when total_variants = 3 then total_impressions end) three_variants, 
       sum(case when total_variants = 2 then total_impressions end) two_variants
       from grp_gdoop_bizops_db.np_pers_tst_opt 
       group by ds, clientplatform;
       
select 
    eventdate,
    clientplatform, 
    variant_name,
    count(distinct bcookie) unique_imps
from 
grp_gdoop_bizops_db.np_pers_test_imp
group by eventdate, clientplatform, variant_name;




select
    eventdate,
    clientplatform, 
    variantname,
    sponsoredpersonalised,
    count(consumerid) total_impressions, 
    count(distinct consumerid) unique_consumer_imp
from grp_gdoop_bizops_db.np_pers_test_imp3
    group by 
    eventdate,
    clientplatform, 
    variantname,
    sponsoredpersonalised;
       
select 
    eventdate,
    clientplatform, 
    variantname,
    sponsoredpersonalised,
    count(consumerid) total_clks, 
    count(distinct consumerid) unique_consumer_clk
from 
grp_gdoop_bizops_db.np_pers_test_clk3
group by 
    eventdate,
    clientplatform, 
    variantname,
    sponsoredpersonalised;

select bcookie, count(1) xyz from grp_gdoop_bizops_db.np_pers_test_clk3 group by bcookie;
select * from grp_gdoop_bizops_db.np_pers_test_clk3 where bcookie = 'A9229298-34E8-BD5F-6FF3-A816C68E185A'

SELECT
clientplatform, 
count(dealuuid)
from
grp_gdoop_pde.junohourly
where
eventdate >= '2022-04-28'
and eventdestination = 'genericClick'
and platform in ('web', 'mobile')
and sponsoredadid is not null
and sponsoredPersonalised is not null
and consumerid is not null
and eventtime >= '1651149000000'
and variantname in ('Control', 'relevance-sl-personalisation-with-fallback', 'relevance-sl-personalisation-without-fallback')
group by clientplatform;



select * from grp_gdoop_bizops_db.np_pers_tst_opt;
select * from grp_gdoop_bizops_db.np_pers_test_imp;
select * from grp_gdoop_bizops_db.np_pers_test_imp2;
select * from grp_gdoop_bizops_db.np_pers_test_imp3;

select * from grp_gdoop_bizops_db.np_pers_test_clk3 where part_of_exp 

select * from prod_groupondw.bld_widget_contents where dt >= '2022-04-28';

select * from grp_gdoop_bizops_db.np_pers_test_imp_web;

select * from grp_gdoop_bizops_db.np_pers_test_imp;

{"cardPermalink":"cardatron:mobile:home:us","collectionCardName":"crossChannel_homepage_sponsored_carousel",
"collectionCardPosition":"3","collectionCardTitleText":"Featured","collectionCardUUID":"12ebbd28-3a95-4823-9258-65eb7e57d0e3",
"collectionTemplateId":"d2cd6438-58b3-4e55-b3fc-6942f0e8da64","collectionTemplateView":"HorizontalTwoUpCompoundCardView"}


{"cardPermalink":"cardatron:mobile:home-treatment:US","collectionCardName":"crossChannel_homepage_sponsored_carousel","collectionCardPosition":"4",
"collectionCardTitleText":"Featured Deals","collectionCardUUID":"12ebbd28-3a95-4823-9258-65eb7e57d0e3",
"collectionTemplateId":"b3ff84dd-b312-4b13-bd6c-66cfb8ffc7d1","collectionTemplateView":"HorizontalCompoundCardEndTileView",
"idfv":"C04376D8-8485-4FFB-8FD7-90821EBD419C","isD3CExperimentOn":true,"isD3CExperimentOn_3PIP":true,"isD3CExperimentOn_Booking":true,"isD3CExperimentOn_Goods":false,
"screen_instance_id":"DEF5BCB9-7F04-4834-AB77-6A13F0CD4B73_1651181824933","wolfhoundPageId":"","wolfhoundPageUrl":""}

{"cardPermalink":"cardatron:mobile:home-cx90:us","collectionCardName":"crossChannel_homepage_sponsored_carousel",
"collectionCardPosition":"3","collectionCardTitleText":"Featured","collectionCardUUID":"12ebbd28-3a95-4823-9258-65eb7e57d0e3",
"collectionTemplateId":"d2cd6438-58b3-4e55-b3fc-6942f0e8da64","collectionTemplateView":"HorizontalTwoUpCompoundCardView"}

{"cardPermalink":"cardatron:mobile:home-cx90:us","collectionCardName":"crossChannel_homepage_sponsored_carousel",
"collectionCardPosition":"3","collectionCardTitleText":"Featured","collectionCardUUID":"12ebbd28-3a95-4823-9258-65eb7e57d0e3",
"collectionTemplateId":"d2cd6438-58b3-4e55-b3fc-6942f0e8da64","collectionTemplateView":"HorizontalTwoUpCompoundCardView"}



select dt, platform, count(distinct concat(user_browser_id, dt)) as unique_impressions
from prod_groupondw.bld_widget_contents 
where lower(platform) in ('desktop', 'touch')
and dt >= date_sub(CURRENT_DATE, 30) 
and user_browser_id <> '' 
and user_browser_id is not null
and lower(page_country) IN ('us', 'ca')
and lower(page_hostname) like '%groupon%'
and lower(page_type) like '%homepage%'
and lower(widget_content_name) like '%crosschannel_homepage_sponsored_carousel%'
and lower(widget_content_type) = 'compound'
group by dt, platform;

select * from grp_gdoop_optimize_db.impressions where experiment_name = 'AND_Homepage_SponsoredSwap_NA';

select 
bcookie,
extrainfo,
eventdate,
position
from grp_gdoop_pde.junoHourly
where eventdate >= '2022-04-01'
and lower(eventdestination) = 'genericimpression'
and lower(event) = 'genericimpression'
and country in ('US','CA')
--and lower(trim(consumerid)) = '18010e36-415c-11ea-8ecc-0242ac120002'
and bcookie = '7178083F-BCB6-D8B1-919B-9D41C8625973'
and lower(trim(extrainfo)) like '%crosschannel_homepage_sponsored_carousel%' 
and get_json_object(extrainfo, '$.sponsoredAdId') is not null


where eventdate >= '2022-04-01' and eventdate <= '2022-04-19'
and lower(trim(regexp_replace(clientplatform,'\\t|\\n|\\r|\\u0001', ''))) in ('iphone', 'android')
and country in ('US','CA')
and platform = 'mobile'
and bcookie is not null 
and bcookie <> ''
and lower(trim(bcookie)) <> 'null'
and lower(brand) = 'groupon' 
and bcookie not in ('android_automation_test' ,'ios_automation_test','android_perf_test_nightly','ios_perf_test_nightly')
and case when 'userAgent' like '%bot%' or useragent like '%crawler%' or useragent like '%search%' or useragent like '%spider%' or useragent like '%spyder%' then 1 else 0 end = 0
and lower(trim(extrainfo)) like '%crosschannel_homepage_sponsored_carousel%' 
and lower(impressiontype) = 'collection_card_impression'
and lower(eventdestination) = 'genericimpression'
and lower(event) = 'genericimpression'



select * from grp_gdoop_optimize_db.impressions;
select * from grp_gdoop_bizops_db.aog_sl_bid_pagecategory;

select eventdate, count(1) from grp_gdoop_bizops_db.aog_sl_bid_pagecategory group by eventdate order by eventdate desc;

select eventdate, count(1) from grp_gdoop_bizops_db.aog_sl_bid_pagecategory_etl group by eventdate order by eventdate desc;


select exp_users.experiment_name, dt, platform, exp_users.variant_name, count(concat(bld.bcookie, bld.dt)) as clicks
from 
(
select *
from prod_groupondw.bld_events
where lower(platform) in ('desktop', 'touch')
and dt >= '2022-03-01'
and event = 'click' 
and bot_flag = '0'
and bcookie <> '' 
and bcookie is not null
and lower(page_country) IN ('us', 'ca')
and lower(page_hostname) like '%groupon%'
and lower(page_type) like '%homepage%'
and lower(widget_content_name) like '%crosschannel_homepage_sponsored_carousel%'
and lower(widget_content_type) = 'compound'
) bld;


select *
from prod_groupondw.bld_widgets
where lower(platform) in ('desktop', 'touch')
and dt = '2022-02-01'
and event = 'click' 
and bot_flag = '0'
and bcookie <> '' 
and bcookie is not null
and lower(page_country) IN ('us', 'ca')
and lower(page_hostname) like '%groupon%'
and lower(page_type) like '%homepage%'
and lower(widget_content_name) like '%crosschannel_homepage_sponsored_carousel%'
and lower(widget_content_type) = 'compound'
;


select *
from prod_groupondw.bld_widget_contents 
where lower(platform) in ('desktop', 'touch')
and dt = '2022-02-24'
and user_browser_id = '8a6d60fb-aa7f-ddc3-7c63-dffb6dba8574'
and lower(page_country) IN ('us', 'ca')
and lower(widget_content_name) like '%crosschannel_homepage_sponsored_carousel%'
;

{u'cardUUID': u'12ebbd28-3a95-4823-9258-65eb7e57d0e3', 
u'templateView': u'TitledCompoundCardView', 
u'templateUUID': u'f8b3c1a1-dab5-4776-83d6-a148d4d03f9a',
u'cardName': u'crossChannel_homepage_sponsored_carousel'}



----------------------------------------------------------------------------------------------------------------------------

select distinct event
from prod_groupondw.bld_events
where lower(platform) in ('desktop', 'touch')
and dt >= '2022-03-01'
;

select eventdate from grp_gdoop_bizops_db.np_impressions_sl_app group by eventdate order by eventdate desc;




select bcookie, eventdate, clientplatform
from grp_gdoop_pde.junoHourly 
where eventdate > '2021-12-07'
and lower(trim(regexp_replace(clientplatform,'\\t|\\n|\\r|\\u0001', ''))) in ('iphone', 'android')
and country in ('US','CA')
and platform = 'mobile'
and bcookie is not null 
and bcookie <> ''
and lower(trim(bcookie)) <> 'null'
and lower(brand) = 'groupon' 
and bcookie not in ('android_automation_test' ,'ios_automation_test','android_perf_test_nightly','ios_perf_test_nightly')
and case when 'userAgent' like '%bot%' or useragent like '%crawler%' or useragent like '%search%' or useragent like '%spider%' or useragent like '%spyder%' then 1 else 0 end = 0
and lower(trim(extrainfo)) like '%crosschannel_homepage_sponsored_carousel%'
and lower(clicktype) in ('collection_card_click', 'collection_card_impression_click')
and lower(eventdestination) = 'genericclick'
and lower(event) = 'genericclick';



select 
   dealuuid,
   widgetname,
   aogcampaignid,
   bcookie,
 ----  extrainfo,
   eventdestination, 
   clientplatform,
   consumerid,
   event,
   eventtime,
   optionposition,
   parent, 
   position,
   sponsoredAdId
from grp_gdoop_pde.junoHourly 
where  eventdestination = 'dealImpression'
and eventdate = '2022-02-24'
and country in ('US','CA')
and sponsoredadid is not null
---and bcookie = '7178083F-BCB6-D8B1-919B-9D41C8625973'
;


select 
   distinct eventdestination
from grp_gdoop_pde.junoHourly 
where  eventdate = '2022-02-24'
and country in ('US','CA')
and sponsoredadid is not null


select 
   *
from grp_gdoop_pde.junoHourly
where eventdate >= '2022-03-07'
and country in ('US','CA')
--and lower(trim(consumerid)) = '18010e36-415c-11ea-8ecc-0242ac120002'
and bcookie = '7178083F-BCB6-D8B1-919B-9D41C8625973'
and sponsoredAdId is not null
;


select *
from grp_gdoop_pde.junoHourly 
where eventdate >= '2022-02-24' and eventdate <= '2022-02-27'
and lower(trim(regexp_replace(clientplatform,'\\t|\\n|\\r|\\u0001', ''))) in ('iphone', 'android')
and country in ('US','CA')
and platform = 'mobile'
and bcookie is not null 
and bcookie <> ''
and lower(trim(bcookie)) <> 'null'
and lower(brand) = 'groupon' 
and bcookie not in ('android_automation_test' ,'ios_automation_test','android_perf_test_nightly','ios_perf_test_nightly')
and bcookie = '7178083F-BCB6-D8B1-919B-9D41C8625973'
and case when 'userAgent' like '%bot%' or useragent like '%crawler%' or useragent like '%search%' or useragent like '%spider%' or useragent like '%spyder%' then 1 else 0 end = 0
and lower(trim(extrainfo)) like '%crosschannel_homepage_sponsored_carousel%' 
and lower(impressiontype) = 'collection_card_impression'
and lower(eventdestination) = 'genericimpression'
and lower(event) = 'genericimpression'
;


select 
   *
from grp_gdoop_pde.junoHourly
where  --eventdestination = 'searchBrowseView'
--and event = 'genericPageView'
rawevent = 'GRP2'
and platform = 'mobile'
and eventdate >= '2022-02-24'
and country in ('US','CA')
--and lower(trim(extrainfo)) like '%crosschannel_homepage_sponsored_carousel%'
and lower(trim(consumerid)) = '18010e36-415c-11ea-8ecc-0242ac120002';

-------------F74779A6-7507-77D1-2D05-E9148C8AEAA5
-------------7178083F-BCB6-D8B1-919B-9D41C8625973
-------------8a6d60fb-aa7f-ddc3-7c63-dffb6dba8574



select 
   *
from grp_gdoop_pde.junoHourly
where  eventdestination = 'dealImpression'
and eventdate >= '2022-03-01'
and country in ('US','CA')
and bcookie = '8a6d60fb-aa7f-ddc3-7c63-dffb6dba8574'
and sponsoredAdId is not null
;

/*Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36
1.64572132504222E9_aab1970458e4b43079ce0b3e27162a2f
tracky_json_nginx
tracky_json_nginx
3ec71082-e92d-4819-8710-82e92d6819b3
3ec71082-e92d-4819-8710-82e92d6819b3-1645721221686-TH0*/




select distinct event, eventdestination
from grp_gdoop_pde.junoHourly 
where eventdate = '2022-02-24'
and country in ('US','CA')
and bcookie is not null 
and bcookie = '8a6d60fb-aa7f-ddc3-7c63-dffb6dba8574'
and lower(trim(bcookie)) <> 'null'
and lower(brand) = 'groupon';

select 
    eventdate,
    case when clientplatform in ('iPhone', 'iPad', 'Android') then 'App' else clientplatform end platform,
    count(distinct dealuuid) total_deals, 
    sum(total_impressions) total_impressions,
    sum(total_impression_aog) total_impression_aog, 
    sum(total_clicks) total_clicks
from 
( select 
        eventdate,
        dealuuid,
        clientplatform,
        count(case when deal_landing = 'Sponsored' then 1 end) total_impressions,
        count(distinct case when deal_landing = 'Sponsored' then aogcampaignid end) total_impression_aog,
        count(distinct case when deal_landing = 'Sponsored' then dv_time end) total_clicks, 
        count(distinct case when deal_landing = 'Sponsored' then sli.order_uuid end) orders_sold, 
        count(case when deal_landing = 'Organic' then 1 end) total_impressions_org, 
        count(distinct case when deal_landing = 'Organic' then aogcampaignid end) total_impression_aog_org,
        count(distinct case when deal_landing = 'Organic' then dv_time end) total_clicks_org, 
        count(distinct case when deal_landing = 'Organic' then sli.order_uuid end) orders_sold_org
   from 
        (select * from grp_gdoop_bizops_db.np_impressions_sl
         where eventdate >= '2022-01-01' and eventdate <= '2022-01-31'
         and page = 'homepage'
        union all
        select * from grp_gdoop_bizops_db.np_impressions_sl_app
        where eventdate >= '2022-01-01' and eventdate <= '2022-01-31' and page = 'homepage') as sli
    group by eventdate, dealuuid, clientplatform) as fin
group by eventdate, case when clientplatform in ('iPhone', 'iPad', 'Android') then 'App' else clientplatform end;


select 
  distinct clientplatform
   from 
        (select * from grp_gdoop_bizops_db.np_impressions_sl
         where eventdate >= '2021-01-01' and eventdate <= '2021-01-31'
        union all
        select * from grp_gdoop_bizops_db.np_impressions_sl_app
        where eventdate >= '2021-01-01' and eventdate <= '2021-01-31') as sli;




select 
   count(1)
from 
(select 
  finalize_on.*
from 
(select 
   finalize.*, 
   xyz.acct_owner_name,
   row_number () over(partition by finalize.merchant_uuid order by total_ad_revenue desc) rank_of_deals
from 
(select 
    a.*, 
    case when c.merchant_uuid is not null then 0 
         when b.total_adspend > 0 then 0 
         else 1 end include,
    case when b.merchant_uuid is not null then 1 else 0 end merchant_on_previous_list, 
    case when merchant_active = 1 then 1 else 0 end merchant_created_a_campaign
from grp_gdoop_bizops_db.np_target_list_all as a 
left join (select merchant_uuid, 
                  max(merchant_active) merchant_active, 
                  case when sum(total_adspend) is null then 0 else sum(total_adspend) end total_adspend
           from grp_gdoop_bizops_db.np_sl_ss_list_tableau
           group by merchant_uuid) as b on a.merchant_uuid = b.merchant_uuid
left join
(select b.merchant_uuid from grp_gdoop_bizops_db.np_sl_ad_snapshot as a 
            left join grp_gdoop_bizops_db.pai_deals as b on a.sku = b.deal_uuid
group by b.merchant_uuid) as c on a.merchant_uuid = c.merchant_uuid) as finalize
left join grp_gdoop_bizops_db.pai_merchants as xyz on finalize.merchant_uuid = xyz.merchant_uuid
where 
grt_l2_cat_name in ('L2 - Things to Do - Leisure', 'L2 - Home & Auto', 'L2 - Health / Beauty / Wellness') and 
include = 1 and cvr >= 0.04 and total_ad_revenue >= 200) as finalize_on 
where rank_of_deals = 1
order by total_ad_revenue desc) xyz;



select * from grp_gdoop_bizops_db.np_sl_ss_list_tableau;
select * from grp_gdoop_bizops_db.pai_merchants;

select * from 
(select merchant_uuid, 
                  max(merchant_active) merchant_active, 
                  sum(total_adspend) total_adspend 
           from grp_gdoop_bizops_db.np_sl_ss_list_tableau
           group by merchant_uuid) as fin where total_adspend is null and merchant_active = 1


select * from grp_gdoop_bizops_db.np_sl_ss_list_tableau;
select distinct grt_l2_cat_name from grp_gdoop_bizops_db.avb_aog_sl_l30aggview;
select * from grp_gdoop_bizops_db.np_target_list_all;
select * from grp_gdoop_bizops_db.pai_merchants;
select * from dwh_base_sec_view.sf_account;


select count(1) from grp_gdoop_bizops_db.np_target_list_all;
select count(1) from grp_gdoop_bizops_db.avb_aog_sl_l30aggview;

select * from grp_gdoop_bizops_db.np_sl_ss_list_tableau;

select * from grp_gdoop_bizops_db.np_citrus_sl_bid2 order by report_date desc;

describe table grp_gdoop_bizops_db.np_citrus_sl_bid2;
select count(1) from grp_gdoop_bizops_db.np_citrus_sl_bid2;

select * 
from grp_gdoop_bizops_db.np_case_study_post_all2 where days_from_launch <= 30;

select * from grp_gdoop_bizops_db.np_temp_case_waster where deal_uuid = 'abe208b0-f79a-4ffc-b00a-c7ab3174531e';

select a.*, 'pre' timeline
    from grp_gdoop_bizops_db.np_case_study_pre2 as a
          where cast(eventdate as date) >= cast('2020-01-01' as date)
                and cast(position as integer) <= cast(20 as integer);

SELECT * from ai_reporting.sl_bid_pagecategory;


select * from grp_gdoop_bizops_db.np_final_case;
select * from grp_gdoop_bizops_db.np_case_study_post_all2;

select
select 
       fin.dealuuid,
       b.deal_permalink, 
       c.merchant_uuid,
       d.merchant_name,
       fin.min_launch_date,
       fin.min_live_on_groupon,
       fin.live_days_on_sl,
       fin.number_of_days_comparison,
       fin.positions,
       sum(case when timeline = 'pre' then total_imps end)  total_imps_pre, 
       sum(case when timeline = 'post' and deal_landing = 'Organic' then total_imps end) total_imps_post_org, 
       sum(case when timeline = 'post' and deal_landing = 'Sponsored' then total_imps end) total_imps_post_sl,
       sum(case when timeline = 'pre' then total_clicks end)  total_clicks_pre, 
       sum(case when timeline = 'post' and deal_landing = 'Organic' then total_clicks end) total_clicks_post_org, 
       sum(case when timeline = 'post' and deal_landing = 'Sponsored' then total_clicks end) total_clicks_post_sl,
       sum(case when timeline = 'pre' then merch_revenue end)  merch_revenue_pre, 
       sum(case when timeline = 'post' and deal_landing = 'Organic' then merch_revenue end) merch_revenue_post_org, 
       sum(case when timeline = 'post' and deal_landing = 'Sponsored' then merch_revenue end) merch_revenue_post_sl,
       sum(case when timeline = 'pre' then total_orders end)  total_orders_pre, 
       sum(case when timeline = 'post' and deal_landing = 'Organic' then total_orders end) total_orders_post_org, 
       sum(case when timeline = 'post' and deal_landing = 'Sponsored' then total_orders end) total_orders_post_sl
from
from
(select * from grp_gdoop_bizops_db.np_case_study_pre_all 
where dealuuid in ('3fda84f3-f402-4bc1-ba45-4f945c286275')
union all
select * from grp_gdoop_bizops_db.np_case_study_post_all
where dealuuid in ('3fda84f3-f402-4bc1-ba45-4f945c286275')) as fin



select * from grp_gdoop_bizops_db.np_case_study_pre_all  where dealuuid = '3fda84f3-f402-4bc1-ba45-4f945c286275';
select * from grp_gdoop_bizops_db.np_case_study_post_all where dealuuid = '3fda84f3-f402-4bc1-ba45-4f945c286275';
                   
                   
                   
select * from grp_gdoop_bizops_db.np_case_study_pre_all where "position" is null;

select eventdate from grp_gdoop_bizops_db.np_sl_all_deals order by eventdate desc;
select max(eventdate) from grp_gdoop_bizops_db.np_impressions_sl where dealuuid = 'abe208b0-f79a-4ffc-b00a-c7ab3174531e';

create table grp_gdoop_bizops_db.np_sl_roas_final_deal stored as orc as 
select
           date_sub(next_day(eventdate, 'MON'), 1) date_start_end,
           dealuuid, 
           coupon,
           max(merch_segmentation) merch_segmentation,
           max(l1) l1,
           max(l2) l2,
           sum(deal_rev) merch_rev, 
           sum(deal_rev_same_day) merch_rev_same_day, 
           sum(deal_rev_30_day) merch_rev_30_day, 
           sum(deal_rev_120_day) merch_rev_120_day, 
           sum(deal_rev_all_red) merch_rev_all_red,
           sum(sli_impressions) sli_impressions, 
           sum(total_impression_aog) total_impression_aog,
           sum(sli_clicks) sli_clicks, 
           sum(orders_sold) orders_sold,
           sum(deal_rev_org) merch_rev_org, 
           sum(deal_rev_same_day_org) merch_rev_same_day_org,
           sum(deal_rev_30_day_org) merch_rev_30_day_org,
           sum(deal_rev_120_day_org) merch_rev_120_day_org,
           sum(deal_rev_all_red_org) merch_rev_all_red_org,
           sum(sli_impressions_org) sli_impressions_org,
           sum(total_impression_aog_org) total_impression_aog_org,
           sum(sli_clicks_org) sli_clicks_org,
           sum(orders_sold_org) orders_sold_org,
           count(distinct eventdate) number_of_days,
           sum(total_groupon_rev) total_groupon_rev,
           sum(citrus_impressions) citrus_impressions,
           sum(citrus_clicks) citrus_clicks
           from
       grp_gdoop_bizops_db.np_sl_performance_base
       group by 
           date_sub(next_day(eventdate, 'MON'), 1),
           dealuuid, 
           coupon;


select * from grp_gdoop_bizops_db.np_sl_roas_final_deal where date_start_end >= '2021-08-01';


select * from ad_reporting_na_prod.citrus_master_report_citrus_51_v1;



drop table grp_gdoop_bizops_db.np_sl_min_deal;
create table grp_gdoop_bizops_db.np_sl_min_deal stored as orc as
select 
     done.*, 
     case when live_days_on_sl <= diff_btw_sl_min_groupon_min and live_days_on_sl < 30 then live_days_on_sl
          when diff_btw_sl_min_groupon_min <= live_days_on_sl and diff_btw_sl_min_groupon_min < 30 then diff_btw_sl_min_groupon_min
          else 30
          end number_of_days_comparison
from
(select 
     fin.*, 
     datediff(to_date(fin.max_live_date), to_date(fin.min_launch_date)) live_days_on_sl, 
     fin2.min_live_on_groupon, 
     datediff(to_date(fin.min_launch_date), to_date(fin2.min_live_on_groupon)) diff_btw_sl_min_groupon_min
from 
(select
     a.dealuuid,
     max(b.merchant_uuid) merchant_uuid,
     cast(min(eventdate) as date) min_launch_date, 
     cast(max(eventdate) as date) max_live_date
  from grp_gdoop_bizops_db.np_sl_all_deals as a
  left join grp_gdoop_bizops_db.pai_deals as b on a.dealuuid = b.deal_uuid
  group by a.dealuuid) as fin
left join 
(select deal_uuid, min(load_date) min_live_on_groupon from user_groupondw.active_deals group by deal_uuid) as fin2 on fin.dealuuid = fin2.deal_uuid
) as done
;



create table  grp_gdoop_bizops_db.np_impressions_sl_pre stored as orc as
   select
      case when cl.aogcampaignid is not null then 'Sponsored' else 'Organic' end deal_landing,
      trim(lower(regexp_replace(search_query,'\\+',' '))) search_query,
      position,
      ogp,
      nor,
      nob,
      dv_time,
      bcookie,
      order_uuid,
      cl.dealuuid,
      clientplatform,
      aogcampaignid,
      '' extrainfo,
      case when rawpagetype = 'browse/deals/index' and fullurl like '%context=local%' or fullurl like '%category=%' then 'browse'
           when rawpagetype in ('browse/deals/index') and search_query is not null  and search_query!='' then 'search'
           when rawpagetype in ('homepage' ,'homepage/index', 'featured/deals/index') then 'homepage'
           when rawpagetype in ( 'browse/deals/index') then 'browse' --'featured'
           when rawpagetype in ('nearby/deals/index', --featured? or local only
                                 'goods/browse/index',
                                 'goods/index',
                                 'giftshop/deals/show',
                                 'giftshop/deals/index',
                                  'channels/show',
                                  'beautynow_promoted',
                                  'beautynow_salon',
                                  'beautynow_appointment_receipt',
                                  'beautynow_SELECT_appointment_time',
                                  'beautynow_SELECT_service') then 'browse'
           when rawpagetype like '%-%-%-%' then 'occasions'
           else rawpagetype
           end page,
        cl.eventdate
     from ai_reporting.sl_imp_clicks cl
     join (select dealuuid, min(eventdate) min_eventdate, max(eventdate) max_eventdate
           from grp_gdoop_bizops_db.np_sl_all_deals
           group by dealuuid
           ) dl on cl.dealuuid = dl.dealuuid
     where
        cast(cl.eventdate as date) < cast(dl.min_eventdate as date);


create table  grp_gdoop_bizops_db.np_impressions_slapp_pre stored as orc as
   select
          case when cl.aogcampaignid is not null then 'Sponsored' else 'Organic' end deal_landing,
          trim(lower(regexp_replace(search_query,'\\+',' '))) search_query,
          cast(cast(position as int )-1 as string) position ,
          ogp,
          nor,
          nob,
          dv_time,
          bcookie,
          order_uuid,
          cl.dealuuid,
          clientplatform,
          aogcampaignid,
          extrainfo,
          case when search_query is not null and search_query!='' and search_query!='All+Deals' then 'search'
               when (get_json_object(lower(extrainfo), '$.type' ) is null and get_json_object(extrainfo, '$.tabName' ) ='home_tab') then 'homepage' ---or channel='all'
               when rawpagetype in ('wolfhound_mobile_page', 'GlobalSearchResult', 'MapLedSearch') then 'browse'
               else 'other' end page,
          cl.eventdate
     from ai_reporting.sl_imp_clicks_app  cl
     join (select dealuuid, min(eventdate) min_eventdate, max(eventdate) max_eventdate
           from grp_gdoop_bizops_db.np_sl_all_deals
           group by dealuuid
           ) dl on cl.dealuuid = dl.dealuuid
     where
        cast(cl.eventdate as date) < cast(dl.min_eventdate as date);





drop table grp_gdoop_bizops_db.np_tab_case_study;
create table grp_gdoop_bizops_db.np_tab_case_study stored as orc as 
select
     fin.eventdate,
     fin.deal_landing,
     fin.timeline,
     dl_sl.min_launch_date,
     dl_sl.max_live_date,
     dl_sl.live_days_on_sl,
     dl_sl.min_live_on_groupon, 
     dl_sl.diff_btw_sl_min_groupon_min,
     fin.dealuuid,
     dl_sl.merchant_uuid,
     fin.position,
     count(1) impressions,
     count(distinct aogcampaignid) impressions_aog,
     count(distinct dv_time) clicks,
     count(distinct bcookie) distinct_users, 
     sum(nob-nor) merch_revenue, 
     count(distinct order_uuid) total_orders   
from
  (select a.*, 'post' timeline
    from grp_gdoop_bizops_db.np_impressions_sl as a
  union all
  select a.*, 'post' timeline
    from grp_gdoop_bizops_db.np_impressions_sl_app as a
  union all
   select a.*, 'pre' timeline
    from grp_gdoop_bizops_db.np_impressions_sl_pre as a
         where cast(eventdate as date) >= cast('2020-01-01' as date)
     union all
  select a.*, 'pre' timeline
    from grp_gdoop_bizops_db.np_impressions_slapp_pre as a
          where cast(eventdate as date) >= cast('2020-01-01' as date)
          ) as fin
    join grp_gdoop_bizops_db.np_sl_min_deal as dl_sl on fin.dealuuid = dl_sl.dealuuid
group by 
    fin.eventdate,
    fin.deal_landing,
    fin.timeline,
    dl_sl.min_launch_date,
    dl_sl.max_live_date,
    dl_sl.live_days_on_sl,
    dl_sl.min_live_on_groupon, 
    dl_sl.diff_btw_sl_min_groupon_min,
    fin.dealuuid,
    dl_sl.merchant_uuid,
    fin.position
;



drop table grp_gdoop_bizops_db.np_sl_tab_perf_comp;
create table grp_gdoop_bizops_db.np_sl_tab_perf_comp stored as orc as 
select
     dl_sl.min_launch_date,
     dl_sl.max_live_date,
     dl_sl.live_days_on_sl,
     dl_sl.min_live_on_groupon, 
     dl_sl.number_of_days_comparison,
     dl_sl.diff_btw_sl_min_groupon_min,
     dl_sl.merchant_uuid,
     fin.dealuuid,
     fin.position,
     fin.eventdate,
     fin.deal_landing,
     fin.timeline,
     count(1) impressions,
     count(distinct aogcampaignid) impressions_aog,
     count(distinct dv_time) clicks,
     count(distinct bcookie) distinct_users, 
     sum(nob-nor) merch_revenue, 
     count(distinct order_uuid) total_orders   
from
   grp_gdoop_bizops_db.np_sl_min_deal as dl_sl
left join
  (select a.*, 'post' timeline
    from grp_gdoop_bizops_db.np_impressions_sl as a
  union all
  select a.*, 'post' timeline
    from grp_gdoop_bizops_db.np_impressions_sl_app as a
  union all
   select a.*, 'pre' timeline
    from grp_gdoop_bizops_db.np_impressions_sl_pre as a
         where cast(eventdate as date) >= cast('2020-01-01' as date)
     union all
  select a.*, 'pre' timeline
    from grp_gdoop_bizops_db.np_impressions_slapp_pre as a
          where cast(eventdate as date) >= cast('2020-01-01' as date)
          ) as fin on fin.dealuuid = dl_sl.dealuuid
group by 
    fin.eventdate,
    fin.deal_landing,
    fin.timeline,
    dl_sl.min_launch_date,
    dl_sl.max_live_date,
    dl_sl.live_days_on_sl,
    dl_sl.min_live_on_groupon, 
    dl_sl.diff_btw_sl_min_groupon_min,
    dl_sl.number_of_days_comparison,
    fin.dealuuid,
    dl_sl.merchant_uuid,
    fin.position
;


select 
       dealuuid,
       min_launch_date,
       min_live_on_groupon,
       live_days_on_sl,
       diff_btw_sl_min_groupon_min,
       sum(case when timeline = 'pre' then total_imps end)  total_imps_pre, 
       sum(case when timeline = 'post' and deal_landing = 'Organic' then total_imps end) total_imps_post_org, 
       sum(case when timeline = 'post' and deal_landing = 'Sponsored' then total_imps end) total_imps_post_sl,
       sum(case when timeline = 'pre' then total_clicks end)  total_clicks_pre, 
       sum(case when timeline = 'post' and deal_landing = 'Organic' then total_clicks end) total_clicks_post_org, 
       sum(case when timeline = 'post' and deal_landing = 'Sponsored' then total_clicks end) total_clicks_post_sl,
       sum(case when timeline = 'pre' then merch_revenue end)  merch_revenue_pre, 
       sum(case when timeline = 'post' and deal_landing = 'Organic' then merch_revenue end) merch_revenue_post_org, 
       sum(case when timeline = 'post' and deal_landing = 'Sponsored' then merch_revenue end) merch_revenue_post_sl,
       sum(case when timeline = 'pre' then total_orders end)  total_orders_pre, 
       sum(case when timeline = 'post' and deal_landing = 'Organic' then total_orders end) total_orders_post_org, 
       sum(case when timeline = 'post' and deal_landing = 'Sponsored' then total_orders end) total_orders_post_sl
from 
(select 
       dealuuid, 
       eventdate, 
       min_launch_date, 
       max_live_date,
       min_live_on_groupon,
       live_days_on_sl,
       diff_btw_sl_min_groupon_min,
       case when live_days_on_sl <= diff_btw_sl_min_groupon_min and live_days_on_sl < 30 then live_days_on_sl
          when diff_btw_sl_min_groupon_min <= live_days_on_sl and diff_btw_sl_min_groupon_min < 30 then diff_btw_sl_min_groupon_min
          else 30
          end number_of_days_comparison,
       deal_landing,  
       timeline, 
       position, 
       sum(impressions) total_imps, 
       sum(clicks) total_clicks,
       sum(merch_revenue) merch_revenue, 
       sum(total_orders) total_orders
from 
    grp_gdoop_bizops_db.np_sl_tab_perf_comp
where 
     cast("position" as integer) <= 20
group by 
     dealuuid, 
       eventdate, 
       min_launch_date, 
       max_live_date,
       min_live_on_groupon,
       live_days_on_sl,
       diff_btw_sl_min_groupon_min,
       case when live_days_on_sl <= diff_btw_sl_min_groupon_min and live_days_on_sl < 30 then live_days_on_sl
          when diff_btw_sl_min_groupon_min <= live_days_on_sl and diff_btw_sl_min_groupon_min < 30 then diff_btw_sl_min_groupon_min
          else 30
          end,
       deal_landing,  
       timeline, 
       position
) as fin
where
    cast(eventdate as date) <= date_add('day', number_of_days_comparison, cast(min_launch_date as date)) 
and 
    cast(eventdate as date) >= date_add('day', - number_of_days_comparison , cast(min_launch_date as date))
group by 
    dealuuid,
       min_launch_date,
       min_live_on_groupon,
       live_days_on_sl,
       diff_btw_sl_min_groupon_min;
      
2021-06-15

select 
    count(1)
from 
(select 
    dealuuid, 
    eventdate,
    position, 
    sum(cnz) cnz
from
	(select dealuuid, eventdate, position ,count(1) cnz 
	        from ai_reporting.sl_imp_clicks
	        where dealuuid = '2466d980-7007-43e2-8373-cddfaed5677e' and cast(eventdate as date) <= cast('2021-08-20' as date) and cast(eventdate as date) >= cast('2021-06-20' as date)
	        group by 1,2,3
	union all
	select dealuuid, eventdate, position ,count(1) cnz 
	       from ai_reporting.sl_imp_clicks_app
	       where dealuuid = '2466d980-7007-43e2-8373-cddfaed5677e' and cast(eventdate as date) <= cast('2021-08-20' as date) and cast(eventdate as date) >= cast('2021-06-20' as date)
	       group by 1,2,3) as fin 
group by 1,2,3
order by 2,3)
;



select * from grp_gdoop_bizops_db.np_sl_min_deal 
where dealuuid = 'b6b2b4df-0749-417d-94cf-e60637841969';


select 
    a.dealuuid, 
    deal_permalink, 
    a.min_launch_date, 
    b.min_launch_date, 
    b.max_live_date, 
    b.min_live_on_groupon, 
    b.number_of_days_comparison,
    total_imps_pre, 
    total_imps_post_sl, 
    total_imps_post_org
from grp_gdoop_bizops_db.np_sl_tab_comp_input as a 
left join grp_gdoop_bizops_db.np_sl_min_deal  as b on a.dealuuid = b.dealuuid
where total_imps_post_sl >= 40 and a.min_launch_date >= cast('2021-06-01' as date) and b.number_of_days_comparison >= 15
order by total_imps_pre
;


select 
    dealuuid, 
    deal_landing, 
    timeline, 
    case when cast("position" as integer) <= 5 then 'position <=5'
            when cast("position" as integer) <= 15 then 'position <=15'
            when cast("position" as integer) <= 30 then 'position <= 30'
            when cast("position" as integer) <= 50 then 'position <=50'
            else 'position > 50' end position_case, 
    number_of_days_comparison,
    sum(total_imps) impressions
from 
(select 
       dealuuid, 
       eventdate, 
       min_launch_date, 
       max_live_date,
       min_live_on_groupon,
       live_days_on_sl,
       diff_btw_sl_min_groupon_min,
       case when live_days_on_sl <= diff_btw_sl_min_groupon_min and live_days_on_sl < 30 then live_days_on_sl
          when diff_btw_sl_min_groupon_min <= live_days_on_sl and diff_btw_sl_min_groupon_min < 30 then diff_btw_sl_min_groupon_min
          else 30
          end number_of_days_comparison,
       deal_landing,  
       timeline, 
       position, 
       sum(impressions) total_imps, 
       sum(clicks) total_clicks,
       sum(merch_revenue) merch_revenue, 
       sum(total_orders) total_orders
from 
    grp_gdoop_bizops_db.np_sl_tab_perf_comp
where 
    dealuuid = 'bce7495b-8947-4414-9c8b-3f4c7e72d865'
group by 
     dealuuid, 
       eventdate, 
       min_launch_date, 
       max_live_date,
       min_live_on_groupon,
       live_days_on_sl,
       diff_btw_sl_min_groupon_min,
       case when live_days_on_sl <= diff_btw_sl_min_groupon_min and live_days_on_sl < 30 then live_days_on_sl
          when diff_btw_sl_min_groupon_min <= live_days_on_sl and diff_btw_sl_min_groupon_min < 30 then diff_btw_sl_min_groupon_min
          else 30
          end,
       deal_landing,  
       timeline, 
       position
) as fin
where
    cast(eventdate as date) <= date_add('day', number_of_days_comparison, cast(min_launch_date as date)) 
and 
    cast(eventdate as date) >= date_add('day', - number_of_days_comparison , cast(min_launch_date as date))
group by 
    1,2,3,4,5
order by 1,2,3,4;
   

   




drop table grp_gdoop_bizops_db.np_sl_tab_comp_input;
create table grp_gdoop_bizops_db.np_sl_tab_comp_input stored as orc as 
select 
       dealuuid,
       b.deal_permalink, 
       min_launch_date,
       min_live_on_groupon,
       live_days_on_sl,
       diff_btw_sl_min_groupon_min,
       number_of_days_comparison,
       sum(case when timeline = 'pre' then total_imps end)  total_imps_pre, 
       sum(case when timeline = 'post' and deal_landing = 'Organic' then total_imps end) total_imps_post_org, 
       sum(case when timeline = 'post' and deal_landing = 'Sponsored' then total_imps end) total_imps_post_sl,
       sum(case when timeline = 'pre' then total_clicks end)  total_clicks_pre, 
       sum(case when timeline = 'post' and deal_landing = 'Organic' then total_clicks end) total_clicks_post_org, 
       sum(case when timeline = 'post' and deal_landing = 'Sponsored' then total_clicks end) total_clicks_post_sl,
       sum(case when timeline = 'pre' then merch_revenue end)  merch_revenue_pre, 
       sum(case when timeline = 'post' and deal_landing = 'Organic' then merch_revenue end) merch_revenue_post_org, 
       sum(case when timeline = 'post' and deal_landing = 'Sponsored' then merch_revenue end) merch_revenue_post_sl,
       sum(case when timeline = 'pre' then total_orders end)  total_orders_pre, 
       sum(case when timeline = 'post' and deal_landing = 'Organic' then total_orders end) total_orders_post_org, 
       sum(case when timeline = 'post' and deal_landing = 'Sponsored' then total_orders end) total_orders_post_sl
from
(select
       dealuuid,
       eventdate,
       min_launch_date, 
       max_live_date,
       min_live_on_groupon,
       live_days_on_sl,
       diff_btw_sl_min_groupon_min,
       number_of_days_comparison,
       deal_landing, 
       timeline,
       position,
       sum(impressions) total_imps,
       sum(clicks) total_clicks,
       sum(merch_revenue) merch_revenue,
       sum(total_orders) total_orders
from 
    grp_gdoop_bizops_db.np_sl_tab_perf_comp
where 
     cast(position as int) <= 20
group by 
     dealuuid, 
       eventdate, 
       min_launch_date, 
       max_live_date,
       min_live_on_groupon,
       live_days_on_sl,
       diff_btw_sl_min_groupon_min,
       number_of_days_comparison,
       deal_landing,  
       timeline, 
       position
) as fin
left join (select deal_id, max(deal_permalink) deal_permalink from user_edwprod.dim_gbl_deal_lob group by deal_id) as b on fin.dealuuid = b.deal_id
where
    cast(eventdate as date) <= date_add(cast(min_launch_date as date), number_of_days_comparison) 
and 
    cast(eventdate as date) >= date_add(cast(min_launch_date as date), - number_of_days_comparison)
group by 
    dealuuid,
    b.deal_permalink,
       min_launch_date,
       min_live_on_groupon,
       live_days_on_sl,
       diff_btw_sl_min_groupon_min,
       number_of_days_comparison
;


select 
       dealuuid, 
       min_launch_date,
       deal_landing,  
       timeline, 
       position, 
       sum(total_imps) total_imps, 
       sum(total_clicks) total_clicks,
       sum(merch_revenue) merch_revenue, 
       sum(total_orders) total_orders
from 
(select dealuuid, 
       eventdate, 
       min_launch_date, 
       max_live_date,
       case when live_days_on_sl < 30 then live_days_on_sl else 30 end date_diff_actual,
       deal_landing,  
       timeline, 
       position, 
       sum(impressions) total_imps, 
       sum(clicks) total_clicks,
       sum(merch_revenue) merch_revenue, 
       sum(total_orders) total_orders
from grp_gdoop_bizops_db.np_tab_case_study
where 
     cast(position as integer) <= 20
group by 1,2,3,4,5,6,7,8
) as fin
where
    cast(eventdate as date) <= date_add('day', date_diff_actual, cast(min_launch_date as date)) 
and 
    cast(eventdate as date) >= date_add('day', - date_diff_actual , cast(min_launch_date as date))
group by 1,2,3,4,5
;


select 
    a.*, 
    b.adspend, 
    b.orders_rev, 
    b.total_ords, 
    b.total_imps,
    b.total_clicks
from 
(select 
       dealuuid,
       eventdate,
       sum(total_imps) total_imps,
       sum(total_clicks) total_clicks,
       sum(merch_revenue) merch_revenue,
       sum(total_orders) total_orders
from 
(select dealuuid, 
       eventdate, 
       min_launch_date, 
       max_live_date,
       case when live_days_on_sl < 30 then live_days_on_sl else 30 end date_diff_actual,
       deal_landing,
       timeline, 
       position, 
       sum(impressions) total_imps, 
       sum(clicks) total_clicks,
       sum(merch_revenue) merch_revenue, 
       sum(total_orders) total_orders
from grp_gdoop_bizops_db.np_tab_case_study
where 
     cast(position as integer) <= 20 and timeline = 'post' and deal_landing = 'Sponsored'
group by 1,2,3,4,5,6,7,8
) as fin
where
    cast(eventdate as date) <= date_add('day', date_diff_actual, cast(min_launch_date as date)) 
and 
    cast(eventdate as date) >= date_add('day', - date_diff_actual , cast(min_launch_date as date))
group by 1,2) as a 
left join grp_gdoop_bizops_db.np_sl_performance_met as b on a.dealuuid = b.deal_id and cast(a.eventdate as date) = cast(b.report_date as date);


select * from grp_gdoop_bizops_db.np_sl_performance_met;
select * from grp_gdoop_bizops_db.np_tab_case_study;


--------TD Query

use grp_gdoop_bizops_db;
create temporary table np_sl_performance_met_temp (
report_date string,
deal_id string,
total_imps int,
total_clicks int, 
total_ords int,
adspend double,
orders_rev double,
ss_ctr double,
ss_cnv double, 
roas double
) row format delimited
fields terminated by ','
lines terminated by '\n';LOAD DATA INPATH 'hdfs://cerebro-namenode/user/grp_gdoop_BizOps/pg_orders/np_sl_performance_met_temp.csv'
OVERWRITE INTO TABLE grp_gdoop_bizops_db.np_sl_performance_met_temp;
drop table if exists grp_gdoop_bizops_db.np_sl_performance_met;
create table grp_gdoop_bizops_db.np_sl_performance_met stored as orc as
select * from grp_gdoop_bizops_db.np_sl_performance_met_temp




/*SELECT 
      cast(report_date as date) report_date,
      deal_id,
      sum(impressions) total_imps, 
      sum(clicks) total_clicks, 
      sum(conversions) total_ords, 
      sum(total_spend_amount) adspend,
      sum(price_with_discount) orders_rev, 
      cast(total_clicks as float)/NULLIFZERO(total_imps) ss_ctr, 
      cast(total_ords as float)/NULLIFZERO(total_clicks) ss_cnv, 
      cast(orders_rev as float)/NULLIFZERO(adspend) roas
FROM sandbox.np_ss_performance_met
group by 1,2;
*
*/    








select dealuuid, 
       eventdate, 
       min_launch_date, 
       max_live_date,
       case when live_days_on_sl < 30 then live_days_on_sl else 30 end date_diff_actual,
       deal_landing,  
       timeline, 
       position, 
       sum(impressions) total_imps, 
       sum(clicks) total_clicks,
       sum(merch_revenue) merch_revenue, 
       sum(total_orders) total_orders
from grp_gdoop_bizops_db.np_tab_case_study 
where 
     cast(position as integer) <= 20
     and
     cast(eventdate as date) <= date_add('day', 45 , cast(min_launch_date as date)) 
     and 
     cast(eventdate as date) >= date_add('day', -45 , cast(min_launch_date as date))
group by 1,2,3,4,5,6,7,8;
      




select * from grp_gdoop_bizops_db.np_sl_min_deal
where dealuuid 
      in 
      ('2888a2c3-5602-468b-b48b-486461661ccf',
       'c429c8aa-b166-4798-bf59-20a2e670b0f2',
       'd9c59ead-6ca3-48b0-b20b-889276246b80',
       '27931cc6-8c96-4a0a-8962-13e1b1eadb63',
       '3c727a84-4d32-4c1a-9ae2-fb3584d5cb9f')
order by 1;



     
     
     
     
select * from grp_gdoop_bizops_db.np_sl_all_deals where dealuuid = '6812e2d8-ae32-48eb-ac8c-9fa6baf19512' order by eventdate;
select eventdate from ai_reporting.sl_imp_clicks where dealuuid = '6812e2d8-ae32-48eb-ac8c-9fa6baf19512' and aogcampaignid is not null order by eventdate;
select min(eventdate) from grp_gdoop_bizops_db.np_impressions_sl where dealuuid = '6812e2d8-ae32-48eb-ac8c-9fa6baf19512';
select min(eventdate) from grp_gdoop_bizops_db.np_impressions_sl_app where dealuuid = '6812e2d8-ae32-48eb-ac8c-9fa6baf19512';
select max(eventdate) from grp_gdoop_bizops_db.np_impressions_sl_pre where dealuuid = '2888a2c3-5602-468b-b48b-486461661ccf';
select *from grp_gdoop_bizops_db.np_impressions_sl_pre where deal_landing <> 'Organic';




select dealuuid, min(eventdate) min_eventdate, max(eventdate) max_eventdate
from grp_gdoop_bizops_db.np_sl_all_deals
where dealuuid = '2888a2c3-5602-468b-b48b-486461661ccf'
group by dealuuid;



     case when cast(fin.eventdate as date) >= cast(dl_sl.min_launch_date as date) 
          and cast(fin.eventdate as date) <= date_add('day', 30 , cast(dl_sl.min_launch_date as date)) 
          then 1 else 0 end after_30_days_launch,
     case when cast(fin.eventdate as date) < cast(dl_sl.min_launch_date as date) 
          and cast(fin.eventdate as date) >= date_add('day', -30 , cast(dl_sl.min_launch_date as date)) 
          then 1 else 0 end before_30_day_launch,
       
----------------
select
    case when consumeridsource is null then 'null' else consumeridsource end consumeridsou,
    case when userbrowserid is not null then 1 else 0 end ubidid_not_null,
    case when consumerid is not null then 1 else 0 end consumerid_not_null,
    case when merchantid is not null then 1 else 0 end merchantid_not_null,
    count(1)
from
    grp_gdoop_pde.junoHourly
where
    eventdate >= '2021-07-15'
    and clientplatform in ('web','Touch')
    and eventDestination = 'other'
    and event = 'merchantPageView'
    and country = 'US'
    and pageapp = 'sponsored-campaign-itier'
group by 1,2,3,4
;


select
    case when merchantid is not null then 1 else 0 end merchantid_not_null,
    count(1)
from
    grp_gdoop_pde.junoHourly
where
    eventdate >= '2021-07-15'
    and clientplatform in ('web','Touch')
    and eventDestination = 'other'
    and event = 'merchantPageView'
    and country = 'US'
    and pageapp = 'sponsored-campaign-itier'
group by 1
;

select
*
   from
    grp_gdoop_pde.junoHourly
where
    eventdate >= '2021-08-15'
    and clientplatform in ('web','Touch')
    and eventDestination = 'other'
    and event = 'merchantPageView'
    and country = 'US'
    and pageapp = 'sponsored-campaign-itier'
;

select * from grp_gdoop_bizops_db.np_ss_sl_user_granular2;


select * from grp_gdoop_bizops_db.pai_merchants where merchant_uuid = 'a80ff25d-8b08-4600-b5cf-6d5441be2f46';


select * from grp_gdoop_bizops_db.np_ss_sl_user_granular2 where merchantid = 'e1aec7a1-f15b-483c-a75f-15bd8b1102e8' order by eventdate, row_num_rank;



select a.* from grp_gdoop_bizops_db.np_ss_sl_user_granular2 as a
join grp_gdoop_bizops_db.np_temp_merch_ss_sl as b on a.merchantid = b.merchantid and a.eventdate = b.eventdate and b.row_num_max_cat = 'more than 50'
order by eventdate, merchantid, row_num_rank;

select b.row_num_max_cat ,b.eventweek, a.merchantid, rawpagetype, count(1) from grp_gdoop_bizops_db.np_ss_sl_user_granular2 as a
join grp_gdoop_bizops_db.np_temp_merch_ss_sl as b on a.merchantid = b.merchantid and a.eventdate = b.eventdate
group by b.row_num_max_cat, b.eventweek, a.merchantid, rawpagetype
order by b.row_num_max_cat, b.eventweek, a.merchantid;


create table grp_gdoop_bizops_db.np_ss_sl_user_granular stored as orc as
select
    consumerid,
    merchantid,
    consumeridsource,
    rawpagetype,
    eventtime,
    eventdate,
    ROW_NUMBER () over ( partition by merchantid, eventdate order by eventtime asc) row_num_rank
from
    grp_gdoop_pde.junoHourly
where
    eventdate >= '2021-07-15'
    and clientplatform in ('web','Touch')
    and eventDestination = 'other'
    and event = 'merchantPageView'
    and country = 'US'
    and pageapp = 'sponsored-campaign-itier'
    and merchantid is not null
;

create table grp_gdoop_bizops_db.np_ss_sl_user_granular2 stored as orc as
select
    consumerid,
    merchantid,
    consumeridsource,
    rawpagetype,
    eventtime,
    eventdate,
    ROW_NUMBER () over ( partition by consumeridsource,merchantid, eventdate order by eventtime asc) row_num_rank
from
    grp_gdoop_pde.junoHourly
where
    eventdate >= '2021-07-15'
    and clientplatform in ('web','Touch')
    and eventDestination = 'other'
    and event = 'merchantPageView'
    and country = 'US'
    and pageapp = 'sponsored-campaign-itier'
    and merchantid is not null
;
