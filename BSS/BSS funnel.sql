create table grp_gdoop_bizops_db.nvp_bss_funnel2 stored as orc as
select q.merchant_uuid,
    case when p.created is null then 'No' else 'Yes' end as onboarded,
    case when p.created is null then '' else p.created end as onboarded_at,
    concat('https://www.groupon.com/deals/',d.groupon_permalink) as deal_link,
    d.gapp_enabled,
    d.gapp_registered
from grp_gdoop_bizops_db.sh_bt_questionnaires q
left join grp_gdoop_bizops_db.sh_bt_partners p on p.merchant_uuid = q.merchant_uuid
left join grp_gdoop_bizops_db.sh_bt_deals d on p.id = d.partners_id and p.country = d.country

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


insert overwrite table grp_gdoop_bizops_db.nvp_jouno_btimp_st1 partition (eventyear, eventmonth);

select 
    *
from
    grp_gdoop_pde.junoHourly
where eventdate >= '2021-01-01'
    and  widgetname ='bss_hub_hours_and_staff_view'
;






drop table sandbox.nvp_bss_funnel;
CREATE MULTISET TABLE sandbox.nvp_bss_funnel,NO FALLBACK ,
      NO BEFORE JOURNAL,
      NO AFTER JOURNAL,
      CHECKSUM = DEFAULT,
      DEFAULT MERGEBLOCKRATIO,
      MAP = TD_MAP1
      (
        merchant_uuid VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
        onboarded integer,
        onboarded_at VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
        deal_uuid VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC,
        deal_link VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,
        gapp_enabled integer,
        gapp_registered VARCHAR(64) CHARACTER SET LATIN NOT CASESPECIFIC
      )
 NO PRIMARY INDEX;


drop table grp_gdoop_bizops_db.nvp_bss_funnel;
create table grp_gdoop_bizops_db.nvp_bss_funnel stored as orc as
select
    q.merchant_uuid,
    max(case when p.created is null then 0 else 1 end) as onboarded,
    max(case when p.created is null then '' else p.created end) as onboarded_at,
    d.groupon_real_deal_uuid deal_uuid,
    max(concat('https://www.groupon.com/deals/',d.groupon_permalink)) as deal_link,
    max(d.gapp_enabled) gapp_enabled,
    max(d.gapp_registered) gapp_registered
from grp_gdoop_bizops_db.sh_bt_questionnaires q
left join grp_gdoop_bizops_db.sh_bt_partners p on p.merchant_uuid = q.merchant_uuid
left join grp_gdoop_bizops_db.sh_bt_deals d on p.id = d.partners_id and p.country = d.country
group by 
  q.merchant_uuid,
  d.groupon_real_deal_uuid
;



create volatile table nvp_live_merchs as (
select 
   b.merchant_uuid, 
   a.deal_uuid, 
   a.has_gcal, 
   gdl.grt_l2_cat_description l2, 
   gdl.country_code
   from 
  (select 
   deal_uuid, 
   max(has_gcal) has_gcal
   from 
   sandbox.sh_bt_active_deals_log_v4 
   where product_is_active_flag = 1
   and partner_inactive_flag = 0
   group by 1) as a 
   join
    user_edwprod.dim_offer_ext as b on a.deal_uuid = b.product_uuid
   join 
    user_edwprod.dim_gbl_deal_lob as gdl on a.deal_uuid = gdl.deal_id
   ) with data unique primary index (merchant_uuid,deal_uuid) on commit preserve rows;
  

delete from sandbox.nvp_bss_tableau;
insert into sandbox.nvp_bss_tableau
select 
  merch_info.*, 
  gdl.grt_l2_cat_description l2,
  gdl.country_code, 
  b.has_gcal, 
  sf.account_name, 
  sf.account_owner,
  sf.account_id
  from 
  (select 
       a.merchant_uuid,  
       a.onboarded, 
       a.onboarded_at, 
       COALESCE(a.deal_uuid, dim.deal_uuid2) deal_uuid, 
       case when a.deal_uuid is null then concat('https://www.groupon.com/deals/',dim.deal_uuid2) else a.deal_link end as deal_link, 
       a.gapp_enabled, 
       a.gapp_registered
      from sandbox.nvp_bss_funnel as a 
      left join (select merchant_uuid, max(product_uuid) deal_uuid2 from user_edwprod.dim_offer_ext group by 1) 
           as dim on a.merchant_uuid = dim.merchant_uuid) as merch_info
  left join 
   nvp_live_merchs as b on merch_info.deal_uuid = b.deal_uuid
  left join 
    user_edwprod.dim_gbl_deal_lob as gdl on merch_info.deal_uuid = gdl.deal_id
  left join (
        sel deal_uuid,
            max(o1.id) opportunity_id,
            max(o1.division) division,
            max(sfa.name) account_name,
            max(full_name) account_owner,
            max(o1.accountid) account_id
        from user_edwprod.sf_opportunity_1 o1
        join user_edwprod.sf_opportunity_2 o2 on o1.id = o2.id
        join user_edwprod.sf_account sfa on o1.accountid = sfa.id
        left join user_groupondw.dim_sf_person sfp on sfa.ownerid = sfp.person_id
        group by 1
        ) sf on merch_info.deal_uuid = sf.deal_uuid
;
   
grant select on sandbox.nvp_bss_tableau to public;




drop table sandbox.nvp_bss_tableau;
create multiset table sandbox.nvp_bss_tableau as (
select 
  merch_info.*, 
  gdl.grt_l2_cat_description l2,
  gdl.country_code, 
  b.has_gcal, 
  sf.account_name, 
  sf.account_owner,
  sf.account_id
  from 
  (select 
       a.merchant_uuid,  
       a.onboarded, 
       a.onboarded_at, 
       COALESCE(a.deal_uuid, dim.deal_uuid2) deal_uuid, 
       case when a.deal_uuid is null then concat('https://www.groupon.com/deals/',dim.deal_uuid2) else a.deal_link end as deal_link, 
       a.gapp_enabled, 
       a.gapp_registered
      from sandbox.nvp_bss_funnel as a 
      left join (select merchant_uuid, max(product_uuid) deal_uuid2 from user_edwprod.dim_offer_ext group by 1) 
           as dim on a.merchant_uuid = dim.merchant_uuid) as merch_info
  left join 
   nvp_live_merchs as b on merch_info.deal_uuid = b.deal_uuid
  left join 
    user_edwprod.dim_gbl_deal_lob as gdl on merch_info.deal_uuid = gdl.deal_id
  left join (
        sel deal_uuid,
            max(o1.id) opportunity_id,
            max(o1.division) division,
            max(sfa.name) account_name,
            max(full_name) account_owner,
            max(o1.accountid) account_id
        from user_edwprod.sf_opportunity_1 o1
        join user_edwprod.sf_opportunity_2 o2 on o1.id = o2.id
        join user_edwprod.sf_account sfa on o1.accountid = sfa.id
        left join user_groupondw.dim_sf_person sfp on sfa.ownerid = sfp.person_id
        group by 1
        ) sf on merch_info.deal_uuid = sf.deal_uuid
) with data;


grant select on sandbox.nvp_bss_tableau to public;

select 
   cast(substr(onboarded_at, 1,10) as date),
count(distinct deal_uuid) deals_onboarded 
from sandbox.nvp_bss_funnel
group by 1 order by 1 desc;




