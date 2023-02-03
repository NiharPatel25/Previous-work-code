select 
   a.*, 
   b.*, 
   case when a.merchant_uuid = b.merchant_uuid_2 then 1 else 0 end 
from 
(select 
   doe.product_uuid, 
   merchant_uuid 
from user_edwprod.dim_offer_ext doe 
group by 1,2) as a 
left join 
(select 
   deal_uuid, 
   merchant_uuid merchant_uuid_2
from sandbox.pai_deals
) as b on a.product_uuid = b.deal_uuid
order by 5 asc;

select product_uuid, count(distinct merchant_uuid) xyz from user_edwprod.dim_offer_ext group by 1 having xyz >1;

select * from user_edwprod.dim_offer_ext doe where product_uuid  = '507ae437-187e-4ddc-8bc8-442e2fbd8654'

select * from np_postvetting_relationships_v where child_deal_id = '47a3f127-f85d-4647-b488-74eaf113bcd1';
select * from np_postvetting_relationships_v where parent_deal_id = 'b9115a19-36a5-4537-ac56-c1ff507fc3c6';



 FROM user_edwprod.sf_opportunity_1 as a 
    LEFT JOIN user_edwprod.sf_opportunity_2 b ON o1.ID = b.ID
    LEFT JOIN
      (SELECT salesforce_account_id AS account_id,
             SUBSTR(MAX(CONCAT(cast(updated_at as date), merchant_uuid)), 11) AS merchant_uuid,
             SUBSTR(MAX(CONCAT(cast(updated_at as date), name)), 11) AS merchant_name
       FROM user_edwprod.dim_merchants
       WHERE dwh_active = 1
       GROUP BY 1) dm1
    ON dm1.account_id = a.account_id;

select * from sandbox.pai_merchants;

COALESCE(dmp.primary_dealservice_cat_id, grt.pds_cat_id, grt3.pds_cat_id, '') AS pds_cat_id,---missing
         COALESCE(grt.pds_cat_name, o1.primary_deal_services) AS pds,
         o1.dmapi_flag,
         coalesce(grt.grt_l2_cat_name, grt3.grt_l2_cat_name) AS grt_l2_cat_name,
         coalesce(grt.grt_l1_cat_name, grt3.grt_l1_cat_name) AS grt_l1_cat_name,

ma.account_id AS accountid,
         ma.country_code,
         ma.deal_uuid,
         ma.opportunity_id,
         ma.closedate AS close_date,
         ma.opportunity_name,
         ma.launch_date,
         COALESCE(ma.division, dag.opp_division) AS division,
         COALESCE(ma.metal_at_close, 'Nickel') AS metal_at_close,
         COALESCE(day_prev_owner.billingcity, close_owner.billingcity) AS billingcity,
         ma.grt_l1_cat_name,
         ma.pds,
         ma.vertical,
         ma.close_order,
         ma.close_recency,
         CASE WHEN n_c.account_id IS NOT NULL THEN n_c.closedate ELSE NULL END AS next_close_on,
         nl.launch_order,
         CASE WHEN datediff(ma.deal_paused_date, CURRENT_DATE) < 2 THEN NULL ELSE ma.deal_paused_date END deal_paused_date,
         ma.stagename,
         ma.go_live_date,
         ma.Straight_to_Private_Sale,
         ma.dmapi_flag,
         COALESCE(day_prev_owner.title_rw, close_owner.title_rw) AS title_rw,
         COALESCE(day_prev_owner.team, close_owner.team) AS team
         



---drop table sandbox.np_opportunity_details_1;
---create multiset table sandbox.pai_opp_mtd_attrib as (;


delete from sandbox.pai_opp_mtd_attrib;
insert into sandbox.pai_opp_mtd_attrib
SELECT
         o1.country_code,
         COALESCE(o1.account_id, dm1.account_id, dm.account_id) AS account_id,
         COALESCE(dm1.merchant_uuid, dm.merchant_uuid) AS merchant_uuid,
         COALESCE(dm1.merchant_name, dm.merchant_name) AS merchant_name,
         o1.id,
         o2.deal_uuid,
         o1.opportunity_id,
         o1.closedate,
         o1.division,
         o1.opportunity_name,
         o1.go_live_date,
         o1.Straight_to_Private_Sale,
         o1.ownerid,
         o1.deal_attribute,
         o1.stagename,
         o1.dmapi_flag,
         o1.por_relaunch,
         o1.cloned_from,
         ad.launch_date,
         COALESCE(dmp.primary_dealservice_cat_id, grt.pds_cat_id) AS pds_cat_id,---missing
         COALESCE(grt.pds_cat_name, o1.primary_deal_services) AS primary_deal_services,
         grt.grt_l2_cat_name,
         grt.grt_l1_cat_name,
         
  FROM
    (SELECT   
			 CASE WHEN COALESCE(feature_country, 'US') = 'GB' THEN 'UK'
                  WHEN COALESCE(feature_country, 'US') IN ('VI', 'FM', 'PR', 'MH') THEN 'US'
                  ELSE COALESCE(feature_country, 'US')
             END  AS country_code,
             opportunity_id,
             closedate,
             accountid AS account_id,
             id,
             division,
             opportunity_name,
             primary_deal_services,
             go_live_date,
             Straight_to_Private_Sale,
             ownerid,
             deal_attribute,
             stagename AS stagename,
             CASE WHEN LOWER(opportunity_name) LIKE '%dmapi%' or LOWER(opportunity_name) LIKE '%*G1M*%' THEN 1 ELSE 0 END AS dmapi_flag,
             CASE
                WHEN (opportunity_name LIKE '%*POR RL W1*%' OR opportunity_name LIKE '%POR_%') THEN 1
                WHEN opportunity_name LIKE ('%*POR Wave 2a RL*%') THEN 1
                WHEN opportunity_name LIKE ('%*POR Wave 2b RL*%') THEN 1
                WHEN opportunity_name LIKE ('%*POR ULA RL*%') THEN 1
                WHEN opportunity_name LIKE ('%*POR Wave 3a RL*%') THEN 1
                WHEN opportunity_name LIKE ('%*POR Wave 3b RL*%') THEN 1
                WHEN opportunity_name LIKE ('%*POR Wave 3c RL*%') THEN 1
                WHEN opportunity_name LIKE ('%*POR Wave 3d RL*%') THEN 1
                WHEN opportunity_name LIKE ('%*POR Wave 3e RL*%') THEN 1
                WHEN opportunity_name LIKE ('%*POR Wave 4 RL*%') THEN 1
                WHEN opportunity_name LIKE ('%POR WAVE%') THEN 1
                ELSE 0 END AS por_relaunch,
             cloned_from
      FROM user_edwprod.sf_opportunity_1
      WHERE LENGTH(opportunity_id) = 15
      		AND opportunity_id IS NOT NULL
            AND LOWER(stagename) IN ('closed lost', 'closed won', 'merchant not interested')
            AND COALESCE(feature_country, 'US') IN ('AE', 'AU', 'BE', 'CA', 'DE', 'ES', 'FR', 'GB', 'IE', 'IT', 'NL', 'PL', 'QC', 'UK', 'US')
) o1
    LEFT JOIN (SELECT DISTINCT deal_uuid, Id FROM user_edwprod.sf_opportunity_2 WHERE LENGTH(opportunity_id) = 15 AND opportunity_id IS NOT NULL) o2 ON o1.ID = o2.ID
    LEFT JOIN (SELECT DISTINCT deal_uuid, primary_dealservice_cat_id FROM user_edwprod.deal_merch_product) dmp ON dmp.deal_uuid = o2.deal_uuid ----in mtd this is joined to active_deals
    LEFT JOIN user_dw.v_dim_pds_grt_map grt ON dmp.primary_dealservice_cat_id = grt.pds_cat_id
    ----LEFT JOIN dw.mv_dim_pds_grt_map grt3 ON o1.primary_deal_services = grt3.pds_cat_name looks like one to many
    LEFT JOIN user_edwprod.dim_offer_ext doe ON doe.product_uuid = o2.deal_uuid
    LEFT JOIN
	    (SELECT merchant_uuid,
	              SUBSTR(MAX(CONCAT(cast(updated_at as date), salesforce_account_id)), 11) AS account_id,
	              SUBSTR(MAX(CONCAT(cast(updated_at as date), name)), 11) AS merchant_name
	       FROM user_edwprod.dim_merchants
	       WHERE dwh_active = 1
	       GROUP BY 1
	    ) dm ON doe.merchant_uuid = dm.merchant_uuid
	LEFT JOIN
    (
      SELECT CASE WHEN COALESCE(country_code, 'US') = 'GB' THEN 'UK'
                  WHEN COALESCE(country_code, 'US') IN ('VI', 'FM', 'PR', 'MH') THEN 'US'
                  ELSE COALESCE(country_code, 'US')
             END AS country_code,
             deal_uuid,
             MIN(load_date) AS launch_date,
             MAX(load_date) AS deal_paused_date
      FROM user_groupondw.active_deals
      WHERE country_code IN ('AE', 'AU', 'BE', 'CA', 'DE', 'ES', 'FR', 'GB', 'IE', 'IT', 'NL', 'PL', 'QC', 'UK', 'US')
      GROUP BY 1,2
    ) ad ON o2.deal_uuid = ad.deal_uuid AND o1.country_code = ad.country_code
    LEFT JOIN
      (SELECT salesforce_account_id AS account_id,
             SUBSTR(MAX(CONCAT(cast(updated_at as date), merchant_uuid)), 11) AS merchant_uuid,
             SUBSTR(MAX(CONCAT(cast(updated_at as date), name)), 11) AS merchant_name
      FROM user_edwprod.dim_merchants
      WHERE dwh_active = 1
      GROUP BY 1
       ) dm1 ON dm1.account_id = COALESCE(o1.account_id, dm.account_id)
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23
;



drop table sandbox.np_opportunity_details_2;
create multiset table sandbox.np_opportunity_details_2

delete from sandbox.np_opportunity_details_2;
insert into sandbox.np_opportunity_details_2
select 
  distinct 
  o1.*, 
  o2.deal_uuid
from 
(SELECT   
			 CASE WHEN COALESCE(feature_country, 'US') = 'GB' THEN 'UK'
                  WHEN COALESCE(feature_country, 'US') IN ('VI', 'FM', 'PR', 'MH') THEN 'US'
                  ELSE COALESCE(feature_country, 'US')
             END  AS country_code,
             opportunity_id,
             accountid AS account_id,
             id,
             cloned_from
      FROM user_edwprod.sf_opportunity_1
      WHERE opportunity_id IS NOT NULL
) o1
    LEFT JOIN (SELECT DISTINCT deal_uuid, Id FROM user_edwprod.sf_opportunity_2 WHERE LENGTH(opportunity_id) = 15 AND opportunity_id IS NOT NULL) o2 ON o1.ID = o2.ID
;
    
SELECT   
			 CASE WHEN COALESCE(feature_country, 'US') = 'GB' THEN 'UK'
                  WHEN COALESCE(feature_country, 'US') IN ('VI', 'FM', 'PR', 'MH') THEN 'US'
                  ELSE COALESCE(feature_country, 'US')
             END  AS country_code,
             opportunity_id,
             accountid AS account_id,
             id,
             CASE WHEN LOWER(opportunity_name) LIKE '%dmapi%' or LOWER(opportunity_name) LIKE '%*G1M*%' THEN 1 ELSE 0 END AS dmapi_flag,
             CASE
                WHEN (opportunity_name LIKE '%*POR RL W1*%' OR opportunity_name LIKE '%POR_%') THEN 1
                WHEN opportunity_name LIKE ('%*POR Wave 2a RL*%') THEN 1
                WHEN opportunity_name LIKE ('%*POR Wave 2b RL*%') THEN 1
                WHEN opportunity_name LIKE ('%*POR ULA RL*%') THEN 1
                WHEN opportunity_name LIKE ('%*POR Wave 3a RL*%') THEN 1
                WHEN opportunity_name LIKE ('%*POR Wave 3b RL*%') THEN 1
                WHEN opportunity_name LIKE ('%*POR Wave 3c RL*%') THEN 1
                WHEN opportunity_name LIKE ('%*POR Wave 3d RL*%') THEN 1
                WHEN opportunity_name LIKE ('%*POR Wave 3e RL*%') THEN 1
                WHEN opportunity_name LIKE ('%*POR Wave 4 RL*%') THEN 1
                WHEN opportunity_name LIKE ('%POR WAVE%') THEN 1
                ELSE 0 END AS por_relaunch,
             cloned_from
      FROM user_edwprod.sf_opportunity_1
      WHERE opportunity_id IS NOT NULL
    
grant select on sandbox.pai_opp_mtd_attrib to public;
GRANT ALL ON sandbox.pai_opp_mtd_attrib TO abautista, nihpatel,akuthiala, jkerin, ub_bizops WITH GRANT OPTION;

grant select on sandbox.np_opportunity_details_2 to public;
GRANT ALL ON sandbox.np_opportunity_details_2 TO abautista, nihpatel,akuthiala, jkerin, ub_bizops WITH GRANT OPTION;



/*
 * select deal_uuid, count(1) xyz from sandbox.np_opportunity_details_2 group by 1 having xyz > 1;
select deal_uuid from sandbox.np_opportunity_details_2;

create volatile table jc_postvetting_relationships_v as (
    select
      a.deal_id as parent_deal_id
      , o1.opportunity_id as parent_opp_id
      , ivd.invalid_data__c as parent_invalid_data
      , mtd.deal_uuid as child_deal_id
      , mtd.opportunity_id as child_opp_id
      , ivd2.invalid_data__c as child_invalid_data
    from sb_merchant_experience.history_event a
    join user_edwprod.sf_opportunity_2 o2 on a.deal_id = o2.deal_uuid
    join user_edwprod.sf_opportunity_1 o1 on o2.id = o1.id
    join jc_deal_invalid_data_temp ivd on ivd.deal_uuid = a.deal_id
    -- join to child
    join user_edwprod.sf_opportunity_1 o12 on o12.cloned_from = o1.id
    join sandbox.jc_merchant_mtd_attrib mtd on o12.opportunity_id = mtd.opportunity_id
    join jc_deal_invalid_data_temp ivd2 on ivd2.deal_uuid = mtd.deal_uuid
    where event_type = 'POST_VETTING_STARTED' and a.event_date < current_date
  ) with data no primary index on commit preserve rows
;*/


-------PREVIOUS LOGIC:
/*create multiset volatile table np_postvet_temp as (
select 
    a.deal_id as parent_deal_id, 
    c.deal_uuid as child_deal_id 
from sb_merchant_experience.history_event a
join sandbox.np_opportunity_details_1 as b on a.deal_id = b.deal_uuid
join sandbox.np_opportunity_details_1 as c on c.cloned_from = b.id
where event_type = 'POST_VETTING_STARTED' and a.event_date < current_date
) with data on commit preserve rows
; */

----NEW LOGIC
/*create multiset volatile table np_postvet_temp0 as 
(select 
       history_data.jsonextractvalue('$.additionalInfo.deal.clonedFrom') as parent_deal_id,
	   deal_id as all_deals
          from sb_merchant_experience.history_event
          where event_type = 'DRAFT_DEAL_CREATION'
          and event_date < current_date
          group by 1,2
) with data on commit preserve rows;
create multiset volatile table np_postvet_temp0 as 
(select 
       history_data.jsonextractvalue('$.additionalInfo.deal.clonedFrom') as parent_deal_id,
	   deal_id as all_deals
          from sb_merchant_experience.history_event
          where event_type = 'DRAFT_DEAL_CREATION'
          and history_data.jsonextractvalue('$.additionalInfo.deal.clonedFrom') is null
          and event_date < current_date
          group by 1,2
) with data on commit preserve rows
**/

create multiset volatile table np_postvet_temp1 as 
(select 
       history_data.jsonextractvalue('$.additionalInfo.deal.clonedFrom') as parent_deal_id, 
       deal_id as child_deal_id
          from sb_merchant_experience.history_event
          where event_type = 'DRAFT_DEAL_CREATION'
          and history_data.jsonextractvalue('$.additionalInfo.deal.clonedFrom') is not null
          and event_date < current_date
          group by 1,2
) with data on commit preserve rows;

create multiset volatile table np_postvet_temp2 as (
select 
    a.deal_id as parent_deal_id, 
    c.deal_uuid as child_deal_id 
from sb_merchant_experience.history_event a
join sandbox.np_opportunity_details_2 as b on a.deal_id = b.deal_uuid
join sandbox.np_opportunity_details_2 as c on c.cloned_from = b.id
where event_type = 'POST_VETTING_STARTED' and a.event_date < current_date
) with data on commit preserve rows
;

create multiset volatile table np_postvet_temp3 as (
select 
   coalesce(b.parent_deal_id, a.parent_deal_id) parent_deal_id, 
   coalesce(b.child_deal_id, a.child_deal_id) child_deal_id,
   case when b.parent_deal_id is not null then 1 else 0 end submitted
from 
  np_postvet_temp1 as a 
  full outer join np_postvet_temp2 as b on a.parent_deal_id = b.parent_deal_id and a.child_deal_id = b.child_deal_id
) with data on commit preserve rows;

create multiset volatile table np_non_dupe as (
     select deal_id, 
       case when b.deal_uuid is not null then 1 else 0 end submitted
     from sb_merchant_experience.history_event as a
     left join sandbox.np_opportunity_details_2 as b on a.deal_id = b.deal_uuid
     where event_type = 'DRAFT_DEAL_CREATION'
          and history_data.jsonextractvalue('$.additionalInfo.deal.clonedFrom') is null
          and event_date < current_date
     group by 1,2
) with data on commit preserve rows;

create multiset volatile table np_non_dupe2 as (
	select 
		deal_id prime_deal_id, 
		deal_id parent_deal_id, 
		cast(null as varchar(36)) child_deal_id, 
		submitted
	from np_non_dupe
union all 
     select  
          coalesce(dupe.deal_id, a.parent_deal_id) prime_deal_id,
          coalesce(dupe.deal_id, a.parent_deal_id) parent_deal_id, 
  	      a.child_deal_id, 
  	      a.submitted
  	from 
  	   np_non_dupe dupe
  	 join      
  	   np_postvet_temp3 as a on dupe.deal_id = a.parent_deal_id
) with data on commit preserve rows;

----SUBMITTED CHILDS THAT BELONG TO THE SAME PARENT IS VERY RARE. 368/50000 ~ 0.6%
----SUBMITTED PARENTS NOT FOUND IN DRAFT MODE IS VERY LOW 50/50000. SUBMITTED PARENTS FOR CLONED NOT FOUND IN DRAFT PARENTS CLONED IS ALSO VERY LOW 300/50000


/*select parent_deal_id, count(distinct child_deal_id) xyz 
from np_postvet_temp2 group by 1 having xyz > 1;

select * from np_postvet_temp3 where parent_deal_id = '037255dc-df70-40b1-9e1a-7644c58408f4';

select 
   count(distinct a.parent_deal_id), 
   count(distinct all_deals), 
   count(distinct c.parent_deal_id)
from np_postvet_temp2 as a 
left join np_postvet_temp0 as b on a.parent_deal_id = all_deals
left join np_postvet_temp2 as c on a.child_deal_id = c.parent_deal_id

*
**/



drop table sandbox.np_deal_clone_rel;
create multiset table sandbox.np_deal_clone_rel (
    prime_deal_id varchar(36)
	,parent_deal_id varchar(36)
	, child_deal_id varchar(36)
	, depth smallint
	, submitted smallint
);

insert into sandbox.np_deal_clone_rel
with recursive cte (prime_deal_id, parent_deal_id, child_deal_id, depth, submitted)
as 
( select  
          prime_deal_id,
          parent_deal_id, 
  	      child_deal_id, 
  	      case when child_deal_id is null then cast(1 as int) else cast(2 as int) end as depth,
  	      submitted
  	from 
  	   np_non_dupe2
union all 
   select
      cte.prime_deal_id
      ,b.parent_deal_id
      , b.child_deal_id
      , cast(cte.depth + 1 as int) as depth
      , b.submitted
  	from cte
  	join np_postvet_temp3 b
  		on cte.child_deal_id = b.parent_deal_id
)
select * from cte;

grant all on sandbox.np_deal_clone_rel to abautista, jkerin with grant option;





select * from sandbox.np_deal_clone_rel;
select * from sandbox.np_deal_clone_rel where prime_deal_id = 'eeb6d7d9-a910-49cc-9ebb-a0996b9d1c34';
select * from sandbox.np_deal_clone_rel order by prime_deal_id, depth;

insert into sandbox.np_deal_clone_rel
with recursive cte (prime_deal_id, parent_deal_id, child_deal_id, depth)
as 
( select  
          coalesce(dupe.deal_id, a.parent_deal_id) prime_deal_id,
          coalesce(dupe.deal_id, a.parent_deal_id) parent_deal_id, 
  	      child_deal_id, 
  	      cast(1 as int) as depth
  	from 
  	(select deal_id
          from sb_merchant_experience.history_event
          where event_type = 'DRAFT_DEAL_CREATION'
          and history_data.jsonextractvalue('$.additionalInfo.deal.clonedFrom') is null
          and event_date < current_date
          group by 1) dupe
  	 left join      
  	   np_postvet_temp as a on dupe.deal_id = a.parent_deal_id
union all 
   select
      cte.prime_deal_id
      ,b.parent_deal_id
      , b.child_deal_id
      , cast(cte.depth + 1 as int) as depth
  	from cte
  	join np_postvet_temp b
  		on cte.child_deal_id = b.parent_deal_id and b.child_deal_id is not null
)
select * from cte;



/*
 * ----PARENTS WITH 2 CHILDREN FOR ONE CASE I SAW ITS HAPPENING IN the opportunity_id table. And it has also happened for dmapi opportunity name
 * select parent_deal_id, count(distinct child_deal_id), count(1) as xyz from np_postvet_temp group by 1 having xyz > 1;
select * from np_postvet_temp where parent_deal_id = '5ce580ff-2a56-48d4-8609-5ef38554751f';
select * from sb_merchant_experience.history_event as a 
where event_type = 'POST_VETTING_STARTED' 
      and a.event_date < current_date 
      and deal_id = 'bae5fc26-038d-4259-b10b-2c62d26ae8f0';
     
select * from sandbox.np_opportunity_details_1 where deal_uuid = '597ab5b2-1ae0-4dc2-85b0-9a9bb7032a43';
select * from sandbox.np_opportunity_details_1 where cloned_from = '006C000001A9AkMIAV';

select * from user_edwprod.sf_opportunity_2 where deal_uuid in ('597ab5b2-1ae0-4dc2-85b0-9a9bb7032a43','ee073760-2a4a-4f29-80c9-b86ce80e2564');

select cloned_from, count(distinct id) xyz from user_edwprod.sf_opportunity_1 group by 1 having xyz > 1;

select * from user_edwprod.sf_opportunity_1 where id in ('0063c00001JropUAAR','0063c00001JrnmlAAB');

{"userAgent": "jms.topic.salesforce.opportunity.detailed_update", "restricted": {}, "additionalInfo": {"dealStatus": "HOLD"}}



 * select * from np_postvet_temp where parent_deal_id = 'b667b7f5-2e50-4fa8-b295-7035d86399f3';
select * from np_postvet_temp where child_deal_id = 'b667b7f5-2e50-4fa8-b295-7035d86399f3';


create multiset volatile table np_postvet_temp2 as 
(select 
   history_data.jsonextractvalue('$.additionalInfo.deal.clonedFrom') parent_deal_id,
   deal_id as deal_id
from sb_merchant_experience.history_event he 
where event_type = 'DRAFT_DEAL_CREATION'
      and event_date < current_date
 ) with data on commit preserve rows;

select 
  sum(not_matching_data), 
  count(1)
from 
(select 
  a.parent_deal_id p1, 
  b.deal_id p2, 
case when b.deal_id is null then 1 else 0 end not_matching_data
from np_postvet_temp as a 
left join np_postvet_temp2 as b on a.parent_deal_id = b.deal_id
where case when b.deal_id is null then 1 else 0 end = 1
) as f;

----one case wherethere was a child with no cloned_from info in there. and the other 
----faee4c8c-ed70-4fe3-bfb9-5517ccc7c26a      8110fce0-c548-415d-ad4a-2cbb6bf7ab10

select * 
from sb_merchant_experience.history_event he 
where event_type in ('DRAFT_DEAL_CREATION','PRE_VETTING_UPDATE')
and deal_id = 'faee4c8c-ed70-4fe3-bfb9-5517ccc7c26a';

select deal_id
        from sb_merchant_experience.history_event
        where event_type in ('PRE_VETTING_UPDATE')

      
----sample parent_deal_id = 47a3f127-f85d-4647-b488-74eaf113bcd1

select deal_id, history_data.jsonextractvalue('$.additionalInfo.deal.clonedFrom')
from sb_merchant_experience.history_event
where event_type = 'DRAFT_DEAL_CREATION'
and event_date < current_date
and deal_id = 'b9115a19-36a5-4537-ac56-c1ff507fc3c6';
*/







select
      a.deal_id as parent_deal_id
      , o1.opportunity_id as parent_opp_id
      , ivd.invalid_data__c as parent_invalid_data
      , mtd.deal_uuid as child_deal_id
      , mtd.opportunity_id as child_opp_id
      , ivd2.invalid_data__c as child_invalid_data
from sb_merchant_experience.history_event a
    join user_edwprod.sf_opportunity_2 o2 on a.deal_id = o2.deal_uuid
    join user_edwprod.sf_opportunity_1 o1 on o2.id = o1.id
    join jc_deal_invalid_data_temp ivd on ivd.deal_uuid = a.deal_id
    -- join to child
    join user_edwprod.sf_opportunity_1 o12 on o12.cloned_from = o1.id
    join sandbox.jc_merchant_mtd_attrib mtd on o12.opportunity_id = mtd.opportunity_id
    join jc_deal_invalid_data_temp ivd2 on ivd2.deal_uuid = mtd.deal_uuid
    where event_type = 'POST_VETTING_STARTED' and a.event_date < current_date;



create volatile table jc_deal_invalid_data_temp as (
    select
      c.deal_uuid
      --, FROM_BYTES(cast(from_bytes(TO_BYTES('3B', 'base16'), 'ascii') as varchar(1)), 'ascii') as cast(from_bytes(TO_BYTES('3B', 'base16'), 'ascii') as varchar(1))
      , case 
	      when pv.deal_id is not null and b.deal_id is null and coalesce(o3.invalid_data__c, idt.invalid_data__c) <> pv.invalid_data__c 
	      		then concat(concat(coalesce(o3.invalid_data__c, idt.invalid_data__c), cast(from_bytes(TO_BYTES('3B', 'base16'), 'ascii') as varchar(1))),pv.invalid_data__c)
          when pv.deal_id is not null and b.deal_id is null 
          		then pv.invalid_data__c
          when pv.deal_id is not null and b.deal_id is not null and coalesce(o3.invalid_data__c, idt.invalid_data__c) is null 
          		then pv.invalid_data__c
          when pv.deal_id is not null and b.deal_id is not null 
          		then concat(concat(coalesce(o3.invalid_data__c, idt.invalid_data__c), cast(from_bytes(TO_BYTES('3B', 'base16'), 'ascii') as varchar(1))),pv.invalid_data__c)
          when pv.deal_id is null 
          		then coalesce(o3.invalid_data__c, idt.invalid_data__c)
        else null end invalid_data__c
    from sandbox.np_opportunity_details_1 c
    join dwh_base_sec_view.sf_opportunity_3 o3 on o3.id = c.id
    left join sandbox.jc_invalid_data_test idt on idt.opportunity_id = c.opportunity_id
    left join (
        select
          oreplace(oreplace(invalid_data__c, '"',''),',',FROM_BYTES(TO_BYTES('3B', 'base16'), 'ascii')) invalid_data__c
          , holdAt
          , deal_id
        from (
          select
            substr(invalid_data_front_trim, 1, len_invalid_data_front_trim - 3) invalid_data__c
            , holdAt
            , deal_id
          from (
            select
              substr(cast(history_data.jsonextract('$.additionalInfo.invalidData') as varchar(75)),4) invalid_data_front_trim
              , length(invalid_data_front_trim) len_invalid_data_front_trim
              , history_data.jsonextractvalue('$.additionalInfo.holdAt') holdAt
              , deal_id
            from sb_merchant_experience.history_event a
            where event_type in ('METRO_CONTRACT_SIGNIN')
            and holdAt in ('Prevetting')
            ) a
        ) a
      ) pv on pv.deal_id = c.deal_uuid
    left join (
        select deal_id
        from sb_merchant_experience.history_event
        where event_type in ('PRE_VETTING_UPDATE')
        ) b on b.deal_id = c.deal_uuid
    where c.feature_country in ('US', 'UK', 'GB', 'AU', 'PL', 'FR', 'IT', 'AE', 'DE', 'CA', 'ES')
      and c.grt_l1_cat_name = 'L1 - Local'
      and c.stagename in ('Closed Won', 'Closed Lost', 'Merchant Not Interested')
      and c.por_relaunch = 0
      and c.dmapi_flag = 1
  ) with data primary index(deal_uuid) on commit preserve rows
;



   
create volatile table jc_postvetting_relationships_v as (
    select
      a.deal_id as parent_deal_id
      , o1.opportunity_id as parent_opp_id
      , ivd.invalid_data__c as parent_invalid_data
      , mtd.deal_uuid as child_deal_id
      , mtd.opportunity_id as child_opp_id
      , ivd2.invalid_data__c as child_invalid_data
    from sb_merchant_experience.history_event a
    join user_edwprod.sf_opportunity_2 o2 on a.deal_id = o2.deal_uuid
    join user_edwprod.sf_opportunity_1 o1 on o2.id = o1.id
    join jc_deal_invalid_data_temp ivd on ivd.deal_uuid = a.deal_id
    -- join to child
    join user_edwprod.sf_opportunity_1 o12 on o12.cloned_from = o1.id
    join sandbox.jc_merchant_mtd_attrib mtd on o12.opportunity_id = mtd.opportunity_id
    join jc_deal_invalid_data_temp ivd2 on ivd2.deal_uuid = mtd.deal_uuid
    where event_type = 'POST_VETTING_STARTED' and a.event_date < current_date
  ) with data no primary index on commit preserve rows
;

insert into sandbox.jc_postvetting_agg_v
with recursive cte (parent_deal_id, child_deal_id, depth, invalid_data__c)
  as
  (
  	select parent_deal_id, 
  	      child_deal_id, 
  	      cast(1 as int) as depth
  	from jc_postvetting_relationships_v a
  	 join (
        select deal_id
        from sb_merchant_experience.history_event
        where event_type = 'DRAFT_DEAL_CREATION'
          and history_data.jsonextractvalue('$.additionalInfo.deal.clonedFrom') is null
          and event_date < current_date
      ) dupe
      on dupe.deal_id = a.parent_deal_id

  	union all

  	select
      cte.parent_deal_id
      , b.child_deal_id
      , cast(cte.depth + 1 as int) as depth
      , concat(concat(cte.invalid_data__c, cast(from_bytes(TO_BYTES('3B', 'base16'), 'ascii') as varchar(1))),b.child_invalid_data) invalid_data__c
  	from cte
  	join jc_postvetting_relationships_v b
  		on cte.child_deal_id = b.parent_deal_id and b.child_deal_id is not null

  )
  select * from cte
;