-------deal info
jrg_pause_status_test
jrg_mm_deal_edit_test




 

----------------------------------------------------------------mdm_funnel
create volatile table jrg_deal_attributes as (
select
	doe.product_uuid as deal_uuid
    , max(doe.merchant_uuid) as merchant_uuid
    , max(mtd.accountid) as account_id
    , max(gdl.grt_l2_cat_description) as l2
    , max(gdl.grt_l3_cat_description) as l3
    , max(pds.pds_cat_name) as pds
    , max(case when sf.feature_country in ('US','CA') then 'NAM' else 'INTL' end) as region
    , max(sf.feature_country) as country_code
    , max(sf.division) as division
    , max(case when lower(mda.metal_at_close) in ('platinum','gold','silver') then 's+' else 'b-' end) as metal_at_close
    , max(case when mtd.account_owner like '%Metro%' then 'Metro'
    			when mtd.account_owner in ('BD','MD', 'BD/MD') then 'MD'
    			when mtd.account_owner in ('Inbound','Existing MS') then 'MS'
				else 'Other' end) as account_owner
    , max(inventory_service_name) as inv_service_id
    , max(rads.account_score) as rads_score
from user_edwprod.dim_offer_ext doe
join user_edwprod.dim_merchant m
	on m.merchant_uuid = doe.merchant_uuid
join user_dw.v_dim_pds_grt_map pds
	on doe.pds_cat_id = pds.pds_cat_id
join user_edwprod.dim_gbl_deal_lob gdl
	on doe.product_uuid = gdl.deal_id
left join sandbox.rev_mgmt_deal_attributes  mda
	on doe.product_uuid = mda.deal_id
left join (
	select *
		from (
            select
                merchant_id
                , account_score
                , report_month
                , row_number() over (partition by merchant_id order by report_month desc) rownumdesc
            from sandbox.local_intl_rads
        ) t where rownumdesc = 1
    ) rads
    on m.salesforce_account_id = rads.merchant_id
join (
        select
        	deal_uuid
        	, max(case when feature_country = 'US' and mtd_attribution = 'BD' and dmapi_flag = 1 then 'MD'
        			   when feature_country = 'US' then mtd_attribution
        			  when feature_country <> 'US' then mtd_attribution_intl end) account_owner
        	, max(accountid) as accountid
 from sandbox.jc_merchant_mtd_attrib def
        group by deal_uuid
    ) mtd
	on doe.product_uuid = mtd.deal_uuid
left join (
        select
            o2.deal_uuid,
            max(o1.division) division
            , max(o1.feature_country) feature_country
        from user_edwprod.sf_opportunity_1  o1
        join user_edwprod.sf_opportunity_2 o2 on o1.id = o2.id
        group by o2.deal_uuid
    ) sf
    on doe.product_uuid = sf.deal_uuid
    group by doe.product_uuid
)with data primary index(deal_uuid) on commit preserve rows;------- user info test

drop table sandbox.jrg_mdm_users;create table sandbox.jrg_mdm_users as (
select 
	eventdate
	, merchant_uuid
	, platform
	, sub_platform
	, row_number() over (partition by eventdate, merchant_uuid  order by event_time desc) rownumdesc
from sandbox.pai_merchant_center_visits
where coalesce(merchant_uuid,'')<>''
QUALIFY rownumdesc=1
)with data  primary index(merchant_uuid, eventdate);-------- edit pauses status test

drop table sandbox.jrg_pause_status_test;create table sandbox.jrg_pause_status_test as (
Select
	a.change_uuid
	, dd.day_rw as report_date
	, cast(dw.week_end as date) report_wk
	, cast(dm.month_start as date) as report_mth
	, cast(dq.quarter_start as date) as report_qtr
	, chang.deal_uuid
	, mda.l2
    , mda.l3
    , mda.pds
    , mda.region
    , mda.country_code
    , mda.division
    , mda.account_owner
    , mda.metal_at_close
    , mda.rads_score
	, chang.merchant_uuid
	, chang.requester_uuid
	, chang.reviewer_id
	, chang.salesforce_case_uuid
	, chang.change_status
	, a.created_at
	, a.updated_at
	, a.review_uuid
	, a.attribute_id
	, cast(a.changed_value as JSON).jsonextractvalue('$.status.changeReasons[0].uuid') as pause_id1
	, b.displayText as pause_reason1
	, cast(a.changed_value as JSON).jsonextractvalue('$.status.changeReasons[1].uuid') as pause_id2
	, c.displayText as pause_reason2
	, cast(a.changed_value as JSON).jsonextractvalue('$.status.changeReasons[2].uuid') as pause_id3
	, d.displayText as pause_reason3
	, cast(a.changed_value as JSON).jsonextractvalue('$.status.changeReasons[3].uuid') as pause_id4
	, e.displayText as pause_reason4
	, cast(a.changed_value as JSON).jsonextractvalue('$.status.additionalDetails') as merchant_free_text
	, chang.review_status
	, chang.reject_reason
	, chang.rejection_subcategory
	, chang.review_comment
	, chang.review_time
	, chang.created_at as case_created_at
	, chang.updated_at as case_updated_at
	, chang.nots_uuid
	, chang.amendment_uuid
	, chang.salesforce_contract_id
	, chang.reviewer_name
	, app.platform
from sb_merchant_experience.merchant_self_service_change_details as a
left join sandbox.mdm_pause_reasons as b
	on b.id = pause_id1
left join sandbox.mdm_pause_reasons as c
	on c.id = pause_id2
left join sandbox.mdm_pause_reasons as d
	on d.id = pause_id3
left join sandbox.mdm_pause_reasons as e
	on e.id = pause_id4
left join sb_merchant_experience.merchant_self_service_changes chang
	on chang.change_uuid = a.change_uuid
left join jrg_deal_attributes mda
	on mda.deal_uuid = chang.deal_uuid
 left join sandbox.jrg_mdm_users app
  on chang.merchant_uuid = app.merchant_uuid
  and app.eventdate = trunc(a.created_at)
join user_groupondw.dim_day dd
	on a.created_at = dd.day_rw
join user_groupondw.dim_week dw
	on dd.week_key = dw.week_key
join user_groupondw.dim_month dm
	on dd.month_key = dm.month_key
join user_groupondw.dim_quarter dq
	on dd.quarter_key = dq.quarter_key
where upper(a.attribute_id) = 'STATUS'
and TRANSLATE_CHK(a.changed_value USING UNICODE_TO_LATIN) = 0
and original_value <> '{}'
and chang.merchant_uuid not in('1e347a7d-7213-407e-8721-8dbb0b2649c3', 'c4d29c02-040c-4bdf-a574-930f776643ad', '91df40db-90e6-4c53-b6ae-427a56fdd9e7',  '315cfad2-a12f-11e1-a6dc-00259060ae36','88d5a60e-07af-4ffa-aa4d-47d51d74c973', 'a91f843f-9e8f-4492-883b-91076b9426be', '278482e8-fa59-4b36-9c3d-5dbe8a3591b5', '9fb80846-6632-4e4c-beb4-cc28c69404dc','389440f1-540f-42c1-b7e0-94ea0fdc1b56', '51b0965e-4c4f-4050-9f3f-f1c2caf07700', 'ab45041b-c738-4fb7-adb4-796a7bd9c2f5', '7585ae11-ac53-44fc-b638-588df0562d8c', '8c845921-c050-4d4c-9413-40f6126feb84', '88d5a60e-07af-4ffa-aa4d-47d51d74c973','72f49ac3-82b4-4672-84ad-bab171f4feb8', '5779a22e-d8a7-4d22-af5a-cea5c2563739')
and chang.deal_uuid not in ('2c213278-9024-45e4-b980-df987afdf4de' '2c213278-9024-45e4-b980-df987afdf4de')
and chang.review_status <> 'DELETED'
)with data  primary index(change_uuid);-------- deal edits test

drop table sandbox.jrg_mdm_deal_edit_test;create table  sandbox.jrg_mdm_deal_edit_test as(
--voucher cap
with mdm_voucher_cap as (
SELECT
review_uuid
, created_at
, updated_at
, change_uuid
, attribute_id
, 'voucher_cap' as detail
, substr(split_part(split_part(changed_value,'"products": {"id": ',2),',',1),2,36) as service_id
, changed_value
, split_part(split_part(changed_value,'"maxVoucher": ',2),',',1) as new_value
, original_value
, split_part(split_part(original_value,'"maxVoucher": {"Value": ',2),',',1) as old_value
from sb_merchant_experience.merchant_self_service_change_details
where  attribute_id = 'PRODUCTS'
and original_value <> '{}'
)

-- deal image
, mdm_deal_image as (
SELECT
review_uuid
, created_at
, updated_at
, change_uuid
, attribute_id
, 'deal_image' as detail
, '' as service_id
, changed_value
, split_part(split_part(changed_value,'{"images": ',2),'}',1)  as new_value
, original_value
, split_part(split_part(original_value,'{"value": ',2),',',1) as old_value
from sb_merchant_experience.merchant_self_service_change_details
where upper(attribute_id) = 'IMAGES'
and original_value <> '{}'
--and TRANSLATE_CHK(changed_value USING UNICODE_TO_LATIN) = 0
)

, mdm_description as (
select 
review_uuid
, created_at
, updated_at
, change_uuid
, attribute_id
, 'description' as detail
, '' as service_id
, changed_value
, cast(cast(changed_value as JSON).jsonextract('$.description') as VARCHAR(4000)) new_value
, original_value
, cast(cast(original_value as JSON).jsonextract('$.value') as VARCHAR(4000)) old_value
from sb_merchant_experience.merchant_self_service_change_details 
where attribute_id = 'DESCRIPTION'
and TRANSLATE_CHK(changed_value USING UNICODE_TO_LATIN) = 0
and TRANSLATE_CHK(original_value USING UNICODE_TO_LATIN) = 0
)
-- new option created
, mdm_new_option as (
SELECT
review_uuid
, created_at
, updated_at
, change_uuid
, attribute_id
, 'new_option' as detail
, cast(changed_value as JSON).jsonextractvalue('$.products.id') as service_id
, changed_value
, changed_value as new_value
, original_value
, original_value as old_value
from sb_merchant_experience.merchant_self_service_change_details
where upper(attribute_id) = 'PRODUCTS'
and original_value = '{}'
and TRANSLATE_CHK(changed_value USING UNICODE_TO_LATIN) = 0
)


--price   UPDATED
,  mdm_price as (
SELECT
review_uuid
, created_at
, updated_at
, change_uuid
, attribute_id
, 'price' as detail
, substr(split_part(split_part(changed_value,'"products": {"id": ',2),',',1),2,36) as service_id
, changed_value
, split_part(split_part(changed_value,'"price": {"amount": ',2),',',1) as new_value
, original_value
, split_part(split_part(original_value,'"price": {"amount": ',2),',',1) as old_value
from sb_merchant_experience.merchant_self_service_change_details a
where upper(attribute_id) = 'PRODUCTS'
and original_value <> '{}'

)

-- TITLE cant be updated
, mdm_title as (
SELECT
review_uuid
, created_at
, updated_at
, change_uuid
, attribute_id
, 'title' as detail
, cast(changed_value as JSON).jsonextractvalue('$.products.id') as service_id
, changed_value
, cast(changed_value as JSON).jsonextractvalue('$.products.title') as new_value
, original_value
, cast(original_value as JSON).jsonextractvalue('$.title') as old_value
from sb_merchant_experience.merchant_self_service_change_details
where upper(attribute_id) = 'PRODUCTS'
and original_value <> '{}'
and TRANSLATE_CHK(changed_value USING UNICODE_TO_LATIN) = 0
and TRANSLATE_CHK(original_value USING UNICODE_TO_LATIN) = 0
)

,mdm_reorder as (
SELECT
abc.review_uuid
, abc.created_at
, abc.updated_at
, abc.change_uuid
, abc.attribute_id
, 'reorder' as detail
, substr(split_part(split_part(abc.changed_value,'"products": {"id": ',2),',',1),2,36) as service_id
, abc.changed_value
, split_part(split_part(abc.changed_value, 'displayOrder": ', 2), ',', 1)  as new_value
, abc.original_value
, abc.original_value as old_value
from sb_merchant_experience.merchant_self_service_change_details abc
where upper(attribute_id) = 'PRODUCTS'
and split_part(split_part(changed_value, 'isReordered": ', 2), ',', 1) = 'true' 
)

--discount
, mdm_discount as (
SELECT
review_uuid
, created_at
, updated_at
, change_uuid
, attribute_id
, 'discount_percent' as detail
, substr(split_part(split_part(changed_value,'"products": {"id": ',2),',',1),2,36) as service_id
, changed_value
, split_part(split_part(changed_value,'"discount": ',2),',',1) as new_value
, original_value
, split_part(split_part(original_value,'"discount": ',2),',',1) as old_value
from sb_merchant_experience.merchant_self_service_change_details
where upper(attribute_id) = 'PRODUCTS'
and original_value <> '{}'
--and TRANSLATE_CHK(changed_value USING UNICODE_TO_LATIN) = 0
)


--isActive
, mdm_pause as (
SELECT
review_uuid
, created_at
, updated_at
, change_uuid
, attribute_id
, 'paused_option' as detail
, substr(split_part(split_part(changed_value,'"products": {"id": ',2),',',1),2,36) as service_id
, changed_value
, case when split_part(split_part(changed_value,'"isActive": ',2),',',1) = 'true' then 'Live' else 'Paused' END as new_value
, original_value
, case when split_part(split_part(original_value,'"isActive": {"value": ',2),',',1) = 'true' then 'Live' else 'Paused' END as old_value
from sb_merchant_experience.merchant_self_service_change_details
where upper(attribute_id) = 'PRODUCTS'
and original_value <> '{}'
--and TRANSLATE_CHK(changed_value USING UNICODE_TO_LATIN) = 0
)

--discounted price UPDATED
, mdm_voucher as (
SELECT
review_uuid
, created_at
, updated_at
, change_uuid
, attribute_id
, 'voucher_price' as detail
, substr(split_part(split_part(changed_value,'"products": {"id": ',2),',',1),2,36) as service_id
, changed_value
, split_part(split_part(changed_value,'"discountedPrice": {"amount": ',2),',',1) as new_value
, original_value
, split_part(split_part(original_value,'"discountedPrice": {"amount": ',2),',',1) as old_value
from sb_merchant_experience.merchant_self_service_change_details
where upper(attribute_id) = 'PRODUCTS'
and original_value <> '{}'
)

--date
, mdm_start_date as (
SELECT
review_uuid
, created_at
, updated_at
, change_uuid
, attribute_id
, 'start_date' as detail
, '' as service_id
, changed_value
, split_part(split_part(changed_value,'{"startDate" : ',2),'T',1) new_value
, original_value
, split_part(split_part(original_value,'{"value" : ',2),'T',1) as old_value
from sb_merchant_experience.merchant_self_service_change_details
where upper(attribute_id) = 'START_DATE'
)

--highlights cant update
, mdm_highlights as (
SELECT
review_uuid
, created_at
, updated_at
, change_uuid
, attribute_id
, 'deal_highlights' as detail
, '' as service_id
, changed_value
, split_part(split_part(changed_value,'{"highlights" : ',2),'"}',1) as new_value
, original_value
, split_part(split_part(original_value,'{"value" : ',2),'", ',1)  as old_value
from sb_merchant_experience.merchant_self_service_change_details
where upper(attribute_id) = 'HIGHLIGHTS'
)

-- locations
, mdm_redemption_locations as (
SELECT
review_uuid
, created_at
, updated_at
, change_uuid
, attribute_id
, 'redemption_location' as detail
, '' as service_id
, changed_value
, split_part(split_part(changed_value,'{"redemptionLocationIds": ',2),'}',1) as new_value
, original_value
,  split_part(split_part(original_value,'{"value": ',2), ', "baseHash',1) as old_value
from sb_merchant_experience.merchant_self_service_change_details
where upper(attribute_id) = 'REDEMPTION_LOCATIONS'
and original_value <> '{}'
)

--fine print--multiple result
, mdm_fine_print as (
SELECT
review_uuid
, created_at
, updated_at
, change_uuid
, attribute_id
, 'fine_print' as detail
, '' as service_id
, changed_value
, cast(cast(changed_value as JSON).jsonextract('$.consumerContractTerms') as VARCHAR(4000)) as new_value
, original_value
, cast(cast(original_value as JSON).jsonextract('$.value') as VARchar(4000)) as old_value
from sb_merchant_experience.merchant_self_service_change_details
where upper(attribute_id) = 'CONSUMER_CONTRACT_TERMS'
and TRANSLATE_CHK(changed_value USING UNICODE_TO_LATIN) = 0
and TRANSLATE_CHK(original_value USING UNICODE_TO_LATIN) = 0

)


, mdm_end_date as (
SELECT
review_uuid
, created_at
, updated_at
, change_uuid
, attribute_id
, 'end_date' as detail
, '' as service_id
, changed_value
, split_part(split_part(changed_value,'{"endDate" : ',2),'T',1) new_value
, original_value
, split_part(split_part(original_value,'{"value" : ',2),'T',1) as old_value
from sb_merchant_experience.merchant_self_service_change_details
where upper(attribute_id) = 'END_DATE'
and original_value <> '{}'
)


select
	abc.change_uuid
	, dd.day_rw as report_date
	, cast(dw.week_end as date) as report_wk
	, cast(dm.month_start as date) as report_mth
	, cast(dq.quarter_start as date) as report_qtr
	, d.deal_uuid
	, mda.l2
    , mda.l3
    , mda.pds
    , mda.region
    , mda.country_code
    , mda.division
    , mda.account_owner
    , mda.metal_at_close
    , mda.rads_score
-- mtd attribution/account owner, deal pause date, straight to private sale,
	, abc.review_uuid
	, abc.created_at
	, abc.updated_at
	, (case when abc.detail = 'voucher_cap' and cast(new_value as integer)<cast(old_value as integer) then 'voucher_cap_decrease'
			when abc.detail = 'voucher_cap' and cast( new_value as integer)> cast(old_value as integer) then 'voucher_cap_increase'
			when abc.detail = 'voucher_price' and cast(new_value as integer)<cast(old_value as integer) then 'voucher_price_decrease'
			when abc.detail = 'voucher_price' and cast(new_value as integer)>cast(old_value as integer) then 'voucher_price_increase'
			when abc.detail = 'price' and cast(new_value as integer)<cast(old_value as integer) then 'price_decrease'
			when abc.detail = 'price' and cast(new_value as integer)>cast(old_value as integer) then 'price_increase'
                        when abc.detail = 'paused_option' and new_value='Live' then 'unpaused_option'
                        when abc.detail='paused_option' and new_value='Paused' then 'paused_option'
			else abc.detail end) as detail
	, abc.attribute_id
	, abc.service_id as inv_product_uuid
	, abc.changed_value
	, abc.new_value
	, abc.original_value
	, abc.old_value
	, case when abc.old_value <> abc.new_value then 1 else 0 end as detail_values_adjusted
	, d.review_status
	, d.merchant_uuid
	, d.requester_uuid
	, d.reject_reason
	, d.rejection_subcategory
	, d.review_comment
	, d.review_time
	, d.created_at as case_created_at
	, d.updated_at as case_updated_at
	, d.nots_uuid
	, d.amendment_uuid
	, d.salesforce_case_uuid
	, d.reviewer_name
	, app.platform
from (
	select * from mdm_discount
		UNION
	select  * from mdm_start_date
		UNION ALL
	select  * from mdm_price
		UNION ALL
	select  * from mdm_new_option
		UNION ALL
	select  * from mdm_voucher_cap
		UNION ALL
	select  * from mdm_voucher
		UNION ALL
	select  * from mdm_pause
		UNION ALL
	select  * from mdm_title
		union all
	select  * from mdm_end_date
		union all
	select * from mdm_deal_image
		union all
	select * from mdm_fine_print
	union all
	select * from mdm_redemption_locations
		union all
	select * from mdm_highlights
                union all 
         select * from mdm_reorder
                 union all
         select * from mdm_description ) as abc
left join sb_merchant_experience.merchant_self_service_changes d
	on d.change_uuid = abc.change_uuid
left join jrg_deal_attributes mda
	on mda.deal_uuid = d.deal_uuid
 left join sandbox.jrg_mdm_users app
 	on d.merchant_uuid = app.merchant_uuid
  and app.eventdate = trunc(abc.created_at)
join user_groupondw.dim_day dd
	on abc.created_at = dd.day_rw
join user_groupondw.dim_week dw
	on dd.week_key = dw.week_key
join user_groupondw.dim_month dm
	on dd.month_key = dm.month_key
join user_groupondw.dim_quarter dq
	on dd.quarter_key = dq.quarter_key
where d.merchant_uuid not in('1e347a7d-7213-407e-8721-8dbb0b2649c3', '315cfad2-a12f-11e1-a6dc-00259060ae36', 'a91f843f-9e8f-4492-883b-91076b9426be', '278482e8-fa59-4b36-9c3d-5dbe8a3591b5', '9fb80846-6632-4e4c-beb4-cc28c69404dc', '389440f1-540f-42c1-b7e0-94ea0fdc1b56', '51b0965e-4c4f-4050-9f3f-f1c2caf07700', 'ab45041b-c738-4fb7-adb4-796a7bd9c2f5')
    and d.deal_uuid not in ('2c213278-9024-45e4-b980-df987afdf4de','2c213278-9024-45e4-b980-df987afdf4de')
    and d.review_status <> 'DELETED'
    and detail_values_adjusted=1
)with data  primary index(change_uuid, review_uuid);--------reject reasons 

drop table sandbox.jrg_reject_reason;create table sandbox.jrg_reject_reason as (
select
d.change_uuid
, cast(dw.week_end as date) as report_wk
, f.vertical
, f.pds_name as pds
, f.metal_at_close
, def.launch_date
, def.mtd_attribution as account_owner
, def.deal_paused_date
, def.straight_to_private_sale
, def.division
, d.deal_uuid
, d.merchant_uuid
, d.requester_uuid
, d.reviewer_id
, d.salesforce_case_uuid
, d.change_status
, d.review_status
, d.reject_reason
, d.rejection_subcategory
, d.review_comment
, d.review_time
, d.created_at as case_created_at
, d.updated_at as case_updated_at
, d.nots_uuid
, d.amendment_uuid
, d.salesforce_contract_id
, d.reviewer_name
, app.platform
, case when coalesce(jrg.detail,'')='' and chang.attribute_id='STATUS' then 'paused_deal'
		when COALESCE(jrg.detail,'')='' and chang.attribute_id='DESCRIPTION' then 'deal_description'
		else jrg.detail end as edit_detail
, CURRENT_DATE as date_refreshed
from sb_merchant_experience.merchant_self_service_changes d
join user_groupondw.dim_day dd 
	on d.created_at = dd.day_rw
join user_groupondw.dim_week dw 
	on dd.week_key = dw.week_key
left join sandbox.rev_mgmt_deal_attributes f 
	on f.deal_id = d.deal_uuid
left join sandbox.jc_merchant_mtd_attrib def
	on def.deal_uuid = d.deal_uuid
 left join sandbox.jrg_mdm_users app 
 	on d.merchant_uuid = app.merchant_uuid
  and app.eventdate = trunc(d.created_at)
left join sandbox.jrg_mdm_deal_edit_test jrg 
 	on jrg.change_uuid = d.change_uuid
 left join sb_merchant_experience.merchant_self_service_change_details chang
 	on chang.change_uuid = d.change_uuid
where upper(d.review_status)='REJECTED'
)with data  primary index(change_uuid);---------- set up set up

create volatile table jrg_mdm_users_table as (
select
dd.day_rw as report_date
, cast(dw.week_end as date) report_wk
, cast(dm.month_start as date) as report_mth
, cast(dq.quarter_start as date) as report_qtr
, mm.l2
, mm.l3
, mm.top_pds as pds
, case when mm.country_code in ('US', 'CA') then 'NAM' else 'US' end as region
, mm.country_code
, mm.division
, mm.acct_owner as account_owner
, mm.current_metal_segment as metal_at_close
, bld.platform
, rads.account_score as rads_score
  , count(distinct bld.merchant_uuid) as n_users
from (
  select
    eventdate
    , merchant_uuid
    , platform
    , sub_platform
    , country_code
    , row_number() over (partition by eventdate, merchant_uuid  order by event_time desc) rownumdesc
  from sandbox.pai_merchant_center_visits
  where coalesce(merchant_uuid,'')<>'' and live_flag=1
  QUALIFY rownumdesc=1
  ) bld
join sandbox.pai_merchants mm
on mm.merchant_uuid = bld.merchant_uuid
left join (
        select 
          merchant_id
          , max(account_score) account_score  
        from (
              select
                  merchant_id
                  , account_score
                  , report_month
                  , row_number() over (partition by merchant_id order by report_month desc) rownumdesc
              from sandbox.local_intl_rads
          ) t 
        where rownumdesc = 1 group by 1
  ) rads
on mm.account_id = rads.merchant_id
join user_groupondw.dim_day dd
on bld.eventdate = dd.day_rw
join user_groupondw.dim_week dw
  on dd.week_key = dw.week_key
join user_groupondw.dim_month dm
  on dd.month_key = dm.month_key
join user_groupondw.dim_quarter dq 
on dd.quarter_key = dq.quarter_key
where mm.l1='Local'
and mm.country_code in('AE','AU', 'CA', 'DE', 'ES', 'FR', 'GB', 'IT', 'PL', 'US', 'UK')
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
  ) with data primary index(report_date, report_wk, report_mth, report_qtr, l2, l3, pds, region, country_code, division, account_owner, metal_at_close, platform, rads_score ) on commit preserve rows;create volatile table jrg_mdm_edits as (
select
a.report_date
   			 	, a.report_wk
    			, a.report_mth
    			, a.report_qtr
          , mm.l2
          , mm.l3
          , mm.top_pds as pds
          , case when mm.country_code in ('US', 'CA') then 'NAM' else 'US' end as region
          , mm.country_code
          , mm.division
          , mm.acct_owner as account_owner
          , mm.current_metal_segment as metal_at_close
    			, a.rads_score
    			, a.platform
    			, coalesce(count(distinct a.merchant_uuid),0) as n_users
    			, coalesce(count (distinct a.change_uuid),0) as n_edits
    			, coalesce(count(distinct a.deal_uuid),0) as n_deals_edited
    			, coalesce(count(distinct a.merchant_uuid),0)as n_mercs_w_edit
    			, coalesce(count(distinct case when a.review_status = 'APPROVED' then a.change_uuid end),0) as n_edits_approved
    			, coalesce(count(distinct case when a.review_status = 'APPROVED' then a.deal_uuid end),0) as n_deal_edits_approved
    			, coalesce(count(distinct case when a.review_status = 'APPROVED' then a.merchant_uuid end),0) as n_merc_edits_approved
  				from sandbox.jrg_mdm_deal_edit_test a
          join sandbox.pai_merchants mm 
            on mm.merchant_uuid = a.merchant_uuid
  				group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14

)with data unique primary index (report_date, report_wk, report_mth, report_qtr, l2, l3, pds, region, country_code, division, account_owner, metal_at_close, platform, rads_score) on commit preserve rows;create volatile table jrg_mdm_pauses as (
select
 				a.report_date
   			 	, a.report_wk
    			, a.report_mth
    			, a.report_qtr
          , mm.l2
          , mm.l3
          , mm.top_pds as pds
          , case when mm.country_code in ('US', 'CA') then 'NAM' else 'US' end as region
          , mm.country_code
          , mm.division
          , mm.acct_owner as account_owner
          , mm.current_metal_segment as metal_at_close
    			, a.rads_score
    			, a.platform
    			, coalesce(count (distinct a.change_uuid),0) as n_pauses
    			, coalesce(count (distinct a.deal_uuid),0) as n_deals_paused
   				, coalesce(count (distinct a.merchant_uuid),0) as n_mercs_w_pause
    			, coalesce(count(distinct case when a.review_status = 'APPROVED' then a.change_uuid end),0) as n_pauses_approved
  				, coalesce(count(distinct case when a.review_status = 'APPROVED' then a.deal_uuid end),0) as n_deal_pauses_approved
    			, coalesce(count(distinct case when a.review_status = 'APPROVED' then a.merchant_uuid end),0) as n_merc_pauses_approved
    			from sandbox.jrg_pause_status_test a
          join sandbox.pai_merchants mm 
            on mm.merchant_uuid = a.merchant_uuid
    			group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
)with data unique primary index (report_date, report_wk, report_mth, report_qtr, l2, l3, pds, region, country_code, division, account_owner, metal_at_close, platform, rads_score) on commit preserve rows;create volatile table jrg_attributes as (
  select distinct report_date, report_wk, report_mth, report_qtr, l2, l3, pds, region, country_code, division, account_owner, metal_at_close, rads_score, platform
from (
    select report_date, report_wk, report_mth, report_qtr, l2, l3, pds, region, country_code, division, account_owner, metal_at_close, rads_score, platform
    from jrg_mdm_users_table

    union all

    select report_date, report_wk, report_mth, report_qtr, l2, l3, pds, region, country_code, division, account_owner, metal_at_close, rads_score, platform
    from jrg_mdm_edits

    union all

    select report_date, report_wk, report_mth, report_qtr, l2, l3, pds, region, country_code, division, account_owner, metal_at_close, rads_score, platform
    from jrg_mdm_pauses
) tef
--group by report_date, report_wk, report_mth, report_qtr, l2, l3, pds, region, country_code, division, account_owner, metal_at_close, rads_score, platform

  )  with data unique primary index (report_date, report_wk, report_mth, report_qtr, l2, l3, pds, region, country_code, division, account_owner, metal_at_close, platform, rads_score) on commit preserve rows;drop table sandbox.jrg_mdm_funnel;create table sandbox.jrg_mdm_funnel as (
	select
	  d.report_date,
	        d.report_wk,
	        d.report_mth,
	        d.report_qtr,
	        d.l2,
	        d.l3,
	        d.pds,
	        d.region,
	        d.country_code,
	        d.division,
	        d.account_owner,
	        d.metal_at_close,
	        d.rads_score,
	        d.platform,
	        coalesce(u.n_users,0) n_users,
	        coalesce(o.n_edits,0) n_edits,
	        coalesce(o.n_deals_edited,0) n_deals_edited   ,
	        coalesce(o.n_mercs_w_edit,0) n_mercs_w_edit,
	        coalesce(o.n_edits_approved,0) n_edits_approved,
	        coalesce(o.n_deal_edits_approved,0) as n_deal_edits_approved,
	        coalesce(o.n_merc_edits_approved,0) as n_merc_edits_approved,
	        coalesce(e.n_pauses,0) n_pauses,
	        coalesce(e.n_deals_paused,0) n_deals_paused   ,
	        coalesce(e.n_mercs_w_pause,0) n_mercs_w_pause,
	        coalesce(e.n_pauses_approved,0) n_pauses_approved,
	        coalesce(e.n_deal_pauses_approved,0) n_deal_pauses_approved,
	        coalesce(e.n_merc_pauses_approved,0) n_merc_pauses_approved
	    from jrg_attributes d
	    left join jrg_mdm_users_table u
	        on coalesce(d.report_date,date'2099-01-01') = coalesce(u.report_date,date'2099-01-01')
	        and coalesce(d.report_wk,date'2099-01-01') = coalesce(u.report_wk,date'2099-01-01')
	        and coalesce(d.report_mth,date'2099-01-01') = coalesce(u.report_mth, date'2099-01-01')
	        and coalesce(d.report_qtr,date'2099-01-01') = coalesce(u.report_qtr,date'2099-01-01')
	        and coalesce(d.l2,'n/a') = coalesce(u.l2,'n/a')
	        and coalesce(d.l3,'n/a') = coalesce(u.l3,'n/a')
	        and coalesce(d.pds,'n/a') = coalesce(u.pds,'n/a')
	        and coalesce(d.region,'n/a') = coalesce(u.region,'n/a')
	        and coalesce(d.country_code,'n/a') = coalesce(u.country_code,'n/a')
	        and coalesce(d.division,'n/a') = coalesce(u.division,'n/a')
	        and coalesce(d.account_owner,'n/a') = coalesce(u.account_owner,'n/a')
	        and coalesce(d.metal_at_close,'n/a') = coalesce(u.metal_at_close,'n/a')
	        and coalesce(d.rads_score,'n/a') = coalesce(u.rads_score,'n/a')
	        and coalesce(d.platform,'n/a') = coalesce(u.platform,'n/a')
	    left join jrg_mdm_pauses e
	        on coalesce(d.report_date,date'2099-01-01') = coalesce(e.report_date,date'2099-01-01')
	        and coalesce(d.report_wk,date'2099-01-01') = coalesce(e.report_wk,date'2099-01-01')
	        and coalesce(d.report_mth,date'2099-01-01') = coalesce(e.report_mth,date'2099-01-01')
	        and coalesce(d.report_qtr,date'2099-01-01') = coalesce(e.report_qtr,date'2099-01-01')
	        and coalesce(d.l2,'n/a') = coalesce(e.l2,'n/a')
	        and coalesce(d.l3,'n/a') = coalesce(e.l3,'n/a')
	        and coalesce(d.pds,'n/a') = coalesce(e.pds,'n/a')
	        and coalesce(d.region,'n/a') = coalesce(e.region,'n/a')
	        and coalesce(d.country_code,'n/a') = coalesce(e.country_code,'n/a')
	        and coalesce(d.division,'n/a') = coalesce(e.division,'n/a')
	        and coalesce(d.account_owner,'n/a') = coalesce(e.account_owner,'n/a')
	        and coalesce(d.metal_at_close,'n/a') = coalesce(e.metal_at_close,'n/a')
	        and coalesce(d.rads_score,'n/a') = coalesce(e.rads_score,'n/a')
	        and coalesce(d.platform,'n/a') = coalesce(e.platform,'n/a')
	  left join jrg_mdm_edits o
	        on coalesce(d.report_date,date'2099-01-01') = coalesce(o.report_date,date'2099-01-01')
	        and coalesce(d.report_wk,date'2099-01-01') = coalesce(o.report_wk,date'2099-01-01')
	        and coalesce(d.report_mth,date'2099-01-01') = coalesce(o.report_mth,date'2099-01-01')
	        and coalesce(d.report_qtr,date'2099-01-01') = coalesce(o.report_qtr,date'2099-01-01')
	        and coalesce(d.l2,'n/a') = coalesce(o.l2,'n/a')
	        and coalesce(d.l3,'n/a') = coalesce(o.l3,'n/a')
	        and coalesce(d.pds,'n/a') = coalesce(o.pds,'n/a')
	        and coalesce(d.region,'n/a') = coalesce(o.region,'n/a')
	        and coalesce(d.country_code,'n/a') = coalesce(o.country_code,'n/a')
	        and coalesce(d.division,'n/a') = coalesce(o.division,'n/a')
	        and coalesce(d.account_owner,'n/a') = coalesce(o.account_owner,'n/a')
	        and coalesce(d.metal_at_close,'n/a') = coalesce(o.metal_at_close,'n/a')
	        and coalesce(d.rads_score,'n/a') = coalesce(o.rads_score,'n/a')
	        and coalesce(d.platform,'n/a') = coalesce(o.platform,'n/a')

	    ) with data unique primary index (report_date, report_wk, report_mth, report_qtr, l2, l3, pds, region, country_code, division, account_owner, metal_at_close, platform, rads_score);
	    grant select on sandbox.jrg_mdm_deal_edit_test to public;grant select on sandbox.jrg_pause_status_test to public
	   ;
	   grant select on sandbox.jrg_reject_reason to public;grant select on sandbox.jrg_mdm_funnel to public
	   
	   
	   
--------------------------------------------------------------------pause_from_mdm
	   
	   
create volatile table jrg_actually_paused as (
select
	a.deal_uuid
    , max(load_date) last_date
	, case when last_date > current_date -2 then 1 else 0 end still_live
    from user_groupondw.active_deals a
    group by 1
    --where a.DEAL_UUID='94890bcd-0cb5-4cdf-844e-9c2024eff642'
) with data primary index(deal_uuid) on commit preserve rows;drop table sandbox.jrg_all_deal_pauses;create table sandbox.jrg_all_deal_pauses as (
select
    b.*
    , case
        when pause_group in (
            'CLO'
            ,'Legal'
            ,'OOB'
            ,'OOB - Temp'
            ,'Will Run Another Deal'
            ) then 'Hard'
        when pause_group in(
            'No Customers'
            ,'Not Prepped for Launch'
            ,'Other'
            ,'Payments'
            ,'Poor Quality Customers'
            ,'POR'
            ,'Promotions'
            ,'Removing Some Inventory'
            ,'Too Many Customers'
            ,'Unhappy w/ Support'
            ,'Unit Economics'
            ) then 'Soft'
        end as pause_type
     , (case when coalesce(ba.deal_uuid,'')<>'' then 1 else 0 end ) actual_pause
from(
    select
        a.*
        , case
            when pause_reason in(
                'deal_paused_risk_user'
                ,'legal_hold'
                ,'merchant_is_dnr_undesirable_category_dnc'
                ,'product_quality'
                ,'restrictions'
                ) then 'Legal'
            when pause_reason in(
                'I am not getting enough sales/customers.'
                ,'too_few_customers'
                ) then 'No Customers'
            when pause_reason in(
                'preview_edits_not_made_before_launch'
                ,'scheduling_conflict'
                ,'temporary_pause_to_resolve_internal_groupon_issue'
                ,'unprepared_for_launch'
                ) then 'Not Prepped for Launch'
            when pause_reason in(
                'business_closed_permanently'
                ,'change_of_ownership_new_owner_accepting'
                ,'change_of_ownership_new_owner_not_accepting'
                ,'I am selling my business.'
                ,'merchant_no_longer_offers_service'
                ,'My business is closing permanently.'
                ,'other_coronavirus'
                ) then 'OOB'
            when pause_reason in(
                'business_closed_seasonally'
                ,'merchant_out_of_product'
                ,'My business is closing temporarily.'
                ,'other_covid_temp_pause_not_oob'
                ,'vacation_family_emergency_renovations_short_staffed'
                ) then 'OOB - Temp'
            when pause_reason in(
                'payment_issue'
                ) then 'Payments'
            when pause_reason in(
                'I am not happy with type of customer I get through Groupon.'
            	,'I''m not happy with type of customer I get through Groupon.'
                ,'poor_quality_customers'
                ) then 'Poor Quality Customers'
            when pause_reason in(
                'pay_on_redemption'
                ,'por'
                ,'POR Auto Relaunch 2020'
                ) then 'POR'
            when pause_reason in(
                'promo_code_ils'
                ) then 'Promotions'
            when pause_reason in(
                'removing_full_menu'
                ,'removing_locations'
                ) then 'Removing Some Inventory'
            when pause_reason in(
                'capacity_too_many_customers'
                ,'This campaign is sending me too many customers.'
                ) then 'Too Many Customers'
            when pause_reason in(
                'I am not happy with Groupons merchant tools or support.'
            	,'I''m not happy with Groupon''s merchant tools or support.'
                ,'merchant_unhappy_with_solutions_proposed_merchant_hasnt_received_satisfactory_response'
                ) then 'Unhappy w/ Support'
            when pause_reason in(
                'continuous_campaign_branding_doesnt_always_want_discount'
                ,'deal_structure'
                ,'deal_structure_concerns_margin_roi'
                ,'I am entering busy season and don''t need to offer customers a discount.'
                ,'I am entering busy season and dont need to offer customers a discount.'
                ,'I have achieved my customer acquisition goals with this campaign.'
                ,'I no longer want to offer a discount.'
                ,'margin'
                ,'service_fee'
                ,'This campaign is not working for me financially.'
                ) then 'Unit Economics'
            when pause_reason in(
                'merchant_unresponsive'
                ,'merchant_refuses_to_offer_a_reason_merchant_unresponsive'
                ) then 'Unresponsive'
            when pause_reason in(
                'I want to create a different campaign.'
                ,'new_deal_in_negotiation_will_launch_new_deal'
                ) then 'Will Run Another Deal'
            else 'Other'
        end as pause_group
    from(
    select
        cast(pd.created_ts as date) pause_date
        , pd.deal_uuid
        , doe.merchant_uuid
        , mtd.account_owner
        , mtd.vertical
        , da.metal_at_close
        , pd.pause_reason
        , (case when coalesce(sss.deal_uuid,'')<>'' then 'mdm' else 'deal_estate' end) as paused_source
        , pd.created_ts
    from user_groupondw.paused_deals pd
    join (	select
    		product_uuid
    		, max(merchant_uuid) merchant_uuid
    		from user_edwprod.dim_offer_ext
    		group by 1
    		)doe
        on doe.product_uuid = pd.deal_uuid
    left join (
			select
        	deal_uuid
        	, max(vertical) vertical
        	, max(case when feature_country = 'US' and mtd_attribution = 'BD' and dmapi_flag = 1 then 'MD'
        			   when feature_country = 'US' then mtd_attribution
        			  when feature_country <> 'US' then mtd_attribution_intl end) as account_owner
        	, max(accountid) as accountid
 			from sandbox.jc_merchant_mtd_attrib def
        	group by deal_uuid) mtd
        on pd.deal_uuid = mtd.deal_uuid
    left join sandbox.rev_mgmt_deal_attributes da
        on pd.deal_uuid = da.deal_id
    left join (
    			select * from
    			sandbox.jrg_pause_status_test
    			where review_status='APPROVED') sss
    	on sss.deal_uuid = pd.deal_uuid
    	and cast(sss.case_updated_at as date) = cast(pd.event_at as date)
    where cast(pd.event_at as date) >= date '2019-01-01'
        and pause_reason not like 'clo_%'
    ) a
)b
left join jrg_actually_paused ba
	on b.deal_uuid = ba.deal_uuid
    and ba.still_live = 0
    and ba.last_date = b.pause_date
)with data  primary index(pause_date, deal_uuid);

create volatile table  jrg_deal_location as (
SELECT
gre.deal_uuid,
case when division in  ('Seattle' , 'Denver', 'Detroit', 'Long Island') then 'Phase 1'
     when division in  ('Dallas', 'Forth Worth') then 'Phase 2'
     when division in ('Las Vegas', 'Fort Lauderdale', 'Phoenix', 'Miami', 'Orange County', 'San Diego'
          , 'Atlanta', 'Inland Empire', 'Tampa', 'Orlando', 'New York', 'Houston', 'Los Angeles'
          , 'Minneapolis', 'San Francisco') then 'Phase 3'
     else 'Non-TO' end as market
, max(metal_segment) as metal
, max(gdl.grt_l2_cat_description) as l2
, max(gdl.grt_l3_cat_description) as l3
, max(pds.pds_cat_name) as pds
, max(gre.feature_country) as country_code
, max(case when gre.feature_country in ('US','CA') then 'NAM' else 'INTL' end) as region
, max(sh.acct_owner) sh_accounts
, max(mtd.account_owner) acct_owner
from(
		sel 
			deal_uuid,
			max(o1.division) division,
                        max(o1.feature_country) feature_country,
			max(case when opportunity_name like 'dmapi%' then 1 else 0 end) is_metro,
			max(case when lower(sda.merchant_seg_at_closed_won) in ('platinum','gold','silver') then 's+' else 'b-' end) metal_segment,
			max(sfa.name) account_name
		from user_edwprod.sf_opportunity_1 o1
		join user_edwprod.sf_opportunity_2 o2 on o1.id = o2.id
		join user_edwprod.sf_account sfa on o1.accountid = sfa.id
		join dwh_base_sec_view.sf_deal_attribute sda on o1.deal_attribute = sda.id
		group by 1
	) gre
left join user_edwprod.dim_gbl_deal_lob gdl
	on gdl.deal_id = gre.deal_uuid
left join sandbox.sh_acct_and_opp_owner sh
	on sh.deal_uuid = gre.deal_uuid
left join user_dw.v_dim_pds_grt_map pds 
	on gdl.pds_cat_id = pds.pds_cat_id
left join (
        	select
        		deal_uuid
        		, max(case when feature_country = 'US' and mtd_attribution = 'BD' and dmapi_flag = 1 then 'MD'
        			   when feature_country = 'US' then mtd_attribution
        			  when feature_country <> 'US' then mtd_attribution_intl end) as account_owner
        		, max(accountid) as accountid
 			from sandbox.jc_merchant_mtd_attrib def
        	group by deal_uuid
    ) mtd
	on gre.deal_uuid = mtd.deal_uuid
group by 1,2
)with data primary index(deal_uuid) on commit preserve rows;

drop table sandbox.jrg_pause_pct;create table  sandbox.jrg_pause_pct as (
select
     dd.day_rw as report_date,
     cast(dw.week_end as date) as report_wk,
	 cast(dm.month_start as date) as report_mth,
	 cast(dq.quarter_start as date) as report_qtr,
    (case when coalesce(sh_accounts,'')='' then 'Other' else sh_accounts end)as account_owner,
    (case when acct_owner in ('New Metro','Existing Metro') then 'Metro'
   	 		when acct_owner in ('BD','MD', 'BD/MD') then 'MD'
   	 		when acct_owner in ('Inbound','Existing MS') then 'MS'
   	 		else 'Other' end) acct_owner,
   	 country_code,
   	 metal,
   	 l2,
   	 l3,
   	 pds,
     region,
   pause_group,
   	market,
    count(distinct t.deal_uuid) deals_paused,
    count(distinct case when paused_source = 'mdm' then t.deal_uuid end) deals_paused_ss
from (
sel cast((created_ts) as date) pause_date,
        a.deal_uuid,
        b.sh_accounts,
        b.acct_owner,
        a.vertical,
        b.metal,
        b.l2,
        b.l3,
        b.pds,
        --a.pause_reason,
        a.paused_source,
        a.pause_group,
        b.market,
        b.region,
        b.country_code
    from sandbox.jrg_all_deal_pauses a
    left join jrg_deal_location b
    	on b.deal_uuid = a.deal_uuid
) t
join user_groupondw.dim_day dd 
	on t.pause_date = dd.day_rw
join user_groupondw.dim_week dw 
	on dd.week_key = dw.week_key
join user_groupondw.dim_month dm
	on dd.month_key = dm.month_key
join user_groupondw.dim_quarter dq
	on dd.quarter_key = dq.quarter_key
where  pause_group <> 'POR'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
)with data primary index(report_wk);grant select on  sandbox.jrg_pause_pct to dv_engineering
	  
-------------------------------------------------------------------- MDM CASE

create volatile table jrg_account_details as (

	select
	accountid
	, vertical
	--, roles
	--, account_owner
	, (case when roles like 'Merchant Development%' then 'MDD' else account_owner end) as account_owner
	, ownerid


from (
select 
	accountid
	, max(vertical) as vertical
	, max(title_rw) as roles
	, max(mtd_attribution) as account_owner
	, max(ownerid) as ownerid
from sandbox.jc_merchant_mtd_attrib
group by 1
)dug
)with data primary index(accountid) on commit preserve rows;drop table sandbox.jrg_mdm_response_time;create table sandbox.jrg_mdm_response_time as (
select
	cast(dw.week_end as date) as report_wk
	, cas.createddate
	, cas.ID
	, cas.accountid 
	, cas.resolution_time_in_days__c
	, (cas.resolution_time_in_days__c * 24) as resolution_time_hours
	, (case when resolution_time_hours<24 then 1 else 0 end) as under_24_hours
	, cas.recordtypeid
	, cas.feature_country_account
	, rec.record_type_name
	, mtd.vertical as vertical
	, (case when mtd.account_owner='MD' then 'MD'
    	 when mtd.account_owner='New Metro' then 'Metro'
    	 when mtd.account_owner='Existing Metro' then 'Metro'
    	 when mtd.account_owner='Existing MS' then 'MS'
    	 when mtd.account_owner = 'MDD' then 'MDD'
    	 else 'Other' end)  as account_owner
	, mtd.ownerid as ownerid
	, per.person_id
from user_groupondw.sf_case cas 
left join user_groupondw.dim_sf_record_type rec 
	on rec.record_type_id = cas.recordtypeid
left join jrg_account_details mtd
	on mtd.accountid = cas.accountid
left join user_groupondw.dim_sf_person per
	on per.person_id = mtd.ownerid
join user_groupondw.dim_day dd 
	on cas.createddate = dd.day_rw
join user_groupondw.dim_week dw 
	on dd.week_key = dw.week_key
where rec.record_type_name='MC Self Service'
) with data primary index(ID);grant select on sandbox.jrg_mdm_response_time  to dv_engineering

--------------------------------------------------------------------edits_from_mdm




create volatile table jrg_cases_detail as (
select
	a.case_number
	, a.id
	, a.account_id
	, a.opened_date
	, a.closed_date
	, a.issue_category
	, a.issue_details
	, a.issue_subcategory_detail
	, (case when a.case_origin_adapted ='Campaign Editor (MDM)' then 1 else 0 end) as mdm_case
	, (case when  a.segmentation in ('Silver', 'Gold', 'Platinum') then 's+' else 'b-' end) metal_at_close
	, sf.feature_country country_code
	, (case when sf.feature_country in ('CA', 'US') then 'NAM' else 'INTL' end) region
	, b.l2
	, b.l3
	, b.pds
	, (case when mtd.account_owner in ('New Metro','Existing Metro') then 'Metro'
   	 			when mtd.account_owner in ('BD','MD', 'BD/MD') then 'MD'
   	 			when mtd.account_owner in ('Inbound','Existing MS') then 'MS'
   	 			else 'Other' end) account_owner
from sandbox.jrg_case_raw a
left join (
	        select
	            o1.account_id_18 as accountid 
	            , max(o1.feature_country) feature_country
	        from dwh_base_sec_view.sf_account o1
	        group by 1
	    ) sf
	on sf.accountid = a.account_id 
left join (
				select
        		accountid
        		, max(case when feature_country = 'US' and mtd_attribution = 'BD' and dmapi_flag = 1 then 'MD'
        			  	when feature_country = 'US' then mtd_attribution
        			  	when feature_country <> 'US' then mtd_attribution_intl end) as account_owner
 				from sandbox.jc_merchant_mtd_attrib def
        		group by accountid
		) mtd
	on mtd.accountid=a.account_id
left join (
			select 
				account_id 
				, max(l2) l2 
				, max(l3) l3
				, max(top_pds) pds
			from sandbox.pai_merchants
			group by 1
		) b 
	on b.account_id = a.account_id 
where a.issue_category in ('Deal Specific','Contract','Style Edit','Deal Edit')
and a.channel='Local'
) with data unique primary index (id) on commit preserve rows;drop table sandbox.jrg_edit_pct;create table sandbox.jrg_edit_pct as (
select
	dd.day_rw as report_date
	, cast(dw.week_end as date) as report_wk
	, cast(dm.month_start as date) as report_mth
	, cast(dq.quarter_start as date) as report_qtr
	, a.region
	, a.country_code
	, a.l2
	, a.l3
	, a.pds
	, a.metal_at_close
	, a.account_owner
	, count(distinct a.id) as cases_opened
	, count(distinct case when mdm_case=1 then a.id end) as cases_opened_mdm
	, cast(cases_opened_mdm as dec(18,3)) / cast(cases_opened as dec(18,3)) pct_mdm
from jrg_cases_detail a
join user_groupondw.dim_day dd
	on a.opened_date  = dd.day_rw
join user_groupondw.dim_week dw
	on dd.week_key = dw.week_key
join user_groupondw.dim_month dm
	on dd.month_key = dm.month_key
join user_groupondw.dim_quarter dq
	on dd.quarter_key = dq.quarter_key
where a.id not in (select distinct id from sandbox.jrg_case_raw where co_country not in ('United States', 'Canada') and issue_subcategory_detail in ('Original Value','Groupon Price') )
group by 1,2,3,4,5,6,7,8,9,10,11
)with data  primary index(report_wk,account_owner,region);grant select on  sandbox.jrg_edit_pct to dv_engineering;create volatile table jrg_rep_map as (
select
    o2.deal_uuid
    , o2.Opportunity_ID
    , o1.CloseDate
    , sfa.id accountid
    , sfa.name as account_name
    , sfa.ownerid
    , sfp.full_name
    , ros1.m1
    , ros1.rep
    , ros1.segment
    , ros1.title_rw
    , max(o1.division) division
from dwh_base_sec_view.sf_account sfa
 join dwh_base_sec_view.opportunity_1 o1
    on sfa.id = o1.accountid
join dwh_base_sec_view.opportunity_2 o2
    on o1.id = o2.id
join sandbox.jc_merchant_mtd_attrib mtd
    on mtd.deal_uuid = o2.deal_uuid
left join user_groupondw.dim_sf_person sfp
    on sfp.person_id = sfa.ownerid
left join sandbox.ops_roster_all ros1
    on ros1.roster_date = current_date-1
    and sfp.ultipro_id = ros1.emplid
group by 1,2,3,4,5,6,7,8,9,10,11
) with data unique primary index (deal_uuid) on commit preserve rows;

drop table sandbox.jrg_add_option;create table sandbox.jrg_add_option as (
select 
	mdm.report_date
	, mdm.report_wk
	, mdm.report_mth
	, mdm.report_qtr
	, mdm.review_uuid 
	, mdm.created_at 
	, mdm.updated_at 
	, mdm.change_uuid 
	, mdm.inv_product_uuid as service_id
	, mdm.deal_uuid 
	, mdm.merchant_uuid 
	, pm.merchant_name 
	, pm.account_id 
	, cast(coalesce(pp.first_live_date, mdm.report_date) as date) option_launched_date
	, pp.last_live_date option_pause_date
	, pp.is_live
	, coalesce(v.units_sold,0) options_units_sold
	, mdm.review_status
	, mdm.reject_reason 
	, mdm.salesforce_case_uuid 
	, mdm.l2 as vertical
	, mdm.country_code 
	, mdm.account_owner as acct_owner 
	, mtd.new_v_existing
   	, maps.rep as account_owner
   	, maps.m1 as account_owner_manager
   	, count(distinct case when option_launched_date - interval '1' day = ad.load_date then ad.options_live end) options_before
   	, count(distinct case when option_launched_date   = ad.load_date then ad.options_live end ) options_day_of
   	, count(distinct case when option_launched_date + interval '1' day  = ad.load_date then ad.options_live end ) options_after
from sandbox.jrg_mdm_deal_edit_test mdm 
join sandbox.pai_merchants pm 
	on pm.merchant_uuid = mdm.merchant_uuid 
left join jrg_rep_map maps
	on maps.deal_uuid = mdm.deal_uuid
left join (
			select 
				inventory_id
				, max(case when load_date = current_date-1 then 1 else 0 end) is_live
        		, min(load_date) first_live_date
        		, max(load_date) last_live_date
    		from user_groupondw.fact_active_deals
    		group by 1
		) pp 
	on pp.inventory_id = mdm.inv_product_uuid 
left join (
			select
  				inv_product_uuid
  				, sum(transaction_qty) units_sold
    		from user_edwprod.fact_gbl_transactions
    		where action = 'authorize'
    		and is_order_canceled = 0
    		and is_zero_amount = 0
    		group by 1
		) v 
	on v.inv_product_uuid = mdm.inv_product_uuid 
left join (
			select
        	deal_uuid
        	, max(case when close_order = 1 then 'New Merchant' else 'Existing Merchant' end) new_v_existing
 			from sandbox.jc_merchant_mtd_attrib def
        group by 1
		)mtd
	on mtd.deal_uuid = mdm.deal_uuid
left join (
			select 
				deal_uuid
				, load_date
				, inventory_id options_live
    		from user_groupondw.fact_active_deals
    		--group by 1,2
    		where load_date>'2020-04-01'
		) ad 
	on ad.deal_uuid = mdm.deal_uuid 
where mdm.review_status='APPROVED'
and mdm.detail='new_option'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26
) with data primary index(review_uuid);grant select on sandbox.jrg_add_option to public;
grant select on sandbox.jrg_mdm_deal_edit_test to public;grant select on sandbox.jrg_pause_status_test to public

	   
----------------------------------------------------------------------------------pause_free_text

drop table sandbox.jrg_pause_free_text;create table sandbox.jrg_pause_free_text as(
select 
  ps.report_wk as wk
  , ps.deal_uuid
  , ps.l2 as vertical
  , ps.account_owner
  , ps.metal_at_close
  , m.name merchant_name
  , u.lifetime_units_sold
  , ps.merchant_free_text
  , ps.pause_reason1 as pause_reason
, ps.country_code
, ps.region
from sandbox.jrg_pause_status_test ps

left join user_edwprod.dim_merchant m
  on m.merchant_uuid= ps.merchant_uuid

left join(
  select
    deal_id
    , sum(net_transactions_qty - zdo_net_transactions_qty) lifetime_units_sold
  from user_edwprod.agg_gbl_financials_deal gfd
  group by 1
) u on u.deal_id = ps.deal_uuid

where lower(ps.review_status) = 'approved'
) with data primary index(wk, deal_uuid);grant select on  sandbox.jrg_pause_free_text to dv_engineering


------------------------------------------------------------------------------------resolution

drop table sandbox.jrg_mdm_resolution_time;create volatile table jrg_account_details as (
	select
    	dm.salesforce_account_id,
        max(gdl.grt_l2_cat_description) l2,
        max(gdl.country_code) country_code,
        max(case when gdl.country_code in ('US','CA') then 'NAM' else 'INTL' end) region,
        max(case when mtd.feature_country = 'US' and mtd.mtd_attribution = 'BD' and mtd.dmapi_flag = 1 then 'MD'
        			  	when mtd.feature_country = 'US' then mtd.mtd_attribution
        			  	when mtd.feature_country <> 'US' then mtd.mtd_attribution_intl end) as account_owner
    from user_edwprod.dim_gbl_deal_lob gdl
    join user_edwprod.dim_offer_ext doe
    	on gdl.deal_id = doe.product_uuid
    join user_edwprod.dim_merchant dm
    	on doe.merchant_uuid = dm.merchant_uuid
    join sandbox.jc_merchant_mtd_attrib mtd
		on gdl.deal_id = mtd.deal_uuid
    group by 1
)with data primary index(salesforce_account_id) on commit preserve rows;
create table sandbox.jrg_mdm_resolution_time as (
select
	dd.day_rw as report_date,
	cast(dw.week_end as date) as report_wk,
	cast(dm.month_start as date) as report_mth,
	cast(dq.quarter_start as date) as report_qtr,
	vw.origin,
	vw.case_number,
	vw.id,
	vw.account_id,
	vw.opened_date,
	vw.closed_date,
	--vw.msat_score_sum,
	--vw.msat_surveys,
	(vw.resolution_time * 24) resolution_time_hours,
	vw.channel,
	vw.centre,
	vw.account_owner,
	vw.co_country,
	--vw.exclusion_reason,
	vw.issue_category,
	vw.issue_details,
 	vw.issue_subcategory_detail,
 	vw.record_type_name,
 	vw.segmentation,
 	jg.l2,
 	jg.country_code,
 	jg.region,
 	(case when jg.account_owner in ('New Metro','Existing Metro') then 'Metro'
   	 		when jg.account_owner in ('BD','MD', 'BD/MD') then 'MD'
   	 		when jg.account_owner in ('Inbound','Existing MS') then 'MS'
   	 		else 'Other' end) acct_owner,
 	(case when vw.issue_details in ('Change of Contract Terms','Capacity Change','Change Expiry Date', 'Clarification of Terms','Feature Date/Period',
						'Incorrect Deal Description','Copy', 'Image', 'Website', 'Incorrect Merchant Description', 'Merchant Pages',
						'Video', 'Not Scheduled/Not Published', 'Change Company Details')
	and vw.issue_category in ('Deal Specific','Contract','Style Edit','Deal Edit','Tool Specific')
	then 1 else 0 end) edit_flag
    , case when coalesce(mdm.salesforce_case_uuid,'')<>'' then 1 else 0 end mdm_flag
from sandbox.ms_tableau_cases_vw vw
left join jrg_account_details jg
	on jg.salesforce_account_id = vw.account_id
left join sb_merchant_experience.merchant_self_service_changes mdm
	on mdm.salesforce_case_uuid = vw.id
join user_groupondw.dim_day dd
	on cast(vw.opened_date as date) = dd.day_rw
join user_groupondw.dim_week dw
	on dd.week_key = dw.week_key
join user_groupondw.dim_month dm
	on dd.month_key = dm.month_key
join user_groupondw.dim_quarter dq
	on dd.quarter_key = dq.quarter_key
where cast(opened_date as date) between '2020-07-01' and current_date
and vw.issue_details<>'SPAM/Autoresponder'
)with data primary index(id);grant select on sandbox.jrg_mdm_resolution_time to dv_engineering
