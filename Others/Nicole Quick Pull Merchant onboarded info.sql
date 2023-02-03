drop table np_currently_live_merchant;
create volatile table np_currently_live_merchant as (
sel b.merchant_uuid, 
    min(load_date) launch_date, 
    max(country) country
from sandbox.sh_bt_active_deals_log as a 
     left join user_edwprod.dim_offer_ext as b on a.deal_uuid = b.product_uuid
     where a.load_date > current_date - 3 and partner_inactive_flag = 0 and product_is_active_flag = 1
    group by 1) with data on commit preserve rows;
     

drop table np_merchant_info;
create volatile table np_merchant_info as (
sel b.merchant_uuid, 
    min(cast(a.load_date as date)) launch_date, 
    max(a.country) country,
    max(sfa.id) account_id,
    max(sfa.name) account_name,
    max(grt_l2_cat_name) vertical,
    max(sfa.category_v3) sfa_vertical
from sandbox.sh_bt_active_deals_log as a 
     left join user_edwprod.dim_offer_ext as b on a.deal_uuid = b.product_uuid
     left join user_edwprod.dim_gbl_deal_lob as c on a.deal_uuid = c.deal_id
     left join dwh_base_sec_view.opportunity_2 o2 on o2.deal_uuid = a.deal_uuid
     left join dwh_base_sec_view.opportunity_1 o1 on o2.id = o1.id
     left join dwh_base_sec_view.sf_account sfa on o1.accountid = sfa.id
     where partner_inactive_flag = 0 
           and product_is_active_flag = 1
group by 1
) with data unique primary index(merchant_uuid) on commit preserve rows



select 
    a.*, 
    case when b.merchant_uuid is not null then 1 else 0 end currently_live_merchant, 
    cont.email,
    cont.email_opt_out, 
    cont.news_opt_in,
    surveys_opt_in,
    cont2.firstname contact_firstname,
    cont2.lastname contact_lastname,
    cont.contact_id, 
    cont2.eloqua_master__c
from 
    np_merchant_info as a 
    left join np_currently_live_merchant as b on a.merchant_uuid = b.merchant_uuid
    left join user_groupondw.dim_contact cont on a.account_id = cont.account_id
    left join dwh_load_sf_view.sf_contact cont2 on substr(cont.contact_id,1,15) = substr(cont2.id,1,15)
    order by a.launch_date, a.merchant_uuid
;    



select * from user_groupondw.dim_contact;
select * from dwh_load_sf_view.sf_contact;
select * from user_groupondw.dim_opportunity_contactrole;


create volatile table sh_booking_solution as (
    select o2.deal_uuid,
        max(o1.id) opp_id,
        max(sfa.id) account_id,
        max(case when lower(sfa.scheduler_setup_type) in ('pen & paper','none') then 'pen & paper'
            when sfa.scheduler_setup_type is null then 'no data'
            else 'some booking tool'
            end) current_booking_solution,
        max(sfa.scheduler_setup_type) detailed_booking_solution,
        max(sfa.name) account_name,
        max(company_type) company_type,
        max(o1.division) division,
        max(sfp.full_name) account_owner, 
        max(metal_at_close) metal_segmentation
    from dwh_base_sec_view.sf_opportunity_1 o1
    join dwh_base_sec_view.sf_opportunity_2 o2 on o1.id = o2.id
    join dwh_base_sec_view.sf_account sfa on o1.accountid = sfa.id
    left join user_groupondw.dim_sf_person sfp on sfa.ownerid = sfp.person_id
    left join sandbox.rev_mgmt_deal_attributes mat on o2.deal_uuid = mat.deal_id
    group by o2.deal_uuid
) with data unique primary index (deal_uuid) on commit preserve rows;





select account_id, count(distinct email) cnz from user_groupondw.dim_contact group by 1 having cnz >1 ;

select * from user_groupondw.dim_contact where account_id = '001C000001kF7FYIA0';

select * from user_groupondw.dim_contact;


create table sandbox.jc_mr_aug_survey as (
    select
      cont.contact_id
      , cont2.firstname contact_firstname
      , cont2.lastname contact_lastname
      , cont.email contact_email
      , dm.name merchant_name
      , cont.account_id salesforce_account_id
      , dm.merchant_uuid
      , sfa.billingcity
      , sfa.category_v3 as vertical
      , sfa.merchant_segmentation__c as metal
      , case when lv.deals_live is not null then 1 else 0 end as curr_live_flag
      , case when lv_e.accountid is not null then 1 else 0 end as ever_live_flag
      , case when lv_e.accountid is not null and lv_e.acct_attrit_date < '2020-08-18' then lv_e.acct_attrit_date else null end as acct_pause_date
    from user_groupondw.dim_contact cont
    join user_groupondw.dim_opportunity_contactrole crol
      on substr(crol.contact_id,1,15) = substr(cont.contact_id,1,15)
    join dwh_load_sf_view.sf_contact cont2
      on substr(cont.contact_id,1,15) = substr(cont2.id,1,15)
    join user_edwprod.dim_merchant dm
      on cont.account_id = dm.salesforce_account_id
    left join dwh_base_sec_view.sf_account sfa
      on sfa.account_id_18 = cont.account_id
    left join (
      select accountid, count(distinct ad.deal_uuid) deals_live
      from user_groupondw.active_deals ad
      left join dwh_base_sec_view.opportunity_2 o2
        on o2.deal_uuid = ad.deal_uuid
      left join dwh_base_sec_view.opportunity_1 o1
        on o2.id = o1.id
      where load_date = '2020-08-18'
      group by 1
      ) lv
      on lv.accountid = cont.account_id
    left join (
      select accountid, max(load_date) as acct_attrit_date
      from user_groupondw.active_deals ad
      left join dwh_base_sec_view.opportunity_2 o2
        on o2.deal_uuid = ad.deal_uuid
      left join dwh_base_sec_view.opportunity_1 o1
        on o2.id = o1.id
      group by 1
      ) lv_e
      on lv_e.accountid = cont.account_id
    where cont.email_opt_out = 0
      and cont.surveys_opt_in = 1
      and crol.isprimary = 'true'
      and char_length(cont.account_id) = 18
      and sfa.feature_country = 'US'
      and vertical in ('Beauty / Wellness / Healthcare', 'Food & Drink', 'Leisure Offers / Activities')
  ) with data primary index (contact_id)
;