select 
      cast(generated_datetime as date) generated_date,
      trunc(generated_datetime, 'iw') + 6 week_date,
      merchant_id, 
      deal_id,
      account_owner,
      acct_owner_name,
      contact_full_name,
      account_name,
      sum(impressioned) impressions,
      sum(clicked) clicks, 
      sum(impression_spend_amount) impressions_spend_amount, 
      sum(total_spend_amount) total_spend_amount
from user_gp.ads_reconciled_report a 
left join 
(       select d.deal_uuid, 
               max(m.account_id) account_id, 
               max(m.acct_owner) account_owner, 
               max(acct_owner_name) acct_owner_name,
               max(contact_full_name) contact_full_name,
               max(sfa.name) account_name, 
               max(merchant_segmentation__c) merch_segmentation
        from sandbox.pai_deals d 
        join sandbox.pai_merchants m on d.merchant_uuid = m.merchant_uuid
        join dwh_base_sec_view.sf_account sfa on m.account_id = sfa.id
        group by d.deal_uuid
    ) sf on a.deal_id = sf.deal_uuid
group by 1,2,3,4,5,6,7,8
;


select * 
from sandbox.citrusad_team_wallet where is_self_serve = 0;


select * from user_gp.ads_rcncld_intrmdt_rpt;

