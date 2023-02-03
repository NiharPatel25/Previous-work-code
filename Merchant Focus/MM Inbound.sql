 drop table np_ss_inelig;
 create volatile table np_ss_inelig as (
      sel deal_uuid,
          max(case when o2.merchant_provided_codes is not null then 1 else 0 end) has_custom_codes,
          max(case when o1.multi_account_multi_deal is not null then 1 else 0 end) is_mamd,
          max(case when o1.primary_deal_services in ('Merchant Service (PDS)','House / Room - Sexy','Animal Husbandry / Breeding','Animal Rental','ATM','Betting','Boiler Repair / Checkup','Implants','Breast Enhancement - Non-Surgical','Breast Lift','Breast Lift - Non-Surgical',
                  'Breast Reduction','Breast Reduction - Non-Surgical','Brow / Forehead Lift','Brow / Forehead Lift - Non-Surgical','Car - Luxury','Car - Standard','Cardiologist','Casino Games','Cheek Augmentation','Child Care','Children''s Party Performer','Chimney Sweep',
                  'Cigarette','Credit Card','Escort Services','Eyebrow Transplant','Facelift - Surgical','Facelift - Thread Lift','Fat / Cellulite Reduction - Non-Branded','Testing - Fertility','Fireworks','Fur','Furnace / Dryer Vent','Furnace Tune Up','Gastric Band',
                  'Genealogy / Ancestry Research','General Handyman','Gluteoplasty / Butt Lift','Gluteoplasty / Butt Lift - Non-Surgical','Government / Public Services','Gun / Firearm','Hormone Replacement Therapy','Hormone Replacement Therapy - Testosterone',
                  'Human chorionic gonadotropin / hCG','Human chorionic gonadotropin / hCG - Homeopathic','HVAC Cleaning','HVAC Service / Repair','Injection - B12','Injection - Bellafill','Injection - Botox','Injection - Botox - Men','Injection - Dermal Filler',
                  'Injection - Filler & Freezer','Injection - Hyaluronic Acid','Injection - Organic','Injection - Other Branded (Dysport)','Injection - Ozone Therapy','Injection - Plasma','Injection - Restylane','Injection - Sculptra','Injection - Vitamin C',
                  'Injection - Voluma','Mesotherapy','Inspection - Heating / Ventilation / A/C','Insurance - Life','Insurance','iOs Jailbreaking','Liposuction - Surgical','Lottery','Marijuana','Mortgage','Nose Job','Off-Road Vehicle - For Purchase','Betting - Online',
                  'Party - Children''s','Photobooth','Puppy / Kitty Mill','Sensual Courses','Sensual Massage Course','Shooting Range','Slot Machine','Strip Club','Surgery - Chin','Surgery - Ear Pinning','Surgery - Eyelid','Sweepstakes','Toy - Adult / Erotic','Tummy Tuck',
                  'Vaginoplasty / Vaginal Rejuvenation','Facial - PRP / Platelet Rich Plasma / Vampire','Fat / Cellulite Reduction - Branded (Zerona, Velashape)','Boiler Repair / Checkup',
                  'Breast Enhancement - Non-Surgical', 'Breast Lift', 'Breast Lift - Non-Surgical', 'Breast Reduction', 'Breast Reduction - Non-Surgical', 'Brow / Forehead Lift', 'Brow / Forehead Lift - Non-Surgical', 'Cardiologist', 'Casino Games', 'Cheek Augmentation',
                  'Child Care', 'Children''s Party Performer', 'Chimney Sweep', 'Eyebrow Transplant', 'Facelift - Surgical', 'Facelift - Thread Lift', 'Fat / Cellulite Reduction - Branded (Zerona, Velashape)', 'Fat / Cellulite Reduction - Non-Branded', 'Fireworks', 'Furnace / Dryer Vent',
                  'Furnace Tune Up', 'Gastric Band', 'Genealogy / Ancestry Research', 'General Handyman', 'Gluteoplasty / Butt Lift', 'Gluteoplasty / Butt Lift - Non-Surgical', 'HVAC Cleaning', 'HVAC Service / Repair', 'Implants', 'Injection - B12', 'Injection - Bellafill', 'Injection - Botox',
                  'Injection - Botox - Men', 'Injection - Dermal Filler', 'Injection - Filler & Freezer', 'Injection - Hyaluronic Acid', 'Injection - Organic', 'Injection - Other Branded (Dysport)', 'Injection - Ozone Therapy', 'Injection - Plasma', 'Injection - Restylane', 'Injection - Sculptra',
                  'Injection - Vitamin C', 'Injection - Voluma', 'Inspection - Heating / Ventilation / A/C', 'Liposuction - Surgical', 'Mesotherapy', 'Nose Job', 'Party - Children''s', 'Shooting Range','Surgery - Chin', 'Surgery - Ear Pinning', 'Surgery - Eyelid',
                  'Toy - Adult / Erotic',
                  'Tummy Tuck')
              then 1 else 0 end) ineligible_pds,
          max(below_price_floor) below_price_floor,
          coalesce(max(rda.enterprise_flag),0) enterprise_deal
      from user_edwprod.sf_opportunity_1 o1
      join user_edwprod.sf_opportunity_2 o2 on o1.id = o2.id
      left join sandbox.rev_mgmt_deal_attributes rda on rda.opportunity_id = o1.opportunity_id
      join (
          sel product_uuid,
              max(case when pds_cat_name in ('Botox Hair Treatment','Injection - Botox - Men','Botox - Dental','Injection - Botox') and offer_description like '%20%' and contract_sell_price < 159 then 1
                       when pds_cat_name in ('Botox Hair Treatment','Injection - Botox - Men','Botox - Dental','Injection - Botox') and offer_description like '%40%' and contract_sell_price < 319 then 1
                       when pds_cat_name = 'Facial - HydraFacial' and contract_sell_price < 149 then 1
                       when pds_cat_name = 'CoolSculpting' and contract_sell_price < 600 then 1
                       /*when pds_cat_name = 'Micro-Needling' and contract_sell_price < 600 then 1*/
                       when pds_cat_name in ('HVAC Cleaning','HVAC Service / Repair','Inspection - Heating / Ventilation / A/C') and contract_sell_price < 99 then 1
                  else 0 end) below_price_floor
          from user_edwprod.dim_offer_ext doe
          join user_dw.v_dim_pds_grt_map pds on doe.pds_cat_id = pds.pds_cat_id
          group by 1
      ) doe on o2.deal_uuid = doe.product_uuid
      group by 1
  ) with data unique primary index (deal_uuid) on commit preserve rows;

 

---first launch owner was pulled from mtd_attrib which is not removed temporarily
----for the deal one we had acct_owner_detailed which is also from mtd but its the one about attribution 

create volatile table sh_lnch_merch as (
select 
  sne.merchant_uuid, 
  coalesce(sne.country_code, pm.country_code) country_code, 
  case when coalesce(sne.country_code, pm.country_code) in ('US','CA') then 'NAM' else 'INTL' end region,
  min(sne.launch_date) first_launch_date, 
  max(sne.campaign_group_lvl1_2) campaign_group_lvl1_2, 
  max(sne.campaign_group_lvl1_1) campaign_group_lvl1_1, 
  max(sne.campaign_paid_category) campaign_paid_category, 
  max(sne.grt_l2_cat_name) l2, 
  max(sne.metal) metal, 
  max(case when sne.leadsource = 'metro - self service' then 1 else 0 end) from_inbound, 
  max(pm.acct_owner_detailed) acct_owner_detailed
from sandbox.pai_lead_opp_mtd_attrib sne
left join sandbox.pai_merchants pm on sne.merchant_uuid = pm.merchant_uuid
where launch_order = 1
group by 1,2,3
  ) with data unique primary index (merchant_uuid, country_code) on commit preserve rows;
 
--------max(mtd.acct_owner) acct_owner, this is removed as this is also from mtd
 
create volatile table sh_lnchs_deal as (
select 
  sne.deal_uuid, 
  sne.merchant_uuid,
  sne.launch_date, 
  case when inel.deal_uuid is null then 1 else 0 end ss_eligible_flag,
  case when coalesce(t_o.tier,2) = 2 then 1 else 0 end deal_type_flag,
  case when launch_order = 1 then 'new' else 'existing' end first_launch,
  pd.l2 l2,
  pd.l3 l3,
  case when lower(pd.metal_at_close) in ('gold','silver','platinum','nickel','bronze') then lower(pd.metal_at_close) else 'unsegmented' end metal,
  slm.from_inbound,
  case when coalesce(sne.country_code, pd.country_code) in ('US','CA') then 'NAM' else 'INTL' end region,
  coalesce(sne.country_code, pd.country_code) country_code, 
  slm.first_launch_date merchant_launch_date,
  case when dp.is_stps = 1 then 1 else 0 end is_stps,
  sne.dmapi_flag is_ss, 
  is_top_pds is_primary_service
from sandbox.pai_lead_opp_mtd_attrib sne
left join sandbox.pai_deals pd on sne.deal_uuid = pd.deal_uuid
left join user_edwprod.dim_product dp on sne.deal_uuid = dp.product_uuid
left join (
          select *
          from np_ss_inelig
          where has_custom_codes + is_mamd + ineligible_pds + below_price_floor + enterprise_deal > 0
        ) inel
        on inel.deal_uuid = sne.deal_uuid
left join sandbox.eh_to_closes t_o on t_o.deal_uuid = sne.deal_uuid
left join sh_lnch_merch slm on sne.merchant_uuid = slm.merchant_uuid and slm.country_code =  coalesce(sne.country_code, pd.country_code)
where sne.deal_uuid is not null
) with data unique primary index (deal_uuid, merchant_uuid) on commit preserve rows
;



create volatile table sh_fin_dl as (
   select 
         t.report_date ,
         t.deal_id, 
         pd.merchant_uuid,
         units_sold,
         nob,
         nor,
         ogp,
         refund_usd,
         GB_usd,
         ((sum(refund_usd)*1.000)/(sum(GB_usd)*1.000)*100)as ref_per
  from 
       (
         sel 
            report_date,
            deal_id,
            sum(net_transactions_qty - zdo_net_transactions_qty) units_sold,
            sum(nob_loc * coalesce(er.approved_avg_exchange_rate,1)) nob,
            sum(nor_loc * coalesce(er.approved_avg_exchange_rate,1)) nor
        from user_edwprod.agg_gbl_financials_deal f
        join user_groupondw.dim_day dd on dd.day_rw = f.report_date
        join (
           sel currency_from,
               currency_to,
               fx_neutral_exchange_rate,
               approved_avg_exchange_rate,
               period_key
            from user_groupondw.gbl_fact_exchange_rate
            where currency_to = 'USD'
            group by 1,2,3,4,5
            ) er on f.currency_code = er.currency_from and dd.month_key = er.period_key
            group by 1,2
       ) t
       left join sandbox.pai_deals pd on t.deal_id = pd.deal_uuid
       join (
           sel report_date,
               deal_id,
               sum(ogp_loc * coalesce(er.approved_avg_exchange_rate,1)) ogp
            from user_edwprod.agg_gbl_ogp_financials_deal f
            join user_groupondw.dim_day dd on dd.day_rw = f.report_date
            join (
                 sel currency_from,
                     currency_to,
                     fx_neutral_exchange_rate,
                     approved_avg_exchange_rate,
                     period_key
                  from user_groupondw.gbl_fact_exchange_rate
                  where currency_to = 'USD'
                  group by 1,2,3,4,5
                  ) er on f.currency_code = er.currency_from and dd.month_key = er.period_key
                  group by 1,2) o on t.report_date = o.report_date and t.deal_id = o.deal_id
       left join (
            select  
                 deal_uuid,
                 sum(case when action = 'refund' then -1* REFUND_AMOUNT_USD end) as refund_usd, 
                 sum(case when action = 'capture' then GROSS_BOOKINGS_USD end) as GB_usd
             from user_dw.v_fact_gbl_transactions 
             where order_date >= '2021-11-01'
             group by 1) r on r.deal_uuid =o.deal_id
        where  GB_usd <> 0
        group by 1,2,3,4,5,6,7,8,9
  ) with data unique primary index (report_date, deal_id) on commit preserve rows;
  
 create volatile table sh_still_live_dl as (
      sel distinct 
          sld.deal_uuid,
          sld.launch_date,
          pd.last_live_date,
          pd.last_live_date - sld.launch_date days_live
      from sh_lnchs_deal sld
      join sandbox.pai_deals pd on sld.deal_uuid = pd.deal_uuid
) with data unique primary index (deal_uuid) on commit preserve rows;


create table sandbox.np_rep_vs_ss_v1 as (
  sel td_quarter_end(ld.launch_date) launch_qtr,
      td_month_end(ld.launch_date) launch_mth,
      cast(dw.week_end as date) launch_wk,
      first_launch,
      l2,
      l3,
      metal,
      from_inbound,
      region,
      country_code,
      is_stps,
      is_ss,
      is_primary_service,
      ss_eligible_flag,
      deal_type_flag,
      count(distinct ld.deal_uuid) deals_launched,
      count(distinct case when l.days_live >= 7 then ld.deal_uuid end) n_retained_7day,
      count(distinct case when l.days_live >= 30 then ld.deal_uuid end) n_retained_30day,
      count(distinct case when l.days_live >= 90 then ld.deal_uuid end) n_retained_90day,
      count(distinct case when l.days_live >= 180 then ld.deal_uuid end) n_retained_180day,
      count(distinct case when l.days_live >= 365 then ld.deal_uuid end) n_retained_365day,
      sum(case when f.report_date between ld.launch_date and ld.launch_date + interval '6' day then f.nob end) nob_7day,
      sum(case when f.report_date between ld.launch_date and ld.launch_date + interval '29' day then f.nob end) nob_30day,
      sum(case when f.report_date between ld.launch_date and ld.launch_date + interval '89' day then f.nob end) nob_90day,
      sum(case when f.report_date between ld.launch_date and ld.launch_date + interval '179' day then f.nob end) nob_180day,
      sum(case when f.report_date between ld.launch_date and ld.launch_date + interval '364' day then f.nob end) nob_365day,
      sum(case when f.report_date between ld.launch_date and ld.launch_date + interval '6' day then f.nor end) nor_7day,
      sum(case when f.report_date between ld.launch_date and ld.launch_date + interval '29' day then f.nor end) nor_30day,
      sum(case when f.report_date between ld.launch_date and ld.launch_date + interval '89' day then f.nor end) nor_90day,
      sum(case when f.report_date between ld.launch_date and ld.launch_date + interval '179' day then f.nor end) nor_180day,
      sum(case when f.report_date between ld.launch_date and ld.launch_date + interval '364' day then f.nor end) nor_365day,
      sum(case when f.report_date between ld.launch_date and ld.launch_date + interval '6' day then f.ogp end) ogp_7day,
      sum(case when f.report_date between ld.launch_date and ld.launch_date + interval '29' day then f.ogp end) ogp_30day,
      sum(case when f.report_date between ld.launch_date and ld.launch_date + interval '89' day then f.ogp end) ogp_90day,
      sum(case when f.report_date between ld.launch_date and ld.launch_date + interval '179' day then f.ogp end) ogp_180day,
      sum(case when f.report_date between ld.launch_date and ld.launch_date + interval '364' day then f.ogp end) ogp_365day,
      sum(case when f.report_date between ld.launch_date and ld.launch_date + interval '6' day then f.units_sold end) units_7day,
      sum(case when f.report_date between ld.launch_date and ld.launch_date + interval '29' day then f.units_sold end) units_30day,
      sum(case when f.report_date between ld.launch_date and ld.launch_date + interval '89' day then f.units_sold end) units_90day,
      sum(case when f.report_date between ld.launch_date and ld.launch_date + interval '179' day then f.units_sold end) units_180day,
      sum(case when f.report_date between ld.launch_date and ld.launch_date + interval '364' day then f.units_sold end) units_365day,
      average(case when f.report_date between ld.merchant_launch_date and ld.merchant_launch_date + interval '6' day then f.ref_per end) refund_7day,
      average(case when f.report_date between ld.merchant_launch_date and ld.merchant_launch_date + interval '29' day then f.ref_per end) refund_30day,
      average(case when f.report_date between ld.merchant_launch_date and ld.merchant_launch_date + interval '89' day then f.ref_per end) refund_90day,
      average(case when f.report_date between ld.merchant_launch_date and ld.merchant_launch_date + interval '179' day then f.ref_per end) refund_180day,
      average(case when f.report_date between ld.merchant_launch_date and ld.merchant_launch_date + interval '364' day then f.ref_per end) refund_365day
  from sh_lnchs_deal ld
  join user_groupondw.dim_day dd on ld.launch_date = dd.day_rw
  join user_groupondw.dim_week dw on dd.week_key = dw.week_key
  left join sh_still_live_dl l on ld.deal_uuid = l.deal_uuid
  left join sh_fin_dl f on ld.deal_uuid = f.deal_id
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
  ) with data no primary index;


---there is one merchant that is coming up twice not sure why
select * from sandbox.pai_opp_mtd_attrib where deal_uuid = '8c22c549-cbba-4d13-b7c4-73d23f4aea05';
select * from user_edwprod.dim_merchants where salesforce_account_id like '%001C000001gEt3vIAC%';
select * from sandbox.np_merch_lead_asgnmt where account_id = '0013c00001zF2PVAA0';
select deal_uuid, count(1) xyz from sh_lnchs_deal group by 1 having xyz > 1;
select * from sh_lnchs_deal where deal_uuid = '8c22c549-cbba-4d13-b7c4-73d23f4aea05';
select * from sandbox.pai_lead_opp_mtd_attrib where deal_uuid = '2bc35647-aa04-43df-a443-b8f0c0f2c247';
---join user_edwprod.dim_gbl_deal_lob gdl on sne.deal_uuid = gdl.deal_id


 

------OLD 2

 select enterprise_flag from sandbox.rev_mgmt_deal_attributes sample 5;
 
 create volatile table jc_ss_inelig as (
      sel deal_uuid,
          max(case when o2.merchant_provided_codes is not null then 1 else 0 end) has_custom_codes,
          max(case when o1.multi_account_multi_deal is not null then 1 else 0 end) is_mamd,
          max(case when o1.primary_deal_services in ('Merchant Service (PDS)','House / Room - Sexy','Animal Husbandry / Breeding','Animal Rental','ATM','Betting','Boiler Repair / Checkup','Implants','Breast Enhancement - Non-Surgical','Breast Lift','Breast Lift - Non-Surgical',
                  'Breast Reduction','Breast Reduction - Non-Surgical','Brow / Forehead Lift','Brow / Forehead Lift - Non-Surgical','Car - Luxury','Car - Standard','Cardiologist','Casino Games','Cheek Augmentation','Child Care','Children''s Party Performer','Chimney Sweep',
                  'Cigarette','Credit Card','Escort Services','Eyebrow Transplant','Facelift - Surgical','Facelift - Thread Lift','Fat / Cellulite Reduction - Non-Branded','Testing - Fertility','Fireworks','Fur','Furnace / Dryer Vent','Furnace Tune Up','Gastric Band',
                  'Genealogy / Ancestry Research','General Handyman','Gluteoplasty / Butt Lift','Gluteoplasty / Butt Lift - Non-Surgical','Government / Public Services','Gun / Firearm','Hormone Replacement Therapy','Hormone Replacement Therapy - Testosterone',
                  'Human chorionic gonadotropin / hCG','Human chorionic gonadotropin / hCG - Homeopathic','HVAC Cleaning','HVAC Service / Repair','Injection - B12','Injection - Bellafill','Injection - Botox','Injection - Botox - Men','Injection - Dermal Filler',
                  'Injection - Filler & Freezer','Injection - Hyaluronic Acid','Injection - Organic','Injection - Other Branded (Dysport)','Injection - Ozone Therapy','Injection - Plasma','Injection - Restylane','Injection - Sculptra','Injection - Vitamin C',
                  'Injection - Voluma','Mesotherapy','Inspection - Heating / Ventilation / A/C','Insurance - Life','Insurance','iOs Jailbreaking','Liposuction - Surgical','Lottery','Marijuana','Mortgage','Nose Job','Off-Road Vehicle - For Purchase','Betting - Online',
                  'Party - Children''s','Photobooth','Puppy / Kitty Mill','Sensual Courses','Sensual Massage Course','Shooting Range','Slot Machine','Strip Club','Surgery - Chin','Surgery - Ear Pinning','Surgery - Eyelid','Sweepstakes','Toy - Adult / Erotic','Tummy Tuck',
                  'Vaginoplasty / Vaginal Rejuvenation','Facial - PRP / Platelet Rich Plasma / Vampire','Fat / Cellulite Reduction - Branded (Zerona, Velashape)','Boiler Repair / Checkup',
                  'Breast Enhancement - Non-Surgical', 'Breast Lift', 'Breast Lift - Non-Surgical', 'Breast Reduction', 'Breast Reduction - Non-Surgical', 'Brow / Forehead Lift', 'Brow / Forehead Lift - Non-Surgical', 'Cardiologist', 'Casino Games', 'Cheek Augmentation',
                  'Child Care', 'Children''s Party Performer', 'Chimney Sweep', 'Eyebrow Transplant', 'Facelift - Surgical', 'Facelift - Thread Lift', 'Fat / Cellulite Reduction - Branded (Zerona, Velashape)', 'Fat / Cellulite Reduction - Non-Branded', 'Fireworks', 'Furnace / Dryer Vent',
                  'Furnace Tune Up', 'Gastric Band', 'Genealogy / Ancestry Research', 'General Handyman', 'Gluteoplasty / Butt Lift', 'Gluteoplasty / Butt Lift - Non-Surgical', 'HVAC Cleaning', 'HVAC Service / Repair', 'Implants', 'Injection - B12', 'Injection - Bellafill', 'Injection - Botox',
                  'Injection - Botox - Men', 'Injection - Dermal Filler', 'Injection - Filler & Freezer', 'Injection - Hyaluronic Acid', 'Injection - Organic', 'Injection - Other Branded (Dysport)', 'Injection - Ozone Therapy', 'Injection - Plasma', 'Injection - Restylane', 'Injection - Sculptra',
                  'Injection - Vitamin C', 'Injection - Voluma', 'Inspection - Heating / Ventilation / A/C', 'Liposuction - Surgical', 'Mesotherapy', 'Nose Job', 'Party - Children''s', 'Shooting Range','Surgery - Chin', 'Surgery - Ear Pinning', 'Surgery - Eyelid',
                  'Toy - Adult / Erotic',
                  'Tummy Tuck')
              then 1 else 0 end) ineligible_pds,
          max(below_price_floor) below_price_floor,
          coalesce(max(rda.enterprise_flag),0) enterprise_deal
      from user_edwprod.sf_opportunity_1 o1
      join user_edwprod.sf_opportunity_2 o2 on o1.id = o2.id
      left join sandbox.rev_mgmt_deal_attributes rda on rda.opportunity_id = o1.opportunity_id
      join (
          sel product_uuid,
              max(case when pds_cat_name in ('Botox Hair Treatment','Injection - Botox - Men','Botox - Dental','Injection - Botox') and offer_description like '%20%' and contract_sell_price < 159 then 1
                       when pds_cat_name in ('Botox Hair Treatment','Injection - Botox - Men','Botox - Dental','Injection - Botox') and offer_description like '%40%' and contract_sell_price < 319 then 1
                       when pds_cat_name = 'Facial - HydraFacial' and contract_sell_price < 149 then 1
                       when pds_cat_name = 'CoolSculpting' and contract_sell_price < 600 then 1
                       /*when pds_cat_name = 'Micro-Needling' and contract_sell_price < 600 then 1*/
                       when pds_cat_name in ('HVAC Cleaning','HVAC Service / Repair','Inspection - Heating / Ventilation / A/C') and contract_sell_price < 99 then 1
                  else 0 end) below_price_floor
          from user_edwprod.dim_offer_ext doe
          join user_dw.v_dim_pds_grt_map pds on doe.pds_cat_id = pds.pds_cat_id
          group by 1
      ) doe on o2.deal_uuid = doe.product_uuid
      group by 1
  ) with data unique primary index (deal_uuid) on commit preserve rows;
  
 
create volatile table sh_launch_dates as (
      sel sne.deal_uuid,
          sne.merchant_uuid,
          sne.launch_date,
          max(case when inel.deal_uuid is null then 1 else 0 end) ss_eligible_flag,
          max(case when coalesce(t_o.tier,2) = 2 then 1 else 0 end) deal_type_flag,
          max(sne.is_new) first_launch,
          max(gdl.grt_l2_cat_description) l2,
          max(pd.l3) l3,
          max(case when lower(pd.metal_at_close) in ('gold','silver','platinum','nickel','bronze') then lower(pd.metal_at_close) else 'unsegmented' end) metal,
          max(case when leads.merchant_uuid is not null then 1 else 0 end) from_inbound,
          max(case when gdl.country_code in ('US','CA') then 'NAM' else 'INTL' end) region,
          max(gdl.country_code) country_code,
          min(snem.first_launch_date) merchant_launch_date,
          max(case when dp.is_stps = 1 then 1 else 0 end) is_stps,
          max(mtd.acct_owner) acct_owner,
          max(case when dmapi.deal_uuid is not null then 1 else 0 end) is_ss,
          max(is_primary_service) is_primary_service
      from sandbox.sh_new_existing sne
      join (
          sel merchant_uuid, min(launch_date) first_launch_date
          from sandbox.sh_new_existing
          group by 1
      ) snem on sne.merchant_uuid = snem.merchant_uuid
      join user_edwprod.dim_gbl_deal_lob gdl on sne.deal_uuid = gdl.deal_id
      left join user_edwprod.dim_product dp on sne.deal_uuid = dp.product_uuid
      left join sandbox.eh_to_closes t_o on t_o.deal_uuid = sne.deal_uuid
      left join sandbox.pai_deals pd on pd.deal_uuid = sne.deal_uuid
      left join (
          select *
          from jc_ss_inelig
          where has_custom_codes + is_mamd + ineligible_pds + below_price_floor + enterprise_deal > 0
        ) inel
        on inel.deal_uuid = sne.deal_uuid
      left join (
          sel dm.merchant_uuid,
              mtd.launch_date,
              coalesce(mtd_attribution,mtd_attribution_intl) acct_owner
          from sandbox.jc_merchant_mtd_attrib mtd
          join user_edwprod.dim_merchant dm on mtd.accountid = dm.salesforce_account_id
          qualify row_number() over (partition by merchant_uuid order by launch_date desc) = 1
      ) mtd on sne.merchant_uuid = mtd.merchant_uuid
      left join (
          sel deal_uuid
          from sandbox.jc_merchant_mtd_attrib
          where dmapi_flag = 1
          group by 1
      ) dmapi on sne.deal_uuid = dmapi.deal_uuid
      left join (
          sel o2.deal_uuid,
              max(sda.merchant_seg_at_closed_won) metal_at_close
          from dwh_base_sec_view.opportunity_1 o1
          join dwh_base_sec_view.opportunity_2 o2 on o1.id = o2.id
          join dwh_base_sec_view.sf_deal_attribute sda on o1.deal_attribute = sda.id
          group by 1
      ) metal on sne.deal_uuid = metal.deal_uuid
      left join (
          sel dm.merchant_uuid
          from user_groupondw.sf_lead dl
          join dwh_base_sec_view.sf_account sfa on dl.convertedaccountid = sfa.id
          join user_edwprod.dim_merchant dm on sfa.id = dm.salesforce_account_id
          where cast(dl.createddate as date) <= cast(sfa.createddate as date)
          and lower(dl.leadsource) = 'metro - self service'
          group by 1
      ) leads on sne.merchant_uuid = leads.merchant_uuid
      left join (
          sel product_uuid,
              avg((groupon_value - contract_sell_price) / groupon_value) avg_pct_discount
          from user_edwprod.dim_offer_ext
          where groupon_value > 0
          group by 1
      ) doe on sne.deal_uuid = doe.product_uuid
      left join (
          sel deal_uuid
          from sandbox.jc_merchant_mtd_attrib
          where por_relaunch = 1
      ) por_rel on sne.deal_uuid = por_rel.deal_uuid
      left join (
          sel deal_uuid, max(deal_tier) deal_tier
          from sandbox.rev_mgmt_tiered_offerings
          group by 1
      ) rmto on sne.deal_uuid = rmto.deal_uuid
      where gdl.grt_l1_cat_name = 'L1 - Local'
      and sne.merchant_uuid in (sel merchant_uuid from user_edwprod.dim_offer_ext where inventory_service_name in ('voucher','vis'))
      and por_rel.deal_uuid is null
      group by 1,2,3
  ) with data unique primary index (deal_uuid) on commit preserve rows;
  
 
 create volatile table sh_fin as (
    select t.report_date ,
   t.deal_id, 
   units_sold,
   nob,
   nor,
   ogp,
   refund_usd,
   GB_usd,
   ((sum(refund_usd)*1.000)/(sum(GB_usd)*1.000)*100)as ref_per
  from (
   sel report_date,
    deal_id,
    sum(net_transactions_qty - zdo_net_transactions_qty) units_sold,
    sum(nob_loc * coalesce(er.approved_avg_exchange_rate,1)) nob,
    sum(nor_loc * coalesce(er.approved_avg_exchange_rate,1)) nor
   from user_edwprod.agg_gbl_financials_deal f
   join user_groupondw.dim_day dd on dd.day_rw = f.report_date
   join (
    sel currency_from,
     currency_to,
     fx_neutral_exchange_rate,
     approved_avg_exchange_rate,
     period_key
    from user_groupondw.gbl_fact_exchange_rate
    where currency_to = 'USD'
    group by 1,2,3,4,5
   ) er on f.currency_code = er.currency_from and dd.month_key = er.period_key
   group by 1,2
  ) t
  join (
   sel report_date,
    deal_id,
    sum(ogp_loc * coalesce(er.approved_avg_exchange_rate,1)) ogp
   from user_edwprod.agg_gbl_ogp_financials_deal f
   join user_groupondw.dim_day dd on dd.day_rw = f.report_date
   join (
    sel currency_from,
     currency_to,
     fx_neutral_exchange_rate,
     approved_avg_exchange_rate,
     period_key
    from user_groupondw.gbl_fact_exchange_rate
    where currency_to = 'USD'
    group by 1,2,3,4,5
   ) er on f.currency_code = er.currency_from and dd.month_key = er.period_key
   group by 1,2
  ) o on t.report_date = o.report_date and t.deal_id = o.deal_id
  left join (
   select  
   deal_uuid,
   sum(case when action = 'refund' then -1* REFUND_AMOUNT_USD end) as refund_usd, 
   sum(case when action = 'capture' then GROSS_BOOKINGS_USD end) as GB_usd
   from user_dw.v_fact_gbl_transactions 
   where order_date >= '2021-11-01'
   group by 1) r on r.deal_uuid =o.deal_id
   where  GB_usd <> 0
  group by 1,2,3,4,5,6,7,8
  ) with data unique primary index (report_date, deal_id) on commit preserve rows;
  
 
 create volatile table sh_still_live as (
      sel sld.deal_uuid,
          sld.launch_date,
          max(ad.load_date) last_live_date,
          last_live_date - sld.launch_date days_live
      from sh_launch_dates sld
      join user_groupondw.active_deals ad on sld.deal_uuid = ad.deal_uuid
      where ad.sold_out = 'false'
      group by 1,2
  ) with data unique primary index (deal_uuid) on commit preserve rows;
  
 
 drop table sandbox.jc_rep_vs_ss_v1;create table sandbox.jc_rep_vs_ss_v1 as (
  sel td_quarter_end(ld.launch_date) launch_qtr,
      td_month_end(ld.launch_date) launch_mth,
      cast(dw.week_end as date) launch_wk,
      first_launch,
      l2,
      l3,
      metal,
      from_inbound,
      region,
      country_code,
      is_stps,
      is_ss,
      acct_owner,
      is_primary_service,
      ss_eligible_flag,
      deal_type_flag,
      count(distinct ld.deal_uuid) deals_launched,
      count(distinct case when l.days_live >= 7 then ld.deal_uuid end) n_retained_7day,
      count(distinct case when l.days_live >= 30 then ld.deal_uuid end) n_retained_30day,
      count(distinct case when l.days_live >= 90 then ld.deal_uuid end) n_retained_90day,
      count(distinct case when l.days_live >= 180 then ld.deal_uuid end) n_retained_180day,
      count(distinct case when l.days_live >= 365 then ld.deal_uuid end) n_retained_365day,
      sum(case when f.report_date between ld.launch_date and ld.launch_date + interval '6' day then f.nob end) nob_7day,
      sum(case when f.report_date between ld.launch_date and ld.launch_date + interval '29' day then f.nob end) nob_30day,
      sum(case when f.report_date between ld.launch_date and ld.launch_date + interval '89' day then f.nob end) nob_90day,
      sum(case when f.report_date between ld.launch_date and ld.launch_date + interval '179' day then f.nob end) nob_180day,
      sum(case when f.report_date between ld.launch_date and ld.launch_date + interval '364' day then f.nob end) nob_365day,
      sum(case when f.report_date between ld.launch_date and ld.launch_date + interval '6' day then f.nor end) nor_7day,
      sum(case when f.report_date between ld.launch_date and ld.launch_date + interval '29' day then f.nor end) nor_30day,
      sum(case when f.report_date between ld.launch_date and ld.launch_date + interval '89' day then f.nor end) nor_90day,
      sum(case when f.report_date between ld.launch_date and ld.launch_date + interval '179' day then f.nor end) nor_180day,
      sum(case when f.report_date between ld.launch_date and ld.launch_date + interval '364' day then f.nor end) nor_365day,
      sum(case when f.report_date between ld.launch_date and ld.launch_date + interval '6' day then f.ogp end) ogp_7day,
      sum(case when f.report_date between ld.launch_date and ld.launch_date + interval '29' day then f.ogp end) ogp_30day,
      sum(case when f.report_date between ld.launch_date and ld.launch_date + interval '89' day then f.ogp end) ogp_90day,
      sum(case when f.report_date between ld.launch_date and ld.launch_date + interval '179' day then f.ogp end) ogp_180day,
      sum(case when f.report_date between ld.launch_date and ld.launch_date + interval '364' day then f.ogp end) ogp_365day,
      sum(case when f.report_date between ld.launch_date and ld.launch_date + interval '6' day then f.units_sold end) units_7day,
      sum(case when f.report_date between ld.launch_date and ld.launch_date + interval '29' day then f.units_sold end) units_30day,
      sum(case when f.report_date between ld.launch_date and ld.launch_date + interval '89' day then f.units_sold end) units_90day,
      sum(case when f.report_date between ld.launch_date and ld.launch_date + interval '179' day then f.units_sold end) units_180day,
      sum(case when f.report_date between ld.launch_date and ld.launch_date + interval '364' day then f.units_sold end) units_365day,
      average(case when f.report_date between ld.merchant_launch_date and ld.merchant_launch_date + interval '6' day then f.ref_per end) refund_7day,
      average(case when f.report_date between ld.merchant_launch_date and ld.merchant_launch_date + interval '29' day then f.ref_per end) refund_30day,
      average(case when f.report_date between ld.merchant_launch_date and ld.merchant_launch_date + interval '89' day then f.ref_per end) refund_90day,
      average(case when f.report_date between ld.merchant_launch_date and ld.merchant_launch_date + interval '179' day then f.ref_per end) refund_180day,
      average(case when f.report_date between ld.merchant_launch_date and ld.merchant_launch_date + interval '364' day then f.ref_per end) refund_365day
  from sh_launch_dates ld
  join user_groupondw.dim_day dd on ld.launch_date = dd.day_rw
  join user_groupondw.dim_week dw on dd.week_key = dw.week_key
  left join sh_still_live l on ld.deal_uuid = l.deal_uuid
  left join sh_fin f on ld.deal_uuid = f.deal_id
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
  ) with data no primary index;
  
 grant sel on sandbox.jc_rep_vs_ss_v1 to public


-------STEP 1 
/*
sandbox.sh_mm_roi_dash_v1;
select * from sandbox.pai_lead_opp_mtd_attrib sample 5;
select * from sandbox.sh_new_existing sample 5;

select 
   max(launch_date) mx
from sandbox.pai_lead_opp_mtd_attrib
where deal_uuid not in
(select distinct deal_uuid from sandbox.sh_new_existing) 
and launch_date is not null
;

select * from sandbox.sh_new_existing sne where deal_uuid = '63f49120-4fc9-44af-bd84-7a92562c7260';
select * from user_groupondw.active_deals where deal_uuid = '63f49120-4fc9-44af-bd84-7a92562c7260';
select * from user_edwprod.dim_offer_ext where product_uuid = '63f49120-4fc9-44af-bd84-7a92562c7260';

select min(launch_date), max(launch_date) from sandbox.sh_new_existing;

 select
                product_uuid,
                max(merchant_uuid) merchant_uuid,
                max(pds_cat_name) pds
            from user_edwprod.dim_offer_ext e
            join user_dw.v_dim_pds_grt_map p on e.pds_cat_id = p.pds_cat_id
            where e.product_uuid = '63f49120-4fc9-44af-bd84-7a92562c7260'
           group by 1;
          
select * from user_dw.v_dim_pds_grt_map where pds_cat_id = '239f3815-19d7-4061-bad3-67d95f946038';


select 
    count(merchant_uuid) mer
from 
(SELECT  
   a.merchant_uuid, 
   a.country_code acount,
   b.country_code bcount
from 
sandbox.pai_lead_opp_mtd_attrib as a 
left join sandbox.pai_merchants as b on a.merchant_uuid = b.merchant_uuid 
where a.country_code <> b.country_code) as fin;

sel deal_uuid, count(distinct country_code) cc from sandbox.pai_lead_opp_mtd_attrib group by 1 having cc > 1;
sel * from sandbox.pai_merchants where merchant_uuid = '352c1c3f-f7ec-4742-86ec-f7c8f74c4ecd';
select distinct inventory_service_name  from sandbox.pai_deals;
select merchant_uuid, count(distinct country_code) dlz from sandbox.pai_deals group by 1 having dlz > 1;

select 
   a.deal_uuid
from 
sandbox.pai_deals as a 
left join sandbox.pai_lead_opp_mtd_attrib as b on a.deal_uuid = b.deal_uuid and a.country_code = b.country_code 
where b.deal_uuid is null
sample 5;

select * from sandbox.pai_deals where deal_uuid = '8d0eb046-ba8f-4f4a-bad2-db1196e84032';
select * from sandbox.pai_lead_opp_mtd_attrib where deal_uuid = '8d0eb046-ba8f-4f4a-bad2-db1196e84032';*/


---first launch owner was pulled from mtd_attrib which is not removed temporarily
----for the deal one we had acct_owner_detailed which is also from mtd but its the one about attribution 

create volatile table sh_lnch_merch as (
select 
  sne.merchant_uuid, 
  coalesce(sne.country_code, pm.country_code) country_code, 
  case when coalesce(sne.country_code, pm.country_code) in ('US','CA') then 'NAM' else 'INTL' end region,
  min(sne.launch_date) first_launch_date, 
  max(sne.campaign_group_lvl1_2) campaign_group_lvl1_2, 
  max(sne.campaign_group_lvl1_1) campaign_group_lvl1_1, 
  max(sne.campaign_paid_category) campaign_paid_category, 
  max(sne.grt_l2_cat_name) l2, 
  max(sne.metal) metal, 
  max(case when sne.leadsource = 'metro - self service' then 1 else 0 end) from_inbound, 
  max(pm.acct_owner_detailed) acct_owner_detailed
from sandbox.pai_lead_opp_mtd_attrib sne
left join sandbox.pai_merchants pm on sne.merchant_uuid = pm.merchant_uuid
where launch_order = 1
group by 1,2,3
  ) with data unique primary index (merchant_uuid, country_code) on commit preserve rows
;

select * from sandbox.pai_lead_opp_mtd_attrib where LeadSource like '%metro%';

create volatile table sh_fin as (
   select 
         t.report_date ,
         t.deal_id, 
         pd.merchant_uuid,
         units_sold,
         nob,
         nor,
         ogp,
         refund_usd,
         GB_usd,
         ((sum(refund_usd)*1.000)/(sum(GB_usd)*1.000)*100)as ref_per
  from 
       (
         sel 
            report_date,
            deal_id,
            sum(net_transactions_qty - zdo_net_transactions_qty) units_sold,
            sum(nob_loc * coalesce(er.approved_avg_exchange_rate,1)) nob,
            sum(nor_loc * coalesce(er.approved_avg_exchange_rate,1)) nor
        from user_edwprod.agg_gbl_financials_deal f
        join user_groupondw.dim_day dd on dd.day_rw = f.report_date
        join (
           sel currency_from,
               currency_to,
               fx_neutral_exchange_rate,
               approved_avg_exchange_rate,
               period_key
            from user_groupondw.gbl_fact_exchange_rate
            where currency_to = 'USD'
            group by 1,2,3,4,5
            ) er on f.currency_code = er.currency_from and dd.month_key = er.period_key
            group by 1,2
       ) t
       left join sandbox.pai_deals pd on t.deal_id = pd.deal_uuid
       join (
           sel report_date,
               deal_id,
               sum(ogp_loc * coalesce(er.approved_avg_exchange_rate,1)) ogp
            from user_edwprod.agg_gbl_ogp_financials_deal f
            join user_groupondw.dim_day dd on dd.day_rw = f.report_date
            join (
                 sel currency_from,
                     currency_to,
                     fx_neutral_exchange_rate,
                     approved_avg_exchange_rate,
                     period_key
                  from user_groupondw.gbl_fact_exchange_rate
                  where currency_to = 'USD'
                  group by 1,2,3,4,5
                  ) er on f.currency_code = er.currency_from and dd.month_key = er.period_key
                  group by 1,2) o on t.report_date = o.report_date and t.deal_id = o.deal_id
       left join (
            select  
                 deal_uuid,
                 sum(case when action = 'refund' then -1* REFUND_AMOUNT_USD end) as refund_usd, 
                 sum(case when action = 'capture' then GROSS_BOOKINGS_USD end) as GB_usd
             from user_dw.v_fact_gbl_transactions 
             where order_date >= '2021-11-01'
             group by 1) r on r.deal_uuid =o.deal_id
        where  GB_usd <> 0
        group by 1,2,3,4,5,6,7,8,9
  ) with data unique primary index (report_date, deal_id) on commit preserve rows;
  
  
 create volatile table sh_still_live as (
   	select sld.merchant_uuid
   	  , sld.country_code
      , last_live_date - sld.first_launch_date as days_live
   	from (select distinct merchant_uuid, country_code, first_launch_date from sh_launch_dates) sld
   	join sandbox.pai_merchants pm on sld.merchant_uuid = pm.merchant_uuid
  ) with data unique primary index (merchant_uuid, country_code) on commit preserve rows;

 select from_inbound, count(1) from sandbox.np_mm_roi_dash_v1 group by 1;
  
create table sandbox.np_mm_roi_dash_v1 as (
      sel td_quarter_end(ld.first_launch_date) launch_qtr,
          td_month_end(ld.first_launch_date) launch_mth,
          cast(dw.week_end as date) launch_wk,
          ld.l2,
          ld.metal,
          ld.from_inbound,
          ld.region,
          ld.country_code,
          ld.acct_owner_detailed,
          ld.campaign_group_lvl1_2,
          ld.campaign_group_lvl1_1,
          ld.campaign_paid_category,
          --first_launch_owner,
          count(distinct ld.merchant_uuid) merchants_launched,
          count(distinct case when l.days_live >= 7 then ld.merchant_uuid end) n_retained_7day,
          count(distinct case when l.days_live >= 30 then ld.merchant_uuid end) n_retained_30day,
          count(distinct case when l.days_live >= 90 then ld.merchant_uuid end) n_retained_90day,
          count(distinct case when l.days_live >= 180 then ld.merchant_uuid end) n_retained_180day,
          count(distinct case when l.days_live >= 365 then ld.merchant_uuid end) n_retained_365day,
          sum(case when f.report_date between ld.first_launch_date and ld.first_launch_date + interval '6' day then f.nob end) nob_7day,
          sum(case when f.report_date between ld.first_launch_date and ld.first_launch_date + interval '29' day then f.nob end) nob_30day,
          sum(case when f.report_date between ld.first_launch_date and ld.first_launch_date + interval '89' day then f.nob end) nob_90day,
          sum(case when f.report_date between ld.first_launch_date and ld.first_launch_date + interval '179' day then f.nob end) nob_180day,
          sum(case when f.report_date between ld.first_launch_date and ld.first_launch_date + interval '364' day then f.nob end) nob_365day,
          sum(case when f.report_date between ld.first_launch_date and ld.first_launch_date + interval '6' day then f.nor end) nor_7day,
          sum(case when f.report_date between ld.first_launch_date and ld.first_launch_date + interval '29' day then f.nor end) nor_30day,
          sum(case when f.report_date between ld.first_launch_date and ld.first_launch_date + interval '89' day then f.nor end) nor_90day,
          sum(case when f.report_date between ld.first_launch_date and ld.first_launch_date + interval '179' day then f.nor end) nor_180day,
          sum(case when f.report_date between ld.first_launch_date and ld.first_launch_date + interval '364' day then f.nor end) nor_365day,
          sum(case when f.report_date between ld.first_launch_date and ld.first_launch_date + interval '6' day then f.ogp end) ogp_7day,
          sum(case when f.report_date between ld.first_launch_date and ld.first_launch_date + interval '29' day then f.ogp end) ogp_30day,
          sum(case when f.report_date between ld.first_launch_date and ld.first_launch_date + interval '89' day then f.ogp end) ogp_90day,
          sum(case when f.report_date between ld.first_launch_date and ld.first_launch_date + interval '179' day then f.ogp end) ogp_180day,
          sum(case when f.report_date between ld.first_launch_date and ld.first_launch_date + interval '364' day then f.ogp end) ogp_365day,
          sum(case when f.report_date between ld.first_launch_date and ld.first_launch_date + interval '6' day then f.units_sold end) units_7day,
          sum(case when f.report_date between ld.first_launch_date and ld.first_launch_date + interval '29' day then f.units_sold end) units_30day,
          sum(case when f.report_date between ld.first_launch_date and ld.first_launch_date + interval '89' day then f.units_sold end) units_90day,
          sum(case when f.report_date between ld.first_launch_date and ld.first_launch_date + interval '179' day then f.units_sold end) units_180day,
          sum(case when f.report_date between ld.first_launch_date and ld.first_launch_date + interval '364' day then f.units_sold end) units_365day,
          average(case when f.report_date between ld.first_launch_date and ld.first_launch_date + interval '6' day then f.ref_per end) refund_7day,
          average(case when f.report_date between ld.first_launch_date and ld.first_launch_date + interval '29' day then f.ref_per end) refund_30day,
          average(case when f.report_date between ld.first_launch_date and ld.first_launch_date + interval '89' day then f.ref_per end) refund_90day,
          average(case when f.report_date between ld.first_launch_date and ld.first_launch_date + interval '179' day then f.ref_per end) refund_180day,
          average(case when f.report_date between ld.first_launch_date and ld.first_launch_date + interval '364' day then f.ref_per end) refund_365day
      from sh_lnch_merch ld
      join user_groupondw.dim_day dd on ld.first_launch_date = dd.day_rw
      join user_groupondw.dim_week dw on dd.week_key = dw.week_key
      left join sh_still_live l on ld.merchant_uuid = l.merchant_uuid and ld.country_code = l.country_code
      left join sh_fin f on ld.merchant_uuid = f.merchant_uuid
      group by 1,2,3,4,5,6,7,8,9,10,11,12
  ) with data no primary index;
 
select region from sandbox.pai_deals sample 5;
------------------------ OLD 1
select 
   merchant_uuid, 
   count(1) xyz 
from 
(select distinct merchant_uuid, first_launch_date from sh_launch_dates) as fin 
group by 1 
having xyz > 1;

---first launch owner was pulled from mtd_attrib which is not removed temporarily
create volatile table sh_launch_dates as (
      sel sne.merchant_uuid,
          snem.first_launch_date,
          max(mkt.highest_touch) highest_touch_grp,
          max(first_launch_owner) first_launch_owner,
          max(
            case when mkt.highest_touch in (
              'SEM-Brand', 'SEM-NB', 'Display'
            ) then 1 else 0 end
          ) pd_mm_relaunch_flag, ----ridiculous
          max(pm.l2) l2,
          max(case when lower(pm.current_metal_segment) in ('gold','silver','platinum') then 'S+' else 'B-' end) metal,
          max(case when leads.merchant_uuid is not null then 1 else 0 end) from_inbound,
          max(case when pm.country_code in ('US','CA') then 'NAM' else 'INTL' end) region,
          max(pm.country_code) country_code,
          min(snem.first_launch_date) merchant_launch_date,
          max(pm.acct_owner_detailed) acct_owner_detailed
      from sandbox.sh_new_existing sne
      join (
          sel merchant_uuid, min(launch_date) first_launch_date
          from sandbox.sh_new_existing
          group by 1
      ) snem on sne.merchant_uuid = snem.merchant_uuid
      left join sandbox.pai_merchants pm on sne.merchant_uuid = pm.merchant_uuid
      left join sandbox.sm_w2l_mktg_acct_attrib mkt on mkt.accountid = pm.account_id
      join user_edwprod.dim_gbl_deal_lob gdl on sne.deal_uuid = gdl.deal_id
      left join user_edwprod.dim_product dp on sne.deal_uuid = dp.product_uuid
      left join (
          sel a.deal_uuid
            , max(case when a.dmapi_flag = 1 then 2 when pd.opp_owner_detailed = 'BDR' then 1 else 0 end) first_launch_owner
          from sandbox.jc_merchant_mtd_attrib a
          join sandbox.sh_new_existing b on a.deal_uuid = b.deal_uuid
          join sandbox.pai_deals pd on b.deal_uuid = pd.deal_uuid
          where b.is_new = 'new'
          group by 1
      ) dmapi on sne.deal_uuid = dmapi.deal_uuid
      left join (
          sel o2.deal_uuid,
              max(sda.merchant_seg_at_closed_won) metal_at_close
          from dwh_base_sec_view.opportunity_1 o1
          join dwh_base_sec_view.opportunity_2 o2 on o1.id = o2.id
          join dwh_base_sec_view.sf_deal_attribute sda on o1.deal_attribute = sda.id
          group by 1
      ) metal on sne.deal_uuid = metal.deal_uuid
      left join (
          sel dm.merchant_uuid
          from user_groupondw.sf_lead dl
          join dwh_base_sec_view.sf_account sfa on dl.convertedaccountid = sfa.id
          join user_edwprod.dim_merchant dm on sfa.id = dm.salesforce_account_id
          where cast(dl.createddate as date) <= cast(sfa.createddate as date)
          and lower(dl.leadsource) = 'metro - self service'
          group by 1
      ) leads on sne.merchant_uuid = leads.merchant_uuid
      where gdl.grt_l1_cat_name = 'L1 - Local'
      and sne.merchant_uuid in (sel merchant_uuid from user_edwprod.dim_offer_ext where inventory_service_name in ('voucher','vis'))
      group by 1,2
  ) with data unique primary index (merchant_uuid) on commit preserve rows;
  
  create volatile table sh_fin as (
   select 
         t.report_date ,
         t.deal_id, 
         units_sold,
         nob,
         nor,
         ogp,
         refund_usd,
         GB_usd,
         ((sum(refund_usd)*1.000)/(sum(GB_usd)*1.000)*100)as ref_per
  from 
       (
         sel 
            report_date,
            deal_id,
            sum(net_transactions_qty - zdo_net_transactions_qty) units_sold,
            sum(nob_loc * coalesce(er.approved_avg_exchange_rate,1)) nob,
            sum(nor_loc * coalesce(er.approved_avg_exchange_rate,1)) nor
        from user_edwprod.agg_gbl_financials_deal f
        join user_groupondw.dim_day dd on dd.day_rw = f.report_date
        join (
           sel currency_from,
               currency_to,
               fx_neutral_exchange_rate,
               approved_avg_exchange_rate,
               period_key
            from user_groupondw.gbl_fact_exchange_rate
            where currency_to = 'USD'
            group by 1,2,3,4,5
            ) er on f.currency_code = er.currency_from and dd.month_key = er.period_key
            group by 1,2
       ) t
       join (
           sel report_date,
               deal_id,
               sum(ogp_loc * coalesce(er.approved_avg_exchange_rate,1)) ogp
            from user_edwprod.agg_gbl_ogp_financials_deal f
            join user_groupondw.dim_day dd on dd.day_rw = f.report_date
            join (
                 sel currency_from,
                     currency_to,
                     fx_neutral_exchange_rate,
                     approved_avg_exchange_rate,
                     period_key
                  from user_groupondw.gbl_fact_exchange_rate
                  where currency_to = 'USD'
                  group by 1,2,3,4,5
                  ) er on f.currency_code = er.currency_from and dd.month_key = er.period_key
                  group by 1,2) o on t.report_date = o.report_date and t.deal_id = o.deal_id
       left join (
            select  
                 deal_uuid,
                 sum(case when action = 'refund' then -1* REFUND_AMOUNT_USD end) as refund_usd, 
                 sum(case when action = 'capture' then GROSS_BOOKINGS_USD end) as GB_usd
             from user_dw.v_fact_gbl_transactions 
             where order_date >= '2021-11-01'
             group by 1) r on r.deal_uuid =o.deal_id
        where  GB_usd <> 0
        group by 1,2,3,4,5,6,7,8
  ) with data unique primary index (report_date, deal_id) on commit preserve rows;
  
  
 create volatile table sh_still_live as (
   	select sld.merchant_uuid
      , last_live_date - sld.first_launch_date as days_live
   	from sh_launch_dates sld
   	join sandbox.pai_merchants pm on sld.merchant_uuid = pm.merchant_uuid
  ) with data unique primary index (merchant_uuid) on commit preserve rows;
  drop table sandbox.jc_mm_roi_dash_v1;
 
 

  
  create table sandbox.jc_mm_roi_dash_v1 as (
      sel td_quarter_end(ld.merchant_launch_date) launch_qtr,
          td_month_end(ld.merchant_launch_date) launch_mth,
          cast(dw.week_end as date) launch_wk,
          ld.l2,
          metal,
          from_inbound,
          region,
          ld.country_code,
          ld.acct_owner_detailed,
          highest_touch_grp,
          pd_mm_relaunch_flag,
          first_launch_owner,
          count(distinct ld.merchant_uuid) merchants_launched,
          count(distinct case when l.days_live >= 7 then ld.merchant_uuid end) n_retained_7day,
          count(distinct case when l.days_live >= 30 then ld.merchant_uuid end) n_retained_30day,
          count(distinct case when l.days_live >= 90 then ld.merchant_uuid end) n_retained_90day,
          count(distinct case when l.days_live >= 180 then ld.merchant_uuid end) n_retained_180day,
          count(distinct case when l.days_live >= 365 then ld.merchant_uuid end) n_retained_365day,
          sum(case when f.report_date between ld.merchant_launch_date and ld.merchant_launch_date + interval '6' day then f.nob end) nob_7day,
          sum(case when f.report_date between ld.merchant_launch_date and ld.merchant_launch_date + interval '29' day then f.nob end) nob_30day,
          sum(case when f.report_date between ld.merchant_launch_date and ld.merchant_launch_date + interval '89' day then f.nob end) nob_90day,
          sum(case when f.report_date between ld.merchant_launch_date and ld.merchant_launch_date + interval '179' day then f.nob end) nob_180day,
          sum(case when f.report_date between ld.merchant_launch_date and ld.merchant_launch_date + interval '364' day then f.nob end) nob_365day,
          sum(case when f.report_date between ld.merchant_launch_date and ld.merchant_launch_date + interval '6' day then f.nor end) nor_7day,
          sum(case when f.report_date between ld.merchant_launch_date and ld.merchant_launch_date + interval '29' day then f.nor end) nor_30day,
          sum(case when f.report_date between ld.merchant_launch_date and ld.merchant_launch_date + interval '89' day then f.nor end) nor_90day,
          sum(case when f.report_date between ld.merchant_launch_date and ld.merchant_launch_date + interval '179' day then f.nor end) nor_180day,
          sum(case when f.report_date between ld.merchant_launch_date and ld.merchant_launch_date + interval '364' day then f.nor end) nor_365day,
          sum(case when f.report_date between ld.merchant_launch_date and ld.merchant_launch_date + interval '6' day then f.ogp end) ogp_7day,
          sum(case when f.report_date between ld.merchant_launch_date and ld.merchant_launch_date + interval '29' day then f.ogp end) ogp_30day,
          sum(case when f.report_date between ld.merchant_launch_date and ld.merchant_launch_date + interval '89' day then f.ogp end) ogp_90day,
          sum(case when f.report_date between ld.merchant_launch_date and ld.merchant_launch_date + interval '179' day then f.ogp end) ogp_180day,
          sum(case when f.report_date between ld.merchant_launch_date and ld.merchant_launch_date + interval '364' day then f.ogp end) ogp_365day,
          sum(case when f.report_date between ld.merchant_launch_date and ld.merchant_launch_date + interval '6' day then f.units_sold end) units_7day,
          sum(case when f.report_date between ld.merchant_launch_date and ld.merchant_launch_date + interval '29' day then f.units_sold end) units_30day,
          sum(case when f.report_date between ld.merchant_launch_date and ld.merchant_launch_date + interval '89' day then f.units_sold end) units_90day,
          sum(case when f.report_date between ld.merchant_launch_date and ld.merchant_launch_date + interval '179' day then f.units_sold end) units_180day,
          sum(case when f.report_date between ld.merchant_launch_date and ld.merchant_launch_date + interval '364' day then f.units_sold end) units_365day,
          average(case when f.report_date between ld.merchant_launch_date and ld.merchant_launch_date + interval '6' day then f.ref_per end) refund_7day,
          average(case when f.report_date between ld.merchant_launch_date and ld.merchant_launch_date + interval '29' day then f.ref_per end) refund_30day,
          average(case when f.report_date between ld.merchant_launch_date and ld.merchant_launch_date + interval '89' day then f.ref_per end) refund_90day,
          average(case when f.report_date between ld.merchant_launch_date and ld.merchant_launch_date + interval '179' day then f.ref_per end) refund_180day,
          average(case when f.report_date between ld.merchant_launch_date and ld.merchant_launch_date + interval '364' day then f.ref_per end) refund_365day
      from sh_launch_dates ld
      join sandbox.pai_deals pd on ld.merchant_uuid = pd.merchant_uuid
      join user_groupondw.dim_day dd on ld.merchant_launch_date = dd.day_rw
      join user_groupondw.dim_week dw on dd.week_key = dw.week_key
      left join sh_still_live l on ld.merchant_uuid = l.merchant_uuid
      left join sh_fin f on pd.deal_uuid = f.deal_id
      group by 1,2,3,4,5,6,7,8,9,10,11,12
  ) with data no primary index;
 
 
grant sel on sandbox.jc_mm_roi_dash_v1 to public;
  drop table sh_launch_dates;
  drop table sh_fin;
  drop table sh_still_live;
  
 
 grant sel on sandbox.jc_mm_roi_dash_v1 to public;
  
 select * from sandbox.jc_mm_roi_dash_v1 where launch_mth >= '2022-10-01';

  
 grant sel on sandbox.jc_rep_vs_ss_v1 to public
 