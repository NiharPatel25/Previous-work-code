/Users/nihpatel/Documents/Atom Scripts/Merchant Focus/Pai_merchant_center_visits.sql--------td to hive

SELECT    
      merchant_uuid,
      country_code,
      feature_country,
      upd_account_id,
      upd_merchant_uuid,
      upd_merchant_name,
      account_uuid,
      null_acct_ind,
      merchant_pick_one,
      user_pick_one
FROM sandbox.jj_gbl_merchant_contact_mc;




----------------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS grp_gdoop_bizops_db.jj_gbl_merchant_contact_mc_tmp PURGE;
CREATE TABLE IF NOT EXISTS grp_gdoop_bizops_db.jj_gbl_merchant_contact_mc_tmp
(
  merchant_uuid string,
  country_code string,
  feature_country string,
  upd_account_id string,
  upd_merchant_uuid string,
  upd_merchant_name string,
  account_uuid string, 
  null_acct_ind string,
  merchant_pick_one string,
  user_pick_one string
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
LINES TERMINATED BY '\n';

LOAD DATA INPATH 'hdfs://cerebro-namenode/user/grp_gdoop_BizOps/optimus_jobs/jj_gbl_merchant_contact_mc.csv'
OVERWRITE INTO TABLE grp_gdoop_bizops_db.jj_gbl_merchant_contact_mc_tmp;

ANALYZE TABLE grp_gdoop_bizops_db.jj_gbl_merchant_contact_mc_tmp COMPUTE STATISTICS;
ANALYZE TABLE grp_gdoop_bizops_db.jj_gbl_merchant_contact_mc_tmp COMPUTE STATISTICS FOR COLUMNS;
DROP TABLE IF EXISTS grp_gdoop_bizops_db.jj_gbl_merchant_contact_mc PURGE;
CREATE TABLE grp_gdoop_bizops_db.jj_gbl_merchant_contact_mc
(
  merchant_uuid string,
  country_code string,
  feature_country string,
  upd_account_id string,
  upd_merchant_uuid string,
  upd_merchant_name string,
  account_uuid string, 
  metal string,
  metal_group string,
  vertical string,
  acct_owner string,
  account_owner_group string,
  merchant_perm string,
  null_acct_ind tinyint,
  merchant_pick_one tinyint,
  user_pick_one tinyint,
  merch_perm_ind tinyint
)
CLUSTERED BY (merchant_uuid) 
SORTED BY (merchant_uuid) INTO 7 BUCKETS
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

INSERT OVERWRITE TABLE grp_gdoop_bizops_db.jj_gbl_merchant_contact_mc
SELECT   
      a.merchant_uuid,
      a.country_code,
      a.feature_country,
      a.upd_account_id,
      a.upd_merchant_uuid,
      a.upd_merchant_name,
      a.account_uuid, 
      COALESCE(sfa.metal, 'Unknown') AS metal,
      COALESCE(sfa.metal_group, 'B-') AS metal_group,
      COALESCE(mtd.vertical, 'Unknown') AS vertical,
      b.acct_owner,
      CASE
           WHEN b.acct_owner IN ('Existing Metro', 'New Metro') THEN 'Metro'
           WHEN b.acct_owner = 'Existing MS' THEN 'Existing MS'
           WHEN b.acct_owner = 'MD' THEN 'MD'
           ELSE 'Other'
      END AS account_owner_group,
      COALESCE(sfa.merchant_permalink, '') AS merchant_perm,
      a.null_acct_ind,
      a.merchant_pick_one,
      a.user_pick_one,
      CAST(CASE WHEN sfa.merchant_permalink IS NULL THEN 0 ELSE 1 END AS TINYINT) AS merch_perm_ind
FROM 
(
SELECT   
      TRIM(REGEXP_REPLACE(merchant_uuid, '\\t|\\n|\\r|\\u0001', '')) AS merchant_uuid,
      TRIM(REGEXP_REPLACE(country_code, '\\t|\\n|\\r|\\u0001', '')) AS country_code,
      TRIM(REGEXP_REPLACE(feature_country, '\\t|\\n|\\r|\\u0001', '')) AS feature_country,
      TRIM(REGEXP_REPLACE(upd_account_id, '\\t|\\n|\\r|\\u0001', '')) AS upd_account_id,
      TRIM(REGEXP_REPLACE(upd_merchant_uuid, '\\t|\\n|\\r|\\u0001', '')) AS upd_merchant_uuid,
      TRIM(REGEXP_REPLACE(upd_merchant_name,' \\t|\\n|\\r|\\u0001', '')) AS upd_merchant_name,
      TRIM(REGEXP_REPLACE(account_uuid, '\\t|\\n|\\r|\\u0001', '')) AS account_uuid, 
      CAST(TRIM(REGEXP_REPLACE(null_acct_ind, '\\t|\\n|\\r|\\u0001', '')) AS TINYINT) AS null_acct_ind,
      CAST(TRIM(REGEXP_REPLACE(merchant_pick_one, '\\t|\\n|\\r|\\u0001', '')) AS TINYINT) AS merchant_pick_one,
      CAST(TRIM(REGEXP_REPLACE(user_pick_one, '\\t|\\n|\\r|\\u0001', '')) AS TINYINT) AS user_pick_one
FROM grp_gdoop_bizops_db.jj_gbl_merchant_contact_mc_tmp
) a
LEFT JOIN 
(
SELECT   
      account_id_18 AS account_id,
      merchant_permalink,
      COALESCE(MAX(LOWER(merchant_segmentation__c)), 'Unknown') AS metal,
      CASE 
          WHEN MAX(LOWER(merchant_segmentation__c)) IN ('silver', 'gold', 'platinum') 
          THEN 'S+' 
          ELSE 'B-'
      END AS metal_group
FROM dwh_base_sec_view.sf_account 
WHERE account_id_18 IS NOT NULL
GROUP BY account_id_18, merchant_permalink
) sfa 
ON a.upd_account_id = sfa.account_id
LEFT JOIN
(
SELECT   
       account_id,  
       COALESCE(vertical, 'Unknown') AS vertical
 FROM
(
 SELECT   
       accountid AS account_id,
       MAX(CASE WHEN grt_l1_cat_name IN ('L1 - Travel')              THEN 'Travel'
                WHEN grt_l1_cat_name IN ('L1 - Shopping')            THEN 'Goods'
                WHEN vertical IN ('L2 - Charity')                    THEN 'Charity'
                WHEN vertical IN ('L2 - Retail')                     THEN 'Retail'
                WHEN vertical IN ('L2 - Health / Beauty / Wellness') THEN 'HBW'
                WHEN vertical IN ('L2 - Food & Drink')               THEN 'F&D'
                WHEN vertical IN ('L2 - Home & Auto')                THEN 'H&A'
                WHEN vertical IN ('L2 - Things to Do - Leisure')     THEN 'TTD-Leisure'
                WHEN vertical IN ('L2 - Things to Do - Live')        THEN 'TTD-Live'
                ELSE vertical
            END) AS vertical
 FROM grp_gdoop_sup_analytics_db.jc_merchant_mtd_attrib
 WHERE close_recency = 1
 GROUP BY accountid
 ) a
) mtd 
ON a.upd_account_id = mtd.account_id
LEFT JOIN 
(
SELECT   
      accountid,
      COALESCE(SUBSTR(MAX(CASE
            WHEN feature_country IN ('CA', 'US') 
            THEN CONCAT('3', mtd_attribution)
            WHEN LOWER(mtd_attribution_intl) LIKE '%metro%' 
            THEN CONCAT('2', mtd_attribution_intl)
            ELSE CONCAT('1', 'Rep')
      END), 2), 'Rep') AS acct_owner
 FROM grp_gdoop_sup_analytics_db.jc_merchant_mtd_attrib
 WHERE close_recency = 1
 GROUP BY accountid
) b 
ON a.upd_account_id = b.accountid;DROP TABLE grp_gdoop_bizops_db.jj_gbl_merchant_contact_mc_tmp PURGE;ANALYZE TABLE grp_gdoop_bizops_db.jj_gbl_merchant_contact_mc COMPUTE STATISTICS;ANALYZE TABLE grp_gdoop_bizops_db.jj_gbl_merchant_contact_mc COMPUTE STATISTICS FOR COLUMNS




---------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE TABLE grp_gdoop_bizops_db.pai_juno_merchantID_validation
(
  event_date string,
  tot_rec_count string,
  tot_mc_val_join string,
  tot_user_val_join string,
  tot_bcookie_val_join string,
  tot_user_or_bcookie_val_join string,
  tot_bcookie_mapping_user_val_join string,
  tot_no_val_join string,
  tot_mc_val_join_pct string,
  tot_user_val_join_pct string,
  tot_bcookie_val_join_pct string,
  tot_user_or_bcookie_pct string,
  tot_bcookie_mapping_user_val_join_pct string,
  tot_no_val_join_pct string,
  null_rec_pct string
)
CLUSTERED BY (event_date) SORTED BY (event_date) INTO 1 BUCKETS
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');


INSERT OVERWRITE TABLE grp_gdoop_bizops_db.pai_juno_merchantID_validation
SELECT
       event_date,
       tot_rec_count,                                                                                                                 -- C
       tot_mc_val_join,                                                                                                               -- D
       tot_user_val_join,                                                                                                             -- E
       tot_bcookie_val_join,                                                                                                          -- F
       tot_user_val_join + tot_bcookie_val_join AS tot_user_or_bcookie_val_join,                                                      -- G = E + F
       tot_bcookie_mapping_user_val_join,                                                                                             -- H
       tot_no_val_join,                                                                                                               -- I
       CONCAT( ROUND( (tot_mc_val_join * 100.00)/(tot_rec_count * 1.00), 2), '%') AS tot_mc_val_join_pct,                                                      -- J = (D * 100)/C
       CONCAT( ROUND( (tot_user_val_join * 100.00)/(tot_rec_count * 1.00), 2), '%') AS tot_user_val_join_pct,                                                  -- K = (E * 100)/C
       CONCAT( ROUND( (tot_bcookie_val_join * 100.00)/(tot_rec_count * 1.00), 2), '%') AS tot_bcookie_val_join_pct,                                               -- L = (F * 100)/C
       CONCAT( ROUND( ((tot_user_val_join + tot_bcookie_val_join) * 100.00)/(tot_rec_count * 1.00), 2), '%') AS tot_user_or_bcookie_pct,                       -- M = (G * 100)/C
       CONCAT( ROUND( (tot_bcookie_mapping_user_val_join * 100.00)/(tot_bcookie_val_join * 1.00), 2), '%') AS tot_bcookie_mapping_user_val_join_pct,           -- N = (H * 100)/F
       CONCAT( ROUND( (tot_no_val_join * 100.00)/(tot_rec_count * 1.00), 2), '%') AS tot_no_val_join_pct,                                                      -- O = (D * 100)/C
       CONCAT( ROUND( ((tot_no_val_join * 100.00)/(tot_rec_count * 1.00)) + ((tot_bcookie_val_join * 100.00)/(tot_rec_count * 1.00)) - ((tot_bcookie_mapping_user_val_join * 100.00)/(tot_bcookie_val_join * 1.00)), 2), '%') AS null_rec_pct    -- P = (O+(L-N))
FROM
(
SELECT
       a.event_date,
       COUNT(*) AS tot_rec_count,
       SUM(mc_val_join_ind) AS tot_mc_val_join,
       SUM(user_val_join_ind) AS tot_user_val_join,
       SUM(bcookie_val_join_ind) AS tot_bcookie_val_join,
       SUM(CASE WHEN COALESCE(b.user_uuid, '-') <> '-' THEN 1 ELSE 0 END) AS tot_bcookie_mapping_user_val_join,
       SUM(no_val_join_ind) AS tot_no_val_join
FROM
(
SELECT
      eventdate AS event_date,
      
      LOWER(TRIM(REGEXP_REPLACE(COALESCE(bcookie, userbrowserid),'\\t|\\n|\\r|\\u0001', ''))) AS bcookie,
      
      COALESCE(LOWER(TRIM(REGEXP_REPLACE(COALESCE(consumerid, useruuid), '\\t|\\n|\\r|\\u0001', ''))), 
      SPLIT(SPLIT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(fullurl, url), '%(25)+', '%'), '%26', '&'), '%2F', '/'), '%3D', '='), '%3F', '?'), '%3A', ':'), '%23', '#'), 'uu=')[1], '(#|&)')[0]) AS user_uuid,
      
      LOWER(TRIM(REGEXP_REPLACE(merchantid,'\\t|\\n|\\r|\\u0001', ''))) AS merchant_uuid,
      
      CAST(CASE WHEN LOWER(TRIM(REGEXP_REPLACE(merchantid,'\\t|\\n|\\r|\\u0001', ''))) IS NOT NULL THEN 1 ELSE 0 END AS TINYINT) AS mc_val_join_ind,
      
      CAST(CASE WHEN LOWER(TRIM(REGEXP_REPLACE(merchantid,'\\t|\\n|\\r|\\u0001', ''))) IS NULL
                 AND COALESCE(LOWER(TRIM(REGEXP_REPLACE(COALESCE(consumerid, useruuid), '\\t|\\n|\\r|\\u0001', ''))), 
                     SPLIT(SPLIT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(fullurl, url), '%(25)+', '%'), '%26', '&'), '%2F', '/'), '%3D', '='), '%3F', '?'), '%3A', ':'), '%23', '#'), 'uu=')[1], '(#|&)')[0]) IS NOT NULL
                THEN 1 
                ELSE 0 
      END AS TINYINT) AS user_val_join_ind,

      CAST(CASE WHEN LOWER(TRIM(REGEXP_REPLACE(merchantid,'\\t|\\n|\\r|\\u0001', ''))) IS NULL 
                 AND COALESCE(LOWER(TRIM(REGEXP_REPLACE(COALESCE(consumerid, useruuid), '\\t|\\n|\\r|\\u0001', ''))), 
                     SPLIT(SPLIT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(fullurl, url), '%(25)+', '%'), '%26', '&'), '%2F', '/'), '%3D', '='), '%3F', '?'), '%3A', ':'), '%23', '#'), 'uu=')[1], '(#|&)')[0]) IS NULL
                 AND LOWER(TRIM(REGEXP_REPLACE(COALESCE(bcookie, userbrowserid),'\\t|\\n|\\r|\\u0001', '')))    IS NOT NULL
                THEN 1 
                ELSE 0 
      END AS TINYINT) AS bcookie_val_join_ind,

      CAST(CASE WHEN LOWER(TRIM(REGEXP_REPLACE(merchantid,'\\t|\\n|\\r|\\u0001', ''))) IS NULL 
                 AND COALESCE(LOWER(TRIM(REGEXP_REPLACE(COALESCE(consumerid, useruuid), '\\t|\\n|\\r|\\u0001', ''))), 
                     SPLIT(SPLIT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(fullurl, url), '%(25)+', '%'), '%26', '&'), '%2F', '/'), '%3D', '='), '%3F', '?'), '%3A', ':'), '%23', '#'), 'uu=')[1], '(#|&)')[0]) IS NULL
                 AND LOWER(TRIM(REGEXP_REPLACE(COALESCE(bcookie, userbrowserid),'\\t|\\n|\\r|\\u0001', ''))) IS NULL
                THEN 1 
                ELSE 0 
      END AS TINYINT) AS no_val_join_ind
      
FROM grp_gdoop_pde.junohourly
WHERE eventdate BETWEEN DATE_SUB(CURRENT_DATE, 14) AND DATE_SUB(CURRENT_DATE, 1)
  AND platform = 'web'
  AND eventdestination IN ('dealImpression', 'genericBucket', 'genericClick', 'other', 'purchaseFunnel', 'searchBrowseView')
  AND LOWER(pageapp) IN ('merchant-center-minsky', 'android-mobile-merchant', 'ios-mobile-merchant', 'merchant-support-echo', 'metro-ui', 'merchant-center-auth','merchant-advisor-itier')
  AND event <> 'merchantPageView'
  AND country IN ('AE', 'AU', 'BE', 'CA', 'DE', 'ES', 'FR', 'GB', 'IE', 'IT', 'NL', 'PL', 'QC', 'UK', 'US')
  
UNION ALL
  
SELECT
      eventdate AS event_date,
      
      LOWER(TRIM(REGEXP_REPLACE(COALESCE(bcookie, userbrowserid),'\\t|\\n|\\r|\\u0001', ''))) AS bcookie, 
      
      COALESCE(LOWER(TRIM(REGEXP_REPLACE(COALESCE(consumerid, useruuid), '\\t|\\n|\\r|\\u0001', ''))), 
      SPLIT(SPLIT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(fullurl, url), '%(25)+', '%'), '%26', '&'), '%2F', '/'), '%3D', '='), '%3F', '?'), '%3A', ':'), '%23', '#'), 'uu=')[1], '(#|&)')[0]) AS user_uuid,
      
      LOWER(TRIM(REGEXP_REPLACE(merchantid,'\\t|\\n|\\r|\\u0001', ''))) AS merchant_uuid,
      
      CAST(CASE WHEN LOWER(TRIM(REGEXP_REPLACE(merchantid,'\\t|\\n|\\r|\\u0001', ''))) IS NOT NULL THEN 1 ELSE 0 END AS TINYINT) AS mc_val_join_ind,
      
      CAST(CASE WHEN LOWER(TRIM(REGEXP_REPLACE(merchantid,'\\t|\\n|\\r|\\u0001', ''))) IS NULL
                 AND COALESCE(LOWER(TRIM(REGEXP_REPLACE(COALESCE(consumerid, useruuid), '\\t|\\n|\\r|\\u0001', ''))), 
                     SPLIT(SPLIT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(fullurl, url), '%(25)+', '%'), '%26', '&'), '%2F', '/'), '%3D', '='), '%3F', '?'), '%3A', ':'), '%23', '#'), 'uu=')[1], '(#|&)')[0]) IS NOT NULL
                THEN 1 
                ELSE 0 
      END AS TINYINT) AS user_val_join_ind,
      
      CAST(CASE WHEN LOWER(TRIM(REGEXP_REPLACE(merchantid,'\\t|\\n|\\r|\\u0001', ''))) IS NULL 
                 AND COALESCE(LOWER(TRIM(REGEXP_REPLACE(COALESCE(consumerid, useruuid), '\\t|\\n|\\r|\\u0001', ''))), 
                     SPLIT(SPLIT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(fullurl, url), '%(25)+', '%'), '%26', '&'), '%2F', '/'), '%3D', '='), '%3F', '?'), '%3A', ':'), '%23', '#'), 'uu=')[1], '(#|&)')[0]) IS NULL
                 AND LOWER(TRIM(REGEXP_REPLACE(COALESCE(bcookie, userbrowserid),'\\t|\\n|\\r|\\u0001', ''))) IS NOT NULL
                THEN 1 
                ELSE 0 
      END AS TINYINT) AS bcookie_val_join_ind,
      
      CAST(CASE WHEN LOWER(TRIM(REGEXP_REPLACE(merchantid,'\\t|\\n|\\r|\\u0001', ''))) IS NULL 
                 AND COALESCE(LOWER(TRIM(REGEXP_REPLACE(COALESCE(consumerid, useruuid), '\\t|\\n|\\r|\\u0001', ''))), 
                     SPLIT(SPLIT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(fullurl, url), '%(25)+', '%'), '%26', '&'), '%2F', '/'), '%3D', '='), '%3F', '?'), '%3A', ':'), '%23', '#'), 'uu=')[1], '(#|&)')[0]) IS NULL
                 AND LOWER(TRIM(REGEXP_REPLACE(COALESCE(bcookie, userbrowserid),'\\t|\\n|\\r|\\u0001', ''))) IS NULL
                THEN 1 
                ELSE 0 
      END AS TINYINT) AS no_val_join_ind
FROM grp_gdoop_pde.junohourly
WHERE eventdate BETWEEN DATE_SUB(CURRENT_DATE, 14) AND DATE_SUB(CURRENT_DATE, 1)
  AND platform = 'other'
  AND eventdestination = 'other'
  AND event = 'merchantPageView'
  AND country IN ('AE', 'AU', 'BE', 'CA', 'DE', 'ES', 'FR', 'GB', 'IE', 'IT', 'NL', 'PL', 'QC', 'UK', 'US')
) a
LEFT OUTER JOIN
(
    SELECT
           event_date,
           LOWER(TRIM(bcookie)) AS bcookie,
           MAX(COALESCE(user_uuid, '-')) AS user_uuid
    FROM prod_groupondw.user_bcookie_mapping a
    WHERE event_date BETWEEN DATE_SUB(CURRENT_DATE, 14) AND DATE_SUB(CURRENT_DATE, 1)
    AND COALESCE(LOWER(TRIM(bcookie)), '') <> ''
    AND COALESCE(LOWER(TRIM(user_uuid)), '') <> ''
    AND LOWER(country_code) IN ('ae', 'au', 'be', 'ca', 'de', 'es', 'fr', 'gb', 'ie', 'it', 'nl', 'pl', 'qc', 'uk', 'us')
    GROUP BY event_date,
             LOWER(TRIM(bcookie))
) b
ON  a.bcookie = b.bcookie
AND a.event_date = b.event_date
AND a.bcookie_val_join_ind = 1
GROUP BY a.event_date
) a






---------------------------------------------------------------------------------------------------------------------------------------------------------------------


INSERT OVERWRITE TABLE grp_gdoop_bizops_db.pai_merchant_center_visits 
PARTITION (eventdate)
SELECT 
      fin.country AS country_code,
      fin.bcookie,
      fin.user_uuid,
      fin.upd_merchant_uuid AS merchant_uuid,
      fin.upd_account_id AS account_id,
      fin.upd_merchant_name AS account_name,
      COALESCE(fin.metal, 'Unknown') AS metal,
      COALESCE(fin.metal_group, 'B-') AS metal_group,
      COALESCE(fin.vertical, 'Unknown') AS vertical,
      fin.acct_owner,
      COALESCE(fin.account_owner_group, 'Other') AS account_owner_group,
      fin.page_url,
      fin.page_path,
      fin.page_app,
      fin.page_type,
      fin.mc_page,
      fin.platform,
      CASE 
           WHEN LOWER(fin.platform) = 'mobile' THEN COALESCE(fin.device_category, 'Mobile-Other')
           ELSE INITCAP(fin.platform)
      END AS sub_platform,
      fin.user_device,
      fin.device_category,  
      fin.user_device_type,
      fin.appversion AS app_version,
      fin.utm_campaign,
      fin.utm_medium,
      fin.utm_source,
      fin.referrer_domain,
      fin.event,
      fin.eventdestination AS event_destination,
      fin.widget_name,
      fin.user_agent,
      fin.uqeventkey,
      fin.mc_cb_drft_ind,
      fin.mc_cb_ind,
      COALESCE(live.live_flag, 0) AS live_flag,
      fin.orig_merchant_uuid,
      fin.orig_user_uuid,
      CASE WHEN 
                fin.upd_merchant_uuid IS NOT NULL AND 
                fin.orig_merchant_uuid IS NOT NULL AND
                COALESCE(fin.orig_merchant_uuid, '-') <> fin.upd_merchant_uuid 
           THEN 1 
           ELSE 0 
      END AS orig_merch_upd,
      CASE WHEN 
                fin.upd_merchant_uuid IS NOT NULL AND 
                COALESCE(fin.orig_merchant_uuid, '-') = fin.upd_merchant_uuid 
           THEN 1 
           ELSE 0 
      END AS orig_merch_ind,
      CASE WHEN 
                fin.upd_merchant_uuid IS NOT NULL AND 
                fin.orig_user_uuid IS NOT NULL AND
                fin.orig_merchant_uuid IS NULL 
           THEN 1 
           ELSE 0     
      END AS user_merch_ind,
      CASE WHEN 
                fin.upd_merchant_uuid IS NOT NULL AND 
                fin.orig_user_uuid IS NULL AND
                fin.orig_merchant_uuid IS NULL 
           THEN 1 
           ELSE 0 
      END AS bcookie_merch_ind,
      CASE WHEN 
                fin.upd_merchant_uuid IS NULL AND 
                fin.orig_user_uuid IS NULL AND
                fin.orig_merchant_uuid IS NULL 
           THEN 1 
           ELSE 0 
      END AS no_merch_ind,
      fin.event_time,
      fin.dt
FROM
(
SELECT /*+ mapjoin(mc1, mc2, mc3, mc4) */
      a.bcookie,
      COALESCE(a.user_uuid, mc2.account_uuid, mc3.account_uuid, mc4.account_uuid, b.user_uuid) AS user_uuid,
      a.user_uuid AS orig_user_uuid,
      a.merchant_uuid AS orig_merchant_uuid,
      a.eventtime AS event_time,
      a.pageapp AS page_app,
      a.page_url,
      a.pagepath AS page_path,
      a.page_type,
      CASE 
           WHEN LOWER(a.page_type) = 'homepage' THEN 'Home'
           WHEN LOWER(a.page_type) = 'deal_index' THEN 'Deal View'
           WHEN LOWER(a.page_type) = 'deal_list' THEN 'Campaigns List'
           WHEN LOWER(a.page_type) = 'deal_details' THEN 'Campaigns - Details'
           WHEN LOWER(a.page_type) = 'performance_exposure' THEN 'Campaigns - Exposure Tab'
           WHEN LOWER(a.page_type) = 'performance_sales' THEN 'Campaigns - Sales Tab'
           WHEN LOWER(a.page_type) = 'performance_redemption' THEN 'Campaigns - Redemption Tab'
           WHEN LOWER(a.page_type) = 'deal_history' THEN 'Campaigns - History Tab'
           WHEN LOWER(a.page_type) = 'performance_earnings' THEN 'Campaigns - Earnings Tab'
           WHEN LOWER(a.page_type) = 'payment_history' THEN 'Payments'
           WHEN LOWER(a.page_type) = 'payment_details' THEN 'Payments - Payment Details'
           WHEN LOWER(a.page_type) = 'payment_campaign_transactions' THEN 'Payments - Transactions'
           WHEN LOWER(a.page_type) LIKE '%voucher%' THEN 'Voucher List'
           WHEN LOWER(a.page_type) LIKE '%demographics%' THEN 'Customer_Demographics'
           WHEN LOWER(a.page_type) LIKE '%feedback%' AND LOWER(a.page_type) NOT LIKE '%flutter%' THEN 'Customer Feedback'
           WHEN LOWER(a.page_type) LIKE '%admin%' OR LOWER(a.page_type) LIKE '%change_account%' THEN 'Admin - Landing'
           WHEN LOWER(a.page_type) = 'place_list' THEN 'Admin - Locations'
           WHEN LOWER(a.page_type) IN ('inbox_case', 'inbox_list') THEN 'Inbox'
           WHEN LOWER(a.page_type) IN ('faq', 'working-with-groupon', 'help-and-contact', 'support') THEN 'Support'
           WHEN LOWER(a.page_type) LIKE '%contact%us%' THEN 'Contact Us'
           WHEN LOWER(a.page_type) = 'email' THEN 'Contact Us - Email'
           WHEN LOWER(a.page_type) = 'change_account' THEN 'Change Merchant'
           WHEN LOWER(a.page_type) = 'BT_NEW_BOOKING-landing' THEN 'Booking Tool'
           ELSE 'Other'
      END AS mc_page,
      a.country,
      a.platform,
      CASE 
          WHEN LOWER(a.platform) = 'mobile' THEN SPLIT(SPLIT(a.useragent, '\\(')[1], '\073')[0]
          ELSE 'Other'
      END AS user_device,
      CASE WHEN LOWER(a.platform) = 'mobile' 
           THEN CASE WHEN a.useragent LIKE '%android%'
                     THEN CASE 
                               -- SPLIT(SPLIT(useragent, '\\(')[1], '<semicolon>')[0] is user_device, when LOWER(a.platform) = 'mobile' 
                               WHEN LOWER(SPLIT(SPLIT(a.useragent, '\\(')[1], '\073')[0]) LIKE '%samsung%gt-p%' OR
                                    LOWER(SPLIT(SPLIT(a.useragent, '\\(')[1], '\073')[0]) LIKE '%samsung%sm-p%' OR
                                    LOWER(SPLIT(SPLIT(a.useragent, '\\(')[1], '\073')[0]) LIKE '%samsung%sm-t%' OR
                                    LOWER(SPLIT(SPLIT(a.useragent, '\\(')[1], '\073')[0]) LIKE '%book%' OR
                                    LOWER(SPLIT(SPLIT(a.useragent, '\\(')[1], '\073')[0]) LIKE 'lenovo%'
                               THEN 'Android-Tablet'
                               -- SPLIT(SPLIT(useragent, '\\(')[1], '<semicolon>')[0] is user_device, when LOWER(a.platform) = 'mobile' 
                               WHEN LOWER(SPLIT(SPLIT(a.useragent, '\\(')[1], '\073')[0]) LIKE '%samsung%' 
                               THEN 'Android-Phone'
                               ELSE 'Android-Phone' 
                          END
                     WHEN a.useragent LIKE '%ios%'  
                     THEN CASE 
                               -- SPLIT(SPLIT(useragent, '\\(')[1], '<semicolon>')[0] is user_device, when LOWER(a.platform) = 'mobile' 
                               WHEN LOWER(SPLIT(SPLIT(a.useragent, '\\(')[1], '\073')[0]) LIKE '%iphone%' OR
                                    LOWER(SPLIT(SPLIT(a.useragent, '\\(')[1], '\073')[0]) LIKE '%ipod%'
                               THEN 'iOS-Phone'
                               -- SPLIT(SPLIT(useragent, '\\(')[1], '<semicolon>')[0] is user_device, when LOWER(a.platform) = 'mobile' 
                               WHEN LOWER(SPLIT(SPLIT(a.useragent, '\\(')[1], '\073')[0]) LIKE '%ipad%' 
                               THEN 'iOS-Tablet'
                               ELSE 'iOS-Other' 
                          END
                     ELSE 'Mobile-Other'
                END
           -- when LOWER(a.platform) != 'mobile' 
           ELSE LOWER(a.platform)
      END AS device_category,  
      a.devicetype AS user_device_type,
      CASE
           WHEN LOWER(a.platform) = 'mobile'
           THEN REGEXP_REPLACE(SPLIT(a.useragent, '\\/')[1], '(\\d+\.\\d+)\.*', '$1')
      END AS appversion,
      COALESCE(a.utm_campaign, SPLIT(SPLIT(a.page_url, 'utm_campaign=')[1], '(#|&)') [0]) AS utm_campaign,
      COALESCE(a.utm_medium, SPLIT(SPLIT(a.page_url, 'utm_medium=')[1], '(#|&)') [0]) AS utm_medium,
      COALESCE(a.utm_source, SPLIT(SPLIT(a.page_url, 'utm_source=')[1], '(#|&)') [0]) AS utm_source, 
      a.referrer_domain,
      a.event,
      a.eventdestination,
      a.widgetname AS widget_name,
      a.useragent AS user_agent,
      a.uqeventkey,
      a.mc_cb_drft_ind,
      a.mc_cb_ind,
      a.eventdate AS dt,
      COALESCE(mc1.country_code, mc2.country_code, mc3.country_code, mc4.country_code, b.country_code, a.country) AS feature_country,
      COALESCE(mc1.upd_merchant_uuid, mc2.upd_merchant_uuid, mc3.upd_merchant_uuid, mc4.upd_merchant_uuid, b.upd_merchant_uuid, a.merchant_uuid) AS upd_merchant_uuid,
      COALESCE(mc1.upd_merchant_name, mc2.upd_merchant_name, mc3.upd_merchant_name, mc4.upd_merchant_name, b.upd_merchant_name) AS upd_merchant_name,
      COALESCE(mc1.upd_account_id, mc2.upd_account_id, mc3.upd_account_id, mc4.upd_account_id, b.upd_account_id) AS upd_account_id,
      COALESCE(mc1.metal, mc2.metal, mc3.metal, mc4.metal, b.metal) AS metal,
      COALESCE(mc1.metal_group, mc2.metal_group, mc3.metal_group, mc4.metal_group, b.metal_group) AS metal_group,
      COALESCE(mc1.vertical, mc2.vertical, mc3.vertical, mc4.vertical, b.vertical) AS vertical,
      COALESCE(mc1.acct_owner, mc2.acct_owner, mc3.acct_owner, mc4.acct_owner, b.acct_owner) AS acct_owner,
      COALESCE(mc1.account_owner_group, mc2.account_owner_group, mc3.account_owner_group, mc4.account_owner_group, b.account_owner_group) AS account_owner_group,
      CAST(CASE WHEN COALESCE(mc1.upd_merchant_uuid, mc2.upd_merchant_uuid, mc3.upd_merchant_uuid, mc4.upd_merchant_uuid, b.upd_merchant_uuid, a.merchant_uuid) IS NULL THEN 0 ELSE 1 END AS TINYINT) AS merch_join_ind,
      bcookie_join_ind
FROM 
(
SELECT
      LOWER(TRIM(REGEXP_REPLACE(COALESCE(bcookie, userbrowserid), '\\t|\\n|\\r|\\u0001', ''))) AS bcookie,
      COALESCE(LOWER(TRIM(REGEXP_REPLACE(COALESCE(consumerid, useruuid), '\\t|\\n|\\r|\\u0001', ''))), 
      SPLIT(SPLIT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(fullurl, url), '%(25)+', '%'), '%26', '&'), '%2F', '/'), '%3D', '='), '%3F', '?'), '%3A', ':'), '%23', '#'), 'uu=')[1], '(#|&)')[0]) AS user_uuid,
      LOWER(TRIM(REGEXP_REPLACE(merchantid,'\\t|\\n|\\r|\\u0001', ''))) AS merchant_uuid,
      eventtime,
      pageapp,
      REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(fullurl, url), '%(25)+', '%'), '%26', '&'), '%2F', '/'), '%3D', '='), '%3F', '?'), '%3A', ':'), '%23', '#') AS page_url,
      pagepath,
      COALESCE(rawpagetype, pageviewtype) AS page_type,
      CASE WHEN country IN ('CA', 'QC') THEN 'CA'
           WHEN country IN ('GB', 'UK') THEN 'UK'
           ELSE country
      END AS country,
      CASE 
           WHEN COALESCE(LOWER(TRIM(REGEXP_REPLACE(clientplatform,'\\t|\\n|\\r|\\u0001', ''))), LOWER(TRIM(REGEXP_REPLACE(platform,'\\t|\\n|\\r|\\u0001', '')))) IN ('web', 'desktop') THEN 'Web'
           WHEN COALESCE(LOWER(TRIM(REGEXP_REPLACE(clientplatform,'\\t|\\n|\\r|\\u0001', ''))), LOWER(TRIM(REGEXP_REPLACE(platform,'\\t|\\n|\\r|\\u0001', '')))) IN ('touch') THEN 'Touch'
           WHEN LOWER(TRIM(REGEXP_REPLACE(clientplatform,'\\t|\\n|\\r|\\u0001', ''))) = 'mobile-merchant' THEN 'Mobile'
           ELSE 'Web'
      END AS platform,
      devicetype,
      campaign AS utm_campaign,
      medium AS utm_medium,
      source AS utm_source,
      referrerdomain AS referrer_domain,
      event,
      eventdestination,
      widgetname,
      useragent,
      uqeventkey,
      eventdate,
      SPLIT(SPLIT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(fullurl, url), '%(25)+', '%'), '%26', '&'), '%2F', '/'), '%3D', '='), '%3F', '?'), '%3A', ':'), '%23', '#'), 'mid=')[1], '(#|&)')[0] AS url_merchant_prmlnk,
      --- Campaign Builder
      CAST(CASE WHEN LOWER(pageapp) = 'metro-ui' AND LOWER(COALESCE(rawpagetype, pageviewtype)) = 'options' THEN 1 ELSE 0 END AS TINYINT) AS mc_cb_drft_ind,
      CAST(CASE WHEN LOWER(pageapp) = 'metro-ui' AND LOWER(COALESCE(rawpagetype, pageviewtype)) IN ('options', 'images','nutshell-deal-options', 'nutshell-deal-highlight', 'nutshell-about-business', 'fineprint', 'redemption-locations', 'launch-date','bank-tax-info', 'contract', 'congratulations') THEN 1 ELSE 0 END AS TINYINT) AS mc_cb_ind,
      --- Campaign Builder
      CAST(CASE WHEN SPLIT(SPLIT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(fullurl, url), '%(25)+', '%'), '%26', '&'), '%2F', '/'), '%3D', '='), '%3F', '?'), '%3A', ':'), '%23', '#'), 'mid=')[1], '(#|&)')[0] IS NULL THEN 0 ELSE 1 END AS TINYINT) AS prm_lnk_ind,
      CAST(CASE WHEN LOWER(TRIM(REGEXP_REPLACE(merchantid,'\\t|\\n|\\r|\\u0001', ''))) IS NULL THEN 0 ELSE 1 END AS TINYINT) AS mc_join_ind,
      CAST(CASE WHEN COALESCE(LOWER(TRIM(REGEXP_REPLACE(COALESCE(consumerid, useruuid), '\\t|\\n|\\r|\\u0001', ''))), 
                     SPLIT(SPLIT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(fullurl, url), '%(25)+', '%'), '%26', '&'), '%2F', '/'), '%3D', '='), '%3F', '?'), '%3A', ':'), '%23', '#'), 'uu=')[1], '(#|&)')[0]) IS NULL 
                THEN 0 ELSE 1 END AS TINYINT) AS user_join_ind,
      CAST(CASE 
                WHEN LOWER(TRIM(REGEXP_REPLACE(merchantid,'\\t|\\n|\\r|\\u0001', ''))) IS NULL
                 AND COALESCE(LOWER(TRIM(REGEXP_REPLACE(COALESCE(consumerid, useruuid), '\\t|\\n|\\r|\\u0001', ''))), 
                     SPLIT(SPLIT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(fullurl, url), '%(25)+', '%'), '%26', '&'), '%2F', '/'), '%3D', '='), '%3F', '?'), '%3A', ':'), '%23', '#'), 'uu=')[1], '(#|&)')[0]) IS NULL
                 AND LOWER(TRIM(REGEXP_REPLACE(COALESCE(bcookie, userbrowserid),'\\t|\\n|\\r|\\u0001', ''))) IS NOT NULL
                THEN 1 ELSE 0 END AS TINYINT) AS bcookie_join_ind
FROM grp_gdoop_pde.junohourly
WHERE eventdate BETWEEN DATE_SUB(CURRENT_DATE, 14) AND DATE_SUB(CURRENT_DATE, 1)
  AND platform = 'web'
  AND eventdestination IN ('dealImpression', 'genericBucket', 'genericClick', 'other', 'purchaseFunnel', 'searchBrowseView')
  AND LOWER(pageapp) IN ('merchant-center-minsky', 'android-mobile-merchant', 'ios-mobile-merchant', 'merchant-support-echo', 'metro-ui', 'merchant-center-auth','merchant-advisor-itier')
  AND event <> 'merchantPageView'
  AND country IN ('AE', 'AU', 'BE', 'CA', 'DE', 'ES', 'FR', 'GB', 'IE', 'IT', 'NL', 'PL', 'QC', 'UK', 'US')
  
UNION ALL
  
SELECT
      LOWER(TRIM(REGEXP_REPLACE(COALESCE(bcookie, userbrowserid), '\\t|\\n|\\r|\\u0001', ''))) AS bcookie,
      COALESCE(LOWER(TRIM(REGEXP_REPLACE(COALESCE(consumerid, useruuid), '\\t|\\n|\\r|\\u0001', ''))), 
      SPLIT(SPLIT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(fullurl, url), '%(25)+', '%'), '%26', '&'), '%2F', '/'), '%3D', '='), '%3F', '?'), '%3A', ':'), '%23', '#'), 'uu=')[1], '(#|&)')[0]) AS user_uuid,
      LOWER(TRIM(REGEXP_REPLACE(merchantid,'\\t|\\n|\\r|\\u0001', ''))) AS merchant_uuid, 
      eventtime, 
      pageapp, 
      REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(fullurl, url), '%(25)+', '%'), '%26', '&'), '%2F', '/'), '%3D', '='), '%3F', '?'), '%3A', ':'), '%23', '#') AS page_url,
      pagepath,
      COALESCE(rawpagetype, pageviewtype) AS page_type,
      CASE WHEN country IN ('CA', 'QC') THEN 'CA'
           WHEN country IN ('GB', 'UK') THEN 'UK'
           ELSE country
      END AS country,
      CASE 
           WHEN COALESCE(LOWER(TRIM(REGEXP_REPLACE(clientplatform,'\\t|\\n|\\r|\\u0001', ''))), LOWER(TRIM(REGEXP_REPLACE(platform,'\\t|\\n|\\r|\\u0001', '')))) IN ('web', 'desktop') THEN 'Web'
           WHEN COALESCE(LOWER(TRIM(REGEXP_REPLACE(clientplatform,'\\t|\\n|\\r|\\u0001', ''))), LOWER(TRIM(REGEXP_REPLACE(platform,'\\t|\\n|\\r|\\u0001', '')))) IN ('touch') THEN 'Touch'
           WHEN LOWER(TRIM(REGEXP_REPLACE(clientplatform,'\\t|\\n|\\r|\\u0001', ''))) = 'mobile-merchant' THEN 'Mobile'
           ELSE 'Web'
      END AS platform,
      devicetype,  
      campaign AS utm_campaign,
      medium AS utm_medium,
      source AS utm_source, 
      referrerdomain AS referrer_domain,
      event, 
      eventdestination, 
      widgetname, 
      useragent,
      uqeventkey,
      eventdate,
      SPLIT(SPLIT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(fullurl, url), '%(25)+', '%'), '%26', '&'), '%2F', '/'), '%3D', '='), '%3F', '?'), '%3A', ':'), '%23', '#'), 'mid=')[1], '(#|&)')[0] AS url_merchant_prmlnk,
      --- Campaign Builder
      CAST(CASE WHEN LOWER(pageapp) = 'metro-ui' AND LOWER(COALESCE(rawpagetype, pageviewtype)) = 'options' THEN 1 ELSE 0 END AS TINYINT) AS mc_cb_drft_ind,
      CAST(CASE WHEN LOWER(pageapp) = 'metro-ui' AND LOWER(COALESCE(rawpagetype, pageviewtype)) IN ('options', 'images','nutshell-deal-options', 'nutshell-deal-highlight', 'nutshell-about-business', 'fineprint', 'redemption-locations', 'launch-date','bank-tax-info', 'contract', 'congratulations') THEN 1 ELSE 0 END AS TINYINT) AS mc_cb_ind,
      --- Campaign Builder
      CAST(CASE WHEN SPLIT(SPLIT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(fullurl, url), '%(25)+', '%'), '%26', '&'), '%2F', '/'), '%3D', '='), '%3F', '?'), '%3A', ':'), '%23', '#'), 'mid=')[1], '(#|&)')[0] IS NULL THEN 0 ELSE 1 END AS TINYINT) AS prm_lnk_ind,
      CAST(CASE WHEN LOWER(TRIM(REGEXP_REPLACE(merchantid,'\\t|\\n|\\r|\\u0001', ''))) IS NULL THEN 0 ELSE 1 END AS TINYINT) AS mc_join_ind,
      CAST(CASE WHEN COALESCE(LOWER(TRIM(REGEXP_REPLACE(COALESCE(consumerid, useruuid), '\\t|\\n|\\r|\\u0001', ''))), 
                     SPLIT(SPLIT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(fullurl, url), '%(25)+', '%'), '%26', '&'), '%2F', '/'), '%3D', '='), '%3F', '?'), '%3A', ':'), '%23', '#'), 'uu=')[1], '(#|&)')[0]) IS NULL 
                THEN 0 ELSE 1 END AS TINYINT) AS user_join_ind,
      CAST(CASE 
                WHEN LOWER(TRIM(REGEXP_REPLACE(merchantid,'\\t|\\n|\\r|\\u0001', ''))) IS NULL
                 AND COALESCE(LOWER(TRIM(REGEXP_REPLACE(COALESCE(consumerid, useruuid), '\\t|\\n|\\r|\\u0001', ''))), 
                     SPLIT(SPLIT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(fullurl, url), '%(25)+', '%'), '%26', '&'), '%2F', '/'), '%3D', '='), '%3F', '?'), '%3A', ':'), '%23', '#'), 'uu=')[1], '(#|&)')[0]) IS NULL
                 AND LOWER(TRIM(REGEXP_REPLACE(COALESCE(bcookie, userbrowserid),'\\t|\\n|\\r|\\u0001', ''))) IS NOT NULL
                THEN 1 ELSE 0 END AS TINYINT) AS bcookie_join_ind
FROM grp_gdoop_pde.junohourly
WHERE eventdate BETWEEN DATE_SUB(CURRENT_DATE, 14) AND DATE_SUB(CURRENT_DATE, 1)
  AND platform = 'other'
  AND eventdestination = 'other'
  AND event = 'merchantPageView'
  AND country IN ('AE', 'AU', 'BE', 'CA', 'DE', 'ES', 'FR', 'GB', 'IE', 'IT', 'NL', 'PL', 'QC', 'UK', 'US')
) a
LEFT OUTER JOIN grp_gdoop_bizops_db.jj_gbl_merchant_contact_mc mc1 ON mc1.merchant_uuid = a.merchant_uuid AND mc1.account_uuid = a.user_uuid AND a.mc_join_ind = 1 AND a.user_join_ind = 1
LEFT OUTER JOIN (SELECT * FROM grp_gdoop_bizops_db.jj_gbl_merchant_contact_mc WHERE merchant_pick_one = 1) mc2 ON mc2.merchant_uuid = a.merchant_uuid AND a.mc_join_ind = 1
LEFT OUTER JOIN (SELECT * FROM grp_gdoop_bizops_db.jj_gbl_merchant_contact_mc WHERE merchant_pick_one = 1 AND merch_perm_ind = 1) mc3 ON mc3.merchant_perm = a.url_merchant_prmlnk AND a.mc_join_ind = 0 AND a.prm_lnk_ind = 1
LEFT OUTER JOIN (SELECT * FROM grp_gdoop_bizops_db.jj_gbl_merchant_contact_mc WHERE null_acct_ind = 0 AND user_pick_one = 1) mc4 ON mc4.account_uuid = a.user_uuid AND a.mc_join_ind = 0 AND a.user_join_ind = 1
LEFT OUTER JOIN
(
 SELECT DISTINCT
        event_date,
        country_code,
        user_uuid, 
        bcookie,
        upd_merchant_uuid,
        upd_merchant_name,
        upd_account_id,
        metal,
        metal_group,
        vertical,
        acct_owner,
        account_owner_group,
        rnk
  FROM
  (
    SELECT /*+ mapjoin(mc5) */
          a.event_date, 
          CASE WHEN a.country_code IN ('CA', 'QC') THEN 'CA'
               WHEN a.country_code IN ('GB', 'UK') THEN 'UK'
               ELSE a.country_code
          END AS country_code,
          a.user_uuid, 
          LOWER(TRIM(a.bcookie)) AS bcookie,
          mc5.upd_merchant_uuid,
          mc5.upd_merchant_name,
          mc5.upd_account_id,
          mc5.metal,
          mc5.metal_group,
          mc5.vertical,
          mc5.acct_owner,
          mc5.account_owner_group,
          ROW_NUMBER() OVER (PARTITION BY LOWER(TRIM(bcookie)),
                                          event_date,
                                          CASE WHEN a.country_code IN ('CA', 'QC') THEN 'CA' 
                                               WHEN a.country_code IN ('GB', 'UK') THEN 'UK'
                                               ELSE a.country_code
                                          END 
                                          ORDER BY COALESCE(mc5.upd_merchant_uuid, '-') DESC, COALESCE(a.user_uuid, '-') DESC) AS rnk -- ASCII Value of '-' is 45
    FROM prod_groupondw.user_bcookie_mapping a
    LEFT OUTER JOIN 
     ( 
        SELECT
               country_code, 
               feature_country, 
               upd_account_id, 
               upd_merchant_uuid, 
               upd_merchant_name,
               account_uuid,
               metal,
               metal_group,
               vertical,
               acct_owner,
               account_owner_group
        FROM grp_gdoop_bizops_db.jj_gbl_merchant_contact_mc
        WHERE null_acct_ind = 0 
        AND user_pick_one = 1
     ) mc5 ON
     mc5.account_uuid = a.user_uuid
    WHERE a.event_date BETWEEN DATE_SUB(CURRENT_DATE, 14) AND DATE_SUB(CURRENT_DATE, 1)
    AND COALESCE(LOWER(TRIM(a.bcookie)), '') <> ''
    AND COALESCE(LOWER(TRIM(a.user_uuid)), '') <> ''
    AND LOWER(a.country_code) IN ('ae', 'au', 'be', 'ca', 'de', 'es', 'fr', 'gb', 'ie', 'it', 'nl', 'pl', 'qc', 'uk', 'us')
    AND mc5.upd_merchant_uuid IS NOT NULL
  ) a
  WHERE rnk = 1
) b
ON  a.bcookie = b.bcookie
AND a.country = b.country_code
AND a.eventdate = b.event_date
AND a.bcookie_join_ind = 1
) fin
LEFT OUTER JOIN
(
  SELECT 
         DISTINCT
         ad.load_date,
         doe.merchant_uuid,
         CAST(1 AS TINYINT) AS live_flag
  FROM  
  (
    SELECT DISTINCT
           load_date, 
           deal_uuid
    FROM prod_groupondw.active_deals
    WHERE sold_out = 'false'
    AND country_code IN ('AE', 'AU', 'BE', 'CA', 'DE', 'ES', 'FR', 'GB', 'IE', 'IT', 'NL', 'PL', 'QC', 'UK', 'US') 
    AND load_date > '2014-12-31'
    AND load_date <> '2017-08-24'
    AND load_date BETWEEN DATE_SUB(CURRENT_DATE, 14) AND DATE_SUB(CURRENT_DATE, 1)
 ) ad
 INNER JOIN
 (
       SELECT /*+ mapjoin(mc6) */
              DISTINCT
              product_uuid AS deal_uuid,
              COALESCE(mc6.upd_merchant_uuid, doe.merchant_uuid) AS merchant_uuid
       FROM edwprod.dim_offer_ext doe
       LEFT OUTER JOIN (SELECT upd_merchant_uuid, merchant_uuid FROM grp_gdoop_bizops_db.jj_gbl_merchant_contact_mc WHERE merchant_pick_one = 1) mc6 ON mc6.merchant_uuid = doe.merchant_uuid
       WHERE COALESCE(mc6.upd_merchant_uuid, doe.merchant_uuid) IS NOT NULL
 ) doe
 ON doe.deal_uuid = ad.deal_uuid
) live
ON fin.upd_merchant_uuid = live.merchant_uuid 
AND fin.dt = live.load_date
AND fin.merch_join_ind = 1