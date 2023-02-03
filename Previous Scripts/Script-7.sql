SELECT
            deal_uuid,
            New_Clients_Res,
            CAST(CASE 
                     WHEN New_Clients_Res = 0
                     THEN CAST(0 AS SMALLINT)
                        
                     WHEN New_Clients_Res = 1 AND REGEXP_SIMILAR(fine_print1, '.*[l|p]ast[\sa-z]*\d+\s+month.*', 'i') = 1 
                     THEN CAST(REGEXP_REPLACE(REGEXP_REPLACE(fine_print1, '(.*?)[l|p]ast[\sa-z]*(\d+)\s+mont[hs|h](.*)', '\2'), ' ', '') * 30 AS SMALLINT)
                     WHEN New_Clients_Res = 1 AND REGEXP_SIMILAR(fine_print1, '.*[l|p]ast[\sa-z]*\d+\s+year.*', 'i') = 1 
                     THEN CAST(REGEXP_REPLACE(REGEXP_REPLACE(fine_print1, '(.*?)[l|p]ast[\sa-z]*(\d+)\s+yea[rs|r](.*)', '\2'), ' ', '') * 365 AS SMALLINT)
                        
                     WHEN New_Clients_Res = 1 AND REGEXP_SIMILAR(fine_print1, '.*[clients|customers|patients|client|customer|patient] not active within[\sa-z]*\d+\s+month.*', 'i') = 1 
                     THEN CAST(REGEXP_REPLACE(REGEXP_REPLACE(fine_print1, '(.*?)[clients|customers|patients|client|customer|patient] not active within[\sa-z]*(\d+)\s+mont[hs|h](.*)', '\2'), ' ', '') * 30 AS SMALLINT)
                     WHEN New_Clients_Res = 1 AND REGEXP_SIMILAR(fine_print1, '.*[clients|customers|patients|client|customer|patient] not active within[\sa-z]*\d+\s+year.*', 'i') = 1 
                     THEN CAST(REGEXP_REPLACE(REGEXP_REPLACE(fine_print1, '(.*?)[clients|customers|patients|client|customer|patient] not active within[\sa-z]*(\d+)\s+yea[rs|r](.*)', '\2'), ' ', '') * 365 AS SMALLINT)
                        
                     WHEN New_Clients_Res = 1 AND REGEXP_SIMILAR(fine_print1, '.*valid for new [clients|customers|patients|client|customer|patient] or those who have not visited within[\sa-z]*\d+\s+month.*', 'i') = 1 
                     THEN CAST(REGEXP_REPLACE(REGEXP_REPLACE(fine_print1, '(.*?)valid for new [clients|customers|patients|client|customer|patient] or those who have not visited within[\sa-z]*(\d+)\s+mont[hs|h](.*)', '\2'), ' ', '') * 30 AS SMALLINT)
                     WHEN New_Clients_Res = 1 AND REGEXP_SIMILAR(fine_print1, '.*valid for new [clients|customers|patients|client|customer|patient] or those who have not visited within[\sa-z]*\d+\s+year.*', 'i') = 1 
                     THEN CAST(REGEXP_REPLACE(REGEXP_REPLACE(fine_print1, '(.*?)valid for new [clients|customers|patients|client|customer|patient] or those who have not visited within[\sa-z]*(\d+)\s+yea[rs|r](.*)', '\2'), ' ', '') * 365 AS SMALLINT)
                                       
                     WHEN New_Clients_Res = 1 AND REGEXP_SIMILAR(fine_print1, '.*[clients|customers|patients|client|customer|patient] not active within[\sa-z]*\d+\s+month.*', 'i') = 1 
                     THEN CAST(REGEXP_REPLACE(REGEXP_REPLACE(fine_print1, '(.*?)[clients|customers|patients|client|customer|patient] not active within[\sa-z]*(\d+)\s+mont[hs|h](.*)', '\2'), ' ', '') * 30 AS SMALLINT)
                     WHEN New_Clients_Res = 1 AND REGEXP_SIMILAR(fine_print1, '.*[clients|customers|patients|client|customer|patient] not active within[\sa-z]*\d+\s+year.*', 'i') = 1 
                     THEN CAST(REGEXP_REPLACE(REGEXP_REPLACE(fine_print1, '(.*?)[clients|customers|patients|client|customer|patient] not active within[\sa-z]*(\d+)\s+yea[rs|r](.*)', '\2'), ' ', '') * 365 AS SMALLINT)                               
                                       
                     WHEN New_Clients_Res = 1 AND REGEXP_SIMILAR(fine_print1, '.*not valid for [clients|customers|patients|client|customer|patient] active within[\sa-z]*\d+\s+month.*', 'i') = 1 
                     THEN CAST(REGEXP_REPLACE(REGEXP_REPLACE(fine_print1, '(.*?)not valid for [clients|customers|patients|client|customer|patient] active within[\sa-z]*(\d+)\s+mont[hs|h](.*)', '\2'), ' ', '') * 30 AS SMALLINT)
                     WHEN New_Clients_Res = 1 AND REGEXP_SIMILAR(fine_print1, '.*not valid for [clients|customers|patients|client|customer|patient] active within[\sa-z]*\d+\s+year.*', 'i') = 1 
                     THEN CAST(REGEXP_REPLACE(REGEXP_REPLACE(fine_print1, '(.*?)not valid for [clients|customers|patients|client|customer|patient] active within[\sa-z]*(\d+)\s+yea[rs|r](.*)', '\2'), ' ', '') * 365 AS SMALLINT)
                                       
                     WHEN New_Clients_Res = 1 AND REGEXP_SIMILAR(fine_print1, '.*have not visited in[\sa-z]*\d+\s+month.*', 'i') = 1 
                     THEN CAST(REGEXP_REPLACE(REGEXP_REPLACE(fine_print1, '(.*?)have not visited in[\sa-z]*(\d+)\s+mont[hs|h](.*)', '\2'), ' ', '') * 30 AS SMALLINT)
                     WHEN New_Clients_Res = 1 AND REGEXP_SIMILAR(fine_print1, '.*have not visited in[\sa-z]*\d+\s+year.*', 'i') = 1 
                     THEN CAST(REGEXP_REPLACE(REGEXP_REPLACE(fine_print1, '(.*?)have not visited in[\sa-z]*(\d+)\s+yea[rs|r](.*)', '\2'), ' ', '') * 365 AS SMALLINT)
                                       
                     WHEN New_Clients_Res = 1 AND REGEXP_SIMILAR(fine_print1, '.*[new clients only|new customers only|new patients only|new client only|new customer only|new patient only].*', 'i') = 1
                     THEN -1
                ELSE 0
            END AS SMALLINT)  AS New_Client_Window,
            Tm_Dt_Res,
            Add_Fee_Res,
            Menu_Ser_Res,
            Repurchase_Res,
            Repurchase_Window,
            Appointment_Res,
            Holiday_Res,
            take_away_flag     
        FROM 
        (
             SELECT
                 dd.uuid AS deal_uuid,
                 dd.fine_print1,
                 dd.take_away_flag,
                 CAST(MAX(COALESCE(nc.ind, 0)) AS BYTEINT) AS New_Clients_Res,
                 CAST(MAX(COALESCE(tdr.ind, 0)) AS BYTEINT) AS Tm_Dt_Res,
                 CAST(MAX(COALESCE(af.ind, 0)) AS BYTEINT) AS Add_Fee_Res,
                 CAST(MAX(COALESCE(msr.ind, 0)) AS BYTEINT) AS Menu_Ser_Res,
                 CAST(MAX(COALESCE(pur.ind, 0)) AS BYTEINT) AS Repurchase_Res,
                 CAST(MAX(COALESCE(dd.Repurchase_Window, 0)) AS SMALLINT) AS Repurchase_Window,
                 CAST(MAX(COALESCE(app.ind, 0)) AS BYTEINT) AS Appointment_Res,
                 CAST(MAX(COALESCE(hdr.ind, 0)) AS BYTEINT) AS Holiday_Res
             FROM 
             (
               SELECT uuid,
               REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(fine_print), '\bthree\b', '3'), '\bsix\b', '6'), '-', ' '), 'no blackout', '') AS fine_print1,
               CASE 
                    WHEN REGEXP_SIMILAR(fine_print1, '.*may be repurchased every[\sa-z]*\d+\s+day.*', 'i') = 1 
                    THEN CAST(REGEXP_REPLACE(REGEXP_REPLACE(fine_print1, '(.*?)may be repurchased every[\sa-z]*(\d+)[\s]*da[ys|y](.*)', '\2'), ' ', '') AS SMALLINT)
                    WHEN REGEXP_SIMILAR(fine_print1, '.*may be repurchased every[\sa-z]*\d+\s+week.*', 'i') = 1 
                    THEN CAST(REGEXP_REPLACE(REGEXP_REPLACE(fine_print1,'(.*?)may be repurchased every[\sa-z]*(\d+)\s+wee[ks|k](.*)', '\2'), ' ', '') * 7 AS SMALLINT)
                    WHEN REGEXP_SIMILAR(fine_print1, '.*may be repurchased every[\sa-z]*\d+\s+month.*', 'i') = 1 
                    THEN CAST(REGEXP_REPLACE(REGEXP_REPLACE(fine_print1,'(.*?)may be repurchased every[\sa-z]*(\d+)\s+mont[hs|h](.*)', '\2'), ' ', '') * 30 AS SMALLINT)
                    WHEN REGEXP_SIMILAR(fine_print1, '.*may be repurchased every[\sa-z]*\d+\s+year.*', 'i') = 1 
                    THEN CAST(REGEXP_REPLACE(REGEXP_REPLACE(fine_print1,'(.*?)may be repurchased every[\sa-z]*(\d+)\s+yea[rs|r](.*)', '\2'), ' ', '') * 365 AS SMALLINT)
                    WHEN REGEXP_SIMILAR(fine_print1, '.*may be repurchased every[\sa-z]*\d+(\.|$)', 'i') = 1 
                    THEN CAST(REGEXP_REPLACE(REGEXP_REPLACE(fine_print1, '(.*?)may be repurchased every[\sa-z]*\b(\d+)\b(.*)', '\2'), ' ', '') AS SMALLINT)
                    ELSE 0
               END AS Repurchase_Window,
               CAST((CASE 
                 WHEN REGEXP_SIMILAR(fine_print1, '.*not valid towar(ds|d) (delivery|carryout|carry-out|carry-out|carry|take out|take-out|takeout).*', 'i') = 1
                 THEN 0
                 WHEN REGEXP_SIMILAR(fine_print1, '.*not valid for (delivery|carryout|carry-out|carry-out|carry|take out|take-out|takeout).*', 'i') = 1
                 THEN 0
                 WHEN REGEXP_SIMILAR(fine_print1, '.*(dine in only|dine-in only|reservation required).*', 'i') = 1
                 THEN 0
                 
                 WHEN REGEXP_SIMILAR(fine_print1, '.*valid towar(ds|d) (delivery|carryout|carry-out|carry-out|carry|take out|take-out|takeout).*', 'i') = 1
                 THEN 1
                 WHEN REGEXP_SIMILAR(fine_print1, '.*valid for (delivery|carryout|carry-out|carry-out|carry|take out|take-out|takeout).*', 'i') = 1
                 THEN 1
                 WHEN REGEXP_SIMILAR(fine_print1, '.*valid for (dine in|dine-in) or (delivery|carryout|carry-out|carry-out|carry|take out|take-out|takeout).*', 'i') = 1
                 THEN 1
                 WHEN REGEXP_SIMILAR(fine_print1, '.*valid for (delivery|carryout|carry-out|carry-out|carry|take out|take-out|takeout) or (dine in|dine-in).*', 'i') = 1
                 THEN 1
                 WHEN REGEXP_SIMILAR(fine_print1, '.*(((delivery|carryout|carry-out|carry-out|take out|take-out|takeout) only)|not valid toward taxes, gratuity or delivery fees).*', 'i') = 1
                 THEN 1
                 
                 ELSE -1
               END) AS BYTEINT) AS take_away_flag  
               FROM user_groupondw.dim_deal a
               WHERE uuid IN (SELECT deal_uuid FROM sandbox.temp_deal_fp_list)
             ) dd
             LEFT JOIN
             (
             SELECT text_res,
                 1 AS ind
                 FROM sandbox.kg_deal_structure_res
                 WHERE restrict_type = 'New Clients') nc
             ON 
             REGEXP_SUBSTR(dd.fine_print1, nc.text_res, 1, 1, 'i') = nc.text_res
             LEFT JOIN
             (
             SELECT text_res,
                 1 AS ind
                 FROM sandbox.kg_deal_structure_res
                 WHERE restrict_type = 'Time/Date Restrictions') tdr
             ON 
             REGEXP_SUBSTR(dd.fine_print1, tdr.text_res, 1, 1, 'i') = tdr.text_res
             LEFT JOIN
             (
             SELECT text_res,
                 1 AS ind
                 FROM sandbox.kg_deal_structure_res
                 WHERE restrict_type = 'Holiday Restrictions') hdr
             ON 
             REGEXP_SUBSTR(dd.fine_print1, hdr.text_res, 1, 1, 'i') = hdr.text_res
             LEFT JOIN
             (
             SELECT text_res,
                 1 AS ind
                 FROM sandbox.kg_deal_structure_res
                 WHERE restrict_type = 'Additional Fees') af
             ON 
             REGEXP_SUBSTR(dd.fine_print1, af.text_res, 1, 1, 'i') = af.text_res
             LEFT JOIN
             (
             SELECT text_res,
                 1 AS ind
                 FROM sandbox.kg_deal_structure_res
                 WHERE restrict_type = 'Menu/Service restrictions') msr
             ON 
             REGEXP_SUBSTR(dd.fine_print1, msr.text_res, 1, 1, 'i') = msr.text_res
             LEFT JOIN
             (
             SELECT text_res,
                 1 AS ind
                 FROM sandbox.kg_deal_structure_res
                 WHERE restrict_type = 'Repurchase restrictions') pur
             ON 
             REGEXP_SUBSTR(dd.fine_print1, pur.text_res, 1, 1, 'i') = pur.text_res
             LEFT JOIN
             (
             SELECT text_res,
                 1 AS ind
                 FROM sandbox.kg_deal_structure_res
                 WHERE restrict_type = 'Reservations/Appointment Needed') app
             ON 
             REGEXP_SUBSTR(dd.fine_print1, app.text_res, 1, 1, 'i') = app.text_res
             GROUP BY 1, 2, 3
        ) fin;             
        
    SET start_value = end_value + 1;
    SET end_value = end_value + in_step_value;
    END WHILE;
    
    COLLECT STATISTICS COLUMN (deal_uuid) ON sandbox.kg_deal_structure_final; 

END;

CALL sandbox.tot_deal_fp_proc(1500000);         
