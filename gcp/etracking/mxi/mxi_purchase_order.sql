CREATE OR REPLACE PROCEDURE `bc-te-dlake-dev-s7b3.cln_etracking_us.etracking_purchase_order`()
BEGIN

CREATE OR REPLACE TABLE `bc-te-dlake-dev-s7b3.cln_etracking_us.tmp_purchase_order` AS

WITH
    --Separacion los request en 3 grupos, request sin PO, request con PO y po sin request
    REQUEST_NONE AS (       
        SELECT 
            REQ.REQ_PART_DB_ID, 
            REQ.REQ_PART_ID, 
            REQ.REQ_MASTER_ID,
            REQ.CREATION_DT,
            REQ.REQ_HR_ID,
            REQ.REQ_NOTE,
            REQ.REQ_TYPE_CD,
            PO_HEADER.PO_DB_ID, 
            PO_HEADER.PO_ID, 
            PO_LINE.PO_LINE_ID, 
            ROW_NUMBER() OVER (partition by REQ_PART_ID ORDER BY REQ.CREATION_DT DESC, PO_LINE.CREATION_DT DESC) as RN  
        FROM `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.req_part` as REQ
        LEFT JOIN `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.po_header` as PO_HEADER
            ON  REQ.PO_DB_ID = PO_HEADER.PO_DB_ID AND 
                REQ.PO_ID = PO_HEADER.PO_ID
        LEFT JOIN `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.po_line` AS PO_LINE
            ON  REQ.PO_DB_ID = PO_LINE.PO_DB_ID AND 
                REQ.PO_ID = PO_LINE.PO_ID AND
                REQ.PO_LINE_ID = PO_LINE.PO_LINE_ID
        LEFT JOIN `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.evt_event` as EVT
            ON  REQ.REQ_PART_ID = EVT.EVENT_ID
        WHERE   PO_HEADER.PO_ID IS NULL 
            AND EVT.EVENT_STATUS_CD <> 'PRCANCEL' 
            AND EVT.EVENT_STATUS_CD <> 'PRCOMPLETE' 
            AND REQ.REQ_TYPE_CD = 'STOCK'
                

    ),

    REQUEST_NONE_F AS(
        SELECT 
            REQ_PART_DB_ID, 
            REQ_PART_ID, 
            REQ_MASTER_ID, 
            CAST(CREATION_DT AS TIMESTAMP) AS REQ_CREATION_DT, 
            REQ_HR_ID,
            REQ_NOTE,
            REQ_TYPE_CD,
            PO_ID, 
            PO_DB_ID, 
            PO_LINE_ID
        FROM REQUEST_NONE
        WHERE RN = 1
    ),

    REQUEST_PO AS (       
        SELECT 
            REQ.REQ_PART_DB_ID, 
            REQ.REQ_PART_ID, 
            REQ.REQ_MASTER_ID,
            REQ.REQ_HR_ID,
            REQ.REQ_NOTE,
            REQ.REQ_TYPE_CD,
            REQ.CREATION_DT,   
            PO_HEADER.PO_ID, 
            PO_HEADER.PO_DB_ID, 
            PO_LINE.PO_LINE_ID, 
            ROW_NUMBER() OVER (partition by PO_HEADER.PO_ID, PO_LINE.PO_LINE_ID ORDER BY PO_HEADER.CREATION_DT DESC, PO_LINE.CREATION_DT DESC, REQ.CREATION_DT ASC) as RN 
        --la partición dejarÃ¡ un req por linea 
        FROM `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.req_part` as REQ
        LEFT JOIN `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.po_header` as PO_HEADER
            ON  REQ.PO_DB_ID = PO_HEADER.PO_DB_ID AND 
                REQ.PO_ID = PO_HEADER.PO_ID
        LEFT JOIN `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.po_line` AS PO_LINE
            ON  REQ.PO_DB_ID = PO_LINE.PO_DB_ID AND 
                REQ.PO_ID = PO_LINE.PO_ID AND
                REQ.PO_LINE_ID = PO_LINE.PO_LINE_ID  
        LEFT JOIN `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.evt_event` AS EVT_P
            ON  REQ.PO_DB_ID = EVT_P.EVENT_DB_ID AND 
                REQ.PO_ID = EVT_P.EVENT_ID
        WHERE   PO_HEADER.PO_ID IS NOT NULL AND 
                PO_HEADER.PO_TYPE_CD = 'PURCHASE' AND 
                PO_LINE.PO_LINE_TYPE_CD <> 'MISC' AND 
                PO_LINE.DELETED_BOOL = 0 AND
                EVT_P.EVENT_SDESC IS NOT NULL
    ),    
    REQUEST_PO_F AS(
        SELECT 
            REQ_PART_DB_ID, 
            REQ_PART_ID, 
            REQ_MASTER_ID, 
            CAST(CREATION_DT AS TIMESTAMP) AS REQ_CREATION_DT,
            REQ_HR_ID,
            REQ_NOTE,
            REQ_TYPE_CD,
            PO_ID, 
            PO_DB_ID, 
            PO_LINE_ID
        FROM REQUEST_PO
        WHERE RN = 1
    ),

    PO_NONE AS (
        SELECT 
            REQ.REQ_PART_DB_ID, 
            REQ.REQ_PART_ID, 
            REQ.REQ_MASTER_ID, 
            REQ.CREATION_DT,
            REQ.REQ_HR_ID, 
            REQ.REQ_NOTE,
            REQ.REQ_TYPE_CD,
            PO_HEADER.PO_ID, 
            PO_HEADER.PO_DB_ID, 
            PO_LINE.PO_LINE_ID,
            ROW_NUMBER() OVER (partition by PO_HEADER.PO_ID, PO_LINE.PO_LINE_ID ORDER BY PO_HEADER.CREATION_DT DESC, PO_LINE.CREATION_DT DESC) as RN
        FROM `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.po_header` as PO_HEADER
        LEFT JOIN `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.po_line` as PO_LINE
            ON  PO_HEADER.PO_DB_ID = PO_LINE.PO_DB_ID AND 
                PO_HEADER.PO_ID = PO_LINE.PO_ID        
        LEFT JOIN `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.req_part` as REQ
            ON  REQ.PO_DB_ID = PO_HEADER.PO_DB_ID AND 
                REQ.PO_ID = PO_HEADER.PO_ID AND
                REQ.PO_LINE_ID = PO_LINE.PO_LINE_ID
        LEFT JOIN REQUEST_PO_F 
            ON  REQUEST_PO_F.PO_DB_ID = PO_LINE.PO_DB_ID AND 
                REQUEST_PO_F.PO_ID = PO_LINE.PO_ID AND
                REQUEST_PO_F.PO_LINE_ID = PO_LINE.PO_LINE_ID
        LEFT JOIN `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.evt_event` AS EVT_P
            ON  PO_HEADER.PO_DB_ID = EVT_P.EVENT_DB_ID AND 
                PO_HEADER.PO_ID = EVT_P.EVENT_ID   
        WHERE   PO_HEADER.PO_TYPE_CD = 'PURCHASE' AND 
                PO_HEADER.PO_ID IS NOT NULL AND 
                REQUEST_PO_F.REQ_PART_ID IS NULL AND 
                PO_LINE.PO_LINE_TYPE_CD <> 'MISC' AND 
                PO_LINE.DELETED_BOOL = 0 AND
                EVT_P.EVENT_SDESC IS NOT NULL

    ),

    PO_NONE_F AS (
        SELECT 
            REQ_PART_DB_ID, 
            REQ_PART_ID, 
            REQ_MASTER_ID, 
            CAST(CREATION_DT AS TIMESTAMP) AS REQ_CREATION_DT,
            REQ_HR_ID,
            REQ_NOTE,
            REQ_TYPE_CD,
            PO_ID, 
            PO_DB_ID, 
            PO_LINE_ID
        FROM PO_NONE
        WHERE RN = 1
    ),
    
    UNIR AS (
        SELECT * FROM REQUEST_NONE_F
        UNION DISTINCT 
        SELECT * FROM REQUEST_PO_F
        UNION DISTINCT 
        SELECT * FROM PO_NONE_F
    ),

    EVT_EVENT AS (
        SELECT *, ROW_NUMBER() OVER(partition by EVENT_ID ORDER BY EVENT_GDT DESC) AS RN_EVENT 
        FROM `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.evt_event`
    ),

    EVT_EVENT_F AS(
        SELECT *
        FROM EVT_EVENT
        WHERE RN_EVENT = 1
    ),

    APROBACION AS(
        SELECT  
            PO_ID,
            PO_DB_ID,
            PO_AUTH_LVL_CD,
            AOG_OVERRIDE_BOOL,
            CAST(AUTH_DT AS TIMESTAMP) AS FIRST_AUTH_DT,
            ROW_NUMBER() OVER (partition by PO_ID ORDER BY AUTH_DT DESC) as RN
        FROM `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.po_auth`
    ),
    
    APROBACION_F AS (
        SELECT *
        FROM APROBACION
        WHERE RN=1
    ),

    PRIMERA_APROBACION AS (
        SELECT 
            EVENT_ID, 
            EVENT_DB_ID, 
            STAGE_DT AS PRIMERA_APROBACION_DT
        FROM
            (SELECT 
                *, 
                ROW_NUMBER() OVER (partition by EVENT_ID ORDER BY STAGE_GDT ASC) as RN
            FROM `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.evt_stage`
            WHERE
                EVENT_STATUS_CD = 'POAUTH')
            WHERE RN = 1
    ),

    RECEIVE AS(
        SELECT 
            *, 
            ROW_NUMBER() OVER (partition by EVENT_ID ORDER BY STAGE_GDT DESC) as RN
        FROM `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.evt_stage`
        WHERE 
            EVENT_STATUS_CD ='IXCMPLT'
    ),

    RECEIVE_1 AS (
        SELECT 
            EVENT_DB_ID,
            EVENT_ID,
            CAST(STAGE_GDT AS TIMESTAMP) AS RECEIVE_DT
        FROM RECEIVE
        WHERE 
            RN=1
    ),

    ISSUE AS(
        SELECT 
            *, 
            ROW_NUMBER() OVER (partition by EVENT_ID ORDER BY STAGE_GDT ASC) as RN
        FROM `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.evt_stage`
        WHERE 
            EVENT_STATUS_CD ='POISSUED'
    ),

    ISSUE_1 AS (
        SELECT 
            EVENT_DB_ID,
            EVENT_ID,
            CAST(STAGE_GDT AS TIMESTAMP) AS PO_FIRST_ISSUE_DT
        FROM ISSUE
        WHERE 
            RN=1),

    QUAR_1 AS (
        SELECT 
            INV.INV_NO_ID, 
            INV.INV_NO_DB_ID, 
            INV.BARCODE_SDESC, 
            INV.CREATION_DT, 
            SSL.PO_ID, 
            SSL.PO_DB_ID, 
            SSL.PO_LINE_ID, 
            ROW_NUMBER() OVER (partition by INV.INV_NO_ID ORDER BY PO_HEADER.CREATION_DT DESC) as RN  
        FROM `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.inv_inv` as INV
        LEFT JOIN `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.ship_shipment_line` AS SSL
            ON  INV.INV_NO_ID = SSL.INV_NO_ID AND 
                INV.INV_NO_DB_ID = SSL.INV_NO_DB_ID
        LEFT JOIN `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.VW_EVT_PO` AS PO
            ON  SSL.PO_ID = PO.po_id AND    
                SSL.PO_DB_ID = PO.po_db_id 
        LEFT JOIN  `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.po_header` AS PO_HEADER
            ON  SSL.PO_ID = PO_HEADER.PO_ID AND 
                SSL.PO_DB_ID = PO_HEADER.PO_DB_ID
        WHERE   INV_COND_CD = 'QUAR' AND 
                event_status_cd <> 'POCANCEL'
    ),

    QUAR_2 AS (
        SELECT  *,
            ROW_NUMBER() OVER (partition by PO_ID, PO_LINE_ID ORDER BY CREATION_DT DESC) as RN2  
        FROM QUAR_1 
        WHERE RN =1),

    PL_SHIPMENT AS (
        SELECT 
                SSL.*, 
                SS.CUSTOMS_SDESC,
                EVT.EVENT_STATUS_CD,
                EVT.event_sdesc,
                ROW_NUMBER() OVER (partition by SSL.PO_ID, SSL.PO_LINE_ID ORDER BY SSL.CREATION_DT DESC) as RN  -- si una linea de po posee más de un shipment, estamos dejando él último

        FROM `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.ship_shipment_line` AS SSL
        LEFT JOIN `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.ship_shipment` AS SS 
            ON  SSL.SHIPMENT_ID = SS.SHIPMENT_ID AND
                SSL.SHIPMENT_DB_ID = SS.SHIPMENT_DB_ID 
        LEFT JOIN EVT_EVENT_F AS EVT
            ON  SSL.SHIPMENT_ID = EVT.EVENT_ID AND
                SSL.SHIPMENT_DB_ID = EVT.EVENT_DB_ID            
        WHERE SSL.PO_ID IS NOT NULL AND SS.SHIPMENT_TYPE_CD = 'PURCHASE'
    ),

    PL_SHIPMENT_F AS (
        SELECT *
        FROM PL_SHIPMENT
        WHERE RN =1
        ),

    ROUTING_1 AS (
        SELECT 
			SSL.SHIPMENT_ID,
            SSL.SHIPMENT_DB_ID,
			(CASE
				WHEN RE_SHIP_TO_ID IS NOT NULL THEN SHIP_TO_LOC_ID
				ELSE NULL
			END) AS HUB,
			SSM.SEGMENT_ID,
			SSM.SEGMENT_DB_ID,
			SSM.SEGMENT_ORD

        FROM `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.po_header` AS PH
			LEFT JOIN `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.po_line` AS PL
				ON  PH.PO_ID = PL.PO_ID AND
					PH.PO_DB_ID = PL.PO_DB_ID
			LEFT JOIN `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.ship_shipment_line` AS SSL
				ON  PH.PO_ID = SSL.PO_ID AND
					PH.PO_DB_ID = SSL.PO_DB_ID	AND
					PL.PO_LINE_ID = SSL.PO_LINE_ID
			LEFT JOIN `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.ship_segment_map` AS SSM
				ON 	SSL.SHIPMENT_ID = SSM.SHIPMENT_ID AND
					SSL.SHIPMENT_DB_ID = SSM.SHIPMENT_DB_ID
			WHERE 
				PH.PO_TYPE_CD = 'PURCHASE'	
			),

	R_LLEGADA AS (
		SELECT
			DISTINCT 
			R.SHIPMENT_ID,
			R.SHIPMENT_DB_ID,
			R.HUB,
			SM_D.SEGMENT_STATUS_CD AS LLEGADA_STATUS,
            CAST(SM_D.COMPLETE_DT AS TIMESTAMP) AS LLEGADA_COMPLETE_HUB_DT,
            ROW_NUMBER() OVER (partition by R.SHIPMENT_ID ORDER BY R.SEGMENT_ORD ASC) as RN  
		FROM ROUTING_1 AS R
				INNER JOIN `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.ship_segment` AS SM_D 
					ON  R.SEGMENT_ID = SM_D.SEGMENT_ID AND 
						R.SEGMENT_DB_ID = SM_D.SEGMENT_DB_ID AND
						R.HUB = SM_D.SHIP_TO_ID --el hub es igual a un locationd e llegada, es decir, aca se ve el routing que llega al hub
		WHERE SM_D.SEGMENT_STATUS_CD <> 'CANCEL'

	),

	R_SALIDA AS (
		SELECT
			DISTINCT 
			R.SHIPMENT_ID,
			R.SHIPMENT_DB_ID,
			R.HUB,
			SM_O.SEGMENT_STATUS_CD AS SALIDA_STATUS,
            CAST(SM_O.COMPLETE_DT AS TIMESTAMP) AS LLEGADA_COMPLETE_HUB_DT,
            ROW_NUMBER() OVER (partition by R.SHIPMENT_ID ORDER BY R.SEGMENT_ORD DESC) as RN  
		FROM ROUTING_1 AS R
				INNER JOIN `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.ship_segment` AS SM_O 
					ON  R.SEGMENT_ID = SM_O.SEGMENT_ID AND 
						R.SEGMENT_DB_ID = SM_O.SEGMENT_DB_ID AND
						R.HUB = SM_O.SHIP_FROM_ID
		WHERE SM_O.SEGMENT_STATUS_CD <> 'CANCEL'
	),

    R_SALIDA_F AS (
        SELECT *
		FROM R_SALIDA 
        WHERE RN = 1
    ),
    R_LLEGADA_F AS (
        SELECT *
		FROM R_LLEGADA 
        WHERE RN = 1
    ),

    SHIPMENT_INBOUND AS (   
        SELECT                                                                                                                                                                                                                                   
            EVT_P.EVENT_SDESC AS po_number,                                                                                                                                                                                                      
            EVT_S.EVENT_SDESC AS shipment_sdesc,                                                                                                                                                                                                 
            SS.WAYBILL_SDESC AS AWB_INBOUND,    
            PH.PO_ID, 
            PH.PO_DB_ID,
            PL.PO_LINE_ID,
            ROW_NUMBER() OVER (partition by EVT_P.EVENT_SDESC, PL.PO_LINE_ID ORDER BY SS.CREATION_DT DESC) as RN --UNA POR PO                                                                                                                                   
        FROM `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.po_line` PL
        LEFT JOIN  `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.po_header` AS PH                                                                                                                                                                         
            ON  PH.PO_ID = PL.PO_ID
        LEFT JOIN `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.evt_event` AS EVT_P                                                                                                                                                                 
            ON  PH.PO_ID = EVT_P.EVENT_ID                                                                                                                                                                                                        
            AND PH.PO_DB_ID = EVT_P.EVENT_DB_ID                                                                                                                                                                                                  
        LEFT JOIN `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.ship_shipment_line` AS SSL
            ON  PH.PO_ID = SSL.PO_ID                                                                                                                                                                                                              
            AND PH.PO_DB_ID = SSL.PO_DB_ID                    
            AND PL.PO_LINE_ID = SSL.PO_LINE_ID
        LEFT JOIN `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.ship_shipment` AS SS                                                                                                                                                                
            ON  SSL.SHIPMENT_ID = SS.SHIPMENT_ID
            AND SSL.SHIPMENT_DB_ID = SS.SHIPMENT_DB_ID
        LEFT JOIN `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.evt_event` AS EVT_S                                                                                                                                                                 
            ON  SS.SHIPMENT_ID = EVT_S.EVENT_ID                                                                                                                                                                                                  
            AND SS.SHIPMENT_DB_ID = EVT_S.EVENT_DB_ID                                                                                                                                                                                            
        WHERE                                                                                                                                                                                                                                    
                PH.PO_TYPE_CD = 'PURCHASE'                                                                                                                                                                                                         
            AND EVT_P.EVENT_STATUS_CD <> 'POCANCEL'                                                                                                                                                                                              
            AND SS.SHIPMENT_TYPE_CD = 'PURCHASE'                                                                                                                                                                                                   
            AND EVT_S.EVENT_STATUS_CD <> 'IXCANCEL'                                                                                                                                                                                              
       ),
    SHIPMENT_INBOUND_F AS (
         SELECT *                                                                                                                                                                                                                                 
         FROM SHIPMENT_INBOUND                                                                                                                                                                                                                    
         WHERE RN = 1                                                                                                                                                                                                                             
    ), 

  f as  (SELECT
        DISTINCT
--      DATOS
        (CASE 
            WHEN (UNIR.REQ_PART_ID IS NOT NULL AND UNIR.PO_ID IS NULL) THEN 'REQUEST CREATED' 
            WHEN (UNIR.PO_ID IS NOT NULL AND EVT_P.EVENT_STATUS_CD = 'POCANCEL' ) THEN 'PO CANCEL'

            WHEN ((UNIR.PO_ID IS NOT NULL) AND (PL.RECEIVED_DT IS NULL) AND (Q.BARCODE_SDESC IS NOT NULL)) THEN 'PO QUAR'
            WHEN (UNIR.PO_ID IS NOT NULL AND EVT_P.EVENT_STATUS_CD = 'POPARTIAL') THEN 'PO PARTIAL'
            WHEN (UNIR.PO_ID IS NOT NULL AND PL_S.EVENT_STATUS_CD = 'IXPEND' AND PL_S.CUSTOMS_SDESC IS NOT NULL) then 'PRE RECEPCION'
            
            WHEN (UNIR.PO_ID IS NOT NULL AND EVT_P.EVENT_STATUS_CD = 'POOPEN'  AND PH.AUTH_STATUS_CD <> 'REQUESTED') THEN 'PO OPEN'
            WHEN (UNIR.PO_ID IS NOT NULL AND EVT_P.EVENT_STATUS_CD = 'POOPEN'  AND PH.AUTH_STATUS_CD = 'REQUESTED') THEN 'PO OPEN - REQUESTED'
            WHEN (UNIR.PO_ID IS NOT NULL AND EVT_P.EVENT_STATUS_CD = 'POOPEN'  AND PH.AUTH_STATUS_CD = 'REQUESTED') THEN 'PO OPEN - REQUESTED'

            WHEN (UNIR.PO_ID IS NOT NULL AND EVT_P.EVENT_STATUS_CD = 'POAUTH' ) THEN 'PO AUTORIZADA'
            WHEN (UNIR.PO_ID IS NOT NULL AND (EVT_P.EVENT_STATUS_CD = 'POISSUED' OR EVT_P.EVENT_STATUS_CD = 'POACKNOWLEDGED') AND (R_LLEGADA_F.LLEGADA_STATUS <> 'CMPLT' OR R_LLEGADA_F.LLEGADA_STATUS IS NULL)) THEN 'PO ISSUED'
            WHEN (UNIR.PO_ID IS NOT NULL AND (EVT_P.EVENT_STATUS_CD = 'POISSUED' OR EVT_P.EVENT_STATUS_CD = 'POACKNOWLEDGED') AND R_LLEGADA_F.LLEGADA_STATUS = 'CMPLT' AND R_SALIDA_F.SALIDA_STATUS = 'PEND') THEN 'PO IN HUB'
            WHEN (UNIR.PO_ID IS NOT NULL AND (EVT_P.EVENT_STATUS_CD = 'POISSUED' OR EVT_P.EVENT_STATUS_CD = 'POACKNOWLEDGED') AND R_LLEGADA_F.LLEGADA_STATUS = 'CMPLT' AND R_SALIDA_F.SALIDA_STATUS = 'INTR') THEN 'PO HUB HACIA DESTINO'
            WHEN (UNIR.PO_ID IS NOT NULL AND (EVT_P.EVENT_STATUS_CD = 'PORECEIVED' OR EVT_P.EVENT_STATUS_CD = 'POCLOSED') AND RE.RECEIVE_DT IS NOT NULL AND PL.RECEIVED_DT IS NULL) THEN 'PO RECEIVED'

            WHEN (UNIR.PO_ID IS NOT NULL AND (EVT_P.EVENT_STATUS_CD = 'PORECEIVED' OR EVT_P.EVENT_STATUS_CD = 'POCLOSED') AND PL.RECEIVED_DT IS NOT NULL) THEN 'RFI'
            ELSE 'OTRO'   


        END) po_global_status ,-- STATUS_GLOBAL,
        EVT_P.EVENT_SDESC AS po_code,
        PH.PO_TYPE_CD po_type_code,
        OV.VENDOR_CD vendor_code,
        EVT_R.EVENT_SDESC AS request_barcode, --REQ_BARCODE,
        UNIR.REQ_NOTE request_note_desc,  
        UNIR.REQ_TYPE_CD request_type,
        OV.VENDOR_NAME vendor_name,
        EVT_P.EVENT_STATUS_CD as status_code,
--        EVP.EVENT_STATUS_SDESC status_desc,
        IFNULL(PH.AUTH_STATUS_CD,'N/A') auth_code,
        PA.PO_AUTH_LVL_CD auth_level_code,
        PA.AOG_OVERRIDE_BOOL aog_averride_flag,
--        EVP.CONTACT_USERNAME username_code,
        IFNULL(PH.REQ_PRIORITY_CD, 'N/A') AS po_priority_code,
        PH.CURRENCY_CD currency_code,
        EVP.TOTAL_PRICE_QT total_price_amt, 

--INV_LOC.LOC_CD AS SHIP_TO, INV_LOC2.LOC_CD AS RE_EX_TO_LOC

        (CASE
            WHEN PH.RE_SHIP_TO_ID IS NULL THEN INV_LOC.LOC_CD 
            ELSE INV_LOC2.LOC_CD
        END) 
        AS ship_to_location_code,
        (CASE
            WHEN PH.RE_SHIP_TO_ID IS NOT NULL THEN INV_LOC.LOC_CD
            ELSE NULL
        END) 
        AS ship_to_hub_code, --HUB,
        R_LLEGADA_F.LLEGADA_STATUS routing_arrival_hub,
        R_SALIDA_F.SALIDA_STATUS   routing_departure_hub,
        PH.TRANSPORT_TYPE_CD transport_type_code,
        EQP.PART_NO_OEM part_oem_code,
        EQP.PART_NO_SDESC part_oem_desc,
        EQS.STOCK_NO_NAME stock_name,
        EQS.STOCK_NO_CD stock_code,
        PL.LINE_NO_ORD po_line_num,
        PL.LINE_LDESC po_line_desc,
--      EVP.EXCHG_QT,
        PL.ORDER_QT order_qty,
        PL.RECEIVED_QT received_qty,
        PL.QTY_UNIT_CD qty_unit_code,
        PL.UNIT_PRICE unit_amt,
        PL.LINE_PRICE po_line_amt,
--      PL.BASE_UNIT_PRICE,
        PL.PO_LINE_TYPE_CD po_line_type_code,
        PL.PRICE_TYPE_CD price_type_code,
        HR.HR_CD po_bp_owner_code,
        HR_REQ.HR_CD as request_bp_owner_code, -- HR_CD_REQ, -->NUEVO


--      FECHAS
        IFNULL(CAST(PH.CREATION_DT AS datetime), CAST(UNIR.REQ_CREATION_DT AS datetime)) AS po_creation_dt,
        CAST(PL.CREATION_DT AS datetime) AS po_line_creation_dt,
        CAST(EVP.NEEDED_BY_DT AS datetime) AS po_needed_by_dt,
        CAST(EVP.PROMISED_BY_DT AS datetime) AS po_promesed_by_dt,
        CAST(ISSUE.po_first_issue_dt AS datetime) AS po_first_issue_dt,
        CAST(PH.ISSUED_DT AS datetime) AS po_last_issue_dt,
        CAST(RE.RECEIVE_DT AS datetime) AS shipment_receive_dt,
        CAST(PH.closed_dt AS datetime) AS po_closed_dt,
        CAST(PL.RECEIVED_DT AS datetime) AS po_line_received_dt,

        CAST(R_LLEGADA_F.LLEGADA_COMPLETE_HUB_DT AS datetime) complete_hub_arrival_dt,  ----------------------------------------
        CAST(UNIR.REQ_CREATION_DT AS datetime) request_creation_dt,-------------------------------------------
        CAST(FA.PRIMERA_APROBACION_DT AS datetime) po_first_auth_dt,
        CAST(PL.RECEIVED_DT AS datetime) AS po_rfi_dt,

--          LLAVES
        UNIR.PO_DB_ID po_db_id,
        UNIR.PO_ID po_id,
        UNIR.REQ_PART_ID request_part_id,
        PH.VENDOR_DB_ID vendor_db_id,
        PH.VENDOR_ID vendor_id,
--        EVP.VENDOR_LOC_DB_ID vendor_loc_db_id,
--        EVP.VENDOR_LOC_ID vendor_loc_id,
        PL.PART_NO_DB_ID part_no_db_id,
        PL.PART_NO_ID part_no_id,
        PL.PO_LINE_ID po_line_id,
        EQS.STOCK_NO_ID stock_no_id,
        EQS.STOCK_NO_DB_ID stock_no_db_id,
        SIN.AWB_INBOUND inbound_awb_code
    FROM UNIR
    LEFT JOIN `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.po_header` AS PH
        ON  UNIR.PO_ID = PH.PO_ID AND
            UNIR.PO_DB_ID = PH.PO_DB_ID
    LEFT JOIN EVT_EVENT_F AS EVT_P
        ON  PH.PO_DB_ID = EVT_P.EVENT_DB_ID AND 
            PH.PO_ID = EVT_P.EVENT_ID   
    LEFT JOIN `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.VW_EVT_PO` AS EVP
        ON  UNIR.PO_ID = EVP.PO_ID AND 
            UNIR.PO_DB_ID = EVP.PO_DB_ID
    LEFT JOIN `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.po_line` AS PL
        ON  UNIR.PO_ID = PL.PO_ID AND
            UNIR.PO_DB_ID = PL.PO_DB_ID AND 
            UNIR.PO_LINE_ID = PL.PO_LINE_ID
    LEFT JOIN `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.org_vendor` AS OV
        ON  PH.VENDOR_ID =   OV.VENDOR_ID AND        
            PH.VENDOR_DB_ID = OV.VENDOR_DB_ID
    LEFT JOIN `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.eqp_part_no`AS EQP 
        ON  PL.PART_NO_ID =   EQP.PART_NO_ID AND        
            PL.PART_NO_DB_ID = EQP.PART_NO_DB_ID
    LEFT JOIN `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.eqp_stock_no` AS EQS
        ON  EQS.STOCK_NO_ID = EQP.STOCK_NO_ID AND        
            EQS.STOCK_NO_DB_ID = EQP.STOCK_NO_DB_ID
    LEFT JOIN APROBACION_F AS PA
        ON  UNIR.PO_ID = PA.PO_ID AND
            UNIR.PO_DB_ID = PA.PO_DB_ID
    LEFT JOIN PRIMERA_APROBACION AS FA
        ON  UNIR.PO_ID = FA.EVENT_ID AND
            UNIR.PO_DB_ID = FA.EVENT_DB_ID                        
    LEFT JOIN PL_SHIPMENT_F AS PL_S 
        ON  UNIR.PO_ID = PL_S.PO_ID AND
            UNIR.PO_DB_ID = PL_S.PO_DB_ID AND
            UNIR.PO_LINE_ID = PL_S.PO_LINE_ID
    LEFT JOIN RECEIVE_1 AS RE
        ON  PL_S.SHIPMENT_ID = RE.EVENT_ID AND
            PL_S.SHIPMENT_DB_ID = RE.EVENT_DB_ID
    LEFT JOIN ISSUE_1 AS ISSUE
        ON  UNIR.PO_ID = ISSUE.EVENT_ID AND
            UNIR.PO_DB_ID = ISSUE.EVENT_DB_ID
    LEFT JOIN EVT_EVENT_F AS EVT_R 
        ON  UNIR.REQ_PART_ID = EVT_R.EVENT_ID  AND
            UNIR.REQ_PART_DB_ID = EVT_R.EVENT_DB_ID
    LEFT JOIN `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.inv_loc` INV_LOC
        ON  PH.SHIP_TO_LOC_ID = INV_LOC.LOC_ID AND
            PH.SHIP_TO_LOC_DB_ID = INV_LOC.LOC_DB_ID
    LEFT JOIN `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.inv_loc` INV_LOC2
        ON  PH.RE_SHIP_TO_ID = INV_LOC2.LOC_ID AND
            PH.SHIP_TO_LOC_DB_ID = INV_LOC2.LOC_DB_ID
    LEFT JOIN R_LLEGADA_F ON 
        PL_S.shipment_id = R_LLEGADA_F.SHIPMENT_ID 
    LEFT JOIN R_SALIDA_F ON 
        PL_S.shipment_id = R_SALIDA_F.SHIPMENT_ID
    LEFT JOIN `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.org_hr` AS HR
        ON  PH.CONTACT_HR_ID = HR.HR_ID AND
            PH.CONTACT_HR_DB_ID = HR.HR_DB_ID
    LEFT JOIN `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.org_hr` AS HR_REQ
        ON  UNIR.REQ_HR_ID = HR_REQ.HR_ID
    LEFT JOIN QUAR_2 AS Q 
        ON  UNIR.PO_ID = Q.PO_ID AND
            UNIR.PO_DB_ID = Q.PO_DB_ID AND 
            UNIR.PO_LINE_ID = Q.PO_LINE_ID
    LEFT JOIN SHIPMENT_INBOUND_F AS SIN   
        ON  PH.po_id = SIN.PO_ID
        AND PH.po_db_id = SIN.PO_DB_ID
        AND PL.PO_LINE_ID = SIN.PO_LINE_ID

)
SELECT * FROM F;

CREATE OR REPLACE TABLE `bc-te-dlake-dev-s7b3.cln_etracking_us.purchase_order` AS

WITH PO_FECHAS AS (
SELECT  *,
        --
        -- REQUEST A CREACION
        ROUND((DATETIME_DIFF(
                              IFNULL (PO_LINE_CREATION_DT,       IF (PO_GLOBAL_STATUS='REQUEST CREATED'
                                                                ,CURRENT_DATETIME()
                                                                ,REQUEST_CREATION_DT))
                             ,IFNULL (REQUEST_CREATION_DT, IFNULL (PO_LINE_CREATION_DT,  IF (PO_GLOBAL_STATUS='REQUEST CREATED'
                                                                                            ,CURRENT_DATETIME()
                                                                                            ,REQUEST_CREATION_DT))) 
                             ,MINUTE) / 60
              ),1) request_creation_val
        --
        --  CREACION A FIRST_AUTH   
        ,ROUND((DATETIME_DIFF(
                              IFNULL (PO_FIRST_AUTH_DT,      IF (PO_GLOBAL_STATUS in ('PO OPEN', 'PO OPEN - REQUESTED')
                                                                ,CURRENT_DATETIME()
                                                                ,IFNULL(PO_LINE_CREATION_DT, REQUEST_CREATION_DT)))
                             ,IFNULL (PO_LINE_CREATION_DT, REQUEST_CREATION_DT)
                             ,MINUTE) / 60
               ),1) creation_first_auth_val
        --
        --  FIRST_AUTH A FIRST_ISSUE   
        ,ROUND((DATETIME_DIFF(
                              IFNULL(PO_FIRST_ISSUE_DT,      IF (PO_GLOBAL_STATUS='PO AUTORIZADA'
                                                                ,CURRENT_DATETIME()
                                                                ,IFNULL(PO_FIRST_AUTH_DT, IFNULL(PO_LINE_CREATION_DT, REQUEST_CREATION_DT))))
                             ,IFNULL(PO_FIRST_AUTH_DT, IFNULL(PO_LINE_CREATION_DT, REQUEST_CREATION_DT))
                             ,MINUTE) / 60
               ),1) first_auth_first_issue_val
        --
        -- FISRT_ISSUE A LLEGADA COMPLETE HUB
        ,ROUND((DATETIME_DIFF(
                              IFNULL(COMPLETE_HUB_ARRIVAL_DT, IF (PO_GLOBAL_STATUS='PO ISSUED'
                                                                 ,CURRENT_DATETIME()
                                                                 ,IFNULL(PO_FIRST_ISSUE_DT, IFNULL(PO_FIRST_AUTH_DT, IFNULL(PO_LINE_CREATION_DT, REQUEST_CREATION_DT)))))
                             ,IFNULL(PO_FIRST_ISSUE_DT, IFNULL(PO_FIRST_AUTH_DT, IFNULL(PO_LINE_CREATION_DT, REQUEST_CREATION_DT)))
                             ,MINUTE) / 60
               ),1) first_issue_llegada_complete_hub_val
        --
        -- LLEGADA COMPLETE HUB A RECEIVED
        ,ROUND((DATETIME_DIFF(
                              IFNULL(SHIPMENT_RECEIVE_DT,     IF (PO_GLOBAL_STATUS in ('PO IN HUB','PO HUB HACIA DESTINO')
                                                                 ,CURRENT_DATETIME()
                                                                 ,IFNULL(COMPLETE_HUB_ARRIVAL_DT,IFNULL(PO_FIRST_ISSUE_DT, IFNULL(PO_FIRST_AUTH_DT, IFNULL(PO_LINE_CREATION_DT, REQUEST_CREATION_DT))))))
                             ,IFNULL(COMPLETE_HUB_ARRIVAL_DT,IFNULL(PO_FIRST_ISSUE_DT, IFNULL(PO_FIRST_AUTH_DT, IFNULL(PO_LINE_CREATION_DT, REQUEST_CREATION_DT))))
                             ,MINUTE) / 60
               ),1) llegada_complete_hub_received_val
        --
        -- RECEIVED A RFI
        ,ROUND((DATETIME_DIFF (
                              IFNULL (PO_RFI_DT, IF (PO_GLOBAL_STATUS IN ('PO RECEIVED', 'PRE RECEPCION', 'PO QUAR', 'PO PARTIAL')
                                                    ,CURRENT_DATETIME()
                                                    ,IFNULL(SHIPMENT_RECEIVE_DT,IFNULL(COMPLETE_HUB_ARRIVAL_DT,IFNULL(PO_FIRST_ISSUE_DT,IFNULL(PO_FIRST_AUTH_DT,IFNULL(PO_LINE_CREATION_DT,REQUEST_CREATION_DT)))))))
                             ,IFNULL(SHIPMENT_RECEIVE_DT,IFNULL(COMPLETE_HUB_ARRIVAL_DT,IFNULL(PO_FIRST_ISSUE_DT,IFNULL(PO_FIRST_AUTH_DT,IFNULL(PO_LINE_CREATION_DT,REQUEST_CREATION_DT)))))
                             ,MINUTE) / 60
                ),1) receive_rfi_val
        -- 
        -- Clasifica los "peores" estagios y recupera las menores fechas de la PO
        --
        ,MIN(REQUEST_CREATION_DT)      over (partition by po_code) STAGE_REQUEST_CREATION_DT     -- REQUEST CREATED
        ,MIN(PO_LINE_CREATION_DT)      over (partition by po_code) STAGE_CREATION_DT             -- PO OPEN, PO OPEN - REQUESTED, PO CANCEL
        ,MIN(PO_FIRST_AUTH_DT)         over (partition by po_code) STAGE_FIRST_AUTH_DT           -- PO AUTORIZADA
        ,MIN(PO_FIRST_ISSUE_DT)        over (partition by po_code) STAGE_FIRST_ISSUE_DT          -- PO ISSUE
        ,MIN(COMPLETE_HUB_ARRIVAL_DT)  over (partition by po_code) STAGE_COMPLETE_HUB_ARRIVAL_DT -- PO IN HUB. PO HUB HACIA DESTINO
        ,MIN(SHIPMENT_RECEIVE_DT)      over (partition by po_code) STAGE_SHIPMENT_RECEIVE_DT     -- PO RECEIVED, PRE RECEPCION, PO QUAR, PO PARTIAL
        ,MIN(PO_RFI_DT)                over (partition by po_code) STAGE_RFI_DT                  -- PO RFI, OTRO
        ,MIN(CASE PO_GLOBAL_STATUS
                           WHEN 'REQUEST CREATED'      THEN 1 
                           WHEN 'PO OPEN'              THEN 2
                           WHEN 'PO OPEN - REQUESTED'  THEN 3
                           WHEN 'PO AUTORIZADA'        THEN 4
                           WHEN 'PO ISSUED'            THEN 5
                           WHEN 'PO IN HUB'            THEN 6
                           WHEN 'PO HUB HACIA DESTINO' THEN 7
                           WHEN 'PRE RECEPCION'         THEN 8
                           WHEN 'PO PARTIAL'           THEN 9
                           WHEN 'PO RECEIVED'          THEN 10
                           WHEN 'PO QUAR'              THEN 11
                           WHEN 'RFI'                  THEN 12
                           WHEN 'OTRO'                 THEN 13
                           WHEN 'PO CANCEL'            THEN 14
                           ELSE 99
                 END) over (partition by po_code) stage_code
FROM `bc-te-dlake-dev-s7b3.cln_etracking_us.tmp_purchase_order`
-- WHERE F.PO_GLOBAL_STATUS = 'RFI'
),
--#
--# Parte 2) Implementación de los flags de SLA
--#
FINAL_3 AS (
SELECT   *,
         --
         -- Acumula las horas de las etapas         
         ROUND((f.request_creation_val + f.creation_first_auth_val + f.first_auth_first_issue_val + f.first_issue_llegada_complete_hub_val + f.llegada_complete_hub_received_val + f.receive_rfi_val),1) stage_val
         --
         -- Acumula las horas de SLAs por prioridad
       ,(SELECT SUM(gs.sla_val) FROM `bc-te-dlake-dev-s7b3.cln_etracking_us.general_sla`gs WHERE gs.process_id = 2 AND gs.stage_id IN (1,2,3,4,5,6) AND gs.req_priority_code = f.po_priority_code
	    AND F.po_creation_dt>=GS.valid_from
		AND (GS.valid_until is null or F.po_creation_dt<=gs.valid_until)
	   ) sla_standard_val
         --
         -- FLAG SLA
         -- Compara el acumulado de horas de la etapa y compara con el SLA de la etapa, acumulando las horas de las etapas anteriores, 
         -- caso las fechas estÃ©n vacias 
         --
         -- request a creación
         --
        ,IF (f.request_creation_val       > (SELECT gs.sla_val FROM `bc-te-dlake-dev-s7b3.cln_etracking_us.general_sla` gs
                                             WHERE  gs.process_id = 2 AND gs.stage_id = 1 AND gs.req_priority_code = f.po_priority_code
                                             AND F.po_creation_dt>=GS.valid_from
											 AND (GS.valid_until is null or F.po_creation_dt<=gs.valid_until)
											 ),1,0) request_creation_flag
         --
         -- creación a first_auth
         --
        ,IF (f.creation_first_auth_val    > (SELECT SUM(gs.sla_val) sla_val FROM `bc-te-dlake-dev-s7b3.cln_etracking_us.general_sla` gs
                                             WHERE  gs.process_id = 2 AND gs.req_priority_code = f.po_priority_code
                                             AND   (gs.stage_id   = 2
                                             OR     CASE WHEN (f.po_line_creation_dt is null) THEN gs.stage_id IN (1) END) 
                                             AND F.po_creation_dt>=GS.valid_from
											 AND (GS.valid_until is null or F.po_creation_dt<=gs.valid_until)
											 ),1,0) creation_first_auth_flag
         --
         -- first_auth a first_issue
         --
        ,IF (f.first_auth_first_issue_val > (SELECT SUM(gs.sla_val) sla_val FROM `bc-te-dlake-dev-s7b3.cln_etracking_us.general_sla` gs
                                             WHERE  gs.process_id = 2 AND gs.req_priority_code = f.po_priority_code 
                                             AND   (gs.stage_id   = 3
                                             OR     CASE WHEN (f.po_first_auth_dt is null) THEN gs.stage_id IN (2) END
                                             OR     CASE WHEN (f.po_line_creation_dt   is null and f.po_first_auth_dt is null) THEN gs.stage_id IN (1) END
                                             ) 
                                             AND F.po_creation_dt>=GS.valid_from
											 AND (GS.valid_until is null or F.po_creation_dt<=gs.valid_until)
											 ),1,0) first_auth_first_issue_flag
         --
         -- first_issue a llegada_complete_hub
         --
        ,IF (f.first_issue_llegada_complete_hub_val > (SELECT SUM(gs.sla_val) sla_val FROM `bc-te-dlake-dev-s7b3.cln_etracking_us.general_sla` gs
                                                       WHERE  gs.process_id = 2 AND gs.req_priority_code = f.po_priority_code 
                                                       AND   (gs.stage_id   = 4
                                                       OR     CASE WHEN (f.po_first_issue_dt is null) THEN gs.stage_id IN (3) END
                                                       OR     CASE WHEN (f.po_first_auth_dt  is null and f.po_first_issue_dt is null) THEN gs.stage_id IN (2) END 
                                                       OR     CASE WHEN (f.po_line_creation_dt    is null and f.po_first_auth_dt  is null and f.po_first_issue_dt is null) THEN gs.stage_id IN (1) END
                                                       ) 
                                                       AND F.po_creation_dt>=GS.valid_from
													   AND (GS.valid_until is null or F.po_creation_dt<=gs.valid_until)
													   ),1,0) first_issue_llegada_complete_hub_flag
         --
         -- llegada_complete_hub a received
         --
        ,IF (f.llegada_complete_hub_received_val    > (SELECT SUM(gs.sla_val) sla_val FROM `bc-te-dlake-dev-s7b3.cln_etracking_us.general_sla` gs
                                                       WHERE  gs.process_id = 2 AND gs.req_priority_code = f.po_priority_code 
                                                       AND   (gs.stage_id   = 5
                                                       OR     CASE WHEN (f.complete_hub_arrival_dt is null) THEN gs.stage_id IN (4) END
                                                       OR     CASE WHEN (f.po_first_issue_dt       is null and f.complete_hub_arrival_dt is null) THEN gs.stage_id IN (3) END 
                                                       OR     CASE WHEN (f.po_first_auth_dt        is null and f.po_first_issue_dt       is null and f.complete_hub_arrival_dt is null) THEN gs.stage_id IN (2) END
                                                       OR     CASE WHEN (f.po_line_creation_dt          is null and f.po_first_auth_dt        is null and f.po_first_issue_dt       is null and f.complete_hub_arrival_dt is null) THEN gs.stage_id IN (1) END
                                                       ) 
                                                       AND F.po_creation_dt>=GS.valid_from
													   AND (GS.valid_until is null or F.po_creation_dt<=gs.valid_until)
													   ),1,0) llegada_complete_hub_received_flag
         --
         -- received a rfi
         --
        ,IF (f.receive_rfi_val                      > (SELECT SUM(gs.sla_val) sla_val FROM `bc-te-dlake-dev-s7b3.cln_etracking_us.general_sla` gs
                                                       WHERE  gs.process_id = 2 AND gs.req_priority_code = f.po_priority_code 
                                                       AND   (gs.stage_id   = 6
                                                       OR     CASE WHEN (f.shipment_receive_dt     is null) THEN gs.stage_id IN (5) END
                                                       OR     CASE WHEN (f.complete_hub_arrival_dt is null and f.shipment_receive_dt     is null) THEN gs.stage_id IN (4) END
                                                       OR     CASE WHEN (f.po_first_issue_dt       is null and f.complete_hub_arrival_dt is null and f.shipment_receive_dt     is null) THEN gs.stage_id IN (3) END 
                                                       OR     CASE WHEN (f.po_first_auth_dt        is null and f.po_first_issue_dt       is null and f.complete_hub_arrival_dt is null and f.shipment_receive_dt     is null) THEN gs.stage_id IN (2) END
                                                       OR     CASE WHEN (f.po_line_creation_dt          is null and f.po_first_auth_dt        is null and f.po_first_issue_dt       is null and f.complete_hub_arrival_dt is null and f.shipment_receive_dt is null) THEN gs.stage_id IN (1) END
                                                       ) 
                                                       AND F.po_creation_dt>=GS.valid_from
													   AND (GS.valid_until is null or F.po_creation_dt<=gs.valid_until)
													   ),1,0) receive_rfi_flag
         --
         -- Suma las horas de la cadena y las compara con el acumulado de SLA, de acuerdo donde se encuentra en la cadena
         --
        ,IF    (f.request_creation_val                 + f.creation_first_auth_val           + f.first_auth_first_issue_val + 
                f.first_issue_llegada_complete_hub_val + f.llegada_complete_hub_received_val + f.receive_rfi_val > (
                SELECT SUM(sla_val)
                FROM `bc-te-dlake-dev-s7b3.cln_etracking_us.general_sla` gs
                WHERE gs.process_id = 2
                AND   gs.req_priority_code = f.po_priority_code
                AND   (CASE WHEN f.po_global_status in ('PO OPEN', 'REQUEST CREATED', 'PO OPEN - REQUESTED', 'PO CANCEL') THEN stage_id IN (1) 
                            WHEN f.po_global_status  = 'PO AUTORIZADA'                                                    THEN stage_id IN (1,2) 
                            WHEN f.po_global_status  = 'PO ISSUED'                                                        THEN stage_id IN (1,2,3) 
                            WHEN f.po_global_status IN ('PO IN HUB','PO HUB HACIA DESTINO')                               THEN stage_id IN (1,2,3,4) 
                            WHEN f.po_global_status IN ('PO RECEIVED', 'PO PARTIAL', 'PRE RECEPCION')                     THEN stage_id IN (1,2,3,4,5) 
                            WHEN f.po_global_status in ('RFI', 'PO QUAR', 'OTRO')                                         THEN stage_id IN (1,2,3,4,5,6) 
                            ELSE stage_id IN (1,2,3,4,5,6) 
                            END)
                 AND F.po_creation_dt>=GS.valid_from
				 AND (GS.valid_until is null or F.po_creation_dt<=gs.valid_until)
				 ),1,0) stage_flag,
        --
FROM po_fechas f)
--#
--# Clasifica estagios y recupera las menores fechas de la PO (comentado y transferido arriba para el inicio - "query queda sin recursos")
--#
/*
STAGE as (
         SELECT  po_code
                ,MIN(PO_LINE_CREATION_DT)     STAGE_CREATION_DT
                ,MIN(PO_FIRST_AUTH_DT)        STAGE_FIRST_AUTH_DT
                ,MIN(PO_FIRST_ISSUE_DT)       STAGE_FIRST_ISSUE_DT
                ,MIN(COMPLETE_HUB_ARRIVAL_DT) STAGE_COMPLETE_HUB_ARRIVAL_DT
                ,MIN(SHIPMENT_RECEIVE_DT)     STAGE_SHIPMENT_RECEIVE_DT
                ,MIN(PO_RFI_DT)               STAGE_RFI_DT
                ,MIN(CASE PO_GLOBAL_STATUS
                           WHEN 'REQUEST CREATED' THEN 1 
                           WHEN 'PO OPEN'         THEN 2 
                           WHEN 'PO CANCEL'       THEN 3
                           WHEN 'PO AUTORIZADA'   THEN 4
                           WHEN 'PO ISSUED'       THEN 5
                           WHEN 'PO IN HUB'       THEN 6
                           WHEN 'PO HUB HACIA DESTINO'  THEN 7
                           WHEN 'PO PARTIAL'      THEN 8
                           WHEN 'PO RECEIVED'     THEN 9
                           WHEN 'RFI'             THEN 10
                           ELSE 1
                 END) stage_code
    FROM FINAL_3 F
    GROUP BY 1
)
*/
--#
--# Parte 3) Query Final, con indicares generales
--#
SELECT F.*
       --
       -- Indicador de flag por status
      ,CASE PO_GLOBAL_STATUS
            WHEN 'REQUEST CREATED'      THEN IF (request_creation_flag = 0,0,1)
            WHEN 'PO OPEN'              THEN IF (request_creation_flag = 0,0,1)
            WHEN 'PO OPEN - REQUESTED'  THEN IF (request_creation_flag = 0,0,1)
            WHEN 'PO AUTORIZADA'        THEN IF (first_auth_first_issue_flag = 0,0,1)
            WHEN 'PO ISSUED'            THEN IF (first_auth_first_issue_flag = 0,0,1)
            WHEN 'PO IN HUB'            THEN IF (first_issue_llegada_complete_hub_flag = 0,0,1)
            WHEN 'PO HUB HACIA DESTINO' THEN IF (first_issue_llegada_complete_hub_flag = 0,0,1)
            WHEN 'PRE RECEPCION'         THEN IF (receive_rfi_flag = 0,0,1)
            WHEN 'PO PARTIAL'           THEN IF (receive_rfi_flag = 0,0,1)
            WHEN 'PO RECEIVED'          THEN IF (receive_rfi_flag = 0,0,1)
            WHEN 'RFI'                  THEN IF (receive_rfi_flag = 0,0,1)
            WHEN 'PO QUAR'              THEN IF (receive_rfi_flag = 0,0,1)
            WHEN 'PO CANCEL'            THEN IF (request_creation_flag = 0,0,1)
            WHEN 'OTRO'                 THEN IF (request_creation_flag = 0,0,1)
       END po_unit_stage_flag
       --
       -- Compara el SLA acumulado total con las horas totales de la cadena, para ver el consumo de la cadena
      ,CASE WHEN round (ifnull(f.stage_val / f.sla_standard_val,0),4)  > 1     then 'SLA Exceeded' 
            WHEN round (ifnull(f.stage_val / f.sla_standard_val,0),4)  >= 0.90 and round (f.stage_val / f.sla_standard_val,4)  < 1    then 'Over 90%' 
            WHEN round (ifnull(f.stage_val / f.sla_standard_val,0),4)  >= 0.70 and round (f.stage_val / f.sla_standard_val,4)  < 0.90 then 'Between 70% - 89%' 
            WHEN round (ifnull(f.stage_val / f.sla_standard_val,0),4)  >= 0.50 and round (f.stage_val / f.sla_standard_val,4)  < 0.70 then 'Between 50% - 69%' 
            WHEN round (ifnull(f.stage_val / f.sla_standard_val,0),4)  <  0.50 then 'Less than 50%' 
       END  stage_over_sla_desc
       --
      ,CASE stage_code
            WHEN  1   THEN 'REQUEST CREATED'
            WHEN  2   THEN 'PO OPEN'
            WHEN  3   THEN 'PO OPEN - REQUESTED'
            WHEN  4   THEN 'PO AUTORIZADA'
            WHEN  5   THEN 'PO ISSUED'
            WHEN  6   THEN 'PO IN HUB'
            WHEN  7   THEN 'PO HUB HACIA DESTINO'
            WHEN  8   THEN 'PRE RECEPCION'
            WHEN  9   THEN 'PO PARTIAL'
            WHEN  10  THEN 'PO RECEIVED'
            WHEN  11  THEN 'PO QUAR'
            WHEN  12  THEN 'RFI'
            WHEN  13  THEN 'OTRO'
            WHEN  14  THEN 'PO CANCEL'
            ELSE  'OTRO'      
            END po_stage_code
      --
      -- STAGE_REQUEST_CREATION_DT     -- REQUEST CREATED
      -- STAGE_CREATION_DT             -- PO OPEN, PO OPEN - REQUESTED, PO CANCEL
      -- STAGE_FIRST_AUTH_DT           -- PO AUTORIZADA
      -- STAGE_FIRST_ISSUE_DT          -- PO ISSUE
      -- STAGE_COMPLETE_HUB_ARRIVAL_DT -- PO IN HUB. PO HUB HACIA DESTINO
      -- STAGE_SHIPMENT_RECEIVE_DT     -- PO RECEIVED, PRE RECEPCION, PO QUAR, PO PARTIAL
      -- STAGE_RFI_DT                  -- PO RFI, OTRO
      --
     ,CASE stage_code
         WHEN 1  THEN STAGE_REQUEST_CREATION_DT
         WHEN 2  THEN STAGE_CREATION_DT
         WHEN 3  THEN STAGE_CREATION_DT
         WHEN 4  THEN STAGE_FIRST_AUTH_DT
         WHEN 5  THEN STAGE_FIRST_ISSUE_DT
         WHEN 6  THEN STAGE_COMPLETE_HUB_ARRIVAL_DT
         WHEN 7  THEN STAGE_COMPLETE_HUB_ARRIVAL_DT
         WHEN 8  THEN STAGE_SHIPMENT_RECEIVE_DT
         WHEN 9  THEN IFNULL(STAGE_RFI_DT, STAGE_SHIPMENT_RECEIVE_DT)
         WHEN 10 THEN IFNULL(STAGE_RFI_DT, STAGE_SHIPMENT_RECEIVE_DT)
         WHEN 11 THEN IFNULL(STAGE_RFI_DT, STAGE_SHIPMENT_RECEIVE_DT)
         WHEN 12 THEN STAGE_RFI_DT
         WHEN 13 THEN STAGE_RFI_DT
         WHEN 14 THEN STAGE_CREATION_DT
         WHEN 99 THEN STAGE_RFI_DT
      END po_stage_dt
    --
        ,ex.status status_expeditors
        ,ex.Pick_Up_Notification
        ,ex.Picked_Up
        ,ex.Freight_Received
        ,ex.Documents_Received
        ,ex.Documents_Scanned
        ,ex.Bill_of_Lading_Processed
        ,ex.Booked
        ,ex.Transferred_to_Airline_or_GHA
        ,ex.Confirmed_on_Board
        ,ex.Arrived_At_Master_Destination
        ,ex.Broker_Turnover
        ,awb.doc_prefix
        ,awb.doc_number
        ,awb.min_flight_date
        ,awb.max_flight_date
        ,awb.awb_pieces_qty
        ,awb.awb_kg_chargeable
        ,awb.set_flight_number
        ,awb.set_origin_system
        ,awb.awb_departure_dt
        ,awb.awb_arrival_dt
FROM FINAL_3 F
left join `bc-te-dlake-dev-s7b3.cln_etracking_us.tmp_expeditors_reference`  ex 
on F.po_code=ex. referenece and ex.row_id=1
left join ( -- Latam Cargo --
           SELECT 
                  leg.doc_prefix
                 ,leg.doc_number
                 ,STRING_AGG(DISTINCT(CAST (leg.flight_number AS STRING)),'/') set_flight_number
                 ,STRING_AGG(DISTINCT(      leg.data_origin_flg)         ,'/') set_origin_system
                 ,min(leg.flight_date)       min_flight_date
                 ,max(leg.flight_date)       max_flight_date
                 ,max(leg.awb_pieces_qty)    awb_pieces_qty
                 ,max(leg.awb_kg_chargeable) awb_kg_chargeable
                  --
                 ,MIN(CAST(CONCAT(SUBSTR(CAST(TRIC_FCH_SALIDA_REAL_LOCAL AS STRING),1,10),'T',TIME_ADD(TIME "00:00:00", INTERVAL CAST(TRIC_HRA_SALIDA_REAL_LOCAL AS INT64) SECOND)) AS datetime )) as awb_departure_dt
                 ,MAX(CAST(CONCAT(SUBSTR(CAST(TRIC_FCH_ARRIBO_REAL_LOCAL AS STRING),1,10),'T',TIME_ADD(TIME "00:00:00", INTERVAL CAST(TRIC_HRA_ARRIBO_REAL_LOCAL AS INT64) SECOND)) AS datetime )) as awb_arrival_dt                  
                  --
                  --,MIN(DATETIME_ADD(tric.TRIC_FCH_SALIDA_REAL_GMT, INTERVAL CAST(ori.time_diff_minute AS INT64) MINUTE)) awb_departure_dt
                  --,MAX(DATETIME_ADD(tric.TRIC_FCH_ARRIBO_REAL_GMT, INTERVAL CAST(des.time_diff_minute AS INT64) MINUTE)) awb_arrival_dt
           FROM       `bc-te-dlake-dev-s7b3.mst_cargo_us.leg_awb_entity` leg 
           LEFT JOIN  `bc-te-dlake-dev-s7b3.stg_ora_exnwitin_fastcl_us.inventario_transporte_carga` AS INTC ON intc.LNAR_CDG_IATA        = leg.airline_code
                                                                                                           AND intc.CDVL_NMR             = leg.flight_number
                                                                                                           AND intc.VLOS_FCH             = leg.flight_date -- leg.dep_leg_date
           LEFT JOIN  `bc-te-dlake-dev-s7b3.stg_ora_exnwitin_fastcl_us.tramos_inventario_carga`    AS TRIC  ON intc.intc_seq_cdg         = tric.intc_seq_cdg 
                                                                                                           AND tric.arpr_cdg_destino     = leg.airpt_destination_leg
                                                                                                           AND tric.arpr_cdg_origen      = leg.airpt_origin_leg
         --LEFT JOIN  `bc-te-dlake-dev-s7b3.mst_entity_flight_us.time_difference` ori                       ON leg.airpt_origin_leg      = ori.airport_code
         --                                                                                                AND leg.dep_leg_date BETWEEN ori.time_diff_start_time_utc AND ori.time_diff_end_time_utc
         --LEFT JOIN  `bc-te-dlake-dev-s7b3.mst_entity_flight_us.time_difference` des                       ON leg.airpt_destination_leg = des.airport_code
         --                                                                                                AND leg.dep_leg_date BETWEEN des.time_diff_start_time_utc AND des.time_diff_end_time_utc
           GROUP BY leg.doc_prefix
                   ,leg.doc_number
          ) awb       
           on awb.doc_prefix = SAFE_CAST(SUBSTR(LPAD(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(f.inbound_awb_code,'-',''),' ',''),'"',''),11,'0'),1,3) AS INTEGER) and 
              awb.doc_number = SAFE_CAST(SUBSTR(LPAD(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(f.inbound_awb_code,'-',''),' ',''),'"',''),11,'0'),4,8) AS NUMERIC);


DROP TABLE IF EXISTS `bc-te-dlake-dev-s7b3.cln_etracking_us.tmp_purchase_order`;

CREATE TABLE if not exists `bc-te-dlake-dev-s7b3.cln_etracking_us.etracking_timezone`
as SELECT  'SSC'               master_process_code, 
           'PURCHASE ORDER'    process_code, 
            current_datetime ("America/Santiago") chilean_update_dt,
            current_datetime ("America/Sao_Paulo") brazilian_update_dt,
            current_datetime ("America/New_York") american_update_dt, 
            current_datetime ("Europe/Madrid") european_update_dt,
            current_datetime ("America/Lima") peruvian_update_dt;

DELETE `bc-te-dlake-dev-s7b3.cln_etracking_us.etracking_timezone` 
WHERE master_process_code = 'SSC' 
AND   process_code        = 'PURCHASE ORDER';

INSERT INTO `bc-te-dlake-dev-s7b3.cln_etracking_us.etracking_timezone` values 
('SSC', 
 'PURCHASE ORDER', 
 current_datetime ("America/Santiago"),  -- santiago
 current_datetime ("America/Sao_Paulo"), -- sAo paulo
 current_datetime ("America/New_York"),  -- miami
 current_datetime ("Europe/Madrid"),     -- madrid
 current_datetime ("America/Lima")       -- lima            
);


END;