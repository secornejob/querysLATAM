CREATE OR REPLACE PROCEDURE `bc-te-dlake-dev-s7b3.cln_etracking_us.etracking_exchange`()
BEGIN

CREATE OR REPLACE TABLE `bc-te-dlake-dev-s7b3.cln_etracking_us.tmp_exch_ship_in` AS
WITH 

IN_TRANS AS (
    SELECT *
        ,ROW_NUMBER() OVER (partition by EVENT_ID ORDER BY STAGE_GDT DESC) as RN
    FROM `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.evt_stage`
    WHERE EVENT_STATUS_CD ='IXINTR'
)

,IN_TRANS_F AS (
    SELECT EVENT_DB_ID,
        EVENT_ID,
        CAST(STAGE_GDT AS DATETIME) AS IN_TRANSIT_DT
    FROM IN_TRANS
    WHERE RN = 1
)

,RECEIVE AS (
    SELECT *, 
        ROW_NUMBER() OVER (partition by EVENT_ID ORDER BY STAGE_GDT DESC) as RN
    FROM `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.evt_stage`
    WHERE EVENT_STATUS_CD ='IXCMPLT'
)

,RECEIVE_F AS (
    SELECT EVENT_DB_ID,
        EVENT_ID,
        CAST(STAGE_GDT AS DATETIME) AS RECEIVE_DT
    FROM RECEIVE
    WHERE RN = 1
)

,SHIPMENT_INBOUND AS (
    SELECT 
        EVT_P.EVENT_SDESC AS po_number, 
        EVT_S.EVENT_SDESC AS shipment_sdesc,
        SS.SHIPMENT_TYPE_CD,
        PH.PO_ID, 
        PH.PO_DB_ID,
        SS.SHIPMENT_ID,
        SS.SHIPMENT_DB_ID,
        SSL.INV_NO_ID, 
        SSL.PART_NO_ID, 
        SSL.SERIAL_NO_OEM, 
        INV.BARCODE_SDESC AS INDIA_BARCODE_INBOUND,
        INV.INV_COND_CD,
        SS.WAYBILL_SDESC AS AWB_INBOUND,
        SS.CUSTOMS_SDESC,
        EVT_S.EVENT_STATUS_CD AS SHIPMENT_INBOUND_STATUS, 
        SS.CREATION_DT AS SHIPMENT_INBOUND_CREATION,
        REC.RECEIVE_DT AS SHIPMENT_INBOUND_COMPLETE,
        ROW_NUMBER() OVER (partition by EVT_P.EVENT_SDESC ORDER BY SS.CREATION_DT DESC) as RN --UNA POR PO
    FROM `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.po_header` AS PH 
    LEFT JOIN `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.evt_event` AS EVT_P 
        ON  PH.PO_ID = EVT_P.EVENT_ID
        AND PH.PO_DB_ID = EVT_P.EVENT_DB_ID
    LEFT JOIN `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.ship_shipment` AS SS
        ON  PH.PO_ID = SS.PO_ID
        AND PH.PO_DB_ID = SS.PO_DB_ID
    LEFT JOIN `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.evt_event` AS EVT_S 
        ON  SS.SHIPMENT_ID = EVT_S.EVENT_ID
        AND SS.SHIPMENT_DB_ID = EVT_S.EVENT_DB_ID
    LEFT JOIN `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.ship_shipment_line` AS SSL
        ON  SS.SHIPMENT_ID = SSL.SHIPMENT_ID
        AND SS.SHIPMENT_DB_ID = SSL.SHIPMENT_DB_ID
    LEFT JOIN RECEIVE_F AS REC 
        ON SS.SHIPMENT_ID = REC.EVENT_ID
        AND SS.SHIPMENT_DB_ID =REC.EVENT_DB_ID
    LEFT JOIN `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.inv_inv` AS INV
        ON  SSL.INV_NO_ID = INV.INV_NO_ID
        AND SSL.INV_NO_DB_ID = INV.INV_NO_DB_ID
    WHERE 
            PH.PO_TYPE_CD = 'EXCHANGE'
        AND EVT_P.EVENT_STATUS_CD <> 'POCANCEL'
        AND SS.SHIPMENT_TYPE_CD = 'PURCHASE'
        AND EVT_S.EVENT_STATUS_CD <> 'IXCANCEL'
)

    SELECT * 
    FROM SHIPMENT_INBOUND
    WHERE RN = 1
;

CREATE OR REPLACE TABLE `bc-te-dlake-dev-s7b3.cln_etracking_us.tmp_exch_ship_out` AS
WITH 

IN_TRANS AS (
    SELECT *
        ,ROW_NUMBER() OVER (partition by EVENT_ID ORDER BY STAGE_GDT DESC) as RN
    FROM `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.evt_stage`
    WHERE EVENT_STATUS_CD ='IXINTR'
)

,IN_TRANS_F AS (
    SELECT EVENT_DB_ID,
        EVENT_ID,
        CAST(STAGE_GDT AS DATETIME) AS IN_TRANSIT_DT
    FROM IN_TRANS
    WHERE RN = 1
)

,RECEIVE AS (
    SELECT *, 
        ROW_NUMBER() OVER (partition by EVENT_ID ORDER BY STAGE_GDT DESC) as RN
    FROM `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.evt_stage`
    WHERE EVENT_STATUS_CD ='IXCMPLT'
)

,RECEIVE_F AS (
    SELECT EVENT_DB_ID,
        EVENT_ID,
        CAST(STAGE_GDT AS DATETIME) AS RECEIVE_DT
    FROM RECEIVE
    WHERE RN = 1
)

,SHIPMENT_OUTBOUND AS (
    SELECT 
        EVT_P.EVENT_SDESC AS po_number, 
        EVT_S.EVENT_SDESC AS shipment_sdesc,
        SS.SHIPMENT_TYPE_CD,
        PH.PO_ID, 
        PH.PO_DB_ID,
        SS.SHIPMENT_ID,
        SS.SHIPMENT_DB_ID,
        SSL.INV_NO_ID, 
        SSL.PART_NO_ID, 
        SSL.SERIAL_NO_OEM, 
        INV.BARCODE_SDESC AS INDIA_BARCODE_OUTBOUND,        
        INV.INV_COND_CD,
        SS.WAYBILL_SDESC AS AWB_OUTBOUND, 
        EVT_S.EVENT_STATUS_CD AS SHIPMENT_OUTBOUND_STATUS, 
        SS.CREATION_DT AS SHIPMENT_OUTBOUND_CREATION,
        INT.IN_TRANSIT_DT  AS SHIPMENT_OUTBOUND_IN_TRANSIT, 
        REC.RECEIVE_DT AS SHIPMENT_OUTBOUND_COMPLETE,

        ROW_NUMBER() OVER (partition by EVT_P.EVENT_SDESC ORDER BY SS.CREATION_DT DESC) as RN --UNA POR PO
    FROM `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.po_header` AS PH 
    LEFT JOIN `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.evt_event` AS EVT_P 
        ON  PH.PO_ID = EVT_P.EVENT_ID
        AND PH.PO_DB_ID = EVT_P.EVENT_DB_ID
    LEFT JOIN `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.ship_shipment` AS SS
        ON  PH.PO_ID = SS.PO_ID
        AND PH.PO_DB_ID = SS.PO_DB_ID
    LEFT JOIN `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.evt_event` AS EVT_S 
        ON  SS.SHIPMENT_ID = EVT_S.EVENT_ID
        AND SS.SHIPMENT_DB_ID = EVT_S.EVENT_DB_ID
    LEFT JOIN `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.ship_shipment_line` AS SSL
        ON  SS.SHIPMENT_ID = SSL.SHIPMENT_ID
        AND SS.SHIPMENT_DB_ID = SSL.SHIPMENT_DB_ID
    LEFT JOIN RECEIVE_F AS REC 
        ON SS.SHIPMENT_ID = REC.EVENT_ID
        AND SS.SHIPMENT_DB_ID =REC.EVENT_DB_ID
    LEFT JOIN IN_TRANS_F  AS INT 
        ON  SS.SHIPMENT_ID = INT.EVENT_ID
        AND SS.SHIPMENT_DB_ID =INT.EVENT_DB_ID
    LEFT JOIN `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.inv_inv` AS INV
        ON  SSL.INV_NO_ID = INV.INV_NO_ID
        AND SSL.INV_NO_DB_ID = INV.INV_NO_DB_ID
    WHERE 
            PH.PO_TYPE_CD = 'EXCHANGE'
        AND EVT_P.EVENT_STATUS_CD <> 'POCANCEL'
        AND SS.SHIPMENT_TYPE_CD = 'SENDXCHG'
        AND EVT_S.EVENT_STATUS_CD <> 'IXCANCEL'
)

    SELECT * 
    FROM SHIPMENT_OUTBOUND
    WHERE RN = 1
;

CREATE OR REPLACE TABLE `bc-te-dlake-dev-s7b3.cln_etracking_us.tmp_exchange` AS

WITH 

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
        STAGE_DT AS FIRST_AUTH_DT
    FROM
        (SELECT 
            *, 
            ROW_NUMBER() OVER (partition by EVENT_ID ORDER BY STAGE_GDT ASC) as RN
        FROM `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.evt_stage`
        WHERE
            EVENT_STATUS_CD = 'POAUTH')
        WHERE RN = 1
)

,ISSUE AS(
    SELECT 
        *, 
        ROW_NUMBER() OVER (partition by EVENT_ID ORDER BY STAGE_GDT ASC) as RN
    FROM `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.evt_stage`
    WHERE 
        EVENT_STATUS_CD ='POISSUED'
)

,ISSUE_F AS (
    SELECT 
        EVENT_DB_ID,
        EVENT_ID,
        CAST(STAGE_GDT AS TIMESTAMP) AS PO_FIRST_ISSUE_DT
    FROM ISSUE
    WHERE 
        RN=1)

,PO_LINE AS (
    SELECT 
        PH.PO_ID, 
        PH.PO_DB_ID, 
        PL.PART_NO_ID,
        PL.PART_NO_DB_ID,
        PL.RECEIVED_DT, 
        ROW_NUMBER() OVER (partition by PH.PO_ID ORDER BY PL.CREATION_DT DESC) as RN
    FROM `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.po_header` AS PH
    LEFT JOIN `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.po_line` AS PL
        ON  PH.PO_ID = PL.PO_ID 
        AND PH.PO_DB_ID = PL.PO_DB_ID
    WHERE 
            PH.PO_TYPE_CD = 'EXCHANGE'
        AND PL.PO_LINE_TYPE_CD <> 'MISC'
        AND PL.DELETED_BOOL = 0
)

,PO_LINE_F AS (
    SELECT * FROM  PO_LINE 
    WHERE RN = 1
)

,ROUTING AS (
    SELECT 
        SHIP.PO_ID,
        SHIP.PO_DB_ID,
        SHIP.SHIPMENT_ID,
        SHIP.SHIPMENT_DB_ID,
        (CASE
            WHEN PH.RE_SHIP_TO_ID IS NOT NULL THEN SHIP_TO_LOC_ID
            ELSE NULL
        END) AS HUB,
        SSM.SEGMENT_ID,
        SSM.SEGMENT_DB_ID,
        SSM.SEGMENT_ORD
        --FROM SHIPMENT_INBOUND_F AS SIN
        FROM 
                (   
                SELECT PO_ID,PO_DB_ID,SHIPMENT_ID,SHIPMENT_DB_ID FROM `bc-te-dlake-dev-s7b3.cln_etracking_us.tmp_exch_ship_in`
                UNION DISTINCT 
                SELECT PO_ID,PO_DB_ID,SHIPMENT_ID,SHIPMENT_DB_ID FROM `bc-te-dlake-dev-s7b3.cln_etracking_us.tmp_exch_ship_out`
                ) AS SHIP
        LEFT JOIN `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.po_header` AS PH
            ON  SHIP.po_id = PH.PO_ID
            AND SHIP.po_db_id = PH.PO_DB_ID 
        LEFT JOIN `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.ship_segment_map` AS SSM
            ON 	SHIP.SHIPMENT_ID = SSM.SHIPMENT_ID 
            AND SHIP.SHIPMENT_DB_ID = SSM.SHIPMENT_DB_ID
)

,R_LLEGADA AS (
    SELECT
        DISTINCT 
        R.PO_ID,
        R.PO_DB_ID,
        R.SHIPMENT_ID,
        R.SHIPMENT_DB_ID,
        R.HUB,
        SM_D.SEGMENT_STATUS_CD AS LLEGADA_STATUS,
        CAST(SM_D.COMPLETE_DT AS TIMESTAMP) AS LLEGADA_COMPLETE_HUB_DT,
        ROW_NUMBER() OVER (partition by R.SHIPMENT_ID ORDER BY R.SEGMENT_ORD ASC) as RN  
    FROM ROUTING AS R
        INNER JOIN `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.ship_segment` AS SM_D 
            ON  R.SEGMENT_ID = SM_D.SEGMENT_ID 
            AND R.SEGMENT_DB_ID = SM_D.SEGMENT_DB_ID 
            AND R.HUB = SM_D.SHIP_TO_ID
    WHERE SM_D.SEGMENT_STATUS_CD <> 'CANCEL'
)

,R_SALIDA AS (
    SELECT
        DISTINCT 
        R.PO_ID,
        R.PO_DB_ID,
        R.SHIPMENT_ID,
        R.SHIPMENT_DB_ID,
        R.HUB,
        SM_O.SEGMENT_STATUS_CD AS SALIDA_STATUS,
        CAST(SM_O.COMPLETE_DT AS TIMESTAMP) AS SALIDA_COMPLETE_HUB_DT,
        ROW_NUMBER() OVER (partition by R.SHIPMENT_ID ORDER BY R.SEGMENT_ORD DESC) as RN  
    FROM ROUTING AS R
        INNER JOIN `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.ship_segment` AS SM_O 
            ON  R.SEGMENT_ID = SM_O.SEGMENT_ID 
            AND R.SEGMENT_DB_ID = SM_O.SEGMENT_DB_ID 
            AND R.HUB = SM_O.SHIP_FROM_ID
    WHERE SM_O.SEGMENT_STATUS_CD <> 'CANCEL'
	)

,R_SALIDA_F AS (
    SELECT *
    FROM R_SALIDA 
    WHERE RN = 1
)

,R_LLEGADA_F AS (
    SELECT *
    FROM R_LLEGADA 
    WHERE RN = 1
)
--VER LÓGICA DE QUAR EN PO

,QUAR AS (
    SELECT 
        INV.INV_NO_ID, 
        INV.INV_NO_DB_ID, 
        INV.BARCODE_SDESC, 
        PH.PO_ID,
        PH.PO_DB_ID,
        PH.CREATION_DT,            
        ROW_NUMBER() OVER (partition by INV.INV_NO_ID ORDER BY PH.CREATION_DT DESC) as RN  
    FROM `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.inv_inv` as INV
    LEFT JOIN `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.ship_shipment_line` AS SSL
        ON  INV.INV_NO_ID = SSL.INV_NO_ID 
        AND INV.INV_NO_DB_ID = SSL.INV_NO_DB_ID
    LEFT JOIN  `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.po_header` AS PH
        ON  SSL.PO_ID = PH.PO_ID 
        AND SSL.PO_DB_ID = PH.PO_DB_ID
    LEFT JOIN `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.evt_event` AS EVT_P 
        ON  PH.PO_ID = EVT_P.EVENT_ID
        AND PH.PO_DB_ID = EVT_P.EVENT_DB_ID
    WHERE 
            INV_COND_CD = 'QUAR' 
        AND EVT_P.EVENT_STATUS_CD <> 'POCANCEL'
)
    
,QUAR_F AS (
        SELECT  *
        FROM QUAR 
        WHERE RN =1
)

,RFI AS (
    SELECT EVT_INV.INV_NO_ID,
        EVT_INV.INV_NO_DB_ID,
        EVT.EVENT_ID,
        CAST(EVT.EVENT_GDT AS DATETIME) AS RFI_DT,
        ROW_NUMBER() OVER (partition by EVT_INV.INV_NO_ID, EVT.EVENT_ID ORDER BY EVT.EVENT_GDT DESC) as RN
    FROM `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.evt_inv` AS EVT_INV
    LEFT JOIN (
        SELECT EVENT_ID, 
            EVENT_GDT 
        FROM `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.evt_event`
        WHERE EVENT_STATUS_CD = 'ACRFI'
            OR EVENT_STATUS_CD = 'ICRRFI'
    ) AS EVT ON EVT_INV.EVENT_ID = EVT.EVENT_ID
)
,RFI_1 AS (
    SELECT 
        INV_NO_ID,
        INV_NO_DB_ID,
        EVENT_ID,
        RFI_DT
    FROM RFI
    WHERE RN = 1
),

PIVOTE_RFI AS (
SELECT  
        SI.PO_ID, 
        SI.PO_DB_ID,
        SI.SHIPMENT_ID,
        SI.INV_NO_ID,
        SI.SHIPMENT_INBOUND_COMPLETE,
        RFI.RFI_DT,
ROW_NUMBER() OVER (PARTITION BY SI.SHIPMENT_ID ORDER BY RFI.RFI_DT ASC) AS RN 
FROM `bc-te-dlake-dev-s7b3.cln_etracking_us.tmp_exch_ship_in` AS SI
LEFT JOIN RFI_1 AS RFI 
    ON SI.INV_NO_ID = RFI.INV_NO_ID
WHERE SI.SHIPMENT_INBOUND_COMPLETE <= RFI.RFI_DT),

PIVOTE_RFI_F AS(
    SELECT * FROM PIVOTE_RFI WHERE RN = 1
)

,FINAL AS(
    SELECT
        ROW_NUMBER() OVER (partition by PH.PO_ID ORDER BY PH.CREATION_DT DESC) as RN  ,
        EVT_P.EVENT_SDESC AS PO_NUMBER,
        EVT_P.EVENT_STATUS_CD,
        (CASE 
            WHEN (PH.PO_ID IS NOT NULL AND EVT_P.EVENT_STATUS_CD = 'POCANCEL' ) THEN 'PO CANCEL'
            WHEN (PH.PO_ID IS NOT NULL AND EVT_P.EVENT_STATUS_CD = 'POOPEN' AND PH.AUTH_STATUS_CD <> 'REQUESTED') THEN 'PO OPEN'
            WHEN (PH.PO_ID IS NOT NULL AND EVT_P.EVENT_STATUS_CD = 'POOPEN' AND PH.AUTH_STATUS_CD = 'REQUESTED') THEN 'PO OPEN - REQUESTED'
            WHEN (PH.PO_ID IS NOT NULL AND EVT_P.EVENT_STATUS_CD = 'POAUTH' ) THEN 'PO AUTORIZADA'
            WHEN (PH.PO_ID IS NOT NULL AND EVT_S.EVENT_STATUS_CD = 'IXPEND' AND SIN.CUSTOMS_SDESC IS NOT NULL) then 'PRE RECEPCION'
            WHEN (PH.PO_ID IS NOT NULL AND (EVT_P.EVENT_STATUS_CD = 'POISSUED' OR EVT_P.EVENT_STATUS_CD = 'POACKNOWLEDGED') AND (R_LLEGADA_F_IN.LLEGADA_STATUS <> 'CMPLT' OR R_LLEGADA_F_IN.LLEGADA_STATUS IS NULL)) THEN 'PO ISSUED'
            WHEN (PH.PO_ID IS NOT NULL AND (EVT_P.EVENT_STATUS_CD = 'POISSUED' OR EVT_P.EVENT_STATUS_CD = 'POACKNOWLEDGED') AND R_LLEGADA_F_IN.LLEGADA_STATUS = 'CMPLT' AND R_SALIDA_F_IN.SALIDA_STATUS = 'PEND') THEN 'PO IN HUB'
            --CORREGIR EL CASO DE HUB TO DESTINO EN PO
            WHEN (PH.PO_ID IS NOT NULL AND (EVT_P.EVENT_STATUS_CD = 'POISSUED' OR EVT_P.EVENT_STATUS_CD = 'POACKNOWLEDGED') AND R_LLEGADA_F_IN.LLEGADA_STATUS = 'CMPLT' AND R_SALIDA_F_IN.SALIDA_STATUS = 'INTR') THEN 'PO HUB HACIA DESTINO'
            WHEN (PH.PO_ID IS NOT NULL AND (EVT_P.EVENT_STATUS_CD = 'PORECEIVED' OR EVT_P.EVENT_STATUS_CD = 'POCLOSED') AND SIN.SHIPMENT_INBOUND_COMPLETE IS NOT NULL AND PIVOTE_RFI_F.RFI_DT IS NULL) THEN 'PO RECEIVED'
            WHEN (PH.PO_ID IS NOT NULL AND EVT_P.EVENT_STATUS_CD = 'POPARTIAL') THEN 'PO PARTIAL'
            WHEN ((PH.PO_ID IS NOT NULL) AND (QUAR_F.INV_NO_ID IS NOT NULL)) THEN 'PO QUAR'
            WHEN (PH.PO_ID IS NOT NULL AND (EVT_P.EVENT_STATUS_CD = 'PORECEIVED' OR EVT_P.EVENT_STATUS_CD = 'POCLOSED') AND PIVOTE_RFI_F.RFI_DT IS NOT NULL) THEN 'RFI'
            WHEN (PH.PO_ID IS NOT NULL AND (EVT_P.EVENT_STATUS_CD = 'PORECEIVED' OR EVT_P.EVENT_STATUS_CD = 'POCLOSED') AND (SIN.SHIPMENT_ID IS  NULL AND SOUT.SHIPMENT_ID IS NULL)) THEN 'MRO REGULARIZACION'
            WHEN ((PH.PO_ID IS NOT NULL) AND (EVT_P.EVENT_STATUS_CD = 'POISSUED') AND (SIN.SHIPMENT_ID IS NOT NULL)) THEN 'ORDEN PENDIENTE REGULARIZACION'
            ELSE 'OTRO' END)         
        AS INBOUND_STATUS,
        (CASE 
            WHEN (PH.PO_ID IS NOT NULL AND EVT_P.EVENT_STATUS_CD = 'POCANCEL' ) THEN 'PO CANCEL'
            WHEN (PH.PO_ID IS NOT NULL AND SOUT.SHIPMENT_OUTBOUND_STATUS = 'IXPEND' AND R_LLEGADA_F_OUT.LLEGADA_STATUS <> 'CMPLT' ) THEN 'PENDIENTE'
            WHEN (PH.PO_ID IS NOT NULL AND SOUT.SHIPMENT_OUTBOUND_STATUS = 'IXINTR' ) THEN 'EN TRANSITO'

            WHEN (PH.PO_ID IS NOT NULL AND R_LLEGADA_F_OUT.LLEGADA_STATUS = 'CMPLT' AND R_SALIDA_F_OUT.SALIDA_STATUS = 'PEND') THEN 'EN HUB'
            WHEN (PH.PO_ID IS NOT NULL AND R_LLEGADA_F_OUT.LLEGADA_STATUS = 'CMPLT' AND R_SALIDA_F_OUT.SALIDA_STATUS = 'INTR') THEN 'EN TRANSIT DESDE HUB HACIA DESTINO'

            WHEN (PH.PO_ID IS NOT NULL AND SOUT.SHIPMENT_OUTBOUND_STATUS = 'IXCMPLT') THEN 'COMPLETED'  
            ELSE 'OTRO' END)     
        AS OUTBOUND_STATUS,
        SIN.shipment_sdesc AS INBOUND_SHIPMENT,
        SIN.SHIPMENT_INBOUND_STATUS,
        SIN.SHIPMENT_INBOUND_COMPLETE,
        SIN.SERIAL_NO_OEM AS IN_SERIAL_NO_OEM,
        SIN.INDIA_BARCODE_INBOUND,
        SIN.shipment_type_cd AS IN_shipment_type_cd,

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

        SOUT.shipment_sdesc AS OUTBOUND_SHIPMENT,
        SOUT.SHIPMENT_OUTBOUND_STATUS,
        SOUT.SHIPMENT_OUTBOUND_COMPLETE,
        SOUT.SERIAL_NO_OEM AS OUT_SERIAL_NO_OEM,
        SOUT.INDIA_BARCODE_OUTBOUND,
        SOUT.shipment_type_cd AS OUT_shipment_type_cd,

        R_LLEGADA_F_IN.LLEGADA_STATUS AS IN_HUB_LLEGADA_STATUS,
        R_LLEGADA_F_IN.LLEGADA_COMPLETE_HUB_DT AS IN_HUB_LLEGADA_COMPLETE_DT,
        R_SALIDA_F_IN.SALIDA_STATUS AS IN_HUB_SALIDA_STATUS ,
        R_SALIDA_F_IN.SALIDA_COMPLETE_HUB_DT AS IN_HUB_SALIDA_COMPLETE_DT,

        R_LLEGADA_F_OUT.LLEGADA_STATUS AS OUT_HUB_LLEGADA_STATUS,
        R_LLEGADA_F_OUT.LLEGADA_COMPLETE_HUB_DT AS OUT_HUB_LLEGADA_COMPLETE_DT,
        R_SALIDA_F_OUT.SALIDA_STATUS AS OUT_HUB_SALIDA_STATUS ,
        R_SALIDA_F_OUT.SALIDA_COMPLETE_HUB_DT AS OUT_HUB_SALIDA_COMPLETE_DT,




        OV.VENDOR_CD,
        PH.AUTH_STATUS_CD,
        PH.RE_SHIP_TO_ID,
        OV.VENDOR_NAME,

        PA.PO_AUTH_LVL_CD,
        PA.AOG_OVERRIDE_BOOL,
        PH.REQ_PRIORITY_CD,
        PH.CURRENCY_CD,
        EVP.TOTAL_PRICE_QT , 
        PH.TRANSPORT_TYPE_CD transport_type_code,
        EQP.PART_NO_OEM part_oem_code,
        EQP.PART_NO_SDESC part_oem_desc,
        EQS.STOCK_NO_NAME stock_name,
        EQS.STOCK_NO_CD stock_code,
        HR.HR_CD,
        SIN.AWB_INBOUND,
        SOUT.AWB_OUTBOUND,
--      FECHAS 
        PIVOTE_RFI_F.RFI_DT AS RECEIVED_DT,
        CAST(PH.CREATION_DT AS datetime) AS po_creation_dt,
        CAST(EVP.NEEDED_BY_DT AS datetime) AS po_needed_by_dt,
        CAST(EVP.PROMISED_BY_DT AS datetime) AS po_promesed_by_dt,
        CAST(ISSUE.po_first_issue_dt AS datetime) AS po_first_issue_dt,
        CAST(PH.ISSUED_DT AS datetime) AS po_last_issue_dt,
        CAST(PH.closed_dt AS datetime) AS po_closed_dt,
        CAST(FA.FIRST_AUTH_DT AS datetime) po_first_auth_dt,
--      REMOCION Y TRANSFER                                                                                                                                                                                                                  
        RT.INVENTORY_BARCODE,                                                                                                                                                                                                                
        RT.PO_BARCODE_NUMBER,                                                                                                                                                                                                                
        RT.CREATION_SCHEDULE_DT,                                                                                                                                                                                                             
        RT.PO_CODE,                                                                                                                                                                                                                          
        RT.STAGE_DATE,                                                                                                                                                                                                                       
        RT.PROCESS_TYPE,                                                                                                                                                                                                                     
        RT.REMOVAL_DT,                                                                                                                                                                                                                       
        RT.CATEGORY_REMOVAL_VAL,                                                                                                                                                                                                             
        RT.WORK_PACKAGE_LOCATION_CODE,                                                                                                                                                                                                       
        RT.TASK_BARCODE,                                                                                                                                                                                                                     
        RT.TRANSFER_BARCODE,                                                                                                                                                                                                                 
        RT.TRANSFER_TYPE,                                                                                                                                                                                                                    
        RT.TRANSFER_CREATION_DT,                                                                                                                                                                                                             
        RT.TRANSFER_COMPLETE_DT,                                                                                                                                                                                                             
        RT.TRANSFER_STATUS,                                                                                                                                                                                                                  
        RT.TRANSFER_LOCATION_FROM_CODE,                                                                                                                                                                                                      
        RT.TRANSFER_LOCATION_FROM_TYPE,                                                                                                                                                                                                      
        RT.TRANSFER_LOCATION_TO_CODE,                                                                                                                                                                                                        
        RT.TRANSFER_LOCATION_TO_TYPE                                                                                                                                                                                                         

    FROM `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.po_header` AS PH
    LEFT JOIN `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.evt_event` AS EVT_P 
        ON  PH.PO_ID = EVT_P.EVENT_ID
        AND PH.PO_DB_ID = EVT_P.EVENT_DB_ID
    LEFT JOIN `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.VW_EVT_PO` AS EVP
        ON  PH.PO_ID = EVP.PO_ID AND
            PH.PO_DB_ID = EVP.PO_DB_ID
    LEFT JOIN `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.org_vendor` AS OV
        ON  PH.VENDOR_ID =   OV.VENDOR_ID AND        
            PH.VENDOR_DB_ID = OV.VENDOR_DB_ID
    LEFT JOIN PO_LINE_F AS PL 
        ON  PH.PO_ID = PL.PO_ID 
        AND PH.PO_DB_ID = PL.PO_DB_ID 
    LEFT JOIN `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.eqp_part_no`AS EQP 
        ON  PL.PART_NO_ID =   EQP.PART_NO_ID AND        
            PL.PART_NO_DB_ID = EQP.PART_NO_DB_ID
    LEFT JOIN `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.eqp_stock_no` AS EQS
        ON  EQP.STOCK_NO_ID = EQS.STOCK_NO_ID AND        
            EQP.STOCK_NO_DB_ID = EQS.STOCK_NO_DB_ID
    LEFT JOIN `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.org_hr` AS HR
        ON  PH.CONTACT_HR_ID = HR.HR_ID AND
            PH.CONTACT_HR_DB_ID = HR.HR_DB_ID
    LEFT JOIN APROBACION_F AS PA
        ON  PH.PO_ID = PA.PO_ID AND
            PH.PO_DB_ID = PA.PO_DB_ID
    LEFT JOIN PRIMERA_APROBACION AS FA
        ON  PH.PO_ID = FA.EVENT_ID AND
            PH.PO_DB_ID = FA.EVENT_DB_ID
    LEFT JOIN ISSUE_F AS ISSUE
        ON  PH.PO_ID = ISSUE.EVENT_ID AND
            PH.PO_DB_ID = ISSUE.EVENT_DB_ID
    LEFT JOIN `bc-te-dlake-dev-s7b3.cln_etracking_us.tmp_exch_ship_in` AS SIN   
        ON  PH.po_id = SIN.PO_ID
        AND PH.po_db_id = SIN.PO_DB_ID
    LEFT JOIN `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.evt_event` AS EVT_S
        ON  EVT_S.EVENT_ID     = SIN.SHIPMENT_ID
        AND EVT_S.EVENT_DB_ID  = SIN.SHIPMENT_DB_ID
    LEFT JOIN `bc-te-dlake-dev-s7b3.cln_etracking_us.tmp_exch_ship_out` AS SOUT   
        ON  PH.po_id = SOUT.PO_ID
        AND PH.po_db_id = SOUT.PO_DB_ID
    LEFT JOIN R_LLEGADA_F AS R_LLEGADA_F_IN
        ON  PH.po_id = R_LLEGADA_F_IN.PO_ID
        AND PH.po_db_id = R_LLEGADA_F_IN.PO_DB_ID
        AND SIN.SHIPMENT_ID = R_LLEGADA_F_IN.SHIPMENT_ID
        AND SIN.SHIPMENT_DB_ID = R_LLEGADA_F_IN.SHIPMENT_DB_ID
    LEFT JOIN R_SALIDA_F AS R_SALIDA_F_IN 
        ON  PH.po_id = R_SALIDA_F_IN.PO_ID
        AND PH.po_db_id = R_SALIDA_F_IN.PO_DB_ID
        AND SIN.SHIPMENT_ID = R_SALIDA_F_IN.SHIPMENT_ID
        AND SIN.SHIPMENT_DB_ID = R_SALIDA_F_IN.SHIPMENT_DB_ID

    LEFT JOIN R_LLEGADA_F AS R_LLEGADA_F_OUT
        ON  PH.po_id = R_LLEGADA_F_OUT.PO_ID
        AND PH.po_db_id = R_LLEGADA_F_OUT.PO_DB_ID
        AND SOUT.SHIPMENT_ID = R_LLEGADA_F_OUT.SHIPMENT_ID
        AND SOUT.SHIPMENT_DB_ID = R_LLEGADA_F_OUT.SHIPMENT_DB_ID

    LEFT JOIN R_SALIDA_F AS R_SALIDA_F_OUT 
        ON  PH.po_id = R_SALIDA_F_OUT.PO_ID
        AND PH.po_db_id = R_SALIDA_F_OUT.PO_DB_ID
        AND SOUT.SHIPMENT_ID = R_SALIDA_F_OUT.SHIPMENT_ID
        AND SOUT.SHIPMENT_DB_ID = R_SALIDA_F_OUT.SHIPMENT_DB_ID
    LEFT JOIN QUAR_F 
        ON  PH.po_id = QUAR_F.PO_ID
        AND PH.po_db_id = QUAR_F.PO_DB_ID
    LEFT JOIN PIVOTE_RFI_F
        ON  PH.po_id = PIVOTE_RFI_F.PO_ID
        AND PH.po_db_id = PIVOTE_RFI_F.PO_DB_ID
    LEFT JOIN `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.inv_loc` INV_LOC
        ON  PH.SHIP_TO_LOC_ID = INV_LOC.LOC_ID AND
            PH.SHIP_TO_LOC_DB_ID = INV_LOC.LOC_DB_ID
    LEFT JOIN `bc-te-dlake-dev-s7b3.stg_ora_exmro_mro_us.inv_loc` INV_LOC2
        ON  PH.RE_SHIP_TO_ID = INV_LOC2.LOC_ID AND
            PH.SHIP_TO_LOC_DB_ID = INV_LOC2.LOC_DB_ID
    LEFT JOIN `bc-te-dlake-dev-s7b3.cln_etracking_us.removal_transfer` RT                                                                                                                                                                
        ON  RT.PO_CODE = EVT_P.EVENT_SDESC                                                                                                                                                                                                   

    WHERE 
        PH.PO_TYPE_CD = 'EXCHANGE'
)


SELECT * FROM FINAL;
-- WHERE po_creation_dt > '2020-01-01' 

DROP TABLE IF EXISTS `bc-te-dlake-dev-s7b3.cln_etracking_us.tmp_exch_ship_in`;

DROP TABLE IF EXISTS `bc-te-dlake-dev-s7b3.cln_etracking_us.tmp_exch_ship_out`;


CREATE OR REPLACE TABLE `bc-te-dlake-dev-s7b3.cln_etracking_us.exchange` AS

WITH tmp_exchange_base AS (
SELECT 
       PO_NUMBER                       AS po_code,
       EVENT_STATUS_CD                 AS po_global_status,
       --
       OUTBOUND_STATUS                 AS exchange_outbound_status_code,
       OUTBOUND_SHIPMENT               AS shipment_outbound_code,
       SHIPMENT_OUTBOUND_STATUS        AS shipment_outbound_status_code,
       --
       INBOUND_STATUS                  AS exchange_inbound_status_code,
       INBOUND_SHIPMENT                AS shipment_inbound_code,
       SHIPMENT_INBOUND_STATUS         AS shipment_inbound_status_code,
       --
       IN_SERIAL_NO_OEM                AS inbound_serial_oem_code,
       INDIA_BARCODE_INBOUND           AS inbound_barcode,    
       IN_shipment_type_cd             AS inbound_shipment_type_code,
       OUT_SERIAL_NO_OEM               AS outbound_serial_oem_code,
       INDIA_BARCODE_OUTBOUND          AS outbound_barcode,
       OUT_shipment_type_cd            AS outbound_shipment_type_code,
       --
       VENDOR_CD                       AS vendor_code,
       AUTH_STATUS_CD                  AS auth_code,
     --RE_SHIP_TO_ID                   AS ship_to_location_code,    
       ship_to_location_code, 
       VENDOR_NAME                     AS vendor_name,
       PO_AUTH_LVL_CD                  AS auth_level_code,
       AOG_OVERRIDE_BOOL               AS aog_override_flag,
       REQ_PRIORITY_CD                 AS req_priority_code,
       CURRENCY_CD                     AS currency_code,
       TOTAL_PRICE_QT                  AS total_price_amt,
       transport_type_code             AS transport_type_code,
       part_oem_code                   AS part_oem_code,
       part_oem_desc                   AS part_oem_desc,
       stock_name                      AS stock_name,
       stock_code                      AS stock_code,
       HR_CD                           AS po_bp_owner_code,
       --
     --current_datetime(null)          AS REMOVAL_DT,
       current_datetime(null)          AS TURN_IN_DT,
       current_datetime(null)          AS DELIVERY_DISPATCH_DT,
       --
       OUT_HUB_LLEGADA_STATUS          AS outbound_complete_hub_arrival_status_code, 
       CAST(OUT_HUB_LLEGADA_COMPLETE_DT AS DATETIME) AS outbound_complete_hub_arrival_dt,
       OUT_HUB_SALIDA_STATUS           AS outbound_complete_hub_departure_status_code,
       CAST(OUT_HUB_SALIDA_COMPLETE_DT AS DATETIME)  AS outbound_complete_hub_departure_dt, 
       SHIPMENT_OUTBOUND_COMPLETE      AS shipment_outbound_complete_dt,
       --
       po_creation_dt                  AS po_creation_dt,
       po_first_auth_dt                AS po_first_auth_dt,
       po_first_issue_dt               AS po_first_issue_dt,
       RECEIVED_DT                     AS exchange_received_dt,
       --
       IN_HUB_LLEGADA_STATUS           AS inbound_complete_hub_arrival_status_code,
       CAST(IN_HUB_LLEGADA_COMPLETE_DT AS DATETIME)  AS inbound_complete_hub_arrival_dt,
       IN_HUB_SALIDA_STATUS            AS inbound_complete_hub_departure_status_code,
       CAST(IN_HUB_SALIDA_COMPLETE_DT  AS DATETIME)  AS inbound_complete_hub_departure_dt,
       SHIPMENT_INBOUND_COMPLETE       AS shipment_inbound_complete_dt,
       --
       po_last_issue_dt                AS po_last_issue_dt,
       po_closed_dt                    AS po_closed_dt,
       po_needed_by_dt                 AS po_needed_by_dt,
       po_promesed_by_dt               AS po_promised_by_dt,
       --
       awb_outbound                    AS outbound_awb_code,
       awb_inbound                     AS inbound_awb_code,
       -- remocion y transfer                                                                                                                                                                                                                                
       INVENTORY_BARCODE,                                                                                                                                                                                                                                    
       PO_BARCODE_NUMBER,                                                                                                                                                                                                                                    
       CREATION_SCHEDULE_DT,                                                                                                                                                                                                                                 
     --PO_CODE,                                                                                                                                                                                                                                              
       STAGE_DATE,                                                                                                                                                                                                                                           
       PROCESS_TYPE,                                                                                                                                                                                                                                         
       REMOVAL_DT,                                                                                                                                                                                                                                           
       CATEGORY_REMOVAL_VAL,                                                                                                                                                                                                                                 
       WORK_PACKAGE_LOCATION_CODE,                                                                                                                                                                                                                           
       TASK_BARCODE,                                                                                                                                                                                                                                         
       TRANSFER_BARCODE,                                                                                                                                                                                                                                     
       TRANSFER_TYPE,                                                                                                                                                                                                                                        
       TRANSFER_CREATION_DT,                                                                                                                                                                                                                                 
       TRANSFER_COMPLETE_DT,                                                                                                                                                                                                                                 
       TRANSFER_STATUS,                                                                                                                                                                                                                                      
       TRANSFER_LOCATION_FROM_CODE,                                                                                                                                                                                                                          
       TRANSFER_LOCATION_FROM_TYPE,                                                                                                                                                                                                                          
       TRANSFER_LOCATION_TO_CODE,                                                                                                                                                                                                                            
       TRANSFER_LOCATION_TO_TYPE                                                                                                                                                                                                                             

FROM `bc-te-dlake-dev-s7b3.cln_etracking_us.tmp_exchange`),
--
PO_FECHAS AS (
SELECT  *,
        -- 
        -- Outbound
        --
        -- REMOCION A TRANSFER
        ROUND((DATETIME_DIFF(
                              IFNULL (TRANSFER_CREATION_DT, IF (exchange_outbound_status_code='PENDIENTE' 
                                                               ,CURRENT_DATETIME()
                                                               ,REMOVAL_DT))
                             ,IFNULL (REMOVAL_DT, IFNULL (TRANSFER_CREATION_DT, IF (exchange_outbound_status_code='PENDIENTE'
                                                                                   ,CURRENT_DATETIME()
                                                                                   ,REMOVAL_DT))) 
                             ,MINUTE) / 60
              ),1) removal_transfer_val
        --
        -- TRANSFER A DELIVERY_DISPATCH
        ,ROUND((DATETIME_DIFF(
                              IFNULL (TRANSFER_COMPLETE_DT,    IF (exchange_outbound_status_code='EN TRANSITO'
                                                                 ,CURRENT_DATETIME()
                                                                 ,IFNULL(TRANSFER_CREATION_DT, REMOVAL_DT)))
                             ,IFNULL (TRANSFER_CREATION_DT, REMOVAL_DT)
                             ,MINUTE) / 60
               ),1) transfer_delivery_dispatch_val
        --       
        -- DELIVERY_DISPATCH A COMPLETE_HUB_ARRIVAL
        ,ROUND((DATETIME_DIFF(
                              IFNULL(OUTBOUND_COMPLETE_HUB_ARRIVAL_DT,  IF (exchange_outbound_status_code IN ('EN TRANSITO','EN HUB','EN TRANSIT DESDE HUB HACIA DESTINO')
                                                                           ,CURRENT_DATETIME()
                                                                           ,IFNULL(TRANSFER_COMPLETE_DT, IFNULL (TRANSFER_CREATION_DT, REMOVAL_DT))))
                             ,IFNULL(TRANSFER_COMPLETE_DT, IFNULL (TRANSFER_CREATION_DT, REMOVAL_DT))
                             ,MINUTE) / 60
               ),1) delivery_dispatch_outbound_complete_hub_arrival_val
        --
        -- OUTBOUND_COMPLETE_HUB_ARRIVAL_DT A OUTBOUND_COMPLETE_HUB_DEPARTURE_DT
        ,ROUND((DATETIME_DIFF(
                              IFNULL(OUTBOUND_COMPLETE_HUB_DEPARTURE_DT,  IF (exchange_outbound_status_code IN ('EN TRANSITO','EN HUB','EN TRANSIT DESDE HUB HACIA DESTINO')
                                                                             ,CURRENT_DATETIME()
                                                                             ,IFNULL (OUTBOUND_COMPLETE_HUB_ARRIVAL_DT, IFNULL(TRANSFER_COMPLETE_DT, IFNULL (TRANSFER_CREATION_DT, REMOVAL_DT))))) 
                             ,IFNULL (OUTBOUND_COMPLETE_HUB_ARRIVAL_DT, IFNULL(TRANSFER_COMPLETE_DT, IFNULL (TRANSFER_CREATION_DT, REMOVAL_DT))) 
                             ,MINUTE) / 60
               ),1) outbound_complete_hub_arrival_outbound_complete_hub_departure_val
        --
        -- INBOUND_COMPLETE_HUB_DEPARTURE_DT A SHIPMENT_OUTBOUND_COMPLETE_DT
        ,ROUND((DATETIME_DIFF(
                              IFNULL(SHIPMENT_OUTBOUND_COMPLETE_DT, IF (exchange_outbound_status_code='COMPLETED' 
                                                                       ,CURRENT_DATETIME()
                                                                       ,IFNULL(OUTBOUND_COMPLETE_HUB_DEPARTURE_DT, IFNULL(OUTBOUND_COMPLETE_HUB_ARRIVAL_DT, IFNULL(TRANSFER_COMPLETE_DT, IFNULL (TRANSFER_CREATION_DT, REMOVAL_DT))))))
                             ,IFNULL(OUTBOUND_COMPLETE_HUB_DEPARTURE_DT, IFNULL(OUTBOUND_COMPLETE_HUB_ARRIVAL_DT, IFNULL(TRANSFER_COMPLETE_DT, IFNULL (TRANSFER_CREATION_DT, REMOVAL_DT))))
                             ,MINUTE) / 60
               ),1) outbound_complete_hub_departure_shipment_outbound_complete_val
        -- --
        -- -- DELIVERY_DISPATCH A SHIPMENT OUTBOUND
        --,ROUND((DATETIME_DIFF(
        --                      IFNULL(SHIPMENT_OUTBOUND_COMPLETE_DT,  IF (exchange_outbound_status_code= 'PENDIENTE' -- 'PO SEND SHIPMENT'
        --                                                                ,CURRENT_DATETIME()
        --                                                                ,IFNULL(DELIVERY_DISPATCH_DT, IFNULL(TRANSFER_COMPLETE_DT, REMOVAL_DT))))
        --                     ,IFNULL(DELIVERY_DISPATCH_DT, IFNULL(TRANSFER_COMPLETE_DT, REMOVAL_DT))
        --                     ,MINUTE) / 60
        --       ),1) delivery_dispatch_shipment_outbound_complete_val
        --
        -- 
        -- Inbound
        --
        --  CREACION A FIRST_AUTH   
        ,ROUND((DATETIME_DIFF(
                              IFNULL (PO_FIRST_AUTH_DT,      IF (exchange_inbound_status_code IN ('PO OPEN', 'PO OPEN - REQUESTED') --'PO AUTORIZADA'
                                                                ,CURRENT_DATETIME()
                                                                ,PO_CREATION_DT))
                             ,PO_CREATION_DT
                             ,MINUTE) / 60
               ),1) creation_first_auth_val
        -- 
        --  FIRST_AUTH A FIRST_ISSUE   
        ,ROUND((DATETIME_DIFF(
                              IFNULL(PO_FIRST_ISSUE_DT,      IF (exchange_inbound_status_code='PO AUTORIZADA' --''PO ISSUED'
                                                                ,CURRENT_DATETIME()
                                                                ,IFNULL(PO_FIRST_AUTH_DT, PO_CREATION_DT)))
                             ,IFNULL(PO_FIRST_AUTH_DT, PO_CREATION_DT)
                             ,MINUTE) / 60
               ),1) first_auth_first_issue_val
        --
        -- FIRST_ISSUE A COMPLETE_HUB_ARRIVAL
        ,ROUND((DATETIME_DIFF(
                              IFNULL(INBOUND_COMPLETE_HUB_ARRIVAL_DT,    IF (exchange_inbound_status_code='PO ISSUED' --'PO IN HUB'
                                                                            ,CURRENT_DATETIME()
                                                                            ,IFNULL(PO_FIRST_ISSUE_DT, IFNULL(PO_FIRST_AUTH_DT, PO_CREATION_DT))))
                             ,IFNULL(PO_FIRST_ISSUE_DT, IFNULL(PO_FIRST_AUTH_DT, PO_CREATION_DT))
                             ,MINUTE) / 60
               ),1) first_issue_inbound_complete_hub_arrival_val
        --
        -- INBOUND_COMPLETE_HUB_ARRIVAL_DT A INBOUND_COMPLETE_HUB_DEPARTURE_DT
        ,ROUND((DATETIME_DIFF(
                              IFNULL(INBOUND_COMPLETE_HUB_DEPARTURE_DT,  IF (exchange_inbound_status_code = 'PO IN HUB'   
                                                                            ,CURRENT_DATETIME()
                                                                            ,IFNULL (INBOUND_COMPLETE_HUB_ARRIVAL_DT, IFNULL(PO_FIRST_ISSUE_DT, IFNULL(PO_FIRST_AUTH_DT, PO_CREATION_DT))))) 
                             ,IFNULL(INBOUND_COMPLETE_HUB_ARRIVAL_DT, IFNULL(PO_FIRST_ISSUE_DT, IFNULL(PO_FIRST_AUTH_DT, PO_CREATION_DT))) 
                             ,MINUTE) / 60
               ),1) inbound_complete_hub_arrival_inbound_complete_hub_departure_val
        --
        -- INBOUND_COMPLETE_HUB_DEPARTURE_DT A SHIPMENT_INBOUND_COMPLETE_DT
        ,ROUND((DATETIME_DIFF(
                              IFNULL(SHIPMENT_INBOUND_COMPLETE_DT, IF (exchange_inbound_status_code = 'PO HUB HACIA DESTINO'
                                                                       ,CURRENT_DATETIME()
                                                                       ,IFNULL(INBOUND_COMPLETE_HUB_DEPARTURE_DT, IFNULL(INBOUND_COMPLETE_HUB_ARRIVAL_DT, IFNULL(PO_FIRST_ISSUE_DT, IFNULL(PO_FIRST_AUTH_DT, PO_CREATION_DT))))))
                             ,IFNULL(INBOUND_COMPLETE_HUB_DEPARTURE_DT, IFNULL(INBOUND_COMPLETE_HUB_ARRIVAL_DT, IFNULL(PO_FIRST_ISSUE_DT, IFNULL(PO_FIRST_AUTH_DT, PO_CREATION_DT))))
                             ,MINUTE) / 60
               ),1) inbound_complete_hub_departure_shipment_inbound_complete_val
        --
        --
        --  SHIPMENT_INBOUND_COMPLETE_DT A RFI
        ,ROUND((DATETIME_DIFF(
                              IFNULL (EXCHANGE_RECEIVED_DT, IF (exchange_inbound_status_code IN ('PO RECEIVED', 'PRE RECEPCION', 'PO QUAR', 'PO PARTIAL')
                                                             ,CURRENT_DATETIME()
                                                             ,IFNULL(SHIPMENT_INBOUND_COMPLETE_DT, IFNULL(INBOUND_COMPLETE_HUB_DEPARTURE_DT, IFNULL(INBOUND_COMPLETE_HUB_ARRIVAL_DT, IFNULL(PO_FIRST_ISSUE_DT, IFNULL(PO_FIRST_AUTH_DT, PO_CREATION_DT)))))))
                             ,IFNULL(SHIPMENT_INBOUND_COMPLETE_DT, IFNULL(INBOUND_COMPLETE_HUB_DEPARTURE_DT, IFNULL(INBOUND_COMPLETE_HUB_ARRIVAL_DT, IFNULL(PO_FIRST_ISSUE_DT, IFNULL(PO_FIRST_AUTH_DT, PO_CREATION_DT)))))
                             ,MINUTE) / 60
               ),1) shipment_inbound_complete_rfi_val
        -- 
        -- Clasifica los "peores" estagios y recupera las menores fechas de la PO
        -- PARA REPARACIONES SOLO HAY 1 LINEA POR PO - NO NECESARIO PONER MINIMAS FECHAS
        --
        ,REMOVAL_DT                         STAGE_REMOVAL_DT                  
        ,TRANSFER_CREATION_DT               STAGE_TRANSFER_DT 
        ,TRANSFER_COMPLETE_DT               STAGE_DELIVERY_DISPATCH_DT    
        ,SHIPMENT_OUTBOUND_COMPLETE_DT      STAGE_SHIPMENT_OUTBOUND_COMPLETE_DT  
        ,OUTBOUND_COMPLETE_HUB_ARRIVAL_DT   STAGE_OUTBOUND_COMPLETE_HUB_ARRIVAL_DT
        ,OUTBOUND_COMPLETE_HUB_DEPARTURE_DT STAGE_OUTBOUND_COMPLETE_HUB_DEPARTURE_DT
         --
        ,PO_CREATION_DT                     STAGE_PO_CREATION_DT              
        ,PO_FIRST_AUTH_DT                   STAGE_PO_FIRST_AUTH_DT            
        ,PO_FIRST_ISSUE_DT                  STAGE_PO_FIRST_ISSUE_DT           
        ,INBOUND_COMPLETE_HUB_ARRIVAL_DT    STAGE_INBOUND_COMPLETE_HUB_ARRIVAL_DT
        ,INBOUND_COMPLETE_HUB_DEPARTURE_DT  STAGE_INBOUND_COMPLETE_HUB_DEPARTURE_DT
        ,SHIPMENT_INBOUND_COMPLETE_DT       STAGE_SHIPMENT_INBOUND_COMPLETE_DT
        ,EXCHANGE_RECEIVED_DT               STAGE_EXCHANGE_RECEIVED_DT          
        --        
        ,CASE exchange_outbound_status_code
                              WHEN 'PENDIENTE'                           THEN 1
                              WHEN 'EN TRANSITO'                         THEN 2                           
                              WHEN 'EN HUB'                              THEN 3 
                              WHEN 'EN TRANSIT DESDE HUB HACIA DESTINO'  THEN 4 
                              WHEN 'COMPLETED'                           THEN 5
                              WHEN 'PO CANCEL'                           THEN 6 
                              WHEN 'OTRO'                                THEN 7 
                              ELSE 99 END
                              stage_outbound_code
        --
        ,CASE exchange_inbound_status_code
                              WHEN 'PO OPEN'                         THEN 1
                              WHEN 'PO OPEN - REQUESTED'             THEN 2
                              WHEN 'PO AUTORIZADA'                   THEN 3
                              WHEN 'PO ISSUED'                       THEN 4
                              WHEN 'PO IN HUB'                       THEN 5
                              WHEN 'PO HUB HACIA DESTINO'            THEN 6
                              WHEN 'PRE RECEPCION'                   THEN 7                              
                              WHEN 'PO PARTIAL'                      THEN 8
                              WHEN 'PO RECEIVED'                     THEN 9
                              WHEN 'PO QUAR'                         THEN 10
                              WHEN 'RFI'                             THEN 11   
                              WHEN 'ORDEN PENDIENTE REGULARIZACION'  THEN 12
                              WHEN 'MRO REGULARIZACION'              THEN 13
                              WHEN 'PO CANCEL'                       THEN 14
                              WHEN 'OTRO'                            THEN 99 
                              END
                              stage_inbound_code
        --
FROM tmp_exchange_base
),
--#
--# Parte 2) Implementación de los flags de SLA
--#
FINAL_3 AS (
SELECT   *,
         --
         -- Acumula las horas de las etapas         
         ROUND (f.removal_transfer_val + 
                f.transfer_delivery_dispatch_val +
                f.delivery_dispatch_outbound_complete_hub_arrival_val +
                f.outbound_complete_hub_arrival_outbound_complete_hub_departure_val +
                f.outbound_complete_hub_departure_shipment_outbound_complete_val) outbound_stage_val
         --
        ,ROUND (f.creation_first_auth_val +                             
                f.first_auth_first_issue_val +                           
                f.first_issue_inbound_complete_hub_arrival_val +                 
                f.inbound_complete_hub_arrival_inbound_complete_hub_departure_val +
                f.inbound_complete_hub_departure_shipment_inbound_complete_val +
                f.shipment_inbound_complete_rfi_val, 1) inbound_stage_val
         --
         -- Acumula las horas de SLAs por prioridad
       ,(SELECT SUM(gs.sla_val) FROM `bc-te-dlake-dev-s7b3.cln_etracking_us.general_sla`gs WHERE gs.process_id = 4 AND gs.stage_id IN (1,2,3,4,5,6)    AND gs.req_priority_code = f.req_priority_code
					AND F.po_creation_dt>=GS.valid_from
					AND (GS.valid_until is null or F.po_creation_dt<=gs.valid_until)	   
	   ) sla_outbound_standard_val
       ,(SELECT SUM(gs.sla_val) FROM `bc-te-dlake-dev-s7b3.cln_etracking_us.general_sla`gs WHERE gs.process_id = 4 AND gs.stage_id IN (7,8,9,10,11,12) AND gs.req_priority_code = f.req_priority_code
                    AND F.po_creation_dt>=GS.valid_from
					AND (GS.valid_until is null or F.po_creation_dt<=gs.valid_until)
	   ) sla_inbound_standard_val
         --
         -- FLAG SLA 
         -- Compara el acumulado de horas de la etapa y compara con el SLA de la etapa, acumulando las horas de las etapas anteriores, 
         -- caso las fechas estÃ©n vacias 
         --
         -- OUTBOUND
         --
         -- removal a turn in
         --
        ,IF (f.removal_transfer_val       > (SELECT gs.sla_val FROM `bc-te-dlake-dev-s7b3.cln_etracking_us.general_sla` gs
                                             WHERE  gs.process_id = 4  AND gs.req_priority_code = f.req_priority_code
                                             AND    gs.stage_id = 1 
                                             AND    F.po_creation_dt>=GS.valid_from
                                             AND   (GS.valid_until is null or F.po_creation_dt<=gs.valid_until)
                                             ),1,0) removal_transfer_flag
         --
         -- turn in a delivery_dispatch
         --
        ,IF (f.transfer_delivery_dispatch_val > 
                                            (SELECT SUM(gs.sla_val) sla_val FROM `bc-te-dlake-dev-s7b3.cln_etracking_us.general_sla` gs
                                             WHERE  gs.process_id = 4 AND gs.req_priority_code = f.req_priority_code 
                                             AND   (gs.stage_id   = 5 -- 2,3,4
                                             OR     CASE WHEN (f.transfer_complete_dt is null) THEN gs.stage_id IN (1) END
                                             ) 
                                             AND F.po_creation_dt>=GS.valid_from
                                             AND (GS.valid_until is null or F.po_creation_dt<=gs.valid_until)
                                             ),1,0) transfer_delivery_dispatch_flag
         --
         -- delivery_dispatch a complete hub arrival -- <<< no tenemos sla para esta etapa - <<< por el momento, se compara con la etapa entrega >>>
         --
        ,IF (f.delivery_dispatch_outbound_complete_hub_arrival_val >
                                            (SELECT SUM(gs.sla_val) sla_val FROM `bc-te-dlake-dev-s7b3.cln_etracking_us.general_sla` gs
                                             WHERE  gs.process_id = 4 AND gs.req_priority_code = f.req_priority_code 
                                             AND   (gs.stage_id   = 5 -- 2,3,4
                                             OR     CASE WHEN (f.transfer_complete_dt is null) THEN gs.stage_id IN (1) END
                                             ) 
                                             AND F.po_creation_dt>=GS.valid_from
                                             AND (GS.valid_until is null or F.po_creation_dt<=gs.valid_until)
                                             ),1,0) delivery_dispatch_outbound_complete_hub_arrival_flag
         --
         -- complete hub arrival a complete hub departure -- <<< no tenemos sla para esta etapa - <<< por el momento, se compara con la etapa entrega >>>
         --
        ,IF (f.outbound_complete_hub_arrival_outbound_complete_hub_departure_val >
                                            (SELECT SUM(gs.sla_val) sla_val FROM `bc-te-dlake-dev-s7b3.cln_etracking_us.general_sla` gs
                                             WHERE  gs.process_id = 4 AND gs.req_priority_code = f.req_priority_code 
                                             AND   (gs.stage_id   = 5 -- 2,3,4
                                             OR     CASE WHEN (f.transfer_complete_dt is null) THEN gs.stage_id IN (1) END
                                             ) 
                                             AND F.po_creation_dt>=GS.valid_from
                                             AND (GS.valid_until is null or F.po_creation_dt<=gs.valid_until)
                                             ),1,0) outbound_complete_hub_arrival_outbound_complete_hub_departure_flag 
         --
         --  complete hub departure a shipment outbound complete
         --
        ,IF (f.delivery_dispatch_outbound_complete_hub_arrival_val +
             f.outbound_complete_hub_arrival_outbound_complete_hub_departure_val +  -- <<<  como no tenemos etapas de SLA de HUB. sumo los valores >>>
             f.outbound_complete_hub_departure_shipment_outbound_complete_val >     -- <<<  como no tenemos etapas de SLA de HUB. sumo los valores >>>
                                           (SELECT SUM(gs.sla_val) sla_val FROM `bc-te-dlake-dev-s7b3.cln_etracking_us.general_sla` gs
                                             WHERE  gs.process_id = 4 AND gs.req_priority_code = f.req_priority_code 
                                             AND   (gs.stage_id   = 6
                                             OR     CASE WHEN (f.delivery_dispatch_dt is null) THEN gs.stage_id IN (5) END
                                             OR     CASE WHEN (f.delivery_dispatch_dt is null and f.transfer_complete_dt is null) THEN gs.stage_id IN (1) END
                                             ) 
                                             AND F.po_creation_dt>=GS.valid_from
                                             AND (GS.valid_until is null or F.po_creation_dt<=gs.valid_until)
                                             ),1,0) outbound_complete_hub_departure_shipment_outbound_complete_flag
         --
         -- INBOUND
         --
         --
         -- creation a first auth 
         --
        ,IF (f.creation_first_auth_val >    (SELECT SUM(gs.sla_val) sla_val FROM `bc-te-dlake-dev-s7b3.cln_etracking_us.general_sla` gs
                                             WHERE  gs.process_id = 4 AND gs.req_priority_code = f.req_priority_code 
                                             AND    gs.stage_id   = 7
                                             AND F.po_creation_dt>=GS.valid_from
                                             AND (GS.valid_until is null or F.po_creation_dt<=gs.valid_until)
                                             ),1,0) creation_first_auth_flag
         --
         -- first_auth a first_issue
         --
        ,IF (f.first_auth_first_issue_val > (SELECT SUM(gs.sla_val) sla_val FROM `bc-te-dlake-dev-s7b3.cln_etracking_us.general_sla` gs
                                             WHERE  gs.process_id = 4 AND gs.req_priority_code = f.req_priority_code 
                                             AND   (gs.stage_id   = 8
                                             OR     CASE WHEN (f.po_first_auth_dt is null) THEN gs.stage_id IN (7) END
                                             ) 
                                             AND F.po_creation_dt>=GS.valid_from
                                             AND (GS.valid_until is null or F.po_creation_dt<=gs.valid_until)
                                             ),1,0) first_auth_first_issue_flag

         --
         -- first_issue a complete_hub_arrival + complete_hub_arrival a complete_hub_departure
         --
        ,IF (f.first_issue_inbound_complete_hub_arrival_val + f.inbound_complete_hub_arrival_inbound_complete_hub_departure_val >   -- <<< sumo la entrada y salida >>>
                                            (SELECT SUM(gs.sla_val) sla_val FROM `bc-te-dlake-dev-s7b3.cln_etracking_us.general_sla` gs
                                             WHERE  gs.process_id = 4 AND gs.req_priority_code = f.req_priority_code 
                                             AND   (gs.stage_id   = 9
                                             OR     CASE WHEN (f.po_first_issue_dt is null) THEN gs.stage_id IN (8) END
                                             OR     CASE WHEN (f.po_first_issue_dt is null and f.po_first_auth_dt is null) THEN gs.stage_id IN (7) END
                                             ) 
                                             AND F.po_creation_dt>=GS.valid_from
                                             AND (GS.valid_until is null or F.po_creation_dt<=gs.valid_until)
                                             ),1,0) first_issue_complete_hub_departure_flag
         --
         -- complete_hub_departure_shipment_inbound_complete_val
         --
        ,IF (f.inbound_complete_hub_departure_shipment_inbound_complete_val > 
                                            (SELECT SUM(gs.sla_val) sla_val FROM `bc-te-dlake-dev-s7b3.cln_etracking_us.general_sla` gs
                                             WHERE  gs.process_id = 4 AND gs.req_priority_code = f.req_priority_code
                                             AND   (gs.stage_id   = 11
                                             OR     CASE WHEN (f.inbound_complete_hub_departure_dt is null) THEN gs.stage_id IN (9) END
                                             OR     CASE WHEN (f.inbound_complete_hub_departure_dt is null and f.po_first_issue_dt is null) THEN gs.stage_id IN (8) END
                                             OR     CASE WHEN (f.inbound_complete_hub_departure_dt is null and f.po_first_issue_dt is null and po_first_auth_dt is null) THEN gs.stage_id IN (7) END
                                             ) 
                                             AND F.po_creation_dt>=GS.valid_from
                                             AND (GS.valid_until is null or F.po_creation_dt<=gs.valid_until)
                                             ),1,0) inbound_complete_hub_departure_shipment_inbound_complete_flag
         -- 
         -- shipment_inbound_complete a rfi
         --                                    
        ,IF (f.shipment_inbound_complete_rfi_val >  
                                            (SELECT SUM(gs.sla_val) sla_val FROM `bc-te-dlake-dev-s7b3.cln_etracking_us.general_sla` gs
                                             WHERE  gs.process_id = 4 AND gs.req_priority_code = f.req_priority_code 
                                             AND   (gs.stage_id   = 12
                                             OR     CASE WHEN (f.shipment_inbound_complete_dt is null) THEN gs.stage_id IN (11) END
                                             OR     CASE WHEN (f.shipment_inbound_complete_dt is null and f.inbound_complete_hub_departure_dt is null) THEN gs.stage_id IN (9) END
                                             OR     CASE WHEN (f.shipment_inbound_complete_dt is null and f.inbound_complete_hub_departure_dt is null and f.po_first_issue_dt is null) THEN gs.stage_id IN (8) END
                                             OR     CASE WHEN (f.shipment_inbound_complete_dt is null and f.inbound_complete_hub_departure_dt is null and f.po_first_issue_dt is null and f.po_first_auth_dt is null) THEN gs.stage_id IN (7) END
                                             ) 
                                             AND F.po_creation_dt>=GS.valid_from
                                             AND (GS.valid_until is null or F.po_creation_dt<=gs.valid_until)
                                             ),1,0) shipment_inbound_complete_rfi_flag

         --
         -- Suma las horas de la cadena y las compara con el acumulado de SLA, de acuerdo donde se encuentra en la cadena
         --
        ,IF    ((f.removal_transfer_val + 
                 f.transfer_delivery_dispatch_val +
                 f.delivery_dispatch_outbound_complete_hub_arrival_val +
                 f.outbound_complete_hub_arrival_outbound_complete_hub_departure_val +
                 f.outbound_complete_hub_departure_shipment_outbound_complete_val)
                                                                    > (SELECT SUM(sla_val)
                                                                       FROM `bc-te-dlake-dev-s7b3.cln_etracking_us.general_sla` gs
                                                                       WHERE gs.process_id = 4
                                                                       AND   gs.req_priority_code = f.req_priority_code
                                                                       AND   (CASE WHEN f.exchange_outbound_status_code = 'PENDIENTE'     THEN stage_id IN (1,2,3,4)
                                                                                   WHEN f.exchange_outbound_status_code = 'EN TRANSITO'   THEN stage_id IN (5) 
                                                                                   WHEN f.exchange_outbound_status_code = 'EN HUB'        THEN stage_id IN (5) 
                                                                                   WHEN f.exchange_outbound_status_code = 'EN TRANSIT DESDE HUB HACIA DESTINO' THEN stage_id IN (5)  
                                                                                   WHEN f.exchange_outbound_status_code = 'COMPLETED'     THEN stage_id IN (6) 
                                                                                   ELSE stage_id IN (1,2,3,4,5,6) 
                                                                                   END)	
                                                                                   AND F.po_creation_dt>=GS.valid_from
                                                                                   AND (GS.valid_until is null or F.po_creation_dt<=gs.valid_until)
                                                                                   ),1,0) stage_outbound_flag
         --
        ,IF    (f.creation_first_auth_val                              +    
                f.first_auth_first_issue_val                           +  
                f.first_issue_inbound_complete_hub_arrival_val                 +  
                f.inbound_complete_hub_arrival_inbound_complete_hub_departure_val      +
                f.inbound_complete_hub_departure_shipment_inbound_complete_val +  
                f.shipment_inbound_complete_rfi_val
                                                                    > (SELECT SUM(sla_val)
                                                                       FROM `bc-te-dlake-dev-s7b3.cln_etracking_us.general_sla` gs
                                                                       WHERE gs.process_id = 4
                                                                       AND   gs.req_priority_code = f.req_priority_code
                                                                       AND   (CASE WHEN f.exchange_inbound_status_code in ('PO OPEN', 'PO OPEN - REQUESTED') THEN stage_id IN (7) 
                                                                                   WHEN f.exchange_inbound_status_code = 'PO AUTORIZADA'                     THEN stage_id IN (7,8) 
                                                                                   WHEN f.exchange_inbound_status_code = 'PO ISSUED'                         THEN stage_id IN (7,8,9) 
                                                                                   WHEN f.exchange_inbound_status_code = 'PO IN HUB'                         THEN stage_id IN (7,8,9,10)
                                                                                   WHEN f.exchange_inbound_status_code = 'PO HUB HACIA DESTINO'              THEN stage_id IN (7,8,9,10)
                                                                                   WHEN f.exchange_inbound_status_code IN ('PO RECEIVED', 'PRE RECEPCION', 'PO PARTIAL') THEN stage_id IN (7,8,9,10,11)  
                                                                                   WHEN f.exchange_inbound_status_code IN ('RFI', 'PO QUAR','OTRO')          THEN stage_id IN (7,8,9,10,11,12)
                                                                                   ELSE stage_id IN (7,8,9,10,11,12) 
                                                                                   END)
                                                                        AND F.po_creation_dt>=GS.valid_from
                                                                        AND (GS.valid_until is null or F.po_creation_dt<=gs.valid_until)
                                                                        ),1,0) stage_inbound_flag
        --
FROM po_fechas f)
--#
--# Parte 3) Query Final, con indicares generales
--#

--
-- SLAs por etapa - outbound / inbound
--
SELECT F.*
      ,CASE exchange_outbound_status_code 
            WHEN 'PENDIENTE'                      THEN IF (removal_transfer_flag                                 = 0,0,1)
            WHEN 'EN TRANSITO'                    THEN IF (transfer_delivery_dispatch_flag                       = 0,0,1)
            WHEN 'EN HUB'                              THEN IF (delivery_dispatch_outbound_complete_hub_arrival_flag = 0,0,1) -- no tenemos etapas de sla HUB outbound
            WHEN 'EN TRANSIT DESDE HUB HACIA DESTINO'  THEN IF (outbound_complete_hub_arrival_outbound_complete_hub_departure_flag  = 0,0,1) -- no tenemos etapas de sla HUB outbound
            WHEN 'COMPLETED'                      THEN IF (outbound_complete_hub_departure_shipment_outbound_complete_flag     = 0,0,1) 
            WHEN 'PO CANCEL'                      THEN IF (removal_transfer_flag                                 = 0,0,1)
            WHEN 'OTRO'                           THEN IF (removal_transfer_flag                                 = 0,0,1)
            ELSE 0
       END po_unit_outbound_stage_flag
       --
      ,CASE exchange_inbound_status_code 
            WHEN 'PO OPEN'                        THEN IF (creation_first_auth_flag                              = 0,0,1)
            WHEN 'PO OPEN - REQUESTED'            THEN IF (creation_first_auth_flag                              = 0,0,1)
            WHEN 'PO AUTORIZADA'                  THEN IF (creation_first_auth_flag                              = 0,0,1)
            WHEN 'PO ISSUED'                      THEN IF (first_auth_first_issue_flag                           = 0,0,1)
            WHEN 'PO IN HUB'                      THEN IF (first_issue_complete_hub_departure_flag               = 0,0,1)
            WHEN 'PO HUB HACIA DESTINO'           THEN IF (inbound_complete_hub_departure_shipment_inbound_complete_flag = 0,0,1)
            WHEN 'PRE RECEPCION'                  THEN IF (shipment_inbound_complete_rfi_flag                    = 0,0,1)
            WHEN 'PO PARTIAL'                     THEN IF (shipment_inbound_complete_rfi_flag                    = 0,0,1)
            WHEN 'PO RECEIVED'                    THEN IF (shipment_inbound_complete_rfi_flag                    = 0,0,1)
            WHEN 'PO QUAR'                        THEN IF (shipment_inbound_complete_rfi_flag                    = 0,0,1)  
            WHEN 'RFI'                            THEN IF (shipment_inbound_complete_rfi_flag                    = 0,0,1)
            WHEN 'ORDEN PENDIENTE REGULARIZACION' THEN IF (inbound_complete_hub_departure_shipment_inbound_complete_flag = 0,0,1)   
            WHEN 'MRO REGULARIZACION'             THEN IF (first_auth_first_issue_flag                           = 0,0,1)
            WHEN 'PO CANCEL'                      THEN IF (creation_first_auth_flag                              = 0,0,1)  
            WHEN 'OTRO'                           THEN IF (creation_first_auth_flag                              = 0,0,1)
            ELSE 0
            END po_unit_inbound_stage_flag
       --
       -- Compara el SLA acumulado total con las horas totales de la cadena, para ver el consumo de la cadena
       --
      ,CASE WHEN round (ifnull(f.outbound_stage_val / f.sla_outbound_standard_val,0),4)  > 1     then 'SLA Exceeded' 
            WHEN round (ifnull(f.outbound_stage_val / f.sla_outbound_standard_val,0),4)  >= 0.90 and round (f.outbound_stage_val / f.sla_outbound_standard_val,4)  < 1    then 'Over 90%' 
            WHEN round (ifnull(f.outbound_stage_val / f.sla_outbound_standard_val,0),4)  >= 0.70 and round (f.outbound_stage_val / f.sla_outbound_standard_val,4)  < 0.90 then 'Between 70% - 89%' 
            WHEN round (ifnull(f.outbound_stage_val / f.sla_outbound_standard_val,0),4)  >= 0.50 and round (f.outbound_stage_val / f.sla_outbound_standard_val,4)  < 0.70 then 'Between 50% - 69%' 
            WHEN round (ifnull(f.outbound_stage_val / f.sla_outbound_standard_val,0),4)  <  0.50 then 'Less than 50%' 
       END  stage_outbound_over_sla_desc
       --
      ,CASE WHEN round (ifnull(f.inbound_stage_val / f.sla_inbound_standard_val,0),4)  > 1     then 'SLA Exceeded' 
            WHEN round (ifnull(f.inbound_stage_val / f.sla_inbound_standard_val,0),4)  >= 0.90 and round (f.inbound_stage_val / f.sla_inbound_standard_val,4)  < 1    then 'Over 90%' 
            WHEN round (ifnull(f.inbound_stage_val / f.sla_inbound_standard_val,0),4)  >= 0.70 and round (f.inbound_stage_val / f.sla_inbound_standard_val,4)  < 0.90 then 'Between 70% - 89%' 
            WHEN round (ifnull(f.inbound_stage_val / f.sla_inbound_standard_val,0),4)  >= 0.50 and round (f.inbound_stage_val / f.sla_inbound_standard_val,4)  < 0.70 then 'Between 50% - 69%' 
            WHEN round (ifnull(f.inbound_stage_val / f.sla_inbound_standard_val,0),4)  <  0.50 then 'Less than 50%' 
       END  stage_inbound_over_sla_desc
       -- 
      ,CASE stage_outbound_code                       
            WHEN 1  THEN 'PENDIENTE'                   
            WHEN 2  THEN 'EN TRANSITO'   
            WHEN 3  THEN 'EN HUB'
            WHEN 4  THEN 'EN TRANSIT DESDE HUB HACIA DESTINO'  
            WHEN 5  THEN 'COMPLETED'               
            WHEN 6  THEN 'PO CANCEL'
            WHEN 7  THEN 'OTRO' 
            WHEN 99 THEN 'OTRO'                      
      END po_outbound_stage_code
       --
      ,CASE stage_inbound_code
            WHEN 1  THEN 'PO OPEN'                     
            WHEN 2  THEN 'PO OPEN - REQUESTED'         
            WHEN 3  THEN 'PO AUTORIZADA'               
            WHEN 4  THEN 'PO ISSUED'                   
            WHEN 5  THEN 'PO IN HUB'                     
            WHEN 6  THEN 'PO HUB HACIA DESTINO'          
            WHEN 7  THEN 'PRE RECEPCION'                        
            WHEN 8  THEN 'PO PARTIAL'                    
            WHEN 9  THEN 'PO RECEIVED'
            WHEN 10 THEN 'PO QUAR'                       
            WHEN 11 THEN 'RFI'
            WHEN 12 THEN 'ORDEN PENDIENTE REGULARIZACION'
            WHEN 13 THEN 'MRO REGULARIZACION'            
            WHEN 14 THEN 'PO CANCEL'                     
            WHEN 99 THEN 'OTRO'
      END po_inbound_stage_code
      --
     ,CASE stage_outbound_code
         WHEN 1  THEN STAGE_TRANSFER_DT
         WHEN 2  THEN STAGE_DELIVERY_DISPATCH_DT  
         WHEN 3  THEN STAGE_OUTBOUND_COMPLETE_HUB_ARRIVAL_DT
         WHEN 4  THEN STAGE_OUTBOUND_COMPLETE_HUB_DEPARTURE_DT
         WHEN 5  THEN STAGE_SHIPMENT_OUTBOUND_COMPLETE_DT
         WHEN 6  THEN STAGE_PO_CREATION_DT
         WHEN 7  THEN STAGE_PO_CREATION_DT
         WHEN 99 THEN STAGE_PO_CREATION_DT
      END po_stage_outbound_dt
      --
     ,CASE stage_inbound_code    
         WHEN 1  THEN STAGE_PO_CREATION_DT   
         WHEN 2  THEN STAGE_PO_FIRST_AUTH_DT   
         WHEN 4  THEN STAGE_PO_FIRST_ISSUE_DT  
         WHEN 5  THEN STAGE_INBOUND_COMPLETE_HUB_ARRIVAL_DT             
         WHEN 6  THEN STAGE_INBOUND_COMPLETE_HUB_DEPARTURE_DT      
         WHEN 7  THEN STAGE_EXCHANGE_RECEIVED_DT   
         WHEN 8  THEN STAGE_EXCHANGE_RECEIVED_DT         
         WHEN 9  THEN STAGE_EXCHANGE_RECEIVED_DT             
         WHEN 10 THEN STAGE_EXCHANGE_RECEIVED_DT
         WHEN 11 THEN STAGE_EXCHANGE_RECEIVED_DT
         WHEN 12 THEN STAGE_SHIPMENT_INBOUND_COMPLETE_DT
         WHEN 13 THEN STAGE_PO_FIRST_ISSUE_DT
         WHEN 14 THEN STAGE_PO_CREATION_DT
         WHEN 99 THEN STAGE_PO_CREATION_DT
      END po_stage_inbound_dt
    --
        ,exo.out_shipper
        ,exo.out_consignee
        ,exo.out_status  AS out_status_expeditors
        ,exo.out_pick_up_notification
        ,exo.out_picked_up
        ,exo.out_freight_received
        ,exo.out_documents_received
        ,exo.out_documents_scanned
        ,exo.out_bill_of_lading_processed
        ,exo.out_booked
        ,exo.out_transferred_to_airline_or_gha
        ,exo.out_confirmed_on_board
        ,exo.out_arrived_at_master_destination
        ,exo.out_broker_turnover
        --
        ,exi.in_shipper
        ,exi.in_consignee
        ,exi.in_status   AS in_status_expeditors
        ,exi.in_pick_up_notification
        ,exi.in_picked_up
        ,exi.in_freight_received
        ,exi.in_documents_received
        ,exi.in_documents_scanned
        ,exi.in_bill_of_lading_processed
        ,exi.in_booked
        ,exi.in_transferred_to_airline_or_gha
        ,exi.in_confirmed_on_board
        ,exi.in_arrived_at_master_destination
        ,exi.in_broker_turnover
        --
        ,out_awb.outbound_doc_prefix
        ,out_awb.outbound_doc_number
        ,out_awb.outbound_set_flight_number
        ,out_awb.outbound_set_origin_system
        ,out_awb.outbound_min_flight_date
        ,out_awb.outbound_max_flight_date
        ,out_awb.outbound_awb_pieces_qty
        ,out_awb.outbound_awb_kg_chargeable
        ,out_awb.outbound_awb_departure_dt
        ,out_awb.outbound_awb_arrival_dt                  
         --
        ,in_awb.inbound_doc_prefix
        ,in_awb.inbound_doc_number
        ,in_awb.inbound_set_flight_number
        ,in_awb.inbound_set_origin_system
        ,in_awb.inbound_min_flight_date
        ,in_awb.inbound_max_flight_date
        ,in_awb.inbound_awb_pieces_qty
        ,in_awb.inbound_awb_kg_chargeable
        ,in_awb.inbound_awb_departure_dt
        ,in_awb.inbound_awb_arrival_dt                  
FROM FINAL_3 F
left join `bc-te-dlake-dev-s7b3.cln_etracking_us.tmp_expeditors_reference_outbound` exo on f.po_code=exo.out_reference and exo.row_id=1
left join `bc-te-dlake-dev-s7b3.cln_etracking_us.tmp_expeditors_reference_inbound`  exi on f.po_code=exi.in_reference  and exi.row_id=1
left join ( -- Latam Cargo -- outbound
           SELECT 
                  leg.doc_prefix as outbound_doc_prefix
                 ,leg.doc_number as outbound_doc_number
                 ,STRING_AGG(DISTINCT(CAST (leg.flight_number AS STRING)),'/') outbound_set_flight_number
                 ,STRING_AGG(DISTINCT(      leg.data_origin_flg)         ,'/') outbound_set_origin_system
                 ,min(leg.flight_date)       outbound_min_flight_date
                 ,max(leg.flight_date)       outbound_max_flight_date
                 ,max(leg.awb_pieces_qty)    outbound_awb_pieces_qty
                 ,max(leg.awb_kg_chargeable) outbound_awb_kg_chargeable
                  --
                 ,MIN(CAST(CONCAT(SUBSTR(CAST(TRIC_FCH_SALIDA_REAL_LOCAL AS STRING),1,10),'T',TIME_ADD(TIME "00:00:00", INTERVAL CAST(TRIC_HRA_SALIDA_REAL_LOCAL AS INT64) SECOND)) AS datetime )) as outbound_awb_departure_dt
                 ,MAX(CAST(CONCAT(SUBSTR(CAST(TRIC_FCH_ARRIBO_REAL_LOCAL AS STRING),1,10),'T',TIME_ADD(TIME "00:00:00", INTERVAL CAST(TRIC_HRA_ARRIBO_REAL_LOCAL AS INT64) SECOND)) AS datetime )) as outbound_awb_arrival_dt                  
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
          ) out_awb       
           on out_awb.outbound_doc_prefix = SAFE_CAST(SUBSTR(LPAD(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(f.outbound_awb_code,'-',''),' ',''),'"',''),11,'0'),1,3) AS INTEGER) and 
              out_awb.outbound_doc_number = SAFE_CAST(SUBSTR(LPAD(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(f.outbound_awb_code,'-',''),' ',''),'"',''),11,'0'),4,8) AS NUMERIC)
left join ( -- Latam Cargo -- inbound
           SELECT 
                  leg.doc_prefix as inbound_doc_prefix
                 ,leg.doc_number as inbound_doc_number
                 ,STRING_AGG(DISTINCT(CAST (leg.flight_number AS STRING)),'/') inbound_set_flight_number
                 ,STRING_AGG(DISTINCT(      leg.data_origin_flg)         ,'/') inbound_set_origin_system
                 ,min(leg.flight_date)       inbound_min_flight_date
                 ,max(leg.flight_date)       inbound_max_flight_date
                 ,max(leg.awb_pieces_qty)    inbound_awb_pieces_qty
                 ,max(leg.awb_kg_chargeable) inbound_awb_kg_chargeable
                  --
                 ,MIN(CAST(CONCAT(SUBSTR(CAST(TRIC_FCH_SALIDA_REAL_LOCAL AS STRING),1,10),'T',TIME_ADD(TIME "00:00:00", INTERVAL CAST(TRIC_HRA_SALIDA_REAL_LOCAL AS INT64) SECOND)) AS datetime )) as inbound_awb_departure_dt
                 ,MAX(CAST(CONCAT(SUBSTR(CAST(TRIC_FCH_ARRIBO_REAL_LOCAL AS STRING),1,10),'T',TIME_ADD(TIME "00:00:00", INTERVAL CAST(TRIC_HRA_ARRIBO_REAL_LOCAL AS INT64) SECOND)) AS datetime )) as inbound_awb_arrival_dt                  
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
          ) in_awb       
           on in_awb.inbound_doc_prefix = SAFE_CAST(SUBSTR(LPAD(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(f.inbound_awb_code,'-',''),' ',''),'"',''),11,'0'),1,3) AS INTEGER) and 
              in_awb.inbound_doc_number = SAFE_CAST(SUBSTR(LPAD(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(f.inbound_awb_code,'-',''),' ',''),'"',''),11,'0'),4,8) AS NUMERIC);
--FROM FINAL_3 F;

DROP TABLE IF EXISTS  `bc-te-dlake-dev-s7b3.cln_etracking_us.tmp_exchange`;


CREATE TABLE if not exists `bc-te-dlake-dev-s7b3.cln_etracking_us.etracking_timezone`
as SELECT  'SSC'               master_process_code, 
           'EXCHANGE'          process_code, 
            current_datetime ("America/Santiago") chilean_update_dt,
            current_datetime ("America/Sao_Paulo") brazilian_update_dt,
            current_datetime ("America/New_York") american_update_dt, 
            current_datetime ("Europe/Madrid") european_update_dt,
            current_datetime ("America/Lima") peruvian_update_dt;
            
DELETE `bc-te-dlake-dev-s7b3.cln_etracking_us.etracking_timezone` 
WHERE master_process_code = 'SSC' 
AND   process_code        = 'EXCHANGE';

INSERT INTO `bc-te-dlake-dev-s7b3.cln_etracking_us.etracking_timezone` values 
('SSC', 
 'EXCHANGE', 
 current_datetime ("America/Santiago"),  -- santiago
 current_datetime ("America/Sao_Paulo"), -- sAo paulo
 current_datetime ("America/New_York"),  -- miami
 current_datetime ("Europe/Madrid"),     -- madrid
 current_datetime ("America/Lima")       -- lima
);

END;