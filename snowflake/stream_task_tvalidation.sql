##Events in the raw table
create or replace stream RETAIL.RAW.EVENTS_STRM on table EVENTS append_only = true;







##Task auto_update_order_status

create or replace task RETAIL.DWH.TASK_AUTO_UPDATE_ORDER_STATUS
	warehouse=COMPUTE_WH
	schedule='USING CRON */2 * * * * Europe/Paris'
	COMMENT='Automatically update ORDER_STATUS from EVENTS every 2 minutes'
	as MERGE INTO RETAIL.DWH.ORDER_STATUS AS T
          USING (
            SELECT ORDER_ID, CUSTOMER_ID, NEW_STATUS, STATUS_TS AS LAST_EVENT_TS
            FROM (
              SELECT ORDER_ID, CUSTOMER_ID, NEW_STATUS, STATUS_TS,
                     ROW_NUMBER() OVER (PARTITION BY ORDER_ID ORDER BY STATUS_TS DESC) AS RN
              FROM RETAIL.RAW.EVENTS
            )
            WHERE RN = 1
          ) AS S
          ON T.ORDER_ID = S.ORDER_ID
          WHEN MATCHED
            AND (S.LAST_EVENT_TS > T.LAST_UPDATE_TS OR T.LAST_UPDATE_TS IS NULL)
            AND (T.CURRENT_STATUS <> S.NEW_STATUS OR T.CURRENT_STATUS IS NULL)
          THEN UPDATE SET
            T.PREVIOUS_STATUS = T.CURRENT_STATUS,
            T.CURRENT_STATUS  = S.NEW_STATUS,
            T.LAST_UPDATE_TS  = S.LAST_EVENT_TS,
            T.CUSTOMER_ID     = S.CUSTOMER_ID
          WHEN NOT MATCHED THEN
            INSERT (ORDER_ID, CUSTOMER_ID, PREVIOUS_STATUS, CURRENT_STATUS, LAST_UPDATE_TS)
            VALUES (S.ORDER_ID, S.CUSTOMER_ID, NULL, S.NEW_STATUS, S.LAST_EVENT_TS);








## Validation of the stream task
-- Raw vs DWH counts
SELECT COUNT(*) AS raw_events FROM RETAIL.RAW.ORDER_STATUS_EVENTS;
SELECT COUNT(*) AS dwh_orders FROM RETAIL.DWH.ORDER_STATUS;

-- Freshness
SELECT MAX(LAST_UPDATE_TS) AS dwh_last_update FROM RETAIL.DWH.ORDER_STATUS;

-- Status distribution
SELECT CURRENT_STATUS, COUNT(*)
FROM RETAIL.DWH.ORDER_STATUS
GROUP BY 1
ORDER BY 2 DESC;

-- DWH order ids must exist in RAW.ORDER (if loaded)
SELECT COUNT(*) AS missing_in_raw_orders
FROM RETAIL.DWH.ORDER_STATUS d
LEFT JOIN RETAIL.RAW."ORDER" o ON o.ORDER_ID = d.ORDER_ID
WHERE o.ORDER_ID IS NULL;

-- Latest event per order agrees with DWH
WITH latest AS (
  SELECT ORDER_ID, NEW_STATUS,
         ROW_NUMBER() OVER (PARTITION BY ORDER_ID ORDER BY STATUS_TS DESC) rn
  FROM RETAIL.RAW.EVENTS
)
SELECT COUNT(*) AS mismatches
FROM latest l
JOIN RETAIL.DWH.ORDER_STATUS d USING (ORDER_ID)
WHERE l.rn = 1 AND l.NEW_STATUS <> d.CURRENT_STATUS;


