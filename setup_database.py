"""
Script to set up the DWH table and run the initial MERGE process.
This creates the ORDER_STATUS table in RETAIL.DWH and merges data from EVENTS.
"""

import snowflake.connector
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Snowflake connection
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    database="RETAIL",
    role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
)

def setup_dwh():
    """Create DWH schema and ORDER_STATUS table."""
    cur = conn.cursor()
    try:
        # Create DWH schema
        cur.execute("CREATE SCHEMA IF NOT EXISTS RETAIL.DWH")
        print("Created DWH schema")
        
        # Create ORDER_STATUS table
        cur.execute("""
        CREATE OR REPLACE TABLE RETAIL.DWH.ORDER_STATUS (
          ORDER_ID NUMBER PRIMARY KEY,
          CUSTOMER_ID NUMBER,
          PREVIOUS_STATUS STRING,
          CURRENT_STATUS STRING,
          LAST_UPDATE_TS DATE
        )
        """)
        print("Created ORDER_STATUS table")
        
    finally:
        cur.close()

def run_merge():
    """Run the MERGE process to populate ORDER_STATUS from EVENTS."""
    cur = conn.cursor()
    try:
        # MERGE query
        merge_sql = """
        MERGE INTO RETAIL.DWH.ORDER_STATUS AS T
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
          VALUES (S.ORDER_ID, S.CUSTOMER_ID, NULL, S.NEW_STATUS, S.LAST_EVENT_TS)
        """
        
        cur.execute(merge_sql)
        print("MERGE completed successfully")
        
        # Check results
        cur.execute("SELECT COUNT(*) FROM RETAIL.DWH.ORDER_STATUS")
        count = cur.fetchone()[0]
        print(f"ORDER_STATUS table now has {count} rows")
        
    finally:
        cur.close()

if __name__ == "__main__":
    print("Setting up DWH table and running MERGE...")
    setup_dwh()
    run_merge()
    print("Setup completed!")
    conn.close()
