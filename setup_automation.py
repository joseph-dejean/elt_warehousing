"""
Script to set up Snowflake Stream and Task for automation.
This creates the automation process for requirement 6.
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

def setup_stream():
    """Create a stream on the EVENTS table."""
    cur = conn.cursor()
    try:
        # Create stream
        cur.execute("""
        CREATE OR REPLACE STREAM RETAIL.RAW.EVENTS_STRM
          ON TABLE RETAIL.RAW.EVENTS
          APPEND_ONLY = TRUE
        """)
        print("Created stream: RETAIL.RAW.EVENTS_STRM")
        
    finally:
        cur.close()

def setup_task():
    """Create and schedule the automation task."""
    cur = conn.cursor()
    try:
        # Create task
        cur.execute("""
        CREATE OR REPLACE TASK RETAIL.DWH.TASK_AUTO_UPDATE_ORDER_STATUS
          WAREHOUSE = COMPUTE_WH
          SCHEDULE = 'USING CRON */2 * * * * Europe/Paris'
          COMMENT = 'Automatically update ORDER_STATUS from EVENTS every 2 minutes'
        AS
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
        """)
        print("Created task: RETAIL.DWH.TASK_AUTO_UPDATE_ORDER_STATUS")
        
        # Enable the task
        cur.execute("ALTER TASK RETAIL.DWH.TASK_AUTO_UPDATE_ORDER_STATUS RESUME")
        print("Task enabled and scheduled to run every 2 minutes")
        
    finally:
        cur.close()

def check_automation():
    """Check the status of the automation setup."""
    cur = conn.cursor()
    try:
        # Check stream
        cur.execute("SHOW STREAMS IN SCHEMA RETAIL.RAW")
        streams = cur.fetchall()
        print(f"\nStreams in RETAIL.RAW: {len(streams)}")
        for stream in streams:
            print(f"  - {stream[1]}.{stream[2]}")
        
        # Check tasks
        cur.execute("SHOW TASKS IN SCHEMA RETAIL.DWH")
        tasks = cur.fetchall()
        print(f"\nTasks in RETAIL.DWH: {len(tasks)}")
        for task in tasks:
            print(f"  - {task[1]}.{task[2]} (State: {task[6]})")
            
    finally:
        cur.close()

if __name__ == "__main__":
    print("Setting up Snowflake automation...")
    setup_stream()
    setup_task()
    check_automation()
    print("\nSnowflake automation setup completed!")
    print("The task will automatically update ORDER_STATUS every 2 minutes")
    conn.close()
