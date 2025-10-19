"""consumer_to_snowflake.py
A simple Kafka consumer that reads order events from a Kafka topic and writes them to Snowflake.
It batches events for efficiency and merges them into a target DWH table.
run docker logs -f consumer to see the events being consumed
"""

import json
import os
import random
import time
from collections import deque
from datetime import datetime, timezone

import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
from kafka import KafkaConsumer
from snowflake.connector.pandas_tools import write_pandas

BATCH_SIZE = 200
BATCH_SECONDS = 10

# Load env from .env if present
load_dotenv()
TOPIC_NAME = os.getenv("KAFKA_TOPIC_NAME")  # set a default topic name in the .env file

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="latest",
    enable_auto_commit=True,
    group_id="snowflake_loader",
)

conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database="RETAIL",
    schema="RAW",
    role=os.getenv("SNOWFLAKE_ROLE"),
)


def merge_raw_into_dwh(conn):
    """Merge strategy to upload raw data into the DWH table.
    - For each order, keep only the latest event (status + ts stay consistent)
    - Update the DWH table only if the event is newer and the status has changed
    - Insert new orders if they do not exist yet
    """
    print("Merging raw data into DWH ...")
    target_table = "RETAIL.DWH.ORDER_STATUS"
    source_table = "RETAIL.RAW.EVENTS"
    sql = f"""
    MERGE INTO {target_table} AS T
    USING (
        -- Pick the single latest event row per ORDER_ID (status + ts stay consistent)
        SELECT ORDER_ID,
            CUSTOMER_ID,
            NEW_STATUS,
            STATUS_TS AS LAST_EVENT_TS
        FROM (
            SELECT ORDER_ID,
                CUSTOMER_ID,
                NEW_STATUS,
                STATUS_TS,
                ROW_NUMBER() OVER (
                    PARTITION BY ORDER_ID
                    ORDER BY STATUS_TS DESC, EVENT_ID DESC
                ) AS RN
            FROM {source_table}
        )
        WHERE RN = 1
    ) AS S
    ON T.ORDER_ID = S.ORDER_ID

    WHEN MATCHED -- Update process: only if the event is newer and the status has changed
        AND (S.LAST_EVENT_TS > T.LAST_UPDATE_TS OR T.LAST_UPDATE_TS IS NULL)
        AND (T.CURRENT_STATUS <> S.NEW_STATUS OR T.CURRENT_STATUS IS NULL)
    THEN UPDATE SET
        T.PREVIOUS_STATUS = T.CURRENT_STATUS,
        T.CURRENT_STATUS  = S.NEW_STATUS,
        T.LAST_UPDATE_TS  = S.LAST_EVENT_TS,
        T.CUSTOMER_ID     = S.CUSTOMER_ID

    WHEN NOT MATCHED THEN -- Insert process: new order
        INSERT (ORDER_ID, CUSTOMER_ID, PREVIOUS_STATUS, CURRENT_STATUS, LAST_UPDATE_TS)
        VALUES (S.ORDER_ID, S.CUSTOMER_ID, NULL, S.NEW_STATUS, S.LAST_EVENT_TS)
    ;
    """
    cur = conn.cursor()
    try:
        cur.execute(sql)
    finally:
        cur.close()


def flush_batch(conn, buffer):
    """ "
    Flush the current buffer to Snowflake and clear it.
    """
    raw_table = "EVENTS"
    raw_database = "RETAIL"
    raw_schema = "RAW"
    if not buffer:
        return
    df = pd.DataFrame(buffer)
    
    # Set schema context
    cur = conn.cursor()
    cur.execute("USE SCHEMA RETAIL.RAW")
    cur.close()
    
    write_pandas(
        conn,
        df,
        raw_table,
        database=raw_database,
        schema=raw_schema,
        auto_create_table=False,
        overwrite=False,
    )
    print(f"Inserted {len(df)} rows into {raw_database}.{raw_schema}.{raw_table}")
    merge_raw_into_dwh(conn)
    print("Merged raw data into DWH")
    buffer.clear()  # clear the buffer after flushing


if __name__ == "__main__":
    buffer = deque()  # buffer to hold events before batch writing
    last_flush = time.time()
    print(f"Consuming from topic '{TOPIC_NAME}' and writing to Snowflake ... Ctrl+C to stop.")

    try:
        for msg in consumer:
            evt = msg.value
            buffer.append(
                {
                    "EVENT_ID": random.randint(100000, 999999),  # Use number instead of UUID
                    "ORDER_ID": evt["order_id"],
                    "CUSTOMER_ID": evt["customer_id"],
                    "NEW_STATUS": evt["new_status"],
                    "STATUS_TS": datetime.now(timezone.utc).date(),  # Use date instead of timestamp
                }
            )

            if (
                len(buffer) >= BATCH_SIZE or (time.time() - last_flush) >= BATCH_SECONDS
            ):  # HERE I have set a time to flush and a batch size to process
                # (to prevent too many small writes or too large ones) (In production you would use a more robust mechanism)
                flush_batch(conn, buffer)
                last_flush = time.time()
                print(f"Flushed {BATCH_SIZE} events to Snowflake at {last_flush}")
    except KeyboardInterrupt:
        pass
    finally:
        flush_batch(conn, buffer)
        conn.close()
