"""
Kafka producer that reads existing orders from Snowflake and generates status events.
This creates a streaming scenario for requirement 5.
"""

import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
from kafka import KafkaProducer

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

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers="localhost:19092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def get_existing_orders():
    """Get existing orders from Snowflake to generate events for them."""
    cur = conn.cursor()
    try:
        cur.execute("SELECT ORDER_ID, CUSTOMER_ID FROM RETAIL.RAW.\"ORDER\" LIMIT 20")
        rows = cur.fetchall()
        return [{"ORDER_ID": row[0], "CUSTOMER_ID": row[1]} for row in rows]
    finally:
        cur.close()

def generate_status_events_stream(orders):
    """Generate status events and send them to Kafka topic."""
    statuses = ["CREATED", "PAID", "PACKED", "SHIPPED", "DELIVERED"]
    topic = "orders"
    
    print(f"Starting Kafka producer for {len(orders)} orders...")
    print(f"Sending events to topic: {topic}")
    print("Generating events every 2 seconds...")
    
    for order in orders:
        # Generate 2-4 events per order
        num_events = random.randint(2, 4)
        selected_statuses = random.sample(statuses, num_events)
        
        for status in selected_statuses:
            event = {
                "event_id": str(uuid.uuid4()),
                "order_id": order["ORDER_ID"],
                "customer_id": order["CUSTOMER_ID"],
                "new_status": status,
                "status_ts": datetime.now(timezone.utc).isoformat(),
                "source": "kafka_producer"
            }
            
            # Send to Kafka
            producer.send(topic, value=event)
            print(f"Sent: Order {order['ORDER_ID']} -> {status}")
            
            # Wait 2 seconds between events
            time.sleep(2)
    
    producer.flush()
    print("All events sent to Kafka!")

if __name__ == "__main__":
    try:
        orders = get_existing_orders()
        if not orders:
            print("No orders found. Please run the flocon_script.py first.")
            exit(1)
        
        generate_status_events_stream(orders)
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        producer.close()
        conn.close()
