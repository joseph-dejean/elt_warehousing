import os
import pandas as pd
import snowflake.connector
import streamlit as st
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

st.set_page_config(page_title="Order Status Monitor", layout="wide")

@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database="RETAIL",
        schema="DWH",
        role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    )

@st.cache_data(ttl=15)
def load_data():
    conn = get_connection()
    q = """
      SELECT ORDER_ID, CUSTOMER_ID, CURRENT_STATUS, PREVIOUS_STATUS, LAST_UPDATE_TS
      FROM RETAIL.DWH.ORDER_STATUS
      ORDER BY LAST_UPDATE_TS DESC
      LIMIT 1000
    """
    cur = conn.cursor()
    try:
        cur.execute(q)
        cols = [c[0] for c in cur.description]
        rows = cur.fetchall()
        return pd.DataFrame(rows, columns=cols)
    finally:
        cur.close()

@st.cache_data(ttl=15)
def load_events():
    conn = get_connection()
    q = """
      SELECT COUNT(*) as total_events, 
             COUNT(DISTINCT ORDER_ID) as unique_orders,
             MAX(STATUS_TS) as latest_event
      FROM RETAIL.RAW.EVENTS
    """
    cur = conn.cursor()
    try:
        cur.execute(q)
        row = cur.fetchone()
        return {
            "total_events": row[0],
            "unique_orders": row[1], 
            "latest_event": row[2]
        }
    finally:
        cur.close()

st.title("📊 Order Status Monitoring Dashboard")

# Load data
df = load_data()
events_stats = load_events()

# Metrics row
col1, col2, col3, col4 = st.columns(4)
with col1: 
    st.metric("Tracked Orders", f"{df['ORDER_ID'].nunique()}")
with col2: 
    st.metric("Total Events", f"{events_stats['total_events']}")
with col3: 
    st.metric("Latest Update", str(df["LAST_UPDATE_TS"].max()) if len(df) > 0 else "N/A")
with col4: 
    st.metric("Unique Orders in Events", f"{events_stats['unique_orders']}")

# Status distribution chart
st.subheader("📈 Status Distribution")
if len(df) > 0:
    status_counts = df["CURRENT_STATUS"].value_counts()
    st.bar_chart(status_counts)
else:
    st.write("No data available")

# Data table
st.subheader("📋 Order Status Details")
if len(df) > 0:
    st.dataframe(df, use_container_width=True)
else:
    st.write("No data available")

# Raw events info
st.subheader("🔍 Raw Events Summary")
st.write(f"**Total Events in RAW.EVENTS:** {events_stats['total_events']}")
st.write(f"**Latest Event:** {events_stats['latest_event']}")
st.write(f"**Orders with Events:** {events_stats['unique_orders']}")


