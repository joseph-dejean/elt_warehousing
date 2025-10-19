import datetime
import random
import os

import pandas as pd
from faker import Faker
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv


def gaussian_clamped(rng: random.Random, mu: float, sigma: float, a: float, b: float) -> float:
    # Box-Muller with clamp
    val = rng.gauss(mu, sigma)
    return max(a, min(b, val))


def generate_orders(
    orders: int,
    seed: int = 42,
    start: str = "2024-01-01",
    end: str = "2025-09-01",
    inventory: pd.DataFrame = None,
    customers: pd.DataFrame = None,
) -> pd.DataFrame:
    """
    Generate orders data and return as a pandas DataFrame.

    Args:
        orders: Number of orders to generate
        seed: Random seed for reproducibility
        start: Start date (YYYY-MM-DD)
        end: End date (YYYY-MM-DD)
        inventory: Path to inventory_data.csv (optional, improves realism)
        customers: Path to customers.csv (optional, improves realism)

    Returns:
        pandas.DataFrame: DataFrame containing orders data
    """
    rng = random.Random(seed)

    start_dt = datetime.datetime.fromisoformat(start).replace(tzinfo=datetime.timezone.utc)
    end_dt = datetime.datetime.fromisoformat(end).replace(tzinfo=datetime.timezone.utc)
    delta_seconds = int((end_dt - start_dt).total_seconds())

    # Load lookups if available
    product_ids = list(range(1000, 1250))
    prices = {}
    if inventory is not None and not inventory.empty:
        product_ids = inventory["product_id"].tolist()
        prices = dict(zip(inventory["product_id"], inventory["unit_price"], strict=True))

    customer_ids = list(range(1, 1001))
    if customers is not None and not customers.empty:
        customer_ids = customers["customer_id"].tolist()

    rows = []
    for i in range(1, orders + 1):
        pid = rng.choice(product_ids)
        qty = rng.choices([1, 2, 3, 4, 5], weights=[0.6, 0.2, 0.12, 0.06, 0.02])[0]
        sold_at = start_dt + datetime.timedelta(seconds=rng.randint(0, delta_seconds))
        cid = rng.choice(customer_ids)
        unit_price = prices.get(pid, round(rng.uniform(5, 500), 2))
        order_total = qty * unit_price
        rows.append(
            {
                # These column names are being corrected to match the Snowflake table
                "ORDER_ID": i,
                "PRODUCT_ID": pid,
                "CUSTOMER_ID": cid,
                "QUANTITY": qty,
                "UNIT_PRICE": unit_price,
                "ORDER_TOTAL": order_total, # This column must be added to match the table
                "SOLD_AT": sold_at,
            }
        )

    df = pd.DataFrame(rows)

    # Ensure timestamp column is properly formatted
    df["SOLD_AT"] = pd.to_datetime(df["SOLD_AT"])

    print(f"✅ Generated {len(df)} orders as DataFrame")
    return df


def generate_inventory_data(products: int, seed: int = 42) -> pd.DataFrame:
    """
    Generate inventory data and return as a pandas DataFrame.

    Args:
        products: Number of products to generate
        seed: Random seed for reproducibility

    Returns:
        pandas.DataFrame: DataFrame containing inventory data
    """
    rng = random.Random(seed)

    rows = []
    product_ids = list(range(1000, 1000 + products))
    for pid in product_ids:
        cat, names = rng.choice(CATEGORIES)
        base = rng.choice(names)
        adj = rng.choice(ADJECTIVES)
        product_name = f"{adj} {base}"
        # Base price by category with some variance
        base_price = {"Apparel": 39, "Electronics": 299, "Home & Kitchen": 79, "Beauty": 25, "Grocery": 12}[cat]
        price = round(gaussian_clamped(rng, base_price, base_price * 0.25, base_price * 0.4, base_price * 1.8), 2)
        # Stock skewed: long tail
        stock_qty = int(gaussian_clamped(rng, 80, 60, 0, 400))
        rows.append(
            {
                "product_id": pid,
                "product_name": product_name,
                "category": cat,
                "unit_price": price,
                "stock_quantity": stock_qty,
            }
        )

    df = pd.DataFrame(rows)
    print(f"✅ Generated {len(df)} inventory data as DataFrame")
    return df


def generate_customers(customers: int, seed: int = 42) -> pd.DataFrame:
    """
    Generate customer data and return as a pandas DataFrame.

    Args:
        customers: Number of customers to generate
        seed: Random seed for reproducibility

    Returns:
        pandas.DataFrame: DataFrame containing customer data
    """
    rng = random.Random(seed)
    fake = Faker()
    Faker.seed(seed)

    rows = []
    channels = [("online", 0.65), ("store", 0.35)]

    for cid in range(1, customers + 1):
        name = fake.name()
        email = fake.email()
        city = fake.city()
        channel = rng.choices([c for c, _ in channels], weights=[w for _, w in channels])[0]
        rows.append(
            {
                "customer_id": cid,
                "name": name,
                "email": email,
                "city": city,
                "channel": channel,
            }
        )

    df = pd.DataFrame(rows)
    print(f"✅ Generated {len(df)} customers as DataFrame")
    return df


CATEGORIES = [
    ("Apparel", ["T-Shirt"]),
]
ADJECTIVES = ["Classic", "Premium", "Eco", "Urban", "Sport", "Comfort", "Pro", "Lite", "Max", "Essential"]


def load_dataframe_to_snowflake(df, table_name, conn_params):
    """
    Loads a pandas DataFrame into a Snowflake table.
    """
    print(f"Connecting to Snowflake...")
    conn = None
    try:
        conn = snowflake.connector.connect(**conn_params)
        print("Successfully connected to Snowflake.")
        
        # Set the schema context to avoid temp stage issues
        cur = conn.cursor()
        cur.execute(f"USE SCHEMA {conn_params['database']}.{conn_params['schema']}")
        cur.close()
        
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=table_name,
            database=conn_params['database'],
            schema=conn_params['schema'],
            overwrite=True,
            use_logical_type=True
        )

        print(f"✅ Loaded {nrows} rows into {table_name}")
        
    except Exception as e:
        print(f"Error loading data to Snowflake: {e}")
    finally:
        if conn:
            conn.close()
            print("Connection to Snowflake closed.")


if __name__ == "__main__":
    # ----------------------------------------
    # 1. Generate the data
    # ----------------------------------------
    print("--- Starting Data Generation ---")
    customers_df = generate_customers(customers=100, seed=42)
    inventory_df = generate_inventory_data(products=100, seed=42)
    orders_df = generate_orders(orders=100, seed=42, inventory=inventory_df, customers=customers_df)
    
    # Correct column names to match the Snowflake table
    orders_df = orders_df.rename(columns={
        "id": "ORDER_ID",
        "product_id": "PRODUCT_ID",
        "customer_id": "CUSTOMER_ID",
        "quantity": "QUANTITY",
        "unit_price": "UNIT_PRICE",
        "order_total": "ORDER_TOTAL",
        "sold_at": "SOLD_AT"
    })
    
    # Ensure correct column order
    orders_df = orders_df[[
        "ORDER_ID", "CUSTOMER_ID", "PRODUCT_ID", "QUANTITY", 
        "UNIT_PRICE", "ORDER_TOTAL", "SOLD_AT"
    ]]


    # ----------------------------------------
    # 2. Load the data into Snowflake
    # ----------------------------------------
    print("\n--- Starting Data Loading to Snowflake ---")

    # Load credentials from environment (.env supported)
    load_dotenv()
    SNOWFLAKE_CONN_PARAMS = {
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        "database": os.getenv("SNOWFLAKE_DATABASE", "RETAIL_LAB"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
        "role": os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    }

    # Call the function to load the data
    load_dataframe_to_snowflake(
        df=orders_df,
        table_name="ORDER",
        conn_params=SNOWFLAKE_CONN_PARAMS
    )