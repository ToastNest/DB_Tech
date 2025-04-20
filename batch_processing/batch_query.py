import pandas as pd
import psycopg2
import time

df = pd.read_csv("../data/sales_data.csv")

# Insert into PostgreSQL
conn = psycopg2.connect(
    dbname="sales_data",
    user="postgres",
    password="postgres",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

# Create table (once)
cur.execute("""
    CREATE TABLE IF NOT EXISTS orders (
        order_id TEXT,
        date TEXT,
        status TEXT,
        sales_channel TEXT,
        category TEXT,
        qty INT,
        amount FLOAT
    )
""")
# Create table if it doesn't exist
cur.execute("""
    CREATE TABLE IF NOT EXISTS batch_orders (
        order_id TEXT,
        category TEXT,
        amount FLOAT
    )
""")
conn.commit()

# Insert into batch_orders
start = time.time()
for _, row in df.iterrows():
    cur.execute("""
        INSERT INTO batch_orders (order_id, category, amount)
        VALUES (%s, %s, %s)
    """, (row['Order ID'], row['Category'], row['Amount']))
conn.commit()
print("Batch insert time:", time.time() - start)

# Optional query example
start = time.time()
cur.execute("""
    SELECT category, SUM(amount) AS total_revenue
    FROM batch_orders
    GROUP BY category
    ORDER BY total_revenue DESC
    LIMIT 3;
""")
result = cur.fetchall()
print("Batch query time:", time.time() - start)
print(result)
conn.commit()