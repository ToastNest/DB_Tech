import psycopg2
import time

# PostgreSQL connection settings
conn = psycopg2.connect(
    dbname="sales_data",
    user="postgres",
    password="postgres",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

tables = ["batch_orders", "stream_orders"]

# Queries to benchmark
queries = {
    "Total Revenue per Category": """
        SELECT category, SUM(amount) 
        FROM {table} 
        GROUP BY category
    """,
    "Top 3 Categories by Revenue": """
        SELECT category, SUM(amount) AS total_revenue
        FROM {table}
        GROUP BY category
        ORDER BY total_revenue DESC
        LIMIT 3
    """,
    "Average Order Value per Category": """
        SELECT category, AVG(amount)
        FROM {table}
        GROUP BY category
    """,
    "High Value Orders (>1000)": """
        SELECT COUNT(*) 
        FROM {table}
        WHERE amount > 1000
    """,
}

print("\n--- Query Benchmark Results ---\n")

for query_name, query_sql in queries.items():
    print(f"🔍 {query_name}")
    for table in tables:
        formatted_query = query_sql.format(table=table)
        start = time.time()
        cur.execute(formatted_query)
        _ = cur.fetchall()  # Or use fetchone() if COUNT, or skip if only benchmarking
        duration = time.time() - start
        print(f"  {table:<15} -> {duration:.4f} seconds")
    print()

cur.close()
conn.close()
