import pandas as pd
from kafka import KafkaProducer
import json
import time

df = pd.read_csv("../data/sales_data.csv")
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

for _, row in df.iterrows():
    data = {
        "order_id": row["Order ID"],
        "category": row["Category"],
        "amount": row["Amount"]
    }
    print(data)
    producer.send("sales", value=data)
    # time.sleep(0.1)  # Simulate real-time streaming
