import json
import time
import pandas as pd
from kafka import KafkaProducer
import mysql.connector

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

db = mysql.connector.connect(
    host="mysql",
    user="root",
    password="root",
    database="aqi_monitoring"
)

cursor = db.cursor()

cursor.execute("""
SELECT last_row
FROM producer_checkpoint
WHERE id=1
""")

start_index = cursor.fetchone()[0]

print(f"Starting from row {start_index}")



# Read CSV
df = pd.read_csv("data/AQI_dataset.csv")

topic = "aqi-topic"
batch_size = 100

print(f"Sending {len(df)} records to Kafka in batches of {batch_size}...")

# Send data in batches
for start in range(start_index, len(df), batch_size):
    batch = df.iloc[start:start + batch_size]

    futures = []

    for _, row in batch.iterrows():
        future = producer.send(topic, row.to_dict())
        futures.append(future)
        print(row.to_dict())

    # Ensure all messages in the batch are delivered
    for future in futures:
        metadata = future.get(timeout=10)

    producer.flush()
    next_row = start + len(batch)

    cursor.execute("""
    UPDATE producer_checkpoint
    SET last_row=%s
    WHERE id=1
    """, (next_row,))

    db.commit()

    print(f"Checkpoint updated to {next_row}")
    print(f"Batch {(start // batch_size) + 1} sent ({len(batch)} records).")

    # Pause before sending the next batch
    time.sleep(1)

print("All records sent successfully.")
cursor.close()
db.close()
producer.close()
