import json
import time
from confluent_kafka import Consumer, KafkaException, KafkaError
import psycopg2
from datetime import datetime

conn = psycopg2.connect(
    host="localhost",
    database="kafka_db",
    user="kafka_user",
    password="kafka_pass"
)

cursor = conn.cursor()

conf = {
    "bootstrap.servers": "localhost:9094",
    "group.id": "clickstream-processing-group-1",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}

consumer = Consumer(conf)

topic = "clickstream_events"
consumer.subscribe([topic])
print(f"Subscribed to '{topic}'. Waiting for messages...")

count = 0
try:
    
    while True:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"Reached end of partition {msg.topic()} [{msg.partition()}]")
            else:
                raise KafkaException(msg.error())
        else:
            user_id = msg.key().decode("utf-8")
            data = json.loads(msg.value().decode("utf-8"))

            cursor.execute(
                """
                insert into events (
                    user_id, event, page, product_id, order_id,
                    amount, device, country, timestamp
                ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                data.get("user_id"),
                data.get("event"),
                data.get("page"),
                data.get("product_id"),
                data.get("order_id"),
                data.get("amount"),
                data.get("device"),
                data.get("country"),
                datetime.fromtimestamp(data.get("timestamp"))

                )
            )

            cursor.execute(
                """
                insert into user_metrics (user_id, total_events)
                values (%s, 1)
                on conflict (user_id)
                do update set total_events = user_metrics.total_events + 1
                """, (data.get("user_id"),)
            )

            if data.get("event") == "page_view" and data.get("page"):
                cursor.execute(
                    """
                    insert into page_metrics (page, total_views)
                    values (%s, 1)
                    on conflict (page)
                    do update set total_views = page_metrics.total_views + 1 
                    """,
                    (data.get("page"),)
                )

            print(
                f"Received: user={user_id} "
                f"| event={data['event']} "
                f"| partition={msg.partition()} "
                f"| offset={msg.offset()}"
            )

            count +=1

            time.sleep(0.1)
            
            if count % 5 == 0:
                conn.commit()
                consumer.commit(asynchronous=False)


except KeyboardInterrupt:
    print("\nShutting down...")            
finally:
    print("Closing consumer.")    
    conn.commit()
    consumer.commit(asynchronous=False)
    consumer.close()
