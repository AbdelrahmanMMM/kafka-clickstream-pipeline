import json 
import time
import random

from confluent_kafka import Producer


config = {
    "bootstrap.servers": "localhost:9094",
    "client.id": "python-producer-2",

    "acks": "all",
    "enable.idempotence": True,
    "retries": 5,
    "max.in.flight.requests.per.connection": 1,

    "linger.ms": 20,
}

producer = Producer(config)

topic = "clickstream_events"

users = [f"user-{i}" for i in range(1, 101)]
pages = ["/home", "/products", "/cart", "/search", "/profile"]
devices = ["mobile", "desktop", "tablet"]
countries = ["EG", "SA", "AE", "US", "UK"]
events = ["page_view", "add_to_cart", "purchase", "search", "login", "logout"]

def generate_event():
    event = random.choice(events)
                          
    data = {
        "user_id": random.choice(users),
        "event": event,
        "device": random.choice(devices),
        "country": random.choice(countries),
        "timestamp": time.time()
    }                          

    if event == "page_view":
        data["page"] = random.choice(pages)

    elif event == "add_to_cart":
        data["product_id"] = f"prod-{random.randint(100, 999)}"

    elif event == "purchase":
        data["order_id"] = f"ord-{random.randint(1000, 9999)}"
        data["amount"] = round(random.uniform(10, 500), 2)

    elif event == "search":
        data["query"] = random.choice(["kafka", "python", "laptop", "phone"])

    return data

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery Failed: {err}")
    else:
        print(f'Delivered to "{msg.topic()}" [partition {msg.partition()}] @ offset {msg.offset()}')



clickstream_data = [
    {"user_id": "user-1", "event": "page_view", "page": "/products"},
    {"user_id": "user-2", "event": "page_view", "page": "/home"},
    {"user_id": "user-1", "event": "add_to_cart", "product_id": "prod-123"},
    {"user_id": "user-3", "event": "search", "query": "kafka python"},
    {"user_id": "user-2", "event": "purchase", "order_id": "ord-456"},
    {"user_id": "user-1", "event": "logout"},
]

print("Starting to produce messages...")
for i in range(200):
    data = generate_event()
    data["timestamp"] = time.time()

    key = data["user_id"]
    value = json.dumps(data)

    producer.produce(
        topic,
        key = key.encode("utf-8"),
        value = value.encode("utf-8"),
        callback = delivery_report
    )

    producer.poll(0)
    time.sleep(1.5)

print("\nFlushing... waiting for all acknowledgements.")
remaining = producer.flush(timeout=10)
if remaining > 0:
    print(f"Warning: {remaining} messages were not delivered!")
else:
    print("All messages successfully produced.")
