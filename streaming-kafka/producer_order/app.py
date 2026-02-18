"""
Campus Food Order Producer
Generates student orders and publishes to Kafka
"""
import json
import random
import time
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from kafka import KafkaProducer
from common.ids import CAMPUS_RESTAURANTS

# Kafka Configuration
ORDER_KAFKA_TOPIC = "order-events"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

# Restaurant data
RESTAURANTS = list(CAMPUS_RESTAURANTS.keys())


def generate_order(order_id):
    """Generate a random campus food order"""
    restaurant = random.choice(RESTAURANTS)
    items = random.sample(CAMPUS_RESTAURANTS[restaurant], k=random.randint(1, 3))
    
    return {
        "order_id": f"ORD-{order_id:06d}",
        "student_id": f"STU-{(order_id % 1000):04d}",
        "restaurant": restaurant,
        "items": items,
        "total_amount": round(random.uniform(8.99, 45.99), 2),
        "status": "placed",
        "timestamp": time.time()
    }


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--count', type=int, default=100)
    parser.add_argument('--delay', type=int, default=0)
    parser.add_argument('--server', type=str, default=KAFKA_BOOTSTRAP_SERVERS)
    args = parser.parse_args()
    
    # Initialize producer
    producer = KafkaProducer(bootstrap_servers=args.server)
    
    print(f"Producing {args.count} orders to topic: {ORDER_KAFKA_TOPIC}")
    if args.delay > 0:
        print(f"Delay: {args.delay}ms between orders")
    
    start_time = time.time()
    
    # Produce orders
    for i in range(args.count):
        order = generate_order(i)
        producer.send(ORDER_KAFKA_TOPIC, json.dumps(order).encode("utf-8"))
        
        if (i + 1) % 1000 == 0:
            print(f"Sent {i + 1} orders...")
        
        if args.delay > 0:
            time.sleep(args.delay / 1000.0)
    
    producer.flush()
    elapsed = time.time() - start_time
    
    print(f"\nCompleted!")
    print(f"  Total: {args.count} orders")
    print(f"  Time: {elapsed:.2f}s")
    print(f"  Rate: {args.count/elapsed:.2f} orders/sec")


if __name__ == "__main__":
    main()
