import json
import random
import time
from kafka import KafkaConsumer, KafkaProducer

# Kafka Configuration
ORDER_KAFKA_TOPIC = "order-events"
INVENTORY_KAFKA_TOPIC = "inventory-events"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"


def check_inventory(order, failure_rate, throttle_ms):

    # Simulate processing time
    if throttle_ms > 0:
        time.sleep(throttle_ms / 1000.0)
    
    # Simulate inventory check
    items_available = random.random() > failure_rate
    status = "available" if items_available else "out_of_stock"
    
    return {
        "order_id": order["order_id"],
        "restaurant": order["restaurant"],
        "inventory_status": status,
        "items_available": items_available,
        "timestamp": time.time()
    }


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--group', type=str, default='inventory-group')
    parser.add_argument('--throttle', type=int, default=0)
    parser.add_argument('--failure-rate', type=float, default=0.1)
    parser.add_argument('--max-messages', type=int, default=None)
    parser.add_argument('--server', type=str, default=KAFKA_BOOTSTRAP_SERVERS)
    args = parser.parse_args()
    
    # Initialize consumer and producer
    consumer = KafkaConsumer(
        ORDER_KAFKA_TOPIC,
        bootstrap_servers=args.server,
        group_id=args.group,
        auto_offset_reset='earliest'
    )
    
    producer = KafkaProducer(bootstrap_servers=args.server)
    
    print(f"Inventory Consumer started (Group: {args.group})")
    print(f"Failure rate: {args.failure_rate*100}%")
    if args.throttle > 0:
        print(f"Throttle: {args.throttle}ms")
    
    processed = 0
    failed = 0
    start_time = time.time()

    # Process orders
    try:
        for message in consumer:
            order = json.loads(message.value.decode())
            
            # Check inventory
            inventory_result = check_inventory(order, args.failure_rate, args.throttle)
            
            # Send result
            producer.send(
                INVENTORY_KAFKA_TOPIC,
                json.dumps(inventory_result).encode("utf-8")
            )
            
            processed += 1
            if not inventory_result["items_available"]:
                failed += 1
            
            if processed % 100 == 0:
                producer.flush()
            
            if processed % 1000 == 0:
                elapsed = time.time() - start_time
                rate = processed / elapsed if elapsed > 0 else 0
                print(f"Processed: {processed} | Rate: {rate:.2f} msg/s | Failed: {failed}")
            
            if args.max_messages and processed >= args.max_messages:
                break

    except KeyboardInterrupt:
        print("\nShutting down gracefully...")
    finally:
        producer.flush()
        consumer.close()

    elapsed = time.time() - start_time
    
    print(f"\nCompleted!")
    print(f"  Total: {processed} orders")
    print(f"  Available: {processed - failed}")
    print(f"  Out of Stock: {failed}")
    if processed > 0:
        print(f"  Failure Rate: {(failed/processed*100):.2f}%")
    print(f"  Time: {elapsed:.2f}s")


if __name__ == "__main__":
    main()