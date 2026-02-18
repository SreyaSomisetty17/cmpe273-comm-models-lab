"""
Analytics Consumer
Computes metrics from order and inventory streams
"""
import json
import time
from collections import defaultdict
from kafka import KafkaConsumer

# Kafka Configuration
ORDER_KAFKA_TOPIC = "order-events"
INVENTORY_KAFKA_TOPIC = "inventory-events"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"


def save_metrics(metrics, filename="metrics_report.txt"):
    """Save metrics to file"""
    with open(filename, 'w') as f:
        f.write("=" * 70 + "\n")
        f.write("ANALYTICS METRICS REPORT\n")
        f.write("=" * 70 + "\n\n")
        
        f.write("OVERALL METRICS:\n")
        f.write(f"  Total Orders: {metrics['total_orders']}\n")
        f.write(f"  Total Inventory Checks: {metrics['total_inventory']}\n")
        f.write(f"  Failed Orders: {metrics['failed_orders']}\n")
        if metrics['total_inventory'] > 0:
            rate = (metrics['failed_orders'] / metrics['total_inventory']) * 100
            f.write(f"  Failure Rate: {rate:.2f}%\n")

        if metrics['minute_buckets']:
            opm = list(metrics['minute_buckets'].values())
            f.write(f"  Avg Orders/Min: {sum(opm)/len(opm):.1f}\n")
            f.write(f"  Peak Orders/Min: {max(opm)}\n")
        
        f.write("\nORDERS BY RESTAURANT:\n")
        for restaurant, count in sorted(
            metrics['orders_by_restaurant'].items(),
            key=lambda x: x[1],
            reverse=True
        ):
            revenue = metrics['revenue_by_restaurant'][restaurant]
            f.write(f"  {restaurant}: {count} orders (${revenue:.2f})\n")
        
        f.write("\n" + "=" * 70 + "\n")
    
    print(f"Metrics saved to {filename}")


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--group', type=str, default='analytics-group')
    parser.add_argument('--max-messages', type=int, default=None)
    parser.add_argument('--server', type=str, default=KAFKA_BOOTSTRAP_SERVERS)
    args = parser.parse_args()
    
    # Initialize consumer
    consumer = KafkaConsumer(
        ORDER_KAFKA_TOPIC,
        INVENTORY_KAFKA_TOPIC,
        bootstrap_servers=args.server,
        group_id=args.group,
        auto_offset_reset='earliest'
    )
    
    print(f"Analytics Consumer started (Group: {args.group})")
    print("Processing order and inventory events...")
    
    # Metrics storage
    metrics = {
        'total_orders': 0,
        'total_inventory': 0,
        'failed_orders': 0,
        'orders_by_restaurant': defaultdict(int),
        'revenue_by_restaurant': defaultdict(float),
        'minute_buckets': defaultdict(int)
    }
    
    processed = 0
    start_time = time.time()
    
    # Process events
    try:
        for message in consumer:
            event = json.loads(message.value.decode())
            topic = message.topic
            
            if topic == ORDER_KAFKA_TOPIC:
                metrics['total_orders'] += 1
                metrics['orders_by_restaurant'][event['restaurant']] += 1
                metrics['revenue_by_restaurant'][event['restaurant']] += event['total_amount']
                bucket = int(event.get('timestamp', time.time()) // 60)
                metrics['minute_buckets'][bucket] += 1
            
            elif topic == INVENTORY_KAFKA_TOPIC:
                metrics['total_inventory'] += 1
                if not event['items_available']:
                    metrics['failed_orders'] += 1
            
            processed += 1
            
            if processed % 5000 == 0:
                elapsed = time.time() - start_time
                rate = processed / elapsed if elapsed > 0 else 0
                print(f"Processed: {processed} messages ({rate:.2f} msg/s)")
            
            if args.max_messages and processed >= args.max_messages:
                break

    except KeyboardInterrupt:
        print("\nShutting down gracefully...")
    finally:
        consumer.close()
    
    # Display results
    print(f"\n{'='*70}")
    print("ANALYTICS RESULTS")
    print(f"{'='*70}")
    print(f"Total Orders: {metrics['total_orders']}")
    print(f"Total Inventory Checks: {metrics['total_inventory']}")
    print(f"Failed Orders: {metrics['failed_orders']}")
    
    if metrics['total_inventory'] > 0:
        failure_rate = (metrics['failed_orders'] / metrics['total_inventory']) * 100
        print(f"Failure Rate: {failure_rate:.2f}%")

    if metrics['minute_buckets']:
        opm = list(metrics['minute_buckets'].values())
        print(f"Avg Orders/Min : {sum(opm)/len(opm):.1f}")
        print(f"Peak Orders/Min: {max(opm)}")
    
    print(f"\nOrders by Restaurant:")
    for restaurant, count in sorted(
        metrics['orders_by_restaurant'].items(),
        key=lambda x: x[1],
        reverse=True
    ):
        revenue = metrics['revenue_by_restaurant'][restaurant]
        print(f"  {restaurant}: {count} orders (${revenue:.2f})")
    
    print(f"{'='*70}\n")
    
    # Save to file
    save_metrics(metrics)


if __name__ == "__main__":
    main()