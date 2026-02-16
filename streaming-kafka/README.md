# Campus Food Ordering System - Kafka Streaming Implementation

A distributed event-driven system for campus food ordering using Apache Kafka, demonstrating streaming patterns, consumer groups, and real-time analytics.

## 📋 System Overview

### Architecture
```
Student Orders → [Producer] → Kafka (order-events)
                                    ↓
                              [Inventory Consumer] → Kafka (inventory-events)
                                    ↓
                              [Analytics Consumer] → Metrics & Reports
```

### Components

1. **Producer (producer_order/)**: Generates and publishes OrderPlaced events
   - Simulates student orders from 4 campus restaurants
   - Configurable throughput and delay

2. **Inventory Consumer (inventory_consumer/)**: Processes orders and checks inventory
   - Consumes order-events stream
   - Simulates inventory checks with configurable failure rate
   - Publishes inventory-events stream

3. **Analytics Consumer (analytics_consumer/)**: Computes real-time metrics
   - Consumes both order-events and inventory-events
   - Calculates orders per minute
   - Calculates failure rate
   - Generates reports

## 🚀 Setup Instructions

### Prerequisites
- Docker and Docker Compose
- Python 3.8+
- pip

### Step 1: Start Kafka Infrastructure

```bash
cd streaming-kafka
docker-compose up -d
```

Wait for Kafka to be ready (~30 seconds):
```bash
docker-compose logs -f kafka
# Wait until you see "started (kafka.server.KafkaServer)"
```

### Step 2: Install Python Dependencies

```bash
pip install -r requirements.txt
```

### Step 3: Verify Setup

```bash
# Check containers are running
docker-compose ps

# Should show 3 containers:
# - zookeeper
# - kafka  
# - kafka-ui
```

## 📊 Individual Component Usage

### Produce Orders

```bash
# Produce 10,000 orders
python3 producer_order/app.py --count 10000

# Produce with throttling (for lag testing)
python3 producer_order/app.py --count 5000 --delay 10
```

**Arguments:**
- `--count`: Number of orders to produce (default: 100)
- `--delay`: Delay between orders in ms (default: 0)
- `--server`: Kafka server (default: localhost:9092)

### Run Inventory Consumer

```bash
# Normal processing
python3 inventory_consumer/app.py

# With throttling (to create lag)
python3 inventory_consumer/app.py --throttle 50 --max-messages 1000

# With high failure rate
python3 inventory_consumer/app.py --failure-rate 0.3
```

**Arguments:**
- `--group`: Consumer group ID (default: inventory-group)
- `--throttle`: Processing delay in ms (default: 0)
- `--failure-rate`: Inventory failure rate 0-1 (default: 0.1)
- `--max-messages`: Max messages to process (default: infinite)

### Run Analytics Consumer

```bash
# Process and compute metrics
python3 analytics_consumer/app.py --max-messages 10000

# With different consumer group (for replay)
python3 analytics_consumer/app.py --group analytics-v2
```

**Arguments:**
- `--group`: Consumer group ID (default: analytics-group)
- `--max-messages`: Max messages to process (default: infinite)
- `--report-interval`: Show metrics every N messages (default: 5000)

## 🔄 Replay Demonstration

### Method 1: Reset Consumer Offset

```bash
# First run
python3 analytics_consumer/app.py --group replay-demo --max-messages 5000

# Reset offset to beginning
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:29092 \
  --group replay-demo \
  --reset-offsets \
  --to-earliest \
  --all-topics \
  --execute

# Second run (replay) - should produce same metrics
python3 analytics_consumer/app.py --group replay-demo --max-messages 5000
```

### Method 2: Use Different Consumer Group

```bash
# First group reads from latest
python3 analytics_consumer/app.py --group analytics-run1 --max-messages 5000

# Second group reads from beginning (replay)
python3 analytics_consumer/app.py --group analytics-run2 --max-messages 5000
```

## 📊 Component Usage

### Producer - Generate Order Events

**Basic Usage:**
```bash
python3 producer_order/app.py --count 1000
```

**With Throttling (for testing consumer lag):**
```bash
python3 producer_order/app.py --count 5000 --delay 10
```

**Parameters:**
- `--count`: Number of orders to produce (default: 100)
- `--delay`: Delay between orders in ms (default: 0)
- `--server`: Kafka server (default: localhost:9092)

**Example Output:**
```
============================================================
Production Summary:
  Total Orders: 1000
  Successful: 1000
  Failed: 0
  Time Elapsed: 8.52s
  Throughput: 117.37 orders/sec
============================================================
```

---

### Inventory Consumer - Process Orders

**Basic Usage:**
```bash
python3 inventory_consumer/app.py
```

**With Throttling:**
```bash
python3 inventory_consumer/app.py --throttle 50 --max-messages 1000
```

**With High Failure Rate:**
```bash
python3 inventory_consumer/app.py --failure-rate 0.3
```

**Parameters:**
- `--group`: Consumer group ID (default: inventory-group)
- `--throttle`: Processing delay in ms (default: 0)
- `--failure-rate`: Inventory failure rate 0-1 (default: 0.1)
- `--max-messages`: Max messages to process (default: infinite)
- `--server`: Kafka server (default: localhost:9092)

**Example Output:**
```
============================================================
Inventory Processing Summary:
  Total Processed: 1000
  Available: 900
  Out of Stock: 100
  Failure Rate: 10.00%
  Time Elapsed: 3.12s
  Throughput: 320.51 orders/sec
============================================================
```

---

### Analytics Consumer - Compute Metrics

**Basic Usage:**
```bash
python3 analytics_consumer/app.py --max-messages 2000
```

**With Custom Consumer Group:**
```bash
python3 analytics_consumer/app.py --group analytics-v2
```

**Parameters:**
- `--group`: Consumer group ID (default: analytics-group)
- `--max-messages`: Max messages to process (default: infinite)
- `--report-interval`: Show metrics every N messages (default: 5000)
- `--server`: Kafka server (default: localhost:9092)

**Example Output:**
```
================================================================================
ANALYTICS METRICS REPORT (Group: analytics-group)
================================================================================

📊 OVERALL METRICS:
  Total Orders Received: 1000
  Total Inventory Checks: 1000
  Failed Orders: 98
  Failure Rate: 9.80%

⏱️  ORDERS PER MINUTE:
  2026-02-15 10:23: 453 orders
  2026-02-15 10:24: 547 orders
  
  Average: 500.00 orders/min
  Peak: 547 orders/min

🍽️  ORDERS BY RESTAURANT:
  Student Union: 267 orders ($3854.67)
  Spartan Eats: 249 orders ($3718.24)
  Tower Cafe: 251 orders ($3698.19)
  Engineering Hub: 233 orders ($3489.45)
================================================================================

Metrics saved to: metrics_report.txt
```

---

## 🔄 Common Workflows

### Workflow 1: Basic Data Flow
```bash
# Terminal 1 - Produce orders
python3 producer_order/app.py --count 500

# Terminal 2 - Process inventory
python3 inventory_consumer/app.py --max-messages 500

# Terminal 3 - Analyze
python3 analytics_consumer/app.py --max-messages 1000
```

### Workflow 2: Continuous Streaming
```bash
# Terminal 1 - Continuous production
python3 producer_order/app.py --count 10000 --delay 10

# Terminal 2 - Real-time inventory processing
python3 inventory_consumer/app.py

# Terminal 3 - Real-time analytics
python3 analytics_consumer/app.py
```

### Workflow 3: Replay for Analytics
```bash
# First run
python3 analytics_consumer/app.py --group replay-demo --max-messages 5000

# Reset consumer offset to beginning
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:29092 \
  --group replay-demo \
  --reset-offsets \
  --to-earliest \
  --all-topics \
  --execute

# Replay - should produce consistent metrics
python3 analytics_consumer/app.py --group replay-demo --max-messages 5000
```

## 📈 Monitoring & Verification

### Kafka UI
Access at http://localhost:8080
- View topics and messages
- Monitor consumer groups and lag
- Check partition distribution

### Check Kafka Topics
```bash
docker exec kafka kafka-topics \
  --list \
  --bootstrap-server localhost:29092
```

Expected topics:
- `order-events`
- `inventory-events`

### View Messages in Topic
```bash
# View order events
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic order-events \
  --from-beginning \
  --max-messages 5

# View inventory events
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic inventory-events \
  --from-beginning \
  --max-messages 5
```

### Check Consumer Groups
```bash
# List all consumer groups
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:29092 \
  --list

# Check specific group lag
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:29092 \
  --group inventory-group \
  --describe
```

### Consumer Lag Monitoring
```bash
# Check lag for analytics group
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:29092 \
  --group analytics-group \
  --describe
```

Output shows:
- Current offset (where consumer is)
- Log end offset (latest message)
- Lag (difference between the two)

## 🔧 Configuration

### Kafka Topics
- `order-events`: 3 partitions, replication factor 1
- `inventory-events`: 3 partitions, replication factor 1

### Campus Restaurants
- **Spartan Eats**: Burgers, pizza, sandwiches
- **Tower Cafe**: Coffee, bagels, wraps
- **Engineering Hub**: Ramen, sushi, bento
- **Student Union**: Tacos, burritos, nachos

## 🛠️ Troubleshooting

### Kafka not starting
```bash
# Check logs
docker-compose logs kafka

# Restart services
docker-compose down
docker-compose up -d
```

### Consumer not receiving messages
```bash
# Check topic exists
docker exec kafka kafka-topics --list --bootstrap-server localhost:29092

# Check messages in topic
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic order-events \
  --from-beginning \
  --max-messages 5
```

### Reset everything
```bash
# Stop all services
docker-compose down -v  # -v removes volumes

# Restart
docker-compose up -d
```

## 🎓 Key Implementation Concepts

1. **Event Streaming**: Producer publishes events, multiple consumers process independently
2. **Consumer Groups**: Multiple instances can process messages in parallel
3. **Offset Management**: Consumers track their position in the stream
4. **Replay Capability**: Reset offset to reprocess messages (idempotent analytics)
5. **Consumer Lag**: Monitoring backpressure when consumers fall behind
6. **Failure Handling**: System continues despite inventory failures
7. **Real-time Metrics**: Aggregating data from multiple event streams

## 🏗️ Implementation Details

### Data Models (common/ids.py)

**OrderEvent:**
```python
{
    "order_id": "ORD-000001",
    "student_id": "STU-0001", 
    "restaurant": "Spartan Eats",
    "items": ["burger", "fries"],
    "total_amount": 12.99,
    "timestamp": "2026-02-15T10:23:15.123456",
    "status": "placed"
}
```

**InventoryEvent:**
```python
{
    "order_id": "ORD-000001",
    "restaurant": "Spartan Eats",
    "inventory_status": "available",
    "items_available": true,
    "timestamp": "2026-02-15T10:24:05.234567",
    "processing_time_ms": 5.23
}
```

### Campus Restaurants
- **Spartan Eats**: Burgers, pizza, sandwiches
- **Tower Cafe**: Coffee, bagels, wraps
- **Engineering Hub**: Ramen, sushi, bento
- **Student Union**: Tacos, burritos, nachos

### Kafka Configuration
- **Topics**: 3 partitions each, replication factor 1
- **Producer**: Acks=all, ensures message ordering
- **Consumers**: Auto-commit enabled, earliest offset for new groups

## 📚 Additional Resources

- [Kafka Python Documentation](https://kafka-python.readthedocs.io/)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Consumer Groups Explained](https://kafka.apache.org/documentation/#consumergroups)
