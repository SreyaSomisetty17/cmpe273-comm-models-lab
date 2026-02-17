# CMPE 273 - Communication Models Lab

Campus food ordering workflow implemented using three communication models:
- **Part A**: Synchronous REST
- **Part B**: Async Messaging with RabbitMQ
- **Part C**: Streaming with Kafka

---

# Part A: Synchronous REST

## Architecture

```
Client ──▶ OrderService ──▶ InventoryService
                │                  │
                │◀─── reserve ─────┘
                │
                └──▶ NotificationService
                           │
                ◀─── send ─┘
```

All calls are **synchronous and blocking**. OrderService waits for each downstream service before proceeding.

## Services

| Service | Port | Endpoints |
|---------|------|-----------|
| OrderService | 8080 | `POST /order` |
| InventoryService | 8081 | `POST /reserve`, `POST /inject` |
| NotificationService | 8082 | `POST /send` |

## Build and Run

```bash
docker compose -f sync-rest/docker-compose.yml up --build
```

## Run Tests

```bash
cd sync-rest/tests
mvn clean test
```

## Failure Injection

Use InventoryService `/inject` endpoint to simulate delay or failure:

```bash
# Add 2s delay
curl -X POST http://localhost:8081/inject -H "Content-Type: application/json" \
  -d '{"delayMs":2000,"forceFail":false}'

# Force failure
curl -X POST http://localhost:8081/inject -H "Content-Type: application/json" \
  -d '{"delayMs":0,"forceFail":true}'

# Reset to normal
curl -X POST http://localhost:8081/inject -H "Content-Type: application/json" \
  -d '{"delayMs":0,"forceFail":false}'
```

---

## Test Results

| Scenario | Avg Latency (ms) | P95 (ms) | Status |
|----------|------------------|----------|--------|
| Baseline (N=20) | 11.95 | 19 | 200 OK |
| Inventory 2s delay | 2085.8 | 2301 | 200 OK |
| Inventory failure | - | - | 502 Bad Gateway |

## Reasoning

**Delay propagation**: Synchronous calls block. OrderService waits for Inventory before calling Notification. A 2s delay in Inventory adds ~2s to total response time.

**Failure handling**: When Inventory fails, OrderService returns HTTP 502. Notification is never called—the chain stops at the failed step.

**Key insight**: Sync REST creates tight coupling. Any downstream delay or failure directly impacts client response time.
