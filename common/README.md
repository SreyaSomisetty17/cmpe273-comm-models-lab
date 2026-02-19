# Common Utilities

Shared helpers used by all three communication-model labs.

| File | Purpose |
|------|---------|
| `ids.py` | `generate_order_id()` – creates short, unique order IDs (`ORD-<ts>-<hex>`) |

### Data Models (used by streaming-kafka)
- `OrderEvent` – Complete order data for Kafka events
- `InventoryEvent` – Inventory check results for Kafka events
- `OrderStatus` & `InventoryStatus` – Status enums

### Shared Data
- `CAMPUS_RESTAURANTS` – Dictionary of campus restaurants and menus

## Usage

### For REST/RabbitMQ
```python
from common.ids import generate_order_id
order_id = generate_order_id()  # "ORD-123456-a1b2c3"
```

### For Kafka Streaming
```python
from common.ids import OrderEvent, CAMPUS_RESTAURANTS
order = OrderEvent(...)
```