"""
Shared ID generation utility used across all services.
Generates short, human-readable unique order IDs.
"""

import time
import uuid


def generate_order_id() -> str:
    """Generate a unique order ID with a readable prefix."""
    ts = int(time.time() * 1000) % 1_000_000
    short = uuid.uuid4().hex[:6]
    return f"ORD-{ts}-{short}"


if __name__ == "__main__":
    for _ in range(5):
        print(generate_order_id())

# KAFKA STREAMING DATA MODELS (Added for Part C)

from dataclasses import dataclass, asdict
from datetime import datetime
from enum import Enum
import json


class OrderStatus(Enum):
    PLACED = "placed"
    CONFIRMED = "confirmed"
    PREPARING = "preparing"
    READY = "ready"
    DELIVERED = "delivered"
    FAILED = "failed"


class InventoryStatus(Enum):
    AVAILABLE = "available"
    LOW_STOCK = "low_stock"
    OUT_OF_STOCK = "out_of_stock"


@dataclass
class OrderEvent:
    """Order event data model for Kafka streaming"""
    order_id: str
    student_id: str
    restaurant: str
    items: list
    total_amount: float
    timestamp: str
    status: str = OrderStatus.PLACED.value
    
    def to_json(self):
        return json.dumps(asdict(self))
    
    @classmethod
    def from_json(cls, json_str):
        data = json.loads(json_str)
        return cls(**data)


@dataclass
class InventoryEvent:
    """Inventory event data model for Kafka streaming"""
    order_id: str
    restaurant: str
    inventory_status: str
    items_available: bool
    timestamp: str
    processing_time_ms: float = 0.0
    
    def to_json(self):
        return json.dumps(asdict(self))
    
    @classmethod
    def from_json(cls, json_str):
        data = json.loads(json_str)
        return cls(**data)


# ============================================================================
# SHARED CAMPUS DATA
# ============================================================================

CAMPUS_RESTAURANTS = {
    "Spartan Eats": ["burger", "fries", "salad", "pizza", "sandwich"],
    "Tower Cafe": ["coffee", "bagel", "muffin", "wrap", "smoothie"],
    "Engineering Hub": ["ramen", "sushi", "bento", "rice bowl", "tea"],
    "Student Union": ["tacos", "burrito", "nachos", "quesadilla", "chips"],
}


def generate_student_id(index: int) -> str:
    """Generate student ID for testing"""
    return f"STU-{(index % 1000):04d}"


def get_current_timestamp() -> str:
    """Get current timestamp in ISO format"""
    return datetime.utcnow().isoformat()


if __name__ == "__main__":
    print("Testing ID generation:")
    for _ in range(5):
        print(f"  {generate_order_id()}")
    
    print("\nCampus Restaurants:")
    for name, items in CAMPUS_RESTAURANTS.items():
        print(f"  {name}: {', '.join(items)}")