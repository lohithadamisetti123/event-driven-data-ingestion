from pydantic import BaseModel, Field
from typing import Dict, Any
from uuid import uuid4
from datetime import datetime

class RawEvent(BaseModel):
    event_id: str = Field(
        default_factory=lambda: str(uuid4()),
        description="Unique ID for the event",
    )
    event_type: str = Field(
        ...,
        description="Type of the event, e.g., 'user_registered', 'product_viewed'",
        min_length=1,
        max_length=50,
    )
    timestamp: datetime = Field(
        ...,
        description="ISO 8601 formatted timestamp of the event",
    )
    data: Dict[str, Any] = Field(
        ...,
        description="Arbitrary event payload data",
    )
