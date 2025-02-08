import datetime
from sqlalchemy import Column, Integer, String, DateTime, MetaData, UUID
from sqlalchemy.ext.declarative import declarative_base
from pydantic import BaseModel, model_validator
from datetime import datetime
from typing import Optional
import uuid
from typing_extensions import Self
from diffa.config import ConfigManager

Base = declarative_base()
config = ConfigManager()

class DiffRecord(Base):
    __tablename__ = config.get_table("diffa")
    metadata = MetaData(schema=config.get_schema("diffa"))
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    table_name = Column(String)
    start_check_date = Column(DateTime)
    end_check_date = Column(DateTime)
    source_count = Column(Integer)
    target_count = Column(Integer)
    status = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)
    last_reconciled_at = Column(DateTime)


class DiffRecordSchema(BaseModel):
    id: Optional[int] = uuid.uuid4()
    table_name: str
    start_check_date: datetime
    end_check_date: datetime
    source_count: int
    target_count: int
    status: str
    created_at: datetime = datetime.utcnow()
    last_reconciled_at: Optional[datetime] = None

    class Config:
        orm_mode = True  # Enable ORM mode to allow loading from SQLAlchemy models

    @model_validator(mode="after")
    def validate_status(self) -> Self:
        allowed = {"valid", "invalid", "reconciled"}
        if self.status not in allowed:
            raise ValueError(f"status must be one of {allowed}")
        return self
