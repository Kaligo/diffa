from datetime import date, datetime
from typing import Optional
from dataclasses import dataclass, field
import hashlib

from sqlalchemy import (
    Column,
    Integer,
    String,
    MetaData,
    Date,
    Boolean,
    DateTime,
)
from sqlalchemy.ext.declarative import declarative_base
from pydantic import BaseModel, model_validator

from diffa.config import ConfigManager
from diffa.utils import Logger

logger = Logger(__name__)
Base = declarative_base()
config = ConfigManager()


class DiffaCheck(Base):
    """SQLAlchemy Model for Diffa state management"""

    __tablename__ = config.get_table("diffa")
    metadata = MetaData(schema=config.get_schema("diffa"))
    id = Column(String, primary_key=True)
    source_database = Column(String, primary_key=True)
    source_schema = Column(String, primary_key=True)
    source_table = Column(String, primary_key=True)
    target_database = Column(String, primary_key=True)
    target_schema = Column(String, primary_key=True)
    target_table = Column(String, primary_key=True)
    check_date = Column(Date, primary_key=True)
    source_count = Column(Integer)
    target_count = Column(Integer)
    is_valid = Column(Boolean)
    diff_count = Column(Integer)
    updated_at = Column(DateTime, default=datetime.utcnow)


class DiffaCheckSchema(BaseModel):
    """Pydantic Model (validation) for Diffa state management"""

    id: str = None
    source_database: str
    source_schema: str
    source_table: str
    target_database: str
    target_schema: str
    target_table: str
    check_date: date
    source_count: int
    target_count: int
    is_valid: bool
    diff_count: int

    @classmethod
    def create_id(
        cls,
        source_database: str,
        source_schema: str,
        source_table: str,
        target_database: str,
        target_schema: str,
        target_table: str,
        check_date: date,
    ):
        """Create a unique ID for the diffa check"""
        hash_input = f"{source_database}{source_schema}{source_table}{target_database}{target_schema}{target_table}{check_date}"
        return hashlib.sha256(hash_input.encode()).hexdigest()

    class Config:
        from_attributes = (
            True  # Enable ORM mode to allow loading from SQLAlchemy models
        )
        validate_assignment = True

    @model_validator(mode="after")
    def set_id_if_missing(self):
        if self.id is None:
            self.id = self.create_id(
                self.source_database,
                self.source_schema,
                self.source_table,
                self.target_database,
                self.target_schema,
                self.target_table,
                self.check_date,
            )
        return self


@dataclass
class CountCheck:
    """A single count check in Source/Target Database"""

    cnt: int
    check_date: date


@dataclass
class MergedCountCheck:
    """A merged count check after checking count in Source/Target Databases"""

    source_count: int
    target_count: int
    check_date: date
    is_valid: bool = field(init=False)

    def __post_init__(self):
        self.is_valid = True if self.source_count <= self.target_count else False

    @classmethod
    def from_counts(
        cls, source: Optional[CountCheck] = None, target: Optional[CountCheck] = None
    ):
        if source and target:
            if source.check_date != target.check_date:
                raise ValueError("Source and target counts are not for the same date.")
        elif not source and not target:
            raise ValueError("Both source and target counts are missing.")

        check_date = source.check_date if source else target.check_date
        source_count = source.cnt if source else 0
        target_count = target.cnt if target else 0

        return cls(source_count, target_count, check_date)

    def to_diffa_check_schema(
        self,
        source_database: str,
        source_schema: str,
        source_table: str,
        target_database: str,
        target_schema: str,
        target_table: str,
    ) -> DiffaCheckSchema:
        """Convert the merged count check to a DiffaCheckSchema"""

        if not self.is_valid:
            logger.info(
                "Diff: "
                f"Source Count: {self.source_count}, "
                f"Target Count: {self.target_count}, "
                f"Check Date: {self.check_date}, "
                f"Is Valid: {self.is_valid} "
            )

        return DiffaCheckSchema(
            source_database=source_database,
            source_schema=source_schema,
            source_table=source_table,
            target_database=target_database,
            target_schema=target_schema,
            target_table=target_table,
            check_date=self.check_date,
            source_count=self.source_count,
            target_count=self.target_count,
            is_valid=self.is_valid,
            diff_count=self.target_count - self.source_count,
        )
