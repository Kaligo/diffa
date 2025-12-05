from datetime import date
from typing import Optional, List, Tuple, Any
from dataclasses import dataclass, fields, make_dataclass
from functools import reduce
import uuid

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
from sqlalchemy.dialects.postgresql import UUID
from pydantic import BaseModel, model_validator

from diffa.config import ConfigManager
from diffa.utils import Logger

logger = Logger(__name__)
Base = declarative_base()
config = ConfigManager()


class DiffaCheck(Base):
    """SQLAlchemy Model for Diffa state management"""

    __tablename__ = config.diffa_check.get_db_table()
    metadata = MetaData(schema=config.diffa_check.get_db_schema())
    id = Column(UUID, primary_key=True)
    source_database = Column(String)
    source_schema = Column(String)
    source_table = Column(String)
    target_database = Column(String)
    target_schema = Column(String)
    target_table = Column(String)
    check_date = Column(Date)
    source_count = Column(Integer)
    target_count = Column(Integer)
    is_valid = Column(Boolean)
    diff_count = Column(Integer)
    updated_at = Column(DateTime)


class DiffaCheckSchema(BaseModel):
    """Pydantic Model (validation) for Diffa state management"""

    id: uuid.UUID = None
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
        return uuid.uuid5(uuid.NAMESPACE_DNS, hash_input)

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


class DiffaCheckRun(Base):
    """SQLAlchemy Model for Diffa state management"""

    __tablename__ = config.diffa_check_run.get_db_table()
    metadata = MetaData(schema=config.diffa_check_run.get_db_schema())
    run_id = Column(UUID, primary_key=True)
    source_database = Column(String)
    source_schema = Column(String)
    source_table = Column(String)
    target_database = Column(String)
    target_schema = Column(String)
    target_table = Column(String)
    status = Column(String)
    updated_at = Column(DateTime)


class DiffaCheckRunSchema(BaseModel):
    """Pydantic Model (validation) for Diffa state management"""

    run_id: uuid.UUID = None
    source_database: str
    source_schema: str
    source_table: str
    target_database: str
    target_schema: str
    target_table: str
    status: str

    @classmethod
    def create_id(cls):
        """Create a unique ID for a single diffa check run"""
        return uuid.uuid4()

    class Config:
        from_attributes = (
            True  # Enable ORM mode to allow loading from SQLAlchemy models
        )
        validate_assignment = True

    @model_validator(mode="after")
    def set_id_if_missing(self):
        if self.run_id is None:
            self.run_id = self.create_id()
        return self

    @model_validator(mode="after")
    def validate_status(self):
        if self.status not in ["RUNNING", "COMPLETED", "FAILED"]:
            raise ValueError(f"Invalid status: {self.status}")
        return self


@dataclass(frozen=True)
class CountCheck:
    """A single count check in Source/Target Database"""

    cnt: int
    check_date: date

    @classmethod
    def create_with_dimensions(cls, dimension_cols: Optional[List[str]] = None):
        """Factory method to create a CountCheck class with dimension fields"""

        return make_dataclass(
            cls.__name__,
            [(col, str) for col in sorted(dimension_cols)] if dimension_cols else [],
            bases=(cls,),
            frozen=True,
        )

    @classmethod
    def get_base_fields(cls) -> List[Tuple[str, type]]:
        return [("check_date", date), ("cnt", int)]

    @classmethod
    def get_dimension_fields(cls) -> List[Tuple[str, type]]:
        base_fields = {name for name, _ in cls.get_base_fields()}

        return [(f.name, f.type) for f in fields(cls) if f.name not in base_fields]

    def get_dimension_values(self):
        # check_date is still considered as a dimension field. In fact, it's a main dimension field.
        return {
            f[0]: getattr(self, f[0])
            for f in self.get_dimension_fields() + [("check_date", date)]
        }

    def to_flatten_dimension_format(self) -> dict:
        return {tuple(self.get_dimension_values().items()): self}


class MergedCountCheck:
    """A merged count check after checking count in Source/Target Databases"""

    def __init__(
        self,
        source_count: int,
        target_count: int,
        check_date: date,
        is_valid: Optional[bool] = None,
        **kwargs: Any,
    ):
        self.source_count = source_count
        self.target_count = target_count
        self.check_date = check_date
        for key, value in kwargs.items():
            setattr(self, key, value)

        self.is_valid = (
            is_valid if is_valid is not None else source_count <= target_count
        )

    def __eq__(self, other):
        if not isinstance(other, MergedCountCheck):
            return NotImplemented
        return self.__dict__ == other.__dict__

    def __lt__(self, other):
        if not isinstance(other, MergedCountCheck):
            return NotImplemented
        dynamic_fields = [
            f
            for f in self.__dict__.keys()
            if f not in ["source_count", "target_count", "check_date", "is_valid"]
        ]
        precedence = (
            ["check_date"]
            + dynamic_fields
            + ["source_count", "target_count", "is_valid"]
        )

        return tuple(getattr(self, f) for f in precedence) < tuple(
            getattr(other, f) for f in precedence
        )

    def __str__(self):
        return f"MergedCountCheck({", ".join(f"{k}={v!r}" for k, v in self.__dict__.items())})"

    @classmethod
    def create_with_dimensions(cls, dimension_fields: List[Tuple[str, type]]):
        """Factory method to dynamically create a MergedCountCheck with a CountCheck schema"""

        return type(
            cls.__name__,
            (cls,),
            reduce(
                lambda x, y: x | y, map(lambda x: {x[0]: x[1]}, dimension_fields), {}
            ),
        )

    @classmethod
    def from_counts(
        cls, source: Optional[CountCheck] = None, target: Optional[CountCheck] = None
    ):
        count_check = source if source else target
        merged_count_check_values = count_check.get_dimension_values()
        merged_count_check_values["source_count"] = source.cnt if source else 0
        merged_count_check_values["target_count"] = target.cnt if target else 0

        return cls(**merged_count_check_values)

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
