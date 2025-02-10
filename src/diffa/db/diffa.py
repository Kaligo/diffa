from typing import Iterable, Optional
import uuid
from datetime import datetime

from sqlalchemy import create_engine, Column, Integer, String, DateTime, MetaData, UUID
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from pydantic import BaseModel, model_validator
from typing_extensions import Self

from diffa.config import ConfigManager
from diffa.db.base import Database
from diffa.utils import Logger

logger = Logger(__name__)
Base = declarative_base()
config = ConfigManager()

class DiffRecord(Base):
    """SQLAlchemy Model for Diffa state management"""

    __tablename__ = config.get_table("diffa")
    metadata = MetaData(schema=config.get_schema("diffa"))
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    source_database = Column(String)
    source_schema = Column(String)
    source_table = Column(String)
    target_database = Column(String)
    target_schema = Column(String)
    target_table = Column(String)
    start_check_date = Column(DateTime)
    end_check_date = Column(DateTime)
    source_count = Column(Integer)
    target_count = Column(Integer)
    status = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)
    last_reconciled_at = Column(DateTime)


class DiffRecordSchema(BaseModel):
    """Pydantic Model (validation) for Diffa state management"""

    id: Optional[uuid.UUID] = uuid.uuid4()
    source_database: str
    source_schema: str
    source_table: str
    target_database: str
    target_schema: str
    target_table: str
    start_check_date: datetime
    end_check_date: datetime
    source_count: int
    target_count: int
    status: str
    created_at: datetime = datetime.utcnow()
    last_reconciled_at: Optional[datetime] = None

    class Config:
        from_attributes = True  # Enable ORM mode to allow loading from SQLAlchemy models

    @model_validator(mode="after")
    def validate_status(self) -> Self:
        allowed = {"valid", "invalid", "reconciled"}
        if self.status not in allowed:
            raise ValueError(f"status must be one of {allowed}")
        return self

class SQLAlchemyDiffaDatabase(Database):
    """SQLAlchemy Database Adapter for Diffa state management"""

    def __init__(self, db_config: dict):
        super().__init__(db_config)
        self.engine = None
        self.session = None

    def connect(self):
        self.engine = create_engine(self.db_config["db_url"] + "?sslmode=prefer") # Prefer SSL mode
        self.session = sessionmaker(bind=self.engine)()

    def execute_query(self, query: str, sql_params: dict = None):
        self.connect()
        try:
            result = self.session.execute(query, sql_params)
            for row in result:
                yield dict(row)
        finally:
            self.close()

    def execute_non_query(self, query, params: dict = None):
        self.connect()
        try:
            with self.session.begin(): # Ensures transaction integrity
                self.session.execute(query, params)
        finally:
            self.close()

    def close(self):
        self.session.close()

    def get_invalid_diff_records(self) -> Iterable[DiffRecordSchema]:
        """Get all invalid diff records"""
        self.connect()
        try:
            diff_records = (
                self.session.query(DiffRecord)
                .filter(DiffRecord.status == "Invalid")
                .all()
            )
            for record in diff_records:
                yield DiffRecordSchema.model_validate(record)
        except Exception as e:
            logger.error(f"An error occurred: {e}")
            raise e
        finally:
            self.close()

    def save_diff_record(self, diff_record: DiffRecordSchema):
        """Save a diff record"""
        self.connect()
        record_dict = diff_record.model_dump()
        try:
            self.session.add(DiffRecord(**record_dict))
            self.session.commit()
        except Exception as e:
            logger.error(f"An error occurred: {e}")
            raise e
        finally:
            self.close()

    def create_diff_table(self):
        """Create the diff table"""
        self.connect()
        try:
            DiffRecord.metadata.create_all(self.engine)
        finally:
            self.close()
