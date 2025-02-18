from typing import Optional
from datetime import datetime, date

from sqlalchemy import create_engine, Column, Integer, String, DateTime, MetaData, UUID, Date
from sqlalchemy.sql.functions import now
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
    source_database = Column(String, primary_key=True)
    source_schema = Column(String, primary_key=True)
    source_table = Column(String, primary_key=True)
    target_database = Column(String, primary_key=True)
    target_schema = Column(String, primary_key=True)
    target_table = Column(String, primary_key=True)
    check_date = Column(Date)
    source_count = Column(Integer)
    target_count = Column(Integer)
    status = Column(String)
    updated_at = Column(DateTime, default=now(), onupdate=now())


class DiffRecordSchema(BaseModel):
    """Pydantic Model (validation) for Diffa state management"""

    source_database: str
    source_schema: str
    source_table: str
    target_database: str
    target_schema: str
    target_table: str
    check_date: date
    source_count: int
    target_count: int
    status: str
    updated_at: datetime = datetime.utcnow()

    class Config:
        from_attributes = (
            True  # Enable ORM mode to allow loading from SQLAlchemy models
        )

    @model_validator(mode="after")
    def validate_status(self) -> Self:
        allowed = {"match", "mismatch"}
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
        self.engine = create_engine(
            self.db_config["db_url"] + "?sslmode=prefer"
        )  # Prefer SSL mode
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
            with self.session.begin():  # Ensures transaction integrity
                self.session.execute(query, params)
        finally:
            self.close()

    def close(self):
        self.session.close()

    def get_latest_record(
        self,
        source_database: str,
        source_schema: str,
        source_table: str,
        target_database: str,
        target_schema: str,
        target_table: str,
    ) -> Optional[dict]:
        """Get the mismatch record. If not found, return None"""
        self.connect()
        try:
            diff_record = (
                self.session.query(DiffRecord)
                .filter(DiffRecord.source_database == source_database)
                .filter(DiffRecord.source_schema == source_schema)
                .filter(DiffRecord.source_table == source_table)
                .filter(DiffRecord.target_database == target_database)
                .filter(DiffRecord.target_schema == target_schema)
                .filter(DiffRecord.target_table == target_table)
                .order_by(DiffRecord.updated_at.desc())
                .first()
            )
            return (
                DiffRecordSchema.model_validate(diff_record).model_dump()
                if diff_record
                else None
            )
        finally:
            self.close()

    def save_diff_record(self, diff_record: DiffRecordSchema):
        """Save a diff record"""
        self.connect()
        record_dict = diff_record.model_dump()
        try:
            self.session.merge(DiffRecord(**record_dict))
            self.session.commit()
        finally:
            self.close()
