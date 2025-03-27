from abc import ABC, abstractmethod
from contextlib import contextmanager

import psycopg2
import psycopg2.extras
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class Connection(ABC):
    """Base class for all database connection adapters"""

    def __init__(self, db_config: dict):
        self.db_config = db_config

    @abstractmethod
    def connect(self):
        """Connect to the database"""

    def close(self):
        """Close the database connection"""


class PostgresConnection(Connection):
    """Connection adapter for PostgreSQL"""

    def __init__(self, db_config: dict):
        super().__init__(db_config)
        self.conn = None

    def connect(self):
        if not self.conn:
            self.conn = psycopg2.connect(
                host=self.db_config["host"],
                port=self.db_config["port"],
                database=self.db_config["database"],
                user=self.db_config["user"],
                password=self.db_config["password"],
                sslmode="prefer",  # Prefer SSL mode
            )
            self.conn.set_session(autocommit=True)
        return self.conn

    def close(self):
        if self.conn is not None:
            self.conn.close()
        self.conn = None


class DiffaConnection(Connection):
    """Connection adapter for Diffa State DB"""

    def __init__(self, db_config: dict):
        super().__init__(db_config)
        self.conn = None

    def __get_engine(self):
        return create_engine(
            self.db_config["db_uri"] + "?sslmode=prefer"
        )  # Prefer SSL mode

    def connect(self):
        if not self.conn:
            self.conn = self.__get_engine().connect()
        return self.conn

    @contextmanager
    def db_session(self):
        """Context Manager for DB session"""

        session = sessionmaker(bind=self.__get_engine())()
        yield session
        session.close()

    def close(self):
        if not self.conn:
            self.conn.close()
        self.conn = None
