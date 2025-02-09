from abc import ABC, abstractmethod


class Database(ABC):
    """ Base class for all database adapters """
    def __init__(self, db_config: dict):
        self.db_config = db_config
        self.conn = None

    @abstractmethod
    def connect(self):
        """ Connect to the database """

    @abstractmethod
    def execute_query(self, query: str):
        """ Execute a query """

    @abstractmethod
    def execute_non_query(self, query: str, params: dict = None):
        """ Execute a non-query """

    def close(self):
        """ Close the database connection """
