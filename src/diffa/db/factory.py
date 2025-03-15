from diffa.db.base import Database
from diffa.db.postgresql import PosgrestDatabase

class DatabaseFactory:
    """Factory pattern to select the correct database adapter."""

    _adapter = {
        "postgresql": PosgrestDatabase,
        "redshift": PosgrestDatabase,
    }

    @staticmethod
    def create_database(db_config: dict) -> Database:
        """Create a database adapter"""
        scheme = db_config["scheme"]
        if scheme not in DatabaseFactory._adapter:
            raise ValueError(f"Invalid database type: {scheme}")
        return DatabaseFactory._adapter[scheme](db_config)
